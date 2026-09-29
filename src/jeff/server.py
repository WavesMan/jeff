"""Serve a Jeff checkpoint through the decision API: a situation and questions in, one answer per question out."""

from __future__ import annotations

import asyncio
import base64
import binascii
import hmac
import os
import threading
import time
import uuid
from _thread import LockType
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Literal, cast

from fastapi import Depends, FastAPI, Header, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import HTMLResponse, JSONResponse, Response
from PIL import Image, UnidentifiedImageError
from pydantic import BaseModel, ConfigDict, Field, JsonValue, field_validator
from starlette.concurrency import run_in_threadpool
from starlette.middleware.base import RequestResponseEndpoint

from jeff.types import Answer, DecisionInput, DecisionResponse, JSONValue, Question as DecisionQuestion

if TYPE_CHECKING:
    from jeff.model import DecisionModel

type Content = str | dict[str, JsonValue] | list[JsonValue]
DEFAULT_MODEL = "jeff-qwen3.8-27b"
ALIASES = {"jeff", "jeff-latest", DEFAULT_MODEL}


@dataclass
class Service:
    model: DecisionModel | None = None
    name: str = DEFAULT_MODEL
    checkpoint: str = "checkpoints/selected"
    release_date: str = ""
    lock: LockType = field(default_factory=threading.Lock)
    batch_queue: BatchQueue | None = None


service = Service()


class Question(BaseModel):
    model_config = ConfigDict(extra="forbid")
    instructions: Content | None = None


class Choice(Question):
    type: Literal["choice"]
    criteria: dict[str, Content | None] = Field(min_length=1, max_length=255)


class Score(Question):
    type: Literal["score"]
    criteria: list[Content] = Field(min_length=2, max_length=10)


class Noul(Question):
    type: Literal["noul"]
    criteria: dict[Literal["true", "false"], Content | None] | None = None


class EvaluationRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    model: str
    state: Content
    questions: dict[str, Annotated[Choice | Score | Noul, Field(discriminator="type")]] = Field(min_length=1)
    images: list[str] = Field(default_factory=list, max_length=4)

    @field_validator("model")
    @classmethod
    def known_model(cls, value: str) -> str:
        if value not in ALIASES | {service.name}:
            raise ValueError(f"Unknown model. Use {service.name} or jeff-latest.")
        return value

    @field_validator("images")
    @classmethod
    def valid_images(cls, values: list[str]) -> list[str]:
        for value in values:
            if len(value) > 12_000_000:
                raise ValueError("Each image must be at most 8 MB before base64 encoding.")
            header, separator, encoded = value.partition(",")
            if not separator or header not in {
                "data:image/png;base64", "data:image/jpeg;base64", "data:image/webp;base64",
            }:
                raise ValueError("Images must be base64 PNG, JPEG, or WebP data URLs.")
            try:
                content = base64.b64decode(encoded, validate=True)
                if len(content) > 8_000_000:
                    raise ValueError("Each image must be at most 8 MB.")
                with Image.open(BytesIO(content)) as image:
                    if image.width * image.height > 16_000_000:
                        raise ValueError("Each image must have at most 16 million pixels.")
                    if image.format not in {"PNG", "JPEG", "WEBP"}:
                        raise ValueError("Unsupported image format.")
                    image.verify()
            except (binascii.Error, OSError, SyntaxError, UnidentifiedImageError, Image.DecompressionBombError) as error:
                raise ValueError("Invalid image data.") from error
        return values


class BatchEvaluationRequest(BaseModel):
    """A bounded collection of ordinary System One requests."""

    model_config = ConfigDict(extra="forbid")
    model: str
    requests: list[EvaluationRequest] = Field(min_length=1, max_length=4)

    @field_validator("model")
    @classmethod
    def known_model(cls, value: str) -> str:
        if value not in ALIASES | {service.name}:
            raise ValueError(f"Unknown model. Use {service.name} or jeff-latest.")
        return value


@dataclass
class PendingBatch:
    body: BatchEvaluationRequest
    future: asyncio.Future[dict[str, JSONValue]]
    submitted_at: float


class BatchQueue:
    """Bounded, FIFO model queue for the batch API.

    The gateway performs cross-HTTP-request aggregation.  This queue makes the
    model-side API safe for direct callers too: it never exposes the legacy
    endpoint's 529 busy-lock behaviour and it applies backpressure at 32 jobs.
    """

    def __init__(self, maximum: int, wait_ms: int) -> None:
        self.queue: asyncio.Queue[PendingBatch] = asyncio.Queue(maxsize=maximum)
        self.wait_seconds = wait_ms / 1000
        self.worker: asyncio.Task[None] | None = None

    def start(self) -> None:
        self.worker = asyncio.create_task(self.run())

    async def close(self) -> None:
        if self.worker is not None:
            self.worker.cancel()
            try:
                await self.worker
            except asyncio.CancelledError:
                pass

    async def submit(self, body: BatchEvaluationRequest) -> dict[str, JSONValue]:
        if self.worker is None:
            raise RuntimeError("Batch queue is not running.")
        future: asyncio.Future[dict[str, JSONValue]] = asyncio.get_running_loop().create_future()
        try:
            self.queue.put_nowait(PendingBatch(body, future, time.perf_counter()))
        except asyncio.QueueFull as error:
            raise HTTPException(429, "The batch queue is full. Retry shortly.", headers={"Retry-After": "1"}) from error
        return await asyncio.shield(future)

    async def run(self) -> None:
        while True:
            pending = await self.queue.get()
            # A short delay lets independently arriving direct batch requests
            # share the single model forward pass, while preserving FIFO order.
            group = [pending]
            deadline = time.perf_counter() + self.wait_seconds
            while len(group) < int(os.getenv("JEFF_MAX_BATCH_SIZE", "4")):
                remaining = deadline - time.perf_counter()
                if remaining <= 0:
                    break
                try:
                    group.append(await asyncio.wait_for(self.queue.get(), remaining))
                except TimeoutError:
                    break
            model = service.model
            started = time.perf_counter()
            try:
                if model is None:
                    raise RuntimeError("The model is not ready.")
                bodies = [request for item in group for request in item.body.requests]
                responses, input_tokens = await run_in_threadpool(predict_many, model, bodies)
                inference_ms = (time.perf_counter() - started) * 1000
                position = 0
                for item in group:
                    count = len(item.body.requests)
                    answers = [response["answers"] for response in responses[position:position + count]]
                    position += count
                    item.future.set_result({
                        "model": service.name,
                        "answers": answers,
                        "usage": {
                            "input_tokens": input_tokens,
                            "output_tokens": 0,
                            "batch_size": len(bodies),
                            "queue_wait_ms": round((started - item.submitted_at) * 1000, 3),
                            "inference_ms": round(inference_ms, 3),
                        },
                    })
            except Exception as error:
                for item in group:
                    if not item.future.done():
                        item.future.set_exception(error)
            finally:
                for _ in group:
                    self.queue.task_done()


def authenticate(authorization: str | None = Header(default=None)) -> None:
    key = os.getenv("JEFF_API_KEY")
    if key and not hmac.compare_digest((authorization or "").encode(), f"Bearer {key}".encode()):
        raise HTTPException(401, "Missing or invalid API key.", headers={"WWW-Authenticate": "Bearer"})


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    from jeff.model import DecisionModel

    service.checkpoint = os.getenv("JEFF_CHECKPOINT", "checkpoints/selected")
    from jeff.models import device_from_environment, load_decision_model

    backend = os.getenv("JEFF_BACKEND", "pytorch")
    if backend == "mlx":  # Apple GPUs: fast Metal kernels for Qwen3.5 (text only)
        from jeff.mlx_backend import MlxDecisionModel
        service.model = await run_in_threadpool(MlxDecisionModel, service.checkpoint)
    elif backend == "pytorch":
        service.model = await run_in_threadpool(load_decision_model, checkpoint=service.checkpoint, device=device_from_environment())
    else:
        raise ValueError(f"JEFF_BACKEND={backend!r}; use pytorch or mlx")
    service.name = f"jeff-{service.model.base_model.rsplit('/', 1)[-1].lower()}"
    modified = (Path(service.checkpoint) / "decision_config.json").stat().st_mtime
    service.release_date = datetime.fromtimestamp(modified, timezone.utc).date().isoformat()
    batch_queue = BatchQueue(int(os.getenv("JEFF_MAX_QUEUE", "32")), int(os.getenv("JEFF_BATCH_WAIT_MS", "3")))
    batch_queue.start()
    service.batch_queue = batch_queue
    try:
        yield
    finally:
        await batch_queue.close()
        service.batch_queue = None
        service.model = None


app = FastAPI(title="Jeff", version="0.2.0", lifespan=lifespan)


@app.middleware("http")
async def request_metadata(request: Request, call_next: RequestResponseEndpoint) -> Response:
    started, identifier = time.perf_counter(), uuid.uuid4().hex
    response = await call_next(request)
    response.headers["x-request-id"] = identifier
    response.headers["server-timing"] = f"total;dur={(time.perf_counter() - started) * 1000:.1f}"
    return response


@app.exception_handler(RequestValidationError)
async def validation_error(request: Request, error: RequestValidationError) -> JSONResponse:
    return JSONResponse(status_code=422, content={"detail": [
        {"loc": item["loc"], "msg": item["msg"], "type": item["type"]}
        for item in error.errors()
    ]})


@app.get("/", response_class=HTMLResponse, include_in_schema=False)
def playground() -> str:
    return Path(__file__).with_name("playground.html").read_text()


@app.get("/health", response_model=None)
def health() -> dict[str, JSONValue]:
    return {"status": "ready" if service.model is not None else "loading", "model": service.name,
            "checkpoint": service.checkpoint, "authentication": bool(os.getenv("JEFF_API_KEY")),
            "modalities": ["text"] if getattr(service.model, "backend", None) == "mlx" else ["text", "image"]}


@app.get("/v1/models", dependencies=[Depends(authenticate)], response_model=None)
def models() -> dict[str, JSONValue]:
    return {"models": [
        {"name": name, "description": "Local Jeff text and image decisions.", "release_date": service.release_date}
        for name in sorted(ALIASES | {service.name})
    ]}


def predict_many(model: DecisionModel, bodies: list[EvaluationRequest]) -> tuple[list[DecisionResponse], int]:
    """Run many HTTP requests through bounded model forwards and restore answers."""
    import torch
    from jeff.model import answer

    questions_by_body: list[dict[str, DecisionQuestion]] = []
    answers_by_body: list[dict[str, Answer]] = []
    rows: list[tuple[int, str, DecisionInput]] = []
    for body_index, body in enumerate(bodies):
        questions = {key: cast(DecisionQuestion, question.model_dump(exclude_none=True))
                     for key, question in body.questions.items()}
        questions_by_body.append(questions)
        answers_by_body.append({})
        rows.extend((body_index, identifier, {"state": body.state, "question": question, "images": list(body.images)})
                    for identifier, question in questions.items())
    input_tokens = 0
    if getattr(model, "backend", None) == "mlx":
        inputs = [row for _, _, row in rows]
        for (body_index, identifier, _), (values, tokens) in zip(rows, model.decide(inputs), strict=True):  # type: ignore[attr-defined]
            answers_by_body[body_index][identifier] = answer(questions_by_body[body_index][identifier], values)
            input_tokens += tokens
        return ([{"model": service.name, "answers": answers, "usage": {"input_tokens": 0, "output_tokens": 0}}
                 for answers in answers_by_body], input_tokens)
    max_rows = int(os.getenv("JEFF_MAX_BATCH_ROWS", "8"))
    if max_rows < 1:
        raise ValueError("JEFF_MAX_BATCH_ROWS must be positive.")
    with torch.inference_mode():
        for start in range(0, len(rows), max_rows):
            chunk = rows[start:start + max_rows]
            batch = model.prepare([row for _, _, row in chunk])
            distributions: list[list[float]] = (model(batch) / model.temperature).softmax(-1).cpu().tolist()
            for (body_index, identifier, _), values, count in zip(chunk, distributions, batch.counts, strict=True):
                answers_by_body[body_index][identifier] = answer(questions_by_body[body_index][identifier], values[:count])
            input_tokens += batch.input_tokens
    return ([{"model": service.name, "answers": answers, "usage": {"input_tokens": 0, "output_tokens": 0}}
             for answers in answers_by_body], input_tokens)


def predict(model: DecisionModel, body: EvaluationRequest) -> DecisionResponse:
    responses, input_tokens = predict_many(model, [body])
    response = responses[0]
    response["usage"] = {"input_tokens": input_tokens, "output_tokens": 0}
    return response


@app.post("/v1/systemone", dependencies=[Depends(authenticate)], response_model=None)
async def system_one(body: EvaluationRequest) -> DecisionResponse:
    model = service.model
    if model is None:
        raise HTTPException(503, "The model is not ready.")
    if not service.lock.acquire(blocking=False):
        raise HTTPException(529, "The model is busy. Retry shortly.", headers={"Retry-After": "1"})
    try:
        return await run_in_threadpool(predict, model, body)
    except ValueError as error:
        raise HTTPException(422, str(error)) from error
    finally:
        service.lock.release()


@app.post("/v1/systemone:batch", dependencies=[Depends(authenticate)], response_model=None)
async def system_one_batch(body: BatchEvaluationRequest) -> dict[str, JSONValue]:
    """Queue a bounded batch without exposing the legacy endpoint's busy lock."""
    batch_queue = getattr(service, "batch_queue", None)
    if service.model is None or batch_queue is None:
        raise HTTPException(503, "The model is not ready.")
    try:
        return await batch_queue.submit(body)
    except ValueError as error:
        raise HTTPException(422, str(error)) from error
    except RuntimeError as error:
        raise HTTPException(503, str(error)) from error


def main() -> None:
    import uvicorn

    uvicorn.run("jeff.server:app", host=os.getenv("JEFF_HOST", "127.0.0.1"), port=int(os.getenv("PORT", "8000")))


if __name__ == "__main__":
    main()
