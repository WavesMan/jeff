"""Serve a Jeff checkpoint through the decision API: a situation and questions in, one answer per question out."""

from __future__ import annotations

import base64
import binascii
import copy
import hashlib
import hmac
import json
import os
import threading
import time
import uuid
from _thread import LockType
from collections import OrderedDict
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
    max_options: int = 0  # the most options the model was trained on; set from decision_config.json
    lock: LockType = field(default_factory=threading.Lock)
    cache_lock: threading.RLock = field(default_factory=threading.RLock)
    result_cache: OrderedDict[str, DecisionResponse] = field(default_factory=OrderedDict)
    result_cache_size: int = 1024
    active_lock: threading.Lock = field(default_factory=threading.Lock)
    active_requests: int = 0


service = Service()


def request_cache_key(body: EvaluationRequest) -> str:
    """Create a stable key from the complete validated request, including images."""
    canonical = json.dumps(
        body.model_dump(mode="json"),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def cached_result(key: str) -> DecisionResponse | None:
    if service.result_cache_size <= 0:
        return None
    with service.cache_lock:
        value = service.result_cache.get(key)
        if value is None:
            return None
        service.result_cache.move_to_end(key)
        return copy.deepcopy(value)


def store_result(key: str, value: DecisionResponse) -> None:
    if service.result_cache_size <= 0:
        return
    with service.cache_lock:
        service.result_cache[key] = copy.deepcopy(value)
        service.result_cache.move_to_end(key)
        while len(service.result_cache) > service.result_cache_size:
            service.result_cache.popitem(last=False)


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


def authenticate(authorization: str | None = Header(default=None)) -> None:
    key = os.getenv("JEFF_API_KEY")
    if key and not hmac.compare_digest((authorization or "").encode(), f"Bearer {key}".encode()):
        raise HTTPException(401, "Missing or invalid API key.", headers={"WWW-Authenticate": "Bearer"})


def max_options(config: dict[str, JSONValue], path: Path) -> int:
    """The largest number of options the checkpoint can answer: the most its training questions had. Answer codes past
    that (for example AA, AB, ... after Z) were never trained, so the model would silently never pick those options."""
    value = config.get("max_options")
    if not isinstance(value, int) or isinstance(value, bool) or value < 2:
        raise ValueError(f"{path} needs \"max_options\": the largest number of options the model was trained on "
                         "(the trainer writes it; the Jeff models released on 2026-09-28 handle 26).")
    return value


def check_option_counts(body: EvaluationRequest) -> None:
    for key, question in body.questions.items():
        count = len(question.criteria) if isinstance(question, Choice) else 0
        if count > service.max_options:
            raise HTTPException(422, f"Question {key!r} has {count} options, but this model handles at most "
                                     f"{service.max_options}. Shortlist the options first, or split the question.")


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    from jeff.model import DecisionModel

    service.checkpoint = os.getenv("JEFF_CHECKPOINT", "checkpoints/selected")
    try:
        service.result_cache_size = max(0, int(os.getenv("JEFF_RESULT_CACHE_SIZE", "1024")))
    except ValueError as error:
        raise ValueError("JEFF_RESULT_CACHE_SIZE must be a non-negative integer") from error
    with service.cache_lock:
        service.result_cache.clear()
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
    config_path = Path(service.checkpoint) / "decision_config.json"
    service.max_options = max_options(json.loads(config_path.read_text()), config_path)
    modified = config_path.stat().st_mtime
    service.release_date = datetime.fromtimestamp(modified, timezone.utc).date().isoformat()
    try:
        yield
    finally:
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
    with service.active_lock:
        active_requests = service.active_requests
    return {"status": "ready" if service.model is not None else "loading", "model": service.name,
            "checkpoint": service.checkpoint, "max_options": service.max_options,
            "active_requests": active_requests,
            "result_cache": {"enabled": service.result_cache_size > 0, "size": service.result_cache_size,
                             "entries": len(service.result_cache)},
            "authentication": bool(os.getenv("JEFF_API_KEY")),
            "modalities": ["text"] if getattr(service.model, "backend", None) == "mlx" else ["text", "image"]}


@app.get("/v1/models", dependencies=[Depends(authenticate)], response_model=None)
def models() -> dict[str, JSONValue]:
    return {"models": [
        {"name": name, "description": "Local Jeff text and image decisions.", "release_date": service.release_date}
        for name in sorted(ALIASES | {service.name})
    ]}


def predict(model: DecisionModel, body: EvaluationRequest) -> DecisionResponse:
    import torch
    from jeff.model import answer

    questions = {key: cast(DecisionQuestion, question.model_dump(exclude_none=True))
                 for key, question in body.questions.items()}
    identifiers = list(questions)
    rows: list[DecisionInput] = [{"state": body.state, "question": question, "images": list(body.images)}
                                 for question in questions.values()]
    answers: dict[str, Answer] = {}
    input_tokens = 0
    if getattr(model, "backend", None) == "mlx":
        for identifier, (values, tokens) in zip(identifiers, model.decide(rows), strict=True):  # type: ignore[attr-defined]
            answers[identifier] = answer(questions[identifier], values)
            input_tokens += tokens
        return {"model": service.name, "answers": answers, "usage": {"input_tokens": input_tokens, "output_tokens": 0}}
    with torch.inference_mode():
        for start in range(0, len(rows), 8):
            batch = model.prepare(rows[start:start + 8])
            distributions: list[list[float]] = (model(batch) / model.temperature).softmax(-1).cpu().tolist()
            for identifier, values, count in zip(identifiers[start:start + 8], distributions, batch.counts, strict=True):
                answers[identifier] = answer(questions[identifier], values[:count])
            input_tokens += batch.input_tokens
    return {"model": service.name, "answers": answers, "usage": {"input_tokens": input_tokens, "output_tokens": 0}}


@app.post("/v1/systemone", dependencies=[Depends(authenticate)], response_model=None)
async def system_one(body: EvaluationRequest) -> DecisionResponse:
    with service.active_lock:
        service.active_requests += 1
    try:
        model = service.model
        if model is None:
            raise HTTPException(503, "The model is not ready.")
        check_option_counts(body)

        key = request_cache_key(body)
        cached = cached_result(key)
        if cached is not None:
            cached["cache_hit"] = True
            cached["model_called"] = False
            return JSONResponse(content=cached, headers={
                "X-Cache": "HIT",
                "X-Model-Called": "false",
                "X-Server-Completed": "true",
            })

        if not service.lock.acquire(blocking=False):
            raise HTTPException(529, "The model is busy. Retry shortly.", headers={"Retry-After": "1"})
        try:
            result = await run_in_threadpool(predict, model, body)
            store_result(key, result)
            result["cache_hit"] = False
            result["model_called"] = True
            return JSONResponse(content=result, headers={
                "X-Cache": "MISS",
                "X-Model-Called": "true",
                "X-Server-Completed": "true",
            })
        except ValueError as error:
            raise HTTPException(422, str(error)) from error
        finally:
            service.lock.release()
    finally:
        with service.active_lock:
            service.active_requests -= 1


def main() -> None:
    import uvicorn

    uvicorn.run("jeff.server:app", host=os.getenv("JEFF_HOST", "127.0.0.1"), port=int(os.getenv("PORT", "8000")))


if __name__ == "__main__":
    main()
