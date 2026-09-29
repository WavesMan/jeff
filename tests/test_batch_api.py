from pydantic import ValidationError
import pytest

from jeff.server import BatchEvaluationRequest


def request() -> dict[str, object]:
    return {"model": "jeff-latest", "state": "An order was cancelled.", "questions": {
        "route": {"type": "choice", "criteria": {"refund": "Refund", "review": "Review"}},
    }}


def test_batch_request_accepts_four_requests() -> None:
    body = BatchEvaluationRequest.model_validate({"model": "jeff-latest", "requests": [request()] * 4})
    assert len(body.requests) == 4


def test_batch_request_rejects_more_than_four_requests() -> None:
    with pytest.raises(ValidationError):
        BatchEvaluationRequest.model_validate({"model": "jeff-latest", "requests": [request()] * 5})
