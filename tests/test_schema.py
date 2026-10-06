from __future__ import annotations

import json
import uuid

import pytest
from pydantic import ValidationError

from aoe.schema import Span

BASE = {
    "trace_id": str(uuid.uuid4()),
    "span_id": str(uuid.uuid4()),
    "gen_ai.operation.name": "chat",
    "node_name": "classify",
    "pipeline_name": "support_ticket_triage",
    "start_time_ns": 1_000_000_000_000,
    "end_time_ns": 1_000_412_000_000,
}


def test_otel_aliases_round_trip() -> None:
    span = Span.model_validate(
        {**BASE, "gen_ai.request.model": "claude-haiku-4-5", "gen_ai.usage.input_tokens": 412}
    )
    assert span.model_name == "claude-haiku-4-5"
    assert span.input_tokens == 412

    wire = json.loads(span.model_dump_json(by_alias=True))
    # The wire format must stay OTel-shaped, not snake_cased on the way out.
    assert "gen_ai.request.model" in wire
    assert "gen_ai.usage.input_tokens" in wire
    assert "model_name" not in wire


def test_duration_is_derived_in_microseconds() -> None:
    assert Span.model_validate(BASE).duration_us == 412_000


def test_end_before_start_is_rejected() -> None:
    with pytest.raises(ValidationError):
        Span.model_validate({**BASE, "end_time_ns": BASE["start_time_ns"] - 1})


def test_unknown_fields_are_rejected() -> None:
    """extra='forbid' keeps a typo'd field from being silently dropped."""
    with pytest.raises(ValidationError):
        Span.model_validate({**BASE, "gen_ai.usage.input_tokns": 5})


def test_attribute_count_is_bounded() -> None:
    with pytest.raises(ValidationError):
        Span.model_validate({**BASE, "attributes": {str(i): i for i in range(65)}})


def test_negative_token_counts_are_rejected() -> None:
    with pytest.raises(ValidationError):
        Span.model_validate({**BASE, "gen_ai.usage.output_tokens": -1})


def test_trace_end_defaults_off() -> None:
    assert Span.model_validate(BASE).trace_end is False
