"""Wire schema for telemetry in flight (design doc §4.1).

Field naming follows the OpenTelemetry GenAI semantic conventions (`gen_ai.*`)
rather than a bespoke format. Those conventions are still in Development status
upstream, so this is "aligned with", not "compliant to" — see design doc §7.9.
The aliases below are the on-the-wire names; the Python attribute names are
snake_case so the rest of the codebase reads normally.
"""

from __future__ import annotations

import uuid
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

SpanStatus = Literal["ok", "error"]
# OTel GenAI operation names. Note `execute_tool`, not `tool_call` — the doc's
# §4.1 sketch used the older spelling.
OperationName = Literal["chat", "execute_tool", "invoke_agent"]


class Span(BaseModel):
    """One node execution. A full pipeline run is a trace of 3-5 of these."""

    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    trace_id: uuid.UUID
    span_id: uuid.UUID
    parent_span_id: uuid.UUID | None = None

    operation_name: OperationName = Field(alias="gen_ai.operation.name")
    model_name: str | None = Field(default=None, alias="gen_ai.request.model")
    provider_name: str | None = Field(default=None, alias="gen_ai.provider.name")

    node_name: str = Field(min_length=1, max_length=128)
    pipeline_name: str = Field(min_length=1, max_length=128)

    start_time_ns: int = Field(ge=0)
    end_time_ns: int = Field(ge=0)

    input_tokens: int = Field(default=0, ge=0, alias="gen_ai.usage.input_tokens")
    output_tokens: int = Field(default=0, ge=0, alias="gen_ai.usage.output_tokens")
    # Present when prompt caching is in play. input_tokens is the UNCACHED
    # remainder, so these are additive, not a subset — see config/pricing.yaml.
    cache_read_tokens: int = Field(default=0, ge=0, alias="gen_ai.usage.cache_read_input_tokens")
    cache_write_tokens: int = Field(
        default=0, ge=0, alias="gen_ai.usage.cache_creation_input_tokens"
    )

    status: SpanStatus = "ok"
    error_message: str | None = Field(default=None, max_length=2000)

    # Optional. When the emitter does not supply a cost, the worker computes it
    # from config/pricing.yaml. Emitters should generally leave this unset so
    # there is exactly one costing implementation.
    cost_usd: float | None = Field(default=None, ge=0)

    # Explicit end-of-trace signal. Set on the root `invoke_agent` span, emitted
    # last. The timeout reaper is the backstop for when this never arrives
    # (design doc §7.5 — we implement both, see DEVIATIONS.md #2).
    trace_end: bool = False

    attributes: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _check_time_ordering(self) -> Span:
        if self.end_time_ns < self.start_time_ns:
            raise ValueError("end_time_ns must be >= start_time_ns")
        return self

    @field_validator("attributes")
    @classmethod
    def _bound_attributes(cls, v: dict[str, Any]) -> dict[str, Any]:
        if len(v) > 64:
            raise ValueError("attributes may contain at most 64 keys")
        return v

    @property
    def duration_us(self) -> int:
        return (self.end_time_ns - self.start_time_ns) // 1_000


class SpanBatch(BaseModel):
    """POST /v1/spans accepts either this or a bare Span."""

    model_config = ConfigDict(extra="forbid")

    spans: list[Span] = Field(min_length=1)


class IngestAccepted(BaseModel):
    accepted: int
    # Spans rejected by per-span validation inside an otherwise-valid batch.
    rejected: int = 0
    stream_depth: int
    errors: list[str] = Field(default_factory=list)


class HealthResponse(BaseModel):
    status: Literal["ok", "degraded"]
    redis: bool
    postgres: bool | None = None
    stream_depth: int | None = None
    version: str


# ---------------------------------------------------------------------------
# Internal representation the worker passes around after decoding a stream entry.
# Kept separate from `Span` because it carries transport metadata (the Redis
# entry id, and the XADD timestamp used to measure stream lag).
# ---------------------------------------------------------------------------


class StreamedSpan(BaseModel):
    model_config = ConfigDict(extra="forbid")

    entry_id: str
    enqueued_at_ms: int
    span: Span

    @property
    def lag_ms(self) -> int:
        """Milliseconds between XADD and the worker picking the entry up."""
        import time

        return max(0, int(time.time() * 1000) - self.enqueued_at_ms)


# ---------------------------------------------------------------------------
# Live feed payload (worker -> Redis pub/sub -> query API -> dashboard WS).
# Losing one of these is a cosmetic failure, not a data-loss failure, which is
# why this leg is pub/sub and the ingest leg is Streams (design doc §5.3.5).
# ---------------------------------------------------------------------------


class LiveTraceEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: Literal["trace_finalized", "cost_anomaly", "path_drift"] = "trace_finalized"
    trace_id: uuid.UUID
    pipeline_name: str
    started_at: str
    duration_ms: float
    total_cost_usd: float
    status: str
    path: list[str]
    span_count: int
    # Populated on the anomaly/drift variants.
    detail: dict[str, Any] = Field(default_factory=dict)
