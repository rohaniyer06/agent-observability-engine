"""Response contract for the query API (design doc §5.4).

This module is the shared boundary between the API service and the dashboard.
`dashboard/src/types/api.ts` is a hand-maintained mirror of these shapes — if you
change one, change the other.

Time convention: every duration crossing this boundary is FLOAT MILLISECONDS,
regardless of the fact that storage is in microseconds. Timestamps are ISO-8601
UTC strings so the frontend never has to guess at a numeric epoch's unit.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field

Severity = Literal["warn", "critical"]
TraceStatus = Literal["ok", "error", "partial"]


# ---------------------------------------------------------------------------
# Traces
# ---------------------------------------------------------------------------


class TraceSummary(BaseModel):
    trace_id: str
    pipeline_name: str
    started_at: str
    ended_at: str | None
    duration_ms: float | None
    total_cost_usd: float
    total_input_tokens: int
    total_output_tokens: int
    span_count: int
    status: TraceStatus
    path: list[str]
    finalized_by: Literal["trace_end", "timeout"]


class SpanDetail(BaseModel):
    span_id: str
    trace_id: str
    parent_span_id: str | None
    node_name: str
    operation_name: str
    model_name: str | None
    provider_name: str | None
    start_time_ns: int
    end_time_ns: int
    duration_ms: float
    input_tokens: int
    output_tokens: int
    cost_usd: float
    status: str
    error_message: str | None
    attributes: dict[str, Any]


class TraceDetail(BaseModel):
    trace: TraceSummary
    spans: list[SpanDetail]


class TracePage(BaseModel):
    """Cursor pagination, not OFFSET.

    OFFSET pagination over a table this write-heavy degrades as the offset grows
    and can skip or duplicate rows when new traces land mid-scroll. The cursor is
    an opaque encoding of (started_at, trace_id).
    """

    items: list[TraceSummary]
    next_cursor: str | None = None
    has_more: bool = False


# ---------------------------------------------------------------------------
# Latency metrics
# ---------------------------------------------------------------------------


class LatencyPoint(BaseModel):
    bucket_start: str
    p50_ms: float
    p95_ms: float
    p99_ms: float
    max_ms: float
    count: int
    error_count: int


class LatencySeries(BaseModel):
    node_name: str
    points: list[LatencyPoint]


class LatencyResponse(BaseModel):
    window: str
    bucket_seconds: int
    series: list[LatencySeries]
    # True when these numbers came from pre-aggregated rollups (they always
    # should). Surfaced so the dashboard can prove it is not scanning raw spans.
    from_rollups: bool = True


class NodeStat(BaseModel):
    node_name: str
    count: int
    p99_ms: float
    error_rate: float


class SummaryResponse(BaseModel):
    window: str
    trace_count: int
    span_count: int
    error_trace_count: int
    error_rate: float
    total_cost_usd: float
    traces_per_second: float
    p50_ms: float
    p95_ms: float
    p99_ms: float
    open_anomalies: int
    drift_events: int
    nodes: list[NodeStat]


# ---------------------------------------------------------------------------
# Anomalies & drift
# ---------------------------------------------------------------------------


class CostAnomaly(BaseModel):
    id: int
    trace_id: str
    pipeline_name: str
    detected_at: str
    expected_cost_usd: float
    actual_cost_usd: float
    deviation_pct: float
    z_score: float
    sample_size: int
    severity: Severity


class AnomalyPage(BaseModel):
    items: list[CostAnomaly]
    next_cursor: str | None = None
    has_more: bool = False


class DriftEvent(BaseModel):
    id: int
    pipeline_name: str
    observed_path: list[str]
    path_signature: str
    baseline_paths: list[str]
    trace_id: str
    occurrence_count: int
    first_seen_at: str
    detected_at: str


class PathStat(BaseModel):
    """One row of the path distribution — feeds the Sankey/flow view."""

    path: list[str]
    path_signature: str
    occurrences: int
    share: float
    is_baseline: bool
    is_seeded: bool


class DriftResponse(BaseModel):
    pipeline_name: str
    total_traces: int
    paths: list[PathStat]
    events: list[DriftEvent]


class FlowEdge(BaseModel):
    """Pre-computed Sankey edge so the dashboard does not re-derive it."""

    source: str
    target: str
    value: int
    is_baseline: bool


class FlowResponse(BaseModel):
    pipeline_name: str
    nodes: list[str]
    edges: list[FlowEdge]


# ---------------------------------------------------------------------------
# System
# ---------------------------------------------------------------------------


class SystemStats(BaseModel):
    stream_depth: int
    pending_entries: int
    consumer_count: int
    consumers: list[dict[str, Any]] = Field(default_factory=list)
    traces_awaiting_finalization: int
    spans_ingested: int
    spans_processed: int
    spans_duplicate: int
    traces_finalized: int
    traces_finalized_by_timeout: int
    entries_reclaimed: int
    unknown_models: list[str] = Field(default_factory=list)
    backpressure_threshold: int


class PricingResponse(BaseModel):
    version: int
    as_of: str
    source: str
    currency: str
    models: dict[str, dict[str, Any]]
    cache_multipliers: dict[str, float]
    unknown_models_seen: list[str]


class LoadTestRun(BaseModel):
    id: int
    label: str
    started_at: str
    ended_at: str
    target_rps: float
    achieved_rps: float
    spans_sent: int
    spans_accepted: int
    spans_shed_503: int
    errors: int
    ingest_p50_ms: float
    ingest_p95_ms: float
    ingest_p99_ms: float
    ingest_max_ms: float
    stream_lag_p50_ms: float | None
    stream_lag_p99_ms: float | None
    max_stream_depth: int | None
    notes: str | None
