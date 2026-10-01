"""`/v1/metrics/*` — the endpoints the dashboard polls.

HARD CONSTRAINT (design doc §4.2 blocker, §7.2 risk): nothing in this module
reads the `spans` table. Every number here comes from `latency_rollups` or from
the per-trace columns on `traces`, both of which the worker pre-aggregates. A
`percentile_cont` over raw spans looks fine on a laptop with 10k rows and falls
over at the exact moment the load test is producing the numbers the project
exists to quote — so the "sub-second dashboard" claim would be false precisely
when it is being measured. Raw spans are read in one place only:
`GET /v1/traces/{trace_id}`.
"""

from __future__ import annotations

from fastapi import APIRouter, Query

from aoe.api import queries
from aoe.api.pagination import parse_window, window_start
from aoe.apimodels import LatencyResponse, SummaryResponse
from aoe.config import get_settings
from aoe.db.pool import get_pool

router = APIRouter(tags=["metrics"])


def _effective_bucket_seconds(requested: int, granularity: int) -> int:
    """Rollups are stored per-minute; we can coarsen a bucket, never refine one.

    A request for finer-than-stored resolution is served at the stored
    granularity rather than rejected — the caller gets real data and the response
    reports the bucket size actually used. Coarser requests snap up to a whole
    multiple so a group never straddles half of a stored bucket, which would put
    the same minute's samples into two different points.
    """
    if requested <= granularity:
        return granularity
    return ((requested + granularity - 1) // granularity) * granularity


@router.get("/v1/metrics/latency", response_model=LatencyResponse)
async def latency(
    node: list[str] | None = Query(default=None, description="Repeatable; omit for all nodes"),
    window: str = "1h",
    bucket_seconds: int = Query(default=0, ge=0, le=86_400),
) -> LatencyResponse:
    settings = get_settings()
    since = window_start(window)  # 400s on a window off the allowlist
    granularity = settings.rollup_bucket_seconds
    effective = _effective_bucket_seconds(bucket_seconds or granularity, granularity)

    pool = await get_pool()
    series = await queries.fetch_latency_series(
        pool,
        since=since,
        bucket_seconds=effective,
        # `= ANY($n::text[])` rather than a built-up IN list: one bound parameter,
        # one plan, no user string anywhere near the statement text.
        nodes=node or None,
    )
    # from_rollups is not decoration — it is the dashboard's proof that this
    # response did not come from a raw-span scan.
    return LatencyResponse(
        window=window, bucket_seconds=effective, series=series, from_rollups=True
    )


@router.get("/v1/metrics/summary", response_model=SummaryResponse)
async def summary(window: str = "1h", pipeline: str | None = None) -> SummaryResponse:
    window_seconds = parse_window(window)
    since = window_start(window)

    pool = await get_pool()
    async with pool.acquire() as conn:
        totals = await queries.fetch_window_totals(conn, since=since, pipeline=pipeline)
        # Overall percentiles come from the rollups, as MAX across nodes and
        # buckets. That is an upper bound, not the true window percentile: you
        # cannot exactly merge percentiles from pre-aggregated rows without the
        # underlying histograms. It is reported as a bound on purpose — the
        # alternative is `percentile_cont` over `spans`, which this service does
        # not do. See queries.fetch_overall_percentiles.
        overall = await queries.fetch_overall_percentiles(conn, since=since)
        nodes = await queries.fetch_node_stats(conn, since=since)

    trace_count = int(totals["trace_count"] or 0)
    error_trace_count = int(totals["error_trace_count"] or 0)
    return SummaryResponse(
        window=window,
        trace_count=trace_count,
        span_count=int(totals["span_count"] or 0),
        error_trace_count=error_trace_count,
        error_rate=(error_trace_count / trace_count) if trace_count else 0.0,
        total_cost_usd=float(totals["total_cost_usd"] or 0),
        # Throughput over the nominal window, not over the observed span of the
        # data. A quiet window should read as a low rate, not be normalised away.
        traces_per_second=trace_count / window_seconds,
        p50_ms=overall["p50_ms"],
        p95_ms=overall["p95_ms"],
        p99_ms=overall["p99_ms"],
        open_anomalies=int(totals["open_anomalies"] or 0),
        drift_events=int(totals["drift_events"] or 0),
        nodes=nodes,
    )


@router.get("/v1/metrics/nodes", response_model=list[str])
async def nodes() -> list[str]:
    """Node names the charts can be filtered by.

    Sourced from `latency_rollups`, not from `spans` — same rule, and it also
    means the picker only ever offers nodes that actually have chartable data.
    """
    pool = await get_pool()
    return await queries.fetch_node_names(pool)
