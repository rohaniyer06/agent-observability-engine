"""Every SQL statement the query API issues, plus the row -> response mappers.

Kept out of the route handlers so the statements that decide whether this service
is fast are all readable in one place, and so a handler is never more than
"validate params, call a query, assemble the model".

THE RULE THIS FILE ENFORCES (design doc §4.2 blocker, §7.2 risk):
`latency_rollups` and `traces` answer every aggregate question. The `spans`
table is read by exactly ONE function here — `fetch_spans()`, the single-trace
drill-down. The moment a dashboard query does `percentile_cont` over raw spans,
the "sub-second dashboard" claim is false, and it fails at precisely the wrong
time: during the load test that exists to produce the number you want to quote.
Every parameter is bound ($1, $2, ...); nothing user-supplied is ever formatted
into a statement.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import asyncpg

from aoe.apimodels import (
    CostAnomaly,
    DriftEvent,
    LatencyPoint,
    LatencySeries,
    LoadTestRun,
    NodeStat,
    PathStat,
    SpanDetail,
    TraceSummary,
)
from aoe.config import get_settings

# asyncpg's Pool and Connection both expose fetch/fetchrow/fetchval. Accepting
# either means a handler can pass the pool directly for a one-shot read and a
# connection when several reads should share one.
Queryable = asyncpg.Pool | asyncpg.Connection

_TRACE_COLUMNS = """
    trace_id, pipeline_name, started_at, ended_at, total_duration_us,
    total_cost_usd, total_input_tokens, total_output_tokens, span_count,
    status, path, finalized_by
"""


# ---------------------------------------------------------------------------
# Traces
# ---------------------------------------------------------------------------

# Keyset pagination, never OFFSET. OFFSET has to walk and discard every skipped
# row (so page 200 costs 200 pages of work), and on a table this write-heavy it
# also skips or duplicates rows when new traces land between page fetches — the
# reader's offset shifts under them. The row comparison below resumes from an
# exact point instead, at constant cost per page. trace_id is only a tiebreaker
# for traces sharing a started_at; started_at alone is not unique.
_SELECT_TRACES = f"""
SELECT {_TRACE_COLUMNS}
FROM traces
WHERE ($1::text        IS NULL OR pipeline_name = $1)
  AND ($2::text        IS NULL OR status = $2)
  AND ($3::timestamptz IS NULL OR started_at >= $3)
  -- Node filter reads traces.path, NOT the spans table.
  AND ($4::text        IS NULL OR $4 = ANY(path))
  AND ($5::timestamptz IS NULL OR (started_at, trace_id) < ($5::timestamptz, $6::uuid))
ORDER BY started_at DESC, trace_id DESC
LIMIT $7::bigint
"""


async def fetch_traces(
    conn: Queryable,
    *,
    limit: int,
    pipeline: str | None = None,
    status: str | None = None,
    since: datetime | None = None,
    node: str | None = None,
    cursor: tuple[datetime, uuid.UUID] | None = None,
) -> list[TraceSummary]:
    rows = await conn.fetch(
        _SELECT_TRACES,
        pipeline,
        status,
        since,
        node,
        cursor[0] if cursor else None,
        cursor[1] if cursor else None,
        limit,
    )
    return [_trace_summary(r) for r in rows]


async def fetch_trace(conn: Queryable, trace_id: uuid.UUID) -> TraceSummary | None:
    row = await conn.fetchrow(
        f"SELECT {_TRACE_COLUMNS} FROM traces WHERE trace_id = $1",
        trace_id,
    )
    return _trace_summary(row) if row else None


async def fetch_spans(conn: Queryable, trace_id: uuid.UUID) -> list[SpanDetail]:
    """The ONLY read of the raw `spans` table in this service.

    Bounded by definition — a trace is 3-5 spans (design doc §4.1) — which is why
    this one is allowed to touch raw rows while every aggregate endpoint is not.
    Ordered by start_time_ns rather than created_at: created_at is arrival order
    at the worker, which is not execution order once a batch is reordered in
    flight, and the waterfall view needs execution order.
    """
    rows = await conn.fetch(
        """
        SELECT span_id, trace_id, parent_span_id, node_name, operation_name,
               model_name, provider_name, start_time_ns, end_time_ns, duration_us,
               input_tokens, output_tokens, cost_usd, status, error_message, attributes
        FROM spans
        WHERE trace_id = $1
        ORDER BY start_time_ns ASC, span_id ASC
        """,
        trace_id,
    )
    return [_span_detail(r) for r in rows]


# ---------------------------------------------------------------------------
# Latency — rollups only
# ---------------------------------------------------------------------------

# Re-bucketing happens in SQL by flooring the stored bucket_start onto a coarser
# grid. See `fetch_latency_series` for why MAX is the honest merge for a
# percentile and SUM is exact for a count.
_SELECT_LATENCY = """
SELECT node_name,
       to_timestamp((extract(epoch FROM bucket_start)::bigint / $1::bigint) * $1::bigint)
           AS bucket_start,
       max(p50_us)      AS p50_us,
       max(p95_us)      AS p95_us,
       max(p99_us)      AS p99_us,
       max(max_us)      AS max_us,
       sum(count)       AS count,
       sum(error_count) AS error_count
FROM latency_rollups
WHERE bucket_start >= $2
  AND ($3::text[] IS NULL OR node_name = ANY($3::text[]))
GROUP BY node_name, 2
ORDER BY node_name ASC, 2 ASC
"""


async def fetch_latency_series(
    conn: Queryable,
    *,
    since: datetime,
    bucket_seconds: int,
    nodes: list[str] | None = None,
) -> list[LatencySeries]:
    """Pre-aggregated percentiles. Never touches `spans`.

    When `bucket_seconds` equals the stored rollup granularity each group holds
    exactly one row, so MAX/SUM are identity operations and the numbers are
    exactly what the worker's histogram computed.

    When `bucket_seconds` is coarser, MAX(p99) across the merged rows is an UPPER
    BOUND, not the true p99 of the union. Percentiles are not additive: merging
    them exactly needs the underlying histograms, and those live in Redis with a
    15-minute TTL (`rollup_key_ttl_s`), not in Postgres. Taking the max of the
    constituent p99s is the only merge that cannot understate the tail, which is
    the right direction to be wrong in for a latency alarm — but the caller is
    told about it rather than being handed a bound dressed up as an exact value.
    """
    rows = await conn.fetch(_SELECT_LATENCY, bucket_seconds, since, nodes)

    # Rows arrive ordered by (node_name, bucket_start), so one pass groups them.
    series: list[LatencySeries] = []
    for row in rows:
        if not series or series[-1].node_name != row["node_name"]:
            series.append(LatencySeries(node_name=row["node_name"], points=[]))
        series[-1].points.append(
            LatencyPoint(
                bucket_start=_iso(row["bucket_start"]),
                p50_ms=_us_to_ms(row["p50_us"]),
                p95_ms=_us_to_ms(row["p95_us"]),
                p99_ms=_us_to_ms(row["p99_us"]),
                max_ms=_us_to_ms(row["max_us"]),
                count=int(row["count"] or 0),
                error_count=int(row["error_count"] or 0),
            )
        )
    return series


async def fetch_node_names(conn: Queryable) -> list[str]:
    rows = await conn.fetch("SELECT DISTINCT node_name FROM latency_rollups ORDER BY node_name")
    return [r["node_name"] for r in rows]


async def fetch_node_stats(conn: Queryable, *, since: datetime) -> list[NodeStat]:
    """Per-node totals for the summary card. Rollups only.

    Note there is no pipeline filter: `latency_rollups` is keyed by
    (node_name, bucket_start) only, so node latency is inherently cross-pipeline.
    Adding a pipeline dimension would multiply the rollup cardinality for a
    single-pipeline system; if a second pipeline reuses a node name, this merges
    them, and that is a deliberate trade recorded here rather than hidden.
    """
    rows = await conn.fetch(
        """
        SELECT node_name,
               sum(count)       AS count,
               max(p99_us)      AS p99_us,
               sum(error_count) AS error_count
        FROM latency_rollups
        WHERE bucket_start >= $1
        GROUP BY node_name
        ORDER BY node_name
        """,
        since,
    )
    return [
        NodeStat(
            node_name=r["node_name"],
            count=int(r["count"] or 0),
            p99_ms=_us_to_ms(r["p99_us"]),
            error_rate=(int(r["error_count"] or 0) / int(r["count"])) if r["count"] else 0.0,
        )
        for r in rows
    ]


async def fetch_overall_percentiles(conn: Queryable, *, since: datetime) -> dict[str, float]:
    """Whole-window p50/p95/p99, from rollups.

    MAX across every (node, minute) in the window. Same caveat as
    `fetch_latency_series`, one step further: this merges across nodes as well as
    across time, so it is the slowest node's tail, i.e. an upper bound on the
    per-node experience. It is emphatically NOT an end-to-end trace percentile —
    for that, see `total_duration_us` on `traces`. Reported as a bound because
    the alternative is a `percentile_cont` over raw spans, which is the exact
    query the design doc forbids.
    """
    row = await conn.fetchrow(
        """
        SELECT max(p50_us) AS p50_us, max(p95_us) AS p95_us, max(p99_us) AS p99_us
        FROM latency_rollups
        WHERE bucket_start >= $1
        """,
        since,
    )
    return {
        "p50_ms": _us_to_ms(row["p50_us"] if row else None),
        "p95_ms": _us_to_ms(row["p95_us"] if row else None),
        "p99_ms": _us_to_ms(row["p99_us"] if row else None),
    }


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------


async def fetch_window_totals(
    conn: Queryable, *, since: datetime, pipeline: str | None
) -> asyncpg.Record:
    """Trace/cost/anomaly/drift counts for the window, in one round trip.

    `span_count` is summed from the `traces` rollup column rather than counted
    out of `spans` — same rule as everything else here.
    """
    return await conn.fetchrow(
        """
        WITH t AS (
            SELECT count(*)                                           AS trace_count,
                   coalesce(sum(span_count), 0)                       AS span_count,
                   -- 'partial' means the reaper closed an incomplete trace, which
                   -- is an incompleteness signal, not a failed run. Only explicit
                   -- 'error' counts toward the error rate.
                   count(*) FILTER (WHERE status = 'error')           AS error_trace_count,
                   coalesce(sum(total_cost_usd), 0)                   AS total_cost_usd
            FROM traces
            WHERE started_at >= $1 AND ($2::text IS NULL OR pipeline_name = $2)
        ), a AS (
            SELECT count(*) AS open_anomalies
            FROM cost_anomalies
            WHERE detected_at >= $1 AND ($2::text IS NULL OR pipeline_name = $2)
        ), d AS (
            SELECT count(*) AS drift_events
            FROM path_drift_events
            WHERE detected_at >= $1 AND ($2::text IS NULL OR pipeline_name = $2)
        )
        SELECT t.*, a.open_anomalies, d.drift_events FROM t, a, d
        """,
        since,
        pipeline,
    )


# ---------------------------------------------------------------------------
# Anomalies
# ---------------------------------------------------------------------------

_SELECT_ANOMALIES = """
SELECT id, trace_id, pipeline_name, detected_at, expected_cost_usd, actual_cost_usd,
       deviation_pct, z_score, sample_size, severity
FROM cost_anomalies
WHERE ($1::text        IS NULL OR severity = $1)
  AND ($2::timestamptz IS NULL OR detected_at >= $2)
  AND ($3::timestamptz IS NULL OR (detected_at, id) < ($3::timestamptz, $4::int))
ORDER BY detected_at DESC, id DESC
LIMIT $5::bigint
"""


async def fetch_anomalies(
    conn: Queryable,
    *,
    limit: int,
    severity: str | None = None,
    since: datetime | None = None,
    cursor: tuple[datetime, int] | None = None,
) -> list[CostAnomaly]:
    rows = await conn.fetch(
        _SELECT_ANOMALIES,
        severity,
        since,
        cursor[0] if cursor else None,
        cursor[1] if cursor else None,
        limit,
    )
    return [
        CostAnomaly(
            id=r["id"],
            trace_id=str(r["trace_id"]),
            pipeline_name=r["pipeline_name"],
            detected_at=_iso(r["detected_at"]),
            expected_cost_usd=_num(r["expected_cost_usd"]),
            actual_cost_usd=_num(r["actual_cost_usd"]),
            deviation_pct=float(r["deviation_pct"]),
            z_score=float(r["z_score"]),
            sample_size=int(r["sample_size"]),
            severity=r["severity"],
        )
        for r in rows
    ]


# ---------------------------------------------------------------------------
# Path drift
# ---------------------------------------------------------------------------


async def fetch_path_stats(
    conn: Queryable, *, pipeline: str, limit: int | None = None
) -> tuple[list[PathStat], int]:
    """Path distribution for a pipeline, with `share` and `is_baseline` derived.

    `sum(...) OVER ()` runs before LIMIT, so `share` is a share of ALL of the
    pipeline's traces even when the caller asks for the top N paths. Computing it
    from the returned page instead would make every share depend on the page size.

    Baseline rule (DEVIATIONS.md #5): a path is baseline if it was explicitly
    seeded, or — once the pipeline has enough traces for the question to mean
    anything — if it accounts for at least `drift_baseline_freq_pct` of them.
    That is what stops "rare but normal" from reading as drift (design doc §7.6).
    LIMIT NULL is Postgres for "no limit", which is how the flow endpoint gets
    the complete baseline set.
    """
    settings = get_settings()
    rows = await conn.fetch(
        """
        SELECT path_signature, path, occurrences, is_seeded,
               sum(occurrences) OVER () AS pipeline_total
        FROM pipeline_paths
        WHERE pipeline_name = $1
        ORDER BY occurrences DESC, path_signature ASC
        LIMIT $2::bigint
        """,
        pipeline,
        limit,
    )
    if not rows:
        return [], 0

    total = int(rows[0]["pipeline_total"] or 0)
    armed = total >= settings.drift_min_traces
    stats: list[PathStat] = []
    for r in rows:
        occurrences = int(r["occurrences"] or 0)
        share = (occurrences / total) if total else 0.0
        stats.append(
            PathStat(
                path=list(r["path"] or []),
                path_signature=r["path_signature"],
                occurrences=occurrences,
                # Fraction 0..1. The threshold is configured as a percentage.
                share=share,
                is_baseline=bool(r["is_seeded"])
                or (armed and share * 100.0 >= settings.drift_baseline_freq_pct),
                is_seeded=bool(r["is_seeded"]),
            )
        )
    return stats, total


async def fetch_drift_events(
    conn: Queryable, *, pipeline: str, limit: int
) -> list[DriftEvent]:
    rows = await conn.fetch(
        """
        SELECT id, pipeline_name, observed_path, path_signature, baseline_paths,
               trace_id, occurrence_count, first_seen_at, detected_at
        FROM path_drift_events
        WHERE pipeline_name = $1
        ORDER BY detected_at DESC, id DESC
        LIMIT $2::bigint
        """,
        pipeline,
        limit,
    )
    return [
        DriftEvent(
            id=r["id"],
            pipeline_name=r["pipeline_name"],
            observed_path=list(r["observed_path"] or []),
            path_signature=r["path_signature"],
            baseline_paths=list(r["baseline_paths"] or []),
            trace_id=str(r["trace_id"]),
            occurrence_count=int(r["occurrence_count"] or 0),
            first_seen_at=_iso(r["first_seen_at"]),
            detected_at=_iso(r["detected_at"]),
        )
        for r in rows
    ]


async def fetch_observed_paths(
    conn: Queryable, *, pipeline: str, since: datetime
) -> list[tuple[list[str], int]]:
    """Distinct paths actually walked in the window, with counts.

    Grouped in the database rather than pulled row-by-row: the flow diagram cares
    about ~a dozen distinct paths, not about the hundreds of thousands of traces
    that walked them.
    """
    rows = await conn.fetch(
        """
        SELECT path, count(*) AS occurrences
        FROM traces
        WHERE pipeline_name = $1 AND started_at >= $2 AND cardinality(path) > 0
        GROUP BY path
        ORDER BY occurrences DESC
        """,
        pipeline,
        since,
    )
    return [(list(r["path"]), int(r["occurrences"])) for r in rows]


# ---------------------------------------------------------------------------
# Load tests
# ---------------------------------------------------------------------------


async def fetch_load_test_runs(conn: Queryable, *, limit: int) -> list[LoadTestRun]:
    rows = await conn.fetch(
        """
        SELECT id, label, started_at, ended_at, target_rps, achieved_rps, spans_sent,
               spans_accepted, spans_shed_503, errors, ingest_p50_ms, ingest_p95_ms,
               ingest_p99_ms, ingest_max_ms, stream_lag_p50_ms, stream_lag_p99_ms,
               max_stream_depth, notes
        FROM load_test_runs
        ORDER BY started_at DESC, id DESC
        LIMIT $1::bigint
        """,
        limit,
    )
    return [
        LoadTestRun(
            id=r["id"],
            label=r["label"],
            started_at=_iso(r["started_at"]),
            ended_at=_iso(r["ended_at"]),
            target_rps=float(r["target_rps"]),
            achieved_rps=float(r["achieved_rps"]),
            spans_sent=int(r["spans_sent"]),
            spans_accepted=int(r["spans_accepted"]),
            spans_shed_503=int(r["spans_shed_503"]),
            errors=int(r["errors"]),
            ingest_p50_ms=float(r["ingest_p50_ms"]),
            ingest_p95_ms=float(r["ingest_p95_ms"]),
            ingest_p99_ms=float(r["ingest_p99_ms"]),
            ingest_max_ms=float(r["ingest_max_ms"]),
            stream_lag_p50_ms=_opt_float(r["stream_lag_p50_ms"]),
            stream_lag_p99_ms=_opt_float(r["stream_lag_p99_ms"]),
            max_stream_depth=int(r["max_stream_depth"])
            if r["max_stream_depth"] is not None
            else None,
            notes=r["notes"],
        )
        for r in rows
    ]


async def ping(conn: Queryable) -> bool:
    return await conn.fetchval("SELECT 1") == 1


# ---------------------------------------------------------------------------
# Row -> response model
# ---------------------------------------------------------------------------


def _iso(value: datetime) -> str:
    """ISO-8601 UTC with a Z suffix.

    apimodels.py's stated convention. Naive values are treated as UTC — Postgres
    TIMESTAMPTZ never produces one, but a caller passing a hand-built datetime
    should not silently get a local-time string on the wire.
    """
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _num(value: Decimal | float | None) -> float:
    return float(value) if value is not None else 0.0


def _opt_float(value: float | None) -> float | None:
    return float(value) if value is not None else None


def _us_to_ms(value: int | None) -> float:
    """Storage is microseconds (DEVIATIONS.md #6); the wire is float ms."""
    return (int(value) / 1000.0) if value is not None else 0.0


def _trace_summary(row: asyncpg.Record) -> TraceSummary:
    duration_us = row["total_duration_us"]
    return TraceSummary(
        trace_id=str(row["trace_id"]),
        pipeline_name=row["pipeline_name"],
        started_at=_iso(row["started_at"]),
        ended_at=_iso(row["ended_at"]) if row["ended_at"] else None,
        duration_ms=(int(duration_us) / 1000.0) if duration_us is not None else None,
        total_cost_usd=_num(row["total_cost_usd"]),
        total_input_tokens=int(row["total_input_tokens"]),
        total_output_tokens=int(row["total_output_tokens"]),
        span_count=int(row["span_count"]),
        status=row["status"],
        path=list(row["path"] or []),
        finalized_by=row["finalized_by"],
    )


def _span_detail(row: asyncpg.Record) -> SpanDetail:
    attributes: Any = row["attributes"]
    return SpanDetail(
        span_id=str(row["span_id"]),
        trace_id=str(row["trace_id"]),
        parent_span_id=str(row["parent_span_id"]) if row["parent_span_id"] else None,
        node_name=row["node_name"],
        operation_name=row["operation_name"],
        model_name=row["model_name"],
        provider_name=row["provider_name"],
        start_time_ns=int(row["start_time_ns"]),
        end_time_ns=int(row["end_time_ns"]),
        duration_ms=int(row["duration_us"]) / 1000.0,
        input_tokens=int(row["input_tokens"]),
        output_tokens=int(row["output_tokens"]),
        cost_usd=_num(row["cost_usd"]),
        status=row["status"],
        error_message=row["error_message"],
        attributes=attributes if isinstance(attributes, dict) else {},
    )
