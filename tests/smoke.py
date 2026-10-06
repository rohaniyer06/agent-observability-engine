"""End-to-end smoke test: POST -> stream -> worker -> Postgres.

Run with `make smoke`. This is the check the design doc's §6 build order insists
on before anyone looks at the dashboard: a pretty UI on top of a broken ingestion
path is the standard failure mode for this kind of project.

Exits non-zero on failure, with a diagnosis rather than a traceback.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import time
import uuid

import asyncpg
import httpx

from aoe import redis_keys
from aoe.config import get_settings
from aoe.redis_client import close_redis, consumer_group_backlog, get_redis

PIPELINE = "smoke_test_pipeline"
NODES = [("extract", 120, 300, 40), ("classify", 380, 420, 90), ("decide", 210, 380, 60)]
# 300*1e-6 + 40*5e-6 + 420*1e-6 + 90*5e-6 + 380*1e-6 + 60*5e-6
EXPECTED_COST = 0.00205


def build_trace() -> tuple[str, list[dict]]:
    trace_id, root_id = str(uuid.uuid4()), str(uuid.uuid4())
    t0 = time.time_ns()
    cursor = t0
    spans: list[dict] = []
    for node, dur_ms, tin, tout in NODES:
        spans.append(
            {
                "trace_id": trace_id,
                "span_id": str(uuid.uuid4()),
                "parent_span_id": root_id,
                "gen_ai.operation.name": "chat",
                "gen_ai.request.model": "claude-haiku-4-5",
                "gen_ai.provider.name": "anthropic",
                "node_name": node,
                "pipeline_name": PIPELINE,
                "start_time_ns": cursor,
                "end_time_ns": cursor + dur_ms * 1_000_000,
                "gen_ai.usage.input_tokens": tin,
                "gen_ai.usage.output_tokens": tout,
                "status": "ok",
            }
        )
        cursor += dur_ms * 1_000_000
    spans.append(
        {
            "trace_id": trace_id,
            "span_id": root_id,
            "parent_span_id": None,
            "gen_ai.operation.name": "invoke_agent",
            "node_name": "invoke_agent",
            "pipeline_name": PIPELINE,
            "start_time_ns": t0,
            "end_time_ns": cursor,
            "status": "ok",
            "trace_end": True,
        }
    )
    return trace_id, spans


def fail(message: str, *hints: str) -> int:
    print(f"\n  SMOKE TEST FAILED: {message}")
    for hint in hints:
        print(f"    - {hint}")
    return 1


async def main_async(timeout_s: float) -> int:
    settings = get_settings()
    trace_id, spans = build_trace()

    print(f"  ingest   {settings.ingest_url}")
    print(f"  trace_id {trace_id}")

    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            resp = await client.post(
                f"{settings.ingest_url}/v1/spans", json={"spans": spans}
            )
        except httpx.ConnectError:
            return fail(
                "cannot reach the ingestion service",
                f"is it running? `make ingest` (expected at {settings.ingest_url})",
            )
        if resp.status_code == 503:
            return fail(
                "ingestion shed the request (503 backpressure)",
                "the worker is behind, or the backlog threshold is set too low",
            )
        if resp.status_code != 202:
            return fail(f"POST /v1/spans returned {resp.status_code}", resp.text[:300])
        body = resp.json()
        if body.get("accepted") != len(spans):
            return fail(f"only {body.get('accepted')}/{len(spans)} spans accepted",
                        json.dumps(body)[:300])
    print(f"  accepted {len(spans)} spans")

    conn = await asyncpg.connect(dsn=settings.postgres_dsn)
    redis = await get_redis()
    try:
        deadline = time.monotonic() + timeout_s
        span_rows = trace_row = None
        while time.monotonic() < deadline:
            span_rows = await conn.fetchval(
                "SELECT count(*) FROM spans WHERE trace_id = $1", uuid.UUID(trace_id)
            )
            trace_row = await conn.fetchrow(
                "SELECT path, total_cost_usd, span_count, status, finalized_by"
                " FROM traces WHERE trace_id = $1",
                uuid.UUID(trace_id),
            )
            if trace_row is not None:
                break
            await asyncio.sleep(0.25)

        if trace_row is None:
            backlog = await consumer_group_backlog(
                redis, redis_keys.STREAM_SPANS, settings.worker_consumer_group
            )
            if span_rows:
                return fail(
                    f"{span_rows} spans stored but the trace never finalized",
                    "the consumer is running but the finalizer is not — check `aoe-worker` logs",
                )
            return fail(
                "spans never reached Postgres",
                f"consumer-group backlog is {backlog}",
                "is `aoe-worker` running?" if backlog else "the stream is empty — did XADD succeed?",
            )

        path = list(trace_row["path"])
        cost = float(trace_row["total_cost_usd"])
        print(f"  stored   {span_rows} spans")
        print(f"  path     {'>'.join(path)}")
        print(f"  cost     ${cost:.8f}")
        print(f"  status   {trace_row['status']} (finalized_by={trace_row['finalized_by']})")

        expected_path = [n for n, *_ in NODES]
        if path != expected_path:
            return fail(
                f"path is {path}, expected {expected_path}",
                "the root invoke_agent envelope must not appear in the path",
            )
        if abs(cost - EXPECTED_COST) > 1e-8:
            return fail(f"cost is ${cost:.8f}, expected ${EXPECTED_COST:.8f}",
                        "check config/pricing.yaml against the rates used here")
        if trace_row["span_count"] != len(spans):
            return fail(f"span_count is {trace_row['span_count']}, expected {len(spans)}")
        if trace_row["finalized_by"] != "trace_end":
            return fail(
                f"finalized_by is {trace_row['finalized_by']}, expected trace_end",
                "the explicit end-of-trace signal was missed and the reaper closed it instead",
            )
    finally:
        await conn.close()
        await close_redis()

    print("\n  SMOKE TEST PASSED — ingest -> stream -> worker -> Postgres is healthy\n")
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="End-to-end pipeline smoke test")
    parser.add_argument("--timeout", type=float, default=20.0)
    args = parser.parse_args()
    raise SystemExit(asyncio.run(main_async(args.timeout)))


if __name__ == "__main__":
    main()
