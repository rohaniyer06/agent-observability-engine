"""Integration checks. Require `make up` + `make migrate`.

These pin the two things unit tests structurally cannot: that the migration
runner is honest about drift, and that "backlog" means backlog.
"""

from __future__ import annotations

import uuid
from pathlib import Path

import asyncpg
import pytest

from aoe.db.migrate import run_migrations
from aoe.redis_client import close_redis, consumer_group_backlog, ensure_consumer_group, get_redis

pytestmark = pytest.mark.integration


async def test_migrations_are_idempotent(settings) -> None:
    applied = await run_migrations(settings.postgres_dsn, Path(settings.migrations_dir))
    assert applied == [], "a second run must be a no-op"

    conn = await asyncpg.connect(dsn=settings.postgres_dsn)
    try:
        tables = {
            r["tablename"]
            for r in await conn.fetch(
                "SELECT tablename FROM pg_tables WHERE schemaname = 'public'"
            )
        }
    finally:
        await conn.close()

    assert {
        "spans",
        "traces",
        "latency_rollups",
        "cost_anomalies",
        "path_drift_events",
        "pipeline_paths",
    } <= tables


async def test_modified_migration_is_rejected(settings, tmp_path) -> None:
    """A migration edited after it ran must fail loudly, not re-apply silently."""
    (tmp_path / "001_x.sql").write_text("SELECT 1;")
    dsn = settings.postgres_dsn
    conn = await asyncpg.connect(dsn=dsn)
    try:
        await conn.execute("DELETE FROM schema_migrations WHERE version = '001_x'")
    finally:
        await conn.close()

    await run_migrations(dsn, tmp_path)
    (tmp_path / "001_x.sql").write_text("SELECT 2;")  # tamper
    with pytest.raises(RuntimeError, match="modified after it was applied"):
        await run_migrations(dsn, tmp_path)


async def test_backlog_is_not_stream_length(settings) -> None:
    """Regression for DEVIATIONS.md bug A.

    XACK does not shorten a stream. Anything using XLEN as a backlog signal
    reports a growing backlog on a fully-drained system — which is what made
    ingestion shed 38% of a healthy load run.
    """
    redis = await get_redis()
    stream = f"aoe:test:stream:{uuid.uuid4()}"
    group = "test_group"
    try:
        await ensure_consumer_group(redis, stream, group)
        for i in range(10):
            await redis.xadd(stream, {"payload": str(i)})

        assert await consumer_group_backlog(redis, stream, group) == 10

        entries = await redis.xreadgroup(
            groupname=group, consumername="c1", streams={stream: ">"}, count=10
        )
        ids = [eid for _s, batch in entries for eid, _f in batch]
        await redis.xack(stream, group, *ids)

        assert await redis.xlen(stream) == 10, "XLEN still counts acked entries"
        assert await consumer_group_backlog(redis, stream, group) == 0, (
            "a fully acked group has no backlog"
        )
    finally:
        await redis.delete(stream)
        await close_redis()
