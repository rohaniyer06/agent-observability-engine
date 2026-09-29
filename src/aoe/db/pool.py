"""asyncpg connection pool. No ORM.

An ingestion pipeline's hot path is bulk inserts and a handful of hand-written
aggregate queries; an ORM buys nothing here and costs a layer of indirection
over exactly the statements whose shape matters most.
"""

from __future__ import annotations

import asyncio
import json

import asyncpg

_pool: asyncpg.Pool | None = None
_lock = asyncio.Lock()


async def _init_connection(conn: asyncpg.Connection) -> None:
    # Hand JSONB back as dicts rather than strings.
    await conn.set_type_codec(
        "jsonb",
        encoder=json.dumps,
        decoder=json.loads,
        schema="pg_catalog",
    )


async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is not None:
        return _pool
    async with _lock:
        if _pool is None:
            from aoe.config import get_settings

            settings = get_settings()
            _pool = await asyncpg.create_pool(
                dsn=settings.postgres_dsn,
                min_size=settings.pg_min_pool,
                max_size=settings.pg_max_pool,
                init=_init_connection,
                command_timeout=30,
            )
    return _pool


async def close_pool() -> None:
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None
