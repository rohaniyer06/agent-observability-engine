"""Query API service — read side + live fan-out (design doc §5.4). Port 8001.

Separate process from ingestion on purpose: the load generator saturates the
ingest event loop, and dashboard reads must not queue behind it. See docs/API.md.
"""

from __future__ import annotations

import contextlib
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from aoe import __version__
from aoe.api import (
    routes_anomalies,
    routes_live,
    routes_metrics,
    routes_system,
    routes_traces,
)
from aoe.api.routes_live import LiveFeedBroker
from aoe.config import get_settings
from aoe.db.pool import close_pool, get_pool
from aoe.logging import log_fields, setup_logging
from aoe.redis_client import close_redis, get_redis
from aoe.redis_keys import PUBSUB_LIVE

settings = get_settings()
log = logging.getLogger("aoe.api")


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    setup_logging(settings.log_level, "aoe.api")

    # Warm both clients, but do not refuse to boot if one is down. /health is the
    # endpoint that reports dependency state; a read service that exits because
    # Postgres blipped during startup is strictly less useful than one that comes
    # up degraded and says so.
    for name, warm in (("postgres", get_pool), ("redis", get_redis)):
        try:
            await warm()
        except Exception as exc:
            log_fields(log, logging.WARNING, "startup: dependency unavailable",
                       dependency=name, error=str(exc))

    # ONE pub/sub subscription for the whole process, regardless of how many
    # dashboard sockets connect (routes_live.LiveFeedBroker).
    broker = LiveFeedBroker(PUBSUB_LIVE)
    await broker.start()
    app.state.live_broker = broker
    try:
        yield
    finally:
        await broker.stop()
        with contextlib.suppress(Exception):
            await close_redis()
        with contextlib.suppress(Exception):
            await close_pool()


app = FastAPI(
    title="Agent Observability Engine — Query API",
    version=__version__,
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origin_list,
    allow_credentials=True,
    # Read-only service. OPTIONS is handled by the middleware itself.
    allow_methods=["GET"],
    allow_headers=["*"],
)

for module in (routes_traces, routes_metrics, routes_anomalies, routes_system, routes_live):
    app.include_router(module.router)
