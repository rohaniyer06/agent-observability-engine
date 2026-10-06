"""Shared fixtures and the integration-marker policy.

Integration tests are skipped (not failed) when Docker isn't up, so `make test`
is meaningful on a laptop with nothing running.
"""

from __future__ import annotations

import socket
from urllib.parse import urlparse

import pytest

from aoe.config import get_settings


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers", "integration: requires Postgres and Redis (make up)"
    )


def _reachable(host: str, port: int, timeout: float = 0.4) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


@pytest.fixture(scope="session")
def settings():
    return get_settings()


@pytest.fixture(scope="session")
def infra_available(settings) -> bool:
    pg = urlparse(settings.postgres_dsn)
    rd = urlparse(settings.redis_url)
    return _reachable(pg.hostname or "localhost", pg.port or 5432) and _reachable(
        rd.hostname or "localhost", rd.port or 6379
    )


@pytest.fixture(autouse=True)
def _skip_integration_without_infra(request, infra_available):
    if request.node.get_closest_marker("integration") and not infra_available:
        pytest.skip("Postgres/Redis not reachable — run `make up`")
