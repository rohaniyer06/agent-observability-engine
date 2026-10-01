"""Cursor pagination and the shared query-parameter validators.

Cursors are opaque: base64url of a small JSON tuple whose first element tags
which listing the cursor belongs to. Opaque so the sort key can change without
breaking a client that stored one; tagged so a `/v1/traces` cursor replayed
against `/v1/anomalies` is rejected rather than silently decoded into a
meaningless keyset.

Timestamps round-trip through `datetime.isoformat()` at microsecond precision —
exactly what Postgres keeps in a TIMESTAMPTZ — so the keyset comparison is an
equality-exact resume point, not an approximation that can skip a row.

Decode failures raise `HTTPException(400)` rather than a bare ValueError. A
malformed cursor is a bad request; letting it surface as a 500 would be a lie
about whose fault it is.
"""

from __future__ import annotations

import base64
import binascii
import json
import uuid
from datetime import UTC, datetime, timedelta

from fastapi import HTTPException

# Design doc §5.5 offers 5 min / 1 hr / 24 hr; the rest are the obvious
# in-between steps a latency dashboard wants. Anything else is a 400 — an
# unbounded user-supplied interval is a table scan waiting to happen.
WINDOWS: dict[str, int] = {
    "5m": 300,
    "15m": 900,
    "1h": 3_600,
    "6h": 21_600,
    "24h": 86_400,
    "7d": 604_800,
}


def parse_window(window: str) -> int:
    """Window label -> seconds. 400 on anything off the allowlist."""
    seconds = WINDOWS.get(window)
    if seconds is None:
        raise HTTPException(
            status_code=400,
            detail=f"invalid window '{window}'; expected one of {sorted(WINDOWS)}",
        )
    return seconds


def window_start(window: str, now: datetime | None = None) -> datetime:
    ref = now or datetime.now(UTC)
    return ref - timedelta(seconds=parse_window(window))


def clamp_limit(limit: int, maximum: int) -> int:
    return max(1, min(limit, maximum))


def require_one_of(name: str, value: str | None, allowed: set[str]) -> str | None:
    """Allowlist a string query param, or 400. `None` passes through as no filter."""
    if value is None:
        return None
    if value not in allowed:
        raise HTTPException(
            status_code=400,
            detail=f"invalid {name} '{value}'; expected one of {sorted(allowed)}",
        )
    return value


# ---------------------------------------------------------------------------
# Cursors
# ---------------------------------------------------------------------------


def _encode(tag: str, parts: list[str]) -> str:
    raw = json.dumps([tag, *parts], separators=(",", ":")).encode("utf-8")
    # Strip '=' padding: it survives a URL round trip badly and base64 decoding
    # can re-derive it.
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _decode(tag: str, cursor: str) -> list[str]:
    try:
        padded = cursor + "=" * (-len(cursor) % 4)
        parts = json.loads(base64.urlsafe_b64decode(padded.encode("ascii")))
    except (ValueError, binascii.Error, UnicodeDecodeError) as exc:
        raise HTTPException(status_code=400, detail="malformed cursor") from exc

    if not isinstance(parts, list) or len(parts) < 2 or parts[0] != tag:
        raise HTTPException(status_code=400, detail="cursor does not belong to this listing")
    return [str(p) for p in parts[1:]]


def _ts(value: datetime | str) -> str:
    # Response models already carry ISO-8601 strings, so a handler can build the
    # cursor straight from the last item it is about to return without a
    # parse/format round trip. datetime.fromisoformat accepts the 'Z' suffix on
    # 3.11+, which is the project's floor.
    return value if isinstance(value, str) else value.isoformat()


def encode_trace_cursor(started_at: datetime | str, trace_id: str | uuid.UUID) -> str:
    return _encode("t", [_ts(started_at), str(trace_id)])


def decode_trace_cursor(cursor: str) -> tuple[datetime, uuid.UUID]:
    started_at, trace_id = _decode("t", cursor)[:2]
    try:
        return datetime.fromisoformat(started_at), uuid.UUID(trace_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="malformed cursor") from exc


def encode_anomaly_cursor(detected_at: datetime | str, anomaly_id: int) -> str:
    return _encode("a", [_ts(detected_at), str(anomaly_id)])


def decode_anomaly_cursor(cursor: str) -> tuple[datetime, int]:
    detected_at, anomaly_id = _decode("a", cursor)[:2]
    try:
        return datetime.fromisoformat(detected_at), int(anomaly_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="malformed cursor") from exc
