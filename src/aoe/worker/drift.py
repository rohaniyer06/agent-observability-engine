"""Path drift detection.

DEVIATIONS.md #5. The design doc's §7.6 warning is the whole problem here: with
four nodes and a couple of deliberate failure branches, a naive "is this path in
the allowlist" check flags every non-happy-path run and the drift panel becomes
the noisiest thing on the dashboard. Two changes fix that:

* the baseline is *learned* as well as seeded — a path that accounts for enough
  of the traffic is normal by definition, whether or not anyone wrote it down;
* drift events are deduplicated behind a cooldown, so a novel path taken 400
  times is one row with occurrence_count 400 rather than 400 rows.

`path_signature` and `evaluate_path` are pure. The Redis/Postgres wrappers are
below them.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import TYPE_CHECKING

from aoe import redis_keys

if TYPE_CHECKING:  # pragma: no cover - typing only
    import asyncpg
    from redis.asyncio import Redis

    from aoe.config import Settings

SIGNATURE_SEPARATOR = ">"


def path_signature(path: list[str]) -> str:
    """Ordered node names joined with '>' — e.g. 'extract>classify>escalate'."""
    return SIGNATURE_SEPARATOR.join(path)


@dataclass(frozen=True)
class DriftVerdict:
    is_drift: bool
    is_novel: bool  # signature never observed before
    baseline_signatures: list[str]  # sorted


# A path has to be seen more than once before frequency can vouch for it.
# Seeded paths bypass this entirely — an operator declaring a path expected is
# stronger evidence than any amount of observation.
_MIN_BASELINE_OCCURRENCES = 2


def evaluate_path(
    signature: str,
    counts: dict[str, int],  # signature -> observed count, INCLUDING this trace
    seeded: set[str],  # explicitly-seeded baseline signatures
    *,
    min_traces: int,
    baseline_freq_pct: float,
) -> DriftVerdict:
    """Decide whether an observed path is a departure from the pipeline's norm."""
    total = sum(counts.values())
    observed = counts.get(signature, 0)

    baseline = set(seeded)
    # Below min_traces the frequency test has no statistical footing: the first
    # path a pipeline ever takes would be 100% of traffic and instantly become
    # "baseline", which is how a drift detector learns to detect nothing. Until
    # then only what an operator seeded counts as normal.
    if total >= min_traces:
        # count/total*100 >= pct, without the division.
        #
        # The occurrence floor is what makes the frequency rule mean what it
        # says. Frequency alone is a share test, and a share test is trivially
        # satisfied by a first-ever sighting while the denominator is small: at
        # the default 1%, one observation out of 87 traces clears 1% and the
        # brand-new path is filed as normal. That silently disables drift
        # detection for every pipeline with fewer than 100/pct traces — which is
        # precisely the range a demo or a fresh deployment lives in. One
        # observation is never evidence that a path is routine; requiring a
        # repeat is what separates "common enough to be normal" from "happened".
        baseline.update(
            sig
            for sig, n in counts.items()
            if n >= _MIN_BASELINE_OCCURRENCES and n * 100.0 >= total * baseline_freq_pct
        )

    return DriftVerdict(
        is_drift=signature not in baseline,
        is_novel=observed <= 1,
        baseline_signatures=sorted(baseline),
    )


# ---------------------------------------------------------------------------
# IO wrappers
# ---------------------------------------------------------------------------


async def observe_path(
    redis: Redis,
    pipeline_name: str,
    signature: str,
    seeded: set[str],
    settings: Settings,
) -> DriftVerdict:
    """Count this trace's path and score it. One round trip."""
    key = redis_keys.path_counts(pipeline_name)
    pipe = redis.pipeline(transaction=False)
    pipe.hincrby(key, signature, 1)
    pipe.hgetall(key)
    _, raw = await pipe.execute()

    counts = {sig: int(n) for sig, n in raw.items()}
    return evaluate_path(
        signature,
        counts,
        seeded,
        min_traces=settings.drift_min_traces,
        baseline_freq_pct=settings.drift_baseline_freq_pct,
    )


async def claim_drift_window(
    redis: Redis,
    pipeline_name: str,
    signature: str,
    cooldown_ms: int,
) -> bool:
    """True when this caller opened a new cooldown window for the path.

    True  -> insert a fresh path_drift_events row.
    False -> a window is already open; bump occurrence_count on the existing row.

    SET NX is the claim, so two workers finalizing two traces on the same novel
    path in the same millisecond still produce exactly one event row.
    """
    ok = await redis.set(
        redis_keys.drift_cooldown(pipeline_name, signature),
        str(int(time.time() * 1000)),
        px=max(1, cooldown_ms),
        nx=True,
    )
    return bool(ok)


_UPSERT_PIPELINE_PATH = """
INSERT INTO pipeline_paths (pipeline_name, path_signature, path, occurrences)
VALUES ($1, $2, $3, 1)
ON CONFLICT (pipeline_name, path_signature) DO UPDATE
SET occurrences  = pipeline_paths.occurrences + 1,
    last_seen_at = now()
"""

_INSERT_DRIFT_EVENT = """
INSERT INTO path_drift_events (
    pipeline_name, baseline_paths, observed_path, path_signature, trace_id
) VALUES ($1, $2, $3, $4, $5)
RETURNING id, occurrence_count
"""

# Bump the newest event for this signature rather than inserting a duplicate.
_BUMP_DRIFT_EVENT = """
UPDATE path_drift_events
SET occurrence_count = occurrence_count + 1,
    detected_at      = now()
WHERE id = (
    SELECT id FROM path_drift_events
    WHERE pipeline_name = $1 AND path_signature = $2
    ORDER BY id DESC LIMIT 1
)
RETURNING id, occurrence_count
"""


async def record_path_observation(
    conn: asyncpg.Connection,
    pipeline_name: str,
    signature: str,
    path: list[str],
) -> None:
    """Count the path in `pipeline_paths` — every trace, not just drifting ones.

    This table is what makes the baseline self-calibrating, so it has to see the
    normal traffic too; counting only drift would leave the denominator empty.
    """
    await conn.execute(_UPSERT_PIPELINE_PATH, pipeline_name, signature, path)


async def persist_drift_event(
    conn: asyncpg.Connection,
    pipeline_name: str,
    signature: str,
    path: list[str],
    trace_id: str,
    verdict: DriftVerdict,
    *,
    new_window: bool,
) -> tuple[int, int] | None:
    """Insert a new drift event or bump the open one. Returns (id, occurrences)."""
    if not new_window:
        row = await conn.fetchrow(_BUMP_DRIFT_EVENT, pipeline_name, signature)
        if row is not None:
            return int(row["id"]), int(row["occurrence_count"])
        # Cooldown key outlived its row (a wiped database, or a cooldown set by a
        # previous run). Fall through and insert rather than lose the event.

    row = await conn.fetchrow(
        _INSERT_DRIFT_EVENT,
        pipeline_name,
        verdict.baseline_signatures,
        path,
        signature,
        trace_id,
    )
    if row is None:  # pragma: no cover - INSERT ... RETURNING always yields a row
        return None
    return int(row["id"]), int(row["occurrence_count"])


class SeededPaths:
    """TTL cache over `pipeline_paths.is_seeded`.

    Seeding is an operator action (design doc §7.6) that changes on human
    timescales, so re-reading it per finalized trace would be one query per trace
    for a set that changes once a day.
    """

    def __init__(self, ttl_s: float = 30.0) -> None:
        self._ttl_s = ttl_s
        self._cache: dict[str, tuple[float, set[str]]] = {}

    async def get(self, conn: asyncpg.Connection, pipeline_name: str) -> set[str]:
        now = time.monotonic()
        cached = self._cache.get(pipeline_name)
        if cached is not None and cached[0] > now:
            return cached[1]

        rows = await conn.fetch(
            "SELECT path_signature FROM pipeline_paths "
            "WHERE pipeline_name = $1 AND is_seeded",
            pipeline_name,
        )
        seeded = {r["path_signature"] for r in rows}
        self._cache[pipeline_name] = (now + self._ttl_s, seeded)
        return seeded
