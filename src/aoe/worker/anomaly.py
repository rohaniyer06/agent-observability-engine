"""Cost anomaly detection.

DEVIATIONS.md #4: a modified z-score (median + MAD) rather than the design doc's
mean/stddev z-score. Per-trace cost is right-skewed — one retry loop doubles it —
so the mean and the stddev are both dragged by exactly the outliers the detector
exists to catch, and the threshold desensitises itself at the moment it matters.
The median and the MAD do not move when a few samples blow up.

`evaluate_cost` is pure: window in, verdict out. Everything that touches Redis or
Postgres sits below it, so the rule that decides what counts as an incident is
testable without any infrastructure at all.
"""

from __future__ import annotations

import statistics
from dataclasses import dataclass
from typing import TYPE_CHECKING

from aoe import redis_keys

if TYPE_CHECKING:  # pragma: no cover - typing only
    import asyncpg
    from redis.asyncio import Redis

    from aoe.config import Settings

# Puts MAD on the same scale as a standard deviation for normal data:
# 0.6745 is the 0.75 quantile of the standard normal, so MAD / 0.6745 estimates
# sigma. Reporting on a sigma-equivalent scale is what lets the thresholds stay
# expressed as "z-scores" and mean roughly what a reader expects.
_MAD_SCALE = 0.6745
# Companion constant for the mean-absolute-deviation fallback above.
_MEAN_AD_SCALE = 1.253314


@dataclass(frozen=True)
class AnomalyVerdict:
    is_anomaly: bool
    severity: str | None  # "warn" | "critical" | None
    z_score: float
    expected_cost_usd: float  # the window median
    deviation_pct: float  # (actual - median) / median * 100, 0.0 if median == 0
    sample_size: int


def evaluate_cost(
    cost_usd: float,
    window: list[float],
    *,
    min_samples: int,
    warn_z: float,
    critical_z: float,
) -> AnomalyVerdict:
    """Score one trace's cost against the recent window for its pipeline."""
    sample_size = len(window)
    median = statistics.median(window) if window else 0.0
    deviation_pct = ((cost_usd - median) / median * 100.0) if median else 0.0

    # Cold start (DEVIATIONS.md #4). With a handful of samples any spread
    # estimate is noise and nearly everything trips the threshold, which reads as
    # a broken feature rather than a sensitive one. Report the numbers, do not
    # fire on them.
    if sample_size < min_samples:
        return AnomalyVerdict(False, None, 0.0, median, deviation_pct, sample_size)

    mad = statistics.median([abs(x - median) for x in window])
    if mad > 0:
        z_score = _MAD_SCALE * (cost_usd - median) / mad
    else:
        # MAD collapses to zero whenever half or more of the window is identical
        # — flat-rate pipelines hit this constantly, so this branch is the common
        # case, not the corner case.
        #
        # Falling back to mean/stddev here would undo the entire reason for
        # choosing a robust estimator: stddev is inflated by the very outliers
        # the detector exists to catch, so a window carrying a few extreme
        # samples stops flagging anything. The standard substitute (Iglewicz &
        # Hoaglin) is the MEAN absolute deviation about the median, which only
        # degenerates when every sample is identical.
        mean_ad = statistics.fmean([abs(x - median) for x in window])
        if mean_ad <= 0:
            # Every sample identical. Every spread estimate is zero and the
            # z-score is undefined; dividing anyway yields inf, which would flag
            # the first non-identical cost as critical no matter how small the
            # difference. Report the deviation, withhold the score.
            return AnomalyVerdict(False, None, 0.0, median, deviation_pct, sample_size)
        z_score = (cost_usd - median) / (_MEAN_AD_SCALE * mean_ad)

    # Only excursions ABOVE the median are incidents — a trace that came in cheap
    # is not a cost problem. The score itself is still reported signed so the
    # dashboard can show which side of normal a trace landed on.
    severity: str | None = None
    if cost_usd > median:
        if z_score >= critical_z:
            severity = "critical"
        elif z_score >= warn_z:
            severity = "warn"

    return AnomalyVerdict(
        is_anomaly=severity is not None,
        severity=severity,
        z_score=z_score,
        expected_cost_usd=median,
        deviation_pct=deviation_pct,
        sample_size=sample_size,
    )


# ---------------------------------------------------------------------------
# IO wrappers
# ---------------------------------------------------------------------------


async def observe_cost(
    redis: Redis,
    pipeline_name: str,
    cost_usd: float,
    settings: Settings,
) -> AnomalyVerdict:
    """Record this trace's cost in the rolling window and score it.

    One round trip: push, trim, read. The window returned includes the cost being
    scored — with a 500-sample window a single new observation moves the median
    by at most one rank, and excluding it would mean a second round trip to read
    before writing.
    """
    key = redis_keys.cost_window(pipeline_name)
    pipe = redis.pipeline(transaction=False)
    pipe.lpush(key, f"{float(cost_usd):.10f}")
    pipe.ltrim(key, 0, settings.cost_window_size - 1)
    pipe.lrange(key, 0, settings.cost_window_size - 1)
    _, _, raw = await pipe.execute()

    window = [float(v) for v in raw]
    return evaluate_cost(
        cost_usd,
        window,
        min_samples=settings.cost_min_samples,
        warn_z=settings.cost_warn_zscore,
        critical_z=settings.cost_critical_zscore,
    )


_INSERT_ANOMALY = """
INSERT INTO cost_anomalies (
    trace_id, pipeline_name, expected_cost_usd, actual_cost_usd,
    deviation_pct, z_score, sample_size, severity
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (trace_id) DO NOTHING
RETURNING id
"""


# An open window suppresses repeats, but never an incident this many times
# worse than the one that opened it. Two orders of magnitude apart are not the
# same event, and a detector that hides the bigger one is worse than a noisy one.
_COOLDOWN_BREAKTHROUGH_FACTOR = 2.0


async def claim_cost_window(
    redis: Redis,
    pipeline_name: str,
    severity: str,
    cooldown_ms: int,
    z_score: float,
) -> bool:
    """True when this anomaly should be recorded rather than folded into an open window.

    Plain per-severity dedup is not enough. A sustained cost shift trips the
    threshold on every trace until the rolling window re-centres, so without a
    window one incident becomes dozens of rows. But with a naive window, a
    genuinely catastrophic trace gets swallowed because a merely-bad one claimed
    the window seconds earlier — which is how you lose the alert that mattered.

    So the window stores the magnitude that opened it, and anything materially
    worse takes the window over instead of being suppressed.
    """
    if cooldown_ms <= 0:
        return True

    key = redis_keys.cost_cooldown(pipeline_name, severity)
    magnitude = abs(z_score)

    # SET NX is the claim, so two workers finalizing over-budget traces in the
    # same millisecond still record exactly one row between them.
    if await redis.set(key, f"{magnitude:.6f}", px=cooldown_ms, nx=True):
        return True

    try:
        open_magnitude = float(await redis.get(key) or 0.0)
    except (TypeError, ValueError):
        open_magnitude = 0.0

    if magnitude >= open_magnitude * _COOLDOWN_BREAKTHROUGH_FACTOR:
        # Re-arm at the higher magnitude so the escalation itself does not then
        # become the thing that spams.
        await redis.set(key, f"{magnitude:.6f}", px=cooldown_ms)
        return True
    return False


async def persist_anomaly(
    conn: asyncpg.Connection,
    trace_id: str,
    pipeline_name: str,
    actual_cost_usd: float,
    verdict: AnomalyVerdict,
) -> bool:
    """Insert the anomaly row. False means a redelivery already recorded it."""
    # Imported here, not at module scope, so the pure detector above stays
    # importable with nothing but the standard library installed.
    from aoe.worker.writer import to_numeric

    row = await conn.fetchrow(
        _INSERT_ANOMALY,
        trace_id,
        pipeline_name,
        to_numeric(verdict.expected_cost_usd),
        to_numeric(actual_cost_usd),
        verdict.deviation_pct,
        verdict.z_score,
        verdict.sample_size,
        verdict.severity,
    )
    return row is not None
