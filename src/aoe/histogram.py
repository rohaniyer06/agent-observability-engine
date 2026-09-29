"""Log-linear (HDR-style) histogram used for percentile rollups.

Why this instead of the design doc's t-digest-in-the-worker sketch: the worker is
a *pool*. A t-digest living in one worker's memory cannot be merged with the
digests in its siblings, and `latency_rollups` has PK (node_name, bucket_start),
so two workers flushing the same minute would fight over the row.

A bucketed histogram sidesteps both problems. Bucket assignment is a pure
function of the value, so every worker computes the same index, and `HINCRBY` on
a Redis hash merges them atomically. A single flusher then reads one merged
histogram per (node, minute) and upserts one row.

Accuracy: SUB_BUCKET_BITS = 5 gives 32 sub-buckets per power of two, so the
relative error on any reported percentile is bounded at 1/64 ~= 1.6%. For a
latency dashboard that is well inside the noise floor.

Footprint: values are microseconds. A 1000-second observation maps to index
~860, so a full histogram is under ~900 hash fields regardless of throughput.
That is the whole point — cost is bounded by the *range* of values, not by how
many you record.
"""

from __future__ import annotations

SUB_BUCKET_BITS = 5
SUB_BUCKET_COUNT = 1 << SUB_BUCKET_BITS  # 32


def bucket_index(value_us: int) -> int:
    """Map a microsecond duration onto its histogram bucket."""
    if value_us <= 0:
        return 0
    if value_us < SUB_BUCKET_COUNT:
        # Linear region: exact, one bucket per microsecond.
        return value_us

    magnitude = value_us.bit_length() - 1  # >= SUB_BUCKET_BITS here
    shift = magnitude - SUB_BUCKET_BITS
    return ((shift + 1) << SUB_BUCKET_BITS) + ((value_us >> shift) - SUB_BUCKET_COUNT)


def bucket_lower_bound(index: int) -> int:
    """Smallest microsecond value that lands in `index`."""
    if index < SUB_BUCKET_COUNT:
        return index
    shift = (index >> SUB_BUCKET_BITS) - 1
    return (SUB_BUCKET_COUNT + (index & (SUB_BUCKET_COUNT - 1))) << shift


def bucket_upper_bound(index: int) -> int:
    """Largest microsecond value that lands in `index`."""
    return bucket_lower_bound(index + 1) - 1


def bucket_midpoint(index: int) -> int:
    """Representative value for a bucket.

    Midpoint rather than upper bound: upper-bound reporting biases every
    percentile high by up to the bucket width, which on a p99 chart reads as a
    systematic latency inflation that isn't real.
    """
    return (bucket_lower_bound(index) + bucket_upper_bound(index)) // 2


def percentiles(counts: dict[int, int], quantiles: list[float]) -> dict[float, int]:
    """Estimate quantiles from a bucket_index -> count mapping.

    Returns {quantile: microseconds}. An empty histogram yields zeros so callers
    never have to special-case a quiet minute.
    """
    total = sum(counts.values())
    if total == 0:
        return dict.fromkeys(quantiles, 0)

    ordered = sorted(counts.items())
    results: dict[float, int] = {}

    # One pass over the buckets, advancing through the (sorted) quantile list.
    targets = sorted(quantiles)
    ti = 0
    cumulative = 0
    for index, count in ordered:
        cumulative += count
        while ti < len(targets):
            # Rank of the requested quantile, 1-based.
            rank = targets[ti] * total
            if cumulative + 1e-9 >= rank:
                results[targets[ti]] = bucket_midpoint(index)
                ti += 1
            else:
                break
        if ti >= len(targets):
            break

    # Anything left over sits in the final bucket (float rounding at q=1.0).
    if ordered:
        last_index = ordered[-1][0]
        for q in targets[ti:]:
            results[q] = bucket_midpoint(last_index)

    return {q: results[q] for q in quantiles}


class LocalHistogram:
    """In-process histogram. Used by the load generator and by tests.

    The worker does not use this — it writes straight to Redis via HINCRBY so
    that concurrent workers merge. This class exists for the single-process case
    where a round trip per observation would be silly.
    """

    __slots__ = ("counts", "total", "max_value")

    def __init__(self) -> None:
        self.counts: dict[int, int] = {}
        self.total = 0
        self.max_value = 0

    def record(self, value_us: int) -> None:
        idx = bucket_index(value_us)
        self.counts[idx] = self.counts.get(idx, 0) + 1
        self.total += 1
        if value_us > self.max_value:
            self.max_value = value_us

    def merge(self, other: LocalHistogram) -> None:
        for idx, count in other.counts.items():
            self.counts[idx] = self.counts.get(idx, 0) + count
        self.total += other.total
        self.max_value = max(self.max_value, other.max_value)

    def quantiles(self, qs: list[float]) -> dict[float, int]:
        return percentiles(self.counts, qs)

    def summary(self) -> dict[str, float]:
        q = self.quantiles([0.5, 0.95, 0.99])
        return {
            "count": self.total,
            "p50_ms": q[0.5] / 1000.0,
            "p95_ms": q[0.95] / 1000.0,
            "p99_ms": q[0.99] / 1000.0,
            "max_ms": self.max_value / 1000.0,
        }
