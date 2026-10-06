"""The accuracy claim in DEVIATIONS.md #1 has to be proven, not asserted.

If these bounds don't hold, every percentile the dashboard shows is wrong.
"""

from __future__ import annotations

import random

import pytest

from aoe.histogram import (
    LocalHistogram,
    bucket_index,
    bucket_lower_bound,
    bucket_midpoint,
    bucket_upper_bound,
    percentiles,
)

VALUES = [0, 1, 31, 32, 33, 63, 64, 65, 1_000, 12_345, 999_999, 1_000_000, 60_000_000]


@pytest.mark.parametrize("value", VALUES)
def test_value_falls_inside_its_own_bucket(value: int) -> None:
    idx = bucket_index(value)
    assert bucket_lower_bound(idx) <= value <= bucket_upper_bound(idx)


def test_bucket_index_is_monotonic() -> None:
    previous = -1
    for value in range(0, 200_000, 97):
        idx = bucket_index(value)
        assert idx >= previous
        previous = idx


def test_relative_error_stays_within_the_documented_bound() -> None:
    # 1/64 is the theoretical bound for 5 sub-bucket bits; 2% leaves headroom
    # for the midpoint estimator without letting a real regression through.
    idx = bucket_index(987_654)
    assert abs(bucket_midpoint(idx) - 987_654) / 987_654 < 1.0 / 64


def test_percentiles_track_exact_values_on_a_skewed_sample() -> None:
    rng = random.Random(7)
    values = [int(rng.lognormvariate(11, 1)) for _ in range(200_000)]
    hist = LocalHistogram()
    for v in values:
        hist.record(v)

    ordered = sorted(values)
    for q in (0.5, 0.95, 0.99):
        exact = ordered[int(q * len(ordered)) - 1]
        estimate = hist.quantiles([q])[q]
        assert abs(estimate - exact) / exact < 0.02, f"q={q} exact={exact} est={estimate}"


def test_merge_equals_recording_everything_into_one() -> None:
    """The whole multi-worker design rests on this being true."""
    rng = random.Random(11)
    left_values = [rng.randint(1, 5_000_000) for _ in range(5_000)]
    right_values = [rng.randint(1, 5_000_000) for _ in range(5_000)]

    left, right, combined = LocalHistogram(), LocalHistogram(), LocalHistogram()
    for v in left_values:
        left.record(v)
        combined.record(v)
    for v in right_values:
        right.record(v)
        combined.record(v)
    left.merge(right)

    assert left.counts == combined.counts
    assert left.total == combined.total
    assert left.max_value == combined.max_value


def test_empty_histogram_returns_zeros_rather_than_raising() -> None:
    assert percentiles({}, [0.5, 0.99]) == {0.5: 0, 0.99: 0}
    assert LocalHistogram().summary()["count"] == 0


def test_bucket_count_is_bounded_by_range_not_volume() -> None:
    """Memory must scale with the spread of values, not how many arrive."""
    rng = random.Random(3)
    hist = LocalHistogram()
    for _ in range(500_000):
        hist.record(rng.randint(1, 60_000_000))
    assert len(hist.counts) < 1_000
