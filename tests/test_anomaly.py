"""Cost anomaly detection — DEVIATIONS.md #4 and bug D."""

from __future__ import annotations

from aoe.worker.anomaly import evaluate_cost

KW = {"min_samples": 30, "warn_z": 3.5, "critical_z": 6.0}


def test_cold_start_never_fires() -> None:
    """Below min_samples the statistic is meaningless; firing there is noise."""
    verdict = evaluate_cost(99.0, [0.001] * 5, **KW)
    assert verdict.is_anomaly is False
    assert verdict.severity is None
    assert verdict.sample_size == 5


def test_large_outlier_is_critical() -> None:
    window = [0.0010 + (i % 5) * 1e-5 for i in range(60)]
    verdict = evaluate_cost(0.05, window, **KW)
    assert verdict.is_anomaly is True
    assert verdict.severity == "critical"
    assert verdict.deviation_pct > 1000


def test_cheap_trace_never_fires() -> None:
    """A trace costing less than the median is not a cost incident."""
    window = [0.0010] * 60
    verdict = evaluate_cost(0.0001, window, **KW)
    assert verdict.is_anomaly is False


def test_identical_window_does_not_divide_by_zero() -> None:
    """MAD is 0 when every sample matches; must not produce inf/NaN."""
    verdict = evaluate_cost(0.0010, [0.0010] * 60, **KW)
    assert verdict.is_anomaly is False
    assert verdict.z_score == verdict.z_score  # not NaN


def test_robust_to_a_skewed_window() -> None:
    """The reason for median+MAD over mean+stddev.

    A handful of huge samples drag mean and stddev enough that a plain z-score
    stops flagging real outliers. Median and MAD barely move.
    """
    # Costs must vary for MAD to be non-zero, which is the realistic case; the
    # five extreme samples are what a plain stddev z-score would choke on.
    window = [0.001 + i * 1e-6 for i in range(55)] + [1.0] * 5
    verdict = evaluate_cost(0.05, window, **KW)
    assert verdict.is_anomaly is True, "median+MAD must still catch a 50x outlier"

    # Demonstrate the failure being avoided: the same input under mean/stddev.
    import statistics
    mean, stddev = statistics.fmean(window), statistics.pstdev(window)
    assert (0.05 - mean) / stddev < 3.5, "stddev z-score would have missed this"


def test_expected_cost_is_the_median() -> None:
    window = [0.001] * 30 + [0.003] * 30
    verdict = evaluate_cost(0.5, window, **KW)
    assert 0.001 <= verdict.expected_cost_usd <= 0.003
