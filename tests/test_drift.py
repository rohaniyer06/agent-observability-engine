"""Path drift — DEVIATIONS.md #5 and bug C."""

from __future__ import annotations

from aoe.worker.drift import evaluate_path, path_signature

KW = {"min_traces": 50, "baseline_freq_pct": 1.0}
HAPPY = "extract>classify>decide"


def test_signature_is_ordered_and_joined() -> None:
    assert path_signature(["extract", "classify", "decide"]) == HAPPY
    assert path_signature(["classify", "extract"]) != path_signature(["extract", "classify"])


def test_seeded_paths_are_never_drift() -> None:
    verdict = evaluate_path(HAPPY, {HAPPY: 1}, {HAPPY}, **KW)
    assert verdict.is_drift is False


def test_first_sighting_is_drift_even_when_it_clears_the_share_threshold() -> None:
    """Regression for bug C.

    One observation out of 87 traces is 1.15%, which clears the 1% frequency
    bar. Before the occurrence floor this filed a brand-new path as baseline and
    drift detection was silently dead below ~100 traces.
    """
    counts = {HAPPY: 86, "extract>decide>classify>escalate": 1}
    verdict = evaluate_path("extract>decide>classify>escalate", counts, {HAPPY}, **KW)
    assert verdict.is_drift is True
    assert verdict.is_novel is True


def test_a_repeated_unseeded_path_still_earns_baseline() -> None:
    """The self-calibrating half must keep working."""
    counts = {HAPPY: 80, "extract>classify>escalate": 20}
    verdict = evaluate_path("extract>classify>escalate", counts, {HAPPY}, **KW)
    assert verdict.is_drift is False


def test_below_min_traces_only_seeded_counts_as_normal() -> None:
    counts = {HAPPY: 3, "extract": 2}
    assert evaluate_path("extract", counts, {HAPPY}, **KW).is_drift is True
    assert evaluate_path(HAPPY, counts, {HAPPY}, **KW).is_drift is False


def test_rare_unseeded_path_is_drift() -> None:
    counts = {HAPPY: 990, "extract>classify": 2}
    verdict = evaluate_path("extract>classify", counts, {HAPPY}, **KW)
    assert verdict.is_drift is True
