from __future__ import annotations

from pathlib import Path

import pytest

from aoe.config import get_settings
from aoe.pricing import load_pricing


@pytest.fixture(scope="module")
def table():
    return load_pricing(Path(get_settings().pricing_file))


def test_basic_cost(table) -> None:
    # 1M input at $1/M + 1M output at $5/M
    assert table.cost_usd("claude-haiku-4-5", 1_000_000, 1_000_000) == pytest.approx(6.0)


def test_cached_tokens_are_additive_not_a_subset(table) -> None:
    """`input_tokens` is the UNCACHED remainder on an Anthropic response.

    Treating it as the total is the standard way a cost dashboard silently
    under-reports, so pin the arithmetic.
    """
    cost = table.cost_usd(
        "claude-haiku-4-5",
        input_tokens=1_000,
        output_tokens=500,
        cache_read_tokens=4_000,
        cache_write_tokens=2_000,
    )
    expected = (
        1_000 * 1e-6              # uncached input
        + 500 * 5e-6              # output
        + 4_000 * 1e-6 * 0.1      # cache reads bill at 0.1x
        + 2_000 * 1e-6 * 1.25     # 5-minute cache writes bill at 1.25x
    )
    assert cost == pytest.approx(expected)


def test_one_hour_cache_writes_cost_more(table) -> None:
    five_min = table.cost_usd("claude-haiku-4-5", 0, 0, cache_write_tokens=1_000)
    one_hour = table.cost_usd(
        "claude-haiku-4-5", 0, 0, cache_write_tokens=1_000, cache_ttl="1h"
    )
    assert one_hour > five_min


def test_unknown_model_is_flagged_not_guessed(table) -> None:
    assert table.cost_usd("some-model-that-does-not-exist", 10_000, 10_000) == 0.0
    assert "some-model-that-does-not-exist" in table.unknown_models
    assert "some-model-that-does-not-exist" in table.as_dict()["unknown_models_seen"]


def test_table_carries_its_provenance(table) -> None:
    """A cost figure with no stated source is not a trustworthy number (§7.7)."""
    assert table.as_of and table.as_of != "unknown"
    assert table.source.startswith("http")
