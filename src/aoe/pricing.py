"""Cost attribution from the static pricing table (design doc §7.7).

The whole point of loading this from a YAML file rather than inlining constants
is that the assumption stays visible. `PricingTable.as_of` and `.source` are
surfaced by the query API at /system/pricing so the dashboard can say, out loud,
which snapshot the dollar figures came from.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from pathlib import Path

import yaml


@dataclass(frozen=True)
class ModelRate:
    """USD per 1M tokens."""

    input: float
    output: float
    note: str | None = None


@dataclass
class PricingTable:
    version: int
    as_of: str
    source: str
    currency: str
    models: dict[str, ModelRate]
    cache_write_5m_multiplier: float = 1.25
    cache_write_1h_multiplier: float = 2.0
    cache_read_multiplier: float = 0.1
    # Models seen at runtime with no entry in the table. Counted, not guessed.
    unknown_models: set[str] = field(default_factory=set)

    def rate_for(self, model_name: str | None) -> ModelRate | None:
        if not model_name:
            return None
        rate = self.models.get(model_name)
        if rate is None:
            self.unknown_models.add(model_name)
        return rate

    def cost_usd(
        self,
        model_name: str | None,
        input_tokens: int,
        output_tokens: int,
        cache_read_tokens: int = 0,
        cache_write_tokens: int = 0,
        cache_ttl: str = "5m",
    ) -> float:
        """Cost of one model call.

        `input_tokens` is the uncached remainder — cache_read/cache_write are
        additive, matching the shape of `usage` on an Anthropic response. Getting
        this wrong is the most common way a cost dashboard quietly under-reports.
        """
        rate = self.rate_for(model_name)
        if rate is None:
            return 0.0

        per_token_in = rate.input / 1_000_000.0
        per_token_out = rate.output / 1_000_000.0

        write_multiplier = (
            self.cache_write_1h_multiplier if cache_ttl == "1h" else self.cache_write_5m_multiplier
        )

        return (
            input_tokens * per_token_in
            + output_tokens * per_token_out
            + cache_read_tokens * per_token_in * self.cache_read_multiplier
            + cache_write_tokens * per_token_in * write_multiplier
        )

    def as_dict(self) -> dict:
        return {
            "version": self.version,
            "as_of": self.as_of,
            "source": self.source,
            "currency": self.currency,
            "models": {
                name: {"input": r.input, "output": r.output, "note": r.note}
                for name, r in sorted(self.models.items())
            },
            "cache_multipliers": {
                "write_5m": self.cache_write_5m_multiplier,
                "write_1h": self.cache_write_1h_multiplier,
                "read": self.cache_read_multiplier,
            },
            "unknown_models_seen": sorted(self.unknown_models),
        }


def load_pricing(path: str | Path) -> PricingTable:
    raw = yaml.safe_load(Path(path).read_text())
    multipliers = raw.get("cache_multipliers", {})
    return PricingTable(
        version=int(raw.get("version", 1)),
        as_of=str(raw.get("as_of", "unknown")),
        source=str(raw.get("source", "")),
        currency=str(raw.get("currency", "USD")),
        models={
            name: ModelRate(
                input=float(spec["input"]),
                output=float(spec["output"]),
                note=spec.get("note"),
            )
            for name, spec in (raw.get("models") or {}).items()
        },
        cache_write_5m_multiplier=float(multipliers.get("write_5m", 1.25)),
        cache_write_1h_multiplier=float(multipliers.get("write_1h", 2.0)),
        cache_read_multiplier=float(multipliers.get("read", 0.1)),
    )


_lock = threading.Lock()
_cached: PricingTable | None = None


def get_pricing(path: str | Path | None = None) -> PricingTable:
    global _cached
    with _lock:
        if _cached is None:
            if path is None:
                from aoe.config import get_settings

                path = get_settings().pricing_file
            _cached = load_pricing(path)
        return _cached


def reset_pricing_cache() -> None:
    """Test hook."""
    global _cached
    with _lock:
        _cached = None
