"""Pluggable LLM backend for the harness (DEVIATIONS.md #9).

Two implementations behind one interface:

* `AnthropicProvider` — a real API call. This is what runs by default, and is
  what makes the §2 "no synthetic-only testing" decision true.
* `SimulatedProvider` — deterministic, seeded, zero cost, zero network. Exists
  so the repo is runnable without a key and so a 100k-span load test does not
  cost real money.

The graph, the instrumentation, and the entire telemetry path are identical
either way. Only the thing that produces tokens changes.
"""

from __future__ import annotations

import asyncio
import logging
import math
import os
import random
from abc import ABC, abstractmethod
from dataclasses import dataclass

from aoe.harness.tickets import ACTIONS, CATEGORIES, SEVERITIES
from aoe.logging import log_fields

log = logging.getLogger("aoe.harness.providers")


@dataclass
class LLMResult:
    text: str
    input_tokens: int
    output_tokens: int
    model: str
    provider: str  # "anthropic" | "simulated"
    # Present when prompt caching is in play. `input_tokens` is the UNCACHED
    # remainder, so these are additive — see config/pricing.yaml.
    cache_read_tokens: int = 0
    cache_write_tokens: int = 0


class ProviderError(RuntimeError):
    """A model call that did not produce usable text.

    Carries whatever usage the API managed to report so the span still shows the
    tokens the failed call actually burned. A failure that reports zero cost is
    a lie the cost dashboard will repeat.
    """

    def __init__(
        self,
        message: str,
        *,
        usage: LLMResult | None = None,
        retryable: bool = False,
    ) -> None:
        super().__init__(message)
        self.usage = usage
        self.retryable = retryable


class ProviderRefusal(ProviderError):
    """`stop_reason == "refusal"`: HTTP 200, safety classifier declined.

    Not an exception path in the SDK, so it has to be checked explicitly before
    touching `response.content` — which on a pre-output refusal is empty.
    """


class LLMProvider(ABC):
    """One model call. No conversation state — every node call is independent."""

    name: str
    model: str

    @abstractmethod
    async def complete(
        self,
        *,
        system: str,
        user: str,
        max_tokens: int,
        node: str | None = None,
    ) -> LLMResult:
        """Run one completion.

        `node` is advisory metadata, not part of the request: the simulated
        backend uses it to pick a per-node latency distribution (so the p99 chart
        is not four identical lines) and the real backend ignores it entirely.
        """

    async def aclose(self) -> None:  # pragma: no cover - trivial default
        return None


# ---------------------------------------------------------------------------
# Real backend
# ---------------------------------------------------------------------------


class AnthropicProvider(LLMProvider):
    name = "anthropic"

    def __init__(self, settings) -> None:
        from anthropic import AsyncAnthropic

        # Credentials resolve from the environment (ANTHROPIC_API_KEY, or an
        # `ant auth login` profile). Never inject a key here.
        self._client = AsyncAnthropic()
        self.model = settings.harness_model

    async def complete(
        self,
        *,
        system: str,
        user: str,
        max_tokens: int,
        node: str | None = None,
    ) -> LLMResult:
        import anthropic

        try:
            response = await self._client.messages.create(
                model=self.model,
                max_tokens=max_tokens,
                system=system,
                messages=[{"role": "user", "content": user}],
            )
        # Most specific first: RateLimitError is a subclass of APIStatusError,
        # and APIConnectionError is a sibling (no HTTP response at all).
        except anthropic.RateLimitError as exc:
            raise ProviderError(f"rate limited: {exc}", retryable=True) from exc
        except anthropic.APIStatusError as exc:
            raise ProviderError(
                f"api error {exc.status_code}: {exc.message}",
                retryable=exc.status_code >= 500,
            ) from exc
        except anthropic.APIConnectionError as exc:
            raise ProviderError(f"connection error: {exc}", retryable=True) from exc

        usage = self._usage(response)

        # Check the stop reason BEFORE reading content: a refusal can come back
        # with an empty content array, and `content[0]` would raise IndexError
        # instead of the error we actually want to record.
        if response.stop_reason == "refusal":
            detail = getattr(response, "stop_details", None)
            category = getattr(detail, "category", None) if detail else None
            raise ProviderRefusal(f"model refused (category={category})", usage=usage)

        text = "".join(
            block.text for block in response.content if getattr(block, "type", None) == "text"
        )
        usage.text = text
        return usage

    def _usage(self, response) -> LLMResult:
        u = response.usage
        return LLMResult(
            text="",
            # NOTE: usage.input_tokens is the uncached remainder, not the total.
            # Adding the cache counters into it double-bills the prompt.
            input_tokens=getattr(u, "input_tokens", 0) or 0,
            output_tokens=getattr(u, "output_tokens", 0) or 0,
            model=getattr(response, "model", self.model) or self.model,
            provider=self.name,
            # Not present on every model or every response shape.
            cache_read_tokens=getattr(u, "cache_read_input_tokens", 0) or 0,
            cache_write_tokens=getattr(u, "cache_creation_input_tokens", 0) or 0,
        )

    async def aclose(self) -> None:
        await self._client.close()


# ---------------------------------------------------------------------------
# Simulated backend
# ---------------------------------------------------------------------------

# (median seconds, lognormal sigma, median output tokens) per node.
# The medians are deliberately spread so the per-node latency chart shows four
# distinguishable curves: extract is a cheap field pull, classify and decide are
# short judgement calls, escalate writes prose and is the slow one.
_NODE_PROFILES: dict[str, tuple[float, float, int]] = {
    "extract": (0.11, 0.30, 60),
    "classify": (0.30, 0.45, 14),
    "decide": (0.26, 0.40, 12),
    "escalate": (0.58, 0.55, 45),
}
_DEFAULT_PROFILE = (0.25, 0.45, 32)

# Fraction of simulated calls that hit a warm prompt cache. The system prompts
# here are stable across every run, which is exactly the shape prompt caching
# exists for — modelling it keeps the cache columns in the cost path exercised
# rather than permanently zero.
_CACHE_HIT_RATE = 0.20


def _approx_tokens(text: str) -> int:
    return max(1, len(text) // 4)


class SimulatedProvider(LLMProvider):
    """Deterministic stand-in that speaks the same tiny protocol as the real one.

    Determinism caveat, stated rather than buried: draws come from a single
    seeded RNG in call order. At `--concurrency 1` a run is bit-reproducible;
    above that the interleaving decides which run gets which draw, so the
    *distribution* is reproducible but an individual trace is not.
    """

    name = "simulated"

    def __init__(self, settings) -> None:
        # Priced at Haiku rates in config/pricing.yaml so simulated and real
        # runs produce comparable dollar figures.
        self.model = "sim-haiku"
        self._seed = settings.harness_seed
        self._rng = random.Random(settings.harness_seed)

    async def complete(
        self,
        *,
        system: str,
        user: str,
        max_tokens: int,
        node: str | None = None,
    ) -> LLMResult:
        median_s, sigma, out_median = _NODE_PROFILES.get(node or "", _DEFAULT_PROFILE)

        latency_s = self._rng.lognormvariate(math.log(median_s), sigma)
        await asyncio.sleep(min(latency_s, median_s * 12))

        system_tokens = _approx_tokens(system)
        user_tokens = _approx_tokens(user)
        cache_read = 0
        if self._rng.random() < _CACHE_HIT_RATE:
            cache_read = system_tokens
            system_tokens = 0

        output_tokens = int(self._rng.lognormvariate(math.log(out_median), 0.25))
        output_tokens = max(1, min(output_tokens, max_tokens))

        return LLMResult(
            text=self._text(node, user, out_median),
            input_tokens=system_tokens + user_tokens,
            output_tokens=output_tokens,
            model=self.model,
            provider=self.name,
            cache_read_tokens=cache_read,
        )

    def _text(self, node: str | None, user: str, out_median: int) -> str:
        # Category is a fact about the ticket, so derive it from the ticket text:
        # the same input always classifies the same way. Severity and action are
        # judgement calls, so they come from the run RNG and vary.
        content_rng = random.Random(f"{self._seed}:content:{user}")

        if node == "extract":
            return (
                "product_area: " + content_rng.choice(("billing", "auth", "core_app", "api"))
                + "\nreported_issue: " + self._summary_line(user)
                + "\nurgency_signal: " + content_rng.choice(("none", "blocking", "deadline"))
            )
        if node == "classify":
            category = content_rng.choice(CATEGORIES)
            severity = self._rng.choices(SEVERITIES, weights=(0.45, 0.38, 0.17))[0]
            return f"category: {category}\nseverity: {severity}"
        if node == "decide":
            action = self._rng.choices(ACTIONS, weights=(0.34, 0.34, 0.14, 0.18))[0]
            return f"action: {action}"
        if node == "escalate":
            return (
                "Escalating to the on-call support lead: customer impact is "
                "confirmed and the first-line runbook did not resolve it."
            )
        return "ok"

    @staticmethod
    def _summary_line(user: str) -> str:
        first = user.strip().splitlines()[0] if user.strip() else "unspecified"
        return first.removeprefix("Subject:").strip()[:80] or "unspecified"


# ---------------------------------------------------------------------------
# Selection
# ---------------------------------------------------------------------------


def build_provider(settings) -> LLMProvider:
    """Resolve `AOE_HARNESS_PROVIDER` into a concrete backend.

    Announced loudly at startup on purpose: a demo where you cannot tell whether
    real API calls happened is worthless.
    """
    choice = (settings.harness_provider or "auto").strip().lower()
    has_key = bool(os.environ.get("ANTHROPIC_API_KEY"))

    if choice == "anthropic":
        if not has_key:
            raise RuntimeError(
                "AOE_HARNESS_PROVIDER=anthropic but ANTHROPIC_API_KEY is not set. "
                "Export a key, or use --provider simulated / AOE_HARNESS_PROVIDER=simulated."
            )
        provider: LLMProvider = AnthropicProvider(settings)
        reason = "explicitly requested"
    elif choice == "simulated":
        provider = SimulatedProvider(settings)
        reason = "explicitly requested"
    elif choice == "auto":
        if has_key:
            provider = AnthropicProvider(settings)
            reason = "auto: ANTHROPIC_API_KEY is set"
        else:
            provider = SimulatedProvider(settings)
            reason = "auto: ANTHROPIC_API_KEY is not set"
    else:
        raise RuntimeError(
            f"unknown AOE_HARNESS_PROVIDER={choice!r} (expected auto|anthropic|simulated)"
        )

    log_fields(
        log,
        logging.INFO,
        "harness provider selected",
        provider=provider.name,
        model=provider.model,
        reason=reason,
        real_api_calls=provider.name == "anthropic",
    )
    return provider
