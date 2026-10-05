"""Fabricated telemetry that looks like the harness's telemetry.

Two rules this module exists to enforce:

1. **Whole traces, never loose spans.** The worker groups by `trace_id`,
   finalizes on `trace_end`, and derives `path` from the node sequence. A stream
   of unrelated spans exercises the INSERT path and nothing else — no
   finalization, no path drift, no per-trace cost. A load test that skips those
   is testing a queue, not this system.
2. **The same shape the real pipeline produces.** Same node mix
   (`extract>classify>decide`, sometimes `>escalate`), same lognormal-ish
   latency spread, plus a deliberate minority of error spans and cost outliers
   so the anomaly detectors have something real to find. Feeding the detectors a
   perfectly uniform stream makes them look like they work when they have simply
   never been asked a question.

Payloads are built through the real `aoe.schema.Span` model and dumped with
`by_alias=True`, so every byte on the wire carries the `gen_ai.*` names. That
matters for more than tidiness: it means a `422` in the load-test results is a
server-side bug, never a generator-side one.
"""

from __future__ import annotations

import math
import random
import time
import uuid
from dataclasses import dataclass
from typing import Any

from aoe.schema import Span

# Priced in config/pricing.yaml at Haiku rates, so a synthetic run's dollar
# figures stay comparable with a real harness run.
MODEL = "sim-haiku"
PROVIDER = "simulated"

# The root span wraps the whole run. `node_name` is required by the schema, so
# it carries the operation name; the worker is expected to keep the root out of
# `path` (path = node names traversed, design doc §4.2).
ROOT_NODE = "invoke_agent"

BASE_PATH: tuple[str, ...] = ("extract", "classify", "decide")


@dataclass(frozen=True)
class NodeProfile:
    """Per-node latency and token shape.

    `median_us` is the lognormal median and `sigma` its shape parameter — the
    right family for request latency (positive, right-skewed, occasional long
    tail) and specifically not a normal distribution, which would produce a
    symmetric p99 that no real service has.
    """

    median_us: float
    sigma: float
    input_median: float
    output_median: float
    token_sigma: float = 0.30


NODE_PROFILES: dict[str, NodeProfile] = {
    # Long prompt, structured output.
    "extract": NodeProfile(median_us=380_000, sigma=0.40, input_median=900, output_median=140),
    # Short classification; the harness injects timeouts here, hence the wider spread.
    "classify": NodeProfile(median_us=240_000, sigma=0.48, input_median=520, output_median=40),
    "decide": NodeProfile(median_us=190_000, sigma=0.35, input_median=380, output_median=60),
    # Retry loop in the harness (§5.1), so both slower and much fatter-tailed.
    "escalate": NodeProfile(median_us=700_000, sigma=0.60, input_median=640, output_median=220),
}

_ERROR_MESSAGES: dict[str, str] = {
    "extract": "upstream timeout while reading ticket body",
    "classify": "model call exceeded 5s deadline",
    "decide": "policy lookup returned no match",
    "escalate": "retry budget exhausted after 3 attempts",
}


@dataclass
class SyntheticConfig:
    pipeline_name: str = "support_ticket_triage"
    model: str = MODEL
    # Matches AOE_HARNESS_ESCALATE_RETRY_RATE's order of magnitude, so the path
    # mix the load test produces is the mix the drift baseline already knows.
    escalate_rate: float = 0.18
    # Matches AOE_HARNESS_HARD_ERROR_RATE. Errors truncate the trace, which also
    # produces the short paths the drift detector should see occasionally.
    error_rate: float = 0.03
    # One trace in a hundred spends ~10x the output tokens. Small enough that the
    # rolling median is unmoved (so the modified z-score keeps its sensitivity),
    # large enough to be unambiguous when it fires.
    cost_outlier_rate: float = 0.01
    cost_outlier_multiplier: float = 10.0
    # Fraction of traces whose first node reads a cached prompt prefix, so the
    # cache-token branch of the cost math is exercised under load rather than
    # only in unit tests.
    cache_hit_rate: float = 0.30
    seed: int = 1337


def _lognormal(rng: random.Random, median: float, sigma: float) -> float:
    """Lognormal draw with the given median (median = exp(mu))."""
    return median * math.exp(rng.gauss(0.0, sigma))


def generate_trace(rng: random.Random, cfg: SyntheticConfig, now_ns: int) -> list[Span]:
    """One trace: 3-5 spans, root `invoke_agent` last and carrying `trace_end`.

    Root last is not cosmetic — it is the contract the worker's finalizer relies
    on (DEVIATIONS.md #2). Emitting it first would let a trace finalize before
    its own children arrived and truncate the path.
    """
    trace_id = uuid.uuid4()
    root_id = uuid.uuid4()

    nodes = list(BASE_PATH)
    if rng.random() < cfg.escalate_rate:
        nodes.append("escalate")

    fail_at = rng.randrange(len(nodes)) if rng.random() < cfg.error_rate else -1
    outlier_at = rng.randrange(len(nodes)) if rng.random() < cfg.cost_outlier_rate else -1

    spans: list[Span] = []
    cursor = now_ns + 1_000_000  # root opens ~1ms before the first child

    for i, node in enumerate(nodes):
        profile = NODE_PROFILES[node]
        start = cursor
        end = start + int(_lognormal(rng, profile.median_us, profile.sigma) * 1_000)
        cursor = end + int(_lognormal(rng, 2_000, 0.5) * 1_000)  # inter-node gap

        errored = i == fail_at
        prompt_tokens = max(1, int(_lognormal(rng, profile.input_median, profile.token_sigma)))
        output_tokens = (
            0
            if errored
            else max(1, int(_lognormal(rng, profile.output_median, profile.token_sigma)))
        )
        if i == outlier_at and not errored:
            output_tokens = int(output_tokens * cfg.cost_outlier_multiplier)

        cache_read = 0
        if i == 0 and rng.random() < cfg.cache_hit_rate:
            # Split the prompt into a cached prefix and an uncached remainder.
            # `input_tokens` stays the REMAINDER — the two are additive, not
            # nested (see config/pricing.yaml).
            cache_read = int(prompt_tokens * 0.6)
        input_tokens = max(1, prompt_tokens - cache_read)

        spans.append(
            Span(
                trace_id=trace_id,
                span_id=uuid.uuid4(),
                parent_span_id=root_id,
                operation_name="chat",
                model_name=cfg.model,
                provider_name=PROVIDER,
                node_name=node,
                pipeline_name=cfg.pipeline_name,
                start_time_ns=start,
                end_time_ns=end,
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                cache_read_tokens=cache_read,
                status="error" if errored else "ok",
                error_message=_ERROR_MESSAGES[node] if errored else None,
                # cost_usd is deliberately left unset: the worker owns costing so
                # there is exactly one implementation of it.
                attributes={"synthetic": True, "source": "loadgen", "node_index": i},
            )
        )
        if errored:
            break  # the pipeline stops at a hard failure, so the path truncates

    root = Span(
        trace_id=trace_id,
        span_id=root_id,
        parent_span_id=None,
        operation_name="invoke_agent",
        # No model on the wrapper span: it is not an LLM call, and naming a model
        # here would inflate cost and pollute the unknown-model counter.
        model_name=None,
        provider_name=PROVIDER,
        node_name=ROOT_NODE,
        pipeline_name=cfg.pipeline_name,
        start_time_ns=now_ns,
        end_time_ns=spans[-1].end_time_ns + 500_000,
        status="error" if fail_at >= 0 else "ok",
        trace_end=True,
        attributes={
            "synthetic": True,
            "source": "loadgen",
            "expected_path": ">".join(s.node_name for s in spans),
        },
    )
    spans.append(root)
    return spans


def serialize(spans: list[Span]) -> list[dict[str, Any]]:
    """Wire form: `gen_ai.*` aliases, UUIDs as strings."""
    return [s.model_dump(mode="json", by_alias=True) for s in spans]


class TraceSource:
    """A pre-serialized corpus of traces, re-stamped on the way out.

    Building each payload through pydantic costs ~30us/span. At the rates this
    generator is meant to reach that is generator-side CPU competing with the
    event loop that is supposed to be pacing sends — it would inflate the very
    latency numbers the run exists to produce.

    So the corpus is built once (through the real model, keeping the schema
    guarantee) and each send copies a template and patches the five fields that
    must be unique: the ids and the timestamps. Fresh ids matter for more than
    realism — span inserts are `ON CONFLICT (span_id) DO NOTHING`, so a reused id
    would be silently deduplicated and the run would measure nothing.
    """

    __slots__ = ("_corpus", "_base_ns", "_cursor", "traces_emitted", "spans_emitted")

    def __init__(self, cfg: SyntheticConfig, corpus_size: int = 512) -> None:
        rng = random.Random(cfg.seed)
        self._base_ns = time.time_ns()
        self._corpus = [
            serialize(generate_trace(rng, cfg, self._base_ns)) for _ in range(max(1, corpus_size))
        ]
        self._cursor = 0
        self.traces_emitted = 0
        self.spans_emitted = 0

    def __len__(self) -> int:
        return len(self._corpus)

    @property
    def mean_spans_per_trace(self) -> float:
        return sum(len(t) for t in self._corpus) / len(self._corpus)

    def next_trace(self, now_ns: int | None = None) -> list[dict[str, Any]]:
        template = self._corpus[self._cursor % len(self._corpus)]
        self._cursor += 1

        now_ns = time.time_ns() if now_ns is None else now_ns
        delta = now_ns - self._base_ns
        trace_id = str(uuid.uuid4())
        root_id = str(uuid.uuid4())

        out: list[dict[str, Any]] = []
        for span in template:
            # Shallow copy: `attributes` is shared across sends and never mutated.
            fresh = dict(span)
            is_root = span["parent_span_id"] is None
            fresh["trace_id"] = trace_id
            fresh["span_id"] = root_id if is_root else str(uuid.uuid4())
            fresh["parent_span_id"] = None if is_root else root_id
            fresh["start_time_ns"] = span["start_time_ns"] + delta
            fresh["end_time_ns"] = span["end_time_ns"] + delta
            out.append(fresh)

        self.traces_emitted += 1
        self.spans_emitted += len(out)
        return out
