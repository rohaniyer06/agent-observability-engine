"""The 4-node LangGraph triage pipeline (design doc §5.1).

    extract -> classify -> decide -> [escalate]

`escalate` is conditional, which is what makes path drift a real signal rather
than a formality: the happy path is three nodes, the escalation path is four,
and the injected failure modes produce two more shapes on top of that.

Domain framing is a generic SaaS support inbox (design doc §7.8). Node names are
fixed by the doc; everything else here is invented.
"""

from __future__ import annotations

import asyncio
import logging
import random
from dataclasses import dataclass, field
from typing import Any, TypedDict

from langgraph.graph import END, StateGraph

from aoe.harness.instrument import TraceRecorder
from aoe.harness.providers import LLMProvider
from aoe.harness.tickets import ACTIONS, CATEGORIES, SEVERITIES, Ticket, render_ticket

log = logging.getLogger("aoe.harness.graph")

NODE_EXTRACT = "extract"
NODE_CLASSIFY = "classify"
NODE_DECIDE = "decide"
NODE_ESCALATE = "escalate"

# One retry only. Bounded so the set of legitimate paths stays enumerable —
# an unbounded retry loop makes BASELINE_PATHS infinite and drift detection
# meaningless.
MAX_ESCALATE_ATTEMPTS = 2

# Budget for the classify model call. Real, not decorative: a genuinely slow
# call trips it the same way the injected failure does.
CLASSIFY_TIMEOUT_S = 2.0

# Per-node output caps. This is a latency/cost harness, not a prompt-engineering
# exercise — the prompts are short and the ceilings are low on purpose.
_MAX_TOKENS = {
    NODE_EXTRACT: 120,
    NODE_CLASSIFY: 32,
    NODE_DECIDE: 24,
    NODE_ESCALATE: 96,
}


# ---------------------------------------------------------------------------
# Baseline paths (design doc §7.6)
# ---------------------------------------------------------------------------
#
# Every path this harness can legitimately produce, INCLUDING the ones the
# deliberate failure injection creates. These are expected behaviour, not drift.
# Seed them (`aoe-harness --seed-baselines`) before a load run or the drift
# detector spends the demo flagging failure modes we engineered ourselves.
#
# The truncated paths come from `harness_hard_error_rate`: the run dies at the
# Nth node it executes, N in {1, 2, 3}, and the failing node's span is still
# emitted with status=error before the trace is closed out.
BASELINE_PATHS: list[list[str]] = [
    # Hard error at the 1st / 2nd node.
    [NODE_EXTRACT],
    [NODE_EXTRACT, NODE_CLASSIFY],
    # Happy path (decide chose anything other than escalate), and the hard-error
    # -at-3rd-node truncation, which lands on the same shape.
    [NODE_EXTRACT, NODE_CLASSIFY, NODE_DECIDE],
    # decide chose escalate.
    [NODE_EXTRACT, NODE_CLASSIFY, NODE_DECIDE, NODE_ESCALATE],
    # ...and the escalate retry (harness_escalate_retry_rate) repeating a node.
    [NODE_EXTRACT, NODE_CLASSIFY, NODE_DECIDE, NODE_ESCALATE, NODE_ESCALATE],
    # classify timed out (harness_classify_timeout_rate) -> straight to escalate.
    [NODE_EXTRACT, NODE_CLASSIFY, NODE_ESCALATE],
    [NODE_EXTRACT, NODE_CLASSIFY, NODE_ESCALATE, NODE_ESCALATE],
]


def path_signature(path: list[str]) -> str:
    """Collapse an ordered path to a single stable key.

    Used as the `pipeline_paths.path_signature` / `path_drift_events.path_signature`
    join key. The worker must use the same encoding.
    """
    return ">".join(path)


class HarnessHardError(RuntimeError):
    """Injected mid-run failure (`AOE_HARNESS_HARD_ERROR_RATE`).

    Kills the run partway so the pipeline has traces that end with `status=error`
    and a truncated path — the thing an observability tool exists to surface.
    """


# ---------------------------------------------------------------------------
# Runtime context + state
# ---------------------------------------------------------------------------


@dataclass
class RunContext:
    """Per-run dependencies, carried in the graph state.

    LangGraph state is an in-memory dict here (no checkpointer), so putting live
    objects in it is fine and saves recompiling the graph once per run.
    """

    recorder: TraceRecorder
    provider: LLMProvider
    settings: Any
    rng: random.Random
    run_index: int
    # Which node the injected hard error fires at, counted in execution order
    # (1 = the first node the run executes). None = this run does not hard-fail.
    hard_error_at_seq: int | None = None
    node_seq: int = 0
    parse_fallbacks: int = 0

    @classmethod
    def create(
        cls,
        *,
        recorder: TraceRecorder,
        provider: LLMProvider,
        settings: Any,
        run_index: int,
    ) -> RunContext:
        # Seeded off the run index rather than a shared stream, so run #7's
        # injected failures are the same on every execution of the harness.
        rng = random.Random(f"{settings.harness_seed}:{run_index}")
        hard_at: int | None = None
        if rng.random() < settings.harness_hard_error_rate:
            # Every run executes at least three nodes (extract, classify, then
            # decide or escalate), so 1..3 always fires — the observed hard-error
            # rate matches the configured one exactly.
            hard_at = rng.randint(1, 3)
        return cls(
            recorder=recorder,
            provider=provider,
            settings=settings,
            rng=rng,
            run_index=run_index,
            hard_error_at_seq=hard_at,
        )

    def enter_node(self, node_name: str) -> None:
        self.node_seq += 1
        if self.hard_error_at_seq == self.node_seq:
            raise HarnessHardError(f"injected hard failure at {node_name} (run {self.run_index})")


class TriageState(TypedDict, total=False):
    ctx: RunContext
    ticket: Ticket
    fields: dict[str, str]
    category: str
    severity: str
    action: str
    classify_failed: bool
    escalate_attempts: int
    escalate_retry_pending: bool
    escalation_note: str


# ---------------------------------------------------------------------------
# Prompts — short and task-shaped. Nothing here is tuned for quality.
# ---------------------------------------------------------------------------

EXTRACT_SYSTEM = (
    "Extract structured fields from a customer support ticket.\n"
    "Reply with exactly three lines, no prose:\n"
    "product_area: <one or two words>\n"
    "reported_issue: <one short sentence>\n"
    "urgency_signal: none | blocking | deadline"
)

CLASSIFY_SYSTEM = (
    "Classify a customer support ticket.\n"
    "Reply with exactly two lines, no prose:\n"
    f"category: one of {', '.join(CATEGORIES)}\n"
    f"severity: one of {', '.join(SEVERITIES)}"
)

DECIDE_SYSTEM = (
    "Choose the next action for a triaged support ticket.\n"
    "Reply with exactly one line, no prose:\n"
    f"action: one of {', '.join(ACTIONS)}"
)

ESCALATE_SYSTEM = (
    "Write a one-sentence escalation summary for an on-call support lead. "
    "State the impact and what was already tried. No preamble."
)


# ---------------------------------------------------------------------------
# Parsing — tolerant on purpose. A model that ignores the format should degrade
# the pipeline's decision quality, not crash the telemetry harness.
# ---------------------------------------------------------------------------


def _parse_kv(text: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for raw in text.splitlines():
        line = raw.strip().lstrip("-*• ").strip()
        if ":" not in line:
            continue
        key, _, value = line.partition(":")
        key = key.strip().strip("*`").lower().replace(" ", "_")
        value = value.strip().strip("*`.").lower()
        if key and value:
            out.setdefault(key, value)
    return out


def _pick(value: str | None, allowed: tuple[str, ...], fallback: str) -> tuple[str, bool]:
    """Return (choice, used_fallback)."""
    if not value:
        return fallback, True
    if value in allowed:
        return value, False
    for candidate in allowed:
        if candidate in value:
            return candidate, False
    return fallback, True


# ---------------------------------------------------------------------------
# Nodes
# ---------------------------------------------------------------------------


async def extract_node(state: TriageState) -> dict[str, Any]:
    ctx = state["ctx"]
    ticket = state["ticket"]
    async with ctx.recorder.node(NODE_EXTRACT) as span:
        span.attributes["ticket_id"] = ticket.ticket_id
        span.attributes["channel"] = ticket.channel
        span.attributes["plan"] = ticket.plan
        ctx.enter_node(NODE_EXTRACT)

        result = await ctx.provider.complete(
            system=EXTRACT_SYSTEM,
            user=render_ticket(ticket),
            max_tokens=_token_cap(ctx, NODE_EXTRACT),
            node=NODE_EXTRACT,
        )
        span.record(result)
        fields = _parse_kv(result.text)
        span.attributes["fields_extracted"] = len(fields)
    return {"fields": fields}


async def classify_node(state: TriageState) -> dict[str, Any]:
    ctx = state["ctx"]
    ticket = state["ticket"]
    fields = state.get("fields", {})

    async with ctx.recorder.node(NODE_CLASSIFY) as span:
        span.attributes["ticket_id"] = ticket.ticket_id
        ctx.enter_node(NODE_CLASSIFY)

        inject = ctx.rng.random() < ctx.settings.harness_classify_timeout_rate
        span.attributes["timeout_injected"] = inject

        call = (
            _hang(CLASSIFY_TIMEOUT_S * 2)
            if inject
            else ctx.provider.complete(
                system=CLASSIFY_SYSTEM,
                user=_classify_prompt(ticket, fields),
                max_tokens=_token_cap(ctx, NODE_CLASSIFY),
                node=NODE_CLASSIFY,
            )
        )
        try:
            result = await asyncio.wait_for(call, timeout=CLASSIFY_TIMEOUT_S)
        except TimeoutError:
            # Real timeout, real error span, and a genuinely different path:
            # an unclassified ticket goes straight to a human.
            span.fail(f"classify timed out after {CLASSIFY_TIMEOUT_S:.1f}s")
            span.attributes["route"] = NODE_ESCALATE
            return {"classify_failed": True, "category": "unknown", "severity": "high"}

        span.record(result)
        parsed = _parse_kv(result.text)
        category, cat_fallback = _pick(parsed.get("category"), CATEGORIES, "bug_report")
        severity, sev_fallback = _pick(parsed.get("severity"), SEVERITIES, "medium")
        if cat_fallback or sev_fallback:
            ctx.parse_fallbacks += 1
            span.attributes["parse_fallback"] = True
        span.attributes["category"] = category
        span.attributes["severity"] = severity

    return {"category": category, "severity": severity, "classify_failed": False}


async def decide_node(state: TriageState) -> dict[str, Any]:
    ctx = state["ctx"]
    ticket = state["ticket"]

    async with ctx.recorder.node(NODE_DECIDE) as span:
        span.attributes["ticket_id"] = ticket.ticket_id
        ctx.enter_node(NODE_DECIDE)

        result = await ctx.provider.complete(
            system=DECIDE_SYSTEM,
            user=_decide_prompt(ticket, state),
            max_tokens=_token_cap(ctx, NODE_DECIDE),
            node=NODE_DECIDE,
        )
        span.record(result)
        parsed = _parse_kv(result.text)
        action, fallback = _pick(parsed.get("action"), ACTIONS, "route_to_human")
        if fallback:
            ctx.parse_fallbacks += 1
            span.attributes["parse_fallback"] = True
        span.attributes["action"] = action

    return {"action": action}


async def escalate_node(state: TriageState) -> dict[str, Any]:
    ctx = state["ctx"]
    ticket = state["ticket"]
    attempt = state.get("escalate_attempts", 0) + 1

    async with ctx.recorder.node(NODE_ESCALATE) as span:
        span.attributes["ticket_id"] = ticket.ticket_id
        span.attributes["attempt"] = attempt
        ctx.enter_node(NODE_ESCALATE)

        result = await ctx.provider.complete(
            system=ESCALATE_SYSTEM,
            user=_escalate_prompt(ticket, state),
            max_tokens=_token_cap(ctx, NODE_ESCALATE),
            node=NODE_ESCALATE,
        )
        span.record(result)

        # Injected dispatch failure: the summary was written, handing it to the
        # on-call rota failed. Retrying repeats the node inside the same trace,
        # which is why `[... escalate, escalate]` is a baseline path and not drift.
        retry = (
            attempt < MAX_ESCALATE_ATTEMPTS
            and ctx.rng.random() < ctx.settings.harness_escalate_retry_rate
        )
        if retry:
            span.fail("escalation dispatch failed; retrying")
        span.attributes["retry_pending"] = retry

    return {
        "escalate_attempts": attempt,
        "escalate_retry_pending": retry,
        "escalation_note": result.text.strip(),
    }


async def _hang(seconds: float) -> Any:
    """Stand-in for a model call that never comes back inside the budget."""
    await asyncio.sleep(seconds)
    raise AssertionError("unreachable: wait_for should have cancelled this")


def _token_cap(ctx: RunContext, node: str) -> int:
    return min(_MAX_TOKENS.get(node, 64), ctx.settings.harness_max_tokens)


def _classify_prompt(ticket: Ticket, fields: dict[str, str]) -> str:
    extracted = "\n".join(f"{k}: {v}" for k, v in fields.items()) or "(none extracted)"
    return f"{render_ticket(ticket)}\n\nExtracted fields:\n{extracted}"


def _decide_prompt(ticket: Ticket, state: TriageState) -> str:
    return (
        f"{render_ticket(ticket)}\n\n"
        f"category: {state.get('category', 'unknown')}\n"
        f"severity: {state.get('severity', 'unknown')}\n"
        f"plan: {ticket.plan}"
    )


def _escalate_prompt(ticket: Ticket, state: TriageState) -> str:
    reason = "classification failed" if state.get("classify_failed") else state.get("action", "")
    return (
        f"{render_ticket(ticket)}\n\n"
        f"category: {state.get('category', 'unknown')}\n"
        f"severity: {state.get('severity', 'unknown')}\n"
        f"escalation reason: {reason}"
    )


# ---------------------------------------------------------------------------
# Routing
# ---------------------------------------------------------------------------


def route_after_classify(state: TriageState) -> str:
    return NODE_ESCALATE if state.get("classify_failed") else NODE_DECIDE


def route_after_decide(state: TriageState) -> str:
    return NODE_ESCALATE if state.get("action") == "escalate" else END


def route_after_escalate(state: TriageState) -> str:
    if (
        state.get("escalate_retry_pending")
        and state.get("escalate_attempts", 0) < MAX_ESCALATE_ATTEMPTS
    ):
        return NODE_ESCALATE
    return END


def build_graph():
    """Compile the pipeline once; every run reuses it."""
    graph = StateGraph(TriageState)
    graph.add_node(NODE_EXTRACT, extract_node)
    graph.add_node(NODE_CLASSIFY, classify_node)
    graph.add_node(NODE_DECIDE, decide_node)
    graph.add_node(NODE_ESCALATE, escalate_node)

    graph.set_entry_point(NODE_EXTRACT)
    graph.add_edge(NODE_EXTRACT, NODE_CLASSIFY)
    graph.add_conditional_edges(
        NODE_CLASSIFY,
        route_after_classify,
        {NODE_DECIDE: NODE_DECIDE, NODE_ESCALATE: NODE_ESCALATE},
    )
    graph.add_conditional_edges(
        NODE_DECIDE,
        route_after_decide,
        {NODE_ESCALATE: NODE_ESCALATE, END: END},
    )
    # Self-edge: the retry that makes a node legitimately repeat in one trace.
    graph.add_conditional_edges(
        NODE_ESCALATE,
        route_after_escalate,
        {NODE_ESCALATE: NODE_ESCALATE, END: END},
    )
    return graph.compile()


@dataclass
class RunOutcome:
    run_index: int
    trace_id: str
    path: list[str]
    status: str
    error: str | None = None
    ticket_id: str = ""
    action: str = ""
    spans: int = 0
    error_spans: int = 0
    input_tokens: int = 0
    output_tokens: int = 0
    cache_read_tokens: int = 0
    cache_write_tokens: int = 0
    model: str = ""
    attributes: dict[str, Any] = field(default_factory=dict)


async def run_once(
    compiled,
    *,
    ticket: Ticket,
    ctx: RunContext,
) -> RunOutcome:
    """Drive one trace end to end and close it out.

    The trace is finalized in a `finally` so a hard error still produces a root
    span with `trace_end=True` — a run that dies without one is exactly the case
    the worker's timeout reaper exists for, and we should not rely on it here.
    """
    recorder = ctx.recorder
    state: TriageState = {"ctx": ctx, "ticket": ticket, "escalate_attempts": 0}
    status = "ok"
    error: str | None = None
    final: dict[str, Any] = {}

    try:
        final = await compiled.ainvoke(state)
    except HarnessHardError as exc:
        status = "error"
        error = str(exc)
    except Exception as exc:  # noqa: BLE001 - one bad run must not stop the batch
        status = "error"
        error = f"{type(exc).__name__}: {exc}"
        log.warning("run %s failed: %s", ctx.run_index, error)
    else:
        if recorder.error_span_count:
            # Nodes recovered, but the trace is not clean. `partial` is a trace
            # status the worker owns; the harness reports `error` and lets the
            # worker decide.
            status = "error"

    recorder.finish(status=status, error_message=error)

    return RunOutcome(
        run_index=ctx.run_index,
        trace_id=str(recorder.trace_id),
        path=list(recorder.path),
        status=status,
        error=error,
        ticket_id=ticket.ticket_id,
        action=str(final.get("action", "")),
        spans=recorder.span_count + 1,  # + the root span
        error_spans=recorder.error_span_count,
        input_tokens=recorder.usage.input_tokens,
        output_tokens=recorder.usage.output_tokens,
        cache_read_tokens=recorder.usage.cache_read_tokens,
        cache_write_tokens=recorder.usage.cache_write_tokens,
        model=recorder.model_name,
    )
