"""Test harness agent — a real 4-node LangGraph pipeline (design doc §5.1).

`extract -> classify -> decide -> escalate` over a generic support-ticket triage
domain (§7.8). Each node wraps its model call in instrumentation that emits an
OTel-GenAI-aligned span into the ingestion endpoint, and the run emits a root
`invoke_agent` span last carrying `trace_end=true`.

Everything importable from here is stable for callers outside the package;
`run.main()` is the `aoe-harness` console script.
"""

from aoe.harness.graph import (
    BASELINE_PATHS,
    MAX_ESCALATE_ATTEMPTS,
    HarnessHardError,
    RunContext,
    RunOutcome,
    TriageState,
    build_graph,
    path_signature,
    run_once,
)
from aoe.harness.instrument import (
    ROOT_NODE_NAME,
    EmitterStats,
    NodeSpan,
    SpanEmitter,
    TraceRecorder,
)
from aoe.harness.providers import (
    AnthropicProvider,
    LLMProvider,
    LLMResult,
    ProviderError,
    ProviderRefusal,
    SimulatedProvider,
    build_provider,
)
from aoe.harness.tickets import ACTIONS, CATEGORIES, SEVERITIES, TICKETS, Ticket

__all__ = [
    "ACTIONS",
    "BASELINE_PATHS",
    "CATEGORIES",
    "MAX_ESCALATE_ATTEMPTS",
    "ROOT_NODE_NAME",
    "SEVERITIES",
    "TICKETS",
    "AnthropicProvider",
    "EmitterStats",
    "HarnessHardError",
    "LLMProvider",
    "LLMResult",
    "NodeSpan",
    "ProviderError",
    "ProviderRefusal",
    "RunContext",
    "RunOutcome",
    "SimulatedProvider",
    "SpanEmitter",
    "Ticket",
    "TraceRecorder",
    "TriageState",
    "build_graph",
    "build_provider",
    "path_signature",
    "run_once",
]
