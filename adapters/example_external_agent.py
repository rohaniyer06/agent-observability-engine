"""Runnable example: instrumenting an app that knows nothing about this repo.

Run the stack (`make stack`), then:  python adapters/example_external_agent.py

The pipeline here stands in for one whose internal node names are not yours to
publish. Note that NAME_MAP is the only place the real names appear — they never
reach the wire, the database, or the dashboard.
"""

from __future__ import annotations

import random
import time

from aoe_client import TelemetryEmitter

# Real internal step names -> the generic labels that get published.
# In a real deployment this is the only proprietary-aware line in the file,
# and it lives in your production repo, not this one.
NAME_MAP = {
    "svc_payload_normalizer": "extract",
    "svc_intent_ranker": "classify",
    "svc_policy_router": "decide",
    "svc_tier2_handoff": "escalate",
}

INTERNAL_STEPS = ["svc_payload_normalizer", "svc_intent_ranker", "svc_policy_router"]


def fake_model_call(rng: random.Random, step: str) -> tuple[int, int]:
    """Stands in for whatever your node actually does."""
    time.sleep(rng.uniform(0.05, 0.35))
    return rng.randint(200, 900), rng.randint(30, 180)


def main() -> None:
    emitter = TelemetryEmitter(
        pipeline_name="ticket_triage",     # generic; not the real pipeline name
        ingest_url="http://localhost:8000",
        name_map=NAME_MAP,
        on_unmapped="reject",              # an unmapped node is dropped, not leaked
    )
    rng = random.Random(20260902)

    for run in range(12):
        with emitter.trace(source="example", run_index=run) as trace:
            for step in INTERNAL_STEPS:
                with trace.node(step, model="claude-haiku-4-5", provider="anthropic") as span:
                    tin, tout = fake_model_call(rng, step)
                    span.input_tokens, span.output_tokens = tin, tout

            # A branch, so the flow diagram has more than one path to show.
            if rng.random() < 0.3:
                with trace.node("svc_tier2_handoff", model="claude-haiku-4-5") as span:
                    tin, tout = fake_model_call(rng, "svc_tier2_handoff")
                    span.input_tokens, span.output_tokens = tin, tout

            # An unmapped node: proves the reject policy keeps it off the wire.
            if run == 5:
                with trace.node("svc_undocumented_internal_step"):
                    time.sleep(0.01)

    emitter.shutdown()
    print("emitter stats:", emitter.stats())


if __name__ == "__main__":
    main()
