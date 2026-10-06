# Instrumenting a real agent

`aoe_client.py` is a single file with no dependency on this repo. Copy it into
the application you want to observe; it only speaks HTTP to the ingestion
endpoint. The observed app should never have to import the observability system.

## The contract

Only six fields are required. Everything else has a default.

| Field | Notes |
|---|---|
| `trace_id`, `span_id` | UUIDs. `parent_span_id` optional. |
| `gen_ai.operation.name` | `chat`, `execute_tool`, or `invoke_agent` |
| `node_name`, `pipeline_name` | 1–128 chars |
| `start_time_ns`, `end_time_ns` | `time.time_ns()`; end must be ≥ start |

Optional but worth sending: `gen_ai.request.model` (no model means no cost
attribution), token counts, `status`, and `attributes`.

**Do not send `cost_usd`.** The worker prices every span from
`config/pricing.yaml` so there is one costing implementation, not two that drift.

## Minimal integration

```python
from aoe_client import TelemetryEmitter

emitter = TelemetryEmitter(pipeline_name="ticket_triage")

with emitter.trace(request_id=req.id) as trace:
    with trace.node("extract", model="claude-haiku-4-5") as span:
        resp = client.messages.create(...)
        span.record_anthropic_usage(resp)     # handles the cache-token split

emitter.shutdown()   # flushes; call once at process exit
```

`trace()` emits the root `invoke_agent` span with `trace_end=True` when the block
exits, which closes the trace immediately instead of waiting out the 5s idle
timeout. Exceptions mark the span `error` and re-raise — instrumentation never
swallows your errors.

## Publishing a pipeline whose node names are not yours to publish

`node_name` and `pipeline_name` reach the database and the dashboard. For a
proprietary pipeline those names *are* the internal taxonomy, and a dashboard
screenshot publishes it.

Map at the boundary so real names never leave the process:

```python
emitter = TelemetryEmitter(
    pipeline_name="ticket_triage",                    # generic label
    name_map={"svc_payload_normalizer": "extract"},   # real -> generic
    on_unmapped="reject",                             # drop anything unmapped
)
```

`on_unmapped="reject"` is the default and the safe one: a node you forgot to map
is dropped rather than published under its real name. `emitter.stats()` reports
`unmapped_nodes` so you can see what was skipped. Keep the map in the proprietary
repo — this repo should never contain it.

Also keep customer data out of `attributes`. It is stored verbatim as JSONB.

## Verifying it works

```bash
make stack                                   # ingest + worker + query API
python adapters/example_external_agent.py    # a worked example
make smoke                                   # independent end-to-end check
```

Then confirm the data landed, and that only mapped names did:

```sql
SELECT path_signature, count(*) FROM traces
 WHERE pipeline_name = 'ticket_triage' GROUP BY 1;

-- must return 0
SELECT count(*) FROM spans WHERE node_name LIKE 'svc_%';
```

If traces do not appear: `GET /v1/system/stats` shows the consumer-group backlog
and whether the worker is draining. `emitter.stats()` shows `dropped` (ingest
unreachable) and `shed_503` (server applying backpressure — expected under load,
not an error).

## Seed the drift baselines first

Path-drift detection flags any path it has not been told is normal. Before a real
run, insert the paths your pipeline legitimately produces into `pipeline_paths`
with `is_seeded = true`, or every ordinary branch is reported as drift on first
sight. `aoe-harness --seed-baselines` does this for the bundled harness; for an
external pipeline, insert the rows directly.

## LangGraph without touching node bodies

`aoe_client.py` also sketches a callback-based path. The token-usage extraction
there is LangChain-version sensitive (`AIMessage.usage_metadata` on LC ≥ 0.2,
`llm_output["token_usage"]` before that) — verify it against your version before
relying on the numbers. The context-manager path above is the tested one.
