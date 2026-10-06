# API contract

Two HTTP services. They are separate processes on purpose: ingestion is the hot
path that the load generator hammers, and dashboard fan-out must not share an
event loop with it.

| Service | Default port | Owns |
|---|---|---|
| Ingestion (`aoe.ingest.app`) | 8000 | `POST /v1/spans`, health |
| Query API (`aoe.api.app`) | 8001 | All read endpoints + `WS /v1/live` |

Response bodies are defined in `src/aoe/apimodels.py` and mirrored in
`dashboard/src/types/api.ts`.

---

## Ingestion — port 8000

### `POST /v1/spans`

Accepts a single span object **or** `{"spans": [...]}`. Field names follow the
OTel GenAI conventions (see `src/aoe/schema.py`).

```json
{
  "spans": [{
    "trace_id": "3f1c...",
    "span_id": "a91e...",
    "parent_span_id": null,
    "gen_ai.operation.name": "chat",
    "gen_ai.request.model": "claude-haiku-4-5",
    "gen_ai.provider.name": "anthropic",
    "node_name": "classify",
    "pipeline_name": "support_ticket_triage",
    "start_time_ns": 1770000000000000000,
    "end_time_ns": 1770000000420000000,
    "gen_ai.usage.input_tokens": 412,
    "gen_ai.usage.output_tokens": 88,
    "status": "ok",
    "trace_end": false,
    "attributes": {"ticket_priority": "high"}
  }]
}
```

| Status | Meaning |
|---|---|
| `202` | Accepted → `{accepted, rejected, stream_depth, errors}` |
| `400` | Body is not a span or a span batch |
| `413` | Batch exceeds `AOE_MAX_BATCH_SPANS`, or body exceeds `AOE_MAX_BODY_BYTES` |
| `422` | Every span in the batch failed validation |
| `429` | Per-IP rate limit (only when `AOE_RATE_LIMIT_RPS > 0`) |
| `503` | Backpressure. Carries `Retry-After`. |

A partially-valid batch returns `202` with a non-zero `rejected` count and the
per-span errors listed — one malformed span must not cost you the other 499.

**Backpressure (design doc §5.2).** Once stream depth crosses
`AOE_BACKPRESSURE_STREAM_DEPTH` the endpoint returns `503` + `Retry-After`
rather than blocking. The depth reading is cached for `AOE_BACKPRESSURE_POLL_MS`
so the hot path does not pay an `XLEN` round trip per request.

### `GET /health`
`{status, redis, stream_depth, version}` — `503` when Redis is unreachable.

---

## Query API — port 8001

All list endpoints use **cursor** pagination (opaque `next_cursor`), never
`OFFSET`. Windows accept `5m`, `15m`, `1h`, `6h`, `24h`, `7d`.

| Endpoint | Query params | Returns |
|---|---|---|
| `GET /v1/traces` | `limit`, `cursor`, `pipeline`, `status`, `window`, `node` | `TracePage` |
| `GET /v1/traces/{trace_id}` | — | `TraceDetail` (trace + ordered spans) |
| `GET /v1/metrics/latency` | `node` (repeatable), `window`, `bucket_seconds` | `LatencyResponse` |
| `GET /v1/metrics/summary` | `window`, `pipeline` | `SummaryResponse` |
| `GET /v1/metrics/nodes` | — | `string[]` |
| `GET /v1/anomalies` | `limit`, `cursor`, `severity`, `window` | `AnomalyPage` |
| `GET /v1/drift` | `pipeline`, `limit` | `DriftResponse` |
| `GET /v1/drift/flow` | `pipeline`, `window` | `FlowResponse` |
| `GET /v1/system/stats` | — | `SystemStats` |
| `GET /v1/system/pricing` | — | `PricingResponse` |
| `GET /v1/loadtests` | `limit` | `LoadTestRun[]` |
| `GET /health` | — | `HealthResponse` |

**`/v1/metrics/latency` reads `latency_rollups` only.** It never touches the
`spans` table. Raw spans are read for exactly one thing: `GET /v1/traces/{id}`
drill-down. That is the design doc §4.2 blocker warning, enforced.

### `WS /v1/live`

Server-push of `LiveTraceEvent` JSON objects (see `apimodels.py`). The worker
publishes finalized traces onto a Redis pub/sub channel; this service subscribes
once and fans out to connected sockets.

The socket is intentionally read-only and lossy: a dropped live-feed frame is a
missed animation, not lost telemetry. Durability lives on the ingest leg.

Handshake: on connect the server sends one `{"type": "hello", "backlog": [...]}`
frame carrying the most recent traces so a freshly-opened dashboard is not blank
until the next event lands.
