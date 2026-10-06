# Agent Observability & Telemetry Engine

A self-hosted observability platform for LangGraph/agentic systems. It ingests
streaming execution telemetry from a running agent, buffers it durably, persists
raw spans plus pre-aggregated metrics, and serves a real-time analytical
dashboard surfacing P99 latency, cost anomalies, and agent path drift.

This is not a wrapper around a tracing SDK. The ingestion path, the durable
buffer, the aggregation layer, and the query layer are all built here.

---

## Architecture

```
  4-node LangGraph          FastAPI ingest            Redis Streams
  harness agent    ──POST──▶  /v1/spans      ──XADD──▶  aoe:stream:spans
  (extract→classify                │                    consumer group:
   →decide→escalate)               │                    telemetry_workers
                          backpressure: 503 +                  │
  synthetic load ──POST──▶  Retry-After once                   │ XREADGROUP
  generator                 depth > threshold          ┌───────┴────────┐
                                                       │  worker pool   │
                                                       │  • insert spans│
                                                       │  • finalize    │
                                                       │  • rollups     │
                                                       │  • anomalies   │
                                                       │  • drift       │
                                                       └───┬────────┬───┘
                                           ┌───────────────┘        └──────────┐
                                           ▼                                   ▼
                                    PostgreSQL                           Redis
                                    • spans                              • trace state
                                    • traces                             • deadline ZSET
                                    • latency_rollups                    • latency
                                    • cost_anomalies                       histograms
                                    • path_drift_events                  • pub/sub live
                                    • pipeline_paths                       feed
                                           │                                   │
                                           └──────────┬────────────────────────┘
                                                      ▼
                                             FastAPI query API
                                             REST + WS /v1/live
                                                      │
                                                      ▼
                                            React dashboard (Vite)
```

Three decisions worth naming up front, because they're the ones that get asked
about:

**Redis Streams, not Pub/Sub.** Pub/Sub is fire-and-forget — no listener means
the message is gone, which defeats the purpose of a durability buffer under
bursty load. Streams give an append-only log (`XADD`), consumer-group parallel
draining (`XREADGROUP`), and at-least-once delivery with a pending-entries list
we reclaim on a timer (`XAUTOCLAIM`). Pub/Sub *is* used for one thing — pushing
finalized traces to the dashboard — because losing a live-feed frame is a
cosmetic failure, not a data-loss one.

**Percentiles come from merged histograms, never from SQL over raw spans.**
`percentile_cont` across hundreds of thousands of span rows looks fine in
development and falls over during the load test you're running to produce your
best numbers. Each worker `HINCRBY`s into a shared log-linear histogram keyed by
(node, minute); a single flusher computes p50/p95/p99 and upserts
`latency_rollups`. The dashboard reads that table and nothing else.

**Trace completion uses two signals, not one.** An explicit `trace_end` from the
harness, plus a timeout reaper for traces whose agent died mid-run. Both feed one
deadline queue, so there's a single finalization code path. `traces.finalized_by`
records which fired.

Full rationale for every departure from the design doc: [DEVIATIONS.md](DEVIATIONS.md).

---

## Quickstart

Requires Docker, Python 3.11+, and Node 20+.

```bash
make up          # Postgres + Redis
make migrate     # apply SQL migrations
make stack       # ingest (:8000) + query API (:8001) + worker, all in one terminal
```

In a second terminal:

```bash
make smoke       # end-to-end: POST a trace → stream → worker → Postgres. PASS/FAIL.
make harness     # run the real 4-node agent, 20 traces
make dashboard   # http://localhost:5173
```

`make smoke` is the check that matters. A dashboard on top of a broken ingestion
path is the standard failure mode for this kind of project — run the smoke test
before you trust anything the UI shows you.

### Running the harness against a real LLM

The harness graph is real LangGraph either way; only the LLM call swaps out.

```bash
export ANTHROPIC_API_KEY=sk-ant-...
make harness                       # auto-detects the key, uses claude-haiku-4-5
```

Without a key it runs a deterministic simulated provider (no network, no cost)
with realistic per-node latency and token distributions. Force either with
`AOE_HARNESS_PROVIDER=anthropic|simulated`. The startup log always states which
provider was selected — a demo where you can't tell whether real calls happened
isn't worth much.

### Load test

```bash
make loadtest RPS=2000 DURATION=60 LABEL=baseline
```

Results are written to `load_test_runs` and printed as a paste-ready block. Seed
the drift baselines first (`aoe-harness --seed-baselines`), or the detector will
flag the failure modes the harness was deliberately built to produce — see design
doc §7.6.

---

## Layout

```
config/pricing.yaml      static per-token pricing table (§7.7)
migrations/              forward-only numbered SQL + checksum guard
src/aoe/
  config.py              all settings, AOE_-prefixed env overrides
  schema.py              wire model — OTel GenAI gen_ai.* field names
  apimodels.py           query API response contract
  pricing.py             cost attribution (cache-token aware)
  histogram.py           log-linear histogram (percentile engine)
  redis_keys.py          every Redis key in one place
  ingest/                FastAPI ingestion + backpressure + rate limit
  worker/                consumer, writer, finalizer, rollup, anomaly, drift
  api/                   query endpoints + live WebSocket
  harness/               4-node LangGraph agent + instrumentation
loadgen/                 asyncio load generator
dashboard/               Vite + React + TS + Tailwind + Recharts
tests/                   unit + integration + smoke
```

---

## Operational notes

**Redis memory.** `XADD` uses approximate `MAXLEN` trimming
(`AOE_STREAM_MAXLEN`, default 1M) and Redis runs `maxmemory-policy noeviction`.
That combination is deliberate: if the buffer genuinely fills, `XADD` should fail
loudly and ingestion should shed with 503, rather than Redis silently evicting
un-acked telemetry.

**Worker crash recovery.** Unacked entries sit in the consumer group's pending
entries list. A periodic `XAUTOCLAIM` reclaims anything idle longer than
`AOE_WORKER_RECLAIM_IDLE_MS`. Because redelivery is guaranteed, every write is
idempotent — `ON CONFLICT (span_id) DO NOTHING` on spans, upsert on traces,
unique constraint on `cost_anomalies.trace_id`. Suppressed duplicates are counted
and exposed at `/v1/system/stats`.

**Cost figures.** Rates come from `config/pricing.yaml`, a manually-refreshed
snapshot stamped with an `as_of` date, surfaced in the product at
`/v1/system/pricing`. Models with no entry are counted and flagged rather than
guessed at. Cached tokens are priced separately (reads at 0.1×, writes at 1.25×)
because `input_tokens` on an Anthropic response is the uncached remainder, not
the total — folding them together silently under-reports.

**Scaling the worker.** `aoe-worker --concurrency N` runs N consumer tasks in one
process; run multiple processes for more. Percentile rollups merge correctly
across all of them by construction.

---

## Numbers

Fill in from your own load run (`make loadtest`, then `GET /v1/loadtests`):

Measured on one laptop (Docker Postgres + Redis, 4 worker consumer tasks,
1 ingest process). Reproduce with `make loadtest RPS=8000 DURATION=40`.

| Metric | 3k run | 8k run |
|---|---|---|
| Offered / achieved | 3,000 / 3,002 spans/s | 8,000 / 8,002 spans/s |
| Accepted | 100% (0 shed) | 100% (0 shed) |
| Ingestion latency p50 / p95 / p99 | 7.1 / 10.1 / 12.9 ms | 14.5 / 20.2 / 54.8 ms |
| Buffer→storage lag p50 / p99 | under measurement floor | 1.9 / 80.9 ms |
| Peak consumer-group backlog | 101 entries | 2,114 entries |
| Worker kept up | yes | yes |

Two caveats worth stating rather than burying:

* The load generator reported `starved 1 times` on both runs, so the client may
  be part of the ceiling. These are lower bounds on what the service can take.
* A large share of lag samples fall under what the measurement can resolve
  (`created_at` defaults to `now()`, which is transaction-start time). Those are
  excluded from the percentiles and counted in the summary rather than being
  folded in as zeros — see DEVIATIONS.md G.

---

## Testing

```bash
make test        # unit only, no infra needed
make test-all    # includes integration (requires `make up`)
make lint
```
