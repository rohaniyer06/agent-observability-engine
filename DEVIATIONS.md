# Design Decisions & Trade-offs

Where the implementation departs from the obvious or first-draft approach, the
reason is recorded here rather than left for a reader to discover in the code.
Every item below was a deliberate call, made before building, not a fix bolted
on afterward.

Scope stayed fixed throughout: no guardrail/prompt-injection layer, no second
agent, no synthetic-only testing, and the harness stays a small generic
pipeline.

---

## 1. Percentile engine: Redis log-linear histogram, not per-worker t-digest

**The straightforward approach** is to compute rolling percentiles inside the
async worker using a t-digest or reservoir sampler, and keep a per-node sorted
set in Redis for cheap approximate reads.

**Built instead:** a log-linear (HDR-style) histogram stored as a Redis hash per
`(node_name, minute)`, incremented with `HINCRBY`.

**Why:** the worker is a *pool*. Two problems that approach hits:

1. A t-digest in worker A's memory cannot be merged with worker B's. Percentiles
   would be per-worker, not per-node.
2. `latency_rollups` has PK `(node_name, bucket_start)`. Two workers flushing the
   same minute contend for the same row, and last-write-wins silently discards
   half the data.

Bucket assignment is a pure function of the value, so every worker computes the
same index and `HINCRBY` merges them atomically. A single flusher then reads one
already-merged histogram and does one upsert.

The ZSET is dropped rather than kept alongside: it stores every observed duration
(unbounded memory, O(log n) insert, needs its own trimming policy) to answer a
question the histogram answers in O(1) with ~1.6% relative error and a footprint
bounded by value *range* rather than value *count*.

Implementation: `src/aoe/histogram.py`. 5 sub-bucket bits → 32 sub-buckets per
octave → error ≤ 1/64. A 1000-second observation lands at index ~860, so a full
histogram is under ~900 hash fields at any throughput.

## 2. Trace finalization: both signals, one code path

**The obvious approach** is to pick one completion signal — either an explicit
`trace_end` event from the agent, or a timeout-based finalizer — and most
observability write-ups only cover one.

**Built instead:** both, unified behind a single deadline queue.

**Why:** picking either alone leaves a real hole. Timeout-only adds a fixed
5-second lag to every trace and can close a slow trace early. `trace_end`-only
loses any trace where the harness crashed, was killed, or had its last span
dropped — precisely the traces an observability tool exists to show you.

**How:** a Redis ZSET `aoe:trace:deadlines` maps `trace_id → finalize-at epoch ms`.

- A normal span arriving sets the deadline to `now + trace_idle_timeout_ms`.
- A span with `trace_end: true` pulls the deadline in to `now + trace_end_grace_ms`
  (default 500ms) rather than finalizing on the spot — children can still be in
  flight behind it in a batch, and finalizing immediately would truncate the path.
- The finalizer pops `ZRANGEBYSCORE 0 now` on a timer.

One finalization path, and `traces.finalized_by` records which signal fired so
"how many traces did we have to reap?" is a query, not a guess.

## 3. At-least-once delivery requires idempotent writes

**Not an explicit requirement** — but a direct consequence of one that was:
crash recovery via `XAUTOCLAIM` redelivery (reclaiming spans a dead worker
never acknowledged) is what makes the durability claim true. And it *will*
redeliver spans that were already written when a worker died between the
INSERT and the XACK. Without idempotency, that same crash-recovery mechanism
produces duplicate spans, inflated token counts, and double-counted costs —
the fix for one problem silently causing another.

**Built:** every span insert is `ON CONFLICT (span_id) DO NOTHING`; trace
finalization is `ON CONFLICT (trace_id) DO UPDATE`; `cost_anomalies` has a UNIQUE
constraint on `trace_id`. The worker counts suppressed duplicates and exposes
them at `/v1/system/stats` so redelivery is observable rather than invisible.

## 4. Cost anomaly: modified z-score (median + MAD), not stddev z-score

**The straightforward approach:** compare each trace's cost against a rolling
mean and standard deviation, flag anything beyond a z-score threshold.

**Built instead:** modified z-score — `0.6745 * (x - median) / MAD` — with a
minimum sample count before the detector arms at all.

**Why:** two failure modes in the plain version, both of which show up as noise
in a demo.

1. Per-trace cost is right-skewed (a retry loop or a long ticket doubles it). The
   mean and stddev are both dragged by the same outliers you are trying to
   detect, so the detector desensitises itself exactly when it matters.
2. Cold start. With n < 30 the stddev is meaningless and nearly everything
   trips the threshold. `AOE_COST_MIN_SAMPLES` (default 30) gates this.

Median + MAD is the standard robust substitute and costs nothing extra — the
rolling window was already being kept.

## 5. Path drift: dedup + self-calibrating baseline

**The initial design:** one `path_drift_events` row per drifting trace, with
the baseline seeded up front from a fixed list of expected branches.

**Built:** kept the per-trace row for drill-down, and added:

- `pipeline_paths` — every distinct path with a running count, so the baseline is
  learned rather than only hardcoded. A path is baseline if it was explicitly
  seeded *or* it accounts for ≥ `AOE_DRIFT_BASELINE_FREQ_PCT` of traces once
  `AOE_DRIFT_MIN_TRACES` have been seen.
- `path_signature` + a Redis cooldown key, so a novel path that occurs 400 times
  produces one event with `occurrence_count: 400`, not 400 rows.

**Why:** a fixed allowlist only half-solves the problem it's meant to —
it still can't tell "genuinely novel" from "rare but normal" for anything not
on the list. Without the cooldown, the drift table is the noisiest panel on the
dashboard and the feature reads as broken.

## 6. Storage unit: microseconds, not milliseconds

**The initial schema** stored `duration_ms INT` — whole milliseconds.

**Built:** `duration_us BIGINT` in `spans`, `total_duration_us` in `traces`,
`p50_us/p95_us/p99_us` in `latency_rollups`. The API still returns float
milliseconds, so nothing downstream changes.

**Why:** this is a latency product. Rounding to whole milliseconds at the storage
layer throws away resolution that the p50 chart on fast nodes actually needs, and
costs nothing to keep.

## 7. `WS /v1/live` lives on the query API, not the ingestion service

**The initial plan** put the WebSocket endpoint in the ingestion layer, fed by
pub/sub messages from the worker.

**Built:** it sits on the query API service instead.

**Why:** the whole point of feeding it from the worker is to avoid coupling
ingestion latency to dashboard fan-out. Hosting the socket on the ingestion
process only completes half of that — N dashboard sockets would still share an
event loop with the endpoint the load generator is saturating. Moving it to the
read service finishes the decoupling, and has the side benefit that the
dashboard talks to exactly one origin for both REST and WS.

## 8. No FK from `spans.trace_id` to `traces.trace_id`

**The initial schema** included a foreign key:
`trace_id UUID REFERENCES traces(trace_id)`.

**Why dropped:** spans are written as they arrive, and the `traces` row does not
exist until finalization. A real FK would force either a placeholder-row insert
on first sight of a trace (an extra write on the hot path, and a row that lies
about `status` until it's fixed up) or an ordering constraint the pipeline cannot
honour. The worker is the only writer to both tables and enforces the
relationship. `cost_anomalies` and `path_drift_events` drop their FKs for the
same reason — they are written in the same transaction as the trace row.

## 9. Harness LLM provider is pluggable

**The requirement:** no synthetic-only testing — the harness has to be a real
agent making real decisions, not a scripted stand-in.

**Built:** a real 4-node LangGraph pipeline whose LLM call goes through a
provider interface with two implementations — `anthropic` (real API call) and
`simulated` (deterministic, seeded, realistic latency and token distributions).
`AOE_HARNESS_PROVIDER=auto` picks the real one when `ANTHROPIC_API_KEY` is set.

**Why:** the graph, the instrumentation, and the entire telemetry path are
identical either way, so this doesn't weaken the "must be real" requirement —
the real agent is what runs by default. It makes the repo runnable without a
key and keeps a 100k-span load test from costing real money, which is the
actual reason the simulated path exists.

---

# Bugs found and fixed during integration

These were not design departures — they were defects found by running the system
end to end. Recorded because each one is a trap the next person would hit.

## A. `XLEN` is not backlog

Backpressure, `/v1/system/stats`, and the load generator's drain check all used
`XLEN` as "how far behind are we". A Redis stream is an append-only log: `XACK`
does not remove an entry, so `XLEN` counts everything ever written and never
falls as workers catch up.

Consequences, all observed:

* **Ingestion shed 38% of a healthy load run.** With the threshold at 200k, the
  service began returning 503 once 200k spans had *ever* been ingested — the
  measured backlog at that moment was 1,779. Same offered load after the fix:
  100% accepted, 0 shed.
* The dashboard's headline health number was cumulative volume wearing a
  backlog's label.
* The load generator declared a drain FAILURE on a run where the worker had in
  fact processed every span (`spans_processed == spans_ingested`, 0 pending).

Fixed with `redis_client.consumer_group_backlog()`, which reads the consumer
group's `lag` (added but not yet delivered) plus `pending` (delivered but not yet
acked) from `XINFO GROUPS`.

## B. The root span was being counted as a pipeline step

`fold_state` built `path` from every span in the trace, including the root
`invoke_agent` envelope, producing `extract>invoke_agent>classify>decide`. That
corrupts the path signature, guarantees a permanent mismatch against the seeded
baselines, and draws a phantom node in the flow diagram. The envelope still
contributes cost, tokens and the outer duration — it just is not a step.

## C. Frequency alone let a first sighting become "baseline"

`evaluate_path` admitted any signature clearing `baseline_freq_pct` of traffic.
A share test is trivially satisfied while the denominator is small: at the
default 1%, one observation out of 87 traces clears the bar, so a brand-new path
was filed as normal. Drift detection was therefore silently dead below
`100/pct` traces — exactly the range a demo or fresh deployment lives in.

Fixed with an occurrence floor: frequency can only vouch for a path seen more
than once. Seeded paths bypass it. Verified with a negative control — a novel
path now fires exactly one event, with no false positives across 60 harness runs.

## D. Cost anomalies: storm, then the opposite

A sustained cost shift trips the z-score on every trace until the rolling window
re-centres — one incident, 25 rows. Adding a per-severity cooldown fixed that and
introduced something worse: a genuine 20x outlier was **silently swallowed**
because a lesser `critical` had claimed the window seconds earlier.

The window now stores the magnitude that opened it, and an anomaly at least 2x
worse takes it over instead of being suppressed. Dedup for repeats; never
suppression of a materially worse event.

## E. `random.Random()` seeded with a tuple

Two sites in the harness seeded from a tuple, which Python rejects. Every run
failed identically and the harness reported "40 runs completed, 40 with errors"
rather than crashing — a reminder that a failure path which still produces
plausible-looking output is the expensive kind.

## F. Query params whose default failed their own validator

Three endpoints declared `Query(default=0, ge=1)` with a body that reads `0` as
"unset". Omitting the parameter produced a 422 before the body ever ran.

## G. Stream lag was reported as 0.00ms

The lag sampler clamped negative values with `max(0, ...)`. `spans.created_at`
defaults to `now()`, which in Postgres is *transaction-start* time and is
therefore stamped before the row is actually written; combined with residual
host/container clock skew, genuine sub-millisecond lag came out negative and was
reported as a confident `0.00ms`.

Now the calibrated skew is subtracted, sub-floor samples are counted and excluded
rather than folded in as zeros, and a run where *everything* falls under the floor
records NULL instead of 0. A number this system cannot measure should not be
reported as a result.

## H. Known inert inconsistency: doubled-node baseline paths

`harness.graph.BASELINE_PATHS` seeds two paths containing `escalate` twice
(the retry loop). The finalizer deliberately de-duplicates repeated nodes when
building `path`, so those two signatures can never be observed and their
`pipeline_paths` rows sit at `occurrences = 0`.

Left as-is rather than removed: the dedup is the correct behaviour (counting a
retry as a different path shape would report every retry as drift), and the extra
seeds are harmless insurance if that policy is ever revisited. Noted here so the
zero-count rows are not mistaken for a bug.
