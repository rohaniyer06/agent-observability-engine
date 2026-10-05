"""Open-loop synthetic load driver for `POST /v1/spans` (design doc §5.6, §8).

The numbers this produces are the headline numbers, which is exactly why the
methodology is spelled out rather than assumed:

**Open-loop pacing.** Sends are scheduled against an absolute wall-clock plan
computed from `--rps`, not against "previous response returned". A closed-loop
generator quietly lowers its offered load whenever the server slows down, so the
server is always measured at a rate it can already handle and saturation never
shows up. Here the schedule is fixed: if the server is slow, requests bunch up
against the concurrency ceiling and that queueing lands in the latency numbers.

**Latency is measured from the scheduled time, not the actual send time.** This
is the coordinated-omission correction. If a request was supposed to go out at
t=10.000s and the client was blocked until t=10.400s, the 400ms of client-side
queueing is part of what a user would have experienced and is counted. The
uncorrected service time is printed alongside so the gap between them is visible
rather than a choice you have to trust.

**A 503 is a pass, not a failure.** Backpressure is the endpoint doing what
§5.2 specifies. Shed spans are counted separately from errors.

**Stream lag is measured, not assumed.** An HTTP-only load test reports a
beautiful p99 while the buffer behind the endpoint grows without bound. See
`LagProbe` for the method and its limitations, and `DepthMonitor` for the
not-draining check that refuses to let this run print a clean headline number
while the worker is falling behind.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import httpx

from aoe import redis_keys
from aoe.config import get_settings
from aoe.histogram import LocalHistogram
from aoe.redis_client import consumer_group_backlog
from loadgen.synthetic import SyntheticConfig, TraceSource

# ---------------------------------------------------------------------------
# Counters
# ---------------------------------------------------------------------------


@dataclass
class Counters:
    requests: int = 0
    spans_sent: int = 0
    spans_accepted: int = 0
    # Rejected inside an otherwise-accepted batch. Should always be 0 — the
    # payloads are built through the real Span model — so a non-zero value here
    # means the server's schema and this repo's schema have diverged.
    spans_rejected: int = 0
    spans_shed_503: int = 0
    errors: int = 0
    status_counts: dict[str, int] = field(default_factory=dict)
    # Times the scheduler had to wait on the payload producer. Non-zero means
    # the generator, not the server, was the bottleneck.
    producer_starvations: int = 0

    def note_status(self, key: str, n: int = 1) -> None:
        self.status_counts[key] = self.status_counts.get(key, 0) + n


# ---------------------------------------------------------------------------
# Stream depth / drain detection
# ---------------------------------------------------------------------------


class DepthMonitor:
    """Samples the consumer group's backlog on a timer.

    Two jobs. `max_stream_depth` for the record, and the drain check: a run where
    depth climbs through the send phase and does not fall afterwards means the
    worker is not keeping up, and every latency number in the summary is a
    measurement of Redis rather than of the pipeline. That case gets a loud
    warning instead of a headline.
    """

    def __init__(self, redis: Any, stream: str, group: str, interval_s: float) -> None:
        self._redis = redis
        self._stream = stream
        self._group = group
        self._interval = interval_s
        self.samples: list[tuple[float, int]] = []
        self.max_depth = 0
        self.failed = False

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            try:
                depth = await consumer_group_backlog(self._redis, self._stream, self._group)
                self.samples.append((time.perf_counter(), depth))
                self.max_depth = max(self.max_depth, depth)
            except Exception:  # noqa: BLE001 - a dead Redis must not kill the run
                self.failed = True
            try:
                await asyncio.wait_for(stop.wait(), timeout=self._interval)
            except TimeoutError:
                pass

    def analyse(self, send_end_perf: float) -> tuple[bool, list[str]]:
        """Returns (draining_ok, warnings)."""
        warnings: list[str] = []
        if self.failed and not self.samples:
            return True, ["stream depth could not be sampled (Redis unreachable from the generator)"]
        if len(self.samples) < 3:
            return True, ["stream depth sampled too few times to judge drain behaviour"]

        during = [d for t, d in self.samples if t <= send_end_perf]
        after = [(t, d) for t, d in self.samples if t > send_end_perf]

        ok = True
        if len(during) >= 8:
            q = max(1, len(during) // 4)
            head = sum(during[:q]) / q
            tail = sum(during[-q:]) / q
            if tail > max(head * 1.5, head + 1_000):
                ok = False
                warnings.append(
                    f"stream depth rose monotonically during the run "
                    f"({head:,.0f} -> {tail:,.0f} entries): the worker is not keeping up with ingest"
                )

        if after:
            at_end = during[-1] if during else after[0][1]
            final = after[-1][1]
            if final > 500 and final > at_end * 0.9:
                ok = False
                warnings.append(
                    f"stream did not drain after sending stopped "
                    f"({at_end:,} -> {final:,} entries over "
                    f"{after[-1][0] - after[0][0]:.1f}s): is `aoe-worker` running?"
                )
        return ok, warnings


async def consumer_group_present(redis: Any, stream: str, group: str) -> bool:
    try:
        groups = await redis.xinfo_groups(stream)
    except Exception:  # noqa: BLE001 - no stream yet is the same answer as no group
        return False
    return any(str(g.get("name")) == group for g in groups)


# ---------------------------------------------------------------------------
# Stream lag
# ---------------------------------------------------------------------------


class LagProbe:
    """End-to-end lag: XADD -> the span visible as a row in Postgres.

    **Method.** A deterministic sample of sent spans is tracked by `span_id`. A
    background task polls `spans` for those ids and reads back `created_at`,
    which the worker's INSERT stamps. Lag is `created_at - t_response`.

    **Why `created_at` rather than "when my poll saw it".** Polling every 250ms
    and calling the poll time the arrival time would quantise every measurement
    upward by up to a full poll interval, which at real lags of tens of
    milliseconds is mostly measurement error. Reading the timestamp the database
    recorded removes the poll interval from the number entirely — the poller can
    then be as lazy as it likes.

    **Why not the XINFO alternative.** `last-generated-id` minus
    `last-delivered-id` is cheaper and needs no database, but it measures
    *delivery* to the consumer group, not persistence. A worker that reads the
    stream quickly and then stalls writing to Postgres looks perfectly healthy by
    that metric — which is precisely the failure this number is supposed to
    catch. It is also a gauge that collapses to zero in any idle gap.

    **Limitations, stated rather than buried:**

    * `t_response` is used as the XADD instant. The true XADD happened somewhere
      between the request being written and the 202 coming back, so this
      *understates* lag by at most that request's own ingestion latency — which
      is reported a few lines above it in the summary.
    * `created_at` defaults to `now()`, which in Postgres is transaction-start
      time. For the worker's batched insert that is marginally earlier than the
      row actually landing, so this understates by at most one batch's
      transaction duration.
    * Both endpoints are read from different clocks. Host and container clocks
      are checked at startup and the whole measurement is dropped (reported as
      NULL, never guessed) if they disagree by more than `_MAX_SKEW_MS`.
    * Spans that are still unresolved when the run ends are counted and
      reported. A large unresolved fraction means the numbers describe only the
      spans that made it, and the summary says so.
    """

    _MAX_SKEW_MS = 250.0

    def __init__(self, conn: Any, poll_interval_s: float, max_outstanding: int = 20_000) -> None:
        self._conn = conn
        self._interval = poll_interval_s
        self._max_outstanding = max_outstanding
        self.outstanding: dict[str, float] = {}
        self.hist = LocalHistogram()
        self.sampled = 0
        self.resolved = 0
        # Samples whose corrected lag came out <= 0. Counted rather than quietly
        # folded into the p50 as a zero: `spans.created_at` defaults to now(),
        # which in Postgres is TRANSACTION-START time and therefore stamped
        # slightly before the row is actually written. Combined with residual
        # host/container clock skew that puts genuine sub-millisecond lag below
        # what this method can resolve. Reporting those as "0.00ms" would be
        # inventing precision the measurement does not have.
        self.below_resolution = 0
        self.skew_ms: float | None = None
        self.usable = False

    async def calibrate(self) -> str | None:
        """Compare host and Postgres clocks. Returns a warning string, or None."""
        t0 = time.time()
        pg_ms = await self._conn.fetchval("SELECT EXTRACT(EPOCH FROM clock_timestamp()) * 1000.0")
        t1 = time.time()
        self.skew_ms = float(pg_ms) - (t0 + t1) / 2.0 * 1000.0
        if abs(self.skew_ms) > self._MAX_SKEW_MS:
            self.usable = False
            return (
                f"host/Postgres clock skew is {self.skew_ms:+.0f}ms — stream lag cannot be "
                "measured this way and will be recorded as NULL rather than guessed"
            )
        self.usable = True
        return None

    def track(self, span_id: str, at_epoch_ms: float) -> None:
        if not self.usable or len(self.outstanding) >= self._max_outstanding:
            return
        self.outstanding[span_id] = at_epoch_ms
        self.sampled += 1

    async def _poll_once(self, chunk: int = 500) -> None:
        if not self.outstanding:
            return
        # dicts keep insertion order, so this drains oldest-first.
        ids = list(self.outstanding)[:chunk]
        rows = await self._conn.fetch(
            "SELECT span_id, EXTRACT(EPOCH FROM created_at) * 1000.0 AS created_ms "
            "FROM spans WHERE span_id = ANY($1::uuid[])",
            [uuid.UUID(i) for i in ids],
        )
        for row in rows:
            sent_at = self.outstanding.pop(str(row["span_id"]), None)
            if sent_at is None:
                continue
            # Remove the systematic host/Postgres clock offset measured at
            # startup, so what is left is transit time rather than clock drift.
            lag_ms = (float(row["created_ms"]) - (self.skew_ms or 0.0)) - sent_at
            if lag_ms <= 0:
                self.below_resolution += 1
            else:
                self.hist.record(int(lag_ms * 1_000))
            self.resolved += 1

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            try:
                await self._poll_once()
            except Exception:  # noqa: BLE001 - a lag probe must never fail the run
                pass
            try:
                await asyncio.wait_for(stop.wait(), timeout=self._interval)
            except TimeoutError:
                pass
        with_suppressed = getattr(self, "_final", None)  # placeholder for clarity
        del with_suppressed
        try:
            await self._poll_once()  # one last sweep for stragglers
        except Exception:  # noqa: BLE001
            pass


# ---------------------------------------------------------------------------
# Send path
# ---------------------------------------------------------------------------


async def _produce(
    queue: asyncio.Queue,
    source: TraceSource,
    total_spans: int,
    batch_size: int,
) -> None:
    """Build batches ahead of the scheduler.

    Payload construction runs on the same event loop that paces sends, so doing
    it inline would let generator CPU distort the schedule. Traces are kept whole
    inside a batch: splitting one across two requests can deliver the
    `trace_end` root before its own children and truncate the recorded path.
    """
    produced = 0
    while produced < total_spans:
        batch: list[dict[str, Any]] = []
        while len(batch) < batch_size:
            trace = source.next_trace()
            batch.extend(trace)
            if len(batch) + len(trace) > batch_size:
                break
        produced += len(batch)
        await queue.put(batch)
    await queue.put(None)


async def _dispatch(
    client: httpx.AsyncClient,
    url: str,
    batch: list[dict[str, Any]],
    deadline_perf: float,
    counters: Counters,
    corrected: LocalHistogram,
    service: LocalHistogram,
    lag: LagProbe | None,
    sample_this: bool,
    sem: asyncio.Semaphore,
) -> None:
    try:
        n = len(batch)
        counters.requests += 1
        counters.spans_sent += n
        sent_perf = time.perf_counter()
        try:
            resp = await client.post(url, json={"spans": batch})
        except Exception as exc:  # noqa: BLE001 - transport failures are results, not crashes
            counters.errors += n
            counters.note_status(f"exc:{type(exc).__name__}")
            return
        done_perf = time.perf_counter()

        # Corrected: from when the request was *due*. Service: from when it
        # actually went out. The difference is client-side queueing.
        corrected.record(max(0, int((done_perf - deadline_perf) * 1_000_000)))
        service.record(max(0, int((done_perf - sent_perf) * 1_000_000)))

        code = resp.status_code
        counters.note_status(str(code))
        if code == 202:
            try:
                body = resp.json()
                counters.spans_accepted += int(body.get("accepted", 0))
                counters.spans_rejected += int(body.get("rejected", 0))
            except Exception:  # noqa: BLE001
                counters.spans_accepted += n
            if lag is not None and sample_this:
                lag.track(batch[0]["span_id"], time.time() * 1000.0)
        elif code == 503:
            # Backpressure working as designed (§5.2) — a measurement, not a failure.
            counters.spans_shed_503 += n
        else:
            counters.errors += n
    finally:
        sem.release()


@dataclass
class RunResult:
    counters: Counters
    corrected: LocalHistogram
    service: LocalHistogram
    started_at: datetime
    ended_at: datetime
    send_seconds: float
    max_stream_depth: int | None
    depth_warnings: list[str]
    draining_ok: bool
    lag_p50_ms: float | None
    lag_p99_ms: float | None
    lag_sampled: int
    lag_resolved: int
    lag_note: str | None


async def _run_load(args: argparse.Namespace) -> RunResult:
    settings = get_settings()
    url = args.target.rstrip("/") + "/v1/spans"

    cfg = SyntheticConfig(pipeline_name=args.pipeline, seed=args.seed)
    source = TraceSource(cfg, corpus_size=args.corpus_size)
    total_spans = int(args.rps * args.duration)

    counters = Counters()
    corrected = LocalHistogram()
    service = LocalHistogram()

    # --- infra side-channels ------------------------------------------------
    redis = None
    depth: DepthMonitor | None = None
    group_missing = False
    try:
        from aoe.redis_client import make_client

        redis = make_client(settings.redis_url)
        await redis.ping()
        depth = DepthMonitor(
            redis,
            redis_keys.STREAM_SPANS,
            settings.worker_consumer_group,
            args.depth_poll_ms / 1000.0,
        )
        group_missing = not await consumer_group_present(
            redis, redis_keys.STREAM_SPANS, settings.worker_consumer_group
        )
    except Exception as exc:  # noqa: BLE001
        print(f"warn: Redis unreachable ({exc}) — stream depth will not be sampled", file=sys.stderr)
        redis = None

    pg_conn = None
    lag: LagProbe | None = None
    lag_note: str | None = None
    try:
        import asyncpg

        pg_conn = await asyncpg.connect(dsn=settings.postgres_dsn)
        lag = LagProbe(pg_conn, args.lag_poll_ms / 1000.0)
        lag_note = await lag.calibrate()
        if not lag.usable:
            lag = None
    except Exception as exc:  # noqa: BLE001
        lag_note = f"Postgres unreachable ({exc}) — stream lag not measured"
        print(f"warn: {lag_note}", file=sys.stderr)

    if group_missing:
        print(
            "\n*** no consumer group on the ingest stream — the worker has never run. ***\n"
            "*** Everything below measures the HTTP endpoint and Redis only.          ***\n",
            file=sys.stderr,
        )

    # --- run ----------------------------------------------------------------
    stop = asyncio.Event()
    background: list[asyncio.Task] = []
    if depth is not None:
        background.append(asyncio.create_task(depth.run(stop)))
    if lag is not None:
        background.append(asyncio.create_task(lag.run(stop)))

    queue: asyncio.Queue = asyncio.Queue(maxsize=max(8, args.concurrency * 4))
    sem = asyncio.Semaphore(args.concurrency)
    inflight: set[asyncio.Task] = set()

    limits = httpx.Limits(
        max_connections=args.concurrency,
        max_keepalive_connections=args.concurrency,
    )
    started_at = datetime.now(UTC)
    sample_every = max(1, total_spans // max(1, args.lag_sample) // max(1, args.batch_size))

    async with httpx.AsyncClient(limits=limits, timeout=httpx.Timeout(args.timeout)) as client:
        producer = asyncio.create_task(_produce(queue, source, total_spans, args.batch_size))
        t0 = time.perf_counter()
        spans_scheduled = 0
        batch_index = 0

        while True:
            try:
                batch = queue.get_nowait()
            except asyncio.QueueEmpty:
                counters.producer_starvations += 1
                batch = await queue.get()
            if batch is None:
                break

            # Absolute schedule. Falling behind never reduces offered load — the
            # next deadline is already in the past and fires immediately.
            deadline = t0 + spans_scheduled / args.rps
            slack = deadline - time.perf_counter()
            if slack > 0.0005:
                await asyncio.sleep(slack)
            else:
                # Below the event loop's sleep resolution; yield instead so a
                # tight schedule does not turn into a busy-wait.
                await asyncio.sleep(0)

            await sem.acquire()
            task = asyncio.create_task(
                _dispatch(
                    client,
                    url,
                    batch,
                    deadline,
                    counters,
                    corrected,
                    service,
                    lag,
                    batch_index % sample_every == 0,
                    sem,
                )
            )
            inflight.add(task)
            task.add_done_callback(inflight.discard)

            spans_scheduled += len(batch)
            batch_index += 1

        await producer
        if inflight:
            await asyncio.wait(inflight, timeout=args.timeout + 5)
        send_end_perf = time.perf_counter()
        send_seconds = send_end_perf - t0

    ended_at = datetime.now(UTC)

    # Keep watching after the send phase: this is where "the endpoint was fast
    # but nothing drained" becomes visible, and it gives sampled spans time to
    # land so the lag figure is not just the ones that were already quick.
    if args.drain_wait > 0 and (depth is not None or lag is not None):
        print(f"observing drain for {args.drain_wait:.0f}s...", file=sys.stderr)
        await asyncio.sleep(args.drain_wait)

    stop.set()
    for task in background:
        try:
            await asyncio.wait_for(task, timeout=10)
        except Exception:  # noqa: BLE001
            task.cancel()

    draining_ok, depth_warnings = (True, [])
    max_depth: int | None = None
    if depth is not None:
        draining_ok, depth_warnings = depth.analyse(send_end_perf)
        max_depth = depth.max_depth if depth.samples else None

    lag_p50 = lag_p99 = None
    lag_sampled = lag_resolved = 0
    if lag is not None:
        lag_sampled, lag_resolved = lag.sampled, lag.resolved
        measurable = lag.hist.total
        if measurable == 0 and lag_resolved > 0:
            # Every sampled span landed at or before its own send timestamp once
            # clock skew was removed. That is not "zero lag" — it is lag below
            # what a now()-stamped row and two clocks can resolve. Report NULL.
            lag_note = (
                f"all {lag_resolved:,} resolved samples came in under the "
                "measurement floor (now() is transaction-start, plus residual "
                "clock skew) — stream lag recorded as NULL rather than 0"
            )
        elif measurable > 0:
            q = lag.hist.quantiles([0.5, 0.99])
            lag_p50, lag_p99 = q[0.5] / 1000.0, q[0.99] / 1000.0
            if lag.below_resolution:
                lag_note = (
                    f"{lag.below_resolution:,}/{lag_resolved:,} samples fell under the "
                    "measurement floor and are excluded from these percentiles"
                )
            if lag_sampled and lag_resolved < lag_sampled * 0.5:
                lag_note = (
                    f"only {lag_resolved}/{lag_sampled} sampled spans reached Postgres — "
                    "the lag figures describe the ones that did"
                )
        else:
            lag_note = "no sampled span reached Postgres — stream lag recorded as NULL"

    if redis is not None:
        await redis.aclose()
    if pg_conn is not None and args.no_record:
        await pg_conn.close()
        pg_conn = None

    result = RunResult(
        counters=counters,
        corrected=corrected,
        service=service,
        started_at=started_at,
        ended_at=ended_at,
        send_seconds=send_seconds,
        max_stream_depth=max_depth,
        depth_warnings=depth_warnings + (
            ["no consumer group on the ingest stream — the worker has never run"]
            if group_missing
            else []
        ),
        draining_ok=draining_ok and not group_missing,
        lag_p50_ms=lag_p50,
        lag_p99_ms=lag_p99,
        lag_sampled=lag_sampled,
        lag_resolved=lag_resolved,
        lag_note=lag_note,
    )

    if not args.no_record:
        if pg_conn is None:
            print("warn: no Postgres connection — run not recorded", file=sys.stderr)
        else:
            try:
                run_id = await _record(pg_conn, args, result)
                print(f"recorded as load_test_runs.id = {run_id}", file=sys.stderr)
            except Exception as exc:  # noqa: BLE001
                print(f"warn: could not record run ({exc}); numbers above stand", file=sys.stderr)
            finally:
                await pg_conn.close()

    return result


# ---------------------------------------------------------------------------
# Recording + reporting
# ---------------------------------------------------------------------------


def _quantiles_ms(hist: LocalHistogram) -> tuple[float, float, float, float]:
    q = hist.quantiles([0.5, 0.95, 0.99])
    return q[0.5] / 1000.0, q[0.95] / 1000.0, q[0.99] / 1000.0, hist.max_value / 1000.0


def _notes(args: argparse.Namespace, result: RunResult) -> str:
    parts = [
        f"batch={args.batch_size}",
        f"concurrency={args.concurrency}",
        f"target={args.target}",
        f"pipeline={args.pipeline}",
        f"requests={result.counters.requests}",
        f"rejected={result.counters.spans_rejected}",
        f"lag_sample={result.lag_resolved}/{result.lag_sampled}",
        f"drain={'ok' if result.draining_ok else 'NOT DRAINING'}",
    ]
    if result.counters.producer_starvations:
        parts.append(f"producer_starvations={result.counters.producer_starvations}")
    if result.lag_note:
        parts.append(f"lag_note={result.lag_note}")
    if args.notes:
        parts.append(args.notes)
    return "; ".join(parts)


async def _record(conn: Any, args: argparse.Namespace, result: RunResult) -> int:
    p50, p95, p99, pmax = _quantiles_ms(result.corrected)
    c = result.counters
    return await conn.fetchval(
        """
        INSERT INTO load_test_runs (
            label, started_at, ended_at, target_rps, achieved_rps,
            spans_sent, spans_accepted, spans_shed_503, errors,
            ingest_p50_ms, ingest_p95_ms, ingest_p99_ms, ingest_max_ms,
            stream_lag_p50_ms, stream_lag_p99_ms, max_stream_depth, notes
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)
        RETURNING id
        """,
        args.label,
        result.started_at,
        result.ended_at,
        float(args.rps),
        c.spans_sent / result.send_seconds if result.send_seconds > 0 else 0.0,
        c.spans_sent,
        c.spans_accepted,
        c.spans_shed_503,
        c.errors,
        p50,
        p95,
        p99,
        pmax,
        result.lag_p50_ms,
        result.lag_p99_ms,
        result.max_stream_depth,
        _notes(args, result),
    )


def _fmt(value: float | None, unit: str = "ms") -> str:
    return "n/a" if value is None else f"{value:,.2f}{unit}"


def print_summary(args: argparse.Namespace, result: RunResult) -> None:
    c = result.counters
    p50, p95, p99, pmax = _quantiles_ms(result.corrected)
    s50, s95, s99, smax = _quantiles_ms(result.service)
    achieved = c.spans_sent / result.send_seconds if result.send_seconds > 0 else 0.0
    accept_rate = (c.spans_accepted / c.spans_sent * 100) if c.spans_sent else 0.0

    line = "=" * 78
    out = [
        "",
        line,
        f" AOE load test — {args.label}",
        line,
        f" started            {result.started_at.isoformat(timespec='seconds')}",
        f" offered            {args.rps:,.0f} spans/s for {args.duration:g}s "
        f"(batch<={args.batch_size}, concurrency={args.concurrency})",
        f" achieved           {achieved:,.0f} spans/s over {result.send_seconds:.1f}s "
        f"({c.requests:,} requests)",
        "",
        " throughput",
        f"   spans_sent       {c.spans_sent:,}",
        f"   spans_accepted   {c.spans_accepted:,}  ({accept_rate:.1f}%)",
        f"   spans_shed_503   {c.spans_shed_503:,}  (backpressure working as designed, §5.2)",
        f"   spans_rejected   {c.spans_rejected:,}  (per-span validation failures inside a 202)",
        f"   errors           {c.errors:,}",
        f"   status codes     {json.dumps(c.status_counts, sort_keys=True)}",
        "",
        " ingestion latency (from scheduled send time — coordinated-omission corrected)",
        f"   p50              {_fmt(p50)}",
        f"   p95              {_fmt(p95)}",
        f"   p99              {_fmt(p99)}",
        f"   max              {_fmt(pmax)}",
        f"   service-only     p50 {_fmt(s50)}  p95 {_fmt(s95)}  p99 {_fmt(s99)}  max {_fmt(smax)}",
        "",
        " buffer",
        f"   max_stream_depth {result.max_stream_depth if result.max_stream_depth is not None else 'n/a'}",
        f"   stream_lag_p50   {_fmt(result.lag_p50_ms)}",
        f"   stream_lag_p99   {_fmt(result.lag_p99_ms)}",
        f"   lag samples      {result.lag_resolved:,} resolved / {result.lag_sampled:,} tracked"
        "  (XADD -> row visible in Postgres)",
    ]
    if result.lag_note:
        out.append(f"   note             {result.lag_note}")
    if c.producer_starvations:
        out.append(
            f"   generator        starved {c.producer_starvations:,} times — the load generator, "
            "not the server, may have been the bottleneck"
        )

    out.append("")
    if result.draining_ok and not result.depth_warnings:
        out.append(" drain check        OK — the worker kept up with ingest")
    else:
        out.append(" drain check        *** FAILED ***")
        for w in result.depth_warnings:
            out.append(f"   ! {w}")
        out.append(
            "   The latency numbers above describe the HTTP endpoint only. Do not quote"
        )
        out.append("   them as pipeline throughput until this is green.")

    out.append(line)
    if result.draining_ok and not result.depth_warnings and c.errors == 0:
        out.append(
            f" §8.1: sustained {achieved:,.0f} events/sec at {p99:,.1f}ms p99 ingestion latency"
        )
        if result.lag_p99_ms is not None:
            out.append(
                f"       with {result.lag_p99_ms:,.0f}ms p99 buffer-to-storage lag "
                f"(peak stream depth {result.max_stream_depth:,})"
            )
        out.append(line)
    print("\n".join(out))


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    settings = get_settings()
    p = argparse.ArgumentParser(
        prog="aoe-loadgen",
        description="Open-loop synthetic load against POST /v1/spans.",
    )
    p.add_argument(
        "--rps",
        type=float,
        default=1000.0,
        help="offered SPAN rate (events/sec). Request rate is roughly rps/batch-size.",
    )
    p.add_argument("--duration", type=float, default=30.0, help="seconds of offered load")
    p.add_argument("--concurrency", type=int, default=64, help="max in-flight requests")
    p.add_argument(
        "--batch-size",
        type=int,
        default=50,
        help=f"max spans per request (server caps at AOE_MAX_BATCH_SPANS={settings.max_batch_spans})",
    )
    p.add_argument("--label", default="adhoc", help="label recorded with the run")
    p.add_argument("--target", default=settings.ingest_url, help="ingestion service base URL")
    p.add_argument("--pipeline", default=settings.pipeline_name)
    p.add_argument("--notes", default="", help="free text appended to the recorded notes")
    p.add_argument("--no-record", action="store_true", help="skip the load_test_runs insert")
    p.add_argument("--seed", type=int, default=1337)
    p.add_argument("--corpus-size", type=int, default=512, help="distinct traces pre-generated")
    p.add_argument("--timeout", type=float, default=15.0, help="per-request timeout (s)")
    p.add_argument("--depth-poll-ms", type=float, default=250.0, help="XLEN sampling interval")
    p.add_argument("--lag-poll-ms", type=float, default=500.0, help="lag probe polling interval")
    p.add_argument("--lag-sample", type=int, default=2000, help="spans tracked for stream lag")
    p.add_argument(
        "--drain-wait",
        type=float,
        default=10.0,
        help="seconds to keep watching the stream after sending stops (0 disables)",
    )
    return p


def _validate(args: argparse.Namespace) -> None:
    settings = get_settings()
    if args.rps <= 0:
        raise SystemExit("--rps must be > 0")
    if args.duration <= 0:
        raise SystemExit("--duration must be > 0")
    if args.concurrency < 1:
        raise SystemExit("--concurrency must be >= 1")
    if args.batch_size < 1:
        raise SystemExit("--batch-size must be >= 1")
    if args.batch_size > settings.max_batch_spans:
        raise SystemExit(
            f"--batch-size {args.batch_size} exceeds the server's AOE_MAX_BATCH_SPANS "
            f"({settings.max_batch_spans}); every request would be rejected with 413"
        )


async def _preflight(target: str) -> None:
    url = target.rstrip("/") + "/health"
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.get(url)
    except Exception as exc:  # noqa: BLE001
        raise SystemExit(
            f"ingestion service unreachable at {url}: {exc}\nis `make ingest` running?"
        ) from exc
    if resp.status_code != 200:
        print(f"warn: {url} returned {resp.status_code}: {resp.text[:200]}", file=sys.stderr)


async def _amain(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    _validate(args)
    await _preflight(args.target)
    result = await _run_load(args)
    print_summary(args, result)
    return 0 if result.draining_ok and result.counters.errors == 0 else 1


def main() -> None:
    try:
        raise SystemExit(asyncio.run(_amain()))
    except KeyboardInterrupt:  # pragma: no cover
        raise SystemExit(130) from None


if __name__ == "__main__":  # pragma: no cover
    main()
