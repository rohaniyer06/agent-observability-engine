"""CLI entry point for the harness agent (`aoe-harness`).

Drives N traces through the LangGraph pipeline at a configurable concurrency and
rate, emitting OTel-GenAI-aligned spans into `POST /v1/spans` the whole time.

The cost figure printed at the end is computed locally for the operator's
benefit only. The authoritative number is the worker's — there is exactly one
costing implementation and this is not it (design doc §7.7).
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import sys
from collections import Counter

from aoe.config import get_settings
from aoe.harness.graph import (
    BASELINE_PATHS,
    RunContext,
    RunOutcome,
    build_graph,
    path_signature,
    run_once,
)
from aoe.harness.instrument import SpanEmitter, TraceRecorder
from aoe.harness.providers import build_provider
from aoe.harness.tickets import ticket_for_run
from aoe.logging import log_fields, setup_logging
from aoe.pricing import get_pricing

log = logging.getLogger("aoe.harness")

SEED_SQL = """
INSERT INTO pipeline_paths (pipeline_name, path_signature, path, occurrences, is_seeded)
VALUES ($1, $2, $3, 0, TRUE)
ON CONFLICT (pipeline_name, path_signature) DO UPDATE
    SET is_seeded = TRUE,
        path = EXCLUDED.path
"""


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="aoe-harness",
        description="Run the generic 4-node support-ticket triage pipeline and emit spans.",
    )
    parser.add_argument("--runs", type=int, default=20, help="number of traces (default 20)")
    parser.add_argument(
        "--concurrency", type=int, default=4, help="traces in flight at once (default 4)"
    )
    parser.add_argument(
        "--rps",
        type=float,
        default=None,
        help="optional launch pacing in traces/second (default: as fast as concurrency allows)",
    )
    parser.add_argument(
        "--provider",
        choices=("auto", "anthropic", "simulated"),
        default=None,
        help="override AOE_HARNESS_PROVIDER",
    )
    parser.add_argument(
        "--seed-baselines",
        action="store_true",
        help=(
            "write BASELINE_PATHS into pipeline_paths (is_seeded=true) before running, so the "
            "drift detector does not flag the failure modes this harness injects on purpose"
        ),
    )
    parser.add_argument("--pipeline", default=None, help="override AOE_PIPELINE_NAME")
    return parser.parse_args(argv)


async def seed_baselines(settings, pipeline_name: str) -> int:
    """Insert every legitimate path as a seeded baseline (design doc §7.6)."""
    from aoe.db.pool import close_pool, get_pool

    rows = [(pipeline_name, path_signature(p), p) for p in BASELINE_PATHS]
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.executemany(SEED_SQL, rows)
    finally:
        with contextlib.suppress(Exception):
            await close_pool()
    return len(rows)


async def _run_all(args: argparse.Namespace) -> int:
    settings = get_settings()
    overrides = {}
    if args.provider:
        overrides["harness_provider"] = args.provider
    if overrides:
        settings = settings.model_copy(update=overrides)
    pipeline_name = args.pipeline or settings.pipeline_name

    setup_logging(settings.log_level, "aoe.harness")

    if args.seed_baselines:
        try:
            count = await seed_baselines(settings, pipeline_name)
            print(
                f"seeded {count} baseline paths for pipeline {pipeline_name!r} "
                f"(signature encoding: {path_signature(['a', 'b'])!r})"
            )
        except Exception as exc:  # noqa: BLE001 - seeding is best-effort, the run is the point
            print(
                f"WARNING: could not seed baseline paths ({type(exc).__name__}: {exc}). "
                "Drift detection will flag the harness's own injected failure modes.",
                file=sys.stderr,
            )

    if args.runs <= 0:
        return 0

    try:
        provider = build_provider(settings)
    except RuntimeError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    print(
        f"harness: provider={provider.name} model={provider.model} "
        f"pipeline={pipeline_name} runs={args.runs} concurrency={args.concurrency} "
        f"ingest={settings.ingest_url}"
        + (f" rps={args.rps}" if args.rps else ""),
        flush=True,
    )
    if provider.name == "simulated":
        print(
            "  NOTE: simulated provider — no network calls, no spend. "
            "Set ANTHROPIC_API_KEY (or --provider anthropic) for real model calls.",
            flush=True,
        )

    compiled = build_graph()
    emitter = SpanEmitter(settings)
    outcomes: list[RunOutcome] = []

    async with emitter:
        semaphore = asyncio.Semaphore(max(1, args.concurrency))

        async def one(index: int) -> RunOutcome:
            async with semaphore:
                recorder = TraceRecorder(
                    emitter,
                    pipeline_name=pipeline_name,
                    model_name=provider.model,
                    provider_name=provider.name,
                    attributes={"run_index": index, "harness_provider": provider.name},
                )
                ctx = RunContext.create(
                    recorder=recorder,
                    provider=provider,
                    settings=settings,
                    run_index=index,
                )
                ticket = ticket_for_run(index, ctx.rng)
                return await run_once(compiled, ticket=ticket, ctx=ctx)

        tasks: list[asyncio.Task[RunOutcome]] = []
        loop = asyncio.get_running_loop()
        interval = 1.0 / args.rps if args.rps and args.rps > 0 else 0.0
        next_launch = loop.time()
        try:
            for index in range(args.runs):
                if interval:
                    delay = next_launch - loop.time()
                    if delay > 0:
                        await asyncio.sleep(delay)
                    next_launch += interval
                tasks.append(asyncio.create_task(one(index)))
            outcomes = list(await asyncio.gather(*tasks))
        except KeyboardInterrupt:
            for task in tasks:
                task.cancel()
            outcomes = [t.result() for t in tasks if t.done() and not t.cancelled()]
            print("interrupted; flushing what we have", file=sys.stderr)
        finally:
            # Closing the emitter drains the queue, so the summary below reports
            # what actually reached the ingest endpoint rather than what we hoped.
            await provider.aclose()

    _print_summary(outcomes, emitter, provider, pipeline_name)
    return 0


def _print_summary(outcomes, emitter: SpanEmitter, provider, pipeline_name: str) -> None:
    pricing = get_pricing()
    total_cost = 0.0
    in_tokens = out_tokens = 0
    for o in outcomes:
        in_tokens += o.input_tokens
        out_tokens += o.output_tokens
        total_cost += pricing.cost_usd(
            o.model,
            o.input_tokens,
            o.output_tokens,
            o.cache_read_tokens,
            o.cache_write_tokens,
        )

    paths = Counter(path_signature(o.path) for o in outcomes)
    baseline = {path_signature(p) for p in BASELINE_PATHS}
    errors = sum(1 for o in outcomes if o.status == "error")
    stats = emitter.stats

    print()
    print("=" * 68)
    print(f"harness summary  pipeline={pipeline_name}  provider={provider.name}")
    print("=" * 68)
    print(f"  runs completed      {len(outcomes)}")
    print(f"  traces with errors  {errors}")
    print(f"  spans submitted     {stats.submitted}")
    print(f"  spans accepted      {stats.emitted}")
    print(f"  spans dropped       {stats.dropped}")
    if stats.backpressure_batches:
        print(f"  batches shed (503)  {stats.backpressure_batches}")
    if stats.failed_batches:
        print(f"  batches failed      {stats.failed_batches}")
    print(f"  tokens in/out       {in_tokens} / {out_tokens}")
    print(f"  local cost estimate ${total_cost:.6f}   (authoritative number is the worker's)")
    print()
    print("  path distribution")
    for signature, count in paths.most_common():
        flag = " " if signature in baseline else " <- NOT in BASELINE_PATHS"
        print(f"    {count:>5}  {signature}{flag}")
    print("=" * 68)

    log_fields(
        log,
        logging.INFO,
        "harness run complete",
        pipeline=pipeline_name,
        provider=provider.name,
        runs=len(outcomes),
        error_traces=errors,
        local_cost_usd=round(total_cost, 8),
        **stats.as_dict(),
    )


def main() -> None:
    args = parse_args()
    try:
        code = asyncio.run(_run_all(args))
    except KeyboardInterrupt:
        code = 130
    raise SystemExit(code)


if __name__ == "__main__":  # pragma: no cover
    main()
