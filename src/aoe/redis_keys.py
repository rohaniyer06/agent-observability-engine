"""Every Redis key the system uses, in one place.

Keeping these centralised is what makes it possible to reason about the memory
footprint and the TTL story without grepping four services.
"""

from __future__ import annotations

PREFIX = "aoe"

# --- Durable ingest buffer ---------------------------------------------------
# Append-only log. Consumer group gives at-least-once delivery with a pending
# entries list we reclaim via XAUTOCLAIM (design doc §7.4).
STREAM_SPANS = f"{PREFIX}:stream:spans"

# --- Live dashboard fan-out --------------------------------------------------
# Pub/sub is correct HERE and wrong for ingestion: a dropped live-feed update is
# a missed animation frame, a dropped span is lost telemetry.
PUBSUB_LIVE = f"{PREFIX}:pubsub:live"

# --- In-flight trace state ---------------------------------------------------
def trace_state(trace_id: str) -> str:
    """HASH: accumulating state for a trace that has not been finalized yet."""
    return f"{PREFIX}:trace:{trace_id}"


# ZSET: trace_id -> finalize-at deadline (epoch ms). The finalizer pops
# everything with score <= now. Both the explicit trace_end path and the idle
# timeout path write into this one queue, so there is a single finalization
# code path regardless of which signal fired.
TRACE_DEADLINES = f"{PREFIX}:trace:deadlines"

# --- Latency histograms ------------------------------------------------------
def latency_histogram(node_name: str, bucket_epoch_s: int) -> str:
    """HASH: bucket_index -> count, for one (node, minute).

    HINCRBY is atomic, so N workers merge into the same histogram for free.
    This replaces the design doc's per-node ZSET of raw durations, which grows
    without bound and cannot be merged across workers (DEVIATIONS.md #1).
    """
    return f"{PREFIX}:hist:{node_name}:{bucket_epoch_s}"


def latency_meta(node_name: str, bucket_epoch_s: int) -> str:
    """HASH: count / error_count / max_us sidecar for a histogram bucket."""
    return f"{PREFIX}:histmeta:{node_name}:{bucket_epoch_s}"


# SET of "node_name|bucket_epoch_s" strings that have unflushed data.
HISTOGRAM_DIRTY = f"{PREFIX}:hist:dirty"

# --- Cost anomaly detection --------------------------------------------------
def cost_window(pipeline_name: str) -> str:
    """LIST: recent per-trace total costs, newest first. LPUSH + LTRIM."""
    return f"{PREFIX}:cost:{pipeline_name}"


def cost_cooldown(pipeline_name: str, severity: str) -> str:
    """STRING with TTL: suppresses repeat cost alerts for one ongoing incident.

    Keyed by severity as well as pipeline so an incident that escalates from
    warn to critical still surfaces instead of being swallowed by the open
    window of the lesser alert.
    """
    return f"{PREFIX}:cost:cooldown:{pipeline_name}:{severity}"


# --- Path drift --------------------------------------------------------------
def path_counts(pipeline_name: str) -> str:
    """HASH: path_signature -> observation count."""
    return f"{PREFIX}:paths:{pipeline_name}"


def drift_cooldown(pipeline_name: str, path_signature: str) -> str:
    """STRING with TTL: suppresses repeat drift events for the same path."""
    return f"{PREFIX}:drift:cooldown:{pipeline_name}:{path_signature}"


# --- Ingest backpressure -----------------------------------------------------
# STRING with a short TTL holding the last XLEN reading, so the hot path does
# not pay a round trip per request just to decide whether to shed load.
STREAM_DEPTH_CACHE = f"{PREFIX}:ingest:depth"

# --- Worker observability ----------------------------------------------------
# HASH of counters the query API exposes at /system/stats.
WORKER_STATS = f"{PREFIX}:worker:stats"
