-- Design doc §4.2, with the deviations recorded in DEVIATIONS.md.
--
-- Unit convention: everything is stored in MICROSECONDS (duration_us) and
-- exposed by the query API in milliseconds. The doc specified `duration_ms INT`;
-- microseconds is a lossless superset and this is a latency product, so throwing
-- away sub-millisecond resolution at the storage layer is not a trade worth making.

CREATE TABLE IF NOT EXISTS traces (
    trace_id            UUID PRIMARY KEY,
    pipeline_name       TEXT        NOT NULL,
    started_at          TIMESTAMPTZ NOT NULL,
    ended_at            TIMESTAMPTZ,
    total_duration_us   BIGINT,
    total_cost_usd      NUMERIC(12, 8) NOT NULL DEFAULT 0,
    total_input_tokens  INT         NOT NULL DEFAULT 0,
    total_output_tokens INT         NOT NULL DEFAULT 0,
    span_count          INT         NOT NULL DEFAULT 0,
    status              TEXT        NOT NULL CHECK (status IN ('ok', 'error', 'partial')),
    path                TEXT[]      NOT NULL DEFAULT '{}',
    -- Stable join key for drift bookkeeping: the ordered path collapsed to a
    -- single string, so it can be a hash key in Redis and a GROUP BY here.
    path_signature      TEXT        NOT NULL DEFAULT '',
    -- 'trace_end'  -> the harness told us the trace was complete
    -- 'timeout'    -> the reaper closed it out because no spans arrived in time
    finalized_by        TEXT        NOT NULL DEFAULT 'timeout'
                          CHECK (finalized_by IN ('trace_end', 'timeout')),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_traces_started_at    ON traces (started_at DESC);
CREATE INDEX IF NOT EXISTS idx_traces_pipeline_time ON traces (pipeline_name, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_traces_status        ON traces (status) WHERE status <> 'ok';

CREATE TABLE IF NOT EXISTS spans (
    span_id        UUID PRIMARY KEY,
    trace_id       UUID        NOT NULL,
    parent_span_id UUID,
    node_name      TEXT        NOT NULL,
    operation_name TEXT        NOT NULL,
    model_name     TEXT,
    provider_name  TEXT,
    start_time_ns  BIGINT      NOT NULL,
    end_time_ns    BIGINT      NOT NULL,
    duration_us    BIGINT      NOT NULL,
    input_tokens   INT         NOT NULL DEFAULT 0,
    output_tokens  INT         NOT NULL DEFAULT 0,
    cost_usd       NUMERIC(12, 8) NOT NULL DEFAULT 0,
    status         TEXT        NOT NULL,
    error_message  TEXT,
    attributes     JSONB       NOT NULL DEFAULT '{}'::jsonb,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
    -- NOTE: deliberately no FK to traces(trace_id). Spans arrive before their
    -- trace row is finalized, so a FK would force either an ordering constraint
    -- we cannot honour or a placeholder-row dance. Referential integrity is
    -- enforced by the worker, which is the only writer.
);

CREATE INDEX IF NOT EXISTS idx_spans_trace_id     ON spans (trace_id);
CREATE INDEX IF NOT EXISTS idx_spans_node_created ON spans (node_name, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_spans_created_at   ON spans (created_at DESC);

-- Pre-aggregated per-minute percentile rollups. The dashboard reads THIS, never
-- raw spans (design doc §4.2 blocker warning). Percentiles are computed from a
-- log-linear histogram merged in Redis across all workers, then upserted here.
CREATE TABLE IF NOT EXISTS latency_rollups (
    node_name    TEXT        NOT NULL,
    bucket_start TIMESTAMPTZ NOT NULL,
    p50_us       BIGINT      NOT NULL,
    p95_us       BIGINT      NOT NULL,
    p99_us       BIGINT      NOT NULL,
    max_us       BIGINT      NOT NULL,
    count        BIGINT      NOT NULL,
    error_count  BIGINT      NOT NULL DEFAULT 0,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (node_name, bucket_start)
);

CREATE INDEX IF NOT EXISTS idx_latency_rollups_bucket ON latency_rollups (bucket_start DESC);

CREATE TABLE IF NOT EXISTS cost_anomalies (
    id                SERIAL PRIMARY KEY,
    trace_id          UUID        NOT NULL,
    pipeline_name     TEXT        NOT NULL,
    detected_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    expected_cost_usd NUMERIC(12, 8) NOT NULL,
    actual_cost_usd   NUMERIC(12, 8) NOT NULL,
    deviation_pct     DOUBLE PRECISION NOT NULL,
    -- Modified z-score (median + MAD), not a raw stddev z-score. Cost
    -- distributions are right-skewed; see DEVIATIONS.md #4.
    z_score           DOUBLE PRECISION NOT NULL,
    sample_size       INT         NOT NULL,
    severity          TEXT        NOT NULL CHECK (severity IN ('warn', 'critical')),
    UNIQUE (trace_id)
);

CREATE INDEX IF NOT EXISTS idx_cost_anomalies_detected ON cost_anomalies (detected_at DESC);

CREATE TABLE IF NOT EXISTS path_drift_events (
    id              SERIAL PRIMARY KEY,
    pipeline_name   TEXT        NOT NULL,
    baseline_paths  TEXT[]      NOT NULL DEFAULT '{}',
    observed_path   TEXT[]      NOT NULL,
    path_signature  TEXT        NOT NULL,
    trace_id        UUID        NOT NULL,
    -- How many traces have taken this novel path since it was first seen. Lets
    -- the dashboard show "novel path, 47 occurrences" instead of 47 rows.
    occurrence_count BIGINT     NOT NULL DEFAULT 1,
    first_seen_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    detected_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_path_drift_detected  ON path_drift_events (detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_path_drift_signature ON path_drift_events (pipeline_name, path_signature);

-- Every distinct path a pipeline has ever taken, with a running count. This is
-- what makes drift detection self-calibrating instead of a hardcoded allowlist:
-- a path becomes "baseline" once it is either explicitly seeded or observed
-- often enough to be normal (design doc §7.6).
CREATE TABLE IF NOT EXISTS pipeline_paths (
    pipeline_name  TEXT        NOT NULL,
    path_signature TEXT        NOT NULL,
    path           TEXT[]      NOT NULL,
    occurrences    BIGINT      NOT NULL DEFAULT 0,
    is_seeded      BOOLEAN     NOT NULL DEFAULT FALSE,
    first_seen_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (pipeline_name, path_signature)
);
