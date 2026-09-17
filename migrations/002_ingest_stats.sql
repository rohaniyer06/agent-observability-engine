-- Load-test bookkeeping (design doc §5.6). Kept out of 001 so the core schema
-- stays exactly the product surface; this table exists purely so a load run's
-- headline numbers survive the process that produced them.

CREATE TABLE IF NOT EXISTS load_test_runs (
    id                 SERIAL PRIMARY KEY,
    label              TEXT        NOT NULL,
    started_at         TIMESTAMPTZ NOT NULL,
    ended_at           TIMESTAMPTZ NOT NULL,
    target_rps         DOUBLE PRECISION NOT NULL,
    achieved_rps       DOUBLE PRECISION NOT NULL,
    spans_sent         BIGINT      NOT NULL,
    spans_accepted     BIGINT      NOT NULL,
    spans_shed_503     BIGINT      NOT NULL DEFAULT 0,
    errors             BIGINT      NOT NULL DEFAULT 0,
    ingest_p50_ms      DOUBLE PRECISION NOT NULL,
    ingest_p95_ms      DOUBLE PRECISION NOT NULL,
    ingest_p99_ms      DOUBLE PRECISION NOT NULL,
    ingest_max_ms      DOUBLE PRECISION NOT NULL,
    -- Wall-clock gap between XADD and the worker acking the entry. This is the
    -- number that tells you whether the buffer was actually keeping up.
    stream_lag_p50_ms  DOUBLE PRECISION,
    stream_lag_p99_ms  DOUBLE PRECISION,
    max_stream_depth   BIGINT,
    notes              TEXT,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_load_test_runs_started ON load_test_runs (started_at DESC);
