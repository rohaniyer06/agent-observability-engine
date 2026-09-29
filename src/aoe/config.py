"""Single source of configuration for every process in the system.

Everything is overridable by environment variable with an `AOE_` prefix (see
.env.example). Defaults are chosen so that `make up && make migrate` gives you a
working system with no .env at all.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

REPO_ROOT = Path(__file__).resolve().parents[2]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="AOE_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # ---- Infrastructure ------------------------------------------------------
    postgres_dsn: str = "postgresql://aoe:aoe@localhost:55432/aoe"
    redis_url: str = "redis://localhost:56379/0"
    pg_min_pool: int = 2
    pg_max_pool: int = 20

    # ---- Ingestion service ---------------------------------------------------
    ingest_host: str = "0.0.0.0"
    ingest_port: int = 8000
    max_batch_spans: int = 500
    max_body_bytes: int = 1_048_576
    backpressure_stream_depth: int = 200_000
    backpressure_poll_ms: int = 250
    backpressure_retry_after_s: int = 2
    stream_maxlen: int = 1_000_000
    rate_limit_rps: float = 0.0  # 0 disables

    # ---- Query API service ---------------------------------------------------
    api_host: str = "0.0.0.0"
    api_port: int = 8001
    # 5173 is `npm run dev`, 4173 is `npm run preview` (the production build).
    cors_origins: str = (
        "http://localhost:5173,http://127.0.0.1:5173,"
        "http://localhost:4173,http://127.0.0.1:4173"
    )
    default_page_size: int = 50
    max_page_size: int = 500

    # ---- Worker pool ---------------------------------------------------------
    worker_consumer_group: str = "telemetry_workers"
    worker_batch_size: int = 500
    worker_block_ms: int = 2000
    worker_reclaim_idle_ms: int = 30_000
    worker_reclaim_interval_ms: int = 5_000

    # Trace finalization (DEVIATIONS.md #2): a trace closes when either the
    # harness declares it done (trace_end + short grace, to let late children
    # land) or nothing new arrives for `trace_idle_timeout_ms`. Both paths run
    # through the same deadline queue.
    trace_idle_timeout_ms: int = 5_000
    trace_end_grace_ms: int = 500
    finalizer_interval_ms: int = 500
    finalizer_batch_size: int = 500

    # Latency rollups
    rollup_flush_interval_ms: int = 5_000
    rollup_bucket_seconds: int = 60
    # Redis histogram hashes are kept this long past their bucket so a restarted
    # flusher can still pick up an in-flight minute.
    rollup_key_ttl_s: int = 900

    # Cost anomaly detection (modified z-score over a rolling window)
    cost_window_size: int = 500
    cost_min_samples: int = 30
    cost_warn_zscore: float = 3.5
    cost_critical_zscore: float = 6.0
    # A sustained shift in cost (a model swap, a prompt that got longer) is ONE
    # incident, but it trips the z-score on every trace until the rolling window
    # re-centres. Without a cooldown that is dozens of rows describing a single
    # event, which reads as a broken detector rather than a working one.
    cost_event_cooldown_ms: int = 60_000

    # Path drift detection
    drift_min_traces: int = 50
    drift_baseline_freq_pct: float = 1.0
    drift_event_cooldown_ms: int = 60_000

    # ---- Harness agent -------------------------------------------------------
    harness_provider: str = "auto"  # auto | anthropic | simulated
    harness_model: str = "claude-haiku-4-5"
    harness_max_tokens: int = 512
    harness_seed: int = 1337
    ingest_url: str = "http://localhost:8000"
    pipeline_name: str = "support_ticket_triage"
    # Deliberate failure injection (design doc §5.1) so drift/error detection has
    # real signal rather than a synthetic happy path.
    harness_classify_timeout_rate: float = 0.08
    harness_escalate_retry_rate: float = 0.15
    harness_hard_error_rate: float = 0.03

    # ---- Paths ---------------------------------------------------------------
    pricing_file: str = str(REPO_ROOT / "config" / "pricing.yaml")
    migrations_dir: str = str(REPO_ROOT / "migrations")

    # ---- Misc ----------------------------------------------------------------
    log_level: str = "INFO"

    @property
    def cors_origin_list(self) -> list[str]:
        return [o.strip() for o in self.cors_origins.split(",") if o.strip()]


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
