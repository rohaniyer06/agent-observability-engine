// Hand-maintained mirror of src/aoe/apimodels.py. If you change one, change the
// other — this file is the contract between the query API and the dashboard.
//
// Time convention: every duration is FLOAT MILLISECONDS. Timestamps are
// ISO-8601 UTC strings.

export type Severity = "warn" | "critical";
export type TraceStatus = "ok" | "error" | "partial";
export type FinalizedBy = "trace_end" | "timeout";

export interface TraceSummary {
  trace_id: string;
  pipeline_name: string;
  started_at: string;
  ended_at: string | null;
  duration_ms: number | null;
  total_cost_usd: number;
  total_input_tokens: number;
  total_output_tokens: number;
  span_count: number;
  status: TraceStatus;
  path: string[];
  finalized_by: FinalizedBy;
}

export interface SpanDetail {
  span_id: string;
  trace_id: string;
  parent_span_id: string | null;
  node_name: string;
  operation_name: string;
  model_name: string | null;
  provider_name: string | null;
  start_time_ns: number;
  end_time_ns: number;
  duration_ms: number;
  input_tokens: number;
  output_tokens: number;
  cost_usd: number;
  status: string;
  error_message: string | null;
  attributes: Record<string, unknown>;
}

export interface TraceDetail {
  trace: TraceSummary;
  spans: SpanDetail[];
}

export interface TracePage {
  items: TraceSummary[];
  next_cursor: string | null;
  has_more: boolean;
}

export interface LatencyPoint {
  bucket_start: string;
  p50_ms: number;
  p95_ms: number;
  p99_ms: number;
  max_ms: number;
  count: number;
  error_count: number;
}

export interface LatencySeries {
  node_name: string;
  points: LatencyPoint[];
}

export interface LatencyResponse {
  window: string;
  bucket_seconds: number;
  series: LatencySeries[];
  from_rollups: boolean;
}

export interface NodeStat {
  node_name: string;
  count: number;
  p99_ms: number;
  error_rate: number;
}

export interface SummaryResponse {
  window: string;
  trace_count: number;
  span_count: number;
  error_trace_count: number;
  error_rate: number;
  total_cost_usd: number;
  traces_per_second: number;
  p50_ms: number;
  p95_ms: number;
  p99_ms: number;
  open_anomalies: number;
  drift_events: number;
  nodes: NodeStat[];
}

export interface CostAnomaly {
  id: number;
  trace_id: string;
  pipeline_name: string;
  detected_at: string;
  expected_cost_usd: number;
  actual_cost_usd: number;
  deviation_pct: number;
  z_score: number;
  sample_size: number;
  severity: Severity;
}

export interface AnomalyPage {
  items: CostAnomaly[];
  next_cursor: string | null;
  has_more: boolean;
}

export interface DriftEvent {
  id: number;
  pipeline_name: string;
  observed_path: string[];
  path_signature: string;
  baseline_paths: string[];
  trace_id: string;
  occurrence_count: number;
  first_seen_at: string;
  detected_at: string;
}

export interface PathStat {
  path: string[];
  path_signature: string;
  occurrences: number;
  share: number;
  is_baseline: boolean;
  is_seeded: boolean;
}

export interface DriftResponse {
  pipeline_name: string;
  total_traces: number;
  paths: PathStat[];
  events: DriftEvent[];
}

export interface FlowEdge {
  source: string;
  target: string;
  value: number;
  is_baseline: boolean;
}

export interface FlowResponse {
  pipeline_name: string;
  nodes: string[];
  edges: FlowEdge[];
}

export interface SystemStats {
  stream_depth: number;
  pending_entries: number;
  consumer_count: number;
  consumers: Array<Record<string, unknown>>;
  traces_awaiting_finalization: number;
  spans_ingested: number;
  spans_processed: number;
  spans_duplicate: number;
  traces_finalized: number;
  traces_finalized_by_timeout: number;
  entries_reclaimed: number;
  unknown_models: string[];
  backpressure_threshold: number;
}

export interface PricingResponse {
  version: number;
  as_of: string;
  source: string;
  currency: string;
  models: Record<string, { input: number; output: number; note: string | null }>;
  cache_multipliers: Record<string, number>;
  unknown_models_seen: string[];
}

export interface LoadTestRun {
  id: number;
  label: string;
  started_at: string;
  ended_at: string;
  target_rps: number;
  achieved_rps: number;
  spans_sent: number;
  spans_accepted: number;
  spans_shed_503: number;
  errors: number;
  ingest_p50_ms: number;
  ingest_p95_ms: number;
  ingest_p99_ms: number;
  ingest_max_ms: number;
  stream_lag_p50_ms: number | null;
  stream_lag_p99_ms: number | null;
  max_stream_depth: number | null;
  notes: string | null;
}

/** Pushed over WS /v1/live. Mirrors aoe.schema.LiveTraceEvent. */
export interface LiveTraceEvent {
  type: "trace_finalized" | "cost_anomaly" | "path_drift";
  trace_id: string;
  pipeline_name: string;
  started_at: string;
  duration_ms: number;
  total_cost_usd: number;
  status: string;
  path: string[];
  span_count: number;
  detail: Record<string, unknown>;
}
