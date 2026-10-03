/**
 * Thin typed client for the query API (docs/API.md, port 8001).
 *
 * One place for: the base URL, query-string building, and error normalisation.
 * WS /v1/live is served from this same origin (DEVIATIONS.md item 7), so the
 * socket URL is derived here rather than configured separately.
 *
 * Every response type is imported from `../types/api` — the hand-maintained
 * mirror of the backend's Pydantic models. Nothing is redefined here.
 */
import type {
  AnomalyPage,
  DriftResponse,
  FlowResponse,
  LatencyResponse,
  LoadTestRun,
  PricingResponse,
  Severity,
  SummaryResponse,
  SystemStats,
  TraceDetail,
  TracePage,
  TraceStatus,
} from "../types/api";

const RAW_BASE = import.meta.env.VITE_API_URL ?? "http://localhost:8001";

/** No trailing slash, so `${API_BASE}/v1/...` is always well-formed. */
export const API_BASE = RAW_BASE.replace(/\/+$/, "");

/** http -> ws, https -> wss. Same origin as REST. */
export const LIVE_WS_URL = `${API_BASE.replace(/^http/i, "ws")}/v1/live`;

/** Windows the API accepts (docs/API.md). `window` is a plain string in the
 *  response models, so this union lives here rather than in types/api.ts. */
export type WindowKey = "5m" | "15m" | "1h" | "6h" | "24h" | "7d";

export const WINDOW_OPTIONS: ReadonlyArray<{ value: WindowKey; label: string }> = [
  { value: "5m", label: "5 min" },
  { value: "15m", label: "15 min" },
  { value: "1h", label: "1 hour" },
  { value: "6h", label: "6 hours" },
  { value: "24h", label: "24 hours" },
  { value: "7d", label: "7 days" },
];

/**
 * Rollup bucket width per window. `/v1/metrics/latency` reads `latency_rollups`
 * only, whose finest grain is one minute, so 60s is the floor; wider windows ask
 * for wider buckets so a 24h view is ~96 points per node rather than 1,440.
 */
export const BUCKET_SECONDS: Record<WindowKey, number> = {
  "5m": 60,
  "15m": 60,
  "1h": 60,
  "6h": 300,
  "24h": 900,
  "7d": 3600,
};

export class ApiError extends Error {
  readonly status: number;
  readonly url: string;

  constructor(message: string, status: number, url: string) {
    super(message);
    this.name = "ApiError";
    this.status = status;
    this.url = url;
  }
}

type QueryValue = string | number | boolean | readonly string[] | null | undefined;

function buildUrl(path: string, params?: Record<string, QueryValue>): string {
  const url = new URL(`${API_BASE}${path}`);
  if (params) {
    for (const [key, value] of Object.entries(params)) {
      if (value === null || value === undefined || value === "") continue;
      if (Array.isArray(value)) {
        // Repeatable params (e.g. ?node=a&node=b) — never a comma-joined string.
        for (const item of value) if (item !== "") url.searchParams.append(key, item);
      } else {
        url.searchParams.set(key, String(value));
      }
    }
  }
  return url.toString();
}

async function get<T>(
  path: string,
  params?: Record<string, QueryValue>,
  signal?: AbortSignal,
): Promise<T> {
  const url = buildUrl(path, params);
  const res = await fetch(url, {
    signal,
    headers: { Accept: "application/json" },
  });

  if (!res.ok) {
    let detail = res.statusText;
    try {
      const body = (await res.json()) as unknown;
      if (body && typeof body === "object" && "detail" in body) {
        const d = (body as { detail: unknown }).detail;
        if (typeof d === "string") detail = d;
      }
    } catch {
      /* non-JSON error body — keep the status text */
    }
    throw new ApiError(detail || `request failed`, res.status, url);
  }

  return (await res.json()) as T;
}

/** Turns anything thrown by `get` into one line a panel can render. */
export function describeError(err: unknown): string {
  if (err instanceof ApiError) return `${err.status} · ${err.message}`;
  if (err instanceof DOMException && err.name === "AbortError") return "request cancelled";
  if (err instanceof TypeError) {
    // fetch() rejects with TypeError when the host is unreachable or CORS blocks it.
    return `Cannot reach the query API at ${API_BASE}. Is it running? (make api)`;
  }
  if (err instanceof Error) return err.message;
  return String(err);
}

export function isAbort(err: unknown): boolean {
  return err instanceof DOMException && err.name === "AbortError";
}

// ---------------------------------------------------------------- endpoints

export const api = {
  traces: (
    params: {
      limit?: number;
      cursor?: string | null;
      pipeline?: string;
      status?: TraceStatus;
      window?: WindowKey;
      node?: string;
    },
    signal?: AbortSignal,
  ) => get<TracePage>("/v1/traces", params, signal),

  trace: (traceId: string, signal?: AbortSignal) =>
    get<TraceDetail>(`/v1/traces/${encodeURIComponent(traceId)}`, undefined, signal),

  latency: (
    params: { node?: readonly string[]; window?: WindowKey; bucket_seconds?: number },
    signal?: AbortSignal,
  ) => get<LatencyResponse>("/v1/metrics/latency", params, signal),

  summary: (params: { window?: WindowKey; pipeline?: string }, signal?: AbortSignal) =>
    get<SummaryResponse>("/v1/metrics/summary", params, signal),

  nodes: (signal?: AbortSignal) => get<string[]>("/v1/metrics/nodes", undefined, signal),

  anomalies: (
    params: { limit?: number; cursor?: string | null; severity?: Severity; window?: WindowKey },
    signal?: AbortSignal,
  ) => get<AnomalyPage>("/v1/anomalies", params, signal),

  drift: (params: { pipeline?: string; limit?: number }, signal?: AbortSignal) =>
    get<DriftResponse>("/v1/drift", params, signal),

  driftFlow: (params: { pipeline?: string; window?: WindowKey }, signal?: AbortSignal) =>
    get<FlowResponse>("/v1/drift/flow", params, signal),

  systemStats: (signal?: AbortSignal) => get<SystemStats>("/v1/system/stats", undefined, signal),

  pricing: (signal?: AbortSignal) => get<PricingResponse>("/v1/system/pricing", undefined, signal),

  loadTests: (params: { limit?: number }, signal?: AbortSignal) =>
    get<LoadTestRun[]>("/v1/loadtests", params, signal),
};
