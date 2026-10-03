/** Presentation helpers. Every duration from the API is float milliseconds. */

const intFmt = new Intl.NumberFormat(undefined, { maximumFractionDigits: 0 });
const compactFmt = new Intl.NumberFormat(undefined, {
  notation: "compact",
  maximumFractionDigits: 1,
});

export function fmtInt(n: number | null | undefined): string {
  if (n === null || n === undefined || !Number.isFinite(n)) return "—";
  return intFmt.format(n);
}

/** 1,284 / 12.9K / 4.2M — for stat-tile values that must stay short. */
export function fmtCompact(n: number | null | undefined): string {
  if (n === null || n === undefined || !Number.isFinite(n)) return "—";
  return Math.abs(n) < 10_000 ? intFmt.format(n) : compactFmt.format(n);
}

export function fmtMs(ms: number | null | undefined): string {
  if (ms === null || ms === undefined || !Number.isFinite(ms)) return "—";
  if (ms < 1) return `${ms.toFixed(2)} ms`;
  if (ms < 1000) return `${ms.toFixed(ms < 10 ? 1 : 0)} ms`;
  if (ms < 60_000) return `${(ms / 1000).toFixed(2)} s`;
  return `${(ms / 60_000).toFixed(1)} min`;
}

/** Axis ticks: bare number, no unit (the axis label carries "ms"). */
export function fmtMsTick(ms: number): string {
  if (!Number.isFinite(ms)) return "";
  if (ms >= 10_000) return `${intFmt.format(Math.round(ms / 1000))}k`;
  if (ms >= 100) return intFmt.format(Math.round(ms));
  return String(Math.round(ms * 10) / 10);
}

export function fmtUsd(v: number | null | undefined): string {
  if (v === null || v === undefined || !Number.isFinite(v)) return "—";
  if (v === 0) return "$0";
  const abs = Math.abs(v);
  if (abs < 0.01) return `$${v.toFixed(5)}`;
  if (abs < 1) return `$${v.toFixed(4)}`;
  if (abs < 1000) return `$${v.toFixed(2)}`;
  return `$${compactFmt.format(v)}`;
}

export function fmtRate(v: number | null | undefined, digits = 1): string {
  if (v === null || v === undefined || !Number.isFinite(v)) return "—";
  return v.toFixed(digits);
}

/**
 * The API sends `error_rate` and `PathStat.share` as ratios. Some backends
 * emit them already multiplied; auto-detect rather than render "4200%".
 */
export function fmtRatioPct(v: number | null | undefined, digits = 1): string {
  if (v === null || v === undefined || !Number.isFinite(v)) return "—";
  const pct = v <= 1 ? v * 100 : v;
  return `${pct.toFixed(digits)}%`;
}

/** `deviation_pct` is already a percentage per its name. Signed for direction. */
export function fmtSignedPct(v: number | null | undefined, digits = 1): string {
  if (v === null || v === undefined || !Number.isFinite(v)) return "—";
  const sign = v > 0 ? "+" : "";
  return `${sign}${v.toFixed(digits)}%`;
}

function toDate(iso: string | null | undefined): Date | null {
  if (!iso) return null;
  const d = new Date(iso);
  return Number.isNaN(d.getTime()) ? null : d;
}

/** 14:03:22 — local time, the resolution a live feed needs. */
export function fmtClock(iso: string | null | undefined): string {
  const d = toDate(iso);
  if (!d) return "—";
  return d.toLocaleTimeString(undefined, {
    hour12: false,
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}

/** Axis ticks on the latency chart: seconds only matter on the short windows. */
export function fmtBucketTick(iso: string, withSeconds: boolean): string {
  const d = toDate(iso);
  if (!d) return "";
  return d.toLocaleTimeString(undefined, {
    hour12: false,
    hour: "2-digit",
    minute: "2-digit",
    ...(withSeconds ? { second: "2-digit" as const } : {}),
  });
}

export function fmtDateTime(iso: string | null | undefined): string {
  const d = toDate(iso);
  if (!d) return "—";
  return `${d.toLocaleDateString(undefined, { month: "short", day: "numeric" })} ${fmtClock(iso)}`;
}

export function fmtRelative(epochMs: number | null | undefined): string {
  if (!epochMs) return "never";
  const secs = Math.max(0, Math.round((Date.now() - epochMs) / 1000));
  if (secs < 2) return "just now";
  if (secs < 60) return `${secs}s ago`;
  const mins = Math.round(secs / 60);
  if (mins < 60) return `${mins}m ago`;
  return `${Math.round(mins / 60)}h ago`;
}

/** Trace and span ids are long hex; the head is enough to eyeball-match. */
export function shortId(id: string | null | undefined, len = 10): string {
  if (!id) return "—";
  return id.length <= len ? id : id.slice(0, len);
}
