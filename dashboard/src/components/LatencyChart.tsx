import { useMemo, useState } from "react";
import {
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

import { api, BUCKET_SECONDS, type WindowKey } from "../lib/api";
import { fmtBucketTick, fmtMs, fmtMsTick } from "../lib/format";
import { buildColorMap, slotColor } from "../lib/palette";
import { usePalette } from "../lib/theme";
import { usePolling } from "../lib/usePolling";
import { Badge } from "./ui";
import { Panel } from "./Panel";

type Quantile = "p50_ms" | "p95_ms" | "p99_ms";
const QUANTILES: ReadonlyArray<{ key: Quantile; label: string; dash?: string }> = [
  { key: "p99_ms", label: "p99" },
  { key: "p95_ms", label: "p95", dash: "5 3" },
  { key: "p50_ms", label: "p50", dash: "2 3" },
];

export function LatencyChart({ window: win, paused }: { window: WindowKey; paused: boolean }) {
  const palette = usePalette();
  const [hidden, setHidden] = useState<ReadonlySet<string>>(new Set());

  const { data, error, loading, refetch } = usePolling(
    (signal) => api.latency({ window: win, bucket_seconds: BUCKET_SECONDS[win] }, signal),
    { intervalMs: 15_000, paused, key: win },
  );

  const series = data?.series ?? [];
  const colors = useMemo(() => buildColorMap(series.map((s) => s.node_name)), [series]);

  // Recharts wants one row per x value with a column per line.
  const rows = useMemo(() => {
    const byBucket = new Map<string, Record<string, number | string>>();
    for (const s of series) {
      if (hidden.has(s.node_name)) continue;
      for (const p of s.points) {
        const row = byBucket.get(p.bucket_start) ?? { bucket_start: p.bucket_start };
        for (const q of QUANTILES) row[`${s.node_name}::${q.key}`] = p[q.key];
        byBucket.set(p.bucket_start, row);
      }
    }
    return [...byBucket.values()].sort((a, b) =>
      String(a.bucket_start).localeCompare(String(b.bucket_start)),
    );
  }, [series, hidden]);

  const withSeconds = BUCKET_SECONDS[win] < 60;

  return (
    <Panel
      title="Latency by node"
      subtitle={
        // This claim is the entire point of the percentile architecture, so it
        // is stated in the UI rather than left implicit.
        data?.from_rollups ? (
          <span className="inline-flex items-center gap-1.5">
            <Badge tone="accent">pre-aggregated</Badge>
            served from latency_rollups — raw spans are never scanned
          </span>
        ) : (
          "p50 / p95 / p99 per node"
        )
      }
      loading={loading}
      error={error}
      onRetry={refetch}
      empty={!loading && rows.length === 0}
      emptyHint="No rollups in this window. Run `make harness` or `make loadtest`."
    >
      <div className="h-72 w-full">
        <ResponsiveContainer width="100%" height="100%">
          <LineChart data={rows} margin={{ top: 4, right: 8, bottom: 4, left: 4 }}>
            <CartesianGrid stroke={palette.grid} strokeDasharray="2 4" vertical={false} />
            <XAxis
              dataKey="bucket_start"
              tickFormatter={(v: string) => fmtBucketTick(v, withSeconds)}
              stroke={palette.axis}
              tick={{ fontSize: 11, fill: palette.axis }}
              minTickGap={28}
            />
            <YAxis
              tickFormatter={fmtMsTick}
              stroke={palette.axis}
              tick={{ fontSize: 11, fill: palette.axis }}
              width={52}
            />
            <Tooltip
              contentStyle={{
                background: palette.surface2,
                border: `1px solid ${palette.border}`,
                borderRadius: 6,
                fontSize: 12,
              }}
              labelFormatter={(v: string) => fmtBucketTick(v, true)}
              formatter={(value: number, name: string) => [fmtMs(value), name]}
            />
            <Legend
              wrapperStyle={{ fontSize: 11 }}
              onClick={(entry) => {
                const node = String(entry.dataKey ?? "").split("::")[0];
                setHidden((prev) => {
                  const next = new Set(prev);
                  next.has(node) ? next.delete(node) : next.add(node);
                  return next;
                });
              }}
            />
            {series.flatMap((s) =>
              hidden.has(s.node_name)
                ? []
                : QUANTILES.map((q) => (
                    <Line
                      key={`${s.node_name}::${q.key}`}
                      dataKey={`${s.node_name}::${q.key}`}
                      name={`${s.node_name} ${q.label}`}
                      stroke={slotColor(palette, colors.get(s.node_name))}
                      strokeDasharray={q.dash}
                      strokeWidth={q.key === "p99_ms" ? 2 : 1.25}
                      dot={false}
                      isAnimationActive={false}
                      connectNulls
                    />
                  )),
            )}
          </LineChart>
        </ResponsiveContainer>
      </div>
      <p className="mt-2 text-xs text-ink-faint">
        Click a legend entry to hide a node. Buckets are {BUCKET_SECONDS[win]}s.
      </p>
    </Panel>
  );
}
