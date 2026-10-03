import { api, type WindowKey } from "../lib/api";
import { fmtDateTime, fmtSignedPct, fmtUsd, shortId } from "../lib/format";
import { usePolling } from "../lib/usePolling";
import { Badge } from "./ui";
import { Panel } from "./Panel";

export function AnomalyTable({
  window: win,
  paused,
  onSelectTrace,
}: {
  window: WindowKey;
  paused: boolean;
  onSelectTrace: (traceId: string) => void;
}) {
  const { data, error, loading, refetch } = usePolling(
    (signal) => api.anomalies({ window: win, limit: 25 }, signal),
    { intervalMs: 30_000, paused, key: win },
  );

  const items = data?.items ?? [];

  return (
    <Panel
      title="Cost anomalies"
      subtitle="Modified z-score (median + MAD) over a rolling per-pipeline window"
      loading={loading}
      error={error}
      onRetry={refetch}
      empty={!loading && items.length === 0}
      emptyHint="None detected. The detector stays silent until 30 samples exist."
    >
      <div className="overflow-x-auto">
        <table className="w-full min-w-[40rem] text-sm">
          <thead>
            <tr className="border-b border-edge text-left text-xs text-ink-dim">
              <th className="py-2 pr-3 font-medium">Severity</th>
              <th className="py-2 pr-3 font-medium">Trace</th>
              <th className="py-2 pr-3 text-right font-medium">Expected</th>
              <th className="py-2 pr-3 text-right font-medium">Actual</th>
              <th className="py-2 pr-3 text-right font-medium">Deviation</th>
              <th className="py-2 pr-3 text-right font-medium">z</th>
              <th className="py-2 pr-3 text-right font-medium">n</th>
              <th className="py-2 font-medium">Detected</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-edge">
            {items.map((a) => (
              <tr
                key={a.id}
                onClick={() => onSelectTrace(a.trace_id)}
                className="cursor-pointer hover:bg-surface-alt"
              >
                <td className="py-2 pr-3">
                  <Badge tone={a.severity === "critical" ? "critical" : "warning"}>
                    {a.severity}
                  </Badge>
                </td>
                <td className="py-2 pr-3 font-mono text-xs text-ink-dim">
                  {shortId(a.trace_id)}
                </td>
                <td className="py-2 pr-3 text-right tabular-nums">
                  {fmtUsd(a.expected_cost_usd)}
                </td>
                <td className="py-2 pr-3 text-right tabular-nums text-ink">
                  {fmtUsd(a.actual_cost_usd)}
                </td>
                <td className="py-2 pr-3 text-right tabular-nums text-critical">
                  {fmtSignedPct(a.deviation_pct)}
                </td>
                <td className="py-2 pr-3 text-right tabular-nums">{a.z_score.toFixed(1)}</td>
                <td className="py-2 pr-3 text-right tabular-nums text-ink-faint">
                  {a.sample_size}
                </td>
                <td className="py-2 text-xs text-ink-dim">{fmtDateTime(a.detected_at)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </Panel>
  );
}
