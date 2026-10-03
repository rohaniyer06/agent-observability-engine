import { api } from "../lib/api";
import { fmtCompact, fmtInt, fmtRelative } from "../lib/format";
import { usePolling } from "../lib/usePolling";
import { Badge, StatTile } from "./ui";

/**
 * Proof that the ingestion path is real and healthy, given prominence rather
 * than tucked in a footer.
 *
 * `stream_depth` here is the consumer group's BACKLOG (lag + pending), not
 * XLEN. XACK does not shorten a stream, so XLEN would climb forever on a
 * perfectly drained system — see DEVIATIONS.md bug A.
 */
export function SystemStrip({ paused }: { paused: boolean }) {
  const { data, error, lastUpdated } = usePolling((signal) => api.systemStats(signal), {
    intervalMs: 5_000,
    paused,
  });

  const backlog = data?.stream_depth ?? 0;
  const threshold = data?.backpressure_threshold ?? 0;
  const ratio = threshold > 0 ? backlog / threshold : 0;
  const tone = ratio >= 1 ? "critical" : ratio >= 0.5 ? "warning" : "good";

  return (
    <section
      aria-label="Pipeline health"
      className="rounded-lg border border-edge bg-surface p-4"
    >
      <div className="mb-3 flex items-center justify-between gap-3">
        <h2 className="text-sm font-semibold text-ink">Pipeline health</h2>
        <div className="flex items-center gap-2 text-xs text-ink-faint">
          {error ? (
            <Badge tone="critical">query API unreachable</Badge>
          ) : (
            <Badge tone={tone}>
              {ratio >= 1 ? "shedding load" : ratio >= 0.5 ? "backlog building" : "healthy"}
            </Badge>
          )}
          <span>{lastUpdated ? fmtRelative(lastUpdated) : "—"}</span>
        </div>
      </div>

      {error ? (
        <p className="text-sm text-critical">{error}</p>
      ) : (
        <div className="grid grid-cols-2 gap-3 sm:grid-cols-3 lg:grid-cols-6">
          <StatTile
            label="Backlog"
            value={fmtCompact(backlog)}
            hint={threshold ? `shed at ${fmtCompact(threshold)}` : undefined}
            tone={tone}
          />
          <StatTile label="Pending (unacked)" value={fmtInt(data?.pending_entries)} />
          <StatTile label="Consumers" value={fmtInt(data?.consumer_count)} />
          <StatTile label="Spans processed" value={fmtCompact(data?.spans_processed)} />
          <StatTile
            label="Duplicates suppressed"
            value={fmtCompact(data?.spans_duplicate)}
            hint="at-least-once redelivery"
          />
          <StatTile
            label="Traces finalized"
            value={fmtCompact(data?.traces_finalized)}
            hint={`${fmtInt(data?.traces_finalized_by_timeout)} by timeout`}
          />
        </div>
      )}

      {data && data.unknown_models.length > 0 ? (
        <p className="mt-3 text-xs text-warning">
          {data.unknown_models.length} model(s) seen with no pricing entry — their cost is
          recorded as $0: {data.unknown_models.join(", ")}
        </p>
      ) : null}
    </section>
  );
}
