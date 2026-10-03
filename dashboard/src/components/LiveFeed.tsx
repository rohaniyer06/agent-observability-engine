import { fmtMs, fmtRelative, fmtUsd, shortId } from "../lib/format";
import { useLiveFeed } from "../lib/useLiveFeed";
import { Badge, Button, NodeChips } from "./ui";
import { Panel } from "./Panel";

const STATUS_TONE = {
  open: "good",
  connecting: "warning",
  reconnecting: "warning",
  closed: "critical",
} as const;

export function LiveFeed({
  frozen,
  onSelectTrace,
}: {
  frozen: boolean;
  onSelectTrace: (traceId: string) => void;
}) {
  const feed = useLiveFeed({ maxRows: 100, frozen });

  return (
    <Panel
      title="Live feed"
      subtitle="Finalized traces, pushed over WebSocket"
      right={
        <div className="flex items-center gap-2">
          {/* A dashboard that has silently disconnected is worse than one that
              says so — the rows just stop and everything looks calm. */}
          <Badge tone={STATUS_TONE[feed.status]}>{feed.status}</Badge>
          {feed.status !== "open" ? (
            <Button onClick={feed.reconnectNow}>
              Reconnect
            </Button>
          ) : null}
        </div>
      }
      empty={feed.rows.length === 0}
      emptyHint={
        feed.status === "open"
          ? "Connected and waiting. Run `make harness` to produce traces."
          : "Waiting for the query API on :8001."
      }
    >
      <div className="max-h-[26rem] overflow-y-auto">
        <ul className="divide-y divide-edge">
          {feed.rows.map((row) => {
            const e = row.event;
            return (
              <li key={row.seq}>
                <button
                  type="button"
                  onClick={() => onSelectTrace(e.trace_id)}
                  className="flex w-full items-center gap-3 px-1 py-2 text-left hover:bg-surface-alt focus:bg-surface-alt focus:outline-none"
                  aria-label={`Open trace ${e.trace_id}`}
                >
                  <Badge
                    tone={
                      e.type === "cost_anomaly"
                        ? "critical"
                        : e.type === "path_drift"
                          ? "warning"
                          : e.status === "error"
                            ? "serious"
                            : e.status === "partial"
                              ? "warning"
                              : "good"
                    }
                  >
                    {e.type === "trace_finalized" ? e.status : e.type.replace("_", " ")}
                  </Badge>
                  <span className="w-24 shrink-0 font-mono text-xs text-ink-dim">
                    {shortId(e.trace_id)}
                  </span>
                  <span className="min-w-0 flex-1 overflow-hidden">
                    <NodeChips path={e.path} />
                  </span>
                  <span className="w-16 shrink-0 text-right text-xs tabular-nums text-ink-dim">
                    {fmtMs(e.duration_ms)}
                  </span>
                  <span className="w-20 shrink-0 text-right text-xs tabular-nums text-ink-dim">
                    {fmtUsd(e.total_cost_usd)}
                  </span>
                  <span className="w-14 shrink-0 text-right text-xs text-ink-faint">
                    {fmtRelative(row.receivedAt)}
                  </span>
                </button>
              </li>
            );
          })}
        </ul>
      </div>
      <p className="mt-2 text-xs text-ink-faint">
        {feed.totalReceived} received this session · showing the most recent {feed.rows.length}
      </p>
    </Panel>
  );
}
