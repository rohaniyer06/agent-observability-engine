import { useEffect, useState } from "react";

import { api, describeError } from "../lib/api";
import { fmtDateTime, fmtInt, fmtMs, fmtUsd } from "../lib/format";
import { buildColorMap, slotColor } from "../lib/palette";
import { usePalette } from "../lib/theme";
import type { TraceDetail } from "../types/api";
import { Badge, Button } from "./ui";

/** Span waterfall + per-span detail for one trace. */
export function TraceDrawer({ traceId, onClose }: { traceId: string; onClose: () => void }) {
  const palette = usePalette();
  const [detail, setDetail] = useState<TraceDetail | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const controller = new AbortController();
    setDetail(null);
    setError(null);
    api
      .trace(traceId, controller.signal)
      .then(setDetail)
      .catch((err) => {
        if (!controller.signal.aborted) setError(describeError(err));
      });
    return () => controller.abort();
  }, [traceId]);

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => e.key === "Escape" && onClose();
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose]);

  const spans = detail?.spans ?? [];
  const t0 = spans.length ? Math.min(...spans.map((s) => s.start_time_ns)) : 0;
  const t1 = spans.length ? Math.max(...spans.map((s) => s.end_time_ns)) : 0;
  const span = Math.max(1, t1 - t0);
  const colors = buildColorMap([...new Set(spans.map((s) => s.node_name))]);

  return (
    <div className="fixed inset-0 z-50 flex justify-end bg-black/50" onClick={onClose}>
      <aside
        role="dialog"
        aria-label="Trace detail"
        className="flex h-full w-full max-w-2xl flex-col overflow-y-auto border-l border-edge bg-surface"
        onClick={(e) => e.stopPropagation()}
      >
        <header className="sticky top-0 flex items-start justify-between gap-3 border-b border-edge bg-surface px-4 py-3">
          <div className="min-w-0">
            <h2 className="text-sm font-semibold text-ink">Trace</h2>
            <p className="truncate font-mono text-xs text-ink-dim">{traceId}</p>
          </div>
          <Button onClick={onClose} ariaLabel="Close trace detail">
            Close
          </Button>
        </header>

        <div className="flex-1 p-4">
          {error ? (
            <p className="text-sm text-critical">{error}</p>
          ) : !detail ? (
            <p className="text-sm text-ink-dim">Loading…</p>
          ) : (
            <>
              <dl className="mb-4 grid grid-cols-2 gap-3 text-sm sm:grid-cols-4">
                {[
                  ["Status", detail.trace.status],
                  ["Duration", fmtMs(detail.trace.duration_ms)],
                  ["Cost", fmtUsd(detail.trace.total_cost_usd)],
                  ["Spans", fmtInt(detail.trace.span_count)],
                  ["Tokens in", fmtInt(detail.trace.total_input_tokens)],
                  ["Tokens out", fmtInt(detail.trace.total_output_tokens)],
                  ["Finalized by", detail.trace.finalized_by],
                  ["Started", fmtDateTime(detail.trace.started_at)],
                ].map(([k, v]) => (
                  <div key={k}>
                    <dt className="text-2xs uppercase tracking-wide text-ink-muted">{k}</dt>
                    <dd className="mt-0.5 truncate text-ink">{v}</dd>
                  </div>
                ))}
              </dl>

              <h3 className="mb-2 text-xs font-semibold uppercase tracking-wide text-ink-dim">
                Span waterfall
              </h3>
              <ul className="space-y-1.5">
                {spans.map((s) => {
                  const left = ((s.start_time_ns - t0) / span) * 100;
                  const width = Math.max(0.6, ((s.end_time_ns - s.start_time_ns) / span) * 100);
                  return (
                    <li key={s.span_id} className="text-xs">
                      <div className="mb-0.5 flex items-center justify-between gap-2">
                        <span className="flex items-center gap-1.5 truncate">
                          <span className="font-medium text-ink">{s.node_name}</span>
                          <span className="text-ink-faint">{s.operation_name}</span>
                          {s.status !== "ok" ? (
                            <Badge tone="serious">{s.status}</Badge>
                          ) : null}
                        </span>
                        <span className="shrink-0 tabular-nums text-ink-dim">
                          {fmtMs(s.duration_ms)} · {fmtUsd(s.cost_usd)}
                        </span>
                      </div>
                      <div className="h-2.5 w-full rounded bg-surface-alt">
                        <div
                          className="h-2.5 rounded"
                          style={{
                            marginLeft: `${left}%`,
                            width: `${width}%`,
                            background: slotColor(palette, colors.get(s.node_name)),
                          }}
                          title={`${s.node_name}: ${fmtMs(s.duration_ms)}`}
                        />
                      </div>
                      {s.error_message ? (
                        <p className="mt-0.5 text-2xs text-critical">{s.error_message}</p>
                      ) : null}
                    </li>
                  );
                })}
              </ul>
            </>
          )}
        </div>
      </aside>
    </div>
  );
}
