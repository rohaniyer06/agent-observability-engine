import { useMemo } from "react";

import { api, type WindowKey } from "../lib/api";
import { fmtInt, fmtRatioPct } from "../lib/format";
import { usePalette } from "../lib/theme";
import { usePolling } from "../lib/usePolling";
import { Badge, NodeChips } from "./ui";
import { Panel } from "./Panel";

const NODE_W = 96;
const NODE_H = 30;
const COL_GAP = 72;
const ROW_GAP = 14;

/**
 * Hand-rolled SVG Sankey. Recharts has no Sankey, and pulling in a second
 * charting library for one diagram is a poor trade — the layout here is a
 * column per pipeline depth, which is exactly the shape of an agent path.
 */
export function PathFlow({ window: win, paused }: { window: WindowKey; paused: boolean }) {
  const palette = usePalette();
  const flow = usePolling((signal) => api.driftFlow({ window: win }, signal), {
    intervalMs: 30_000,
    paused,
    key: win,
  });
  const drift = usePolling((signal) => api.drift({ limit: 20 }, signal), {
    intervalMs: 30_000,
    paused,
  });

  const layout = useMemo(() => {
    const edges = flow.data?.edges ?? [];
    if (edges.length === 0) return null;

    // Depth = longest path from __start__, so a node that can be reached late
    // (a retry, an escalation) sits in its latest column rather than its first.
    const depth = new Map<string, number>([["__start__", 0]]);
    for (let pass = 0; pass < 12; pass++) {
      let changed = false;
      for (const e of edges) {
        const d = (depth.get(e.source) ?? 0) + 1;
        if (d > (depth.get(e.target) ?? -1)) {
          depth.set(e.target, d);
          changed = true;
        }
      }
      if (!changed) break;
    }

    const columns = new Map<number, string[]>();
    for (const [node, d] of depth) {
      const col = columns.get(d) ?? [];
      col.push(node);
      columns.set(d, col);
    }

    const pos = new Map<string, { x: number; y: number }>();
    let maxRows = 0;
    for (const [d, nodes] of [...columns].sort((a, b) => a[0] - b[0])) {
      nodes.sort();
      maxRows = Math.max(maxRows, nodes.length);
      nodes.forEach((n, i) => {
        pos.set(n, { x: d * (NODE_W + COL_GAP), y: i * (NODE_H + ROW_GAP) });
      });
    }

    const maxValue = Math.max(...edges.map((e) => e.value), 1);
    const width = (Math.max(...depth.values()) + 1) * (NODE_W + COL_GAP);
    const height = maxRows * (NODE_H + ROW_GAP);
    return { edges, pos, maxValue, width, height };
  }, [flow.data]);

  const paths = drift.data?.paths ?? [];

  return (
    <Panel
      title="Path drift"
      subtitle="Dominant path vs. drift branches"
      right={
        drift.data ? (
          <span className="text-xs text-ink-faint">
            {fmtInt(drift.data.total_traces)} traces
          </span>
        ) : null
      }
      loading={flow.loading || drift.loading}
      error={flow.error ?? drift.error}
      onRetry={() => {
        flow.refetch();
        drift.refetch();
      }}
      empty={!flow.loading && layout === null}
      emptyHint="No paths recorded yet. Run `make harness`."
    >
      {layout ? (
        <div className="overflow-x-auto">
          <svg
            width={layout.width}
            height={layout.height + 8}
            role="img"
            aria-label="Agent path flow diagram"
            className="min-w-full"
          >
            {layout.edges.map((e, i) => {
              const a = layout.pos.get(e.source);
              const b = layout.pos.get(e.target);
              if (!a || !b) return null;
              const x1 = a.x + NODE_W;
              const y1 = a.y + NODE_H / 2;
              const x2 = b.x;
              const y2 = b.y + NODE_H / 2;
              const mid = (x1 + x2) / 2;
              // Ribbon width encodes volume; drift branches are the ones that
              // need to stand out, so baseline edges stay neutral.
              const w = Math.max(1.5, (e.value / layout.maxValue) * 14);
              return (
                <path
                  key={i}
                  d={`M ${x1} ${y1} C ${mid} ${y1}, ${mid} ${y2}, ${x2} ${y2}`}
                  fill="none"
                  stroke={e.is_baseline ? palette.flowBaseline : palette.warning}
                  strokeWidth={w}
                  strokeOpacity={e.is_baseline ? 0.5 : 0.85}
                >
                  <title>{`${e.source} → ${e.target}: ${e.value}`}</title>
                </path>
              );
            })}
            {[...layout.pos].map(([node, p]) => (
              <g key={node}>
                <rect
                  x={p.x}
                  y={p.y}
                  width={NODE_W}
                  height={NODE_H}
                  rx={5}
                  fill={palette.surface2}
                  stroke={palette.border}
                />
                <text
                  x={p.x + NODE_W / 2}
                  y={p.y + NODE_H / 2 + 4}
                  textAnchor="middle"
                  fontSize={11}
                  fill={node === "__start__" ? palette.textMuted : palette.textPrimary}
                >
                  {node === "__start__" ? "start" : node}
                </text>
              </g>
            ))}
          </svg>
        </div>
      ) : null}

      {paths.length > 0 ? (
        <div className="mt-4 overflow-x-auto">
          <table className="w-full min-w-[32rem] text-sm">
            <thead>
              <tr className="border-b border-edge text-left text-xs text-ink-dim">
                <th className="py-2 pr-3 font-medium">Path</th>
                <th className="py-2 pr-3 text-right font-medium">Traces</th>
                <th className="py-2 pr-3 text-right font-medium">Share</th>
                <th className="py-2 font-medium">Classification</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-edge">
              {paths.map((p) => (
                <tr key={p.path_signature}>
                  <td className="py-2 pr-3">
                    <NodeChips path={p.path} />
                  </td>
                  <td className="py-2 pr-3 text-right tabular-nums">
                    {fmtInt(p.occurrences)}
                  </td>
                  <td className="py-2 pr-3 text-right tabular-nums text-ink-dim">
                    {fmtRatioPct(p.share)}
                  </td>
                  <td className="py-2">
                    {p.is_seeded ? (
                      <Badge tone="neutral">seeded</Badge>
                    ) : p.is_baseline ? (
                      <Badge tone="good">baseline</Badge>
                    ) : (
                      <Badge tone="warning">drift</Badge>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : null}
    </Panel>
  );
}
