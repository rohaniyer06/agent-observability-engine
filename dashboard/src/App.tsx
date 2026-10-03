import { useState } from "react";

import { api, API_BASE, WINDOW_OPTIONS, type WindowKey } from "./lib/api";
import { fmtInt, fmtMs, fmtRate, fmtRatioPct, fmtUsd } from "./lib/format";
import { useTheme } from "./lib/theme";
import { usePolling } from "./lib/usePolling";
import { AnomalyTable } from "./components/AnomalyTable";
import { LatencyChart } from "./components/LatencyChart";
import { LiveFeed } from "./components/LiveFeed";
import { PathFlow } from "./components/PathFlow";
import { SystemStrip } from "./components/SystemStrip";
import { TraceDrawer } from "./components/TraceDrawer";
import { Button, SegmentedControl, StatTile } from "./components/ui";

export default function App() {
  const [win, setWin] = useState<WindowKey>("1h");
  const [paused, setPaused] = useState(false);
  const [traceId, setTraceId] = useState<string | null>(null);
  const { theme, toggleTheme } = useTheme();

  const summary = usePolling((signal) => api.summary({ window: win }, signal), {
    intervalMs: 5_000,
    paused,
    key: win,
  });
  const pricing = usePolling((signal) => api.pricing(signal), { intervalMs: 300_000, paused });

  const s = summary.data;

  return (
    <div className="min-h-screen bg-canvas text-ink">
      <header className="border-b border-edge bg-surface">
        <div className="mx-auto flex max-w-[100rem] flex-wrap items-center gap-3 px-4 py-3">
          <div className="mr-auto min-w-0">
            <h1 className="text-base font-semibold">Agent Observability Engine</h1>
            <p className="truncate text-xs text-ink-faint">{API_BASE}</p>
          </div>

          <SegmentedControl<WindowKey>
            label="Time window"
            value={win}
            options={WINDOW_OPTIONS}
            onChange={setWin}
          />
          {/* Polling is pausable on purpose: a "real-time" dashboard that
              refetches everything on a tight loop becomes its own load test. */}
          <Button onClick={() => setPaused((p) => !p)} active={paused} ariaPressed={paused}>
            {paused ? "Resume" : "Pause"}
          </Button>
          <Button onClick={toggleTheme} ariaLabel="Toggle colour theme">
            {theme === "dark" ? "Light" : "Dark"}
          </Button>
        </div>
      </header>

      <main className="mx-auto max-w-[100rem] space-y-4 p-4">
        <SystemStrip paused={paused} />

        <section
          aria-label="Summary"
          className="grid grid-cols-2 gap-3 sm:grid-cols-3 lg:grid-cols-6"
        >
          <StatTile label="Traces" value={fmtInt(s?.trace_count)} emphasis />
          <StatTile label="Traces/sec" value={fmtRate(s?.traces_per_second, 2)} />
          <StatTile
            label="Error rate"
            value={fmtRatioPct(s?.error_rate)}
            tone={(s?.error_rate ?? 0) > 0.1 ? "warning" : "neutral"}
          />
          <StatTile label="p99" value={fmtMs(s?.p99_ms)} hint={`p50 ${fmtMs(s?.p50_ms)}`} />
          <StatTile label="Cost" value={fmtUsd(s?.total_cost_usd)} />
          <StatTile
            label="Anomalies"
            value={fmtInt(s?.open_anomalies)}
            hint={`${fmtInt(s?.drift_events)} drift`}
            tone={(s?.open_anomalies ?? 0) > 0 ? "warning" : "neutral"}
          />
        </section>

        <LatencyChart window={win} paused={paused} />

        <div className="grid grid-cols-1 gap-4 xl:grid-cols-2">
          <LiveFeed frozen={paused} onSelectTrace={setTraceId} />
          <AnomalyTable window={win} paused={paused} onSelectTrace={setTraceId} />
        </div>

        <PathFlow window={win} paused={paused} />
      </main>

      <footer className="mx-auto max-w-[100rem] px-4 pb-8 text-xs text-ink-faint">
        {pricing.data ? (
          <p>
            Cost figures use pricing table v{pricing.data.version}, snapshot{" "}
            {pricing.data.as_of} ({pricing.data.currency}) —{" "}
            <a
              href={pricing.data.source}
              className="underline hover:text-ink-dim"
              target="_blank"
              rel="noreferrer"
            >
              source
            </a>
            . Manually refreshed; not fetched at runtime.
            {pricing.data.unknown_models_seen.length > 0
              ? ` Unpriced models seen: ${pricing.data.unknown_models_seen.join(", ")}.`
              : ""}
          </p>
        ) : (
          <p>Cost figures come from a static, versioned pricing table.</p>
        )}
      </footer>

      {traceId ? <TraceDrawer traceId={traceId} onClose={() => setTraceId(null)} /> : null}
    </div>
  );
}
