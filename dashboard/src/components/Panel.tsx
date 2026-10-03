import type { ReactNode } from "react";

import { Button } from "./ui";

/**
 * Every panel renders one of four states. Getting this wrong is how a dashboard
 * greets a reviewer with a permanent spinner on a system that is simply empty —
 * the most likely first impression, since a fresh database has no traces.
 */
export function Panel({
  title,
  subtitle,
  right,
  loading,
  error,
  empty,
  emptyHint,
  onRetry,
  children,
  className = "",
}: {
  title: string;
  subtitle?: ReactNode;
  right?: ReactNode;
  loading?: boolean;
  error?: string | null;
  empty?: boolean;
  emptyHint?: ReactNode;
  onRetry?: () => void;
  children: ReactNode;
  className?: string;
}) {
  return (
    <section
      className={`flex min-w-0 flex-col rounded-lg border border-edge bg-surface ${className}`}
      aria-busy={loading || undefined}
    >
      <header className="flex items-start justify-between gap-3 border-b border-edge px-4 py-3">
        <div className="min-w-0">
          <h2 className="text-sm font-semibold text-ink">{title}</h2>
          {subtitle ? <p className="mt-0.5 text-xs text-ink-dim">{subtitle}</p> : null}
        </div>
        {right ? <div className="shrink-0">{right}</div> : null}
      </header>

      <div className="min-w-0 flex-1 p-4">
        {error ? (
          <div className="flex flex-col items-start gap-2 py-6">
            <p className="text-sm text-critical">{error}</p>
            {onRetry ? (
              <Button onClick={onRetry}>
                Retry
              </Button>
            ) : null}
          </div>
        ) : empty ? (
          <div className="py-8 text-center">
            <p className="text-sm text-ink-dim">No data yet</p>
            {emptyHint ? <p className="mt-1 text-xs text-ink-faint">{emptyHint}</p> : null}
          </div>
        ) : (
          // Previous data is held at reduced opacity during a refetch rather
          // than being replaced by a skeleton, so the layout never jumps.
          <div className={loading ? "opacity-50 transition-opacity" : "transition-opacity"}>
            {children}
          </div>
        )}
      </div>
    </section>
  );
}
