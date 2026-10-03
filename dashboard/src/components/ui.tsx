import type { ReactNode } from "react";

/* ------------------------------------------------------------------ tones */
/**
 * Status colours are reserved for state and always ship with an icon + label —
 * never colour alone. Class strings are written out in full so Tailwind's
 * content scanner can see them (no runtime string interpolation).
 */
export type Tone = "neutral" | "good" | "warning" | "serious" | "critical" | "accent";

interface ToneClasses {
  text: string;
  chip: string;
  fill: string;
  track: string;
}

const TONES: Record<Tone, ToneClasses> = {
  neutral: {
    text: "text-ink-secondary",
    chip: "bg-surface-3 text-ink-secondary ring-1 ring-hairline",
    fill: "bg-ink-muted",
    track: "bg-ink-muted/20",
  },
  accent: {
    text: "text-ink",
    chip: "bg-series-1/15 text-ink ring-1 ring-series-1/40",
    fill: "bg-series-1",
    track: "bg-series-1/20",
  },
  good: {
    text: "text-good",
    chip: "bg-good/15 text-good ring-1 ring-good/40",
    fill: "bg-good",
    track: "bg-good/20",
  },
  warning: {
    text: "text-warning",
    chip: "bg-warning/15 text-warning ring-1 ring-warning/40",
    fill: "bg-warning",
    track: "bg-warning/20",
  },
  serious: {
    text: "text-serious",
    chip: "bg-serious/15 text-serious ring-1 ring-serious/40",
    fill: "bg-serious",
    track: "bg-serious/20",
  },
  critical: {
    text: "text-critical",
    chip: "bg-critical/15 text-critical ring-1 ring-critical/40",
    fill: "bg-critical",
    track: "bg-critical/20",
  },
};

const TONE_GLYPH: Record<Tone, string> = {
  neutral: "•",
  accent: "•",
  good: "✓",
  warning: "▲",
  serious: "▲",
  critical: "✕",
};

/* ------------------------------------------------------------------ badge */

export function Badge({
  tone = "neutral",
  children,
  withGlyph = true,
  title,
}: {
  tone?: Tone;
  children: ReactNode;
  withGlyph?: boolean;
  title?: string;
}) {
  return (
    <span
      title={title}
      className={`inline-flex items-center gap-1 rounded px-1.5 py-0.5 text-2xs font-medium uppercase tracking-wide ${TONES[tone].chip}`}
    >
      {withGlyph && (
        <span aria-hidden="true" className="text-[0.6rem] leading-none">
          {TONE_GLYPH[tone]}
        </span>
      )}
      {children}
    </span>
  );
}

/* ------------------------------------------------------------------ button */

export function Button({
  onClick,
  children,
  active = false,
  disabled = false,
  title,
  ariaLabel,
  ariaPressed,
  className = "",
}: {
  onClick: () => void;
  children: ReactNode;
  active?: boolean;
  disabled?: boolean;
  title?: string;
  ariaLabel?: string;
  ariaPressed?: boolean;
  className?: string;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      disabled={disabled}
      title={title}
      aria-label={ariaLabel}
      aria-pressed={ariaPressed}
      className={
        `inline-flex items-center gap-1.5 rounded border px-2.5 py-1 text-xs font-medium transition-colors ` +
        `disabled:cursor-not-allowed disabled:opacity-40 ` +
        (active
          ? `border-series-1/50 bg-series-1/15 text-ink `
          : `border-hairline bg-surface-2 text-ink-secondary hover:bg-surface-3 hover:text-ink `) +
        className
      }
    >
      {children}
    </button>
  );
}

/* ------------------------------------------------- segmented control */

export function SegmentedControl<T extends string>({
  label,
  value,
  options,
  onChange,
}: {
  label: string;
  value: T;
  options: ReadonlyArray<{ value: T; label: string }>;
  onChange: (next: T) => void;
}) {
  return (
    <div
      role="group"
      aria-label={label}
      className="inline-flex overflow-hidden rounded border border-hairline bg-surface-2"
    >
      {options.map((opt) => {
        const selected = opt.value === value;
        return (
          <button
            key={opt.value}
            type="button"
            aria-pressed={selected}
            onClick={() => onChange(opt.value)}
            className={
              `px-2.5 py-1 text-xs font-medium transition-colors ` +
              (selected
                ? "bg-series-1/20 text-ink"
                : "text-ink-secondary hover:bg-surface-3 hover:text-ink")
            }
          >
            {opt.label}
          </button>
        );
      })}
    </div>
  );
}

/* ------------------------------------------------------------- stat tile */

export function StatTile({
  label,
  value,
  hint,
  tone = "neutral",
  emphasis = false,
}: {
  label: string;
  value: string;
  hint?: ReactNode;
  tone?: Tone;
  emphasis?: boolean;
}) {
  return (
    <div className="min-w-0 rounded-card border border-hairline bg-surface px-3 py-2.5">
      <div className="truncate text-2xs uppercase tracking-wide text-ink-muted">{label}</div>
      {/* Proportional figures on standalone values — tabular-nums is for columns. */}
      <div
        className={`mt-1 truncate font-semibold ${emphasis ? "text-2xl" : "text-xl"} ${
          tone === "neutral" ? "text-ink" : TONES[tone].text
        }`}
      >
        {value}
      </div>
      {hint !== undefined && (
        <div className="mt-0.5 truncate text-2xs text-ink-muted">{hint}</div>
      )}
    </div>
  );
}

/* ----------------------------------------------------------------- meter */

export function Meter({
  label,
  ratio,
  tone,
  valueText,
}: {
  label: string;
  ratio: number;
  tone: Tone;
  valueText: string;
}) {
  const pct = Math.max(0, Math.min(1, Number.isFinite(ratio) ? ratio : 0)) * 100;
  return (
    <div>
      <div className="flex items-baseline justify-between gap-2">
        <span className="text-2xs uppercase tracking-wide text-ink-muted">{label}</span>
        <span className={`num text-2xs ${TONES[tone].text}`}>{valueText}</span>
      </div>
      <div
        role="meter"
        aria-label={label}
        aria-valuenow={Math.round(pct)}
        aria-valuemin={0}
        aria-valuemax={100}
        aria-valuetext={valueText}
        className={`mt-1 h-1.5 w-full overflow-hidden rounded-full ${TONES[tone].track}`}
      >
        <div className={`h-full rounded-full ${TONES[tone].fill}`} style={{ width: `${pct}%` }} />
      </div>
    </div>
  );
}

/* -------------------------------------------------------------- key swatch */

/** Legend key. Lines get a stroke, fills get a rect — the key mirrors the mark. */
export function SeriesKey({ color, shape = "line" }: { color: string; shape?: "line" | "rect" }) {
  return shape === "line" ? (
    <span
      aria-hidden="true"
      className="inline-block h-0.5 w-3.5 shrink-0 rounded-full"
      style={{ backgroundColor: color }}
    />
  ) : (
    <span
      aria-hidden="true"
      className="inline-block h-2.5 w-2.5 shrink-0 rounded-sm"
      style={{ backgroundColor: color }}
    />
  );
}

/* ----------------------------------------------------------------- misc */

export function NodeChips({ path }: { path: readonly string[] }) {
  if (path.length === 0) return <span className="text-ink-muted">—</span>;
  return (
    <span className="inline-flex flex-wrap items-center gap-x-1 gap-y-0.5">
      {path.map((node, i) => (
        <span key={`${node}-${i}`} className="inline-flex items-center gap-1">
          {i > 0 && (
            <span aria-hidden="true" className="text-ink-muted">
              ›
            </span>
          )}
          <span className="rounded bg-surface-3 px-1 py-px font-mono text-2xs text-ink-secondary">
            {node}
          </span>
        </span>
      ))}
    </span>
  );
}

export { TONES };
