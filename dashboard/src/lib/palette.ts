/**
 * Hex mirror of the CSS custom properties in `src/index.css`.
 *
 * SVG presentation attributes and Recharts props want a resolved colour, not a
 * `var()`, so the chart marks read from here while the surrounding chrome uses
 * the Tailwind tokens. One source of truth per medium, both listing the same
 * validated ramp — if you change one, change the other.
 *
 * The eight-slot categorical order is fixed and never cycled: colour follows the
 * entity (a node keeps its hue when other nodes are toggled off), and a ninth
 * series is folded into the table view rather than given a generated hue.
 */

export type ThemeName = "dark" | "light";

export interface ChartPalette {
  /** Categorical slots 1-8, in fixed order. Index 0 === slot 1. */
  series: readonly [string, string, string, string, string, string, string, string];
  surface: string;
  surface2: string;
  plane: string;
  textPrimary: string;
  textSecondary: string;
  textMuted: string;
  grid: string;
  axis: string;
  border: string;
  good: string;
  warning: string;
  serious: string;
  critical: string;
  /** Neutral ribbon colour for baseline edges in the flow diagram. */
  flowBaseline: string;
}

export const CHART_PALETTE: Record<ThemeName, ChartPalette> = {
  dark: {
    series: [
      "#3987e5",
      "#d95926",
      "#199e70",
      "#c98500",
      "#d55181",
      "#008300",
      "#9085e9",
      "#e66767",
    ],
    surface: "#1a1a19",
    surface2: "#222220",
    plane: "#0d0d0d",
    textPrimary: "#ffffff",
    textSecondary: "#c3c2b7",
    textMuted: "#898781",
    grid: "#2c2c2a",
    axis: "#383835",
    border: "#2f2f2c",
    good: "#0ca30c",
    warning: "#fab219",
    serious: "#ec835a",
    critical: "#d03b3b",
    flowBaseline: "#5a5a57",
  },
  light: {
    series: [
      "#2a78d6",
      "#eb6834",
      "#1baf7a",
      "#eda100",
      "#e87ba4",
      "#008300",
      "#4a3aa7",
      "#e34948",
    ],
    surface: "#fcfcfb",
    surface2: "#f2f1ec",
    plane: "#f9f9f7",
    textPrimary: "#0b0b0b",
    textSecondary: "#52514e",
    textMuted: "#898781",
    grid: "#e1e0d9",
    axis: "#c3c2b7",
    border: "#e4e3dd",
    good: "#0ca30c",
    warning: "#fab219",
    serious: "#ec835a",
    critical: "#d03b3b",
    flowBaseline: "#9a9992",
  },
};

/** Hard cap. Past this the tail folds into the table view — never a new hue. */
export const MAX_SERIES = 8;

/**
 * Stable entity -> colour-slot assignment. The slot comes from the entity's
 * position in the full sorted roster, so hiding a series never repaints the
 * survivors. Entities past slot 8 get no colour (they live in the table view).
 */
export function buildColorMap(entities: readonly string[]): Map<string, string | null> {
  const sorted = [...new Set(entities)].sort((a, b) => a.localeCompare(b));
  const map = new Map<string, string | null>();
  sorted.forEach((name, i) => map.set(name, i < MAX_SERIES ? `series-${i + 1}` : null));
  return map;
}

/** Resolve a slot token ("series-3") against a palette. */
export function slotColor(palette: ChartPalette, slot: string | null | undefined): string {
  if (!slot) return palette.textMuted;
  const n = Number.parseInt(slot.replace("series-", ""), 10);
  return palette.series[n - 1] ?? palette.textMuted;
}
