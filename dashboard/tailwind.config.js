/**
 * Every colour is a CSS custom property holding *space-separated RGB channels*,
 * so Tailwind's opacity modifiers (`bg-warning/10`) still work. The channel
 * values live in `src/index.css` (`:root` = dark, `[data-theme="light"]` =
 * light) and are mirrored as literal hex in `src/lib/palette.ts` for the SVG /
 * Recharts marks, which need a resolved colour rather than a var().
 */
const channel = (name) => `rgb(var(${name}) / <alpha-value>)`;

/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  theme: {
    extend: {
      colors: {
        plane: channel("--plane"),
        surface: {
          DEFAULT: channel("--surface-1"),
          2: channel("--surface-2"),
          3: channel("--surface-3"),
        },
        ink: {
          DEFAULT: channel("--text-primary"),
          secondary: channel("--text-secondary"),
          muted: channel("--text-muted"),
        },
        hairline: channel("--border"),
        grid: channel("--grid"),
        axis: channel("--axis"),
        good: channel("--status-good"),
        warning: channel("--status-warning"),
        serious: channel("--status-serious"),
        critical: channel("--status-critical"),
        series: {
          1: channel("--series-1"),
          2: channel("--series-2"),
          3: channel("--series-3"),
          4: channel("--series-4"),
          5: channel("--series-5"),
          6: channel("--series-6"),
          7: channel("--series-7"),
          8: channel("--series-8"),
        },
      },
      fontFamily: {
        sans: [
          "system-ui",
          "-apple-system",
          "Segoe UI",
          "Roboto",
          "Helvetica Neue",
          "Arial",
          "sans-serif",
        ],
        mono: ["ui-monospace", "SFMono-Regular", "Menlo", "Consolas", "monospace"],
      },
      fontSize: {
        "2xs": ["0.6875rem", { lineHeight: "1rem" }],
      },
      borderRadius: {
        card: "0.625rem",
      },
      keyframes: {
        "fade-in": {
          from: { opacity: "0", transform: "translateY(-4px)" },
          to: { opacity: "1", transform: "none" },
        },
        "slide-in": {
          from: { transform: "translateX(100%)" },
          to: { transform: "none" },
        },
      },
      animation: {
        "fade-in": "fade-in 180ms ease-out",
        "slide-in": "slide-in 200ms ease-out",
      },
    },
  },
  plugins: [],
};
