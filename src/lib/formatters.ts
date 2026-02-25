export type FormatType = "decimal1" | "decimal2" | "pct1" | "rpct" | "pmi" | "integer" | "comma" | "awc" | "string";
export type HeatType = "percentile" | "zeroCentered" | "invertedPercentile";

export function formatStat(value: any, format: FormatType): string {
  if (value == null || (typeof value === "number" && isNaN(value))) return "—";

  switch (format) {
    case "decimal1":
      return Number(value).toFixed(1);
    case "decimal2":
      return Number(value).toFixed(2);
    case "pct1":
      return (Number(value) * 100).toFixed(1);
    case "rpct": {
      const v = Number(value) * 100;
      return (v >= 0 ? "+" : "") + v.toFixed(1);
    }
    case "pmi": {
      const v = Number(value);
      return (v >= 0 ? "+" : "") + v.toFixed(2);
    }
    case "integer":
      return Math.round(Number(value)).toString();
    case "comma":
      return Number(value).toLocaleString("en-US", { maximumFractionDigits: 0 });
    case "awc":
      return Number(value).toFixed(1);
    case "string":
      return String(value);
    default:
      return String(value);
  }
}

export function getHeatColor(
  value: number,
  range: { min: number; max: number; maxAbs: number },
  heatType: HeatType
): string {
  if (value == null || isNaN(value)) return "transparent";

  let normalized: number;

  switch (heatType) {
    case "percentile": {
      const span = range.max - range.min;
      if (span === 0) return "transparent";
      normalized = (value - range.min) / span;
      break;
    }
    case "invertedPercentile": {
      const span = range.max - range.min;
      if (span === 0) return "transparent";
      normalized = 1 - (value - range.min) / span;
      break;
    }
    case "zeroCentered": {
      if (range.maxAbs === 0) return "transparent";
      normalized = (value / range.maxAbs + 1) / 2;
      break;
    }
  }

  normalized = Math.max(0, Math.min(1, normalized));

  if (normalized >= 0.5) {
    const intensity = (normalized - 0.5) * 2;
    return `rgba(22, 163, 74, ${(intensity * 0.28).toFixed(3)})`;
  } else {
    const intensity = (0.5 - normalized) * 2;
    return `rgba(220, 38, 38, ${(intensity * 0.28).toFixed(3)})`;
  }
}
