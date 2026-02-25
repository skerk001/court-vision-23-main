import type { FormatType, HeatType } from "./formatters";

export interface ColumnDef {
  key: string;
  label: string;
  format: FormatType;
  heatType?: HeatType;
}

export interface ColumnGroup {
  label: string;
  colorClass: string;
  columns: ColumnDef[];
}

export const PERGAME_COLUMNS: ColumnGroup[] = [
  {
    label: "Box Score",
    colorClass: "bg-indigo-500/20 text-indigo-300",
    columns: [
      { key: "gp", label: "GP", format: "integer" },
      { key: "ppg", label: "PPG", format: "decimal1" },
      { key: "rpg", label: "RPG", format: "decimal1" },
      { key: "apg", label: "APG", format: "decimal1" },
      { key: "spg", label: "SPG", format: "decimal1" },
      { key: "bpg", label: "BPG", format: "decimal1" },
      { key: "tov", label: "TOV", format: "decimal1", heatType: "invertedPercentile" },
    ],
  },
  {
    label: "Shooting",
    colorClass: "bg-emerald-500/20 text-emerald-300",
    columns: [
      { key: "fg_pct", label: "FG%", format: "pct1", heatType: "percentile" },
      { key: "ts_pct", label: "TS%", format: "pct1", heatType: "percentile" },
      { key: "rts_pct", label: "rTS%", format: "rpct", heatType: "zeroCentered" },
    ],
  },
  {
    label: "PMI",
    colorClass: "bg-amber-500/20 text-amber-300",
    columns: [
      { key: "pmi", label: "PMI", format: "pmi", heatType: "zeroCentered" },
      { key: "opmi", label: "OPMI", format: "pmi", heatType: "zeroCentered" },
      { key: "dpmi", label: "DPMI", format: "pmi", heatType: "zeroCentered" },
      { key: "peak_pmi", label: "Best Szn", format: "pmi", heatType: "zeroCentered" },
      { key: "peak_season", label: "Best Yr", format: "string" },
    ],
  },
];

export const TOTALS_COLUMNS: ColumnGroup[] = [
  {
    label: "Counting Stats",
    colorClass: "bg-indigo-500/20 text-indigo-300",
    columns: [
      { key: "gp", label: "GP", format: "integer" },
      { key: "min", label: "MIN", format: "comma" },
      { key: "pts", label: "PTS", format: "comma" },
      { key: "reb", label: "REB", format: "comma" },
      { key: "ast", label: "AST", format: "comma" },
      { key: "stl", label: "STL", format: "comma" },
      { key: "blk", label: "BLK", format: "comma" },
      { key: "total_tov", label: "TOV", format: "comma" },
    ],
  },
  {
    label: "Shooting",
    colorClass: "bg-emerald-500/20 text-emerald-300",
    columns: [
      { key: "fg_pct", label: "FG%", format: "pct1", heatType: "percentile" },
      { key: "ts_pct", label: "TS%", format: "pct1", heatType: "percentile" },
      { key: "rts_pct", label: "rTS%", format: "rpct", heatType: "zeroCentered" },
    ],
  },
  {
    label: "Cumulative",
    colorClass: "bg-amber-500/20 text-amber-300",
    columns: [
      { key: "awc", label: "AWC", format: "awc" },
      { key: "oawc", label: "OAWC", format: "awc" },
      { key: "dawc", label: "DAWC", format: "awc" },
    ],
  },
];

export const CLUTCH_COLUMNS: ColumnGroup[] = [
  {
    label: "CPMI",
    colorClass: "bg-amber-500/20 text-amber-300",
    columns: [
      { key: "cpmi", label: "CPMI", format: "pmi", heatType: "zeroCentered" },
      { key: "clutch_gp", label: "GP", format: "integer" },
      { key: "clutch_w_pct", label: "W%", format: "pct1", heatType: "percentile" },
    ],
  },
  {
    label: "Clutch Stats (Last 5 Min, ±5 Pts)",
    colorClass: "bg-red-500/20 text-red-300",
    columns: [
      { key: "clutch_pts", label: "PPG", format: "decimal1" },
      { key: "clutch_ast", label: "APG", format: "decimal1" },
      { key: "clutch_reb", label: "RPG", format: "decimal1" },
      { key: "clutch_stl", label: "SPG", format: "decimal1" },
      { key: "clutch_fg_pct", label: "FG%", format: "pct1", heatType: "percentile" },
      { key: "clutch_plus_minus", label: "+/−", format: "pmi", heatType: "zeroCentered" },
    ],
  },
];

export const BEST_SEASON_COLUMNS: ColumnGroup[] = [
  {
    label: "PMI",
    colorClass: "bg-amber-500/20 text-amber-300",
    columns: [
      { key: "best_season", label: "Season", format: "string" },
      { key: "gp", label: "GP", format: "integer" },
      { key: "best_pmi", label: "PMI", format: "pmi", heatType: "zeroCentered" },
      { key: "best_opmi", label: "OPMI", format: "pmi", heatType: "zeroCentered" },
      { key: "dpmi", label: "DPMI", format: "pmi", heatType: "zeroCentered" },
      { key: "best_awc", label: "AWC", format: "awc" },
    ],
  },
  {
    label: "Box Score",
    colorClass: "bg-indigo-500/20 text-indigo-300",
    columns: [
      { key: "ppg", label: "PPG", format: "decimal1" },
      { key: "rpg", label: "RPG", format: "decimal1" },
      { key: "apg", label: "APG", format: "decimal1" },
      { key: "spg", label: "SPG", format: "decimal1" },
      { key: "bpg", label: "BPG", format: "decimal1" },
    ],
  },
  {
    label: "Shooting",
    colorClass: "bg-emerald-500/20 text-emerald-300",
    columns: [
      { key: "fg_pct", label: "FG%", format: "pct1", heatType: "percentile" },
      { key: "ts_pct", label: "TS%", format: "pct1", heatType: "percentile" },
    ],
  },
];

export const PMI_CAREER_COLUMNS: ColumnGroup[] = [
  {
    label: "PMI Ratings",
    colorClass: "bg-amber-500/20 text-amber-300",
    columns: [
      { key: "gp", label: "GP", format: "integer" },
      { key: "pmi", label: "PMI", format: "pmi", heatType: "zeroCentered" },
      { key: "opmi", label: "OPMI", format: "pmi", heatType: "zeroCentered" },
      { key: "dpmi", label: "DPMI", format: "pmi", heatType: "zeroCentered" },
      { key: "peak_pmi", label: "Best Szn", format: "pmi", heatType: "zeroCentered" },
    ],
  },
  {
    label: "Box Score",
    colorClass: "bg-indigo-500/20 text-indigo-300",
    columns: [
      { key: "ppg", label: "PPG", format: "decimal1" },
      { key: "rpg", label: "RPG", format: "decimal1" },
      { key: "apg", label: "APG", format: "decimal1" },
      { key: "spg", label: "SPG", format: "decimal1" },
      { key: "bpg", label: "BPG", format: "decimal1" },
    ],
  },
  {
    label: "Career",
    colorClass: "bg-amber-500/20 text-amber-300",
    columns: [
      { key: "awc", label: "AWC", format: "awc" },
      { key: "years", label: "Years", format: "string" },
    ],
  },
];

export const TAB_COLUMNS: Record<string, ColumnGroup[]> = {
  pergame: PERGAME_COLUMNS,
  totals: TOTALS_COLUMNS,
  clutch: CLUTCH_COLUMNS,
  best_season: BEST_SEASON_COLUMNS,
  pmi: PMI_CAREER_COLUMNS,
};

export const TABS = [
  { key: "pergame", label: "Per Game", isPmi: false },
  { key: "totals", label: "Totals", isPmi: false },
  { key: "clutch", label: "Clutch", isPmi: true },
  { key: "best_season", label: "Best Season", isPmi: true },
  { key: "pmi", label: "PMI Career", isPmi: true },
];

export const DEFAULT_SORT: Record<string, string> = {
  pergame: "ppg",
  totals: "pts",
  clutch: "cpmi",
  best_season: "best_pmi",
  pmi: "pmi",
};
