import { useState, useEffect, useMemo } from "react";
import { useParams } from "react-router-dom";
import { motion } from "framer-motion";
import {
  AreaChart, Area, LineChart, Line,
  XAxis, YAxis, CartesianGrid, Tooltip,
  ResponsiveContainer, ReferenceLine,
} from "recharts";
import {
  MOCK_PLAYERS, MOCK_PLAYERS_PLAYOFFS,
  SEASON_DATA_REGULAR, SEASON_DATA_PLAYOFFS,
} from "@/lib/mockData";
import type { SeasonData } from "@/lib/mockData";
import PlayerAvatar from "@/components/shared/PlayerAvatar";
import { formatStat } from "@/lib/formatters";
import {
  TrendingUp, Award, Shield, Flame, Trophy,
  ChevronUp, ChevronDown, ArrowUpDown, Loader2,
} from "lucide-react";

type SortDir = "asc" | "desc";
interface SortConfig { key: keyof SeasonData; dir: SortDir }

const API_BASE = import.meta.env.VITE_API_URL || "http://localhost:8000/api/v1";

const CHART_COLORS = {
  pmi: "#5865F2",
  opmi: "#FBBF24",
  dpmi: "#34D399",
  ppg: "#F472B6",
  rpg: "#60A5FA",
  apg: "#A78BFA",
  awc: "#FB923C",
};

const TOOLTIP_STYLE = {
  backgroundColor: "hsl(220 8% 10%)",
  border: "1px solid hsl(220 7% 25%)",
  borderRadius: "8px",
  fontSize: 13,
  fontWeight: 500,
};

const StatPill = ({
  label, value, sub, icon: Icon, color,
}: {
  label: string; value: string; sub?: string;
  icon: any; color: string;
}) => (
  <motion.div
    initial={{ opacity: 0, y: 10 }}
    animate={{ opacity: 1, y: 0 }}
    className="bg-card rounded-lg border border-border p-4 flex items-center gap-3"
  >
    <div className={`w-10 h-10 rounded-lg bg-gradient-to-br ${color} flex items-center justify-center shrink-0`}>
      <Icon className="w-5 h-5 text-white" />
    </div>
    <div>
      <p className="text-xs text-muted-foreground font-semibold uppercase tracking-wider">{label}</p>
      <p className="text-xl font-bold stat-mono">{value}</p>
      {sub && <p className="text-xs text-muted-foreground">{sub}</p>}
    </div>
  </motion.div>
);

// ── Hook: fetch player from API with mockData fallback ──────────────────────

function usePlayerData(bbrefId: string | undefined) {
  const [apiData, setApiData] = useState<any>(null);
  const [loading, setLoading] = useState(true);
  const [triedApi, setTriedApi] = useState(false);

  useEffect(() => {
    if (!bbrefId) {
      setLoading(false);
      return;
    }

    let cancelled = false;

    // Try API first, fall back to mock on any failure
    fetch(`${API_BASE}/player/${bbrefId}`, { signal: AbortSignal.timeout(3000) })
      .then(res => {
        if (!res.ok) throw new Error(`${res.status}`);
        return res.json();
      })
      .then(data => {
        if (!cancelled) {
          setApiData(data);
          setLoading(false);
          setTriedApi(true);
        }
      })
      .catch(() => {
        if (!cancelled) {
          setApiData(null);
          setLoading(false);
          setTriedApi(true);
        }
      });

    return () => { cancelled = true; };
  }, [bbrefId]);

  // Resolve: API data → mock fallback → null (not found)
  const regPlayer = useMemo(() => {
    if (apiData?.player) return apiData.player;
    if (triedApi || !loading) {
      return MOCK_PLAYERS.find(p => p.bbref_id === bbrefId) || null;
    }
    return null;
  }, [apiData, bbrefId, triedApi, loading]);

  const playoffPlayer = useMemo(() => {
    if (apiData?.playoff_summary) return apiData.playoff_summary;
    if (triedApi || !loading) {
      return MOCK_PLAYERS_PLAYOFFS.find(p => p.bbref_id === bbrefId) || null;
    }
    return null;
  }, [apiData, bbrefId, triedApi, loading]);

  const seasonsRegular: SeasonData[] = useMemo(() => {
    if (apiData?.seasons_regular?.length) return apiData.seasons_regular;
    if (bbrefId) return SEASON_DATA_REGULAR[bbrefId] || [];
    return [];
  }, [apiData, bbrefId]);

  const seasonsPlayoffs: SeasonData[] = useMemo(() => {
    if (apiData?.seasons_playoffs?.length) return apiData.seasons_playoffs;
    if (bbrefId) return SEASON_DATA_PLAYOFFS[bbrefId] || [];
    return [];
  }, [apiData, bbrefId]);

  return { regPlayer, playoffPlayer, seasonsRegular, seasonsPlayoffs, loading };
}


const PlayerProfile = () => {
  const { id } = useParams();
  const [seasonType, setSeasonType] = useState<"regular" | "playoffs">("regular");
  const [sort, setSort] = useState<SortConfig>({ key: "year", dir: "asc" });
  const [chartStat, setChartStat] = useState<"pmi" | "ppg" | "rpg" | "apg">("pmi");

  const { regPlayer, playoffPlayer, seasonsRegular, seasonsPlayoffs, loading } = usePlayerData(id);

  // Current view: regular or playoffs
  const player = seasonType === "playoffs" && playoffPlayer ? playoffPlayer : regPlayer;
  const seasons: SeasonData[] = seasonType === "playoffs" ? seasonsPlayoffs : seasonsRegular;
  const hasPlayoffs = playoffPlayer != null && seasonsPlayoffs.length > 0;

  // Reset to regular season when switching players
  useEffect(() => { setSeasonType("regular"); }, [id]);

  // Loading state
  if (loading) {
    return (
      <main className="container mx-auto px-4 py-24 flex flex-col items-center gap-4">
        <Loader2 className="w-8 h-8 animate-spin text-primary" />
        <p className="text-muted-foreground text-sm font-medium">Loading player...</p>
      </main>
    );
  }

  // Not found — NO fallback to another player
  if (!player) {
    return (
      <main className="container mx-auto px-4 py-24 text-center">
        <h1 className="text-2xl font-bold mb-2">Player Not Found</h1>
        <p className="text-muted-foreground">
          No player found with ID "{id}". Try searching from the leaderboard.
        </p>
      </main>
    );
  }

  const sortedSeasons = [...seasons].sort((a, b) => {
    const av = a[sort.key] ?? 0;
    const bv = b[sort.key] ?? 0;
    return sort.dir === "asc" ? (av > bv ? 1 : -1) : (av < bv ? 1 : -1);
  });

  const chartData = [...seasons].sort((a, b) => a.year - b.year);

  const careerFromSeasons = (() => {
    if (!seasons.length) return null;
    const totalGP = seasons.reduce((s, x) => s + x.gp, 0);
    const totalAWC = seasons.reduce((s, x) => s + (x.awc || 0), 0);
    const peakPMI = Math.max(...seasons.map(s => s.pmi));
    const peakSeason = seasons.find(s => s.pmi === peakPMI)?.season || "";
    return { totalGP, totalAWC: Math.round(totalAWC * 10) / 10, peakPMI: Math.round(peakPMI * 100) / 100, peakSeason };
  })();

  const toggleSort = (key: keyof SeasonData) => {
    setSort(prev =>
      prev.key === key
        ? { key, dir: prev.dir === "asc" ? "desc" : "asc" }
        : { key, dir: "desc" }
    );
  };

  const SortIcon = ({ col }: { col: keyof SeasonData }) => {
    if (sort.key !== col) return <ArrowUpDown className="w-3 h-3 opacity-30" />;
    return sort.dir === "asc" ? <ChevronUp className="w-3 h-3" /> : <ChevronDown className="w-3 h-3" />;
  };

  const columns: { key: keyof SeasonData; label: string; fmt: (v: any) => string; width?: string }[] = [
    { key: "season", label: "Season", fmt: (v) => v, width: "w-20" },
    { key: "gp", label: "GP", fmt: (v) => String(v) },
    { key: "mpg", label: "MPG", fmt: (v) => v?.toFixed(1) ?? "—" },
    { key: "ppg", label: "PPG", fmt: (v) => v?.toFixed(1) ?? "—" },
    { key: "rpg", label: "RPG", fmt: (v) => v?.toFixed(1) ?? "—" },
    { key: "apg", label: "APG", fmt: (v) => v?.toFixed(1) ?? "—" },
    { key: "spg", label: "SPG", fmt: (v) => v?.toFixed(1) ?? "—" },
    { key: "bpg", label: "BPG", fmt: (v) => v?.toFixed(1) ?? "—" },
    { key: "fg_pct", label: "FG%", fmt: (v) => v != null ? (v * 100).toFixed(1) : "—" },
    { key: "ts_pct", label: "TS%", fmt: (v) => v != null ? (v * 100).toFixed(1) : "—" },
    { key: "pmi", label: "PMI", fmt: (v) => v != null ? (v >= 0 ? "+" : "") + v.toFixed(2) : "—" },
    { key: "opmi", label: "OPMI", fmt: (v) => v != null ? (v >= 0 ? "+" : "") + v.toFixed(2) : "—" },
    { key: "dpmi", label: "DPMI", fmt: (v) => v != null ? (v >= 0 ? "+" : "") + v.toFixed(2) : "—" },
    { key: "awc", label: "AWC", fmt: (v) => v != null ? v.toFixed(1) : "—" },
  ];

  const chartOptions: { key: typeof chartStat; label: string }[] = [
    { key: "pmi", label: "PMI" },
    { key: "ppg", label: "PPG" },
    { key: "rpg", label: "RPG" },
    { key: "apg", label: "APG" },
  ];

  return (
    <main className="container mx-auto px-4 py-8 max-w-6xl">
      {/* ── Header ── */}
      <motion.div
        initial={{ opacity: 0, y: 16 }}
        animate={{ opacity: 1, y: 0 }}
        className="flex flex-col sm:flex-row items-start gap-6 mb-8"
      >
        <PlayerAvatar nbaApiId={player.nba_api_id} name={player.full_name} size="lg" />
        <div className="flex-1">
          <h1 className="text-4xl font-extrabold tracking-tight">{player.full_name}</h1>
          <p className="text-lg text-muted-foreground mt-1">
            {player.position} · {player.years}
          </p>
          <div className="flex gap-2 mt-3 flex-wrap">
            <span className={`text-xs font-bold uppercase px-2.5 py-1 rounded ${
              player.is_active ? "bg-primary/15 text-primary" : "bg-muted text-muted-foreground"
            }`}>
              {player.is_active ? "Active" : "Retired"}
            </span>
            <span className="text-xs font-bold uppercase px-2.5 py-1 rounded bg-amber-500/15 text-amber-400">
              {player.gp} GP
            </span>
            <span className="text-xs font-bold uppercase px-2.5 py-1 rounded bg-emerald-500/15 text-emerald-400">
              {player.seasons || seasons.length} Seasons
            </span>
          </div>

          {/* Season type toggle */}
          <div className="flex bg-card border border-border rounded-lg p-0.5 mt-4 w-fit">
            <button
              onClick={() => setSeasonType("regular")}
              className={`px-4 py-1.5 rounded-md text-sm font-bold transition-all ${
                seasonType === "regular"
                  ? "bg-primary text-primary-foreground"
                  : "text-muted-foreground hover:text-foreground"
              }`}
            >
              Regular Season
            </button>
            <button
              onClick={() => setSeasonType("playoffs")}
              className={`px-4 py-1.5 rounded-md text-sm font-bold transition-all ${
                seasonType === "playoffs"
                  ? "bg-primary text-primary-foreground"
                  : "text-muted-foreground hover:text-foreground"
              }`}
            >
              Playoffs {!hasPlayoffs && <span className="text-xs opacity-50 ml-1">(N/A)</span>}
            </button>
          </div>
        </div>
      </motion.div>

      {/* ── Playoffs N/A state ── */}
      {seasonType === "playoffs" && !hasPlayoffs && (
        <motion.div
          initial={{ opacity: 0, y: 12 }}
          animate={{ opacity: 1, y: 0 }}
          className="bg-card rounded-lg border border-border p-8 mb-8 text-center"
        >
          <p className="text-muted-foreground font-medium">No playoff data available for {player.full_name}.</p>
        </motion.div>
      )}

      {/* ── Only show stats when we have data ── */}
      {(seasonType === "regular" || hasPlayoffs) && (
        <>
          {/* ── PMI Summary Pills ── */}
          <motion.div
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            transition={{ delay: 0.1 }}
            className="grid grid-cols-2 md:grid-cols-4 gap-3 mb-8"
          >
            <StatPill
              label="Career PMI" value={formatStat(player.pmi, "pmi")}
              sub={careerFromSeasons ? `Peak: ${formatStat(careerFromSeasons.peakPMI, "pmi")} (${careerFromSeasons.peakSeason})` : `Peak: ${formatStat(player.peak_pmi, "pmi")}`}
              icon={TrendingUp} color="from-indigo-500 to-violet-600"
            />
            <StatPill label="OPMI" value={formatStat(player.opmi, "pmi")} icon={Award} color="from-amber-500 to-orange-600" />
            <StatPill label="DPMI" value={formatStat(player.dpmi, "pmi")} icon={Shield} color="from-emerald-500 to-teal-600" />
            <StatPill
              label="AWC" value={formatStat(player.awc, "awc")}
              sub={`${seasons.length} seasons`}
              icon={Trophy} color="from-sky-500 to-blue-600"
            />
          </motion.div>

          {/* ── PMI Trend Chart ── */}
          {chartData.length > 1 && (
            <motion.div
              initial={{ opacity: 0, y: 12 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: 0.15 }}
              className="bg-card rounded-lg border border-border p-5 mb-6"
            >
              <h2 className="text-lg font-bold mb-4">PMI Over Career</h2>
              <div className="h-[280px]">
                <ResponsiveContainer width="100%" height="100%">
                  <AreaChart data={chartData}>
                    <defs>
                      <linearGradient id="pmiGrad" x1="0" y1="0" x2="0" y2="1">
                        <stop offset="0%" stopColor="#5865F2" stopOpacity={0.3} />
                        <stop offset="100%" stopColor="#5865F2" stopOpacity={0} />
                      </linearGradient>
                      <linearGradient id="opmiGrad" x1="0" y1="0" x2="0" y2="1">
                        <stop offset="0%" stopColor="#FBBF24" stopOpacity={0.2} />
                        <stop offset="100%" stopColor="#FBBF24" stopOpacity={0} />
                      </linearGradient>
                      <linearGradient id="dpmiGrad" x1="0" y1="0" x2="0" y2="1">
                        <stop offset="0%" stopColor="#34D399" stopOpacity={0.2} />
                        <stop offset="100%" stopColor="#34D399" stopOpacity={0} />
                      </linearGradient>
                    </defs>
                    <CartesianGrid strokeDasharray="3 3" stroke="hsl(220 7% 28%)" strokeOpacity={0.4} />
                    <XAxis dataKey="season" tick={{ fontSize: 12, fill: "hsl(210 10% 70%)", fontWeight: 600 }} interval="preserveStartEnd" />
                    <YAxis tick={{ fontSize: 12, fill: "hsl(210 10% 70%)", fontWeight: 600 }} domain={["auto", "auto"]} />
                    <Tooltip
                      contentStyle={TOOLTIP_STYLE}
                      labelStyle={{ color: "hsl(210 10% 93%)", fontWeight: 700, marginBottom: 4 }}
                      formatter={(val: number, name: string) => [(val >= 0 ? "+" : "") + val.toFixed(2), name]}
                    />
                    <ReferenceLine y={0} stroke="hsl(220 7% 35%)" strokeDasharray="3 3" />
                    <Area type="monotone" dataKey="pmi" stroke={CHART_COLORS.pmi} strokeWidth={2.5} fill="url(#pmiGrad)" name="PMI" dot={{ r: 3, fill: CHART_COLORS.pmi }} />
                    <Area type="monotone" dataKey="opmi" stroke={CHART_COLORS.opmi} strokeWidth={1.5} fill="url(#opmiGrad)" name="OPMI" dot={false} strokeDasharray="4 2" />
                    <Area type="monotone" dataKey="dpmi" stroke={CHART_COLORS.dpmi} strokeWidth={1.5} fill="url(#dpmiGrad)" name="DPMI" dot={false} strokeDasharray="4 2" />
                  </AreaChart>
                </ResponsiveContainer>
              </div>
              <div className="flex justify-center gap-5 mt-3">
                <span className="flex items-center gap-1.5 text-sm font-semibold"><span className="w-3 h-1 rounded-full" style={{ backgroundColor: CHART_COLORS.pmi }} /> PMI</span>
                <span className="flex items-center gap-1.5 text-sm font-semibold text-muted-foreground"><span className="w-3 h-1 rounded-full" style={{ backgroundColor: CHART_COLORS.opmi }} /> OPMI</span>
                <span className="flex items-center gap-1.5 text-sm font-semibold text-muted-foreground"><span className="w-3 h-1 rounded-full" style={{ backgroundColor: CHART_COLORS.dpmi }} /> DPMI</span>
              </div>
            </motion.div>
          )}

          {/* ── Stat Trend Chart ── */}
          {chartData.length > 1 && (
            <motion.div
              initial={{ opacity: 0, y: 12 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: 0.2 }}
              className="bg-card rounded-lg border border-border p-5 mb-6"
            >
              <div className="flex items-center justify-between mb-4 flex-wrap gap-3">
                <h2 className="text-lg font-bold">Season Trends</h2>
                <div className="flex gap-1.5">
                  {chartOptions.map(opt => (
                    <button
                      key={opt.key}
                      onClick={() => setChartStat(opt.key)}
                      className={`px-3 py-1 rounded-md text-sm font-bold transition-all ${
                        chartStat === opt.key ? "bg-primary text-primary-foreground" : "bg-muted/30 text-muted-foreground hover:text-foreground"
                      }`}
                    >
                      {opt.label}
                    </button>
                  ))}
                </div>
              </div>
              <div className="h-[260px]">
                <ResponsiveContainer width="100%" height="100%">
                  <LineChart data={chartData}>
                    <CartesianGrid strokeDasharray="3 3" stroke="hsl(220 7% 28%)" strokeOpacity={0.4} />
                    <XAxis dataKey="season" tick={{ fontSize: 12, fill: "hsl(210 10% 70%)", fontWeight: 600 }} interval="preserveStartEnd" />
                    <YAxis tick={{ fontSize: 12, fill: "hsl(210 10% 70%)", fontWeight: 600 }} />
                    <Tooltip
                      contentStyle={TOOLTIP_STYLE}
                      labelStyle={{ color: "hsl(210 10% 93%)", fontWeight: 700 }}
                      formatter={(val: number) => {
                        if (chartStat === "pmi") return [(val >= 0 ? "+" : "") + val.toFixed(2), "PMI"];
                        return [val.toFixed(1), chartStat.toUpperCase()];
                      }}
                    />
                    <Line type="monotone" dataKey={chartStat} stroke={CHART_COLORS[chartStat]} strokeWidth={2.5}
                      dot={{ r: 4, fill: CHART_COLORS[chartStat], strokeWidth: 0 }}
                      activeDot={{ r: 6, strokeWidth: 2, stroke: "white" }}
                    />
                  </LineChart>
                </ResponsiveContainer>
              </div>
            </motion.div>
          )}

          {/* ── Season-by-Season Table ── */}
          <motion.div
            initial={{ opacity: 0, y: 12 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: 0.25 }}
            className="bg-card rounded-lg border border-border overflow-hidden mb-8"
          >
            <div className="px-5 py-4 border-b border-border">
              <h2 className="text-lg font-bold">
                Season-by-Season {seasonType === "playoffs" ? "(Playoffs)" : "(Regular Season)"}
              </h2>
            </div>
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-border bg-muted/20">
                    {columns.map(col => (
                      <th
                        key={col.key}
                        onClick={() => toggleSort(col.key)}
                        className={`px-3 py-2.5 text-left font-bold uppercase tracking-wider text-xs text-muted-foreground cursor-pointer hover:text-foreground transition-colors select-none whitespace-nowrap ${col.width || ""}`}
                      >
                        <span className="inline-flex items-center gap-1">
                          {col.label}
                          <SortIcon col={col.key} />
                        </span>
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {sortedSeasons.map((s, i) => {
                    const isMax = (key: keyof SeasonData) => {
                      const vals = seasons.map(x => Number(x[key])).filter(v => !isNaN(v));
                      return Number(s[key]) === Math.max(...vals);
                    };
                    return (
                      <tr key={s.season} className={`border-b border-border/30 hover:bg-muted/10 transition-colors ${i % 2 === 0 ? "" : "bg-muted/5"}`}>
                        {columns.map(col => {
                          const val = s[col.key];
                          const isPeak = col.key !== "season" && col.key !== "gp" && col.key !== "mpg" && isMax(col.key);
                          const isPMI = ["pmi", "opmi", "dpmi"].includes(col.key);
                          return (
                            <td key={col.key} className={`px-3 py-2 whitespace-nowrap stat-mono text-sm ${
                              isPeak ? "text-amber-400 font-bold" : isPMI ? "text-indigo-300 font-semibold" : col.key === "season" ? "font-semibold text-foreground" : "text-foreground/80"
                            }`}>
                              {col.fmt(val)}
                            </td>
                          );
                        })}
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </motion.div>

          {/* ── Career Summary ── */}
          <motion.div
            initial={{ opacity: 0, y: 12 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: 0.3 }}
            className="grid grid-cols-1 md:grid-cols-2 gap-4 mb-8"
          >
            <div className="bg-card rounded-lg border border-border p-5">
              <h3 className="text-sm font-bold uppercase tracking-wider text-muted-foreground mb-4">Career Averages</h3>
              <div className="grid grid-cols-2 gap-x-6 gap-y-2">
                {[
                  { l: "PPG", v: formatStat(player.ppg, "decimal1") },
                  { l: "RPG", v: formatStat(player.rpg, "decimal1") },
                  { l: "APG", v: formatStat(player.apg, "decimal1") },
                  { l: "SPG", v: formatStat(player.spg, "decimal1") },
                  { l: "BPG", v: formatStat(player.bpg, "decimal1") },
                  { l: "FG%", v: formatStat(player.fg_pct, "pct1") },
                  { l: "TS%", v: formatStat(player.ts_pct, "pct1") },
                  { l: "GP", v: formatStat(player.gp, "comma") },
                ].map(s => (
                  <div key={s.l} className="flex justify-between py-1.5 border-b border-border/30">
                    <span className="text-sm text-muted-foreground font-medium">{s.l}</span>
                    <span className="text-sm font-bold stat-mono">{s.v}</span>
                  </div>
                ))}
              </div>
            </div>

            <div className="bg-card rounded-lg border border-primary/20 p-5" style={{ boxShadow: "0 0 24px -4px hsl(235 86% 65% / 0.15)" }}>
              <h3 className="text-sm font-bold uppercase tracking-wider text-primary mb-4">PMI Ratings</h3>
              <div className="grid grid-cols-2 gap-x-6 gap-y-2">
                {[
                  { l: "PMI", v: formatStat(player.pmi, "pmi") },
                  { l: "OPMI", v: formatStat(player.opmi, "pmi") },
                  { l: "DPMI", v: formatStat(player.dpmi, "pmi") },
                  { l: "Peak PMI", v: formatStat(player.peak_pmi, "pmi") },
                  { l: "rTS%", v: formatStat(player.rts_pct, "rpct") },
                  { l: "AWC", v: formatStat(player.awc, "awc") },
                  { l: "OAWC", v: formatStat(player.oawc, "awc") },
                  { l: "DAWC", v: formatStat(player.dawc, "awc") },
                ].map(s => (
                  <div key={s.l} className="flex justify-between py-1.5 border-b border-border/30">
                    <span className="text-sm text-muted-foreground font-medium">{s.l}</span>
                    <span className="text-sm font-bold stat-mono text-primary">{s.v}</span>
                  </div>
                ))}
              </div>
            </div>
          </motion.div>

          {/* ── Clutch Stats ── */}
          {player.cpmi != null && (
            <motion.div
              initial={{ opacity: 0, y: 12 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: 0.35 }}
              className="bg-card rounded-lg border border-rose-500/20 p-5 mb-8"
              style={{ boxShadow: "0 0 20px -6px hsl(350 65% 55% / 0.12)" }}
            >
              <h3 className="text-sm font-bold uppercase tracking-wider text-rose-400 mb-4 flex items-center gap-2">
                <Flame className="w-4 h-4" /> Clutch Stats
              </h3>
              <div className="grid grid-cols-2 md:grid-cols-4 gap-x-6 gap-y-2">
                {[
                  { l: "CPMI", v: formatStat(player.cpmi, "pmi") },
                  { l: "Clutch GP", v: String(player.clutch_gp || player.clutch_seasons_count || "—") },
                  { l: "Clutch PPG", v: player.clutch_pts?.toFixed(1) || "—" },
                  { l: "Clutch FG%", v: player.clutch_fg_pct ? (player.clutch_fg_pct * 100).toFixed(1) : "—" },
                  { l: "Clutch +/−", v: player.clutch_plus_minus != null ? String(player.clutch_plus_minus) : "—" },
                  { l: "Clutch W%", v: player.clutch_w_pct ? (player.clutch_w_pct * 100).toFixed(1) + "%" : "—" },
                  { l: "Clutch AST", v: player.clutch_ast?.toFixed(1) || "—" },
                  { l: "Clutch STL", v: player.clutch_stl?.toFixed(1) || "—" },
                ].map(s => (
                  <div key={s.l} className="flex justify-between py-1.5 border-b border-border/30">
                    <span className="text-sm text-muted-foreground font-medium">{s.l}</span>
                    <span className="text-sm font-bold stat-mono text-rose-400">{s.v}</span>
                  </div>
                ))}
              </div>
            </motion.div>
          )}
        </>
      )}
    </main>
  );
};

export default PlayerProfile;
