import { useState, useMemo, useCallback } from "react";
import { cn } from "@/lib/utils";
import {
  MOCK_PLAYERS, MOCK_PLAYERS_PLAYOFFS,
  SEASON_DATA_REGULAR, SEASON_DATA_PLAYOFFS,
} from "@/lib/mockData";
import type { PlayerData, SeasonData } from "@/lib/mockData";
import PlayerAvatar from "@/components/shared/PlayerAvatar";
import { formatStat } from "@/lib/formatters";
import type { FormatType } from "@/lib/formatters";
import {
  RadarChart, Radar, PolarGrid, PolarAngleAxis,
  ResponsiveContainer, LineChart, Line, AreaChart, Area,
  XAxis, YAxis, CartesianGrid, Tooltip,
} from "recharts";
import {
  Swords, TrendingUp, Trophy, Flame, Shield,
  Target, Zap,
} from "lucide-react";

/* ─── Colors ─── */
const P1_COLOR = "#5865F2"; // blurple
const P2_COLOR = "#ED4245"; // coral-red
const P1_LIGHT = "#5865F233";
const P2_LIGHT = "#ED424533";

/* ─── Suggested Matchups ─── */
const SUGGESTED_MATCHUPS: { p1: string; p2: string; label: string; icon: React.ReactNode }[] = [
  { p1: "jordami01", p2: "jamesle01", label: "Jordan vs LeBron", icon: <Trophy className="w-3.5 h-3.5" /> },
  { p1: "duncati01", p2: "olajuha01", label: "Duncan vs Hakeem", icon: <Shield className="w-3.5 h-3.5" /> },
  { p1: "bryanko01", p2: "jordami01", label: "Kobe vs Jordan", icon: <Flame className="w-3.5 h-3.5" /> },
  { p1: "gilgesh01", p2: "bryanko01", label: "SGA vs Kobe", icon: <Zap className="w-3.5 h-3.5" /> },
  { p1: "onealsh01", p2: "chambwi01", label: "Shaq vs Wilt", icon: <Swords className="w-3.5 h-3.5" /> },
  { p1: "curryst01", p2: "thomais02", label: "Curry vs Isiah", icon: <TrendingUp className="w-3.5 h-3.5" /> },
  { p1: "jokini01", p2: "olajuha01", label: "Jokić vs Hakeem", icon: <Trophy className="w-3.5 h-3.5" /> },
  { p1: "jamesle01", p2: "bryanko01", label: "LeBron vs Kobe", icon: <Flame className="w-3.5 h-3.5" /> },
];

/* ─── Stat Definitions ─── */
interface StatDef {
  key: string;
  label: string;
  format: FormatType;
}

const CAREER_STATS: StatDef[] = [
  { key: "ppg", label: "PPG", format: "decimal1" },
  { key: "rpg", label: "RPG", format: "decimal1" },
  { key: "apg", label: "APG", format: "decimal1" },
  { key: "spg", label: "SPG", format: "decimal1" },
  { key: "bpg", label: "BPG", format: "decimal1" },
  { key: "fg_pct", label: "FG%", format: "pct1" },
  { key: "ts_pct", label: "TS%", format: "pct1" },
  { key: "rts_pct", label: "rTS%", format: "rpct" },
];

const PMI_STATS: StatDef[] = [
  { key: "pmi", label: "PMI", format: "pmi" },
  { key: "opmi", label: "OPMI", format: "pmi" },
  { key: "dpmi", label: "DPMI", format: "pmi" },
  { key: "awc", label: "AWC", format: "awc" },
  { key: "peak_pmi", label: "Best Szn PMI", format: "pmi" },
];

const CLUTCH_STATS: StatDef[] = [
  { key: "cpmi", label: "CPMI", format: "pmi" },
  { key: "clutch_pts", label: "Clutch PPG", format: "decimal1" },
  { key: "clutch_ast", label: "Clutch APG", format: "decimal1" },
  { key: "clutch_fg_pct", label: "Clutch FG%", format: "pct1" },
  { key: "clutch_w_pct", label: "Clutch W%", format: "pct1" },
  { key: "clutch_gp", label: "Clutch GP", format: "integer" },
];

/* ─── Radar Config ─── */
const RADAR_STATS = [
  { key: "ppg", label: "Scoring", max: 35 },
  { key: "ts_pct", label: "Efficiency", max: 0.65, multiply: 100 },
  { key: "apg", label: "Playmaking", max: 12 },
  { key: "rpg", label: "Rebounding", max: 14 },
  { key: "spg", label: "Steals", max: 3 },
  { key: "bpg", label: "Blocks", max: 4 },
];

/* ─── Helpers ─── */
function findPlayer(id: string, playoffs: boolean): PlayerData | null {
  const source = playoffs ? MOCK_PLAYERS_PLAYOFFS : MOCK_PLAYERS;
  return source.find((p) => p.bbref_id === id) || null;
}

function getSeasons(id: string, playoffs: boolean): SeasonData[] {
  const source = playoffs ? SEASON_DATA_PLAYOFFS : SEASON_DATA_REGULAR;
  return source[id] || [];
}

function getPrime(seasons: SeasonData[]): { years: string; avgPmi: number; seasons: SeasonData[] } {
  if (seasons.length === 0) return { years: "—", avgPmi: 0, seasons: [] };
  if (seasons.length <= 5) {
    const avg = seasons.reduce((s, x) => s + (x.pmi || 0), 0) / seasons.length;
    return {
      years: `${seasons[0].season}–${seasons[seasons.length - 1].season}`,
      avgPmi: avg,
      seasons,
    };
  }
  let bestAvg = -Infinity;
  let bestIdx = 0;
  for (let i = 0; i <= seasons.length - 5; i++) {
    const window = seasons.slice(i, i + 5);
    const avg = window.reduce((s, x) => s + (x.pmi || 0), 0) / 5;
    if (avg > bestAvg) {
      bestAvg = avg;
      bestIdx = i;
    }
  }
  const primeSeasons = seasons.slice(bestIdx, bestIdx + 5);
  return {
    years: `${primeSeasons[0].season}–${primeSeasons[4].season}`,
    avgPmi: bestAvg,
    seasons: primeSeasons,
  };
}

/* ─── Comparison Bar ─── */
function ComparisonBar({ stat, v1, v2, isGold }: { stat: StatDef; v1: number; v2: number; isGold: boolean }) {
  const maxVal = Math.max(Math.abs(v1 || 0), Math.abs(v2 || 0)) || 1;
  const pct1 = (Math.abs(v1 || 0) / maxVal) * 100;
  const pct2 = (Math.abs(v2 || 0) / maxVal) * 100;
  const winner = (v1 || 0) > (v2 || 0) ? 1 : (v2 || 0) > (v1 || 0) ? 2 : 0;

  return (
    <div className="flex items-center gap-2 py-1.5">
      <span className={cn("w-16 text-right text-xs font-semibold tabular-nums", isGold && "text-court-gold", winner === 1 && "text-[#5865F2]")}>
        {formatStat(v1, stat.format)}
      </span>
      <div className="flex-1 flex items-center h-4 gap-px">
        <div className="flex-1 flex justify-end">
          <div className={cn("h-full rounded-l-sm transition-all duration-500", winner === 1 ? "bg-[#5865F2]" : "bg-[#5865F2]/30")} style={{ width: `${pct1}%` }} />
        </div>
        <div className="w-px h-full bg-border/60" />
        <div className="flex-1">
          <div className={cn("h-full rounded-r-sm transition-all duration-500", winner === 2 ? "bg-[#ED4245]" : "bg-[#ED4245]/30")} style={{ width: `${pct2}%` }} />
        </div>
      </div>
      <span className={cn("w-16 text-left text-xs font-semibold tabular-nums", isGold && "text-court-gold", winner === 2 && "text-[#ED4245]")}>
        {formatStat(v2, stat.format)}
      </span>
    </div>
  );
}

/* ─── Section Wrapper ─── */
function Section({ title, icon, children, className }: { title: string; icon?: React.ReactNode; children: React.ReactNode; className?: string }) {
  return (
    <div className={cn("bg-card rounded-xl border border-border overflow-hidden", className)}>
      <div className="px-4 py-3 border-b border-border/50 flex items-center gap-2">
        {icon}
        <h3 className="text-sm font-bold tracking-tight">{title}</h3>
      </div>
      <div className="p-4">{children}</div>
    </div>
  );
}

/* ─── Custom Tooltip ─── */
function ChartTooltip({ active, payload, label }: any) {
  if (!active || !payload?.length) return null;
  return (
    <div className="bg-card/95 backdrop-blur-sm border border-border rounded-lg px-3 py-2 text-xs shadow-lg">
      <p className="font-bold text-muted-foreground mb-1">{label}</p>
      {payload.map((entry: any, i: number) => (
        <p key={i} className="flex items-center gap-2">
          <span className="w-2 h-2 rounded-full" style={{ background: entry.color }} />
          <span className="text-muted-foreground">{entry.name}:</span>
          <span className="font-semibold tabular-nums">
            {typeof entry.value === "number" ? entry.value.toFixed(2) : entry.value}
          </span>
        </p>
      ))}
    </div>
  );
}

/* ═══════════════════════════════════════════════════════ */
/*  MAIN COMPARE COMPONENT                                */
/* ═══════════════════════════════════════════════════════ */
const Compare = () => {
  const [p1Id, setP1Id] = useState("jordami01");
  const [p2Id, setP2Id] = useState("jamesle01");
  const [seasonType, setSeasonType] = useState<"regular" | "playoffs">("regular");

  const isPlayoffs = seasonType === "playoffs";

  const p1Reg = findPlayer(p1Id, false);
  const p2Reg = findPlayer(p2Id, false);
  const p1Ply = findPlayer(p1Id, true);
  const p2Ply = findPlayer(p2Id, true);
  const p1 = isPlayoffs ? (p1Ply || p1Reg) : p1Reg;
  const p2 = isPlayoffs ? (p2Ply || p2Reg) : p2Reg;

  const p1Display = p1Reg || MOCK_PLAYERS[0];
  const p2Display = p2Reg || MOCK_PLAYERS[1];

  const p1SeasonsReg = getSeasons(p1Id, false);
  const p2SeasonsReg = getSeasons(p2Id, false);
  const p1SeasonsPly = getSeasons(p1Id, true);
  const p2SeasonsPly = getSeasons(p2Id, true);
  const p1Seasons = isPlayoffs ? p1SeasonsPly : p1SeasonsReg;
  const p2Seasons = isPlayoffs ? p2SeasonsPly : p2SeasonsReg;

  const p1PrimeReg = useMemo(() => getPrime(p1SeasonsReg), [p1SeasonsReg]);
  const p2PrimeReg = useMemo(() => getPrime(p2SeasonsReg), [p2SeasonsReg]);
  const p1PrimePly = useMemo(() => getPrime(p1SeasonsPly), [p1SeasonsPly]);
  const p2PrimePly = useMemo(() => getPrime(p2SeasonsPly), [p2SeasonsPly]);

  const p1PeakReg = useMemo(() => p1SeasonsReg.length ? [...p1SeasonsReg].sort((a, b) => (b.pmi || 0) - (a.pmi || 0))[0] : null, [p1SeasonsReg]);
  const p2PeakReg = useMemo(() => p2SeasonsReg.length ? [...p2SeasonsReg].sort((a, b) => (b.pmi || 0) - (a.pmi || 0))[0] : null, [p2SeasonsReg]);
  const p1PeakPly = useMemo(() => p1SeasonsPly.length ? [...p1SeasonsPly].sort((a, b) => (b.pmi || 0) - (a.pmi || 0))[0] : null, [p1SeasonsPly]);
  const p2PeakPly = useMemo(() => p2SeasonsPly.length ? [...p2SeasonsPly].sort((a, b) => (b.pmi || 0) - (a.pmi || 0))[0] : null, [p2SeasonsPly]);

  /* ─── Radar Data ─── */
  const radarData = useMemo(() => {
    if (!p1 || !p2) return [];
    return RADAR_STATS.map((rs) => {
      let v1 = Number(p1[rs.key] || 0);
      let v2 = Number(p2[rs.key] || 0);
      if (rs.multiply) { v1 *= rs.multiply; v2 *= rs.multiply; }
      return {
        stat: rs.label,
        p1: Math.min((v1 / (rs.multiply ? rs.max * rs.multiply : rs.max)) * 100, 100),
        p2: Math.min((v2 / (rs.multiply ? rs.max * rs.multiply : rs.max)) * 100, 100),
      };
    });
  }, [p1, p2]);

  /* ─── PMI Timeline Data ─── */
  const pmiTimelineData = useMemo(() => {
    const allYears = new Set<number>();
    p1Seasons.forEach((s) => allYears.add(s.year));
    p2Seasons.forEach((s) => allYears.add(s.year));
    return [...allYears].sort().map((year) => {
      const s1 = p1Seasons.find((s) => s.year === year);
      const s2 = p2Seasons.find((s) => s.year === year);
      return {
        season: s1?.season || s2?.season || `${year}-${String(year + 1).slice(-2)}`,
        p1Pmi: s1?.pmi ?? null,
        p2Pmi: s2?.pmi ?? null,
      };
    });
  }, [p1Seasons, p2Seasons]);

  /* ─── AWC Accumulation Data ─── */
  const awcData = useMemo(() => {
    let cum1 = 0, cum2 = 0;
    const p1Sorted = [...p1SeasonsReg].sort((a, b) => a.year - b.year);
    const p2Sorted = [...p2SeasonsReg].sort((a, b) => a.year - b.year);
    const maxLen = Math.max(p1Sorted.length, p2Sorted.length);
    const data: { seasonNum: number; p1Awc: number | null; p2Awc: number | null }[] = [];
    for (let i = 0; i < maxLen; i++) {
      if (p1Sorted[i]) cum1 += p1Sorted[i].awc || 0;
      if (p2Sorted[i]) cum2 += p2Sorted[i].awc || 0;
      data.push({
        seasonNum: i + 1,
        p1Awc: i < p1Sorted.length ? Math.round(cum1 * 10) / 10 : null,
        p2Awc: i < p2Sorted.length ? Math.round(cum2 * 10) / 10 : null,
      });
    }
    return data;
  }, [p1SeasonsReg, p2SeasonsReg]);

  /* ─── Strengths Data ─── */
  const strengthsData = useMemo(() => {
    if (!p1 || !p2) return [];
    const cats = [
      { label: "Scoring", key: "ppg", max: 35 },
      { label: "Efficiency", key: "ts_pct", max: 0.65, mult: 100 },
      { label: "Playmaking", key: "apg", max: 12 },
      { label: "Rebounding", key: "rpg", max: 14 },
      { label: "Steals", key: "spg", max: 3.5 },
      { label: "Blocks", key: "bpg", max: 4 },
      { label: "OPMI", key: "opmi", max: 12 },
      { label: "DPMI", key: "dpmi", max: 5 },
    ];
    return cats.map((c) => {
      let v1 = Number(p1[c.key] || 0);
      let v2 = Number(p2[c.key] || 0);
      if (c.mult) { v1 *= c.mult; v2 *= c.mult; }
      return {
        label: c.label,
        p1: v1,
        p2: v2,
        p1Pct: Math.min((v1 / (c.mult ? c.max * c.mult : c.max)) * 100, 100),
        p2Pct: Math.min((v2 / (c.mult ? c.max * c.mult : c.max)) * 100, 100),
      };
    });
  }, [p1, p2]);

  const handleMatchup = (mP1: string, mP2: string) => {
    const p1Exists = MOCK_PLAYERS.some((p) => p.bbref_id === mP1);
    const p2Exists = MOCK_PLAYERS.some((p) => p.bbref_id === mP2);
    if (p1Exists) setP1Id(mP1);
    if (p2Exists) setP2Id(mP2);
  };

  if (!p1 || !p2) return null;

  const goldStats = new Set(["pmi", "opmi", "dpmi", "awc", "peak_pmi", "cpmi", "rts_pct"]);

  return (
    <main className="container mx-auto px-4 py-6 max-w-6xl">
      {/* Title */}
      <div className="flex items-center gap-3 mb-5">
        <Swords className="w-7 h-7 text-primary" />
        <h1 className="text-2xl font-extrabold tracking-tight">Compare Players</h1>
      </div>

      {/* Suggested Matchups */}
      <div className="mb-6">
        <p className="text-xs text-muted-foreground mb-2 font-medium uppercase tracking-wider">Popular Matchups</p>
        <div className="flex flex-wrap gap-2">
          {SUGGESTED_MATCHUPS.map((m, i) => {
            const isActive = (p1Id === m.p1 && p2Id === m.p2) || (p1Id === m.p2 && p2Id === m.p1);
            return (
              <button
                key={i}
                onClick={() => handleMatchup(m.p1, m.p2)}
                className={cn(
                  "flex items-center gap-1.5 px-3 py-1.5 rounded-full text-xs font-medium border transition-all",
                  isActive
                    ? "bg-primary/15 border-primary/40 text-primary"
                    : "bg-card border-border text-muted-foreground hover:border-primary/30 hover:text-foreground"
                )}
              >
                {m.icon}
                {m.label}
              </button>
            );
          })}
        </div>
      </div>

      {/* Player Selectors */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-3 mb-6">
        {[
          { id: p1Id, setId: setP1Id, player: p1Display, color: P1_COLOR, label: "Player 1" },
          { id: p2Id, setId: setP2Id, player: p2Display, color: P2_COLOR, label: "Player 2" },
        ].map((sel, i) => (
          <div key={i} className="bg-card rounded-xl border border-border p-3" style={{ borderLeftColor: sel.color, borderLeftWidth: 3 }}>
            <select
              value={sel.id}
              onChange={(e) => sel.setId(e.target.value)}
              className="w-full bg-muted border border-border rounded-md px-3 py-1.5 text-xs mb-3 focus:outline-none focus:ring-1 focus:ring-ring"
            >
              {MOCK_PLAYERS.map((p) => (
                <option key={p.bbref_id} value={p.bbref_id}>
                  {p.full_name}
                </option>
              ))}
            </select>
            <div className="flex items-center gap-3">
              <PlayerAvatar nbaApiId={sel.player.nba_api_id} name={sel.player.full_name} size="md" />
              <div>
                <p className="font-bold text-sm">{sel.player.full_name}</p>
                <p className="text-[11px] text-muted-foreground">
                  {sel.player.position} · {sel.player.years} · {sel.player.gp} GP
                </p>
              </div>
            </div>
          </div>
        ))}
      </div>

      {/* Season Type Toggle */}
      <div className="flex items-center gap-2 mb-6">
        {(["regular", "playoffs"] as const).map((st) => (
          <button
            key={st}
            onClick={() => setSeasonType(st)}
            className={cn(
              "px-4 py-1.5 rounded-full text-xs font-semibold transition-all border",
              seasonType === st
                ? "bg-primary/15 border-primary/40 text-primary"
                : "bg-card border-border text-muted-foreground hover:text-foreground"
            )}
          >
            {st === "regular" ? "Regular Season" : "Playoffs"}
          </button>
        ))}
      </div>

      {/* Head-to-Head Stats */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 mb-6">
        <Section title="Career Stats" icon={<Target className="w-4 h-4 text-muted-foreground" />}>
          <div className="flex justify-between text-[10px] font-bold uppercase tracking-wider text-muted-foreground mb-2 px-16">
            <span style={{ color: P1_COLOR }}>{p1Display.full_name?.split(" ").pop()}</span>
            <span style={{ color: P2_COLOR }}>{p2Display.full_name?.split(" ").pop()}</span>
          </div>
          {CAREER_STATS.map((stat) => (
            <ComparisonBar key={stat.key} stat={stat} v1={p1?.[stat.key] ?? 0} v2={p2?.[stat.key] ?? 0} isGold={goldStats.has(stat.key)} />
          ))}
        </Section>

        <Section title="PMI Metrics" icon={<TrendingUp className="w-4 h-4 text-court-gold" />}>
          <div className="flex justify-between text-[10px] font-bold uppercase tracking-wider text-muted-foreground mb-2 px-16">
            <span style={{ color: P1_COLOR }}>{p1Display.full_name?.split(" ").pop()}</span>
            <span style={{ color: P2_COLOR }}>{p2Display.full_name?.split(" ").pop()}</span>
          </div>
          {PMI_STATS.map((stat) => (
            <ComparisonBar key={stat.key} stat={stat} v1={p1?.[stat.key] ?? 0} v2={p2?.[stat.key] ?? 0} isGold={goldStats.has(stat.key)} />
          ))}
        </Section>
      </div>

      {/* Radar + Strengths */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 mb-6">
        <Section title="Skill Profile" icon={<Shield className="w-4 h-4 text-primary" />}>
          <div className="flex justify-center gap-6 text-[10px] font-bold mb-2">
            <span className="flex items-center gap-1.5">
              <span className="w-3 h-1.5 rounded-full" style={{ background: P1_COLOR }} />
              {p1Display.full_name?.split(" ").pop()}
            </span>
            <span className="flex items-center gap-1.5">
              <span className="w-3 h-1.5 rounded-full" style={{ background: P2_COLOR }} />
              {p2Display.full_name?.split(" ").pop()}
            </span>
          </div>
          <ResponsiveContainer width="100%" height={280}>
            <RadarChart data={radarData} cx="50%" cy="50%" outerRadius="72%">
              <PolarGrid stroke="#374151" strokeOpacity={0.4} />
              <PolarAngleAxis dataKey="stat" tick={{ fill: "#9CA3AF", fontSize: 11, fontWeight: 600 }} />
              <Radar name={p1Display.full_name} dataKey="p1" stroke={P1_COLOR} fill={P1_COLOR} fillOpacity={0.15} strokeWidth={2} />
              <Radar name={p2Display.full_name} dataKey="p2" stroke={P2_COLOR} fill={P2_COLOR} fillOpacity={0.15} strokeWidth={2} />
            </RadarChart>
          </ResponsiveContainer>
        </Section>

        <Section title="Category Breakdown" icon={<Flame className="w-4 h-4 text-orange-400" />}>
          <div className="space-y-2.5 mt-1">
            {strengthsData.map((cat) => {
              const winner = cat.p1 > cat.p2 ? 1 : cat.p2 > cat.p1 ? 2 : 0;
              return (
                <div key={cat.label}>
                  <div className="flex justify-between text-[10px] text-muted-foreground font-semibold mb-0.5">
                    <span className={cn(winner === 1 && "text-[#5865F2]")}>{cat.p1.toFixed(1)}</span>
                    <span className="uppercase tracking-wider">{cat.label}</span>
                    <span className={cn(winner === 2 && "text-[#ED4245]")}>{cat.p2.toFixed(1)}</span>
                  </div>
                  <div className="flex h-2 gap-0.5 rounded-sm overflow-hidden">
                    <div className="flex-1 flex justify-end bg-muted/30 rounded-l-sm">
                      <div className="h-full rounded-l-sm transition-all duration-500" style={{ width: `${cat.p1Pct}%`, background: winner === 1 ? P1_COLOR : `${P1_COLOR}55` }} />
                    </div>
                    <div className="flex-1 bg-muted/30 rounded-r-sm">
                      <div className="h-full rounded-r-sm transition-all duration-500" style={{ width: `${cat.p2Pct}%`, background: winner === 2 ? P2_COLOR : `${P2_COLOR}55` }} />
                    </div>
                  </div>
                </div>
              );
            })}
          </div>
        </Section>
      </div>

      {/* PMI Timeline */}
      <Section title={`PMI Timeline — ${isPlayoffs ? "Playoffs" : "Regular Season"}`} icon={<TrendingUp className="w-4 h-4 text-court-gold" />} className="mb-6">
        <div className="flex justify-center gap-6 text-[10px] font-bold mb-3">
          <span className="flex items-center gap-1.5">
            <span className="w-3 h-1.5 rounded-full" style={{ background: P1_COLOR }} />
            {p1Display.full_name}
          </span>
          <span className="flex items-center gap-1.5">
            <span className="w-3 h-1.5 rounded-full" style={{ background: P2_COLOR }} />
            {p2Display.full_name}
          </span>
        </div>
        <ResponsiveContainer width="100%" height={260}>
          <LineChart data={pmiTimelineData}>
            <CartesianGrid strokeDasharray="3 3" stroke="#374151" strokeOpacity={0.3} />
            <XAxis dataKey="season" tick={{ fill: "#9CA3AF", fontSize: 10 }} interval="preserveStartEnd" />
            <YAxis tick={{ fill: "#9CA3AF", fontSize: 10 }} domain={["auto", "auto"]} tickFormatter={(v: number) => v.toFixed(0)} />
            <Tooltip content={<ChartTooltip />} />
            <ReferenceLine y={0} stroke="#6B7280" strokeDasharray="3 3" />
            <Line type="monotone" dataKey="p1Pmi" stroke={P1_COLOR} strokeWidth={2.5} dot={{ r: 3, fill: P1_COLOR }} activeDot={{ r: 5 }} name={p1Display.full_name} connectNulls={false} />
            <Line type="monotone" dataKey="p2Pmi" stroke={P2_COLOR} strokeWidth={2.5} dot={{ r: 3, fill: P2_COLOR }} activeDot={{ r: 5 }} name={p2Display.full_name} connectNulls={false} />
          </LineChart>
        </ResponsiveContainer>
      </Section>

      {/* AWC Accumulation */}
      <Section title="AWC Accumulation — Career Value Over Time" icon={<Trophy className="w-4 h-4 text-yellow-400" />} className="mb-6">
        <div className="flex justify-center gap-6 text-[10px] font-bold mb-3">
          <span className="flex items-center gap-1.5">
            <span className="w-3 h-1.5 rounded-full" style={{ background: P1_COLOR }} />
            {p1Display.full_name}
          </span>
          <span className="flex items-center gap-1.5">
            <span className="w-3 h-1.5 rounded-full" style={{ background: P2_COLOR }} />
            {p2Display.full_name}
          </span>
        </div>
        <ResponsiveContainer width="100%" height={240}>
          <AreaChart data={awcData}>
            <CartesianGrid strokeDasharray="3 3" stroke="#374151" strokeOpacity={0.3} />
            <XAxis dataKey="seasonNum" tick={{ fill: "#9CA3AF", fontSize: 10 }} label={{ value: "Season #", position: "insideBottom", offset: -5, fill: "#6B7280", fontSize: 10 }} />
            <YAxis tick={{ fill: "#9CA3AF", fontSize: 10 }} tickFormatter={(v: number) => v.toFixed(0)} />
            <Tooltip content={<ChartTooltip />} />
            <Area type="monotone" dataKey="p1Awc" stroke={P1_COLOR} fill={P1_LIGHT} strokeWidth={2} name={p1Display.full_name} connectNulls={false} />
            <Area type="monotone" dataKey="p2Awc" stroke={P2_COLOR} fill={P2_LIGHT} strokeWidth={2} name={p2Display.full_name} connectNulls={false} />
          </AreaChart>
        </ResponsiveContainer>
      </Section>

      {/* Clutch Comparison */}
      <Section title="Clutch Performance (Last 5 min, ±5 pts)" icon={<Zap className="w-4 h-4 text-yellow-400" />} className="mb-6">
        {(p1Reg?.cpmi != null || p2Reg?.cpmi != null) ? (
          <>
            <div className="flex justify-between text-[10px] font-bold uppercase tracking-wider text-muted-foreground mb-2 px-16">
              <span style={{ color: P1_COLOR }}>{p1Display.full_name?.split(" ").pop()}</span>
              <span style={{ color: P2_COLOR }}>{p2Display.full_name?.split(" ").pop()}</span>
            </div>
            {CLUTCH_STATS.map((stat) => (
              <ComparisonBar key={stat.key} stat={stat} v1={p1Reg?.[stat.key] ?? 0} v2={p2Reg?.[stat.key] ?? 0} isGold={goldStats.has(stat.key)} />
            ))}
          </>
        ) : (
          <p className="text-center text-xs text-muted-foreground py-4">Clutch data not available for these players (requires post-1996 seasons)</p>
        )}
      </Section>

      {/* Peak & Prime Analysis */}
      <Section title="Peak & Prime Analysis" icon={<Flame className="w-4 h-4 text-orange-400" />} className="mb-6">
        <div className="overflow-x-auto">
          <table className="w-full text-xs">
            <thead>
              <tr className="border-b border-border/50 text-muted-foreground">
                <th className="text-left py-2 px-2 font-semibold">Metric</th>
                <th className="text-center py-2 px-2 font-bold" style={{ color: P1_COLOR }}>{p1Display.full_name?.split(" ").pop()}</th>
                <th className="text-center py-2 px-2 font-bold" style={{ color: P2_COLOR }}>{p2Display.full_name?.split(" ").pop()}</th>
              </tr>
            </thead>
            <tbody>
              {/* Regular Season */}
              <tr className="border-b border-border/30">
                <td className="py-2 px-2" colSpan={3}><span className="uppercase tracking-wider text-[10px] text-muted-foreground font-medium">Regular Season</span></td>
              </tr>
              {[
                { label: "Best Szn PMI", v1: p1PeakReg ? `${formatStat(p1PeakReg.pmi, "pmi")} (${p1PeakReg.season})` : "—", v2: p2PeakReg ? `${formatStat(p2PeakReg.pmi, "pmi")} (${p2PeakReg.season})` : "—", w: (p1PeakReg?.pmi || 0) > (p2PeakReg?.pmi || 0) ? 1 : 2 },
                { label: "Best Szn PPG", v1: p1PeakReg ? `${p1PeakReg.ppg?.toFixed(1)} (${p1PeakReg.season})` : "—", v2: p2PeakReg ? `${p2PeakReg.ppg?.toFixed(1)} (${p2PeakReg.season})` : "—" },
                { label: "Prime (5yr) PMI", v1: `${p1PrimeReg.avgPmi.toFixed(2)} (${p1PrimeReg.years})`, v2: `${p2PrimeReg.avgPmi.toFixed(2)} (${p2PrimeReg.years})`, w: p1PrimeReg.avgPmi > p2PrimeReg.avgPmi ? 1 : 2 },
                { label: "Career AWC", v1: formatStat(p1Reg?.awc, "awc"), v2: formatStat(p2Reg?.awc, "awc"), w: (p1Reg?.awc || 0) > (p2Reg?.awc || 0) ? 1 : 2 },
                { label: "Seasons", v1: String(p1SeasonsReg.length || p1Reg?.seasons || "—"), v2: String(p2SeasonsReg.length || p2Reg?.seasons || "—") },
              ].map((row, i) => (
                <tr key={`reg-${i}`} className="border-b border-border/20">
                  <td className="py-1.5 px-2 text-muted-foreground">{row.label}</td>
                  <td className={cn("py-1.5 px-2 text-center tabular-nums font-semibold", row.w === 1 && "text-[#5865F2]")}>{row.v1}</td>
                  <td className={cn("py-1.5 px-2 text-center tabular-nums font-semibold", row.w === 2 && "text-[#ED4245]")}>{row.v2}</td>
                </tr>
              ))}

              {/* Playoffs */}
              <tr className="border-b border-border/30">
                <td className="py-2 px-2" colSpan={3}><span className="uppercase tracking-wider text-[10px] text-muted-foreground font-medium">Playoffs</span></td>
              </tr>
              {[
                { label: "Best Playoff PMI", v1: p1PeakPly ? `${formatStat(p1PeakPly.pmi, "pmi")} (${p1PeakPly.season})` : "—", v2: p2PeakPly ? `${formatStat(p2PeakPly.pmi, "pmi")} (${p2PeakPly.season})` : "—", w: (p1PeakPly?.pmi || 0) > (p2PeakPly?.pmi || 0) ? 1 : 2 },
                { label: "Playoff Prime PMI", v1: p1SeasonsPly.length > 0 ? `${p1PrimePly.avgPmi.toFixed(2)} (${p1PrimePly.years})` : "—", v2: p2SeasonsPly.length > 0 ? `${p2PrimePly.avgPmi.toFixed(2)} (${p2PrimePly.years})` : "—", w: p1PrimePly.avgPmi > p2PrimePly.avgPmi ? 1 : 2 },
                { label: "Playoff AWC", v1: formatStat(p1Ply?.awc, "awc"), v2: formatStat(p2Ply?.awc, "awc"), w: (p1Ply?.awc || 0) > (p2Ply?.awc || 0) ? 1 : 2 },
                { label: "Playoff GP", v1: String(p1Ply?.gp || "—"), v2: String(p2Ply?.gp || "—") },
              ].map((row, i) => (
                <tr key={`ply-${i}`} className="border-b border-border/20">
                  <td className="py-1.5 px-2 text-muted-foreground">{row.label}</td>
                  <td className={cn("py-1.5 px-2 text-center tabular-nums font-semibold", row.w === 1 && "text-[#5865F2]")}>{row.v1}</td>
                  <td className={cn("py-1.5 px-2 text-center tabular-nums font-semibold", row.w === 2 && "text-[#ED4245]")}>{row.v2}</td>
                </tr>
              ))}

              {/* Clutch */}
              {(p1Reg?.cpmi != null || p2Reg?.cpmi != null) && (
                <>
                  <tr className="border-b border-border/30">
                    <td className="py-2 px-2" colSpan={3}><span className="uppercase tracking-wider text-[10px] text-muted-foreground font-medium">Clutch</span></td>
                  </tr>
                  {[
                    { label: "CPMI", v1: formatStat(p1Reg?.cpmi, "pmi"), v2: formatStat(p2Reg?.cpmi, "pmi"), w: (p1Reg?.cpmi || 0) > (p2Reg?.cpmi || 0) ? 1 : 2 },
                    { label: "Clutch W%", v1: formatStat(p1Reg?.clutch_w_pct, "pct1"), v2: formatStat(p2Reg?.clutch_w_pct, "pct1"), w: (p1Reg?.clutch_w_pct || 0) > (p2Reg?.clutch_w_pct || 0) ? 1 : 2 },
                  ].map((row, i) => (
                    <tr key={`clutch-${i}`} className="border-b border-border/20">
                      <td className="py-1.5 px-2 text-muted-foreground">{row.label}</td>
                      <td className={cn("py-1.5 px-2 text-center tabular-nums font-semibold", row.w === 1 && "text-[#5865F2]")}>{row.v1}</td>
                      <td className={cn("py-1.5 px-2 text-center tabular-nums font-semibold", row.w === 2 && "text-[#ED4245]")}>{row.v2}</td>
                    </tr>
                  ))}
                </>
              )}
            </tbody>
          </table>
        </div>
      </Section>

      {/* Scoring Trend */}
      <Section title={`Scoring Trend — ${isPlayoffs ? "Playoffs" : "Regular Season"}`} icon={<Flame className="w-4 h-4 text-pink-400" />}>
        <ResponsiveContainer width="100%" height={220}>
          <AreaChart
            data={pmiTimelineData.map((d) => {
              const s1 = p1Seasons.find((s) => s.season === d.season);
              const s2 = p2Seasons.find((s) => s.season === d.season);
              return { ...d, p1Ppg: s1?.ppg ?? null, p2Ppg: s2?.ppg ?? null };
            })}
          >
            <CartesianGrid strokeDasharray="3 3" stroke="#374151" strokeOpacity={0.3} />
            <XAxis dataKey="season" tick={{ fill: "#9CA3AF", fontSize: 10 }} interval="preserveStartEnd" />
            <YAxis tick={{ fill: "#9CA3AF", fontSize: 10 }} domain={["auto", "auto"]} />
            <Tooltip content={<ChartTooltip />} />
            <Area type="monotone" dataKey="p1Ppg" stroke={P1_COLOR} fill={P1_LIGHT} strokeWidth={2} name={p1Display.full_name} connectNulls={false} />
            <Area type="monotone" dataKey="p2Ppg" stroke={P2_COLOR} fill={P2_LIGHT} strokeWidth={2} name={p2Display.full_name} connectNulls={false} />
          </AreaChart>
        </ResponsiveContainer>
      </Section>
    </main>
  );
};

export default Compare;
