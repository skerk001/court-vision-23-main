import { useState, useMemo } from "react";
import { motion, AnimatePresence } from "framer-motion";
import { MOCK_PLAYERS, MOCK_PLAYERS_PLAYOFFS } from "@/lib/mockData";
import PlayerAvatar from "@/components/shared/PlayerAvatar";
import { Link } from "react-router-dom";
import { Crown, ChevronDown, ChevronUp } from "lucide-react";

interface StatCategory {
  key: string;
  label: string;
  format: (v: number) => string;
  gradient: string;
  barColor: string;
  textColor: string;
  group: string;
}

const STAT_CATEGORIES: StatCategory[] = [
  // Box Score
  { key: "ppg", label: "Points Per Game", format: (v) => v.toFixed(1), gradient: "from-orange-500 to-red-500", barColor: "bg-gradient-to-r from-orange-500 to-red-500", textColor: "text-orange-400", group: "Box Score" },
  { key: "rpg", label: "Rebounds Per Game", format: (v) => v.toFixed(1), gradient: "from-blue-500 to-indigo-500", barColor: "bg-gradient-to-r from-blue-500 to-indigo-500", textColor: "text-blue-400", group: "Box Score" },
  { key: "apg", label: "Assists Per Game", format: (v) => v.toFixed(1), gradient: "from-emerald-500 to-teal-500", barColor: "bg-gradient-to-r from-emerald-500 to-teal-500", textColor: "text-emerald-400", group: "Box Score" },
  { key: "spg", label: "Steals Per Game", format: (v) => v.toFixed(1), gradient: "from-yellow-500 to-amber-500", barColor: "bg-gradient-to-r from-yellow-500 to-amber-500", textColor: "text-yellow-400", group: "Box Score" },
  { key: "bpg", label: "Blocks Per Game", format: (v) => v.toFixed(1), gradient: "from-purple-500 to-violet-500", barColor: "bg-gradient-to-r from-purple-500 to-violet-500", textColor: "text-purple-400", group: "Box Score" },
  // Shooting
  { key: "fg_pct", label: "Field Goal %", format: (v) => (v * 100).toFixed(1) + "%", gradient: "from-lime-500 to-green-500", barColor: "bg-gradient-to-r from-lime-500 to-green-500", textColor: "text-lime-400", group: "Shooting" },
  { key: "ts_pct", label: "True Shooting %", format: (v) => (v * 100).toFixed(1) + "%", gradient: "from-teal-500 to-cyan-500", barColor: "bg-gradient-to-r from-teal-500 to-cyan-500", textColor: "text-teal-400", group: "Shooting" },
  // Totals
  { key: "pts", label: "Total Points", format: (v) => v.toLocaleString(), gradient: "from-orange-500 to-red-500", barColor: "bg-gradient-to-r from-orange-500 to-red-500", textColor: "text-orange-400", group: "Totals" },
  { key: "reb", label: "Total Rebounds", format: (v) => v.toLocaleString(), gradient: "from-blue-500 to-indigo-500", barColor: "bg-gradient-to-r from-blue-500 to-indigo-500", textColor: "text-blue-400", group: "Totals" },
  { key: "ast", label: "Total Assists", format: (v) => v.toLocaleString(), gradient: "from-emerald-500 to-teal-500", barColor: "bg-gradient-to-r from-emerald-500 to-teal-500", textColor: "text-emerald-400", group: "Totals" },
  { key: "stl", label: "Total Steals", format: (v) => v.toLocaleString(), gradient: "from-yellow-500 to-amber-500", barColor: "bg-gradient-to-r from-yellow-500 to-amber-500", textColor: "text-yellow-400", group: "Totals" },
  { key: "blk", label: "Total Blocks", format: (v) => v.toLocaleString(), gradient: "from-purple-500 to-violet-500", barColor: "bg-gradient-to-r from-purple-500 to-violet-500", textColor: "text-purple-400", group: "Totals" },
  // Advanced
  { key: "pmi", label: "Career PMI", format: (v) => "+" + v.toFixed(2), gradient: "from-pink-500 to-rose-500", barColor: "bg-gradient-to-r from-pink-500 to-rose-500", textColor: "text-pink-400", group: "Advanced" },
  { key: "opmi", label: "Offensive PMI", format: (v) => "+" + v.toFixed(2), gradient: "from-blue-400 to-blue-600", barColor: "bg-gradient-to-r from-blue-400 to-blue-600", textColor: "text-blue-400", group: "Advanced" },
  { key: "dpmi", label: "Defensive PMI", format: (v) => "+" + v.toFixed(2), gradient: "from-green-400 to-emerald-600", barColor: "bg-gradient-to-r from-green-400 to-emerald-600", textColor: "text-green-400", group: "Advanced" },
  { key: "peak_pmi", label: "Peak PMI", format: (v) => "+" + v.toFixed(2), gradient: "from-fuchsia-400 to-pink-600", barColor: "bg-gradient-to-r from-fuchsia-400 to-pink-600", textColor: "text-fuchsia-400", group: "Advanced" },
  { key: "awc", label: "Win Contribution", format: (v) => v.toFixed(1), gradient: "from-violet-400 to-purple-600", barColor: "bg-gradient-to-r from-violet-400 to-purple-600", textColor: "text-violet-400", group: "Advanced" },
  // Clutch
  { key: "cpmi", label: "Clutch PMI", format: (v) => "+" + v.toFixed(2), gradient: "from-red-500 to-rose-600", barColor: "bg-gradient-to-r from-red-500 to-rose-600", textColor: "text-red-400", group: "Clutch" },
];

const GROUPS = ["Box Score", "Shooting", "Totals", "Advanced", "Clutch"];

const StatLeaderboards = () => {
  const [activeGroup, setActiveGroup] = useState("Box Score");
  const [expanded, setExpanded] = useState<string | null>(null);
  const [seasonType, setSeasonType] = useState<"regular" | "playoffs">("regular");

  const dataSource = seasonType === "regular" ? MOCK_PLAYERS : MOCK_PLAYERS_PLAYOFFS;

  const visibleStats = useMemo(
    () => STAT_CATEGORIES.filter((s) => s.group === activeGroup),
    [activeGroup]
  );

  return (
    <section className="container mx-auto px-4 pb-16">
      <motion.div
        initial={{ opacity: 0, y: 12 }}
        whileInView={{ opacity: 1, y: 0 }}
        viewport={{ once: true }}
        transition={{ duration: 0.5 }}
      >
        <div className="flex items-center justify-between mb-4 flex-wrap gap-3">
          <div>
            <h2 className="text-2xl font-bold tracking-tight mb-0.5">
              All-Time Leaders
            </h2>
            <p className="text-base text-muted-foreground">
              Top 5 across every statistical category.
            </p>
          </div>

          {/* Season type toggle */}
          <div className="flex bg-card border border-border rounded-lg p-0.5">
            <button
              onClick={() => { setSeasonType("regular"); setExpanded(null); }}
              className={`px-3.5 py-1.5 rounded-md text-sm font-bold transition-all ${
                seasonType === "regular"
                  ? "bg-primary text-primary-foreground"
                  : "text-muted-foreground hover:text-foreground"
              }`}
            >
              Regular Season
            </button>
            <button
              onClick={() => { setSeasonType("playoffs"); setExpanded(null); }}
              className={`px-3.5 py-1.5 rounded-md text-sm font-bold transition-all ${
                seasonType === "playoffs"
                  ? "bg-primary text-primary-foreground"
                  : "text-muted-foreground hover:text-foreground"
              }`}
            >
              Playoffs
            </button>
          </div>
        </div>
      </motion.div>

      {/* Group tabs */}
      <div className="flex gap-1.5 mb-5 overflow-x-auto pb-1 scrollbar-hide">
        {GROUPS.map((g) => (
          <button
            key={g}
            onClick={() => { setActiveGroup(g); setExpanded(null); }}
            className={`px-3.5 py-1.5 rounded-md text-sm font-semibold whitespace-nowrap transition-all ${
              activeGroup === g
                ? "bg-primary text-primary-foreground"
                : "bg-card border border-border text-muted-foreground hover:text-foreground hover:bg-muted/30"
            }`}
          >
            {g}
          </button>
        ))}
      </div>

      {/* Stat cards grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-4">
        <AnimatePresence mode="wait">
          {visibleStats.map((stat, si) => {
            const sorted = [...dataSource]
              .filter((p) => p[stat.key] != null && !isNaN(p[stat.key]))
              .sort((a, b) => b[stat.key] - a[stat.key]);
            const top5 = sorted.slice(0, 5);
            const isExpanded = expanded === stat.key;
            const display = isExpanded ? sorted.slice(0, 10) : top5;
            const maxVal = top5[0]?.[stat.key] || 1;

            return (
              <motion.div
                key={stat.key + seasonType}
                initial={{ opacity: 0, y: 16 }}
                animate={{ opacity: 1, y: 0 }}
                exit={{ opacity: 0, y: -8 }}
                transition={{ duration: 0.35, delay: si * 0.05 }}
                className="bg-card rounded-lg border border-border overflow-hidden"
              >
                {/* Card header */}
                <div className={`px-4 py-2.5 bg-gradient-to-r ${stat.gradient} bg-opacity-10`}>
                  <div className="flex items-center justify-between">
                    <h3 className="text-sm font-bold uppercase tracking-wider text-white/95">
                      {stat.label}
                    </h3>
                    <span className="text-xs text-white/60 font-medium uppercase tracking-wider">
                      {seasonType === "playoffs" ? "Playoffs" : "Reg. Season"}
                    </span>
                  </div>
                </div>

                {/* Entries */}
                <div className="p-3 space-y-1.5">
                  {display.map((player, i) => {
                    const val = player[stat.key];
                    const pct = (val / maxVal) * 100;

                    return (
                      <Link
                        to={`/player/${player.bbref_id}`}
                        key={player.bbref_id}
                        className="flex items-center gap-2 group hover:bg-muted/20 rounded-md px-1.5 py-1 transition-colors"
                      >
                        {/* Rank */}
                        <span className="w-5 text-center">
                          {i === 0 ? (
                            <Crown className="w-3.5 h-3.5 text-amber-400 mx-auto" />
                          ) : (
                            <span className="text-xs text-muted-foreground stat-mono font-semibold">
                              {i + 1}
                            </span>
                          )}
                        </span>

                        {/* Avatar */}
                        <PlayerAvatar
                          nbaApiId={player.nba_api_id}
                          name={player.full_name}
                          size="sm"
                        />

                        {/* Name + bar */}
                        <div className="flex-1 min-w-0">
                          <div className="flex items-center justify-between gap-1.5">
                            <span className="text-sm font-semibold truncate group-hover:text-primary transition-colors">
                              {player.full_name}
                            </span>
                            <span className={`text-sm font-bold stat-mono shrink-0 ${stat.textColor}`}>
                              {stat.format(val)}
                            </span>
                          </div>
                          <div className="mt-0.5 h-2 rounded-full bg-muted/40 overflow-hidden">
                            <motion.div
                              initial={{ width: 0 }}
                              animate={{ width: `${pct}%` }}
                              transition={{ duration: 0.6, delay: si * 0.05 + i * 0.08, ease: "easeOut" }}
                              className={`h-full rounded-full ${stat.barColor} opacity-80`}
                            />
                          </div>
                        </div>
                      </Link>
                    );
                  })}
                </div>

                {/* Expand toggle */}
                <button
                  onClick={() => setExpanded(isExpanded ? null : stat.key)}
                  className="w-full flex items-center justify-center gap-1 py-2 border-t border-border/50 text-xs font-semibold uppercase tracking-wider text-muted-foreground hover:text-foreground transition-colors"
                >
                  {isExpanded ? (
                    <>Show Less <ChevronUp className="w-3 h-3" /></>
                  ) : (
                    <>Top 10 <ChevronDown className="w-3 h-3" /></>
                  )}
                </button>
              </motion.div>
            );
          })}
        </AnimatePresence>
      </div>
    </section>
  );
};

export default StatLeaderboards;
