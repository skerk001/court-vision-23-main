import { useState, useMemo, useEffect, useCallback, useRef } from "react";
import { cn } from "@/lib/utils";
import { TAB_COLUMNS, TABS, DEFAULT_SORT } from "@/lib/constants";
import {
  MOCK_PLAYERS, MOCK_PLAYERS_PLAYOFFS,
  SEASON_DATA_REGULAR, SEASON_DATA_PLAYOFFS,
} from "@/lib/mockData";
import type { SeasonData } from "@/lib/mockData";
import DataTable from "@/components/Leaderboard/DataTable";
import FilterBar from "@/components/Leaderboard/FilterBar";
import { ChevronDown } from "lucide-react";

const PAGE_SIZE = 50;
const API_BASE = import.meta.env.VITE_API_URL || "http://localhost:8000/api/v1";

/** Build a flat array of individual player-seasons for the season-level tabs. */
function buildSeasonRows(
  players: typeof MOCK_PLAYERS,
  seasonData: Record<string, SeasonData[]>,
) {
  const rows: Record<string, any>[] = [];
  for (const player of players) {
    const seasons = seasonData[player.bbref_id];
    if (!seasons) continue;
    for (const s of seasons) {
      rows.push({
        ...s,
        full_name: player.full_name,
        bbref_id: player.bbref_id,
        nba_api_id: player.nba_api_id,
        is_active: player.is_active,
        position: player.position,
        years: player.years,
        best_season: s.season,
        best_pmi: s.pmi,
        best_opmi: s.opmi,
        best_awc: s.awc,
      });
    }
  }
  return rows;
}

function useApiPlayers() {
  const [apiRegular, setApiRegular] = useState<any[] | null>(null);
  const [apiPlayoffs, setApiPlayoffs] = useState<any[] | null>(null);
  const [apiSeasonsReg, setApiSeasonsReg] = useState<Record<string, SeasonData[]> | null>(null);
  const [apiSeasonsPly, setApiSeasonsPly] = useState<Record<string, SeasonData[]> | null>(null);

  useEffect(() => {
    // Try loading all data from API
    Promise.all([
      fetch(`${API_BASE}/leaderboard?stat=pmi&limit=2000&season_type=regular`, { signal: AbortSignal.timeout(4000) }).then(r => r.ok ? r.json() : null).catch(() => null),
      fetch(`${API_BASE}/leaderboard?stat=pmi&limit=2000&season_type=playoffs`, { signal: AbortSignal.timeout(4000) }).then(r => r.ok ? r.json() : null).catch(() => null),
    ]).then(([reg, ply]) => {
      if (reg?.players?.length) setApiRegular(reg.players);
      if (ply?.players?.length) setApiPlayoffs(ply.players);
    });

    // Also try fetching season data via a bulk endpoint or fall back to mock
    // For now, season-level data uses mock (API would need a bulk seasons endpoint)
  }, []);

  return {
    regular: apiRegular || MOCK_PLAYERS,
    playoffs: apiPlayoffs || MOCK_PLAYERS_PLAYOFFS,
    seasonsReg: apiSeasonsReg || SEASON_DATA_REGULAR,
    seasonsPly: apiSeasonsPly || SEASON_DATA_PLAYOFFS,
  };
}

const Leaderboard = () => {
  const [activeTab, setActiveTab] = useState("pergame");
  const [seasonType, setSeasonType] = useState("regular");
  const [era, setEra] = useState("all");
  const [minGp, setMinGp] = useState(50);
  const [scope, setScope] = useState("career");
  const [sortKey, setSortKey] = useState("ppg");
  const [sortDir, setSortDir] = useState<"asc" | "desc">("desc");
  const [visibleCount, setVisibleCount] = useState(PAGE_SIZE);
  const sentinelRef = useRef<HTMLDivElement>(null);

  const { regular, playoffs, seasonsReg, seasonsPly } = useApiPlayers();

  // Reset visible count and sort when tab changes
  useEffect(() => {
    setSortKey(DEFAULT_SORT[activeTab] || "ppg");
    setSortDir("desc");
    setVisibleCount(PAGE_SIZE);
  }, [activeTab]);

  // Reset visible count when filters change
  useEffect(() => {
    setVisibleCount(PAGE_SIZE);
  }, [seasonType, era, minGp, scope]);

  const columnGroups = TAB_COLUMNS[activeTab];

  const allSortedData = useMemo(() => {
    const isSeasonTab = activeTab === "best_season";

    let filtered: Record<string, any>[];

    if (isSeasonTab) {
      const players = seasonType === "playoffs" ? playoffs : regular;
      const seasonMap = seasonType === "playoffs" ? seasonsPly : seasonsReg;
      filtered = buildSeasonRows(players, seasonMap);
    } else {
      const source = seasonType === "playoffs" ? playoffs : regular;
      filtered = [...source];
    }

    if (era === "active") filtered = filtered.filter((p) => p.is_active);
    if (era === "retired") filtered = filtered.filter((p) => !p.is_active);
    if (minGp > 0 && !isSeasonTab) filtered = filtered.filter((p) => (p.gp || 0) >= minGp);

    if (activeTab === "clutch") {
      filtered = filtered.filter((p) => p.cpmi != null);
    }

    return [...filtered]
      .sort((a, b) => {
        const va = a[sortKey] ?? (sortDir === "desc" ? -Infinity : Infinity);
        const vb = b[sortKey] ?? (sortDir === "desc" ? -Infinity : Infinity);
        if (typeof va === "string" && typeof vb === "string")
          return sortDir === "desc" ? vb.localeCompare(va) : va.localeCompare(vb);
        return sortDir === "desc" ? Number(vb) - Number(va) : Number(va) - Number(vb);
      })
      .map((p, i) => ({ ...p, rank: i + 1 }));
  }, [activeTab, seasonType, era, minGp, sortKey, sortDir, regular, playoffs, seasonsReg, seasonsPly]);

  const visibleData = useMemo(
    () => allSortedData.slice(0, visibleCount),
    [allSortedData, visibleCount]
  );

  const totalCount = allSortedData.length;
  const hasMore = visibleCount < totalCount;
  const remainingCount = totalCount - visibleCount;

  // Intersection observer for infinite scroll
  useEffect(() => {
    if (!sentinelRef.current || !hasMore) return;

    const observer = new IntersectionObserver(
      (entries) => {
        if (entries[0]?.isIntersecting) {
          setVisibleCount(prev => Math.min(prev + PAGE_SIZE, totalCount));
        }
      },
      { rootMargin: "200px" }
    );

    observer.observe(sentinelRef.current);
    return () => observer.disconnect();
  }, [hasMore, totalCount]);

  const handleSort = (key: string) => {
    if (sortKey === key) {
      setSortDir((d) => (d === "desc" ? "asc" : "desc"));
    } else {
      setSortKey(key);
      setSortDir("desc");
    }
    setVisibleCount(PAGE_SIZE); // reset pagination on re-sort
  };

  const showMore = useCallback(() => {
    setVisibleCount(prev => Math.min(prev + PAGE_SIZE, totalCount));
  }, [totalCount]);

  const showAll = useCallback(() => {
    setVisibleCount(totalCount);
  }, [totalCount]);

  const showScope = activeTab !== "best_season";
  const itemLabel = activeTab === "best_season" ? "seasons" : "players";

  return (
    <main className="container mx-auto px-4 py-8">
      <h1 className="text-3xl font-extrabold tracking-tight mb-6">Leaderboard</h1>

      {/* Tabs */}
      <div className="flex gap-1 mb-5 overflow-x-auto pb-1 -mx-1 px-1">
        {TABS.map((tab) => (
          <button
            key={tab.key}
            onClick={() => setActiveTab(tab.key)}
            className={cn(
              "px-4 py-2 rounded-md text-sm font-semibold whitespace-nowrap transition-all",
              activeTab === tab.key
                ? tab.isPmi
                  ? "bg-court-gold/15 text-court-gold"
                  : "bg-primary/15 text-primary"
                : "text-muted-foreground hover:text-foreground hover:bg-muted/40"
            )}
          >
            {tab.label}
          </button>
        ))}
      </div>

      {/* Filters */}
      <FilterBar
        seasonType={seasonType}
        onSeasonTypeChange={setSeasonType}
        era={era}
        onEraChange={setEra}
        minGp={minGp}
        onMinGpChange={setMinGp}
        scope={scope}
        onScopeChange={setScope}
        showScope={showScope}
      />

      {/* Table */}
      <div className="mt-5">
        <DataTable
          data={visibleData}
          columnGroups={columnGroups}
          sortKey={sortKey}
          sortDir={sortDir}
          onSort={handleSort}
        />
      </div>

      {/* Show More / Status Bar */}
      <div className="mt-4 flex items-center justify-between flex-wrap gap-3">
        <p className="text-xs text-muted-foreground">
          Showing {visibleData.length} of {totalCount} {itemLabel} · PMI v3 · CPMI · {seasonType === "playoffs" ? "Playoffs" : "Regular Season"}
        </p>

        {hasMore && (
          <div className="flex items-center gap-2">
            <button
              onClick={showMore}
              className="flex items-center gap-1.5 px-4 py-2 rounded-lg text-sm font-semibold bg-primary/10 text-primary hover:bg-primary/20 transition-all"
            >
              <ChevronDown className="w-4 h-4" />
              Show More ({Math.min(PAGE_SIZE, remainingCount)})
            </button>
            {remainingCount > PAGE_SIZE && (
              <button
                onClick={showAll}
                className="px-4 py-2 rounded-lg text-sm font-semibold text-muted-foreground hover:text-foreground hover:bg-muted/40 transition-all"
              >
                Show All ({totalCount})
              </button>
            )}
          </div>
        )}

        {!hasMore && totalCount > PAGE_SIZE && (
          <p className="text-xs text-muted-foreground/60">
            All {totalCount} {itemLabel} loaded
          </p>
        )}
      </div>

      {/* Infinite scroll sentinel */}
      {hasMore && <div ref={sentinelRef} className="h-1" />}
    </main>
  );
};

export default Leaderboard;
