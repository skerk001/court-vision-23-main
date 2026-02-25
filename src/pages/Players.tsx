import { useState, useMemo } from "react";
import { Link } from "react-router-dom";
import { cn } from "@/lib/utils";
import { MOCK_PLAYERS } from "@/lib/mockData";
import PlayerAvatar from "@/components/shared/PlayerAvatar";
import { Search } from "lucide-react";

const POSITIONS = ["All", "PG", "SG", "SF", "PF", "C"];
const STATUSES = ["All", "Active", "Retired"];

const Players = () => {
  const [search, setSearch] = useState("");
  const [position, setPosition] = useState("All");
  const [status, setStatus] = useState("All");

  const filtered = useMemo(() => {
    return MOCK_PLAYERS.filter((p) => {
      if (search && !p.full_name.toLowerCase().includes(search.toLowerCase()))
        return false;
      if (position !== "All" && p.position !== position) return false;
      if (status === "Active" && !p.is_active) return false;
      if (status === "Retired" && p.is_active) return false;
      return true;
    });
  }, [search, position, status]);

  return (
    <main className="container mx-auto px-4 py-8">
      <h1 className="text-3xl font-extrabold tracking-tight mb-6">Players</h1>

      {/* Search */}
      <div className="relative mb-5 max-w-md">
        <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-muted-foreground" />
        <input
          type="text"
          placeholder="Search players..."
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          className="w-full bg-card border border-border rounded-lg pl-10 pr-4 py-2.5 text-sm placeholder:text-muted-foreground focus:outline-none focus:ring-1 focus:ring-ring"
        />
      </div>

      {/* Filters */}
      <div className="flex flex-wrap gap-4 mb-6">
        <div className="flex gap-1">
          {POSITIONS.map((pos) => (
            <button
              key={pos}
              onClick={() => setPosition(pos)}
              className={cn(
                "px-3 py-1.5 text-xs font-medium rounded-md transition-colors",
                position === pos
                  ? "bg-primary text-primary-foreground"
                  : "bg-card text-muted-foreground hover:text-foreground border border-border"
              )}
            >
              {pos}
            </button>
          ))}
        </div>
        <div className="flex gap-1">
          {STATUSES.map((s) => (
            <button
              key={s}
              onClick={() => setStatus(s)}
              className={cn(
                "px-3 py-1.5 text-xs font-medium rounded-md transition-colors",
                status === s
                  ? "bg-primary text-primary-foreground"
                  : "bg-card text-muted-foreground hover:text-foreground border border-border"
              )}
            >
              {s}
            </button>
          ))}
        </div>
      </div>

      {/* Player Grid */}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
        {filtered.map((player) => (
          <Link
            key={player.bbref_id}
            to={`/player/${player.bbref_id}`}
            className="bg-card rounded-lg border border-border p-4 hover:border-primary/30 transition-colors group"
          >
            <div className="flex items-center gap-3">
              <PlayerAvatar nbaApiId={player.nba_api_id} name={player.full_name} size="md" />
              <div className="flex-1 min-w-0">
                <p className="text-sm font-bold group-hover:text-primary transition-colors truncate">
                  {player.full_name}
                </p>
                <p className="text-xs text-muted-foreground">
                  {player.position} · {player.years}
                </p>
                <div className="flex gap-4 mt-2">
                  <div>
                    <span className="text-xs text-muted-foreground">PPG </span>
                    <span className="text-xs font-semibold stat-mono">{player.ppg.toFixed(1)}</span>
                  </div>
                  <div>
                    <span className="text-xs text-muted-foreground">PMI </span>
                    <span className="text-xs font-semibold stat-mono text-court-gold">
                      +{player.pmi.toFixed(2)}
                    </span>
                  </div>
                </div>
              </div>
              {player.is_active && (
                <span className="text-[9px] font-bold text-primary bg-primary/10 px-1.5 py-0.5 rounded self-start">
                  ACT
                </span>
              )}
            </div>
          </Link>
        ))}
      </div>

      {filtered.length === 0 && (
        <p className="text-center text-muted-foreground py-12">No players found.</p>
      )}
    </main>
  );
};

export default Players;
