import { useState } from "react";
import { Link, useNavigate } from "react-router-dom";
import { motion } from "framer-motion";
import { Button } from "@/components/ui/button";
import { TrendingUp, Users, Search, BarChart3 } from "lucide-react";
import { MOCK_PLAYERS } from "@/lib/mockData";

const HeroSection = () => {
  const [query, setQuery] = useState("");
  const [suggestions, setSuggestions] = useState<typeof MOCK_PLAYERS>([]);
  const navigate = useNavigate();

  const handleSearch = (val: string) => {
    setQuery(val);
    if (val.length >= 2) {
      const matches = MOCK_PLAYERS.filter((p) =>
        p.full_name.toLowerCase().includes(val.toLowerCase())
      ).slice(0, 5);
      setSuggestions(matches);
    } else {
      setSuggestions([]);
    }
  };

  const goToPlayer = (id: string) => {
    setQuery("");
    setSuggestions([]);
    navigate(`/player/${id}`);
  };

  return (
    <section className="relative overflow-hidden py-14 md:py-20">
      <div className="absolute inset-0 hero-glow" />
      <div className="absolute inset-0 bg-gradient-to-b from-transparent via-transparent to-background" />

      <div className="container relative mx-auto px-4 text-center">
        <motion.div
          initial={{ opacity: 0, y: 16 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.5 }}
        >
          <h1 className="text-4xl md:text-6xl font-extrabold tracking-tighter leading-none">
            COURT<span className="text-primary">SIDE</span>
          </h1>
          <p className="mt-2 text-base md:text-lg text-muted-foreground max-w-xl mx-auto">
            NBA analytics & proprietary impact metrics covering{" "}
            <span className="text-foreground font-semibold">5,000+</span>{" "}
            players from 1946 to present
          </p>
        </motion.div>

        <motion.div
          initial={{ opacity: 0, y: 10 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.5, delay: 0.15 }}
          className="mt-3 flex items-center justify-center gap-2 flex-wrap"
        >
          <span className="inline-flex items-center gap-1.5 bg-primary/12 text-primary px-2.5 py-0.5 rounded-full text-xs font-bold">
            <TrendingUp className="w-3 h-3" />
            PMI System
          </span>
          <span className="inline-flex items-center gap-1.5 bg-emerald-500/12 text-emerald-400 px-2.5 py-0.5 rounded-full text-xs font-bold">
            <BarChart3 className="w-3 h-3" />
            Era-Adjusted
          </span>
          <span className="inline-flex items-center gap-1.5 bg-amber-500/12 text-amber-400 px-2.5 py-0.5 rounded-full text-xs font-bold">
            <Users className="w-3 h-3" />
            78 Seasons
          </span>
        </motion.div>

        {/* Search bar */}
        <motion.div
          initial={{ opacity: 0, y: 10 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.5, delay: 0.25 }}
          className="mt-6 max-w-md mx-auto relative"
        >
          <div className="relative">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-muted-foreground" />
            <input
              type="text"
              value={query}
              onChange={(e) => handleSearch(e.target.value)}
              placeholder="Search any player..."
              className="w-full pl-10 pr-4 py-2.5 rounded-lg bg-card border border-border text-base placeholder:text-muted-foreground focus:outline-none focus:ring-2 focus:ring-primary/50 focus:border-primary transition-all"
            />
          </div>
          {suggestions.length > 0 && (
            <div className="absolute z-50 mt-1 w-full bg-card border border-border rounded-lg shadow-lg overflow-hidden">
              {suggestions.map((p) => (
                <button
                  key={p.bbref_id}
                  onClick={() => goToPlayer(p.bbref_id)}
                  className="w-full flex items-center gap-3 px-4 py-2.5 text-left hover:bg-muted/30 transition-colors text-base"
                >
                  <img
                    src={`https://cdn.nba.com/headshots/nba/latest/260x190/${p.nba_api_id}.png`}
                    alt={p.full_name}
                    className="w-8 h-8 rounded-full object-cover bg-muted"
                    onError={(e) => {
                      (e.target as HTMLImageElement).src = "/placeholder.svg";
                    }}
                  />
                  <div>
                    <span className="font-semibold">{p.full_name}</span>
                    <span className="text-muted-foreground ml-2 text-sm">
                      {p.position} · {p.years}
                    </span>
                  </div>
                  <span className="ml-auto text-sm font-bold stat-mono text-amber-400 font-bold">
                    {p.pmi >= 0 ? "+" : ""}{p.pmi.toFixed(2)}
                  </span>
                </button>
              ))}
            </div>
          )}
        </motion.div>

        <motion.div
          initial={{ opacity: 0, y: 10 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.5, delay: 0.35 }}
          className="mt-5 flex gap-3 justify-center"
        >
          <Link to="/leaderboard">
            <Button size="sm" className="font-bold px-6 text-sm">
              Explore Leaderboard
            </Button>
          </Link>
          <Link to="/players">
            <Button
              variant="outline"
              size="sm"
              className="font-bold px-6 text-sm border-border text-muted-foreground hover:text-foreground"
            >
              Browse Players
            </Button>
          </Link>
        </motion.div>
      </div>
    </section>
  );
};

export default HeroSection;
