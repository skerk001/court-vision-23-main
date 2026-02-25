import { Link } from "react-router-dom";
import { motion } from "framer-motion";
import PlayerAvatar from "@/components/shared/PlayerAvatar";
import { MOCK_PLAYERS } from "@/lib/mockData";
import { Swords } from "lucide-react";

const SpotlightComparison = () => {
  const mj = MOCK_PLAYERS[0];
  const lbj = MOCK_PLAYERS[1];

  return (
    <section className="container mx-auto px-4 pb-10">
      <motion.div
        initial={{ opacity: 0, y: 12 }}
        whileInView={{ opacity: 1, y: 0 }}
        viewport={{ once: true }}
        transition={{ duration: 0.4 }}
      >
        <Link to="/compare" className="block group">
          <div className="bg-card rounded-lg border border-border p-5 flex flex-col md:flex-row items-center justify-between gap-4 hover:border-primary/40 transition-colors">
            {/* Player 1 */}
            <div className="flex items-center gap-3 flex-1">
              <PlayerAvatar nbaApiId={mj.nba_api_id} name={mj.full_name} size="md" />
              <div>
                <p className="text-base font-bold">{mj.full_name}</p>
                <p className="text-sm text-muted-foreground">{mj.position} · {mj.years}</p>
              </div>
              <div className="ml-auto text-right">
                <p className="text-xl font-bold stat-mono text-amber-400">
                  +{mj.pmi.toFixed(2)}
                </p>
                <p className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">PMI</p>
              </div>
            </div>

            <div className="flex items-center gap-2 text-muted-foreground">
              <Swords className="w-4 h-4" />
              <span className="text-sm font-bold uppercase tracking-wider group-hover:text-primary transition-colors">
                Compare
              </span>
            </div>

            {/* Player 2 */}
            <div className="flex items-center gap-3 flex-1">
              <div className="mr-auto text-left">
                <p className="text-xl font-bold stat-mono text-amber-400">
                  +{lbj.pmi.toFixed(2)}
                </p>
                <p className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">PMI</p>
              </div>
              <div className="text-right">
                <p className="text-base font-bold">{lbj.full_name}</p>
                <p className="text-sm text-muted-foreground">{lbj.position} · {lbj.years}</p>
              </div>
              <PlayerAvatar nbaApiId={lbj.nba_api_id} name={lbj.full_name} size="md" />
            </div>
          </div>
        </Link>
      </motion.div>
    </section>
  );
};

export default SpotlightComparison;
