import { motion } from "framer-motion";
import { Zap, Shield, Target, Flame, Trophy } from "lucide-react";

const metrics = [
  {
    key: "PMI",
    label: "Player Metric Index",
    desc: "Composite impact rating combining offense and defense on a standardized, era-adjusted scale.",
    scale: "~0 to +12",
    icon: Zap,
    color: "from-indigo-500 to-violet-500",
    bgGlow: "bg-indigo-500/10",
    borderColor: "border-indigo-500/25",
  },
  {
    key: "OPMI",
    label: "Offensive PMI",
    desc: "Scoring efficiency, volume, and playmaking contribution weighted by position.",
    scale: "~0 to +10",
    icon: Target,
    color: "from-amber-400 to-orange-500",
    bgGlow: "bg-amber-500/10",
    borderColor: "border-amber-500/25",
  },
  {
    key: "DPMI",
    label: "Defensive PMI",
    desc: "Defensive stocks, rebounding, and ML-imputed metrics for pre-1973 seasons.",
    scale: "~0 to +5",
    icon: Shield,
    color: "from-emerald-400 to-teal-500",
    bgGlow: "bg-emerald-500/10",
    borderColor: "border-emerald-500/25",
  },
  {
    key: "CPMI",
    label: "Clutch PMI",
    desc: "Performance in the last 5 minutes of games within ±5 points. Who shows up when it matters.",
    scale: "~0 to +11",
    icon: Flame,
    color: "from-rose-400 to-pink-500",
    bgGlow: "bg-rose-500/10",
    borderColor: "border-rose-500/25",
  },
  {
    key: "AWC",
    label: "Accumulated Win Contribution",
    desc: "Career-cumulative value: PMI × minutes played × league constant. Rewards longevity and sustained excellence.",
    scale: "Cumulative",
    icon: Trophy,
    color: "from-sky-400 to-blue-500",
    bgGlow: "bg-sky-500/10",
    borderColor: "border-sky-500/25",
  },
];

const PMIExplainer = () => {
  return (
    <section className="container mx-auto px-4 pb-14">
      <motion.div
        initial={{ opacity: 0, y: 12 }}
        whileInView={{ opacity: 1, y: 0 }}
        viewport={{ once: true }}
        transition={{ duration: 0.5 }}
      >
        <h2 className="text-2xl font-bold tracking-tight mb-1">
          The PMI System
        </h2>
        <p className="text-base text-muted-foreground mb-5 max-w-2xl">
          Five proprietary metrics built on era-adjusted z-scores, position-interpolated
          regression coefficients, and machine learning for historical gap-filling.
        </p>
      </motion.div>

      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-5 gap-3">
        {metrics.map((m, i) => {
          const Icon = m.icon;
          return (
            <motion.div
              key={m.key}
              initial={{ opacity: 0, y: 16 }}
              whileInView={{ opacity: 1, y: 0 }}
              viewport={{ once: true }}
              transition={{ duration: 0.4, delay: i * 0.07 }}
              className={`relative overflow-hidden rounded-lg border ${m.borderColor} ${m.bgGlow} p-4 group hover:scale-[1.02] transition-transform`}
            >
              <div className={`inline-flex items-center justify-center w-8 h-8 rounded-md bg-gradient-to-br ${m.color} mb-2`}>
                <Icon className="w-4 h-4 text-white" />
              </div>
              <div className="flex items-baseline gap-2 mb-1">
                <h3 className="text-base font-bold">{m.key}</h3>
                <span className="text-xs text-muted-foreground stat-mono font-medium">{m.scale}</span>
              </div>
              <p className="text-base text-muted-foreground leading-relaxed">
                {m.desc}
              </p>
            </motion.div>
          );
        })}
      </div>
    </section>
  );
};

export default PMIExplainer;
