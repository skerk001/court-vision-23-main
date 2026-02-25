import { motion } from "framer-motion";
import { Database, BarChart2, Brain, LineChart } from "lucide-react";

const steps = [
  {
    icon: Database,
    title: "Raw Box Scores",
    desc: "50,000+ player-seasons from 1946–2025",
    color: "from-indigo-500 to-violet-500",
  },
  {
    icon: BarChart2,
    title: "Era-Adjusted Z-Scores",
    desc: "Normalize within each season for fair cross-era comparison",
    color: "from-amber-400 to-orange-500",
  },
  {
    icon: Brain,
    title: "ML Gap-Filling",
    desc: "Impute missing pre-1973 defensive stats via trained models",
    color: "from-emerald-400 to-teal-500",
  },
  {
    icon: LineChart,
    title: "Position-Weighted PMI",
    desc: "Interpolated coefficients produce the final composite rating",
    color: "from-sky-400 to-blue-500",
  },
];

const HowItWorks = () => (
  <section className="container mx-auto px-4 pb-14">
    <motion.div
      initial={{ opacity: 0, y: 12 }}
      whileInView={{ opacity: 1, y: 0 }}
      viewport={{ once: true }}
      transition={{ duration: 0.5 }}
    >
      <h2 className="text-2xl font-bold tracking-tight mb-1">How It Works</h2>
      <p className="text-base text-muted-foreground mb-5 max-w-2xl">
        From raw data to unified player ratings in four steps.
      </p>
    </motion.div>

    <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-3">
      {steps.map((step, i) => {
        const Icon = step.icon;
        return (
          <motion.div
            key={step.title}
            initial={{ opacity: 0, y: 16 }}
            whileInView={{ opacity: 1, y: 0 }}
            viewport={{ once: true }}
            transition={{ duration: 0.4, delay: i * 0.08 }}
            className="relative bg-card rounded-lg border border-border p-4 group"
          >
            {/* Step number */}
            <span className="absolute top-3 right-3 text-xs stat-mono text-muted-foreground/50 font-bold">
              {String(i + 1).padStart(2, "0")}
            </span>

            <div className={`inline-flex items-center justify-center w-9 h-9 rounded-lg bg-gradient-to-br ${step.color} mb-3`}>
              <Icon className="w-4 h-4 text-white" />
            </div>
            <h3 className="text-base font-bold mb-1">{step.title}</h3>
            <p className="text-base text-muted-foreground leading-relaxed">
              {step.desc}
            </p>

            {/* Connector arrow (not on last) */}
            {i < 3 && (
              <div className="hidden lg:block absolute top-1/2 -right-2.5 w-5 text-muted-foreground/30">
                <svg viewBox="0 0 20 20" fill="currentColor" className="w-full h-full">
                  <path d="M10 3l7 7-7 7V3z" />
                </svg>
              </div>
            )}
          </motion.div>
        );
      })}
    </div>

    <motion.div
      initial={{ opacity: 0 }}
      whileInView={{ opacity: 1 }}
      viewport={{ once: true }}
      transition={{ duration: 0.5, delay: 0.4 }}
      className="mt-4 text-center"
    >
      <a
        href="/docs/PMI_Research_Paper.pdf"
        target="_blank"
        rel="noopener noreferrer"
        className="inline-flex items-center gap-1.5 text-sm text-primary hover:underline font-semibold"
      >
        Read the full methodology →
      </a>
    </motion.div>
  </section>
);

export default HowItWorks;
