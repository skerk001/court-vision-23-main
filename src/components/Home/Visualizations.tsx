import { useState } from "react";
import { motion } from "framer-motion";
import {
  RadarChart,
  PolarGrid,
  PolarAngleAxis,
  PolarRadiusAxis,
  Radar,
  ResponsiveContainer,
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Cell,
} from "recharts";
import { MOCK_PLAYERS, MOCK_PLAYERS_PLAYOFFS } from "@/lib/mockData";

const COLORS = [
  "#5865F2", // blurple
  "#57F287", // discord green
  "#FEE75C", // discord yellow
  "#EB459E", // discord fuchsia
  "#5BC0EB", // sky
];

const TOP_5 = [...MOCK_PLAYERS]
  .sort((a, b) => b.pmi - a.pmi)
  .slice(0, 5);

// Normalize each stat to 0-100 scale for radar
function normalize(val: number, min: number, max: number) {
  if (max === min) return 50;
  return ((val - min) / (max - min)) * 100;
}

const radarStats = ["ppg", "rpg", "apg", "spg", "bpg", "ts_pct"];
const radarLabels: Record<string, string> = {
  ppg: "PPG",
  rpg: "RPG",
  apg: "APG",
  spg: "SPG",
  bpg: "BPG",
  ts_pct: "TS%",
};

// Calculate min/max for normalization
const ranges: Record<string, { min: number; max: number }> = {};
radarStats.forEach((s) => {
  const vals = TOP_5.map((p) => p[s]).filter((v) => v != null);
  ranges[s] = { min: Math.min(...vals), max: Math.max(...vals) };
});

const radarData = radarStats.map((stat) => {
  const entry: Record<string, any> = { stat: radarLabels[stat] };
  TOP_5.forEach((p, i) => {
    entry[p.full_name] = normalize(p[stat] ?? 0, ranges[stat].min * 0.8, ranges[stat].max * 1.05);
  });
  return entry;
});

// Bar chart data for PMI breakdown
const pmiBarData = TOP_5.map((p) => ({
  name: p.full_name.split(" ").pop(),
  OPMI: p.opmi,
  DPMI: p.dpmi,
  fullName: p.full_name,
}));

const Visualizations = () => {
  const [vizTab, setVizTab] = useState<"radar" | "pmi">("radar");

  return (
    <section className="container mx-auto px-4 pb-14">
      <motion.div
        initial={{ opacity: 0, y: 12 }}
        whileInView={{ opacity: 1, y: 0 }}
        viewport={{ once: true }}
        transition={{ duration: 0.5 }}
      >
        <h2 className="text-2xl font-bold tracking-tight mb-1">
          Visualize the Greats
        </h2>
        <p className="text-base text-muted-foreground mb-4">
          Top 5 all-time PMI players — interactive visual breakdowns.
        </p>
      </motion.div>

      {/* Tabs */}
      <div className="flex gap-1.5 mb-5">
        <button
          onClick={() => setVizTab("radar")}
          className={`px-3.5 py-1.5 rounded-md text-sm font-semibold transition-all ${
            vizTab === "radar"
              ? "bg-primary text-primary-foreground"
              : "bg-card border border-border text-muted-foreground hover:text-foreground"
          }`}
        >
          Skill Radar
        </button>
        <button
          onClick={() => setVizTab("pmi")}
          className={`px-3.5 py-1.5 rounded-md text-sm font-semibold transition-all ${
            vizTab === "pmi"
              ? "bg-primary text-primary-foreground"
              : "bg-card border border-border text-muted-foreground hover:text-foreground"
          }`}
        >
          PMI Breakdown
        </button>
      </div>

      <motion.div
        key={vizTab}
        initial={{ opacity: 0, y: 12 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: 0.4 }}
        className="bg-card rounded-lg border border-border p-4 md:p-6"
      >
        {vizTab === "radar" ? (
          <div className="flex flex-col lg:flex-row items-center gap-4">
            {/* Radar */}
            <div className="w-full lg:w-2/3 h-[340px]">
              <ResponsiveContainer width="100%" height="100%">
                <RadarChart data={radarData} cx="50%" cy="50%" outerRadius="75%">
                  <PolarGrid stroke="hsl(220 7% 28%)" strokeOpacity={0.4} />
                  <PolarAngleAxis
                    dataKey="stat"
                    tick={{ fontSize: 13, fill: "hsl(210 10% 85%)", fontWeight: 700 }}
                  />
                  <PolarRadiusAxis tick={false} axisLine={false} domain={[0, 100]} />
                  {TOP_5.map((p, i) => (
                    <Radar
                      key={p.bbref_id}
                      name={p.full_name}
                      dataKey={p.full_name}
                      stroke={COLORS[i]}
                      fill={COLORS[i]}
                      fillOpacity={0.08}
                      strokeWidth={2}
                    />
                  ))}
                </RadarChart>
              </ResponsiveContainer>
            </div>

            {/* Legend */}
            <div className="w-full lg:w-1/3 space-y-2">
              {TOP_5.map((p, i) => (
                <div key={p.bbref_id} className="flex items-center gap-2.5">
                  <div
                    className="w-3 h-3 rounded-full shrink-0"
                    style={{ backgroundColor: COLORS[i] }}
                  />
                  <span className="text-sm font-semibold flex-1">{p.full_name}</span>
                  <span className="text-sm stat-mono text-amber-400 font-bold">
                    +{p.pmi.toFixed(2)}
                  </span>
                </div>
              ))}
            </div>
          </div>
        ) : (
          /* PMI Stacked Bar */
          <div className="h-[340px]">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={pmiBarData} layout="vertical" barGap={4}>
                <CartesianGrid
                  strokeDasharray="3 3"
                  stroke="hsl(220 7% 28%)"
                  strokeOpacity={0.3}
                  horizontal={false}
                />
                <XAxis
                  type="number"
                  tick={{ fontSize: 13, fill: "hsl(210 10% 85%)", fontWeight: 600 }}
                  domain={[0, "auto"]}
                />
                <YAxis
                  type="category"
                  dataKey="name"
                  tick={{ fontSize: 13, fill: "hsl(210 10% 85%)", fontWeight: 700 }}
                  width={85}
                />
                <Tooltip
                  contentStyle={{
                    backgroundColor: "hsl(220 8% 10%)",
                    border: "1px solid hsl(220 7% 25%)",
                    borderRadius: "8px",
                    fontSize: 14, fontWeight: 500,
                  }}
                  labelStyle={{ color: "hsl(210 40% 98%)", fontWeight: 700 }}
                  formatter={(val: number, name: string) => [
                    "+" + val.toFixed(2),
                    name,
                  ]}
                  labelFormatter={(label, payload) => {
                    if (payload?.[0]?.payload?.fullName) return payload[0].payload.fullName;
                    return label;
                  }}
                />
                <Bar dataKey="OPMI" stackId="a" radius={[0, 0, 0, 0]} name="Offensive PMI">
                  {pmiBarData.map((_, i) => (
                    <Cell key={i} fill={COLORS[i]} fillOpacity={0.85} />
                  ))}
                </Bar>
                <Bar dataKey="DPMI" stackId="a" radius={[0, 4, 4, 0]} name="Defensive PMI">
                  {pmiBarData.map((_, i) => (
                    <Cell key={i} fill={COLORS[i]} fillOpacity={0.45} />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
            <div className="flex justify-center gap-6 mt-2">
              <span className="flex items-center gap-1.5 text-sm font-semibold text-muted-foreground">
                <span className="w-3 h-3 rounded-sm bg-orange-500/85 inline-block" />
                OPMI (Offensive)
              </span>
              <span className="flex items-center gap-1.5 text-sm font-semibold text-muted-foreground">
                <span className="w-3 h-3 rounded-sm bg-orange-500/45 inline-block" />
                DPMI (Defensive)
              </span>
            </div>
          </div>
        )}
      </motion.div>
    </section>
  );
};

export default Visualizations;
