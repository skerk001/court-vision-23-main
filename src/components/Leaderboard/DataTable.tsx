import { useMemo, useState, useCallback } from "react";
import { cn } from "@/lib/utils";
import { formatStat, getHeatColor } from "@/lib/formatters";
import type { ColumnGroup } from "@/lib/constants";
import PlayerAvatar from "@/components/shared/PlayerAvatar";
import { ChevronDown, ChevronUp } from "lucide-react";
import { Link } from "react-router-dom";

interface DataTableProps {
  data: any[];
  columnGroups: ColumnGroup[];
  sortKey: string;
  sortDir: "asc" | "desc";
  onSort: (key: string) => void;
}

const MEDAL_COLORS: Record<number, string> = {
  1: "bg-amber-500/10 border-l-2 border-l-amber-400",      // gold
  2: "bg-slate-400/8 border-l-2 border-l-slate-400",        // silver
  3: "bg-orange-700/8 border-l-2 border-l-orange-600",      // bronze
};

const DataTable = ({ data, columnGroups, sortKey, sortDir, onSort }: DataTableProps) => {
  const [pinnedId, setPinnedId] = useState<string | null>(null);

  const allColumns = useMemo(
    () => columnGroups.flatMap((g) => g.columns),
    [columnGroups]
  );

  const heatRanges = useMemo(() => {
    const ranges: Record<string, { min: number; max: number; maxAbs: number }> = {};
    for (const col of allColumns) {
      if (col.heatType) {
        const values = data
          .map((d) => d[col.key])
          .filter((v) => v != null && !isNaN(v));
        if (values.length > 0) {
          ranges[col.key] = {
            min: Math.min(...values),
            max: Math.max(...values),
            maxAbs: Math.max(...values.map(Math.abs)),
          };
        }
      }
    }
    return ranges;
  }, [data, allColumns]);

  const togglePin = useCallback((id: string) => {
    setPinnedId(prev => prev === id ? null : id);
  }, []);

  const stickyBg = "bg-card";
  const stickyBodyBg = "bg-background";

  return (
    <div className="overflow-x-auto rounded-lg border border-border max-h-[75vh] overflow-y-auto">
      <table className="w-full text-sm border-collapse">
        {/* ── Sticky thead ── */}
        <thead className="sticky top-0 z-40">
          {/* Group header row */}
          <tr>
            <th
              colSpan={2}
              className={cn(
                "sticky left-0 z-50 border-b border-border px-2 py-1.5",
                stickyBg
              )}
            />
            {columnGroups.map((group) => (
              <th
                key={group.label}
                colSpan={group.columns.length}
                className={cn(
                  "text-center text-[10px] font-bold uppercase tracking-widest py-1.5 border-b border-border",
                  group.colorClass
                )}
              >
                {group.label}
              </th>
            ))}
          </tr>

          {/* Column header row */}
          <tr className={stickyBg}>
            <th
              className={cn(
                "sticky left-0 z-50 w-10 text-center text-[11px] font-semibold text-muted-foreground py-2 border-b border-border",
                stickyBg
              )}
            >
              #
            </th>
            <th
              className={cn(
                "sticky left-10 z-50 text-left text-[11px] font-semibold text-muted-foreground py-2 pl-2 border-b border-border min-w-[180px]",
                stickyBg
              )}
            >
              Player
            </th>
            {allColumns.map((col) => (
              <th
                key={col.key}
                onClick={() => onSort(col.key)}
                className={cn(
                  "text-right text-[11px] font-semibold text-muted-foreground py-2 px-2 border-b border-border cursor-pointer hover:text-foreground transition-colors whitespace-nowrap select-none",
                  stickyBg,
                  sortKey === col.key && "text-foreground"
                )}
              >
                {col.label}
                {sortKey === col.key &&
                  (sortDir === "desc" ? (
                    <ChevronDown className="inline w-3 h-3 ml-0.5 -mt-0.5" />
                  ) : (
                    <ChevronUp className="inline w-3 h-3 ml-0.5 -mt-0.5" />
                  ))}
              </th>
            ))}
          </tr>
        </thead>

        <tbody>
          {data.map((player, i) => {
            const rank = player.rank ?? i + 1;
            const rowKey = (player.bbref_id || "") + "-" + (player.season || "") + "-" + i;
            const isPinned = pinnedId === rowKey;
            const medalClass = MEDAL_COLORS[rank] || "";

            return (
              <tr
                key={rowKey}
                onClick={() => togglePin(rowKey)}
                className={cn(
                  "border-b border-border/40 transition-colors cursor-pointer",
                  isPinned
                    ? "bg-primary/8 ring-1 ring-primary/30"
                    : medalClass || (i % 2 === 1 ? "bg-muted/[0.03]" : ""),
                  !isPinned && "hover:bg-foreground/[0.04]"
                )}
              >
                {/* Rank */}
                <td
                  className={cn(
                    "sticky left-0 z-10 text-center text-xs py-2 stat-mono",
                    stickyBodyBg,
                    rank <= 3 ? "font-bold text-amber-400" : "text-muted-foreground"
                  )}
                >
                  {rank}
                </td>

                {/* Player */}
                <td
                  className={cn(
                    "sticky left-10 z-10 py-1.5 pl-2 pr-4",
                    stickyBodyBg
                  )}
                >
                  <div className="flex items-center gap-2">
                    <PlayerAvatar
                      nbaApiId={player.nba_api_id}
                      name={player.full_name}
                      size="sm"
                    />
                    <div className="flex items-center gap-1.5 min-w-0">
                      <Link
                        to={`/player/${player.bbref_id}`}
                        onClick={(e) => e.stopPropagation()}
                        className="text-xs font-medium text-foreground hover:text-primary transition-colors truncate"
                      >
                        {player.full_name}
                      </Link>
                      {player.is_active && (
                        <span className="shrink-0 text-[9px] font-bold text-primary bg-primary/10 px-1 py-0.5 rounded leading-none">
                          ACT
                        </span>
                      )}
                    </div>
                  </div>
                </td>

                {/* Stat cells */}
                {allColumns.map((col) => {
                  const value = player[col.key];
                  const formatted = formatStat(value, col.format);
                  const heatBg =
                    col.heatType && value != null && !isNaN(value) && heatRanges[col.key]
                      ? getHeatColor(value, heatRanges[col.key], col.heatType)
                      : undefined;

                  const isPositivePmi =
                    (col.format === "pmi" || col.format === "rpct") &&
                    value != null &&
                    value > 0;
                  const isNegativePmi =
                    (col.format === "pmi" || col.format === "rpct") &&
                    value != null &&
                    value < 0;
                  const isSeason = col.key.includes("season") && col.format === "string";

                  return (
                    <td
                      key={col.key}
                      className="text-right text-xs py-2 px-2 stat-mono whitespace-nowrap"
                      style={heatBg ? { backgroundColor: heatBg } : undefined}
                    >
                      <span
                        className={cn(
                          value == null && "text-muted-foreground/40",
                          isPositivePmi && "text-positive",
                          isNegativePmi && "text-negative",
                          isSeason &&
                            "bg-muted/60 text-muted-foreground px-1.5 py-0.5 rounded text-[10px]"
                        )}
                      >
                        {formatted}
                      </span>
                    </td>
                  );
                })}
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
};

export default DataTable;
