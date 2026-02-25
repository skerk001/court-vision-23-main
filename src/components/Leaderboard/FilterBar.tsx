import { cn } from "@/lib/utils";

interface FilterBarProps {
  seasonType: string;
  onSeasonTypeChange: (v: string) => void;
  era: string;
  onEraChange: (v: string) => void;
  minGp: number;
  onMinGpChange: (v: number) => void;
  scope: string;
  onScopeChange: (v: string) => void;
  showScope: boolean;
}

const ToggleGroup = ({
  options,
  value,
  onChange,
}: {
  options: { value: string; label: string }[];
  value: string;
  onChange: (v: string) => void;
}) => (
  <div className="flex rounded-md overflow-hidden border border-border">
    {options.map((opt) => (
      <button
        key={opt.value}
        onClick={() => onChange(opt.value)}
        className={cn(
          "px-3 py-1.5 text-xs font-medium transition-colors",
          value === opt.value
            ? "bg-court-orange text-white"
            : "bg-card text-muted-foreground hover:text-foreground hover:bg-muted/50"
        )}
      >
        {opt.label}
      </button>
    ))}
  </div>
);

const FilterBar = ({
  seasonType,
  onSeasonTypeChange,
  era,
  onEraChange,
  minGp,
  onMinGpChange,
  scope,
  onScopeChange,
  showScope,
}: FilterBarProps) => {
  return (
    <div className="flex flex-wrap gap-3 items-center">
      <ToggleGroup
        options={[
          { value: "regular", label: "Regular Season" },
          { value: "playoffs", label: "Playoffs" },
        ]}
        value={seasonType}
        onChange={onSeasonTypeChange}
      />

      <ToggleGroup
        options={[
          { value: "all", label: "All" },
          { value: "active", label: "Active" },
          { value: "retired", label: "Retired" },
        ]}
        value={era}
        onChange={onEraChange}
      />

      <select
        value={minGp}
        onChange={(e) => onMinGpChange(Number(e.target.value))}
        className="bg-card border border-border rounded-md px-3 py-1.5 text-xs text-muted-foreground focus:outline-none focus:ring-1 focus:ring-ring"
      >
        <option value={0}>No Minimum</option>
        <option value={25}>25+ GP</option>
        <option value={50}>50+ GP (Default)</option>
        <option value={100}>100+ GP</option>
        <option value={200}>200+ GP</option>
        <option value={400}>400+ GP</option>
      </select>

      {showScope && (
        <select
          value={scope}
          onChange={(e) => onScopeChange(e.target.value)}
          className="bg-card border border-border rounded-md px-3 py-1.5 text-xs text-muted-foreground focus:outline-none focus:ring-1 focus:ring-ring"
        >
          <option value="career">Career</option>
          <option value="2024-25">2024-25</option>
          <option value="2023-24">2023-24</option>
          <option value="2022-23">2022-23</option>
        </select>
      )}
    </div>
  );
};

export default FilterBar;
