import { useState } from "react";
import { cn } from "@/lib/utils";

interface PlayerAvatarProps {
  nbaApiId: number | null;
  name: string;
  size?: "sm" | "md" | "lg";
}

const sizeStyles = {
  sm: { img: "w-7 h-[21px]", fallback: "w-7 h-7 text-[9px]" },
  md: { img: "w-16 h-12", fallback: "w-12 h-12 text-sm" },
  lg: { img: "w-32 h-24", fallback: "w-24 h-24 text-2xl" },
};

const PlayerAvatar = ({ nbaApiId, name, size = "md" }: PlayerAvatarProps) => {
  const [error, setError] = useState(false);
  const s = sizeStyles[size];
  const initials = name
    .split(" ")
    .map((n) => n[0])
    .join("")
    .slice(0, 2)
    .toUpperCase();

  if (error || !nbaApiId) {
    return (
      <div
        className={cn(
          s.fallback,
          "rounded bg-muted flex items-center justify-center font-semibold text-muted-foreground shrink-0"
        )}
      >
        {initials}
      </div>
    );
  }

  return (
    <img
      src={`https://cdn.nba.com/headshots/nba/latest/260x190/${nbaApiId}.png`}
      alt={name}
      className={cn(s.img, "object-cover rounded bg-muted shrink-0")}
      onError={() => setError(true)}
      loading="lazy"
    />
  );
};

export default PlayerAvatar;
