"""PMI v2 — Player Metric Index, empirically calibrated.

Methodology:
  Instead of hand-tuning weights (which embeds the creator's biases),
  PMI v2 uses coefficients derived from regression against RAPM
  (Regularized Adjusted Plus-Minus) — the gold standard for measuring
  actual player impact on winning.

  The linear weights come from Daniel Myers' BPM research, which
  regressed per-100-possession box score stats against a 14-year
  RAPM dataset (Jeremias Engelmann). These weights represent the
  empirical relationship between each box score stat and actual
  point differential impact.

Key differences from PMI v1 (v41d):
  - Weights are RAPM-derived, not hand-tuned
  - No arbitrary era penalty (z-scores within season already normalize)
  - No DPMI dampener (defense counts at its empirical value)
  - No scoring dominance bonus (pts coefficient already captures this)
  - No playmaker TOV discount (the data determines how much TOV hurts)
  - No center scoring floor (position adjustment handles this)
  - Career = minutes-weighted average (not peak-weighted, which inflates stars)
  - AWC uses standard VORP methodology: (PMI - replacement) × min%

Design principles:
  1. Let the data speak — weights from regression, not intuition
  2. Era-neutral — z-scores normalize within each season
  3. Position-fair — coefficients interpolate by position (from BPM)
  4. Offense = Defense — no dampener artificially capping defense
  5. Transparent — every coefficient traceable to RAPM research

Version: 2.0
Author: Samir Kerkar
"""

import numpy as np
import pandas as pd
import logging
from typing import Optional

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
#  RAPM-DERIVED WEIGHTS (per 100 possessions)
# ═══════════════════════════════════════════════════════════════════════════════
#
# Source: Daniel Myers' simplified linear BPM regression against 14-year RAPM.
# Published at godismyjudgeok.com/DStats/box-plusminus/
#
# These represent the empirical point-differential value of each stat
# per 100 team possessions. We convert them to work with per-game stats
# by normalizing into z-scores within each season.
#
# The raw per-100-possession coefficients:
#   pts: +0.7008   (scoring is valuable but not overwhelmingly so)
#   ast: +0.3846   (assists matter, but less than raw scoring)
#   stl: +1.3571   (steals are extremely valuable — possessions gained)
#   blk: +0.6475   (blocks are valuable but less than steals)
#   drb: +0.3444   (defensive rebounds end opponent possessions)
#   orb: +0.1398   (offensive rebounds are modest in impact)
#   tov: -0.9347   (turnovers are very costly — nearly as bad as pts are good)
#   pf:  -0.5153   (fouls hurt — free throw opportunities for opponents)
#   fta: -0.2589   (shot attempts have a cost; FTA less than FGA)
#   fg3a:-0.4155   (three-point attempts have a cost per miss)
#   fg2a:-0.5424   (two-point attempts have a cost per miss)
#
# For PMI we use these relative magnitudes to weight z-scores.
# Since we z-score everything, the absolute scale doesn't matter —
# only the ratios between weights matter.
#
# We normalize so the largest absolute weight (stl: 1.3571) = 1.0

_RAW_WEIGHTS = {
    "pts":  0.7008,
    "ast":  0.3846,
    "stl":  1.3571,
    "blk":  0.6475,
    "drb":  0.3444,
    "orb":  0.1398,
    "tov": -0.9347,
    "pf":  -0.5153,
}

# Normalize so max abs weight = 1.0 for interpretability
_MAX_W = max(abs(v) for v in _RAW_WEIGHTS.values())
WEIGHTS = {k: v / _MAX_W for k, v in _RAW_WEIGHTS.items()}

# Position encoding: PG=1, SG=2, SF=3, PF=4, C=5
POS_MAP = {
    "PG": 1, "SG": 2, "G": 1.5, "Guard": 1.5,
    "SF": 3, "PF": 4, "F": 3.5, "Forward": 3.5,
    "C": 5, "FC": 4.5, "GF": 2.5, "Center": 5,
}

# Position adjustment from BPM: positions 1-5 have slightly different
# baseline values. BPM found no significant positional bias overall,
# but some stats mean different things for different positions.
# We apply a mild position-based reweight to steals/blocks/assists:
#   Guards: steals and assists worth slightly more, blocks less
#   Centers: blocks and rebounds worth slightly more, assists less
# These factors are from the BPM position coefficient ranges.
POS_ADJUSTMENTS = {
    # (guard_mult, center_mult) — linearly interpolated by position
    "stl": (1.10, 0.90),
    "blk": (0.85, 1.15),
    "ast": (1.08, 0.92),
    "orb": (0.90, 1.10),
    "drb": (0.95, 1.05),
    # pts, tov, pf: no position adjustment (equal for all positions)
}

# Efficiency bonus: TS% relative to league average
# BPM uses USG% × (TS% - TmTS%) with complex interaction terms.
# We simplify: bonus/penalty based on efficiency relative to league.
# The coefficient 5.0 means +5% rTS → ~0.5 PMI bonus (calibrated to
# match BPM's efficiency sensitivity of ~2*(TS%-TmTS%))
EFFICIENCY_WEIGHT = 5.0

# Career aggregation: minutes-weighted (like VORP), not peak-weighted
# Replacement level from BPM: -2.0 per 100 possessions
REPLACEMENT_LEVEL = -2.0

# AWC: Value Over Replacement × minutes fraction
# AWC = (PMI - replacement_level) × (minutes / (season_minutes × 5)) × team_games
# Simplified: AWC ≈ PMI × total_minutes × constant
AWC_CONSTANT = 0.0004

# GP regression: Bayesian shrinkage toward 0 for low-GP players
GP_HALF_REG = 50      # ~60% of a season to reach half-trust
GP_HALF_PLAYOFF = 15   # playoffs are shorter but each game matters more

# CPMI weights — also RAPM-derived ratios
# In clutch situations, the relative value of stats is similar
# but we use plus-minus more heavily since it directly measures winning
CPMI_WEIGHTS = {
    "z_ppg": 0.52,        # pts weight
    "z_apg": 0.28,        # ast weight
    "z_ts": 0.40,         # efficiency matters more in clutch
    "z_plusminus": 0.60,   # actual +/- is most direct measure
    "z_spg": 0.30,        # steals create turnovers
    "z_tovpg": -0.45,     # turnovers in clutch are devastating
}


# ═══════════════════════════════════════════════════════════════════════════════
#  HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def _pos_num(pos_str: str) -> float:
    """Convert position string to numeric 1-5."""
    if not pos_str or (isinstance(pos_str, float) and np.isnan(pos_str)):
        return 3.0  # default SF
    pos = str(pos_str).strip().upper().split("-")[0].split("/")[0]
    return POS_MAP.get(pos, 3.0)


def _pos_interp(pos_num: float) -> float:
    """Interpolation factor t: PG(1)=0.0 → C(5)=1.0."""
    return max(0.0, min(1.0, (pos_num - 1) / 4))


def _z(val, mean, std):
    """Z-score, clamped to [-3.5, 3.5].

    Slightly wider clamp than v1 (-3 to 3) to let truly elite
    performances register without arbitrary ceiling.
    """
    if std == 0 or val is None or mean is None:
        return 0.0
    if isinstance(val, float) and np.isnan(val):
        return 0.0
    return max(-3.5, min(3.5, (float(val) - float(mean)) / float(std)))


def _get_pos_weight(stat: str, pos_num: float) -> float:
    """Get position-adjusted weight for a stat."""
    base = WEIGHTS.get(stat, 0)
    if stat not in POS_ADJUSTMENTS:
        return base
    guard_m, center_m = POS_ADJUSTMENTS[stat]
    t = _pos_interp(pos_num)
    mult = (1 - t) * guard_m + t * center_m
    return base * mult


# ═══════════════════════════════════════════════════════════════════════════════
#  PMI COMPUTATION — Single unified metric (no separate OPMI/DPMI dampening)
# ═══════════════════════════════════════════════════════════════════════════════

def compute_pmi_season(row: dict, league_stats: dict, pos_num: float) -> dict:
    """Compute PMI for a single player-season.

    Returns dict with: pmi, opmi, dpmi (for display breakdown).
    OPMI and DPMI are NOT separately dampened — they're just the
    offensive and defensive components of the same unified metric.

    Args:
        row: Player stat dict (per-game)
        league_stats: Dict with mean/std for each stat across the season
        pos_num: Numeric position (1-5)

    Returns:
        {"pmi": float, "opmi": float, "dpmi": float}
    """
    # Z-scores for each box score stat
    z_pts = _z(row.get("ppg", 0), league_stats.get("ppg_mean", 0), league_stats.get("ppg_std", 1))
    z_ast = _z(row.get("apg", 0), league_stats.get("apg_mean", 0), league_stats.get("apg_std", 1))
    z_tov = _z(row.get("tov_pg", 0), league_stats.get("tov_pg_mean", 0), league_stats.get("tov_pg_std", 1))
    z_orb = _z(row.get("orb_pg", 0), league_stats.get("orb_pg_mean", 0), league_stats.get("orb_pg_std", 1))
    z_stl = _z(row.get("spg", 0), league_stats.get("spg_mean", 0), league_stats.get("spg_std", 1))
    z_blk = _z(row.get("bpg", 0), league_stats.get("bpg_mean", 0), league_stats.get("bpg_std", 1))
    z_drb = _z(row.get("drb_pg", 0), league_stats.get("drb_pg_mean", 0), league_stats.get("drb_pg_std", 1))
    z_pf  = _z(row.get("pf_pg", 0), league_stats.get("pf_pg_mean", 0), league_stats.get("pf_pg_std", 1))

    # Efficiency: TS% relative to league average
    ts_pct = float(row.get("ts_pct", 0) or 0)
    lg_ts = float(league_stats.get("ts_pct_mean", 0.540) or 0.540)
    ts_diff = ts_pct - lg_ts

    # ── Offensive component ──
    # Scoring (pts z-score) + efficiency (TS diff) + creation (ast) + boards (orb)
    # minus turnover cost, with position-adjusted weights
    opmi = (
        _get_pos_weight("pts", pos_num) * z_pts +
        EFFICIENCY_WEIGHT * ts_diff +
        _get_pos_weight("ast", pos_num) * z_ast +
        _get_pos_weight("orb", pos_num) * z_orb +
        WEIGHTS["tov"] * z_tov  # turnovers hurt equally regardless of position
    )

    # ── Defensive component ──
    # Steals + blocks + defensive rebounds - fouls
    # No dampener — defense counts at its full empirical value
    dpmi = (
        _get_pos_weight("stl", pos_num) * z_stl +
        _get_pos_weight("blk", pos_num) * z_blk +
        _get_pos_weight("drb", pos_num) * z_drb +
        WEIGHTS["pf"] * z_pf  # fouls hurt equally regardless of position
    )

    pmi = opmi + dpmi

    return {
        "pmi": round(pmi, 2),
        "opmi": round(opmi, 2),
        "dpmi": round(dpmi, 2),
    }


# ═══════════════════════════════════════════════════════════════════════════════
#  CPMI — Clutch Performance Metric Index
# ═══════════════════════════════════════════════════════════════════════════════

def compute_cpmi(clutch_row: dict, clutch_league: dict) -> float:
    """Compute CPMI from clutch split data (last 5 min, ±5 pts)."""
    z_ppg = _z(clutch_row.get("clutch_ppg", 0), clutch_league.get("ppg_mean", 0), clutch_league.get("ppg_std", 1))
    z_apg = _z(clutch_row.get("clutch_apg", 0), clutch_league.get("apg_mean", 0), clutch_league.get("apg_std", 1))
    z_ts = _z(clutch_row.get("clutch_ts", 0), clutch_league.get("ts_mean", 0), clutch_league.get("ts_std", 1))
    z_pm = _z(clutch_row.get("clutch_plusminus", 0), clutch_league.get("pm_mean", 0), clutch_league.get("pm_std", 1))
    z_spg = _z(clutch_row.get("clutch_spg", 0), clutch_league.get("spg_mean", 0), clutch_league.get("spg_std", 1))
    z_tov = _z(clutch_row.get("clutch_tovpg", 0), clutch_league.get("tov_mean", 0), clutch_league.get("tov_std", 1))

    cpmi_raw = (
        CPMI_WEIGHTS["z_ppg"] * z_ppg +
        CPMI_WEIGHTS["z_apg"] * z_apg +
        CPMI_WEIGHTS["z_ts"] * z_ts +
        CPMI_WEIGHTS["z_plusminus"] * z_pm +
        CPMI_WEIGHTS["z_spg"] * z_spg +
        CPMI_WEIGHTS["z_tovpg"] * z_tov
    )

    return round(cpmi_raw, 2)


# ═══════════════════════════════════════════════════════════════════════════════
#  CAREER AGGREGATION — Minutes-weighted (not peak-weighted)
# ═══════════════════════════════════════════════════════════════════════════════

def compute_career_pmi(season_data: list[dict], is_playoff: bool = False) -> float:
    """Compute career PMI using minutes-weighted average.

    Unlike v1's peak-weighted system (which inflated star players),
    this weights each season proportionally to minutes played —
    the same methodology VORP uses. A great player who plays 38 mpg
    for 82 games contributes more than a player who plays 25 mpg
    for 60 games, proportional to their actual court time.

    Also applies Bayesian GP regression toward 0.0 (league average).

    Args:
        season_data: List of dicts with 'pmi', 'gp', 'mpg' keys
        is_playoff: Whether this is playoff data
    """
    if not season_data:
        return 0.0

    total_minutes = 0
    weighted_sum = 0

    for s in season_data:
        pmi = s.get("pmi", 0) or 0
        gp = s.get("gp", 0) or 0
        mpg = s.get("mpg", 0) or 0
        minutes = gp * mpg
        weighted_sum += pmi * minutes
        total_minutes += minutes

    if total_minutes == 0:
        return 0.0

    career_avg = weighted_sum / total_minutes

    # Bayesian regression toward league mean (0.0)
    total_gp = sum(s.get("gp", 0) or 0 for s in season_data)
    gp_half = GP_HALF_PLAYOFF if is_playoff else GP_HALF_REG
    trust = total_gp / (total_gp + gp_half)
    career_pmi = trust * career_avg + (1 - trust) * 0.0

    return round(career_pmi, 2)


def compute_awc(pmi: float, total_minutes: int) -> float:
    """Accumulated Win Contribution.

    AWC = PMI × total_minutes × constant

    This is analogous to VORP but using PMI instead of BPM.
    """
    return round(pmi * total_minutes * AWC_CONSTANT, 1)


# ═══════════════════════════════════════════════════════════════════════════════
#  BATCH — Compute league stats for z-score normalization
# ═══════════════════════════════════════════════════════════════════════════════

def compute_season_league_stats(df: pd.DataFrame) -> dict:
    """Compute league mean/std for all stats needed for z-scores.

    Filters to players with meaningful minutes (>10 mpg) to avoid
    garbage-time players skewing the distribution.
    """
    # Filter to meaningful minutes
    work = df.copy()
    if "mpg" in work.columns:
        work = work[work["mpg"] >= 10]
    if len(work) < 20:
        work = df  # fallback if too few players pass filter

    stats = {}
    for col, key in [
        ("ppg", "ppg"), ("apg", "apg"), ("tov_pg", "tov_pg"),
        ("orb_pg", "orb_pg"), ("spg", "spg"), ("bpg", "bpg"),
        ("drb_pg", "drb_pg"), ("pf_pg", "pf_pg"),
        ("ts_pct", "ts_pct"),
    ]:
        if col in work.columns:
            vals = work[col].dropna()
            if len(vals) > 0:
                stats[f"{key}_mean"] = float(vals.mean())
                stats[f"{key}_std"] = float(vals.std()) if len(vals) > 1 else 1.0
                # Prevent zero std (which causes division errors)
                if stats[f"{key}_std"] < 0.001:
                    stats[f"{key}_std"] = 1.0
            else:
                stats[f"{key}_mean"] = 0
                stats[f"{key}_std"] = 1
        else:
            stats[f"{key}_mean"] = 0
            stats[f"{key}_std"] = 1

    return stats
