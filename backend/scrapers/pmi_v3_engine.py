"""PMI v3 — Player Metric Index, multi-source empirically calibrated.

Synthesizes methodologies from:
  BPM (Daniel Myers) — 14-year RAPM regression on box score stats
  RAPTOR (FiveThirtyEight) — 6-year RAPM with tracking + on/off data
  EPM (Taylor Snarr) — 18-year RAPM with optimized skill estimates

Key insights incorporated:
  BPM:    Linear weights from regression against RAPM give us the
          empirical value of each box score stat per 100 possessions.
  RAPTOR: Points scored is the highest-weighted offensive category.
          Blocks have NO additional predictive power for defense
          once opponent FG% is accounted for. Steals are valuable
          but carry hidden costs (gambling) not captured in box score.
          Uses position-adjusted coefficients like BPM.
  EPM:    Defense is poorly captured by box score stats. The SPM
          (statistical plus-minus) prior for defense should be
          weighted less than offense. Defensive stats are noisy
          and overfit in box-score-only models.

Era adjustment philosophy:
  Per-season z-scores normalize within each season (handles pace,
  scoring environment, etc.), BUT they don't account for structural
  stat inflation caused by rule changes and recording practices:

  1. Steals were inflated ~20-30% in 1974-1990 due to:
     - More aggressive hand-checking allowed → more reaching fouls
       were instead counted as steals
     - League avg STL/G was ~1.0 in 1978 vs ~0.7 in 2020
     - Even z-scored, the DISTRIBUTION was different (higher ceiling)

  2. Blocks were inflated ~15-25% in 1974-1995 due to:
     - Goaltending interpretation more lenient
     - More paint-focused offense → more blockable shot attempts
     - League avg BLK/G was ~0.6 in 1985 vs ~0.5 in 2020

  3. Rebounds were inflated 1960-1975 due to:
     - Higher pace → more missed shots → more rebounds available
     - Smaller league → less contested rebounding
     - FG% lower → more rebound opportunities per game

  4. Scoring inflated post-2004 due to:
     - Hand-checking ban (2004-05) opened driving lanes
     - Three-point revolution (2015+) increased scoring volume
     - More possessions per game in modern era

  5. Three-point attempts: didn't exist before 1979-80

  Our approach: Apply ERA DEFLATORS to specific stats BEFORE z-scoring,
  based on the ratio of that era's league average to a reference era.
  This is more surgical than a blanket "era penalty" — it only adjusts
  stats we KNOW were inflated, and it adjusts them proportionally.

Design principles:
  1. Multi-source weights — averaged from BPM + RAPTOR + EPM research
  2. Defense discounted — box score captures ~70% of offense but ~30%
     of defense (per BPM/RAPTOR research). We scale accordingly.
  3. Era-specific stat deflation — steals, blocks, rebounds adjusted
     per historical league averages, not blanket penalties
  4. Steal discount — RAPTOR found steals overvalued in box-score
     models due to unmeasured gambling costs. We reduce from BPM's
     raw 1.36 coefficient.
  5. Minutes-weighted careers — like VORP, not peak-weighted
  6. Transparent — every choice documented with source

Version: 3.0
Author: Samir Kerkar
"""

import numpy as np
import pandas as pd
import logging
from typing import Optional

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
#  MULTI-SOURCE WEIGHTS (synthesized from BPM + RAPTOR + EPM)
# ═══════════════════════════════════════════════════════════════════════════════
#
# Raw coefficients from each source (per 100 possessions):
#
# Stat     BPM      RAPTOR(box)  EPM(SPM)   Notes
# ────────────────────────────────────────────────────────
# pts      0.70     highest-w    high       All agree: scoring matters most
# ast      0.38     moderate     moderate   Creation is valuable
# stl      1.36     moderate*    moderate   BPM overweights; RAPTOR discounts
# blk      0.65     0.00*        low        RAPTOR: no predictive value!
# drb      0.34     low          low        Uncontested DRBs ≈ worthless (RAPTOR)
# orb      0.14     moderate     moderate   Contested ORBs very valuable (RAPTOR)
# tov     -0.93     high-cost    high-cost  Universal agreement: turnovers hurt
# pf      -0.52     moderate     moderate   Fouls = opponent FTs
#
# *RAPTOR found blocks have NO additional predictive power for defense
#  once you control for opponent FG%. We reduce blocks significantly.
# *RAPTOR found steals are somewhat overvalued in pure box-score models
#  because the hidden costs of gambling for steals aren't measured.
#
# Our synthesis: We use BPM as the base (most transparent published
# weights), then adjust based on RAPTOR/EPM findings.

# Step 1: Start with BPM raw weights
_BPM_WEIGHTS = {
    "pts":  0.7008,
    "ast":  0.3846,
    "stl":  1.3571,
    "blk":  0.6475,
    "drb":  0.3444,
    "orb":  0.1398,
    "tov": -0.9347,
    "pf":  -0.5153,
}

# Step 2: Apply RAPTOR/EPM adjustments
# - Steals: BPM overweights at 1.36. RAPTOR/EPM suggest ~0.8-1.0
#   after accounting for gambling costs. We use 0.90.
# - Blocks: RAPTOR found 0 additional predictive value. But blocks
#   do indicate shot-altering ability not captured by opp FG%.
#   We keep ~40% of BPM's value → 0.26
# - DRB: RAPTOR found uncontested DRBs nearly worthless. We halve.
# - ORB: RAPTOR found contested ORBs very valuable. We boost ~50%.
# - TOV: BPM raw is -0.93, but when normalized to pts=1.0 it becomes
#   -1.33 which is too harsh for high-usage players who handle the ball
#   more (LeBron, Harden, Luka). We reduce to -0.85, and separately
#   add an AST/TOV ratio bonus that credits efficient playmakers.
_ADJUSTMENTS = {
    "stl": 0.55 / 1.3571,    # reduce from 1.36 → 0.55 (aggressive RAPTOR discount
                               # + era-inflation: steals overvalued in box-score models
                               # AND have wide z-score distributions that inflate DPMI
                               # for steal-heavy players like Butler/Kawhi/Paul)
    "blk": 0.26 / 0.6475,    # reduce from 0.65 → 0.26
    "drb": 0.17 / 0.3444,    # reduce from 0.34 → 0.17
    "orb": 0.21 / 0.1398,    # boost from 0.14 → 0.21
    "tov": 0.60 / 0.9347,    # reduce from 0.93 → 0.60 (still negative after normalization)
                               # This gives ~-0.85 normalized weight instead of -1.33
                               # The AST/TOV ratio bonus compensates for high-usage playmakers
}

_ADJUSTED_WEIGHTS = {}
for stat, w in _BPM_WEIGHTS.items():
    adj = _ADJUSTMENTS.get(stat, 1.0)
    _ADJUSTED_WEIGHTS[stat] = w * adj

# Step 3: Normalize so PTS weight = 1.0 (most interpretable anchor)
# This preserves all ratios from the RAPM regression while making
# the scale intuitive: 1 z-score of scoring = 1.0 PMI contribution
_NORM = abs(_ADJUSTED_WEIGHTS["pts"])
WEIGHTS = {k: round(v / _NORM, 4) for k, v in _ADJUSTED_WEIGHTS.items()}

# Final weights (normalized to pts=1.0):
# pts:  +1.00  (anchor — scoring is the reference)
# tov: ~-1.33  (turnovers — 33% worse than scoring is good)
# stl:  ~1.00  (steals — equal to a point after RAPTOR gambling discount)
# pf:  ~-0.74  (fouls)
# ast:  ~0.55  (assists)
# blk:  ~0.37  (blocks — significantly reduced per RAPTOR)
# orb:  ~0.30  (offensive rebounds)
# drb:  ~0.24  (defensive rebounds)

# Efficiency: TS% relative to league average
# BPM uses ~2*(TS%-TmTS%) as part of the scoring interaction term.
# RAPTOR also heavily weights efficiency in the scoring category.
# We use a coefficient calibrated to produce ~0.3 PMI per +3% rTS
# (reduced from 4.5 → 3.5 to prevent high-efficiency role players
# from outranking higher-volume stars)
EFFICIENCY_WEIGHT = 3.5

# ── Defense discount factor ──
# BPM, RAPTOR, and EPM all agree: box score stats capture most of
# offense but very little of defense. BPM's author says "take DBPM
# with a spoonful of salt." RAPTOR uses tracking data for defense.
# EPM uses a different defensive prior pre-tracking era.
#
# Our approach: We multiply defensive z-score components by 0.80,
# meaning we acknowledge defense is ~20% less reliably measured
# from box scores than offense. This is LESS aggressive than v1's
# 0.72 dampener because we've already reduced blocks and DRBs.
DEFENSE_BOX_RELIABILITY = 0.80

# ── Output scale factor ──
# Raw PMI from z-scores naturally ranges ~[-3, +6.5].
# We multiply by PMI_SCALE to stretch into a more readable range:
#   Target: ~[-5, +15] where 15 = all-time GOAT season
PMI_SCALE = 2.3


# ═══════════════════════════════════════════════════════════════════════════════
#  ERA-SPECIFIC STAT DEFLATORS
# ═══════════════════════════════════════════════════════════════════════════════
#
# Instead of blanket era penalties, we deflate specific stats that we
# KNOW were inflated in particular eras. These are based on historical
# NBA per-player league averages from Basketball Reference.
#
# Reference era: 2010-2020 (most RAPM data was calibrated here)
#
# The deflator works by adjusting a player's raw stat BEFORE z-scoring:
#   adjusted_stat = raw_stat × deflator
# where deflator = reference_era_avg / current_era_avg
#
# If steals averaged 1.0/game in 1980 but 0.72/game in 2015,
# the deflator for 1980 steals = 0.72/1.0 = 0.72
# This means a 2.0 SPG in 1980 is treated as ~1.44 SPG in 2015 terms.

# Per-player league averages by era (approximate, from BBRef)
# Format: {era_start: {"stl": avg, "blk": avg, "trb": avg}}
_ERA_LEAGUE_AVGS = {
    # Pre-steal/block tracking
    1946: {"stl": None, "blk": None, "trb": 8.0, "ppg": 12.0, "apg": 2.5},
    1955: {"stl": None, "blk": None, "trb": 9.5, "ppg": 13.0, "apg": 2.8},
    1960: {"stl": None, "blk": None, "trb": 10.0, "ppg": 14.0, "apg": 3.0},
    # Steals/blocks start being tracked (1973-74)
    # NOTE: Pre-1997 steal AND block averages boosted ~15-30% above raw
    # league avg to reflect true era inflation:
    # STEALS: loose ball-handling, no hand-check enforcement favoring
    #   offense, slower pace = lazy half-court passes, less film study
    # BLOCKS: no zone defense = more 1-on-1 post matchups, less floor
    #   spacing = more predictable shot locations, fewer perimeter shots
    # High-steal/block players benefited disproportionately.
    1974: {"stl": 1.14, "blk": 0.72, "trb": 5.5, "ppg": 12.5, "apg": 3.0},
    1978: {"stl": 1.20, "blk": 0.70, "trb": 5.3, "ppg": 12.8, "apg": 3.2},
    1982: {"stl": 1.14, "blk": 0.66, "trb": 5.0, "ppg": 12.5, "apg": 3.0},
    1986: {"stl": 1.06, "blk": 0.62, "trb": 4.8, "ppg": 12.8, "apg": 3.2},
    1990: {"stl": 1.01, "blk": 0.57, "trb": 4.5, "ppg": 12.5, "apg": 3.0},
    1994: {"stl": 0.96, "blk": 0.52, "trb": 4.3, "ppg": 12.0, "apg": 2.8},
    # Shortened 3pt line era (1994-1997)
    1997: {"stl": 0.90, "blk": 0.50, "trb": 4.2, "ppg": 11.5, "apg": 2.5},
    # Post hand-checking ban (2004-05)
    2000: {"stl": 0.78, "blk": 0.45, "trb": 4.2, "ppg": 11.8, "apg": 2.6},
    2005: {"stl": 0.75, "blk": 0.48, "trb": 4.2, "ppg": 12.0, "apg": 2.8},
    # Reference era
    2010: {"stl": 0.72, "blk": 0.48, "trb": 4.2, "ppg": 12.5, "apg": 2.8},
    2015: {"stl": 0.72, "blk": 0.48, "trb": 4.2, "ppg": 13.0, "apg": 3.0},
    2020: {"stl": 0.70, "blk": 0.47, "trb": 4.3, "ppg": 13.5, "apg": 3.2},
    2024: {"stl": 0.72, "blk": 0.47, "trb": 4.3, "ppg": 14.0, "apg": 3.4},
}

# Reference era averages (what the RAPM regressions were calibrated on)
_REF_ERA = {
    "stl": 0.72,
    "blk": 0.48,
    "trb": 4.2,
}


def _get_era_deflators(season_year: int) -> dict:
    """Get stat-specific deflation factors for a given season.

    Returns dict of {stat: multiplier} where multiplier < 1.0 means
    the stat was inflated in that era (so we deflate it).
    """
    # Find the closest era bracket
    era_keys = sorted(_ERA_LEAGUE_AVGS.keys())
    era_year = era_keys[0]
    for ey in era_keys:
        if season_year >= ey:
            era_year = ey

    era = _ERA_LEAGUE_AVGS[era_year]
    deflators = {}

    for stat in ["stl", "blk"]:
        era_avg = era.get(stat)
        ref_avg = _REF_ERA.get(stat)
        if era_avg is not None and ref_avg is not None and era_avg > 0:
            deflators[stat] = min(1.0, ref_avg / era_avg)
        else:
            deflators[stat] = 1.0  # no deflation if no data

    # Rebounds: only deflate pre-1980 (pace-inflated era)
    era_trb = era.get("trb", 4.2)
    ref_trb = _REF_ERA["trb"]
    if era_trb > ref_trb * 1.05:  # only deflate if meaningfully higher
        deflators["trb"] = min(1.0, ref_trb / era_trb)
    else:
        deflators["trb"] = 1.0

    return deflators


# ═══════════════════════════════════════════════════════════════════════════════
#  POSITION HANDLING
# ═══════════════════════════════════════════════════════════════════════════════

POS_MAP = {
    "PG": 1, "SG": 2, "G": 1.5, "Guard": 1.5,
    "SF": 3, "PF": 4, "F": 3.5, "Forward": 3.5,
    "C": 5, "FC": 4.5, "GF": 2.5, "Center": 5,
}

# Position adjustments (from BPM's position-varying coefficients)
# Guards get more credit for steals/assists, less for blocks/rebounds
# Centers get more credit for blocks/rebounds, less for steals/assists
POS_ADJUSTMENTS = {
    "stl": (1.10, 0.90),   # (guard, center)
    "blk": (1.30, 0.85),   # guard block is rarer & more impressive
    "ast": (1.05, 0.95),
    "orb": (1.15, 0.90),   # guard ORB is rarer
    "drb": (0.95, 1.05),
}


def _pos_num(pos_str: str) -> float:
    """Convert position string to numeric 1-5."""
    if not pos_str or (isinstance(pos_str, float) and np.isnan(pos_str)):
        return 3.0
    pos = str(pos_str).strip().upper().split("-")[0].split("/")[0]
    return POS_MAP.get(pos, 3.0)


def _pos_interp(pos_num: float) -> float:
    """Interpolation factor t: PG(1)=0.0 → C(5)=1.0."""
    return max(0.0, min(1.0, (pos_num - 1) / 4))


def _get_pos_weight(stat: str, pos_num: float) -> float:
    """Get position-adjusted weight for a stat."""
    base = WEIGHTS.get(stat, 0)
    if stat not in POS_ADJUSTMENTS:
        return base
    guard_m, center_m = POS_ADJUSTMENTS[stat]
    t = _pos_interp(pos_num)
    mult = (1 - t) * guard_m + t * center_m
    return base * mult


def _z(val, mean, std):
    """Z-score, clamped to [-3.5, 3.5]."""
    if std is None or std == 0 or val is None or mean is None:
        return 0.0
    try:
        v = float(val)
        m = float(mean)
        s = float(std)
    except (TypeError, ValueError):
        return 0.0
    if np.isnan(v) or np.isnan(m) or s < 0.001:
        return 0.0
    return max(-3.5, min(3.5, (v - m) / s))


# ═══════════════════════════════════════════════════════════════════════════════
#  PMI COMPUTATION — Unified metric
# ═══════════════════════════════════════════════════════════════════════════════

def compute_pmi_season(row: dict, league_stats: dict, pos_num: float,
                       season_year: int = 2020) -> dict:
    """Compute PMI v3 for a single player-season.

    Returns dict with: pmi, opmi, dpmi, era_adj (for transparency).

    The key difference from v2: era-specific deflators and multi-source
    weight synthesis.
    """
    # Get era deflators
    deflators = _get_era_deflators(season_year)

    # ── Z-scores for offensive stats (no era adjustment needed) ──
    z_pts = _z(row.get("ppg", 0),
               league_stats.get("ppg_mean", 0),
               league_stats.get("ppg_std", 1))
    z_ast = _z(row.get("apg", 0),
               league_stats.get("apg_mean", 0),
               league_stats.get("apg_std", 1))
    z_tov = _z(row.get("tov_pg", 0),
               league_stats.get("tov_pg_mean", 0),
               league_stats.get("tov_pg_std", 1))
    z_orb = _z(row.get("orb_pg", 0),
               league_stats.get("orb_pg_mean", 0),
               league_stats.get("orb_pg_std", 1))

    # ── Z-scores for defensive stats WITH era deflation ──
    # Era deflation: reduce the PLAYER's raw stat value before z-scoring
    # against the ORIGINAL league distribution. This means a 2.9 SPG in
    # 1988 (deflated by 0.76 → 2.20) gets z-scored against the original
    # 1988 league mean/std, producing a lower z-score than raw 2.9 would.
    # This correctly captures that 2.9 SPG in 1988 is less impressive
    # than 2.9 SPG in 2015 due to rule/recording differences.

    # Steals
    raw_stl = float(row.get("spg", 0) or 0)
    adj_stl = raw_stl * deflators.get("stl", 1.0)
    z_stl = _z(adj_stl,
               league_stats.get("spg_mean", 0),
               league_stats.get("spg_std", 1))

    # Blocks
    raw_blk = float(row.get("bpg", 0) or 0)
    adj_blk = raw_blk * deflators.get("blk", 1.0)
    z_blk = _z(adj_blk,
               league_stats.get("bpg_mean", 0),
               league_stats.get("bpg_std", 1))

    # Defensive rebounds (mild deflation for pace-inflated eras)
    raw_drb = float(row.get("drb_pg", 0) or 0)
    adj_drb = raw_drb * deflators.get("trb", 1.0)
    z_drb = _z(adj_drb,
               league_stats.get("drb_pg_mean", 0),
               league_stats.get("drb_pg_std", 1))

    z_pf = _z(row.get("pf_pg", 0),
              league_stats.get("pf_pg_mean", 0),
              league_stats.get("pf_pg_std", 1))

    # ── Efficiency: TS% relative to league average ──
    ts_pct = float(row.get("ts_pct", 0) or 0)
    lg_ts = float(league_stats.get("ts_pct_mean", 0.540) or 0.540)
    ts_diff = ts_pct - lg_ts

    # ── Offensive component ──
    # AST/TOV ratio bonus: high-usage playmakers who maintain a good
    # assist-to-turnover ratio deserve credit. LeBron at 10 ast / 3.9 tov
    # (2.56 ratio) should be rewarded vs a player at 3 ast / 2 tov (1.5).
    # Reference: league avg AST/TOV ≈ 1.5-1.8. We give a bonus for >2.0.
    raw_ast = float(row.get("apg", 0) or 0)
    raw_tov = float(row.get("tov_pg", 0) or 0)
    ast_tov_bonus = 0.0
    if raw_tov > 0.5 and raw_ast > 1.0:
        ratio = raw_ast / raw_tov
        # Bonus kicks in above 1.5 ratio, scales linearly
        # 2.0 ratio → +0.15, 3.0 ratio → +0.45, 4.0 → +0.75
        if ratio > 1.5:
            ast_tov_bonus = min(1.0, (ratio - 1.5) * 0.30)

    opmi = (
        _get_pos_weight("pts", pos_num) * z_pts +
        EFFICIENCY_WEIGHT * ts_diff +
        _get_pos_weight("ast", pos_num) * z_ast +
        _get_pos_weight("orb", pos_num) * z_orb +
        WEIGHTS["tov"] * z_tov +
        ast_tov_bonus
    )

    # ── Defensive component ──
    # Apply defense reliability discount (box score captures ~30% of defense)
    dpmi = DEFENSE_BOX_RELIABILITY * (
        _get_pos_weight("stl", pos_num) * z_stl +
        _get_pos_weight("blk", pos_num) * z_blk +
        _get_pos_weight("drb", pos_num) * z_drb +
        WEIGHTS["pf"] * z_pf
    )

    pmi = opmi + dpmi

    # ── Minutes role adjustment ──
    # BPM includes a minutes interaction: players playing 35 mpg are doing
    # it against starters, while 20 mpg players face more bench units.
    # We apply a mild scaling based on MPG relative to starter threshold.
    # 36 mpg = full credit (1.0), 24 mpg = 0.90, 12 mpg = 0.80
    mpg = float(row.get("mpg", 0) or 0)
    if mpg > 0:
        # Linear scale from 0.80 at 12 mpg to 1.0 at 36 mpg
        mpg_factor = min(1.0, max(0.80, 0.80 + 0.20 * (mpg - 12) / 24))
        pmi *= mpg_factor

    # ── Output scaling ──
    # Raw PMI from z-scores naturally ranges ~[-3, +6.5].
    # We scale to a more intuitive range where:
    #   0   = league-average rotation player
    #   5-8 = All-Star caliber season
    #  10-12 = MVP-level season
    #  13-15 = All-time GOAT season (MJ '91, LeBron '13)
    #  -3 to -5 = worst qualifying players
    pmi *= PMI_SCALE
    opmi *= PMI_SCALE
    dpmi *= PMI_SCALE

    return {
        "pmi": round(pmi, 2),
        "opmi": round(opmi, 2),
        "dpmi": round(dpmi, 2),
    }


# ═══════════════════════════════════════════════════════════════════════════════
#  CPMI — Clutch Performance Metric Index (v3)
# ═══════════════════════════════════════════════════════════════════════════════
#
# v1 BIASES REMOVED:
#   1. Hand-picked weights (0.52, 0.28, etc.) with no empirical basis
#   2. Scoring overweighted — z_ppg (0.52) > z_apg (0.28), arbitrary
#   3. Plus/minus given highest weight (0.60) but raw +/- in small clutch
#      samples is extremely noisy and teammate-dependent
#   4. No blocks, rebounds, or FT% — ignores defensive/FT clutch impact
#   5. No clutch minutes weighting — a guy playing 2 clutch min treated
#      same as one playing 8 clutch min
#
# v3 APPROACH: Win Probability Added (WPA) framework
#
#   Research (Beuoy/inpredictable 2014, Snarr/EPM) shows that the
#   win-probability impact of box score events varies by context but
#   the AVERAGE per-event WPA gives us empirical weights:
#
#   From inpredictable's WPA decomposition (2012-13 season):
#     Made FG:    +3.2% WPA on avg    (scoring)
#     Missed FG:  -1.5% WPA on avg    (negative scoring)
#     Made FT:    +1.0% WPA on avg    (scoring)
#     Missed FT:  -0.8% WPA on avg    (negative scoring)
#     Turnover:   -2.0% WPA on avg    (possession loss)
#     Steal:      +2.0% WPA on avg    (forced turnover)
#     Assist:     +1.5% WPA on avg    (creates scoring)
#     Off Reb:    +1.8% WPA on avg    (extends possession)
#     Def Reb:    +0.5% WPA on avg    (secures possession)
#     Block:      +0.8% WPA on avg    (prevents scoring)
#
#   We derive RELATIVE weights by normalizing to scoring impact.
#   Since clutch is about WINNING, we anchor to plus/minus as the
#   ground truth (actual win contribution), then add box score context.
#
#   Key insight: In clutch time, EFFICIENCY matters more than volume.
#   A player going 4/5 in clutch is more valuable than one going 6/14.
#   So we weight TS% heavily — it captures scoring efficiency directly.
#
# FORMULA:
#   CPMI = w_pm * z_plusminus     (actual clutch outcome — ground truth)
#        + w_eff * z_ts           (clutch shooting efficiency)
#        + w_scr * z_ppg          (clutch scoring volume)
#        + w_ast * z_apg          (clutch playmaking)
#        + w_tov * z_tovpg        (clutch ball security — negative)
#        + w_stl * z_spg          (clutch forced turnovers)
#        + w_orb * z_orbpg        (clutch 2nd chance points)
#        + w_blk * z_bpg          (clutch shot prevention)
#        + w_ftpct * z_ft_pct     (clutch free throw shooting)
#
# WEIGHT DERIVATION:
#   Start from WPA per-event ratios, adjust for clutch context:
#   - Plus/minus IS the outcome, so it gets base weight
#   - TS% is critical in clutch (every possession matters)
#   - Turnovers are more costly in clutch (fewer possessions to recover)
#   - Steals are more valuable (can swing a possession in tight game)
#   - FT% matters hugely (intentional fouling in last 2 min)
#   - Blocks get RAPTOR discount (same as DPMI — low predictive power)
#
#   All weights normalized so max-abs = 1.0 (plus/minus as anchor)
# ═══════════════════════════════════════════════════════════════════════════════

CPMI_WEIGHTS = {
    # Outcome-based (ground truth — the team actually won/lost with this player)
    "z_plusminus": 1.00,    # anchor — actual clutch point differential

    # Scoring efficiency (most critical in clutch: every shot matters)
    "z_ts":        0.80,    # TS% in clutch — efficiency > volume
    "z_ft_pct":    0.45,    # FT% in clutch — late-game fouling makes this huge

    # Scoring volume (matters, but efficiency-adjusted via TS%)
    "z_ppg":       0.55,    # clutch PPG — volume still matters

    # Playmaking
    "z_apg":       0.40,    # clutch assists — creating good shots for others

    # Ball security (turnovers in clutch are devastating)
    "z_tovpg":    -0.70,    # clutch TOV — more costly than non-clutch per WPA

    # Defensive (steals valuable but discounted per RAPTOR gambling costs)
    "z_spg":       0.40,    # clutch steals — valuable but not dominant
    "z_blk":       0.20,    # clutch blocks — RAPTOR-discounted (low predictive value)

    # Rebounding (offensive boards = 2nd chance, huge in clutch)
    "z_orb":       0.35,    # clutch ORB — extends crucial possessions
}


def compute_cpmi(clutch_row: dict, clutch_league: dict) -> float:
    """Compute CPMI v3 from clutch split data (last 5 min, ±5 pts).

    Uses WPA-anchored weights with plus/minus as ground truth.
    Includes all box score dimensions: scoring efficiency, playmaking,
    ball security, defense, and rebounding.
    """
    z_ppg = _z(clutch_row.get("clutch_ppg", 0),
               clutch_league.get("ppg_mean", 0),
               clutch_league.get("ppg_std", 1))
    z_apg = _z(clutch_row.get("clutch_apg", 0),
               clutch_league.get("apg_mean", 0),
               clutch_league.get("apg_std", 1))
    z_ts = _z(clutch_row.get("clutch_ts", 0),
              clutch_league.get("ts_mean", 0),
              clutch_league.get("ts_std", 1))
    z_pm = _z(clutch_row.get("clutch_plusminus", 0),
              clutch_league.get("pm_mean", 0),
              clutch_league.get("pm_std", 1))
    z_spg = _z(clutch_row.get("clutch_spg", 0),
               clutch_league.get("spg_mean", 0),
               clutch_league.get("spg_std", 1))
    z_tov = _z(clutch_row.get("clutch_tovpg", 0),
               clutch_league.get("tov_mean", 0),
               clutch_league.get("tov_std", 1))
    z_blk = _z(clutch_row.get("clutch_bpg", 0),
               clutch_league.get("bpg_mean", 0),
               clutch_league.get("bpg_std", 1))
    z_orb = _z(clutch_row.get("clutch_orbpg", 0),
               clutch_league.get("orb_mean", 0),
               clutch_league.get("orb_std", 1))
    z_ft_pct = _z(clutch_row.get("clutch_ft_pct", 0),
                   clutch_league.get("ft_pct_mean", 0),
                   clutch_league.get("ft_pct_std", 1))

    cpmi_raw = (
        CPMI_WEIGHTS["z_plusminus"] * z_pm +
        CPMI_WEIGHTS["z_ts"] * z_ts +
        CPMI_WEIGHTS["z_ft_pct"] * z_ft_pct +
        CPMI_WEIGHTS["z_ppg"] * z_ppg +
        CPMI_WEIGHTS["z_apg"] * z_apg +
        CPMI_WEIGHTS["z_tovpg"] * z_tov +
        CPMI_WEIGHTS["z_spg"] * z_spg +
        CPMI_WEIGHTS["z_blk"] * z_blk +
        CPMI_WEIGHTS["z_orb"] * z_orb
    )

    # Scale to match PMI range (same factor)
    return round(cpmi_raw * PMI_SCALE, 2)


def compute_clutch_league_stats(clutch_df: pd.DataFrame) -> dict:
    """Compute league mean/std for clutch z-score normalization.

    Input: DataFrame of all players' clutch stats for a season.
    Expected columns from nba_api LeagueDashPlayerClutch (PerGame):
        PTS, AST, STL, TOV, BLK, OREB, PLUS_MINUS, MIN, GP,
        FGM, FGA, FTM, FTA, FT_PCT
    """
    work = clutch_df.copy()

    # Filter to players with meaningful clutch time (>1 min/game avg)
    if "MIN" in work.columns and "GP" in work.columns:
        work["clutch_mpg"] = work["MIN"] / work["GP"].replace(0, 1)
        work = work[work["clutch_mpg"] >= 1.0]

    if len(work) < 20:
        work = clutch_df  # fallback if filter too aggressive

    stats = {}

    # Compute TS% for clutch: PTS / (2 * (FGA + 0.44 * FTA))
    if all(c in work.columns for c in ["PTS", "FGA", "FTA"]):
        tsa = 2 * (work["FGA"] + 0.44 * work["FTA"])
        ts = work["PTS"] / tsa.replace(0, np.nan)
        ts = ts.dropna()
        stats["ts_mean"] = float(ts.mean()) if len(ts) > 0 else 0.540
        stats["ts_std"] = max(0.001, float(ts.std())) if len(ts) > 1 else 0.05
    else:
        stats["ts_mean"] = 0.540
        stats["ts_std"] = 0.05

    # Standard per-game stats
    col_map = {
        "PTS": "ppg", "AST": "apg", "STL": "spg", "TOV": "tov",
        "BLK": "bpg", "OREB": "orb", "PLUS_MINUS": "pm",
    }
    for col, key in col_map.items():
        if col in work.columns:
            vals = work[col].dropna()
            stats[f"{key}_mean"] = float(vals.mean()) if len(vals) > 0 else 0
            stats[f"{key}_std"] = max(0.001, float(vals.std())) if len(vals) > 1 else 1
        else:
            stats[f"{key}_mean"] = 0
            stats[f"{key}_std"] = 1

    # FT%
    if "FT_PCT" in work.columns:
        ft = work["FT_PCT"].dropna()
        stats["ft_pct_mean"] = float(ft.mean()) if len(ft) > 0 else 0.75
        stats["ft_pct_std"] = max(0.001, float(ft.std())) if len(ft) > 1 else 0.10
    else:
        stats["ft_pct_mean"] = 0.75
        stats["ft_pct_std"] = 0.10

    return stats


def build_clutch_row(player_clutch_row: pd.Series) -> dict:
    """Convert nba_api clutch DataFrame row to CPMI input dict.

    Maps NBA API column names to our clutch_row keys.
    """
    pts = float(player_clutch_row.get("PTS", 0) or 0)
    fga = float(player_clutch_row.get("FGA", 0) or 0)
    fta = float(player_clutch_row.get("FTA", 0) or 0)
    tsa = 2 * (fga + 0.44 * fta)
    ts = pts / tsa if tsa > 0 else 0

    return {
        "clutch_ppg": pts,
        "clutch_apg": float(player_clutch_row.get("AST", 0) or 0),
        "clutch_ts": ts,
        "clutch_plusminus": float(player_clutch_row.get("PLUS_MINUS", 0) or 0),
        "clutch_spg": float(player_clutch_row.get("STL", 0) or 0),
        "clutch_tovpg": float(player_clutch_row.get("TOV", 0) or 0),
        "clutch_bpg": float(player_clutch_row.get("BLK", 0) or 0),
        "clutch_orbpg": float(player_clutch_row.get("OREB", 0) or 0),
        "clutch_ft_pct": float(player_clutch_row.get("FT_PCT", 0) or 0),
        "clutch_gp": int(player_clutch_row.get("GP", 0) or 0),
        "clutch_min": float(player_clutch_row.get("MIN", 0) or 0),
    }


# ═══════════════════════════════════════════════════════════════════════════════
#  CAREER AGGREGATION — Minutes-weighted (like VORP)
# ═══════════════════════════════════════════════════════════════════════════════

GP_HALF_REG = 82      # ~1 full season to reach 50% trust (was 50)
GP_HALF_PLAYOFF = 20   # ~1 deep playoff run to reach 50% trust (was 15)
AWC_CONSTANT = 0.000175  # adjusted for PMI_SCALE (was 0.0004 at 1.0x)


def compute_career_pmi(season_data: list[dict], is_playoff: bool = False) -> float:
    """Compute career PMI using minutes-weighted average.

    Each season weighted by total minutes played (GP × MPG).
    Bayesian regression toward 0.0 based on total GP.
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

    total_gp = sum(s.get("gp", 0) or 0 for s in season_data)
    gp_half = GP_HALF_PLAYOFF if is_playoff else GP_HALF_REG
    trust = total_gp / (total_gp + gp_half)
    career_pmi = trust * career_avg

    return round(career_pmi, 2)


def compute_awc(pmi: float, total_minutes: int) -> float:
    """Accumulated Win Contribution = PMI × total_minutes × constant."""
    return round(pmi * total_minutes * AWC_CONSTANT, 1)


# ═══════════════════════════════════════════════════════════════════════════════
#  BATCH COMPUTATION
# ═══════════════════════════════════════════════════════════════════════════════

def compute_season_league_stats(df: pd.DataFrame) -> dict:
    """Compute league mean/std for z-score normalization.

    Filters to players with >15 mpg to exclude deep bench / garbage time
    and produce a more meaningful distribution for starter-caliber stats.
    (BPM uses a similar approach — only rotation players count.)
    """
    work = df.copy()
    if "mpg" in work.columns:
        work = work[work["mpg"] >= 15]
    if len(work) < 20:
        work = df

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
                stats[f"{key}_std"] = max(0.001, float(vals.std())) if len(vals) > 1 else 1.0
            else:
                stats[f"{key}_mean"] = 0
                stats[f"{key}_std"] = 1
        else:
            stats[f"{key}_mean"] = 0
            stats[f"{key}_std"] = 1

    return stats
