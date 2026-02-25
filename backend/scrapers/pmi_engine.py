"""PMI v41d — Player Metric Index computation engine.

Novel basketball analytics metric by Samir Kerkar.
Combines offensive and defensive impact on a standardized, era-adjusted scale.

Components:
  OPMI  — Offensive Player Metric Index (z-score weighted composite)
  DPMI  — Defensive Player Metric Index (with ML imputation for pre-1973)
  PMI   — OPMI + DPMI
  CPMI  — Clutch Performance Metric Index (last 5 min, ±5 pts)
  AWC   — Accumulated Win Contribution (PMI × minutes × constant)

Version history:
  v41e  Separate playoff CPMI from NBA API playoff clutch splits
  v41d  Playoff PMI scale boost, GP regression fix (half=10)
  v41c  Scoring dominance bonus, playoff DPMI dampener (0.48)
  v41b  Era penalty applied to playoff OPMI, Kareem fix
  v41   Volume gate for ts_diff, playmaking rebalance, ML defender boost
  v40   Peak-weighted career avg, playmaker TOV discount
  v39b  Center scoring floor, team offensive context
"""

import numpy as np
import pandas as pd
import logging
from typing import Optional

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
#  CONSTANTS
# ═══════════════════════════════════════════════════════════════════════════════

# Position encoding: PG=1, SG=2, SF=3, PF=4, C=5
POS_MAP = {"PG": 1, "SG": 2, "G": 1.5, "SF": 3, "PF": 4, "F": 3.5, "C": 5, "FC": 4.5, "GF": 2.5}

# Guard coefficients (PG/SG)
W_GUARD = {
    "z_pts": 1.20, "ts_diff": 7.0, "z_ast": 0.75, "z_tov": -0.55,
    "z_orb": 0.15, "z_fta": 0.10, "z_fg3m": 0.10,
}

# Center coefficients (C)
W_CENTER = {
    "z_pts": 0.95, "ts_diff": 7.0, "z_ast": 0.35, "z_tov": -0.40,
    "z_orb": 0.50, "z_fta": 0.10, "z_fg3m": 0.00,
}

# DPMI weights (position-interpolated)
W_DPMI_GUARD = {"z_stl": 0.70, "z_blk": 0.20, "z_drb": 0.35, "z_pf": -0.15}
W_DPMI_CENTER = {"z_stl": 0.30, "z_blk": 0.75, "z_drb": 0.50, "z_pf": -0.10}

DPMI_SCALE = 1.2
DPMI_DAMPENER_REG = 0.85
DPMI_DAMPENER_PLAYOFF = 0.55

# Playoff offensive weight boost — scoring dominance matters more in playoffs
PLAYOFF_Z_PTS_WEIGHT = 1.45  # vs 1.20 regular

# Playoff efficiency discount — ts_diff is less differentiating in playoffs
PLAYOFF_TS_DIFF_MULT = 0.65  # multiply ts_diff weight by this in playoffs

# GP regression
GP_HALF_REG = 60
GP_HALF_PLAYOFF = 12  # ~1 deep run to reach ~50% trust

# AWC constant
AWC_CONSTANT = 0.0004

# Era penalty brackets (start_year, multiplier)
ERA_PENALTIES = [
    (1946, 0.72),
    (1950, 0.76),
    (1955, 0.80),
    (1960, 0.84),
    (1965, 0.88),
    (1970, 0.92),
    (1975, 0.95),
    (1980, 0.97),
    (1985, 1.00),
]

# CPMI weights
CPMI_WEIGHTS = {
    "z_ppg": 1.50, "z_apg": 0.40, "z_ts": 0.35, "z_plusminus": 0.50,
    "z_spg": 0.15, "z_tovpg": -0.35,
}


# ═══════════════════════════════════════════════════════════════════════════════
#  HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def _pos_num(pos_str: str) -> float:
    """Convert position string to numeric 1-5."""
    if not pos_str or pd.isna(pos_str):
        return 3.0  # default SF
    pos = str(pos_str).strip().upper().split("-")[0].split("/")[0]
    return POS_MAP.get(pos, 3.0)


def _pos_interp(pos_num: float) -> float:
    """Interpolation factor t: PG(1)=0 → C(5)=1."""
    return max(0.0, min(1.0, (pos_num - 1) / 4))


def _interp_weights(guard_w: dict, center_w: dict, t: float) -> dict:
    """Interpolate between guard and center coefficient dicts."""
    return {k: (1 - t) * guard_w.get(k, 0) + t * center_w.get(k, 0)
            for k in set(guard_w) | set(center_w)}


def _z(val, mean, std):
    """Z-score, clamped to [-3, 3]."""
    if std == 0 or pd.isna(val) or pd.isna(mean):
        return 0.0
    return max(-3.0, min(3.0, (val - mean) / std))


def _era_multiplier(season_year: int) -> float:
    """Get era inflation penalty multiplier for a given season start year."""
    mult = 1.0
    for start, m in ERA_PENALTIES:
        if season_year >= start:
            mult = m
    return mult


# ═══════════════════════════════════════════════════════════════════════════════
#  OPMI — Offensive Player Metric Index
# ═══════════════════════════════════════════════════════════════════════════════

def compute_opmi(row: dict, league_stats: dict, pos_num: float,
                 is_playoff: bool = False, season_year: int = 2024) -> float:
    """Compute OPMI for a single player-season row.

    Args:
        row: Player stat dict with ppg, apg, ts_pct, tov_pg, orb_pg, fta_pg, fg3m_pg
        league_stats: Dict with mean/std for each stat across the season
        pos_num: Numeric position (1-5)
        is_playoff: Whether this is playoff data
        season_year: Start year of season (for era penalty)

    Returns:
        OPMI value (float)
    """
    t = _pos_interp(pos_num)
    w = _interp_weights(W_GUARD, W_CENTER, t)

    # Override z_pts weight for playoffs
    if is_playoff:
        w["z_pts"] = (1 - t) * PLAYOFF_Z_PTS_WEIGHT + t * (PLAYOFF_Z_PTS_WEIGHT - 0.20)
        w["ts_diff"] *= PLAYOFF_TS_DIFF_MULT  # efficiency less differentiating in playoffs

    # Z-scores
    z_pts = _z(row.get("ppg", 0), league_stats.get("ppg_mean", 0), league_stats.get("ppg_std", 1))
    z_ast = _z(row.get("apg", 0), league_stats.get("apg_mean", 0), league_stats.get("apg_std", 1))
    z_tov = _z(row.get("tov_pg", 0), league_stats.get("tov_pg_mean", 0), league_stats.get("tov_pg_std", 1))
    z_orb = _z(row.get("orb_pg", 0), league_stats.get("orb_pg_mean", 0), league_stats.get("orb_pg_std", 1))
    z_fta = _z(row.get("fta_pg", 0), league_stats.get("fta_pg_mean", 0), league_stats.get("fta_pg_std", 1))
    z_fg3m = _z(row.get("fg3m_pg", 0), league_stats.get("fg3m_pg_mean", 0), league_stats.get("fg3m_pg_std", 1))

    # True shooting diff vs league average
    ts_pct = row.get("ts_pct", 0) or 0
    lg_ts = league_stats.get("ts_pct_mean", 0.540) or 0.540
    ts_diff = ts_pct - lg_ts

    # ── Special Adjustments ──
    # Volume Gate: ts_diff only counts if player scores enough
    volume_gate = max(0.25, min(1.0, (z_pts + 1.0) / 2.0))
    ts_diff_gated = ts_diff * volume_gate

    # Center scoring floor: don't penalize centers for lower scoring
    center_floor = -0.3 * max(0, (pos_num - 2) / 3)
    z_pts = max(z_pts, center_floor)

    # Playmaker TOV discount: high-assist players get partial TOV forgiveness
    if z_ast > 1.0:
        tov_discount = 1 - min(0.30, (z_ast - 1.0) * 0.12)
        z_tov *= tov_discount

    # Compute raw OPMI
    opmi_raw = (
        w["z_pts"] * z_pts +
        w["ts_diff"] * ts_diff_gated +
        w["z_ast"] * z_ast +
        w["z_tov"] * z_tov +
        w["z_orb"] * z_orb +
        w["z_fta"] * z_fta +
        w["z_fg3m"] * z_fg3m
    )

    # Scoring dominance bonus (playoffs only): elite scorers with good efficiency
    # MJ-type players (z_pts > 2.5 AND efficient) get extra credit for carrying
    if is_playoff and z_pts > 2.0 and ts_diff > 0:
        dom_bonus = min(1.2, (z_pts - 2.0) * 0.5 * min(1.0, ts_diff / 0.02))
        opmi_raw += dom_bonus

    # Era penalty
    era_mult = _era_multiplier(season_year)
    opmi = opmi_raw * era_mult

    return round(opmi, 4)


# ═══════════════════════════════════════════════════════════════════════════════
#  DPMI — Defensive Player Metric Index
# ═══════════════════════════════════════════════════════════════════════════════

def compute_dpmi(row: dict, league_stats: dict, pos_num: float,
                 is_playoff: bool = False) -> float:
    """Compute DPMI for a single player-season row.

    For pre-1973 players without steals/blocks, returns 0.0
    (ML imputation should be applied separately via compute_dpmi_ml_imputed).

    Args:
        row: Player stat dict with spg, bpg, drb_pg, pf_pg
        league_stats: Dict with mean/std for defensive stats
        pos_num: Numeric position (1-5)
        is_playoff: Whether this is playoff data

    Returns:
        DPMI value (float)
    """
    spg = row.get("spg", None)
    bpg = row.get("bpg", None)
    drb = row.get("drb_pg", 0) or 0

    # Pre-1973 check: if no steals AND no blocks AND no defensive rebounds,
    # we truly have no defensive data — return 0 for ML imputation later.
    # But if stats were ML-imputed (spg/bpg set to small values), compute normally.
    spg_val = float(spg) if spg is not None else 0.0
    bpg_val = float(bpg) if bpg is not None else 0.0
    
    if spg_val == 0 and bpg_val == 0 and drb == 0:
        return 0.0

    t = _pos_interp(pos_num)
    w = _interp_weights(W_DPMI_GUARD, W_DPMI_CENTER, t)

    z_stl = _z(spg_val, league_stats.get("spg_mean", 0), league_stats.get("spg_std", 1))
    z_blk = _z(bpg_val, league_stats.get("bpg_mean", 0), league_stats.get("bpg_std", 1))
    z_drb = _z(row.get("drb_pg", 0), league_stats.get("drb_pg_mean", 0), league_stats.get("drb_pg_std", 1))
    z_pf = _z(row.get("pf_pg", 0), league_stats.get("pf_pg_mean", 0), league_stats.get("pf_pg_std", 1))

    dpmi_raw = (
        w["z_stl"] * z_stl +
        w["z_blk"] * z_blk +
        w["z_drb"] * z_drb +
        w["z_pf"] * z_pf
    )

    dampener = DPMI_DAMPENER_PLAYOFF if is_playoff else DPMI_DAMPENER_REG
    dpmi = dpmi_raw * DPMI_SCALE * dampener

    return round(dpmi, 4)


# ═══════════════════════════════════════════════════════════════════════════════
#  ML IMPUTATION — Pre-1973 DPMI via GradientBoosting
# ═══════════════════════════════════════════════════════════════════════════════

def train_dpmi_imputer(season_stats_df: pd.DataFrame) -> Optional[object]:
    """Train a GradientBoostingRegressor on post-1973 data to predict DPMI.

    Features: trb_rate, pf_rate, team_win_pct, mpg, is_center, era
    Target:   Known DPMI from post-1973 seasons

    Returns trained model or None if insufficient data.
    """
    try:
        from sklearn.ensemble import GradientBoostingRegressor
    except ImportError:
        logger.warning("scikit-learn not installed, ML DPMI imputation unavailable")
        return None

    # Filter to post-1973 seasons with known DPMI
    df = season_stats_df.copy()
    df = df[df["season_year"] >= 1973]
    df = df[df["dpmi"].notna() & (df["dpmi"] != 0)]
    df = df[df["mpg"] > 10]  # min playing time

    if len(df) < 100:
        logger.warning(f"Only {len(df)} post-73 rows for DPMI training, need 100+")
        return None

    features = ["trb_rate", "pf_rate", "team_win_pct", "mpg", "is_center", "era"]
    for col in features:
        if col not in df.columns:
            df[col] = 0

    X = df[features].fillna(0).values
    y = df["dpmi"].values

    model = GradientBoostingRegressor(
        n_estimators=200, max_depth=4, learning_rate=0.08,
        subsample=0.8, random_state=42
    )
    model.fit(X, y)
    logger.info(f"DPMI imputer trained on {len(df)} rows, R²={model.score(X, y):.3f}")

    return model


def impute_dpmi_ml(row: dict, model, is_playoff: bool = False) -> float:
    """Impute DPMI for a pre-1973 player using trained ML model.

    Also applies elite historical defender boost for dominant rebounders
    on winning teams.
    """
    if model is None:
        return 0.0

    features = np.array([[
        row.get("trb_rate", 0),
        row.get("pf_rate", 0),
        row.get("team_win_pct", 0.5),
        row.get("mpg", 30),
        1.0 if row.get("is_center", False) else 0.0,
        row.get("era", 1960),
    ]])

    dpmi_pred = model.predict(features)[0]

    # Elite historical defender boost
    trb_rate = row.get("trb_rate", 0)
    team_win = row.get("team_win_pct", 0.5)
    if trb_rate > 0.35 and team_win > 0.500:
        boost = min(1.8, (trb_rate - 0.35) * 8.0 * (team_win - 0.500) * 3.0)
        dpmi_pred += boost

    # Apply appropriate dampener
    dampener_ratio = DPMI_DAMPENER_PLAYOFF / DPMI_DAMPENER_REG if is_playoff else 1.0
    dpmi_pred *= dampener_ratio

    return round(max(0, dpmi_pred), 4)


# ═══════════════════════════════════════════════════════════════════════════════
#  CPMI — Clutch Performance Metric Index
# ═══════════════════════════════════════════════════════════════════════════════

def compute_cpmi(clutch_row: dict, clutch_league: dict) -> float:
    """Compute CPMI from clutch split data.

    Clutch = last 5 minutes of games within ±5 points.
    """
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

    # Volume bonus: reward players who actually score in the clutch
    clutch_ppg = clutch_row.get("clutch_ppg", 0) or 0
    vol_bonus = max(0, min(1.5, (clutch_ppg - 1.5) * 0.4))
    cpmi_raw += vol_bonus

    return round(cpmi_raw, 4)


# ═══════════════════════════════════════════════════════════════════════════════
#  CAREER AGGREGATION — Peak-weighted average + Bayesian regression
# ═══════════════════════════════════════════════════════════════════════════════

def compute_career_pmi(season_pmis: list[float], total_gp: int,
                       is_playoff: bool = False,
                       league_mean: float = 0.0) -> float:
    """Compute career PMI from season PMI values using peak-weighted average.

    Seasons are ranked best-to-worst. Weight = sqrt(rank_from_best).
    This ensures peak seasons count more than decline years.
    Then Bayesian-regressed toward league mean based on GP.
    """
    if not season_pmis:
        return 0.0

    # Sort descending (best first)
    sorted_pmis = sorted(season_pmis, reverse=True)
    n = len(sorted_pmis)

    # Peak-weighted average: best season gets sqrt(N), worst gets sqrt(1)
    total_weight = 0
    weighted_sum = 0
    for i, pmi in enumerate(sorted_pmis):
        weight = np.sqrt(n - i)  # rank from best = i, so weight = sqrt(N - i)
        weighted_sum += weight * pmi
        total_weight += weight

    career_avg = weighted_sum / total_weight if total_weight > 0 else 0

    # Bayesian regression toward league mean
    gp_half = GP_HALF_PLAYOFF if is_playoff else GP_HALF_REG
    trust = total_gp / (total_gp + gp_half)
    career_pmi = trust * career_avg + (1 - trust) * league_mean

    return round(career_pmi, 4)


def compute_awc(pmi: float, total_minutes: int) -> float:
    """Accumulated Win Contribution = PMI × minutes × constant."""
    return round(pmi * total_minutes * AWC_CONSTANT, 4)


# ═══════════════════════════════════════════════════════════════════════════════
#  BATCH COMPUTATION — Process entire season DataFrames
# ═══════════════════════════════════════════════════════════════════════════════

def compute_season_league_stats(df: pd.DataFrame) -> dict:
    """Compute league mean/std for all stats needed for z-scores.

    Args:
        df: Season stats DataFrame with per-game columns

    Returns:
        Dict with {stat}_mean and {stat}_std for all relevant stats
    """
    stats = {}
    for col, key in [
        ("ppg", "ppg"), ("apg", "apg"), ("tov_pg", "tov_pg"),
        ("orb_pg", "orb_pg"), ("fta_pg", "fta_pg"), ("fg3m_pg", "fg3m_pg"),
        ("spg", "spg"), ("bpg", "bpg"), ("drb_pg", "drb_pg"), ("pf_pg", "pf_pg"),
        ("ts_pct", "ts_pct"),
    ]:
        if col in df.columns:
            vals = df[col].dropna()
            stats[f"{key}_mean"] = vals.mean() if len(vals) > 0 else 0
            stats[f"{key}_std"] = vals.std() if len(vals) > 1 else 1
        else:
            stats[f"{key}_mean"] = 0
            stats[f"{key}_std"] = 1

    return stats


def compute_pmi_for_season(season_df: pd.DataFrame, season_year: int,
                           is_playoff: bool = False,
                           dpmi_model=None) -> pd.DataFrame:
    """Compute PMI components for all players in a single season.

    Adds columns: opmi, dpmi, pmi, rts_pct to the DataFrame.

    Args:
        season_df: DataFrame with per-game stats for one season
        season_year: Start year of the season
        is_playoff: Whether this is playoff data
        dpmi_model: Trained ML model for pre-1973 DPMI imputation

    Returns:
        DataFrame with PMI columns added
    """
    if season_df.empty:
        return season_df

    df = season_df.copy()
    league = compute_season_league_stats(df)

    opmis, dpmis, pmis, rts_pcts = [], [], [], []

    for _, row in df.iterrows():
        pos = _pos_num(row.get("position", "SF"))
        r = row.to_dict()

        # OPMI
        opmi = compute_opmi(r, league, pos, is_playoff, season_year)

        # DPMI
        dpmi = compute_dpmi(r, league, pos, is_playoff)

        # ML imputation for pre-1973 players with no defensive stats
        if dpmi == 0 and season_year < 1973 and dpmi_model is not None:
            dpmi = impute_dpmi_ml(r, dpmi_model, is_playoff)

        pmi = round(opmi + dpmi, 4)

        # Relative TS%
        lg_ts = league.get("ts_pct_mean", 0.540)
        rts = round((row.get("ts_pct", 0) or 0) - lg_ts, 4)

        opmis.append(opmi)
        dpmis.append(dpmi)
        pmis.append(pmi)
        rts_pcts.append(rts)

    df["opmi"] = opmis
    df["dpmi"] = dpmis
    df["pmi"] = pmis
    df["rts_pct"] = rts_pcts

    return df
