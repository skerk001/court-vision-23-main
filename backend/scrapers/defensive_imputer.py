"""ML Imputation for pre-1973 steals and blocks.

Pre-1973 NBA did not track steals or blocks. To compute PMI for
players like Wilt, Russell, West, Robertson, we need to estimate
what their STL and BLK per game would have been.

Approach:
  Train TWO separate Ridge Regression models on post-1973 data:
    STL model: learns steal rates from position, pace, playmaking, etc.
    BLK model: learns block rates from position, height, rebounding, etc.

  Then apply to pre-1973 players using only stats available in that era.

Model: Ridge Regression (L2 regularized linear model)
  - Stable for extrapolation to out-of-distribution data
  - Regularization prevents overfitting on correlated features
  - Interpretable coefficients we can sanity-check
  - 5-fold cross-validation for honest R² reporting

Feature rationale:
  BLK/G ≈ f(position, height, rebounds, fouls, minutes, FGA/pace, ...)
    - Taller players block more shots (height_inches)
    - Centers block more than guards (pos_num)
    - More rebounds = more time near the rim (trb_pg)
    - More FGA = faster pace = more block opportunities (fga_pg)
    - More fouls can indicate aggressive interior defense (pf_pg)

  STL/G ≈ f(position, height, assists, fouls, minutes, FGA/pace, ...)
    - Guards steal more than centers (pos_num)
    - Shorter/quicker players steal more (height_inches inverse)
    - High-assist players are in passing lanes more (apg)
    - More FGA = faster pace = more steal opportunities (fga_pg)
    - Active hands on defense correlate with fouls (pf_pg)

No hand-tuned boosts or special cases. Position and height are
learned features, not hardcoded rules.

Version: 3.0
Author: Samir Kerkar
"""

import numpy as np
import pandas as pd
import logging
from typing import Optional, Tuple

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
#  FEATURE DEFINITIONS
# ═══════════════════════════════════════════════════════════════════════════════

# Features available in ALL eras (pre-1973 and post-1973)
UNIVERSAL_FEATURES = [
    "pos_num",        # position numeric (1=PG → 5=C)
    "height_inches",  # player height in inches (strong BLK predictor)
    "trb_pg",         # total rebounds per game (rim presence proxy)
    "pf_pg",          # personal fouls per game (defensive aggression)
    "apg",            # assists per game (passing lane awareness for STL)
    "mpg",            # minutes per game (more minutes = more opportunities)
    "ppg",            # points per game (overall player quality)
    "fga_pg",         # field goal attempts per game (player usage)
    "league_fga_pg",  # league avg FGA per game that season (era pace signal)
    "team_win_pct",   # team quality (good teams may have better defenders)
]

# Additional features only available post-1973 (training only)
POST_73_FEATURES = [
    "orb_pg",         # offensive rebounds (rim presence)
    "drb_pg",         # defensive rebounds
    "tov_pg",         # turnovers (ball-handling / gambling proxy)
]

# All features used during training
TRAINING_FEATURES = UNIVERSAL_FEATURES + POST_73_FEATURES

# Position encoding
POS_MAP = {
    "PG": 1, "SG": 2, "G": 1.5, "SF": 3, "PF": 4, "F": 3.5,
    "C": 5, "FC": 4.5, "GF": 2.5, "Guard": 1.5, "Forward": 3.5,
    "Center": 5,
}


def _pos_num(pos_str: str) -> float:
    """Convert position string to numeric 1-5."""
    if not pos_str or (isinstance(pos_str, float) and np.isnan(pos_str)):
        return 3.0
    pos = str(pos_str).strip().upper().split("-")[0].split("/")[0]
    return POS_MAP.get(pos, 3.0)


def _height_to_inches(height_str) -> float:
    """Convert height string '6-9' to inches (81). Returns 0 if invalid."""
    if not height_str or height_str == "":
        return 0.0
    try:
        s = str(height_str).strip()
        if "-" in s:
            parts = s.split("-")
            return float(int(parts[0]) * 12 + int(parts[1]))
        return 0.0
    except (ValueError, IndexError, TypeError):
        return 0.0


class DefensiveStatImputer:
    """Trains and applies models to predict STL/G and BLK/G.

    Usage:
        imputer = DefensiveStatImputer()
        imputer.train(post_73_season_df)
        stl, blk = imputer.predict(player_row)
    """

    def __init__(self):
        self.stl_model = None
        self.blk_model = None
        self.stl_scaler = None
        self.blk_scaler = None
        self.stl_cap = 3.5
        self.blk_cap = 3.5
        self.stl_r2 = None
        self.blk_r2 = None
        self.is_trained = False

    def _prepare_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Ensure all features exist and are numeric."""
        work = df.copy()

        if "pos_num" not in work.columns:
            if "position" in work.columns:
                work["pos_num"] = work["position"].apply(_pos_num)
            else:
                work["pos_num"] = 3.0

        if "height_inches" not in work.columns:
            if "height" in work.columns:
                work["height_inches"] = work["height"].apply(_height_to_inches)
            else:
                work["height_inches"] = 0.0

        if "trb_pg" not in work.columns:
            if "orb_pg" in work.columns and "drb_pg" in work.columns:
                work["trb_pg"] = work["orb_pg"].fillna(0) + work["drb_pg"].fillna(0)
            elif "rpg" in work.columns:
                work["trb_pg"] = work["rpg"]
            else:
                work["trb_pg"] = 0.0

        if "fga_pg" not in work.columns:
            work["fga_pg"] = 0.0

        if "league_fga_pg" not in work.columns:
            work["league_fga_pg"] = 14.0  # ~modern era default

        if "team_win_pct" not in work.columns:
            work["team_win_pct"] = 0.5

        for feat in TRAINING_FEATURES:
            if feat not in work.columns:
                work[feat] = 0.0

        return work

    def train(self, season_df: pd.DataFrame,
              min_mpg: float = 15.0, min_gp: int = 20) -> dict:
        """Train STL and BLK prediction models on post-1973 data.

        Returns dict with training metrics.
        """
        try:
            from sklearn.linear_model import Ridge
            from sklearn.preprocessing import StandardScaler
            from sklearn.model_selection import cross_val_score
        except ImportError:
            logger.warning("scikit-learn not installed")
            return {"error": "scikit-learn not installed"}

        df = self._prepare_features(season_df)

        if "season_year" in df.columns:
            df = df[df["season_year"] >= 1974]
        if "gp" in df.columns:
            df = df[df["gp"] >= min_gp]
        if "mpg" in df.columns:
            df = df[df["mpg"] >= min_mpg]

        df = df[df["spg"].notna() & (df["spg"] > 0)]
        df = df[df["bpg"].notna() & (df["bpg"] >= 0)]

        # Fill missing heights with position-based median
        if (df["height_inches"] == 0).any():
            pos_medians = df[df["height_inches"] > 0].groupby(
                "pos_num")["height_inches"].median()
            for idx in df[df["height_inches"] == 0].index:
                pos = df.loc[idx, "pos_num"]
                df.loc[idx, "height_inches"] = pos_medians.get(pos, 78)

        df = df.dropna(subset=TRAINING_FEATURES + ["spg", "bpg"])

        if len(df) < 200:
            logger.warning(f"Only {len(df)} training rows, need 200+")
            return {"error": f"insufficient data: {len(df)} rows"}

        self.stl_cap = float(df["spg"].quantile(0.95))
        self.blk_cap = float(df["bpg"].quantile(0.95))

        X = df[TRAINING_FEATURES].fillna(0).values.astype(float)
        y_stl = df["spg"].values.astype(float)
        y_blk = df["bpg"].values.astype(float)

        # ── Train STL model ──
        self.stl_scaler = StandardScaler()
        X_stl = self.stl_scaler.fit_transform(X)
        self.stl_model = Ridge(alpha=10.0)
        stl_cv = cross_val_score(self.stl_model, X_stl, y_stl, cv=5, scoring="r2")
        self.stl_model.fit(X_stl, y_stl)
        self.stl_r2 = float(np.mean(stl_cv))

        # ── Train BLK model ──
        self.blk_scaler = StandardScaler()
        X_blk = self.blk_scaler.fit_transform(X)
        self.blk_model = Ridge(alpha=10.0)
        blk_cv = cross_val_score(self.blk_model, X_blk, y_blk, cv=5, scoring="r2")
        self.blk_model.fit(X_blk, y_blk)
        self.blk_r2 = float(np.mean(blk_cv))

        self.is_trained = True

        logger.info(f"Defensive imputer trained: "
                    f"STL R²={self.stl_r2:.4f}, BLK R²={self.blk_r2:.4f}, "
                    f"n={len(df)}")

        return {
            "stl_r2_cv": round(self.stl_r2, 4),
            "blk_r2_cv": round(self.blk_r2, 4),
            "n_train": len(df),
            "stl_cap": round(self.stl_cap, 2),
            "blk_cap": round(self.blk_cap, 2),
        }

    def predict(self, player_row: dict) -> Tuple[float, float]:
        """Predict STL/G and BLK/G for a pre-1973 player.

        Uses only UNIVERSAL_FEATURES. POST_73_FEATURES set to 0.
        """
        if not self.is_trained:
            return (0.0, 0.0)

        features = {}
        for feat in TRAINING_FEATURES:
            if feat in UNIVERSAL_FEATURES:
                val = player_row.get(feat, 0)
                if val is None or (isinstance(val, float) and np.isnan(val)):
                    val = 0
                features[feat] = float(val)
            else:
                features[feat] = 0.0

        # Fallbacks
        if features.get("trb_pg", 0) == 0:
            features["trb_pg"] = float(player_row.get("rpg", 0) or 0)

        if features.get("pos_num", 0) == 0:
            features["pos_num"] = _pos_num(player_row.get("position", "SF"))

        if features.get("height_inches", 0) == 0:
            hi = _height_to_inches(player_row.get("height", ""))
            if hi == 0:
                pos = features["pos_num"]
                pos_height = {1: 74, 2: 77, 3: 79, 4: 81, 5: 83}
                hi = pos_height.get(int(round(pos)), 78)
            features["height_inches"] = hi

        X = np.array([[features[f] for f in TRAINING_FEATURES]])

        X_stl = self.stl_scaler.transform(X)
        stl_pred = max(0.0, min(self.stl_cap, float(self.stl_model.predict(X_stl)[0])))

        X_blk = self.blk_scaler.transform(X)
        blk_pred = max(0.0, min(self.blk_cap, float(self.blk_model.predict(X_blk)[0])))

        return (round(stl_pred, 2), round(blk_pred, 2))

    def get_diagnostics(self) -> dict:
        """Return model diagnostics with unscaled coefficients.

        Shows exactly how much each feature contributes per unit.
        e.g., height_inches: +0.02 means each inch adds ~0.02 BLK/G
        """
        if not self.is_trained:
            return {"error": "not trained"}

        stl_coefs = {}
        blk_coefs = {}
        for i, feat in enumerate(TRAINING_FEATURES):
            stl_coefs[feat] = round(
                float(self.stl_model.coef_[i] / self.stl_scaler.scale_[i]), 6)
            blk_coefs[feat] = round(
                float(self.blk_model.coef_[i] / self.blk_scaler.scale_[i]), 6)

        return {
            "stl_r2_cv": self.stl_r2,
            "blk_r2_cv": self.blk_r2,
            "stl_coefficients": stl_coefs,
            "blk_coefficients": blk_coefs,
            "stl_intercept": round(float(self.stl_model.intercept_), 4),
            "blk_intercept": round(float(self.blk_model.intercept_), 4),
        }
