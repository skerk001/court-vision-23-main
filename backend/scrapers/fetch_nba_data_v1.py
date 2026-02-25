"""Fetch real NBA stats via nba_api and compute PMI v41d.

Outputs JSON files matching court-vision-23's frontend interfaces:
  - players_regular.json  → PlayerData[] for regular season
  - players_playoffs.json → PlayerData[] for playoffs
  - seasons_regular.json  → { [bbref_id]: SeasonData[] }
  - seasons_playoffs.json → { [bbref_id]: SeasonData[] }

Usage:
  python -m backend.scrapers.fetch_nba_data [--top N] [--seasons N]
"""

import json
import time
import logging
import argparse
from pathlib import Path
from typing import Optional

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)
DATA_DIR = Path(__file__).parent.parent / "data"


# ═══════════════════════════════════════════════════════════════════════════════
#  NBA API HEADERS — Required to avoid blocks from stats.nba.com
# ═══════════════════════════════════════════════════════════════════════════════

# NBA.com blocks requests without proper browser headers.
# This must be set BEFORE any nba_api imports.
HEADERS = {
    "Host": "stats.nba.com",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:130.0) Gecko/20100101 Firefox/130.0",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br",
    "x-nba-stats-origin": "stats",
    "x-nba-stats-token": "true",
    "Connection": "keep-alive",
    "Referer": "https://stats.nba.com/",
    "Origin": "https://stats.nba.com",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
    "Pragma": "no-cache",
    "Cache-Control": "no-cache",
}

# Patch nba_api's default headers before any endpoint imports
# (nba_api respects custom headers passed to each endpoint constructor)


# ═══════════════════════════════════════════════════════════════════════════════
#  NBA API HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def _safe_api_call(func, *args, retries=4, delay=2.0, **kwargs):
    """Call nba_api with retries, rate-limiting, and proper headers."""
    # Always inject browser headers (required by NBA.com)
    kwargs["headers"] = HEADERS
    # Ensure reasonable timeout
    if "timeout" not in kwargs:
        kwargs["timeout"] = 60

    for attempt in range(retries):
        try:
            result = func(*args, **kwargs)
            time.sleep(delay)  # Rate limit — NBA.com throttles fast requests
            return result
        except Exception as e:
            wait = delay * (attempt + 2)  # Progressive backoff: 4s, 6s, 8s, 10s
            if attempt < retries - 1:
                logger.warning(f"API call failed (attempt {attempt+1}): {e} — retrying in {wait:.0f}s")
                time.sleep(wait)
            else:
                logger.error(f"API call failed after {retries} attempts: {e}")
                return None
    return None


# ═══════════════════════════════════════════════════════════════════════════════
#  FETCH PLAYER LIST
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_all_players() -> pd.DataFrame:
    """Get all historical NBA players from nba_api."""
    from nba_api.stats.static import players as nba_players

    all_p = nba_players.get_players()
    df = pd.DataFrame(all_p)
    df = df.rename(columns={
        "id": "nba_api_id",
        "full_name": "full_name",
        "is_active": "is_active",
    })
    logger.info(f"Fetched {len(df)} players from nba_api")
    return df


# ═══════════════════════════════════════════════════════════════════════════════
#  FETCH CAREER STATS (per player)
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_player_career(player_id: int) -> Optional[dict]:
    """Fetch career + season-by-season stats for a player."""
    from nba_api.stats.endpoints import playercareerstats

    result = _safe_api_call(
        playercareerstats.PlayerCareerStats,
        player_id=player_id,
        per_mode36="PerGame",
        timeout=60,
    )
    if result is None:
        return None

    data = {}
    try:
        # Regular season per-game
        reg = result.get_data_frames()[0]  # SeasonTotalsRegularSeason
        if not reg.empty:
            data["regular"] = reg

        # Playoff per-game
        playoff = result.get_data_frames()[2]  # SeasonTotalsPostSeason
        if not playoff.empty:
            data["playoffs"] = playoff

        # Career totals
        career_reg = result.get_data_frames()[1]  # CareerTotalsRegularSeason
        if not career_reg.empty:
            data["career_regular"] = career_reg

        career_ply = result.get_data_frames()[3]  # CareerTotalsPostSeason
        if not career_ply.empty:
            data["career_playoffs"] = career_ply

    except (IndexError, Exception) as e:
        logger.warning(f"Error parsing career for {player_id}: {e}")

    return data if data else None


def fetch_player_career_totals(player_id: int) -> Optional[dict]:
    """Fetch career totals (not per-game) for counting stats."""
    from nba_api.stats.endpoints import playercareerstats

    result = _safe_api_call(
        playercareerstats.PlayerCareerStats,
        player_id=player_id,
        per_mode36="Totals",
        timeout=60,
    )
    if result is None:
        return None

    data = {}
    try:
        reg = result.get_data_frames()[0]
        if not reg.empty:
            data["regular"] = reg
        playoff = result.get_data_frames()[2]
        if not playoff.empty:
            data["playoffs"] = playoff
    except (IndexError, Exception):
        pass

    return data if data else None


# ═══════════════════════════════════════════════════════════════════════════════
#  FETCH CLUTCH STATS
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_clutch_stats(season: str, season_type: str = "Regular Season") -> Optional[pd.DataFrame]:
    """Fetch clutch stats for a season (last 5 min, ±5 pts)."""
    from nba_api.stats.endpoints import leaguedashplayerclutch

    result = _safe_api_call(
        leaguedashplayerclutch.LeagueDashPlayerClutch,
        season=season,
        season_type_all_star=season_type,
        clutch_time="Last 5 Minutes",
        ahead_behind="Ahead or Behind",
        point_diff=5,
        per_mode_detailed="PerGame",
        timeout=60,
    )
    if result is None:
        return None

    try:
        df = result.get_data_frames()[0]
        return df if not df.empty else None
    except (IndexError, Exception):
        return None


# ═══════════════════════════════════════════════════════════════════════════════
#  PROCESS PLAYER → FRONTEND SHAPES
# ═══════════════════════════════════════════════════════════════════════════════

def _season_label(season_id: str) -> str:
    """Convert '2023-24' or '22023' format to '2023-24'."""
    s = str(season_id)
    if "-" in s and len(s) <= 8:
        return s
    # nba_api sometimes returns numeric season IDs
    try:
        year = int(s[:4]) if len(s) >= 4 else int(s)
        return f"{year}-{str(year+1)[-2:]}"
    except (ValueError, TypeError):
        return s


def _season_year(season_label: str) -> int:
    """Extract start year from '2023-24' → 2023."""
    try:
        return int(str(season_label).split("-")[0])
    except (ValueError, TypeError):
        return 2024


def _safe_float(val, default=0.0):
    """Safely convert to float."""
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def process_player_seasons(per_game_df: pd.DataFrame, player_info: dict,
                           is_playoff: bool = False) -> list:
    """Convert nba_api per-game DataFrame to list of SeasonData dicts."""
    from backend.scrapers.pmi_v3_engine import _pos_num

    seasons = []
    pos = player_info.get("position", "SF")
    pos_num = _pos_num(pos)

    for _, row in per_game_df.iterrows():
        season = _season_label(str(row.get("SEASON_ID", "")))
        year = _season_year(season)

        gp = int(row.get("GP", 0) or 0)
        if gp == 0:
            continue

        ppg = _safe_float(row.get("PTS"))
        rpg = _safe_float(row.get("REB"))
        apg = _safe_float(row.get("AST"))
        spg = _safe_float(row.get("STL"))
        bpg = _safe_float(row.get("BLK"))
        mpg = _safe_float(row.get("MIN"))
        fg_pct = _safe_float(row.get("FG_PCT"))
        fga = _safe_float(row.get("FGA"))
        fta = _safe_float(row.get("FTA"))
        ftm = _safe_float(row.get("FTM"))
        fg3m = _safe_float(row.get("FG3M"))
        tov = _safe_float(row.get("TOV"))
        orb = _safe_float(row.get("OREB"))
        drb = _safe_float(row.get("DREB"))
        pf = _safe_float(row.get("PF"))

        # True Shooting %
        pts = _safe_float(row.get("PTS"))
        tsa = 2 * (fga + 0.44 * fta)
        ts_pct = pts / tsa if tsa > 0 else 0

        season_dict = {
            "season": season,
            "year": year,
            "gp": gp,
            "mpg": round(mpg, 1),
            "ppg": round(ppg, 1),
            "rpg": round(rpg, 1),
            "apg": round(apg, 1),
            "spg": round(spg, 1),
            "bpg": round(bpg, 1),
            "fg_pct": round(fg_pct, 4) if fg_pct else 0,
            "ts_pct": round(ts_pct, 4),
            # PMI inputs
            "tov_pg": round(tov, 1),
            "orb_pg": round(orb, 1),
            "drb_pg": round(drb, 1),
            "fta_pg": round(fta, 1),
            "fg3m_pg": round(fg3m, 1),
            "pf_pg": round(pf, 1),
            # ML imputer features
            "fga_pg": round(fga, 1),
            "trb_pg": round(rpg, 1),
        }
        seasons.append(season_dict)

    return seasons


def compute_pmi_for_seasons(seasons_list: list, player_info: dict,
                            all_seasons_data: dict,
                            is_playoff: bool = False) -> list:
    """Compute PMI v3 for each season using league-wide z-scores.

    all_seasons_data: { season_label: [list of all player season dicts for that season] }
    """
    from backend.scrapers.pmi_v3_engine import (
        compute_pmi_season, _pos_num,
        compute_season_league_stats, compute_awc,
    )

    pos = player_info.get("position", "SF")
    pos_num = _pos_num(pos)

    for season_dict in seasons_list:
        season = season_dict["season"]
        year = season_dict.get("year", 2020)

        # Get league stats for this season
        league_data = all_seasons_data.get(season, [])
        if league_data:
            league_df = pd.DataFrame(league_data)
            league = compute_season_league_stats(league_df)
        else:
            # Fallback: use approximate league averages
            league = {
                "ppg_mean": 14.0, "ppg_std": 6.5,
                "apg_mean": 2.8, "apg_std": 2.5,
                "tov_pg_mean": 1.5, "tov_pg_std": 0.8,
                "orb_pg_mean": 1.0, "orb_pg_std": 0.8,
                "spg_mean": 0.8, "spg_std": 0.5,
                "bpg_mean": 0.5, "bpg_std": 0.5,
                "drb_pg_mean": 2.5, "drb_pg_std": 1.5,
                "pf_pg_mean": 2.2, "pf_pg_std": 0.8,
                "ts_pct_mean": 0.540, "ts_pct_std": 0.05,
            }

        result = compute_pmi_season(season_dict, league, pos_num, year)

        # AWC for this season
        total_min = round(season_dict["mpg"] * season_dict["gp"])
        awc = compute_awc(result["pmi"], total_min)

        season_dict["opmi"] = result["opmi"]
        season_dict["dpmi"] = result["dpmi"]
        season_dict["pmi"] = result["pmi"]
        season_dict["awc"] = round(awc, 1)
        season_dict["peak_pmi"] = result["pmi"]  # will be updated after all seasons

    # Update peak_pmi to be the max PMI across all seasons
    if seasons_list:
        peak = max(s["pmi"] for s in seasons_list)
        for s in seasons_list:
            s["peak_pmi"] = round(peak, 2)

    return seasons_list


def build_career_summary(seasons: list, totals_df: Optional[pd.DataFrame],
                         player_info: dict, clutch_career: Optional[dict],
                         is_playoff: bool = False) -> dict:
    """Build PlayerData career summary from seasons + totals."""
    from backend.scrapers.pmi_v3_engine import compute_career_pmi, compute_awc

    if not seasons:
        return {}

    total_gp = sum(s["gp"] for s in seasons)
    total_min = sum(round(s["mpg"] * s["gp"]) for s in seasons)

    # Career PMI (minutes-weighted + Bayesian regression)
    career_pmi = compute_career_pmi(seasons, is_playoff)
    career_opmi = compute_career_pmi(
        [{"pmi": s["opmi"], "gp": s["gp"], "mpg": s["mpg"]} for s in seasons],
        is_playoff
    )
    career_dpmi = compute_career_pmi(
        [{"pmi": s["dpmi"], "gp": s["gp"], "mpg": s["mpg"]} for s in seasons],
        is_playoff
    )

    # Peak
    peak_season = max(seasons, key=lambda s: s["pmi"])

    # Career averages (GP-weighted)
    def _wavg(key):
        total = sum(s.get(key, 0) * s["gp"] for s in seasons)
        return round(total / total_gp, 1) if total_gp > 0 else 0

    # Career totals from nba_api totals endpoint
    pts = reb = ast = stl = blk = total_tov = 0
    if totals_df is not None and not totals_df.empty:
        # Sum across all rows (multi-team seasons have multiple rows)
        pts = int(totals_df["PTS"].sum())
        reb = int(totals_df["REB"].sum())
        ast = int(totals_df["AST"].sum())
        stl = int(totals_df["STL"].sum()) if "STL" in totals_df.columns else 0
        blk = int(totals_df["BLK"].sum()) if "BLK" in totals_df.columns else 0
        total_tov = int(totals_df["TOV"].sum()) if "TOV" in totals_df.columns else 0
    else:
        pts = round(_wavg("ppg") * total_gp)
        reb = round(_wavg("rpg") * total_gp)
        ast = round(_wavg("apg") * total_gp)
        stl = round(_wavg("spg") * total_gp)
        blk = round(_wavg("bpg") * total_gp)

    # Years active
    years_list = sorted(set(s["year"] for s in seasons))
    if years_list:
        start = years_list[0]
        end = years_list[-1]
        years_str = f"{start}-{end + 1}" if not player_info.get("is_active") else f"{start}-pres."
    else:
        years_str = "?"

    # AWC
    awc = compute_awc(career_pmi, total_min)
    oawc = compute_awc(career_opmi, total_min)
    dawc = compute_awc(career_dpmi, total_min)

    # Weighted fg_pct and ts_pct
    fg_pct_sum = sum(s.get("fg_pct", 0) * s["gp"] for s in seasons)
    ts_pct_sum = sum(s.get("ts_pct", 0) * s["gp"] for s in seasons)

    result = {
        "full_name": player_info["full_name"],
        "bbref_id": player_info.get("bbref_id", ""),
        "nba_api_id": player_info["nba_api_id"],
        "is_active": player_info.get("is_active", False),
        "position": player_info.get("position", "?"),
        "years": years_str,
        "gp": total_gp,
        "ppg": _wavg("ppg"),
        "rpg": _wavg("rpg"),
        "apg": _wavg("apg"),
        "spg": _wavg("spg"),
        "bpg": _wavg("bpg"),
        "tov": None,  # per-game tov not in career summary
        "fg_pct": round(fg_pct_sum / total_gp, 4) if total_gp > 0 else 0,
        "ts_pct": round(ts_pct_sum / total_gp, 4) if total_gp > 0 else 0,
        "rts_pct": round(ts_pct_sum / total_gp - 0.540, 4) if total_gp > 0 else 0,
        "pmi": round(career_pmi, 2),
        "opmi": round(career_opmi, 2),
        "dpmi": round(career_dpmi, 2),
        "peak_pmi": round(peak_season["pmi"], 2),
        "peak_season": peak_season["season"],
        "pie": 15.0,  # placeholder — requires PIE calculation
        "awc": round(awc, 1),
        "oawc": round(oawc, 1),
        "dawc": round(dawc, 1),
        "min": total_min,
        "pts": pts,
        "reb": reb,
        "ast": ast,
        "stl": stl,
        "blk": blk,
        "total_tov": total_tov,
        "seasons": len(seasons),
    }

    # Add clutch if available
    if clutch_career:
        result.update(clutch_career)

    return result


# ═══════════════════════════════════════════════════════════════════════════════
#  BBREF ID MAPPING
# ═══════════════════════════════════════════════════════════════════════════════

def guess_bbref_id(full_name: str, nba_id: int) -> str:
    """Generate a Basketball Reference ID from a player name.

    Format: first 5 chars of last name + first 2 of first name + '01'
    e.g., 'LeBron James' → 'jamesle01'
    """
    parts = full_name.strip().split()
    if len(parts) < 2:
        return f"player{nba_id}"
    first = parts[0].lower().replace("'", "").replace(".", "")[:2]
    last = parts[-1].lower().replace("'", "").replace(".", "")[:5]
    return f"{last}{first}01"


# ═══════════════════════════════════════════════════════════════════════════════
#  POSITION DETECTION
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_player_info(player_id: int) -> Optional[dict]:
    """Fetch detailed player info including position."""
    from nba_api.stats.endpoints import commonplayerinfo

    result = _safe_api_call(
        commonplayerinfo.CommonPlayerInfo,
        player_id=player_id,
        timeout=60,
    )
    if result is None:
        return None

    try:
        df = result.get_data_frames()[0]
        if df.empty:
            return None
        row = df.iloc[0]

        # Convert height string "6-9" to inches (81)
        height_str = str(row.get("HEIGHT", "") or "")
        height_inches = 0
        if "-" in height_str:
            parts = height_str.split("-")
            try:
                height_inches = int(parts[0]) * 12 + int(parts[1])
            except (ValueError, IndexError):
                height_inches = 0

        return {
            "position": str(row.get("POSITION", "")).split("-")[0].strip() or "SF",
            "height": height_str,
            "height_inches": height_inches,
            "weight": row.get("WEIGHT", ""),
            "draft_year": row.get("DRAFT_YEAR", ""),
            "country": row.get("COUNTRY", ""),
        }
    except Exception:
        return None


# ═══════════════════════════════════════════════════════════════════════════════
#  MAIN INGESTION PIPELINE
# ═══════════════════════════════════════════════════════════════════════════════

def run_ingestion(top_n: int = 100, min_seasons: int = 5, recent_seasons: int = 0):
    """Run the full data ingestion pipeline.

    1. Fetch player list from nba_api
    2. Filter to top N by career games played
    3. Fetch per-game + totals for each player
    4. Compute PMI v41d for each season
    5. Build career summaries
    6. Output JSON files matching frontend interfaces

    Args:
        top_n: Number of players to include (by career GP)
        min_seasons: Minimum seasons played to include
        recent_seasons: If >0, only fetch this many recent seasons (for testing)
    """
    DATA_DIR.mkdir(exist_ok=True)

    print(f"🏀 Courtside Data Ingestion — Top {top_n} players")
    print("=" * 60)

    # Step 1: Get all players
    print("\n📋 Step 1: Fetching player list...")
    all_players = fetch_all_players()

    # Step 2: Fetch career stats for all to find top N by GP
    print(f"\n📊 Step 2: Finding top {top_n} players by career GP...")
    print("  (This fetches career stats for all active + many historical players)")

    # Use league leaders endpoint to get high-GP players efficiently
    from nba_api.stats.endpoints import alltimeleadersgrids

    try:
        leaders = _safe_api_call(
            alltimeleadersgrids.AllTimeLeadersGrids,
            per_mode_simple="Totals",
            season_type="Regular Season",
            topx=top_n * 2,  # fetch more to filter
            timeout=60,
        )
        if leaders:
            gp_df = leaders.get_data_frames()[0]  # GPLeaders
            top_ids = gp_df["PLAYER_ID"].head(top_n * 2).tolist()
            print(f"  Found {len(top_ids)} candidates from all-time leaders")
        else:
            top_ids = all_players.head(top_n * 2)["nba_api_id"].tolist()
    except Exception as e:
        logger.warning(f"Could not fetch all-time leaders: {e}")
        top_ids = all_players.head(top_n * 2)["nba_api_id"].tolist()

    # Step 3: Fetch detailed stats for each candidate
    print(f"\n📈 Step 3: Fetching per-game stats for {len(top_ids)} candidates...")

    all_regular_seasons = {}   # { season_label: [ {season_dict}, ... ] }  for league z-scores
    all_playoff_seasons = {}
    player_data = {}  # { nba_id: { info, regular_seasons, playoff_seasons, ... } }

    for i, nba_id in enumerate(top_ids):
        player_row = all_players[all_players["nba_api_id"] == nba_id]
        if player_row.empty:
            continue
        name = player_row.iloc[0]["full_name"]
        is_active = bool(player_row.iloc[0].get("is_active", False))

        if (i + 1) % 10 == 0 or i == 0:
            print(f"  [{i+1}/{len(top_ids)}] {name}...")

        # Fetch per-game career
        career = fetch_player_career(nba_id)
        if career is None:
            continue

        # Fetch totals
        totals = fetch_player_career_totals(nba_id)

        # Fetch player info (position)
        info = fetch_player_info(nba_id)
        position = info["position"] if info else "SF"

        player_info = {
            "nba_api_id": nba_id,
            "full_name": name,
            "is_active": is_active,
            "position": position,
            "height": info.get("height", "") if info else "",
            "height_inches": info.get("height_inches", 0) if info else 0,
            "bbref_id": guess_bbref_id(name, nba_id),
        }

        # Process regular season
        reg_seasons = []
        if "regular" in career:
            reg_seasons = process_player_seasons(career["regular"], player_info, is_playoff=False)

        # Process playoffs
        ply_seasons = []
        if "playoffs" in career:
            ply_seasons = process_player_seasons(career["playoffs"], player_info, is_playoff=True)

        if len(reg_seasons) < min_seasons:
            continue

        # Accumulate league-wide season data for z-scores
        for s in reg_seasons:
            all_regular_seasons.setdefault(s["season"], []).append(s)
        for s in ply_seasons:
            all_playoff_seasons.setdefault(s["season"], []).append(s)

        player_data[nba_id] = {
            "info": player_info,
            "regular": reg_seasons,
            "playoffs": ply_seasons,
            "totals_regular": totals.get("regular") if totals else None,
            "totals_playoffs": totals.get("playoffs") if totals else None,
        }

    # Trim to top N by career regular season GP
    sorted_players = sorted(
        player_data.values(),
        key=lambda p: sum(s["gp"] for s in p["regular"]),
        reverse=True,
    )[:top_n]

    print(f"\n  Selected {len(sorted_players)} players for final output")

    # Step 3b: Train defensive stat imputer for pre-1973 players
    print("\n🤖 Step 3b: Training ML imputer for pre-1973 steals/blocks...")

    from backend.scrapers.defensive_imputer import DefensiveStatImputer

    # Compute league-average FGA per season (era pace signal)
    league_fga_by_season = {}
    for season_label, season_list in all_regular_seasons.items():
        fga_vals = [s.get("fga_pg", 0) for s in season_list if s.get("fga_pg", 0) > 0]
        if fga_vals:
            league_fga_by_season[season_label] = round(np.mean(fga_vals), 1)

    # Build training DataFrame from ALL post-1973 regular season data
    imputer_rows = []
    for season_label, season_list in all_regular_seasons.items():
        lg_fga = league_fga_by_season.get(season_label, 14.0)
        for s in season_list:
            if s.get("year", 2000) >= 1974 and s.get("spg", 0) > 0:
                row = dict(s)
                row["league_fga_pg"] = lg_fga
                imputer_rows.append(row)

    imputer = DefensiveStatImputer()
    if imputer_rows:
        imputer_df = pd.DataFrame(imputer_rows)
        # Add season_year for the imputer's year filter
        if "season_year" not in imputer_df.columns:
            imputer_df["season_year"] = imputer_df.get("year", 2000)
        imp_metrics = imputer.train(imputer_df)
        print(f"  STL R²={imp_metrics.get('stl_r2_cv', '?')}, "
              f"BLK R²={imp_metrics.get('blk_r2_cv', '?')}, "
              f"n={imp_metrics.get('n_train', 0)}")
    else:
        print("  ⚠️  No post-1973 data for imputer training")

    # Historical league FGA averages for pre-1973 eras (from bbref)
    # 1960s NBA averaged ~18-19 FGA/G per player due to extreme pace
    HISTORICAL_LEAGUE_FGA = {
        1947: 20.0, 1950: 19.5, 1955: 18.5, 1960: 18.0,
        1965: 17.5, 1970: 17.0, 1973: 16.0,
    }

    def _get_historical_league_fga(year: int) -> float:
        """Interpolate historical league FGA for a given year."""
        years = sorted(HISTORICAL_LEAGUE_FGA.keys())
        if year <= years[0]:
            return HISTORICAL_LEAGUE_FGA[years[0]]
        if year >= years[-1]:
            return HISTORICAL_LEAGUE_FGA[years[-1]]
        for i in range(len(years) - 1):
            if years[i] <= year <= years[i + 1]:
                frac = (year - years[i]) / (years[i + 1] - years[i])
                v0 = HISTORICAL_LEAGUE_FGA[years[i]]
                v1 = HISTORICAL_LEAGUE_FGA[years[i + 1]]
                return round(v0 + frac * (v1 - v0), 1)
        return 16.0

    # Step 3c: Impute STL/BLK for pre-1973 seasons
    imputed_count = 0
    if imputer.is_trained:
        for p_data in sorted_players:
            info = p_data["info"]
            for s in p_data["regular"] + p_data["playoffs"]:
                year = s.get("year", 2000)
                has_stl = (s.get("spg", 0) or 0) > 0
                has_blk = (s.get("bpg", 0) or 0) > 0

                if year < 1974 and not has_stl and not has_blk:
                    # Build prediction row from season stats + player info
                    pred_row = {
                        "position": info.get("position", "SF"),
                        "height_inches": info.get("height_inches", 0),
                        "height": info.get("height", ""),
                        "trb_pg": s.get("trb_pg", s.get("rpg", 0)),
                        "rpg": s.get("rpg", 0),
                        "pf_pg": s.get("pf_pg", 0),
                        "apg": s.get("apg", 0),
                        "mpg": s.get("mpg", 0),
                        "ppg": s.get("ppg", 0),
                        "fga_pg": s.get("fga_pg", 0),
                        "league_fga_pg": _get_historical_league_fga(year),
                        "team_win_pct": s.get("team_win_pct", 0.5),
                    }
                    stl, blk = imputer.predict(pred_row)
                    s["spg"] = stl
                    s["bpg"] = blk
                    s["imputed_defense"] = True
                    imputed_count += 1

        print(f"  ✅ Imputed STL/BLK for {imputed_count} pre-1973 player-seasons")

    # Step 4: Compute PMI for all seasons
    print("\n🧮 Step 4: Computing PMI v3 for all player-seasons...")

    players_regular = []
    players_playoffs = []
    seasons_regular = {}
    seasons_playoffs = {}

    for p_data in sorted_players:
        info = p_data["info"]
        name = info["full_name"]
        bbref_id = info["bbref_id"]

        # Compute PMI for regular seasons
        reg = compute_pmi_for_seasons(
            p_data["regular"], info, all_regular_seasons, is_playoff=False
        )

        # Compute PMI for playoff seasons
        ply = compute_pmi_for_seasons(
            p_data["playoffs"], info, all_playoff_seasons, is_playoff=True
        )

        # Build career summaries (clutch added in Step 4b below)
        reg_summary = build_career_summary(
            reg, p_data["totals_regular"], info, clutch_career=None, is_playoff=False
        )
        ply_summary = build_career_summary(
            ply, p_data["totals_playoffs"], info, clutch_career=None, is_playoff=True
        ) if ply else None

        if reg_summary:
            players_regular.append(reg_summary)
        if ply_summary:
            players_playoffs.append(ply_summary)

        # Clean season data for frontend (remove PMI intermediate fields)
        def _clean_season(s):
            return {k: v for k, v in s.items()
                    if k in ("season", "year", "gp", "mpg", "ppg", "rpg", "apg",
                             "spg", "bpg", "fg_pct", "ts_pct", "pmi", "opmi",
                             "dpmi", "awc", "peak_pmi", "cpmi")}

        seasons_regular[bbref_id] = [_clean_season(s) for s in reg]
        if ply:
            seasons_playoffs[bbref_id] = [_clean_season(s) for s in ply]

    # Step 4b: Fetch clutch stats and compute CPMI
    print("\n🔥 Step 4b: Fetching clutch stats and computing CPMI...")

    from backend.scrapers.pmi_v3_engine import (
        compute_cpmi, compute_clutch_league_stats, build_clutch_row,
    )

    # Collect unique seasons across all players (only post-1996 have clutch data)
    all_season_labels = set()
    for p in players_regular:
        bbref = p.get("bbref_id", "")
        if bbref in seasons_regular:
            for s in seasons_regular[bbref]:
                yr = s.get("year", 0)
                if yr >= 1996:
                    all_season_labels.add(s["season"])

    # Fetch clutch data per season and build league stats + player lookups
    clutch_by_season = {}      # { season: DataFrame }
    clutch_league_by_season = {} # { season: league_stats_dict }
    clutch_fetched = 0

    for season_label in sorted(all_season_labels):
        clutch_df = fetch_clutch_stats(season_label)
        if clutch_df is not None and not clutch_df.empty:
            clutch_by_season[season_label] = clutch_df
            clutch_league_by_season[season_label] = compute_clutch_league_stats(clutch_df)
            clutch_fetched += 1
            if clutch_fetched % 5 == 0:
                print(f"  Fetched clutch data for {clutch_fetched} seasons...")
            time.sleep(0.6)  # rate limit

    print(f"  ✅ Fetched clutch stats for {clutch_fetched} seasons")

    # Compute CPMI per player per season and attach to summaries
    # Build nba_api_id → player summary mapping for quick updates
    reg_by_id = {p["nba_api_id"]: p for p in players_regular}

    # Build nba_api_id → bbref_id mapping for season-level attachment
    id_to_bbref = {p["nba_api_id"]: p["bbref_id"] for p in players_regular}

    cpmi_computed = 0
    for season_label, clutch_df in clutch_by_season.items():
        league_stats = clutch_league_by_season[season_label]

        for _, crow in clutch_df.iterrows():
            pid = int(crow.get("PLAYER_ID", 0))
            clutch_row = build_clutch_row(crow)
            cpmi = compute_cpmi(clutch_row, league_stats)

            # Attach CPMI to this player's season data (O(1) lookup)
            bbref_id = id_to_bbref.get(pid)
            if bbref_id and bbref_id in seasons_regular:
                for s in seasons_regular[bbref_id]:
                    if s.get("season") == season_label:
                        s["cpmi"] = cpmi

            # Accumulate per-player clutch data for career CPMI
            if pid in reg_by_id:
                p = reg_by_id[pid]
                if "clutch_seasons" not in p:
                    p["clutch_seasons"] = []
                p["clutch_seasons"].append({
                    "season": season_label,
                    "cpmi": cpmi,
                    "clutch_gp": clutch_row.get("clutch_gp", 0),
                    "clutch_min": clutch_row.get("clutch_min", 0),
                })
                cpmi_computed += 1

    # Compute career CPMI (clutch-minutes-weighted average)
    for p in players_regular:
        clutch_seasons = p.pop("clutch_seasons", [])
        if not clutch_seasons:
            p["cpmi"] = None
            continue

        total_clutch_min = sum(cs.get("clutch_min", 0) for cs in clutch_seasons)
        if total_clutch_min > 0:
            career_cpmi = sum(
                cs["cpmi"] * cs.get("clutch_min", 1) for cs in clutch_seasons
            ) / total_clutch_min
        else:
            career_cpmi = sum(cs["cpmi"] for cs in clutch_seasons) / len(clutch_seasons)

        p["cpmi"] = round(career_cpmi, 2)
        p["clutch_seasons_count"] = len(clutch_seasons)

    print(f"  ✅ Computed CPMI for {cpmi_computed} player-seasons")

    # Step 5: Sort and save
    print("\n💾 Step 5: Saving output files...")

    # Sort by career PMI (descending)
    players_regular.sort(key=lambda p: p.get("pmi", 0), reverse=True)
    players_playoffs.sort(key=lambda p: p.get("pmi", 0), reverse=True)

    def _write_json(data, filename):
        path = DATA_DIR / filename
        with open(path, "w") as f:
            json.dump(data, f, indent=2, default=str)
        print(f"  ✅ {filename}: {len(data) if isinstance(data, list) else len(data)} entries")

    _write_json(players_regular, "players_regular.json")
    _write_json(players_playoffs, "players_playoffs.json")
    _write_json(seasons_regular, "seasons_regular.json")
    _write_json(seasons_playoffs, "seasons_playoffs.json")

    print(f"\n✅ Done! {len(players_regular)} regular + {len(players_playoffs)} playoff players")
    print(f"   {sum(len(v) for v in seasons_regular.values())} regular season rows")
    print(f"   {sum(len(v) for v in seasons_playoffs.values())} playoff season rows")


# ═══════════════════════════════════════════════════════════════════════════════
#  CLI
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(description="Fetch NBA data and compute PMI")
    parser.add_argument("--top", type=int, default=100, help="Number of top players (default: 100)")
    parser.add_argument("--min-seasons", type=int, default=5, help="Min seasons to include (default: 5)")
    parser.add_argument("--recent", type=int, default=0, help="Only recent N seasons (0=all)")
    args = parser.parse_args()

    run_ingestion(top_n=args.top, min_seasons=args.min_seasons, recent_seasons=args.recent)
