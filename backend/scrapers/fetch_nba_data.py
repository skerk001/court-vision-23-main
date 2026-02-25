"""Optimized NBA data ingestion pipeline — batch-first architecture.

SPEED COMPARISON (all NBA history):
  Old pipeline:  ~55 min (3 API calls × 500 players = 1500 calls)
  New pipeline:  ~4 min  (~110 batch API calls total)
  Speedup:       ~14x

ARCHITECTURE:
  Instead of fetching per-player (N calls per player), we fetch per-season
  (1 call returns ALL players for that season). This reduces API calls from
  O(players) to O(seasons).

  Step 1: LeagueDashPlayerBioStats   — ~5 calls → all heights/positions
  Step 2: LeagueDashPlayerStats      — ~78 calls (1 per season × 2 types)
  Step 3: Build player DB locally    — 0 API calls
  Step 3b: ML defensive imputer      — 0 API calls
  Step 4: Compute PMI/CPMI           — 0 API calls
  Step 4b: LeagueDashPlayerClutch    — ~28 calls (1996-2024)
  Step 5: Write JSON output           — 0 API calls

  Total: ~111 API calls at 0.6s spacing ≈ 67s network + ~60s compute

Outputs:
  - players_regular.json  → PlayerData[]
  - players_playoffs.json → PlayerData[]
  - seasons_regular.json  → { [bbref_id]: SeasonData[] }
  - seasons_playoffs.json → { [bbref_id]: SeasonData[] }

Usage:
  python -m backend.scrapers.fetch_nba_data [--min-gp N] [--min-seasons N]
                                            [--start-year YYYY] [--end-year YYYY]
"""

import json
import time
import logging
import argparse
from pathlib import Path
from collections import defaultdict
from typing import Optional

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)
DATA_DIR = Path(__file__).parent.parent / "data"

# ═══════════════════════════════════════════════════════════════════════════════
#  NBA API HEADERS
# ═══════════════════════════════════════════════════════════════════════════════

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

API_DELAY = 0.6  # seconds between calls


# ═══════════════════════════════════════════════════════════════════════════════
#  HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def _api(func, *args, retries=4, delay=API_DELAY, **kwargs):
    """Call nba_api with retries and progressive backoff."""
    kwargs["headers"] = HEADERS
    kwargs.setdefault("timeout", 120)
    for attempt in range(retries):
        try:
            result = func(*args, **kwargs)
            time.sleep(delay)
            return result
        except Exception as e:
            wait = delay * (attempt + 2)
            if attempt < retries - 1:
                logger.warning(f"API attempt {attempt+1}: {e} — retry in {wait:.0f}s")
                time.sleep(wait)
            else:
                logger.error(f"API failed after {retries}: {e}")
                return None


def _season_label(year: int) -> str:
    """2023 → '2023-24'."""
    return f"{year}-{str(year + 1)[-2:]}"


def _sf(val, default=0.0):
    """Safe float."""
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def _bbref_id(name: str, nba_id: int) -> str:
    parts = name.strip().split()
    if len(parts) < 2:
        return f"player{nba_id}"
    first = parts[0].lower().replace("'", "").replace(".", "")[:2]
    last = parts[-1].lower().replace("'", "").replace(".", "")[:5]
    # Use last 4 digits of NBA API ID as disambiguator to avoid collisions
    # (e.g., Anthony Davis vs Antonio Davis both → davisan01 without this)
    suffix = str(nba_id)[-4:].zfill(4)
    return f"{last}{first}_{suffix}"


# ═══════════════════════════════════════════════════════════════════════════════
#  STEP 1: BIO DATA (heights, positions) — ~5 API calls
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_all_bios() -> dict:
    """Fetch height/position for all players across multiple eras.
    Returns {player_id: {height, height_inches, position, ...}}."""
    from nba_api.stats.endpoints import leaguedashplayerbiostats

    bio = {}
    # Sample across eras to catch retired players
    for szn in ["2024-25", "2014-15", "2004-05", "1994-95", "1984-85"]:
        result = _api(
            leaguedashplayerbiostats.LeagueDashPlayerBioStats,
            season=szn,
            season_type_all_star="Regular Season",
            per_mode_simple="PerGame",
        )
        if result is None:
            continue
        try:
            df = result.get_data_frames()[0]
            for _, row in df.iterrows():
                pid = int(row.get("PLAYER_ID", 0))
                if pid in bio:
                    continue
                ht = str(row.get("PLAYER_HEIGHT", "") or "")
                hi = 0
                if "-" in ht:
                    try:
                        p = ht.split("-")
                        hi = int(p[0]) * 12 + int(p[1])
                    except (ValueError, IndexError):
                        pass
                bio[pid] = {
                    "height": ht,
                    "height_inches": hi,
                    "weight": str(row.get("PLAYER_WEIGHT", "") or ""),
                    "country": str(row.get("COUNTRY", "") or ""),
                }
        except Exception as e:
            logger.warning(f"Bio parse {szn}: {e}")
    return bio


# ═══════════════════════════════════════════════════════════════════════════════
#  STEP 2: FETCH ALL SEASONS — O(seasons) not O(players)
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_all_seasons(start: int, end: int) -> dict:
    """Fetch per-game stats for every season. 1 API call per season per type.
    Returns {"regular": {label: DataFrame}, "playoffs": {label: DataFrame}}.
    
    NOTE: LeagueDashPlayerStats may not return data for very old seasons
    (pre-1996 or pre-1983 depending on the endpoint). The caller should
    check which seasons actually returned data and use fetch_historical_players()
    to backfill missing eras.
    """
    from nba_api.stats.endpoints import leaguedashplayerstats

    data = {"regular": {}, "playoffs": {}}
    types = [("Regular Season", "regular"), ("Playoffs", "playoffs")]
    total = (end - start + 1) * len(types)
    n = 0

    for year in range(start, end + 1):
        label = _season_label(year)
        for stype_api, stype_key in types:
            n += 1
            if n % 10 == 0 or n == 1:
                print(f"  [{n}/{total}] ({n/total*100:.0f}%) {label} {stype_key}...")

            result = _api(
                leaguedashplayerstats.LeagueDashPlayerStats,
                season=label,
                season_type_all_star=stype_api,
                per_mode_detailed="PerGame",
            )
            if result is None:
                continue
            try:
                df = result.get_data_frames()[0]
                if not df.empty:
                    df["_SEASON"] = label
                    df["_YEAR"] = year
                    data[stype_key][label] = df
            except Exception:
                continue

    # Report coverage gaps
    fetched_years = sorted(int(k.split("-")[0]) for k in data["regular"].keys())
    if fetched_years:
        print(f"  Batch coverage: {fetched_years[0]}-{fetched_years[-1]} "
              f"({len(fetched_years)} regular seasons)")
        if fetched_years[0] > start:
            print(f"  ⚠️  Missing {start}-{fetched_years[0]-1} — will use per-player fallback")
    return data


def fetch_historical_players(season_data: dict, bio: dict,
                              start_year: int, min_seasons: int) -> dict:
    """Fetch career stats for players whose careers predate batch coverage.
    
    STRATEGY (fast):
      1. LeagueLeaders endpoint — returns ALL players per season in 1 call.
         Works from 1951-52 onward. ~90 API calls for 1951-1995 (reg+ply).
      2. PlayerCareerStats fallback — only for 1946-1950 (~30 players).
    
    Total: ~95 API calls at 0.7s ≈ ~70 seconds (vs ~55 min old approach).
    
    Returns season DataFrames in the same format as fetch_all_seasons()
    so they integrate directly with the existing build_players() pipeline.
    """
    from nba_api.stats.endpoints import leagueleaders, playercareerstats

    # Determine earliest year in batch data
    batch_years = sorted(int(k.split("-")[0]) for k in season_data["regular"].keys())
    if not batch_years:
        return {}
    earliest_batch = batch_years[0]

    if earliest_batch <= start_year:
        print("  No historical gap to fill")
        return {}

    print(f"  Filling gap: {start_year} to {earliest_batch - 1}")

    # ── Phase 1: Batch fetch via LeagueLeaders (1951+) ──────────────────────
    # LeagueLeaders returns all qualifying players per season in a single call.
    # It has: PLAYER_ID, PLAYER, TEAM, GP, MIN, FGM, FGA, FG_PCT, FG3M, FG3A,
    #         FG3_PCT, FTM, FTA, FT_PCT, OREB, DREB, REB, AST, STL, BLK, TOV, PTS
    # This is almost identical to LeagueDashPlayerStats columns.

    ll_start = max(start_year, 1951)  # LeagueLeaders starts at 1951-52
    ll_end = earliest_batch - 1
    
    if ll_start <= ll_end:
        n_calls = (ll_end - ll_start + 1) * 2
        print(f"  Phase 1: LeagueLeaders batch fetch ({ll_start}-{ll_end})")
        print(f"    ~{n_calls} API calls, est. ~{n_calls * 0.8 / 60:.1f} min")
        
        types = [("Regular Season", "regular"), ("Playoffs", "playoffs")]
        n = 0
        for year in range(ll_start, ll_end + 1):
            label = _season_label(year)
            for stype_api, stype_key in types:
                n += 1
                if n % 10 == 0 or n == 1:
                    print(f"    [{n}/{n_calls}] ({n/n_calls*100:.0f}%) {label} {stype_key}...")

                result = _api(
                    leagueleaders.LeagueLeaders,
                    season=label,
                    season_type_all_star=stype_api,
                    per_mode48="PerGame",
                )
                if result is None:
                    continue
                try:
                    df = result.get_data_frames()[0]
                    if df.empty:
                        continue
                    
                    # Rename columns to match LeagueDashPlayerStats format
                    # that build_players() expects
                    rename_map = {
                        "PLAYER": "PLAYER_NAME",
                    }
                    df = df.rename(columns=rename_map)
                    
                    # Add season metadata columns
                    df["_SEASON"] = label
                    df["_YEAR"] = year
                    
                    # Compute per-game stats that LeagueLeaders already provides
                    # (they're already per-game since we used PerGame mode)
                    # But we need MIN, PTS etc as per-game which they already are
                    
                    if label not in season_data[stype_key]:
                        season_data[stype_key][label] = df
                    else:
                        # Merge (unlikely but just in case)
                        season_data[stype_key][label] = pd.concat(
                            [season_data[stype_key][label], df], ignore_index=True
                        ).drop_duplicates(subset=["PLAYER_ID"], keep="first")
                except Exception as e:
                    logger.warning(f"LeagueLeaders parse {label} {stype_key}: {e}")
                    continue

        new_reg = sum(1 for k in season_data["regular"] if int(k.split("-")[0]) < earliest_batch)
        new_ply = sum(1 for k in season_data["playoffs"] if int(k.split("-")[0]) < earliest_batch)
        print(f"    ✅ Added {new_reg} regular + {new_ply} playoff historical seasons")

    # ── Phase 2: Curated pre-1951 players only ─────────────────────────────
    # LeagueLeaders returns 0 players for 1946-1950.
    # Instead of brute-forcing all 4500+ retired players, we use a curated list
    # of the ~15 significant BAA/early NBA players from those seasons.
    
    PRE_1951_LEGENDS = {
        600012: "George Mikan",
        600003: "Bob Cousy",
        78076: "Dolph Schayes",
        77847: "Bob Pettit",
        76056: "Paul Arizin",
        600027: "Max Zaslofsky",
        600028: "Joe Fulks",
        600025: "Ed Macauley",
        600020: "Andy Phillip",
        600016: "Vern Mikkelsen",
        600022: "Jim Pollard",
        600029: "Carl Braun",
        600030: "Harry Gallatin",
        600031: "Fred Scolari",
        600032: "Bobby Wanzer",
    }
    
    pre_ll_players = {}
    if start_year < 1951:
        print(f"\n  Phase 2: Curated pre-1951 legends ({len(PRE_1951_LEGENDS)} players)...")
        
        found = 0
        for pid, name in PRE_1951_LEGENDS.items():
            result = _api(
                playercareerstats.PlayerCareerStats,
                player_id=pid,
                per_mode36="PerGame",
            )
            if result is None:
                continue
            
            try:
                reg_df = result.get_data_frames()[0]
                if reg_df.empty:
                    continue
                
                # Extract only pre-1951 seasons
                for _, row in reg_df.iterrows():
                    sid = str(row.get("SEASON_ID", ""))
                    label = sid if "-" in sid else _season_label(int(sid[:4])) if len(sid) >= 4 else None
                    if not label:
                        continue
                    try:
                        year = int(label.split("-")[0])
                    except:
                        continue
                    if year >= 1951:
                        continue  # Already covered by LeagueLeaders
                    
                    gp = int(row.get("GP", 0) or 0)
                    if gp == 0:
                        continue
                    
                    new_row = pd.DataFrame([{
                        "PLAYER_ID": pid, "PLAYER_NAME": name,
                        "GP": gp,
                        "MIN": float(row.get("MIN", 0) or 0),
                        "PTS": float(row.get("PTS", 0) or 0),
                        "REB": float(row.get("REB", 0) or 0),
                        "AST": float(row.get("AST", 0) or 0),
                        "STL": float(row.get("STL", 0) or 0),
                        "BLK": float(row.get("BLK", 0) or 0),
                        "TOV": float(row.get("TOV", 0) or 0),
                        "OREB": float(row.get("OREB", 0) or 0),
                        "DREB": float(row.get("DREB", 0) or 0),
                        "PF": float(row.get("PF", 0) or 0),
                        "FGA": float(row.get("FGA", 0) or 0),
                        "FTA": float(row.get("FTA", 0) or 0),
                        "FG_PCT": float(row.get("FG_PCT", 0) or 0),
                        "FG3M": float(row.get("FG3M", 0) or 0),
                        "_SEASON": label, "_YEAR": year,
                    }])
                    
                    if label not in season_data["regular"]:
                        season_data["regular"][label] = new_row
                    else:
                        season_data["regular"][label] = pd.concat(
                            [season_data["regular"][label], new_row], ignore_index=True
                        )
                    found += 1
                
                # Playoffs
                try:
                    ply_df = result.get_data_frames()[2]
                    if not ply_df.empty:
                        for _, row in ply_df.iterrows():
                            sid = str(row.get("SEASON_ID", ""))
                            label = sid if "-" in sid else _season_label(int(sid[:4])) if len(sid) >= 4 else None
                            if not label:
                                continue
                            try:
                                year = int(label.split("-")[0])
                            except:
                                continue
                            if year >= 1951:
                                continue
                            gp = int(row.get("GP", 0) or 0)
                            if gp == 0:
                                continue
                            
                            new_row = pd.DataFrame([{
                                "PLAYER_ID": pid, "PLAYER_NAME": name,
                                "GP": gp,
                                "MIN": float(row.get("MIN", 0) or 0),
                                "PTS": float(row.get("PTS", 0) or 0),
                                "REB": float(row.get("REB", 0) or 0),
                                "AST": float(row.get("AST", 0) or 0),
                                "STL": float(row.get("STL", 0) or 0),
                                "BLK": float(row.get("BLK", 0) or 0),
                                "TOV": float(row.get("TOV", 0) or 0),
                                "OREB": float(row.get("OREB", 0) or 0),
                                "DREB": float(row.get("DREB", 0) or 0),
                                "PF": float(row.get("PF", 0) or 0),
                                "FGA": float(row.get("FGA", 0) or 0),
                                "FTA": float(row.get("FTA", 0) or 0),
                                "FG_PCT": float(row.get("FG_PCT", 0) or 0),
                                "FG3M": float(row.get("FG3M", 0) or 0),
                                "_SEASON": label, "_YEAR": year,
                            }])
                            
                            if label not in season_data["playoffs"]:
                                season_data["playoffs"][label] = new_row
                            else:
                                season_data["playoffs"][label] = pd.concat(
                                    [season_data["playoffs"][label], new_row], ignore_index=True
                                )
                except (IndexError, Exception):
                    pass
                    
            except Exception as e:
                logger.warning(f"Pre-1951 fetch error for {name}: {e}")
                continue
        
        print(f"    ✅ Added {found} pre-1951 season rows from {len(PRE_1951_LEGENDS)} legends")

    # Return empty dict — we've injected everything into season_data directly,
    # so build_players() will pick them up automatically.
    # This is cleaner than returning a separate dict to merge.
    total_seasons = len(season_data["regular"])
    total_ply_seasons = len(season_data["playoffs"])
    print(f"\n  Historical fetch complete: {total_seasons} reg + {total_ply_seasons} playoff seasons total")
    return {}


# ═══════════════════════════════════════════════════════════════════════════════
#  STEP 3: BUILD PLAYER DB FROM SEASON DATA (0 API calls)
# ═══════════════════════════════════════════════════════════════════════════════

def build_players(season_data: dict, bio: dict, min_szns: int, min_gp: int) -> dict:
    """Aggregate per-season DataFrames into per-player career data."""
    players = {}

    for stype in ["regular", "playoffs"]:
        for label, df in season_data[stype].items():
            year = int(label.split("-")[0])
            for _, row in df.iterrows():
                pid = int(row.get("PLAYER_ID", 0))
                name = str(row.get("PLAYER_NAME", ""))
                gp = int(row.get("GP", 0) or 0)
                if not name or pid == 0 or gp == 0:
                    continue

                if pid not in players:
                    b = bio.get(pid, {})
                    # Infer position from height
                    hi = b.get("height_inches", 0)
                    pos = "SF"
                    if hi >= 82: pos = "C"
                    elif hi >= 80: pos = "PF"
                    elif 0 < hi <= 74: pos = "PG"
                    elif 0 < hi <= 77: pos = "SG"

                    players[pid] = {
                        "info": {
                            "nba_api_id": pid,
                            "full_name": name,
                            "is_active": False,
                            "position": pos,
                            "height": b.get("height", ""),
                            "height_inches": hi,
                            "bbref_id": _bbref_id(name, pid),
                        },
                        "regular": [],
                        "playoffs": [],
                        "totals_regular": defaultdict(int),
                        "totals_playoffs": defaultdict(int),
                        "_max_yr": 0,
                    }

                p = players[pid]
                if year > p["_max_yr"]:
                    p["_max_yr"] = year

                ppg = _sf(row.get("PTS"))
                rpg = _sf(row.get("REB"))
                apg = _sf(row.get("AST"))
                spg = _sf(row.get("STL"))
                bpg = _sf(row.get("BLK"))
                mpg = _sf(row.get("MIN"))
                fg_pct = _sf(row.get("FG_PCT"))
                fga = _sf(row.get("FGA"))
                fta = _sf(row.get("FTA"))
                fg3m = _sf(row.get("FG3M"))
                tov = _sf(row.get("TOV"))
                orb = _sf(row.get("OREB"))
                drb = _sf(row.get("DREB"))
                pf = _sf(row.get("PF"))

                tsa = 2 * (fga + 0.44 * fta)
                ts = ppg / tsa if tsa > 0 else 0

                sd = {
                    "season": label, "year": year, "gp": gp,
                    "mpg": round(mpg, 1), "ppg": round(ppg, 1),
                    "rpg": round(rpg, 1), "apg": round(apg, 1),
                    "spg": round(spg, 1), "bpg": round(bpg, 1),
                    "fg_pct": round(fg_pct, 4) if fg_pct else 0,
                    "ts_pct": round(ts, 4),
                    "tov_pg": round(tov, 1), "orb_pg": round(orb, 1),
                    "drb_pg": round(drb, 1), "fta_pg": round(fta, 1),
                    "fg3m_pg": round(fg3m, 1), "pf_pg": round(pf, 1),
                    "fga_pg": round(fga, 1), "trb_pg": round(rpg, 1),
                }
                p[stype].append(sd)

                tk = f"totals_{stype}"
                p[tk]["PTS"] += int(round(ppg * gp))
                p[tk]["REB"] += int(round(rpg * gp))
                p[tk]["AST"] += int(round(apg * gp))
                p[tk]["STL"] += int(round(spg * gp))
                p[tk]["BLK"] += int(round(bpg * gp))
                p[tk]["TOV"] += int(round(tov * gp))

    # Post-process
    import datetime
    current_yr = datetime.datetime.now().year
    out = {}
    for pid, p in players.items():
        p["info"]["is_active"] = p["_max_yr"] >= current_yr - 1
        p["regular"].sort(key=lambda s: s["year"])
        p["playoffs"].sort(key=lambda s: s["year"])
        del p["_max_yr"]

        if len(p["regular"]) >= min_szns:
            career_gp = sum(s["gp"] for s in p["regular"])
            if career_gp >= min_gp:
                out[pid] = p
    return out


# ═══════════════════════════════════════════════════════════════════════════════
#  STEP 3B: ML DEFENSIVE IMPUTER (0 API calls)
# ═══════════════════════════════════════════════════════════════════════════════

def run_imputer(players: dict, season_data: dict) -> int:
    """Train on post-1973 data, predict pre-1973 STL/BLK."""
    from backend.scrapers.defensive_imputer import DefensiveStatImputer

    # League FGA per season
    lg_fga = {}
    for label, df in season_data["regular"].items():
        vals = df["FGA"].dropna()
        if len(vals) > 0:
            lg_fga[label] = round(vals.mean(), 1)

    rows = []
    for p in players.values():
        for s in p["regular"]:
            if s["year"] >= 1974 and s.get("spg", 0) > 0:
                r = dict(s)
                r["league_fga_pg"] = lg_fga.get(s["season"], 14.0)
                r["height_inches"] = p["info"].get("height_inches", 0)
                r["position"] = p["info"].get("position", "SF")
                rows.append(r)

    imp = DefensiveStatImputer()
    if not rows:
        return 0

    df = pd.DataFrame(rows)
    if "season_year" not in df.columns:
        df["season_year"] = df.get("year", 2000)
    m = imp.train(df)
    print(f"  STL R²={m.get('stl_r2_cv', '?')}, BLK R²={m.get('blk_r2_cv', '?')}, n={m.get('n_train', 0)}")

    HIST_FGA = {1947: 20.0, 1950: 19.5, 1955: 18.5, 1960: 18.0, 1965: 17.5, 1970: 17.0, 1973: 16.0}

    def _hfga(yr):
        ks = sorted(HIST_FGA.keys())
        if yr <= ks[0]: return HIST_FGA[ks[0]]
        if yr >= ks[-1]: return HIST_FGA[ks[-1]]
        for i in range(len(ks) - 1):
            if ks[i] <= yr <= ks[i+1]:
                f = (yr - ks[i]) / (ks[i+1] - ks[i])
                return round(HIST_FGA[ks[i]] + f * (HIST_FGA[ks[i+1]] - HIST_FGA[ks[i]]), 1)
        return 16.0

    cnt = 0
    if imp.is_trained:
        for p in players.values():
            info = p["info"]
            for s in p["regular"] + p["playoffs"]:
                # Impute for pre-1974 seasons where STL/BLK are missing or zero
                spg_val = s.get("spg", 0) or 0
                bpg_val = s.get("bpg", 0) or 0
                if s["year"] < 1974 and spg_val == 0 and bpg_val == 0:
                    trb = s.get("trb_pg", 0) or s.get("rpg", 0) or 0
                    pr = {
                        "position": info.get("position", "SF"),
                        "height_inches": info.get("height_inches", 0),
                        "height": info.get("height", ""),
                        "trb_pg": trb,
                        "rpg": s.get("rpg", 0),
                        "pf_pg": s.get("pf_pg", 0),
                        "apg": s.get("apg", 0),
                        "mpg": s.get("mpg", 0),
                        "ppg": s.get("ppg", 0),
                        "fga_pg": s.get("fga_pg", 0),
                        "league_fga_pg": _hfga(s["year"]),
                    }
                    stl, blk = imp.predict(pr)
                    s["spg"] = stl
                    s["bpg"] = blk
                    s["imputed_defense"] = True
                    cnt += 1
    return cnt


# ═══════════════════════════════════════════════════════════════════════════════
#  STEP 4: COMPUTE PMI (0 API calls)
# ═══════════════════════════════════════════════════════════════════════════════

def compute_pmi(players: dict, season_data: dict):
    """Compute PMI v41d for all player-seasons.
    
    Uses the v41d engine (pmi_engine.py) with position-interpolated weights,
    playoff scoring boost, era penalties, and DPMI dampening.
    
    CRITICAL: League stats must come from the RAW season DataFrames
    (all players), not from the filtered qualifying players dict.
    """
    from backend.scrapers.pmi_engine import (
        compute_opmi, compute_dpmi, _pos_num,
        compute_season_league_stats, compute_awc,
    )

    # Pre-compute league stats from RAW season DataFrames (ALL players)
    print("  Building league stat cache from raw season data...")
    league_cache = {}

    for stype_key in ["regular", "playoffs"]:
        for label, df in season_data[stype_key].items():
            rows = []
            for _, row in df.iterrows():
                gp = int(row.get("GP", 0) or 0)
                if gp == 0:
                    continue
                mpg = _sf(row.get("MIN"))
                ppg = _sf(row.get("PTS"))
                fga = _sf(row.get("FGA"))
                fta = _sf(row.get("FTA"))
                tsa = 2 * (fga + 0.44 * fta)
                ts = ppg / tsa if tsa > 0 else 0

                rows.append({
                    "ppg": round(ppg, 1),
                    "apg": round(_sf(row.get("AST")), 1),
                    "spg": round(_sf(row.get("STL")), 1),
                    "bpg": round(_sf(row.get("BLK")), 1),
                    "tov_pg": round(_sf(row.get("TOV")), 1),
                    "orb_pg": round(_sf(row.get("OREB")), 1),
                    "drb_pg": round(_sf(row.get("DREB")), 1),
                    "pf_pg": round(_sf(row.get("PF")), 1),
                    "fta_pg": round(_sf(row.get("FTA")), 1),
                    "fg3m_pg": round(_sf(row.get("FG3M")), 1),
                    "ts_pct": round(ts, 4),
                    "mpg": round(mpg, 1),
                })
            if rows:
                league_cache[(label, stype_key)] = compute_season_league_stats(pd.DataFrame(rows))

    print(f"  Cached {len(league_cache)} season-types")

    fallback = {
        "ppg_mean": 14.0, "ppg_std": 6.5, "apg_mean": 2.8, "apg_std": 2.5,
        "tov_pg_mean": 1.5, "tov_pg_std": 0.8, "orb_pg_mean": 1.0, "orb_pg_std": 0.8,
        "fta_pg_mean": 2.5, "fta_pg_std": 1.5, "fg3m_pg_mean": 0.5, "fg3m_pg_std": 0.6,
        "spg_mean": 0.8, "spg_std": 0.5, "bpg_mean": 0.5, "bpg_std": 0.5,
        "drb_pg_mean": 2.5, "drb_pg_std": 1.5, "pf_pg_mean": 2.2, "pf_pg_std": 0.8,
        "ts_pct_mean": 0.540, "ts_pct_std": 0.05,
    }

    for p in players.values():
        pos_num = _pos_num(p["info"].get("position", "SF"))
        for stype in ["regular", "playoffs"]:
            is_playoff = stype == "playoffs"
            for s in p[stype]:
                lg = league_cache.get((s["season"], stype), fallback)
                opmi = compute_opmi(s, lg, pos_num, is_playoff, s["year"])
                dpmi = compute_dpmi(s, lg, pos_num, is_playoff)
                pmi = round(opmi + dpmi, 2)
                mn = round(s["mpg"] * s["gp"])
                s["opmi"] = round(opmi, 2)
                s["dpmi"] = round(dpmi, 2)
                s["pmi"] = pmi
                s["awc"] = round(compute_awc(pmi, mn), 1)

            seasons = p[stype]
            if seasons:
                peak = max(s["pmi"] for s in seasons)
                for s in seasons:
                    s["peak_pmi"] = round(peak, 2)


# ═══════════════════════════════════════════════════════════════════════════════
#  STEP 4B: CLUTCH / CPMI (~28 API calls)
# ═══════════════════════════════════════════════════════════════════════════════

def compute_cpmi_all(players: dict, start: int = 1996, end: int = 2024) -> int:
    """Fetch clutch stats per season and compute CPMI for both regular and playoffs."""
    from nba_api.stats.endpoints import leaguedashplayerclutch
    from backend.scrapers.pmi_engine import compute_cpmi
    from backend.scrapers.pmi_v3_engine import (
        compute_clutch_league_stats, build_clutch_row,
    )

    clutch_reg = {}
    league_reg = {}
    clutch_ply = {}
    league_ply = {}

    # Fetch regular season clutch
    for year in range(start, end + 1):
        label = _season_label(year)
        result = _api(
            leaguedashplayerclutch.LeagueDashPlayerClutch,
            season=label, season_type_all_star="Regular Season",
            clutch_time="Last 5 Minutes", ahead_behind="Ahead or Behind",
            point_diff=5, per_mode_detailed="PerGame",
        )
        if result is None:
            continue
        try:
            df = result.get_data_frames()[0]
            if df.empty:
                continue
            clutch_reg[label] = df
            league_reg[label] = compute_clutch_league_stats(df)
        except Exception:
            continue
        if (year - start + 1) % 5 == 0:
            print(f"  Regular clutch: {year - start + 1}/{end - start + 1}...")

    print(f"  ✅ Fetched regular clutch for {len(clutch_reg)} seasons")

    # Fetch playoff clutch
    for year in range(start, end + 1):
        label = _season_label(year)
        result = _api(
            leaguedashplayerclutch.LeagueDashPlayerClutch,
            season=label, season_type_all_star="Playoffs",
            clutch_time="Last 5 Minutes", ahead_behind="Ahead or Behind",
            point_diff=5, per_mode_detailed="PerGame",
        )
        if result is None:
            continue
        try:
            df = result.get_data_frames()[0]
            if df.empty:
                continue
            clutch_ply[label] = df
            league_ply[label] = compute_clutch_league_stats(df)
        except Exception:
            continue
        if (year - start + 1) % 5 == 0:
            print(f"  Playoff clutch: {year - start + 1}/{end - start + 1}...")

    print(f"  ✅ Fetched playoff clutch for {len(clutch_ply)} seasons")

    cnt = 0
    
    # Process regular season clutch
    for label, df in clutch_reg.items():
        lg = league_reg[label]
        for _, crow in df.iterrows():
            pid = int(crow.get("PLAYER_ID", 0))
            if pid not in players:
                continue
            cr = build_clutch_row(crow)
            cpmi = compute_cpmi(cr, lg)
            p = players[pid]
            for s in p["regular"]:
                if s["season"] == label:
                    s["cpmi"] = cpmi
                    break
            if "_cs" not in p:
                p["_cs"] = []
            p["_cs"].append({
                "cpmi": cpmi, "min": cr.get("clutch_min", 0), "gp": cr.get("clutch_gp", 0),
                "ppg": cr.get("clutch_ppg", 0),
                "apg": cr.get("clutch_apg", 0),
                "spg": cr.get("clutch_spg", 0),
                "bpg": cr.get("clutch_bpg", 0),
                "tovpg": cr.get("clutch_tovpg", 0),
                "orbpg": cr.get("clutch_orbpg", 0),
                "plus_minus": float(crow.get("PLUS_MINUS", 0) or 0),
                "fgm": float(crow.get("FGM", 0) or 0),
                "fga": float(crow.get("FGA", 0) or 0),
                "w": float(crow.get("W", 0) or 0),
                "l": float(crow.get("L", 0) or 0),
                "reb": float(crow.get("REB", 0) or 0),
            })
            cnt += 1

    # Process playoff clutch
    for label, df in clutch_ply.items():
        lg = league_ply[label]
        for _, crow in df.iterrows():
            pid = int(crow.get("PLAYER_ID", 0))
            if pid not in players:
                continue
            cr = build_clutch_row(crow)
            cpmi = compute_cpmi(cr, lg)
            p = players[pid]
            for s in p["playoffs"]:
                if s["season"] == label:
                    s["cpmi"] = cpmi
                    break
            if "_cs_ply" not in p:
                p["_cs_ply"] = []
            p["_cs_ply"].append({
                "cpmi": cpmi, "min": cr.get("clutch_min", 0), "gp": cr.get("clutch_gp", 0),
                "ppg": cr.get("clutch_ppg", 0),
                "apg": cr.get("clutch_apg", 0),
                "spg": cr.get("clutch_spg", 0),
                "bpg": cr.get("clutch_bpg", 0),
                "tovpg": cr.get("clutch_tovpg", 0),
                "orbpg": cr.get("clutch_orbpg", 0),
                "plus_minus": float(crow.get("PLUS_MINUS", 0) or 0),
                "fgm": float(crow.get("FGM", 0) or 0),
                "fga": float(crow.get("FGA", 0) or 0),
                "w": float(crow.get("W", 0) or 0),
                "l": float(crow.get("L", 0) or 0),
                "reb": float(crow.get("REB", 0) or 0),
            })
            cnt += 1

    for p in players.values():
        # Regular season clutch career
        cs = p.pop("_cs", [])
        if not cs:
            p["career_cpmi"] = None
            p["_clutch_career"] = {}
        else:
            tm = sum(c.get("min", 0) for c in cs)
            if tm > 0:
                p["career_cpmi"] = round(sum(c["cpmi"] * c.get("min", 1) for c in cs) / tm, 2)
            else:
                p["career_cpmi"] = round(np.mean([c["cpmi"] for c in cs]), 2)
            p["clutch_gp"] = sum(c.get("gp", 0) for c in cs)

            total_gp = p["clutch_gp"]
            if total_gp > 0:
                def _wgp(k, data):
                    return round(sum(c.get(k, 0) * c.get("gp", 0) for c in data) / sum(c.get("gp", 0) for c in data), 1)
                
                total_fgm = sum(c.get("fgm", 0) * c.get("gp", 0) for c in cs)
                total_fga = sum(c.get("fga", 0) * c.get("gp", 0) for c in cs)
                total_w = sum(c.get("w", 0) for c in cs)
                total_l = sum(c.get("l", 0) for c in cs)
                total_pm = sum(c.get("plus_minus", 0) * c.get("gp", 0) for c in cs)

                p["_clutch_career"] = {
                    "ppg": _wgp("ppg", cs),
                    "apg": _wgp("apg", cs),
                    "rpg": round(sum(c.get("reb", 0) * c.get("gp", 0) for c in cs) / total_gp, 1),
                    "spg": _wgp("spg", cs),
                    "bpg": _wgp("bpg", cs),
                    "fg_pct": round(total_fgm / total_fga, 4) if total_fga > 0 else 0,
                    "plus_minus": round(total_pm / total_gp, 1),
                    "w_pct": round(total_w / (total_w + total_l), 3) if (total_w + total_l) > 0 else 0,
                }
            else:
                p["_clutch_career"] = {}

        # Playoff clutch career
        cs_ply = p.pop("_cs_ply", [])
        if not cs_ply:
            p["career_cpmi_playoffs"] = None
            p["_clutch_career_playoffs"] = {}
        else:
            tm = sum(c.get("min", 0) for c in cs_ply)
            if tm > 0:
                p["career_cpmi_playoffs"] = round(sum(c["cpmi"] * c.get("min", 1) for c in cs_ply) / tm, 2)
            else:
                p["career_cpmi_playoffs"] = round(np.mean([c["cpmi"] for c in cs_ply]), 2)
            p["clutch_gp_playoffs"] = sum(c.get("gp", 0) for c in cs_ply)

            total_gp = p["clutch_gp_playoffs"]
            if total_gp > 0:
                total_fgm = sum(c.get("fgm", 0) * c.get("gp", 0) for c in cs_ply)
                total_fga = sum(c.get("fga", 0) * c.get("gp", 0) for c in cs_ply)
                total_w = sum(c.get("w", 0) for c in cs_ply)
                total_l = sum(c.get("l", 0) for c in cs_ply)
                total_pm = sum(c.get("plus_minus", 0) * c.get("gp", 0) for c in cs_ply)

                def _wgp_p(k):
                    return round(sum(c.get(k, 0) * c.get("gp", 0) for c in cs_ply) / total_gp, 1)

                p["_clutch_career_playoffs"] = {
                    "ppg": _wgp_p("ppg"),
                    "apg": _wgp_p("apg"),
                    "rpg": round(sum(c.get("reb", 0) * c.get("gp", 0) for c in cs_ply) / total_gp, 1),
                    "spg": _wgp_p("spg"),
                    "bpg": _wgp_p("bpg"),
                    "fg_pct": round(total_fgm / total_fga, 4) if total_fga > 0 else 0,
                    "plus_minus": round(total_pm / total_gp, 1),
                    "w_pct": round(total_w / (total_w + total_l), 3) if (total_w + total_l) > 0 else 0,
                }
            else:
                p["_clutch_career_playoffs"] = {}

    return cnt


# ═══════════════════════════════════════════════════════════════════════════════
#  STEP 5: OUTPUT
# ═══════════════════════════════════════════════════════════════════════════════

def build_summary(player: dict, stype: str) -> Optional[dict]:
    """Build PlayerData career summary."""
    from backend.scrapers.pmi_engine import compute_career_pmi, compute_awc

    seasons = player[stype]
    if not seasons:
        return None

    info = player["info"]
    is_ply = stype == "playoffs"
    tg = sum(s["gp"] for s in seasons)
    tm = sum(round(s["mpg"] * s["gp"]) for s in seasons)

    cpmi = compute_career_pmi([s["pmi"] for s in seasons], tg, is_ply)
    copmi = compute_career_pmi([s["opmi"] for s in seasons], tg, is_ply)
    cdpmi = compute_career_pmi([s["dpmi"] for s in seasons], tg, is_ply)

    pk = max(seasons, key=lambda s: s["pmi"])

    def _wa(k):
        t = sum(s.get(k, 0) * s["gp"] for s in seasons)
        return round(t / tg, 1) if tg > 0 else 0

    fg_s = sum(s.get("fg_pct", 0) * s["gp"] for s in seasons)
    ts_s = sum(s.get("ts_pct", 0) * s["gp"] for s in seasons)

    yrs = sorted(set(s["year"] for s in seasons))
    ys = f"{yrs[0]}-pres." if info.get("is_active") and yrs else (f"{yrs[0]}-{yrs[-1]+1}" if yrs else "?")

    tot = player.get(f"totals_{stype}", {})

    r = {
        "full_name": info["full_name"], "bbref_id": info["bbref_id"],
        "nba_api_id": info["nba_api_id"], "is_active": info.get("is_active", False),
        "position": info.get("position", "?"), "years": ys, "gp": tg,
        "ppg": _wa("ppg"), "rpg": _wa("rpg"), "apg": _wa("apg"),
        "spg": _wa("spg"), "bpg": _wa("bpg"), "tov": None,
        "fg_pct": round(fg_s / tg, 4) if tg > 0 else 0,
        "ts_pct": round(ts_s / tg, 4) if tg > 0 else 0,
        "rts_pct": round(ts_s / tg - 0.540, 4) if tg > 0 else 0,
        "pmi": round(cpmi, 2), "opmi": round(copmi, 2), "dpmi": round(cdpmi, 2),
        "peak_pmi": round(pk["pmi"], 2), "peak_season": pk["season"],
        "pie": 15.0,
        "awc": round(compute_awc(cpmi, tm), 1),
        "oawc": round(compute_awc(copmi, tm), 1),
        "dawc": round(compute_awc(cdpmi, tm), 1),
        "min": tm, "pts": tot.get("PTS", 0), "reb": tot.get("REB", 0),
        "ast": tot.get("AST", 0), "stl": tot.get("STL", 0),
        "blk": tot.get("BLK", 0), "total_tov": tot.get("TOV", 0),
        "seasons": len(seasons),
    }
    if stype == "regular":
        r["cpmi"] = player.get("career_cpmi")
        r["clutch_gp"] = player.get("clutch_gp")
        cc = player.get("_clutch_career", {})
        if cc:
            r["clutch_pts"] = cc.get("ppg", 0)
            r["clutch_ast"] = cc.get("apg", 0)
            r["clutch_reb"] = cc.get("rpg", 0)
            r["clutch_stl"] = cc.get("spg", 0)
            r["clutch_blk"] = cc.get("bpg", 0)
            r["clutch_fg_pct"] = cc.get("fg_pct", 0)
            r["clutch_plus_minus"] = cc.get("plus_minus", 0)
            r["clutch_w_pct"] = cc.get("w_pct", 0)
    elif stype == "playoffs":
        r["cpmi"] = player.get("career_cpmi_playoffs")
        r["clutch_gp"] = player.get("clutch_gp_playoffs")
        cc = player.get("_clutch_career_playoffs", {})
        if cc:
            r["clutch_pts"] = cc.get("ppg", 0)
            r["clutch_ast"] = cc.get("apg", 0)
            r["clutch_reb"] = cc.get("rpg", 0)
            r["clutch_stl"] = cc.get("spg", 0)
            r["clutch_blk"] = cc.get("bpg", 0)
            r["clutch_fg_pct"] = cc.get("fg_pct", 0)
            r["clutch_plus_minus"] = cc.get("plus_minus", 0)
            r["clutch_w_pct"] = cc.get("w_pct", 0)
    # Aliases for frontend compatibility
    r["full_name"] = info["full_name"]
    r["name"] = info["full_name"]
    return r


KEEP_KEYS = {
    "season", "year", "gp", "mpg", "ppg", "rpg", "apg",
    "spg", "bpg", "fg_pct", "ts_pct", "pmi", "opmi",
    "dpmi", "awc", "peak_pmi", "cpmi",
}


# ═══════════════════════════════════════════════════════════════════════════════
#  MAIN PIPELINE
# ═══════════════════════════════════════════════════════════════════════════════

def run_ingestion(start_year=1946, end_year=2024, min_seasons=5, min_gp=50, recompute=False):
    DATA_DIR.mkdir(exist_ok=True)
    t0 = time.time()

    # ─── Recompute mode: skip API calls, just recalculate PMI from cached data ───
    if recompute:
        print("🔄 Recompute mode — recalculating PMI from cached season data...")
        cached_sd_path = DATA_DIR / "_cached_season_data.json"
        cached_players_path = DATA_DIR / "_cached_players.json"
        
        if not cached_sd_path.exists() or not cached_players_path.exists():
            print("  ❌ No cached data found. Run full ingestion first.")
            return
        
        print("  Loading cached data...")
        with open(cached_players_path) as f:
            players_raw = json.load(f)
        # Convert totals back to defaultdicts
        players = {}
        for pid_str, p in players_raw.items():
            pid = int(pid_str)
            p["totals_regular"] = defaultdict(int, p.get("totals_regular", {}))
            p["totals_playoffs"] = defaultdict(int, p.get("totals_playoffs", {}))
            players[pid] = p
        
        # Load raw season DataFrames (saved as CSV-like dicts)
        with open(cached_sd_path) as f:
            sd_raw = json.load(f)
        sd = {"regular": {}, "playoffs": {}}
        for stype in ["regular", "playoffs"]:
            for label, rows in sd_raw.get(stype, {}).items():
                sd[stype][label] = pd.DataFrame(rows)
        
        print(f"  ✅ {len(players)} players, {len(sd['regular'])} seasons loaded")
        
        # Re-run PMI
        print("\n🧮 Step 4: Computing PMI v3...")
        compute_pmi(players, sd)
        print("  ✅ Done")
        
        # Skip to output (Step 5)
        _save_output(players, start_year, end_year, t0)
        return

    n_szn = end_year - start_year + 1
    est = n_szn * 2 + 5 + 28
    print(f"🏀 Courtside Batch Ingestion — {start_year}-{end_year}")
    if start_year < 1996:
        hist_calls = (min(1995, end_year) - max(start_year, 1951) + 1) * 2
        print(f"   Est. {est + hist_calls} API calls (batch + historical LeagueLeaders)")
        print(f"   Est. time: ~{(est + hist_calls) * 0.8 / 60:.0f} min")
    else:
        print(f"   Est. {est} API calls, ~{est * 0.8 / 60:.1f} min")
    print("=" * 60)

    # Step 1
    print(f"\n📋 Step 1: Player bios...")
    bio = fetch_all_bios()
    print(f"  ✅ {len(bio)} bios")

    # Step 2
    print(f"\n📊 Step 2: Fetching {n_szn} seasons × 2 types...")
    sd = fetch_all_seasons(start_year, end_year)
    rc = sum(len(d) for d in sd["regular"].values())
    pc = sum(len(d) for d in sd["playoffs"].values())
    print(f"  ✅ {rc:,} reg + {pc:,} playoff player-season rows")

    # Step 2b: Historical fallback — fetch pre-batch players via LeagueLeaders + PlayerCareerStats
    # MUST run before Step 3 so build_players() sees all seasons including historical
    batch_years = sorted(int(k.split("-")[0]) for k in sd["regular"].keys())
    earliest_batch = batch_years[0] if batch_years else end_year
    if start_year < earliest_batch:
        print(f"\n📜 Step 2b: Historical seasons ({start_year}-{earliest_batch - 1})...")
        # This function injects historical season DataFrames directly into sd
        fetch_historical_players(sd, bio, start_year, min_seasons)
        rc2 = sum(len(d) for d in sd["regular"].values())
        pc2 = sum(len(d) for d in sd["playoffs"].values())
        print(f"  ✅ Total after historical: {rc2:,} reg + {pc2:,} playoff player-season rows")
    else:
        print("\n📜 Step 2b: No historical gap (batch covers full range)")

    # Step 3: Now build players from ALL season data (1946-2024)
    print(f"\n🔧 Step 3: Building player DB (min {min_seasons} szns, {min_gp} GP)...")
    players = build_players(sd, bio, min_seasons, min_gp)
    print(f"  ✅ {len(players)} players")

    # Cache intermediate data for recompute mode
    print("\n💾 Caching intermediate data for recompute mode...")
    _cache_data(players, sd)

    # Step 3b
    print("\n🤖 Step 3b: ML imputer...")
    imp_n = run_imputer(players, sd)
    print(f"  ✅ {imp_n} imputed seasons")

    # Step 4
    print("\n🧮 Step 4: Computing PMI v3...")
    compute_pmi(players, sd)
    print("  ✅ Done")

    # Step 4b
    print("\n🔥 Step 4b: Clutch + CPMI...")
    cn = compute_cpmi_all(players, max(start_year, 1996), end_year)
    print(f"  ✅ {cn} CPMIs")

    _save_output(players, start_year, end_year, t0)


def _cache_data(players: dict, sd: dict):
    """Cache intermediate data so --recompute can skip API calls."""
    # Save players dict (convert defaultdicts to regular dicts)
    players_ser = {}
    for pid, p in players.items():
        p_copy = dict(p)
        p_copy["totals_regular"] = dict(p.get("totals_regular", {}))
        p_copy["totals_playoffs"] = dict(p.get("totals_playoffs", {}))
        players_ser[str(pid)] = p_copy
    with open(DATA_DIR / "_cached_players.json", "w") as f:
        json.dump(players_ser, f, default=str)

    # Save raw season DataFrames as JSON
    sd_ser = {"regular": {}, "playoffs": {}}
    for stype in ["regular", "playoffs"]:
        for label, df in sd[stype].items():
            sd_ser[stype][label] = df.to_dict(orient="records")
    with open(DATA_DIR / "_cached_season_data.json", "w") as f:
        json.dump(sd_ser, f, default=str)
    
    sz = (DATA_DIR / "_cached_players.json").stat().st_size / 1024 / 1024
    sz2 = (DATA_DIR / "_cached_season_data.json").stat().st_size / 1024 / 1024
    print(f"  ✅ Cached players ({sz:.1f} MB) + seasons ({sz2:.1f} MB)")


def _save_output(players: dict, start_year: int, end_year: int, t0: float):
    """Step 5: Build summaries and save output JSON files."""
    print("\n💾 Step 5: Saving...")
    pr, pp, sr, sp = [], [], {}, {}
    for p in players.values():
        bid = p["info"]["bbref_id"]
        rs = build_summary(p, "regular")
        ps = build_summary(p, "playoffs")
        if rs: pr.append(rs)
        if ps: pp.append(ps)
        if p["regular"]:
            sr[bid] = [{k: v for k, v in s.items() if k in KEEP_KEYS} for s in p["regular"]]
        if p["playoffs"]:
            sp[bid] = [{k: v for k, v in s.items() if k in KEEP_KEYS} for s in p["playoffs"]]

    pr.sort(key=lambda x: x.get("pmi", 0), reverse=True)
    pp.sort(key=lambda x: x.get("pmi", 0), reverse=True)

    def _w(data, fn):
        path = DATA_DIR / fn
        with open(path, "w") as f:
            json.dump(data, f, indent=2, default=str)
        sz = path.stat().st_size / 1024 / 1024
        c = len(data) if isinstance(data, list) else len(data)
        print(f"  ✅ {fn}: {c} entries ({sz:.1f} MB)")

    _w(pr, "players_regular.json")
    _w(pp, "players_playoffs.json")
    _w(sr, "seasons_regular.json")
    _w(sp, "seasons_playoffs.json")

    el = time.time() - t0
    print(f"\n{'='*60}")
    print(f"✅ Done in {el/60:.1f} min!")
    print(f"   {len(pr)} reg + {len(pp)} playoff players")
    print(f"   {sum(len(v) for v in sr.values()):,} reg + {sum(len(v) for v in sp.values()):,} playoff season rows")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    p = argparse.ArgumentParser(description="Fetch NBA data (batch-optimized)")
    p.add_argument("--start-year", type=int, default=1946)
    p.add_argument("--end-year", type=int, default=2024)
    p.add_argument("--min-seasons", type=int, default=3)
    p.add_argument("--min-gp", type=int, default=50)
    p.add_argument("--recompute", action="store_true",
                   help="Skip API calls, recalculate PMI from cached data")
    a = p.parse_args()
    run_ingestion(a.start_year, a.end_year, a.min_seasons, a.min_gp, a.recompute)
