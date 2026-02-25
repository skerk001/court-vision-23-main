"""Fetch historical NBA players (pre-1996) via PlayerCareerStats endpoint.

This script fetches per-season career stats for ~110 curated legends
whose careers predate the LeagueDashPlayerStats batch coverage (1996+).

It outputs data in the SAME format as the existing pipeline's cached files,
so it can be merged into _cached_players.json and _cached_season_data.json.

Usage:
    python fetch_historical.py [--output-dir DIR] [--delay SECS]

Output:
    historical_players.json   — player dicts (same format as _cached_players.json)
    historical_seasons.json   — season DataFrames as JSON (for league stat computation)

After running, merge into the main pipeline with:
    python merge_historical.py
"""

import json
import time
import math
import sys
from pathlib import Path
from collections import defaultdict

# ═══════════════════════════════════════════════════════════════════════════
#  CURATED LEGENDS LIST — 112 historically significant pre-1996 players
# ═══════════════════════════════════════════════════════════════════════════

HISTORICAL_LEGENDS = {
    # 1940s-50s pioneers
    600012: "George Mikan",
    600003: "Bob Cousy",
    77847: "Bob Pettit",
    78076: "Dolph Schayes",
    76056: "Paul Arizin",
    # 1960s greats
    76375: "Wilt Chamberlain",
    78049: "Bill Russell",
    600015: "Oscar Robertson",
    78497: "Jerry West",
    76127: "Elgin Baylor",
    76882: "Hal Greer",
    77196: "Sam Jones",
    77929: "Willis Reed",
    600001: "Nate Thurmond",
    76166: "Dave Bing",
    76144: "Walt Bellamy",
    77418: "Jerry Lucas",
    78530: "Lenny Wilkens",
    77070: "Bailey Howell",
    77150: "Gus Johnson",
    # 1970s stars
    76003: "Kareem Abdul-Jabbar",
    76681: "Julius Erving",
    77459: "Pete Maravich",
    76750: "Walt Frazier",
    76462: "Dave Cowens",
    77498: "Bob McAdoo",
    600013: "Rick Barry",
    76979: "Elvin Hayes",
    78392: "Wes Unseld",
    76981: "Spencer Haywood",
    600005: "Bob Lanier",
    76054: "Nate Archibald",  # Tiny Archibald
    77097: "Dan Issel",
    600014: "Artis Gilmore",
    76804: "George Gervin",
    78500: "Paul Westphal",
    76545: "Dave DeBusschere",
    600006: "Earl Monroe",
    78510: "Jojo White",
    76753: "World Free",  # World B. Free
    78450: "Bill Walton",
    77160: "Marques Johnson",
    # 1980s stars
    77142: "Magic Johnson",
    1449: "Larry Bird",
    77449: "Moses Malone",
    78318: "Isiah Thomas",
    1122: "Dominique Wilkins",
    1460: "James Worthy",
    305: "Robert Parish",
    1450: "Kevin McHale",
    77141: "Dennis Johnson",
    77626: "Sidney Moncrief",
    76673: "Alex English",
    76504: "Adrian Dantley",
    77264: "Bernard King",
    76016: "Mark Aguirre",
    78149: "Jack Sikma",
    187: "Terry Cummings",
    76176: "Rolando Blackman",
    1453: "Walter Davis",
    76385: "Maurice Cheeks",
    23: "Dennis Rodman",
    78014: "Tree Rollins",
    # Late 80s / Early 90s (careers started pre-1996)
    893: "Michael Jordan",
    165: "Hakeem Olajuwon",
    787: "Charles Barkley",
    252: "Karl Malone",
    304: "John Stockton",
    764: "David Robinson",
    121: "Patrick Ewing",
    17: "Clyde Drexler",
    937: "Scottie Pippen",
    397: "Reggie Miller",
    904: "Chris Mullin",
    782: "Mitch Richmond",
    896: "Tim Hardaway",
    899: "Mark Price",
    134: "Kevin Johnson",
    913: "Larry Johnson",
    297: "Alonzo Mourning",
    87: "Dikembe Mutombo",
    934: "Derrick Coleman",
    779: "Glen Rice",
    270: "Horace Grant",
    96: "Detlef Schrempf",
    105: "Dan Majerle",
    431: "Shawn Kemp",
    56: "Gary Payton",
    358: "Anfernee Hardaway",  # Penny Hardaway
    255: "Grant Hill",
    467: "Jason Kidd",
    708: "Kevin Garnett",
    361: "Clifford Robinson",
    89: "Nick Van Exel",
    84: "Latrell Sprewell",
    436: "Juwan Howard",
    210: "Terrell Brandon",
    344: "Dana Barros",
    76: "Cedric Ceballos",
    339: "Tom Gugliotta",
    185: "Chris Webber",
    469: "Jamal Mashburn",
    129: "Dino Radja",
    389: "Toni Kukoc",
    765: "Hersey Hawkins",
    204: "Jeff Hornacek",
    107: "Dale Ellis",
    64: "Sam Perkins",
    433: "Buck Williams",
    109: "Robert Horry",
    224: "Eddie Jones",
    213: "Antonio Davis",
}


def _sf(val, default=0.0):
    """Safe float."""
    if val is None:
        return default
    try:
        v = float(val)
        return default if math.isnan(v) else v
    except (ValueError, TypeError):
        return default


def _season_label(year: int) -> str:
    """Convert year to 'YYYY-YY' format."""
    return f"{year}-{str(year + 1)[-2:]}"


def _bbref_id(name: str, nba_id: int) -> str:
    parts = name.strip().split()
    if len(parts) < 2:
        return f"player{nba_id}"
    first = parts[0].lower().replace("'", "").replace(".", "")[:2]
    last = parts[-1].lower().replace("'", "").replace(".", "")[:5]
    return f"{last}{first}01"


def _pos_from_height(hi: int) -> str:
    if hi >= 82: return "C"
    elif hi >= 80: return "PF"
    elif hi <= 74 and hi > 0: return "PG"
    elif hi <= 77 and hi > 0: return "SG"
    return "SF"


def _parse_season_rows(df, stype_key):
    """Parse PlayerCareerStats DataFrame rows into our season dict format."""
    seasons = []
    for _, row in df.iterrows():
        gp = int(row.get("GP", 0) or 0)
        if gp == 0:
            continue

        sid = str(row.get("SEASON_ID", ""))
        if "-" in sid:
            label = sid
        elif len(sid) >= 4:
            try:
                label = _season_label(int(sid[:4]))
            except:
                continue
        else:
            continue

        try:
            year = int(label.split("-")[0])
        except:
            continue

        # Per-game stats (PlayerCareerStats with PerGame returns per-game already)
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

        # TS%
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
        seasons.append(sd)

    return seasons


def fetch_all_legends(delay=0.7, batch_seasons_already_fetched=None):
    """Fetch career stats for all curated legends.
    
    Args:
        delay: Seconds between API calls
        batch_seasons_already_fetched: set of season labels already in batch data
            (e.g. {'1996-97', '1997-98', ...}). If provided, we skip seasons
            that are already covered by batch LeagueDashPlayerStats.
    """
    from nba_api.stats.endpoints import playercareerstats, commonplayerinfo

    already_fetched = batch_seasons_already_fetched or set()
    players = {}
    season_rows_by_year = defaultdict(list)  # for league stat computation
    
    total = len(HISTORICAL_LEGENDS)
    fetched = 0
    errors = 0

    print(f"\n🏛️  Fetching {total} historical legends via PlayerCareerStats...")
    print(f"   Skipping seasons already in batch: {len(already_fetched)}")
    print("=" * 60)

    for pid, name in HISTORICAL_LEGENDS.items():
        fetched += 1
        if fetched % 10 == 0 or fetched == 1:
            print(f"  [{fetched}/{total}] {name}...")

        try:
            career = playercareerstats.PlayerCareerStats(
                player_id=pid, per_mode36="PerGame"
            )
            time.sleep(delay)
            
            dfs = career.get_data_frames()
            if not dfs or dfs[0].empty:
                print(f"    ⚠️  No data for {name}")
                errors += 1
                continue

            reg_df = dfs[0]  # SeasonTotalsRegularSeason
            
            # Get position info
            hi = 0
            pos = "SF"
            try:
                pinfo = commonplayerinfo.CommonPlayerInfo(player_id=pid)
                time.sleep(delay)
                pi_df = pinfo.get_data_frames()[0]
                if not pi_df.empty:
                    r = pi_df.iloc[0]
                    raw_pos = str(r.get("POSITION", "") or "")
                    if raw_pos:
                        pos = raw_pos.split("-")[0].strip()
                    ht = str(r.get("HEIGHT", "") or "")
                    if "-" in ht:
                        parts = ht.split("-")
                        try:
                            hi = int(parts[0]) * 12 + int(parts[1])
                        except:
                            pass
            except:
                pass

            if not pos or pos == "":
                pos = _pos_from_height(hi)

            # Parse regular season
            regular = _parse_season_rows(reg_df, "regular")

            # Parse playoffs (usually index 2)
            playoffs = []
            try:
                if len(dfs) > 2 and not dfs[2].empty:
                    playoffs = _parse_season_rows(dfs[2], "playoffs")
            except:
                pass

            # Compute totals
            totals_reg = defaultdict(int)
            totals_ply = defaultdict(int)
            for s in regular:
                gp = s["gp"]
                totals_reg["PTS"] += int(round(s["ppg"] * gp))
                totals_reg["REB"] += int(round(s["rpg"] * gp))
                totals_reg["AST"] += int(round(s["apg"] * gp))
                totals_reg["STL"] += int(round(s["spg"] * gp))
                totals_reg["BLK"] += int(round(s["bpg"] * gp))
                totals_reg["TOV"] += int(round(s["tov_pg"] * gp))
            for s in playoffs:
                gp = s["gp"]
                totals_ply["PTS"] += int(round(s["ppg"] * gp))
                totals_ply["REB"] += int(round(s["rpg"] * gp))
                totals_ply["AST"] += int(round(s["apg"] * gp))
                totals_ply["STL"] += int(round(s["spg"] * gp))
                totals_ply["BLK"] += int(round(s["bpg"] * gp))
                totals_ply["TOV"] += int(round(s["tov_pg"] * gp))

            # Add season rows to league averages pool (only pre-batch seasons)
            for s in regular:
                if s["season"] not in already_fetched:
                    season_rows_by_year[("regular", s["season"])].append(s)
            for s in playoffs:
                if s["season"] not in already_fetched:
                    season_rows_by_year[("playoffs", s["season"])].append(s)

            # Determine active status
            years = sorted(set(s["year"] for s in regular))
            is_active = years and years[-1] >= 2024

            player_entry = {
                "info": {
                    "nba_api_id": pid,
                    "full_name": name,
                    "is_active": is_active,
                    "position": pos,
                    "height": f"{hi // 12}-{hi % 12}" if hi > 0 else "",
                    "height_inches": hi,
                    "bbref_id": _bbref_id(name, pid),
                },
                "regular": regular,
                "playoffs": playoffs,
                "totals_regular": dict(totals_reg),
                "totals_playoffs": dict(totals_ply),
            }

            players[str(pid)] = player_entry

        except Exception as e:
            print(f"    ❌ Error fetching {name}: {e}")
            errors += 1
            continue

    # Summary
    total_reg = sum(len(p["regular"]) for p in players.values())
    total_ply = sum(len(p["playoffs"]) for p in players.values())
    unique_seasons = set()
    for p in players.values():
        for s in p["regular"]:
            unique_seasons.add(s["season"])

    print(f"\n{'='*60}")
    print(f"✅ Fetched {len(players)} historical players ({errors} errors)")
    print(f"   {total_reg} regular + {total_ply} playoff season rows")
    print(f"   Spanning {min(unique_seasons, default='?')} to {max(unique_seasons, default='?')}")

    return players, dict(season_rows_by_year)


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Fetch historical NBA legends")
    parser.add_argument("--output-dir", type=str, default="./backend/data")
    parser.add_argument("--delay", type=float, default=0.7)
    args = parser.parse_args()

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Check if batch data exists to know which seasons to skip
    cached_sd_path = out_dir / "_cached_season_data.json"
    batch_seasons = set()
    if cached_sd_path.exists():
        print("Loading batch season data to detect overlap...")
        with open(cached_sd_path) as f:
            sd_raw = json.load(f)
        for stype in ["regular", "playoffs"]:
            batch_seasons.update(sd_raw.get(stype, {}).keys())
        print(f"  Batch covers {len(batch_seasons)} season-types")

    players, season_rows = fetch_all_legends(
        delay=args.delay,
        batch_seasons_already_fetched=batch_seasons,
    )

    # Save
    hist_players_path = out_dir / "historical_players.json"
    hist_seasons_path = out_dir / "historical_seasons.json"

    with open(hist_players_path, "w") as f:
        json.dump(players, f, indent=2, default=str)
    print(f"\n💾 Saved {hist_players_path} ({hist_players_path.stat().st_size / 1024:.0f} KB)")

    # Convert season_rows keys to strings for JSON
    sr_serializable = {}
    for (stype, label), rows in season_rows.items():
        key = f"{stype}|{label}"
        sr_serializable[key] = rows
    with open(hist_seasons_path, "w") as f:
        json.dump(sr_serializable, f, indent=2, default=str)
    print(f"💾 Saved {hist_seasons_path} ({hist_seasons_path.stat().st_size / 1024:.0f} KB)")

    print(f"\nNext: run `python merge_historical.py` to merge into main pipeline data")


if __name__ == "__main__":
    main()
