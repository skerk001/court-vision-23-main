"""Merge historical legends into the main pipeline cached data.

Reads:
  - _cached_players.json (from batch pipeline, 1996-2024)
  - _cached_season_data.json (from batch pipeline)
  - historical_players.json (from fetch_historical.py)
  - historical_seasons.json (from fetch_historical.py)

Writes:
  - _cached_players.json (merged)
  - _cached_season_data.json (merged with historical season rows for league stats)

After merging, re-run PMI computation:
  python -m backend.scrapers.fetch_nba_data --recompute
"""

import json
from pathlib import Path
from collections import defaultdict


def main():
    data_dir = Path("./backend/data")

    # Load batch data
    print("Loading batch pipeline data...")
    with open(data_dir / "_cached_players.json") as f:
        batch_players = json.load(f)
    with open(data_dir / "_cached_season_data.json") as f:
        batch_sd = json.load(f)

    print(f"  Batch: {len(batch_players)} players, "
          f"{len(batch_sd.get('regular', {}))} regular seasons")

    # Load historical data
    hist_players_path = data_dir / "historical_players.json"
    hist_seasons_path = data_dir / "historical_seasons.json"

    if not hist_players_path.exists():
        print("❌ historical_players.json not found. Run fetch_historical.py first.")
        return

    with open(hist_players_path) as f:
        hist_players = json.load(f)
    with open(hist_seasons_path) as f:
        hist_seasons_raw = json.load(f)

    print(f"  Historical: {len(hist_players)} players")

    # --- Merge players ---
    # Historical players are keyed by player_id (string)
    # Batch players are also keyed by player_id (string)
    # For players in BOTH (e.g., MJ has 1996-2003 in batch AND 1984-2003 in historical),
    # we need to merge their season lists, avoiding duplicates.

    merged = 0
    added = 0
    for pid_str, hist_p in hist_players.items():
        if pid_str in batch_players:
            # Merge: add historical seasons that aren't already in batch
            batch_p = batch_players[pid_str]
            existing_seasons = {(s["season"], "regular") for s in batch_p.get("regular", [])}
            existing_seasons.update({(s["season"], "playoffs") for s in batch_p.get("playoffs", [])})

            for stype in ["regular", "playoffs"]:
                for s in hist_p.get(stype, []):
                    if (s["season"], stype) not in existing_seasons:
                        batch_p[stype].append(s)
                        existing_seasons.add((s["season"], stype))

            # Sort seasons by year
            batch_p["regular"].sort(key=lambda s: s.get("year", 0))
            batch_p["playoffs"].sort(key=lambda s: s.get("year", 0))

            # Merge totals
            for stat in ["PTS", "REB", "AST", "STL", "BLK", "TOV"]:
                batch_total = int(batch_p.get("totals_regular", {}).get(stat, 0) or 0)
                hist_total = int(hist_p.get("totals_regular", {}).get(stat, 0) or 0)
                # Use the larger total (historical has full career)
                if hist_total > batch_total:
                    if "totals_regular" not in batch_p:
                        batch_p["totals_regular"] = {}
                    batch_p["totals_regular"][stat] = hist_total

                batch_total_p = int(batch_p.get("totals_playoffs", {}).get(stat, 0) or 0)
                hist_total_p = int(hist_p.get("totals_playoffs", {}).get(stat, 0) or 0)
                if hist_total_p > batch_total_p:
                    if "totals_playoffs" not in batch_p:
                        batch_p["totals_playoffs"] = {}
                    batch_p["totals_playoffs"][stat] = hist_total_p

            # Update position/height if batch didn't have it
            if not batch_p.get("info", {}).get("height_inches"):
                for key in ["position", "height", "height_inches"]:
                    if hist_p.get("info", {}).get(key):
                        batch_p["info"][key] = hist_p["info"][key]

            merged += 1
        else:
            # New player not in batch — add directly
            batch_players[pid_str] = hist_p
            added += 1

    print(f"  Merged {merged} existing + added {added} new players")
    print(f"  Total: {len(batch_players)} players")

    # --- Merge season data (for league stats computation) ---
    # Historical seasons that aren't in batch need to be added
    # so the PMI engine has league averages for those years
    hist_season_rows = {}
    for key, rows in hist_seasons_raw.items():
        stype, label = key.split("|", 1)
        hist_season_rows[(stype, label)] = rows

    seasons_added = 0
    for (stype, label), rows in hist_season_rows.items():
        if label not in batch_sd.get(stype, {}):
            # New season not in batch — add it
            # Convert list of dicts to the DataFrame-record format batch expects
            if stype not in batch_sd:
                batch_sd[stype] = {}

            # Convert our internal format to match LeagueDashPlayerStats column names
            api_rows = []
            for r in rows:
                api_rows.append({
                    "PLAYER_ID": 0,  # placeholder
                    "GP": r.get("gp", 0),
                    "MIN": r.get("mpg", 0),
                    "PTS": r.get("ppg", 0),
                    "REB": r.get("rpg", 0),
                    "AST": r.get("apg", 0),
                    "STL": r.get("spg", 0),
                    "BLK": r.get("bpg", 0),
                    "TOV": r.get("tov_pg", 0),
                    "OREB": r.get("orb_pg", 0),
                    "DREB": r.get("drb_pg", 0),
                    "PF": r.get("pf_pg", 0),
                    "FGA": r.get("fga_pg", 0),
                    "FTA": r.get("fta_pg", 0),
                    "FG_PCT": r.get("fg_pct", 0),
                    "FG3M": r.get("fg3m_pg", 0),
                    "_SEASON": label,
                    "_YEAR": r.get("year", 0),
                })
            batch_sd[stype][label] = api_rows
            seasons_added += 1

    print(f"  Added {seasons_added} historical season-types for league stats")
    print(f"  Total seasons: {len(batch_sd.get('regular', {}))} regular, "
          f"{len(batch_sd.get('playoffs', {}))} playoffs")

    # --- Save merged data ---
    print("\n💾 Saving merged data...")
    
    with open(data_dir / "_cached_players.json", "w") as f:
        json.dump(batch_players, f, default=str)
    sz = (data_dir / "_cached_players.json").stat().st_size / 1024 / 1024
    print(f"  _cached_players.json: {sz:.1f} MB")

    with open(data_dir / "_cached_season_data.json", "w") as f:
        json.dump(batch_sd, f, default=str)
    sz2 = (data_dir / "_cached_season_data.json").stat().st_size / 1024 / 1024
    print(f"  _cached_season_data.json: {sz2:.1f} MB")

    # Verify
    print(f"\n✅ Merge complete!")
    print(f"   {len(batch_players)} total players")
    
    # Show some legendary names
    legends_check = ["Michael Jordan", "Kareem Abdul-Jabbar", "Wilt Chamberlain",
                     "Magic Johnson", "Larry Bird", "Bill Russell"]
    for name in legends_check:
        for pid_str, p in batch_players.items():
            if p.get("info", {}).get("full_name") == name:
                reg = len(p.get("regular", []))
                ply = len(p.get("playoffs", []))
                yrs = sorted(set(s.get("year", 0) for s in p.get("regular", [])))
                yr_range = f"{yrs[0]}-{yrs[-1]}" if yrs else "?"
                print(f"   ✅ {name}: {reg} reg + {ply} ply seasons ({yr_range})")
                break

    print(f"\n🔄 Next: python -m backend.scrapers.fetch_nba_data --recompute")


if __name__ == "__main__":
    main()
