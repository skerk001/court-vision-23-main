"""Export backend JSON data → frontend mockData.ts

Run from project root after ingestion:
  python -m backend.scrapers.export_to_mock

This reads backend/data/*.json and writes src/lib/mockData.ts
so the frontend works without the API running.
"""

import json
from pathlib import Path

ROOT = Path(__file__).parent.parent.parent
DATA_DIR = ROOT / "backend" / "data"
OUTPUT = ROOT / "src" / "lib" / "mockData.ts"


def main():
    print("📦 Exporting backend data → mockData.ts")

    # Load all data
    players_reg = json.loads((DATA_DIR / "players_regular.json").read_text())
    players_ply = json.loads((DATA_DIR / "players_playoffs.json").read_text())
    seasons_reg = json.loads((DATA_DIR / "seasons_regular.json").read_text())
    seasons_ply = json.loads((DATA_DIR / "seasons_playoffs.json").read_text())

    print(f"  Regular: {len(players_reg)} players, {sum(len(v) for v in seasons_reg.values())} season rows")
    print(f"  Playoffs: {len(players_ply)} players, {sum(len(v) for v in seasons_ply.values())} season rows")

    # Build TypeScript content
    ts = []
    ts.append('export interface PlayerData { [key: string]: any; }\n')
    ts.append('// Season-by-season data: { [bbref_id]: SeasonData[] }')
    ts.append('export interface SeasonData {')
    ts.append('  season: string;')
    ts.append('  year: number;')
    ts.append('  gp: number;')
    ts.append('  ppg: number;')
    ts.append('  rpg: number;')
    ts.append('  apg: number;')
    ts.append('  spg: number;')
    ts.append('  bpg: number;')
    ts.append('  fg_pct: number;')
    ts.append('  ts_pct: number;')
    ts.append('  mpg: number;')
    ts.append('  pmi: number;')
    ts.append('  opmi: number;')
    ts.append('  dpmi: number;')
    ts.append('  awc: number;')
    ts.append('  peak_pmi: number;')
    ts.append('  cpmi?: number;')
    ts.append('}\n')

    # Regular players
    ts.append(f'// {len(players_reg)} regular season players')
    ts.append(f'export const MOCK_PLAYERS: PlayerData[] = {json.dumps(players_reg, indent=2)};')
    ts.append('')

    # Playoff players
    ts.append(f'// {len(players_ply)} playoff players')
    ts.append(f'export const MOCK_PLAYERS_PLAYOFFS: PlayerData[] = {json.dumps(players_ply, indent=2)};')
    ts.append('')

    # Season data regular
    ts.append(f'// Season data for {len(seasons_reg)} players')
    ts.append(f'export const SEASON_DATA_REGULAR: Record<string, SeasonData[]> = {json.dumps(seasons_reg, indent=2)};')
    ts.append('')

    # Season data playoffs
    ts.append(f'// Playoff season data for {len(seasons_ply)} players')
    ts.append(f'export const SEASON_DATA_PLAYOFFS: Record<string, SeasonData[]> = {json.dumps(seasons_ply, indent=2)};')

    content = '\n'.join(ts)

    # Write
    OUTPUT.write_text(content, encoding='utf-8')
    size_mb = OUTPUT.stat().st_size / 1024 / 1024
    print(f"\n✅ Wrote {OUTPUT} ({size_mb:.1f} MB)")
    print("   Restart frontend: npm run dev")


if __name__ == "__main__":
    main()
