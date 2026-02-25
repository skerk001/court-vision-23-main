"""Courtside FastAPI Backend — serves real NBA stats with PMI v41d.

Run:
  cd court-vision-23
  uvicorn backend.app.main:app --reload --port 8000

The React frontend connects to http://localhost:8000/api/v1/
"""

import json
import logging
from pathlib import Path
from contextlib import asynccontextmanager

from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

logger = logging.getLogger(__name__)
DATA_DIR = Path(__file__).parent.parent / "data"

# ── In-memory data store ──────────────────────────────────────────────────────
_data = {
    "players_regular": [],
    "players_playoffs": [],
    "seasons_regular": {},
    "seasons_playoffs": {},
    "loaded": False,
}


def _load_json(filename: str):
    """Load a JSON file from the data directory."""
    path = DATA_DIR / filename
    if path.exists():
        with open(path) as f:
            return json.load(f)
    return None


def load_data():
    """Load all precomputed JSON data into memory."""
    _data["players_regular"] = _load_json("players_regular.json") or []
    _data["players_playoffs"] = _load_json("players_playoffs.json") or []
    _data["seasons_regular"] = _load_json("seasons_regular.json") or {}
    _data["seasons_playoffs"] = _load_json("seasons_playoffs.json") or {}
    _data["loaded"] = len(_data["players_regular"]) > 0

    if _data["loaded"]:
        logger.info(
            f"Loaded {len(_data['players_regular'])} regular + "
            f"{len(_data['players_playoffs'])} playoff players"
        )
    else:
        logger.warning("No data files found in backend/data/. Run: python -m backend.scrapers.fetch_nba_data")


# ── App setup ─────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    load_data()
    yield


app = FastAPI(title="Courtside API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://localhost:3000",
        "http://localhost:8080",
        "http://127.0.0.1:5173",
        "http://127.0.0.1:8080",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Endpoints ─────────────────────────────────────────────────────────────────

@app.get("/api/v1/status")
async def status():
    return {
        "loaded": _data["loaded"],
        "total_players": len(_data["players_regular"]),
        "total_playoff_players": len(_data["players_playoffs"]),
        "total_regular_seasons": sum(len(v) for v in _data["seasons_regular"].values()),
        "total_playoff_seasons": sum(len(v) for v in _data["seasons_playoffs"].values()),
    }


@app.get("/api/v1/search")
async def search(q: str = Query("", min_length=1), limit: int = Query(10, le=50)):
    """Search players by name."""
    query = q.lower()
    results = [
        p for p in _data["players_regular"]
        if query in p.get("full_name", "").lower()
    ][:limit]
    return {"players": results}


@app.get("/api/v1/player/{bbref_id}")
async def player_profile(bbref_id: str):
    """Get player career summary + all season data."""
    # Find player in regular season data
    player = next(
        (p for p in _data["players_regular"] if p.get("bbref_id") == bbref_id),
        None
    )
    if not player:
        return JSONResponse(content={"error": "Player not found"}, status_code=404)

    playoff_player = next(
        (p for p in _data["players_playoffs"] if p.get("bbref_id") == bbref_id),
        None
    )

    return {
        "player": player,
        "playoff_summary": playoff_player,
        "seasons_regular": _data["seasons_regular"].get(bbref_id, []),
        "seasons_playoffs": _data["seasons_playoffs"].get(bbref_id, []),
    }


@app.get("/api/v1/player/{bbref_id}/seasons")
async def player_seasons(
    bbref_id: str,
    season_type: str = Query("regular"),
):
    """Get season-by-season stats with PMI."""
    seasons_map = (
        _data["seasons_regular"] if season_type == "regular"
        else _data["seasons_playoffs"]
    )
    players_list = (
        _data["players_regular"] if season_type == "regular"
        else _data["players_playoffs"]
    )

    seasons = seasons_map.get(bbref_id, [])
    player = next(
        (p for p in players_list if p.get("bbref_id") == bbref_id),
        None
    )

    if not player:
        return JSONResponse(content={"error": "Player not found"}, status_code=404)

    return {
        "player": player,
        "seasons": seasons,
        "season_type": season_type,
    }


@app.get("/api/v1/leaderboard")
async def leaderboard(
    stat: str = Query("pmi"),
    limit: int = Query(50, ge=1, le=2000),
    season_type: str = Query("regular"),
    sort_dir: str = Query("desc"),
):
    """Get leaderboard sorted by any stat."""
    players = (
        _data["players_regular"] if season_type == "regular"
        else _data["players_playoffs"]
    )

    # Filter out players missing the stat
    filtered = [p for p in players if p.get(stat) is not None]

    # Sort
    reverse = sort_dir == "desc"
    filtered.sort(key=lambda p: p.get(stat, 0) or 0, reverse=reverse)

    return {"players": filtered[:limit], "stat": stat, "total": len(filtered)}


@app.get("/api/v1/leaders/{stat}")
async def stat_leaders(
    stat: str,
    limit: int = Query(5, ge=1, le=20),
    season_type: str = Query("regular"),
):
    """Top N leaders for a stat (for homepage cards)."""
    players = (
        _data["players_regular"] if season_type == "regular"
        else _data["players_playoffs"]
    )
    filtered = [p for p in players if p.get(stat) is not None]
    filtered.sort(key=lambda p: p.get(stat, 0) or 0, reverse=True)
    return {"stat": stat, "leaders": filtered[:limit]}


@app.get("/api/v1/players")
async def players_list(
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=10, le=200),
    position: str = Query(None),
    status: str = Query(None),
    search: str = Query(None),
):
    """Paginated player list."""
    players = list(_data["players_regular"])

    if position and position != "all":
        players = [p for p in players if p.get("position") == position]
    if status == "active":
        players = [p for p in players if p.get("is_active")]
    elif status == "retired":
        players = [p for p in players if not p.get("is_active")]
    if search:
        q = search.lower()
        players = [p for p in players if q in p.get("full_name", "").lower()]

    total = len(players)
    start = (page - 1) * per_page
    end = start + per_page

    return {
        "players": players[start:end],
        "page": page,
        "total_pages": max(1, (total + per_page - 1) // per_page),
        "total": total,
    }
