/**
 * Courtside API Client
 *
 * Connects the React frontend to the FastAPI backend.
 * Falls back to mockData.ts when the backend is unavailable.
 *
 * Backend: http://localhost:8000/api/v1/
 * Endpoints:
 *   GET /status               → { loaded, total_players }
 *   GET /search?q=            → { players: [...] }
 *   GET /player/{bbref_id}    → { player, career_regular, career_playoffs }
 *   GET /player/{bbref_id}/seasons?season_type=regular
 *   GET /leaderboard?stat=ppg&limit=50&season_type=regular&scope=career
 *   GET /leaders/{stat}?limit=5
 *   GET /players?page=1&per_page=50
 */

const API_BASE = import.meta.env.VITE_API_URL || "http://localhost:8000/api/v1";

// ── Helpers ──────────────────────────────────────────────────────────────────

let _backendAvailable: boolean | null = null;

async function checkBackend(): Promise<boolean> {
  if (_backendAvailable !== null) return _backendAvailable;
  try {
    const res = await fetch(`${API_BASE}/status`, { signal: AbortSignal.timeout(2000) });
    const data = await res.json();
    _backendAvailable = data.loaded === true;
  } catch {
    _backendAvailable = false;
  }
  return _backendAvailable;
}

/** Reset the backend check (e.g., when user reconnects). */
export function resetBackendCheck() {
  _backendAvailable = null;
}

async function apiFetch<T>(path: string, params?: Record<string, string | number>): Promise<T> {
  const url = new URL(`${API_BASE}${path}`);
  if (params) {
    Object.entries(params).forEach(([k, v]) => {
      if (v !== undefined && v !== null) url.searchParams.set(k, String(v));
    });
  }
  const res = await fetch(url.toString());
  if (!res.ok) {
    throw new Error(`API error ${res.status}: ${res.statusText}`);
  }
  return res.json();
}

// ── Types ────────────────────────────────────────────────────────────────────

export interface PlayerSearchResult {
  player_id: number;
  full_name: string;
  bbref_id: string;
  nba_api_id: number | null;
  primary_position: string;
  is_active: boolean;
}

export interface SeasonStats {
  season: string;
  team: string;
  gp: number;
  gs?: number;
  mpg?: number;
  ppg: number;
  rpg: number;
  apg: number;
  spg?: number;
  bpg?: number;
  tovpg?: number;
  fg_pct?: number;
  fg3_pct?: number;
  ft_pct?: number;
  ts_pct?: number;
  rts_pct?: number;
  per?: number;
  usg_pct?: number;
  ws?: number;
  ws_48?: number;
  ows?: number;
  dws?: number;
  bpm?: number;
  obpm?: number;
  dbpm?: number;
  vorp?: number;
  opmi?: number;
  dpmi?: number;
  pmi?: number;
}

export interface CareerSummary {
  gp?: number;
  ppg?: number;
  rpg?: number;
  apg?: number;
  spg?: number;
  bpg?: number;
  mpg?: number;
  fg_pct?: number;
  ts_pct?: number;
  per?: number;
  ws?: number;
  bpm?: number;
  vorp?: number;
  opmi?: number;
  dpmi?: number;
  pmi?: number;
  peak_pmi?: number;
  peak_season?: string;
  awc?: number;
  seasons?: number;
}

export interface PlayerProfile {
  player: Record<string, unknown>;
  career_regular: SeasonStats[];
  career_playoffs: SeasonStats[];
}

export interface PlayerSeasonsResponse {
  player: Record<string, unknown>;
  seasons: SeasonStats[];
  career_summary: CareerSummary;
  season_type: string;
}

export interface LeaderboardResponse {
  players: Record<string, unknown>[];
}

// ── API Functions ────────────────────────────────────────────────────────────

/** Check if the backend API is running and has data loaded. */
export async function isBackendAvailable(): Promise<boolean> {
  return checkBackend();
}

/** Search for players by name. */
export async function searchPlayers(query: string, limit = 10): Promise<PlayerSearchResult[]> {
  const data = await apiFetch<{ players: PlayerSearchResult[] }>("/search", { q: query, limit });
  return data.players;
}

/** Get full player profile with both regular and playoff career data. */
export async function getPlayerProfile(bbrefId: string): Promise<PlayerProfile> {
  return apiFetch<PlayerProfile>(`/player/${bbrefId}`);
}

/** Get season-by-season stats (with PMI) for a player. */
export async function getPlayerSeasons(
  bbrefId: string,
  seasonType: "regular" | "playoffs" = "regular"
): Promise<PlayerSeasonsResponse> {
  return apiFetch<PlayerSeasonsResponse>(`/player/${bbrefId}/seasons`, {
    season_type: seasonType,
  });
}

/** Get leaderboard data. */
export async function getLeaderboard(params: {
  stat?: string;
  limit?: number;
  season_type?: string;
  min_gp?: number;
  scope?: string;
  era?: string;
  sort?: string;
  tab?: string;
}): Promise<LeaderboardResponse> {
  return apiFetch<LeaderboardResponse>("/leaderboard", params as Record<string, string | number>);
}

/** Get top N leaders for a specific stat. */
export async function getStatLeaders(
  stat: string,
  limit = 5,
  seasonType = "regular"
): Promise<{ stat: string; leaders: Record<string, unknown>[] }> {
  return apiFetch(`/leaders/${stat}`, { limit, season_type: seasonType });
}

/** Get paginated player list. */
export async function getPlayersList(params: {
  page?: number;
  per_page?: number;
  position?: string;
  status?: string;
  search?: string;
}): Promise<{ players: Record<string, unknown>[]; page: number; total_pages: number; total: number }> {
  return apiFetch("/players", params as Record<string, string | number>);
}
