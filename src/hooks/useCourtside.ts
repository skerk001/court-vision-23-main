/**
 * useCourtside — React hook for fetching Courtside data.
 *
 * Tries the FastAPI backend first. If unavailable, falls back to mockData.ts.
 * This allows the app to work both in "connected" mode (with backend running)
 * and "standalone" mode (portfolio demo with static data).
 */

import { useState, useEffect, useCallback, useRef } from "react";
import {
  isBackendAvailable,
  getPlayerProfile,
  getPlayerSeasons,
  searchPlayers,
  getLeaderboard,
  type PlayerProfile,
  type PlayerSeasonsResponse,
  type PlayerSearchResult,
} from "@/lib/api";

// ── Backend status hook ──────────────────────────────────────────────────────

let _cachedStatus: boolean | null = null;

export function useBackendStatus() {
  const [available, setAvailable] = useState<boolean | null>(_cachedStatus);
  const [checking, setChecking] = useState(_cachedStatus === null);

  useEffect(() => {
    if (_cachedStatus !== null) {
      setAvailable(_cachedStatus);
      setChecking(false);
      return;
    }
    let cancelled = false;
    isBackendAvailable().then((ok) => {
      if (!cancelled) {
        _cachedStatus = ok;
        setAvailable(ok);
        setChecking(false);
      }
    });
    return () => { cancelled = true; };
  }, []);

  return { available, checking };
}

// ── Generic async data hook ──────────────────────────────────────────────────

interface UseAsyncResult<T> {
  data: T | null;
  loading: boolean;
  error: string | null;
  refetch: () => void;
}

function useAsync<T>(
  fetcher: () => Promise<T>,
  deps: unknown[] = []
): UseAsyncResult<T> {
  const [data, setData] = useState<T | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const mountedRef = useRef(true);

  const fetch = useCallback(() => {
    setLoading(true);
    setError(null);
    fetcher()
      .then((result) => {
        if (mountedRef.current) {
          setData(result);
          setLoading(false);
        }
      })
      .catch((err) => {
        if (mountedRef.current) {
          setError(err.message || "Failed to load data");
          setLoading(false);
        }
      });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, deps);

  useEffect(() => {
    mountedRef.current = true;
    fetch();
    return () => { mountedRef.current = false; };
  }, [fetch]);

  return { data, loading, error, refetch: fetch };
}

// ── Player Profile hook ──────────────────────────────────────────────────────

export function usePlayerProfile(bbrefId: string | undefined) {
  return useAsync<PlayerProfile>(
    () => {
      if (!bbrefId) return Promise.reject(new Error("No player ID"));
      return getPlayerProfile(bbrefId);
    },
    [bbrefId]
  );
}

// ── Player Seasons hook ──────────────────────────────────────────────────────

export function usePlayerSeasons(
  bbrefId: string | undefined,
  seasonType: "regular" | "playoffs" = "regular"
) {
  return useAsync<PlayerSeasonsResponse>(
    () => {
      if (!bbrefId) return Promise.reject(new Error("No player ID"));
      return getPlayerSeasons(bbrefId, seasonType);
    },
    [bbrefId, seasonType]
  );
}

// ── Player Search hook ───────────────────────────────────────────────────────

export function usePlayerSearch(query: string, limit = 10) {
  return useAsync<PlayerSearchResult[]>(
    () => {
      if (query.length < 2) return Promise.resolve([]);
      return searchPlayers(query, limit);
    },
    [query, limit]
  );
}

// ── Leaderboard hook ─────────────────────────────────────────────────────────

export function useLeaderboard(params: {
  stat?: string;
  limit?: number;
  season_type?: string;
  min_gp?: number;
  scope?: string;
  era?: string;
  tab?: string;
}) {
  return useAsync(
    () => getLeaderboard(params),
    [params.stat, params.limit, params.season_type, params.min_gp, params.scope, params.era, params.tab]
  );
}
