import { useCallback, useEffect, useRef, useState } from "react";
import { describeError, isAbort } from "./api";

export interface PollResult<T> {
  data: T | null;
  error: string | null;
  /** True on the first load and on every param change. Panels hold the previous
   *  render at reduced opacity rather than flashing a skeleton. */
  loading: boolean;
  lastUpdated: number | null;
  refetch: () => void;
}

export interface PollOptions {
  /** Milliseconds between polls. Chosen per panel — nothing polls at 1s. */
  intervalMs: number;
  /** When true, the in-flight fetch finishes but no follow-up is scheduled. */
  paused?: boolean;
  /**
   * Identity of the current params. Changing it refetches immediately while
   * keeping the previous data on screen.
   */
  key?: string;
}

/**
 * A self-rescheduling poller. Deliberately `setTimeout`-after-settle rather than
 * `setInterval`: a slow response can never stack requests on top of itself.
 */
export function usePolling<T>(
  fetcher: (signal: AbortSignal) => Promise<T>,
  { intervalMs, paused = false, key = "" }: PollOptions,
): PollResult<T> {
  const fetcherRef = useRef(fetcher);
  fetcherRef.current = fetcher;

  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [lastUpdated, setLastUpdated] = useState<number | null>(null);
  const [nonce, setNonce] = useState(0);

  useEffect(() => {
    let cancelled = false;
    let timer: number | undefined;
    const controller = new AbortController();

    setLoading(true);

    const tick = async () => {
      try {
        const next = await fetcherRef.current(controller.signal);
        if (cancelled) return;
        setData(next);
        setError(null);
        setLastUpdated(Date.now());
      } catch (err) {
        if (cancelled || isAbort(err)) return;
        // Keep the last good data on screen; the panel shows a stale banner.
        setError(describeError(err));
      } finally {
        if (!cancelled) {
          setLoading(false);
          if (!paused) timer = window.setTimeout(() => void tick(), intervalMs);
        }
      }
    };

    void tick();

    return () => {
      cancelled = true;
      controller.abort();
      if (timer !== undefined) window.clearTimeout(timer);
    };
  }, [intervalMs, paused, key, nonce]);

  const refetch = useCallback(() => setNonce((n) => n + 1), []);

  return { data, error, loading, lastUpdated, refetch };
}
