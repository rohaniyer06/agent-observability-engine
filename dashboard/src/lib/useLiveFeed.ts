import { useCallback, useEffect, useRef, useState } from "react";
import { LIVE_WS_URL } from "./api";
import type { LiveTraceEvent } from "../types/api";

export type ConnectionStatus = "connecting" | "open" | "reconnecting" | "closed";

/**
 * The one frame shape the socket sends that is *not* in types/api.ts: the
 * handshake envelope. `LiveTraceEvent` itself is imported, never redefined.
 */
interface HelloFrame {
  type: "hello";
  backlog: LiveTraceEvent[];
}

/** A rendered row. `seq` is a client-side monotonic id — one trace can produce
 *  several events (finalized, then an anomaly), so trace_id is not a React key. */
export interface LiveRow {
  seq: number;
  event: LiveTraceEvent;
  receivedAt: number;
}

export interface LiveFeedState {
  rows: LiveRow[];
  status: ConnectionStatus;
  /** Consecutive failed connection attempts; 0 once open. */
  attempt: number;
  /** Every event seen since mount, including ones trimmed off the list. */
  totalReceived: number;
  /** Wall-clock of the last frame of any kind (including pong). */
  lastMessageAt: number | null;
  reconnectNow: () => void;
}

interface Options {
  /** Max rendered rows. The DOM cap is what keeps a 1000 RPS run cheap. */
  maxRows?: number;
  /** Freeze the rendered list (the socket stays open). */
  frozen?: boolean;
}

/** Frames are coalesced into React state on this cadence, not per-message. */
const FLUSH_MS = 200;
const PING_MS = 20_000;
const BASE_BACKOFF_MS = 500;
const MAX_BACKOFF_MS = 15_000;

function isRecord(v: unknown): v is Record<string, unknown> {
  return typeof v === "object" && v !== null;
}

function isLiveTraceEvent(v: unknown): v is LiveTraceEvent {
  return (
    isRecord(v) &&
    typeof v.trace_id === "string" &&
    typeof v.type === "string" &&
    Array.isArray(v.path)
  );
}

function isHelloFrame(v: unknown): v is HelloFrame {
  return isRecord(v) && v.type === "hello" && Array.isArray(v.backlog);
}

export function useLiveFeed({ maxRows = 100, frozen = false }: Options = {}): LiveFeedState {
  const [rows, setRows] = useState<LiveRow[]>([]);
  const [status, setStatus] = useState<ConnectionStatus>("connecting");
  const [attempt, setAttempt] = useState(0);
  const [totalReceived, setTotalReceived] = useState(0);
  const [lastMessageAt, setLastMessageAt] = useState<number | null>(null);
  const [nonce, setNonce] = useState(0);

  // Incoming frames land here first and are drained on a timer, so a burst of
  // a thousand messages costs one render instead of a thousand.
  const bufferRef = useRef<LiveRow[]>([]);
  const seqRef = useRef(0);
  const frozenRef = useRef(frozen);
  frozenRef.current = frozen;

  const reconnectNow = useCallback(() => setNonce((n) => n + 1), []);

  useEffect(() => {
    let closedByUs = false;
    let socket: WebSocket | null = null;
    let retryTimer: number | undefined;
    let pingTimer: number | undefined;
    let failures = 0;

    const push = (events: LiveTraceEvent[]) => {
      if (events.length === 0) return;
      const now = Date.now();
      const incoming = events.map((event) => ({ seq: seqRef.current++, event, receivedAt: now }));
      // Newest first, and never hold more than one screenful in the buffer.
      bufferRef.current = [...incoming.reverse(), ...bufferRef.current].slice(0, maxRows);
      setTotalReceived((n) => n + events.length);
      setLastMessageAt(now);
    };

    const connect = () => {
      setStatus(failures === 0 ? "connecting" : "reconnecting");
      let ws: WebSocket;
      try {
        ws = new WebSocket(LIVE_WS_URL);
      } catch {
        scheduleRetry();
        return;
      }
      socket = ws;

      ws.onopen = () => {
        failures = 0;
        setAttempt(0);
        setStatus("open");
        // Keep-alive: idle proxies drop a silent socket. The server answers "pong".
        pingTimer = window.setInterval(() => {
          if (ws.readyState === WebSocket.OPEN) ws.send("ping");
        }, PING_MS);
      };

      ws.onmessage = (ev: MessageEvent) => {
        const raw: unknown = ev.data;
        if (typeof raw !== "string") return;
        setLastMessageAt(Date.now());
        if (raw === "pong") return;

        let parsed: unknown;
        try {
          parsed = JSON.parse(raw);
        } catch {
          return; // A non-JSON text frame is a keep-alive, not telemetry.
        }

        if (isHelloFrame(parsed)) {
          // Handshake backlog so a freshly-opened dashboard is not blank.
          push(parsed.backlog.filter(isLiveTraceEvent).slice(-maxRows));
          return;
        }
        if (isLiveTraceEvent(parsed)) push([parsed]);
      };

      ws.onerror = () => {
        // onclose always follows; retry scheduling lives there.
      };

      ws.onclose = () => {
        if (pingTimer !== undefined) window.clearInterval(pingTimer);
        pingTimer = undefined;
        if (closedByUs) return;
        scheduleRetry();
      };
    };

    const scheduleRetry = () => {
      failures += 1;
      setAttempt(failures);
      setStatus("reconnecting");
      // Capped exponential backoff with jitter, so N dashboards reconnecting
      // after a restart don't arrive in lockstep.
      const capped = Math.min(MAX_BACKOFF_MS, BASE_BACKOFF_MS * 2 ** (failures - 1));
      const delay = capped * (0.7 + Math.random() * 0.6);
      retryTimer = window.setTimeout(connect, delay);
    };

    const flush = window.setInterval(() => {
      if (frozenRef.current) {
        bufferRef.current = bufferRef.current.slice(0, maxRows);
        return;
      }
      if (bufferRef.current.length === 0) return;
      const drained = bufferRef.current;
      bufferRef.current = [];
      setRows((prev) => [...drained, ...prev].slice(0, maxRows));
    }, FLUSH_MS);

    connect();

    return () => {
      closedByUs = true;
      window.clearInterval(flush);
      if (retryTimer !== undefined) window.clearTimeout(retryTimer);
      if (pingTimer !== undefined) window.clearInterval(pingTimer);
      socket?.close();
      setStatus("closed");
    };
  }, [maxRows, nonce]);

  return { rows, status, attempt, totalReceived, lastMessageAt, reconnectNow };
}
