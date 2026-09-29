"""Per-IP token bucket for the ingestion endpoint.

In-process on purpose. A Redis-backed limiter would put a network round trip in
front of every request on the path whose latency this project exists to measure,
which is the wrong trade for a self-hosted single-ingest-process deployment. The
consequence — N ingest processes each enforce their own bucket, so the effective
global limit is N * rps — is the accepted cost, and is documented here rather
than discovered later.

Disabled entirely at the default `AOE_RATE_LIMIT_RPS=0`, because the synthetic
load generator (design doc §5.6) exists to saturate this endpoint and must not be
throttled by the thing it is measuring.
"""

from __future__ import annotations

import time
from collections import OrderedDict

# Past this many tracked IPs we start evicting the least-recently-seen. Without a
# bound, a spray of spoofed source addresses turns the limiter into a memory leak
# in the one process that must not fall over under load.
MAX_TRACKED_KEYS = 10_000


class TokenBucketLimiter:
    """Classic token bucket, one bucket per client key, LRU-bounded.

    Not thread-safe and does not need to be: a uvicorn worker runs one event
    loop, and `allow()` contains no await point, so it cannot interleave.
    """

    __slots__ = ("_rps", "_burst", "_max_keys", "_buckets", "rejected")

    def __init__(
        self,
        rps: float,
        *,
        burst: float | None = None,
        max_keys: int = MAX_TRACKED_KEYS,
    ) -> None:
        self._rps = float(rps)
        # One second of capacity by default: enough to absorb the jitter of a
        # batching client without letting a sustained overload through.
        self._burst = float(burst) if burst is not None else max(self._rps, 1.0)
        self._max_keys = max_keys
        # Ordered by last-seen; the front of the dict is the eviction candidate.
        self._buckets: OrderedDict[str, tuple[float, float]] = OrderedDict()
        self.rejected = 0

    @property
    def enabled(self) -> bool:
        return self._rps > 0

    @property
    def tracked_keys(self) -> int:
        return len(self._buckets)

    def allow(self, key: str) -> bool:
        """Consume one token for `key`. False means the caller should 429."""
        if self._rps <= 0:
            # Return before touching the dict at all — the default configuration
            # must cost nothing on the hot path.
            return True

        now = time.monotonic()
        entry = self._buckets.get(key)
        if entry is None:
            if len(self._buckets) >= self._max_keys:
                self._buckets.popitem(last=False)
            self._buckets[key] = (self._burst - 1.0, now)
            return True

        tokens, last_seen = entry
        tokens = min(self._burst, tokens + (now - last_seen) * self._rps)
        allowed = tokens >= 1.0
        if allowed:
            tokens -= 1.0
        else:
            self.rejected += 1
        self._buckets[key] = (tokens, now)
        self._buckets.move_to_end(key)
        return allowed
