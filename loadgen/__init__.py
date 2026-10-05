"""Synthetic load generator (design doc §5.6).

Deliberately *not* part of `src/aoe/` — this is test tooling, not product. It is
also deliberately not locust/k6: the number that matters here is stream lag
(XADD -> worker persisted it), and an off-the-shelf HTTP load tool can only see
the HTTP leg. Anything that reports "sub-10ms p99" while the buffer behind the
endpoint grows without bound is measuring the wrong system.
"""

from __future__ import annotations

__all__ = ["generate", "synthetic"]
