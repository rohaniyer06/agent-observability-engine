"""Query API service (design doc §5.4).

Read-only. Serves the dashboard's REST history plus the live WebSocket fan-out
(the WS lives here rather than on the ingest service — DEVIATIONS.md #7).
"""
