"""Ingestion service (design doc §5.2).

The hot path: validate spans, XADD them onto the durable buffer, shed load
rather than block when the buffer is backing up. The ASGI app lives in
`aoe.ingest.app:app` and is deliberately not imported here — `import aoe.ingest`
should not pull in FastAPI.
"""
