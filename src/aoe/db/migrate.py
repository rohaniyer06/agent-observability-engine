"""Forward-only SQL migration runner.

Plain numbered .sql files plus a 40-line runner, rather than Alembic. There is no
ORM to autogenerate against, the schema is small, and "here is the exact DDL that
ran" is a better artifact to reason about than a chain of generated revisions.
Each file runs inside a transaction and is recorded in schema_migrations.
"""

from __future__ import annotations

import asyncio
import hashlib
import sys
from pathlib import Path

import asyncpg

BOOTSTRAP = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    version     TEXT PRIMARY KEY,
    checksum    TEXT NOT NULL,
    applied_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""


def discover(migrations_dir: Path) -> list[Path]:
    return sorted(p for p in migrations_dir.glob("*.sql") if p.is_file())


async def run_migrations(dsn: str, migrations_dir: Path) -> list[str]:
    conn = await asyncpg.connect(dsn=dsn)
    applied: list[str] = []
    try:
        await conn.execute(BOOTSTRAP)
        rows = await conn.fetch("SELECT version, checksum FROM schema_migrations")
        seen = {r["version"]: r["checksum"] for r in rows}

        for path in discover(migrations_dir):
            version = path.stem
            sql = path.read_text()
            checksum = hashlib.sha256(sql.encode()).hexdigest()[:16]

            if version in seen:
                if seen[version] != checksum:
                    raise RuntimeError(
                        f"migration {version} was modified after it was applied "
                        f"(recorded {seen[version]}, file is {checksum}). "
                        "Migrations are immutable — add a new file instead."
                    )
                continue

            async with conn.transaction():
                await conn.execute(sql)
                await conn.execute(
                    "INSERT INTO schema_migrations (version, checksum) VALUES ($1, $2)",
                    version,
                    checksum,
                )
            applied.append(version)
    finally:
        await conn.close()
    return applied


async def _amain() -> int:
    from aoe.config import get_settings

    settings = get_settings()
    migrations_dir = Path(settings.migrations_dir)
    if not migrations_dir.is_dir():
        print(f"migrations dir not found: {migrations_dir}", file=sys.stderr)
        return 1

    applied = await run_migrations(settings.postgres_dsn, migrations_dir)
    if applied:
        for v in applied:
            print(f"applied {v}")
    else:
        print("schema already up to date")
    return 0


def main() -> None:
    raise SystemExit(asyncio.run(_amain()))


if __name__ == "__main__":
    main()
