"""Small forward-only SQL migration runner for the control database."""

from __future__ import annotations

import hashlib
from pathlib import Path

import psycopg


def apply_migrations(
    dsn: str, migration_dir: Path, *, registry: str = "control_schema_migrations"
) -> tuple[str, ...]:
    """Apply unapplied SQL files atomically and reject changed applied files."""

    files = sorted(migration_dir.glob("[0-9][0-9][0-9][0-9]_*.sql"))
    if not files:
        raise ValueError(f"no control migrations found in {migration_dir}")
    if registry not in {"control_schema_migrations", "target_schema_migrations"}:
        raise ValueError("unapproved migration registry")
    applied: list[str] = []
    with psycopg.connect(dsn) as conn:
        conn.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (registry,))
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS public.{registry} (
                version text PRIMARY KEY,
                checksum text NOT NULL,
                applied_at timestamptz NOT NULL DEFAULT now()
            )
            """
        )
        for path in files:
            script = path.read_text(encoding="utf-8")
            checksum = hashlib.sha256(script.encode("utf-8")).hexdigest()
            existing = conn.execute(
                f"SELECT checksum FROM public.{registry} WHERE version = %s",
                (path.name,),
            ).fetchone()
            if existing:
                if existing[0] != checksum:
                    raise ValueError(f"applied migration changed: {path.name}")
                continue
            conn.execute(script, prepare=False)
            conn.execute(
                f"INSERT INTO public.{registry}(version, checksum) VALUES (%s, %s)",
                (path.name, checksum),
            )
            applied.append(path.name)
    return tuple(applied)
