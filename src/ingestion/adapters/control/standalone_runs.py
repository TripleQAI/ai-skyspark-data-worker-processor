"""Durable audit for registered utility scripts; never creates certifications."""

import psycopg
from psycopg.types.json import Jsonb

from ingestion.contracts.standalone import UtilityContext, UtilityResult


class PostgresStandaloneRunRepository:
    def __init__(self, dsn: str):
        if not dsn:
            raise ValueError("control database DSN is required")
        self._dsn = dsn

    def start(self, context: UtilityContext) -> None:
        with psycopg.connect(self._dsn) as conn:
            conn.execute(
                "INSERT INTO ingestion.standalone_script_runs "
                "(run_id, script_id, config_hash, target, status) "
                "VALUES (%s, %s, %s, %s, 'running')",
                (context.run_id, context.script_id, context.config_hash,
                 context.target.value if context.target else None),
            )

    def finish(self, run_id: str, *, status: str, error_class: str | None = None,
               result: UtilityResult | None = None) -> None:
        if status not in {"completed", "rejected", "failed"}:
            raise ValueError("invalid standalone run status")
        with psycopg.connect(self._dsn) as conn:
            row = conn.execute(
                "UPDATE ingestion.standalone_script_runs "
                "SET status = %s, error_class = %s, result_json = %s, finished_at = now() "
                "WHERE run_id = %s AND status = 'running' RETURNING run_id",
                (status, error_class, Jsonb(result.model_dump(mode="json")) if result else None,
                 run_id),
            ).fetchone()
            if row is None:
                raise ValueError("standalone run is not active")
