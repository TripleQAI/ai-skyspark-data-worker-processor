"""Read a stored, scoped pipeline version for shared queue workers."""

from __future__ import annotations

import re

import psycopg
from psycopg.rows import dict_row

from ingestion.config.loader import EffectiveConfig, resolve_config_documents


class PostgresConfigRegistry:
    def __init__(self, dsn: str, *, environment: str):
        if not dsn or environment not in {"local", "aws"}:
            raise ValueError("a control DSN and local/aws environment are required")
        self._dsn = dsn
        self._environment = environment

    def load(self, *, config_hash: str, tenant_id: str, project_id: str) -> EffectiveConfig:
        if not re.fullmatch(r"[0-9a-f]{64}", config_hash):
            raise ValueError("job config_hash is invalid")
        with psycopg.connect(self._dsn, row_factory=dict_row) as conn:
            conn.execute("SET TRANSACTION READ ONLY")
            row = conn.execute(
                """
                SELECT profile_json, binding_json, manifest_json
                FROM ingestion.pipeline_versions
                WHERE config_hash = %s AND tenant_id = %s AND project_id = %s
                """,
                (config_hash, tenant_id, project_id),
            ).fetchone()
        if row is None:
            raise ValueError("reviewed configuration is missing from job scope")
        config = resolve_config_documents(
            row["profile_json"], row["binding_json"], row["manifest_json"],
            environment=self._environment,
        )
        if config.config_hash != config_hash:
            raise ValueError("stored configuration hash differs from job")
        return config
