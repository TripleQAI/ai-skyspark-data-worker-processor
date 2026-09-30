"""PostgreSQL publication claims and fenced delivery acknowledgements."""

from __future__ import annotations

import psycopg
from psycopg.rows import dict_row

from ingestion.contracts.publication import PublicationIntent


class PostgresPublicationRepository:
    def __init__(self, dsn: str):
        if not dsn:
            raise ValueError("control database DSN is required")
        self._dsn = dsn

    def _connect(self) -> psycopg.Connection:
        return psycopg.connect(self._dsn, row_factory=dict_row)

    def claim(
        self, *, owner: str, limit: int, lease_seconds: int,
    ) -> tuple[PublicationIntent, ...]:
        if not owner or limit < 1 or lease_seconds < 1:
            raise ValueError("owner, limit, and lease_seconds must be positive")
        with self._connect() as conn:
            claimed = conn.execute(
                """
                WITH picked AS (
                    SELECT outbox.publication_id
                    FROM ingestion.publication_outbox AS outbox
                    JOIN ingestion.jobs AS jobs ON jobs.job_id = outbox.job_id
                    JOIN ingestion.site_run_completions AS done
                      ON done.run_id = jobs.run_id AND done.site_ref = jobs.site_ref
                    WHERE jobs.status = 'certified'
                      AND ((outbox.state = 'pending' AND outbox.next_attempt_at <= now())
                        OR (outbox.state = 'sending' AND outbox.claim_until <= now()))
                    ORDER BY outbox.created_at, outbox.publication_id
                    LIMIT %s FOR UPDATE OF outbox SKIP LOCKED
                )
                UPDATE ingestion.publication_outbox AS outbox
                SET state = 'sending', claim_owner = %s,
                    claim_until = now() + (%s * interval '1 second'),
                    delivery_attempts = delivery_attempts + 1
                FROM picked WHERE outbox.publication_id = picked.publication_id
                RETURNING outbox.publication_id
                """,
                (limit, owner, lease_seconds),
            ).fetchall()
            intents: list[PublicationIntent] = []
            for item in claimed:
                row = conn.execute(
                    """
                    SELECT outbox.publication_id, outbox.delivery_attempts,
                           jobs.job_id, jobs.run_id, jobs.tenant_id, jobs.project_id,
                           jobs.site_ref, jobs.feed, jobs.config_hash,
                           jobs.inventory_version, jobs.window_start, jobs.window_end,
                           cert.certified_at, raw.object_key AS raw_key,
                           raw.checksum AS raw_checksum,
                           sink.sink_kind, sink.batch_key,
                           sink.checksum AS sink_checksum, sink.row_count
                    FROM ingestion.publication_outbox AS outbox
                    JOIN ingestion.jobs AS jobs ON jobs.job_id = outbox.job_id
                    JOIN ingestion.certifications AS cert ON cert.job_id = jobs.job_id
                    JOIN ingestion.raw_artifacts AS raw ON raw.artifact_id = cert.artifact_id
                    JOIN ingestion.sink_receipts AS sink ON sink.batch_key = cert.batch_key
                    WHERE outbox.publication_id = %s
                    """,
                    (item["publication_id"],),
                ).fetchone()
                if row is None:
                    raise ValueError("publication has incomplete certification evidence")
                intents.append(PublicationIntent(**row))
        return tuple(intents)

    def mark_delivered(
        self, *, publication_id: str, owner: str,
        delivery_attempts: int, event_id: str,
    ) -> bool:
        if not event_id:
            raise ValueError("event_id is required")
        with self._connect() as conn:
            row = conn.execute(
                """
                UPDATE ingestion.publication_outbox
                SET state = 'delivered', claim_owner = NULL, claim_until = NULL,
                    event_id = %s, delivered_at = now(), last_error = NULL
                WHERE publication_id = %s AND state = 'sending'
                  AND claim_owner = %s AND delivery_attempts = %s
                  AND claim_until > now()
                RETURNING publication_id
                """,
                (event_id, publication_id, owner, delivery_attempts),
            ).fetchone()
        return row is not None

    def release(
        self, *, publication_id: str, owner: str,
        delivery_attempts: int, delay_seconds: int, error_class: str,
    ) -> bool:
        if delay_seconds < 0:
            raise ValueError("delay_seconds must be nonnegative")
        with self._connect() as conn:
            row = conn.execute(
                """
                UPDATE ingestion.publication_outbox
                SET state = 'pending', claim_owner = NULL, claim_until = NULL,
                    next_attempt_at = now() + (%s * interval '1 second'),
                    last_error = %s
                WHERE publication_id = %s AND state = 'sending'
                  AND claim_owner = %s AND delivery_attempts = %s
                RETURNING publication_id
                """,
                (delay_seconds, error_class[:120], publication_id,
                 owner, delivery_attempts),
            ).fetchone()
        return row is not None
