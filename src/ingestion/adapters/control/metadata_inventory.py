"""Publish a planning inventory only from a complete, certified metadata run."""

from __future__ import annotations

import hashlib
import json
import re
from typing import Any

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from ingestion.adapters.aws.inventory import S3VersionedInventoryStore
from ingestion.adapters.aws.s3_evidence import S3EvidenceVerifier, S3ObjectStore
from ingestion.adapters.db.timescale import TimescaleEvidenceVerifier
from ingestion.adapters.control.inventory import InventoryRecord
from ingestion.adapters.skyspark.metadata import _historized, _reference
from ingestion.config.loader import EffectiveConfig
from ingestion.config.versioned_s3 import _unique_object
from ingestion.contracts.jobs import (
    CertifiedInventory, Job, JobCompletion, RawArtifact, SinkReceipt, SiteInventory,
)
from ingestion.contracts.resources import ResourceConfig


class IncompleteMetadataRun(ValueError):
    """The previous inventory remains active until this run is fully proven."""


def _fingerprint(value: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(
        value, sort_keys=True, separators=(",", ":"), allow_nan=False,
    ).encode()).hexdigest()


def _record(row: dict[str, Any]) -> InventoryRecord:
    return InventoryRecord(
        tenant_id=row["tenant_id"], project_id=row["project_id"],
        version=row["inventory_version"], source_run_id=row["source_run_id"],
        object_ref=row["object_ref"], object_sha256=row["object_sha256"],
        byte_count=row["byte_count"], certified_at=row["certified_at"],
    )


class PostgresMetadataInventoryPublisher:
    def __init__(
        self, dsn: str, *, objects: S3ObjectStore,
        inventory_store: S3VersionedInventoryStore, resources: ResourceConfig,
        target_dsn: str | None = None,
    ):
        if not dsn:
            raise ValueError("control database DSN is required")
        self._dsn = dsn
        self._objects = objects
        self._inventory = inventory_store
        self._resources = resources
        self._verifier = S3EvidenceVerifier(objects, resources.storage)
        self._target_dsn = target_dsn

    def _connect(self) -> psycopg.Connection:
        return psycopg.connect(self._dsn, row_factory=dict_row)

    @staticmethod
    def _existing(conn: psycopg.Connection, run_id: str) -> InventoryRecord | None:
        row = conn.execute(
            "SELECT * FROM ingestion.inventory_versions WHERE source_run_id = %s",
            (run_id,),
        ).fetchone()
        return _record(row) if row else None

    @staticmethod
    def _run_and_jobs(
        conn: psycopg.Connection, run_id: str, config: EffectiveConfig,
        *, lock_run: bool = False,
    ) -> list[dict[str, Any]]:
        run = conn.execute(
            "SELECT * FROM ingestion.runs WHERE run_id = %s" + (" FOR UPDATE" if lock_run else ""),
            (run_id,),
        ).fetchone()
        binding = config.binding
        if (run is None or run["feed"] != "metadata" or run["status"] != "certified"
                or (run["tenant_id"], run["project_id"], run["config_hash"])
                != (binding.tenant_id, binding.project_id, config.config_hash)):
            raise IncompleteMetadataRun("metadata run is not certified for this binding")
        rows = conn.execute(
            """
            SELECT jobs.*, cert.completed_scope, raw.object_key, raw.checksum AS raw_checksum,
                   raw.byte_count AS raw_byte_count, sink.batch_key,
                   sink.sink_kind, sink.row_count, sink.checksum AS sink_checksum
            FROM ingestion.jobs AS jobs
            LEFT JOIN ingestion.certifications AS cert ON cert.job_id = jobs.job_id
            LEFT JOIN ingestion.raw_artifacts AS raw ON raw.artifact_id = cert.artifact_id
            LEFT JOIN ingestion.sink_receipts AS sink ON sink.batch_key = cert.batch_key
            WHERE jobs.run_id = %s ORDER BY jobs.site_ref, jobs.job_id
            """,
            (run_id,),
        ).fetchall()
        if len(rows) != run["expected_job_count"] or not rows:
            raise IncompleteMetadataRun("metadata run job count is incomplete")
        sites = set(binding.approved_sites)
        if {row["site_ref"] for row in rows} != sites:
            raise IncompleteMetadataRun("metadata run does not cover every approved site")
        if any(
            row["status"] != "certified"
            or row["sink_kind"] != config.profile.feeds[row["feed"]].target.value
            or row["object_key"] is None or row["batch_key"] is None
            or row["completed_scope"] != [row["site_ref"]]
            or row["config_hash"] != config.config_hash
            for row in rows
        ):
            raise IncompleteMetadataRun("metadata job lacks matching target certification")
        completed = conn.execute(
            "SELECT site_ref, job_count FROM ingestion.site_run_completions WHERE run_id = %s",
            (run_id,),
        ).fetchall()
        if {row["site_ref"] for row in completed} != sites or any(
            row["job_count"] != sum(job["site_ref"] == row["site_ref"] for job in rows)
            for row in completed
        ):
            raise IncompleteMetadataRun("metadata site completion receipt is missing")
        return rows

    def _load_entities(
        self, rows: list[dict[str, Any]], config: EffectiveConfig,
    ) -> tuple[dict[tuple[str, str], dict[str, Any]], str]:
        binding = config.binding
        entities: dict[tuple[str, str], dict[str, Any]] = {}
        source_snapshot: str | None = None
        for row in rows:
            job = Job.model_validate({
                key: row[key] for key in (
                    "job_id", "run_id", "tenant_id", "project_id", "site_ref",
                    "feed", "scope_ids", "window_start", "window_end",
                    "config_hash", "inventory_version",
                )
            })
            completion = JobCompletion(
                raw=RawArtifact(
                    job_id=job.job_id, object_key=row["object_key"],
                    checksum=row["raw_checksum"], byte_count=row["raw_byte_count"],
                ),
                sink=SinkReceipt(
                    job_id=job.job_id, sink_kind=row["sink_kind"], batch_key=row["batch_key"],
                    row_count=row["row_count"], checksum=row["sink_checksum"],
                ),
                completed_scope=(job.site_ref,),
            )
            if row["sink_kind"] == "s3":
                self._verifier.verify(job, completion)
            elif row["sink_kind"] == "timescale" and self._target_dsn:
                TimescaleEvidenceVerifier(
                    self._target_dsn, self._verifier, self._resources.storage,
                ).verify(job, completion)
            else:
                raise IncompleteMetadataRun("metadata target has no physical verifier")
            manifest = self._objects.load_manifest(
                self._resources.storage.raw_bucket, completion.raw.object_key,
            )
            query_id = manifest["query_id"]
            suffix = f"/{job.site_ref}"
            if (not isinstance(query_id, str) or not query_id.startswith("metadata/")
                    or not query_id.endswith(suffix)):
                raise IncompleteMetadataRun("metadata raw receipt has no source snapshot")
            token = query_id[len("metadata/"):-len(suffix)]
            if not re.fullmatch(r"[A-Za-z0-9_.-]+", token):
                raise IncompleteMetadataRun("metadata source snapshot token is invalid")
            if source_snapshot is None:
                source_snapshot = token
            elif source_snapshot != token:
                raise IncompleteMetadataRun("metadata sites came from different source snapshots")
            if row["sink_kind"] == "s3":
                raw = self._objects.read_bounded(
                    self._resources.storage.certified_bucket, completion.sink.batch_key,
                    max_bytes=self._resources.storage.max_certified_bytes,
                )
                values = [json.loads(line, object_pairs_hook=_unique_object)
                          for line in raw.splitlines()]
            else:
                with psycopg.connect(self._target_dsn) as target_conn:
                    values = [item[0] for item in target_conn.execute(
                        "SELECT payload FROM ingestion_target.metadata_batch_entities "
                        "WHERE batch_key = %s ORDER BY position",
                        (completion.sink.batch_key,),
                    ).fetchall()]
            if len(values) != completion.sink.row_count:
                raise IncompleteMetadataRun("metadata target row count differs")
            for value in values:
                item = self._validate_entity(value, job, binding.approved_sites[job.site_ref])
                key = item["kind"], item["source_id"]
                if key in entities:
                    raise IncompleteMetadataRun("metadata source ID appears in multiple partitions")
                entities[key] = item
        equipment_site = {
            source_id: item["site_ref"] for (kind, source_id), item in entities.items()
            if kind == "equipment"
        }
        for item in entities.values():
            if item["kind"] == "point" and item["equipment_ref"] is not None:
                if equipment_site.get(item["equipment_ref"]) != item["site_ref"]:
                    raise IncompleteMetadataRun("point link crosses or misses an equipment")
        return entities, source_snapshot or ""

    @staticmethod
    def _validate_entity(value: Any, job: Job, source_site_uri: str) -> dict[str, Any]:
        if not isinstance(value, dict) or set(value) not in (
            {"kind", "source_id", "site_ref", "equipment_ref", "historized", "tags"},
            {"kind", "source_id", "site_ref", "equipment_ref", "site_level", "historized", "tags"},
        ):
            raise IncompleteMetadataRun("metadata target has an invalid entity shape")
        kind = value["kind"]
        if (kind not in {"equipment", "point"} or value["site_ref"] != job.site_ref
                or not isinstance(value["source_id"], str) or not value["source_id"]
                or type(value["historized"]) is not bool or not isinstance(value["tags"], dict)
                or _reference(value["tags"].get("id")) != value["source_id"]
                or _reference(value["tags"].get("siteRef")) != source_site_uri):
            raise IncompleteMetadataRun("metadata target entity differs from source scope")
        if kind == "equipment":
            if (value["equipment_ref"] is not None or value["historized"]
                    or "site_level" in value):
                raise IncompleteMetadataRun("equipment target has point-only fields")
        else:
            parent = value["equipment_ref"]
            if (parent is not None and (not isinstance(parent, str) or not parent)
                    or value.get("site_level") is not (parent is None)
                    or value["historized"] is not _historized(value["tags"].get("his"))):
                raise IncompleteMetadataRun("point target has inconsistent links or his tag")
        return value

    def publish(self, *, run_id: str, config: EffectiveConfig) -> InventoryRecord:
        """Reject partial/shrinking runs; atomically register a version and changes."""
        with self._connect() as conn:
            existing = self._existing(conn, run_id)
            if existing:
                return existing
            staged = self._run_and_jobs(conn, run_id, config)
        entities, source_snapshot = self._load_entities(staged, config)
        binding = config.binding
        tenant_id, project_id = binding.tenant_id, binding.project_id
        with self._connect() as conn:
            conn.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                (f"metadata-inventory:{tenant_id}:{project_id}",),
            )
            existing = self._existing(conn, run_id)
            if existing:
                return existing
            current = self._run_and_jobs(conn, run_id, config, lock_run=True)
            if [(row["job_id"], row["batch_key"], row["raw_checksum"]) for row in current] != [
                (row["job_id"], row["batch_key"], row["raw_checksum"]) for row in staged
            ]:
                raise IncompleteMetadataRun("metadata certification changed during publication")
            later = conn.execute(
                """
                SELECT 1 FROM ingestion.inventory_versions AS versions
                JOIN ingestion.runs AS runs ON runs.run_id = versions.source_run_id
                WHERE versions.tenant_id = %s AND versions.project_id = %s
                  AND runs.scheduled_at > (SELECT scheduled_at FROM ingestion.runs WHERE run_id = %s)
                LIMIT 1
                """,
                (tenant_id, project_id, run_id),
            ).fetchone()
            if later:
                raise IncompleteMetadataRun("a newer metadata run is already published")
            previous = conn.execute(
                """
                SELECT kind, source_id, row_sha256 FROM ingestion.inventory_entities
                WHERE tenant_id = %s AND project_id = %s
                """,
                (tenant_id, project_id),
            ).fetchall()
            prior = {(row["kind"], row["source_id"]): row["row_sha256"] for row in previous}
            if set(prior) - set(entities):
                raise IncompleteMetadataRun("metadata would shrink certified inventory without deletion evidence")
            sites: dict[str, SiteInventory] = {}
            for site_ref in binding.approved_sites:
                equipment_ids = sorted(
                    source_id for (kind, source_id), item in entities.items()
                    if kind == "equipment" and item["site_ref"] == site_ref
                )
                point_ids = sorted(
                    source_id for (kind, source_id), item in entities.items()
                    if kind == "point" and item["site_ref"] == site_ref
                )
                exclusions = set(binding.excluded_history_point_ids_by_site.get(site_ref, ()))
                if exclusions - set(point_ids):
                    raise IncompleteMetadataRun("history exclusion references an unknown point")
                historized = sorted(
                    source_id for (kind, source_id), item in entities.items()
                    if kind == "point" and item["site_ref"] == site_ref
                    and item["historized"] and source_id not in exclusions
                )
                sites[site_ref] = SiteInventory(
                    equipment_ids=tuple(equipment_ids), point_ids=tuple(point_ids),
                    historized_point_ids=tuple(historized),
                )
            content_hash = _fingerprint({
                "entities": [
                    [kind, source_id, _fingerprint(item)]
                    for (kind, source_id), item in sorted(entities.items())
                ],
                "exclusions": binding.excluded_history_point_ids_by_site,
            })
            version = hashlib.sha256(
                f"{run_id}:{source_snapshot}:{content_hash}".encode()
            ).hexdigest()
            inventory = CertifiedInventory(
                version=version, tenant_id=tenant_id, project_id=project_id, sites=sites,
            )
            stored = self._inventory.put(
                inventory, source_run_id=run_id,
                bucket=self._resources.storage.certified_bucket,
            )
            recorded = conn.execute(
                """
                INSERT INTO ingestion.inventory_versions
                    (tenant_id, project_id, inventory_version, source_run_id,
                     object_ref, object_sha256, byte_count, certified_at,
                     source_snapshot_token)
                VALUES (%s, %s, %s, %s, %s, %s, %s, now(), %s)
                RETURNING *
                """,
                (tenant_id, project_id, version, run_id, stored.object_ref,
                 stored.sha256, stored.byte_count, source_snapshot),
            ).fetchone()
            for (kind, source_id), item in sorted(entities.items()):
                checksum = _fingerprint(item)
                prior_checksum = prior.get((kind, source_id))
                if prior_checksum != checksum:
                    conn.execute(
                        """
                        INSERT INTO ingestion.inventory_entity_changes
                            (source_run_id, tenant_id, project_id, kind, source_id,
                             change_kind, previous_sha256, current_sha256)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (run_id, tenant_id, project_id, kind, source_id,
                         "added" if prior_checksum is None else "changed",
                         prior_checksum, checksum),
                    )
                conn.execute(
                    """
                    INSERT INTO ingestion.inventory_entities
                        (tenant_id, project_id, kind, source_id, site_ref,
                         equipment_ref, historized, tags, row_sha256, last_seen_run_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (tenant_id, project_id, kind, source_id)
                    DO UPDATE SET site_ref = EXCLUDED.site_ref,
                                  equipment_ref = EXCLUDED.equipment_ref,
                                  historized = EXCLUDED.historized,
                                  tags = EXCLUDED.tags,
                                  row_sha256 = EXCLUDED.row_sha256,
                                  last_seen_run_id = EXCLUDED.last_seen_run_id,
                                  updated_at = now()
                    """,
                    (tenant_id, project_id, kind, source_id, item["site_ref"],
                     item["equipment_ref"], item["historized"], Jsonb(item["tags"]),
                     checksum, run_id),
                )
        return _record(recorded)
