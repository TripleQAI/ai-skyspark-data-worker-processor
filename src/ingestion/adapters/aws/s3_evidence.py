"""Bounded immutable S3 evidence and physical receipt verification."""

from __future__ import annotations

import base64
import hashlib
import io
import json
import re
import tempfile
from collections.abc import Iterable
from typing import Any

import boto3
from botocore.exceptions import ClientError

from ingestion.contracts.jobs import Job, JobCompletion, RawArtifact, SinkReceipt
from ingestion.contracts.resources import StoragePolicy
from ingestion.core.failures import (
    CertifiedBatchTooLarge, NonRetryableJobError, NonRetryableSourceError,
    RawResponseTooLarge,
)


class EvidenceMismatch(NonRetryableJobError):
    """A physical object differs from its reviewed job receipt."""


def _canonical(value: dict[str, object]) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")


def _scope(job: Job) -> tuple[str, ...]:
    return (job.site_ref,) if job.feed.value == "metadata" else job.scope_ids


def _job_prefix(job: Job) -> str:
    for identifier in (job.run_id, job.job_id, job.config_hash):
        if not re.fullmatch(r"[0-9a-f]{64}", identifier):
            raise ValueError("job identifiers must be deterministic SHA-256 values")
    for value in (job.tenant_id, job.project_id):
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]*", value):
            raise ValueError("tenant/project identifiers are invalid for object keys")
    return f"{job.tenant_id}/{job.project_id}/{job.feed.value}/{job.run_id}/{job.job_id}"


class S3ObjectStore:
    def __init__(
        self, *, region_name: str, endpoint_url: str | None = None,
        client: Any | None = None,
    ):
        self._client = client or boto3.client(
            "s3", region_name=region_name, endpoint_url=endpoint_url
        )

    def put_immutable(
        self, bucket: str, key: str, body: io.BufferedIOBase,
        *, byte_count: int, checksum: str, content_type: str,
    ) -> None:
        body.seek(0)
        checksum_b64 = base64.b64encode(bytes.fromhex(checksum)).decode("ascii")
        try:
            self._client.put_object(
                Bucket=bucket, Key=key, Body=body,
                ContentLength=byte_count, ContentType=content_type,
                ChecksumSHA256=checksum_b64, IfNoneMatch="*",
            )
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code")
            if code not in {"412", "PreconditionFailed", "ConditionalRequestConflict"}:
                raise
            self.verify_object(
                bucket, key, expected_checksum=checksum,
                expected_bytes=byte_count, max_bytes=byte_count,
            )

    def put_manifest(self, bucket: str, key: str, manifest: dict[str, object]) -> None:
        data = _canonical(manifest)
        self.put_immutable(
            bucket, f"{key}.manifest.json", io.BytesIO(data),
            byte_count=len(data), checksum=hashlib.sha256(data).hexdigest(),
            content_type="application/json",
        )

    def load_manifest(self, bucket: str, key: str) -> dict[str, object]:
        response = self._client.get_object(Bucket=bucket, Key=f"{key}.manifest.json")
        body = response["Body"]
        try:
            data = body.read(65_537)
            if len(data) > 65_536:
                raise EvidenceMismatch("manifest exceeds bounded size")
            parsed = json.loads(data)
        finally:
            body.close()
        if not isinstance(parsed, dict):
            raise EvidenceMismatch("manifest is not a JSON object")
        return parsed

    def read_bounded(self, bucket: str, key: str, *, max_bytes: int) -> bytes:
        if max_bytes < 0:
            raise ValueError("read cap must be nonnegative")
        response = self._client.get_object(Bucket=bucket, Key=key)
        if response.get("ContentLength", 0) > max_bytes:
            raise EvidenceMismatch("physical object exceeds read cap")
        body = response["Body"]
        try:
            data = body.read(max_bytes + 1)
        finally:
            body.close()
        if len(data) > max_bytes:
            raise EvidenceMismatch("physical object exceeds read cap")
        return data

    def verify_object(
        self, bucket: str, key: str, *, expected_checksum: str,
        expected_bytes: int, max_bytes: int, count_lines: bool = False,
    ) -> int:
        if expected_bytes < 0 or expected_bytes > max_bytes:
            raise EvidenceMismatch("receipt byte count exceeds verification limit")
        response = self._client.get_object(Bucket=bucket, Key=key)
        body = response["Body"]
        digest = hashlib.sha256()
        byte_count = line_count = 0
        final_byte = b""
        try:
            while chunk := body.read(65_536):
                byte_count += len(chunk)
                if byte_count > max_bytes:
                    raise EvidenceMismatch("physical object exceeds verification limit")
                digest.update(chunk)
                if count_lines:
                    line_count += chunk.count(b"\n")
                    final_byte = chunk[-1:]
        finally:
            body.close()
        if byte_count != expected_bytes or digest.hexdigest() != expected_checksum:
            raise EvidenceMismatch("physical object checksum or length differs")
        if count_lines and byte_count and final_byte != b"\n":
            raise EvidenceMismatch("JSONL object has an unterminated final row")
        return line_count


class S3RawStore:
    def __init__(self, objects: S3ObjectStore, policy: StoragePolicy):
        self._objects = objects
        self._policy = policy

    def put(
        self, job: Job, chunks: Iterable[bytes], *, query_id: str,
        completed_scope: tuple[str, ...], row_count: int,
        truncated: bool = False,
    ) -> RawArtifact:
        if not query_id or row_count < 0 or truncated:
            raise NonRetryableSourceError("raw receipt requires a complete query and nonnegative rows")
        expected = _scope(job)
        if len(completed_scope) != len(set(completed_scope)) or set(completed_scope) != set(expected):
            raise NonRetryableSourceError("raw source receipt does not cover requested scope")
        digest = hashlib.sha256()
        byte_count = 0
        with tempfile.SpooledTemporaryFile(max_size=1_048_576) as spool:
            for chunk in chunks:
                if not isinstance(chunk, bytes):
                    raise TypeError("raw response chunks must be bytes")
                byte_count += len(chunk)
                if byte_count > self._policy.max_raw_bytes:
                    raise RawResponseTooLarge("raw response exceeds configured byte cap")
                digest.update(chunk)
                spool.write(chunk)
            checksum = digest.hexdigest()
            key = f"raw/{_job_prefix(job)}/{checksum}.bin"
            self._objects.put_immutable(
                self._policy.raw_bucket, key, spool,
                byte_count=byte_count, checksum=checksum,
                content_type="application/octet-stream",
            )
        self._objects.put_manifest(self._policy.raw_bucket, key, {
            "schema_version": 1, "job_id": job.job_id, "run_id": job.run_id,
            "config_hash": job.config_hash, "query_id": query_id,
            "requested_scope": list(expected),
            "completed_scope": list(completed_scope),
            "row_count": row_count, "truncated": False,
            "checksum": checksum, "byte_count": byte_count,
        })
        return RawArtifact(
            job_id=job.job_id, object_key=key,
            checksum=checksum, byte_count=byte_count,
        )


class S3JsonlSink:
    """Write bounded candidate rows; feed validators remain separate."""

    def __init__(self, objects: S3ObjectStore, policy: StoragePolicy):
        self._objects = objects
        self._policy = policy

    def put(self, job: Job, rows: Iterable[dict[str, object]]) -> SinkReceipt:
        digest = hashlib.sha256()
        byte_count = row_count = 0
        with tempfile.SpooledTemporaryFile(max_size=1_048_576) as spool:
            for row in rows:
                if not isinstance(row, dict):
                    raise NonRetryableJobError("certified rows must be JSON objects")
                encoded = _canonical(row) + b"\n"
                row_count += 1
                byte_count += len(encoded)
                if (
                    row_count > self._policy.max_certified_rows
                    or byte_count > self._policy.max_certified_bytes
                ):
                    raise CertifiedBatchTooLarge("certified batch exceeds configured cap")
                digest.update(encoded)
                spool.write(encoded)
            checksum = digest.hexdigest()
            key = f"certified/{_job_prefix(job)}/{checksum}.jsonl"
            self._objects.put_immutable(
                self._policy.certified_bucket, key, spool,
                byte_count=byte_count, checksum=checksum,
                content_type="application/x-ndjson",
            )
        self._objects.put_manifest(self._policy.certified_bucket, key, {
            "schema_version": 1, "job_id": job.job_id, "run_id": job.run_id,
            "config_hash": job.config_hash, "checksum": checksum,
            "byte_count": byte_count, "row_count": row_count,
        })
        return SinkReceipt(
            job_id=job.job_id, sink_kind="s3", batch_key=key,
            row_count=row_count, checksum=checksum,
        )


class S3EvidenceVerifier:
    def __init__(self, objects: S3ObjectStore, policy: StoragePolicy):
        self._objects = objects
        self._policy = policy

    def verify_raw(self, job: Job, completion: JobCompletion) -> None:
        if completion.raw.job_id != job.job_id:
            raise EvidenceMismatch("evidence belongs to another job")
        expected_prefix = _job_prefix(job)
        if not completion.raw.object_key.startswith(f"raw/{expected_prefix}/"):
            raise EvidenceMismatch("raw object is outside job prefix")
        raw_manifest = self._objects.load_manifest(
            self._policy.raw_bucket, completion.raw.object_key
        )
        if raw_manifest != {
            "schema_version": 1, "job_id": job.job_id, "run_id": job.run_id,
            "config_hash": job.config_hash, "query_id": raw_manifest.get("query_id"),
            "requested_scope": list(_scope(job)),
            "completed_scope": list(completion.completed_scope),
            "row_count": raw_manifest.get("row_count"), "truncated": False,
            "checksum": completion.raw.checksum,
            "byte_count": completion.raw.byte_count,
        } or not raw_manifest.get("query_id") or type(raw_manifest.get("row_count")) is not int or raw_manifest["row_count"] < 0:
            raise EvidenceMismatch("raw manifest differs from job receipt")
        self._objects.verify_object(
            self._policy.raw_bucket, completion.raw.object_key,
            expected_checksum=completion.raw.checksum,
            expected_bytes=completion.raw.byte_count,
            max_bytes=self._policy.max_raw_bytes,
        )

    def verify(self, job: Job, completion: JobCompletion) -> None:
        if completion.sink.sink_kind != "s3":
            raise EvidenceMismatch("this verifier only accepts an S3 target")
        if completion.sink.job_id != job.job_id:
            raise EvidenceMismatch("target evidence belongs to another job")
        self.verify_raw(job, completion)
        expected_prefix = _job_prefix(job)
        if not completion.sink.batch_key.startswith(f"certified/{expected_prefix}/"):
            raise EvidenceMismatch("target object is outside job prefix")
        sink_manifest = self._objects.load_manifest(
            self._policy.certified_bucket, completion.sink.batch_key
        )
        if sink_manifest.get("schema_version") != 1 or sink_manifest.get("job_id") != job.job_id or (
            sink_manifest.get("run_id") != job.run_id
            or sink_manifest.get("config_hash") != job.config_hash
            or sink_manifest.get("checksum") != completion.sink.checksum
            or sink_manifest.get("row_count") != completion.sink.row_count
            or type(sink_manifest.get("byte_count")) is not int
            or sink_manifest["byte_count"] < 0
        ):
            raise EvidenceMismatch("target manifest differs from job receipt")
        rows = self._objects.verify_object(
            self._policy.certified_bucket, completion.sink.batch_key,
            expected_checksum=completion.sink.checksum,
            expected_bytes=sink_manifest["byte_count"],
            max_bytes=self._policy.max_certified_bytes, count_lines=True,
        )
        if rows != completion.sink.row_count:
            raise EvidenceMismatch("target row count differs from physical object")
