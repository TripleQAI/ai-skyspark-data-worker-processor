"""Verify a pinned inventory snapshot before it can drive job planning."""

from __future__ import annotations

import hashlib
import base64
import io
import json
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote

import boto3
from botocore.exceptions import ClientError

from ingestion.adapters.control.inventory import InventoryRecord
from ingestion.config.loader import EffectiveConfig
from ingestion.config.versioned_s3 import _unique_object, parse_versioned_s3_ref
from ingestion.contracts.jobs import CertifiedInventory
from ingestion.contracts.resources import InventoryArtifactPolicy


@dataclass(frozen=True, slots=True)
class StoredInventoryObject:
    object_ref: str
    sha256: str
    byte_count: int


class S3VersionedInventoryStore:
    def __init__(
        self, *, region_name: str, policy: InventoryArtifactPolicy,
        endpoint_url: str | None = None, client: Any | None = None,
    ):
        self._policy = policy
        self._client = client or boto3.client(
            "s3", region_name=region_name, endpoint_url=endpoint_url
        )

    def put(
        self, inventory: CertifiedInventory, *, source_run_id: str, bucket: str,
    ) -> StoredInventoryObject:
        """Write an exact-version, content-addressed planning snapshot."""
        if not bucket or len(source_run_id) != 64 or any(
            character not in "0123456789abcdef" for character in source_run_id
        ):
            raise ValueError("inventory destination or source run is invalid")
        raw = json.dumps(
            inventory.model_dump(mode="json"), sort_keys=True,
            separators=(",", ":"), allow_nan=False,
        ).encode("utf-8")
        if len(raw) > self._policy.max_snapshot_bytes:
            raise ValueError("inventory snapshot exceeds configured byte cap")
        digest = hashlib.sha256(raw).hexdigest()
        key = (
            f"inventory/{inventory.tenant_id}/{inventory.project_id}/"
            f"{source_run_id}/{digest}.json"
        )
        try:
            response = self._client.put_object(
                Bucket=bucket, Key=key, Body=io.BytesIO(raw),
                ContentLength=len(raw), ContentType="application/json",
                ChecksumSHA256=base64.b64encode(bytes.fromhex(digest)).decode(),
                IfNoneMatch="*",
            )
            version_id = response.get("VersionId")
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") not in {
                "412", "PreconditionFailed", "ConditionalRequestConflict",
            }:
                raise
            head = self._client.head_object(Bucket=bucket, Key=key)
            version_id = head.get("VersionId")
            body = self._client.get_object(Bucket=bucket, Key=key)["Body"]
            try:
                existing = body.read(self._policy.max_snapshot_bytes + 1)
            finally:
                body.close()
            if existing != raw:
                raise ValueError("existing inventory object differs from planned snapshot")
        if not version_id or version_id == "null":
            raise ValueError("inventory bucket must have S3 versioning enabled")
        return StoredInventoryObject(
            object_ref=f"s3://{bucket}/{key}?versionId={quote(version_id, safe='')}",
            sha256=digest, byte_count=len(raw),
        )

    def load(
        self, record: InventoryRecord, *, config: EffectiveConfig
    ) -> CertifiedInventory:
        if (record.tenant_id, record.project_id) != (
            config.binding.tenant_id, config.binding.project_id
        ):
            raise ValueError("inventory record is outside approved project")
        if record.byte_count < 1 or record.byte_count > self._policy.max_snapshot_bytes:
            raise ValueError("inventory receipt exceeds configured byte limit")
        ref = parse_versioned_s3_ref(record.object_ref)
        response = self._client.get_object(
            Bucket=ref.bucket, Key=ref.key, VersionId=ref.version_id
        )
        if response.get("VersionId") != ref.version_id:
            raise ValueError("S3 returned a different inventory version")
        if response.get("ContentLength", 0) != record.byte_count:
            raise ValueError("inventory receipt length differs")
        body = response["Body"]
        try:
            raw = body.read(self._policy.max_snapshot_bytes + 1)
        finally:
            body.close()
        if len(raw) != record.byte_count or hashlib.sha256(raw).hexdigest() != record.object_sha256:
            raise ValueError("inventory snapshot checksum or length differs")
        data = json.loads(raw.decode("utf-8"), object_pairs_hook=_unique_object)
        inventory = CertifiedInventory.model_validate(data)
        if (
            inventory.version != record.version
            or inventory.tenant_id != record.tenant_id
            or inventory.project_id != record.project_id
        ):
            raise ValueError("inventory snapshot identity differs from registry")
        if set(inventory.sites) != set(config.binding.approved_sites):
            raise ValueError("inventory snapshot site set differs from approved binding")
        return inventory
