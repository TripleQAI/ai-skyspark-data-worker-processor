"""Load an exact reviewed configuration version from S3."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any
from urllib.parse import parse_qs, unquote, urlsplit

import boto3

from ingestion.config.loader import EffectiveConfig, resolve_config_documents


@dataclass(frozen=True, slots=True)
class S3VersionRef:
    bucket: str
    key: str
    version_id: str


def parse_versioned_s3_ref(value: str) -> S3VersionRef:
    parsed = urlsplit(value)
    query = parse_qs(parsed.query, keep_blank_values=True, strict_parsing=True)
    if (
        parsed.scheme != "s3" or not re.fullmatch(r"[a-z0-9][a-z0-9.-]{2,62}", parsed.netloc)
        or not parsed.path.startswith("/")
        or not parsed.path[1:] or parsed.fragment or set(query) != {"versionId"}
        or len(query["versionId"]) != 1 or not query["versionId"][0]
        or query["versionId"][0] == "null"
    ):
        raise ValueError("configuration reference must pin one non-null S3 version")
    key = unquote(parsed.path[1:])
    if key.startswith("/") or "\\" in key or any(part in ("", ".", "..") for part in key.split("/")):
        raise ValueError("configuration object key is invalid")
    return S3VersionRef(parsed.netloc, key, query["versionId"][0])


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate bundle key: {key}")
        result[key] = value
    return result


class S3VersionedConfigStore:
    def __init__(
        self, *, region_name: str, max_bytes: int,
        endpoint_url: str | None = None, client: Any | None = None,
    ) -> None:
        if max_bytes < 1:
            raise ValueError("max_bytes must be positive")
        self._max_bytes = max_bytes
        self._client = client or boto3.client(
            "s3", region_name=region_name, endpoint_url=endpoint_url
        )

    def load(self, ref: str, *, environment: str) -> EffectiveConfig:
        parsed = parse_versioned_s3_ref(ref)
        response = self._client.get_object(
            Bucket=parsed.bucket, Key=parsed.key, VersionId=parsed.version_id
        )
        if response.get("VersionId") != parsed.version_id:
            raise ValueError("S3 returned a different configuration version")
        if response.get("ContentLength", 0) > self._max_bytes:
            raise ValueError("configuration bundle exceeds byte limit")
        body = response["Body"]
        try:
            raw = body.read(self._max_bytes + 1)
        finally:
            body.close()
        if len(raw) > self._max_bytes:
            raise ValueError("configuration bundle exceeds byte limit")
        bundle = json.loads(raw.decode("utf-8"), object_pairs_hook=_unique_object)
        if not isinstance(bundle, dict) or set(bundle) != {
            "schema_version", "profile", "binding", "manifest"
        } or bundle["schema_version"] != 1:
            raise ValueError("unsupported configuration bundle shape")
        return resolve_config_documents(
            bundle["profile"], bundle["binding"], bundle["manifest"],
            environment=environment,
        )
