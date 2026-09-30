"""Shared plumbing for the reviewed live SkySpark reader scripts.

A reader script receives one job context on stdin, reads SkySpark, writes raw
and certified-candidate evidence to S3, and prints a ``JobCompletion``. The
worker then verifies that physical evidence before certification.
"""

from __future__ import annotations

import json
import os
import sys
from collections.abc import Callable, Iterable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ingestion.adapters.aws.s3_evidence import S3JsonlSink, S3ObjectStore, S3RawStore
from ingestion.adapters.skyspark.live import (
    LiveClient, credentials_from_env, open_live_client, require_observed,
)
from ingestion.adapters.skyspark.replica import ReplicaShape
from ingestion.contracts.config import FeedKind, TargetKind
from ingestion.contracts.jobs import Job, JobCompletion
from ingestion.contracts.resources import ResourceConfig, SourceClientPolicy, load_resources
from ingestion.core.failures import (
    CertifiedBatchTooLarge, NonRetryableJobError, RawResponseTooLarge,
)


@dataclass(frozen=True, slots=True)
class ReaderRequest:
    job: Job
    source: Mapping[str, Any]
    resources: ResourceConfig
    policy: SourceClientPolicy
    replica: ReplicaShape | None

    @property
    def site_uri(self) -> str:
        return self.source["site_uri"]


def load_request(payload: Mapping[str, Any], feed: FeedKind, environ: Mapping[str, str]) -> ReaderRequest:
    job = Job.model_validate(payload["job"])
    if job.feed != feed:
        raise ValueError(f"this reader accepts only {feed} jobs")
    if payload["target"] != TargetKind.S3.value:
        raise ValueError("live readers write certified candidates to S3 only")
    resources = load_resources(Path(environ["RESOURCE_CONFIG_PATH"]))
    source = payload["source"]
    return ReaderRequest(
        job=job, source=source, resources=resources,
        policy=require_observed(resources.source_client),
        replica=ReplicaShape.from_source(source.get("replica")),
    )


@contextmanager
def live_client(request: ReaderRequest, environ: Mapping[str, str]) -> Iterator[LiveClient]:
    username, password = credentials_from_env(environ, request.policy)
    with open_live_client(request.source["endpoint"], username, password) as client:
        yield client


def write_evidence(
    request: ReaderRequest, *, raw_response: bytes, query_id: str,
    completed_scope: tuple[str, ...], rows: Iterable[dict[str, object]], row_count: int,
    environ: Mapping[str, str],
) -> JobCompletion:
    resources = request.resources
    objects = S3ObjectStore(region_name=resources.region, endpoint_url=environ.get("AWS_ENDPOINT_URL"))
    raw = S3RawStore(objects, resources.storage).put(
        request.job, (raw_response,), query_id=query_id,
        completed_scope=completed_scope, row_count=row_count,
    )
    sink = S3JsonlSink(objects, resources.storage).put(request.job, rows)
    return JobCompletion(raw=raw, sink=sink, completed_scope=completed_scope)


def run(feed: FeedKind, read: Callable[[ReaderRequest, LiveClient], JobCompletion],
        *, split_on_size: bool = False) -> None:
    """Script entrypoint: exit 65 quarantines, 75 asks history for a smaller split."""
    try:
        request = load_request(json.load(sys.stdin), feed, os.environ)
        with live_client(request, os.environ) as client:
            completion = read(request, client)
        sys.stdout.write(completion.model_dump_json() + "\n")
    except (RawResponseTooLarge, CertifiedBatchTooLarge, TimeoutError):
        if split_on_size:
            raise SystemExit(75) from None
        raise SystemExit(65) from None
    except NonRetryableJobError:
        raise SystemExit(65) from None
