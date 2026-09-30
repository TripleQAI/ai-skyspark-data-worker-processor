"""Local-only metadata reader against the explicit completeness fixture."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

from ingestion.adapters.aws.s3_evidence import S3JsonlSink, S3ObjectStore, S3RawStore
from ingestion.adapters.db.timescale_entities import TimescaleMetadataSink
from ingestion.adapters.skyspark.metadata import FixtureMetadataSource, read_site_metadata
from ingestion.contracts.config import FeedKind
from ingestion.contracts.jobs import Job, JobCompletion
from ingestion.contracts.resources import load_resources
from ingestion.core.failures import NonRetryableJobError


def main() -> None:
    request = json.load(sys.stdin)
    job = Job.model_validate(request["job"])
    if job.feed != FeedKind.METADATA or request["target"] not in ("s3", "timescale"):
        raise ValueError("contract fixture accepts only configured metadata jobs")
    resources = load_resources(Path(os.environ["RESOURCE_CONFIG_PATH"]))
    source = request["source"]
    reader = FixtureMetadataSource(
        source["endpoint"], max_page_bytes=resources.metadata_read.max_page_bytes,
        timeout_seconds=resources.metadata_read.request_timeout_seconds,
    )
    result = read_site_metadata(
        job, site_uri=source["site_uri"], source=reader,
        policy=resources.metadata_read,
    )
    objects = S3ObjectStore(
        region_name=resources.region, endpoint_url=os.environ["AWS_ENDPOINT_URL"],
    )
    raw = S3RawStore(objects, resources.storage).put(
        job, (result.raw_response,), query_id=result.query_id,
        completed_scope=(job.site_ref,), row_count=len(result.rows),
    )
    sink = (S3JsonlSink(objects, resources.storage).put(job, result.rows)
            if request["target"] == "s3" else
            TimescaleMetadataSink(os.environ["TARGET_DATABASE_URL"], resources.storage).put(
                job, result.rows))
    sys.stdout.write(JobCompletion(
        raw=raw, sink=sink, completed_scope=(job.site_ref,),
    ).model_dump_json() + "\n")


if __name__ == "__main__":
    try:
        main()
    except NonRetryableJobError:
        raise SystemExit(65) from None
