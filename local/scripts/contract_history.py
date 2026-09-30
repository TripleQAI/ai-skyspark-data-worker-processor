"""Local-only complete-coverage history reader for the contract fixture."""

from __future__ import annotations

import json
import os
from pathlib import Path
import sys

from ingestion.adapters.aws.s3_evidence import S3JsonlSink, S3ObjectStore, S3RawStore
from ingestion.adapters.db.timescale import TimescaleHistorySink
from ingestion.adapters.skyspark.history import FixtureHistorySource, read_history
from ingestion.contracts.config import FeedKind
from ingestion.contracts.jobs import Job, JobCompletion
from ingestion.contracts.resources import load_resources
from ingestion.core.failures import (
    CertifiedBatchTooLarge, NonRetryableJobError, RawResponseTooLarge,
)


def main() -> None:
    request = json.load(sys.stdin)
    job = Job.model_validate(request["job"])
    if job.feed != FeedKind.HISTORY or request["target"] not in ("s3", "timescale"):
        raise ValueError("contract fixture accepts only configured history jobs")
    resources = load_resources(Path(os.environ["RESOURCE_CONFIG_PATH"]))
    policy = resources.history_read
    if policy is None:
        raise ValueError("history read policy is required")
    source = request["source"]
    result = read_history(
        job, site_uri=source["site_uri"],
        source=FixtureHistorySource(
            source["endpoint"], max_response_bytes=policy.max_response_bytes,
            timeout_seconds=policy.request_timeout_seconds,
        ), policy=policy,
    )
    objects = S3ObjectStore(
        region_name=resources.region, endpoint_url=os.environ["AWS_ENDPOINT_URL"],
    )
    raw = S3RawStore(objects, resources.storage).put(
        job, (result.raw_response,), query_id=result.query_id,
        completed_scope=result.completed_ids, row_count=len(result.observations),
    )
    sink = (S3JsonlSink(objects, resources.storage).put(
        job, (observation.model_dump(mode="json") for observation in result.observations),
    ) if request["target"] == "s3" else
        TimescaleHistorySink(os.environ["TARGET_DATABASE_URL"], resources.storage).put(
            job, result.observations))
    sys.stdout.write(JobCompletion(
        raw=raw, sink=sink, completed_scope=result.completed_ids,
    ).model_dump_json() + "\n")


if __name__ == "__main__":
    try:
        main()
    except (RawResponseTooLarge, CertifiedBatchTooLarge, TimeoutError):
        raise SystemExit(75) from None
    except NonRetryableJobError:
        raise SystemExit(65) from None
