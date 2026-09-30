"""Local-only complete-coverage nightly equipment rules reader."""

from __future__ import annotations

import json
import os
from pathlib import Path
import sys

from ingestion.adapters.aws.s3_evidence import S3JsonlSink, S3ObjectStore, S3RawStore
from ingestion.adapters.db.timescale_entities import TimescaleRulesSink
from ingestion.adapters.skyspark.rules import FixtureRulesSource, read_rules
from ingestion.contracts.config import FeedKind
from ingestion.contracts.jobs import Job, JobCompletion
from ingestion.contracts.resources import load_resources
from ingestion.core.failures import NonRetryableJobError


def main() -> None:
    request = json.load(sys.stdin)
    job = Job.model_validate(request["job"])
    if job.feed != FeedKind.RULES or request["target"] not in ("s3", "timescale"):
        raise ValueError("contract fixture accepts only configured rules jobs")
    resources = load_resources(Path(os.environ["RESOURCE_CONFIG_PATH"]))
    policy = resources.rules_read
    if policy is None:
        raise ValueError("rules read policy is required")
    source = request["source"]
    result = read_rules(
        job, site_uri=source["site_uri"],
        source=FixtureRulesSource(
            source["endpoint"], max_response_bytes=policy.max_response_bytes,
            timeout_seconds=policy.request_timeout_seconds,
        ), policy=policy, source_timezone=source["rules_timezone"],
        allowed_tz_tags=tuple(source["rules_tz_tags"]),
    )
    objects = S3ObjectStore(
        region_name=resources.region, endpoint_url=os.environ["AWS_ENDPOINT_URL"],
    )
    raw = S3RawStore(objects, resources.storage).put(
        job, (result.raw_response,), query_id=result.query_id,
        completed_scope=result.completed_ids, row_count=len(result.detections),
    )
    sink = (S3JsonlSink(objects, resources.storage).put(
        job, (detection.model_dump(mode="json") for detection in result.detections),
    ) if request["target"] == "s3" else
        TimescaleRulesSink(os.environ["TARGET_DATABASE_URL"], resources.storage).put(
            job, result.detections))
    sys.stdout.write(JobCompletion(
        raw=raw, sink=sink, completed_scope=result.completed_ids,
    ).model_dump_json() + "\n")


if __name__ == "__main__":
    try:
        main()
    except NonRetryableJobError:
        raise SystemExit(65) from None
