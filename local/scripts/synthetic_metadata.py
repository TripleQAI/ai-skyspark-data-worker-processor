"""Local-only metadata fixture that writes physical raw and S3 target evidence."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

from ingestion.adapters.aws.s3_evidence import S3JsonlSink, S3ObjectStore, S3RawStore
from ingestion.contracts.config import FeedKind
from ingestion.contracts.jobs import Job, JobCompletion
from ingestion.contracts.resources import load_resources
from ingestion.core.failures import NonRetryableJobError


def main() -> None:
    request = json.load(sys.stdin)
    job = Job.model_validate(request["job"])
    if job.feed != FeedKind.METADATA or request["target"] != "s3":
        raise ValueError("synthetic fixture accepts only metadata jobs with an S3 target")
    resources = load_resources(Path(os.environ["RESOURCE_CONFIG_PATH"]))
    source = request["source"]
    site_uri = source["site_uri"]
    if not isinstance(site_uri, str) or not site_uri:
        raise ValueError("synthetic fixture requires an approved site URI")
    equipment_id = f"fixture-equip-{job.site_ref}"
    rows = [
        {
            "source_kind": "synthetic-local-fixture",
            "kind": "equipment",
            "source_id": equipment_id,
            "site_ref": job.site_ref,
            "site_uri": site_uri,
        },
        {
            "source_kind": "synthetic-local-fixture",
            "kind": "point",
            "source_id": f"fixture-point-{job.site_ref}",
            "equipment_ref": equipment_id,
            "site_ref": job.site_ref,
            "historized": True,
        },
    ]
    raw_response = json.dumps(
        {
            "source_kind": "synthetic-local-fixture",
            "requested_scope": [job.site_ref],
            "completed_scope": [job.site_ref],
            "rows": rows,
        },
        sort_keys=True, separators=(",", ":"),
    ).encode("utf-8")
    objects = S3ObjectStore(
        region_name=resources.region, endpoint_url=os.environ["AWS_ENDPOINT_URL"],
    )
    raw = S3RawStore(objects, resources.storage).put(
        job, (raw_response,), query_id=f"synthetic-local/{job.site_ref}",
        completed_scope=(job.site_ref,), row_count=len(rows),
    )
    sink = S3JsonlSink(objects, resources.storage).put(job, rows)
    completion = JobCompletion(
        raw=raw, sink=sink, completed_scope=(job.site_ref,),
    )
    sys.stdout.write(completion.model_dump_json() + "\n")


if __name__ == "__main__":
    try:
        main()
    except NonRetryableJobError:
        # The reviewed subprocess protocol reserves 65 for a response that
        # cannot be safely retried as the same partition.
        raise SystemExit(65) from None
