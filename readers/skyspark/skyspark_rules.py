"""Nightly RuleSpark detections for one bounded batch of site equipment.

Replaces orginal-scripts/get_skysparkrulespark.py. Queries only the job's
equipment for one source-local day instead of readAll(equip) project-wide.
"""

from __future__ import annotations

import os

from ingestion.adapters.skyspark.live import LiveRulesSource
from ingestion.adapters.skyspark.reader_runtime import run, write_evidence
from ingestion.adapters.skyspark.rules import read_rules
from ingestion.contracts.config import FeedKind


def read(request, client):
    policy = request.resources.rules_read
    if policy is None:
        raise ValueError("rules read policy is required")
    result = read_rules(
        request.job, site_uri=request.site_uri,
        source=LiveRulesSource(client, request.job, request.policy, request.replica),
        policy=policy, source_timezone=request.source["rules_timezone"],
        allowed_tz_tags=tuple(request.source["rules_tz_tags"]),
    )
    return write_evidence(
        request, raw_response=result.raw_response, query_id=result.query_id,
        completed_scope=result.completed_ids,
        rows=(detection.model_dump(mode="json") for detection in result.detections),
        row_count=len(result.detections), environ=os.environ,
    )


if __name__ == "__main__":
    run(FeedKind.RULES, read)
