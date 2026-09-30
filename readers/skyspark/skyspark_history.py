"""Five-minute point history for one bounded batch of historized points.

Replaces orginal-scripts/get_skysparktimeseriesdata.py. Reads every requested
point in one hisRead call instead of sampling points one at a time.
"""

from __future__ import annotations

import os

from ingestion.adapters.skyspark.history import read_history
from ingestion.adapters.skyspark.live import LiveHistorySource
from ingestion.adapters.skyspark.reader_runtime import run, write_evidence
from ingestion.contracts.config import FeedKind


def read(request, client):
    policy = request.resources.history_read
    if policy is None:
        raise ValueError("history read policy is required")
    result = read_history(
        request.job, site_uri=request.site_uri,
        source=LiveHistorySource(client, request.job, request.policy, request.replica),
        policy=policy,
    )
    return write_evidence(
        request, raw_response=result.raw_response, query_id=result.query_id,
        completed_scope=result.completed_ids,
        rows=(observation.model_dump(mode="json") for observation in result.observations),
        row_count=len(result.observations), environ=os.environ,
    )


if __name__ == "__main__":
    run(FeedKind.HISTORY, read, split_on_size=True)
