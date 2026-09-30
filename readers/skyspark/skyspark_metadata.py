"""Weekly site metadata: equipment and points for one approved site.

Replaces orginal-scripts/get_skysparkentitydata.py. Filters, credentials,
receipt mode, and any replica shape come from reviewed configuration.
"""

from __future__ import annotations

import os

from ingestion.adapters.skyspark.live import LiveMetadataSource
from ingestion.adapters.skyspark.metadata import read_site_metadata
from ingestion.adapters.skyspark.reader_runtime import run, write_evidence
from ingestion.contracts.config import FeedKind


def read(request, client):
    result = read_site_metadata(
        request.job, site_uri=request.site_uri,
        source=LiveMetadataSource(client, request.job, request.policy, request.replica),
        policy=request.resources.metadata_read,
    )
    return write_evidence(
        request, raw_response=result.raw_response, query_id=result.query_id,
        completed_scope=(request.job.site_ref,), rows=result.rows,
        row_count=len(result.rows), environ=os.environ,
    )


if __name__ == "__main__":
    run(FeedKind.METADATA, read)
