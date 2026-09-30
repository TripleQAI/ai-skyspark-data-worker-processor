"""Synthetic reviewed script for the subprocess contract test only."""

import json
import sys


request = json.load(sys.stdin)
job = request["job"]
scope = [job["site_ref"]] if job["feed"] == "metadata" else job["scope_ids"]
result = {
    "raw": {
        "job_id": job["job_id"],
        "object_key": f"synthetic/raw/{job['job_id']}",
        "checksum": "synthetic-raw-checksum",
        "byte_count": 42,
    },
    "sink": {
        "job_id": job["job_id"],
        "sink_kind": request["target"],
        "batch_key": f"synthetic/batch/{job['job_id']}",
        "row_count": len(scope),
        "checksum": "synthetic-sink-checksum",
    },
    "completed_scope": scope,
}
print(json.dumps(result))
