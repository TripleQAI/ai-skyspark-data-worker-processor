"""Contract fixture: reads a private file and emits a versioned receipt."""

import argparse
import json
import os
import sys
import time


parser = argparse.ArgumentParser()
parser.add_argument("--job-context", required=True)
args = parser.parse_args()
with open(args.job_context, encoding="utf-8") as stream:
    request = json.load(stream)
job = request["job"]
mode = os.environ.get("SCRIPT_TEST_MODE", "ok")
if mode == "slow":
    time.sleep(3)
if mode == "bad_json":
    print("not-json")
    sys.exit(0)
scope = [job["site_ref"]] if job["feed"] == "metadata" else job["scope_ids"]
if mode == "wrong_scope":
    scope = ["another-site"]
key = f"synthetic/raw/{job['job_id']}"
result = {
    "schema_version": 2,
    "script_id": request["script_id"],
    "config_hash": job["config_hash"],
    "job_id": job["job_id"],
    "provenance": {"query_ids": ["fixture-query-1"], "source_artifact_key": key},
    "completion": {
        "raw": {"job_id": job["job_id"], "object_key": key,
                "checksum": "fixture-raw", "byte_count": 42},
        "sink": {"job_id": job["job_id"], "sink_kind": request["target"],
                 "batch_key": f"synthetic/batch/{job['job_id']}",
                 "row_count": len(scope), "checksum": "fixture-sink"},
        "completed_scope": scope,
    },
}
print(json.dumps(result))
