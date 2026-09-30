"""Reviewed utility fixture; it has no route to ingestion certification."""

import argparse
import json
import os
import time


parser = argparse.ArgumentParser()
parser.add_argument("--job-context", required=True)
args = parser.parse_args()
with open(args.job_context, encoding="utf-8") as stream:
    context = json.load(stream)
mode = os.environ.get("SCRIPT_TEST_MODE", "ok")
if mode == "slow":
    time.sleep(3)
if mode == "bad_json":
    print("invalid")
else:
    print(json.dumps({"schema_version": 1, "run_id": context["run_id"],
                      "script_id": context["script_id"], "artifacts": []}))
