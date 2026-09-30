"""Re-fetch the bounded real HHC history batches through LocalStack SQS.

This is a read-only verification pass. It compares each SQS-driven SkySpark
response with the source snapshot used to make the synthetic CSVs.
"""

from __future__ import annotations

import argparse
import csv
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import socket
from urllib.parse import urlsplit

import boto3
from phable import open_haystack_client

from run_hhc5431_hour import (config_from, encode, output_dir, reference, rows_of,
                              utc, window_specs, write_csv)
from ingestion.source_probe import PhableProbeClient


def queue_attributes(sqs, url: str) -> dict[str, str]:
    return sqs.get_queue_attributes(
        QueueUrl=url,
        AttributeNames=["ApproximateNumberOfMessages",
                        "ApproximateNumberOfMessagesNotVisible"],
    )["Attributes"]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, required=True)
    args = parser.parse_args()
    config, digest = config_from(args.config)
    source = config["source"]
    username = os.environ.get(source["username_env"])
    password = os.environ.get(source["password_env"])
    if not username or not password:
        raise RuntimeError("SkySpark credential environment variables are missing")
    host = urlsplit(source["api_root"]).hostname
    bypass = ",".join(filter(None, [os.environ.get("NO_PROXY", ""), host]))
    os.environ["NO_PROXY"] = os.environ["no_proxy"] = bypass
    socket.setdefaulttimeout(source["request_timeout_seconds"])

    run = output_dir(config)
    scope = json.loads((run / "source_scope.json").read_text(encoding="utf-8"))
    if scope["config_sha256"] != digest:
        raise RuntimeError("source scope does not match the reviewed config")
    jobs = {}
    for start, end, window_id in window_specs(config):
        for site_index, site in enumerate(scope["source_site_order"]):
            ids = [point["source_point_id"] for point in scope["points"]
                   if point["historized"] and point["source_site_ref"] == site]
            for offset in range(0, len(ids), source["history_batch_ids"]):
                job_id = f"{window_id}-site{site_index}-batch{offset // source['history_batch_ids']}"
                jobs[job_id] = (window_id, start, end,
                                tuple(ids[offset:offset + source["history_batch_ids"]]))
    if len(jobs) != 84:
        raise RuntimeError(f"expected 84 real source fetch jobs, planned {len(jobs)}")

    sqs = boto3.client("sqs", endpoint_url=config["localstack"]["endpoint_url"],
                       region_name=config["localstack"]["region"],
                       aws_access_key_id="test", aws_secret_access_key="test")
    queue_url = sqs.get_queue_url(QueueName=config["localstack"]["queue_name"])["QueueUrl"]
    dlq_url = sqs.get_queue_url(QueueName=config["localstack"]["dlq_name"])["QueueUrl"]
    if any(int(value) for value in queue_attributes(sqs, queue_url).values()):
        raise RuntimeError("work queue is not empty before source verification")
    if any(int(value) for value in queue_attributes(sqs, dlq_url).values()):
        raise RuntimeError("DLQ is not empty before source verification")

    job_ids = list(jobs)
    for offset in range(0, len(job_ids), config["localstack"]["send_batch_size"]):
        batch = job_ids[offset:offset + config["localstack"]["send_batch_size"]]
        response = sqs.send_message_batch(QueueUrl=queue_url, Entries=[
            {"Id": str(index), "MessageBody": encode({
                "run_id": config["run_id"], "kind": "source_fetch_verify", "job_id": job_id})}
            for index, job_id in enumerate(batch)])
        if response.get("Failed") or len(response.get("Successful", [])) != len(batch):
            raise RuntimeError("source verification SQS dispatch failed")

    api_url = source["api_root"] + source["project_uri"]

    def fetch_job(job_id: str):
        window_id, start, end, ids = jobs[job_id]
        with open_haystack_client(api_url, username, password) as raw_client:
            client = PhableProbeClient(raw_client)
            grid = client.history(ids, start, end)
        rows = rows_of(grid)
        columns = {column.name: reference(column.meta.get("id", column.name))
                   for column in grid.cols if column.name != "ts"}
        if len(columns) != len(ids) or set(columns.values()) != set(ids):
            raise RuntimeError(f"source response ID coverage differs for {job_id}")
        values = {}
        out_of_window = 0
        for row in rows:
            timestamp = utc(row["ts"])
            for column, point_id in columns.items():
                value = row.get(column)
                if value is None or type(value).__name__ == "NA":
                    continue
                if not start <= timestamp < end:
                    out_of_window += 1
                    continue
                values.setdefault(point_id, []).append({
                    "observed_at_utc": timestamp.isoformat(),
                    "value_type": type(value).__name__,
                    "value_json": encode(value),
                })
        return window_id, ids, values, out_of_window

    received = set()
    fetched = {window_id: {"ids": set(), "values": {}, "out_of_window": 0, "jobs": 0}
               for _, _, window_id in window_specs(config)}
    with ThreadPoolExecutor(max_workers=config["worker"]["job_slots"]) as pool:
        empty_polls = 0
        while len(received) < len(jobs):
            messages = sqs.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=config["localstack"]["receive_batch_size"],
                WaitTimeSeconds=1,
                VisibilityTimeout=config["localstack"]["visibility_timeout_seconds"],
            ).get("Messages", [])
            if not messages:
                empty_polls += 1
                if empty_polls > 30:
                    raise RuntimeError("LocalStack stopped delivering source fetch jobs")
                continue
            empty_polls = 0
            payloads = [json.loads(message["Body"]) for message in messages]
            if any(payload.get("run_id") != config["run_id"]
                   or payload.get("kind") != "source_fetch_verify"
                   or payload.get("job_id") not in jobs for payload in payloads):
                raise RuntimeError("unexpected SQS source fetch reference")
            fresh = [payload["job_id"] for payload in payloads
                     if payload["job_id"] not in received]
            for window_id, ids, values, out_of_window in pool.map(fetch_job, fresh):
                state = fetched[window_id]
                if state["ids"].intersection(ids):
                    raise RuntimeError("source fetch jobs overlap")
                state["ids"].update(ids)
                state["values"].update(values)
                state["out_of_window"] += out_of_window
                state["jobs"] += 1
            response = sqs.delete_message_batch(QueueUrl=queue_url, Entries=[
                {"Id": str(index), "ReceiptHandle": message["ReceiptHandle"]}
                for index, message in enumerate(messages)])
            if response.get("Failed"):
                raise RuntimeError("source fetch SQS acknowledgment failed")
            received.update(fresh)
            print(f"queued source fetch: {len(received)}/{len(jobs)} jobs", flush=True)

    if any(int(value) for value in queue_attributes(sqs, queue_url).values()):
        raise RuntimeError("work queue is not empty after source verification")
    if any(int(value) for value in queue_attributes(sqs, dlq_url).values()):
        raise RuntimeError("DLQ is not empty after source verification")
    report = []
    for _, _, window_id in window_specs(config):
        state = fetched[window_id]
        snapshot = json.loads((run / window_id / "source_values.json").read_text(encoding="utf-8"))
        for values in state["values"].values():
            values.sort(key=lambda item: item["observed_at_utc"])
        matched = state["values"] == snapshot["values_by_source_point"]
        observations = sum(len(values) for values in state["values"].values())
        if (state["jobs"] != 7 or len(state["ids"]) != 2715
                or state["out_of_window"] != snapshot["out_of_window_value_cells"]
                or observations != snapshot["source_observations"] or not matched):
            raise RuntimeError(f"SQS-driven source read differs from CSV source snapshot: {window_id}")
        report.append((window_id, state["jobs"], len(state["ids"]), observations,
                       state["out_of_window"], "MATCH"))
    report_path = run / "queued_source_fetch_report.csv"
    write_csv(report_path,
              ["window_id", "sqs_fetch_jobs", "real_point_ids_queried",
               "source_observations", "out_of_window_value_cells", "snapshot_match"],
              report)
    print("84 SQS-driven real SkySpark reads matched all 12 CSV source snapshots", flush=True)


if __name__ == "__main__":
    main()
