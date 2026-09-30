"""Independent file/manifest audit for the HHC one-hour LocalStack test."""

from __future__ import annotations

import argparse
import csv
from datetime import datetime, timedelta, timezone
import json
from math import ceil
from pathlib import Path

import boto3
import yaml


def utc(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def line_count(path: Path) -> int:
    count = 0
    with path.open("rb") as handle:
        while chunk := handle.read(8 * 1024 * 1024):
            count += chunk.count(b"\n")
    return count - 1  # one CSV header


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, required=True)
    args = parser.parse_args()
    config = yaml.safe_load(args.config.read_text(encoding="utf-8"))
    run = Path(config["output"]["root"]) / config["output"]["run_directory"]
    scope = json.loads((run / "source_scope.json").read_text(encoding="utf-8"))
    summary = json.loads((run / "run_summary.json").read_text(encoding="utf-8"))
    if summary["windows"] != 12 or summary["synthetic_distinct_point_ids"] != 10_000_000:
        raise RuntimeError("run summary is incomplete")
    points = scope["points"]
    per_facility = len(points)
    full_copies, tail = divmod(10_000_000, per_facility)
    if len(points) != 2716 or full_copies != 3681 or tail != 2404:
        raise RuntimeError("source inventory or synthetic copy count changed")
    sqs = boto3.client("sqs", endpoint_url=config["localstack"]["endpoint_url"],
                       region_name=config["localstack"]["region"],
                       aws_access_key_id="test", aws_secret_access_key="test")
    queue_url = sqs.get_queue_url(QueueName=config["localstack"]["queue_name"])["QueueUrl"]
    dlq_url = sqs.get_queue_url(QueueName=config["localstack"]["dlq_name"])["QueueUrl"]
    for url in (queue_url, dlq_url):
        attrs = sqs.get_queue_attributes(QueueUrl=url,
                                         AttributeNames=["ApproximateNumberOfMessages",
                                                         "ApproximateNumberOfMessagesNotVisible"])["Attributes"]
        if any(int(value) for value in attrs.values()):
            raise RuntimeError("LocalStack SQS or DLQ is not empty")

    start = utc(config["history"]["start_utc"])
    delta = timedelta(minutes=config["history"]["window_minutes"])
    report = []
    for index in range(12):
        left, right = start + index * delta, start + (index + 1) * delta
        window_id = f"{left:%Y%m%dT%H%MZ}_{right:%H%MZ}"
        folder = run / window_id
        snapshot = json.loads((folder / "source_values.json").read_text(encoding="utf-8"))
        result = json.loads((folder / "window_summary.json").read_text(encoding="utf-8"))
        if snapshot["window_id"] != window_id or result["window_id"] != window_id:
            raise RuntimeError("window identity mismatch")
        if snapshot["requested_historized_ids"] != 2715 or snapshot["source_calls"] != 7:
            raise RuntimeError("bounded source call coverage mismatch")
        if result["jobs_sent"] != 29453 or result["jobs_processed"] != 29453:
            raise RuntimeError("SQS job coverage mismatch")
        if result["csv_point_rows"] != 10_000_000 or result["csv_shard_files"] != 20:
            raise RuntimeError("window row coverage mismatch")
        expected_observed = sum(
            (full_copies + (seed < tail))
            for seed, point in enumerate(points)
            if snapshot["values_by_source_point"].get(point["source_point_id"])
        )
        expected_ineligible = sum(
            (full_copies + (seed < tail))
            for seed, point in enumerate(points) if not point["historized"]
        )
        if (result["csv_rows_with_observation"] != expected_observed
                or result["csv_not_historized_rows"] != expected_ineligible):
            raise RuntimeError("window observation or ineligible count differs from source snapshot")
        shards = sorted(folder.glob("points_*.csv"))
        if len(shards) != 20:
            raise RuntimeError("point CSV shard count mismatch")
        count = 0
        for file in shards:
            rows = line_count(file)
            if rows != config["synthetic"]["rows_per_csv_shard"]:
                raise RuntimeError(f"incomplete CSV shard: {file.name} has {rows} rows")
            count += rows
        if count != 10_000_000:
            raise RuntimeError("physical CSV line count mismatch")
        jobs = line_count(folder / "jobs.csv")
        source_rows = line_count(folder / "source_history_observations.csv")
        if jobs != 29453 or source_rows != snapshot["source_observations"]:
            raise RuntimeError("job manifest or source observation CSV count mismatch")
        report.append((window_id, left.isoformat(), right.isoformat(), len(shards), count,
                       jobs, source_rows, expected_observed, expected_ineligible,
                       result["csv_no_observation_rows"], "PASS"))
        print(f"verified {window_id}: 10M rows in 20 shards, {jobs} jobs", flush=True)
    path = run / "verification_report.csv"
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["window_id", "start_utc", "end_utc", "point_shards", "point_rows",
                         "jobs", "source_observations", "rows_with_observation",
                         "not_historized_rows", "no_observation_rows", "status"])
        writer.writerows(report)
    print("all 12 windows passed; SQS and DLQ are empty", flush=True)


if __name__ == "__main__":
    main()
