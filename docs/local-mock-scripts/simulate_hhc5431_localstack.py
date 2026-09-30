"""Fan out a live HHC sample to 10M synthetic, unique, historized point IDs.

This is a local load fixture, not a SkySpark capacity benchmark or a certified
production ingestion run. Synthetic IDs are never submitted to SkySpark.
"""

from __future__ import annotations

import argparse
import csv
from concurrent.futures import ThreadPoolExecutor
import gzip
import json
from math import ceil
from pathlib import Path
import tempfile
import time
from urllib.parse import urlparse
from uuid import uuid4

import boto3
import yaml

ROOT = Path(__file__).resolve().parents[2]


class Shards:
    def __init__(self, directory: Path, stem: str, fields: list[str], max_rows: int):
        self.directory, self.stem, self.fields, self.max_rows = directory, stem, fields, max_rows
        self.files: dict[int, object] = {}
        self.writers: dict[int, csv.writer] = {}
        self.count = 0

    def write(self, index: int, values: tuple[object, ...]) -> None:
        shard = index // self.max_rows
        if shard not in self.writers:
            handle = (self.directory / f"{self.stem}_{shard:03d}.csv").open(
                "w", newline="", encoding="utf-8")
            writer = csv.writer(handle)
            writer.writerow(self.fields)
            self.files[shard], self.writers[shard] = handle, writer
        self.writers[shard].writerow(values)
        self.count += 1

    def close(self) -> None:
        for handle in self.files.values():
            handle.close()


def csv_rows(path: Path, fields: list[str], rows: list[tuple[object, ...]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(fields)
        writer.writerows(rows)


def synthetic_site(copy: int, site_index: int) -> str:
    return f"mock:HHC:5431:facility:{copy:05d}:source-site:{site_index}"


def synthetic_equipment(copy: int, source_index: int) -> str:
    return f"mock:HHC:5431:site:{copy:05d}:equip:{source_index:03d}"


def synthetic_point(copy: int, source_index: int) -> str:
    return f"mock:HHC:5431:site:{copy:05d}:point:{source_index:04d}"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--inventory", type=Path, required=True)
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--endpoint", default="http://127.0.0.1:4566")
    parser.add_argument("--target-points", type=int, default=10_000_000)
    parser.add_argument("--shard-rows", type=int, default=500_000)
    args = parser.parse_args()
    parsed = urlparse(args.endpoint)
    if parsed.scheme != "http" or parsed.hostname not in {"127.0.0.1", "localhost"}:
        raise ValueError("LocalStack endpoint must be host-local")
    if args.target_points < 1 or args.shard_rows < 500:
        raise ValueError("positive target and shard size are required")
    resource = yaml.safe_load((ROOT / "local" / "resources.yaml").read_text(encoding="utf-8"))
    profile = yaml.safe_load((ROOT / "config" / "profiles" / "default.yaml").read_text(encoding="utf-8"))
    history_batch = resource["history_read"]["max_ids"]
    rules_batch = resource["rules_read"]["max_ids"]
    slots = resource["worker"]["job_slots"]
    inventory = json.loads(args.inventory.read_text(encoding="utf-8"))
    snapshot = json.loads(args.snapshot.read_text(encoding="utf-8"))
    source_sites = sorted(
        {row["siteId"] for row in inventory["equipment"]},
        key=lambda site: (-sum(row["siteId"] == site for row in inventory["equipment"]), site),
    )
    source_site_index = {site: i for i, site in enumerate(source_sites)}
    equipment = sorted(inventory["equipment"],
                       key=lambda row: source_site_index[row["siteId"]])
    point_site_by_id = {}
    with (args.snapshot.parent / "live_points.csv").open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            point_site_by_id[row["source_point_id"]] = row["source_site_ref"]
    eligible = {tag["pointId"] for tag in inventory["pointTags"]
                if tag.get("pointTagName") == "his"}
    points = sorted((row for row in inventory["points"] if row["pointId"] in eligible),
                    key=lambda row: source_site_index[point_site_by_id[row["pointId"]]])
    if len(points) != snapshot["source_historized_point_count"]:
        raise RuntimeError("snapshot and inventory historized counts differ")
    equipment_ranges = {}
    point_ranges = {}
    for site_index, site_ref in enumerate(source_sites):
        eq_indices = [i for i, row in enumerate(equipment) if row["siteId"] == site_ref]
        pt_indices = [i for i, row in enumerate(points)
                      if point_site_by_id[row["pointId"]] == site_ref]
        assert eq_indices == list(range(eq_indices[0], eq_indices[-1] + 1))
        assert pt_indices == list(range(pt_indices[0], pt_indices[-1] + 1))
        equipment_ranges[site_index] = (eq_indices[0], len(eq_indices))
        point_ranges[site_index] = (pt_indices[0], len(pt_indices))
    equipment_index = {row["equipmentId"]: i for i, row in enumerate(equipment)}
    source_history = snapshot["history_latest_by_id"]
    source_rules = snapshot["rule_detections_by_equipment"]
    per_site = len(points)
    site_copies = ceil(args.target_points / per_site)
    run_id = f"hhc5431-{uuid4().hex[:10]}"
    bucket = f"{run_id}-local"
    args.output.mkdir(parents=True, exist_ok=True)
    s3 = boto3.client("s3", endpoint_url=args.endpoint, region_name=resource["region"],
                      aws_access_key_id="test", aws_secret_access_key="test")
    sqs = boto3.client("sqs", endpoint_url=args.endpoint, region_name=resource["region"],
                       aws_access_key_id="test", aws_secret_access_key="test")
    scheduler = boto3.client("scheduler", endpoint_url=args.endpoint, region_name=resource["region"],
                             aws_access_key_id="test", aws_secret_access_key="test")
    s3.create_bucket(Bucket=bucket)
    queue_urls: dict[str, str] = {}
    for feed in ("metadata", "rules", "history"):
        name = f"{run_id}-{feed}"
        dlq_name = f"{name}-dlq"
        dlq_url = sqs.create_queue(QueueName=dlq_name)["QueueUrl"]
        dlq_arn = sqs.get_queue_attributes(QueueUrl=dlq_url,
                                           AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
        queue_urls[feed] = sqs.create_queue(
            QueueName=name, Attributes={"RedrivePolicy": json.dumps(
                {"deadLetterTargetArn": dlq_arn, "maxReceiveCount": 4})})["QueueUrl"]

    jobs: dict[str, dict[str, int | str]] = {}
    feed_jobs: dict[str, list[str]] = {feed: [] for feed in queue_urls}
    for copy in range(site_copies):
        count = min(per_site, args.target_points - copy * per_site)
        if count <= 0:
            break
        for site_index, site_ref in enumerate(source_sites):
            eq_start, eq_count = equipment_ranges[site_index]
            pt_start, pt_total = point_ranges[site_index]
            pt_count = max(0, min(pt_total, count - pt_start))
            job_id = f"metadata-{copy:05d}-{site_index}"
            jobs[job_id] = {"feed": "metadata", "copy": copy, "site_index": site_index,
                            "offset": pt_start, "count": pt_count,
                            "equipment_offset": eq_start, "equipment_count": eq_count}
            feed_jobs["metadata"].append(job_id)
            for offset in range(eq_start, eq_start + eq_count, rules_batch):
                job_id = f"rules-{copy:05d}-{site_index}-{(offset-eq_start)//rules_batch:03d}"
                jobs[job_id] = {"feed": "rules", "copy": copy, "site_index": site_index,
                                "offset": offset,
                                "count": min(rules_batch, eq_start + eq_count - offset)}
                feed_jobs["rules"].append(job_id)
            for offset in range(pt_start, pt_start + pt_count, history_batch):
                job_id = f"history-{copy:05d}-{site_index}-{(offset-pt_start)//history_batch:03d}"
                jobs[job_id] = {"feed": "history", "copy": copy, "site_index": site_index,
                                "offset": offset,
                                "count": min(history_batch, pt_start + pt_count - offset)}
                feed_jobs["history"].append(job_id)

    for job in jobs.values():
        site_index = int(job["site_index"])
        if job["feed"] == "metadata":
            eq_start, eq_count = equipment_ranges[site_index]
            assert job["equipment_offset"] == eq_start and job["equipment_count"] == eq_count
        else:
            start, count = (point_ranges if job["feed"] == "history"
                            else equipment_ranges)[site_index]
            assert start <= int(job["offset"])
            assert int(job["offset"]) + int(job["count"]) <= start + count

    for feed, identifiers in feed_jobs.items():
        csv_rows(args.output / f"jobs_{feed}.csv",
                 ["job_id", "feed", "synthetic_site", "source_site_ref", "offset", "id_count", "source_facility_sk"],
                 [(job_id, feed, synthetic_site(int(jobs[job_id]["copy"]), int(jobs[job_id]["site_index"])),
                   source_sites[int(jobs[job_id]["site_index"])],
                   jobs[job_id]["offset"], jobs[job_id]["count"], 5431)
                  for job_id in identifiers])
    schedule_rows = []
    for feed in ("history", "rules", "metadata"):
        schedule = profile["feeds"][feed]["schedule"]
        expression = (f"rate({schedule['every_minutes']} minutes)" if schedule["kind"] == "interval"
                      else f"cron(0 {schedule['at_utc'].split(':')[0]} * * ? *)" if schedule["kind"] == "daily"
                      else f"cron(0 {schedule['at_utc'].split(':')[0]} ? * {schedule['day']} *)")
        name = f"{run_id}-{feed}"
        scheduler.create_schedule(
            Name=name, ScheduleExpression=expression, ScheduleExpressionTimezone="UTC",
            State="DISABLED", FlexibleTimeWindow={"Mode": "OFF"},
            Target={"Arn": "arn:aws:states:us-east-1:000000000000:stateMachine:local-ingestion-mock",
                    "RoleArn": "arn:aws:iam::000000000000:role/local-ingestion-mock",
                    "Input": json.dumps({"feed": feed, "run_id": run_id})})
        checked = scheduler.get_schedule(Name=name)
        if checked["ScheduleExpression"] != expression or checked["State"] != "DISABLED":
            raise RuntimeError("LocalStack Scheduler definition differs from design profile")
        schedule_rows.append((feed, expression, "UTC", "DISABLED", name))
    csv_rows(args.output / "schedules.csv",
             ["feed", "schedule_expression", "timezone", "state", "localstack_name"], schedule_rows)

    point_metadata = Shards(args.output, "synthetic_points", [
        "synthetic_point_id", "synthetic_site_id", "source_site_ref", "synthetic_equipment_id",
        "source_point_id", "source_equipment_id", "point_name", "kind", "unit",
        "facility_sk", "synthetic_replica"], args.shard_rows)
    equipment_metadata = Shards(args.output, "synthetic_equipment", [
        "synthetic_equipment_id", "synthetic_site_id", "source_equipment_id",
        "source_site_ref", "equipment_name", "facility_sk", "synthetic_replica"], args.shard_rows)
    history = Shards(args.output, "synthetic_history_coverage", [
        "synthetic_point_id", "synthetic_site_id", "source_site_ref", "source_point_id",
        "window_start_utc", "window_end_utc", "has_observation",
        "observed_at", "value_type", "value_json", "synthetic_replica"], args.shard_rows)
    rule_scope = Shards(args.output, "synthetic_rule_scope", [
        "synthetic_equipment_id", "synthetic_site_id", "source_site_ref", "source_equipment_id",
        "rule_day", "queried", "synthetic_replica"], args.shard_rows)
    detections = Shards(args.output, "synthetic_rule_detections", [
        "synthetic_equipment_id", "source_equipment_id", "synthetic_site_id", "source_site_ref",
        "rule_day", "source_row_json", "synthetic_replica"], args.shard_rows)

    def work(job_id: str):
        job = jobs[job_id]
        feed, copy, offset, count = job["feed"], int(job["copy"]), int(job["offset"]), int(job["count"])
        site = synthetic_site(copy, int(job["site_index"]))
        if feed == "metadata":
            equip_rows = [(copy * len(equipment) + i,
                           (synthetic_equipment(copy, i), site, equipment[i]["equipmentId"],
                            equipment[i]["siteId"], equipment[i].get("equipmentName", ""), 5431, copy))
                          for i in range(int(job["equipment_offset"]),
                                         int(job["equipment_offset"]) + int(job["equipment_count"]))]
            point_rows = []
            for i in range(offset, offset + count):
                row = points[i]
                source_equip = row.get("equipmentId") or ""
                equip_id = (synthetic_equipment(copy, equipment_index[source_equip])
                            if source_equip in equipment_index else "")
                point_rows.append((copy * per_site + i,
                                   (synthetic_point(copy, i), site, point_site_by_id[row["pointId"]], equip_id,
                                    row["pointId"], source_equip, row.get("pointName", ""),
                                    row.get("kind", ""), row.get("unit") or "", 5431, copy)))
            return feed, equip_rows, point_rows
        if feed == "history":
            rows = []
            for i in range(offset, offset + count):
                source_id = points[i]["pointId"]
                item = source_history.get(source_id)
                rows.append((copy * per_site + i,
                             (synthetic_point(copy, i), site, point_site_by_id[source_id], source_id,
                              snapshot["window_start"], snapshot["window_end"],
                              bool(item), item["observed_at"] if item else "",
                              item["value_type"] if item else "",
                              item["value_json"] if item else "", copy)))
            return feed, rows, []
        scoped = []
        found = []
        for i in range(offset, offset + count):
            row = equipment[i]
            source_id = row["equipmentId"]
            synthetic_id = synthetic_equipment(copy, i)
            scoped.append((copy * len(equipment) + i,
                           (synthetic_id, site, row["siteId"], source_id,
                            snapshot["rule_day"], True, copy)))
            for detection in source_rules.get(source_id, []):
                found.append((synthetic_id, source_id, site, row["siteId"], snapshot["rule_day"],
                              detection["source_row_json"], copy))
        return feed, scoped, found

    run_started = time.monotonic()
    feed_metrics = []
    with ThreadPoolExecutor(max_workers=slots) as pool:
        for feed in ("metadata", "rules", "history"):
            begun = time.monotonic()
            identifiers = feed_jobs[feed]
            queue_url = queue_urls[feed]
            for start in range(0, len(identifiers), 10):
                batch = identifiers[start:start + 10]
                response = sqs.send_message_batch(QueueUrl=queue_url, Entries=[
                    {"Id": str(i), "MessageBody": json.dumps({"job_id": job_id, "run_id": run_id})}
                    for i, job_id in enumerate(batch)])
                if response.get("Failed") or len(response.get("Successful", [])) != len(batch):
                    raise RuntimeError(f"SQS dispatch failed for {feed}")
            processed: set[str] = set()
            empty_polls = 0
            while len(processed) < len(identifiers):
                messages = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10,
                                               WaitTimeSeconds=1, VisibilityTimeout=300).get("Messages", [])
                if not messages:
                    empty_polls += 1
                    if empty_polls > 30:
                        raise RuntimeError(f"SQS {feed} stopped delivering jobs")
                    continue
                empty_polls = 0
                decoded = [json.loads(message["Body"]) for message in messages]
                for payload in decoded:
                    if payload.get("run_id") != run_id or payload.get("job_id") not in jobs:
                        raise RuntimeError("SQS returned an unexpected job reference")
                fresh = [payload["job_id"] for payload in decoded
                         if payload["job_id"] not in processed]
                for result in pool.map(work, fresh):
                    result_feed, first, second = result
                    if result_feed == "metadata":
                        for index, row in first:
                            equipment_metadata.write(index, row)
                        for index, row in second:
                            point_metadata.write(index, row)
                    elif result_feed == "rules":
                        for index, row in first:
                            rule_scope.write(index, row)
                        for row in second:
                            detections.write(detections.count, row)
                    else:
                        for index, row in first:
                            history.write(index, row)
                entries = [{"Id": str(i), "ReceiptHandle": message["ReceiptHandle"]}
                           for i, message in enumerate(messages)]
                response = sqs.delete_message_batch(QueueUrl=queue_url, Entries=entries)
                if response.get("Failed"):
                    raise RuntimeError(f"SQS acknowledgment failed for {feed}")
                processed.update(fresh)
                if len(processed) % 2000 < len(fresh):
                    print(f"{feed}: {len(processed)}/{len(identifiers)} jobs", flush=True)
            feed_metrics.append((feed, len(identifiers), len(processed),
                                 round(time.monotonic() - begun, 2)))
            print(f"{feed}: complete {len(processed)} jobs", flush=True)
    for writer in (point_metadata, equipment_metadata, history, rule_scope, detections):
        writer.close()
    if point_metadata.count != args.target_points or history.count != args.target_points:
        raise RuntimeError("10-million-point coverage mismatch")
    if equipment_metadata.count != site_copies * len(equipment):
        raise RuntimeError("equipment metadata coverage mismatch")
    if rule_scope.count != equipment_metadata.count:
        raise RuntimeError("rule equipment query coverage mismatch")
    csv_rows(args.output / "workflow_metrics.csv",
             ["feed", "jobs_sent", "jobs_processed", "elapsed_seconds"], feed_metrics)

    files = sorted(args.output.glob("*.csv"))
    uploaded = 0
    compressed_bytes = 0
    for path in files:
        with tempfile.TemporaryFile() as staging:
            with gzip.GzipFile(fileobj=staging, mode="wb", compresslevel=1) as zipped:
                with path.open("rb") as source:
                    while chunk := source.read(1024 * 1024):
                        zipped.write(chunk)
            compressed_bytes += staging.tell()
            staging.seek(0)
            s3.upload_fileobj(staging, bucket, f"{run_id}/{path.name}.gz",
                              ExtraArgs={"ContentType": "text/csv", "ContentEncoding": "gzip"})
        uploaded += 1
    objects = list(s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{run_id}/"))
    observed_objects = sum(page.get("KeyCount", 0) for page in objects)
    if observed_objects != uploaded:
        raise RuntimeError("LocalStack S3 object count differs from CSV count")
    metrics = {
        "run_id": run_id, "facility_sk": 5431, "project_uri": snapshot["project_uri"],
        "synthetic_facility_copies": site_copies,
        "synthetic_source_site_partitions": site_copies * len(source_sites),
        "synthetic_unique_points": point_metadata.count,
        "synthetic_equipment": equipment_metadata.count,
        "synthetic_history_coverage_rows": history.count,
        "synthetic_history_rows_with_value": sum(
            min(per_site, args.target_points - copy * per_site) // per_site * len(source_history)
            + sum(points[i]["pointId"] in source_history
                  for i in range(min(per_site, args.target_points - copy * per_site) % per_site))
            for copy in range(site_copies)),
        "synthetic_rule_scope_rows": rule_scope.count,
        "synthetic_rule_detections": detections.count,
        "jobs": {feed: len(identifiers) for feed, identifiers in feed_jobs.items()},
        "worker_slots": slots, "history_batch_ids": history_batch, "rules_batch_ids": rules_batch,
        "localstack_bucket": bucket, "localstack_queue_urls": queue_urls,
        "localstack_s3_csv_gzip_objects": uploaded,
        "localstack_s3_compressed_bytes": compressed_bytes,
        "source_window_start": snapshot["window_start"],
        "source_window_end": snapshot["window_end"],
        "source_rule_day": snapshot["rule_day"],
        "source_historized_points": per_site,
        "source_history_points_with_value": len(source_history),
        "source_rule_detections": snapshot["source_rule_detections"],
        "elapsed_seconds": round(time.monotonic() - run_started, 2),
        "synthetic_data": True, "real_source_calls_per_synthetic_copy": False,
        "source_coverage_certified": False,
    }
    (args.output / "workflow_summary.json").write_text(
        json.dumps(metrics, indent=2, sort_keys=True), encoding="utf-8")
    csv_rows(args.output / "workflow_summary.csv", ["metric", "value"],
             [(key, json.dumps(value, sort_keys=True) if isinstance(value, (dict, list)) else value)
              for key, value in metrics.items()])
    print(json.dumps({key: value for key, value in metrics.items()
                      if key != "localstack_queue_urls"}, sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
