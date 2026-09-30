"""One-hour HHC history test: bounded live reads, then LocalStack SQS fan-out.

Only real, historized SkySpark IDs reach the API. Credentials come from the
environment named by the reviewed YAML. Synthetic IDs stay on the local host.
"""

from __future__ import annotations

import argparse
import csv
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
import hashlib
import json
from math import ceil
import os
from pathlib import Path
import shutil
import socket
import sys
import time
from urllib.parse import urlsplit

import boto3
import yaml

PROJECT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT / "src"))


def utc(value: str | datetime) -> datetime:
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("timestamp has no timezone")
    return parsed.astimezone(timezone.utc)


def reference(value: object) -> str:
    return str(getattr(value, "val", value)).lstrip("@")


def encode(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str, separators=(",", ":"))


def save_json(path: Path, value: object) -> None:
    temp = path.with_suffix(path.suffix + ".partial")
    temp.write_text(json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2), encoding="utf-8")
    temp.replace(path)


def write_csv(path: Path, fields: list[str], rows) -> None:
    temp = path.with_suffix(path.suffix + ".partial")
    with temp.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(fields)
        writer.writerows(rows)
    temp.replace(path)


def rows_of(grid: object) -> list[dict[str, object]]:
    meta = grid.meta if hasattr(grid, "meta") else grid.get("meta", {})
    rows = grid.rows if hasattr(grid, "rows") else grid.get("rows", [])
    if "err" in meta or not isinstance(rows, (list, tuple)):
        raise RuntimeError("SkySpark returned an error or unsupported grid")
    return list(rows)


def config_from(path: Path) -> tuple[dict, str]:
    raw = path.read_bytes()
    config = yaml.safe_load(raw)
    if config["schema_version"] != 1:
        raise ValueError("unsupported test config schema")
    source, history, synthetic, localstack = (
        config["source"], config["history"], config["synthetic"], config["localstack"])
    start, end = utc(history["start_utc"]), utc(history["end_utc"])
    window = timedelta(minutes=history["window_minutes"])
    if ((end - start) != timedelta(hours=1) or window != timedelta(minutes=5)
            or history["expected_windows"] != 12 or start.minute % 5 or end.minute % 5):
        raise ValueError("test requires twelve aligned five-minute windows")
    if (source["history_batch_ids"] > 500 or synthetic["ids_per_job"] > 500
            or synthetic["target_distinct_point_ids"] != 10_000_000):
        raise ValueError("test bounds exceed the reviewed 10M / 500-ID case")
    endpoint = urlsplit(localstack["endpoint_url"])
    if endpoint.scheme != "http" or endpoint.hostname not in {"localhost", "127.0.0.1"}:
        raise ValueError("LocalStack endpoint must be host-local")
    api = urlsplit(source["api_root"])
    if not api.hostname or not source["api_root"].endswith("/api/"):
        raise ValueError("SkySpark API root is malformed")
    if config["output"]["upload_to_s3"]:
        raise ValueError("this SQS-only test does not upload CSVs to S3")
    return config, hashlib.sha256(raw).hexdigest()


def window_specs(config: dict) -> list[tuple[datetime, datetime, str]]:
    start = utc(config["history"]["start_utc"])
    delta = timedelta(minutes=config["history"]["window_minutes"])
    return [(start + i * delta, start + (i + 1) * delta,
             f"{(start + i * delta):%Y%m%dT%H%MZ}_{(start + (i + 1) * delta):%H%MZ}")
            for i in range(config["history"]["expected_windows"])]


def output_dir(config: dict) -> Path:
    root = Path(config["output"]["root"]).resolve()
    result = (root / config["output"]["run_directory"]).resolve()
    if result.parent != root:
        raise ValueError("run directory escapes configured output root")
    result.mkdir(parents=True, exist_ok=True)
    return result


def fetch(config: dict, digest: str, config_path: Path) -> None:
    from phable import open_haystack_client
    from ingestion.source_probe import PhableProbeClient

    source = config["source"]
    username = os.environ.get(source["username_env"])
    password = os.environ.get(source["password_env"])
    if not username or not password:
        raise RuntimeError("SkySpark credential environment variables are missing")
    host = urlsplit(source["api_root"]).hostname
    bypass = ",".join(filter(None, [os.environ.get("NO_PROXY", ""), host]))
    os.environ["NO_PROXY"] = os.environ["no_proxy"] = bypass
    socket.setdefaulttimeout(source["request_timeout_seconds"])
    inventory_path = Path(source["inventory_json"])
    inventory = json.loads(inventory_path.read_text(encoding="utf-8"))
    equipment = inventory["equipment"]
    points = inventory["points"]
    if (len(equipment) != source["expected_equipment"] or len(points) != source["expected_points"]
            or {row["facilitySk"] for row in equipment + points} != {source["facility_sk"]}):
        raise RuntimeError("HHC inventory counts or facility scope differ from config")
    equipment_ids = {row["equipmentId"] for row in equipment}
    point_ids = {row["pointId"] for row in points}
    if len(equipment_ids) != len(equipment) or len(point_ids) != len(points):
        raise RuntimeError("source inventory contains duplicate IDs")
    historized = {tag["pointId"] for tag in inventory["pointTags"]
                  if tag.get("pointTagName") == "his"}
    run = output_dir(config)
    shutil.copy2(config_path, run / "effective_config.yaml")
    api_url = source["api_root"] + source["project_uri"]
    with open_haystack_client(api_url, username, password) as raw_client:
        client = PhableProbeClient(raw_client)
        live_equipment = rows_of(client.eval("readAll(equip)"))
        live_points = rows_of(client.eval("readAll(point)"))
        if {reference(row["id"]) for row in live_equipment} != equipment_ids:
            raise RuntimeError("SkySpark equipment IDs differ from JSON inventory")
        if {reference(row["id"]) for row in live_points} != point_ids:
            raise RuntimeError("SkySpark point IDs differ from JSON inventory")
        site_by_point = {reference(row["id"]):
                         reference(row["siteRef"]) if row.get("siteRef") else ""
                         for row in live_points}
        site_counts = {site: sum(site_by_point[row["pointId"]] == site for row in points)
                       for site in set(site_by_point.values()) if site}
        site_order = sorted(site_counts, key=lambda site: (-site_counts[site], site))
        if len(site_order) != 2:
            raise RuntimeError("expected two observed SkySpark source-site groups")
        for point in points:
            if point["pointId"] in historized and not site_by_point[point["pointId"]]:
                raise RuntimeError("historized point has no source siteRef")
        site_rank = {site: index for index, site in enumerate(site_order)}
        ordered_points = sorted(points, key=lambda point: (
            site_rank.get(site_by_point[point["pointId"]], len(site_order)),
            0 if point["pointId"] in historized else 1))
        equipment_index = {row["equipmentId"]: index for index, row in enumerate(equipment)}
        scoped_points = [{
            "source_point_id": row["pointId"],
            "source_site_ref": site_by_point[row["pointId"]],
            "source_equipment_id": row.get("equipmentId") or "",
            "equipment_index": equipment_index.get(row.get("equipmentId"), None),
            "point_name": row.get("pointName") or "",
            "historized": row["pointId"] in historized,
        } for row in ordered_points]
        save_json(run / "source_scope.json", {
            "config_sha256": digest, "facility_sk": source["facility_sk"],
            "source_site_order": site_order, "equipment_count": len(equipment),
            "point_count": len(points), "historized_point_count": len(historized),
            "points": scoped_points,
        })
        write_csv(run / "source_inventory_check.csv",
                  ["entity", "json_ids", "live_ids", "match"],
                  [("equipment", len(equipment), len(live_equipment), True),
                   ("point", len(points), len(live_points), True)])
        print(f"source metadata: {len(equipment)} equipment, {len(points)} points, "
              f"{len(historized)} historized; starting 12 windows", flush=True)

        for start, end, window_id in window_specs(config):
            folder = run / window_id
            folder.mkdir(exist_ok=True)
            if (folder / "source_values.json").exists():
                existing = json.loads((folder / "source_values.json").read_text(encoding="utf-8"))
                if existing.get("config_sha256") != digest:
                    raise RuntimeError("existing source snapshot uses a different config")
                print(f"source {window_id}: existing capture reused", flush=True)
                continue
            values: dict[str, list[dict[str, object]]] = {}
            observations: list[tuple[str, str, str, str]] = []
            requested: set[str] = set()
            calls = out_of_window = 0
            for site in site_order:
                ids = [row["source_point_id"] for row in scoped_points
                       if row["historized"] and row["source_site_ref"] == site]
                for offset in range(0, len(ids), source["history_batch_ids"]):
                    batch = tuple(ids[offset:offset + source["history_batch_ids"]])
                    grid = client.history(batch, start, end)
                    rows = rows_of(grid)
                    columns = {column.name: reference(column.meta.get("id", column.name))
                               for column in grid.cols if column.name != "ts"}
                    if set(columns.values()) != set(batch) or len(columns) != len(batch):
                        raise RuntimeError(f"history response ID coverage differs in {window_id}")
                    requested.update(batch)
                    calls += 1
                    for row in rows:
                        timestamp = utc(row["ts"])
                        for column, point_id in columns.items():
                            value = row.get(column)
                            if value is None or type(value).__name__ == "NA":
                                continue
                            if not start <= timestamp < end:
                                out_of_window += 1
                                continue
                            item = {"observed_at_utc": timestamp.isoformat(),
                                    "value_type": type(value).__name__, "value_json": encode(value)}
                            values.setdefault(point_id, []).append(item)
                            observations.append((point_id, item["observed_at_utc"],
                                                 item["value_type"], item["value_json"]))
            if requested != historized:
                raise RuntimeError(f"not all historized IDs were queried in {window_id}")
            for point_values in values.values():
                point_values.sort(key=lambda item: item["observed_at_utc"])
            write_csv(folder / "source_history_observations.csv",
                      ["source_point_id", "observed_at_utc", "value_type", "value_json"],
                      observations)
            save_json(folder / "source_values.json", {
                "config_sha256": digest, "window_id": window_id,
                "window_start_utc": start.isoformat(), "window_end_utc": end.isoformat(),
                "requested_historized_ids": len(requested), "source_calls": calls,
                "source_observations": len(observations),
                "source_points_with_values": len(values),
                "out_of_window_value_cells": out_of_window,
                "source_coverage_certified": False, "values_by_source_point": values,
            })
            print(f"source {window_id}: {calls} calls, {len(observations)} observations, "
                  f"{out_of_window} boundary cells excluded", flush=True)


class ShardedCsv:
    def __init__(self, folder: Path, row_limit: int):
        self.folder, self.row_limit = folder, row_limit
        self.handles: dict[int, object] = {}
        self.writers: dict[int, csv.writer] = {}
        self.count = 0
        self.observed = 0
        self.ineligible = 0

    def write(self, index: int, row: tuple[object, ...]) -> None:
        shard = index // self.row_limit
        if shard not in self.writers:
            handle = (self.folder / f"points_{shard:03d}.csv.partial").open(
                "w", newline="", encoding="utf-8")
            writer = csv.writer(handle)
            writer.writerow([
                "synthetic_point_id", "synthetic_facility_id", "synthetic_site_id",
                "synthetic_equipment_id", "source_point_id", "source_site_ref",
                "window_start_utc", "window_end_utc", "history_status",
                "observation_count", "observations_json"])
            self.handles[shard], self.writers[shard] = handle, writer
        self.writers[shard].writerow(row)
        self.count += 1
        self.observed += row[8] == "observed"
        self.ineligible += row[8] == "not_historized"

    def close(self) -> None:
        for handle in self.handles.values():
            handle.close()
        for shard in self.handles:
            (self.folder / f"points_{shard:03d}.csv.partial").replace(
                self.folder / f"points_{shard:03d}.csv")


def process(config: dict, digest: str) -> None:
    run = output_dir(config)
    scope = json.loads((run / "source_scope.json").read_text(encoding="utf-8"))
    if scope["config_sha256"] != digest:
        raise RuntimeError("source scope uses a different config")
    points = scope["points"]
    per_facility = len(points)
    target = config["synthetic"]["target_distinct_point_ids"]
    facility_copies = ceil(target / per_facility)
    site_order = scope["source_site_order"] + [""]
    site_rank = {site: index for index, site in enumerate(site_order)}
    site_ranges = {}
    for site, group in site_rank.items():
        indices = [i for i, point in enumerate(points) if point["source_site_ref"] == site]
        if indices:
            if indices != list(range(indices[0], indices[-1] + 1)):
                raise RuntimeError("source site points are not a contiguous seed group")
            site_ranges[group] = (indices[0], len(indices))
    if len(site_ranges) != 3 or sum(count for _, count in site_ranges.values()) != per_facility:
        raise RuntimeError("expected two source-site groups and one unscoped point")
    jobs: dict[str, tuple[int, int, int, int]] = {}
    for copy in range(facility_copies):
        copy_count = min(per_facility, target - copy * per_facility)
        for group, (start, count) in site_ranges.items():
            selected = max(0, min(count, copy_count - start))
            for offset in range(start, start + selected, config["synthetic"]["ids_per_job"]):
                size = min(config["synthetic"]["ids_per_job"], start + selected - offset)
                job_id = f"{copy:05d}-{group}-{(offset-start)//config['synthetic']['ids_per_job']:03d}"
                jobs[job_id] = (copy, group, offset, size)
    if sum(job[3] for job in jobs.values()) != target:
        raise RuntimeError("planned jobs do not cover exactly 10M synthetic IDs")
    endpoint = config["localstack"]["endpoint_url"]
    sqs = boto3.client("sqs", endpoint_url=endpoint,
                       region_name=config["localstack"]["region"],
                       aws_access_key_id="test", aws_secret_access_key="test")
    queue_name = config["localstack"]["queue_name"]
    dlq_name = config["localstack"]["dlq_name"]
    try:
        dlq_url = sqs.get_queue_url(QueueName=dlq_name)["QueueUrl"]
    except sqs.exceptions.QueueDoesNotExist:
        dlq_url = sqs.create_queue(QueueName=dlq_name)["QueueUrl"]
    dlq_arn = sqs.get_queue_attributes(QueueUrl=dlq_url,
                                       AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    try:
        queue_url = sqs.get_queue_url(QueueName=queue_name)["QueueUrl"]
    except sqs.exceptions.QueueDoesNotExist:
        queue_url = sqs.create_queue(QueueName=queue_name, Attributes={
            "RedrivePolicy": json.dumps({"deadLetterTargetArn": dlq_arn,
                                         "maxReceiveCount": config["localstack"]["max_receive_count"]})
        })["QueueUrl"]
    save_json(run / "localstack_sqs.json", {
        "endpoint_url": endpoint, "queue_url": queue_url, "dlq_url": dlq_url,
        "job_references_per_window": len(jobs), "window_count": 12})
    summary_rows = []

    for start, end, window_id in window_specs(config):
        folder = run / window_id
        snapshot = json.loads((folder / "source_values.json").read_text(encoding="utf-8"))
        if snapshot["config_sha256"] != digest or snapshot["window_id"] != window_id:
            raise RuntimeError("source snapshot does not match the window/config")
        if (folder / "window_summary.json").exists():
            existing = json.loads((folder / "window_summary.json").read_text(encoding="utf-8"))
            if existing["csv_point_rows"] != target:
                raise RuntimeError("existing window summary has incomplete coverage")
            summary_rows.append(existing)
            print(f"worker {window_id}: completed output reused", flush=True)
            continue
        values = snapshot["values_by_source_point"]
        serialized = {point_id: encode(items) for point_id, items in values.items()}
        write_csv(folder / "jobs.csv",
                  ["job_id", "window_id", "synthetic_facility_copy", "site_group",
                   "source_site_ref", "point_offset", "point_count", "mode"],
                  [(job_id, window_id, copy, group, site_order[group], offset, count,
                    "history" if site_order[group] else "coverage_only")
                   for job_id, (copy, group, offset, count) in jobs.items()])
        current = sqs.get_queue_attributes(QueueUrl=queue_url,
                                           AttributeNames=["ApproximateNumberOfMessages",
                                                           "ApproximateNumberOfMessagesNotVisible"])["Attributes"]
        if any(int(value) for value in current.values()):
            raise RuntimeError("SQS queue is not empty before window dispatch")
        began = time.monotonic()
        identifiers = list(jobs)
        for offset in range(0, len(identifiers), config["localstack"]["send_batch_size"]):
            batch = identifiers[offset:offset + config["localstack"]["send_batch_size"]]
            response = sqs.send_message_batch(QueueUrl=queue_url, Entries=[
                {"Id": str(i), "MessageBody": encode({"run_id": config["run_id"],
                                                        "window_id": window_id,
                                                        "job_id": job_id})}
                for i, job_id in enumerate(batch)])
            if response.get("Failed") or len(response.get("Successful", [])) != len(batch):
                raise RuntimeError("LocalStack SQS dispatch failed")
        writer = ShardedCsv(folder, config["synthetic"]["rows_per_csv_shard"])

        def work(job_id: str):
            copy, group, offset, count = jobs[job_id]
            facility = f"mock:{config['synthetic']['facility_copy_label']}:facility:{copy:05d}"
            site = f"{facility}:site:{group}"
            result = []
            for index in range(offset, offset + count):
                point = points[index]
                source_id = point["source_point_id"]
                observations = values.get(source_id, [])
                status = ("not_historized" if not point["historized"] else
                          "observed" if observations else "no_observation")
                equipment_index = point["equipment_index"]
                synthetic_equipment = (f"{facility}:equip:{equipment_index:03d}"
                                       if equipment_index is not None else "")
                row = (
                    f"{facility}:point:{index:04d}", facility, site,
                    synthetic_equipment, source_id, point["source_site_ref"],
                    start.isoformat(), end.isoformat(), status,
                    len(observations), serialized.get(source_id, "[]"),
                )
                result.append((copy * per_facility + index, row))
            return result

        processed: set[str] = set()
        empty_polls = 0
        with ThreadPoolExecutor(max_workers=config["worker"]["job_slots"]) as pool:
            while len(processed) < len(jobs):
                messages = sqs.receive_message(
                    QueueUrl=queue_url,
                    MaxNumberOfMessages=config["localstack"]["receive_batch_size"],
                    WaitTimeSeconds=1,
                    VisibilityTimeout=config["localstack"]["visibility_timeout_seconds"],
                ).get("Messages", [])
                if not messages:
                    empty_polls += 1
                    if empty_polls > 30:
                        raise RuntimeError("LocalStack SQS stopped delivering jobs")
                    continue
                empty_polls = 0
                payloads = [json.loads(message["Body"]) for message in messages]
                if any(payload.get("run_id") != config["run_id"]
                       or payload.get("window_id") != window_id
                       or payload.get("job_id") not in jobs for payload in payloads):
                    raise RuntimeError("SQS returned an unexpected job reference")
                fresh = [payload["job_id"] for payload in payloads
                         if payload["job_id"] not in processed]
                for batch_result in pool.map(work, fresh):
                    for index, row in batch_result:
                        writer.write(index, row)
                response = sqs.delete_message_batch(QueueUrl=queue_url, Entries=[
                    {"Id": str(i), "ReceiptHandle": message["ReceiptHandle"]}
                    for i, message in enumerate(messages)])
                if response.get("Failed"):
                    raise RuntimeError("LocalStack SQS acknowledgment failed")
                processed.update(fresh)
                if len(processed) % 5000 < len(fresh):
                    print(f"worker {window_id}: {len(processed)}/{len(jobs)} jobs", flush=True)
        writer.close()
        if writer.count != target:
            raise RuntimeError("window CSVs do not contain 10M rows")
        remaining = sqs.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"],
        )["Attributes"]
        if any(int(value) for value in remaining.values()):
            raise RuntimeError("SQS queue was not empty after window completion")
        window_summary = {
            "window_id": window_id,
            "window_start_utc": start.isoformat(), "window_end_utc": end.isoformat(),
            "source_calls": snapshot["source_calls"],
            "source_observations": snapshot["source_observations"],
            "source_points_with_values": snapshot["source_points_with_values"],
            "jobs_sent": len(jobs), "jobs_processed": len(processed),
            "csv_point_rows": writer.count, "csv_rows_with_observation": writer.observed,
            "csv_not_historized_rows": writer.ineligible,
            "csv_no_observation_rows": writer.count - writer.observed - writer.ineligible,
            "csv_shard_files": len(writer.handles),
            "elapsed_seconds": round(time.monotonic() - began, 2),
            "synthetic_data": True, "source_coverage_certified": False,
        }
        save_json(folder / "window_summary.json", window_summary)
        write_csv(folder / "window_summary.csv", list(window_summary), [tuple(window_summary.values())])
        summary_rows.append(window_summary)
        free = shutil.disk_usage(run).free / 1024**3
        print(f"worker {window_id}: 10M rows, {writer.observed} with observations, "
              f"{len(jobs)} SQS jobs, {free:.1f} GiB free", flush=True)
        if free < 15 and window_id != window_specs(config)[-1][2]:
            raise RuntimeError("stopping before the next window: less than 15 GiB free")

    write_csv(run / "run_summary.csv", list(summary_rows[0]),
              [tuple(row.values()) for row in summary_rows])
    save_json(run / "run_summary.json", {
        "run_id": config["run_id"], "config_sha256": digest,
        "facility_sk": config["source"]["facility_sk"],
        "source_equipment": scope["equipment_count"],
        "source_points": scope["point_count"],
        "source_historized_points": scope["historized_point_count"],
        "synthetic_facility_copies": facility_copies,
        "synthetic_distinct_point_ids": target,
        "windows": len(summary_rows),
        "total_window_point_rows": sum(row["csv_point_rows"] for row in summary_rows),
        "total_sqs_jobs_processed": sum(row["jobs_processed"] for row in summary_rows),
        "localstack_queue_url": queue_url,
        "localstack_dlq_url": dlq_url,
        "source_coverage_certified": False,
    })
    print("one-hour history test complete", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--phase", choices=("fetch", "process"), required=True)
    args = parser.parse_args()
    config, digest = config_from(args.config)
    if args.phase == "fetch":
        fetch(config, digest, args.config)
    else:
        process(config, digest)


if __name__ == "__main__":
    main()
