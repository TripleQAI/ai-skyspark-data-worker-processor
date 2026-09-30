"""Bounded, read-only HHC-5431 SkySpark capture for the local scale test.

Credentials come only from SKYSPARK_USERNAME/SKYSPARK_PASSWORD in the process
environment. This script never sends synthetic IDs to the source.
"""

from __future__ import annotations

import argparse
import csv
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import sys
import time
from urllib.parse import urlsplit

from phable import open_haystack_client

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))
from ingestion.source_probe import PhableProbeClient  # noqa: E402


def ref(value: object) -> str:
    return str(getattr(value, "val", value)).lstrip("@")


def serialized(value: object) -> str:
    return json.dumps(value, default=str, ensure_ascii=False, sort_keys=True)


def write_csv(path: Path, fields: list[str], rows: list[dict[str, object]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def checked_grid(grid: object) -> list[dict[str, object]]:
    meta = grid.meta if hasattr(grid, "meta") else grid.get("meta", {})
    rows = grid.rows if hasattr(grid, "rows") else grid.get("rows", [])
    if "err" in meta or not isinstance(rows, (list, tuple)):
        raise RuntimeError("SkySpark returned an error or unsupported grid")
    return list(rows)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--inventory", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--api-root", required=True)
    parser.add_argument("--history-batch", type=int, default=500)
    parser.add_argument("--rules-batch", type=int, default=200)
    parser.add_argument("--lag-minutes", type=int, default=10)
    args = parser.parse_args()
    username = os.environ.get("SKYSPARK_USERNAME")
    password = os.environ.get("SKYSPARK_PASSWORD")
    if not username or not password:
        raise RuntimeError("SKYSPARK_USERNAME and SKYSPARK_PASSWORD are required")
    if args.history_batch > 500 or args.rules_batch > 200 or args.lag_minutes < 5:
        raise ValueError("source call limits exceed the reviewed local pilot caps")
    source_bytes = args.inventory.read_bytes()
    inventory = json.loads(source_bytes)
    equipment = inventory["equipment"]
    points = inventory["points"]
    site_refs = sorted({row["siteId"] for row in equipment})
    point_ids = [row["pointId"] for row in points]
    historized = {tag["pointId"] for tag in inventory["pointTags"]
                  if tag.get("pointTagName") == "his"}
    history_ids = [identifier for identifier in point_ids if identifier in historized]
    equipment_ids = [row["equipmentId"] for row in equipment]
    if len(points) != len(set(point_ids)) or len(equipment) != len(set(equipment_ids)):
        raise ValueError("inventory IDs must be unique")
    project_uris = {identifier.split(":", 2)[1] for identifier in point_ids + equipment_ids}
    if len(project_uris) != 1:
        raise ValueError("inventory spans multiple SkySpark projects")
    project_uri = project_uris.pop()
    if not args.api_root.endswith("/api/"):
        raise ValueError("expected the selected credential-free SkySpark API root")
    api_url = args.api_root + project_uri
    host = urlsplit(args.api_root).hostname
    if not host:
        raise ValueError("SkySpark API root has no host")
    bypass = ",".join(filter(None, [os.environ.get("NO_PROXY", ""), host]))
    os.environ["NO_PROXY"] = os.environ["no_proxy"] = bypass
    now = datetime.now(timezone.utc)
    end = now.replace(second=0, microsecond=0)
    end -= timedelta(minutes=end.minute % 5 + args.lag_minutes)
    start = end - timedelta(minutes=5)
    rule_day = (now - timedelta(days=1)).date().isoformat()
    args.output.mkdir(parents=True, exist_ok=True)
    calls: list[dict[str, object]] = []

    def measured(operation: str, requested: int, fn):
        begun = time.monotonic()
        result = fn()
        calls.append({"operation": operation, "requested_ids": requested,
                      "returned_rows": len(checked_grid(result)),
                      "elapsed_seconds": round(time.monotonic() - begun, 3)})
        return result

    with open_haystack_client(api_url, username, password) as raw_client:
        client = PhableProbeClient(raw_client)
        live_equipment = checked_grid(measured("metadata_equipment", len(equipment),
                                               lambda: client.eval("readAll(equip)")))
        live_points = checked_grid(measured("metadata_points", len(points),
                                            lambda: client.eval("readAll(point)")))
        if {ref(row["id"]) for row in live_equipment} != set(equipment_ids):
            raise RuntimeError("live equipment scope does not match HHC inventory")
        if {ref(row["id"]) for row in live_points} != set(point_ids):
            raise RuntimeError("live point scope does not match HHC inventory")
        write_csv(args.output / "live_equipment.csv",
                  ["source_equipment_id", "source_site_ref", "source_tags_json"],
                  [{"source_equipment_id": ref(row["id"]),
                    "source_site_ref": ref(row.get("siteRef", "")),
                    "source_tags_json": serialized(row)} for row in live_equipment])
        write_csv(args.output / "live_points.csv",
                  ["source_point_id", "source_equipment_id", "source_site_ref", "source_tags_json"],
                  [{"source_point_id": ref(row["id"]),
                    "source_equipment_id": ref(row.get("equipRef", "")) if row.get("equipRef") else "",
                    "source_site_ref": ref(row.get("siteRef", "")) if row.get("siteRef") else "",
                    "source_tags_json": serialized(row)} for row in live_points])
        write_csv(args.output / "live_history_exclusions.csv",
                  ["source_point_id", "reason"],
                  [{"source_point_id": identifier, "reason": "not_tagged_his"}
                   for identifier in point_ids if identifier not in historized])

        observations: list[dict[str, object]] = []
        latest: dict[str, dict[str, object]] = {}
        history_completed: set[str] = set()
        for offset in range(0, len(history_ids), args.history_batch):
            batch = tuple(history_ids[offset:offset + args.history_batch])
            grid = measured("history", len(batch),
                            lambda batch=batch: client.history(batch, start, end))
            rows = checked_grid(grid)
            columns = {}
            for column in grid.cols:
                if column.name == "ts":
                    continue
                point_id = ref(column.meta.get("id", column.name))
                columns[column.name] = point_id
            if set(columns.values()) != set(batch):
                raise RuntimeError("history response point columns differ from requested IDs")
            history_completed.update(batch)
            for row in rows:
                timestamp = str(row.get("ts", ""))
                for column_name, point_id in columns.items():
                    value = row.get(column_name)
                    if value is None or type(value).__name__ == "NA":
                        continue
                    item = {"source_point_id": point_id, "observed_at": timestamp,
                            "value_type": type(value).__name__, "value_json": serialized(value)}
                    observations.append(item)
                    if point_id not in latest or timestamp >= str(latest[point_id]["observed_at"]):
                        latest[point_id] = item
        write_csv(args.output / "live_history_observations.csv",
                  ["source_point_id", "observed_at", "value_type", "value_json"], observations)
        if history_completed != set(history_ids):
            raise RuntimeError("not every source point was queried for history")

        source_rules: list[dict[str, object]] = []
        rule_scope: list[dict[str, object]] = []
        by_site = {site: [row["equipmentId"] for row in equipment if row["siteId"] == site]
                   for site in site_refs}
        for site_ref, ids in by_site.items():
            for offset in range(0, len(ids), args.rules_batch):
                batch = ids[offset:offset + args.rules_batch]
                selected = " or ".join(f"id==@{identifier}" for identifier in batch)
                filter_expr = f"equip and siteRef==@{site_ref} and ({selected})"
                scoped = checked_grid(measured("rules_equipment_scope", len(batch),
                                               lambda expr=filter_expr: client.eval(f"readAll({expr})")))
                returned = {ref(row["id"]) for row in scoped}
                if returned != set(batch):
                    raise RuntimeError("rules equipment scope did not match requested IDs")
                rule_scope.extend({"source_equipment_id": identifier, "source_site_ref": site_ref,
                                   "rule_day": rule_day, "queried": True} for identifier in batch)
                found = checked_grid(measured("rules_detections", len(batch),
                                              lambda expr=filter_expr: client.eval(
                                                  f"readAll({expr}).ruleSparks({rule_day})")))
                for row in found:
                    target = ref(row.get("targetRef", ""))
                    if target not in returned:
                        raise RuntimeError("rule detection target outside requested equipment")
                    source_rules.append({"source_equipment_id": target, "rule_day": rule_day,
                                         "source_row_json": serialized(row)})
        write_csv(args.output / "live_rule_scope.csv",
                  ["source_equipment_id", "source_site_ref", "rule_day", "queried"], rule_scope)
        write_csv(args.output / "live_rule_detections.csv",
                  ["source_equipment_id", "rule_day", "source_row_json"], source_rules)

    snapshot = {
        "inventory_sha256": hashlib.sha256(source_bytes).hexdigest(),
        "facility_sk": 5431, "project_uri": project_uri, "source_site_refs": site_refs,
        "api_root": args.api_root, "window_start": start.isoformat(),
        "window_end": end.isoformat(), "rule_day": rule_day,
        "source_equipment_count": len(equipment), "source_point_count": len(points),
        "source_historized_point_count": len(history_ids),
        "source_history_excluded_point_count": len(point_ids) - len(history_ids),
        "source_history_observations": len(observations),
        "source_history_points_with_value": len(latest),
        "source_rule_detections": len(source_rules),
        "history_latest_by_id": latest,
        "rule_detections_by_equipment": {
            identifier: [item for item in source_rules if item["source_equipment_id"] == identifier]
            for identifier in {item["source_equipment_id"] for item in source_rules}
        },
        "calls": calls,
        "source_coverage_certified": False,
    }
    (args.output / "live_snapshot.json").write_text(
        json.dumps(snapshot, ensure_ascii=False, indent=2), encoding="utf-8")
    write_csv(args.output / "live_call_metrics.csv",
              ["operation", "requested_ids", "returned_rows", "elapsed_seconds"], calls)
    print(json.dumps({key: value for key, value in snapshot.items()
                      if key not in {"history_latest_by_id", "rule_detections_by_equipment", "calls"}},
                     sort_keys=True))


if __name__ == "__main__":
    main()
