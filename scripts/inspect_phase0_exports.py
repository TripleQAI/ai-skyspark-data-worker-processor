"""Read-only shape and cross-reference report for four SkySpark CSV exports.

The report contains counts and file hashes, never source IDs or row values.
CSV exports are not source-query completeness receipts.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path


REQUIRED = {
    "equipment": {"fsk", "id", "siteRef"},
    "points": {"fsk", "id", "siteRef", "equipRef", "his"},
    "rules": {"fsk", "targetRef", "ruleRef", "date"},
    "history": {"fsk", "point_id", "ts"},
}
MAX_BYTES = 67_108_864
MAX_ROWS = 1_000_000


def _read(path: Path, required: set[str]) -> tuple[list[dict[str, str]], list[str], str]:
    if not path.is_file() or path.stat().st_size > MAX_BYTES:
        raise ValueError("source export is missing or exceeds the 64 MiB inspection cap")
    with path.open("rb") as stream:
        digest = hashlib.file_digest(stream, "sha256").hexdigest()
    with path.open("r", encoding="utf-8-sig", newline="") as stream:
        reader = csv.DictReader(stream)
        headers = reader.fieldnames or []
        if len(headers) != len(set(headers)) or not required.issubset(headers):
            raise ValueError("source export headers are missing or duplicated")
        rows: list[dict[str, str]] = []
        for row in reader:
            if len(rows) >= MAX_ROWS or None in row:
                raise ValueError("source export has too many or malformed rows")
            rows.append({key: value or "" for key, value in row.items()})
    return rows, headers, digest


def inspect_exports(
    equipment: Path, points: Path, rules: Path, history: Path,
) -> dict[str, object]:
    files = {
        "equipment": equipment,
        "points": points,
        "rules": rules,
        "history": history,
    }
    data = {kind: _read(path, REQUIRED[kind]) for kind, path in files.items()}
    equips = data["equipment"][0]
    point_rows = data["points"][0]
    rule_rows = data["rules"][0]
    history_rows = data["history"][0]
    equipment_ids = {row["id"].strip() for row in equips if row["id"].strip()}
    point_ids = {row["id"].strip() for row in point_rows if row["id"].strip()}
    rule_targets = [row["targetRef"].strip() for row in rule_rows]
    history_points = [row["point_id"].strip() for row in history_rows]
    history_kinds = Counter(
        kind for row in history_rows
        for kind in ("val_bool", "val_str", "val_num", "val_na")
        if row.get(kind, "").strip()
    )
    valid_times = []
    invalid_times = 0
    for row in history_rows:
        try:
            value = datetime.fromisoformat(row["ts"].strip().replace("Z", "+00:00"))
            if value.tzinfo is None:
                raise ValueError("timezone missing")
            valid_times.append(value.astimezone(timezone.utc))
        except ValueError:
            invalid_times += 1
    candidate_rule_keys = [
        (row["targetRef"], row["ruleRef"], row["date"]) for row in rule_rows
    ]
    return {
        "schema_version": 1,
        "source_values_included": False,
        "certified": False,
        "certification_reason": "exports lack query-completeness receipts",
        "files": {
            kind: {
                "name": path.name,
                "sha256": data[kind][2],
                "rows": len(data[kind][0]),
                "columns": data[kind][1],
            }
            for kind, path in files.items()
        },
        "equipment": {
            "unique_ids": len(equipment_ids),
            "missing_or_duplicate_ids": len(equips) - len(equipment_ids),
        },
        "points": {
            "unique_ids": len(point_ids),
            "missing_or_duplicate_ids": len(point_rows) - len(point_ids),
            "site_level_rows": sum(
                bool(row["siteRef"].strip()) and not row["equipRef"].strip()
                for row in point_rows
            ),
            "missing_site_and_equipment_rows": sum(
                not row["siteRef"].strip() and not row["equipRef"].strip()
                for row in point_rows
            ),
        },
        "rules": {
            "rows_with_known_equipment": sum(target in equipment_ids for target in rule_targets),
            "rows_with_unknown_equipment": sum(target not in equipment_ids for target in rule_targets),
            "distinct_target_equipment": len(set(rule_targets)),
            "duplicate_candidate_identity_rows": len(candidate_rule_keys) - len(set(candidate_rule_keys)),
            "has_explicit_revision_or_closure_column": bool(
                {"revision", "closed", "eventId", "detectionId"} & set(data["rules"][1])
            ),
        },
        "history": {
            "rows_with_known_point": sum(point in point_ids for point in history_points),
            "rows_with_unknown_point": sum(point not in point_ids for point in history_points),
            "distinct_points": len(set(history_points)),
            "typed_value_column_counts": dict(history_kinds),
            "invalid_or_timezone_free_timestamps": invalid_times,
            "earliest_utc": min(valid_times).isoformat() if valid_times else None,
            "latest_utc": max(valid_times).isoformat() if valid_times else None,
        },
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Inspect source CSV shapes without certifying them")
    for kind in ("equipment", "points", "rules", "history"):
        parser.add_argument(f"--{kind}", required=True, type=Path)
    args = parser.parse_args(argv)
    report = inspect_exports(args.equipment, args.points, args.rules, args.history)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
