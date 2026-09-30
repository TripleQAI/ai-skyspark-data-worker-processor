"""Bounded read-only JSON fixture for the three SkySpark source contracts.

This is a contract fixture, not a Haystack wire-protocol emulator. It exposes
the source grid shapes needed to develop readers without live credentials.
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


MAX_BODY_BYTES = 32768


class ResponseCapExceeded(ValueError):
    """The fixture source could not return a complete bounded response."""


def load_fixture(path: Path) -> dict:
    data = json.loads(path.read_text(encoding="utf-8"))
    if data.get("schema_version") != 1 or not data.get("project_id"):
        raise ValueError("fixture requires schema_version 1 and project_id")
    if not isinstance(data.get("sites"), dict) or not data["sites"]:
        raise ValueError("fixture requires sites")
    for site_uri, site in data["sites"].items():
        if not site_uri or any(key not in site for key in ("equipment", "points", "history", "rules")):
            raise ValueError("fixture site is incomplete")
    return data


def _utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() != timezone.utc.utcoffset(parsed):
        raise ValueError("UTC timestamp required")
    return parsed


def _ids(body: dict, key: str, known: set[str], maximum: int) -> list[str]:
    values = body.get(key)
    if (
        not isinstance(values, list) or not values or len(values) > maximum
        or any(not isinstance(value, str) or not value for value in values)
        or len(values) != len(set(values)) or not set(values) <= known
    ):
        raise ValueError(f"{key} must contain unique, known IDs within the batch limit")
    return values


def grid(data: dict, site_uri: str, operation: str, body: dict | None = None) -> dict:
    site = data["sites"].get(site_uri)
    if site is None:
        raise KeyError("unknown site")
    body = body or {}
    if operation in {"equipment", "points"}:
        rows = site[operation]
        columns = list(dict.fromkeys(key for row in rows for key in row))
        requested = len(rows)
    elif operation == "history":
        ids = _ids(body, "point_ids", {row["id"] for row in site["points"]}, data["max_batch_ids"])
        start, end = _utc(body.get("window_start")), _utc(body.get("window_end"))
        if start >= end:
            raise ValueError("history window must be nonempty")
        by_ts: dict[str, dict] = {}
        for sample in site["history"]:
            if sample["point_id"] in ids and start <= _utc(sample["ts"]) < end:
                by_ts.setdefault(sample["ts"], {"ts": sample["ts"]})[sample["point_id"]] = sample["value"]
        rows = [by_ts[ts] for ts in sorted(by_ts)]
        columns = ["ts", *ids]
        requested = len(ids)
    elif operation == "rules":
        ids = _ids(body, "equipment_ids", {row["id"] for row in site["equipment"]}, data["max_batch_ids"])
        day = body.get("day")
        source_timezone = body.get("timezone")
        if not isinstance(day, str) or not isinstance(source_timezone, str):
            raise ValueError("rules require an ISO day")
        try:
            parsed_day = date.fromisoformat(day)
            zone = ZoneInfo(source_timezone)
        except (ValueError, ZoneInfoNotFoundError) as exc:
            raise ValueError("rules require an ISO day and IANA timezone") from exc
        start = datetime.combine(parsed_day, time.min, tzinfo=zone).astimezone(timezone.utc)
        end = datetime.combine(parsed_day + timedelta(days=1), time.min, tzinfo=zone).astimezone(timezone.utc)
        if _utc(body.get("window_start")) != start or _utc(body.get("window_end")) != end:
            raise ValueError("rules window must be the complete source-local day")
        rows = [row for row in site["rules"] if row["targetRef"] in ids and row["date"] == day]
        columns = list(dict.fromkeys(key for row in rows for key in row)) or ["targetRef", "ruleRef", "date", "tz", "spark"]
        requested = len(ids)
    else:
        raise KeyError("unknown operation")
    if len(rows) > data["max_response_rows"]:
        raise ResponseCapExceeded("fixture response exceeds row limit")
    result = {
        "meta": {
            "ver": "3.0", "project": data["project_id"], "site": site_uri,
            "synthetic": True, "complete": True, "requested_count": requested,
            "returned_rows": len(rows), "total_rows": len(rows),
            "page_index": 0, "next_page": None,
            "snapshot": hashlib.sha256(json.dumps(data, sort_keys=True).encode()).hexdigest(),
        },
        "cols": [{"name": name} for name in columns],
        "rows": rows,
    }
    if operation == "history":
        result["meta"].update({
            "query_id": hashlib.sha256(json.dumps({
                "project": data["project_id"], "site": site_uri,
                "point_ids": ids, "start": start.isoformat(), "end": end.isoformat(),
                "snapshot": result["meta"]["snapshot"],
            }, sort_keys=True).encode()).hexdigest(),
            "requested_ids": ids, "completed_ids": ids,
            "window_start": start.isoformat(), "window_end": end.isoformat(),
            "truncated": False,
        })
    if operation == "rules":
        result["meta"].update({
            "query_id": hashlib.sha256(json.dumps({
                "project": data["project_id"], "site": site_uri,
                "equipment_ids": ids, "day": day, "timezone": source_timezone,
                "start": start.isoformat(), "end": end.isoformat(),
                "snapshot": result["meta"]["snapshot"],
            }, sort_keys=True).encode()).hexdigest(),
            "requested_ids": ids, "completed_ids": ids,
            "day": day, "timezone": source_timezone,
            "window_start": start.isoformat(), "window_end": end.isoformat(),
            "truncated": False,
        })
    return result


def handler_for(data: dict):
    class Handler(BaseHTTPRequestHandler):
        def _respond(self, status: int, payload: dict) -> None:
            body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _handle(self, method: str) -> None:
            path = urlsplit(self.path).path
            if method == "GET" and path == "/health":
                self._respond(200, {"status": "ok", "project": data["project_id"]})
                return
            parts = path.strip("/").split("/")
            if len(parts) != 4 or parts[0] != "api" or parts[2] != "fixtures":
                self._respond(404, {"error": "unknown route"})
                return
            site_uri, operation = parts[1], parts[3]
            if (operation in {"equipment", "points"} and method != "GET") or (
                operation in {"history", "rules"} and method != "POST"
            ):
                self._respond(405, {"error": "method not allowed"})
                return
            try:
                body = None
                if method == "POST":
                    length = int(self.headers.get("Content-Length", "0"))
                    if length < 1 or length > MAX_BODY_BYTES:
                        raise ValueError("request body exceeds bounds")
                    body = json.loads(self.rfile.read(length))
                    if not isinstance(body, dict):
                        raise ValueError("request body must be an object")
                self._respond(200, grid(data, site_uri, operation, body))
            except KeyError as exc:
                self._respond(404, {"error": str(exc)})
            except ResponseCapExceeded as exc:
                self._respond(413, {"error": str(exc)})
            except (TypeError, ValueError, json.JSONDecodeError) as exc:
                self._respond(400, {"error": str(exc)})

        def do_GET(self) -> None:
            self._handle("GET")

        def do_POST(self) -> None:
            self._handle("POST")

    return Handler


def main() -> None:
    data = load_fixture(Path(os.environ["SKYSPARK_FIXTURE_FILE"]))
    port = int(os.environ.get("PORT", "8080"))
    ThreadingHTTPServer(("0.0.0.0", port), handler_for(data)).serve_forever()


if __name__ == "__main__":
    main()
