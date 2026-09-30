"""The local source fixture enforces scope and models wide history grids."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from threading import Thread
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import pytest


ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location("local_source_fixture", ROOT / "local/fixture/server.py")
assert SPEC and SPEC.loader
fixture_server = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(fixture_server)


def test_fixture_contract_is_bounded_and_equipment_scoped():
    data = fixture_server.load_fixture(ROOT / "local/fixture/data.json")
    history = fixture_server.grid(data, "demoSiteA", "history", {
        "point_ids": ["point-a-1", "point-a-2"],
        "window_start": "2026-09-28T10:00:00Z",
        "window_end": "2026-09-28T10:05:00Z",
    })
    assert [column["name"] for column in history["cols"]] == [
        "ts", "point-a-1", "point-a-2",
    ]
    assert history["rows"] == [
        {"ts": "2026-09-28T10:00:00Z", "point-a-1": 21.5},
        {"ts": "2026-09-28T10:02:00Z", "point-a-2": True},
    ]
    assert fixture_server.grid(data, "demoSiteA", "rules", {
        "equipment_ids": ["equip-a-2"], "day": "2026-09-27",
        "timezone": "UTC", "window_start": "2026-09-27T00:00:00Z",
        "window_end": "2026-09-28T00:00:00Z",
    })["rows"] == []
    assert len(fixture_server.grid(data, "demoSiteA", "rules", {
        "equipment_ids": ["equip-a-1"], "day": "2026-09-27",
        "timezone": "UTC", "window_start": "2026-09-27T00:00:00Z",
        "window_end": "2026-09-28T00:00:00Z",
    })["rows"]) == 1
    with pytest.raises(ValueError, match="known IDs"):
        fixture_server.grid(data, "demoSiteA", "history", {
            "point_ids": ["point-b-1"],
            "window_start": "2026-09-28T10:00:00Z",
            "window_end": "2026-09-28T10:05:00Z",
        })


def test_fixture_http_routes_are_read_only_and_bounded():
    data = fixture_server.load_fixture(ROOT / "local/fixture/data.json")
    server = fixture_server.ThreadingHTTPServer(("127.0.0.1", 0), fixture_server.handler_for(data))
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    root = f"http://127.0.0.1:{server.server_port}"
    try:
        with urlopen(root + "/api/demoSiteB/fixtures/equipment", timeout=2) as response:
            payload = json.load(response)
        assert payload["meta"]["project"] == "demo-project"
        assert [row["id"] for row in payload["rows"]] == ["equip-b-1"]
        request = Request(
            root + "/api/demoSiteB/fixtures/history",
            data=json.dumps({
                "point_ids": ["point-b-1"],
                "window_start": "2026-09-28T10:01:00Z",
                "window_end": "2026-09-28T10:02:00Z",
            }).encode(),
            headers={"Content-Type": "application/json"},
        )
        with urlopen(request, timeout=2) as response:
            assert json.load(response)["rows"][0]["point-b-1"] == 20.0
        with pytest.raises(HTTPError) as error:
            urlopen(root + "/api/unknown/fixtures/points", timeout=2)
        assert error.value.code == 404
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
