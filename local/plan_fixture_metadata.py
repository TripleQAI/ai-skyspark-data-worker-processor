"""One-shot local metadata planner; scheduling time is supplied or UTC now."""

from __future__ import annotations

from datetime import datetime, timezone
import os

from ingestion.control_cli import main as control_main


def main() -> int:
    scheduled_at = os.environ.get("PLANNER_SCHEDULED_AT") or datetime.now(timezone.utc).isoformat()
    return control_main([
        "persist-plan",
        "--profile", os.environ["PLANNER_PROFILE_FILE"],
        "--binding", os.environ["PLANNER_BINDING_FILE"],
        "--manifest", os.environ["PLANNER_MANIFEST_FILE"],
        "--environment", os.environ["PLANNER_ENVIRONMENT"],
        "--resources", os.environ["PLANNER_RESOURCES_FILE"],
        "--feed", os.environ["PLANNER_FEED"],
        "--scheduled-at", scheduled_at,
    ])


if __name__ == "__main__":
    raise SystemExit(main())
