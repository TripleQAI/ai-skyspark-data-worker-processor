"""Local validation and deterministic planning commands.

The production queue/control commands are added in later implementation phases.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path

from ingestion.config.loader import resolve_config
from ingestion.contracts.config import FeedKind
from ingestion.contracts.jobs import CertifiedInventory
from ingestion.core.planner import plan_run


def _timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="skyspark-ingestion")
    parser.add_argument("--profile", type=Path, required=True)
    parser.add_argument("--binding", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--environment", choices=("local", "aws"), required=True)
    subcommands = parser.add_subparsers(dest="command", required=True)
    subcommands.add_parser("validate-config")
    plan = subcommands.add_parser("plan")
    plan.add_argument("--feed", choices=[kind.value for kind in FeedKind], required=True)
    plan.add_argument("--scheduled-at", type=_timestamp, required=True)
    plan.add_argument("--inventory", type=Path)
    plan.add_argument("--window-start", type=_timestamp)
    plan.add_argument("--window-end", type=_timestamp)
    args = parser.parse_args(argv)

    config = resolve_config(
        args.profile, args.binding, args.manifest, environment=args.environment
    )
    if args.command == "validate-config":
        print(json.dumps({"profile_id": config.profile.profile_id, "config_hash": config.config_hash}))
        return 0

    inventory = None
    if args.inventory:
        with args.inventory.open("r", encoding="utf-8") as stream:
            inventory = CertifiedInventory.model_validate_json(stream.read())
    run, jobs = plan_run(
        config,
        FeedKind(args.feed),
        args.scheduled_at,
        inventory=inventory,
        window_start=args.window_start,
        window_end=args.window_end,
    )
    print(
        json.dumps(
            {
                "run_id": run.run_id,
                "feed": run.feed.value,
                "job_count": len(jobs),
                "jobs_by_site": {
                    site: sum(job.site_ref == site for job in jobs)
                    for site in sorted(config.binding.approved_sites)
                },
                "job_refs": [job.job_id for job in jobs],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
