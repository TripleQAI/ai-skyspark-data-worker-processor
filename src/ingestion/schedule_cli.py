"""Preview or create reviewed per-project EventBridge schedules."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from ingestion.adapters.aws.scheduler import SchedulerReconciler
from ingestion.config.loader import resolve_config
from ingestion.core.schedules import build_schedule_specs


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="skyspark-schedules")
    parser.add_argument("--profile", type=Path, required=True)
    parser.add_argument("--binding", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--environment", choices=("local", "aws"), required=True)
    parser.add_argument("--config-ref", required=True)
    parser.add_argument("--group-name", required=True)
    parser.add_argument("--state-machine-arn", required=True)
    parser.add_argument("--role-arn", required=True)
    parser.add_argument("--dead-letter-arn", required=True)
    parser.add_argument("--region", required=True)
    parser.add_argument("--enable", action="store_true")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("preview")
    reconcile = commands.add_parser("reconcile")
    reconcile.add_argument("--apply", action="store_true")
    args = parser.parse_args(argv)

    config = resolve_config(
        args.profile, args.binding, args.manifest, environment=args.environment
    )
    specs = build_schedule_specs(
        config, config_ref=args.config_ref, group_name=args.group_name,
        state_machine_arn=args.state_machine_arn, role_arn=args.role_arn,
        dead_letter_arn=args.dead_letter_arn, enabled=args.enable,
    )
    if args.command == "preview":
        print(json.dumps([spec.summary() for spec in specs], sort_keys=True))
        return 0
    result = SchedulerReconciler(
        region_name=args.region,
        endpoint_url=os.environ.get("AWS_ENDPOINT_URL") if args.environment == "local" else None,
    ).reconcile(specs, apply=args.apply)
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
