"""Render a reviewed Step Functions Standard ASL definition without AWS writes."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from ingestion.contracts.resources import load_resources
from ingestion.core.workflow import build_workflow_definition


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="skyspark-workflow")
    parser.add_argument("--resources", type=Path, required=True)
    parser.add_argument("--planner-arn", required=True)
    parser.add_argument("--status-arn", required=True)
    parser.add_argument("--inventory-arn", required=True)
    args = parser.parse_args(argv)
    definition = build_workflow_definition(
        load_resources(args.resources),
        planner_arn=args.planner_arn,
        status_arn=args.status_arn,
        inventory_arn=args.inventory_arn,
    )
    print(json.dumps(definition, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
