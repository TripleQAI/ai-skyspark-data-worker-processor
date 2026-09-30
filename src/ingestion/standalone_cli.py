"""Run a registered, non-certifying Python utility with control-store audit."""

import argparse
import json
import os
from pathlib import Path

from ingestion.adapters.control.standalone_runs import PostgresStandaloneRunRepository
from ingestion.adapters.standalone_scripts import RegisteredUtilityRunner
from ingestion.config.loader import resolve_config
from ingestion.contracts.standalone import UtilityContext


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="skyspark-standalone-script")
    parser.add_argument("--profile", type=Path, required=True)
    parser.add_argument("--binding", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--environment", choices=("local", "aws"), required=True)
    parser.add_argument("--script-root", type=Path, required=True)
    parser.add_argument("--context", type=Path, required=True)
    parser.add_argument("--control-dsn-env", default="CONTROL_DATABASE_URL")
    args = parser.parse_args(argv)
    config = resolve_config(
        args.profile, args.binding, args.manifest, environment=args.environment,
    )
    dsn = os.environ.get(args.control_dsn_env)
    if not dsn:
        parser.error(f"{args.control_dsn_env} must contain the control database DSN")
    context = UtilityContext.model_validate_json(args.context.read_text(encoding="utf-8"))
    runner = RegisteredUtilityRunner(
        config, args.script_root, PostgresStandaloneRunRepository(dsn),
    )
    result = runner.run(context)
    print(json.dumps({"run_id": result.run_id, "status": "completed",
                      "artifact_count": len(result.artifacts)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
