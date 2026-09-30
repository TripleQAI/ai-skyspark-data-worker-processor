"""Explicit read-only pilot probe; no ingestion or certification side effects."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import boto3

from ingestion.config.loader import _load_yaml
from ingestion.contracts.config import SourceBinding
from ingestion.source_probe import (
    PhableProbeClient, PilotScope, approved_api_url, run_probe,
)


def _credentials(secret_ref: str) -> tuple[str, str]:
    prefix = "aws-secretsmanager://"
    if not secret_ref.startswith(prefix):
        raise ValueError("pilot probe requires an AWS Secrets Manager reference")
    arn = secret_ref[len(prefix):]
    parts = arn.split(":", 6)
    if len(parts) != 7 or parts[:3] != ["arn", "aws", "secretsmanager"] or not parts[3]:
        raise ValueError("pilot secret reference must contain a full Secrets Manager ARN")
    response = boto3.client("secretsmanager", region_name=parts[3]).get_secret_value(
        SecretId=arn
    )
    payload = json.loads(response["SecretString"])
    if not isinstance(payload, dict):
        raise ValueError("pilot secret must be a JSON object")
    username, password = payload.get("username"), payload.get("password")
    if not isinstance(username, str) or not username or not isinstance(password, str) or not password:
        raise ValueError("pilot secret must have nonempty username and password")
    return username, password


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Read-only SkySpark source-contract probe")
    parser.add_argument("--binding", required=True, type=Path)
    parser.add_argument("--scope", required=True, type=Path)
    parser.add_argument("--report", type=Path)
    parser.add_argument("--execute", action="store_true", help="make bounded source reads")
    args = parser.parse_args(argv)
    try:
        binding = SourceBinding.model_validate(_load_yaml(args.binding))
        scope = PilotScope.model_validate_json(args.scope.read_text(encoding="utf-8"))
        url = approved_api_url(binding, scope)
        if not args.execute:
            print(json.dumps({
                "mode": "dry_run",
                "tenant_id": binding.tenant_id,
                "project_id": binding.project_id,
                "site_ref": scope.site_ref,
                "planned_calls": 2 + len(scope.history_batch_sizes) + 2 * len(scope.rules_batch_sizes),
                "source_values_included": False,
            }, sort_keys=True))
            return 0
        if args.report is None:
            raise ValueError("--report is required for an executed probe")
        username, password = _credentials(binding.secret_ref)
        try:
            from phable import open_haystack_client
        except ImportError as exc:
            raise RuntimeError('install the optional source dependency with pip install ".[source]"') from exc
        with open_haystack_client(url, username, password) as source_client:
            report = run_probe(binding, scope, PhableProbeClient(source_client))
        args.report.parent.mkdir(parents=True, exist_ok=True)
        with args.report.open("x", encoding="utf-8") as stream:
            json.dump(report, stream, indent=2, sort_keys=True)
            stream.write("\n")
        print(json.dumps({
            "report": str(args.report),
            "calls": len(report["calls"]),
            "all_calls_succeeded": report["all_calls_succeeded"],
        }, sort_keys=True))
        return 0 if report["all_calls_succeeded"] else 2
    except Exception as exc:
        # Avoid leaking endpoint, IDs, source rows, or credentials from library exceptions.
        print(f"source probe could not run: {type(exc).__name__}", file=sys.stderr)
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
