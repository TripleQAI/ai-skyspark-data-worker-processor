import json
from pathlib import Path

from ingestion.contracts.resources import load_resources
from ingestion.core.workflow import build_workflow_definition


ROOT = Path(__file__).resolve().parents[2]
PLANNER = "arn:aws:lambda:us-east-1:123456789012:function:sample-planner"
STATUS = "arn:aws:lambda:us-east-1:123456789012:function:sample-status"
INVENTORY = "arn:aws:lambda:us-east-1:123456789012:function:sample-inventory"


def test_terraform_asl_template_matches_python_generator():
    resources = load_resources(ROOT / "local/resources.yaml")
    template = (ROOT / "infra/orchestration/workflow.asl.json.tftpl").read_text(
        encoding="utf-8"
    )
    rendered = (template
        .replace("${planner_arn}", PLANNER)
        .replace("${status_arn}", STATUS)
        .replace("${inventory_arn}", INVENTORY)
        .replace("${poll_seconds}", str(resources.workflow.poll_seconds))
        .replace("${timeout_seconds}", str(max(resources.workflow.max_run_seconds.values()) + 600 + 900)))
    assert json.loads(rendered) == build_workflow_definition(
        resources, planner_arn=PLANNER, status_arn=STATUS,
        inventory_arn=INVENTORY,
    )
