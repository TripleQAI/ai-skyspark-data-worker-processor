"""Portable ASL definition for scheduled planning and durable coverage polling."""

from __future__ import annotations

import re

from ingestion.contracts.resources import ResourceConfig


_LAMBDA_ARN = re.compile(
    r"^arn:aws[a-z-]*:lambda:[a-z0-9-]+:[0-9]{12}:function:[A-Za-z0-9_-]+(?::[A-Za-z0-9_-]+)?$"
)


def build_workflow_definition(
    resources: ResourceConfig, *, planner_arn: str, status_arn: str,
    inventory_arn: str,
    planning_allowance_seconds: int = 600,
    inventory_allowance_seconds: int = 900,
) -> dict[str, object]:
    """Build one Standard workflow shared by the three configured feeds."""
    if not all(_LAMBDA_ARN.fullmatch(value) for value in (planner_arn, status_arn, inventory_arn)):
        raise ValueError("planner, status, and inventory must be Lambda function ARNs")
    if planning_allowance_seconds < 1 or planning_allowance_seconds > 3600:
        raise ValueError("planning allowance must be between 1 and 3600 seconds")
    if inventory_allowance_seconds < 1 or inventory_allowance_seconds > 900:
        raise ValueError("inventory allowance must be between 1 and 900 seconds")
    retry = [{
        "ErrorEquals": [
            "Lambda.ServiceException", "Lambda.AWSLambdaException",
            "Lambda.SdkClientException", "Lambda.TooManyRequestsException",
        ],
        "IntervalSeconds": 2,
        "MaxAttempts": 3,
        "BackoffRate": 2,
    }]
    status_payload = {
        f"{field}.$": f"$.plan.run.{field}"
        for field in ("run_id", "tenant_id", "project_id", "config_hash", "feed")
    }
    status_payload["lookback_run_ids.$"] = "$.plan.run.lookback_run_ids"
    return {
        "Comment": "Plan a scheduled SkySpark run and wait for durable certification",
        "StartAt": "PlanRun",
        "TimeoutSeconds": max(resources.workflow.max_run_seconds.values())
        + planning_allowance_seconds + inventory_allowance_seconds,
        "States": {
            "PlanRun": {
                "Type": "Task",
                "Resource": "arn:aws:states:::lambda:invoke",
                "Parameters": {"FunctionName": planner_arn, "Payload.$": "$"},
                "ResultSelector": {"run.$": "$.Payload"},
                "ResultPath": "$.plan",
                "Retry": retry,
                "Next": "WaitForCoverage",
            },
            "WaitForCoverage": {
                "Type": "Wait", "Seconds": resources.workflow.poll_seconds,
                "Next": "ReadRunStatus",
            },
            "ReadRunStatus": {
                "Type": "Task",
                "Resource": "arn:aws:states:::lambda:invoke",
                "Parameters": {"FunctionName": status_arn, "Payload": status_payload},
                "ResultSelector": {"status.$": "$.Payload"},
                "ResultPath": "$.progress",
                "Retry": retry,
                "Next": "EvaluateCoverage",
            },
            "EvaluateCoverage": {
                "Type": "Choice",
                "Choices": [
                    {"Variable": "$.progress.status.state", "StringEquals": "certified", "Next": "ChooseCertifiedFeed"},
                    {"Variable": "$.progress.status.state", "StringEquals": "partial", "Next": "Partial"},
                    {"Variable": "$.progress.status.state", "StringEquals": "blocked", "Next": "Blocked"},
                    {"Variable": "$.progress.status.state", "StringEquals": "pending", "Next": "WaitForCoverage"},
                ],
                "Default": "UnexpectedStatus",
            },
            "ChooseCertifiedFeed": {
                "Type": "Choice",
                "Choices": [{
                    "Variable": "$.plan.run.feed", "StringEquals": "metadata",
                    "Next": "PublishMetadataInventory",
                }],
                "Default": "Certified",
            },
            "PublishMetadataInventory": {
                "Type": "Task",
                "Resource": "arn:aws:states:::lambda:invoke",
                "Parameters": {"FunctionName": inventory_arn, "Payload": status_payload},
                "ResultSelector": {"inventory.$": "$.Payload"},
                "ResultPath": "$.published_inventory",
                "Retry": retry,
                "Next": "Certified",
            },
            "Certified": {"Type": "Succeed"},
            "Partial": {"Type": "Fail", "Error": "PartialCoverage", "Cause": "Some sites were not certified"},
            "Blocked": {"Type": "Fail", "Error": "BlockedCoverage", "Cause": "Run could not certify any sites"},
            "UnexpectedStatus": {"Type": "Fail", "Error": "UnexpectedStatus", "Cause": "Status reader returned an unknown state"},
        },
    }
