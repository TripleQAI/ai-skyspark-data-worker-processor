"""Reconcile project-owned schedules without changing unrelated definitions."""

from __future__ import annotations

import hashlib
import json
from typing import Any

import boto3
from botocore.exceptions import ClientError

from ingestion.core.schedules import ScheduleSpec


_OWNED_FIELDS = (
    "ScheduleExpression", "ScheduleExpressionTimezone", "FlexibleTimeWindow", "State",
)
_PRESERVED_FIELDS = ("ActionAfterCompletion", "StartDate", "EndDate", "KmsKeyArn")
_TARGET_FIELDS = ("Arn", "RoleArn", "Input", "DeadLetterConfig")


class ScheduleDriftError(RuntimeError):
    pass


class SchedulerReconciler:
    def __init__(
        self, *, region_name: str, endpoint_url: str | None = None,
        client: Any | None = None,
    ) -> None:
        self._client = client or boto3.client(
            "scheduler", region_name=region_name, endpoint_url=endpoint_url
        )

    def reconcile(self, specs: tuple[ScheduleSpec, ...], *, apply: bool) -> list[dict[str, str]]:
        outcomes = []
        missing = []
        updates = []
        for spec in specs:
            try:
                current = self._client.get_schedule(Name=spec.name, GroupName=spec.group_name)
            except ClientError as error:
                if error.response.get("Error", {}).get("Code") != "ResourceNotFoundException":
                    raise
                current = None
            if current is None:
                missing.append(spec)
                state = "created" if apply else "missing"
            else:
                if current.get("Description") != spec.request["Description"]:
                    raise ScheduleDriftError(
                        f"schedule {spec.group_name}/{spec.name} is not project-managed"
                    )
                target = current.get("Target", {})
                if set(target) - set(_TARGET_FIELDS) - {"RetryPolicy"}:
                    raise ScheduleDriftError(
                        f"schedule {spec.group_name}/{spec.name} has an unsupported target"
                    )
                changed = any(
                    current.get(field) != spec.request[field] for field in _OWNED_FIELDS
                ) or any(
                    target.get(field) != spec.request["Target"][field]
                    for field in _TARGET_FIELDS
                )
                if changed:
                    update = {
                        field: current[field] for field in _PRESERVED_FIELDS if field in current
                    }
                    update.update(spec.request)
                    update_target = dict(target)
                    update_target.update(spec.request["Target"])
                    update["Target"] = update_target
                    updates.append(update)
                    state = "updated" if apply else "update_needed"
                else:
                    state = "unchanged"
            outcomes.append({"feed": spec.feed.value, "name": spec.name, "state": state})
        # Detect all unmanaged definitions before any AWS mutation. A failed API
        # call can leave earlier changes in place; a rerun safely resumes.
        if apply:
            for spec in missing:
                token = hashlib.sha256(
                    json.dumps(spec.request, sort_keys=True).encode("utf-8")
                ).hexdigest()[:32]
                self._client.create_schedule(**spec.request, ClientToken=token)
            for update in updates:
                token = hashlib.sha256(
                    json.dumps(update, sort_keys=True, default=str).encode("utf-8")
                ).hexdigest()[:32]
                self._client.update_schedule(**update, ClientToken=token)
        return outcomes
