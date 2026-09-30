"""ECS agent endpoint adapter for task scale-in protection."""

from __future__ import annotations

import ipaddress
import json
from typing import Any
from urllib.parse import urlsplit
from urllib.request import ProxyHandler, Request, build_opener

from ingestion.core.task_protection import TaskProtectionError


class ECSAgentTaskProtection:
    def __init__(
        self, agent_uri: str, *, timeout_seconds: float,
        opener: Any | None = None,
    ):
        parsed = urlsplit(agent_uri)
        try:
            address = ipaddress.ip_address(parsed.hostname or "")
        except ValueError as exc:
            raise ValueError("ECS agent URI must use a link-local IP address") from exc
        if (
            parsed.scheme != "http" or not (address.is_link_local or address.is_loopback)
            or parsed.username or parsed.password or parsed.path not in {"", "/"}
            or parsed.query or parsed.fragment or not 0 < timeout_seconds <= 10
        ):
            raise ValueError("invalid ECS agent URI or timeout")
        self._url = agent_uri.rstrip("/") + "/task-protection/v1/state"
        self._timeout = timeout_seconds
        self._opener = opener or build_opener(ProxyHandler({}))

    def set_protection(self, enabled: bool, *, expires_minutes: int) -> None:
        body: dict[str, object] = {"ProtectionEnabled": enabled}
        if enabled:
            body["ExpiresInMinutes"] = expires_minutes
        request = Request(
            self._url, data=json.dumps(body, separators=(",", ":")).encode("utf-8"),
            headers={"Content-Type": "application/json"}, method="PUT",
        )
        with self._opener.open(request, timeout=self._timeout) as response:
            if response.status != 200:
                raise TaskProtectionError("ECS agent refused task protection update")
            raw = response.read(8193)
        if len(raw) > 8192:
            raise TaskProtectionError("ECS agent response exceeded limit")
        try:
            payload = json.loads(raw)
            confirmed = payload["protection"]["ProtectionEnabled"]
        except (ValueError, KeyError, TypeError) as exc:
            raise TaskProtectionError("ECS agent response is invalid") from exc
        if confirmed is not enabled:
            raise TaskProtectionError("ECS agent did not confirm protection state")
