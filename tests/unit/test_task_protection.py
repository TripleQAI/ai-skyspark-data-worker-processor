import json
import threading

import pytest

from ingestion.adapters.aws.ecs_task_protection import ECSAgentTaskProtection
from ingestion.core.task_protection import TaskProtectionError, TaskProtectionManager


def test_ecs_agent_adapter_sends_confirmed_protection_updates():
    class Response:
        status = 200

        def __init__(self, enabled):
            self.enabled = enabled

        def __enter__(self):
            return self

        def __exit__(self, *_):
            pass

        def read(self, limit):
            return json.dumps({
                "protection": {"ProtectionEnabled": self.enabled}
            }).encode()

    class Opener:
        requests = []

        def open(self, request, timeout):
            body = json.loads(request.data)
            self.requests.append((request.full_url, request.get_method(), body, timeout))
            return Response(body["ProtectionEnabled"])

    opener = Opener()
    adapter = ECSAgentTaskProtection(
        "http://169.254.170.2", timeout_seconds=3, opener=opener,
    )
    adapter.set_protection(True, expires_minutes=10)
    adapter.set_protection(False, expires_minutes=10)
    assert opener.requests == [
        ("http://169.254.170.2/task-protection/v1/state", "PUT",
         {"ProtectionEnabled": True, "ExpiresInMinutes": 10}, 3),
        ("http://169.254.170.2/task-protection/v1/state", "PUT",
         {"ProtectionEnabled": False}, 3),
    ]
    with pytest.raises(ValueError, match="link-local"):
        ECSAgentTaskProtection("http://example.com", timeout_seconds=3)
    with pytest.raises(ValueError, match="invalid ECS agent URI"):
        ECSAgentTaskProtection("http://169.254.170.2/unreviewed", timeout_seconds=3)


def test_reference_count_holds_protection_until_every_slot_finishes():
    class Client:
        calls = []

        def set_protection(self, enabled, *, expires_minutes):
            self.calls.append((enabled, expires_minutes))

    client = Client()
    manager = TaskProtectionManager(client, expires_minutes=10, refresh_seconds=60)
    try:
        manager.acquire()
        manager.acquire()
        manager.release()
        assert client.calls == [(True, 10)]
        manager.release()
        assert client.calls == [(True, 10), (False, 10)]
    finally:
        manager.close()


def test_failed_refresh_sets_cancellation_and_refuses_new_work():
    class Client:
        enabled_calls = 0

        def set_protection(self, enabled, *, expires_minutes):
            if enabled:
                self.enabled_calls += 1
                if self.enabled_calls == 2:
                    raise TimeoutError("agent failed")

    manager = TaskProtectionManager(
        Client(), expires_minutes=1, refresh_seconds=0.02,
    )
    try:
        manager.acquire()
        assert manager.lost.wait(1)
        with pytest.raises(TaskProtectionError, match="unavailable"):
            manager.acquire()
        manager.release()
    finally:
        manager.close()


def test_long_job_renews_protection_before_expiry():
    renewed = threading.Event()

    class Client:
        true_calls = 0

        def set_protection(self, enabled, *, expires_minutes):
            if enabled:
                self.true_calls += 1
                if self.true_calls == 2:
                    renewed.set()

    client = Client()
    manager = TaskProtectionManager(
        client, expires_minutes=1, refresh_seconds=0.02,
    )
    try:
        manager.acquire()
        assert renewed.wait(1)
        assert not manager.lost.is_set()
        manager.release()
    finally:
        manager.close()


def test_initial_protection_failure_does_not_admit_work():
    class Client:
        def set_protection(self, enabled, *, expires_minutes):
            raise OSError("agent unavailable")

    manager = TaskProtectionManager(Client(), expires_minutes=10, refresh_seconds=60)
    try:
        with pytest.raises(TaskProtectionError, match="cannot protect"):
            manager.acquire()
        assert manager.lost.is_set()
    finally:
        manager.close()
