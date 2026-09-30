"""Reference-counted ECS task protection across concurrent worker slots."""

from __future__ import annotations

import threading
from typing import Protocol


class TaskProtectionError(RuntimeError):
    """The worker cannot guarantee protection for admitted work."""


class TaskProtectionClient(Protocol):
    def set_protection(self, enabled: bool, *, expires_minutes: int) -> None: ...


class TaskProtectionManager:
    def __init__(
        self, client: TaskProtectionClient, *,
        expires_minutes: int, refresh_seconds: float,
    ):
        if not 1 <= expires_minutes <= 2880:
            raise ValueError("task protection expiry must be 1 to 2880 minutes")
        if not 0 < refresh_seconds < expires_minutes * 30:
            raise ValueError("task protection refresh must be less than half its expiry")
        self._client = client
        self._expires_minutes = expires_minutes
        self._refresh_seconds = refresh_seconds
        self._lock = threading.Lock()
        self._active = 0
        self._protected = False
        self._closed = False
        self._stop = threading.Event()
        self.lost = threading.Event()
        self._thread = threading.Thread(target=self._renew, daemon=True)
        self._thread.start()

    def _renew(self) -> None:
        while not self._stop.wait(self._refresh_seconds):
            with self._lock:
                if self._active and not self.lost.is_set():
                    try:
                        self._client.set_protection(
                            True, expires_minutes=self._expires_minutes
                        )
                    except Exception:
                        self.lost.set()
                        return

    def acquire(self) -> None:
        with self._lock:
            if self._closed or self.lost.is_set():
                raise TaskProtectionError("task protection is unavailable")
            if not self._protected:
                try:
                    self._client.set_protection(
                        True, expires_minutes=self._expires_minutes
                    )
                except Exception as exc:
                    self.lost.set()
                    raise TaskProtectionError("cannot protect ECS task") from exc
                self._protected = True
            self._active += 1

    def release(self) -> None:
        with self._lock:
            if self._active < 1:
                raise RuntimeError("task protection reference underflow")
            self._active -= 1
            if self._active == 0 and self._protected:
                try:
                    self._client.set_protection(False, expires_minutes=self._expires_minutes)
                except Exception as exc:
                    self.lost.set()
                    raise TaskProtectionError("cannot clear ECS task protection") from exc
                self._protected = False

    def close(self) -> None:
        self._stop.set()
        self._thread.join()
        with self._lock:
            if self._active:
                raise RuntimeError("cannot close protection while jobs are active")
            if self._protected:
                self._client.set_protection(False, expires_minutes=self._expires_minutes)
                self._protected = False
            self._closed = True
