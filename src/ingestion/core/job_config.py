"""Bounded cache of immutable job-scoped, hash-checked configurations."""

from __future__ import annotations

import threading
from collections import OrderedDict
from typing import Protocol

from ingestion.config.loader import EffectiveConfig
from ingestion.contracts.jobs import Job


class ConfigRegistry(Protocol):
    def load(self, *, config_hash: str, tenant_id: str, project_id: str) -> EffectiveConfig: ...


class ScopedJobConfigResolver:
    def __init__(self, registry: ConfigRegistry, *, cache_limit: int = 128):
        if cache_limit < 1:
            raise ValueError("config cache limit must be positive")
        self._registry = registry
        self._limit = cache_limit
        self._cache: OrderedDict[str, EffectiveConfig] = OrderedDict()
        self._lock = threading.Lock()

    def for_job(self, job: Job) -> EffectiveConfig:
        with self._lock:
            config = self._cache.get(job.config_hash)
            if config is not None:
                self._cache.move_to_end(job.config_hash)
        if config is None:
            config = self._registry.load(
                config_hash=job.config_hash,
                tenant_id=job.tenant_id,
                project_id=job.project_id,
            )
            with self._lock:
                self._cache[job.config_hash] = config
                self._cache.move_to_end(job.config_hash)
                if len(self._cache) > self._limit:
                    self._cache.popitem(last=False)
        if (
            config.config_hash != job.config_hash
            or config.binding.tenant_id != job.tenant_id
            or config.binding.project_id != job.project_id
            or job.site_ref not in config.binding.approved_sites
            or job.feed not in config.profile.feeds
        ):
            raise ValueError("job is outside its stored configuration scope")
        return config
