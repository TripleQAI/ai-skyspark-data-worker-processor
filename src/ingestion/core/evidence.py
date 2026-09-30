"""Select a physical evidence verifier from the pinned target flag."""

from __future__ import annotations

from collections.abc import Mapping

from ingestion.config.loader import EffectiveConfig
from ingestion.contracts.config import TargetKind
from ingestion.contracts.jobs import Job, JobCompletion
from ingestion.core.worker import EvidenceVerifier
from ingestion.core.job_config import ScopedJobConfigResolver


class FeedRoutedEvidenceVerifier:
    def __init__(
        self, config: EffectiveConfig,
        verifiers: Mapping[TargetKind, EvidenceVerifier],
    ):
        self._config = config
        self._verifiers = dict(verifiers)

    def verify(self, job: Job, completion: JobCompletion) -> None:
        if job.config_hash != self._config.config_hash:
            raise ValueError("job configuration is not pinned to this worker")
        target = self._config.profile.feeds[job.feed].target
        if completion.sink.sink_kind != target.value:
            raise ValueError("completion target differs from pinned profile")
        verifier = self._verifiers.get(target)
        if verifier is None:
            raise ValueError(f"no physical verifier for target: {target}")
        verifier.verify(job, completion)


class ScopedFeedRoutedEvidenceVerifier:
    """Select a sink per job from its stored, hash-checked target flag."""

    def __init__(
        self, resolver: ScopedJobConfigResolver,
        verifiers: Mapping[TargetKind, EvidenceVerifier],
    ):
        self._resolver = resolver
        self._verifiers = dict(verifiers)

    def verify(self, job: Job, completion: JobCompletion) -> None:
        config = self._resolver.for_job(job)
        FeedRoutedEvidenceVerifier(config, self._verifiers).verify(job, completion)
