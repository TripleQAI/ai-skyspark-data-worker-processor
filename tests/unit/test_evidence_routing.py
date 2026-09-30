from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from ingestion.config.loader import resolve_config, resolve_config_documents
from ingestion.contracts.config import FeedKind, TargetKind
from ingestion.contracts.jobs import CertifiedInventory, JobCompletion, RawArtifact, SinkReceipt
from ingestion.core.evidence import FeedRoutedEvidenceVerifier, ScopedFeedRoutedEvidenceVerifier
from ingestion.core.job_config import ScopedJobConfigResolver
from ingestion.core.planner import plan_run


ROOT = Path(__file__).resolve().parents[2]


def test_pinned_target_flag_selects_physical_verifier():
    config = resolve_config(
        ROOT / "config/profiles/default.yaml",
        ROOT / "config/bindings/example-local.yaml",
        ROOT / "config/manifests/plugins.yaml",
        environment="local",
    )
    inventory = CertifiedInventory.model_validate_json(
        (ROOT / "local/fixtures/inventory.json").read_text(encoding="utf-8")
    )
    end = datetime(2026, 9, 26, 15, 5, tzinfo=timezone.utc)
    _, history_jobs = plan_run(
        config, FeedKind.HISTORY, end, inventory=inventory,
        window_start=end - timedelta(minutes=5), window_end=end,
    )
    job = history_jobs[0]
    completion = JobCompletion(
        raw=RawArtifact(
            job_id=job.job_id, object_key="raw/key", checksum="raw", byte_count=1,
        ),
        sink=SinkReceipt(
            job_id=job.job_id, sink_kind="timescale", batch_key="batch/key",
            row_count=0, checksum="sink",
        ),
        completed_scope=job.scope_ids,
    )

    class Spy:
        called = False

        def verify(self, job, completion):
            self.called = True

    s3 = Spy()
    timescale = Spy()
    router = FeedRoutedEvidenceVerifier(
        config, {TargetKind.S3: s3, TargetKind.TIMESCALE: timescale}
    )
    router.verify(job, completion)
    assert timescale.called and not s3.called
    with pytest.raises(ValueError, match="completion target differs"):
        router.verify(
            job, completion.model_copy(update={
                "sink": completion.sink.model_copy(update={"sink_kind": "s3"})
            }),
        )


def test_shared_worker_routes_each_project_by_its_own_target_flag():
    first = resolve_config(
        ROOT / "config/profiles/default.yaml",
        ROOT / "config/bindings/example-local.yaml",
        ROOT / "config/manifests/plugins.yaml",
        environment="local",
    )
    profile = first.profile.model_dump(mode="json")
    profile["feeds"]["metadata"]["target"] = "timescale"
    binding = first.binding.model_dump(mode="json")
    binding["project_id"] = "second-project"
    second = resolve_config_documents(
        profile, binding, first.manifest.model_dump(mode="json"),
        environment="local",
    )

    class Registry:
        def load(self, *, config_hash, tenant_id, project_id):
            return {first.config_hash: first, second.config_hash: second}[config_hash]

    resolver = ScopedJobConfigResolver(Registry(), cache_limit=1)

    class Spy:
        def __init__(self):
            self.jobs = []

        def verify(self, job, completion):
            self.jobs.append(job.job_id)

    s3, timescale = Spy(), Spy()
    router = ScopedFeedRoutedEvidenceVerifier(
        resolver, {TargetKind.S3: s3, TargetKind.TIMESCALE: timescale}
    )
    for config, sink_kind in ((first, "s3"), (second, "timescale")):
        _, jobs = plan_run(
            config, FeedKind.METADATA,
            datetime(2026, 9, 27, 3, tzinfo=timezone.utc),
        )
        job = jobs[0]
        completion = JobCompletion(
            raw=RawArtifact(job_id=job.job_id, object_key="raw/key", checksum="raw", byte_count=1),
            sink=SinkReceipt(job_id=job.job_id, sink_kind=sink_kind, batch_key="batch/key", row_count=0, checksum="sink"),
            completed_scope=(job.site_ref,),
        )
        router.verify(job, completion)
    assert len(s3.jobs) == len(timescale.jobs) == 1
