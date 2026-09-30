from pathlib import Path

import pytest

from ingestion.contracts.resources import RetentionPolicy, load_resources
from ingestion.core.storage_operations import _s3_prefix, retention_preview


ROOT = Path(__file__).resolve().parents[2]


def test_retention_requires_reviewed_values_and_replay_compatibility():
    resources = load_resources(ROOT / "local/resources.yaml")
    with pytest.raises(ValueError, match="not configured"):
        retention_preview(resources)
    too_short = resources.model_copy(update={"retention": RetentionPolicy(
        raw_object_days=30, certified_object_days=400,
        inventory_object_days=400, history_observation_days=730,
    )})
    with pytest.raises(ValueError, match="shorter than approved replay age"):
        retention_preview(too_short)
    reviewed = resources.model_copy(update={"retention": RetentionPolicy(
        raw_object_days=400, certified_object_days=400,
        inventory_object_days=400, history_observation_days=730,
    )})
    preview = retention_preview(reviewed)
    assert preview["applied"] is False
    assert preview["bucket_lifecycle_configurations"][resources.storage.raw_bucket]["Rules"][0]["Filter"] == {
        "Prefix": "raw/",
    }


def test_storage_listing_stops_at_reviewed_cap():
    class S3:
        def list_objects_v2(self, **kwargs):
            if kwargs.get("ContinuationToken"):
                return {"Contents": [{"Size": 5}], "IsTruncated": False}
            return {"Contents": [{"Size": 2}], "IsTruncated": True,
                    "NextContinuationToken": "next"}

    assert _s3_prefix(S3(), "bucket", "raw/", 1) == {
        "objects": 1, "bytes": 2, "complete": False,
    }
    assert _s3_prefix(S3(), "bucket", "raw/", 2) == {
        "objects": 2, "bytes": 7, "complete": True,
    }
