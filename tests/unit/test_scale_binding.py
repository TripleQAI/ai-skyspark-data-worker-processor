from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest
import yaml

from ingestion.config.loader import resolve_config
from ingestion.contracts.config import SourceBinding


ROOT = Path(__file__).resolve().parents[2]
DEV = ROOT / "config" / "environments" / "aws-dev"
_spec = importlib.util.spec_from_file_location("generate_scale_binding", ROOT / "scripts" / "generate_scale_binding.py")
generator = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = generator  # pydantic resolves postponed annotations via sys.modules
_spec.loader.exec_module(generator)


def _scope(tmp_path: Path, **overrides) -> Path:
    data = yaml.safe_load((DEV / "scope.yaml").read_text(encoding="utf-8"))
    data.update({
        "endpoint": "http://skyspark.example:8888/api/demo",
        "secret_ref": "aws-secretsmanager://arn:aws:secretsmanager:us-east-1:111122223333:secret:skyspark-dev",
        "source_site_ref": "p:demo:r:site-1",
        "rules_source_timezone": "America/New_York",
        "rules_source_tz_tags": ["New_York"],
        **overrides,
    })
    path = tmp_path / "scope.yaml"
    path.write_text(yaml.safe_dump(data), encoding="utf-8")
    (tmp_path / "profile.yaml").write_text((DEV / "profile.yaml").read_text(encoding="utf-8"), encoding="utf-8")
    return path


def test_dev_scope_expands_to_1000_replica_sites_that_resolve(tmp_path, capsys):
    scope = _scope(tmp_path)
    assert generator.main(["--scope", str(scope)]) == 0
    output = tmp_path / "private" / "binding.yaml"
    binding = SourceBinding.model_validate(yaml.safe_load(output.read_text(encoding="utf-8")))
    assert len(binding.approved_sites) == 1000
    assert set(binding.approved_sites.values()) == {"p:demo:r:site-1"}
    assert min(binding.approved_sites) == "dev-site-0001" and max(binding.approved_sites) == "dev-site-1000"
    assert binding.source_replica.equipment_per_site == 200
    assert binding.source_replica.points_per_equipment == 50
    config = resolve_config(tmp_path / "profile.yaml", output, DEV / "plugins.yaml", environment="aws")
    assert config.binding == binding
    report = capsys.readouterr().out
    assert '"total_points": 10000000' in report and '"history": 20000' in report


def test_placeholders_block_generation_unless_shape_check(tmp_path, capsys):
    scope = _scope(tmp_path, source_site_ref="REPLACE_WITH_SOURCE_SITE_REF")
    assert generator.main(["--scope", str(scope)]) == 2
    assert "source_site_ref" in capsys.readouterr().err
    assert generator.main(["--scope", str(scope), "--allow-placeholders"]) == 0


def test_replica_site_refs_must_not_contain_dots(tmp_path):
    scope = _scope(tmp_path, sites={"count": 2, "first_index": 1, "ref_format": "dev.site.{index}"})
    with pytest.raises(ValueError, match="replica site references"):
        generator.main(["--scope", str(scope)])
