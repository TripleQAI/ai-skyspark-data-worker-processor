from pathlib import Path
from unittest.mock import patch

import pytest
import yaml
from pydantic import ValidationError

from ingestion.config.loader import _UniqueKeyLoader, resolve_config


ROOT = Path(__file__).resolve().parents[2]
PROFILE = ROOT / "config/profiles/default.yaml"
BINDING = ROOT / "config/bindings/example-local.yaml"
MANIFEST = ROOT / "config/manifests/plugins.yaml"


def _resolve(**kwargs):
    return resolve_config(PROFILE, BINDING, MANIFEST, environment="local", **kwargs)


def test_config_hash_is_stable_and_profile_has_three_feeds():
    first = _resolve()
    second = _resolve()
    assert first.config_hash == second.config_hash
    assert len(first.profile.feeds) == 3


def test_unknown_field_is_rejected():
    data = yaml.safe_load(PROFILE.read_text(encoding="utf-8"))
    data["feeds"]["history"]["unreviewed_code"] = "os.system('anything')"
    with patch("ingestion.config.loader._load_yaml", side_effect=[
        data,
        yaml.safe_load(BINDING.read_text(encoding="utf-8")),
        yaml.safe_load(MANIFEST.read_text(encoding="utf-8")),
    ]):
        with pytest.raises(ValidationError):
            _resolve()


def test_unregistered_reader_is_rejected():
    data = yaml.safe_load(PROFILE.read_text(encoding="utf-8"))
    data["feeds"]["rules"]["reader"] = "unreviewed.reader@1"
    with patch("ingestion.config.loader._load_yaml", side_effect=[
        data,
        yaml.safe_load(BINDING.read_text(encoding="utf-8")),
        yaml.safe_load(MANIFEST.read_text(encoding="utf-8")),
    ]):
        with pytest.raises(ValueError, match="unregistered reader"):
            _resolve()


def test_production_rejects_local_secret_but_accepts_configured_http_root():
    with pytest.raises(ValueError, match="local secret"):
        resolve_config(PROFILE, BINDING, MANIFEST, environment="aws")

    binding = yaml.safe_load(BINDING.read_text(encoding="utf-8"))
    binding["endpoint"] = "http://44.221.91.30:8888/api/"
    binding["secret_ref"] = (
        "aws-secretsmanager://arn:aws:secretsmanager:us-east-1:123456789012:secret:pilot"
    )
    with patch("ingestion.config.loader._load_yaml", side_effect=[
        yaml.safe_load(PROFILE.read_text(encoding="utf-8")),
        binding,
        yaml.safe_load(MANIFEST.read_text(encoding="utf-8")),
    ]):
        resolved = resolve_config(PROFILE, BINDING, MANIFEST, environment="aws")
    assert str(resolved.binding.endpoint) == binding["endpoint"]


def test_binding_rejects_absolute_site_uri():
    data = yaml.safe_load(BINDING.read_text(encoding="utf-8"))
    data["approved_sites"]["site-a"] = "https://outside.example/api/"
    with patch("ingestion.config.loader._load_yaml", side_effect=[
        yaml.safe_load(PROFILE.read_text(encoding="utf-8")),
        data,
        yaml.safe_load(MANIFEST.read_text(encoding="utf-8")),
    ]):
        with pytest.raises(ValidationError, match="relative paths"):
            _resolve()


def test_duplicate_yaml_key_is_rejected():
    with pytest.raises(ValueError, match="duplicate configuration key"):
        yaml.load("schema_version: 1\nschema_version: 1\n", Loader=_UniqueKeyLoader)
