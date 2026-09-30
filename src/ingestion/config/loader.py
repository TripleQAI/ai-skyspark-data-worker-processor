"""Resolve reviewed profile, binding, and plugin manifest files."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from ingestion.contracts.config import PipelineProfile, PluginManifest, SourceBinding


class _UniqueKeyLoader(yaml.SafeLoader):
    pass


def _construct_mapping(loader: _UniqueKeyLoader, node: yaml.MappingNode) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=True)
        if not isinstance(key, str):
            raise ValueError("configuration keys must be strings")
        if key in result:
            raise ValueError(f"duplicate configuration key: {key}")
        result[key] = loader.construct_object(value_node, deep=True)
    return result


_UniqueKeyLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, _construct_mapping
)


def _load_yaml(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as stream:
        data = yaml.load(stream, Loader=_UniqueKeyLoader)
    if not isinstance(data, dict):
        raise ValueError(f"{path} must contain a YAML mapping")
    return data


@dataclass(frozen=True, slots=True)
class EffectiveConfig:
    profile: PipelineProfile
    binding: SourceBinding
    manifest: PluginManifest
    config_hash: str


def resolve_config(
    profile_path: Path,
    binding_path: Path,
    manifest_path: Path,
    *,
    environment: str,
) -> EffectiveConfig:
    """Validate all three inputs before a schedule or worker uses them."""

    return resolve_config_documents(
        _load_yaml(profile_path), _load_yaml(binding_path), _load_yaml(manifest_path),
        environment=environment,
    )


def resolve_config_documents(
    profile_data: dict[str, Any],
    binding_data: dict[str, Any],
    manifest_data: dict[str, Any],
    *,
    environment: str,
) -> EffectiveConfig:
    """Apply identical validation to reviewed files and a pinned S3 bundle."""
    profile = PipelineProfile.model_validate(profile_data)
    binding = SourceBinding.model_validate(binding_data)
    manifest = PluginManifest.model_validate(manifest_data)

    if binding.profile_id != profile.profile_id:
        raise ValueError("binding profile_id does not match the pipeline profile")
    if environment != "local":
        if binding.secret_ref.startswith("local-secret://"):
            raise ValueError("local secret references are not allowed outside local")

    allowed = {reader.id: reader for reader in manifest.readers}
    for feed_kind, feed in profile.feeds.items():
        reader = allowed.get(feed.reader)
        if reader is None or reader.feed != feed_kind:
            raise ValueError(f"unregistered reader for {feed_kind}: {feed.reader}")
        if feed.validator not in reader.validators:
            raise ValueError(f"unregistered validator for {feed_kind}: {feed.validator}")
        if feed.target not in reader.targets:
            raise ValueError(f"unsupported target for {feed_kind}: {feed.target}")

    manifest_canonical = manifest.model_dump(mode="json")
    if not manifest_canonical["utilities"]:
        manifest_canonical.pop("utilities")
    for reader in manifest_canonical["readers"]:
        for capability in ("source_capabilities", "sink_capabilities"):
            if not reader[capability]:
                reader.pop(capability)
        execution = reader.get("execution")
        if execution is not None:
            for field, default in (
                ("optional_env_names", []), ("context_transport", "stdin"),
                ("output_contract", "completion-v1"),
                ("max_context_bytes", 1_048_576),
            ):
                if execution[field] == default:
                    execution.pop(field)
    canonical = {
        "profile": profile.model_dump(mode="json"),
        # New optional binding controls must not change the digest of already
        # pinned runs when their value is absent/default. Explicit exclusions
        # still become part of the reviewed configuration identity.
        "binding": binding.model_dump(mode="json", exclude_defaults=True),
        "manifest": manifest_canonical,
    }
    digest = hashlib.sha256(
        json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return EffectiveConfig(profile, binding, manifest, digest)
