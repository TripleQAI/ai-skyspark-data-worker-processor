"""Execute reviewed utility scripts with durable, non-certifying audit."""

import os
from pathlib import Path

from ingestion.adapters.scripts import RegisteredScriptHandler
from ingestion.config.loader import EffectiveConfig
from ingestion.contracts.standalone import UtilityContext, UtilityResult
from ingestion.core.failures import NonRetryableJobError


class RegisteredUtilityRunner:
    def __init__(self, config: EffectiveConfig, script_root: Path, audit,
                 *, python_executable: str | None = None):
        self._config = config
        self._root = script_root.resolve(strict=True)
        self._audit = audit
        self._executor = RegisteredScriptHandler(
            config, self._root, selected_feeds=set(),
            python_executable=python_executable,
        )
        self._utilities = {item.id: item for item in config.manifest.utilities}

    def run(self, context: UtilityContext) -> UtilityResult:
        self._audit.start(context)
        try:
            if context.config_hash != self._config.config_hash:
                raise NonRetryableJobError("utility context has an unapproved config hash")
            utility = self._utilities.get(context.script_id)
            if utility is None:
                raise NonRetryableJobError("unknown registered utility script ID")
            if ((context.target is None and utility.targets)
                    or (context.target is not None and context.target not in utility.targets)):
                raise NonRetryableJobError("utility target is not approved")
            execution = utility.execution
            path = (self._root / execution.path).resolve(strict=True)
            if not path.is_relative_to(self._root):
                raise NonRetryableJobError("utility script escapes approved root")
            self._executor._verify_digest(path, execution.sha256)
            payload = context.model_dump_json().encode("utf-8")
            if len(payload) > execution.max_context_bytes:
                raise NonRetryableJobError("utility context exceeded limit")
            env = {name: os.environ[name] for name in
                   execution.env_names + execution.optional_env_names if name in os.environ}
            if set(execution.env_names) - set(env):
                raise NonRetryableJobError("required utility environment is missing")
            for name in ("SYSTEMROOT", "PATH"):
                if name in os.environ:
                    env[name] = os.environ[name]
            env["PYTHONUTF8"] = "1"
            env["PYTHONDONTWRITEBYTECODE"] = "1"
            output = self._executor._bounded_run(path, payload, env, execution, ())
            try:
                result = UtilityResult.model_validate_json(output)
                if result.run_id != context.run_id or result.script_id != context.script_id:
                    raise ValueError("result identity differs from registered context")
            except Exception as exc:
                raise NonRetryableJobError("utility returned an invalid output contract") from exc
            self._audit.finish(context.run_id, status="completed", result=result)
            return result
        except Exception as exc:
            status = (
                "rejected" if isinstance(exc, (NonRetryableJobError, ValueError))
                else "failed"
            )
            self._audit.finish(context.run_id, status=status, error_class=type(exc).__name__)
            raise
