"""Run a pinned, reviewed Python reader through a bounded JSON contract."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import tempfile
import threading
import time
from collections import OrderedDict
from pathlib import Path

from ingestion.config.loader import EffectiveConfig
from ingestion.contracts.config import FeedKind, ScriptExecution
from ingestion.contracts.jobs import Job, JobCompletion, ScriptArtifactManifestV2
from ingestion.core.job_config import ScopedJobConfigResolver
from ingestion.core.failures import HistorySplitRequired, NonRetryableJobError


class ScriptExecutionError(RuntimeError):
    """A reviewed script failed without exposing its stderr or credentials."""


class RegisteredScriptHandler:
    def __init__(
        self, config: EffectiveConfig, script_root: Path,
        *, python_executable: str | None = None,
        selected_feeds: set[FeedKind] | None = None,
    ):
        self._config = config
        self._root = script_root.resolve(strict=True)
        self._python = python_executable or sys.executable
        manifests = {reader.id: reader for reader in config.manifest.readers}
        self._scripts: dict[FeedKind, tuple[str, Path, ScriptExecution]] = {}
        for feed_kind, feed in config.profile.feeds.items():
            if selected_feeds is not None and feed_kind not in selected_feeds:
                continue
            reader = manifests[feed.reader]
            execution = reader.execution
            if execution is None:
                continue
            if execution.source_call_slots > config.profile.source_policy.max_concurrent_calls:
                raise ValueError("script source slots exceed project source budget")
            if reader.source_capabilities and feed_kind not in reader.source_capabilities:
                raise ValueError("reader lacks source capability for configured feed")
            if reader.sink_capabilities and feed.target not in reader.sink_capabilities:
                raise ValueError("reader lacks sink capability for configured target")
            if execution.output_contract == "artifact-provenance-v2" and (
                not reader.source_capabilities or not reader.sink_capabilities
            ):
                raise ValueError("v2 script requires explicit source and sink capabilities")
            path = (self._root / execution.path).resolve(strict=True)
            if not path.is_relative_to(self._root):
                raise ValueError("reviewed script escapes approved root")
            self._verify_digest(path, execution.sha256)
            self._scripts[feed_kind] = (reader.id, path, execution)

    @staticmethod
    def _verify_digest(path: Path, expected: str) -> None:
        actual = hashlib.sha256(path.read_bytes()).hexdigest()
        if actual != expected:
            raise ValueError(f"reviewed script checksum changed: {path.name}")

    def source_slots(self, job: Job) -> int:
        registered = self._scripts.get(job.feed)
        if registered is None:
            raise NonRetryableJobError(f"no approved script execution for {job.feed}")
        return registered[2].source_call_slots

    def run(
        self, job: Job, *, cancel_events: tuple[threading.Event, ...] = ()
    ) -> JobCompletion:
        binding = self._config.binding
        if (
            job.config_hash != self._config.config_hash
            or job.tenant_id != binding.tenant_id
            or job.project_id != binding.project_id
            or job.site_ref not in binding.approved_sites
        ):
            raise ValueError("job is outside the pinned script configuration")
        registered = self._scripts.get(job.feed)
        if registered is None:
            raise NonRetryableJobError(f"no approved script execution for {job.feed}")
        script_id, path, execution = registered
        self._verify_digest(path, execution.sha256)
        feed = self._config.profile.feeds[job.feed]
        payload = json.dumps({
            "schema_version": 2 if execution.output_contract == "artifact-provenance-v2" else 1,
            "script_id": script_id,
            "job": job.model_dump(mode="json"),
            "source": {
                "endpoint": str(binding.endpoint),
                "site_uri": binding.approved_sites[job.site_ref],
                "secret_ref": binding.secret_ref,
                "excluded_history_point_ids": binding.excluded_history_point_ids_by_site.get(job.site_ref, ()),
                "rules_timezone": binding.rules_source_timezone,
                "rules_tz_tags": binding.rules_source_tz_tags,
            },
            "target": feed.target.value,
        }, separators=(",", ":")).encode("utf-8")
        if len(payload) > execution.max_context_bytes:
            raise NonRetryableJobError("reviewed script context exceeded limit")
        env = {name: os.environ[name] for name in
               execution.env_names + execution.optional_env_names if name in os.environ}
        missing = set(execution.env_names) - set(env)
        if missing:
            raise ValueError(f"script environment variables are missing: {sorted(missing)}")
        for name in ("SYSTEMROOT", "PATH"):
            if name in os.environ:
                env[name] = os.environ[name]
        env["PYTHONUTF8"] = "1"
        env["PYTHONDONTWRITEBYTECODE"] = "1"
        output = self._bounded_run(
            path, payload, env, execution, cancel_events,
            split_on_timeout=job.feed == FeedKind.HISTORY,
        )
        if any(event.is_set() for event in cancel_events):
            raise ScriptExecutionError("reviewed script lost its execution lease")
        try:
            if execution.output_contract == "artifact-provenance-v2":
                result = ScriptArtifactManifestV2.model_validate_json(output)
                if (result.script_id != script_id or result.config_hash != job.config_hash
                        or result.job_id != job.job_id
                        or result.provenance.source_artifact_key != result.completion.raw.object_key):
                    raise ValueError("script provenance does not match the pinned job")
                completion = result.completion
            else:
                completion = JobCompletion.model_validate_json(output)
            expected = (job.site_ref,) if job.feed == FeedKind.METADATA else job.scope_ids
            if (completion.raw.job_id != job.job_id or completion.sink.job_id != job.job_id
                    or completion.sink.sink_kind != feed.target.value
                    or len(completion.completed_scope) != len(set(completion.completed_scope))
                    or set(completion.completed_scope) != set(expected)
                    or not completion.raw.object_key or not completion.raw.checksum
                    or not completion.sink.batch_key or not completion.sink.checksum):
                raise ValueError("script completion does not match the pinned job")
            return completion
        except Exception as exc:
            raise NonRetryableJobError("script returned an invalid completion contract") from exc

    def _bounded_run(
        self, path: Path, payload: bytes, env: dict[str, str],
        execution: ScriptExecution,
        cancel_events: tuple[threading.Event, ...],
        *, split_on_timeout: bool = False,
    ) -> bytes:
        with tempfile.TemporaryDirectory(prefix="ingestion-script-") as context_dir:
            command = [self._python, "-I", "-B", str(path)]
            if execution.context_transport == "file":
                context_path = Path(context_dir) / "job-context.json"
                context_path.write_bytes(payload)
                os.chmod(context_path, 0o600)
                command.extend(["--job-context", str(context_path)])
                input_payload = b""
            else:
                input_payload = payload
            return self._execute_process(
                command, input_payload, env, execution, cancel_events,
                split_on_timeout=split_on_timeout,
            )

    def _execute_process(
        self, command: list[str], payload: bytes, env: dict[str, str],
        execution: ScriptExecution, cancel_events: tuple[threading.Event, ...],
        *, split_on_timeout: bool,
    ) -> bytes:
        process = subprocess.Popen(
            command, cwd=self._root, env=env, stdin=subprocess.PIPE,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
        stdout = bytearray()
        stderr = bytearray()
        overflow = threading.Event()

        def collect(pipe, buffer: bytearray) -> None:
            while chunk := pipe.read(4096):
                if len(buffer) + len(chunk) > execution.max_output_bytes:
                    overflow.set()
                    try:
                        process.kill()
                    except OSError:
                        pass
                    return
                buffer.extend(chunk)

        def send_input() -> None:
            try:
                process.stdin.write(payload)
                process.stdin.close()
            except (BrokenPipeError, OSError):
                pass

        threads = [
            threading.Thread(target=collect, args=(process.stdout, stdout), daemon=True),
            threading.Thread(target=collect, args=(process.stderr, stderr), daemon=True),
            threading.Thread(target=send_input, daemon=True),
        ]
        for thread in threads:
            thread.start()
        deadline = time.monotonic() + execution.timeout_seconds
        timed_out = cancelled = False
        while process.poll() is None:
            if any(event.is_set() for event in cancel_events):
                cancelled = True
                try:
                    process.kill()
                except OSError:
                    pass
                break
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                timed_out = True
                try:
                    process.kill()
                except OSError:
                    pass
                break
            try:
                process.wait(timeout=min(0.2, remaining))
            except subprocess.TimeoutExpired:
                pass
        process.wait()
        for thread in threads:
            thread.join()
        if timed_out:
            if split_on_timeout:
                raise HistorySplitRequired("reviewed history script timed out")
            raise ScriptExecutionError("reviewed script timed out")
        if cancelled:
            raise ScriptExecutionError("reviewed script lost its execution lease")
        if overflow.is_set():
            raise NonRetryableJobError("reviewed script exceeded output limit")
        if process.returncode == 65:
            # Reviewed scripts use exit 65 for schema/cap failure. The
            # worker quarantines that partition after the subprocess exits.
            raise NonRetryableJobError("reviewed script reported a nonretryable partition")
        if process.returncode == 75 and split_on_timeout:
            raise HistorySplitRequired("reviewed history script requested a smaller partition")
        if process.returncode != 0:
            raise ScriptExecutionError("reviewed script exited unsuccessfully")
        return bytes(stdout)


class ScopedRegisteredScriptHandler:
    """Select the reviewed script manifest from each job's stored config."""

    def __init__(
        self, resolver: ScopedJobConfigResolver, script_root: Path,
        *, cache_limit: int = 128,
    ):
        if cache_limit < 1:
            raise ValueError("script cache limit must be positive")
        self._resolver = resolver
        self._root = script_root.resolve(strict=True)
        self._limit = cache_limit
        self._handlers: OrderedDict[tuple[str, FeedKind], RegisteredScriptHandler] = OrderedDict()
        self._lock = threading.Lock()

    def _handler(self, job: Job) -> RegisteredScriptHandler:
        config = self._resolver.for_job(job)
        key = (config.config_hash, job.feed)
        with self._lock:
            handler = self._handlers.get(key)
            if handler is None:
                handler = RegisteredScriptHandler(
                    config, self._root, selected_feeds={job.feed}
                )
                self._handlers[key] = handler
                if len(self._handlers) > self._limit:
                    self._handlers.popitem(last=False)
            self._handlers.move_to_end(key)
        return handler

    def source_slots(self, job: Job) -> int:
        return self._handler(job).source_slots(job)

    def run(
        self, job: Job, *, cancel_events: tuple[threading.Event, ...] = (),
    ) -> JobCompletion:
        return self._handler(job).run(job, cancel_events=cancel_events)
