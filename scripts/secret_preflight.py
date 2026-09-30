"""Conservative preflight scan of files packaged or intended for source control.

Reports locations and rule names only. It never prints matching values.
"""

from __future__ import annotations

import argparse
import ast
import re
from pathlib import Path


_TEXT_SUFFIXES = {".py", ".yaml", ".yml", ".json", ".tf", ".toml", ".sh", ".ps1"}
_HIGH_CONFIDENCE = {
    "private_key": re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----"),
    "aws_access_key": re.compile(r"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b"),
    "credential_url": re.compile(r"https?://[^\s/@:]+:[^\s/@]+@", re.IGNORECASE),
    "authorization_header": re.compile(r"(?i)authorization\s*[:=]\s*['\"]Bearer\s+[^\s'\"]{12,}"),
}
_ASSIGNMENT = re.compile(
    r"(?i)\b(?:password|passwd|secret|api[_-]?key|access[_-]?token)\b\s*[:=]\s*['\"]([^'\"]+)['\"]"
)
_SENSITIVE_NAME = re.compile(
    r"(?i)^(?:password|passwd|secret|api_?key|access_token|client_secret|secret_access_key)$"
)
_PLACEHOLDER = re.compile(r"(?i)(?:replace|example|dummy|fake|test|changeme|placeholder|<[^>]+>)")


def _looks_real(value: str) -> bool:
    return (
        len(value) >= 8
        and not _PLACEHOLDER.search(value)
        and not re.fullmatch(
            r"[A-Z][A-Z0-9_]*_(?:PASSWORD|TOKEN|SECRET|API_KEY|USERNAME|SECRET_ARN)",
            value,
        )
    )


def scan_file(path: Path) -> list[tuple[int, str]]:
    if path.is_symlink() or not path.is_file() or path.stat().st_size > 1_048_576:
        return []
    data = path.read_text(encoding="utf-8", errors="replace")
    findings: set[tuple[int, str]] = set()
    for line_number, line in enumerate(data.splitlines(), start=1):
        for name, pattern in _HIGH_CONFIDENCE.items():
            if pattern.search(line):
                findings.add((line_number, name))
        match = _ASSIGNMENT.search(line)
        if match and _looks_real(match.group(1)):
            findings.add((line_number, "literal_credential_assignment"))
    if path.suffix == ".py":
        try:
            tree = ast.parse(data)
        except SyntaxError:
            return sorted(findings)
        for node in ast.walk(tree):
            if not isinstance(node, (ast.Assign, ast.AnnAssign)):
                continue
            target_names = [
                target.attr if isinstance(target, ast.Attribute) else target.id
                for target in (node.targets if isinstance(node, ast.Assign) else [node.target])
                if isinstance(target, (ast.Attribute, ast.Name))
            ]
            if not any(_SENSITIVE_NAME.fullmatch(name) for name in target_names):
                continue
            value_node = node.value
            if value_node is None:
                continue
            candidates = (
                value_node.values if isinstance(value_node, ast.BoolOp)
                else [value_node]
            )
            for child in candidates:
                if (
                    isinstance(child, ast.Constant)
                    and isinstance(child.value, str)
                    and _looks_real(child.value)
                ):
                    findings.add((child.lineno, "literal_credential_fallback"))
    return sorted(findings)


def _included(root: Path) -> list[Path]:
    paths: list[Path] = []
    for relative in ("src", "config", "local", "infra", "orginal-scripts", "tests", "scripts"):
        base = root / relative
        if base.is_dir():
            paths.extend(
                path for path in base.rglob("*")
                if path.suffix in _TEXT_SUFFIXES
                and path.name != ".env"
                and "__pycache__" not in path.parts
            )
    for relative in (
        "pyproject.toml", "resources.yaml",
        "Dockerfile.worker", "Dockerfile.control", "Dockerfile.lambda",
    ):
        path = root / relative
        if path.is_file():
            paths.append(path)
    return sorted(set(paths))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Print secret finding locations without values")
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--extra-read-only", type=Path, action="append", default=[])
    args = parser.parse_args(argv)
    root = args.root.resolve(strict=True)
    files = _included(root) + [path.resolve(strict=True) for path in args.extra_read_only]
    count = 0
    for path in files:
        for line, rule in scan_file(path):
            print(f"{path}:{line}: {rule}")
            count += 1
    print(f"Scanned {len(files)} files; findings: {count}")
    return 1 if count else 0


if __name__ == "__main__":
    raise SystemExit(main())
