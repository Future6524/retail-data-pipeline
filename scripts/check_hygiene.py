#!/usr/bin/env python3
"""Free, offline, stdlib-only repository hygiene checks. Run: python scripts/check_hygiene.py [--installed]

What it does (and, deliberately, ONLY this):
  1. lock   - every dependency declared in pyproject.toml (runtime + dev extras) has an exact `==` pin in
              requirements-lock.txt; with --installed, the installed versions equal those pins.
  2. secrets- greps text files for a SMALL list of well-known credential shapes (private-key headers, AWS
              access key ids, GitHub/Slack tokens, quoted literal values assigned to password/secret/token/
              api_key names, SNOWFLAKE_PASSWORD=<literal>) and for credential-looking file names.

What it is NOT: a vulnerability scanner (it never looks up CVEs, so it says nothing about whether the pinned
versions are vulnerable) and not a substitute for a real secret scanner with entropy checks and git-history
scanning. A clean result means "none of these specific patterns were found in the files scanned".
Add `# hygiene: allow` to a line to silence a reviewed false positive.
"""
from __future__ import annotations

import argparse
import os
import re
import sys
import tomllib
from importlib import metadata
from pathlib import Path

SKIP_DIRS = {".git", ".venv", "venv", ".review", ".review-venv", ".demo", "__pycache__",
             ".pytest_cache", "node_modules", "build", "dist", ".tools", ".terraform", ".mypy_cache", ".ruff_cache"}
SKIP_SUFFIXES = {".duckdb", ".wal", ".db", ".pyc", ".png", ".jpg", ".gz", ".zip", ".whl"}
MAX_BYTES = 2_000_000
SUSPICIOUS_NAMES = (".env", ".pem", ".p12", ".pfx", ".key", "id_rsa", "id_ed25519", "credentials", ".netrc", ".pypirc",
                    ".tfstate", ".tfstate.backup", ".tfvars")
# Documentation-only templates that are ALLOWED by name (their contents are still scanned, see scan_env_example).
ALLOWED_EXAMPLE_NAMES = {".env.example", ".env.sample", ".env.template"}
SECRETISH_KEY = re.compile(r"(?i)(PASSWORD|PASSWD|SECRET|TOKEN|API_?KEY|ACCESS_?KEY|PRIVATE)")
PLACEHOLDER_VALUE = re.compile(r"""(?ix)^(?:|<[^>]*>|\$\{[^}]*\}|\$\w+|(?:your|example|placeholder|changeme|replace|xxx)[\w.-]*)$""")

SECRET_PATTERNS = {
    "private key block": re.compile(r"-----BEGIN (?:RSA |EC |DSA |OPENSSH |PGP )?PRIVATE KEY"),
    "AWS access key id": re.compile(r"\b(?:AKIA|ASIA)[0-9A-Z]{16}\b"),
    "GitHub token": re.compile(r"\bgh[pousr]_[A-Za-z0-9]{30,}\b"),
    "Slack token": re.compile(r"\bxox[baprs]-[A-Za-z0-9-]{10,}"),
    "quoted literal credential": re.compile(
        r"""(?ix)(?:password|passwd|secret|api[_-]?key|access[_-]?token|auth[_-]?token)\w*\s*[:=]\s*
            (['"])(?!\s*\1)(?!.*(?:example|placeholder|changeme|<|\$\{|xxx|\.\.\.))[^'"\s]{8,}\1"""),
    "SNOWFLAKE_PASSWORD with literal value": re.compile(
        r"""SNOWFLAKE_PASSWORD\s*=\s*(?!\s*$)(?!['"]?(?:<|\$|\{|your|example|placeholder|xxx|\.\.\.))['"]?[^\s'"]{4,}"""),
}


def norm(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


def req_name(spec: str) -> str:
    return norm(re.split(r"[<>=!~;\[ ]", spec.strip(), maxsplit=1)[0])


def declared_dependencies(root: Path) -> set[str]:
    proj = tomllib.loads((root / "pyproject.toml").read_text())["project"]
    specs = list(proj.get("dependencies", []))
    for extra, reqs in proj.get("optional-dependencies", {}).items():
        if extra not in ("snowflake", "aws"):  # snowflake: unlocked on purpose; aws: pinned in requirements-runtime.txt
            specs += reqs
    return {req_name(s) for s in specs}


def read_lock(root: Path) -> dict[str, str]:
    pins = {}
    for line in (root / "requirements-lock.txt").read_text().splitlines():
        line = line.split("#", 1)[0].strip()
        if line:
            m = re.fullmatch(r"([A-Za-z0-9_.-]+)==([A-Za-z0-9_.+!-]+)", line)
            if not m:
                raise ValueError(f"requirements-lock.txt: line is not an exact pin: {line!r}")
            pins[norm(m.group(1))] = m.group(2)
    return pins


def check_lock(root: Path, installed: bool = False) -> list[str]:
    problems: list[str] = []
    try:
        pins = read_lock(root)
    except (OSError, ValueError) as exc:
        return [str(exc)]
    for dep in sorted(declared_dependencies(root) - pins.keys()):
        problems.append(f"lock: declared dependency {dep!r} has no exact pin in requirements-lock.txt")
    if installed:
        for name, ver in sorted(pins.items()):
            try:
                have = metadata.version(name)
            except metadata.PackageNotFoundError:
                problems.append(f"lock: pinned {name}=={ver} is not installed")
                continue
            if have != ver:
                problems.append(f"lock: {name} installed {have} but pinned {ver}")
    return problems


def check_runtime_pins(root: Path) -> list[str]:
    """requirements-runtime.txt (container image): exact pins only, and shared packages equal the main lock."""
    path = root / "requirements-runtime.txt"
    if not path.is_file():
        return []
    problems: list[str] = []
    try:
        runtime = {}
        for line in path.read_text().splitlines():
            line = line.split("#", 1)[0].strip()
            if line:
                m = re.fullmatch(r"([A-Za-z0-9_.-]+)==([A-Za-z0-9_.+!-]+)", line)
                if not m:
                    problems.append(f"requirements-runtime.txt: line is not an exact pin: {line!r}")
                else:
                    runtime[norm(m.group(1))] = m.group(2)
        lock = read_lock(root)
    except (OSError, ValueError) as exc:
        return [str(exc)]
    for name, ver in sorted(runtime.items()):
        if name in lock and lock[name] != ver:
            problems.append(f"runtime: {name} pinned {ver} but requirements-lock.txt pins {lock[name]}")
    try:
        aws = tomllib.loads((root / "pyproject.toml").read_text())["project"].get("optional-dependencies", {}).get("aws", [])
    except (OSError, ValueError):
        aws = []
    for spec in aws:
        if req_name(spec) not in runtime:
            problems.append(f"runtime: aws extra {req_name(spec)!r} has no exact pin in requirements-runtime.txt")
    if "duckdb" not in runtime:
        problems.append("runtime: duckdb is not pinned in requirements-runtime.txt")
    return problems


def is_skipped_dir(parent: Path, name: str) -> bool:
    """Exact SKIP_DIRS, `.review*` (evidence dirs and their venvs), *.egg-info, and any virtualenv (pyvenv.cfg)."""
    return (name in SKIP_DIRS or name.startswith(".review") or name.endswith(".egg-info")
            or (parent / name / "pyvenv.cfg").is_file())


def iter_files(root: Path):
    found = []
    for dirpath, dirnames, filenames in os.walk(root):  # prune: never descend into skipped dirs
        base = Path(dirpath)
        dirnames[:] = [d for d in dirnames if not is_skipped_dir(base, d)]
        found += [(base / f, (base / f).relative_to(root)) for f in filenames if (base / f).is_file()]
    yield from sorted(found, key=lambda t: t[1])


def is_suspicious_name(name: str) -> bool:
    lowered = name.lower()
    if lowered in ALLOWED_EXAMPLE_NAMES or lowered.endswith((".example", ".sample", ".template")):
        return False  # e.g. .env.example, terraform.tfvars.example: documentation templates
    if lowered.startswith(".env."):  # .env.local, .env.production ... are real environment files
        return True
    return any(lowered == n or lowered.endswith(n) for n in SUSPICIOUS_NAMES)


def scan_env_example(text: str) -> list[tuple[int, str]]:
    """In an allowed .env example every secret-looking KEY must hold an obvious placeholder (or nothing)."""
    bad = []
    for n, line in enumerate(text.splitlines(), 1):
        m = re.match(r"\s*(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*?)\s*$", line)
        if not m or "hygiene: allow" in line:
            continue
        key, value = m.group(1), m.group(2).split(" #", 1)[0].strip().strip("'\"")
        if SECRETISH_KEY.search(key) and not PLACEHOLDER_VALUE.match(value):
            bad.append((n, key))
    return bad


def scan_secrets(root: Path) -> list[str]:
    hits: list[str] = []
    for p, rel in iter_files(root):
        if is_suspicious_name(p.name) and p.name != "check_hygiene.py":
            hits.append(f"{rel}: credential-looking file name")
        if p.suffix in SKIP_SUFFIXES or p.stat().st_size > MAX_BYTES:
            continue
        try:
            text = p.read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError):
            continue
        if p.name.lower() in ALLOWED_EXAMPLE_NAMES:
            hits += [f"{rel}:{n}: example file has a non-placeholder value for {k}" for n, k in scan_env_example(text)]
        for n, line in enumerate(text.splitlines(), 1):
            if "hygiene: allow" in line:
                continue
            for label, rx in SECRET_PATTERNS.items():
                if rx.search(line):
                    hits.append(f"{rel}:{n}: possible {label}")
    return hits


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--root", type=Path, default=Path(__file__).resolve().parent.parent)
    ap.add_argument("--installed", action="store_true", help="also compare installed package versions with the pins")
    args = ap.parse_args(argv)
    problems = check_lock(args.root, args.installed) + check_runtime_pins(args.root) + scan_secrets(args.root)
    for p in problems:
        print("PROBLEM:", p)
    n_files = sum(1 for _ in iter_files(args.root))
    print(f"hygiene: scanned {n_files} files with {len(SECRET_PATTERNS)} secret patterns; "
          f"{'lock pins verified against installed packages; ' if args.installed else ''}{len(problems)} problem(s)")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
