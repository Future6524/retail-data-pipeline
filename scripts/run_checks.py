#!/usr/bin/env python3
"""Run every local check available on this machine (plain Python; no `make` needed).

    python scripts/run_checks.py [--skip NAME ...] [--require-tools]

Python steps run with the interpreter that started this script (``sys.executable``), so a fresh
venv checks itself. actionlint is looked up in ``$RETAIL_TOOLS_DIR/bin`` (default ``.tools/bin``), then on PATH.
A missing tool is reported as SKIPPED, never as passed; --require-tools turns a skip into a failure.
Nothing here contacts a network, a cloud or a credential.
"""
from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
TOOLS = Path(os.environ.get("RETAIL_TOOLS_DIR") or ROOT / ".tools").resolve() / "bin"
PY = sys.executable  # always the interpreter that launched this script


def tool(name: str) -> str | None:
    local = TOOLS / name
    return str(local) if local.exists() else shutil.which(name)


def steps() -> list[tuple[str, list[str] | None, Path, str | None]]:
    """(name, command or None when the tool is missing, cwd, missing-tool hint)"""
    al = tool("actionlint")
    return [
        ("ruff", [PY, "-m", "ruff", "check", "."], ROOT, None),
        ("mypy", [PY, "-m", "mypy"], ROOT, None),
        ("pytest", [PY, "-m", "pytest", "-q"], ROOT, None),
        ("hygiene", [PY, "scripts/check_hygiene.py", "--installed"], ROOT, None),
        ("pipeline-demo", [PY, "-m", "pipeline", "demo", "--workdir", ".demo"], ROOT, None),
        ("actionlint", [al] if al else None, ROOT, "install actionlint 1.7.x and put it on PATH (or set RETAIL_TOOLS_DIR)"),
    ]


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--skip", nargs="*", default=[])
    ap.add_argument("--require-tools", action="store_true")
    args = ap.parse_args(argv)
    env = {**os.environ, "PYTHONDONTWRITEBYTECODE": "1"}
    results: list[tuple[str, str]] = []
    for name, cmd, cwd, hint in steps():
        if name in args.skip:
            results.append((name, "SKIPPED (--skip)"))
            continue
        if cmd is None:
            results.append((name, f"SKIPPED (tool missing; {hint})"))
            continue
        print(f"\n=== {name}: {' '.join(Path(c).name if i == 0 else c for i, c in enumerate(cmd))}", flush=True)
        rc = subprocess.run(cmd, cwd=cwd, env=env, check=False).returncode  # noqa: S603
        results.append((name, "PASS" if rc == 0 else f"FAIL (exit {rc})"))
    print("\n==== summary")
    for name, res in results:
        print(f"{name:20s} {res}")
    failed = any(r.startswith("FAIL") or (args.require_tools and r.startswith("SKIPPED (tool")) for _, r in results)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
