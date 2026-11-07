# Evidence: retail-data-pipeline

All data in this repository is **SYNTHETIC**. This file states what was actually run, by whom, and what was **not** run.

## Who ran what

- **Earlier monorepo work** produced the code. Its own statements are the author's claims, not independent verification.
- **Coordinator, independent post-split run (2026-11-07):** a fresh `.venv` was created in this standalone tree and the checks were run via `scripts/run_checks.py`. Verbatim output was saved by the coordinator in `split-evidence/pipeline-post-split-checks.txt` (outside this repo); the numbers below are taken from it.

## Post-split results

| Check | Result |
|---|---|
| `ruff check .` | PASS (all checks passed) |
| `mypy` | PASS (no issues in 21 source files) |
| `pytest -q` | PASS: **298 passed** in 26.99s |
| `scripts/check_hygiene.py --installed` | PASS: 42 files scanned with 6 secret patterns, lock pins match installed packages, 0 problems |
| `python -m pipeline demo --workdir .demo` | PASS: "all demo invariants held (22/22 checks)"; includes SYNTHETIC injected commit faults, retry, rollback and a recovery rerun whose data digest equals the fault-free path |
| `actionlint` | **SKIPPED** (tool was missing at that moment) |

## Not run / not verified

- **No Snowflake account** was ever used. The Snowflake backend code has never been executed against a real account.
- **No AWS, no `terraform plan|apply`** (not part of this repo at all; see `retail-data-platform-ops`).
- **No Docker build or run.**
- **No real AI/model calls** (not part of this repo).
- **No GitHub Actions execution yet:** the workflows under `.github/workflows/` have not run on GitHub. `actionlint` was skipped in the post-split run, so the workflow files are not locally linted by that run. Python 3.11/3.12 support is claimed in CI config but only the local interpreter was exercised.
- Tests passing shows the tested behaviour works; it does not prove the absence of other bugs. Performance at real data volumes is untested.

## Historic context

Deeper independent reviews were done in the monorepo era (before the split). They are history, not re-verified here. The showcase video and the split are shared with the sibling repos.

## Reproduce

```
python -m venv .venv && .venv/bin/pip install -r requirements-lock.txt -e .
.venv/bin/python scripts/run_checks.py
```
