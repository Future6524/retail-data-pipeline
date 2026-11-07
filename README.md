# retail-data-pipeline (SYNTHETIC data, local-first)

A small, honest data-engineering project: a validated, **idempotent, incremental** loader that turns messy
CSV / JSON / JSONL batch files for **orders, inventory and support tickets** into clean tables in a local
**DuckDB** file, with quarantine of bad rows, checkpoints, bounded backfills, bounded retries and a
deterministic demo that checks itself against an independent ground truth.

* **Every record is synthetic.** A seeded generator invents all data; the retailer is fictional. There are no real customers, orders or companies.
* **Nothing touches a network, cloud or credential** unless you explicitly opt into the optional Snowflake
  backend (which is **not integration-tested**, see [Snowflake](#snowflake-adapter--not-integration-tested)).
* This repository is one of three standalone repositories split from an earlier monorepo (see [`docs/SPLIT.md`](docs/SPLIT.md)).
  The other two are `retail-data-platform-ops` (CI, container, AWS Terraform, local monitoring) and `retail-ai-support-app`
  (ticket triage with a FAKE/OFFLINE provider). Neither is needed to run anything here; the small amount of shared code this repo owns
  (`data/`, `pipeline/`) is vendored (copied) into those repositories, not imported from here.

Showcase video (silent screen recording of the terminal run and the results page): [`docs/showcase.mp4`](docs/showcase.mp4).

| Layer | Verified | NOT verified |
|---|---|---|
| `pipeline/`, `data/` | tests, lint, types, offline demo; see [`EVIDENCE.md`](EVIDENCE.md) | Snowflake backend (never run against a Snowflake account); Python 3.11/3.12 |

## Contents
[Quick start](#quick-start) · [Showcase](#showcase-terminal-progress-bar--openable-results) · [What the demo shows](#what-the-demo-shows) · [Architecture](#architecture) ·
[CLI](#cli) · [Data dictionary](#data-dictionary) · [Semantics](#semantics) · [Inspecting quarantine](#inspecting-quarantine) ·
[Expected results](#expected-results-seed-42) · [Retry, failure, backfill](#retry-failure-and-backfill-walkthroughs) ·
[Tests](#tests) · [Dependencies and checks](#dependency-repeatability-and-local-checks) ·
[Snowflake](#snowflake-adapter--not-integration-tested) · [Limitations](#limitations)

## Quick start

**Prerequisites**

* Python **3.11 or newer** (declared in `pyproject.toml`). It has been run and tested **only on Python 3.13.5 / Linux**; 3.11 and 3.12
  were not available to test on, so treat them as "should work, unverified".
* `python3 -m venv` (the `venv` module) and `pip`. `make` is **not** used or needed anywhere.
* Internet access **once**, to download the pinned packages from PyPI. After that everything runs offline.
* No database server, cloud account, API key or credential is needed.

**Clean install, test, demo** (from the repository root; exact commands):

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements-lock.txt   # exact tested versions of duckdb + pytest + ruff + mypy (+ their deps)
.venv/bin/python -m pip install --no-deps -e .             # the project itself
.venv/bin/python -m pytest -q                              # full test-suite
.venv/bin/python -m pipeline demo --workdir .demo          # generate -> load -> rerun -> backfill -> synthetic faults
.venv/bin/python scripts/run_checks.py                     # ruff, mypy, pytest, hygiene, demo, actionlint (if installed)
```

The demo prints `RESULT: all demo invariants held (N/N checks)` and exits 0 only if every check passed; any `[FAIL]` line exits 1.
`scripts/run_checks.py` runs every Python step with the interpreter that launched it and reports a missing optional tool as
**SKIPPED**, never as passed.

Everything the tools write (`.demo/`, `*.duckdb`) is git-ignored.

## Showcase (terminal progress bar + openable results)

```bash
.venv/bin/python -m pipeline showcase                     # real progress bar, summary table, .demo/showcase/results.html + results.csv
python -m webbrowser -t file:///ABSOLUTE/PATH/.demo/showcase/results.html   # the exact command is printed at the end
```

Light-mode page with a bright orange header row; scroll down for rows and right for columns: 43 per-file rows x 19 columns, then a run summary.
Everything is SYNTHETIC and produced by the real pipeline; the fault run is labelled. Nothing opens a browser or starts a server unless you run the printed command.
Use `--workdir` to choose another directory.

## What the demo shows

`python -m pipeline demo` is fully deterministic (fixed seed 42, fixed "now" of 2026-02-01) and checks **every** run report, not just the rerun:

| Step | What happens | What is checked |
|---|---|---|
| generate | 10 synthetic files in CSV / JSON / JSONL with injected defects, plus `manifest.json` (an independent oracle) | — |
| 1 | load `*_batch1.*` only | 3 files loaded, none failed |
| 2 | load everything | batch1 skipped via checkpoints; late, stale, corrected, duplicate and malformed rows handled; 1 corrupt file rejected |
| 3 | rerun, same inputs | every file `skipped_checkpointed`; state digest unchanged |
| oracle | warehouse vs generator ground truth | row counts for all 3 entities, order totals **per currency**, status breakdowns, inventory by warehouse, refund count |
| 4 | fresh warehouse: two backfill windows, then a full run | windowed checkpoints don't block the full run; final data equals the incremental path |
| 5 | **SYNTHETIC fault injection** (the demo itself makes commits raise `InjectedFault`; it is *not* a real outage) | one file fails once and **recovers on attempt 2**; another fails every time, **exhausts 3 attempts**, is reported `failed`, the other files still load; a rerun without the fault loads only that file and reaches the same data digest |

Excerpt of step 5 (abbreviated):

```text
== 5. SYNTHETIC fault injection (the demo itself makes these commits fail; this is NOT a real outage)
  inventory_batch2.csv     loaded   read=13 ins=0 upd=10 ... attempts=2
  orders_batch3.jsonl      failed   attempts=3 ERROR: InjectedFault: SYNTHETIC injected commit fault ...; not a real outage
   [ok] 5a: orders_batch3.jsonl failed after exactly 3 attempts, nothing committed for it
```

(The `WARNING retrying ...` log lines go to stderr, so they may appear above the stdout text.)

## Architecture

```text
 source files (read-only)              pipeline/                                   warehouse (DuckDB; Snowflake optional)
 ┌───────────────────────┐    ┌───────────────────────────────────────┐    ┌──────────────────────────────────────────┐
 │ orders_*.csv|json|jsonl│    │ sources.py   read bytes once, SHA-256 │    │ one TRANSACTION per source file:         │
 │ inventory_*.…          │───►│              parse CSV / JSON / JSONL │    │   stage (temp tables) ─► MERGE into      │
 │ support_tickets_*.…    │    │ validate.py  types, enums, money,     │    │   orders / inventory / support_tickets   │
 └───────────────────────┘    │              timestamps → canonical   │    │   + quarantine rows                      │
        ▲ never written        │ runner.py    window filter, in-file   │───►│   + watermark (informational)            │
        │                      │              dedupe, retries, report  │    │   + checkpoint  ◄── LAST statement:      │
 data/generator.py ────────────┤ retry.py     bounded exponential      │    │       committed iff everything above is  │
 (seeded, + manifest oracle)   │              backoff                  │    └──────────────────────────────────────────┘
                               │ warehouse/   base.py = backend-neutral│       tables: orders, inventory, support_tickets,
                               │              SQL; duckdb_wh.py,       │               quarantine, checkpoints, watermarks
                               │              snowflake_wh.py          │
                               │ cli.py       generate/run/backfill/   │
                               │              status/demo/showcase     │
                               └───────────────────────────────────────┘
```

| Path | Purpose |
|---|---|
| `data/generator.py` | Deterministic SYNTHETIC batch generator + ground-truth oracle |
| `pipeline/sources.py` | Immutable reading; row-level failure containment for CSV/JSON/JSONL |
| `pipeline/validate.py` | Validation / normalisation to canonical records |
| `pipeline/runner.py` | Orchestration: checkpoints, windows, dedupe, retries, per-file failure containment |
| `pipeline/warehouse/` | `base.py` shared SQL + transaction logic; `duckdb_wh.py` (default); `snowflake_wh.py` (**untested** vs cloud) |
| `pipeline/faults.py` | Clearly-labelled synthetic fault injection (demo and tests only) |
| `pipeline/oracle.py` | Summarises a warehouse in the oracle's shape (per-currency totals etc.) |
| `pipeline/showcase.py`, `pipeline/ui.py` | Showcase command: progress bar, tables, static results page |
| `scripts/check_hygiene.py`, `scripts/run_checks.py` | Offline lock-consistency + secret-pattern check; one-command local checks |
| `tests/` | Unit, integration, adversarial, e2e tests |
| `docs/DESIGN.md` | Deeper design notes (carried over from the monorepo) |

## CLI

All commands: `.venv/bin/python -m pipeline <command>` (or `retail-pipeline <command>` once installed). `-v` shows retry logs.

| Command | Purpose |
|---|---|
| `generate --out DIR [--seed 42] [--orders 40]` | Write synthetic batches + `manifest.json`. `--orders` must be **8 – 100000** (the scenarios need at least 7 distinct orders to correct, cancel and replay); validated *before* anything in DIR is deleted. Only files the generator owns (`*_batch*`, `manifest.json`) are replaced. |
| `run --input DIR --db FILE [--pattern GLOB] [--as-of TS] [--retries N] [--json]` | Incremental, idempotent load. Exit **0** all files ok, **1** at least one file `failed`, **2** usage/fatal error. |
| `backfill --input DIR --db FILE --since TS --until TS [...same options]` | Apply only rows with `since <= updated_at < until`. |
| `status --db FILE` | Row counts, watermarks, quarantine counts by reason, state digest. |
| `demo [--workdir .demo] [--seed 42]` | The self-checking demonstration above. |
| `showcase [--workdir .demo/showcase]` | Progress bar + summary table + `results.html` / `results.csv`. |

Timestamps need an explicit UTC offset (`2026-01-10T00:00:00Z`). `--retries` counts **attempts including the first** (default 3, minimum 1).
`--as-of` is the "now" used for the future-timestamp rule (default: the current time).

**`--json`** prints exactly one JSON document on stdout and nothing else (logs go to stderr):
`{"ok", "totals", "ignored", "files": [...], "state_digest"}`. It stays valid JSON when files fail (exit 1).

```bash
.venv/bin/python -m pipeline run --input .demo/batches --db .demo/w.duckdb --as-of 2026-02-01T00:00:00Z --json | python3 -m json.tool | head
```

## Data dictionary

Source file names must start with `orders_`, `inventory_` or `support_tickets_` and end in `.csv`, `.json` (one JSON array), `.jsonl` or `.ndjson`. Everything else is listed under `ignored` and never loaded. Unknown extra columns/fields are ignored (not stored).

Common to all three entity tables: `updated_at` (event time of this version, UTC `TIMESTAMPTZ`), `row_hash` (SHA-256 of the canonical record; used as the version tie-breaker), `source_file` (which file this version is attributed to, see [provenance](#semantics)).

**`orders`** — key `order_id`
| Column | Type | Rules |
|---|---|---|
| `order_id`, `customer_id` | VARCHAR | required, ≤ 64 chars after trimming |
| `status` | VARCHAR | one of `placed paid shipped delivered cancelled refunded` (case-insensitive input, stored lower-case) |
| `currency` | VARCHAR | one of `USD EUR GBP CAD` (stored upper-case) |
| `total_amount` | DECIMAL(18,2) | exact decimal, `0 ≤ x < 1,000,000,000`, at most 2 decimal places (never rounded) |
| `order_ts`, `updated_at` | TIMESTAMPTZ | ISO-8601 **with** UTC offset; `updated_at ≥ order_ts` |

**`inventory`** — key `(sku, warehouse_id)`: `quantity_on_hand` BIGINT (integer 0 … 1,000,000,000), `unit_cost` DECIMAL(18,2) (same money rules; no currency column).

**`support_tickets`** — key `ticket_id`: `customer_id`, `order_id` (optional), `status` (`open pending resolved closed`), `priority` (`low normal high urgent`), `subject` (required, ≤ 200 chars), `refund_amount` DECIMAL(18,2) (optional, no currency column), `created_at`, `updated_at` (`updated_at ≥ created_at`). In CSV the `order_id` and `refund_amount` columns may be absent.

**`quarantine`** — key `(source_name, file_sha256, row_number)`: `entity`, `reason`, `detail`, `raw_record` (truncated to 4000 chars, NUL escaped). `row_number` is the 1-based data row (CSV/JSON array) or physical line (JSONL); `0` for a whole-file rejection.

**`checkpoints`** — key `(source_name, file_sha256, window_start, window_end)` with `status` (`loaded`/`rejected`), per-file counters and `processed_at`.
**`watermarks`** — `entity`, `max_updated_at`: highest applied event time per entity. **Informational only; never used to filter input.**

Text rules: strings containing NUL or lone surrogates are rejected (`invalid_characters`) because they would otherwise fail the whole file's transaction.
Numeric strings must be plain ASCII (`12`, `-3.50`); `1_0`, `1e2`, `1.`, `.5`, `1,000`, `NaN`, `Infinity` and non-ASCII digits are `invalid_money` / `invalid_integer`. JSON numbers are parsed as exact `Decimal` (never binary floats), so `0.30` is fine and `0.30000000000000004` is `money_precision`.

**Quarantine reasons**

| Reason | Meaning |
|---|---|
| `invalid_json`, `not_an_object`, `duplicate_json_keys` | Bad JSON line/element; non-object; an object repeating a key (silently keeping the last value would lose data) |
| `csv_parse_error` | Bad quoting, or a field over Python's 131,072-character CSV limit. The rest of the file continues (an unterminated quote swallows the remainder of the file into this one quarantined record) |
| `missing_fields`, `extra_fields` | CSV row has fewer / more cells than the header |
| `missing_required`, `wrong_type`, `too_long`, `invalid_value`, `invalid_characters` | Field-level problems |
| `invalid_money`, `negative_money`, `money_precision`, `money_out_of_range`, `invalid_integer`, `negative_quantity`, `quantity_out_of_range` | Number problems |
| `invalid_timestamp`, `naive_timestamp`, `timestamp_out_of_range`, `updated_before_created`, `future_timestamp` | Time problems. `timestamp_out_of_range` = before 2000-01-01 UTC or beyond what the offset conversion can represent (e.g. `0001-01-01T00:00:00+23:59`). `future_timestamp` = `updated_at` more than 5 minutes after `--as-of` |
| `validation_error` | Safety net: an *unexpected* exception while validating one row. It should never appear; if it does, it is a bug report with the row attached |
| `file_rejected` | Whole file unusable: invalid UTF-8, empty or missing CSV header, **duplicate CSV header names**, missing required columns, JSON array that is not valid JSON / not an array |

## Semantics

* **Incremental = per-file checkpoint**, keyed on `(file name, SHA-256 of its bytes, window)`. An unchanged file is skipped; a file whose bytes changed (even under the same name) is reprocessed. A file's bytes are read once, hashed, then parsed from those same bytes.
* **No event-time filter, so no late-data loss.** The watermark is only used to *count* late arrivals (`rows_late`). A row from last week arriving today is applied if it is newer than what is stored for its key.
* **Versions and ties.** A staged row replaces the stored row iff `(updated_at, row_hash)` is greater. Older versions are `stale` and never overwrite. Two different contents with the same `updated_at` are a *tie*: the larger `row_hash` wins, whatever the arrival order.
* **Deterministic provenance.** If two files contain *identical* business rows (same `updated_at`, same `row_hash`), the stored `source_file` is the **lexicographically smallest file name**, regardless of which file arrived first. So the final state of every table, including `source_file`, and its digest, depend only on the *set* of files processed, not their order. (Counters such as `rows_inserted` vs `rows_updated` or `rows_late` describe what happened on that arrival and legitimately depend on order.)
* **In-file duplicates** of one key collapse to the best version before merging (`rows_batch_duplicates`).
* **Atomic per file.** Stage → merge → quarantine → watermark → **checkpoint last**, in one transaction. All or nothing; a failure leaves no checkpoint, so the next run redoes the file (merges are idempotent, so redoing is safe).
* **Failure containment.** One bad row → quarantined. One bad file → `rejected` (recorded, checkpointed, not retried while its bytes are unchanged) or `failed` (unreadable source, parse crash, database error; *not* checkpointed, retried next run). The run always continues with the remaining files and exits 1 if any file `failed`.
* **Retries.** Only transient errors are retried (`TransientError`, `TimeoutError`, `ConnectionError`, DuckDB I/O / connection / transaction errors; `OSError`s like EIO when *reading* a source — but not missing/forbidden files). Exponential backoff 0.5 s, 1 s, 2 s … capped at 8 s. `attempts` in the report is the exact number of tries made.
* **Ambiguous commits.** If `COMMIT` itself raises, the pipeline attempts a `ROLLBACK`, then retries the file. On a network database a failed commit may in fact have been applied (the acknowledgement was lost). So each attempt first looks for its own checkpoint: if found, the earlier commit succeeded; the file is reported `loaded` with a `note` and the stored counters instead of failing on a duplicate checkpoint. Limits: (1) if the connection is gone and the `ROLLBACK` also fails, the outcome cannot be known; the connection is marked unsafe, all further writes on it are refused (`connection state unknown ...`), and a *rerun* settles it, because the checkpoint is either there (skip) or not (redo); (2) this is verified for DuckDB with injected faults only; real network behaviour of Snowflake is untested; (3) it assumes one writer at a time.
* **Backfill** `[since, until)`: `since` inclusive, `until` exclusive, to the microsecond, compared as instants (the offsets of the bounds or of row timestamps do not matter). Windowed checkpoints are separate from un-windowed ones: re-running the same window is a no-op, a different window or a plain run still processes the file. Out-of-window rows are counted (`rows_out_of_window`), not lost.
* **State digest.** SHA-256 over all tables' logical content (+ watermarks, + checkpoint identities unless excluded), excluding wall-clock fields. Same digest ⇔ same state *on this backend and version*. It includes quarantine `detail` texts, so it changes if those messages change.
* **Money.** Always `Decimal`; the demo/oracle never add amounts across currencies. Inventory `unit_cost` and ticket `refund_amount` have no currency in the schema, so they are never summed.

## Inspecting quarantine

`.demo/warehouse.duckdb` (after the demo command) holds the step 1–3 warehouse. Open it read-only from Python (the DuckDB CLI is not required):

```bash
.venv/bin/python - <<'PY'
import duckdb
con = duckdb.connect(".demo/warehouse.duckdb", read_only=True)
for row in con.sql("SELECT reason, COUNT(*) AS n FROM quarantine GROUP BY reason ORDER BY n DESC, reason").fetchall():
    print(row)
PY
```

Useful queries (run by the test-suite, so they stay valid):

```sql
-- how many rows were rejected, and why
SELECT reason, COUNT(*) AS n FROM quarantine GROUP BY reason ORDER BY n DESC, reason;
```
```sql
-- which files lost rows, and which were rejected whole
SELECT source_name, COUNT(*) AS bad_rows, MAX(reason = 'file_rejected') AS whole_file_rejected
FROM quarantine GROUP BY source_name ORDER BY bad_rows DESC, source_name;
```
```sql
-- look at the offending input
SELECT source_name, row_number, reason, detail, raw_record FROM quarantine WHERE reason = 'invalid_money' ORDER BY source_name, row_number;
```
```sql
-- are all quarantined rows tied to a checkpointed file version?
SELECT q.source_name, q.file_sha256 FROM quarantine q
LEFT JOIN checkpoints c ON c.source_name = q.source_name AND c.file_sha256 = q.file_sha256
WHERE c.source_name IS NULL GROUP BY 1, 2;
```
```sql
-- per-file load summary
SELECT source_name, status, rows_read, rows_inserted, rows_updated, rows_unchanged, rows_stale, rows_late,
       rows_quarantined, rows_batch_duplicates FROM checkpoints WHERE window_start = '' ORDER BY source_name;
```
```sql
-- order totals per currency (never add currencies together)
SELECT currency, COUNT(*) AS orders, SUM(total_amount) AS total FROM orders GROUP BY currency ORDER BY currency;
```

Fixing a quarantined row means **re-delivering it in a new file** (new bytes → new checkpoint); quarantine rows are a record, not a work queue.

## Expected results (seed 42)

`generate` with defaults (seed 42, 40 orders) → `run --as-of 2026-02-01T00:00:00Z`. These are checked by the demo and by tests against the generator's independent oracle.

| Entity | Rows |
|---|---|
| orders | 65 (cancelled 1, paid 26, placed 32, refunded 2, shipped 4) |
| inventory | 24 (quantity: WH-EAST 3118, WH-WEST 3601; total 6719) |
| support_tickets | 13 (open 9, resolved 4; 4 with a refund) |
| order totals **per currency** | CAD 5200.48 · EUR 5204.94 · GBP 2805.08 · USD 4291.41 (deliberately no cross-currency sum) |
| quarantine | 18 rows: `invalid_value` 3, `invalid_json` 2, `invalid_money` 2, `missing_required` 2, and 1 each of `file_rejected`, `future_timestamp`, `invalid_integer`, `missing_fields`, `money_precision`, `naive_timestamp`, `negative_money`, `negative_quantity`, `not_an_object` |
| injected | 3 in-file duplicates, 9 stale replays (orders 4, inventory 3, tickets 2), 5 late orders, 1 far-future row, 1 corrupt file |
| checkpoints | 10 (the corrupt file is checkpointed as `rejected`) |

State digest after `run` of the above: `97da1565ba06d6f80b6fb8803b057ec6304a342347dc754ce9927eb79d72e7e8` (for this code version; a change to quarantine message texts or schema legitimately changes it).
Other seeds give different values but the same structure, and every seed in the test matrix (0, 1, 7, 42, 123, 2026, 99999; sizes 8–150) reproduces its own oracle exactly.

## Retry, failure and backfill walkthroughs

*Synthetic fault injection (what the demo does).* `pipeline/faults.py::FaultPlan` makes the `COMMIT` of chosen files raise `InjectedFault` (before the commit, or "after", i.e. the commit happens and the acknowledgement is lost). It is never active in `run` / `backfill`. See demo step 5, or in Python:

```python
from pipeline.faults import FaultPlan
FaultPlan(wh, {"orders_batch3.jsonl": 99}).install()   # always fails -> retries exhausted -> 'failed', other files load
FaultPlan(wh, {"orders_batch2.csv": 1}, mode="after").install()  # ambiguous commit -> recognised on retry, 'loaded' + note
```

*Real failure behaviour you can try without faults.* An unreadable source, e.g. a dangling symlink:

```bash
.venv/bin/python -m pipeline generate --out /tmp/b && ln -s /nonexistent /tmp/b/orders_dangling.csv
.venv/bin/python -m pipeline run --input /tmp/b --db /tmp/w.duckdb --as-of 2026-02-01T00:00:00Z; echo "exit=$?"   # dangling file 'failed', exit=1, everything else loaded
rm /tmp/b/orders_dangling.csv                           # clean up before the backfill example below
```

*Backfill.* Load one day, then another, then everything; windowed runs are repeatable and a later plain run is not blocked:

```bash
.venv/bin/python -m pipeline backfill --input /tmp/b --db /tmp/w2.duckdb --since 2026-01-10T00:00:00Z --until 2026-01-11T00:00:00Z --as-of 2026-02-01T00:00:00Z
.venv/bin/python -m pipeline backfill --input /tmp/b --db /tmp/w2.duckdb --since 2026-01-11T00:00:00Z --until 2026-01-12T00:00:00Z --as-of 2026-02-01T00:00:00Z
.venv/bin/python -m pipeline run      --input /tmp/b --db /tmp/w2.duckdb --as-of 2026-02-01T00:00:00Z
```

## Tests

`.venv/bin/python -m pytest -q` runs the whole suite offline (`-m e2e` selects the CLI subprocess tests).

| File | Focus |
|---|---|
| `test_unit.py` | validators, parsing, retry policy, generator determinism, Snowflake config (SQL text/config only) |
| `test_integration.py` | oracle match for all entities, idempotence, order independence, late/stale/ties, rollback, backfill convergence, source immutability |
| `test_provenance_and_duplicates.py` | identical rows in several files, every arrival order, checkpoint loss, renamed copies, in-file duplicates |
| `test_parsing_adversarial.py` | CSV structure (oversize fields, bad/duplicate headers, quoting, BOM, CRLF), JSON/JSONL edge cases (duplicate keys, deep nesting, huge ints, NaN, U+2028, surrogates, NUL), malformed decimals, timestamp extremes/overflow |
| `test_failures_and_commits.py` | retry exhaustion & exact attempts, backoff, commit failure before/after commit, rollback failure, reopen-from-disk state, source read failures, symlinks |
| `test_backfill_boundaries.py` | exact `[since, until)` edges to the microsecond, timezone-shifted bounds, window checkpoints |
| `test_generator_and_seeds.py` | multiple seeds/sizes reproduce the oracle, shuffled-order equivalence, generator validation before cleanup |
| `test_e2e.py` | CLI: demo, machine-readable `--json`, argument errors, exit codes |
| `test_showcase.py` | progress bar, table renderer, results page, showcase command |
| `test_hygiene.py`, `test_docs.py` | the hygiene script, and that this README's `sql` blocks and seed-42 numbers match the code |

How these were checked (in the monorepo, before the split): the regression tests for the provenance tie-break and the commit rollback were run against the **un-fixed logic** (temporarily reverted) and failed, then passed with the fix. That shows they detect those bugs; it does not prove the absence of others.

## Dependency repeatability and local checks

* `requirements-lock.txt` pins the exact versions used (`duckdb==2.0.0`, `pytest==9.2.0` and their dependencies). It has **no hashes**: it gives repeatable versions, not artifact-integrity verification. `pyproject.toml` keeps looser lower bounds for library use. The optional Snowflake connector is deliberately not locked.
* `python scripts/check_hygiene.py --installed` is a free, offline, stdlib-only check that (1) every declared dependency has an exact pin and the installed versions equal the pins, and (2) scans the repository's text files (skipping `.git`, venvs, `.demo`, `.review*`) for a short list of well-known secret shapes (private-key headers, AWS key ids, GitHub/Slack tokens, quoted literals assigned to password/secret/token/api-key names, `SNOWFLAKE_PASSWORD=<literal>`) and credential-looking file names.
* **What this does *not* tell you:** it is **not a vulnerability scan** (no CVE lookup was done, so nothing is claimed about whether the pinned versions have known vulnerabilities) and not a full secret scanner (no entropy analysis, no git-history scan). "0 problems" means only that none of those specific patterns were found. Running `pip-audit` or a dedicated secret scanner would be a sensible next step; neither was run.

## Snowflake adapter — NOT integration-tested

`--backend snowflake` exists so the shared SQL could be pointed at Snowflake. **It has never been run against a Snowflake account** (none was available or used, and this work never connected to one or installed credentials). Unit tests only cover environment-variable validation and the shape of the generated SQL; they prove nothing about cloud behaviour. Treat everything in this section as an untested design, not a supported feature.

Setup, if you decide to try it on a throw-away account yourself:

```text
pip install -r requirements-lock.txt && pip install --no-deps -e . && pip install snowflake-connector-python   # the connector is not in the lock
export SNOWFLAKE_ACCOUNT=<account identifier>    SNOWFLAKE_USER=<user>
export SNOWFLAKE_WAREHOUSE=<warehouse>           SNOWFLAKE_DATABASE=<database>   SNOWFLAKE_SCHEMA=<schema>
export SNOWFLAKE_ROLE=<role>                     # optional
export SNOWFLAKE_PASSWORD=<read it from a secret manager>   # OR  SNOWFLAKE_AUTHENTICATOR=externalbrowser
python -m pipeline status --backend snowflake    # first call creates the tables IF NOT EXISTS
```

Credentials are read from the environment only when this backend is selected, are not written to disk or logged by this project's own code (the connector library is a separate matter and was not audited). Only password and `authenticator` logins are implemented (no key-pair).

Least-privilege suggestions (**unverified**; check against Snowflake's documentation and your own setup):

* Use a **dedicated role** (never `ACCOUNTADMIN`) and a dedicated schema; do not reuse a role that can touch other data.
* `USAGE` on one **small warehouse** (set `AUTO_SUSPEND`, and consider a resource monitor so a mistake cannot burn credits), `USAGE` on the database and schema.
* The pipeline runs `CREATE TABLE IF NOT EXISTS` for its tables and `CREATE TEMPORARY TABLE` for staging on every start, so the role needs `CREATE TABLE` on the schema (at least for the first run). To narrow it, an administrator can pre-create the tables (DDL is generated in `pipeline/warehouse/base.py`: `table_ddl`, `CONTROL_DDL`) and afterwards the role needs only `SELECT, INSERT, UPDATE` on the tables (+ `DELETE` is not needed on permanent tables). Whether `CREATE TABLE IF NOT EXISTS` on an existing table works without the create privilege has **not** been checked.
* Run a **single writer**. Snowflake does **not enforce** `PRIMARY KEY` constraints, so the duplicate-checkpoint protection that DuckDB gives for free is absent; correctness relies on one run at a time and on the checkpoint-recheck described under "Ambiguous commits".
* Known unverified assumptions: `MERGE` semantics with the exact statements generated here, `qmark` parameter binding with `executemany`, `TIMESTAMPTZ` casts from the staged UTC strings, `BEGIN`/`COMMIT`/`ROLLBACK` behaviour with `autocommit=True`, and error classes treated as transient.

## Limitations

* **Single writer.** No locking; two concurrent runs against one warehouse are unsupported. DuckDB itself rejects a second writer process on a file.
* **Whole files are read into memory** (and CSV fields over 131,072 characters are quarantined). Fine for the batch sizes here; not designed for multi-GB files.
* A JSON **array** file is all-or-nothing on JSON *syntax* (a syntax error or `NaN` rejects the file); JSONL isolates problems per line. Prefer JSONL for untrusted feeds.
* A file that is `rejected` or whose rows were quarantined as `future_timestamp` is not retried automatically while its bytes are unchanged; re-deliver it as a new file (or run later with a later `--as-of` on a *changed* file).
* No deletes/tombstones and no history: the tables hold current state only.
* Ambiguous commit outcomes can only be settled by looking at the database again (done automatically on retry/rerun, as described above); a dead connection cannot be repaired by the pipeline.
* Counters (`inserted`/`updated`/`late`) depend on arrival order even though the final state does not.
* State digests are comparable only within one backend and code version.
* Tested only on Python 3.13.5 / Linux with DuckDB 2.0.0.
* `data/generator.py` is exempt from mypy (heterogeneous synthetic record lists; annotating it is future work). ruff style rules that would churn the code (line length etc.) are ignored; correctness and bandit rules are on.

## License

MIT, see [`LICENSE`](LICENSE). Copyright (c) 2026 Saveliev Dmitry.
