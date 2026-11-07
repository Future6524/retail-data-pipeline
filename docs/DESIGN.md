# Pipeline internals

Companion to the top-level [README](../README.md) (which covers install, CLI, data dictionary and user-facing
semantics). This page explains *how one file travels through the code* and *why* the design is shaped this way.

## One file, step by step (`runner._process_file`)

```text
                 ┌────────────── per file, in sorted-name order; any exception here = that file "failed" ──────────────┐
 list dir ─► entity_for(name)? ──no──► report.ignored
                    │yes
                    ▼
        read bytes + SHA-256   (retry transient OSError; PermissionError / FileNotFound are permanent)
                    │
                    ▼
        checkpoint (name, sha, window)? ──yes──► status = skipped_checkpointed
                    │no
                    ▼
        _prepare (pure, no DB): parse → validate → future check → window filter → in-file dedupe
            │ FileRejected ──► status = rejected; one `file_rejected` quarantine row; checkpoint status 'rejected'
            ▼
        load()  — retried only for transient errors, ONE TRANSACTION per attempt:
            BEGIN
              own checkpoint already there?  ─yes─► return its stored counters  (an earlier ambiguous COMMIT had worked)
              stage → classify (insert/update/unchanged/stale/late) → MERGE
              quarantine rows (insert-if-absent)
              advance watermark (monotonic, informational)
              INSERT checkpoint                       ◄── last statement
            COMMIT     (if the body or COMMIT raises: best-effort ROLLBACK, then re-raise)
```

## Failure matrix

| What goes wrong | Where it is caught | Outcome | Checkpoint written? |
|---|---|---|---|
| One malformed row (any format) | `sources` / `validate` / `runner.reject` | row in `quarantine` with reason + raw text; file continues | yes (file loaded) |
| Unexpected exception validating one row | `runner._prepare` | row quarantined as `validation_error` | yes |
| Whole file structurally bad (encoding, header, JSON syntax) | `FileRejected` | `rejected`, one `file_rejected` row | yes, status `rejected` |
| Source unreadable / vanished / dangling symlink | `runner.run_pipeline` | `failed`; run continues; exit 1 | no |
| Parser/other crash on a file | `runner.run_pipeline` | `failed`; run continues | no |
| Transient DB error in `load()` | `with_retries` | retried with backoff up to `--retries` attempts, then `failed` | only if an attempt commits |
| Permanent DB error | `with_retries` | `failed` after 1 attempt | no (rolled back) |
| `COMMIT` raises | `SqlWarehouse.transaction` | best-effort rollback; retried; a retry that finds its own checkpoint reports `loaded` + `note` | as committed |
| `ROLLBACK` also fails | `SqlWarehouse.transaction` | connection marked unsafe; later transactions refused (`connection state unknown`); rerun settles it | unknown until rerun |
| `KeyboardInterrupt` mid-load | `transaction` (BaseException) | rollback, propagates | no |

## Design decisions and trade-offs

* **Checkpoint per `(file name, content hash, window)` instead of an event-time watermark filter.** A watermark filter drops late data
  silently; a content-hash checkpoint cannot. The cost is re-reading files whose bytes changed, which is exactly when you want to.
* **Checkpoint is the last statement of the transaction.** "Data committed" and "file recorded as done" can never diverge, so a crash anywhere
  leaves either both or neither. Merges are idempotent, so doing a file twice is harmless; that is why every failure path may simply redo the file.
* **Versioning by `(updated_at, row_hash)`.** Deterministic and order-independent: no reliance on arrival order or wall clock. `row_hash`
  hashes the canonical record (normalised money/timestamps/case), so `"10.5"` and `"10.50"` are the same version.
* **Provenance tie-break (smallest `source_file`).** `source_file` is part of the stored row and of the state digest. Without a rule, whichever
  of two byte-identical files arrived first would own the row and the digest would depend on arrival order. The rule is applied in the `MERGE`
  (`WHEN MATCHED AND (… OR (same updated_at AND same row_hash AND s.source_file < t.source_file))`), and such provenance-only changes are
  counted as `unchanged`, because the business data did not change.
* **Row-level containment first, file-level rejection only when trust is gone.** CSV is read with `strict=True`; a `csv.Error` (including the
  131,072-character field limit) quarantines that record and reading resumes at the next line. A duplicated CSV header, however, means a column
  would silently vanish, so the file is rejected. JSONL lines are split on `\n` only (not `str.splitlines()`, which also splits on U+2028 etc. that are legal inside JSON strings).
* **Strict scalars.** Money/integers accept plain ASCII decimal text or JSON numbers (parsed as `Decimal`); NUL and lone surrogates are rejected,
  because they would otherwise fail the whole file's transaction at load time instead of one row's validation.
* **Backend-neutral SQL in `warehouse/base.py`.** Subclasses supply only connection handling, statement execution and error classification.
  Staging tables are session temp tables created outside transactions (Snowflake DDL implicitly commits) and only touched with DML inside them.

## Ambiguous commits, precisely

A `COMMIT` that raises tells you *"I don't know"*, not *"it did not happen"*:

1. Error before the server applied the commit → nothing persisted. We roll back (usually a no-op or a clean abort) and retry.
2. Error after the server applied it (acknowledgement lost) → data and checkpoint **are** persisted. A blind retry would redo the merge harmlessly but
   then fail on the checkpoint `INSERT` (primary key) and misreport a good file as failed. So `load()` first looks for its own checkpoint; if present it
   returns the stored counters and sets `FileReport.note`.
3. Connection dead, rollback impossible → the transaction's fate is unknowable from this process. The warehouse object refuses further writes. A new
   process/run resolves it by reading the checkpoint (present → skipped; absent → redone).

All three paths are exercised with DuckDB through `pipeline/faults.py` and monkeypatched `_commit` (`tests/test_failures_and_commits.py`), including reopening the database file
to prove what was durably committed. DuckDB is a single-process embedded engine: it can simulate these *sequences* but not real network partitions, so
network-level behaviour (in particular on Snowflake) remains unverified.

## Adding things

* **New entity:** add an `EntitySpec` in `schema.py` (columns, key), a validator in `validate.py` (`_VALIDATORS`), register it in `ENTITIES`; extend the generator oracle and `oracle.warehouse_summary`.
* **New backend:** subclass `SqlWarehouse` (`_execute`, `_executemany`, `is_transient`, `close`, optionally `_no_active_transaction`), register it in `warehouse/__init__.py`, and run the whole test-suite against it before trusting it; DuckDB passing says nothing about another engine's `MERGE`, typing or transaction behaviour.
