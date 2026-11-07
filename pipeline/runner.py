"""Orchestration: discover files -> parse -> validate -> window/dedupe -> one transaction per file.

Semantics (see README):
* Incremental = per-file checkpoint on (name, sha256, window). NOT an event-time watermark filter, so
  late-arriving rows are always applied (if newer than what is stored).
* Version rule: a row replaces the stored one only if (updated_at, row_hash) is greater; on an exact
  content tie the lexicographically smaller source file name is kept as provenance. Stale and
  duplicate rows are counted, never applied. The final table state (including source_file) is
  independent of file/row arrival order. Per-file *counts* (inserted vs updated, late) still depend
  on arrival order, by nature.
* A file's data, quarantine rows, watermark and checkpoint commit atomically; failure => rollback and
  nothing is checkpointed, so a retry/rerun reprocesses it from scratch (safe because merges are idempotent).
  If a COMMIT's outcome is ambiguous (error after the server may have committed), the retry detects
  its own checkpoint at the start of the attempt and reports the file as loaded (see `note`).
* Failure containment: any failure of one file (unreadable source, parse crash, database error) marks
  that file `failed` and the run continues with the others; exit status is non-zero.
* Sources are only ever read. Parsed bytes == hashed bytes.
* Backfill = same run with [since, until) on updated_at; windowed checkpoints are separate from
  unwindowed ones, so a later full run still processes the file (out-of-window rows are skipped, not lost).
"""
from __future__ import annotations

import fnmatch
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable

from .errors import FileRejected
from .retry import RetryPolicy, with_retries
from .schema import EntitySpec
from .sources import entity_for, parse, read_bytes
from .validate import Invalid, validate
from .warehouse.base import Checkpoint, SqlWarehouse

log = logging.getLogger("pipeline")
MAX_FUTURE_SKEW = timedelta(minutes=5)


@dataclass
class FileReport:
    file: str
    entity: str
    sha256: str  # "" if the file could not be read
    status: str  # loaded | rejected | skipped_checkpointed | failed
    attempts: int = 0
    counts: dict[str, int] = field(default_factory=dict)
    quarantine_reasons: dict[str, int] = field(default_factory=dict)
    error: str | None = None
    note: str | None = None


@dataclass
class RunReport:
    files: list[FileReport] = field(default_factory=list)
    ignored: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not any(f.status == "failed" for f in self.files)

    def totals(self) -> dict[str, int]:
        t: dict[str, int] = {}
        for f in self.files:
            for k, v in f.counts.items():
                t[k] = t.get(k, 0) + v
        return t

    def as_dict(self) -> dict:
        return {"ok": self.ok, "totals": self.totals(), "ignored": self.ignored,
                "files": [f.__dict__ for f in self.files]}


def _safe_text(s: str, limit: int) -> str:
    """Escape NUL (rejected by many SQL engines) before it reaches the quarantine table."""
    return s[:limit].replace("\x00", "\\x00")


def _iso(dt: datetime | None) -> str:
    return dt.astimezone(timezone.utc).isoformat() if dt else ""


def _prepare(spec: EntitySpec, path: Path, content: bytes, sha: str, since, until, as_of):
    """Pure step: parse + validate. Returns (records, quarantine_rows, counts, reasons)."""
    quarantine: list[dict] = []
    reasons: dict[str, int] = {}
    best: dict[tuple, dict] = {}
    c = dict(rows_read=0, rows_out_of_window=0, rows_batch_duplicates=0)

    def reject(row_number: int, reason: str, detail: str, raw: str) -> None:
        reasons[reason] = reasons.get(reason, 0) + 1
        quarantine.append(dict(source_name=path.name, file_sha256=sha, row_number=row_number, entity=spec.name,
                               reason=reason, detail=_safe_text(detail, 500), raw_record=_safe_text(raw, 4000)))

    try:
        future_limit = as_of + MAX_FUTURE_SKEW
    except OverflowError:
        future_limit = datetime.max.replace(tzinfo=timezone.utc)

    for row in parse(path, content, spec):
        c["rows_read"] += 1
        if row.data is None:
            reject(row.row_number, row.error or "unparseable", row.detail, row.raw)
            continue
        try:
            rec = validate(spec, row.data)
            if rec["updated_at"] > future_limit:
                raise Invalid("future_timestamp", f"updated_at={rec['updated_at'].isoformat()} > as_of")
        except Invalid as exc:
            reject(row.row_number, exc.reason, exc.detail, row.raw)
            continue
        except Exception as exc:  # noqa: BLE001 - one unforeseen bad row must not take the file down
            reject(row.row_number, "validation_error", f"{type(exc).__name__}: {exc}", row.raw)
            continue
        if (since and rec["updated_at"] < since) or (until and rec["updated_at"] >= until):
            c["rows_out_of_window"] += 1
            continue
        rec["source_file"] = path.name
        key = tuple(rec[k] for k in spec.key)
        cur = best.get(key)
        if cur is not None:
            c["rows_batch_duplicates"] += 1
            if (cur["updated_at"], cur["row_hash"]) >= (rec["updated_at"], rec["row_hash"]):
                continue
        best[key] = rec
    c["rows_quarantined"] = len(quarantine)
    return list(best.values()), quarantine, c, reasons


_PERMANENT_READ_ERRORS = (FileNotFoundError, PermissionError, IsADirectoryError, NotADirectoryError)


def _read_is_transient(exc: BaseException) -> bool:
    """Other OSErrors (EIO, stale NFS handle, timeouts) may clear up; missing/forbidden files will not."""
    return isinstance(exc, OSError) and not isinstance(exc, _PERMANENT_READ_ERRORS)


def _candidates(input_dir: Path, pattern: str) -> list[Path]:
    # is_symlink() keeps dangling symlinks visible so they are reported as failed reads, not skipped silently
    return sorted((p for p in Path(input_dir).iterdir()
                   if (p.is_file() or p.is_symlink()) and fnmatch.fnmatch(p.name, pattern)), key=lambda p: p.name)


def run_pipeline(wh: SqlWarehouse, input_dir: Path, *, since: datetime | None = None, until: datetime | None = None,
                 as_of: datetime | None = None, pattern: str = "*", retry: RetryPolicy | None = None,
                 clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
                 on_file: Callable[[FileReport], None] | None = None) -> RunReport:
    """``on_file`` (optional) is called once per recognised file right after it was processed (progress display)."""
    for name, v in (("since", since), ("until", until), ("as_of", as_of)):
        if v is not None and v.tzinfo is None:
            raise ValueError(f"{name} must be timezone-aware")
    if since and until and since >= until:
        raise ValueError("since must be < until")
    as_of = (as_of or clock()).astimezone(timezone.utc)
    retry = retry or RetryPolicy()
    report = RunReport()
    wstart, wend = _iso(since), _iso(until)

    for path in _candidates(input_dir, pattern):
        spec = entity_for(path)
        if spec is None:
            report.ignored.append(path.name)
            continue
        fr = FileReport(path.name, spec.name, "", "loaded")
        report.files.append(fr)
        retries: list[BaseException] = []  # one entry per retry actually taken, so attempts = 1 + len(retries)
        try:
            _process_file(wh, spec, path, fr, retries, since, until, as_of, wstart, wend, retry, clock)
        except Exception as exc:  # noqa: BLE001 - reported; the run continues with the other files
            fr.status, fr.error = "failed", f"{type(exc).__name__}: {exc}"
            fr.attempts = max(fr.attempts, 1 + len(retries))
            log.error("load failed for %s: %s", path.name, fr.error)
        if on_file is not None:
            on_file(fr)
    return report


def _process_file(wh, spec, path, fr, retries, since, until, as_of, wstart, wend, retry, clock) -> None:
    def read() -> tuple[bytes, str]:
        return read_bytes(path)

    try:
        (content, sha), _ = with_retries(read, retry, _read_is_transient, on_retry=lambda n, e: retries.append(e))
    except OSError as exc:
        raise OSError(f"cannot read source file: {exc}") from exc
    fr.sha256 = sha
    if wh.has_checkpoint(path.name, sha, wstart, wend):
        fr.status = "skipped_checkpointed"
        return
    cp = Checkpoint(path.name, sha, wstart, wend, spec.name, "loaded")
    try:
        recs, quarantine, counts, reasons = _prepare(spec, path, content, sha, since, until, as_of)
    except FileRejected as exc:
        fr.status, cp.status = "rejected", "rejected"
        quarantine = [dict(source_name=path.name, file_sha256=sha, row_number=0, entity=spec.name,
                           reason="file_rejected", detail=_safe_text(str(exc), 500), raw_record="")]
        recs, counts, reasons = [], dict(rows_read=0, rows_quarantined=1), {"file_rejected": 1}
    fr.quarantine_reasons = reasons

    def load() -> dict[str, int]:
        with wh.transaction():
            # Retry-safety: if an earlier attempt's COMMIT reported an error but had actually been
            # applied, its checkpoint is already there. Do not redo (the checkpoint INSERT would hit
            # the primary key); report what that attempt stored.
            prior = wh.checkpoint_counts(path.name, sha, wstart, wend)
            if prior is not None:
                fr.note = "checkpoint found at start of attempt: an earlier commit had succeeded"
                return prior
            stats = wh.merge(spec, recs)
            wh.add_quarantine(quarantine)
            if recs:
                wh.advance_watermark(spec.name, max(r["updated_at"] for r in recs))
            cp.counts = {**counts, "rows_inserted": stats.inserted, "rows_updated": stats.updated,
                         "rows_unchanged": stats.unchanged, "rows_stale": stats.stale, "rows_late": stats.late}
            cp.processed_at = clock()
            wh.put_checkpoint(cp)  # last statement: commits iff everything above did
        return cp.counts

    def on_retry(n: int, exc: BaseException) -> None:
        retries.append(exc)
        log.warning("retrying %s after attempt %d: %s", path.name, n, exc)

    fr.counts, _ = with_retries(load, retry, wh.is_transient, on_retry=on_retry)
    fr.attempts = 1 + len(retries)
