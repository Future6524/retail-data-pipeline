"""Project 1 showcase: ``python -m pipeline showcase`` (terminal progress bar + summary table + results.html/csv).

Everything shown is produced by the real pipeline run on SYNTHETIC data: the progress bar advances once per file
that was actually processed, and every number in the tables is read from the run reports (nothing is typed in).
One run injects a SYNTHETIC commit fault on purpose; it is labelled everywhere and is not a real outage.
It never opens a browser or starts a server; it prints the command that opens the results.
"""
from __future__ import annotations

import csv
import fnmatch
import logging
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import TextIO

from .faults import FaultPlan
from .retry import RetryPolicy
from .runner import FileReport, RunReport, _candidates, run_pipeline
from .sources import entity_for
from .ui import ProgressBar, browser_command, sheet_html, table_for, table_html
from .warehouse import open_warehouse

AS_OF = datetime(2026, 2, 1, tzinfo=timezone.utc)
FAULT_LABEL = "SYNTHETIC injected commit fault (NOT a real outage)"
COUNT_COLS = [("Rows read", "rows_read"), ("Inserted", "rows_inserted"), ("Updated", "rows_updated"),
              ("Unchanged", "rows_unchanged"), ("Stale", "rows_stale"), ("Late", "rows_late"),
              ("Batch dups", "rows_batch_duplicates"), ("Quarantined", "rows_quarantined"),
              ("Out of window", "rows_out_of_window")]
FILE_HEADERS = ["Run", "Scenario", "File", "Entity", "Status", "Attempts"] + [h for h, _ in COUNT_COLS] + [
    "SHA-256 (12)", "Error", "Note", "Synthetic fault"]
NUM_FROM = 5  # columns from "Attempts" up to the last count are right-aligned numbers
NUM_COLS = tuple(range(NUM_FROM, NUM_FROM + 1 + len(COUNT_COLS)))


@dataclass
class RunRecord:
    n: int
    scenario: str
    report: RunReport
    fault: str | None = None

    def status_counts(self) -> dict[str, int]:
        out: dict[str, int] = {}
        for f in self.report.files:
            out[f.status] = out.get(f.status, 0) + 1
        return out


def file_row(r: RunRecord, f: FileReport) -> list[object]:
    return [r.n, r.scenario, f.file, f.entity, f.status, f.attempts] + [f.counts.get(k, 0) for _, k in COUNT_COLS] + [
        f.sha256[:12], f.error or "", f.note or "", r.fault or ""]


def all_file_rows(records: list[RunRecord]) -> list[list[object]]:
    return [file_row(r, f) for r in records for f in r.report.files]


def summary_rows(records: list[RunRecord]) -> list[list[object]]:
    rows: list[list[object]] = []
    for r in records:
        sc, t = r.status_counts(), r.report.totals()
        rows.append([r.n, r.scenario, len(r.report.files), sc.get("loaded", 0), sc.get("skipped_checkpointed", 0),
                     sc.get("rejected", 0), sc.get("failed", 0), t.get("rows_inserted", 0), t.get("rows_updated", 0),
                     t.get("rows_quarantined", 0), "ok" if r.report.ok else "FAILED (fault)"])
    return rows


SUMMARY_HEADERS = ["Run", "Scenario", "Files", "Loaded", "Skip", "Reject", "Fail", "Insert", "Update", "Quar.", "Result"]


def write_results(workdir: Path, records: list[RunRecord], seed: int) -> tuple[Path, Path]:
    rows = all_file_rows(records)
    csv_path, html_path = workdir / "results.csv", workdir / "results.html"
    with csv_path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(FILE_HEADERS)
        w.writerows(rows)
    notes = [f"SYNTHETIC data only (seed {seed}); produced by `python -m pipeline showcase`. Nothing here is real customer data.",
             "Run 4 injects a SYNTHETIC commit fault on purpose (labelled in the last column); it is not a real outage.",
             f"{len(rows)} file results across {len(records)} runs. Scroll down for more rows and right for more columns."]
    page = sheet_html("Retail pipeline - file results (SYNTHETIC)", notes, [
        ("Per-file results", table_html(FILE_HEADERS, rows, NUM_COLS, table_id="file-results")),
        ("Run summary", table_html(SUMMARY_HEADERS, summary_rows(records), tuple(range(2, 10)), table_id="run-summary"))])
    html_path.write_text(page, encoding="utf-8")
    return csv_path, html_path


def count_files(batches: Path, pattern: str) -> int:
    return sum(1 for p in _candidates(batches, pattern) if entity_for(p) is not None and fnmatch.fnmatch(p.name, pattern))


def run_showcase(workdir: Path, seed: int = 42, out: TextIO | None = None) -> int:
    plog = logging.getLogger("pipeline")  # retry/error log lines would break the progress bar; attempts are in the table
    old_level, plog.level = plog.level, logging.CRITICAL
    try:
        return _run_showcase(workdir, seed, out or sys.stdout)
    finally:
        plog.setLevel(old_level)


def _run_showcase(workdir: Path, seed: int, out: TextIO) -> int:
    from data import generate_demo_batches

    from .oracle import warehouse_summary
    workdir.mkdir(parents=True, exist_ok=True)
    batches, db = workdir / "batches", workdir / "warehouse.duckdb"
    for stale in (db, Path(str(db) + ".wal")):
        stale.unlink(missing_ok=True)
    print("Retail pipeline showcase  -  SYNTHETIC data (seed %d), real pipeline, nothing simulated" % seed, file=out)
    manifest = generate_demo_batches(batches, seed=seed)
    n_files = len(manifest["files"])
    plan = [("batch 1 load", "*_batch1.*"), ("all files", "*"), ("rerun (no-op)", "*"),
            ("SYNTHETIC fault", "*"), ("recovery", "*")]
    bar = ProgressBar(sum(count_files(batches, pat) for _, pat in plan), out)
    retry = RetryPolicy(max_attempts=3, base_delay=0.02)
    records: list[RunRecord] = []
    checks: list[tuple[str, bool]] = []

    def one(n: int, wh, fault: str | None = None) -> RunRecord:
        scenario, pattern = plan[n - 1]

        def tick(f: FileReport) -> None:
            bar.advance(f"run {n}/5 {scenario}", f"{f.file} {f.status}")
        rep = run_pipeline(wh, batches, as_of=AS_OF, pattern=pattern, retry=retry, on_file=tick)
        rec = RunRecord(n, scenario, rep, fault)
        records.append(rec)
        return rec

    wh = open_warehouse("duckdb", db)
    r1 = one(1, wh)
    r2 = one(2, wh)
    digest = wh.state_digest()
    r3 = one(3, wh)
    checks += [("run 1: batch 1 loaded, none failed", r1.report.ok and r1.status_counts() == {"loaded": 3}),
               ("run 2: 3 skipped via checkpoint, 1 corrupt file rejected, rest loaded",
                r2.report.ok and r2.status_counts() == {"skipped_checkpointed": 3, "rejected": 1, "loaded": n_files - 4}),
               ("run 3: every file skipped, state digest unchanged",
                r3.status_counts() == {"skipped_checkpointed": n_files} and wh.state_digest() == digest),
               ("warehouse matches the generator's ground truth (oracle)", warehouse_summary(wh) == manifest["expected_final"])]
    wh.close()
    wh2 = open_warehouse("duckdb", ":memory:")
    fplan = FaultPlan(wh2, {"inventory_batch2.csv": 1, "orders_batch3.jsonl": 99}).install()
    r4 = one(4, wh2, FAULT_LABEL)
    by = {f.file: f for f in r4.report.files}
    fplan.remove()
    r5 = one(5, wh2)
    checks += [("run 4: injected fault -> run reported failed (orders_batch3.jsonl, 3 attempts); retry recovered inventory_batch2.csv",
                not r4.report.ok and by["orders_batch3.jsonl"].status == "failed" and by["orders_batch3.jsonl"].attempts == 3
                and by["inventory_batch2.csv"].status == "loaded" and by["inventory_batch2.csv"].attempts == 2),
               ("run 5: only the failed file does work and loads", r5.report.ok and
                {f.file for f in r5.report.files if f.status == "loaded"} == {"orders_batch3.jsonl"})]
    wh2.close()
    bar.finish("done")

    print("", file=out)
    print(table_for(out, SUMMARY_HEADERS, summary_rows(records), right=tuple(range(2, 10))), file=out)
    bad = [name for name, ok in checks if not ok]
    csv_path, html_path = write_results(workdir, records, seed)
    rows = sum(len(r.report.files) for r in records)
    print(f"\n{rows} file results, {len(records)} runs, {sum(ok for _, ok in checks)}/{len(checks)} checks passed"
          f"{'' if not bad else '  FAILED: ' + '; '.join(bad)}", file=out)
    print(f"results (HTML, scrollable) : {html_path}\nresults (CSV)              : {csv_path}", file=out)
    print(f"open the results           : {browser_command(html_path)}", file=out)
    return 0 if not bad else 1
