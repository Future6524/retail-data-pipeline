"""CLI: python -m pipeline <generate|run|backfill|status|demo|showcase> ..."""
from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

from .retry import RetryPolicy
from .runner import RunReport, run_pipeline
from .warehouse import open_warehouse

DEMO_AS_OF = datetime(2026, 2, 1, tzinfo=timezone.utc)  # fixed so the demo is reproducible


def aware(s: str) -> datetime:
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            raise argparse.ArgumentTypeError(f"{s!r} needs a UTC offset, e.g. 2026-01-10T00:00:00Z")
        return dt.astimezone(timezone.utc)
    except ValueError:
        raise argparse.ArgumentTypeError(f"{s!r} is not an ISO-8601 timestamp, e.g. 2026-01-10T00:00:00Z") from None
    except OverflowError:
        raise argparse.ArgumentTypeError(f"{s!r} is out of the supported range") from None


def positive_int(s: str) -> int:
    try:
        n = int(s)
    except ValueError:
        raise argparse.ArgumentTypeError(f"{s!r} is not an integer") from None
    if n < 1:
        raise argparse.ArgumentTypeError("must be >= 1 (the first try counts as an attempt)")
    return n


def _common(p: argparse.ArgumentParser) -> None:
    p.add_argument("--backend", choices=["duckdb", "snowflake"], default="duckdb",
                   help="snowflake is NOT integration-tested; needs SNOWFLAKE_* env vars")
    p.add_argument("--db", default=".demo/warehouse.duckdb", help="DuckDB file (ignored for snowflake)")


def _run_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("--input", required=True, help="directory of source files (read-only use)")
    p.add_argument("--pattern", default="*", help="glob of file names to consider")
    p.add_argument("--as-of", type=aware, help="reference 'now' for future-timestamp checks (default: current time)")
    p.add_argument("--retries", type=positive_int, default=3,
                   help="max attempts per file for transient errors, first try included (default 3)")
    p.add_argument("--json", action="store_true",
                   help="print ONE JSON object (report + state_digest) on stdout and nothing else; logs go to stderr")


def _print_report(rep: RunReport, indent: str = "  ") -> None:
    for f in rep.files:
        c = f.counts
        extra = (f"read={c.get('rows_read', 0)} ins={c.get('rows_inserted', 0)} upd={c.get('rows_updated', 0)} "
                 f"same={c.get('rows_unchanged', 0)} stale={c.get('rows_stale', 0)} late={c.get('rows_late', 0)} "
                 f"dup={c.get('rows_batch_duplicates', 0)} quar={c.get('rows_quarantined', 0)} "
                 f"oow={c.get('rows_out_of_window', 0)}") if c else ""
        if f.attempts > 1:
            extra += f" attempts={f.attempts}"
        if f.error:
            extra += f" ERROR: {f.error}"
        if f.note:
            extra += f" NOTE: {f.note}"
        print(f"{indent}{f.file:40s} {f.status:21s} {extra.strip()}")
    if rep.ignored:
        print(f"{indent}ignored (unrecognised): {', '.join(rep.ignored)}")


def _do_run(args, since=None, until=None) -> int:
    wh = open_warehouse(args.backend, args.db)
    try:
        rep = run_pipeline(wh, Path(args.input), since=since, until=until, as_of=args.as_of, pattern=args.pattern,
                           retry=RetryPolicy(max_attempts=args.retries))
        digest = wh.state_digest()
        if args.json:
            print(json.dumps({**rep.as_dict(), "state_digest": digest}, indent=2))
        else:
            _print_report(rep)
            print(f"state digest: {digest}")
        return 0 if rep.ok else 1
    finally:
        wh.close()


def _status(args) -> int:
    wh = open_warehouse(args.backend, args.db)
    try:
        for t, n in wh.table_counts().items():
            print(f"{t:18s} {n}")
        print("watermarks:", {e: w for e, w in wh.query("SELECT entity, CAST(max_updated_at AS VARCHAR) FROM watermarks")})
        print("quarantine by reason:", {r: n for r, n in wh.query(
            "SELECT reason, COUNT(*) FROM quarantine GROUP BY reason ORDER BY reason")})
        print(f"state digest: {wh.state_digest()}")
        return 0
    finally:
        wh.close()


def _generate(args) -> int:
    from data import generate_demo_batches
    m = generate_demo_batches(args.out, seed=args.seed, n_orders=args.orders)
    print(f"wrote {len(m['files'])} SYNTHETIC batch files + manifest.json to {args.out}")
    return 0


class _Checks:
    """Collects named pass/fail checks and prints each one, so a failure says which invariant broke."""

    def __init__(self) -> None:
        self.results: list[tuple[str, bool]] = []

    def __call__(self, name: str, ok: bool) -> bool:
        self.results.append((name, bool(ok)))
        print(f"   [{'ok' if ok else 'FAIL'}] {name}")
        return bool(ok)

    @property
    def all_ok(self) -> bool:
        return all(ok for _, ok in self.results)


def _statuses(rep: RunReport) -> dict[str, int]:
    out: dict[str, int] = {}
    for f in rep.files:
        out[f.status] = out.get(f.status, 0) + 1
    return out


def _demo(args) -> int:
    from data import generate_demo_batches

    from .faults import FaultPlan
    from .oracle import warehouse_summary
    work = Path(args.workdir)
    batches, db = work / "batches", work / "warehouse.duckdb"
    for stale in (db, Path(str(db) + ".wal")):
        stale.unlink(missing_ok=True)
    m = generate_demo_batches(batches, seed=args.seed)
    n_files = len(m["files"])
    print(f"[SYNTHETIC DATA] generated {n_files} files (seed={args.seed}) in {batches}")
    retry = RetryPolicy(max_attempts=3, base_delay=0.05)
    check = _Checks()

    def step(title, wh, **kw) -> RunReport:
        print(f"\n== {title}")
        rep = run_pipeline(wh, batches, as_of=DEMO_AS_OF, retry=retry, **kw)
        _print_report(rep)
        return rep

    wh = open_warehouse("duckdb", db)
    rep1 = step("1. incremental load: batch1 only", wh, pattern="*_batch1.*")
    check("step 1: no failed files, all 3 batch1 files loaded", rep1.ok and _statuses(rep1) == {"loaded": 3})
    rep2 = step("2. next run, all files (batch1 skipped via checkpoint; late/stale/corrections applied)", wh)
    check("step 2: no failed files; 3 skipped, 1 corrupt file rejected, rest loaded",
          rep2.ok and _statuses(rep2) == {"skipped_checkpointed": 3, "rejected": 1, "loaded": n_files - 4})
    d_before = wh.state_digest()
    rep3 = step("3. rerun, identical inputs (expect no-op)", wh)
    d_after = wh.state_digest()
    check("step 3: every file skipped_checkpointed", rep3.ok and _statuses(rep3) == {"skipped_checkpointed": n_files})
    check("step 3: state digest unchanged by the rerun", d_before == d_after)
    inc_data = wh.state_digest(include_checkpoints=False)
    got = warehouse_summary(wh)
    exp = m["expected_final"]
    print("\n== oracle check: generator ground truth vs warehouse (order totals are per currency)")
    for k in exp:
        check(f"oracle {k}: {got[k]}", got[k] == exp[k])
    if got != exp:
        print(f"   warehouse={got}\n   expected ={exp}")
    wh.close()

    wh2 = open_warehouse("duckdb", ":memory:")
    print("\n== 4. fresh warehouse: bounded backfill windows then full run")
    w1, w2 = datetime(2026, 1, 10, tzinfo=timezone.utc), datetime(2026, 1, 12, tzinfo=timezone.utc)
    ra = step("4a. backfill [2026-01-10, 2026-01-12)", wh2, since=w1, until=w2)
    rb = step("4b. backfill [2026-01-12, 2026-02-01)", wh2, since=w2, until=DEMO_AS_OF)
    rc = step("4c. full run (windowed checkpoints don't block it)", wh2)
    check("step 4: no failed files in any backfill/full report", ra.ok and rb.ok and rc.ok)
    check("step 4c: the two windows already covered every row (full run inserts nothing)",
          rc.totals().get("rows_inserted", 0) == 0)
    check("step 4: data digest equals the incremental path", wh2.state_digest(include_checkpoints=False) == inc_data)
    wh2.close()

    print("\n== 5. SYNTHETIC fault injection (the demo itself makes these commits fail; this is NOT a real outage)")
    wh3 = open_warehouse("duckdb", ":memory:")
    plan = FaultPlan(wh3, {"inventory_batch2.csv": 1, "orders_batch3.jsonl": 99}).install()
    print("   plan: inventory_batch2.csv commit fails once (retry should recover);"
          " orders_batch3.jsonl commit always fails (retries should exhaust, run must continue)")
    rf = step("5a. full run with injected commit faults", wh3)
    byname = {f.file: f for f in rf.files}
    check("5a: inventory_batch2.csv recovered on attempt 2",
          byname["inventory_batch2.csv"].status == "loaded" and byname["inventory_batch2.csv"].attempts == 2)
    check("5a: orders_batch3.jsonl failed after exactly 3 attempts, nothing committed for it",
          byname["orders_batch3.jsonl"].status == "failed" and byname["orders_batch3.jsonl"].attempts == 3
          and wh3.query("SELECT COUNT(*) FROM checkpoints WHERE source_name='orders_batch3.jsonl'")[0][0] == 0)
    check("5a: every other file still processed; report is not-ok (exit status would be 1)",
          not rf.ok and _statuses(rf) == {"loaded": n_files - 2, "failed": 1, "rejected": 1})
    check("5a: exactly 4 faults injected (1 + 3)", len(plan.injected) == 4)
    plan.remove()
    rr = step("5b. rerun with the fault removed (only the failed file does work)", wh3)
    check("5b: orders_batch3.jsonl now loaded; everything else skipped",
          rr.ok and {f.file for f in rr.files if f.status == "loaded"} == {"orders_batch3.jsonl"})
    check("5b: data digest equals the fault-free incremental path", wh3.state_digest(include_checkpoints=False) == inc_data)
    wh3.close()

    print(f"\nRESULT: {'all demo invariants held' if check.all_ok else 'INVARIANT VIOLATION'}"
          f" ({sum(ok for _, ok in check.results)}/{len(check.results)} checks)")
    return 0 if check.all_ok else 1


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="retail-pipeline", description="SYNTHETIC retail data pipeline")
    ap.add_argument("-v", "--verbose", action="store_true")
    sub = ap.add_subparsers(dest="cmd", required=True)
    g = sub.add_parser("generate", help="write deterministic SYNTHETIC demo batches")
    g.add_argument("--out", required=True)
    g.add_argument("--seed", type=int, default=42)
    g.add_argument("--orders", type=int, default=40)
    r = sub.add_parser("run", help="incremental, idempotent load")
    _common(r); _run_args(r)
    b = sub.add_parser("backfill", help="load only rows with updated_at in [since, until)")
    _common(b); _run_args(b)
    b.add_argument("--since", type=aware, required=True)
    b.add_argument("--until", type=aware, required=True)
    s = sub.add_parser("status", help="warehouse row counts, watermarks, quarantine summary")
    _common(s)
    d = sub.add_parser("demo", help="generate + load + rerun + backfill, checking invariants")
    d.add_argument("--workdir", default=".demo")
    d.add_argument("--seed", type=int, default=42)
    sc = sub.add_parser("showcase", help="progress bar + summary table + scrollable results.html (SYNTHETIC, real run)")
    sc.add_argument("--workdir", default=".demo/showcase")
    sc.add_argument("--seed", type=int, default=42)
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO if args.verbose else logging.WARNING, format="%(levelname)s %(message)s")
    try:
        if args.cmd == "generate":
            return _generate(args)
        if args.cmd == "run":
            return _do_run(args)
        if args.cmd == "backfill":
            return _do_run(args, args.since, args.until)
        if args.cmd == "status":
            return _status(args)
        if args.cmd == "showcase":
            from .showcase import run_showcase
            return run_showcase(Path(args.workdir), args.seed)
        return _demo(args)
    except Exception as exc:  # noqa: BLE001 - CLI boundary
        print(f"error: {exc}", file=sys.stderr)
        return 2
