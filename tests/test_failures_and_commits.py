"""Retry exhaustion, commit failures (ambiguous and not), rollback, source read failures, fault injection."""
import errno
import os

import pytest
from conftest import AS_OF, FAST, by_file, jl, order, write

from pipeline.errors import PipelineError, TransientError
from pipeline.faults import FaultPlan, InjectedFault
from pipeline.retry import RetryPolicy
from pipeline.runner import run_pipeline
from pipeline.warehouse import open_warehouse


def run(wh, d, **kw):
    kw.setdefault("retry", FAST)
    return run_pipeline(wh, d, as_of=AS_OF, **kw)


def two_files(tmp_path):
    write(tmp_path, "orders_a.jsonl", jl(order("A")))
    write(tmp_path, "orders_b.jsonl", jl(order("B"), order("B2")))
    return tmp_path


def state(wh):
    return (wh.table_counts(), wh.query("SELECT COUNT(*) FROM watermarks")[0][0])


# ------------------------------------------------------------------------------- retry mechanics
def test_retry_policy_rejects_nonsense_attempts():
    with pytest.raises(ValueError):
        RetryPolicy(max_attempts=0)


@pytest.mark.parametrize("attempts", [1, 2, 3, 5])
def test_retry_exhaustion_reports_exact_attempts_and_run_continues(tmp_path, attempts):
    d = two_files(tmp_path)
    wh = open_warehouse("duckdb")
    plan = FaultPlan(wh, {"orders_a.jsonl": 99}).install()
    rep = run(wh, d, retry=RetryPolicy(max_attempts=attempts, sleep=lambda s: None))
    a, b = by_file(rep)["orders_a.jsonl"], by_file(rep)["orders_b.jsonl"]
    assert not rep.ok and a.status == "failed" and a.attempts == attempts and "SYNTHETIC" in a.error
    assert b.status == "loaded" and b.attempts == 1  # later file unaffected
    assert len(plan.injected) == attempts
    assert wh.query("SELECT order_id FROM orders ORDER BY 1") == [("B",), ("B2",)]
    assert wh.query("SELECT COUNT(*) FROM checkpoints WHERE source_name='orders_a.jsonl'") == [(0,)]
    plan.remove()
    rep2 = run(wh, d)  # recovery: only the failed file does work
    assert rep2.ok and {f.file: f.status for f in rep2.files} == {"orders_a.jsonl": "loaded", "orders_b.jsonl": "skipped_checkpointed"}


def test_transient_failure_backoff_sleeps_are_exponential_and_capped(tmp_path):
    d = two_files(tmp_path)
    wh = open_warehouse("duckdb")
    sleeps = []
    FaultPlan(wh, {"orders_a.jsonl": 99}).install()
    run(wh, d, retry=RetryPolicy(max_attempts=6, base_delay=1.0, max_delay=4.0, sleep=sleeps.append))
    assert sleeps == [1.0, 2.0, 4.0, 4.0, 4.0]


def test_non_transient_failure_is_not_retried(tmp_path, monkeypatch):
    d = two_files(tmp_path)
    wh = open_warehouse("duckdb")
    real = wh.put_checkpoint

    def boom(cp):
        if cp.source_name == "orders_a.jsonl":
            raise RuntimeError("disk full")
        real(cp)

    monkeypatch.setattr(wh, "put_checkpoint", boom)
    rep = run(wh, d)
    a = by_file(rep)["orders_a.jsonl"]
    assert a.status == "failed" and a.attempts == 1 and "RuntimeError" in a.error
    assert by_file(rep)["orders_b.jsonl"].status == "loaded"


def test_transient_then_permanent_failure_counts_both_attempts(tmp_path, monkeypatch):
    d = two_files(tmp_path)
    wh = open_warehouse("duckdb")
    real, n = wh.put_checkpoint, []

    def boom(cp):
        if cp.source_name == "orders_a.jsonl":
            n.append(1)
            raise TransientError("blip") if len(n) == 1 else RuntimeError("then permanent")
        real(cp)

    monkeypatch.setattr(wh, "put_checkpoint", boom)
    a = by_file(run(wh, d))["orders_a.jsonl"]
    assert a.status == "failed" and a.attempts == 2 and "RuntimeError" in a.error


def test_timeout_and_connection_errors_are_transient_on_every_backend_base():
    wh = open_warehouse("duckdb")
    assert wh.is_transient(TimeoutError()) and wh.is_transient(ConnectionResetError()) and wh.is_transient(TransientError())
    assert not wh.is_transient(ValueError()) and not wh.is_transient(PipelineError())


def test_injected_fault_is_labelled_synthetic():
    assert issubclass(InjectedFault, TransientError)
    wh = open_warehouse("duckdb")
    with pytest.raises(ValueError):
        FaultPlan(wh, {}, mode="sideways")


# ------------------------------------------------- COMMIT failures (regression: probe 02 items 7-9)
def raising_commit(wh, exc, *, really_commit=False, times=1):
    """Replace wh._commit so that its first `times` calls raise `exc` (optionally after a real COMMIT)."""
    real, left = wh._commit, [times]

    def commit():
        if left[0] > 0:
            left[0] -= 1
            if really_commit:
                real()
            raise exc
        real()

    wh._commit = commit


def test_commit_timeout_before_commit_rolls_back_and_nothing_is_visible(tmp_path):
    """Probe 02: a TimeoutError from _commit left the transaction open, so data + checkpoint were visible
    on the same connection and a rerun skipped the (never committed) file."""
    d = two_files(tmp_path)
    wh = open_warehouse("duckdb")
    raising_commit(wh, TimeoutError("synthetic pre-commit fault"), times=99)
    rep = run(wh, d, pattern="orders_b.jsonl")
    f = rep.files[0]
    assert not rep.ok and f.status == "failed" and f.attempts == 3 and "TimeoutError" in f.error
    assert state(wh) == ({"orders": 0, "inventory": 0, "support_tickets": 0, "quarantine": 0, "checkpoints": 0}, 0)
    assert wh._unsafe is None  # rollback worked, connection is reusable


def test_commit_failure_state_on_reopen_and_correct_retry(tmp_path):
    db = tmp_path / "w.duckdb"
    src = tmp_path / "src"
    src.mkdir()
    two_files(src)
    wh = open_warehouse("duckdb", db)
    raising_commit(wh, TimeoutError("synthetic pre-commit fault"), times=99)
    assert not run(wh, src).ok
    wh.close()
    wh = open_warehouse("duckdb", db)  # REOPEN: what is actually on disk
    assert wh.table_counts() == {"orders": 0, "inventory": 0, "support_tickets": 0, "quarantine": 0, "checkpoints": 0}
    rep = run(wh, src)  # rerun with a healthy connection loads everything exactly once
    assert rep.ok and {f.status for f in rep.files} == {"loaded"}
    wh.close()
    wh = open_warehouse("duckdb", db)
    assert wh.table_counts()["orders"] == 3 and wh.table_counts()["checkpoints"] == 2
    assert {f.status for f in run(wh, src).files} == {"skipped_checkpointed"}


def test_single_commit_timeout_then_retry_commits_correctly(tmp_path):
    db, src = tmp_path / "w.duckdb", tmp_path / "src"
    src.mkdir()
    two_files(src)
    wh = open_warehouse("duckdb", db)
    raising_commit(wh, TimeoutError("synthetic pre-commit fault"), times=1)
    rep = run(wh, src)
    assert rep.ok and by_file(rep)["orders_a.jsonl"].attempts == 2 and by_file(rep)["orders_a.jsonl"].note is None
    wh.close()
    wh = open_warehouse("duckdb", db)
    assert wh.query("SELECT order_id FROM orders ORDER BY 1") == [("A",), ("B",), ("B2",)]
    assert wh.table_counts()["checkpoints"] == 2


def test_ambiguous_commit_that_actually_committed_is_recognised_on_retry(tmp_path):
    """The network ack is lost AFTER the server committed. The retry must not re-insert the checkpoint
    (primary-key error) nor report failure; it must notice its own checkpoint and say so."""
    db, src = tmp_path / "w.duckdb", tmp_path / "src"
    src.mkdir()
    two_files(src)
    wh = open_warehouse("duckdb", db)
    plan = FaultPlan(wh, {"orders_b.jsonl": 1}, mode="after").install()
    rep = run(wh, src)
    b = by_file(rep)["orders_b.jsonl"]
    assert rep.ok and b.status == "loaded" and b.attempts == 2 and "earlier commit had succeeded" in b.note
    assert b.counts["rows_inserted"] == 2 and b.counts["rows_read"] == 2  # real counts recovered from the checkpoint
    assert wh._unsafe is None and plan.injected == ["orders_b.jsonl"]
    wh.close()
    wh = open_warehouse("duckdb", db)
    assert wh.table_counts()["orders"] == 3 and wh.table_counts()["checkpoints"] == 2
    clean = open_warehouse("duckdb")
    run(clean, src)
    assert wh.state_digest() == clean.state_digest()  # identical to a fault-free run


def test_ambiguous_commit_with_quarantine_rows_is_not_duplicated(tmp_path):
    src = tmp_path / "src"
    src.mkdir()
    write(src, "orders_a.jsonl", jl(order("A")) + "{broken\n")
    wh = open_warehouse("duckdb")
    FaultPlan(wh, {"orders_a.jsonl": 1}, mode="after").install()
    rep = run(wh, src)
    assert rep.ok and wh.table_counts()["quarantine"] == 1 and wh.table_counts()["checkpoints"] == 1


def test_rollback_that_fails_marks_connection_unsafe_and_blocks_further_writes(tmp_path):
    """If we cannot confirm the rollback we must not BEGIN/COMMIT again on this connection (on Snowflake a
    nested BEGIN would implicitly commit someone's half-finished work)."""
    d = two_files(tmp_path)
    wh = open_warehouse("duckdb")
    raising_commit(wh, TimeoutError("synthetic"), times=99)

    def bad_rollback():
        raise ConnectionError("synthetic: link down")

    wh._rollback = bad_rollback
    rep = run(wh, d)
    assert not rep.ok and all(f.status == "failed" for f in rep.files)
    assert wh._unsafe and "connection state unknown" in by_file(rep)["orders_b.jsonl"].error
    with pytest.raises(PipelineError):
        with wh.transaction():
            pass


def test_body_exception_rolls_back_and_connection_stays_usable():
    wh = open_warehouse("duckdb")
    with pytest.raises(RuntimeError):
        with wh.transaction():
            wh.query("INSERT INTO watermarks VALUES ('x', TIMESTAMPTZ '2026-01-01 00:00:00+00')")
            raise RuntimeError("boom")
    assert wh.query("SELECT COUNT(*) FROM watermarks") == [(0,)] and wh._unsafe is None
    with wh.transaction():
        wh.query("INSERT INTO watermarks VALUES ('y', TIMESTAMPTZ '2026-01-01 00:00:00+00')")
    assert wh.query("SELECT COUNT(*) FROM watermarks") == [(1,)]


def test_keyboard_interrupt_rolls_back_and_propagates(tmp_path):
    wh = open_warehouse("duckdb")
    with pytest.raises(KeyboardInterrupt):
        with wh.transaction():
            wh.query("INSERT INTO watermarks VALUES ('x', TIMESTAMPTZ '2026-01-01 00:00:00+00')")
            raise KeyboardInterrupt
    assert wh.query("SELECT COUNT(*) FROM watermarks") == [(0,)]


def test_crash_before_commit_leaves_nothing_on_disk(tmp_path):
    """Simulates a process dying mid-load: connection closed without COMMIT."""
    db = tmp_path / "w.duckdb"
    wh = open_warehouse("duckdb", db)
    wh._begin()
    wh.merge(__import__("pipeline.schema", fromlist=["ORDERS"]).ORDERS, [])
    wh.query("INSERT INTO watermarks VALUES ('orders', TIMESTAMPTZ '2026-01-01 00:00:00+00')")
    wh.close()
    wh = open_warehouse("duckdb", db)
    assert wh.query("SELECT COUNT(*) FROM watermarks") == [(0,)]


# ----------------------------------------------------------------------- source read failures
def test_unreadable_source_marks_file_failed_and_run_continues(tmp_path, monkeypatch):
    d = two_files(tmp_path)
    import pipeline.runner as runner
    real = runner.read_bytes

    def read(path):
        if path.name == "orders_a.jsonl":
            raise PermissionError(errno.EACCES, "Permission denied")
        return real(path)

    monkeypatch.setattr(runner, "read_bytes", read)
    wh = open_warehouse("duckdb")
    rep = run(wh, d)
    a = by_file(rep)["orders_a.jsonl"]
    assert not rep.ok and a.status == "failed" and a.attempts == 1 and "cannot read source file" in a.error and a.sha256 == ""
    assert by_file(rep)["orders_b.jsonl"].status == "loaded"


@pytest.mark.skipif(os.geteuid() == 0, reason="root ignores file modes")
def test_real_permission_denied_file(tmp_path):
    d = two_files(tmp_path)
    (d / "orders_a.jsonl").chmod(0)
    try:
        wh = open_warehouse("duckdb")
        rep = run(wh, d)
        assert by_file(rep)["orders_a.jsonl"].status == "failed" and by_file(rep)["orders_b.jsonl"].status == "loaded"
    finally:
        (d / "orders_a.jsonl").chmod(0o644)


def test_transient_io_error_on_read_is_retried(tmp_path, monkeypatch):
    d = two_files(tmp_path)
    import pipeline.runner as runner
    real, n = runner.read_bytes, []

    def read(path):
        if path.name == "orders_a.jsonl" and len(n) < 2:
            n.append(1)
            raise OSError(errno.EIO, "Input/output error")
        return real(path)

    monkeypatch.setattr(runner, "read_bytes", read)
    wh = open_warehouse("duckdb")
    a = by_file(run(wh, d))["orders_a.jsonl"]
    assert a.status == "loaded" and a.attempts == 3


def test_persistent_io_error_exhausts_retries(tmp_path, monkeypatch):
    d = two_files(tmp_path)
    import pipeline.runner as runner
    monkeypatch.setattr(runner, "read_bytes", lambda p: (_ for _ in ()).throw(OSError(errno.EIO, "Input/output error")))
    wh = open_warehouse("duckdb")
    rep = run(wh, d)
    assert [(f.status, f.attempts) for f in rep.files] == [("failed", 3), ("failed", 3)]


def test_dangling_symlink_is_reported_failed_not_silently_skipped(tmp_path):
    d = two_files(tmp_path)
    (d / "orders_dangling.jsonl").symlink_to(d / "does-not-exist")
    wh = open_warehouse("duckdb")
    rep = run(wh, d)
    assert by_file(rep)["orders_dangling.jsonl"].status == "failed" and by_file(rep)["orders_a.jsonl"].status == "loaded"


def test_file_deleted_between_listing_and_read(tmp_path, monkeypatch):
    d = two_files(tmp_path)
    import pipeline.runner as runner
    real = runner.read_bytes

    def read(path):
        if path.name == "orders_a.jsonl":
            path.unlink()
        return real(path)

    monkeypatch.setattr(runner, "read_bytes", read)
    wh = open_warehouse("duckdb")
    rep = run(wh, d)
    assert by_file(rep)["orders_a.jsonl"].status == "failed" and by_file(rep)["orders_b.jsonl"].status == "loaded"


def test_directory_named_like_a_source_is_ignored_and_missing_input_dir_raises(tmp_path):
    (tmp_path / "orders_dir.csv").mkdir()
    write(tmp_path, "orders_a.jsonl", jl(order("A")))
    wh = open_warehouse("duckdb")
    rep = run(wh, tmp_path)
    assert [f.file for f in rep.files] == ["orders_a.jsonl"]
    with pytest.raises(FileNotFoundError):
        run(wh, tmp_path / "nope")


def test_symlinked_source_is_read_through_and_never_modified(tmp_path):
    real = tmp_path / "elsewhere"
    real.mkdir()
    write(real, "x.jsonl", jl(order("A")))
    d = tmp_path / "in"
    d.mkdir()
    (d / "orders_link.jsonl").symlink_to(real / "x.jsonl")
    before = (real / "x.jsonl").read_bytes()
    wh = open_warehouse("duckdb")
    assert run(wh, d).ok and (real / "x.jsonl").read_bytes() == before


def test_unrecognised_files_are_listed_not_loaded(tmp_path):
    write(tmp_path, "readme.txt", "x")
    write(tmp_path, "orders.csv", "x")  # no underscore after the prefix
    write(tmp_path, "orders_a.txt", "x")
    write(tmp_path, "ORDERS_a.jsonl", jl(order()))  # prefix matching is case-sensitive
    wh = open_warehouse("duckdb")
    rep = run(wh, tmp_path)
    assert rep.files == [] and sorted(rep.ignored) == ["ORDERS_a.jsonl", "orders.csv", "orders_a.txt", "readme.txt"]


def test_source_files_not_modified_even_on_failure(tmp_path):
    d = two_files(tmp_path)
    before = {p.name: p.read_bytes() for p in d.iterdir()}
    wh = open_warehouse("duckdb")
    FaultPlan(wh, {"orders_a.jsonl": 99}).install()
    run(wh, d)
    assert {p.name: p.read_bytes() for p in d.iterdir()} == before
