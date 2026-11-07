import random
from datetime import datetime, timezone
from decimal import Decimal

import pytest
from conftest import AS_OF, FAST, tree_hashes

from pipeline.errors import TransientError
from pipeline.oracle import warehouse_summary
from pipeline.runner import run_pipeline
from pipeline.warehouse import open_warehouse


def run(wh, d, **kw):
    return run_pipeline(wh, d, as_of=AS_OF, retry=FAST, **kw)


def counts_by_file(rep):
    return {f.file: f for f in rep.files}


def test_full_load_matches_generator_oracle(wh, batches):
    d, m = batches
    rep = run(wh, d)
    assert rep.ok
    exp = m["expected_final"]
    c = wh.table_counts()
    assert (c["orders"], c["inventory"], c["support_tickets"]) == (exp["orders"], exp["inventory"], exp["support_tickets"])
    assert warehouse_summary(wh) == exp  # every entity, per-currency totals, status breakdowns
    inj = m["injected"]
    assert c["quarantine"] == inj["malformed_total"]
    tot = rep.totals()
    assert tot["rows_batch_duplicates"] == inj["in_file_duplicates"]
    assert tot["rows_stale"] == sum(inj["stale_updates"].values())
    assert counts_by_file(rep)["orders_batch3.jsonl"].counts["rows_late"] == inj["late_arriving_orders"]
    assert counts_by_file(rep)["support_tickets_batch4_corrupt.json"].status == "rejected"


def test_money_is_exact_decimal_and_timestamps_tz_aware(wh, batches):
    run(wh, batches[0])
    assert all(isinstance(x[0], Decimal) for x in wh.query("SELECT total_amount FROM orders"))
    assert wh.query("SELECT typeof(total_amount), typeof(order_ts) FROM orders LIMIT 1")[0] == ("DECIMAL(18,2)", "TIMESTAMP WITH TIME ZONE")


def test_idempotent_rerun_same_state_and_noop(wh, batches):
    d, _ = batches
    run(wh, d)
    before = wh.state_digest()
    rep = run(wh, d)
    assert {f.status for f in rep.files} == {"skipped_checkpointed"}
    assert wh.state_digest() == before


def test_checkpoint_bypass_still_idempotent(wh, batches):
    """Even if checkpoints were lost, reprocessing must not change data (merge-level idempotence)."""
    d, _ = batches
    run(wh, d)
    data_before = wh.state_digest(include_checkpoints=False)
    wh.query("DELETE FROM checkpoints")
    rep = run(wh, d)
    assert rep.totals()["rows_inserted"] == 0 and rep.totals()["rows_updated"] == 0
    assert wh.state_digest(include_checkpoints=False) == data_before


def test_result_independent_of_file_order(batches):
    d, _ = batches
    names = sorted(p.name for p in d.iterdir() if p.name != "manifest.json")
    digests = set()
    for seed in range(3):
        order = names[:]
        random.Random(seed).shuffle(order)
        w = open_warehouse("duckdb")
        for n in order:
            run(w, d, pattern=n)
        digests.add(w.state_digest())
        w.close()
    w = open_warehouse("duckdb")
    run(w, d)
    digests.add(w.state_digest())
    assert len(digests) == 1


def test_late_arrival_not_dropped_by_watermark(wh, tmp_path):
    mk = lambda i, ts: ('{"order_id":"O%d","customer_id":"C","status":"paid","currency":"USD","total_amount":"5.00",'
                        '"order_ts":"%s","updated_at":"%s"}\n' % (i, ts, ts))
    (tmp_path / "orders_new.jsonl").write_text(mk(1, "2026-01-20T00:00:00Z"))
    run(wh, tmp_path)
    (tmp_path / "orders_late.jsonl").write_text(mk(2, "2026-01-02T00:00:00Z"))  # far behind watermark
    rep = run(wh, tmp_path)
    assert wh.table_counts()["orders"] == 2
    assert counts_by_file(rep)["orders_late.jsonl"].counts["rows_late"] == 1


def test_stale_update_never_overwrites_and_tie_is_deterministic(wh, tmp_path):
    row = lambda amt, ts: ('{"order_id":"O1","customer_id":"C","status":"paid","currency":"USD","total_amount":"%s",'
                           '"order_ts":"2026-01-01T00:00:00Z","updated_at":"%s"}\n' % (amt, ts))
    (tmp_path / "orders_a.jsonl").write_text(row("20.00", "2026-01-05T00:00:00Z"))
    (tmp_path / "orders_b.jsonl").write_text(row("10.00", "2026-01-03T00:00:00Z"))  # older
    run(wh, tmp_path)
    assert wh.query("SELECT total_amount FROM orders")[0][0] == Decimal("20.00")
    # equal timestamp, different content: winner is order-independent
    (tmp_path / "orders_c.jsonl").write_text(row("30.00", "2026-01-05T00:00:00Z"))
    run(wh, tmp_path)
    w2 = open_warehouse("duckdb")
    for n in ("orders_c.jsonl", "orders_a.jsonl", "orders_b.jsonl"):
        run(w2, tmp_path, pattern=n)
    assert wh.state_digest(include_checkpoints=False) == w2.state_digest(include_checkpoints=False)


def test_transient_failure_rolls_back_then_retry_succeeds(wh, batches, monkeypatch):
    d, _ = batches
    real, calls = wh.put_checkpoint, []

    def boom(cp):
        calls.append(1)
        if len(calls) == 1:
            raise TransientError("simulated connection drop after merge")
        real(cp)

    monkeypatch.setattr(wh, "put_checkpoint", boom)
    rep = run(wh, d, pattern="orders_batch1.csv")
    assert rep.ok and rep.files[0].attempts == 2
    assert wh.table_counts()["orders"] == 40 and wh.table_counts()["checkpoints"] == 1


def test_permanent_failure_leaves_no_data_no_checkpoint_and_rerun_recovers(wh, batches, monkeypatch):
    d, _ = batches

    def boom(cp):
        raise RuntimeError("disk full")

    real = wh.put_checkpoint
    monkeypatch.setattr(wh, "put_checkpoint", boom)
    rep = run(wh, d, pattern="orders_batch1.csv")
    assert not rep.ok and rep.files[0].status == "failed" and rep.files[0].attempts == 1
    c = wh.table_counts()
    assert c["orders"] == 0 and c["checkpoints"] == 0 and c["quarantine"] == 0
    assert wh.query("SELECT COUNT(*) FROM watermarks")[0][0] == 0
    monkeypatch.setattr(wh, "put_checkpoint", real)
    assert run(wh, d, pattern="orders_batch1.csv").ok and wh.table_counts()["orders"] == 40


def test_sources_are_never_modified(wh, batches):
    d, _ = batches
    before = tree_hashes(d)
    run(wh, d)
    run(wh, d, since=datetime(2026, 1, 10, tzinfo=timezone.utc), until=datetime(2026, 1, 11, tzinfo=timezone.utc))
    assert tree_hashes(d) == before
    assert all(mode & 0o222 == 0 for _, mode in before.values())  # generated read-only


def test_bounded_backfill_then_full_converges_without_loss(batches):
    d, _ = batches
    full = open_warehouse("duckdb")
    run(full, d)
    w = open_warehouse("duckdb")
    cuts = [datetime(2026, 1, 10, tzinfo=timezone.utc), datetime(2026, 1, 11, tzinfo=timezone.utc),
            datetime(2026, 1, 12, tzinfo=timezone.utc), datetime(2026, 1, 14, tzinfo=timezone.utc)]
    for a, b in zip(cuts, cuts[1:]):
        rep = run(w, d, since=a, until=b)
        assert all(f.counts.get("rows_updated", 0) >= 0 for f in rep.files)
    # window run is bounded: nothing older than `since` or at/after `until` was applied
    assert w.query("SELECT COUNT(*) FROM orders WHERE updated_at >= TIMESTAMPTZ '2026-01-14 00:00:00+00'")[0][0] == 0
    run(w, d)  # windowed checkpoints must not suppress the unwindowed run
    assert w.state_digest(include_checkpoints=False) == full.state_digest(include_checkpoints=False)
    # and a repeated identical window is a no-op
    assert {f.status for f in run(w, d, since=cuts[0], until=cuts[1]).files} == {"skipped_checkpointed"}


def test_backfill_window_validation(wh, batches):
    with pytest.raises(ValueError):
        run(wh, batches[0], since=datetime(2026, 1, 1))
    with pytest.raises(ValueError):
        run(wh, batches[0], since=AS_OF, until=AS_OF)


def test_replaced_file_with_same_name_is_reprocessed(wh, tmp_path):
    row = lambda amt: ('{"order_id":"O1","customer_id":"C","status":"paid","currency":"USD","total_amount":"%s",'
                       '"order_ts":"2026-01-01T00:00:00Z","updated_at":"2026-01-0%sT00:00:00Z"}\n' % (amt, amt[0]))
    p = tmp_path / "orders_a.jsonl"
    p.write_text(row("3.00"))
    run(wh, tmp_path)
    p.write_text(row("4.00"))  # corrected file (new bytes -> new checkpoint key, newer version wins)
    run(wh, tmp_path)
    assert wh.query("SELECT total_amount FROM orders")[0][0] == Decimal("4.00")


def test_future_timestamps_quarantined(wh, batches):
    run(wh, batches[0])
    assert wh.query("SELECT COUNT(*) FROM quarantine WHERE reason='future_timestamp'")[0][0] == 1
    assert wh.query("SELECT COUNT(*) FROM orders WHERE order_id='ORD-FUTURE'")[0][0] == 0
