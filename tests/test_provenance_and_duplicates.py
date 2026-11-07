"""Duplicate rows / duplicate files / deterministic provenance (source_file)."""
import itertools
import shutil
from decimal import Decimal

from conftest import AS_OF, FAST, TICKET_BASE, by_file, jl, order, write

from pipeline.runner import run_pipeline
from pipeline.warehouse import open_warehouse


def run(wh, d, **kw):
    return run_pipeline(wh, d, as_of=AS_OF, retry=FAST, **kw)


def load_in_order(d, names):
    w = open_warehouse("duckdb")
    for n in names:
        run(w, d, pattern=n)
    return w


def test_identical_rows_in_two_files_are_arrival_order_independent(tmp_path):
    """Regression (probe 02, items 2-4): same business row in two file names used to keep whichever file
    arrived first, so source_file and the state digest depended on arrival order."""
    write(tmp_path, "orders_a.jsonl", jl(order()))
    write(tmp_path, "orders_b.jsonl", jl(order()))
    results = {}
    for names in itertools.permutations(["orders_a.jsonl", "orders_b.jsonl"]):
        w = load_in_order(tmp_path, names)
        results[names] = (tuple(w.query("SELECT source_file FROM orders")), w.state_digest(include_checkpoints=False))
        w.close()
    assert len({v for v in results.values()}) == 1
    assert next(iter(results.values()))[0] == (("orders_a.jsonl",),)  # deterministic rule: smallest file name


def test_provenance_is_order_independent_for_many_files_and_all_entities(tmp_path):
    names = ["orders_c.jsonl", "orders_a.jsonl", "orders_b.jsonl"]
    for n in names:
        write(tmp_path, n, jl(order(), order("O2", "2026-01-06T00:00:00Z", "7.00")))
    write(tmp_path, "inventory_z.jsonl", jl(dict(sku="S", warehouse_id="W", quantity_on_hand=3, unit_cost="1.00",
                                                 updated_at="2026-01-05T00:00:00Z")))
    write(tmp_path, "inventory_y.jsonl", (tmp_path / "inventory_z.jsonl").read_text())
    write(tmp_path, "support_tickets_2.jsonl", jl(TICKET_BASE))
    write(tmp_path, "support_tickets_1.jsonl", jl(TICKET_BASE))
    all_names = sorted(p.name for p in tmp_path.iterdir())
    digests = set()
    for perm in itertools.islice(itertools.permutations(all_names), 0, 720, 37):
        w = load_in_order(tmp_path, perm)
        digests.add(w.state_digest())
        assert {r[0] for r in w.query("SELECT source_file FROM orders")} == {"orders_a.jsonl"}
        assert w.query("SELECT source_file FROM inventory") == [("inventory_y.jsonl",)]
        assert w.query("SELECT source_file FROM support_tickets") == [("support_tickets_1.jsonl",)]
        w.close()
    assert len(digests) == 1  # state digest (incl. checkpoint identities) identical for every arrival order
    w = open_warehouse("duckdb")
    run(w, tmp_path)  # and a single directory run lands on the same provenance
    assert w.query("SELECT DISTINCT source_file FROM orders") == [("orders_a.jsonl",)]
    assert w.state_digest() in digests


def test_identical_second_file_changes_only_provenance_and_counts_unchanged(wh, tmp_path):
    write(tmp_path, "orders_b.jsonl", jl(order()))
    run(wh, tmp_path)
    write(tmp_path, "orders_a.jsonl", jl(order()))
    rep = run(wh, tmp_path)
    c = by_file(rep)["orders_a.jsonl"].counts
    assert (c["rows_inserted"], c["rows_updated"], c["rows_unchanged"]) == (0, 0, 1)
    assert wh.query("SELECT source_file FROM orders") == [("orders_a.jsonl",)]  # smaller name takes provenance
    write(tmp_path, "orders_c.jsonl", jl(order()))
    run(wh, tmp_path)
    assert wh.query("SELECT source_file FROM orders") == [("orders_a.jsonl",)]  # larger name never steals it


def test_renamed_copy_is_reprocessed_without_changing_business_data(wh, tmp_path):
    p = write(tmp_path, "orders_z.jsonl", jl(order(), order("O2")))
    run(wh, tmp_path)
    before = wh.query("SELECT order_id, total_amount, row_hash FROM orders ORDER BY 1")
    shutil.copy(p, tmp_path / "orders_a.jsonl")
    rep = run(wh, tmp_path)
    assert by_file(rep)["orders_a.jsonl"].status == "loaded"
    assert wh.query("SELECT order_id, total_amount, row_hash FROM orders ORDER BY 1") == before
    assert wh.table_counts()["checkpoints"] == 2


def test_checkpoint_loss_replay_keeps_provenance_stable(wh, tmp_path):
    write(tmp_path, "orders_a.jsonl", jl(order()))
    write(tmp_path, "orders_b.jsonl", jl(order()))
    run(wh, tmp_path)
    d = wh.state_digest(include_checkpoints=False)
    wh.query("DELETE FROM checkpoints")
    rep = run(wh, tmp_path)
    assert rep.totals()["rows_inserted"] == 0 and rep.totals()["rows_updated"] == 0
    assert wh.state_digest(include_checkpoints=False) == d


def test_same_timestamp_different_content_winner_is_hash_not_file_name(tmp_path):
    write(tmp_path, "orders_a.jsonl", jl(order(amt="10.00")))
    write(tmp_path, "orders_b.jsonl", jl(order(amt="20.00")))
    seen = set()
    for names in itertools.permutations(["orders_a.jsonl", "orders_b.jsonl"]):
        w = load_in_order(tmp_path, names)
        seen.add(tuple(w.query("SELECT total_amount, source_file FROM orders")))
        w.close()
    assert len(seen) == 1


def test_duplicate_rows_inside_one_file_counted_once_latest_wins(wh, tmp_path):
    write(tmp_path, "orders_a.jsonl", jl(order(amt="1.00", upd="2026-01-05T00:00:00Z"),
                                          order(amt="3.00", upd="2026-01-07T00:00:00Z"),
                                          order(amt="2.00", upd="2026-01-06T00:00:00Z"),
                                          order(amt="3.00", upd="2026-01-07T00:00:00Z")))
    rep = run(wh, tmp_path)
    c = by_file(rep)["orders_a.jsonl"].counts
    assert c["rows_read"] == 4 and c["rows_batch_duplicates"] == 3 and c["rows_inserted"] == 1
    assert wh.query("SELECT total_amount FROM orders") == [(Decimal("3.00"),)]


def test_composite_key_duplicates_inventory(wh, tmp_path):
    inv = lambda wid, q, u: dict(sku="S", warehouse_id=wid, quantity_on_hand=q, unit_cost="1.00", updated_at=u)
    write(tmp_path, "inventory_a.jsonl", jl(inv("W1", 1, "2026-01-05T00:00:00Z"), inv("W2", 2, "2026-01-05T00:00:00Z"),
                                            inv("W1", 9, "2026-01-06T00:00:00Z")))
    run(wh, tmp_path)
    assert wh.query("SELECT warehouse_id, quantity_on_hand FROM inventory ORDER BY 1") == [("W1", 9), ("W2", 2)]


def test_same_name_same_bytes_in_second_run_is_skipped(wh, tmp_path):
    write(tmp_path, "orders_a.jsonl", jl(order()))
    run(wh, tmp_path)
    assert by_file(run(wh, tmp_path))["orders_a.jsonl"].status == "skipped_checkpointed"
