"""Generator: validation (before any cleanup), determinism across seeds/sizes, oracle == warehouse for ALL entities."""
import random

import pytest
from conftest import AS_OF, FAST, by_file, tree_hashes

from data import generate_demo_batches
from data.generator import MAX_ORDERS, MIN_ORDERS
from pipeline.oracle import warehouse_summary
from pipeline.runner import run_pipeline
from pipeline.warehouse import open_warehouse

SEEDS = [0, 1, 7, 42, 123, 2026, 99_999]


def load(d, order=None):
    w = open_warehouse("duckdb")
    if order is None:
        rep = run_pipeline(w, d, as_of=AS_OF, retry=FAST)
        assert rep.ok
    else:
        for n in order:
            assert run_pipeline(w, d, as_of=AS_OF, retry=FAST, pattern=n).ok
    return w


@pytest.mark.parametrize("seed", SEEDS)
def test_seed_is_reproducible_and_oracle_matches_every_entity(tmp_path, seed):
    m1 = generate_demo_batches(tmp_path / "a", seed=seed)
    m2 = generate_demo_batches(tmp_path / "b", seed=seed)
    assert m1 == m2 and tree_hashes(tmp_path / "a") == tree_hashes(tmp_path / "b")
    w = load(tmp_path / "a")
    assert warehouse_summary(w) == m1["expected_final"]
    inj = m1["injected"]
    assert w.table_counts()["quarantine"] == inj["malformed_total"]
    q = dict(w.query("SELECT reason, COUNT(*) FROM quarantine GROUP BY reason"))
    assert q["future_timestamp"] == inj["future_dated_rows"] and q["file_rejected"] == 1
    w.close()


@pytest.mark.parametrize("seed", SEEDS[:4])
def test_shuffled_file_order_gives_identical_state_for_each_seed(tmp_path, seed):
    generate_demo_batches(tmp_path, seed=seed)
    names = sorted(p.name for p in tmp_path.iterdir() if p.name != "manifest.json")
    ref = load(tmp_path)
    for k in range(4):
        o = names[:]
        random.Random(k).shuffle(o)
        w = load(tmp_path, o)
        assert w.state_digest(include_checkpoints=False) == ref.state_digest(include_checkpoints=False)
        w.close()


@pytest.mark.parametrize("n", [MIN_ORDERS, MIN_ORDERS + 1, 25, 150])
def test_various_sizes_hold_the_oracle(tmp_path, n):
    m = generate_demo_batches(tmp_path, seed=5, n_orders=n)
    w = load(tmp_path)
    assert warehouse_summary(w) == m["expected_final"]


def test_different_seeds_differ(tmp_path):
    a = generate_demo_batches(tmp_path / "a", seed=1)
    b = generate_demo_batches(tmp_path / "b", seed=2)
    assert a["expected_final"] != b["expected_final"]


def test_seed_zero_and_negative_seed_work(tmp_path):
    for s in (0, -1):
        generate_demo_batches(tmp_path / str(s), seed=s)


def test_oracle_never_sums_across_currencies(tmp_path):
    m = generate_demo_batches(tmp_path, seed=42)
    ef = m["expected_final"]
    assert "orders_total_amount_sum" not in ef
    assert set(ef["orders_total_by_currency"]) <= {"USD", "EUR", "GBP", "CAD"} and len(ef["orders_total_by_currency"]) > 1


def test_every_injected_defect_class_is_present(tmp_path):
    m = generate_demo_batches(tmp_path, seed=42)
    rep = run_pipeline(open_warehouse("duckdb"), tmp_path, as_of=AS_OF, retry=FAST)
    inj = m["injected"]
    assert inj["in_file_duplicates"] > 0 and sum(inj["stale_updates"].values()) > 0 and inj["late_arriving_orders"] > 0
    assert rep.totals()["rows_stale"] == sum(inj["stale_updates"].values())
    assert by_file(rep)["orders_batch3.jsonl"].counts["rows_late"] == inj["late_arriving_orders"]


# ----------------------------------------------------------------------- validation (probe 02 item 6)
@pytest.mark.parametrize("n", [-5, 0, 1, 2, MIN_ORDERS - 1, MAX_ORDERS + 1, True, 2.5, "40", None])
def test_bad_order_counts_fail_clearly_and_before_any_cleanup(tmp_path, n):
    sentinel = tmp_path / "orders_batch1.csv"  # a name the generator would delete when regenerating
    sentinel.write_text("precious")
    with pytest.raises(ValueError, match=rf"between {MIN_ORDERS} and {MAX_ORDERS}"):
        generate_demo_batches(tmp_path, seed=1, n_orders=n)
    assert sentinel.read_text() == "precious"  # nothing deleted, nothing written
    assert [p.name for p in tmp_path.iterdir()] == ["orders_batch1.csv"]


def test_generator_rejects_bad_seed_and_file_as_output_dir(tmp_path):
    with pytest.raises(ValueError, match="seed"):
        generate_demo_batches(tmp_path / "x", seed="1")
    assert not (tmp_path / "x").exists()
    f = tmp_path / "afile"
    f.write_text("x")
    with pytest.raises(ValueError, match="not a directory"):
        generate_demo_batches(f)


def test_generator_only_deletes_files_it_owns(tmp_path):
    (tmp_path / "keep_me.txt").write_text("x")
    (tmp_path / "orders_other.jsonl").write_text("x")  # not *_batch*: not ours
    generate_demo_batches(tmp_path, seed=1)
    assert (tmp_path / "keep_me.txt").exists() and (tmp_path / "orders_other.jsonl").exists()
    generate_demo_batches(tmp_path, seed=1)  # regenerate over read-only (0444) files works


def test_smallest_supported_size_actually_works(tmp_path):
    generate_demo_batches(tmp_path, seed=3, n_orders=MIN_ORDERS)
