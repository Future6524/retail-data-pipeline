"""Backfill window [since, until): exact inclusive/exclusive edges, microsecond precision, timezone-shifted
bounds, window checkpoints, and convergence with a plain run."""
from datetime import datetime, timedelta, timezone

import pytest
from conftest import AS_OF, FAST, by_file, jl, order, write

from pipeline.runner import run_pipeline

S = datetime(2026, 1, 10, tzinfo=timezone.utc)
U = datetime(2026, 1, 12, tzinfo=timezone.utc)


def run(wh, d, **kw):
    return run_pipeline(wh, d, as_of=AS_OF, retry=FAST, **kw)


def edge_file(tmp_path):
    rows = [order("before_by_1us", "2026-01-09T23:59:59.999999Z"), order("at_since", "2026-01-10T00:00:00Z"),
            order("after_since_1us", "2026-01-10T00:00:00.000001Z"), order("inside", "2026-01-11T00:00:00Z"),
            order("before_until_1us", "2026-01-11T23:59:59.999999Z"), order("at_until", "2026-01-12T00:00:00Z"),
            order("after_until", "2026-01-12T00:00:00.000001Z")]
    write(tmp_path, "orders_edges.jsonl", jl(*rows))


def ids(wh):
    return {r[0] for r in wh.query("SELECT order_id FROM orders")}


def test_since_inclusive_until_exclusive_to_the_microsecond(wh, tmp_path):
    edge_file(tmp_path)
    rep = run(wh, tmp_path, since=S, until=U)
    assert ids(wh) == {"at_since", "after_since_1us", "inside", "before_until_1us"}
    assert by_file(rep)["orders_edges.jsonl"].counts["rows_out_of_window"] == 3


def test_adjacent_windows_partition_the_data_without_gap_or_overlap(wh, tmp_path):
    edge_file(tmp_path)
    mid = datetime(2026, 1, 11, tzinfo=timezone.utc)
    a = run(wh, tmp_path, since=S, until=mid)
    b = run(wh, tmp_path, since=mid, until=U)
    ins = lambda r: r.totals()["rows_inserted"]
    assert ins(a) + ins(b) == 4 and ids(wh) == {"at_since", "after_since_1us", "inside", "before_until_1us"}


def test_open_ended_windows(wh, tmp_path):
    edge_file(tmp_path)
    run(wh, tmp_path, since=U)
    assert ids(wh) == {"at_until", "after_until"}
    run(wh, tmp_path, until=S)
    assert ids(wh) == {"at_until", "after_until", "before_by_1us"}


def test_bounds_in_other_timezones_are_compared_in_utc(wh, tmp_path):
    edge_file(tmp_path)
    tz = timezone(timedelta(hours=5, minutes=30))
    run(wh, tmp_path, since=S.astimezone(tz), until=U.astimezone(tz))
    assert ids(wh) == {"at_since", "after_since_1us", "inside", "before_until_1us"}
    # same instants expressed in another zone are the SAME window => checkpoint hit, not a new window
    rep = run(wh, tmp_path, since=S, until=U)
    assert {f.status for f in rep.files} == {"skipped_checkpointed"}


def test_row_timestamps_with_offsets_are_windowed_by_instant(wh, tmp_path):
    write(tmp_path, "orders_tz.jsonl", jl(order("in", "2026-01-10T05:30:00+05:30"),      # = 00:00Z  -> in
                                          order("via_offset", "2026-01-09T23:59:59-00:01")))  # = 2026-01-10T00:00:59Z
    run(wh, tmp_path, since=S, until=U)
    assert ids(wh) == {"in", "via_offset"}  # the local date part (01-09) is irrelevant; the instant is inside


def test_different_windows_have_independent_checkpoints_and_full_run_not_blocked(wh, tmp_path):
    edge_file(tmp_path)
    run(wh, tmp_path, since=S, until=U)
    rep_same = run(wh, tmp_path, since=S, until=U)
    assert {f.status for f in rep_same.files} == {"skipped_checkpointed"}
    rep_other = run(wh, tmp_path, since=S, until=U + timedelta(microseconds=1))
    assert {f.status for f in rep_other.files} == {"loaded"}  # 1 us wider = different window key
    rep_full = run(wh, tmp_path)
    assert {f.status for f in rep_full.files} == {"loaded"} and len(ids(wh)) == 7


@pytest.mark.parametrize("since,until", [(S, S), (U, S)])
def test_empty_or_inverted_window_is_an_error(wh, tmp_path, since, until):
    with pytest.raises(ValueError, match="since must be < until"):
        run(wh, tmp_path, since=since, until=until)


def test_window_with_no_matching_rows_loads_nothing_but_checkpoints(wh, tmp_path):
    edge_file(tmp_path)
    rep = run(wh, tmp_path, since=datetime(2030, 1, 1, tzinfo=timezone.utc))
    assert rep.ok and ids(wh) == set() and wh.table_counts()["checkpoints"] == 1
    assert wh.query("SELECT COUNT(*) FROM watermarks") == [(0,)]


def test_backfill_does_not_apply_quarantine_rows_twice_across_windows(wh, tmp_path):
    write(tmp_path, "orders_q.jsonl", jl(order("A", "2026-01-10T12:00:00Z")) + "{bad\n")
    run(wh, tmp_path, since=S, until=U)
    run(wh, tmp_path)  # different checkpoint key; same quarantine key (file, sha, row) -> idempotent
    assert wh.table_counts()["quarantine"] == 1


def test_stale_row_inside_window_still_cannot_overwrite_newer_state(wh, tmp_path):
    write(tmp_path, "orders_new.jsonl", jl(order("A", "2026-01-11T00:00:00Z", "9.00")))
    write(tmp_path, "orders_old.jsonl", jl(order("A", "2026-01-10T06:00:00Z", "1.00")))
    run(wh, tmp_path, pattern="orders_new.jsonl")
    rep = run(wh, tmp_path, since=S, until=U, pattern="orders_old.jsonl")
    assert wh.query("SELECT total_amount FROM orders")[0][0] == __import__("decimal").Decimal("9.00")
    assert rep.totals()["rows_stale"] == 1


def test_future_boundary_applies_inside_backfill_too(wh, tmp_path):
    write(tmp_path, "orders_f.jsonl", jl(order("A", "2026-03-01T00:00:00Z")))
    run(wh, tmp_path, since=datetime(2026, 2, 1, tzinfo=timezone.utc), until=datetime(2026, 4, 1, tzinfo=timezone.utc))
    assert ids(wh) == set() and wh.query("SELECT reason FROM quarantine") == [("future_timestamp",)]
