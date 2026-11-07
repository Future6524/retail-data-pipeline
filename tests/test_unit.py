import json
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from data import generate_demo_batches
from pipeline import sources
from pipeline.errors import FileRejected, PipelineError, TransientError
from pipeline.retry import RetryPolicy, with_retries
from pipeline.schema import ORDERS
from pipeline.validate import Invalid, validate
from pipeline.warehouse.base import merge_sql
from pipeline.warehouse.snowflake_wh import connection_params_from_env

GOOD = dict(order_id="O1", customer_id="C1", status="PAID", currency="usd", total_amount="10.50",
            order_ts="2026-01-01T10:00:00+02:00", updated_at="2026-01-01T09:00:00Z")


def test_validate_normalises_to_utc_decimal():
    r = validate(ORDERS, dict(GOOD))
    assert r["status"] == "paid" and r["currency"] == "USD"
    assert r["total_amount"] == Decimal("10.50") and isinstance(r["total_amount"], Decimal)
    assert r["order_ts"] == datetime(2026, 1, 1, 8, tzinfo=timezone.utc) and r["order_ts"].utcoffset().total_seconds() == 0


@pytest.mark.parametrize("field,value,reason", [
    ("total_amount", "12.345", "money_precision"), ("total_amount", "-1", "negative_money"),
    ("total_amount", "NaN", "invalid_money"), ("total_amount", 1.5, "wrong_type"),
    ("order_ts", "2026-01-01T10:00:00", "naive_timestamp"), ("order_ts", "yesterday", "invalid_timestamp"),
    ("currency", "JPY", "invalid_value"), ("order_id", " ", "missing_required"),
    ("updated_at", "2026-01-01T07:00:00Z", "updated_before_created"),
])
def test_validate_rejects(field, value, reason):
    with pytest.raises(Invalid) as e:
        validate(ORDERS, {**GOOD, field: value})
    assert e.value.reason == reason


def test_row_hash_is_stable_and_content_sensitive():
    a, b = validate(ORDERS, dict(GOOD)), validate(ORDERS, dict(GOOD))
    c = validate(ORDERS, {**GOOD, "total_amount": "10.51"})
    assert a["row_hash"] == b["row_hash"] != c["row_hash"]


def test_json_money_never_becomes_float(tmp_path):
    p = tmp_path / "orders_x.jsonl"
    p.write_text(json.dumps(GOOD).replace('"10.50"', "10.10") + "\n")
    row = next(sources.parse(p, p.read_bytes(), ORDERS))
    assert row.data["total_amount"] == Decimal("10.10")
    assert validate(ORDERS, row.data)["total_amount"] == Decimal("10.10")


def test_parse_csv_jsonl_json_malformed(tmp_path):
    csvp = tmp_path / "orders_a.csv"
    csvp.write_text("order_id,customer_id,status,currency,total_amount,order_ts,updated_at\nO1,C1,paid\nO2,C,paid,USD,1,a,b,EXTRA\n")
    assert [r.error for r in sources.parse(csvp, csvp.read_bytes(), ORDERS)] == ["missing_fields", "extra_fields"]
    jl = tmp_path / "orders_a.jsonl"
    jl.write_text('{"a":1}\n\n{broken\n[1]\n')
    assert [(r.row_number, r.error) for r in sources.parse(jl, jl.read_bytes(), ORDERS)] == [(1, None), (3, "invalid_json"), (4, "not_an_object")]


def test_file_level_rejections(tmp_path):
    bad_hdr = tmp_path / "orders_h.csv"
    bad_hdr.write_text("a,b\n1,2\n")
    with pytest.raises(FileRejected):
        list(sources.parse(bad_hdr, bad_hdr.read_bytes(), ORDERS))
    enc = tmp_path / "orders_e.jsonl"
    enc.write_bytes(b"\xff\xfe\x00")
    with pytest.raises(FileRejected):
        list(sources.parse(enc, enc.read_bytes(), ORDERS))
    js = tmp_path / "orders_j.json"
    js.write_text('{"not": "array"}')
    with pytest.raises(FileRejected):
        list(sources.parse(js, js.read_bytes(), ORDERS))


def test_entity_detection():
    assert sources.entity_for(Path("support_tickets_1.json")).name == "support_tickets"
    assert sources.entity_for(Path("orders_1.txt")) is None
    assert sources.entity_for(Path("random.csv")) is None


def test_retry_only_transient_with_backoff():
    sleeps, calls = [], []

    def flaky():
        calls.append(1)
        if len(calls) < 3:
            raise TransientError("blip")
        return "ok"

    pol = RetryPolicy(max_attempts=3, base_delay=1, sleep=sleeps.append)
    assert with_retries(flaky, pol, lambda e: isinstance(e, TransientError)) == ("ok", 3)
    assert sleeps == [1, 2]
    with pytest.raises(ValueError):
        with_retries(lambda: (_ for _ in ()).throw(ValueError("x")), pol, lambda e: False)


def test_generator_is_deterministic(tmp_path):
    m1 = generate_demo_batches(tmp_path / "a", seed=7)
    m2 = generate_demo_batches(tmp_path / "b", seed=7)
    generate_demo_batches(tmp_path / "c", seed=8)
    files = lambda d: {p.name: p.read_bytes() for p in (tmp_path / d).iterdir()}
    assert m1 == m2 and files("a") == files("b") and files("a") != files("c")
    assert m1["synthetic"] is True


def test_merge_sql_has_stale_guard_and_snowflake_shape():
    sql = merge_sql(ORDERS)
    assert "WHEN MATCHED AND" in sql and "row_hash >" in sql and "WHEN NOT MATCHED" in sql


# --- Snowflake adapter: configuration + SQL text only. These tests do NOT verify
# --- behaviour against Snowflake; the adapter is NOT integration-tested.
def test_snowflake_config_requires_env_and_never_reads_real_env():
    with pytest.raises(PipelineError, match="missing"):
        connection_params_from_env({})
    env = {k: "x" for k in ("SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_WAREHOUSE", "SNOWFLAKE_DATABASE", "SNOWFLAKE_SCHEMA")}
    with pytest.raises(PipelineError, match="PASSWORD"):
        connection_params_from_env(env)
    assert connection_params_from_env({**env, "SNOWFLAKE_PASSWORD": "p"})["account"] == "x"
