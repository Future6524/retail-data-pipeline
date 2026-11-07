"""CSV / JSON / JSONL structure, malformed decimals, timestamps. Each case runs through the real runner so
the assertion is on end-to-end behaviour (what lands in tables / quarantine), not only on the parser."""
import json
from decimal import Decimal

import pytest
from conftest import (
    AS_OF,
    FAST,
    ORDER_CSV_HEADER,
    TICKET_BASE,
    by_file,
    jl,
    order,
    order_csv_row,
    quarantine_reasons,
    write,
)

from pipeline.runner import run_pipeline
from pipeline.schema import ORDERS
from pipeline.validate import Invalid, validate


def run(wh, d, **kw):
    return run_pipeline(wh, d, as_of=AS_OF, retry=FAST, **kw)


def orders(wh):
    return [r[0] for r in wh.query("SELECT order_id FROM orders ORDER BY 1")]


# ----------------------------------------------------------------------------------- CSV structure
def test_oversize_csv_field_quarantines_row_and_run_continues(wh, tmp_path):
    """Regression (probe 02 item 5): a field > csv.field_size_limit used to raise csv.Error and abort the run."""
    big = "x" * 140_000
    write(tmp_path, "orders_a.csv", ORDER_CSV_HEADER + order_csv_row("O1") + f"{big},C,paid,USD,1.00,"
          "2026-01-01T00:00:00Z,2026-01-05T00:00:00Z\n" + order_csv_row("O3"))
    write(tmp_path, "orders_b.csv", ORDER_CSV_HEADER + order_csv_row("O9"))
    write(tmp_path, "support_tickets_a.jsonl", jl(TICKET_BASE))
    rep = run(wh, tmp_path)
    assert rep.ok
    assert orders(wh) == ["O1", "O3", "O9"]  # rows around the bad one, and the other file, all loaded
    assert wh.table_counts()["support_tickets"] == 1
    assert quarantine_reasons(wh) == {"csv_parse_error": 1}
    q = wh.query("SELECT row_number, detail, raw_record FROM quarantine")[0]
    assert q[0] == 2 and "field larger than field limit" in q[1] and len(q[2]) < 200
    assert by_file(rep)["orders_a.csv"].counts["rows_quarantined"] == 1


def test_duplicate_csv_header_rejects_file_instead_of_silently_losing_a_column(wh, tmp_path):
    """Regression: DictReader keeps only the LAST of two same-named columns."""
    write(tmp_path, "orders_dup.csv", "order_id,order_id,customer_id,status,currency,total_amount,order_ts,updated_at\n"
          "A,B,C,paid,USD,1.00,2026-01-01T00:00:00Z,2026-01-05T00:00:00Z\n")
    write(tmp_path, "orders_ok.csv", ORDER_CSV_HEADER + order_csv_row("O1"))
    rep = run(wh, tmp_path)
    assert by_file(rep)["orders_dup.csv"].status == "rejected"
    assert orders(wh) == ["O1"]
    assert "duplicate_header_columns" in wh.query("SELECT detail FROM quarantine WHERE reason='file_rejected'")[0][0]


def test_malformed_quote_in_middle_quarantines_only_that_row(wh, tmp_path):
    write(tmp_path, "orders_q.csv", ORDER_CSV_HEADER + order_csv_row("O1")
          + '"O2"x,C,paid,USD,1.00,2026-01-01T00:00:00Z,2026-01-05T00:00:00Z\n' + order_csv_row("O3"))
    run(wh, tmp_path)
    assert orders(wh) == ["O1", "O3"] and quarantine_reasons(wh) == {"csv_parse_error": 1}
    assert wh.query("SELECT row_number FROM quarantine") == [(2,)]


def test_unterminated_quote_swallows_rest_of_file_into_one_quarantined_row(wh, tmp_path):
    write(tmp_path, "orders_u.csv", ORDER_CSV_HEADER + order_csv_row("O1")
          + '"O2,C,paid,USD,1.00,2026-01-01T00:00:00Z,2026-01-05T00:00:00Z\n' + order_csv_row("O3"))
    rep = run(wh, tmp_path)
    assert rep.ok and orders(wh) == ["O1"] and quarantine_reasons(wh) == {"csv_parse_error": 1}


def test_csv_headers_and_edge_shapes(wh, tmp_path):
    write(tmp_path, "orders_empty.csv", "")
    write(tmp_path, "orders_hdr.csv", ORDER_CSV_HEADER)  # header only: valid, zero rows
    write(tmp_path, "orders_bom.csv", b"\xef\xbb\xbf" + (ORDER_CSV_HEADER + order_csv_row("B1")).encode())
    write(tmp_path, "orders_crlf.csv", (ORDER_CSV_HEADER + order_csv_row("C1")).replace("\n", "\r\n"))
    write(tmp_path, "orders_extra.csv", "extra," + ORDER_CSV_HEADER.rstrip("\n") + "\nzzz," + order_csv_row("E1"))
    write(tmp_path, "orders_blank.csv", ORDER_CSV_HEADER + "\n" + order_csv_row("L1") + "\n\n")
    write(tmp_path, "orders_short.csv", ORDER_CSV_HEADER + "O,C,paid\n" + order_csv_row("S1") + "a,b,c,d,e,f,g,h,i\n")
    rep = by_file(run(wh, tmp_path))
    assert rep["orders_empty.csv"].status == "rejected"
    assert rep["orders_hdr.csv"].status == "loaded" and rep["orders_hdr.csv"].counts["rows_read"] == 0
    assert orders(wh) == ["B1", "C1", "E1", "L1", "S1"]  # unknown extra columns are ignored, not stored
    assert quarantine_reasons(wh) == {"extra_fields": 1, "file_rejected": 1, "missing_fields": 1}


def test_csv_header_with_missing_required_column_is_rejected(wh, tmp_path):
    write(tmp_path, "orders_h.csv", "order_id,customer_id\nA,B\n")
    assert by_file(run(wh, tmp_path))["orders_h.csv"].status == "rejected"


def test_csv_quoted_multiline_subject_is_one_record(wh, tmp_path):
    write(tmp_path, "support_tickets_m.csv", "ticket_id,customer_id,status,priority,subject,created_at,updated_at\n"
          'T1,C,open,low,"line one\nline two, with comma",2026-01-01T00:00:00Z,2026-01-01T00:00:00Z\n')
    run(wh, tmp_path)
    assert wh.query("SELECT subject FROM support_tickets") == [("line one\nline two, with comma",)]


def test_tickets_csv_optional_columns_may_be_absent(wh, tmp_path):
    write(tmp_path, "support_tickets_m.csv", "ticket_id,customer_id,status,priority,subject,created_at,updated_at\n"
          "T1,C,open,low,hi,2026-01-01T00:00:00Z,2026-01-01T00:00:00Z\n")
    run(wh, tmp_path)
    assert wh.query("SELECT order_id, refund_amount FROM support_tickets") == [(None, None)]


def test_invalid_utf8_rejects_only_that_file(wh, tmp_path):
    write(tmp_path, "orders_bad.csv", b"order_id,\xff\xfe\n")
    write(tmp_path, "orders_ok.csv", ORDER_CSV_HEADER + order_csv_row("O1"))
    rep = run(wh, tmp_path)
    assert by_file(rep)["orders_bad.csv"].status == "rejected" and orders(wh) == ["O1"]


# ------------------------------------------------------------------------------------ JSON / JSONL
def test_duplicate_json_keys_are_quarantined_not_last_wins(wh, tmp_path):
    line = ('{"order_id":"A","order_id":"B","customer_id":"C","status":"paid","currency":"USD","total_amount":"1.00",'
            '"order_ts":"2026-01-01T00:00:00Z","updated_at":"2026-01-05T00:00:00Z"}\n')
    write(tmp_path, "orders_a.jsonl", line + jl(order("OK")))
    write(tmp_path, "orders_b.json", "[" + line.strip() + "," + json.dumps(order("OK2")) + "]")
    run(wh, tmp_path)
    assert orders(wh) == ["OK", "OK2"] and quarantine_reasons(wh) == {"duplicate_json_keys": 2}


def test_u2028_and_other_unicode_line_separators_inside_strings_do_not_split_records(wh, tmp_path):
    """Regression-class bug found during review: str.splitlines() splits on U+2028/U+0085/\\x0b..."""
    subj = "a b\u0085c\x0bd\x1ce \U0001F600 é"
    rec = dict(TICKET_BASE, subject=subj)
    write(tmp_path, "support_tickets_u.jsonl", json.dumps(rec, ensure_ascii=False) + "\n")
    run(wh, tmp_path)
    assert wh.query("SELECT subject FROM support_tickets") == [(subj,)] and not quarantine_reasons(wh)


def test_jsonl_crlf_blank_lines_and_missing_trailing_newline(wh, tmp_path):
    body = json.dumps(order("A")) + "\r\n\r\n   \r\n" + json.dumps(order("B"))
    write(tmp_path, "orders_a.jsonl", body)
    rep = run(wh, tmp_path)
    assert orders(wh) == ["A", "B"] and by_file(rep)["orders_a.jsonl"].counts["rows_read"] == 2


def test_jsonl_row_numbers_are_physical_line_numbers(wh, tmp_path):
    write(tmp_path, "orders_a.jsonl", json.dumps(order("A")) + "\n\n{broken\n" + json.dumps(order("B")) + "\n[1]\n")
    run(wh, tmp_path)
    assert wh.query("SELECT row_number, reason FROM quarantine ORDER BY 1") == [(3, "invalid_json"), (5, "not_an_object")]


@pytest.mark.parametrize("name,content,status", [
    ("orders_e1.jsonl", "", "loaded"), ("orders_e2.json", "[]", "loaded"), ("orders_e3.json", "", "rejected"),
    ("orders_e4.json", "{}", "rejected"), ("orders_e5.json", "null", "rejected"), ("orders_e6.json", "[1,2", "rejected"),
    ("orders_e7.json", "[NaN]", "rejected"), ("orders_e8.json", "[" * 200_000, "rejected"),
    ("orders_e9.json", '[{"a":' + "9" * 5000 + "}]", "rejected"), ("orders_e10.json", '["x", 1, null, [], {}]', "loaded"),
    ("orders_e11.ndjson", "\n\n", "loaded"),
])
def test_json_file_level_edge_cases_never_crash_the_run(wh, tmp_path, name, content, status):
    write(tmp_path, name, content)
    write(tmp_path, "orders_good.jsonl", jl(order("GOOD")))
    rep = run(wh, tmp_path)
    assert rep.ok and by_file(rep)[name].status == status and orders(wh) == ["GOOD"]


def test_jsonl_line_level_edge_cases_only_quarantine_the_line(wh, tmp_path):
    lines = ["[" * 200_000, '{"x":' + "9" * 5000 + "}", '{"total_amount": NaN}', '{"a": Infinity}',
             "42", '"s"', "null", "true", '{"order_id": }', json.dumps(order("GOOD"))]
    write(tmp_path, "orders_x.jsonl", "\n".join(lines) + "\n")
    rep = run(wh, tmp_path)
    assert rep.ok and orders(wh) == ["GOOD"]
    assert quarantine_reasons(wh) == {"invalid_json": 5, "not_an_object": 4}


def test_json_raw_record_with_nul_is_stored_escaped(wh, tmp_path):
    write(tmp_path, "orders_x.jsonl", "{\"order_id\": \"a\x00b\"}\n" + jl(order("GOOD")))
    run(wh, tmp_path)
    raw = wh.query("SELECT raw_record FROM quarantine")[0][0]
    assert "\x00" not in raw and "\\x00" in raw and orders(wh) == ["GOOD"]


def test_lone_surrogate_and_nul_in_text_quarantine_row_not_the_whole_file(wh, tmp_path):
    """Regression-class bug found during review: a lone surrogate made DuckDB reject the INSERT, rolling
    back (and permanently failing) the entire file."""
    write(tmp_path, "orders_s.jsonl",
          jl(order("A")) + json.dumps(order("B", customer_id="x\ud800y")) + "\n"
          + json.dumps(order("C", customer_id="a\u0000b")) + "\n" + jl(order("D")))
    rep = run(wh, tmp_path)
    assert rep.ok and by_file(rep)["orders_s.jsonl"].status == "loaded"
    assert orders(wh) == ["A", "D"] and quarantine_reasons(wh) == {"invalid_characters": 2}


def test_unexpected_validator_crash_quarantines_row_and_run_continues(wh, tmp_path, monkeypatch):
    import pipeline.runner as runner
    real = runner.validate

    def flaky(spec, data):
        if data.get("order_id") == "BOOM":
            raise ZeroDivisionError("synthetic validator bug")
        return real(spec, data)

    monkeypatch.setattr(runner, "validate", flaky)
    write(tmp_path, "orders_a.jsonl", jl(order("BOOM"), order("FINE")))
    rep = run(wh, tmp_path)
    assert rep.ok and orders(wh) == ["FINE"] and quarantine_reasons(wh) == {"validation_error": 1}


def test_parser_crash_marks_only_that_file_failed(wh, tmp_path, monkeypatch):
    import pipeline.runner as runner
    real = runner.parse

    def parse(path, content, spec):
        if path.name == "orders_a.jsonl":
            raise RuntimeError("synthetic parser bug")
        return real(path, content, spec)

    monkeypatch.setattr(runner, "parse", parse)
    write(tmp_path, "orders_a.jsonl", jl(order("A")))
    write(tmp_path, "orders_b.jsonl", jl(order("B")))
    rep = run(wh, tmp_path)
    assert not rep.ok and by_file(rep)["orders_a.jsonl"].status == "failed" and orders(wh) == ["B"]
    assert wh.query("SELECT COUNT(*) FROM checkpoints WHERE source_name='orders_a.jsonl'") == [(0,)]


# ---------------------------------------------------------------------------- decimals & integers
GOOD = dict(order_id="O1", customer_id="C1", status="paid", currency="USD", total_amount="10.50",
            order_ts="2026-01-01T00:00:00Z", updated_at="2026-01-02T00:00:00Z")


@pytest.mark.parametrize("value,reason", [
    ("1_0.00", "invalid_money"), ("١٠.00", "invalid_money"), ("１０.00", "invalid_money"),
    ("1,000.00", "invalid_money"), ("1e2", "invalid_money"), ("1e2x", "invalid_money"), ("0x10", "invalid_money"),
    ("1.", "invalid_money"), (".5", "invalid_money"), ("--1", "invalid_money"), ("1 0", "invalid_money"),
    ("Infinity", "invalid_money"), ("-inf", "invalid_money"), ("NaN", "invalid_money"), ("sNaN", "invalid_money"),
    ("$5.00", "invalid_money"), ("5.00 USD", "invalid_money"), ("1." + "0" * 3 + "1", "money_precision"),
    ("0.001", "money_precision"), ("0." + "0" * 5000 + "1", "money_precision"),
    ("-0.01", "negative_money"), ("1000000000.00", "money_out_of_range"), ("9" * 400, "money_out_of_range"),
    (True, "wrong_type"), (None, "missing_required"), ("", "missing_required"), ("   ", "missing_required"),
    ([], "wrong_type"), ({}, "wrong_type"),
])
def test_malformed_money(value, reason):
    with pytest.raises(Invalid) as e:
        validate(ORDERS, {**GOOD, "total_amount": value})
    assert e.value.reason == reason


@pytest.mark.parametrize("value,expected", [
    ("0", "0.00"), ("-0.00", "0.00"), ("+5", "5.00"), ("5.5", "5.50"), (" 7.25 ", "7.25"), ("999999999.99", "999999999.99"),
    ("12.3400", "12.34"), (5, "5.00"), (Decimal("1E+2"), "100.00"), (Decimal("0.10"), "0.10"),
])
def test_money_accepts_exact_forms(value, expected):
    assert validate(ORDERS, {**GOOD, "total_amount": value})["total_amount"] == Decimal(expected)


def test_money_representations_hash_identically():
    a, b, c = (validate(ORDERS, {**GOOD, "total_amount": v}) for v in ("10.5", "10.50", Decimal("10.500")))
    assert a["row_hash"] == b["row_hash"] == c["row_hash"]


def test_json_numbers_with_float_syntax_stay_exact(wh, tmp_path):
    text = ('{"order_id":"A","customer_id":"C","status":"paid","currency":"USD","total_amount":0.30,'
            '"order_ts":"2026-01-01T00:00:00Z","updated_at":"2026-01-05T00:00:00Z"}\n'
            '{"order_id":"B","customer_id":"C","status":"paid","currency":"USD","total_amount":0.30000000000000004,'
            '"order_ts":"2026-01-01T00:00:00Z","updated_at":"2026-01-05T00:00:00Z"}\n'
            '{"order_id":"D","customer_id":"C","status":"paid","currency":"USD","total_amount":1E2,'
            '"order_ts":"2026-01-01T00:00:00Z","updated_at":"2026-01-05T00:00:00Z"}\n')
    write(tmp_path, "orders_f.jsonl", text)
    run(wh, tmp_path)
    assert dict(wh.query("SELECT order_id, total_amount FROM orders")) == {"A": Decimal("0.30"), "D": Decimal("100.00")}
    assert quarantine_reasons(wh) == {"money_precision": 1}


@pytest.mark.parametrize("value,ok", [("0", True), ("3.0", True), ("+5", True), ("1000000000", True), (7, True),
                                       ("1000000001", False), ("-1", False), ("3.5", False), ("1e3", False),
                                       ("1_000", False), (True, False), (None, False), ("", False), ("x", False),
                                       ("9" * 50, False)])
def test_inventory_quantity(value, ok):
    from pipeline.schema import INVENTORY
    rec = dict(sku="S", warehouse_id="W", quantity_on_hand=value, unit_cost="1.00", updated_at="2026-01-05T00:00:00Z")
    if ok:
        assert validate(INVENTORY, rec)["quantity_on_hand"] == int(Decimal(value) if isinstance(value, str) else value)
    else:
        with pytest.raises(Invalid):
            validate(INVENTORY, rec)


# ------------------------------------------------------------------------------------- timestamps
@pytest.mark.parametrize("ts", ["0001-01-01T00:00:00+23:59", "9999-12-31T23:59:59-23:59", "0001-01-01T00:00:00Z",
                                 "1999-12-31T23:59:59Z", "2000-01-01T00:00:00+01:00", "1969-12-31T23:59:59Z"])
def test_extreme_timestamps_are_rejected_not_crashing(ts):
    with pytest.raises(Invalid) as e:
        validate(ORDERS, {**GOOD, "order_ts": ts})
    assert e.value.reason == "timestamp_out_of_range"


@pytest.mark.parametrize("ts,reason", [
    ("2026-01-01", "naive_timestamp"), ("2026-01-01T00:00:00", "naive_timestamp"), ("2026-01-01T25:00:00Z", "invalid_timestamp"),
    ("2026-02-30T00:00:00Z", "invalid_timestamp"), ("2026-01-01T00:00:00+24:00", "invalid_timestamp"),
    ("", "missing_required"), (1767225600, "wrong_type"), (True, "wrong_type"), ("not a date", "invalid_timestamp"),
])
def test_malformed_timestamps(ts, reason):
    with pytest.raises(Invalid) as e:
        validate(ORDERS, {**GOOD, "order_ts": ts})
    assert e.value.reason == reason


@pytest.mark.parametrize("ts,iso", [
    ("2000-01-01T00:00:00Z", "2000-01-01T00:00:00+00:00"), ("2000-01-01T00:00:00+00:00", "2000-01-01T00:00:00+00:00"),
    ("2026-01-01T05:30:00+05:30", "2026-01-01T00:00:00+00:00"), ("2026-01-01T00:00:00-23:59", "2026-01-01T23:59:00+00:00"),
    ("2026-01-01T00:00:00.123456Z", "2026-01-01T00:00:00.123456+00:00"),
])
def test_valid_extreme_offsets_normalise_to_utc(ts, iso):
    assert validate(ORDERS, {**GOOD, "order_ts": ts, "updated_at": "2026-02-01T00:00:00Z"})["order_ts"].isoformat() == iso


def test_extreme_timestamp_row_in_file_is_quarantined_and_run_continues(wh, tmp_path):
    write(tmp_path, "orders_a.jsonl", jl(order("A", upd="9999-12-31T23:59:59-23:59"), order("B"),
                                          order("C", order_ts="0001-01-01T00:00:00+23:59")))
    rep = run(wh, tmp_path)
    assert rep.ok and orders(wh) == ["B"]
    assert quarantine_reasons(wh) == {"timestamp_out_of_range": 2}


def test_future_boundary_is_exactly_five_minutes_past_as_of(wh, tmp_path):
    write(tmp_path, "orders_a.jsonl", jl(order("OK", upd="2026-02-01T00:05:00Z"), order("LATE", upd="2026-02-01T00:05:00.000001Z")))
    run(wh, tmp_path)
    assert orders(wh) == ["OK"] and quarantine_reasons(wh) == {"future_timestamp": 1}


def test_as_of_near_datetime_max_does_not_overflow(wh, tmp_path):
    from datetime import datetime, timezone
    write(tmp_path, "orders_a.jsonl", jl(order("A")))
    rep = run_pipeline(wh, tmp_path, as_of=datetime(9999, 12, 31, 23, 59, tzinfo=timezone.utc), retry=FAST)
    assert rep.ok and orders(wh) == ["A"]


# --------------------------------------------------------------------------------- other fields
@pytest.mark.parametrize("field,value,reason", [
    ("order_id", "x" * 65, "too_long"), ("order_id", 123, "wrong_type"), ("customer_id", None, "missing_required"),
    ("status", "Teleported", "invalid_value"), ("currency", "usdx", "invalid_value"),
])
def test_other_field_rules(field, value, reason):
    with pytest.raises(Invalid) as e:
        validate(ORDERS, {**GOOD, field: value})
    assert e.value.reason == reason


def test_status_and_currency_are_case_and_whitespace_normalised():
    r = validate(ORDERS, {**GOOD, "status": "  SHIPPED ", "currency": " eur", "order_id": "  O9  "})
    assert (r["status"], r["currency"], r["order_id"]) == ("shipped", "EUR", "O9")
