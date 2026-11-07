import json
import subprocess
import sys

import pytest

pytestmark = pytest.mark.e2e


def cli(*args, cwd=None):
    return subprocess.run([sys.executable, "-m", "pipeline", *args], capture_output=True, text=True, cwd=cwd)


def test_generate_run_rerun_status(tmp_path):
    b, db = tmp_path / "b", tmp_path / "w.duckdb"
    assert cli("generate", "--out", str(b)).returncode == 0
    r1 = cli("run", "--input", str(b), "--db", str(db), "--as-of", "2026-02-01T00:00:00Z")
    assert r1.returncode == 0, r1.stderr
    r2 = cli("run", "--input", str(b), "--db", str(db), "--as-of", "2026-02-01T00:00:00Z")
    d = lambda r: [l for l in r.stdout.splitlines() if l.startswith("state digest")]
    assert d(r1) == d(r2) and "skipped_checkpointed" in r2.stdout
    st = cli("status", "--db", str(db))
    assert st.returncode == 0 and "orders" in st.stdout and "future_timestamp" in st.stdout
    assert d(st) == d(r1)


def test_run_json_report_and_backfill(tmp_path):
    b, db = tmp_path / "b", tmp_path / "w.duckdb"
    cli("generate", "--out", str(b))
    r = cli("backfill", "--input", str(b), "--db", str(db), "--since", "2026-01-10T00:00:00Z",
            "--until", "2026-01-11T00:00:00Z", "--as-of", "2026-02-01T00:00:00Z", "--json")
    assert r.returncode == 0
    rep = json.loads(r.stdout)  # the WHOLE stdout must be one JSON document
    assert rep["ok"] and rep["totals"]["rows_out_of_window"] > 0 and len(rep["state_digest"]) == 64


def test_naive_since_is_rejected(tmp_path):
    r = cli("backfill", "--input", str(tmp_path), "--db", str(tmp_path / "x.duckdb"), "--since", "2026-01-10", "--until", "2026-01-11T00:00:00Z")
    assert r.returncode == 2 and "UTC offset" in r.stderr


def test_snowflake_backend_without_env_fails_cleanly(tmp_path):
    import os
    env = {k: v for k, v in os.environ.items() if not k.startswith("SNOWFLAKE_")}
    r = subprocess.run([sys.executable, "-m", "pipeline", "status", "--backend", "snowflake"],
                       capture_output=True, text=True, env=env)
    assert r.returncode == 2 and "error:" in r.stderr  # never attempts a connection


def test_demo_command(tmp_path):
    r = cli("demo", "--workdir", str(tmp_path / "demo"))
    assert r.returncode == 0, r.stdout + r.stderr
    out = r.stdout
    assert "all demo invariants held" in out and "[FAIL]" not in out and "[ok]" in out
    # every report is checked, not just the rerun
    for step in ("step 1:", "step 2:", "step 3:", "step 4:", "5a:", "5b:"):
        assert f"[ok] {step}" in out, step
    # the injected retry is visible and explicitly labelled synthetic, never presented as a real outage
    assert "SYNTHETIC fault injection" in out and "NOT a real outage" in out
    assert "attempts=2" in out and "attempts=3" in out and "ERROR: InjectedFault: SYNTHETIC" in out
    assert "orders_total_by_currency" in out and "orders_total_amount_sum" not in out


def test_demo_is_reproducible(tmp_path):
    a = cli("demo", "--workdir", str(tmp_path / "a")).stdout
    b = cli("demo", "--workdir", str(tmp_path / "b")).stdout
    norm = lambda s, d: s.replace(str(d), "<W>")
    assert norm(a, tmp_path / "a") == norm(b, tmp_path / "b")


def test_json_output_is_one_machine_readable_document_even_on_failure(tmp_path):
    b = tmp_path / "b"
    cli("generate", "--out", str(b), "--seed", "3")
    (b / "orders_dangling.csv").symlink_to(b / "nope")  # forces a failed file => exit 1
    r = cli("run", "--input", str(b), "--db", str(tmp_path / "w.duckdb"), "--as-of", "2026-02-01T00:00:00Z", "--json")
    assert r.returncode == 1
    doc = json.loads(r.stdout)  # no text appended before or after
    assert doc["ok"] is False and len(doc["state_digest"]) == 64
    assert {f["file"]: f["status"] for f in doc["files"]}["orders_dangling.csv"] == "failed"
    assert set(doc) == {"ok", "totals", "ignored", "files", "state_digest"}


def test_json_digest_equals_text_digest(tmp_path):
    b, db = tmp_path / "b", tmp_path / "w.duckdb"
    cli("generate", "--out", str(b))
    args = ("run", "--input", str(b), "--db", str(db), "--as-of", "2026-02-01T00:00:00Z")
    text = cli(*args).stdout
    doc = json.loads(cli(*args, "--json").stdout)
    assert f"state digest: {doc['state_digest']}" in text


@pytest.mark.parametrize("n", ["2", "0", "-3", "7", "100001", "abc"])
def test_generate_rejects_bad_order_counts_clearly(tmp_path, n):
    out = tmp_path / "o"
    (out).mkdir()
    (out / "orders_batch1.csv").write_text("keep")
    r = cli("generate", "--out", str(out), "--orders", n)
    assert r.returncode == 2 and "Traceback" not in r.stderr
    assert ("between 8 and 100000" in r.stderr) or n == "abc"
    assert (out / "orders_batch1.csv").read_text() == "keep"


def test_generate_minimum_orders_works(tmp_path):
    assert cli("generate", "--out", str(tmp_path / "o"), "--orders", "8").returncode == 0


@pytest.mark.parametrize("bad", [["--retries", "0"], ["--retries", "-1"], ["--retries", "x"], ["--as-of", "2026-02-01"],
                                 ["--as-of", "garbage"], ["--as-of", "0001-01-01T00:00:00+23:59"]])
def test_bad_cli_arguments_exit_2_without_traceback(tmp_path, bad):
    r = cli("run", "--input", str(tmp_path), "--db", str(tmp_path / "w.duckdb"), *bad)
    assert r.returncode == 2 and "Traceback" not in r.stderr


def test_missing_input_dir_is_a_clean_error(tmp_path):
    r = cli("run", "--input", str(tmp_path / "nope"), "--db", str(tmp_path / "w.duckdb"))
    assert r.returncode == 2 and "error:" in r.stderr and "Traceback" not in r.stderr


def test_oversize_csv_does_not_abort_cli_run(tmp_path):
    b = tmp_path / "b"
    b.mkdir()
    hdr = "order_id,customer_id,status,currency,total_amount,order_ts,updated_at\n"
    (b / "orders_a.csv").write_text(hdr + "O1,C,paid,USD,1.00,2026-01-01T00:00:00Z,2026-01-02T00:00:00Z\n"
                                     + "x" * 200_000 + ",C,paid,USD,1.00,2026-01-01T00:00:00Z,2026-01-02T00:00:00Z\n")
    (b / "orders_b.csv").write_text(hdr + "O2,C,paid,USD,1.00,2026-01-01T00:00:00Z,2026-01-02T00:00:00Z\n")
    r = cli("run", "--input", str(b), "--db", str(tmp_path / "w.duckdb"), "--as-of", "2026-02-01T00:00:00Z")
    assert r.returncode == 0 and "Traceback" not in r.stderr
    st = cli("status", "--db", str(tmp_path / "w.duckdb")).stdout
    assert "csv_parse_error" in st and "orders" in st


def test_sources_untouched_by_cli_run(tmp_path):
    b = tmp_path / "b"
    cli("generate", "--out", str(b))
    before = {p.name: p.read_bytes() for p in b.iterdir()}
    cli("run", "--input", str(b), "--db", str(tmp_path / "w.duckdb"), "--as-of", "2026-02-01T00:00:00Z")
    assert {p.name: p.read_bytes() for p in b.iterdir()} == before
