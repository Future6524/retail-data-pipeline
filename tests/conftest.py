import hashlib
from datetime import datetime, timezone
from pathlib import Path

import pytest

from data import generate_demo_batches
from pipeline.retry import RetryPolicy
from pipeline.warehouse import open_warehouse

AS_OF = datetime(2026, 2, 1, tzinfo=timezone.utc)
FAST = RetryPolicy(max_attempts=3, sleep=lambda s: None)


def tree_hashes(d: Path) -> dict:
    return {p.name: (hashlib.sha256(p.read_bytes()).hexdigest(), p.stat().st_mode) for p in sorted(d.iterdir())}


@pytest.fixture(scope="session")
def batches(tmp_path_factory):
    d = tmp_path_factory.mktemp("batches")
    manifest = generate_demo_batches(d, seed=42)
    return d, manifest


@pytest.fixture
def wh():
    w = open_warehouse("duckdb", ":memory:")
    yield w
    w.close()


# ---- tiny record builders used by the adversarial suites -------------------------------------
import json as _json  # noqa: E402

ORDER_CSV_HEADER = "order_id,customer_id,status,currency,total_amount,order_ts,updated_at\n"
TICKET_BASE = dict(ticket_id="T1", customer_id="C", status="open", priority="low", subject="s",
                   created_at="2026-01-01T00:00:00Z", updated_at="2026-01-01T00:00:00Z")


def order(oid="O1", upd="2026-01-05T00:00:00Z", amt="5.00", **kw) -> dict:
    return {**dict(order_id=oid, customer_id="C", status="paid", currency="USD", total_amount=amt,
                   order_ts="2026-01-01T00:00:00Z", updated_at=upd), **kw}


def jl(*recs) -> str:
    return "".join(_json.dumps(r) + "\n" for r in recs)


def order_csv_row(oid="O1", upd="2026-01-05T00:00:00Z", amt="5.00") -> str:
    return f"{oid},C,paid,USD,{amt},2026-01-01T00:00:00Z,{upd}\n"


def write(d: Path, name: str, text: str | bytes) -> Path:
    p = d / name
    p.write_bytes(text if isinstance(text, bytes) else text.encode("utf-8"))
    return p


def by_file(rep) -> dict:
    return {f.file: f for f in rep.files}


def quarantine_reasons(wh) -> dict:
    return dict(wh.query("SELECT reason, COUNT(*) FROM quarantine GROUP BY reason ORDER BY reason"))
