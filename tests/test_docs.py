"""Keep README claims tied to reality: its SQL runs, and its seed-42 numbers/digest are what the code produces."""
import re
from pathlib import Path

import pytest
from conftest import AS_OF, FAST

from data import generate_demo_batches
from pipeline.oracle import warehouse_summary
from pipeline.runner import run_pipeline
from pipeline.warehouse import open_warehouse

ROOT = Path(__file__).resolve().parent.parent
README = (ROOT / "README.md").read_text()
SQL_BLOCKS = re.findall(r"```sql\n(.*?)```", README, flags=re.S)


@pytest.fixture(scope="module")
def loaded(tmp_path_factory):
    d = tmp_path_factory.mktemp("seed42")
    m = generate_demo_batches(d, seed=42)
    w = open_warehouse("duckdb")
    assert run_pipeline(w, d, as_of=AS_OF, retry=FAST).ok
    return w, m


def test_readme_has_sql_blocks():
    assert len(SQL_BLOCKS) >= 5


@pytest.mark.parametrize("sql", SQL_BLOCKS, ids=[b.splitlines()[0][:50] for b in SQL_BLOCKS])
def test_readme_sql_executes(loaded, sql):
    loaded[0].query(sql)  # raises on a syntax/column error


def test_readme_seed42_numbers_match_the_code(loaded):
    w, m = loaded
    ef = m["expected_final"]
    assert warehouse_summary(w) == ef
    assert w.state_digest() in README
    for cur, total in ef["orders_total_by_currency"].items():
        assert f"{cur} {total}" in README
    assert f"| orders | {ef['orders']} " in README and f"| inventory | {ef['inventory']} " in README
    assert f"| support_tickets | {ef['support_tickets']} " in README
    assert f"{m['injected']['malformed_total']} rows" in README
    reasons = dict(w.query("SELECT reason, COUNT(*) FROM quarantine GROUP BY reason"))
    assert sum(reasons.values()) == 18 and reasons["invalid_value"] == 3 and reasons["invalid_json"] == 2


def test_readme_documents_required_topics():
    for needle in ("NOT integration-tested", "Python **3.11", "requirements-lock.txt", "least-privilege",
                   "Ambiguous commits", "not a vulnerability scan", "docs/showcase.mp4", "retail-ai-support-app"):
        assert needle.lower() in README.lower(), needle
