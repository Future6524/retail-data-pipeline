"""Tests for scripts/check_hygiene.py. Fake secrets are assembled at runtime so this file itself stays clean."""
import importlib.util
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location("check_hygiene", ROOT / "scripts" / "check_hygiene.py")
hy = importlib.util.module_from_spec(spec)
spec.loader.exec_module(hy)

FAKES = {
    "private key block": "-----BEGIN " + "RSA PRIVATE KEY-----",
    "AWS access key id": "key = " + "AKIA" + "ABCDEFGHIJKLMNOP",
    "GitHub token": "t = gh" + "p_" + "a" * 36,
    "Slack token": "x = xox" + "b-1234567890-abcdef",
    "quoted literal credential": "db_" + "password" + ' = "' + "s3cr3t-value-123" + '"',
    "SNOWFLAKE_PASSWORD with literal value": "export SNOWFLAKE_" + "PASSWORD=" + "hunter2hunter2",
}


@pytest.mark.parametrize("label", sorted(FAKES))
def test_each_pattern_detects_its_planted_secret(tmp_path, label):
    (tmp_path / "x.txt").write_text(FAKES[label] + "\n")
    hits = hy.scan_secrets(tmp_path)
    assert any(label in h for h in hits), hits


@pytest.mark.parametrize("line", [
    'password = os.environ["X"]', 'SNOWFLAKE_PASSWORD=<your password>', "export SNOWFLAKE_PASSWORD=$PW",
    'api_key = ""', 'secret = "changeme-example-value"', "no secrets here",
    "t = " + "AKIA" + "ABCDEFGHIJKLMNOP  # hygiene: allow",
])
def test_benign_lines_are_not_flagged(tmp_path, line):
    (tmp_path / "x.txt").write_text(line + "\n")
    assert hy.scan_secrets(tmp_path) == []


def test_credential_file_names_and_skipped_dirs(tmp_path):
    (tmp_path / ".env").write_text("A=1")
    (tmp_path / ".review").mkdir()
    (tmp_path / ".review" / "x.txt").write_text(FAKES["AWS access key id"])
    hits = hy.scan_secrets(tmp_path)
    assert hits == [".env: credential-looking file name"]


@pytest.mark.parametrize("d", [".review", ".review-venv", ".review-final-venv", ".review-anything", "x.egg-info", ".git", "sub/.venv"])
def test_ignored_dirs_are_pruned(tmp_path, d):
    (tmp_path / d).mkdir(parents=True)
    (tmp_path / d / "cacert.pem").write_text("x")
    (tmp_path / d / "x.txt").write_text(FAKES["AWS access key id"])
    assert hy.scan_secrets(tmp_path) == []


def test_any_virtualenv_is_pruned_but_real_credentials_still_found(tmp_path):
    (tmp_path / "myenv" / "lib").mkdir(parents=True)
    (tmp_path / "myenv" / "pyvenv.cfg").write_text("home = /usr\n")
    (tmp_path / "myenv" / "lib" / "cacert.pem").write_text("x")
    (tmp_path / "review-notes").mkdir()  # NOT a .review* dir: must still be scanned
    (tmp_path / "review-notes" / "x.txt").write_text(FAKES["AWS access key id"])
    (tmp_path / "app").mkdir()
    (tmp_path / "app" / "id_rsa").write_text("x")
    assert hy.scan_secrets(tmp_path) == ["app/id_rsa: credential-looking file name",
                                         "review-notes/x.txt:1: possible AWS access key id"]


def test_lock_check_detects_missing_extra_and_non_exact_pins(tmp_path):
    (tmp_path / "pyproject.toml").write_text('[project]\nname="x"\nversion="1"\ndependencies=["duckdb>=1.4"]\n'
                                              '[project.optional-dependencies]\ndev=["pytest>=8"]\nsnowflake=["snowflake-connector-python"]\n')
    (tmp_path / "requirements-lock.txt").write_text("duckdb==1.0.0\n")
    assert hy.check_lock(tmp_path) == ["lock: declared dependency 'pytest' has no exact pin in requirements-lock.txt"]
    (tmp_path / "requirements-lock.txt").write_text("duckdb>=1\n")
    assert "not an exact pin" in hy.check_lock(tmp_path)[0]
    (tmp_path / "requirements-lock.txt").write_text("duckdb==0.0.1\npytest==0.0.1\n")
    assert len(hy.check_lock(tmp_path, installed=True)) == 2  # both differ from what is installed


def test_the_repository_itself_is_clean_and_lock_matches_installed():
    assert hy.check_lock(ROOT, installed=True) == []
    assert hy.scan_secrets(ROOT) == []
