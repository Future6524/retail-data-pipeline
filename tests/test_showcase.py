"""Pipeline showcase command: real progress, honest tables, light-mode orange-header results page,
no browser, no server, no invented numbers."""
import csv
import io
import re
import threading
import webbrowser
from html.parser import HTMLParser
from pathlib import Path

import pytest

from pipeline.cli import main as p1_main
from pipeline.showcase import FILE_HEADERS
from pipeline.showcase import run_showcase as p1_showcase
from pipeline.ui import ORANGE, ProgressBar, render_table, sheet_html, table_html

ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture(autouse=True)
def no_browser_no_server(monkeypatch):
    def boom(*a, **k):
        raise AssertionError("a showcase must never open a browser or start a server")
    for name in ("open", "open_new", "open_new_tab", "get"):
        monkeypatch.setattr(webbrowser, name, boom)


class Tables(HTMLParser):
    """Collects every <table> as {id, headers, rows} plus facts about the page."""

    def __init__(self):
        super().__init__()
        self.tables, self._t, self._cell, self._row, self.tags = [], None, None, None, set()

    def handle_starttag(self, tag, attrs):
        self.tags.add(tag)
        if tag == "table":
            self._t = {"id": dict(attrs).get("id"), "headers": [], "rows": []}
        elif tag in ("th", "td"):
            self._cell = ""
        elif tag == "tr":
            self._row = []

    def handle_data(self, data):
        if self._cell is not None:
            self._cell += data

    def handle_endtag(self, tag):
        if tag in ("th", "td") and self._t is not None:
            (self._t["headers"] if tag == "th" else self._row).append(self._cell)
            self._cell = None
        elif tag == "tr" and self._row:
            self._t["rows"].append(self._row)
            self._row = None
        elif tag == "table":
            self.tables.append(self._t)
            self._t = None


def parse(page):
    p = Tables()
    p.feed(page)
    return {t["id"]: t for t in p.tables}, p.tags


def is_bright_orange(hex_color):
    r, g, b = (int(hex_color[i:i + 2], 16) for i in (1, 3, 5))
    return r >= 0xF0 and 0x70 <= g <= 0xA0 and b <= 0x30


def assert_light_orange_page(page):
    assert is_bright_orange(ORANGE) and f"background:{ORANGE}" in page          # bright orange header row
    assert "prefers-color-scheme:dark" not in page and "color-scheme:light" in page.replace(" ", "")  # light mode
    assert "<script" not in page.lower() and "http://" not in page and "https://" not in page       # offline, static
    assert "overflow:auto" in page and "position:sticky" in page                                     # scroll down + right
    assert "SYNTHETIC" in page


# ------------------------------------------------------------------------------------------ helpers
class UTF8(io.StringIO):
    encoding = "utf-8"


class TTY(UTF8):
    def isatty(self):
        return True


def test_progress_bar_tty_redraws_one_line_and_reaches_100_only_at_the_end():
    s = TTY()
    bar = ProgressBar(4, s, width=8)
    for i in range(4):
        bar.advance("stage", f"f{i}")
        assert bar.done == i + 1
    bar.finish("done")
    out = s.getvalue()
    assert out.count("\r") == 5 and out.endswith("\n") and "100% 4/4" in out and " 25% 1/4" in out and "\x1b[38;5;208m" in out
    assert "100%" not in out.split("\r")[1]  # nothing reports completion before the work happened


def test_progress_bar_pipe_prints_plain_lines_per_stage_and_final_state():
    s = io.StringIO()
    bar = ProgressBar(3, s, width=6)
    bar.advance("a")
    bar.advance("a")
    bar.advance("b")
    bar.finish("done")
    out = s.getvalue()
    assert "\r" not in out and "\x1b" not in out
    lines = out.splitlines()
    assert len(lines) == 2 and " 1/3" in lines[0] and "100% 3/3" in lines[1]  # one line per stage change; no duplicate at finish
    s2 = io.StringIO()
    bar2 = ProgressBar(3, s2, width=6)
    bar2.advance("a")
    bar2.advance("a")
    bar2.finish("stopped")
    assert s2.getvalue().splitlines()[-1].count("2/3") == 1 and "100%" not in s2.getvalue()  # never claims completion early


def test_render_table_alignment_and_ascii_fallback():
    t = render_table(["Name", "N"], [["a", 5], ["long name", 123]], right=(1,), unicode=False)
    rows = t.splitlines()
    assert rows[0] == "+" + "-" * 11 + "+" + "-" * 5 + "+" and len({len(r) for r in rows}) == 1
    assert "| a         |   5 |" in t and "| long name | 123 |" in t
    assert "┌" in render_table(["x"], [[1]])


def test_sheet_html_escapes_and_has_orange_header():
    page = sheet_html("T <b>", ["n&n"], [("H", table_html(["a<"], [["<i>x</i>"]]))])
    assert "<i>x</i>" not in page and "&lt;i&gt;x&lt;/i&gt;" in page and "T &lt;b&gt;" in page
    assert_light_orange_page(page.replace("SYNTHETIC", "") + "SYNTHETIC")


# ------------------------------------------------------------------------------------------ pipeline showcase
@pytest.fixture(scope="module")
def p1(tmp_path_factory):
    d = tmp_path_factory.mktemp("p1") / "show"
    out = UTF8()
    threads_before = threading.active_count()
    code = p1_showcase(d, 42, out)
    return d, code, out.getvalue(), threads_before


def test_p1_exit_code_progress_and_table(p1):
    d, code, text, _ = p1
    assert code == 0 and "SYNTHETIC" in text and "6/6 checks passed" in text
    rows = list(csv.reader((d / "results.csv").open(encoding="utf-8")))
    assert rows[0] == FILE_HEADERS and len(rows) - 1 == 43
    # the bar's total equals the number of files really processed, and it ends exactly there
    final = [ln for ln in text.splitlines() if ln.startswith("[")][-1]
    assert "100% 43/43" in final
    for needle in ("┌", "│ Run", "Loaded", "Reject", "Fail", "FAILED (fault)", "SYNTHETIC fault", "recovery"):
        assert needle in text, needle
    assert max(len(ln) for ln in text.splitlines() if ln.startswith(("│", "┌", "├", "└"))) <= 110


def test_p1_summary_numbers_are_derived_from_the_file_rows_not_invented(p1):
    d, _, text, _ = p1
    rows = list(csv.DictReader((d / "results.csv").open(encoding="utf-8")))
    for run in "12345":
        mine = [r for r in rows if r["Run"] == run]
        loaded = sum(r["Status"] == "loaded" for r in mine)
        ins = sum(int(r["Inserted"]) for r in mine)
        line = next(ln for ln in text.splitlines() if re.match(rf"│ {run}\s+│", ln))
        cells = [c.strip() for c in line.strip("│").split("│")]
        assert int(cells[2]) == len(mine) and int(cells[3]) == loaded and int(cells[7]) == ins
    fault_rows = [r for r in rows if r["Run"] == "4"]
    assert all("SYNTHETIC" in r["Synthetic fault"] for r in fault_rows)
    assert not any(r["Synthetic fault"] for r in rows if r["Run"] != "4")
    assert any(r["Status"] == "failed" and r["Attempts"] == "3" for r in fault_rows)


def test_p1_results_html_matches_csv_is_light_orange_and_scrollable(p1):
    d, _, text, _ = p1
    page = (d / "results.html").read_text(encoding="utf-8")
    assert_light_orange_page(page)
    tables, _ = parse(page)
    t = tables["file-results"]
    csv_rows = list(csv.reader((d / "results.csv").open(encoding="utf-8")))[1:]
    assert t["headers"] == FILE_HEADERS and len(t["headers"]) >= 15          # enough columns to scroll right
    assert len(t["rows"]) == len(csv_rows) >= 40                              # enough rows to scroll down
    assert [r[2] for r in t["rows"]] == [r[2] for r in csv_rows] and all(len(r) == len(FILE_HEADERS) for r in t["rows"])
    assert "run-summary" in tables and "Per-file results" in page
    first_cmd = f"python -m webbrowser -t {(d / 'results.html').resolve().as_uri()}"
    assert first_cmd in text


def test_p1_starts_no_server_or_threads_and_leaves_only_expected_files(p1):
    d, _, _, threads_before = p1
    assert threading.active_count() == threads_before
    assert {p.name for p in d.iterdir()} == {"batches", "warehouse.duckdb", "results.html", "results.csv"}


def test_p1_non_utf8_stream_gets_an_ascii_table_and_bar(tmp_path):
    out = io.StringIO()  # no encoding attribute -> ASCII fallback, still complete
    assert p1_showcase(tmp_path / "w", 42, out) == 0
    text = out.getvalue()
    assert text.isascii() and "+-----+" in text and "[##########" in text and "| Run" in text


def test_p1_is_repeatable(tmp_path):
    outs = []
    for _ in range(2):
        buf = io.StringIO()
        assert p1_showcase(tmp_path / "w", 42, buf) == 0
        outs.append(list(csv.reader((tmp_path / "w" / "results.csv").open(encoding="utf-8"))))
    assert outs[0] == outs[1]  # nothing time-based is in the CSV


def test_p1_cli_entry(tmp_path, capsys):
    assert p1_main(["showcase", "--workdir", str(tmp_path / "w")]) == 0
    assert "python -m webbrowser -t file://" in capsys.readouterr().out


# ------------------------------------------------------------------------------------------ docs <-> reality
def test_docs_name_the_showcase_command_and_stay_honest():
    readme = (ROOT / "README.md").read_text()
    for needle in ("python -m pipeline showcase", "python -m webbrowser -t", "docs/showcase.mp4"):
        assert needle in readme, needle
    assert len(FILE_HEADERS) == 19 and "43 per-file rows x 19 columns" in readme  # = the tested numbers above
    assert (ROOT / "docs" / "showcase.mp4").is_file()
