"""Small terminal/HTML helpers shared by the two showcase commands (stdlib only).

* ``ProgressBar`` shows REAL progress: callers advance it only when a unit of work has actually finished.
  On a TTY it redraws one line; on a pipe/file it prints one plain line per stage change (no carriage returns).
* ``render_table`` draws a bordered summary table (Unicode box characters, ASCII fallback).
* ``sheet_html`` writes a self-contained, script-free, LIGHT-mode "spreadsheet" page: bright orange header rows,
  sticky headers, and a scroll container so wide/long tables scroll both down and right.
"""
from __future__ import annotations

import html
import sys
from pathlib import Path
from typing import Sequence, TextIO

ORANGE = "#ff8c00"  # bright orange table header; dark text on it has a contrast ratio above 8:1
HEADER_TEXT = "#111111"


def _unicode_ok(stream: TextIO) -> bool:
    enc = (getattr(stream, "encoding", None) or "").lower().replace("-", "")
    return enc.startswith("utf")


class ProgressBar:
    def __init__(self, total: int, stream: TextIO | None = None, width: int = 30, *, color: bool | None = None):
        self.total, self.stream, self.width = max(total, 0), stream or sys.stdout, width
        self.tty = bool(getattr(self.stream, "isatty", lambda: False)())
        self.color = self.tty if color is None else color
        self.fill, self.empty = ("█", "░") if _unicode_ok(self.stream) else ("#", "-")
        self.done, self.stage, self._printed_done = 0, "", -1
        self._last_stage: str | None = None

    def line(self, detail: str = "") -> str:
        frac = 1.0 if self.total == 0 else min(self.done / self.total, 1.0)
        n = round(frac * self.width)
        bar = self.fill * n + self.empty * (self.width - n)
        if self.color:
            bar = f"\x1b[38;5;208m{bar}\x1b[0m"
        text = f"[{bar}] {frac * 100:3.0f}% {self.done}/{self.total}  {self.stage}"
        return f"{text}  {detail}".rstrip()

    def advance(self, stage: str, detail: str = "", step: int = 1) -> None:
        self.done = min(self.done + step, self.total) if self.total else self.done + step
        self.stage = stage
        self._draw(detail)

    def _draw(self, detail: str) -> None:
        if self.tty:
            self.stream.write("\r\x1b[K" + self.line(detail))
        elif self.stage != self._last_stage:  # pipes/logs: one plain line per stage change, never carriage returns
            self.stream.write(self.line(detail) + "\n")
            self._printed_done = self.done
        self._last_stage = self.stage
        self.stream.flush()

    def finish(self, message: str = "") -> None:
        if self.tty:
            self.stream.write("\r\x1b[K" + self.line(message) + "\n")
        elif self._printed_done != self.done:  # make sure the final 100% state is always visible in logs
            self.stream.write(self.line(message) + "\n")
        self.stream.flush()


def _cell(v: object) -> str:
    return "" if v is None else str(v)


def render_table(headers: Sequence[str], rows: Sequence[Sequence[object]], *, right: Sequence[int] = (),
                 unicode: bool = True) -> str:
    cells = [[_cell(c) for c in r] for r in rows]
    widths = [max([len(h)] + [len(r[i]) for r in cells]) for i, h in enumerate(headers)]
    tl, tm, tr, ml, mm, mr, bl, bm, br, h, v = (("┌", "┬", "┐", "├", "┼", "┤", "└", "┴", "┘", "─", "│") if unicode
                                                else ("+", "+", "+", "+", "+", "+", "+", "+", "+", "-", "|"))

    def rule(a: str, b: str, c: str) -> str:
        return a + b.join(h * (w + 2) for w in widths) + c

    def row(vals: Sequence[str]) -> str:
        parts = [f" {x.rjust(w) if i in right else x.ljust(w)} " for i, (x, w) in enumerate(zip(vals, widths))]
        return v + v.join(parts) + v

    out = [rule(tl, tm, tr), row(list(headers)), rule(ml, mm, mr)]
    out += [row(r) for r in cells]
    out.append(rule(bl, bm, br))
    return "\n".join(out)


def table_for(stream: TextIO, headers: Sequence[str], rows: Sequence[Sequence[object]], right: Sequence[int] = ()) -> str:
    return render_table(headers, rows, right=right, unicode=_unicode_ok(stream))


def browser_command(path: Path) -> str:
    """Shell command that opens ``path`` in the default browser. Printed only; nothing opens automatically."""
    return f"python -m webbrowser -t {path.resolve().as_uri()}"


SHEET_CSS = f"""
:root{{color-scheme:light}}
html,body{{margin:0;height:100%}}
body{{font:14px/1.4 system-ui,-apple-system,"Segoe UI",sans-serif;background:#fff;color:#1b1f24;display:flex;flex-direction:column}}
header{{padding:10px 16px;border-bottom:1px solid #d8dce1}}
h1{{font-size:18px;margin:0}} .note{{color:#4a525c;font-size:12px;margin-top:2px}}
.syn{{font-weight:700;color:#7a4f00}}
.banner{{background:#fff3cd;border:2px solid #b8860b;color:#4a3500;font-weight:700;padding:8px 16px;margin:0}}
.sheets{{flex:1;min-height:0;overflow:auto;padding:0 0 24px}}
h2{{font-size:14px;margin:14px 16px 6px}}
.scroll{{overflow:auto;max-height:70vh;margin:0 16px;border:1px solid #d8dce1}}
table{{border-collapse:separate;border-spacing:0;font-size:13px;min-width:100%}}
thead th{{position:sticky;top:0;background:{ORANGE};color:{HEADER_TEXT};text-align:left;padding:7px 12px;white-space:nowrap;
 border-right:1px solid #e07800;border-bottom:2px solid #c96a00}}
td{{padding:5px 12px;white-space:nowrap;border-bottom:1px solid #e6e9ed;border-right:1px solid #f0f2f4}}
td.n{{text-align:right;font-variant-numeric:tabular-nums}}
tbody tr:nth-child(even) td{{background:#fafafa}}
td.bad{{color:#b3261e;font-weight:700}} td.ok{{color:#0a7a3d;font-weight:700}}
"""


def _cell_html(value: object, numeric: bool) -> str:
    text = html.escape(_cell(value))
    cls = "n" if numeric else ""
    if text in ("failed", "FAILED", "FIRING"):
        cls = (cls + " bad").strip()
    elif text in ("loaded", "SUCCESS", "RESOLVED"):
        cls = (cls + " ok").strip()
    return f'<td class="{cls}">{text}</td>' if cls else f"<td>{text}</td>"


def table_html(headers: Sequence[str], rows: Sequence[Sequence[object]], right: Sequence[int] = (), table_id: str = "") -> str:
    head = "".join(f"<th>{html.escape(h)}</th>" for h in headers)
    body = "".join("<tr>" + "".join(_cell_html(c, i in right) for i, c in enumerate(r)) + "</tr>" for r in rows)
    ident = f' id="{html.escape(table_id)}"' if table_id else ""
    return f'<div class="scroll"><table{ident}><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table></div>'


def sheet_html(title: str, notes: Sequence[str], tables: Sequence[tuple[str, str]], banner: str = "") -> str:
    """``tables`` = (heading, table_html) pairs. No scripts, no external resources, light mode only.
    ``banner`` (optional) is a prominent plain-text strip above the title (used for the FAKE PROVIDER notice)."""
    banner_html = f'<div class="banner">{html.escape(banner)}</div>' if banner else ""
    note_html = "".join(f'<div class="note">{html.escape(n)}</div>' for n in notes)
    sections = "".join(f"<h2>{html.escape(h)}</h2>{t}" for h, t in tables)
    return (f'<!doctype html>\n<html lang="en"><head><meta charset="utf-8">'
            f'<meta name="viewport" content="width=device-width, initial-scale=1"><title>{html.escape(title)}</title>'
            f"<style>{SHEET_CSS}</style></head><body>{banner_html}"
            f'<header><h1>{html.escape(title)}</h1>{note_html}</header><div class="sheets">{sections}</div></body></html>\n')
