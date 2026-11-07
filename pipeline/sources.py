"""Immutable source reading. Files are read once as bytes (never written); the SHA-256 identifies
exactly the bytes that were parsed. CSV, JSON (array) and JSONL are supported.

Failure containment: anything wrong with a single record (bad quoting, oversize field, invalid JSON,
duplicate JSON keys, ...) yields a RawRow with `data=None` and a reason, so only that row is
quarantined. Only damage that makes the *whole file* untrustworthy (bad encoding, bad/duplicate CSV
header, JSON-array syntax error) raises FileRejected."""
from __future__ import annotations

import csv
import hashlib
import io
import json
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterator

from .errors import FileRejected
from .schema import ENTITIES, EntitySpec

FORMATS = {".csv": "csv", ".json": "json", ".jsonl": "jsonl", ".ndjson": "jsonl"}


@dataclass(frozen=True)
class RawRow:
    row_number: int  # 1-based data row (CSV/JSON) or line number (JSONL)
    data: dict[str, Any] | None  # None => unparseable
    raw: str
    error: str | None = None  # reason code when data is None
    detail: str = ""


def entity_for(path: Path) -> EntitySpec | None:
    if path.suffix.lower() not in FORMATS:
        return None
    for spec in ENTITIES.values():
        if path.name.startswith(spec.prefix + "_"):
            return spec
    return None


def read_bytes(path: Path) -> tuple[bytes, str]:
    data = path.read_bytes()
    return data, hashlib.sha256(data).hexdigest()


class _DuplicateKeys(dict):
    """Marker returned by the JSON object hook when an object repeats a key (json would silently keep
    the last value, i.e. lose data)."""


def _object_hook(pairs: list[tuple[str, Any]]) -> dict:
    d = dict(pairs)
    return _DuplicateKeys(d) if len(d) != len(pairs) else d


def _reject_constant(name: str) -> Any:
    raise ValueError(f"non-standard JSON constant {name}")


def _loads(text: str) -> Any:
    # Decimal for JSON floats: money must never pass through binary floating point.
    return json.loads(text, parse_float=Decimal, object_pairs_hook=_object_hook, parse_constant=_reject_constant)


def _dump(obj: Any) -> str:
    return json.dumps(obj, default=str, sort_keys=True)


def parse(path: Path, content: bytes, spec: EntitySpec) -> Iterator[RawRow]:
    try:
        text = content.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise FileRejected(f"invalid_encoding: {exc.reason}") from exc
    fmt = FORMATS[path.suffix.lower()]
    if fmt == "csv":
        yield from _csv(text, spec)
    elif fmt == "jsonl":
        yield from _jsonl(text)
    else:
        yield from _json_array(text)


def _csv(text: str, spec: EntitySpec) -> Iterator[RawRow]:
    # strict=True: malformed quoting raises csv.Error instead of being guessed at.
    reader = csv.DictReader(io.StringIO(text, newline=""), strict=True)
    try:
        header = reader.fieldnames or []
    except csv.Error as exc:
        raise FileRejected(f"invalid_csv_header: {exc}") from exc
    dupes = sorted({h for h in header if header.count(h) > 1})
    if dupes:  # DictReader would silently keep only the last duplicate column
        raise FileRejected(f"duplicate_header_columns: {dupes}")
    required = {c.name for c in spec.columns} - {"row_hash", "source_file"}
    optional = {"order_id", "refund_amount"} if spec.name == "support_tickets" else set()
    missing = required - optional - set(header)
    if missing:
        raise FileRejected(f"missing_header_columns: {sorted(missing)}")
    i = 0
    while True:
        i += 1
        try:
            row = next(reader)
        except StopIteration:
            return
        except csv.Error as exc:
            # Includes fields over csv.field_size_limit (131072 chars) and bad quoting. The reader
            # resumes at the next physical line; an unterminated quote swallows the rest of the file
            # into this one bad row.
            yield RawRow(i, None, f"<unreadable csv record near input line {reader.line_num}>", "csv_parse_error",
                         f"{exc} (input line {reader.line_num})")
            continue
        raw = _dump({k: v for k, v in row.items() if k is not None})
        if None in row:
            yield RawRow(i, None, raw + f" EXTRA={row[None]}", "extra_fields")
        elif any(v is None for v in row.values()):
            yield RawRow(i, None, raw, "missing_fields")
        else:
            yield RawRow(i, {k: (v if v != "" else None) for k, v in row.items()}, raw)


def _element(i: int, obj: Any, raw: str) -> RawRow:
    if isinstance(obj, _DuplicateKeys):
        return RawRow(i, None, raw, "duplicate_json_keys")
    if not isinstance(obj, dict):
        return RawRow(i, None, raw, "not_an_object")
    return RawRow(i, obj, raw)


def _jsonl(text: str) -> Iterator[RawRow]:
    # Split on "\n" only: str.splitlines() would also split on U+2028, U+0085, \x0b, \x1c.. which
    # are legal *inside* JSON strings and would corrupt valid records.
    for i, line in enumerate(text.split("\n"), start=1):
        line = line.removesuffix("\r")
        if not line.strip():
            continue
        try:
            obj = _loads(line)
        except (ValueError, RecursionError) as exc:
            yield RawRow(i, None, line, "invalid_json", f"{type(exc).__name__}: {exc}"[:200])
            continue
        yield _element(i, obj, line)


def _json_array(text: str) -> Iterator[RawRow]:
    try:
        obj = _loads(text)
    except (ValueError, RecursionError) as exc:
        raise FileRejected(f"invalid_json: {type(exc).__name__}: {exc}"[:300]) from exc
    if not isinstance(obj, list):
        raise FileRejected("json_top_level_not_array")
    for i, item in enumerate(obj, start=1):
        yield _element(i, item, _dump(item))
