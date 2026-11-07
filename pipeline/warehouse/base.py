"""Backend-neutral warehouse logic. All SQL here is written to run unchanged on DuckDB and
Snowflake (MERGE, CAST, TIMESTAMPTZ/DECIMAL/VARCHAR/BIGINT, qmark parameters). Subclasses supply only
connection handling, statement execution and transient-error classification.

Transaction contract: callers wrap one source file's whole load (stage -> merge -> quarantine ->
watermark -> checkpoint) in `transaction()`. The checkpoint is the last statement, so it is
committed if and only if the data is. Staging tables are session temp tables created OUTSIDE any
transaction (Snowflake DDL implicitly commits) and only touched with DML (DELETE/INSERT) inside one."""
from __future__ import annotations

import hashlib
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Iterable, Iterator, Sequence

from ..errors import PipelineError, TransientError
from ..schema import ENTITIES, QUARANTINE, Column, EntitySpec
from ..validate import stage_value

CONTROL_DDL = [
    """CREATE TABLE IF NOT EXISTS checkpoints (
        source_name VARCHAR NOT NULL, file_sha256 VARCHAR NOT NULL,
        window_start VARCHAR NOT NULL, window_end VARCHAR NOT NULL,
        entity VARCHAR NOT NULL, status VARCHAR NOT NULL,
        rows_read BIGINT, rows_inserted BIGINT, rows_updated BIGINT, rows_unchanged BIGINT,
        rows_stale BIGINT, rows_late BIGINT, rows_quarantined BIGINT, rows_out_of_window BIGINT,
        rows_batch_duplicates BIGINT, processed_at TIMESTAMPTZ,
        PRIMARY KEY (source_name, file_sha256, window_start, window_end))""",
    """CREATE TABLE IF NOT EXISTS watermarks (
        entity VARCHAR NOT NULL PRIMARY KEY, max_updated_at TIMESTAMPTZ NOT NULL)""",
]


def table_ddl(spec: EntitySpec) -> str:
    cols = ", ".join(f"{c.name} {c.sql_type} NOT NULL" if c.name in spec.key or c.name in
                     ("updated_at", "row_hash", "source_file") else f"{c.name} {c.sql_type}"
                     for c in spec.columns)
    return f"CREATE TABLE IF NOT EXISTS {spec.table} ({cols}, PRIMARY KEY ({', '.join(spec.key)}))"


def stage_ddl(spec: EntitySpec) -> str:
    cols = ", ".join(f"{c.name} VARCHAR" for c in spec.columns)
    return f"CREATE TEMPORARY TABLE IF NOT EXISTS {spec.stage} ({cols})"


def _cast(c: Column, alias: str = "s") -> str:
    return f"CAST({alias}.{c.name} AS {c.sql_type})"


def _newer(spec: EntitySpec, provenance: bool = False) -> str:
    """Staged row beats stored row. Order: updated_at, then row_hash (a content hash). With
    provenance=True an exact content tie is also broken by the smaller source_file name, so the same
    set of files always yields the same source_file column whatever order they arrive in."""
    u = _cast(spec.col("updated_at"))
    tie = "s.row_hash > t.row_hash"
    if provenance:
        tie = f"({tie} OR (s.row_hash = t.row_hash AND s.source_file < t.source_file))"
    return f"({u} > t.updated_at OR ({u} = t.updated_at AND {tie}))"


def merge_sql(spec: EntitySpec) -> str:
    on = " AND ".join(f"t.{k} = {_cast(spec.col(k))}" for k in spec.key)
    names = [c.name for c in spec.columns]
    insert = (f"WHEN NOT MATCHED THEN INSERT ({', '.join(names)}) "
              f"VALUES ({', '.join(_cast(c) for c in spec.columns)})")
    matched = ""
    if spec.versioned:  # never overwrite a newer (or equal) version with an older one
        sets = ", ".join(f"{c.name} = {_cast(c)}" for c in spec.columns if c.name not in spec.key)
        matched = f"WHEN MATCHED AND {_newer(spec, provenance=True)} THEN UPDATE SET {sets} "
    return f"MERGE INTO {spec.table} AS t USING {spec.stage} AS s ON {on} {matched}{insert}"


def classify_sql(spec: EntitySpec) -> str:
    on = " AND ".join(f"t.{k} = {_cast(spec.col(k))}" for k in spec.key)
    u = _cast(spec.col("updated_at"))
    return f"""SELECT cls, late, COUNT(*) FROM (
      SELECT CASE WHEN t.{spec.key[0]} IS NULL THEN 'insert'
                  WHEN {_newer(spec)} THEN 'update'
                  WHEN {u} = t.updated_at AND s.row_hash = t.row_hash THEN 'unchanged'
                  ELSE 'stale' END AS cls,
             CASE WHEN CAST(? AS TIMESTAMPTZ) IS NOT NULL AND {u} < CAST(? AS TIMESTAMPTZ)
                  THEN 1 ELSE 0 END AS late
      FROM {spec.stage} s LEFT JOIN {spec.table} t ON {on}) x GROUP BY cls, late"""


@dataclass
class MergeStats:
    inserted: int = 0
    updated: int = 0
    unchanged: int = 0
    stale: int = 0
    late: int = 0  # applied rows whose event time was behind the entity watermark
    def as_dict(self) -> dict[str, int]:
        return dict(self.__dict__)


@dataclass
class Checkpoint:
    source_name: str
    file_sha256: str
    window_start: str
    window_end: str
    entity: str
    status: str  # loaded | rejected
    counts: dict[str, int] = field(default_factory=dict)
    processed_at: datetime | None = None


class SqlWarehouse:
    backend = "abstract"

    # --- subclass hooks -------------------------------------------------
    def _execute(self, sql: str, params: Sequence[Any] = ()) -> list[tuple]:
        raise NotImplementedError

    def _executemany(self, sql: str, rows: Sequence[Sequence[Any]]) -> None:
        raise NotImplementedError

    def _begin(self) -> None:
        self._execute("BEGIN")

    def _commit(self) -> None:
        self._execute("COMMIT")

    def _rollback(self) -> None:
        self._execute("ROLLBACK")

    def is_transient(self, exc: BaseException) -> bool:
        return isinstance(exc, (TransientError, TimeoutError, ConnectionError))

    def _no_active_transaction(self, exc: BaseException) -> bool:
        """True if a failed ROLLBACK only means 'there was no open transaction' (so nothing is left
        dangling). Backends override; the default is the conservative False."""
        return False

    def close(self) -> None:
        raise NotImplementedError

    # --- schema ---------------------------------------------------------
    def init_schema(self) -> None:
        for ddl in CONTROL_DDL:
            self._execute(ddl)
        for spec in (*ENTITIES.values(), QUARANTINE):
            self._execute(table_ddl(spec))
            self._execute(stage_ddl(spec))

    _unsafe: str | None = None  # set when a transaction may be left open; blocks further writes

    @contextmanager
    def transaction(self) -> Iterator[None]:
        """BEGIN ... COMMIT, with a best-effort ROLLBACK if the body *or the COMMIT itself* fails.

        Outcome of a failed COMMIT is, in general, unknowable (see README, "Ambiguous commits"): on a
        network backend the server may have committed before the error reached us. Callers must
        therefore make the whole unit of work retry-safe (the runner re-checks the checkpoint at the
        start of each attempt). If even the ROLLBACK fails we cannot tell whether a transaction is
        still open, so the connection is marked unsafe and every later transaction is refused rather
        than risk committing someone else's half-finished work."""
        if self._unsafe:
            raise PipelineError(f"connection state unknown after earlier failure ({self._unsafe}); "
                                "close it and rerun - nothing was checkpointed for unfinished files")
        self._begin()
        try:
            yield
            self._commit()
        except BaseException as exc:
            try:
                self._rollback()
            except Exception as rb:  # noqa: BLE001 - keep the original error
                if not self._no_active_transaction(rb):
                    self._unsafe = f"{type(exc).__name__} then rollback failed: {type(rb).__name__}"
            raise

    # --- loading --------------------------------------------------------
    def _stage(self, spec: EntitySpec, records: Iterable[dict]) -> int:
        self._execute(f"DELETE FROM {spec.stage}")
        names = [c.name for c in spec.columns]
        rows = [[stage_value(r.get(n)) for n in names] for r in records]
        if rows:
            self._executemany(f"INSERT INTO {spec.stage} ({', '.join(names)}) VALUES "
                              f"({', '.join('?' for _ in names)})", rows)
        return len(rows)

    def merge(self, spec: EntitySpec, records: list[dict]) -> MergeStats:
        """Stage + classify + MERGE. Keys in `records` must be unique (runner dedupes)."""
        stats = MergeStats()
        if not records:
            return stats
        self._stage(spec, records)
        wm = self.get_watermark(spec.name)
        wm_s = stage_value(wm)
        for cls, late, n in self._execute(classify_sql(spec), [wm_s, wm_s]):
            n = int(n)
            if cls == "insert":
                stats.inserted += n
            elif cls == "update":
                stats.updated += n
            elif cls == "unchanged":
                stats.unchanged += n
            else:
                stats.stale += n
            if cls in ("insert", "update") and int(late):
                stats.late += n
        self._execute(merge_sql(spec))
        return stats

    def add_quarantine(self, rows: list[dict]) -> None:
        if rows:
            self._stage(QUARANTINE, rows)
            self._execute(merge_sql(QUARANTINE))  # insert-if-absent: idempotent

    def get_watermark(self, entity: str) -> datetime | None:
        r = self._execute("SELECT CAST(max_updated_at AS VARCHAR) FROM watermarks WHERE entity = ?", [entity])
        return datetime.fromisoformat(r[0][0].replace(" ", "T", 1)) if r else None

    def advance_watermark(self, entity: str, ts: datetime) -> None:
        """Monotonic, informational high-water mark of applied event time. It is NEVER used to
        filter incoming rows, so late data cannot be dropped because of it."""
        self._execute(
            """MERGE INTO watermarks AS t USING (SELECT CAST(? AS VARCHAR) AS entity,
               CAST(? AS TIMESTAMPTZ) AS v) AS s ON t.entity = s.entity
               WHEN MATCHED AND s.v > t.max_updated_at THEN UPDATE SET max_updated_at = s.v
               WHEN NOT MATCHED THEN INSERT (entity, max_updated_at) VALUES (s.entity, s.v)""",
            [entity, stage_value(ts)])

    # --- checkpoints ----------------------------------------------------
    def has_checkpoint(self, source_name: str, sha: str, wstart: str, wend: str) -> bool:
        return bool(self._execute(
            "SELECT 1 FROM checkpoints WHERE source_name=? AND file_sha256=? AND window_start=? AND window_end=?",
            [source_name, sha, wstart, wend]))

    def checkpoint_counts(self, source_name: str, sha: str, wstart: str, wend: str) -> dict[str, int] | None:
        """Counts stored with an existing checkpoint, or None if there is none."""
        r = self._execute(f"SELECT {', '.join(self.COUNT_COLS)} FROM checkpoints WHERE source_name=? AND "
                          "file_sha256=? AND window_start=? AND window_end=?", [source_name, sha, wstart, wend])
        return {c: int(v or 0) for c, v in zip(self.COUNT_COLS, r[0])} if r else None

    COUNT_COLS = ("rows_read", "rows_inserted", "rows_updated", "rows_unchanged", "rows_stale", "rows_late",
                  "rows_quarantined", "rows_out_of_window", "rows_batch_duplicates")

    def put_checkpoint(self, cp: Checkpoint) -> None:
        cols = ["source_name", "file_sha256", "window_start", "window_end", "entity", "status",
                *self.COUNT_COLS, "processed_at"]
        vals = [cp.source_name, cp.file_sha256, cp.window_start, cp.window_end, cp.entity, cp.status,
                *[cp.counts.get(c, 0) for c in self.COUNT_COLS], stage_value(cp.processed_at)]
        ph = ", ".join("CAST(? AS TIMESTAMPTZ)" if c == "processed_at" else "?" for c in cols)
        self._execute(f"INSERT INTO checkpoints ({', '.join(cols)}) VALUES ({ph})", vals)

    # --- inspection -----------------------------------------------------
    def query(self, sql: str, params: Sequence[Any] = ()) -> list[tuple]:
        return self._execute(sql, params)

    def table_counts(self) -> dict[str, int]:
        return {t: int(self._execute(f"SELECT COUNT(*) FROM {t}")[0][0])
                for t in (*(s.table for s in ENTITIES.values()), "quarantine", "checkpoints")}

    def state_digest(self, include_checkpoints: bool = True) -> str:
        """SHA-256 over the logical state (data + quarantine + watermarks [+ checkpoint identity]),
        excluding wall-clock fields. Equal digest <=> equal warehouse state on this backend.
        include_checkpoints=False compares data only (e.g. windowed backfill vs single run)."""
        h = hashlib.sha256()
        for spec in (*ENTITIES.values(), QUARANTINE):
            cols = ", ".join(f"CAST({c.name} AS VARCHAR)" for c in spec.columns)
            order = ", ".join(spec.key)
            h.update(spec.table.encode())
            for row in self._execute(f"SELECT {cols} FROM {spec.table} ORDER BY {order}"):
                h.update(repr(tuple(row)).encode())
        h.update(b"watermarks")
        for row in self._execute("SELECT entity, CAST(max_updated_at AS VARCHAR) FROM watermarks ORDER BY entity"):
            h.update(repr(tuple(row)).encode())
        if not include_checkpoints:
            return h.hexdigest()
        h.update(b"checkpoints")
        for row in self._execute("SELECT source_name, file_sha256, window_start, window_end, entity, status "
                                 "FROM checkpoints ORDER BY source_name, file_sha256, window_start, window_end"):
            h.update(repr(tuple(row)).encode())
        return h.hexdigest()
