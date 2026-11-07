"""DuckDB backend (local, free, the default and the only integration-tested backend)."""
from __future__ import annotations

from pathlib import Path
from typing import Any, Sequence

import duckdb

from .base import SqlWarehouse


class DuckDBWarehouse(SqlWarehouse):
    backend = "duckdb"

    def __init__(self, path: str | Path = ":memory:"):
        if str(path) != ":memory:":
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = duckdb.connect(str(path))
        self.conn.execute("SET TimeZone='UTC'")
        self.init_schema()

    def _execute(self, sql: str, params: Sequence[Any] = ()) -> list[tuple]:
        cur = self.conn.execute(sql, list(params))
        try:
            return cur.fetchall()
        except duckdb.Error:  # statement returned no result set
            return []

    def _executemany(self, sql: str, rows: Sequence[Sequence[Any]]) -> None:
        self.conn.executemany(sql, [list(r) for r in rows])

    def is_transient(self, exc: BaseException) -> bool:
        return super().is_transient(exc) or isinstance(
            exc, (duckdb.IOException, duckdb.ConnectionException, duckdb.TransactionException))

    def _no_active_transaction(self, exc: BaseException) -> bool:
        return isinstance(exc, duckdb.TransactionException) and "no transaction is active" in str(exc).lower()

    def close(self) -> None:
        self.conn.close()
