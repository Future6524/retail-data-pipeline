from __future__ import annotations

from pathlib import Path

from .base import SqlWarehouse


def open_warehouse(backend: str, db_path: str | Path = ":memory:") -> SqlWarehouse:
    if backend == "duckdb":
        from .duckdb_wh import DuckDBWarehouse
        return DuckDBWarehouse(db_path)
    if backend == "snowflake":
        from .snowflake_wh import SnowflakeWarehouse
        return SnowflakeWarehouse()
    raise ValueError(f"unknown backend {backend!r}")
