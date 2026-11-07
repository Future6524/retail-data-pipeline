"""Optional Snowflake backend.

*** NOT INTEGRATION-TESTED. *** This adapter has never been run against a real Snowflake account
(none was available or used). It reuses the exact SQL (MERGE, CAST, qmark params, explicit
BEGIN/COMMIT/ROLLBACK) that is exercised on DuckDB, but dialect behaviour on Snowflake is unverified.
Unit tests only check SQL text generation and configuration handling; they do not prove cloud
behaviour. Review before relying on it, particularly:
  * MERGE source keys must be unique (the runner dedupes before staging);
  * temp staging tables are created outside transactions because Snowflake DDL implicitly commits;
  * TIMESTAMPTZ values are staged as UTC strings and CAST in SQL.

Credentials are read from environment variables ONLY when you explicitly select this backend:
SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA and either
SNOWFLAKE_PASSWORD or SNOWFLAKE_AUTHENTICATOR (e.g. externalbrowser); optional SNOWFLAKE_ROLE.
Install with: pip install '.[snowflake]'
"""
from __future__ import annotations

import os
from typing import Any, Mapping, Sequence

from ..errors import PipelineError, TransientError
from .base import SqlWarehouse

REQUIRED_ENV = ("SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_WAREHOUSE", "SNOWFLAKE_DATABASE",
                "SNOWFLAKE_SCHEMA")


def connection_params_from_env(env: Mapping[str, str] | None = None) -> dict[str, str]:
    env = os.environ if env is None else env
    missing = [k for k in REQUIRED_ENV if not env.get(k)]
    if missing:
        raise PipelineError(f"Snowflake backend selected but env vars missing: {', '.join(missing)}")
    if not (env.get("SNOWFLAKE_PASSWORD") or env.get("SNOWFLAKE_AUTHENTICATOR")):
        raise PipelineError("Set SNOWFLAKE_PASSWORD or SNOWFLAKE_AUTHENTICATOR")
    p = {k[len("SNOWFLAKE_"):].lower(): env[k] for k in
         (*REQUIRED_ENV, "SNOWFLAKE_PASSWORD", "SNOWFLAKE_AUTHENTICATOR", "SNOWFLAKE_ROLE") if env.get(k)}
    return p


class SnowflakeWarehouse(SqlWarehouse):
    backend = "snowflake"

    def __init__(self, params: dict[str, str] | None = None):
        try:
            import snowflake.connector as sf
        except ImportError as exc:
            raise PipelineError("snowflake-connector-python not installed (pip install '.[snowflake]')") from exc
        sf.paramstyle = "qmark"  # same ? placeholders as DuckDB; must be set before connect()
        self._sf = sf
        self.conn = sf.connect(**(params or connection_params_from_env()), autocommit=True)
        self._execute("ALTER SESSION SET TIMEZONE = 'UTC'")
        self.init_schema()

    def _execute(self, sql: str, params: Sequence[Any] = ()) -> list[tuple]:
        cur = self.conn.cursor()
        try:
            cur.execute(sql, list(params) if params else None)
            return list(cur.fetchall()) if cur.description else []
        finally:
            cur.close()

    def _executemany(self, sql: str, rows: Sequence[Sequence[Any]]) -> None:
        cur = self.conn.cursor()
        try:
            cur.executemany(sql, [list(r) for r in rows])  # bulk binding for INSERT ... VALUES
        finally:
            cur.close()

    def is_transient(self, exc: BaseException) -> bool:
        errs = self._sf.errors
        return isinstance(exc, (TransientError, errs.OperationalError, errs.InterfaceError)) or \
            isinstance(exc, (ConnectionError, TimeoutError))

    def close(self) -> None:
        self.conn.close()
