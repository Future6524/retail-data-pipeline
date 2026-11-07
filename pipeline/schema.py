"""Canonical entity specs shared by validation and every warehouse backend."""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Column:
    name: str
    sql_type: str  # types valid in both DuckDB and Snowflake


@dataclass(frozen=True)
class EntitySpec:
    name: str
    table: str
    prefix: str  # source filename prefix
    key: tuple[str, ...]
    columns: tuple[Column, ...]  # all stored columns, incl. key, updated_at, row_hash, source_file
    versioned: bool = True  # upsert only if (updated_at, row_hash) is newer

    @property
    def stage(self) -> str:
        return f"stg_{self.table}"

    def col(self, name: str) -> Column:
        return next(c for c in self.columns if c.name == name)


V, TS, MONEY, INT = "VARCHAR", "TIMESTAMPTZ", "DECIMAL(18,2)", "BIGINT"
_INTERNAL = (Column("row_hash", V), Column("source_file", V))

ORDERS = EntitySpec(
    "orders", "orders", "orders", ("order_id",),
    (Column("order_id", V), Column("customer_id", V), Column("status", V), Column("currency", V),
     Column("total_amount", MONEY), Column("order_ts", TS), Column("updated_at", TS)) + _INTERNAL)

INVENTORY = EntitySpec(
    "inventory", "inventory", "inventory", ("sku", "warehouse_id"),
    (Column("sku", V), Column("warehouse_id", V), Column("quantity_on_hand", INT),
     Column("unit_cost", MONEY), Column("updated_at", TS)) + _INTERNAL)

SUPPORT_TICKETS = EntitySpec(
    "support_tickets", "support_tickets", "support_tickets", ("ticket_id",),
    (Column("ticket_id", V), Column("customer_id", V), Column("order_id", V), Column("status", V),
     Column("priority", V), Column("subject", V), Column("refund_amount", MONEY),
     Column("created_at", TS), Column("updated_at", TS)) + _INTERNAL)

QUARANTINE = EntitySpec(
    "quarantine", "quarantine", "", ("source_name", "file_sha256", "row_number"),
    (Column("source_name", V), Column("file_sha256", V), Column("row_number", INT),
     Column("entity", V), Column("reason", V), Column("detail", V), Column("raw_record", V)),
    versioned=False)

ENTITIES = {e.name: e for e in (ORDERS, INVENTORY, SUPPORT_TICKETS)}
