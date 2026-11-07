"""Summarise a warehouse in the same shape as the generator manifest's `expected_final`, so the demo
and tests can compare ground truth with what was actually loaded. SQL only; no pipeline logic.

Order totals are summed PER CURRENCY: adding USD to EUR to GBP is not a meaningful number.
Inventory unit_cost and ticket refund_amount have no currency column, so they are not summed.
"""
from __future__ import annotations

from .warehouse.base import SqlWarehouse


def warehouse_summary(wh: SqlWarehouse) -> dict:
    q = wh.query
    return {
        "orders": int(q("SELECT COUNT(*) FROM orders")[0][0]),
        "inventory": int(q("SELECT COUNT(*) FROM inventory")[0][0]),
        "support_tickets": int(q("SELECT COUNT(*) FROM support_tickets")[0][0]),
        "orders_total_by_currency": {c: str(t) for c, t in q(
            "SELECT currency, SUM(total_amount) FROM orders GROUP BY currency ORDER BY currency")},
        "orders_by_status": {s: int(n) for s, n in q(
            "SELECT status, COUNT(*) FROM orders GROUP BY status ORDER BY status")},
        "inventory_quantity_sum": int(q("SELECT COALESCE(SUM(quantity_on_hand), 0) FROM inventory")[0][0]),
        "inventory_quantity_by_warehouse": {w: str(t) for w, t in q(
            "SELECT warehouse_id, SUM(quantity_on_hand) FROM inventory GROUP BY warehouse_id ORDER BY warehouse_id")},
        "support_tickets_by_status": {s: int(n) for s, n in q(
            "SELECT status, COUNT(*) FROM support_tickets GROUP BY status ORDER BY status")},
        "support_tickets_with_refund": int(q("SELECT COUNT(*) FROM support_tickets WHERE refund_amount IS NOT NULL")[0][0]),
    }
