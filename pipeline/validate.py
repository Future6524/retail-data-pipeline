"""Validation + cleaning into canonical records (strings for keys/enums, Decimal money,
UTC tz-aware datetimes). Invalid input is never repaired silently: it is rejected with a reason."""
from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, cast

from .schema import EntitySpec

CURRENCIES = {"USD", "EUR", "GBP", "CAD"}
ORDER_STATUSES = {"placed", "paid", "shipped", "delivered", "cancelled", "refunded"}
TICKET_STATUSES = {"open", "pending", "resolved", "closed"}
PRIORITIES = {"low", "normal", "high", "urgent"}
MIN_TS = datetime(2000, 1, 1, tzinfo=timezone.utc)
MAX_MONEY = Decimal("1000000000")
# Plain ASCII decimal text only ("12", "-3.50"). Decimal() alone also accepts "1_0", non-ASCII digits
# (Arabic-Indic, fullwidth), "1.", ".5", exponents and "Infinity"/"NaN"; none belong in a CSV money field.
# (JSON numbers arrive as Decimal objects, not strings, and skip this check.)
_PLAIN_NUMBER = re.compile(r"[+-]?\d+(?:\.\d+)?", re.ASCII)


class Invalid(Exception):
    def __init__(self, reason: str, detail: str = ""):
        super().__init__(reason)
        self.reason, self.detail = reason, detail


def _text(v: Any, field: str, *, required=True, maxlen=200) -> str | None:
    if v is None or (isinstance(v, str) and not v.strip()):
        if required:
            raise Invalid("missing_required", field)
        return None
    if not isinstance(v, str):
        raise Invalid("wrong_type", f"{field}: expected string")
    v = v.strip()
    if len(v) > maxlen:
        raise Invalid("too_long", field)
    if "\x00" in v or any("\ud800" <= ch <= "\udfff" for ch in v):
        # NUL breaks most SQL engines; a lone surrogate cannot be encoded as UTF-8 and would otherwise
        # fail the whole file's transaction at load time.
        raise Invalid("invalid_characters", field)
    return v


def _enum(v: Any, field: str, allowed: set[str]) -> str:
    s = cast(str, _text(v, field))
    s = s.lower() if field != "currency" else s.upper()
    if s not in allowed:
        raise Invalid("invalid_value", f"{field}={s!r}")
    return s


def _to_decimal(v: Any, field: str, reason: str) -> Decimal:
    if isinstance(v, str):
        v = v.strip()
        if not _PLAIN_NUMBER.fullmatch(v):
            raise Invalid(reason, f"{field}={v[:40]!r}")
    try:
        return Decimal(v)
    except InvalidOperation:
        raise Invalid(reason, f"{field}={str(v)[:40]!r}") from None


def _money(v: Any, field: str, *, required=True) -> Decimal | None:
    if v is None or (isinstance(v, str) and not v.strip()):
        if required:
            raise Invalid("missing_required", field)
        return None
    if isinstance(v, bool) or not isinstance(v, (str, int, Decimal)):
        raise Invalid("wrong_type", f"{field}: money must be string/int/decimal")
    d = _to_decimal(v, field, "invalid_money")
    if not d.is_finite():
        raise Invalid("invalid_money", f"{field}={v!r}")
    if d < 0:
        raise Invalid("negative_money", f"{field}={d}")
    if d >= MAX_MONEY:
        raise Invalid("money_out_of_range", f"{field}={d}")
    if d != d.quantize(Decimal("0.01")):
        raise Invalid("money_precision", f"{field}={str(d)[:40]} has more than 2 decimal places")
    return Decimal("0.00") if d == 0 else d.quantize(Decimal("0.01"))  # "-0.00" -> "0.00"


def _int(v: Any, field: str) -> int:
    if v is None or (isinstance(v, str) and not v.strip()):
        raise Invalid("missing_required", field)
    if isinstance(v, bool) or not isinstance(v, (str, int, Decimal)):
        raise Invalid("wrong_type", field)
    d = _to_decimal(v, field, "invalid_integer")
    if not d.is_finite() or d != d.to_integral_value():
        raise Invalid("invalid_integer", f"{field}={v!r}")
    if d < 0:
        raise Invalid("negative_quantity", f"{field}={d}")
    if d > 10**9:
        raise Invalid("quantity_out_of_range", field)
    return int(d)


def _ts(v: Any, field: str, *, required=True) -> datetime | None:
    if v is None or (isinstance(v, str) and not v.strip()):
        if required:
            raise Invalid("missing_required", field)
        return None
    if not isinstance(v, str):
        raise Invalid("wrong_type", f"{field}: timestamp must be ISO-8601 string")
    try:
        dt = datetime.fromisoformat(v.strip())
    except ValueError:
        raise Invalid("invalid_timestamp", f"{field}={v!r}") from None
    if dt.tzinfo is None or dt.utcoffset() is None:
        raise Invalid("naive_timestamp", f"{field}={v!r} has no UTC offset")
    try:
        dt = dt.astimezone(timezone.utc)
    except OverflowError:  # e.g. 0001-01-01T00:00:00+23:59 or 9999-12-31T23:59:59-23:59
        raise Invalid("timestamp_out_of_range", f"{field}={v[:40]!r}") from None
    if dt < MIN_TS:
        raise Invalid("timestamp_out_of_range", f"{field}={v[:40]!r}")
    return dt


def _orders(d: dict) -> dict:
    r: dict[str, Any] = {
        "order_id": _text(d.get("order_id"), "order_id", maxlen=64),
        "customer_id": _text(d.get("customer_id"), "customer_id", maxlen=64),
        "status": _enum(d.get("status"), "status", ORDER_STATUSES),
        "currency": _enum(d.get("currency"), "currency", CURRENCIES),
        "total_amount": _money(d.get("total_amount"), "total_amount"),
        "order_ts": _ts(d.get("order_ts"), "order_ts"),
        "updated_at": _ts(d.get("updated_at"), "updated_at"),
    }
    if r["updated_at"] < r["order_ts"]:
        raise Invalid("updated_before_created", "updated_at < order_ts")
    return r


def _inventory(d: dict) -> dict:
    return {
        "sku": _text(d.get("sku"), "sku", maxlen=64),
        "warehouse_id": _text(d.get("warehouse_id"), "warehouse_id", maxlen=64),
        "quantity_on_hand": _int(d.get("quantity_on_hand"), "quantity_on_hand"),
        "unit_cost": _money(d.get("unit_cost"), "unit_cost"),
        "updated_at": _ts(d.get("updated_at"), "updated_at"),
    }


def _tickets(d: dict) -> dict:
    r: dict[str, Any] = {
        "ticket_id": _text(d.get("ticket_id"), "ticket_id", maxlen=64),
        "customer_id": _text(d.get("customer_id"), "customer_id", maxlen=64),
        "order_id": _text(d.get("order_id"), "order_id", required=False, maxlen=64),
        "status": _enum(d.get("status"), "status", TICKET_STATUSES),
        "priority": _enum(d.get("priority"), "priority", PRIORITIES),
        "subject": _text(d.get("subject"), "subject"),
        "refund_amount": _money(d.get("refund_amount"), "refund_amount", required=False),
        "created_at": _ts(d.get("created_at"), "created_at"),
        "updated_at": _ts(d.get("updated_at"), "updated_at"),
    }
    if r["updated_at"] < r["created_at"]:
        raise Invalid("updated_before_created", "updated_at < created_at")
    return r


_VALIDATORS: dict[str, Callable[[dict], dict]] = {
    "orders": _orders, "inventory": _inventory, "support_tickets": _tickets}


def row_hash(rec: dict) -> str:
    payload = json.dumps({k: (v.isoformat() if isinstance(v, datetime) else str(v) if v is not None else None)
                          for k, v in sorted(rec.items())}, sort_keys=True)
    return hashlib.sha256(payload.encode()).hexdigest()


def validate(spec: EntitySpec, data: dict) -> dict:
    """Return canonical record (with row_hash) or raise Invalid."""
    rec = _VALIDATORS[spec.name](data)
    rec["row_hash"] = row_hash(rec)
    return rec


def stage_value(v: Any) -> str | None:
    """Canonical string form used for warehouse staging (identical for every backend)."""
    if v is None:
        return None
    if isinstance(v, datetime):
        return v.astimezone(timezone.utc).isoformat(sep=" ", timespec="microseconds")
    return str(v)
