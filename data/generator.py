"""Deterministic SYNTHETIC retail batch generator (seeded; no wall-clock or environment input).

Writes 4 batches in mixed formats (CSV / JSONL / JSON) that deliberately exercise:
  batch1  clean baseline
  batch2  new rows, corrected orders, in-file duplicates, malformed rows
  batch3  LATE arrivals (event time before already-loaded data), STALE updates (older versions
          arriving after newer ones), newer corrections, a far-future timestamp
  batch4  one structurally corrupt JSON file (whole-file rejection)
and a manifest.json holding an INDEPENDENT oracle (final state computed here by construction,
not by the pipeline) so tests/demo can check the pipeline against ground truth.

Money is multi-currency (USD/EUR/GBP/CAD), so the oracle never adds amounts across currencies: order
totals are reported per currency. Inventory unit_cost and ticket refund_amount carry no currency in the
schema and are therefore deliberately not summed at all.
"""
from __future__ import annotations

import csv
import io
import json
import random
import re
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

MIN_ORDERS = 8      # the batch2/batch3 scenarios need 7 distinct orders to correct/cancel/replay (8 are sampled)
MAX_ORDERS = 100_000  # ids are ORD-%06d; also keeps the generator's memory use bounded
T0 = datetime(2026, 1, 10, tzinfo=timezone.utc)
H = timedelta(hours=1)
MIN = timedelta(minutes=1)
TZS = [timezone.utc, timezone(timedelta(hours=-5)), timezone(timedelta(hours=5, minutes=30)),
       timezone(timedelta(hours=1))]
COLS = {
    "orders": ["order_id", "customer_id", "status", "currency", "total_amount", "order_ts", "updated_at"],
    "inventory": ["sku", "warehouse_id", "quantity_on_hand", "unit_cost", "updated_at"],
    "support_tickets": ["ticket_id", "customer_id", "order_id", "status", "priority", "subject",
                        "refund_amount", "created_at", "updated_at"],
}
KEYS = {"orders": ("order_id",), "inventory": ("sku", "warehouse_id"), "support_tickets": ("ticket_id",)}
MONEY = {"total_amount", "unit_cost", "refund_amount"}
TIMES = {"order_ts", "updated_at", "created_at"}
SUBJECTS = ["Late delivery", "Damaged item", "Wrong size", "Refund request", "Missing parcel", "Billing question"]


class Raw(str):
    """Literal line/element inserted verbatim (used for hand-made malformed content)."""


def money(cents: int) -> Decimal:
    return Decimal(cents).scaleb(-2)


class _Gen:
    def __init__(self, seed: int):
        self.rng = random.Random(seed)
        self.truth: dict[str, dict[tuple, dict]] = {e: {} for e in COLS}
        self.counts = {"malformed": {}, "batch_duplicates": 0, "stale": {"orders": 0, "inventory": 0, "support_tickets": 0},
                       "late_orders": 0, "future_rows": 0}
        self.seq = {"order": 0, "ticket": 0}

    # -- truth ----------------------------------------------------------------
    def put(self, entity: str, rec: dict, *, truth: bool = True) -> dict:
        key = tuple(rec[k] for k in KEYS[entity])
        if truth:
            cur = self.truth[entity].get(key)
            assert cur is None or cur["updated_at"] != rec["updated_at"], "version collision"
            if cur is None or rec["updated_at"] > cur["updated_at"]:
                self.truth[entity][key] = rec
        return rec

    def bad(self, file: str, n: int = 1):
        self.counts["malformed"][file] = self.counts["malformed"].get(file, 0) + n

    # -- record builders -------------------------------------------------------
    def new_order(self, lo: datetime, hours: int, status=None, upd_off: int | None = None) -> dict:
        self.seq["order"] += 1
        ts = lo + timedelta(minutes=self.rng.randrange(hours * 60))
        return dict(order_id=f"ORD-{self.seq['order']:06d}", customer_id=f"CUST-{self.rng.randint(1, 30):04d}",
                    status=status or self.rng.choice(["placed", "paid"]), currency=self.rng.choice(["USD", "EUR", "GBP", "CAD"]),
                    total_amount=money(self.rng.randint(500, 50000)), order_ts=ts,
                    updated_at=ts + (upd_off or self.rng.randint(5, 30)) * MIN)

    def new_ticket(self, lo: datetime, orders: list[dict]) -> dict:
        self.seq["ticket"] += 1
        ts = lo + timedelta(minutes=self.rng.randrange(24 * 60))
        o = self.rng.choice(orders) if self.rng.random() < 0.8 else None
        return dict(ticket_id=f"TCK-{self.seq['ticket']:05d}", customer_id=o["customer_id"] if o else "CUST-0001",
                    order_id=o["order_id"] if o else None, status="open", priority=self.rng.choice(["low", "normal", "high", "urgent"]),
                    subject=self.rng.choice(SUBJECTS), refund_amount=None, created_at=ts, updated_at=ts)

    # -- serialization ------------------------------------------------------------
    def ser(self, entity: str, rec: dict, fmt: str) -> dict:
        out = {}
        for c in COLS[entity]:
            v = rec[c]
            if v is None:
                out[c] = "" if fmt == "csv" else None
            elif c in TIMES:
                out[c] = v.astimezone(self.rng.choice(TZS)).isoformat()
            elif c in MONEY:
                s = f"{v:.2f}"
                out[c] = f"@@NUM:{s}@@" if fmt in ("json", "jsonl") else s  # real JSON numbers, no floats
            else:
                out[c] = v
        return out


def _write(path: Path, entity: str, fmt: str, items: list) -> None:
    cols = COLS[entity]
    if fmt == "csv":
        buf = io.StringIO()
        w = csv.DictWriter(buf, fieldnames=cols, lineterminator="\n")
        w.writeheader()
        for it in items:
            if isinstance(it, Raw):
                buf.write(it + "\n")
            else:
                w.writerow(it)
        text = buf.getvalue()
    else:
        parts = [it if isinstance(it, Raw) else json.dumps(it) for it in items]
        if fmt == "corrupt":  # truncated JSON array: whole-file rejection
            text = "\n".join(parts) + "\n"
        else:
            text = "\n".join(parts) + "\n" if fmt == "jsonl" else "[\n" + ",\n".join(parts) + "\n]\n"
        text = re.sub(r'"@@NUM:([0-9.]+)@@"', r"\1", text)
    if path.exists():
        path.unlink()
    path.write_text(text, encoding="utf-8")
    path.chmod(0o444)  # immutable source: the pipeline must never write to it


def generate_demo_batches(out_dir: str | Path, seed: int = 42, n_orders: int = 40) -> dict:
    """Write the demo batches + manifest.json to out_dir; returns the manifest dict."""
    # Validate everything BEFORE touching the output directory (cleanup below deletes old batch files).
    if isinstance(n_orders, bool) or not isinstance(n_orders, int) or not MIN_ORDERS <= n_orders <= MAX_ORDERS:
        raise ValueError(f"n_orders must be an integer between {MIN_ORDERS} and {MAX_ORDERS} "
                         f"(the batch scenarios need at least {MIN_ORDERS} orders); got {n_orders!r}")
    if isinstance(seed, bool) or not isinstance(seed, int):
        raise ValueError(f"seed must be an integer; got {seed!r}")
    out = Path(out_dir)
    if out.exists() and not out.is_dir():
        raise ValueError(f"output path exists and is not a directory: {out}")
    out.mkdir(parents=True, exist_ok=True)
    for pat in ("orders_batch*", "inventory_batch*", "support_tickets_batch*", "manifest.json"):
        for old in out.glob(pat):  # only files this generator owns
            if old.is_file():
                old.unlink()
    g = _Gen(seed)
    r = g.rng
    S = g.ser
    files: dict[str, list] = {}

    # ---------------- batch 1: clean baseline ----------------
    o1 = [g.put("orders", g.new_order(T0, 24)) for _ in range(n_orders)]
    skus = [f"SKU-{i:03d}" for i in range(1, 13)]
    inv1 = [g.put("inventory", dict(sku=s, warehouse_id=w, quantity_on_hand=r.randint(0, 500), unit_cost=money(r.randint(100, 9000)),
                                     updated_at=T0 + 24 * H + r.randint(0, 59) * MIN + timedelta(seconds=i * 2 + j)))
            for i, s in enumerate(skus) for j, w in enumerate(["WH-EAST", "WH-WEST"])]
    t1 = [g.put("support_tickets", g.new_ticket(T0, o1)) for _ in range(10)]
    files["orders_batch1.csv"] = ("orders", "csv", [S("orders", x, "csv") for x in o1])
    files["inventory_batch1.jsonl"] = ("inventory", "jsonl", [S("inventory", x, "jsonl") for x in inv1])
    files["support_tickets_batch1.json"] = ("support_tickets", "json", [S("support_tickets", x, "json") for x in t1])

    # ---------------- batch 2: new + corrected + dups + malformed ----------------
    o2_new = [g.put("orders", g.new_order(T0 + 24 * H, 24)) for _ in range(20)]
    corrected = r.sample(o1, 8)  # first 6 corrected here; 2 more later; 4 of these get stale replays
    fixes2 = []
    for k, o in enumerate(corrected[:6]):
        n = dict(o, status="shipped", total_amount=o["total_amount"] + money(r.choice([0, 0, 250, -100])),
                 updated_at=T0 + 48 * H + (k + 1) * MIN)
        fixes2.append(g.put("orders", n))
    o2_items = [S("orders", x, "csv") for x in o2_new + fixes2]
    dups = r.sample(o2_items, 3)
    o2_items += [dict(d) for d in dups]  # exact in-file duplicates
    g.counts["batch_duplicates"] += 3
    f = "orders_batch2.csv"
    ex = o2_new[0]
    for mut in (dict(currency="XXX"), dict(total_amount="-5.00"), dict(total_amount="12.345"), dict(total_amount="abc"),
                dict(order_ts="2026-01-11 10:00:00"), dict(order_id=""), dict(status="teleported")):
        o2_items.append(dict(S("orders", dict(ex, order_id=f"ORD-BAD{len(o2_items):04d}"), "csv"), **mut))
        g.bad(f)
    o2_items.append(Raw("ORD-SHORT,CUST-0001,placed"))  # too few fields
    g.bad(f)
    files[f] = ("orders", "csv", o2_items)

    inv2 = []
    upd_inv = r.sample(sorted(g.truth["inventory"]), 10)
    for k, key in enumerate(upd_inv):
        base = g.truth["inventory"][key]
        inv2.append(g.put("inventory", dict(base, quantity_on_hand=r.randint(0, 500), updated_at=T0 + 48 * H + (k + 1) * MIN)))
    inv2_items = [S("inventory", x, "csv") for x in inv2]
    f = "inventory_batch2.csv"
    for mut in (dict(quantity_on_hand="-4"), dict(quantity_on_hand="3.5"), dict(unit_cost="1e2x")):
        inv2_items.append(dict(inv2_items[0], **mut))
        g.bad(f)
    files[f] = ("inventory", "csv", inv2_items)

    t2 = []
    for k, base in enumerate(r.sample(list(g.truth["support_tickets"].values()), 4)):
        t2.append(g.put("support_tickets", dict(base, status="resolved", refund_amount=money(r.randint(500, 5000)),
                                                updated_at=T0 + 48 * H + (k + 1) * MIN)))
    t2 += [g.put("support_tickets", g.new_ticket(T0 + 24 * H, o1 + o2_new)) for _ in range(3)]
    t2_items = [S("support_tickets", x, "jsonl") for x in t2]
    f = "support_tickets_batch2.jsonl"
    t2_items.append(Raw('{"ticket_id": "TCK-TRUNC", "customer_id": "CUST-0001", "sta'))  # truncated line
    t2_items.append(dict(t2_items[0], ticket_id="TCK-BADSTATUS", status="on_fire"))
    t2_items.append(dict(t2_items[0], ticket_id="TCK-NOSUBJ", subject=""))
    g.bad(f, 3)
    files[f] = ("support_tickets", "jsonl", t2_items)

    # ---------------- batch 3: late + stale + newer corrections ----------------
    late = [g.put("orders", g.new_order(T0, 24)) for _ in range(5)]  # event time in day 1, arrives after day 3 data
    g.counts["late_orders"] = len(late)
    stale = []
    for o in corrected[:4]:  # older version than the batch2 correction -> must lose
        stale.append(dict(o, status="paid", total_amount=o["total_amount"] + money(999), updated_at=o["order_ts"] + (60 + r.randint(0, 50)) * MIN))
        g.counts["stale"]["orders"] += 1
    newer = []
    for k, o in enumerate(corrected[4:6]):  # corrected again, newer than batch2
        newer.append(g.put("orders", dict(o, status="refunded", updated_at=T0 + 72 * H + (k + 1) * MIN)))
    cancelled = g.put("orders", dict(corrected[6], status="cancelled", updated_at=T0 + 72 * H + 10 * MIN))
    o3 = [S("orders", x, "jsonl") for x in late + stale + newer + [cancelled]]
    f = "orders_batch3.jsonl"
    o3.append(S("orders", dict(late[0], order_id="ORD-FUTURE", updated_at=datetime(2099, 1, 1, tzinfo=timezone.utc)), "jsonl"))
    g.bad(f)
    g.counts["future_rows"] += 1
    o3.append(Raw("{not json"))
    g.bad(f)
    files[f] = ("orders", "jsonl", o3)

    inv3 = []
    for key in upd_inv[:3]:  # stale: timestamp between batch1 and batch2 versions
        base = g.truth["inventory"][key]
        inv3.append(dict(base, quantity_on_hand=base["quantity_on_hand"] + 7, updated_at=T0 + 36 * H + len(inv3) * MIN))
        g.counts["stale"]["inventory"] += 1
    files["inventory_batch3.json"] = ("inventory", "json", [S("inventory", x, "json") for x in inv3] + [Raw('"stray string"')])
    g.bad("inventory_batch3.json")

    t3 = []
    for tk in t2[:2]:  # stale replay of the original 'open' state
        t3.append(dict(tk, status="open", refund_amount=None, updated_at=tk["created_at"] + 5 * MIN))
        g.counts["stale"]["support_tickets"] += 1
    files["support_tickets_batch3.csv"] = ("support_tickets", "csv", [S("support_tickets", x, "csv") for x in t3])

    # ---------------- batch 4: corrupt whole file ----------------
    files["support_tickets_batch4_corrupt.json"] = ("support_tickets", "corrupt", [Raw('[{"ticket_id": "TCK-X", ')])
    g.bad("support_tickets_batch4_corrupt.json")

    for name, (entity, fmt, items) in files.items():
        _write(out / name, entity, fmt, items)

    tr = g.truth
    manifest = {
        "synthetic": True, "seed": seed, "files": sorted(files),
        "injected": {"malformed_by_file": g.counts["malformed"], "malformed_total": sum(g.counts["malformed"].values()),
                     "in_file_duplicates": g.counts["batch_duplicates"], "stale_updates": g.counts["stale"],
                     "late_arriving_orders": g.counts["late_orders"], "future_dated_rows": g.counts["future_rows"]},
        "expected_final": {
            "orders": len(tr["orders"]), "inventory": len(tr["inventory"]), "support_tickets": len(tr["support_tickets"]),
            "orders_total_by_currency": _sum_by(tr["orders"].values(), "currency", "total_amount"),
            "orders_by_status": _count(x["status"] for x in tr["orders"].values()),
            "inventory_quantity_sum": sum(x["quantity_on_hand"] for x in tr["inventory"].values()),
            "inventory_quantity_by_warehouse": _sum_by(tr["inventory"].values(), "warehouse_id", "quantity_on_hand"),
            "support_tickets_by_status": _count(x["status"] for x in tr["support_tickets"].values()),
            "support_tickets_with_refund": sum(1 for x in tr["support_tickets"].values() if x["refund_amount"] is not None),
        },
    }
    mp = out / "manifest.json"
    mp.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    mp.chmod(0o444)
    return manifest


def _count(it) -> dict:
    d: dict = {}
    for x in it:
        d[x] = d.get(x, 0) + 1
    return dict(sorted(d.items()))


def _sum_by(rows, group: str, value: str) -> dict:
    d: dict = {}
    for x in rows:
        d[x[group]] = d.get(x[group], 0) + x[value]
    return {k: str(v) for k, v in sorted(d.items())}
