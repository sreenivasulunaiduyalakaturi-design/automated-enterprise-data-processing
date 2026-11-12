"""
Automated Enterprise Data Processing - base project
---------------------------------------------------
Pipeline: INGEST -> VALIDATE/CLEAN -> TRANSFORM -> LOAD (SQLite) -> REPORT -> ARCHIVE
Only the Python standard library is used (Python 3.8+).

Usage:
    python pipeline.py --demo             # create sample input files
    python pipeline.py                    # process everything in ./input once
    python pipeline.py --watch 30         # automate: check ./input every 30 sec
"""
import argparse
import csv
import json
import logging
import random
import shutil
import sqlite3
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path

BASE = Path(__file__).parent
INPUT_DIR, ARCHIVE_DIR = BASE / "input", BASE / "archive"
REJECT_DIR, REPORT_DIR = BASE / "rejected", BASE / "reports"
DB_PATH, LOG_PATH = BASE / "enterprise.db", BASE / "pipeline.log"
TAX_RATE = 0.18
REQUIRED = ["order_id", "customer", "region", "product", "quantity", "unit_price"]

for d in (INPUT_DIR, ARCHIVE_DIR, REJECT_DIR, REPORT_DIR):
    d.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(LOG_PATH), logging.StreamHandler()],
)
log = logging.getLogger("edp")


# ---------- 1. INGEST ----------
def ingest(path: Path):
    """Read a CSV or JSON file into a list of dicts."""
    if path.suffix.lower() == ".csv":
        with open(path, newline="", encoding="utf-8") as f:
            return list(csv.DictReader(f))
    if path.suffix.lower() == ".json":
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, list) else [data]
    raise ValueError(f"Unsupported file type: {path.suffix}")


# ---------- 2. VALIDATE & CLEAN ----------
def clean_record(rec: dict) -> dict:
    """Return a cleaned record or raise ValueError describing the problem."""
    rec = {k.strip().lower(): (v.strip() if isinstance(v, str) else v) for k, v in rec.items()}
    for field in REQUIRED:
        if rec.get(field) in (None, ""):
            raise ValueError(f"missing field '{field}'")
    try:
        qty = int(float(rec["quantity"]))
        price = float(rec["unit_price"])
    except (TypeError, ValueError):
        raise ValueError("quantity/unit_price not numeric")
    if qty <= 0 or price < 0:
        raise ValueError("quantity must be > 0 and price >= 0")
    return {
        "order_id": str(rec["order_id"]).upper(),
        "customer": rec["customer"].title(),
        "region": rec["region"].title(),
        "product": rec["product"].title(),
        "quantity": qty,
        "unit_price": round(price, 2),
    }


def validate(records):
    good, bad, seen = [], [], set()
    for i, rec in enumerate(records, start=1):
        try:
            c = clean_record(rec)
            if c["order_id"] in seen:
                raise ValueError("duplicate order_id")
            seen.add(c["order_id"])
            good.append(c)
        except ValueError as e:
            bad.append({**rec, "_row": i, "_error": str(e)})
    return good, bad


# ---------- 3. TRANSFORM ----------
def transform(records):
    ts = datetime.now().isoformat(timespec="seconds")
    for r in records:
        r["subtotal"] = round(r["quantity"] * r["unit_price"], 2)
        r["tax"] = round(r["subtotal"] * TAX_RATE, 2)
        r["total"] = round(r["subtotal"] + r["tax"], 2)
        r["processed_at"] = ts
    return records


# ---------- 4. LOAD ----------
def load(records, source):
    with sqlite3.connect(DB_PATH) as con:
        con.execute("""CREATE TABLE IF NOT EXISTS orders(
            order_id TEXT PRIMARY KEY, customer TEXT, region TEXT, product TEXT,
            quantity INTEGER, unit_price REAL, subtotal REAL, tax REAL, total REAL,
            processed_at TEXT, source_file TEXT)""")
        con.executemany(
            """INSERT OR REPLACE INTO orders VALUES
               (:order_id,:customer,:region,:product,:quantity,:unit_price,
                :subtotal,:tax,:total,:processed_at,:source_file)""",
            [{**r, "source_file": source} for r in records],
        )


# ---------- 5. REPORT ----------
def report():
    with sqlite3.connect(DB_PATH) as con:
        rows = con.execute(
            """SELECT region, COUNT(*), SUM(quantity), ROUND(SUM(total),2)
               FROM orders GROUP BY region ORDER BY SUM(total) DESC""").fetchall()
    out = REPORT_DIR / f"sales_by_region_{datetime.now():%Y%m%d_%H%M%S}.csv"
    with open(out, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["region", "orders", "units", "revenue"])
        w.writerows(rows)
    log.info("Report written: %s", out.name)
    return rows


# ---------- 6. ORCHESTRATION ----------
def process_file(path: Path):
    log.info("Processing %s", path.name)
    try:
        good, bad = validate(ingest(path))
    except Exception as e:
        log.error("Failed to read %s: %s", path.name, e)
        shutil.move(str(path), REJECT_DIR / path.name)
        return
    if bad:
        rej = REJECT_DIR / f"rejected_{path.stem}.json"
        rej.write_text(json.dumps(bad, indent=2))
        log.warning("%d bad rows -> %s", len(bad), rej.name)
    if good:
        load(transform(good), path.name)
    log.info("%s: %d loaded, %d rejected", path.name, len(good), len(bad))
    shutil.move(str(path), ARCHIVE_DIR / f"{datetime.now():%Y%m%d_%H%M%S}_{path.name}")


def run_once():
    files = sorted(p for p in INPUT_DIR.iterdir() if p.suffix.lower() in (".csv", ".json"))
    if not files:
        log.info("No new files.")
        return
    for f in files:
        process_file(f)
    for row in report():
        log.info("Region=%s orders=%s units=%s revenue=%s", *row)


def make_demo():
    regions, products = ["north", "south", "east", "west"], ["laptop", "monitor", "keyboard", "mouse"]
    rows = [{"order_id": f"ord{1000+i}", "customer": f"customer {i%7}",
             "region": random.choice(regions), "product": random.choice(products),
             "quantity": random.randint(1, 10), "unit_price": round(random.uniform(10, 900), 2)}
            for i in range(30)]
    rows.append({**rows[0]})                                   # duplicate
    rows.append({**rows[1], "order_id": "ORD9999", "quantity": "abc"})  # bad number
    rows.append({**rows[2], "order_id": "ORD9998", "region": ""})       # missing field
    with open(INPUT_DIR / "orders_demo.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader(); w.writerows(rows)
    (INPUT_DIR / "orders_demo.json").write_text(json.dumps([
        {"order_id": "ord5001", "customer": "acme corp", "region": "north",
         "product": "laptop", "quantity": 3, "unit_price": 750.5}]))
    log.info("Demo files created in %s", INPUT_DIR)


def main():
    ap = argparse.ArgumentParser(description="Automated Enterprise Data Processing")
    ap.add_argument("--demo", action="store_true", help="generate sample input files")
    ap.add_argument("--watch", type=int, metavar="SEC", help="re-run every SEC seconds")
    args = ap.parse_args()
    if args.demo:
        make_demo()
    if args.watch:
        log.info("Watching %s every %ss (Ctrl+C to stop)", INPUT_DIR, args.watch)
        while True:
            run_once()
            time.sleep(args.watch)
    else:
        run_once()


if __name__ == "__main__":
    main()
