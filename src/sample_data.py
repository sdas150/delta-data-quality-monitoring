"""Generate two daily order files with a KNOWN number of injected data problems.

orders_day1.csv   1,010 rows (1,000 + 10 duplicate rows) with faults injected into disjoint rows
orders_day2.csv   300 rows  -> a sudden volume drop (the table-level 'volume vs previous run' check should fail)
expected_failures.json   how many rows each rule SHOULD flag, so the pipeline can prove it catches everything

Usage:  python src/sample_data.py --out data
Standard library only.
"""
import argparse
import csv
import json
import random
from datetime import date, timedelta
from pathlib import Path

COLUMNS = ["order_id", "customer_id", "email", "amount", "quantity", "order_date", "status"]
GOOD_STATUSES = ["placed", "shipped", "delivered", "cancelled"]

# how many rows get each fault in day 1 (each fault goes into its own rows, so counts are exact)
FAULTS_DAY1 = {
    "required_customer_id": 12,   # missing customer_id
    "amount_negative": 8,         # counted under amount_range
    "amount_too_high": 5,         # counted under amount_range
    "valid_status": 7,
    "valid_email": 6,
    "no_future_date": 4,
    "quantity_reasonable": 9,     # WARN severity: row is kept, but reported
    "unique_order_id": 10,        # duplicated rows appended at the end
}


def clean_row(order_id: int, rnd: random.Random) -> dict:
    cust = rnd.randint(1, 300)
    return {
        "order_id": order_id,
        "customer_id": cust,
        "email": f"user{cust}@example.com",
        "amount": round(rnd.uniform(5, 500), 2),
        "quantity": rnd.randint(1, 5),
        "order_date": (date(2024, 1, 1) + timedelta(days=rnd.randint(0, 300))).isoformat(),
        "status": rnd.choice(GOOD_STATUSES),
    }


def write_orders(path: Path, rows: list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=COLUMNS)
        w.writeheader()
        w.writerows(rows)
    print(f"wrote {len(rows):>5} rows -> {path}")


def build_day1(rnd: random.Random):
    rows = [clean_row(i, rnd) for i in range(1, 1001)]
    needed = sum(FAULTS_DAY1.values())
    picks = rnd.sample(range(len(rows)), needed)
    it = iter(picks)

    def take(n):
        return [next(it) for _ in range(n)]

    for i in take(FAULTS_DAY1["required_customer_id"]):
        rows[i]["customer_id"] = ""
    for i in take(FAULTS_DAY1["amount_negative"]):
        rows[i]["amount"] = -abs(rows[i]["amount"])
    for i in take(FAULTS_DAY1["amount_too_high"]):
        rows[i]["amount"] = 250000.0
    for i in take(FAULTS_DAY1["valid_status"]):
        rows[i]["status"] = rnd.choice(["unknown", "PENDING!", "", "n/a"])
    for i in take(FAULTS_DAY1["valid_email"]):
        rows[i]["email"] = rnd.choice(["not-an-email", "user@", "@example.com", "a b@c.com"])
    for i in take(FAULTS_DAY1["no_future_date"]):
        rows[i]["order_date"] = "2035-01-01"
    for i in take(FAULTS_DAY1["quantity_reasonable"]):
        rows[i]["quantity"] = 0
    # duplicates: exact copies of otherwise-clean rows appended at the end
    for i in take(FAULTS_DAY1["unique_order_id"]):
        rows.append(dict(rows[i]))
    return rows


def build_day2(rnd: random.Random):
    rows = [clean_row(i, rnd) for i in range(2001, 2301)]
    for i in rnd.sample(range(len(rows)), 3):
        rows[i]["email"] = "not-an-email"
    return rows


def expected_manifest() -> dict:
    d1 = FAULTS_DAY1
    amount = d1["amount_negative"] + d1["amount_too_high"]
    day1_rule_counts = {
        "required_customer_id": d1["required_customer_id"],
        "amount_range": amount,
        "valid_status": d1["valid_status"],
        "valid_email": d1["valid_email"],
        "no_future_date": d1["no_future_date"],
        "quantity_reasonable": d1["quantity_reasonable"],
        "unique_order_id": d1["unique_order_id"],
        "required_order_id": 0,
    }
    error_rows_day1 = sum(v for k, v in day1_rule_counts.items() if k != "quantity_reasonable")
    return {
        "orders_day1.csv": {"rows": 1010, "rule_failures": day1_rule_counts, "error_rows": error_rows_day1},
        "orders_day2.csv": {
            "rows": 300,
            "rule_failures": {"valid_email": 3, "required_customer_id": 0, "amount_range": 0, "valid_status": 0,
                              "no_future_date": 0, "quantity_reasonable": 0, "unique_order_id": 0,
                              "required_order_id": 0},
            "error_rows": 3,
        },
    }


def main(out: Path, seed: int = 7) -> None:
    rnd = random.Random(seed)
    write_orders(out / "orders_day1.csv", build_day1(rnd))
    write_orders(out / "orders_day2.csv", build_day2(rnd))
    (out / "expected_failures.json").write_text(json.dumps(expected_manifest(), indent=2))
    print(f"wrote manifest -> {out / 'expected_failures.json'}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", type=Path, default=Path("data"))
    ap.add_argument("--seed", type=int, default=7)
    a = ap.parse_args()
    main(a.out, a.seed)
