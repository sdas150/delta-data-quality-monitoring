"""Checks that the generated files really contain the faults that the manifest promises.
Independent plain-Python re-implementation of the rules; run:  python -m unittest discover -s tests -v
"""
import csv
import json
import re
import sys
import tempfile
import unittest
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import sample_data  # noqa: E402

EMAIL = re.compile(r"[^@ ]+@[^@ ]+[.][a-zA-Z]{2,}")
STATUSES = {"placed", "shipped", "delivered", "cancelled"}


def to_float(v):
    try:
        return float(v)
    except ValueError:
        return None


def evaluate(rows):
    """Return (rule_failures, error_rows) using the same rule definitions as the notebook."""
    fails = {k: 0 for k in ["required_order_id", "required_customer_id", "amount_range", "valid_status",
                            "valid_email", "no_future_date", "quantity_reasonable", "unique_order_id"]}
    error_rows = 0
    seen = set()
    for r in rows:
        flags = {}
        flags["required_order_id"] = not r["order_id"]
        flags["required_customer_id"] = not r["customer_id"]
        amt = to_float(r["amount"])
        flags["amount_range"] = not (amt is not None and 0 < amt <= 100000)
        flags["valid_status"] = r["status"] not in STATUSES
        flags["valid_email"] = EMAIL.fullmatch(r["email"]) is None
        flags["no_future_date"] = not r["order_date"] or date.fromisoformat(r["order_date"]) > date.today()
        qty = int(r["quantity"]) if r["quantity"] else None
        flags["quantity_reasonable"] = not (qty is not None and 1 <= qty <= 50)
        flags["unique_order_id"] = r["order_id"] in seen
        seen.add(r["order_id"])
        for k, v in flags.items():
            fails[k] += int(v)
        if any(v for k, v in flags.items() if k != "quantity_reasonable"):
            error_rows += 1
    return fails, error_rows


class SampleDataTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        cls.out = Path(cls.tmp.name)
        sample_data.main(cls.out)
        cls.manifest = json.loads((cls.out / "expected_failures.json").read_text())

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()

    def load(self, name):
        with (self.out / name).open(newline="", encoding="utf-8") as f:
            return list(csv.DictReader(f))

    def test_files_match_manifest(self):
        for name, exp in self.manifest.items():
            with self.subTest(file=name):
                rows = self.load(name)
                self.assertEqual(len(rows), exp["rows"])
                fails, error_rows = evaluate(rows)
                for rule, n in exp["rule_failures"].items():
                    self.assertEqual(fails[rule], n, rule)
                self.assertEqual(error_rows, exp["error_rows"])

    def test_day1_error_rate_stays_below_notebook_ceiling(self):
        exp = self.manifest["orders_day1.csv"]
        self.assertLess(exp["error_rows"] / exp["rows"], 0.10)

    def test_day2_triggers_volume_drop_but_not_absolute_row_range(self):
        d1, d2 = self.manifest["orders_day1.csv"]["rows"], self.manifest["orders_day2.csv"]["rows"]
        self.assertLess(d2 / d1, 0.5)      # volume_vs_previous_run must fail
        self.assertGreaterEqual(d2, 200)   # row_count_range (min 200) must still pass

    def test_generation_is_reproducible(self):
        with tempfile.TemporaryDirectory() as d2:
            sample_data.main(Path(d2))
            for name in ["orders_day1.csv", "orders_day2.csv"]:
                self.assertEqual((self.out / name).read_text(), (Path(d2) / name).read_text())


if __name__ == "__main__":
    unittest.main()
