"""A tiny data-quality framework for PySpark / Databricks.

Concepts
--------
RowRule     SQL expression that must be TRUE for a row to be good (NULL counts as a failure)
UniqueRule  the key columns must be unique; the 2nd, 3rd... occurrence of a key is the failing row
severity    "error" -> row is quarantined;  "warn" -> row is kept but the failure is reported
table checks  row count range, error-rate ceiling, and volume compared with the previous run

apply_rules() returns the dataframe annotated with which rules each row failed; split_good_bad()
separates it; summarize() and table_checks() produce result records for the monitoring table.
"""
import operator
from dataclasses import dataclass
from datetime import datetime
from functools import reduce
from typing import Dict, List, Optional, Sequence, Tuple

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

HELPER_PREFIXES = ("_fail_",)
HELPER_COLUMNS = {"_row_id", "_failed_rules", "_has_error"}

RESULTS_SCHEMA = (
    "run_id string, run_ts timestamp, dataset string, source_file string, check_name string, "
    "check_type string, severity string, rows_checked long, rows_failed long, pass_rate double, "
    "status string, detail string"
)


class DataQualityError(Exception):
    """Raised after results are recorded when a blocking table-level check failed."""


@dataclass(frozen=True)
class RowRule:
    name: str
    expr: str
    severity: str = "error"


@dataclass(frozen=True)
class UniqueRule:
    name: str
    columns: Tuple[str, ...]
    severity: str = "error"


def apply_rules(df: DataFrame, row_rules: Sequence[RowRule], unique_rules: Sequence[UniqueRule] = ()) -> DataFrame:
    names = [r.name for r in (*row_rules, *unique_rules)]
    if len(names) != len(set(names)):
        raise ValueError("rule names must be unique")

    work = df.withColumn("_row_id", F.monotonically_increasing_id())
    for r in row_rules:
        work = work.withColumn(f"_fail_{r.name}", ~F.coalesce(F.expr(r.expr), F.lit(False)))
    for u in unique_rules:
        w = Window.partitionBy(*[F.col(c) for c in u.columns]).orderBy("_row_id")
        work = work.withColumn(f"_fail_{u.name}", F.row_number().over(w) > 1)

    rules = [*row_rules, *unique_rules]
    failed_rules = F.concat_ws(",", *[F.when(F.col(f"_fail_{r.name}"), F.lit(r.name)) for r in rules])
    error_flags = [F.col(f"_fail_{r.name}") for r in rules if r.severity == "error"]
    has_error = reduce(operator.or_, error_flags, F.lit(False))
    return work.withColumn("_failed_rules", failed_rules).withColumn("_has_error", has_error)


def _data_columns(df: DataFrame) -> List[str]:
    return [c for c in df.columns if c not in HELPER_COLUMNS and not c.startswith(HELPER_PREFIXES)]


def split_good_bad(annotated: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """Return (good rows with original columns only, bad rows with `failed_rules` and `raw_record`)."""
    cols = _data_columns(annotated)
    good = annotated.where(~F.col("_has_error")).select(*cols)
    bad = annotated.where(F.col("_has_error")).select(
        F.col("_failed_rules").alias("failed_rules"),
        F.to_json(F.struct(*cols)).alias("raw_record"),
    )
    return good, bad


def summarize(annotated: DataFrame, row_rules: Sequence[RowRule], unique_rules: Sequence[UniqueRule] = ()) -> Dict:
    """One pass over the data: total rows, error rows and failures per rule."""
    rules = [*row_rules, *unique_rules]
    aggs = [F.count(F.lit(1)).alias("_total"),
            F.sum(F.col("_has_error").cast("long")).alias("_error_rows")]
    aggs += [F.sum(F.col(f"_fail_{r.name}").cast("long")).alias(r.name) for r in rules]
    row = annotated.agg(*aggs).collect()[0].asDict()
    return {
        "total": int(row["_total"] or 0),
        "error_rows": int(row["_error_rows"] or 0),
        "rule_failures": {r.name: int(row[r.name] or 0) for r in rules},
    }


def _status(failed: bool, severity: str) -> str:
    if not failed:
        return "PASS"
    return "FAIL" if severity == "error" else "WARN"


def rule_results(summary: Dict, row_rules: Sequence[RowRule], unique_rules: Sequence[UniqueRule]) -> List[Dict]:
    out = []
    total = summary["total"]
    for r in (*row_rules, *unique_rules):
        failed = summary["rule_failures"][r.name]
        out.append({
            "check_name": r.name,
            "check_type": "unique" if isinstance(r, UniqueRule) else "row",
            "severity": r.severity,
            "rows_checked": total,
            "rows_failed": failed,
            "pass_rate": (1 - failed / total) if total else None,
            "status": _status(failed > 0, r.severity),
            "detail": r.expr if isinstance(r, RowRule) else "unique(" + ",".join(r.columns) + ")",
        })
    return out


def table_checks(
    summary: Dict,
    previous_total: Optional[int],
    *,
    min_rows: int,
    max_rows: int,
    max_error_rate: float,
    min_ratio_vs_previous: float = 0.5,
) -> List[Dict]:
    total, errors = summary["total"], summary["error_rows"]
    out = []

    ok = min_rows <= total <= max_rows
    out.append({"check_name": "row_count_range", "check_type": "table", "severity": "error",
                "rows_checked": total, "rows_failed": 0 if ok else 1, "pass_rate": 1.0 if ok else 0.0,
                "status": _status(not ok, "error"), "detail": f"expected {min_rows}..{max_rows}, got {total}"})

    rate = errors / total if total else 1.0
    ok = rate <= max_error_rate
    out.append({"check_name": "max_error_rate", "check_type": "table", "severity": "error",
                "rows_checked": total, "rows_failed": errors, "pass_rate": 1 - rate,
                "status": _status(not ok, "error"),
                "detail": f"error rate {rate:.2%}, ceiling {max_error_rate:.0%}"})

    if previous_total:
        ratio = total / previous_total
        ok = ratio >= min_ratio_vs_previous
        out.append({"check_name": "volume_vs_previous_run", "check_type": "table", "severity": "error",
                    "rows_checked": total, "rows_failed": 0 if ok else 1, "pass_rate": 1.0 if ok else 0.0,
                    "status": _status(not ok, "error"),
                    "detail": f"{total} rows vs {previous_total} previous = {ratio:.0%} (minimum {min_ratio_vs_previous:.0%})"})
    return out


def to_results_df(spark: SparkSession, results: List[Dict], run_id: str, run_ts: datetime,
                  dataset: str, source_file: str) -> DataFrame:
    rows = [(run_id, run_ts, dataset, source_file, r["check_name"], r["check_type"], r["severity"],
             int(r["rows_checked"]), int(r["rows_failed"]),
             None if r["pass_rate"] is None else float(r["pass_rate"]), r["status"], r["detail"])
            for r in results]
    return spark.createDataFrame(rows, RESULTS_SCHEMA)
