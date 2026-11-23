-- Pipeline outcome report. Replace main.dq_demo with your catalog.schema.

-- 1. One line per run: did it pass, and what failed?
SELECT run_id,
       source_file,
       MIN(run_ts)                                              AS run_ts,
       MAX(CASE WHEN check_name = 'row_count_range' THEN rows_checked END) AS rows_in,
       SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END)         AS failed_checks,
       SUM(CASE WHEN status = 'WARN' THEN 1 ELSE 0 END)         AS warnings,
       CASE WHEN SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) = 0 THEN 'HEALTHY' ELSE 'BLOCKED' END AS outcome,
       concat_ws(', ', collect_set(CASE WHEN status = 'FAIL' THEN check_name END)) AS failing_checks
FROM main.dq_demo.dq_run_results
GROUP BY run_id, source_file
ORDER BY run_ts DESC;

-- 2. Which rules fail most, across all runs?
SELECT check_name, check_type, severity,
       SUM(rows_failed)                       AS total_rows_failed,
       COUNT(*)                               AS runs_checked,
       ROUND(AVG(pass_rate) * 100, 2)         AS avg_pass_rate_pct
FROM main.dq_demo.dq_run_results
GROUP BY check_name, check_type, severity
HAVING SUM(rows_failed) > 0
ORDER BY total_rows_failed DESC;

-- 3. Pass-rate trend per rule (for a dashboard line chart)
SELECT date_trunc('day', run_ts) AS day, check_name, ROUND(AVG(pass_rate) * 100, 2) AS pass_rate_pct
FROM main.dq_demo.dq_run_results
WHERE check_type IN ('row', 'unique')
GROUP BY 1, 2
ORDER BY 1, 2;

-- 4. Quarantine breakdown: what kinds of bad rows do we get?
SELECT source_file, failed_rules, COUNT(*) AS rows
FROM main.dq_demo.quarantine_orders
GROUP BY source_file, failed_rules
ORDER BY rows DESC;

-- 5. TROUBLESHOOTING: show actual bad records for one rule
SELECT source_file,
       failed_rules,
       get_json_object(raw_record, '$.order_id')    AS order_id,
       get_json_object(raw_record, '$.customer_id') AS customer_id,
       get_json_object(raw_record, '$.email')       AS email,
       get_json_object(raw_record, '$.amount')      AS amount,
       get_json_object(raw_record, '$.status')      AS status
FROM main.dq_demo.quarantine_orders
WHERE failed_rules LIKE '%valid_email%'
LIMIT 25;

-- 6. Volume monitor: rows per run, with change vs previous run
SELECT source_file, run_ts, rows_checked AS rows_in,
       LAG(rows_checked) OVER (ORDER BY run_ts) AS previous_rows,
       ROUND(rows_checked / LAG(rows_checked) OVER (ORDER BY run_ts) * 100, 0) AS pct_of_previous
FROM main.dq_demo.dq_run_results
WHERE check_name = 'row_count_range'
ORDER BY run_ts;

-- 7. Reconciliation: every input row is either in Silver or in quarantine (duplicates are the only expected gap)
SELECT
  (SELECT MAX(rows_checked) FROM main.dq_demo.dq_run_results
     WHERE check_name = 'row_count_range' AND source_file = 'orders_day1.csv') AS rows_in,
  (SELECT COUNT(*) FROM main.dq_demo.silver_orders WHERE order_id < 2000)      AS rows_in_silver,
  (SELECT COUNT(*) FROM main.dq_demo.quarantine_orders WHERE source_file = 'orders_day1.csv') AS rows_quarantined;
