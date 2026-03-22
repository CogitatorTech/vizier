-- End-to-end test: Complete capture → analyze → apply → benchmark loop
LOAD 'zig-out/lib/vizier.duckdb_extension';

-- ======================================
-- 1. Create realistic schema
-- ======================================
CREATE TABLE orders AS
  SELECT i AS id, i % 1000 AS customer_id, (random() * 500)::DOUBLE AS amount,
         '2025-01-01'::DATE + (i % 730)::INTEGER AS order_date,
         CASE WHEN i % 5 = 0 THEN 'shipped' WHEN i % 5 = 1 THEN 'pending' ELSE 'delivered' END AS status
  FROM range(50000) t(i);

CREATE TABLE customers AS
  SELECT i AS id, 'customer_' || i AS name,
         CASE WHEN i % 4 = 0 THEN 'US' WHEN i % 4 = 1 THEN 'EU' WHEN i % 4 = 2 THEN 'APAC' ELSE 'LATAM' END AS region
  FROM range(1000) t(i);

CREATE TABLE events AS
  SELECT i AS id, i % 200 AS account_id, NOW() - INTERVAL (i * 2) MINUTE AS ts,
         CASE WHEN i % 3 = 0 THEN 'click' WHEN i % 3 = 1 THEN 'view' ELSE 'purchase' END AS event_type
  FROM range(100000) t(i);

-- ======================================
-- 2. Capture a realistic workload
-- ======================================
-- Point lookups
SELECT * FROM vizier_capture('SELECT * FROM orders WHERE id = 42');
SELECT * FROM vizier_capture('SELECT * FROM customers WHERE id = 100');

-- Filtered scans
SELECT * FROM vizier_capture('SELECT * FROM orders WHERE customer_id = 42 AND order_date >= DATE ''2025-06-01''');
SELECT * FROM vizier_capture('SELECT * FROM orders WHERE customer_id = 99 AND order_date BETWEEN DATE ''2025-01-01'' AND DATE ''2025-12-31''');
SELECT * FROM vizier_capture('SELECT * FROM events WHERE account_id = 10 AND ts >= DATE ''2026-01-01''');
SELECT * FROM vizier_capture('SELECT * FROM events WHERE account_id = 20 AND ts BETWEEN DATE ''2025-06-01'' AND DATE ''2026-06-01''');

-- Joins
SELECT * FROM vizier_capture('SELECT o.id, c.name FROM orders o JOIN customers c ON o.customer_id = c.id WHERE c.region = ''US''');
SELECT * FROM vizier_capture('SELECT o.id, c.name FROM orders o JOIN customers c ON o.customer_id = c.id WHERE o.amount > 200');

-- Aggregations
SELECT * FROM vizier_capture('SELECT customer_id, count(*), sum(amount) FROM orders GROUP BY customer_id');
SELECT * FROM vizier_capture('SELECT customer_id, avg(amount) FROM orders GROUP BY customer_id HAVING count(*) > 5');
SELECT * FROM vizier_capture('SELECT region, count(*) FROM customers GROUP BY region');

-- Flush
SELECT * FROM vizier_flush();

-- ======================================
-- 3. Inspect physical design
-- ======================================
SELECT '--- Overview ---' AS section;
SELECT * FROM vizier.overview();

SELECT '--- Orders table inspection ---' AS section;
SELECT column_name, column_type, approx_rows, predicate_count, predicate_kinds
FROM vizier.inspect_table('orders');

SELECT '--- Workload summary ---' AS section;
SELECT * FROM vizier.workload_summary;

-- ======================================
-- 4. Analyze and review recommendations
-- ======================================
SELECT * FROM vizier_analyze();

SELECT '--- All recommendations ---' AS section;
SELECT recommendation_id, kind, table_name, round(score, 2) AS score, round(confidence, 2) AS confidence
FROM vizier.recommendations;

-- ======================================
-- 5. Dry-run apply the top recommendation (use literal ID 1)
-- ======================================
SELECT '--- Dry-run top recommendation ---' AS section;
SELECT * FROM vizier_apply(1, dry_run => true);

-- ======================================
-- 6. Apply and benchmark
-- ======================================
SELECT '--- Benchmark before apply ---' AS section;
SELECT * FROM vizier_benchmark('SELECT * FROM orders WHERE customer_id = 42', 5);

SELECT '--- Apply top recommendation ---' AS section;
SELECT * FROM vizier_apply(1);

SELECT '--- Benchmark after apply ---' AS section;
SELECT * FROM vizier_benchmark('SELECT * FROM orders WHERE customer_id = 42', 5);

-- ======================================
-- 7. Final state
-- ======================================
SELECT '--- Applied actions ---' AS section;
SELECT * FROM vizier.applied_actions;

SELECT '--- Recommendation status ---' AS section;
SELECT recommendation_id, kind, table_name, status FROM vizier.recommendation_store ORDER BY recommendation_id;
