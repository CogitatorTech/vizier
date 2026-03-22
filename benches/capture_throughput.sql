-- Benchmark: Capture and flush throughput
-- Measures capture and flush operations at scale.
LOAD 'zig-out/lib/vizier.duckdb_extension';

-- ======================================
-- Capture: 50 individual queries
-- ======================================
SELECT '=== Capturing 50 queries ===' AS section;

SELECT * FROM vizier_capture('SELECT * FROM t WHERE a = 1');
SELECT * FROM vizier_capture('SELECT * FROM t WHERE a = 2');
SELECT * FROM vizier_capture('SELECT * FROM t WHERE a = 3');
SELECT * FROM vizier_capture('SELECT * FROM t WHERE b = ''x''');
SELECT * FROM vizier_capture('SELECT * FROM t WHERE b = ''y''');
SELECT * FROM vizier_capture('SELECT * FROM t WHERE c > 100');
SELECT * FROM vizier_capture('SELECT * FROM t WHERE c < 50');
SELECT * FROM vizier_capture('SELECT * FROM t WHERE d BETWEEN 1 AND 10');
SELECT * FROM vizier_capture('SELECT * FROM t WHERE e IN (1, 2, 3)');
SELECT * FROM vizier_capture('SELECT * FROM t WHERE f LIKE ''%test%''');
SELECT * FROM vizier_capture('SELECT * FROM u WHERE a = 1');
SELECT * FROM vizier_capture('SELECT * FROM u WHERE a = 2');
SELECT * FROM vizier_capture('SELECT * FROM u WHERE b = ''hello''');
SELECT * FROM vizier_capture('SELECT * FROM u WHERE c >= 10');
SELECT * FROM vizier_capture('SELECT * FROM u WHERE d <= 100');
SELECT * FROM vizier_capture('SELECT * FROM v WHERE x = 1');
SELECT * FROM vizier_capture('SELECT * FROM v WHERE x = 2');
SELECT * FROM vizier_capture('SELECT * FROM v WHERE y > 50');
SELECT * FROM vizier_capture('SELECT * FROM v WHERE z BETWEEN 0 AND 1');
SELECT * FROM vizier_capture('SELECT * FROM v WHERE w IN (''a'', ''b'')');
SELECT * FROM vizier_capture('SELECT a, count(*) FROM t GROUP BY a');
SELECT * FROM vizier_capture('SELECT b, sum(c) FROM t GROUP BY b');
SELECT * FROM vizier_capture('SELECT x, avg(y) FROM v GROUP BY x');
SELECT * FROM vizier_capture('SELECT a, b, count(*) FROM t GROUP BY a, b');
SELECT * FROM vizier_capture('SELECT t.a, u.b FROM t JOIN u ON t.id = u.id WHERE t.a > 5');
SELECT * FROM vizier_capture('SELECT u.a, v.x FROM u JOIN v ON u.id = v.id WHERE v.y > 10');
SELECT * FROM vizier_capture('SELECT * FROM t WHERE a = 4 AND c > 200');
SELECT * FROM vizier_capture('SELECT * FROM t WHERE a = 5 AND b = ''z''');
SELECT * FROM vizier_capture('SELECT * FROM u WHERE a = 3 AND b LIKE ''pre%''');
SELECT * FROM vizier_capture('SELECT * FROM u WHERE c > 50 AND d < 200');
SELECT * FROM vizier_capture('SELECT * FROM v WHERE x = 3 AND y >= 100');
SELECT * FROM vizier_capture('SELECT * FROM v WHERE x = 4 AND z > 0.5');
SELECT * FROM vizier_capture('SELECT * FROM t WHERE a = 6');
SELECT * FROM vizier_capture('SELECT * FROM t WHERE a = 7');
SELECT * FROM vizier_capture('SELECT * FROM t WHERE a = 8');
SELECT * FROM vizier_capture('SELECT * FROM u WHERE a = 4');
SELECT * FROM vizier_capture('SELECT * FROM u WHERE a = 5');
SELECT * FROM vizier_capture('SELECT * FROM u WHERE a = 6');
SELECT * FROM vizier_capture('SELECT * FROM v WHERE x = 5');
SELECT * FROM vizier_capture('SELECT * FROM v WHERE x = 6');
SELECT * FROM vizier_capture('SELECT * FROM t WHERE a = 1 AND b = ''x'' AND c > 0');
SELECT * FROM vizier_capture('SELECT * FROM t WHERE a = 2 AND c BETWEEN 10 AND 100');
SELECT * FROM vizier_capture('SELECT t.a FROM t JOIN u ON t.a = u.a JOIN v ON u.a = v.x WHERE t.c > 50');
SELECT * FROM vizier_capture('SELECT * FROM t ORDER BY a, b');
SELECT * FROM vizier_capture('SELECT * FROM u ORDER BY a DESC');
SELECT * FROM vizier_capture('SELECT * FROM v ORDER BY x, y');
SELECT * FROM vizier_capture('SELECT a, count(*) FROM t GROUP BY a HAVING count(*) > 2');
SELECT * FROM vizier_capture('SELECT x, count(*) FROM v GROUP BY x HAVING count(*) > 1');
SELECT * FROM vizier_capture('SELECT * FROM t WHERE a IS NULL');
SELECT * FROM vizier_capture('SELECT * FROM u WHERE b IS NOT NULL AND c > 0');

-- ======================================
-- Flush
-- ======================================
SELECT '=== Flushing 50 queries ===' AS section;
SELECT * FROM vizier_flush();

-- ======================================
-- Bulk capture
-- ======================================
CREATE TABLE bulk_log AS
  SELECT 'SELECT * FROM orders WHERE customer_id = ' || i::VARCHAR AS sql_text
  FROM range(500) t(i);

SELECT '=== Bulk capture (500 queries) ===' AS section;
SELECT * FROM vizier_capture_bulk('bulk_log', 'sql_text');
SELECT * FROM vizier_flush();

-- ======================================
-- Summary
-- ======================================
SELECT '=== Final state ===' AS section;
SELECT count(*) AS total_queries FROM vizier.workload_queries;
SELECT count(*) AS total_predicates FROM vizier.workload_predicates;
