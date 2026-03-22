-- Benchmark: Advisor throughput
-- Measures recommendation generation with varying workload sizes.
LOAD 'zig-out/lib/vizier.duckdb_extension';

-- Generate tables
CREATE TABLE t1 (id INTEGER, a INTEGER, b VARCHAR, c DOUBLE);
CREATE TABLE t2 (id INTEGER, x INTEGER, y VARCHAR, z DOUBLE);
CREATE TABLE t3 (id INTEGER, p INTEGER, q VARCHAR, r DOUBLE);

-- Small workload: 10 queries
SELECT * FROM vizier_capture('SELECT * FROM t1 WHERE a = 1');
SELECT * FROM vizier_capture('SELECT * FROM t1 WHERE a = 2 AND b = ''x''');
SELECT * FROM vizier_capture('SELECT * FROM t1 WHERE c > 100');
SELECT * FROM vizier_capture('SELECT * FROM t2 WHERE x = 1');
SELECT * FROM vizier_capture('SELECT * FROM t2 WHERE x = 2 AND y LIKE ''%test%''');
SELECT * FROM vizier_capture('SELECT * FROM t2 WHERE z BETWEEN 10 AND 50');
SELECT * FROM vizier_capture('SELECT * FROM t3 WHERE p = 1');
SELECT * FROM vizier_capture('SELECT * FROM t3 WHERE p IN (1, 2, 3)');
SELECT * FROM vizier_capture('SELECT t1.id FROM t1 JOIN t2 ON t1.id = t2.id WHERE t1.a > 5');
SELECT * FROM vizier_capture('SELECT p, count(*) FROM t3 GROUP BY p');
SELECT * FROM vizier_flush();

SELECT '=== Small workload: 10 queries ===' AS section;
SELECT * FROM vizier_analyze();

SELECT kind, count(*) AS cnt FROM vizier.recommendation_store GROUP BY kind ORDER BY cnt DESC;

-- Medium workload: add 20 more queries
SELECT * FROM vizier_capture('SELECT * FROM t1 WHERE a = 3 AND c < 50');
SELECT * FROM vizier_capture('SELECT * FROM t1 WHERE b = ''hello'' AND c >= 10');
SELECT * FROM vizier_capture('SELECT * FROM t2 WHERE x = 3');
SELECT * FROM vizier_capture('SELECT * FROM t2 WHERE z > 200');
SELECT * FROM vizier_capture('SELECT * FROM t3 WHERE q = ''val1''');
SELECT * FROM vizier_capture('SELECT * FROM t3 WHERE r BETWEEN 0 AND 100');
SELECT * FROM vizier_capture('SELECT a, count(*) FROM t1 GROUP BY a');
SELECT * FROM vizier_capture('SELECT a, avg(c) FROM t1 GROUP BY a');
SELECT * FROM vizier_capture('SELECT x, sum(z) FROM t2 GROUP BY x');
SELECT * FROM vizier_capture('SELECT p, max(r) FROM t3 GROUP BY p');
SELECT * FROM vizier_capture('SELECT t1.a, t2.x FROM t1 JOIN t2 ON t1.a = t2.x WHERE t1.c > 50');
SELECT * FROM vizier_capture('SELECT t2.x, t3.p FROM t2 JOIN t3 ON t2.id = t3.id WHERE t2.z > 100');
SELECT * FROM vizier_capture('SELECT * FROM t1 WHERE a >= 10 AND a <= 20');
SELECT * FROM vizier_capture('SELECT * FROM t2 WHERE x >= 5 AND y = ''specific''');
SELECT * FROM vizier_capture('SELECT * FROM t3 WHERE p = 10 AND q LIKE ''prefix%''');
SELECT * FROM vizier_capture('SELECT * FROM t1 WHERE id = 42');
SELECT * FROM vizier_capture('SELECT * FROM t2 WHERE id = 99');
SELECT * FROM vizier_capture('SELECT * FROM t3 WHERE id = 1');
SELECT * FROM vizier_capture('SELECT a, b, count(*) FROM t1 GROUP BY a, b');
SELECT * FROM vizier_capture('SELECT x, y, count(*) FROM t2 GROUP BY x, y');
SELECT * FROM vizier_flush();

SELECT '=== Medium workload: 30 queries ===' AS section;
SELECT * FROM vizier_analyze();

SELECT kind, count(*) AS cnt FROM vizier.recommendation_store GROUP BY kind ORDER BY cnt DESC;
