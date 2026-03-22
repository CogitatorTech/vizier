-- Benchmark: TPC-H-like workload analysis
-- Generates TPC-H-style tables, captures a realistic analytical workload,
-- runs all advisors, and reports recommendations with timing.
LOAD 'zig-out/lib/vizier.duckdb_extension';

-- ======================================
-- Data generation
-- ======================================
CREATE TABLE lineitem AS
  SELECT i AS l_orderkey, i % 200000 AS l_partkey, i % 10000 AS l_suppkey,
         (i % 7 + 1)::INTEGER AS l_quantity,
         (random() * 50000)::DOUBLE AS l_extendedprice,
         (random() * 0.1)::DOUBLE AS l_discount,
         (random() * 0.08)::DOUBLE AS l_tax,
         CASE WHEN i % 4 = 0 THEN 'A' WHEN i % 4 = 1 THEN 'R' WHEN i % 4 = 2 THEN 'N' ELSE 'O' END AS l_returnflag,
         CASE WHEN i % 2 = 0 THEN 'F' ELSE 'O' END AS l_linestatus,
         '1992-01-01'::DATE + (i % 2556)::INTEGER AS l_shipdate,
         '1992-01-01'::DATE + (i % 2556 + 30)::INTEGER AS l_commitdate
  FROM range(500000) t(i);

CREATE TABLE orders AS
  SELECT i AS o_orderkey, i % 150000 AS o_custkey,
         (random() * 500000)::DOUBLE AS o_totalprice,
         '1992-01-01'::DATE + (i % 2556)::INTEGER AS o_orderdate,
         CASE WHEN i % 5 = 0 THEN '1-URGENT' WHEN i % 5 = 1 THEN '2-HIGH' WHEN i % 5 = 2 THEN '3-MEDIUM' WHEN i % 5 = 3 THEN '4-NOT SPECIFIED' ELSE '5-LOW' END AS o_orderpriority,
         CASE WHEN i % 3 = 0 THEN 'F' WHEN i % 3 = 1 THEN 'O' ELSE 'P' END AS o_orderstatus
  FROM range(150000) t(i);

CREATE TABLE customer AS
  SELECT i AS c_custkey, 'Customer#' || lpad(i::VARCHAR, 9, '0') AS c_name,
         CASE WHEN i % 25 = 0 THEN 'BUILDING' WHEN i % 25 < 5 THEN 'AUTOMOBILE' WHEN i % 25 < 10 THEN 'MACHINERY' WHEN i % 25 < 15 THEN 'HOUSEHOLD' ELSE 'FURNITURE' END AS c_mktsegment,
         i % 25 AS c_nationkey
  FROM range(150000) t(i);

SELECT '=== Data generated ===' AS status,
       (SELECT count(*) FROM lineitem) AS lineitem_rows,
       (SELECT count(*) FROM orders) AS orders_rows,
       (SELECT count(*) FROM customer) AS customer_rows;

-- ======================================
-- Capture TPC-H-like queries
-- ======================================

-- Q1: Pricing summary (aggregation heavy)
SELECT * FROM vizier_capture('SELECT l_returnflag, l_linestatus, sum(l_quantity) AS sum_qty, sum(l_extendedprice) AS sum_base_price, sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price, avg(l_quantity) AS avg_qty, avg(l_extendedprice) AS avg_price, avg(l_discount) AS avg_disc, count(*) AS count_order FROM lineitem WHERE l_shipdate <= DATE ''1998-12-01'' GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus');

-- Q3: Shipping priority (join + filter + aggregation)
SELECT * FROM vizier_capture('SELECT l_orderkey, sum(l_extendedprice * (1 - l_discount)) AS revenue, o_orderdate FROM customer c JOIN orders o ON c.c_custkey = o.o_custkey JOIN lineitem l ON l.l_orderkey = o.o_orderkey WHERE c.c_mktsegment = ''BUILDING'' AND o.o_orderdate < DATE ''1995-03-15'' AND l.l_shipdate > DATE ''1995-03-15'' GROUP BY l_orderkey, o_orderdate ORDER BY revenue DESC');

-- Q4: Order priority checking (exists subquery pattern)
SELECT * FROM vizier_capture('SELECT o_orderpriority, count(*) AS order_count FROM orders WHERE o_orderdate >= DATE ''1993-07-01'' AND o_orderdate < DATE ''1993-10-01'' GROUP BY o_orderpriority ORDER BY o_orderpriority');

-- Q6: Forecasting revenue (scan + filter)
SELECT * FROM vizier_capture('SELECT sum(l_extendedprice * l_discount) AS revenue FROM lineitem WHERE l_shipdate >= DATE ''1994-01-01'' AND l_shipdate < DATE ''1995-01-01'' AND l_discount BETWEEN 0.05 AND 0.07 AND l_quantity < 24');

-- Q10: Returned item reporting (join heavy)
SELECT * FROM vizier_capture('SELECT c.c_custkey, c.c_name, sum(l_extendedprice * (1 - l_discount)) AS revenue FROM customer c JOIN orders o ON c.c_custkey = o.o_custkey JOIN lineitem l ON l.l_orderkey = o.o_orderkey WHERE o.o_orderdate >= DATE ''1993-10-01'' AND o.o_orderdate < DATE ''1994-01-01'' AND l.l_returnflag = ''R'' GROUP BY c.c_custkey, c.c_name ORDER BY revenue DESC');

-- Q12: Shipping modes and order priority
SELECT * FROM vizier_capture('SELECT l_shipdate, count(*) FROM lineitem WHERE l_shipdate >= DATE ''1994-01-01'' AND l_shipdate < DATE ''1995-01-01'' AND l_commitdate < l_shipdate GROUP BY l_shipdate');

-- Q14: Promotion effect
SELECT * FROM vizier_capture('SELECT sum(CASE WHEN l_returnflag = ''A'' THEN l_extendedprice * (1 - l_discount) ELSE 0 END) / sum(l_extendedprice * (1 - l_discount)) AS promo_revenue FROM lineitem WHERE l_shipdate >= DATE ''1995-09-01'' AND l_shipdate < DATE ''1995-10-01''');

-- Repeated queries (simulate real workload frequency)
SELECT * FROM vizier_capture('SELECT * FROM orders WHERE o_custkey = 42');
SELECT * FROM vizier_capture('SELECT * FROM orders WHERE o_custkey = 100');
SELECT * FROM vizier_capture('SELECT * FROM orders WHERE o_custkey = 999');
SELECT * FROM vizier_capture('SELECT * FROM lineitem WHERE l_orderkey = 1000');
SELECT * FROM vizier_capture('SELECT * FROM lineitem WHERE l_orderkey = 5000');

SELECT * FROM vizier_flush();

-- ======================================
-- Analyze
-- ======================================
SELECT '=== Running advisors ===' AS status;
SELECT * FROM vizier_analyze();

-- ======================================
-- Results
-- ======================================
SELECT '=== Workload summary ===' AS section;
SELECT * FROM vizier.workload_summary;

SELECT '=== Recommendations ===' AS section;
SELECT recommendation_id, kind, table_name, round(score, 3) AS score, round(confidence, 2) AS conf,
       round(estimated_gain, 3) AS gain, round(estimated_build_cost, 3) AS build_cost,
       reason
FROM vizier.recommendations;

SELECT '=== Overview ===' AS section;
SELECT * FROM vizier.overview();

-- ======================================
-- Benchmark sample queries
-- ======================================
SELECT '=== Benchmarks ===' AS section;
SELECT 'Q6 scan' AS query, * FROM vizier_benchmark('SELECT sum(l_extendedprice * l_discount) AS revenue FROM lineitem WHERE l_shipdate >= DATE ''1994-01-01'' AND l_shipdate < DATE ''1995-01-01'' AND l_discount BETWEEN 0.05 AND 0.07 AND l_quantity < 24', 10);

SELECT 'Point lookup' AS query, * FROM vizier_benchmark('SELECT * FROM orders WHERE o_custkey = 42', 10);
