-- Test: Apply, dry-run, apply_all, compare, benchmark
LOAD 'zig-out/lib/vizier.duckdb_extension';

CREATE TABLE bench_orders AS SELECT i AS id, i % 100 AS customer_id, i * 1.5 AS amount FROM range(10000) t(i);

SELECT * FROM vizier_capture('SELECT * FROM bench_orders WHERE customer_id = 42');
SELECT * FROM vizier_capture('SELECT * FROM bench_orders WHERE customer_id = 7');
SELECT * FROM vizier_flush();
SELECT * FROM vizier_analyze();

-- Dry-run: preview without executing
SELECT * FROM vizier_apply(1, dry_run => true);
SELECT status FROM vizier.recommendation_store WHERE recommendation_id = 1;
-- Should still be 'pending'

-- Apply_all with dry_run
SELECT * FROM vizier_apply_all(min_score => 0.0, max_actions => 10, dry_run => true);

-- Actually apply one
SELECT * FROM vizier_apply(1);
SELECT status FROM vizier.recommendation_store WHERE recommendation_id = 1;
-- Should be 'applied'

-- Check applied actions log
SELECT * FROM vizier.applied_actions;

-- Benchmark a query
SELECT * FROM vizier_benchmark('SELECT count(*) FROM bench_orders WHERE customer_id = 42', 10);

-- Benchmark results table
SELECT count(*) AS benchmark_count FROM vizier.benchmark_results;
