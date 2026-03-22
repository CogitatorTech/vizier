-- Test: Capture, deduplication, and flush
LOAD 'zig-out/lib/vizier.duckdb_extension';

-- Capture queries (third is a duplicate of the first after normalization)
SELECT * FROM vizier_capture('SELECT * FROM events WHERE account_id = 42 AND ts >= DATE ''2026-01-01''');
SELECT * FROM vizier_capture('SELECT count(*) FROM orders GROUP BY customer_id');
SELECT * FROM vizier_capture('SELECT * FROM events WHERE account_id = 99 AND ts >= DATE ''2026-06-01''');

-- Flush to metadata tables
SELECT * FROM vizier_flush();

-- Verify: 2 distinct query patterns (events queries normalize the same)
SELECT count(*) AS distinct_queries FROM vizier.workload_queries;

-- Verify: duplicate incremented execution_count
SELECT query_signature, execution_count, sample_sql FROM vizier.workload_queries ORDER BY execution_count DESC;

-- Verify: predicates extracted
SELECT table_name, column_name, predicate_kind FROM vizier.workload_predicates ORDER BY table_name, column_name;

-- Verify: JSON metadata populated
SELECT tables_json, columns_json FROM vizier.workload_queries;

-- Verify: workload summary view
SELECT * FROM vizier.workload_summary;
