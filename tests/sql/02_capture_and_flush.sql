-- Test: Capture, deduplication, and flush
load 'zig-out/lib/vizier.duckdb_extension';

-- Capture queries (third is a duplicate of the first after normalization)
select * from vizier_capture('select * from events where account_id = 42 and ts >= date ''2026-01-01''');
select * from vizier_capture('select count(*) from orders group by customer_id');
select * from vizier_capture('select * from events where account_id = 99 and ts >= date ''2026-06-01''');

-- Flush to metadata tables
select * from vizier_flush();

-- Verify: 2 distinct query patterns (events queries normalize the same)
select count(*) as distinct_queries from vizier.workload_queries;

-- Verify: duplicate incremented execution_count
select query_signature, execution_count, sample_sql from vizier.workload_queries order by execution_count desc;

-- Verify: predicates extracted
select table_name, column_name, predicate_kind from vizier.workload_predicates order by table_name, column_name;

-- Verify: JSON metadata populated
select tables_json, columns_json from vizier.workload_queries;

-- Verify: workload summary view
select * from vizier.workload_summary;
