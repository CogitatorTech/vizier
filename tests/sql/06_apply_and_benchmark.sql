-- Test: Apply, dry-run, apply_all, compare, benchmark
load 'zig-out/lib/vizier.duckdb_extension';

create table bench_orders as select i as id, i % 100 as customer_id, i * 1.5 as amount from range(10000) t(i);

select * from vizier_capture('select * from bench_orders where customer_id = 42');
select * from vizier_capture('select * from bench_orders where customer_id = 7');
select * from vizier_flush();
select * from vizier_analyze();

-- Dry-run: preview without executing
select * from vizier_apply(1, dry_run => true);
select status from vizier.recommendation_store where recommendation_id = 1;
-- Should still be 'pending'

-- Apply_all with dry_run
select * from vizier_apply_all(min_score => 0.0, max_actions => 10, dry_run => true);

-- Actually apply one
select * from vizier_apply(1);
select status from vizier.recommendation_store where recommendation_id = 1;
-- Should be 'applied'

-- Check applied actions log
select * from vizier.applied_actions;

-- Benchmark a query
select * from vizier_benchmark('select count(*) from bench_orders where customer_id = 42', 10);

-- Benchmark results table
select count(*) as benchmark_count from vizier.benchmark_results;
