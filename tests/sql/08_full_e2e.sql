-- End-to-end test: Complete capture -> analyze -> apply -> benchmark loop
load 'zig-out/lib/vizier.duckdb_extension';

-- ======================================
-- 1. Create realistic schema
-- ======================================
create table orders as
  select i as id, i % 1000 as customer_id, (random() * 500)::double as amount,
         '2025-01-01'::date + (i % 730)::integer as order_date,
         case when i % 5 = 0 then 'shipped' when i % 5 = 1 then 'pending' else 'delivered' end as status
  from range(50000) t(i);

create table customers as
  select i as id, 'customer_' || i as name,
         case when i % 4 = 0 then 'US' when i % 4 = 1 then 'EU' when i % 4 = 2 then 'APAC' else 'LATAM' end as region
  from range(1000) t(i);

create table events as
  select i as id, i % 200 as account_id, now() - interval (i * 2) minute as ts,
         case when i % 3 = 0 then 'click' when i % 3 = 1 then 'view' else 'purchase' end as event_type
  from range(100000) t(i);

-- ======================================
-- 2. Capture a realistic workload
-- ======================================
-- Point lookups
select * from vizier_capture('select * from orders where id = 42');
select * from vizier_capture('select * from customers where id = 100');

-- Filtered scans
select * from vizier_capture('select * from orders where customer_id = 42 and order_date >= date ''2025-06-01''');
select * from vizier_capture('select * from orders where customer_id = 99 and order_date between date ''2025-01-01'' and date ''2025-12-31''');
select * from vizier_capture('select * from events where account_id = 10 and ts >= date ''2026-01-01''');
select * from vizier_capture('select * from events where account_id = 20 and ts between date ''2025-06-01'' and date ''2026-06-01''');

-- Joins
select * from vizier_capture('select o.id, c.name from orders o join customers c on o.customer_id = c.id where c.region = ''US''');
select * from vizier_capture('select o.id, c.name from orders o join customers c on o.customer_id = c.id where o.amount > 200');

-- Aggregations
select * from vizier_capture('select customer_id, count(*), sum(amount) from orders group by customer_id');
select * from vizier_capture('select customer_id, avg(amount) from orders group by customer_id having count(*) > 5');
select * from vizier_capture('select region, count(*) from customers group by region');

-- Flush
select * from vizier_flush();

-- ======================================
-- 3. Inspect physical design
-- ======================================
select '--- Overview ---' as section;
select * from vizier.overview();

select '--- Orders table inspection ---' as section;
select column_name, column_type, approx_rows, predicate_count, predicate_kinds
from vizier.inspect_table('orders');

select '--- Workload summary ---' as section;
select * from vizier.workload_summary;

-- ======================================
-- 4. Analyze and review recommendations
-- ======================================
select * from vizier_analyze();

select '--- All recommendations ---' as section;
select recommendation_id, kind, table_name, round(score, 2) as score, round(confidence, 2) as confidence
from vizier.recommendations;

-- ======================================
-- 5. Dry-run apply the top recommendation (use literal ID 1)
-- ======================================
select '--- Dry-run top recommendation ---' as section;
select * from vizier_apply(1, dry_run => true);

-- ======================================
-- 6. Apply and benchmark
-- ======================================
select '--- Benchmark before apply ---' as section;
select * from vizier_benchmark('select * from orders where customer_id = 42', 5);

select '--- Apply top recommendation ---' as section;
select * from vizier_apply(1);

select '--- Benchmark after apply ---' as section;
select * from vizier_benchmark('select * from orders where customer_id = 42', 5);

-- ======================================
-- 7. Final state
-- ======================================
select '--- Applied actions ---' as section;
select * from vizier.applied_actions;

select '--- Recommendation status ---' as section;
select recommendation_id, kind, table_name, status from vizier.recommendation_store order by recommendation_id;
