-- Benchmark: TPC-H-like workload
-- Generates TPC-H-style tables (500K+ rows), captures analytical queries,
-- runs advisors, then benchmarks queries before and after applying recommendations.
load 'zig-out/lib/vizier.duckdb_extension';

-- ======================================================================
-- 1. Data generation
-- ======================================================================
select '>>> Data generation' as benchmark;
create table _t1 as select epoch_ms(now()::timestamp) as t;

create table lineitem as
  select i as l_orderkey, i % 200000 as l_partkey, i % 10000 as l_suppkey,
         (i % 7 + 1)::integer as l_quantity,
         (random() * 50000)::double as l_extendedprice,
         (random() * 0.1)::double as l_discount,
         (random() * 0.08)::double as l_tax,
         case when i % 4 = 0 then 'A' when i % 4 = 1 then 'R' when i % 4 = 2 then 'N' else 'O' end as l_returnflag,
         case when i % 2 = 0 then 'F' else 'O' end as l_linestatus,
         '1992-01-01'::date + (i % 2556)::integer as l_shipdate,
         '1992-01-01'::date + (i % 2556 + 30)::integer as l_commitdate
  from range(500000) t(i);

create table orders as
  select i as o_orderkey, i % 150000 as o_custkey,
         (random() * 500000)::double as o_totalprice,
         '1992-01-01'::date + (i % 2556)::integer as o_orderdate,
         case when i % 5 = 0 then '1-URGENT' when i % 5 = 1 then '2-HIGH' when i % 5 = 2 then '3-MEDIUM' when i % 5 = 3 then '4-NOT SPECIFIED' else '5-LOW' end as o_orderpriority,
         case when i % 3 = 0 then 'F' when i % 3 = 1 then 'O' else 'P' end as o_orderstatus
  from range(150000) t(i);

create table customer as
  select i as c_custkey, 'Customer#' || lpad(i::varchar, 9, '0') as c_name,
         case when i % 25 = 0 then 'BUILDING' when i % 25 < 5 then 'AUTOMOBILE' when i % 25 < 10 then 'MACHINERY' when i % 25 < 15 then 'HOUSEHOLD' else 'FURNITURE' end as c_mktsegment,
         i % 25 as c_nationkey
  from range(150000) t(i);

select '  data gen: ' || (epoch_ms(now()::timestamp) - t)::varchar || ' ms' as result from _t1;

select 'lineitem' as tbl, count(*) as rows from lineitem
union all select 'orders', count(*) from orders
union all select 'customer', count(*) from customer;

-- ======================================================================
-- 2. Capture TPC-H-like queries
-- ======================================================================
select '>>> Capture: 12 TPC-H-style queries' as benchmark;
create or replace table _t1 as select epoch_ms(now()::timestamp) as t;

select * from vizier_capture('select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price, sum(l_extendedprice * (1 - l_discount)) as sum_disc_price, avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order from lineitem where l_shipdate <= date ''1998-12-01'' group by l_returnflag, l_linestatus order by l_returnflag, l_linestatus');
select * from vizier_capture('select l_orderkey, sum(l_extendedprice * (1 - l_discount)) as revenue, o_orderdate from customer c join orders o on c.c_custkey = o.o_custkey join lineitem l on l.l_orderkey = o.o_orderkey where c.c_mktsegment = ''BUILDING'' and o.o_orderdate < date ''1995-03-15'' and l.l_shipdate > date ''1995-03-15'' group by l_orderkey, o_orderdate order by revenue desc');
select * from vizier_capture('select o_orderpriority, count(*) as order_count from orders where o_orderdate >= date ''1993-07-01'' and o_orderdate < date ''1993-10-01'' group by o_orderpriority order by o_orderpriority');
select * from vizier_capture('select sum(l_extendedprice * l_discount) as revenue from lineitem where l_shipdate >= date ''1994-01-01'' and l_shipdate < date ''1995-01-01'' and l_discount between 0.05 and 0.07 and l_quantity < 24');
select * from vizier_capture('select c.c_custkey, c.c_name, sum(l_extendedprice * (1 - l_discount)) as revenue from customer c join orders o on c.c_custkey = o.o_custkey join lineitem l on l.l_orderkey = o.o_orderkey where o.o_orderdate >= date ''1993-10-01'' and o.o_orderdate < date ''1994-01-01'' and l.l_returnflag = ''R'' group by c.c_custkey, c.c_name order by revenue desc');
select * from vizier_capture('select l_shipdate, count(*) from lineitem where l_shipdate >= date ''1994-01-01'' and l_shipdate < date ''1995-01-01'' and l_commitdate < l_shipdate group by l_shipdate');
select * from vizier_capture('select sum(case when l_returnflag = ''A'' then l_extendedprice * (1 - l_discount) else 0 end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue from lineitem where l_shipdate >= date ''1995-09-01'' and l_shipdate < date ''1995-10-01''');
select * from vizier_capture('select * from orders where o_custkey = 42');
select * from vizier_capture('select * from orders where o_custkey = 100');
select * from vizier_capture('select * from orders where o_custkey = 999');
select * from vizier_capture('select * from lineitem where l_orderkey = 1000');
select * from vizier_capture('select * from lineitem where l_orderkey = 5000');

select '  capture 12 queries: ' || (epoch_ms(now()::timestamp) - t)::varchar || ' ms' as result from _t1;

select '>>> Flush' as benchmark;
create or replace table _t1 as select epoch_ms(now()::timestamp) as t;
select * from vizier_flush();
select '  flush time:         ' || (epoch_ms(now()::timestamp) - t)::varchar || ' ms' as result from _t1;

-- ======================================================================
-- 3. Analyze
-- ======================================================================
select '>>> Analyze: run all advisors' as benchmark;
create or replace table _t1 as select epoch_ms(now()::timestamp) as t;
select * from vizier_analyze();
select '  analyze time:       ' || (epoch_ms(now()::timestamp) - t)::varchar || ' ms' as result from _t1;

select '>>> Recommendations' as benchmark;
select recommendation_id as id, kind, table_name,
       round(score, 3) as score, round(confidence, 2) as conf,
       round(estimated_gain, 3) as est_gain
from vizier.recommendations;

-- ======================================================================
-- 4. Benchmark queries (10 iterations each)
-- ======================================================================
select '>>> Benchmark: Q6 full scan (10 runs)' as benchmark;
select runs, round(min_ms, 2) as min_ms, round(avg_ms, 2) as avg_ms,
       round(p95_ms, 2) as p95_ms, round(max_ms, 2) as max_ms, status
from vizier_benchmark(
  'select sum(l_extendedprice * l_discount) as revenue from lineitem where l_shipdate >= date ''1994-01-01'' and l_shipdate < date ''1995-01-01'' and l_discount between 0.05 and 0.07 and l_quantity < 24',
  10
);

select '>>> Benchmark: point lookup BEFORE index (10 runs)' as benchmark;
select runs, round(min_ms, 2) as min_ms, round(avg_ms, 2) as avg_ms,
       round(p95_ms, 2) as p95_ms, round(max_ms, 2) as max_ms, status
from vizier_benchmark(
  'select * from orders where o_custkey = 42',
  10
);

select '>>> Benchmark: Q4 aggregation (10 runs)' as benchmark;
select runs, round(min_ms, 2) as min_ms, round(avg_ms, 2) as avg_ms,
       round(p95_ms, 2) as p95_ms, round(max_ms, 2) as max_ms, status
from vizier_benchmark(
  'select o_orderpriority, count(*) as order_count from orders where o_orderdate >= date ''1993-07-01'' and o_orderdate < date ''1993-10-01'' group by o_orderpriority order by o_orderpriority',
  10
);

-- ======================================================================
-- 5. Apply and re-benchmark
-- ======================================================================
select '>>> Apply: recommendation 1' as benchmark;
select * from vizier_apply(1);

select '>>> Benchmark: point lookup AFTER index (10 runs)' as benchmark;
select runs, round(min_ms, 2) as min_ms, round(avg_ms, 2) as avg_ms,
       round(p95_ms, 2) as p95_ms, round(max_ms, 2) as max_ms, status
from vizier_benchmark(
  'select * from orders where o_custkey = 42',
  10
);

-- ======================================================================
-- Summary
-- ======================================================================
select '>>> Applied actions' as benchmark;
select action_id, recommendation_id, success, notes from vizier.applied_actions;

drop table _t1;
