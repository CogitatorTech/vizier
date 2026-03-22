-- Benchmark: Capture and flush throughput
-- Measures how fast Vizier can capture and persist queries at scale.
load 'zig-out/lib/vizier.duckdb_extension';

-- ======================================================================
-- 1. Individual capture: 50 queries
-- ======================================================================
select '>>> Individual capture: 50 queries' as benchmark;

create table _t1 as select epoch_ms(now()::timestamp) as t;

select * from vizier_capture('select * from t where a = 1');
select * from vizier_capture('select * from t where a = 2');
select * from vizier_capture('select * from t where a = 3');
select * from vizier_capture('select * from t where b = ''x''');
select * from vizier_capture('select * from t where b = ''y''');
select * from vizier_capture('select * from t where c > 100');
select * from vizier_capture('select * from t where c < 50');
select * from vizier_capture('select * from t where d between 1 and 10');
select * from vizier_capture('select * from t where e in (1, 2, 3)');
select * from vizier_capture('select * from t where f like ''%test%''');
select * from vizier_capture('select * from u where a = 1');
select * from vizier_capture('select * from u where a = 2');
select * from vizier_capture('select * from u where b = ''hello''');
select * from vizier_capture('select * from u where c >= 10');
select * from vizier_capture('select * from u where d <= 100');
select * from vizier_capture('select * from v where x = 1');
select * from vizier_capture('select * from v where x = 2');
select * from vizier_capture('select * from v where y > 50');
select * from vizier_capture('select * from v where z between 0 and 1');
select * from vizier_capture('select * from v where w in (''a'', ''b'')');
select * from vizier_capture('select a, count(*) from t group by a');
select * from vizier_capture('select b, sum(c) from t group by b');
select * from vizier_capture('select x, avg(y) from v group by x');
select * from vizier_capture('select a, b, count(*) from t group by a, b');
select * from vizier_capture('select t.a, u.b from t join u on t.id = u.id where t.a > 5');
select * from vizier_capture('select u.a, v.x from u join v on u.id = v.id where v.y > 10');
select * from vizier_capture('select * from t where a = 4 and c > 200');
select * from vizier_capture('select * from t where a = 5 and b = ''z''');
select * from vizier_capture('select * from u where a = 3 and b like ''pre%''');
select * from vizier_capture('select * from u where c > 50 and d < 200');
select * from vizier_capture('select * from v where x = 3 and y >= 100');
select * from vizier_capture('select * from v where x = 4 and z > 0.5');
select * from vizier_capture('select * from t where a = 6');
select * from vizier_capture('select * from t where a = 7');
select * from vizier_capture('select * from t where a = 8');
select * from vizier_capture('select * from u where a = 4');
select * from vizier_capture('select * from u where a = 5');
select * from vizier_capture('select * from u where a = 6');
select * from vizier_capture('select * from v where x = 5');
select * from vizier_capture('select * from v where x = 6');
select * from vizier_capture('select * from t where a = 1 and b = ''x'' and c > 0');
select * from vizier_capture('select * from t where a = 2 and c between 10 and 100');
select * from vizier_capture('select t.a from t join u on t.a = u.a join v on u.a = v.x where t.c > 50');
select * from vizier_capture('select * from t order by a, b');
select * from vizier_capture('select * from u order by a desc');
select * from vizier_capture('select * from v order by x, y');
select * from vizier_capture('select a, count(*) from t group by a having count(*) > 2');
select * from vizier_capture('select x, count(*) from v group by x having count(*) > 1');
select * from vizier_capture('select * from t where a is null');
select * from vizier_capture('select * from u where b is not null and c > 0');

select '  capture 50 queries: ' || (epoch_ms(now()::timestamp) - t)::varchar || ' ms' as result from _t1;

-- ======================================================================
-- 2. Flush: persist 50 captured queries
-- ======================================================================
select '>>> Flush: 50 pending queries' as benchmark;
create or replace table _t1 as select epoch_ms(now()::timestamp) as t;
select * from vizier_flush();
select '  flush 50 queries:   ' || (epoch_ms(now()::timestamp) - t)::varchar || ' ms' as result from _t1;

select count(*) as distinct_queries from vizier.workload_queries;
select count(*) as predicates_extracted from vizier.workload_predicates;

-- ======================================================================
-- 3. Bulk capture: 500 queries from a table
-- ======================================================================
create table bulk_log as
  select 'select * from orders where customer_id = ' || i::varchar as sql_text
  from range(500) t(i);

select '>>> Bulk capture: 500 queries from table' as benchmark;
create or replace table _t1 as select epoch_ms(now()::timestamp) as t;
select * from vizier_capture_bulk('bulk_log', 'sql_text');
select '  bulk capture 500:   ' || (epoch_ms(now()::timestamp) - t)::varchar || ' ms' as result from _t1;

select '>>> Flush: after bulk capture' as benchmark;
create or replace table _t1 as select epoch_ms(now()::timestamp) as t;
select * from vizier_flush();
select '  flush bulk:         ' || (epoch_ms(now()::timestamp) - t)::varchar || ' ms' as result from _t1;

-- ======================================================================
-- Summary
-- ======================================================================
select '>>> Final counts' as benchmark;
select count(*) as total_queries from vizier.workload_queries;
select count(*) as total_predicates from vizier.workload_predicates;

drop table _t1;
