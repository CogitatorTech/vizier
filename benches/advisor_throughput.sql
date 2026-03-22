-- Benchmark: Advisor throughput
-- Measures how fast vizier_analyze() generates recommendations
-- at different workload sizes.
load 'zig-out/lib/vizier.duckdb_extension';

create table t1 (id integer, a integer, b varchar, c double);
create table t2 (id integer, x integer, y varchar, z double);
create table t3 (id integer, p integer, q varchar, r double);

-- ======================================================================
-- Small workload: 10 queries
-- ======================================================================
select '>>> Capture + flush: 10 queries' as benchmark;

select * from vizier_capture('select * from t1 where a = 1');
select * from vizier_capture('select * from t1 where a = 2 and b = ''x''');
select * from vizier_capture('select * from t1 where c > 100');
select * from vizier_capture('select * from t2 where x = 1');
select * from vizier_capture('select * from t2 where x = 2 and y like ''%test%''');
select * from vizier_capture('select * from t2 where z between 10 and 50');
select * from vizier_capture('select * from t3 where p = 1');
select * from vizier_capture('select * from t3 where p in (1, 2, 3)');
select * from vizier_capture('select t1.id from t1 join t2 on t1.id = t2.id where t1.a > 5');
select * from vizier_capture('select p, count(*) from t3 group by p');
select * from vizier_flush();

select '>>> Analyze: 10 queries' as benchmark;
create table _t1 as select epoch_ms(now()::timestamp) as t;
select * from vizier_analyze();
select '  analyze time: ' || (epoch_ms(now()::timestamp) - t)::varchar || ' ms' as result from _t1;

select kind, count(*) as cnt from vizier.recommendation_store group by kind order by cnt desc;

-- ======================================================================
-- Medium workload: 30 queries total
-- ======================================================================
select '>>> Capture + flush: 20 more queries (30 total)' as benchmark;

select * from vizier_capture('select * from t1 where a = 3 and c < 50');
select * from vizier_capture('select * from t1 where b = ''hello'' and c >= 10');
select * from vizier_capture('select * from t2 where x = 3');
select * from vizier_capture('select * from t2 where z > 200');
select * from vizier_capture('select * from t3 where q = ''val1''');
select * from vizier_capture('select * from t3 where r between 0 and 100');
select * from vizier_capture('select a, count(*) from t1 group by a');
select * from vizier_capture('select a, avg(c) from t1 group by a');
select * from vizier_capture('select x, sum(z) from t2 group by x');
select * from vizier_capture('select p, max(r) from t3 group by p');
select * from vizier_capture('select t1.a, t2.x from t1 join t2 on t1.a = t2.x where t1.c > 50');
select * from vizier_capture('select t2.x, t3.p from t2 join t3 on t2.id = t3.id where t2.z > 100');
select * from vizier_capture('select * from t1 where a >= 10 and a <= 20');
select * from vizier_capture('select * from t2 where x >= 5 and y = ''specific''');
select * from vizier_capture('select * from t3 where p = 10 and q like ''prefix%''');
select * from vizier_capture('select * from t1 where id = 42');
select * from vizier_capture('select * from t2 where id = 99');
select * from vizier_capture('select * from t3 where id = 1');
select * from vizier_capture('select a, b, count(*) from t1 group by a, b');
select * from vizier_capture('select x, y, count(*) from t2 group by x, y');
select * from vizier_flush();

select '>>> Analyze: 30 queries' as benchmark;
create or replace table _t1 as select epoch_ms(now()::timestamp) as t;
select * from vizier_analyze();
select '  analyze time: ' || (epoch_ms(now()::timestamp) - t)::varchar || ' ms' as result from _t1;

select '>>> Recommendation breakdown' as benchmark;
select kind, count(*) as cnt from vizier.recommendation_store group by kind order by cnt desc;
select count(*) as total_recommendations from vizier.recommendation_store;

drop table _t1;
