-- Test: Full analyze -> recommend -> explain -> score_breakdown flow
load 'zig-out/lib/vizier.duckdb_extension';

create table events as select i as id, i % 50 as account_id, now() - interval (i) minute as ts from range(10000) t(i);

-- Capture a workload with repeated patterns
select * from vizier_capture('select * from events where account_id = 1');
select * from vizier_capture('select * from events where account_id = 2');
select * from vizier_capture('select * from events where account_id = 3 and ts >= date ''2026-01-01''');
select * from vizier_capture('select * from events where account_id = 4 and ts between date ''2026-01-01'' and date ''2026-06-01''');
select * from vizier_capture('select account_id, count(*) from events group by account_id');
select * from vizier_capture('select account_id, sum(id) from events group by account_id');
select * from vizier_flush();

-- Analyze
select * from vizier_analyze();

-- View all recommendations ranked by score
select recommendation_id, kind, table_name, score, confidence, estimated_gain, estimated_build_cost
from vizier.recommendations;

-- Explain a specific recommendation
select * from vizier.explain(1);

-- Score breakdown
select * from vizier.score_breakdown(1);

-- Per-table analysis
select * from vizier.analyze_table('events');

-- Filtered workload analysis
select * from vizier.analyze_workload('2020-01-01'::timestamp, 1);
