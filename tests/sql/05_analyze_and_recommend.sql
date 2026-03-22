-- Test: Full analyze → recommend → explain → score_breakdown flow
LOAD 'zig-out/lib/vizier.duckdb_extension';

CREATE TABLE events AS SELECT i AS id, i % 50 AS account_id, NOW() - INTERVAL (i) MINUTE AS ts FROM range(10000) t(i);

-- Capture a workload with repeated patterns
SELECT * FROM vizier_capture('SELECT * FROM events WHERE account_id = 1');
SELECT * FROM vizier_capture('SELECT * FROM events WHERE account_id = 2');
SELECT * FROM vizier_capture('SELECT * FROM events WHERE account_id = 3 AND ts >= DATE ''2026-01-01''');
SELECT * FROM vizier_capture('SELECT * FROM events WHERE account_id = 4 AND ts BETWEEN DATE ''2026-01-01'' AND DATE ''2026-06-01''');
SELECT * FROM vizier_capture('SELECT account_id, count(*) FROM events GROUP BY account_id');
SELECT * FROM vizier_capture('SELECT account_id, sum(id) FROM events GROUP BY account_id');
SELECT * FROM vizier_flush();

-- Analyze
SELECT * FROM vizier_analyze();

-- View all recommendations ranked by score
SELECT recommendation_id, kind, table_name, score, confidence, estimated_gain, estimated_build_cost
FROM vizier.recommendations;

-- Explain a specific recommendation
SELECT * FROM vizier.explain(1);

-- Score breakdown
SELECT * FROM vizier.score_breakdown(1);

-- Per-table analysis
SELECT * FROM vizier.analyze_table('events');

-- Filtered workload analysis
SELECT * FROM vizier.analyze_workload('2020-01-01'::TIMESTAMP, 1);
