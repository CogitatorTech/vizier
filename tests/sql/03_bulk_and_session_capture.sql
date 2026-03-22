-- Test: Bulk capture and session capture
LOAD 'zig-out/lib/vizier.duckdb_extension';

-- Bulk capture from a query log table
CREATE TABLE query_log (sql_text VARCHAR);
INSERT INTO query_log VALUES ('SELECT * FROM orders WHERE id = 1');
INSERT INTO query_log VALUES ('SELECT * FROM orders WHERE id = 2');
INSERT INTO query_log VALUES ('SELECT count(*) FROM events GROUP BY aid');

SELECT * FROM vizier_capture_bulk('query_log', 'sql_text');
SELECT * FROM vizier_flush();
SELECT count(*) AS after_bulk FROM vizier.workload_queries;

-- Session capture
SELECT * FROM vizier_start_capture();
SELECT vizier_session_log('SELECT * FROM users WHERE email = ''test@example.com''');
SELECT vizier_session_log('SELECT * FROM users WHERE email = ''other@example.com''');
SELECT * FROM vizier_stop_capture();

SELECT count(*) AS after_session FROM vizier.workload_queries;
