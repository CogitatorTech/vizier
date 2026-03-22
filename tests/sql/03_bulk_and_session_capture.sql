-- Test: Bulk capture and session capture
load 'zig-out/lib/vizier.duckdb_extension';

-- Bulk capture from a query log table
create table query_log (sql_text varchar);
insert into query_log values ('select * from orders where id = 1');
insert into query_log values ('select * from orders where id = 2');
insert into query_log values ('select count(*) from events group by aid');

select * from vizier_capture_bulk('query_log', 'sql_text');
select * from vizier_flush();
select count(*) as after_bulk from vizier.workload_queries;

-- Session capture
select * from vizier_start_capture();
select vizier_session_log('select * from users where email = ''test@example.com''');
select vizier_session_log('select * from users where email = ''other@example.com''');
select * from vizier_stop_capture();

select count(*) as after_session from vizier.workload_queries;
