-- Test: Extension loads and version function works
load 'zig-out/lib/vizier.duckdb_extension';

select vizier_version();

-- Verify schema tables exist
select table_name from information_schema.tables
where table_schema = 'vizier'
order by table_name;
