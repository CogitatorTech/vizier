-- Test: Extension loads and version function works
LOAD 'zig-out/lib/vizier.duckdb_extension';

SELECT vizier_version();

-- Verify schema tables exist
SELECT table_name FROM information_schema.tables
WHERE table_schema = 'vizier'
ORDER BY table_name;
