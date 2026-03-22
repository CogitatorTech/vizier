-- Test: Configuration and settings
load 'zig-out/lib/vizier.duckdb_extension';

-- View default settings
select * from vizier.settings order by key;

-- Update a setting
select vizier_configure('benchmark_runs', '20');
select vizier_configure('min_confidence', '0.8');

-- Verify
select key, value from vizier.settings where key in ('benchmark_runs', 'min_confidence') order by key;
