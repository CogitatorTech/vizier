-- Test: Configuration and settings
LOAD 'zig-out/lib/vizier.duckdb_extension';

-- View default settings
SELECT * FROM vizier.settings ORDER BY key;

-- Update a setting
SELECT vizier_configure('benchmark_runs', '20');
SELECT vizier_configure('min_confidence', '0.8');

-- Verify
SELECT key, value FROM vizier.settings WHERE key IN ('benchmark_runs', 'min_confidence') ORDER BY key;
