## Project Roadmap

This document outlines the features implemented in Vizier and the future goals for the project.

> [!IMPORTANT]
> This roadmap is a work in progress and is subject to change.

### Workload Metadata Capture

- [x] Individual query capture via `vizier_capture(sql)`
- [x] Metadata persistence via `vizier_flush()`
- [x] Duplicate detection with execution counting
- [x] `vizier.workload_summary` workload overview view
- [x] Bulk capture from a query log table via `vizier_capture_bulk(table, column)`
- [x] Session capture via `vizier_start_capture()` and `vizier_stop_capture()`
- [x] Import from DuckDB profiling output via `vizier_import_profile()`
- [x] Query normalization (literal replacement, keyword uppercasing, and whitespace collapsing)
- [x] Predicate extraction (filter columns, join columns, and group-by columns)
- [x] Table reference extraction with alias resolution
- [x] `tables_json` and `columns_json` population on a flush
- [x] Both sides of `JOIN` conditions extracted (like `on a.x = b.y` captures both `a.x` and `b.y`)
- [x] `p95_time_ms` tracking in `workload_queries`
- [x] Selectivity estimation via `vizier_analyze()`, stored in `workload_predicates.avg_selectivity`

### Schema Inspection

- [x] `vizier.inspect_table(name)` per-column view: type, nullability, size, indexes, and predicate usage
- [x] `vizier.overview()` database physical design overview
- [x] Approximate cardinality, null fraction, and row-group storage layout analysis

### Advisors

- [x] ART index recommendations for selective equality predicates
- [x] Point lookup and selective join detection
- [x] Redundant index detection via `drop_redundant_index`
- [x] Maintenance cost estimation
- [x] Table rewrite recommendations with better row order
- [x] Repeated filter prefix detection (like `account_id` and `ts`)
- [x] Row-group pruning improvement estimation
- [x] Sort quality scoring
- [x] Sort order recommendations before export
- [x] Row group size recommendations
- [x] Partitioning strategy suggestions
- [x] Repeated aggregation detection
- [x] Precomputed rollup table recommendations
- [x] `CREATE TABLE` and `INSERT` SQL generation
- [x] Sort both sides of frequent joins by the join key for merge-join compatibility

### Recommendation Engine

- [x] `vizier.recommendation_store` storage table
- [x] Frequency-based scoring with confidence levels
- [x] `vizier.recommendations` ranked view
- [x] `vizier.explain(id)` reasoning breakdown
- [x] `vizier.score_breakdown(id)` scoring components
- [x] `no_action` kind for well-optimized tables
- [x] `estimated_gain`, `estimated_build_cost`, and `estimated_maintenance_cost` columns
- [x] Per-table analysis via `vizier.analyze_table(name)`
- [x] Filtered workload analysis via `vizier.analyze_workload(since, min_executions)`

### Apply, Benchmark, and Rollback

- [x] `vizier.applied_actions` tracking table
- [x] `vizier.benchmark_results` tracking table
- [x] `vizier_apply(id)` recommendation execution with status tracking
- [x] `vizier_apply(id, dry_run => true)` preview without executing
- [x] `vizier_apply_all(min_score, max_actions, dry_run)` batch apply with threshold
- [x] `vizier_benchmark(sql, runs)` query performance measurement
- [x] `vizier_compare(id)` before/after comparison with `EXPLAIN` plan diff
- [x] Inverse operation storage for each applied recommendation (e.g., `DROP INDEX` for `CREATE INDEX`)
- [x] `vizier_rollback(id)` inverse execution and rollback marking
- [x] `vizier_rollback_all()` undo all applied recommendations in reverse order
- [x] `vizier.change_history` view with timestamps and rollback availability
- [x] Irreversible recommendation warnings (status returns "applied (irreversible)")

### Configuration

- [x] `vizier.settings` key-value configuration table
- [x] `vizier_configure(key, value)` setting update function
- [x] Default settings for benchmark_runs, min_confidence, min_query_count, and per-advisor tuning knobs

### Natural Language Explanations

- [x] Template-based explanation generator using table size, query frequency, and selectivity
- [x] Estimated speedup range (like "10-50x faster") and write overhead estimate
- [x] `vizier.explain_human(id)` human-readable explanation macro

### Workload Replay

- [x] `vizier_replay()` re-run all captured queries and collect timing
- [x] `vizier_replay(table_name => 'x')` replay only queries that touch a specific table
- [x] `vizier.replay_totals` workload time comparison before/after
- [x] `vizier.replay_summary` regression detection view
- [x] `vizier.replay_results` per-query replay breakdown table

### Development and Testing

- [x] Unit, property-based, integration, and standalone SQL tests
- [x] SQL-based benchmarks against real-world datasets (`benches/` and `make bench`)
- [x] Cross-platform build support (Linux, macOS, Windows, and FreeBSD)
- [x] CI pipeline with 9-platform cross-compile

### Persistent State

Vizier state is in-memory and lost when DuckDB exits. Persistence allows query patterns
to accumulate over days and weeks.

- [x] `vizier_init(path)` persistent state file initialization
- [x] Autoload state on extension init if `state_path` setting is configured
- [x] Auto-save metadata tables on `vizier_flush()` when `state_path` is configured
- [x] `vizier_export(path)` and `vizier_import(path)` for state transfer across environments
- [ ] Attach-based persistence using `ATTACH ':vizier_state:' AS vizier` to a file-backed database

### Automatic Query Capture

- [ ] Profiling-based capture via `PRAGMA enable_profiling` with polling
- [ ] Execution time, rows scanned, and memory usage capture from profiling data
- [ ] Proxy mode: TCP proxy that intercepts queries to DuckDB
- [ ] `query_log` table hook (if and when DuckDB exposes it via extension API)

### Cost-Based Scoring

Replace frequency-based scoring with cost-aware scoring that factors in query performance.

- [x] Recommendation weighting by `total_time_ms * execution_count` instead of frequency alone
- [x] Configurable scoring weights via `vizier.settings`
- [x] `EXPLAIN`-based predicate extraction with estimated row counts from query plans
- [x] Estimated rows factored into index advisor scoring
- [ ] Full optimizer cost comparison (before vs after) for improvement prediction before applying

### Web Dashboard

- [ ] Embedded HTTP server in the extension (or standalone `vizier-dashboard` binary)
- [ ] Workload heatmap: tables and columns colored by query frequency and time spent
- [ ] Recommendation list with score bars, one-click apply, and dry-run preview
- [ ] Before/after plan diff viewer and timeline of applied changes
- [ ] Physical design overview: tables, indexes, sizes, and predicate coverage
- [x] Static HTML report export via `vizier_report(path)`

### LLM-Powered Explanations

- [ ] Optional LLM API integration for natural language explanations
