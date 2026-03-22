## Project Roadmap

This document outlines the features implemented in Vizier and the future goals for the project.

> [!IMPORTANT]
> This roadmap is a work in progress and is subject to change.

### Workload Metadata Capture

- [x] `vizier_capture(sql)` capture individual queries
- [x] `vizier_flush()` persist captured queries to metadata tables
- [x] Duplicate detection with execution counting
- [x] `vizier.workload_summary` view
- [x] `vizier_capture_bulk(table, column)` bulk capture from a query log table
- [x] Session capture (using `vizier_start_capture()` and `vizier_stop_capture()`)
- [x] Import from DuckDB profiling output (`vizier_import_profile()`)
- [x] Query normalization (replace literals with ?, uppercase keywords, collapse whitespace)
- [x] Predicate extraction (filter columns, join columns, group-by columns, etc.)
- [x] Table reference extraction with alias resolution
- [x] `tables_json` and `columns_json` populated on a flush

### Schema and Physical Design Inspection

- [x] Metadata tables for workload tracking
- [x] `vizier.inspect_table(name)` per-column view: type, nullability, size, indexes, predicate usage
- [x] `vizier.overview()` full database physical design overview
- [x] Approximate cardinality and null fraction estimation
- [x] Row-group and storage layout analysis

### Index Advisor

- [x] ART index recommendations for selective equality predicates
- [x] Frequency-based scoring and confidence
- [x] Point lookup and selective join detection
- [x] Redundant index detection (`drop_redundant_index`)
- [x] Maintenance cost estimation

### Sort and Clustering Advisor

- [x] Recommend table rewrites with better row order
- [x] Detect repeated filter prefixes (like `account_id` and `ts`)
- [x] Row-group pruning improvement estimation
- [x] Sort quality scoring

### Parquet Layout Advisor

- [x] Recommend sort order before export
- [x] Row group size recommendations
- [x] Partitioning strategy suggestions

### Summary Table Advisor

- [x] Detect repeated expensive aggregations
- [x] Recommend precomputed rollup tables
- [x] Generate `CREATE TABLE` and `INSERT SQL`

### Join-Path Advisor

- [x] Sort both sides of frequent joins by the join key for merge-join compatibility

### Recommendation Engine

- [x] `vizier.recommendation_store` table
- [x] Frequency-based scoring with confidence levels
- [x] `vizier.recommendations` view (ranked by score)
- [x] `vizier.explain(id)` detailed reasoning
- [x] `vizier.score_breakdown(id)` transparent scoring components
- [x] `no_action` kind for well-optimized tables
- [x] `estimated_gain`, `estimated_build_cost`, `estimated_maintenance_cost` columns
- [x] Per-table analysis via `vizier.analyze_table(name)`
- [x] Filtered workload analysis via `vizier.analyze_workload(since, min_executions)`

### Apply and Benchmark

- [x] `vizier.applied_actions` tracking table
- [x] `vizier.benchmark_results` tracking table
- [x] `vizier_apply(id)` execute a recommendation with status tracking
- [x] `vizier_apply(id, dry_run => true)` preview without executing
- [x] `vizier_apply_all(min_score, max_actions, dry_run)` batch apply with threshold
- [x] `vizier_benchmark(sql, runs)` measure query performance
- [x] `vizier_compare(id)` before and after comparison

### Configuration

- [x] `vizier.settings` table with key-value configuration
- [x] `vizier_configure(key, value)` function to update settings
- [x] Default settings for benchmark_runs, min_confidence, min_query_count

### Additional Metadata

- [x] `p95_time_ms` in `workload_queries`
- [x] `avg_selectivity` in `workload_predicates`

### Extractor

- [x] Both sides of JOIN conditions extracted (e.g., `on a.x = b.y` captures both `a.x` and `b.y`)

### Settings-Driven Configuration

- [x] Index advisor reads `index_score_divisor` and `large_table_bytes` from `vizier.settings`
- [x] Sort advisor reads `sort_score_divisor` and `sort_min_predicates` from `vizier.settings`

### Selectivity Estimation

- [x] `vizier_analyze()` computes `approx_count_distinct / count(*)` per (table, column)
- [x] Results stored in `workload_predicates.avg_selectivity`

### Query Plan Comparison

- [x] `vizier_compare(id)` captures EXPLAIN plans before and after applying a recommendation
- [x] Plans stored in `benchmark_results.plan_before` and `benchmark_results.plan_after`

### Development and Testing

- [x] Unit and regression tests
- [x] Property-based tests
- [x] Integration tests
- [x] Standalone SQL tests (`tests/sql/`, `make test-sql`)
- [x] SQL-based benchmarks (`benches/`, `make bench`)
- [x] Cross-platform build support (Linux, macOS, Windows, FreeBSD)
- [x] CI pipeline with DuckDB integration tests and 9-platform cross-compile
- [x] End-to-end test suite with sample workloads
- [x] Benchmarks against real-world datasets (TPC-H-like)

---

## v3 Roadmap

> Features that would make Vizier 10x more useful for real-world adoption.

### Persistent State

Vizier state is currently in-memory and lost when DuckDB exits. Persistence makes Vizier
usable for real workloads where query patterns accumulate over days/weeks.

- [x] `vizier_init(path)` — initialize Vizier with a persistent state file (sets state_path + loads)
- [x] Auto-load state on extension init if `state_path` setting is configured
- [x] Auto-save metadata tables on `vizier_flush()` when `state_path` configured
- [x] `vizier_export(path)` — export full Vizier state (workload + recommendations + actions) to a file
- [x] `vizier_import(path)` — import state from another environment (dev → staging → prod analysis)
- [ ] Attach-based persistence using `ATTACH ':vizier_state:' AS vizier` to a file-backed database

### Automatic Query Capture

Manual `vizier_capture()` per query is too much friction. Automatic capture makes Vizier
zero-effort to adopt.

- [ ] Profiling-based capture: `vizier_start_capture()` enables `PRAGMA enable_profiling` and polls the profiling output file on a timer
- [ ] Capture execution time from profiling data (`total_time_ms`, `p95_time_ms` populated from real measurements)
- [ ] Capture rows scanned and memory usage from profiling output when available
- [ ] Proxy mode: lightweight TCP proxy that intercepts queries to DuckDB, captures them, and forwards to the real instance
- [ ] Hook into DuckDB's `query_log` table (if/when DuckDB exposes it via extension API)

### Cost-Based Scoring

Replace heuristic frequency-based scoring with cost-aware scoring that factors in actual
query performance, not just how often a query runs.

- [x] Weight recommendations by `total_time_ms * execution_count` (total CPU impact) instead of frequency alone
- [ ] Use `EXPLAIN` cost estimates to predict improvement before applying
- [ ] Factor in estimated rows scanned from EXPLAIN output
- [ ] Compare optimizer cost before vs after for each recommendation
- [x] Configurable scoring weights via `vizier.settings` (`cost_weight_time`, `cost_weight_frequency`, `cost_weight_selectivity`)

### Web Dashboard

A single-page web UI that makes Vizier accessible to non-SQL users and provides
visual insight into workload patterns and recommendations.

- [ ] Embedded HTTP server in the extension (or standalone `vizier-dashboard` binary)
- [ ] Workload heatmap: tables and columns colored by query frequency and time spent
- [ ] Recommendation list with score bars, one-click apply, dry-run preview
- [ ] Before/after plan diff viewer (side-by-side EXPLAIN comparison)
- [ ] Timeline view: applied changes and their measured impact over time
- [ ] Physical design overview: tables, indexes, sizes, predicate coverage
- [x] Export dashboard state as static HTML report via `vizier_report(path)`

### Workload Replay and Regression Testing

After applying a recommendation, automatically validate that the entire workload
improved (not just the target query).

- [x] `vizier_replay()` — re-run all captured queries and collect timing
- [x] `vizier_replay(table_name => 'x')` — replay only queries that touch a specific table
- [x] Compare total workload time before vs after via `vizier.replay_totals` view
- [x] Detect regressions via `vizier.replay_summary` view (compares replay timing vs stored avg_time_ms)
- [x] Store replay results in `vizier.replay_results` table with per-query breakdown

### Schema Change Tracking with Rollback

Make `vizier_apply()` reversible so users feel safe applying recommendations in production.

- [x] Store inverse operation for each applied recommendation (e.g., `DROP INDEX` for `CREATE INDEX`)
- [x] `vizier_rollback(id)` — execute the inverse and mark the recommendation as rolled back
- [x] `vizier.change_history` view showing all applied changes with timestamps and rollback availability
- [x] Warn before applying irreversible recommendations (status returns "applied (irreversible)")
- [x] `vizier_rollback_all()` — undo all applied recommendations in reverse order

### Natural Language Explanations

Replace terse technical reasons with clear, actionable explanations that non-expert
users can understand and act on.

- [x] Template-based explanation generator that uses table size, query frequency, and selectivity to produce human-readable text
- [x] Include estimated speedup range (e.g., "10-50x faster") based on selectivity and table size
- [x] Include write overhead estimate in plain language (e.g., "adds ~3-5% to INSERT operations")
- [x] `vizier.explain_human(id)` macro that returns the natural language explanation
- [ ] Optionally integrate with LLM API for richer explanations (configurable, opt-in)

### IDE Integration

Bring Vizier recommendations directly into the developer's editor.

- [ ] VS Code extension: sidebar panel showing recommendations for the current project
- [ ] Inline diagnostics: warning squiggles on queries that would benefit from an index
- [ ] Code actions: "Create recommended index" as a quick fix
- [ ] JetBrains plugin (DataGrip, CLion): same features via the DuckDB JDBC driver
- [ ] LSP-based protocol so any editor can integrate
