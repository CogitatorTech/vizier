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

### Development and Testing

- [x] Unit and regression tests
- [x] Property-based tests
- [x] Integration tests
- [x] Cross-platform build support
- [x] End-to-end test suite with sample workloads
- [x] Benchmarks against real-world datasets
