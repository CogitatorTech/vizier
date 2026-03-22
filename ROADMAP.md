## Project Roadmap

This document outlines the features implemented in Vizier and the future goals for the project.

> [!IMPORTANT]
> This roadmap is a work in progress and is subject to change.

### Workload Metadata Capture

- [x] `vizier_capture_query(sql)` capture individual queries
- [x] `vizier_flush()` persist captured queries to metadata tables
- [x] Duplicate detection with execution counting
- [x] `vizier.workload_summary` view
- [ ] `vizier_capture_queries(table, column)` bulk capture from a query log table
- [ ] Session capture (`vizier_start_capture()` / `vizier_stop_capture()`)
- [ ] Import from DuckDB profiling output (`vizier_import_profile()`)
- [ ] Query normalization (strip literals, normalize whitespace, etc.)
- [x] Predicate extraction (filter columns, join columns, group-by columns, etc.)
- [x] Table reference extraction with alias resolution
- [x] `tables_json` and `columns_json` populated on flush

### Schema and Physical Design Inspection

- [x] Metadata tables for workload tracking
- [x] `vizier.inspect_table(name)` per-column view: type, nullability, size, indexes, predicate usage
- [ ] `vizier_inspect_design()` full database physical design overview
- [ ] Approximate cardinality and null fraction estimation
- [ ] Row-group and storage layout analysis

### Index Advisor

- [ ] ART index recommendations for selective equality predicates
- [ ] Point lookup and selective join detection
- [ ] Redundant index detection (`drop_redundant_index`)
- [ ] Maintenance cost estimation

### Sort and Clustering Advisor

- [ ] Recommend table rewrites with better row order
- [ ] Detect repeated filter prefixes (like `tenant_id` + `ts`)
- [ ] Row-group pruning improvement estimation
- [ ] Sort quality scoring

### Parquet Layout Advisor

- [ ] Recommend sort order before export
- [ ] Row group size recommendations
- [ ] Partitioning strategy suggestions

### Summary Table Advisor

- [ ] Detect repeated expensive aggregations
- [ ] Recommend precomputed rollup tables
- [ ] Generate `CREATE TABLE` + `INSERT SQL`

### Recommendation Engine

- [x] `vizier.recommendation_store` table
- [ ] Scoring benefit estimate, build cost, maintenance cost, confidence
- [ ] `vizier_recommendations()` ranked list of suggestions
- [ ] `vizier_explain_recommendation(id)` detailed reasoning
- [ ] `vizier_score_breakdown(id)` transparent scoring components

### Apply and Benchmark

- [x] `vizier.applied_actions` tracking table
- [x] `vizier.benchmark_results` tracking table
- [ ] `vizier_apply(id)` execute a recommendation
- [ ] `vizier_apply(id, dry_run => true)` preview without executing
- [ ] `vizier_benchmark_query(sql, runs)` measure query performance
- [ ] `vizier_compare_recommendation(id)` before/after comparison

### Development and Testing

- [x] Unit tests
- [x] Integration testing via `make test-extension`
- [x] Cross-platform build support
- [ ] End-to-end test suite with sample workloads
- [ ] Benchmarks against real-world datasets
