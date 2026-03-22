## Project Roadmap

This document outlines the features implemented in Vizier and the future goals for the project.

> [!IMPORTANT]
> This roadmap is a work in progress and is subject to change.

### Workload Metadata Capture

- [x] `vizier_capture(sql)` capture individual queries
- [x] `vizier_flush()` persist captured queries to metadata tables
- [x] Duplicate detection with execution counting
- [x] `vizier.workload_summary` view
- [ ] `vizier_capture_bulk(table, column)` bulk capture from a query log table
- [ ] Session capture (using `vizier_start_capture()` and `vizier_stop_capture()`)
- [ ] Import from DuckDB profiling output (`vizier_import_profile()`)
- [x] Query normalization (replace literals with ?, uppercase keywords, collapse whitespace)
- [x] Predicate extraction (filter columns, join columns, group-by columns, etc.)
- [x] Table reference extraction with alias resolution
- [x] `tables_json` and `columns_json` populated on a flush

### Schema and Physical Design Inspection

- [x] Metadata tables for workload tracking
- [x] `vizier.inspect_table(name)` per-column view: type, nullability, size, indexes, predicate usage
- [ ] `vizier_overview()` full database physical design overview
- [ ] Approximate cardinality and null fraction estimation
- [ ] Row-group and storage layout analysis

### Index Advisor

- [x] ART index recommendations for selective equality predicates
- [x] Frequency-based scoring and confidence
- [ ] Point lookup and selective join detection
- [ ] Redundant index detection (`drop_redundant_index`)
- [ ] Maintenance cost estimation

### Sort and Clustering Advisor

- [x] Recommend table rewrites with better row order
- [x] Detect repeated filter prefixes (like `account_id` and `ts`)
- [ ] Row-group pruning improvement estimation
- [ ] Sort quality scoring

### Parquet Layout Advisor

- [ ] Recommend sort order before export
- [ ] Row group size recommendations
- [ ] Partitioning strategy suggestions

### Summary Table Advisor

- [ ] Detect repeated expensive aggregations
- [ ] Recommend precomputed rollup tables
- [ ] Generate `CREATE TABLE` and `INSERT SQL`

### Recommendation Engine

- [x] `vizier.recommendation_store` table
- [x] Frequency-based scoring with confidence levels
- [x] `vizier.recommendations` view (ranked by score)
- [ ] `vizier_explain(id)` detailed reasoning
- [ ] `vizier_score_breakdown(id)` transparent scoring components

### Apply and Benchmark

- [x] `vizier.applied_actions` tracking table
- [x] `vizier.benchmark_results` tracking table
- [x] `vizier_apply(id)` execute a recommendation with status tracking
- [ ] `vizier_apply(id, dry_run => true)` preview without executing
- [ ] `vizier_benchmark(sql, runs)` measure query performance
- [ ] `vizier_compare(id)` before and after comparison

### Development and Testing

- [x] Unit and regression tests
- [x] Property-based tests
- [x] Integration tests
- [x] Cross-platform build support
- [ ] End-to-end test suite with sample workloads
- [ ] Benchmarks against real-world datasets
