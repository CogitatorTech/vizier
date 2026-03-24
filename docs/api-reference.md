# API Reference

## Scalar functions

### `vizier_version()`

Returns the Vizier extension version string.

```sql
select vizier_version();
```

### `vizier_configure(key, value)`

Updates a setting in the `vizier.settings` table. Returns the new value.

```sql
select vizier_configure('benchmark_runs', '10');
```

### `vizier_session_log(sql)`

Logs a query to the active capture session. Used between `vizier_start_capture()` and `vizier_stop_capture()`.

```sql
select vizier_session_log('select * from events where id = 1');
```

## Table functions

### `vizier_init(path)`

Initializes the Vizier schema and metadata tables. If `path` is provided, sets the persistent state file.

```sql
select * from vizier_init('/path/to/state.db');
```

### `vizier_capture(sql)`

Captures a single query for workload analysis. The query is normalized (literals replaced with `?`, keywords uppercased, and whitespace collapsed) and deduplicated by signature.

```sql
select * from vizier_capture('select * from events where account_id = 42');
```

### `vizier_capture_bulk(table, column)`

Reads SQL statements from a table column and captures them.

```sql
select * from vizier_capture_bulk('query_log', 'sql_text');
```

### `vizier_start_capture()`

Starts session-based capture. Queries logged via `vizier_session_log()` are buffered until `vizier_stop_capture()` is called.

```sql
select * from vizier_start_capture();
```

### `vizier_stop_capture()`

Stops session capture, imports logged queries, and flushes them to metadata tables.

```sql
select * from vizier_stop_capture();
```

### `vizier_import_profile(path)`

Imports queries from a DuckDB JSON profiling output file.

```sql
select * from vizier_import_profile('/path/to/profile.json');
```

### `vizier_flush()`

Persists all captured queries from the in-memory buffer to the `vizier.workload_queries` and `vizier.workload_predicates` tables. Also populates `tables_json`, `columns_json`, and selectivity data.

```sql
select * from vizier_flush();
```

### `vizier_analyze()`

Runs all advisors on the captured workload and populates `vizier.recommendation_store`. The advisors are:

| Advisor | Recommendation kind | Description |
|---|---|---|
| Index | `create_index` | ART indexes for selective equality and join predicates |
| Sort | `rewrite_sorted_table` | Table rewrites with better row order for scan pruning |
| Redundant index | `drop_redundant_index` | Indexes that are a prefix of another index |
| Parquet sort | `parquet_sort_order` | Sort order for Parquet exports |
| Parquet partition | `parquet_partition` | Partitioning strategy for Parquet exports |
| Parquet row group | `parquet_row_group_size` | Row group size for Parquet exports |
| Summary table | `create_summary_table` | Precomputed rollup tables for repeated aggregations |
| Join path | `sort_for_join` | Sort by join key for merge-join compatibility |
| No action | `no_action` | Tables in the workload with no pending recommendations |

```sql
select * from vizier_analyze();
```

### `vizier_apply(id, dry_run)`

Executes a recommendation from `vizier.recommendation_store`. With `dry_run => true`, shows the SQL without executing.

```sql
select * from vizier_apply(1);
select * from vizier_apply(1, dry_run => true);
```

### `vizier_apply_all(min_score, max_actions, dry_run)`

Applies all pending recommendations above a score threshold.

| Parameter | Type | Description |
|---|---|---|
| `min_score` | double | Minimum score (0 to 1) |
| `max_actions` | bigint | Maximum recommendations to apply |
| `dry_run` | boolean | Preview without executing |

```sql
select * from vizier_apply_all(min_score => 0.7, max_actions => 5, dry_run => false);
```

### `vizier_rollback(action_id)`

Reverses a single applied recommendation by executing its stored inverse SQL.

```sql
select * from vizier_rollback(1);
```

### `vizier_rollback_all()`

Undoes all applied recommendations in reverse order.

```sql
select * from vizier_rollback_all();
```

### `vizier_benchmark(sql, runs)`

Runs a query multiple times and returns timing statistics.

```sql
select * from vizier_benchmark('select count(*) from events', 10);
```

### `vizier_compare(id)`

Benchmarks a recommendation's target queries before and after applying. Captures `EXPLAIN` plans for both.

```sql
select * from vizier_compare(1);
```

### `vizier_replay(table_name)`

Re-runs all captured queries and records timing in `vizier.replay_results`. Optionally filtered by table.

```sql
select * from vizier_replay();
select * from vizier_replay(table_name => 'events');
```

### `vizier_save(path)`

Saves all Vizier state to a DuckDB file.

```sql
select * from vizier_save('/tmp/vizier_state.db');
```

### `vizier_load(path)`

Loads Vizier state from a previously saved file.

```sql
select * from vizier_load('/tmp/vizier_state.db');
```

### `vizier_report(path)`

Generates a standalone HTML report.

```sql
select * from vizier_report('/tmp/report.html');
```

### `vizier_dashboard(path)`

Generates an interactive HTML dashboard.

```sql
select * from vizier_dashboard('/tmp/dashboard.html');
```

## Views

### `vizier.recommendations`

Pending recommendations ranked by score.

| Column | Type | Description |
|---|---|---|
| `recommendation_id` | integer | Unique ID |
| `kind` | varchar | Recommendation type |
| `table_name` | varchar | Target table |
| `columns_json` | varchar | Affected columns (JSON) |
| `score` | double | Recommendation score (0 to 1) |
| `confidence` | double | Confidence level (0 to 1) |
| `reason` | varchar | Why this was recommended |
| `sql_text` | varchar | SQL to execute |
| `status` | varchar | pending, applied, or rolled_back |
| `estimated_gain` | varchar | Estimated performance improvement |
| `estimated_build_cost` | varchar | Cost to build (like index creation time) |
| `estimated_maintenance_cost` | varchar | Ongoing cost (like write overhead) |

### `vizier.workload_summary`

Summary of captured queries ordered by execution count.

| Column | Type | Description |
|---|---|---|
| `sig` | varchar | Query signature hash |
| `sql` | varchar | Sample SQL |
| `runs` | integer | Execution count |
| `last_seen` | varchar | Last capture timestamp |
| `avg_ms` | double | Average execution time |

### `vizier.change_history`

Applied changes with rollback availability.

| Column | Type | Description |
|---|---|---|
| `action_id` | integer | Action ID |
| `recommendation_id` | integer | Source recommendation |
| `kind` | varchar | Recommendation type |
| `table_name` | varchar | Target table |
| `sql_text` | varchar | SQL that was executed |
| `inverse_sql` | varchar | SQL to undo |
| `success` | boolean | Whether execution succeeded |
| `applied_at` | varchar | Timestamp |
| `can_rollback` | boolean | Whether rollback is available |

### `vizier.replay_summary`

Replay results with regression detection.

| Column | Type | Description |
|---|---|---|
| `query_signature` | varchar | Query signature hash |
| `sample_sql` | varchar | Sample SQL |
| `avg_ms` | double | Replay average time |
| `status` | varchar | Execution status |
| `verdict` | varchar | improved, stable, or regression |

### `vizier.replay_totals`

Aggregate replay metrics.

| Column | Type | Description |
|---|---|---|
| `queries_replayed` | integer | Number of queries replayed |
| `replay_total_ms` | double | Total replay time |
| `baseline_total_ms` | double | Total baseline time |
| `ratio` | double | Replay time / baseline time |
| `overall_verdict` | varchar | improved, stable, or regression |

## Table macros

### `vizier.inspect_table(name)`

Per-column view of a table showing type, nullability, size, index count, and predicate usage from the captured workload.

```sql
select * from vizier.inspect_table('events');
```

### `vizier.overview()`

All user tables with sizes, index counts, and predicate hotspots.

```sql
select * from vizier.overview();
```

### `vizier.explain(id)`

Full details of a single recommendation.

```sql
select * from vizier.explain(1);
```

### `vizier.explain_human(id)`

Natural language explanation with estimated speedup and write overhead.

```sql
select * from vizier.explain_human(1);
```

### `vizier.score_breakdown(id)`

Per-predicate scoring breakdown showing how each column contributes to the score.

```sql
select * from vizier.score_breakdown(1);
```

### `vizier.compare(id)`

Before/after benchmark comparison with verdict (excellent, good, marginal, or regression).

```sql
select * from vizier.compare(1);
```

### `vizier.analyze_table(name)`

Pending recommendations filtered to a single table.

```sql
select * from vizier.analyze_table('events');
```

### `vizier.analyze_workload(since, min_executions)`

Captured queries filtered by time and minimum execution count.

```sql
select * from vizier.analyze_workload('2026-01-01', 5);
```

## Metadata tables

### `vizier.workload_queries`

Captured query patterns with execution statistics.

| Column | Type | Description |
|---|---|---|
| `query_signature` | varchar | Normalized query hash |
| `normalized_sql` | varchar | Normalized SQL (literals replaced with `?`) |
| `sample_sql` | varchar | Original SQL sample |
| `first_seen` | varchar | First capture timestamp |
| `last_seen` | varchar | Last capture timestamp |
| `execution_count` | integer | Number of times captured |
| `total_time_ms` | double | Total execution time |
| `avg_time_ms` | double | Average execution time |
| `p95_time_ms` | double | 95th percentile execution time |
| `tables_json` | varchar | Referenced tables (JSON) |
| `columns_json` | varchar | Referenced columns (JSON) |

### `vizier.workload_predicates`

Predicates extracted from captured queries.

| Column | Type | Description |
|---|---|---|
| `query_signature` | varchar | Source query signature |
| `table_name` | varchar | Table referenced |
| `column_name` | varchar | Column referenced |
| `predicate_kind` | varchar | equality, range, in_list, like, or group_by |
| `frequency` | integer | How often this predicate appears |
| `avg_selectivity` | double | Estimated selectivity (0 to 1) |

### `vizier.recommendation_store`

All generated recommendations. Same columns as the `vizier.recommendations` view, but includes all statuses (not just pending).

### `vizier.applied_actions`

Audit log of applied recommendations.

| Column | Type | Description |
|---|---|---|
| `action_id` | integer | Unique action ID |
| `recommendation_id` | integer | Source recommendation |
| `applied_at` | varchar | Timestamp |
| `sql_text` | varchar | SQL that was executed |
| `inverse_sql` | varchar | SQL to undo |
| `success` | boolean | Whether execution succeeded |
| `notes` | varchar | Status notes |

### `vizier.benchmark_results`

Benchmark comparison results.

| Column | Type | Description |
|---|---|---|
| `benchmark_id` | integer | Unique benchmark ID |
| `recommendation_id` | integer | Source recommendation |
| `query_signature` | varchar | Query tested |
| `baseline_ms` | double | Baseline timing |
| `candidate_ms` | double | Post-change timing |
| `speedup` | double | baseline_ms / candidate_ms |
| `plan_before` | varchar | `EXPLAIN` plan before |
| `plan_after` | varchar | `EXPLAIN` plan after |
| `recorded_at` | varchar | Timestamp |

### `vizier.replay_results`

Per-query replay results.

| Column | Type | Description |
|---|---|---|
| `replay_id` | integer | Unique replay ID |
| `query_signature` | varchar | Query signature |
| `sample_sql` | varchar | Sample SQL |
| `avg_ms` | double | Replay average time |
| `status` | varchar | Execution status |
| `replayed_at` | varchar | Timestamp |

### `vizier.session_log`

Queries captured during a session.

| Column | Type | Description |
|---|---|---|
| `sql_text` | varchar | Captured SQL |
| `captured_at` | varchar | Timestamp |

## Settings

Default configuration values in `vizier.settings`:

| Key | Default | Description |
|---|---|---|
| `min_query_count` | 1 | Minimum query frequency before generating recommendations |
| `min_confidence` | 0.4 | Minimum confidence threshold |
| `benchmark_runs` | 5 | Default benchmark iterations |
| `max_pending_captures` | 4096 | Maximum queries in capture buffer |
| `auto_flush` | false | Flush after each capture |
| `state_path` | (empty) | Auto-save/load state file path |
| `index_score_divisor` | 10 | Index advisor score divisor |
| `sort_score_divisor` | 20 | Sort advisor score divisor |
| `sort_min_predicates` | 2 | Minimum predicates before sort advisor fires |
| `large_table_bytes` | 100000000 | Table size threshold for maintenance cost warnings |
| `compare_bench_runs` | 5 | Benchmark runs for `vizier_compare` |
| `cost_weight_time` | 1.0 | Weight for query time impact in scoring |
| `cost_weight_frequency` | 1.0 | Weight for query frequency in scoring |
| `cost_weight_selectivity` | 1.5 | Boost for high-selectivity columns in scoring |
