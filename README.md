<div align="center">

<h2>Vizier</h2>

[![Tests](https://img.shields.io/github/actions/workflow/status/CogitatorTech/vizier/tests.yml?label=tests&style=flat&labelColor=282c34&logo=github)](https://github.com/CogitatorTech/vizier/actions/workflows/tests.yml)
[![Zig Version](https://img.shields.io/badge/Zig-0.15.2-orange?logo=zig&labelColor=282c34)](https://ziglang.org/download/)
[![License](https://img.shields.io/badge/license-MIT-007ec6?label=license&style=flat&labelColor=282c34&logo=open-source-initiative)](https://github.com/CogitatorTech/vizier/blob/main/LICENSE)

A database advisor and finetuner for DuckDB

</div>

---

Vizier is a DuckDB extension for analyzing and finetuning DuckDB databases.
It provides a set of functions to capture query metadata, analyze workloads, inspect physical database and table design,
and generate recommendations for improving performance.

### Why Vizier?

Typically, when you have a DuckDB database, you're on your own to figure out things like:

- Which columns need indexes?
- Should I rewrite my table sorted differently?
- What sort order should my Parquet exports use?
- Are any of my indexes redundant?
- Which queries are my hottest bottleneck?

There are tools that can help with these in more systematic ways.
For example, for PostgreSQL (like pg_qualstats and HypoPG) and SQL Server (Database Engine Tuning Advisor).
But there is no equivalent for DuckDB (that I'm aware of).
Vizier is a tool that aims to fill this gap.

### How It Works?

You feed it your workload (the queries you want to run), and it tells you what to change:

```sql
-- Capture your real queries
select * from vizier_capture('select * from events where account_id = 42 and ts >= date ''2026-01-01''');
select * from vizier_flush();

-- Get recommendations
select * from vizier_analyze();
select * from vizier.recommendations;
-- → Create index idx_events_account_id on events(account_id)
-- → Rewrite events sorted by (account_id, ts) for scan pruning

-- Apply and measure
select * from vizier_apply(1);
select * from vizier_benchmark('select * from events where account_id = 42', 10);
```

### Who Should Use Vizier?

1. Analytics teams with growing DuckDB databases who notice queries slowing down but don't know what indexes or sort orders would help
2. Data engineers exporting to Parquet who want optimal sort order and partitioning without trial-and-error
3. Anyone migrating to DuckDB from a database that had an advisor, so they expect this tooling to exist

### What It is Not?

It's not an auto-tuner.
It doesn't magically rewrite your tables.
It recommends, explains why, lets you dry-run, and measures the impact.
But the user still has to make the decision to apply the recommendation.

### Features

* Capture query patterns and execution stats
* Inspect current physical design (indexes, table sizes, cardinalities, etc.)
* Analyze the workload to find performance bottlenecks
* Recommend changes (indexes, sort orders, Parquet layouts, summary tables, etc.)
* Apply recommendations with dry-run support
* Benchmark before and after applying changes

---

### Quickstart

#### Build from Source

```bash
# Clone Vizier repository
git clone --depth=1 https://github.com/CogitatorTech/vizier.git
cd vizier

# Build the extension
make build-all

# Run all tests (optional; needs DuckDB in PATH)
make test

# Try out Vizier (needs DuckDB in PATH)
make duckdb
```

#### Simple Example

```sql
load vizier;

-- Capture queries from your workload
select * from vizier_capture('select * from events where account_id = 42 and ts >= date ''2026-01-01''');
select * from vizier_capture('select count(*) from orders group by customer_id');
select * from vizier_capture('select * from events where account_id = 42 and ts >= date ''2026-01-01''');

-- Persist captured queries to metadata tables
select * from vizier_flush();

-- View workload summary (ordered by frequency)
select * from vizier.workload_summary;
```

Example output:

```
┌──────────────┬────────────────────────────────────────────────────────────────────────┬───────┬─────────────────────┬────────┐
│     sig      │                                  sql                                   │ runs  │      last_seen      │ avg_ms │  
│   varchar    │                                varchar                                 │ int32 │       varchar       │ double │  
├──────────────┼────────────────────────────────────────────────────────────────────────┼───────┼─────────────────────┼────────┤  
│ 8c9d7175aab0 │ SELECT * FROM events WHERE account_id = 42 AND ts >= DATE '2026-01-01' │     2 │ 2026-03-22 16:44:06 │    0.0 │  
│ 2af899d9dba6 │ SELECT count(*) FROM orders GROUP BY customer_id                       │     1 │ 2026-03-22 16:44:06 │    0.0 │  
└──────────────┴────────────────────────────────────────────────────────────────────────┴───────┴─────────────────────┴────────┘  
```

#### Current API

| Function                              | Type           | Description                                                    |
|---------------------------------------|----------------|----------------------------------------------------------------|
| `vizier_version()`                    | Scalar         | Returns the Vizier version                                     |
| `vizier_capture(sql)`                 | Table function | Captures a query (normalized and deduplicated)                 |
| `vizier_capture_bulk(table, column)`  | Table function | Bulk capture SQL queries from a table column                   |
| `vizier_start_capture()`              | Table function | Starts a capture session                                       |
| `vizier_stop_capture()`               | Table function | Stops capture, imports logged queries, and flushes             |
| `vizier_session_log(sql)`             | Scalar         | Logs a query to the active capture session                     |
| `vizier_import_profile(path)`         | Table function | Imports queries from a DuckDB JSON profiling file              |
| `vizier_flush()`                      | Table function | Persists captured queries to metadata tables                   |
| `vizier_analyze()`                    | Table function | Runs all advisors and generates recommendations                |
| `vizier_apply(id, dry_run => bool)`   | Table function | Executes a recommendation (or previews with `dry_run => true`) |
| `vizier_apply_all(min_score, ...)`    | Table function | Applies all recommendations above a score threshold            |
| `vizier_compare(id)`                  | Table function | Benchmarks before/after with plan capture                      |
| `vizier_benchmark(sql, runs)`         | Table function | Runs a query N times and returns timing stats                  |
| `vizier_rollback(action_id)`          | Table function | Reverses an applied recommendation using stored inverse SQL    |
| `vizier_rollback_all()`               | Table function | Undoes all applied recommendations in reverse order            |
| `vizier_replay()`                     | Table function | Re-runs all captured queries and measures timing               |
| `vizier_save(path)`                   | Table function | Saves all Vizier state to a DuckDB file                        |
| `vizier_load(path)`                   | Table function | Loads Vizier state from a previously saved file                |
| `vizier_report(path)`                 | Table function | Generates a standalone HTML report                             |
| `vizier_configure(key, value)`        | Scalar         | Updates a Vizier configuration setting                         |
| `vizier.inspect_table(name)`          | Table macro    | Per-column view: type, size, storage info, predicate usage     |
| `vizier.overview()`                   | Table macro    | All user tables with sizes, indexes, and predicate hotspots    |
| `vizier.explain(id)`                  | Table macro    | Detailed reasoning for a recommendation                        |
| `vizier.explain_human(id)`            | Table macro    | Natural language explanation for a recommendation              |
| `vizier.score_breakdown(id)`          | Table macro    | Transparent scoring components for a recommendation            |
| `vizier.compare(id)`                  | Table macro    | Before/after benchmark comparison view                         |
| `vizier.analyze_table(name)`          | Table macro    | Recommendations filtered by table                              |
| `vizier.analyze_workload(since, min)` | Table macro    | Workload queries filtered by time and execution count          |
| `vizier.recommendations`              | View           | Pending recommendations ranked by score                        |
| `vizier.workload_summary`             | View           | Summary of captured workload by frequency                      |
| `vizier.change_history`               | View           | Applied changes with rollback availability                     |
| `vizier.replay_summary`               | View           | Replay results with regression detection                       |
| `vizier.settings`                     | Table          | Configuration key-value pairs                                  |

---

### Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for details on how to make a contribution.

### License

This project is licensed under the MIT License (see [LICENSE](LICENSE)).
