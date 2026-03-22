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

The main idea is to give users a full capture, analyze, recommend, and apply loop so they can experiment with.

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
LOAD vizier;

-- Capture queries from your workload
SELECT * FROM vizier_capture('SELECT * FROM events WHERE account_id = 42 AND ts >= DATE ''2026-01-01''');
SELECT * FROM vizier_capture('SELECT count(*) FROM orders GROUP BY customer_id');
SELECT * FROM vizier_capture('SELECT * FROM events WHERE account_id = 42 AND ts >= DATE ''2026-01-01''');

-- Persist captured queries to metadata tables
SELECT * FROM vizier_flush();

-- View workload summary (ordered by frequency)
SELECT * FROM vizier.workload_summary;
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

| Function                      | Type           | Description                                             |
|-------------------------------|----------------|---------------------------------------------------------|
| `vizier_version()`            | Scalar         | Returns the extension version                           |
| `vizier_capture(sql)`         | Table function | Captures a query (normalized and deduplicated)          |
| `vizier_flush()`              | Table function | Persists captured queries to metadata tables            |
| `vizier_analyze()`            | Table function | Runs index and sort advisors, generates recommendations |
| `vizier_apply(id)`            | Table function | Executes a recommendation and logs the action           |
| `vizier.inspect_table(name)`  | Table macro    | Per-column view: type, size, indexes, predicate usage   |
| `vizier.recommendations`      | View           | Pending recommendations ranked by score                 |
| `vizier.workload_summary`     | View           | Summary of captured workload by frequency               |
| `vizier.workload_queries`     | Table          | Raw captured query data                                 |
| `vizier.workload_predicates`  | Table          | Predicate tracking per query                            |
| `vizier.recommendation_store` | Table          | All recommendations (pending, applied, and failed)      |
| `vizier.applied_actions`      | Table          | Applied recommendation log                              |
| `vizier.benchmark_results`    | Table          | Before and after benchmark data                         |

---

### Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for details on how to make a contribution.

### License

This project is licensed under the MIT License (see [LICENSE](LICENSE)).
