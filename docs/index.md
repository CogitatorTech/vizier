# Vizier

**A database advisor and finetuner for DuckDB.**

---

Vizier is a DuckDB extension that analyzes your (query) workload and recommends physical design changes like indexes, sort orders, Parquet layouts,
and summary tables to improve query performance.

## Why Vizier?

When you have a DuckDB database, you are typically on your own to figure out things like:

- Should I rewrite my table sorted differently?
- What sort order should my Parquet exports use?
- Are any of my indexes redundant?
- Which queries are my bottleneck?

There are tools like [pg_qualstats](https://github.com/powa-team/pg_qualstats) (for PostgreSQL) and
[Database Engine Tuning Advisor](https://learn.microsoft.com/en-us/sql/relational-databases/performance/database-engine-tuning-advisor?view=sql-server-ver17)
(for SQL Server) that try to solve these problems, but nothing equivalent exists for DuckDB.
Vizier aims to fill that gap.

## How It Works?

You feed Vizier your workload (a collection of queries), and it tells you what to change in your database to make it run faster.

Install Vizier from the hosted DuckDB extension repository, then use the workflow below.

```sql
install
vizier from 'https://cogitatortech.github.io/vizier/extensions';
load
vizier;

-- Capture your real queries
select *
from vizier_capture('select * from events where account_id = 42 and ts >= date ''2026-01-01''');
select *
from vizier_flush();

-- Get recommendations
select *
from vizier_analyze();
select *
from vizier.recommendations;
-- Create index idx_events_account_id on events(account_id)
-- Rewrite events sorted by (account_id, ts) for scan pruning

-- Apply and measure the impact
select *
from vizier_apply(1);
select *
from vizier_benchmark('select * from events where account_id = 42', 10);
```

## When to Use Vizier?

Vizier is most useful for:

- Analyzing your query patterns and recommends the sort order and partitioning strategy for Parquet exports. Getting the
  sort order wrong means 10-100x worse row-group pruning.
- Optimizing persistent DuckDB tables. If you have tables that get scanned repeatedly with the same filter patterns, Vizier identifies which columns
  to sort by for scan pruning.
- Understanding workload patterns. `vizier.workload_summary`, `vizier.inspect_table()`, and `vizier.overview()` give you a quick picture of which
  tables and columns are under the most pressure. This can be useful when inheriting a database you did not build.

Note that Vizier is less useful for:

- Ad-hoc notebook analysis (no repeating patterns)
- Tiny datasets (DuckDB is already fast).
- Read-only Parquet scans like reading Parquet files on an S3 bucket.

## What Vizier Is Not

Vizier is not an auto-tuner.
It does not rewrite your tables for you.
It recommends changes, explains why, lets you dry-run, and measures the impact.
So you make the decision to apply at the end.

## Features

- Fully cross-platform (Windows, Linux, macOS, and FreeBSD)
- Supports DuckDB version `1.2.0` or newer
- Query pattern and execution stat capture
- Physical design inspection (indexes, table sizes, cardinalities, and storage layout)
- Workload analysis with bottleneck detection
- Index, sort order, Parquet layout, and summary table recommendations
- Dry-run support and before/after benchmarking
- Rollback for applied recommendations
- Persistent state across sessions
- Static HTML report generation

## Table of Contents

- [Getting Started](getting-started.md)
- [Examples](examples.md)
- [API Reference](api-reference.md)
- [Current Limitations](limitations.md)
