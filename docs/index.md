# Vizier

**A database advisor and finetuner for DuckDB.**

---

Vizier is a DuckDB extension that analyzes your (query) workload and recommends physical design changes like indexes, sort orders, Parquet layouts,
and summary tables to improve query performance.

## Why Vizier?

When you have a DuckDB database, you are typically on your own to figure out things like:

- Which columns need indexes?
- Should I rewrite my table sorted differently?
- What sort order should my Parquet exports use?
- Are any of my indexes redundant?
- Which queries are my bottleneck?

Tools like [pg_qualstats](https://github.com/powa-team/pg_qualstats) (for PostgreSQL) and
[Database Engine Tuning Advisor](https://learn.microsoft.com/en-us/sql/relational-databases/performance/database-engine-tuning-advisor?view=sql-server-ver17)
(for SQL Server) solve these problems, but nothing equivalent exists for DuckDB.
Vizier aims to fill that gap.

## How It Works?

You feed Vizier your workload (a collection of queries), and it tells you what to change in your database to make it run faster.

The example below shows the typical workflow of using Vizier.

```sql
-- Capture your real queries
select * from vizier_capture('select * from events where account_id = 42 and ts >= date ''2026-01-01''');
select * from vizier_flush();

-- Get recommendations
select * from vizier_analyze();
select * from vizier.recommendations;
-- Create index idx_events_account_id on events(account_id)
-- Rewrite events sorted by (account_id, ts) for scan pruning

-- Apply and measure the impact
select * from vizier_apply(1);
select * from vizier_benchmark('select * from events where account_id = 42', 10);
```

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
