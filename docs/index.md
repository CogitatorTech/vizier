# Vizier

A database advisor and finetuner for DuckDB.

---

Vizier is a DuckDB extension that analyzes your query workload and recommends physical design changes:
indexes, sort orders, Parquet layouts, and summary tables.

## Why Vizier?

When you have a DuckDB database, you are on your own to figure out things like:

- Which columns need indexes?
- Should I rewrite my table sorted differently?
- What sort order should my Parquet exports use?
- Are any of my indexes redundant?
- Which queries are my bottleneck?

Tools like pg_qualstats (PostgreSQL) and Database Engine Tuning Advisor (SQL Server) solve these problems,
but nothing equivalent exists for DuckDB. Vizier fills that gap.

## How it works

You feed Vizier your workload and it tells you what to change:

```sql
-- Capture your real queries
select * from vizier_capture('select * from events where account_id = 42 and ts >= date ''2026-01-01''');
select * from vizier_flush();

-- Get recommendations
select * from vizier_analyze();
select * from vizier.recommendations;
-- create index idx_events_account_id on events(account_id)
-- rewrite events sorted by (account_id, ts) for scan pruning

-- Apply and measure
select * from vizier_apply(1);
select * from vizier_benchmark('select * from events where account_id = 42', 10);
```

## What Vizier is not

Vizier is not an auto-tuner. It does not rewrite your tables for you.
It recommends changes, explains why, lets you dry-run, and measures the impact.
You make the decision to apply.

## Features

- Query pattern and execution stat capture
- Physical design inspection (indexes, table sizes, cardinalities, and storage layout)
- Workload analysis with bottleneck detection
- Index, sort order, Parquet layout, and summary table recommendations
- Dry-run support and before/after benchmarking
- Rollback for applied recommendations
- Persistent state across sessions
- Static HTML report generation

## Documentation

- [Getting Started](getting-started.md)
- [Examples](examples.md)
- [API Reference](api-reference.md)
