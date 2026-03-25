# Current Limitations

Vizier is in early development. This page documents known limitations, so you can decide whether Vizier fits your use case and interpret
its recommendations accordingly.

## Query Capture Is Manual

Vizier needs you to explicitly capture queries via `vizier_capture()`, `vizier_capture_bulk()`, or session capture.
There is no automatic capture that transparently records all queries as they run.
This means the quality of recommendations depends entirely on whether you feed Vizier a representative workload.

Automatic capture (via DuckDB profiling output or a query log hook) is planned but not yet implemented.

## SQL Parsing Is Heuristic

The predicate extractor is a hand-rolled tokenizer, not a full SQL parser.
It works for common `SELECT` queries with `WHERE`, `JOIN`, and `GROUP BY` clauses, but it will miss or misparse things like:

- Subqueries and CTEs
- Window functions with `PARTITION BY`
- Computed predicates like `WHERE f(col) > 0`
- `CASE WHEN` expressions in filter positions
- DuckDB-specific syntax (lambda functions, list comprehensions, and `QUALIFY`)

The tokenizer also has fixed limits: 1024 tokens and 64 predicates per query.
Queries that exceed these limits are silently truncated.

There is currently no way to tell whether a query was fully parsed or partially extracted.

## Scoring Is Frequency-Based, Not Cost-Based

Recommendations are scored using query frequency, predicate counts, and hand-tuned thresholds 
(like "divide frequency by 10" or "boost by 1.2x for low selectivity").
These heuristics are reasonable defaults but are not grounded in actual query execution costs.

The "estimated_gain" values (like "10-50x faster") are generated from templates, not from measured or predicted performance.
Treat them as rough indicators, not guarantees.

Cost-based scoring using `EXPLAIN` output is planned but not yet implemented.

## ART Indexes Have Limited Value in DuckDB

DuckDB's ART indexes are primarily useful for point lookups and primary key enforcement.
The DuckDB optimizer often skips ART indexes for range scans because columnar min/max zone maps already provide pruning.
The index advisor may recommend indexes that DuckDB does not use for the queries you care about.

Before applying an index recommendation, use `vizier_compare(id)` to verify the `EXPLAIN` plan actually uses the index.

## Sort and Parquet Advisors Do Not Reason about Tradeoffs

When different queries benefit from different sort orders (like some filtering by `account_id`
and others by `region`), the sort and Parquet advisors pick the most frequent pattern.
They do not report the tradeoff or warn that other queries may regress.

Use `vizier_replay()` after applying a sort recommendation to check for regressions across the full workload.

## Large API Surface

Vizier exposes 22 functions, 8 macros, 5 views, 8 metadata tables, and 14 settings.
Some of these may change or be removed in future versions.
Pin your usage to specific Vizier versions if you depend on the API in scripts or pipelines.

## Best Suited for Persistent, Repeated Workloads

Vizier works best when queries repeat over time against a persistent DuckDB database.
It is less useful for:

- Ad-hoc analytics where queries change constantly
- Read-only Parquet-over-S3 scans where you cannot add indexes or rewrite tables
- ETL pipelines where the engineer already knows the access patterns

If your workload does not have repeating patterns, Vizier's frequency-based scoring will not produce meaningful recommendations.
