# Current Limitations

Vizier is in early development. This page documents known limitations, so you can decide whether Vizier fits your use case and interpret
its recommendations accordingly.

## Query Capture Is Manual

Vizier needs you to explicitly capture queries via `vizier_capture()`, `vizier_capture_bulk()`, or session capture.
There is no automatic capture that transparently records all queries as they run.
This means the quality of recommendations depends entirely on whether you feed Vizier a representative workload.

Automatic capture (via DuckDB profiling output or a query log hook) is planned but not yet implemented.

## SQL Parsing Uses Two Strategies

Vizier uses two extraction strategies during a flush:

1. EXPLAIN-based extraction (preferred default): runs `EXPLAIN` on the captured query using DuckDB's own parser.
   This produces accurate table references, filter predicates, and estimated row counts directly from the
   query plan. Note that it only works when the referenced tables exist in the current database.

2. Tokenizer-based extraction (fallback): a hand-rolled tokenizer that parses SQL text without
   database access. Used when `EXPLAIN` fails (e.g., tables do not exist, or the query uses unsupported syntax).

The tokenizer fallback has known limitations. It will miss or misparse:

- Subqueries and CTEs
- Window functions with `PARTITION BY`
- Computed predicates like `WHERE f(col) > 0`
- `CASE WHEN` expressions in filter positions
- DuckDB-specific syntax (lambda functions, list comprehensions, and `QUALIFY`)

The tokenizer also has fixed limits: 1024 tokens and 64 predicates per query.
Queries that exceed these limits are silently truncated.

For best results, create the tables in your database before capturing and flushing queries so that
EXPLAIN-based extraction can run.

## Scoring Is Partially Cost-Aware

Recommendations are scored using query frequency, predicate counts, and hand-tuned thresholds.
When EXPLAIN-based extraction succeeds, the estimated row count from the query plan is factored
into scoring (queries scanning more rows get a larger boost from index recommendations).

The "estimated_gain" values (like "10-50x faster") are still generated from templates, not from
measured performance. Treat them as rough indicators, not guarantees.

Full cost-based scoring that compares optimizer cost before and after a recommendation is not yet
implemented.

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

## Best Suited for Persistent, Repeated Workloads

Vizier works best when queries repeat over time against a persistent DuckDB database.
It is less useful for:

- Ad-hoc analytics where queries change constantly
- Read-only Parquet-over-S3 scans where you cannot add indexes or rewrite tables
- ETL pipelines where the engineer already knows the access patterns

If your workload does not have repeating patterns, Vizier's frequency-based scoring will not produce meaningful recommendations.
