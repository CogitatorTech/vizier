# Examples

## Index Recommendations

Capture queries with equality filters and let Vizier detect index candidates:

```sql
load vizier;

-- Simulate a workload with repeated equality lookups
select * from vizier_capture('select * from customers where email = ''alice@example.com''');
select * from vizier_capture('select * from customers where email = ''bob@example.com''');
select * from vizier_capture('select * from customers where email = ''carol@example.com''');
select * from vizier_flush();

-- Analyze and review
select * from vizier_analyze();
select * from vizier.recommendations;
-- kind: create_index
-- sql_text: create index idx_customers_email on customers(email)
```

## Sort Order Recommendations

When queries repeatedly filter on the same column prefixes, Vizier recommends sorting the table
for row-group pruning:

```sql
select * from vizier_capture('select * from events where account_id = 1 and ts >= ''2026-01-01''');
select * from vizier_capture('select * from events where account_id = 2 and ts >= ''2026-02-01''');
select * from vizier_capture('select * from events where account_id = 3 and ts between ''2026-01-01'' and ''2026-06-01''');
select * from vizier_flush();

select * from vizier_analyze();
select * from vizier.recommendations;
-- kind: rewrite_sorted_table
-- sql_text: create table events_sorted as select * from events order by account_id, ts
```

## Redundant Index Detection

If you have indexes where one is a prefix of another, Vizier flags the shorter one:

```sql
select * from vizier_analyze();
select * from vizier.recommendations where kind = 'drop_redundant_index';
-- sql_text: drop index idx_orders_customer_id
-- reason: prefix of idx_orders_customer_id_date
```

## Parquet Layout Recommendations

For tables exported to Parquet, Vizier recommends sort orders and partitioning:

```sql
select * from vizier_analyze();
select * from vizier.recommendations where kind = 'parquet_sort_order';
-- sql_text: copy events to 'events.parquet' (format parquet, row_group_size 100000) order by account_id, ts

select * from vizier.recommendations where kind = 'parquet_partition';
-- sql_text: copy events to 'events_partitioned' (format parquet, partition_by (region))
```

## Summary Table Recommendations

When the same aggregation runs repeatedly, Vizier recommends a precomputed table:

```sql
select * from vizier_capture('select customer_id, sum(amount) from orders group by customer_id');
select * from vizier_capture('select customer_id, sum(amount) from orders group by customer_id');
select * from vizier_capture('select customer_id, sum(amount) from orders group by customer_id');
select * from vizier_flush();

select * from vizier_analyze();
select * from vizier.recommendations where kind = 'create_summary_table';
```

## Inspecting Table Design

Review the physical design of a specific table:

```sql
select * from vizier.inspect_table('events');
```

This shows each column with its type, nullability, size, index count, and predicate usage
from the captured workload.

For a database-wide overview:

```sql
select * from vizier.overview();
```

## Understanding a Recommendation

Get the scoring breakdown for a recommendation:

```sql
select * from vizier.explain(1);
select * from vizier.score_breakdown(1);
select * from vizier.explain_human(1);
```

`vizier.explain_human` returns a natural language explanation with estimated speedup and write overhead.

## Batch Apply with Threshold

Apply all recommendations above a minimum score:

```sql
-- Preview what would be applied
select * from vizier_apply_all(min_score => 0.7, max_actions => 10, dry_run => true);

-- Apply
select * from vizier_apply_all(min_score => 0.7, max_actions => 10, dry_run => false);
```

## Rollback

Undo a single applied recommendation:

```sql
select * from vizier_rollback(1);
```

Or undo all applied recommendations in reverse order:

```sql
select * from vizier_rollback_all();
```

Review what was applied and what can be rolled back:

```sql
select * from vizier.change_history;
```

## Workload Replay

After applying changes, replay the captured workload to detect regressions:

```sql
select * from vizier_replay();
select * from vizier.replay_summary;
select * from vizier.replay_totals;
```

Replay only queries that touch a specific table:

```sql
select * from vizier_replay(table_name => 'events');
```

## Configuration

View and update settings:

```sql
select * from vizier.settings;
select vizier_configure('benchmark_runs', '10');
select vizier_configure('min_confidence', '0.6');
```

## HTML Report

Generate a static HTML report of the current analysis:

```sql
select * from vizier_report('/tmp/vizier_report.html');
```
