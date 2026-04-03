const duckdb = @import("duckdb");
const sql_runner = @import("sql_runner.zig");

const create_schema_sql =
    "create schema if not exists vizier";

const create_workload_queries_sql =
    \\create table if not exists vizier.workload_queries (
    \\    query_signature varchar unique,
    \\    normalized_sql varchar,
    \\    sample_sql varchar,
    \\    first_seen timestamp default current_timestamp,
    \\    last_seen timestamp default current_timestamp,
    \\    execution_count integer default 1,
    \\    total_time_ms double default 0,
    \\    avg_time_ms double default 0,
    \\    p95_time_ms double default 0,
    \\    tables_json varchar default '[]',
    \\    columns_json varchar default '[]',
    \\    estimated_rows bigint default 0
    \\)
;

const create_workload_predicates_sql =
    \\create table if not exists vizier.workload_predicates (
    \\    query_signature varchar,
    \\    table_name varchar,
    \\    column_name varchar,
    \\    predicate_kind varchar,
    \\    frequency integer default 1,
    \\    avg_selectivity double default 0
    \\)
;

const create_recommendation_store_sql =
    \\create table if not exists vizier.recommendation_store (
    \\    recommendation_id integer default nextval('vizier.rec_id_seq'),
    \\    created_at timestamp default current_timestamp,
    \\    kind varchar,
    \\    table_name varchar,
    \\    columns_json varchar,
    \\    score double,
    \\    confidence double,
    \\    reason varchar,
    \\    sql_text varchar,
    \\    status varchar default 'pending',
    \\    estimated_gain double default 0,
    \\    estimated_build_cost double default 0,
    \\    estimated_maintenance_cost double default 0
    \\)
;

const create_applied_actions_sql =
    \\create table if not exists vizier.applied_actions (
    \\    action_id integer default nextval('vizier.action_id_seq'),
    \\    recommendation_id integer,
    \\    applied_at timestamp default current_timestamp,
    \\    sql_text varchar,
    \\    inverse_sql varchar default '',
    \\    success boolean,
    \\    notes varchar
    \\)
;

const create_benchmark_results_sql =
    \\create table if not exists vizier.benchmark_results (
    \\    benchmark_id integer default nextval('vizier.bench_id_seq'),
    \\    recommendation_id integer,
    \\    query_signature varchar,
    \\    baseline_ms double,
    \\    candidate_ms double,
    \\    speedup double,
    \\    plan_before varchar default '',
    \\    plan_after varchar default '',
    \\    recorded_at timestamp default current_timestamp
    \\)
;

const create_replay_results_sql =
    \\create table if not exists vizier.replay_results (
    \\    replay_id integer default nextval('vizier.bench_id_seq'),
    \\    query_signature varchar,
    \\    sample_sql varchar,
    \\    avg_ms double,
    \\    status varchar,
    \\    replayed_at timestamp default current_timestamp
    \\)
;

const create_settings_sql =
    \\create table if not exists vizier.settings (
    \\    key varchar unique,
    \\    value varchar,
    \\    description varchar
    \\)
;

const create_session_log_sql =
    \\create table if not exists vizier.session_log (
    \\    sql_text varchar,
    \\    captured_at timestamp default current_timestamp
    \\)
;

const insert_default_settings_sql =
    \\insert or ignore into vizier.settings (key, value, description) values
    \\  ('min_query_count', '1', 'Minimum query frequency before generating recommendations'),
    \\  ('min_confidence', '0.2', 'Minimum score threshold for recommendations (lower values keep more)'),
    \\  ('benchmark_runs', '5', 'Default number of benchmark iterations'),
    \\  ('max_pending_captures', '4096', 'Maximum queries in capture buffer'),
    \\  ('auto_flush', 'false', 'Automatically flush after each capture'),
    \\  ('state_path', '', 'Path to auto-save/load state file (empty = disabled)'),
    \\  ('index_score_divisor', '10', 'Divisor for index advisor score (lower = more sensitive)'),
    \\  ('sort_score_divisor', '20', 'Divisor for sort advisor score'),
    \\  ('sort_min_predicates', '2', 'Minimum predicates per column before sort advisor fires'),
    \\  ('large_table_bytes', '100000000', 'Table size threshold (bytes) for maintenance cost warnings'),
    \\  ('compare_bench_runs', '5', 'Number of benchmark runs for vizier_compare'),
    \\  ('cost_weight_time', '1.0', 'Weight for query time impact in scoring (higher = time matters more)'),
    \\  ('cost_weight_frequency', '1.0', 'Weight for query frequency in scoring (higher = frequency matters more)'),
    \\  ('cost_weight_selectivity', '1.5', 'Boost for high-selectivity columns (higher = more index-friendly)')
;

const create_rec_id_seq_sql =
    "create sequence if not exists vizier.rec_id_seq start 1";

const create_action_id_seq_sql =
    "create sequence if not exists vizier.action_id_seq start 1";

const create_bench_id_seq_sql =
    "create sequence if not exists vizier.bench_id_seq start 1";

const create_workload_summary_view_sql =
    \\create view if not exists vizier.workload_summary as
    \\select query_signature as sig, sample_sql as sql, execution_count as runs,
    \\  strftime(last_seen, '%Y-%m-%d %H:%M:%S') as last_seen, avg_time_ms as avg_ms
    \\from vizier.workload_queries
    \\order by execution_count desc
;

const create_inspect_table_macro_sql =
    \\create or replace macro vizier.inspect_table(tbl) as table
    \\  select
    \\    c.table_name::varchar as table_name,
    \\    c.column_name::varchar as column_name,
    \\    c.data_type::varchar as column_type,
    \\    c.is_nullable::boolean as is_nullable,
    \\    t.estimated_size::bigint as estimated_size_bytes,
    \\    t.index_count::bigint as index_count,
    \\    coalesce(s.approx_row_count, 0)::bigint as approx_rows,
    \\    coalesce(s.has_nulls, false)::boolean as has_nulls,
    \\    coalesce(s.compression, '')::varchar as compression,
    \\    coalesce(s.row_group_count, 0)::bigint as row_groups,
    \\    coalesce(p.pred_count, 0)::bigint as predicate_count,
    \\    coalesce(p.pred_kinds, '')::varchar as predicate_kinds
    \\  from duckdb_columns() c
    \\  join duckdb_tables() t
    \\    on c.database_name = t.database_name
    \\    and c.schema_name = t.schema_name
    \\    and c.table_name = t.table_name
    \\  left join (
    \\    select column_name,
    \\      count(distinct row_group_id)::bigint as row_group_count,
    \\      max("count")::bigint as approx_row_count,
    \\      bool_or(stats like '%Has Null: true%') as has_nulls,
    \\      max(compression)::varchar as compression
    \\    from pragma_storage_info(tbl)
    \\    where segment_type != 'VALIDITY'
    \\    group by column_name
    \\  ) s on c.column_name = s.column_name
    \\  left join (
    \\    select column_name,
    \\      count(*)::bigint as pred_count,
    \\      string_agg(distinct predicate_kind, ',') as pred_kinds
    \\    from vizier.workload_predicates
    \\    where table_name = tbl
    \\    group by column_name
    \\  ) p on c.column_name = p.column_name
    \\  where c.table_name = tbl
    \\    and c.internal = false
    \\  order by c.column_index
;

const create_explain_macro_sql =
    \\create or replace macro vizier.explain(rec_id) as table
    \\  select
    \\    r.recommendation_id as id,
    \\    r.kind,
    \\    r.table_name,
    \\    r.columns_json as columns,
    \\    r.score,
    \\    r.confidence,
    \\    r.reason,
    \\    r.sql_text as sql,
    \\    r.status,
    \\    strftime(r.created_at, '%Y-%m-%d %H:%M:%S') as created_at
    \\  from vizier.recommendation_store r
    \\  where r.recommendation_id = rec_id
;

const create_overview_macro_sql =
    \\create or replace macro vizier.overview() as table
    \\  select
    \\    t.table_name,
    \\    t.estimated_size::bigint as size_bytes,
    \\    t.column_count::bigint as cols,
    \\    t.index_count::bigint as indexes,
    \\    t.has_primary_key::boolean as has_pk,
    \\    coalesce(p.total_predicates, 0)::bigint as predicates,
    \\    coalesce(p.distinct_columns, 0)::bigint as hot_cols,
    \\    coalesce(r.pending_recs, 0)::bigint as pending_recs
    \\  from duckdb_tables() t
    \\  left join (
    \\    select table_name,
    \\      count(*)::bigint as total_predicates,
    \\      count(distinct column_name)::bigint as distinct_columns
    \\    from vizier.workload_predicates
    \\    group by table_name
    \\  ) p on t.table_name = p.table_name
    \\  left join (
    \\    select table_name,
    \\      count(*)::bigint as pending_recs
    \\    from vizier.recommendation_store
    \\    where status = 'pending'
    \\    group by table_name
    \\  ) r on t.table_name = r.table_name
    \\  where t.internal = false
    \\    and t.schema_name not in ('vizier', 'information_schema')
    \\  order by coalesce(p.total_predicates, 0) desc, t.table_name
;

const create_score_breakdown_macro_sql =
    \\create or replace macro vizier.score_breakdown(rec_id) as table
    \\  select
    \\    r.recommendation_id as id,
    \\    r.kind,
    \\    r.table_name,
    \\    p.column_name,
    \\    p.predicate_kind,
    \\    count(*)::bigint as frequency,
    \\    case
    \\      when r.kind = 'create_index' then round(least(count(*)::double / 10.0, 1.0), 4)
    \\      when r.kind = 'rewrite_sorted_table' then round(least(count(*)::double / 20.0, 1.0), 4)
    \\      else 0.0
    \\    end as column_contribution,
    \\    r.score as total_score,
    \\    r.confidence
    \\  from vizier.recommendation_store r
    \\  join vizier.workload_predicates p
    \\    on r.table_name = p.table_name
    \\  where r.recommendation_id = rec_id
    \\    and (
    \\      (r.kind = 'create_index'
    \\        and p.predicate_kind in ('equality', 'in_list')
    \\        and r.columns_json like '%' || p.column_name || '%')
    \\      or
    \\      (r.kind = 'rewrite_sorted_table'
    \\        and p.predicate_kind in ('equality', 'range'))
    \\    )
    \\  group by r.recommendation_id, r.kind, r.table_name, r.score, r.confidence,
    \\    p.column_name, p.predicate_kind
    \\  order by frequency desc
;

const create_compare_macro_sql =
    \\create or replace macro vizier.compare(rec_id) as table
    \\  select
    \\    b.benchmark_id,
    \\    r.recommendation_id,
    \\    r.kind,
    \\    r.table_name,
    \\    r.status as rec_status,
    \\    b.query_signature,
    \\    b.baseline_ms,
    \\    b.candidate_ms,
    \\    b.speedup,
    \\    case
    \\      when b.speedup > 2.0 then 'excellent'
    \\      when b.speedup > 1.5 then 'good'
    \\      when b.speedup > 1.0 then 'marginal'
    \\      when b.speedup is null then 'no data'
    \\      else 'regression'
    \\    end::varchar as verdict,
    \\    strftime(b.recorded_at, '%Y-%m-%d %H:%M:%S') as recorded_at
    \\  from vizier.recommendation_store r
    \\  left join vizier.benchmark_results b
    \\    on r.recommendation_id = b.recommendation_id
    \\  where r.recommendation_id = rec_id
;

const create_analyze_table_macro_sql =
    \\create or replace macro vizier.analyze_table(tbl) as table
    \\  select
    \\    r.recommendation_id as id,
    \\    r.kind,
    \\    r.table_name,
    \\    r.columns_json as columns,
    \\    r.score,
    \\    r.confidence,
    \\    r.reason,
    \\    r.sql_text as sql,
    \\    r.status
    \\  from vizier.recommendation_store r
    \\  where r.table_name = tbl
    \\  order by r.score desc
;

const create_analyze_workload_macro_sql =
    \\create or replace macro vizier.analyze_workload(since, min_executions) as table
    \\  select
    \\    w.query_signature as sig,
    \\    w.sample_sql as sql,
    \\    w.execution_count as runs,
    \\    strftime(w.last_seen, '%Y-%m-%d %H:%M:%S') as last_seen,
    \\    w.avg_time_ms as avg_ms,
    \\    w.tables_json as tables,
    \\    w.columns_json as columns
    \\  from vizier.workload_queries w
    \\  where w.last_seen >= since
    \\    and w.execution_count >= min_executions
    \\  order by w.execution_count desc
;

const create_explain_human_macro_sql =
    \\create or replace macro vizier.explain_human(rec_id) as table
    \\  select
    \\    r.recommendation_id as id,
    \\    r.kind,
    \\    r.table_name,
    \\    case r.kind
    \\      when 'create_index' then
    \\        'Your queries frequently filter ' || r.table_name || ' by ' || r.columns_json
    \\        || '. Adding an index would let DuckDB find matching rows in O(log n) instead of scanning '
    \\        || case when coalesce(t.estimated_size, 0) > 0
    \\          then 'all ' || t.estimated_size::varchar || ' bytes'
    \\          else 'the entire table'
    \\        end || '.'
    \\        || case when r.estimated_gain >= 0.8 then ' Estimated speedup: 10-50x for matching queries.'
    \\                when r.estimated_gain >= 0.4 then ' Estimated speedup: 2-10x for matching queries.'
    \\                else ' Estimated speedup: up to 2x for matching queries.'
    \\        end
    \\        || case when coalesce(t.estimated_size, 0) > 100000000
    \\          then ' Write overhead: adds ~3-5% to INSERT/UPDATE operations on this '
    \\            || (coalesce(t.estimated_size, 0) / 1000000)::bigint || 'MB table.'
    \\          else ' Write overhead: minimal (small table).'
    \\        end
    \\        || case when r.confidence >= 0.9 then ' High confidence.'
    \\                when r.confidence >= 0.7 then ' Good confidence.'
    \\                else ' Moderate confidence; verify with a benchmark.'
    \\        end
    \\      when 'rewrite_sorted_table' then
    \\        'Table ' || r.table_name || ' would benefit from being sorted by ' || r.columns_json
    \\        || '. DuckDB stores data in row groups and can skip entire groups when the sort order matches your filter columns.'
    \\        || ' This is especially effective for range queries (>=, BETWEEN).'
    \\        || case when coalesce(t.estimated_size, 0) > 10000000
    \\          then ' Estimated speedup: 2-20x for range scans on this '
    \\            || (coalesce(t.estimated_size, 0) / 1000000)::bigint || 'MB table.'
    \\          else ' Estimated speedup: modest for small tables, significant as data grows.'
    \\        end
    \\        || ' Note: this rewrites the entire table and is not reversible.'
    \\      when 'drop_redundant_index' then
    \\        'Index ' || r.columns_json || ' on ' || r.table_name
    \\        || ' is redundant because a wider index already covers the same columns.'
    \\        || ' Dropping it saves storage and removes ~2% write overhead with no impact on read performance.'
    \\      when 'parquet_sort_order' then
    \\        'When exporting ' || r.table_name || ' to Parquet, sorting by ' || r.columns_json
    \\        || ' will produce row groups with min/max ranges that DuckDB can use to skip irrelevant data during reads.'
    \\      when 'parquet_partition' then
    \\        'Partitioning ' || r.table_name || ' by ' || r.columns_json
    \\        || ' when exporting to Parquet will create separate files per partition value.'
    \\        || ' This lets DuckDB skip entire files for queries that filter on the partition column.'
    \\      when 'create_summary_table' then
    \\        'Your workload repeatedly aggregates ' || r.table_name || ' grouped by ' || r.columns_json
    \\        || '. A precomputed summary table would serve these queries instantly instead of re-scanning the source data each time.'
    \\      when 'sort_for_join' then
    \\        'Sorting ' || r.table_name || ' by ' || r.columns_json
    \\        || ' would enable merge joins with other tables joined on this column, which can be significantly faster than hash joins for large datasets.'
    \\      when 'no_action' then
    \\        'Table ' || r.table_name || ' looks well-optimized for your current workload. No changes recommended.'
    \\      else r.reason
    \\    end as explanation,
    \\    r.sql_text as suggested_sql,
    \\    round(r.score, 2) as score,
    \\    round(r.confidence, 2) as confidence
    \\  from vizier.recommendation_store r
    \\  left join duckdb_tables() t on r.table_name = t.table_name
    \\  where r.recommendation_id = rec_id
;

const create_recommendations_view_sql =
    \\create or replace view vizier.recommendations as
    \\select recommendation_id, kind, table_name, columns_json,
    \\  score, confidence, reason, sql_text, status, created_at,
    \\  estimated_gain, estimated_build_cost, estimated_maintenance_cost
    \\from vizier.recommendation_store
    \\where status = 'pending'
    \\order by score desc
;

const create_replay_summary_view_sql =
    \\create or replace view vizier.replay_summary as
    \\select
    \\  r.query_signature,
    \\  r.sample_sql,
    \\  round(r.avg_ms, 2) as avg_ms,
    \\  r.status,
    \\  case
    \\    when w.avg_time_ms > 0 and r.avg_ms > w.avg_time_ms * 1.2 then 'regression'
    \\    when w.avg_time_ms > 0 and r.avg_ms < w.avg_time_ms * 0.8 then 'improved'
    \\    else 'stable'
    \\  end as verdict,
    \\  strftime(r.replayed_at, '%Y-%m-%d %H:%M:%S') as replayed_at
    \\from vizier.replay_results r
    \\left join vizier.workload_queries w on r.query_signature = w.query_signature
    \\order by r.avg_ms desc
;

const create_replay_totals_view_sql =
    \\create or replace view vizier.replay_totals as
    \\select
    \\  count(*) as queries_replayed,
    \\  round(sum(r.avg_ms), 2) as replay_total_ms,
    \\  round(coalesce(sum(w.avg_time_ms), 0.0), 2) as baseline_total_ms,
    \\  case when coalesce(sum(w.avg_time_ms), 0.0) > 0
    \\    then round(sum(r.avg_ms) / sum(w.avg_time_ms), 2)
    \\    else null
    \\  end as ratio,
    \\  case when coalesce(sum(w.avg_time_ms), 0.0) > 0 and sum(r.avg_ms) < sum(w.avg_time_ms) * 0.9
    \\    then 'improved'
    \\    when coalesce(sum(w.avg_time_ms), 0.0) > 0 and sum(r.avg_ms) > sum(w.avg_time_ms) * 1.1
    \\    then 'regressed'
    \\    else 'stable'
    \\  end as overall_verdict,
    \\  sum(case when r.status = 'error' then 1 else 0 end)::bigint as errors
    \\from vizier.replay_results r
    \\left join vizier.workload_queries w on r.query_signature = w.query_signature
;

const create_change_history_view_sql =
    \\create or replace view vizier.change_history as
    \\select
    \\  a.action_id,
    \\  a.recommendation_id,
    \\  r.kind,
    \\  r.table_name,
    \\  a.sql_text,
    \\  a.inverse_sql,
    \\  a.success,
    \\  a.notes,
    \\  strftime(a.applied_at, '%Y-%m-%d %H:%M:%S') as applied_at,
    \\  case when a.inverse_sql != '' then true else false end as can_rollback
    \\from vizier.applied_actions a
    \\join vizier.recommendation_store r on a.recommendation_id = r.recommendation_id
    \\order by a.action_id desc
;

// Schema migrations for existing databases.
// These run after table creation and use "IF NOT EXISTS" to be idempotent.
const migrate_add_estimated_rows_sql =
    \\alter table vizier.workload_queries add column if not exists estimated_rows bigint default 0
;

const ddl_statements = [_][*:0]const u8{
    create_schema_sql,
    create_settings_sql,
    create_session_log_sql,
    insert_default_settings_sql,
    create_rec_id_seq_sql,
    create_action_id_seq_sql,
    create_bench_id_seq_sql,
    create_workload_queries_sql,
    migrate_add_estimated_rows_sql,
    create_workload_predicates_sql,
    create_recommendation_store_sql,
    create_applied_actions_sql,
    create_benchmark_results_sql,
    create_replay_results_sql,
    create_workload_summary_view_sql,
    create_inspect_table_macro_sql,
    create_explain_macro_sql,
    create_overview_macro_sql,
    create_score_breakdown_macro_sql,
    create_compare_macro_sql,
    create_analyze_table_macro_sql,
    create_analyze_workload_macro_sql,
    create_explain_human_macro_sql,
    create_recommendations_view_sql,
    create_replay_summary_view_sql,
    create_replay_totals_view_sql,
    create_change_history_view_sql,
};

/// Initialize the vizier schema and all metadata tables.
/// Uses the provided database handle to open connections and run DDL.
pub fn initSchema(db: duckdb.duckdb_database) bool {
    for (ddl_statements) |sql| {
        sql_runner.run(db, sql) catch return false;
    }
    return true;
}
