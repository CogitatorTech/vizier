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
    \\    columns_json varchar default '[]'
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
    \\    recorded_at timestamp default current_timestamp
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
    \\  ('min_confidence', '0.4', 'Minimum confidence threshold for recommendations'),
    \\  ('benchmark_runs', '5', 'Default number of benchmark iterations'),
    \\  ('max_pending_captures', '4096', 'Maximum queries in capture buffer'),
    \\  ('auto_flush', 'false', 'Automatically flush after each capture')
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

const create_recommendations_view_sql =
    \\create or replace view vizier.recommendations as
    \\select recommendation_id, kind, table_name, columns_json,
    \\  score, confidence, reason, sql_text, status, created_at,
    \\  estimated_gain, estimated_build_cost, estimated_maintenance_cost
    \\from vizier.recommendation_store
    \\where status = 'pending'
    \\order by score desc
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
    create_workload_predicates_sql,
    create_recommendation_store_sql,
    create_applied_actions_sql,
    create_benchmark_results_sql,
    create_workload_summary_view_sql,
    create_inspect_table_macro_sql,
    create_explain_macro_sql,
    create_overview_macro_sql,
    create_score_breakdown_macro_sql,
    create_compare_macro_sql,
    create_analyze_table_macro_sql,
    create_analyze_workload_macro_sql,
    create_recommendations_view_sql,
};

/// Initialize the vizier schema and all metadata tables.
/// Uses the provided database handle to open connections and run DDL.
pub fn initSchema(db: duckdb.duckdb_database) bool {
    for (ddl_statements) |sql| {
        sql_runner.run(db, sql) catch return false;
    }
    return true;
}
