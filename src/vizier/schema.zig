const duckdb = @import("duckdb");
const sql_runner = @import("sql_runner.zig");

const create_schema_sql =
    "create schema if not exists vizier"
;

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
    \\    frequency integer default 1
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
    \\    status varchar default 'pending'
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

const create_rec_id_seq_sql =
    "create sequence if not exists vizier.rec_id_seq start 1"
;

const create_action_id_seq_sql =
    "create sequence if not exists vizier.action_id_seq start 1"
;

const create_bench_id_seq_sql =
    "create sequence if not exists vizier.bench_id_seq start 1"
;

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
    \\    coalesce(p.pred_count, 0)::bigint as predicate_count,
    \\    coalesce(p.pred_kinds, '')::varchar as predicate_kinds
    \\  from duckdb_columns() c
    \\  join duckdb_tables() t
    \\    on c.database_name = t.database_name
    \\    and c.schema_name = t.schema_name
    \\    and c.table_name = t.table_name
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

const create_recommendations_view_sql =
    \\create or replace view vizier.recommendations as
    \\select recommendation_id, kind, table_name, columns_json,
    \\  score, confidence, reason, sql_text, status, created_at
    \\from vizier.recommendation_store
    \\where status = 'pending'
    \\order by score desc
;

const ddl_statements = [_][*:0]const u8{
    create_schema_sql,
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
