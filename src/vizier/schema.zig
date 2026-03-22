const duckdb = @import("duckdb");
const sql_runner = @import("sql_runner.zig");

const create_schema_sql =
    "CREATE SCHEMA IF NOT EXISTS vizier";

const create_workload_queries_sql =
    \\CREATE TABLE IF NOT EXISTS vizier.workload_queries (
    \\    query_hash BIGINT UNIQUE,
    \\    normalized_sql VARCHAR,
    \\    sample_sql VARCHAR,
    \\    first_seen TIMESTAMP DEFAULT current_timestamp,
    \\    last_seen TIMESTAMP DEFAULT current_timestamp,
    \\    execution_count INTEGER DEFAULT 1,
    \\    total_time_ms DOUBLE DEFAULT 0,
    \\    avg_time_ms DOUBLE DEFAULT 0,
    \\    tables_json VARCHAR DEFAULT '[]',
    \\    columns_json VARCHAR DEFAULT '[]'
    \\)
;

const create_workload_predicates_sql =
    \\CREATE TABLE IF NOT EXISTS vizier.workload_predicates (
    \\    query_hash BIGINT,
    \\    table_name VARCHAR,
    \\    column_name VARCHAR,
    \\    predicate_kind VARCHAR,
    \\    frequency INTEGER DEFAULT 1
    \\)
;

const create_recommendation_store_sql =
    \\CREATE TABLE IF NOT EXISTS vizier.recommendation_store (
    \\    recommendation_id INTEGER DEFAULT nextval('vizier.rec_id_seq'),
    \\    created_at TIMESTAMP DEFAULT current_timestamp,
    \\    kind VARCHAR,
    \\    table_name VARCHAR,
    \\    columns_json VARCHAR,
    \\    score DOUBLE,
    \\    confidence DOUBLE,
    \\    reason VARCHAR,
    \\    sql_text VARCHAR,
    \\    status VARCHAR DEFAULT 'pending'
    \\)
;

const create_applied_actions_sql =
    \\CREATE TABLE IF NOT EXISTS vizier.applied_actions (
    \\    action_id INTEGER DEFAULT nextval('vizier.action_id_seq'),
    \\    recommendation_id INTEGER,
    \\    applied_at TIMESTAMP DEFAULT current_timestamp,
    \\    sql_text VARCHAR,
    \\    success BOOLEAN,
    \\    notes VARCHAR
    \\)
;

const create_benchmark_results_sql =
    \\CREATE TABLE IF NOT EXISTS vizier.benchmark_results (
    \\    benchmark_id INTEGER DEFAULT nextval('vizier.bench_id_seq'),
    \\    recommendation_id INTEGER,
    \\    query_hash BIGINT,
    \\    baseline_ms DOUBLE,
    \\    candidate_ms DOUBLE,
    \\    speedup DOUBLE,
    \\    recorded_at TIMESTAMP DEFAULT current_timestamp
    \\)
;

const create_rec_id_seq_sql =
    "CREATE SEQUENCE IF NOT EXISTS vizier.rec_id_seq START 1";

const create_action_id_seq_sql =
    "CREATE SEQUENCE IF NOT EXISTS vizier.action_id_seq START 1";

const create_bench_id_seq_sql =
    "CREATE SEQUENCE IF NOT EXISTS vizier.bench_id_seq START 1";

const create_workload_summary_view_sql =
    \\CREATE VIEW IF NOT EXISTS vizier.workload_summary AS
    \\SELECT query_hash, sample_sql, execution_count, last_seen, avg_time_ms
    \\FROM vizier.workload_queries
    \\ORDER BY execution_count DESC
;

const create_inspect_table_macro_sql =
    \\CREATE OR REPLACE MACRO vizier.inspect_table(tbl) AS TABLE
    \\  SELECT
    \\    c.table_name::VARCHAR AS table_name,
    \\    c.column_name::VARCHAR AS column_name,
    \\    c.data_type::VARCHAR AS column_type,
    \\    c.is_nullable::BOOLEAN AS is_nullable,
    \\    t.estimated_size::BIGINT AS estimated_size_bytes,
    \\    t.index_count::BIGINT AS index_count,
    \\    COALESCE(p.pred_count, 0)::BIGINT AS predicate_count,
    \\    COALESCE(p.pred_kinds, '')::VARCHAR AS predicate_kinds
    \\  FROM duckdb_columns() c
    \\  JOIN duckdb_tables() t
    \\    ON c.database_name = t.database_name
    \\    AND c.schema_name = t.schema_name
    \\    AND c.table_name = t.table_name
    \\  LEFT JOIN (
    \\    SELECT column_name,
    \\      COUNT(*)::BIGINT AS pred_count,
    \\      STRING_AGG(DISTINCT predicate_kind, ',') AS pred_kinds
    \\    FROM vizier.workload_predicates
    \\    WHERE table_name = tbl
    \\    GROUP BY column_name
    \\  ) p ON c.column_name = p.column_name
    \\  WHERE c.table_name = tbl
    \\    AND c.internal = false
    \\  ORDER BY c.column_index
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
};

/// Initialize the vizier schema and all metadata tables.
/// Uses the provided database handle to open connections and run DDL.
pub fn initSchema(db: duckdb.duckdb_database) bool {
    for (ddl_statements) |sql| {
        sql_runner.run(db, sql) catch return false;
    }
    return true;
}
