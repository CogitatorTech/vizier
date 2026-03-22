/// SQL query for workload summary.
/// Used by the C table function callback.
pub const summary_sql: [*:0]const u8 =
    \\select query_signature::bigint, sample_sql, execution_count::bigint, last_seen::varchar, avg_time_ms
    \\from vizier.workload_queries
    \\order by execution_count desc
;
