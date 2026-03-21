/// SQL query for workload summary.
/// Used by the C table function callback.
pub const summary_sql: [*:0]const u8 =
    \\SELECT query_hash::BIGINT, sample_sql, execution_count::BIGINT, last_seen::VARCHAR, avg_time_ms
    \\FROM vizier.workload_queries
    \\ORDER BY execution_count DESC
;
