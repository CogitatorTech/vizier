const std = @import("std");

/// SQL to run the index advisor.
/// Finds columns with high-frequency equality/in_list predicates that have no existing index.
/// Detects point lookups (single-predicate queries) and join predicates (multi-table queries)
/// and boosts their scores accordingly.
/// Inserts recommendations into vizier.recommendation_store.
pub const index_advisor_sql: [*:0]const u8 =
    \\insert into vizier.recommendation_store
    \\  (kind, table_name, columns_json, score, confidence, reason, sql_text,
    \\   estimated_gain, estimated_build_cost, estimated_maintenance_cost)
    \\select
    \\  'create_index',
    \\  p.table_name,
    \\  '["' || p.column_name || '"]',
    \\  least((p.freq + p.point_lookups + p.join_hits)::double
    \\    / (select value::double from vizier.settings where key = 'index_score_divisor'), 1.0)
    \\    * case
    \\      when coalesce(t.estimated_size, 0) > (select value::bigint from vizier.settings where key = 'large_table_bytes') * 10 then 0.7
    \\      when coalesce(t.estimated_size, 0) > (select value::bigint from vizier.settings where key = 'large_table_bytes') then 0.85
    \\      else 1.0
    \\    end
    \\    * (1.0 + least(coalesce(p.total_impact, 0.0) / 1000.0, 1.0))
    \\    * case when coalesce(p.min_selectivity, 1.0) < 0.01
    \\        then (select value::double from vizier.settings where key = 'cost_weight_selectivity')
    \\           when coalesce(p.min_selectivity, 1.0) < 0.1 then 1.2
    \\           else 1.0
    \\    end,
    \\  case
    \\    when p.point_lookups > 0 then 0.95
    \\    when p.join_hits > 0 then 0.9
    \\    when p.freq >= 5 then 0.9
    \\    when p.freq >= 2 then 0.7
    \\    else 0.5
    \\  end,
    \\  'Column ' || p.column_name || ' on ' || p.table_name || ': '
    \\    || p.freq || ' predicate(s)'
    \\    || case when p.point_lookups > 0 then ', ' || p.point_lookups || ' point lookup(s)' else '' end
    \\    || case when p.join_hits > 0 then ', ' || p.join_hits || ' join predicate(s)' else '' end
    \\    || case when coalesce(t.estimated_size, 0) > 100000000 then ' (high write cost: ' || (coalesce(t.estimated_size, 0) / 1000000)::bigint || 'MB table)' else '' end,
    \\  'create index idx_' || p.table_name || '_' || p.column_name || ' on ' || p.table_name || '(' || p.column_name || ')',
    \\  least((p.freq + p.point_lookups + p.join_hits)::double / 5.0, 1.0),
    \\  coalesce(t.estimated_size, 0)::double / 1000000000.0,
    \\  case when coalesce(t.estimated_size, 0) > 100000000 then 0.3 else 0.1 end
    \\from (
    \\  select
    \\    sub.table_name,
    \\    sub.column_name,
    \\    count(*) as freq,
    \\    sum(case when sub.is_point_lookup then 1 else 0 end) as point_lookups,
    \\    sum(case when sub.is_join then 1 else 0 end) as join_hits,
    \\    sum(coalesce(sub.query_impact, 0.0)) as total_impact,
    \\    min(coalesce(sub.avg_selectivity, 1.0)) as min_selectivity
    \\  from (
    \\    select p1.*,
    \\      (select count(*) from vizier.workload_predicates p2
    \\        where p2.query_signature = p1.query_signature
    \\        and p2.predicate_kind in ('equality', 'range', 'in_list', 'like')
    \\      ) = 1 as is_point_lookup,
    \\      (select w.tables_json from vizier.workload_queries w
    \\        where w.query_signature = p1.query_signature
    \\      ) like '%,%' as is_join,
    \\      (select w.total_time_ms * w.execution_count from vizier.workload_queries w
    \\        where w.query_signature = p1.query_signature
    \\      ) as query_impact
    \\    from vizier.workload_predicates p1
    \\    where p1.predicate_kind in ('equality', 'in_list')
    \\  ) sub
    \\  group by sub.table_name, sub.column_name
    \\) p
    \\left join duckdb_tables() t on p.table_name = t.table_name
    \\where not exists (
    \\  select 1 from vizier.recommendation_store r
    \\  where r.kind = 'create_index'
    \\    and r.table_name = p.table_name
    \\    and r.columns_json = '["' || p.column_name || '"]'
    \\    and r.status = 'pending'
    \\)
;

/// SQL to run the sort/clustering advisor.
/// Finds tables with repeated range/equality predicates that could benefit from sorting.
/// Recommends rewriting the table sorted by the most frequently filtered columns.
pub const sort_advisor_sql: [*:0]const u8 =
    \\insert into vizier.recommendation_store
    \\  (kind, table_name, columns_json, score, confidence, reason, sql_text,
    \\   estimated_gain, estimated_build_cost, estimated_maintenance_cost)
    \\select
    \\  'rewrite_sorted_table',
    \\  p.table_name,
    \\  '[' || string_agg('"' || p.column_name || '"', ',' order by p.freq desc) || ']',
    \\  least(sum(p.freq)::double / (select value::double from vizier.settings where key = 'sort_score_divisor'), 1.0)
    \\    * case
    \\      when coalesce(max(t.estimated_size), 0) > 10000000 then 1.2
    \\      when coalesce(max(t.estimated_size), 0) > 1000000 then 1.1
    \\      else 1.0
    \\    end,
    \\  case
    \\    when count(*)::double / greatest(coalesce(max(t.column_count), 1), 1) >= 0.5 then 0.9
    \\    when sum(p.freq) >= 10 then 0.85
    \\    when sum(p.freq) >= 3 then 0.6
    \\    else 0.4
    \\  end,
    \\  'Table ' || p.table_name || ' has ' || sum(p.freq) || ' range/equality predicates across '
    \\    || count(*) || ' of ' || coalesce(max(t.column_count), 0) || ' column(s)'
    \\    || ' (sort quality: ' || round(count(*)::double / greatest(coalesce(max(t.column_count), 1), 1) * 100, 0)::bigint || '%)'
    \\    || '; rewriting sorted may improve scan pruning'
    \\    || case when coalesce(max(t.estimated_size), 0) > 1000000
    \\      then ' (estimated ' || (coalesce(max(t.estimated_size), 0) / 1000000)::bigint || 'MB — row-group pruning likely beneficial)'
    \\      else ''
    \\    end,
    \\  'create or replace table ' || p.table_name || ' as select * from ' || p.table_name || ' order by ' || string_agg(p.column_name, ', ' order by p.freq desc),
    \\  least(sum(p.freq)::double / 10.0, 1.0),
    \\  coalesce(max(t.estimated_size), 0)::double / 500000000.0,
    \\  0.0
    \\from (
    \\  select table_name, column_name, count(*) as freq
    \\  from vizier.workload_predicates
    \\  where predicate_kind in ('equality', 'range')
    \\  group by table_name, column_name
    \\  having count(*) >= (select value::integer from vizier.settings where key = 'sort_min_predicates')
    \\) p
    \\left join duckdb_tables() t on p.table_name = t.table_name
    \\where not exists (
    \\  select 1 from vizier.recommendation_store r
    \\  where r.kind = 'rewrite_sorted_table'
    \\    and r.table_name = p.table_name
    \\    and r.status = 'pending'
    \\)
    \\group by p.table_name
;

/// SQL to run the redundant index advisor.
/// Detects indexes whose columns are a prefix of another index on the same table.
/// Recommends dropping the redundant (shorter) index.
pub const redundant_index_advisor_sql: [*:0]const u8 =
    \\insert into vizier.recommendation_store
    \\  (kind, table_name, columns_json, score, confidence, reason, sql_text,
    \\   estimated_gain, estimated_build_cost, estimated_maintenance_cost)
    \\select
    \\  'drop_redundant_index',
    \\  i1.table_name,
    \\  '["' || i1.index_name || '"]',
    \\  0.7,
    \\  0.85,
    \\  'Index ' || i1.index_name || ' (' || i1.expressions || ') on ' || i1.table_name
    \\    || ' is redundant — covered by ' || i2.index_name || ' (' || i2.expressions || ')',
    \\  'drop index ' || i1.index_name,
    \\  0.3, 0.0, -0.1
    \\from duckdb_indexes() i1
    \\join duckdb_indexes() i2
    \\  on i1.table_name = i2.table_name
    \\  and i1.database_name = i2.database_name
    \\  and i1.schema_name = i2.schema_name
    \\  and i1.index_oid != i2.index_oid
    \\  and starts_with(i2.expressions, replace(i1.expressions, ']', ''))
    \\  and length(i2.expressions) > length(i1.expressions)
    \\where not exists (
    \\  select 1 from vizier.recommendation_store r
    \\  where r.kind = 'drop_redundant_index'
    \\    and r.table_name = i1.table_name
    \\    and r.columns_json = '["' || i1.index_name || '"]'
    \\    and r.status = 'pending'
    \\)
    \\  and i1.schema_name not in ('vizier', 'information_schema')
;

/// SQL to run the parquet layout advisor.
/// Recommends sort order for Parquet exports based on workload predicates.
/// Targets tables with range/equality predicates to improve Parquet row-group pruning.
pub const parquet_sort_advisor_sql: [*:0]const u8 =
    \\insert into vizier.recommendation_store
    \\  (kind, table_name, columns_json, score, confidence, reason, sql_text,
    \\   estimated_gain, estimated_build_cost, estimated_maintenance_cost)
    \\select
    \\  'parquet_sort_order',
    \\  p.table_name,
    \\  '[' || string_agg('"' || p.column_name || '"', ',' order by p.freq desc) || ']',
    \\  least(sum(p.freq)::double / 15.0, 1.0),
    \\  case when count(*) >= 3 then 0.85 when count(*) >= 2 then 0.7 else 0.5 end,
    \\  'Export ' || p.table_name || ' to Parquet sorted by '
    \\    || string_agg(p.column_name, ', ' order by p.freq desc)
    \\    || ' for optimal row-group pruning (' || sum(p.freq) || ' predicates across ' || count(*) || ' column(s))',
    \\  'copy (select * from ' || p.table_name || ' order by '
    \\    || string_agg(p.column_name, ', ' order by p.freq desc)
    \\    || ') to ''' || p.table_name || '.parquet'' (format parquet)',
    \\  least(sum(p.freq)::double / 10.0, 1.0), 0.1, 0.0
    \\from (
    \\  select table_name, column_name, count(*) as freq
    \\  from vizier.workload_predicates
    \\  where predicate_kind in ('equality', 'range')
    \\  group by table_name, column_name
    \\) p
    \\where not exists (
    \\  select 1 from vizier.recommendation_store r
    \\  where r.kind = 'parquet_sort_order'
    \\    and r.table_name = p.table_name
    \\    and r.status = 'pending'
    \\)
    \\group by p.table_name
    \\having count(*) >= 1
;

/// SQL to run the parquet partitioning advisor.
/// Recommends partitioning by low-frequency equality columns (potential partition keys).
pub const parquet_partition_advisor_sql: [*:0]const u8 =
    \\insert into vizier.recommendation_store
    \\  (kind, table_name, columns_json, score, confidence, reason, sql_text,
    \\   estimated_gain, estimated_build_cost, estimated_maintenance_cost)
    \\select
    \\  'parquet_partition',
    \\  p.table_name,
    \\  '["' || p.column_name || '"]',
    \\  least(p.freq::double / 8.0, 1.0),
    \\  case when p.freq >= 5 then 0.8 when p.freq >= 2 then 0.6 else 0.4 end,
    \\  'Partition ' || p.table_name || ' by ' || p.column_name
    \\    || ' when exporting to Parquet (' || p.freq || ' equality predicate(s))',
    \\  'copy (select * from ' || p.table_name || ') to ''' || p.table_name
    \\    || '_partitioned'' (format parquet, partition_by (' || p.column_name || '))',
    \\  least(p.freq::double / 5.0, 1.0), 0.2, 0.0
    \\from (
    \\  select table_name, column_name, count(*) as freq
    \\  from vizier.workload_predicates
    \\  where predicate_kind = 'equality'
    \\  group by table_name, column_name
    \\  having count(*) >= 2
    \\) p
    \\where not exists (
    \\  select 1 from vizier.recommendation_store r
    \\  where r.kind = 'parquet_partition'
    \\    and r.table_name = p.table_name
    \\    and r.columns_json = '["' || p.column_name || '"]'
    \\    and r.status = 'pending'
    \\)
;

/// SQL to run the parquet row group size advisor.
/// Recommends row group sizes based on table size and predicate density.
/// Smaller row groups improve pruning for range-heavy workloads on large tables.
pub const parquet_row_group_advisor_sql: [*:0]const u8 =
    \\insert into vizier.recommendation_store
    \\  (kind, table_name, columns_json, score, confidence, reason, sql_text,
    \\   estimated_gain, estimated_build_cost, estimated_maintenance_cost)
    \\select
    \\  'parquet_row_group_size',
    \\  p.table_name,
    \\  '[]',
    \\  least(p.range_preds::double / 10.0, 1.0)
    \\    * case when coalesce(t.estimated_size, 0) > 10000000 then 1.0 else 0.5 end,
    \\  case when p.range_preds >= 5 then 0.8 when p.range_preds >= 2 then 0.6 else 0.4 end,
    \\  'Table ' || p.table_name || ' has ' || p.range_preds || ' range predicate(s) across '
    \\    || p.range_cols || ' column(s)'
    \\    || case when coalesce(t.estimated_size, 0) > 10000000
    \\      then '; smaller row groups (e.g. 50000) may improve pruning on this '
    \\        || (coalesce(t.estimated_size, 0) / 1000000)::bigint || 'MB table'
    \\      else '; consider row_group_size=100000 for better pruning'
    \\    end,
    \\  'copy (select * from ' || p.table_name
    \\    || ' order by ' || p.top_col
    \\    || ') to ''' || p.table_name || '.parquet'' (format parquet, row_group_size '
    \\    || case when coalesce(t.estimated_size, 0) > 10000000 then '50000' else '100000' end
    \\    || ')',
    \\  least(p.range_preds::double / 5.0, 1.0), 0.1, 0.0
    \\from (
    \\  select agg.table_name, agg.range_preds, agg.range_cols, top.column_name as top_col
    \\  from (
    \\    select table_name, count(*) as range_preds, count(distinct column_name) as range_cols
    \\    from vizier.workload_predicates
    \\    where predicate_kind = 'range'
    \\    group by table_name
    \\    having count(*) >= 2
    \\  ) agg
    \\  join (
    \\    select table_name, column_name, count(*) as freq
    \\    from vizier.workload_predicates
    \\    where predicate_kind = 'range'
    \\    group by table_name, column_name
    \\  ) top on agg.table_name = top.table_name
    \\  qualify row_number() over (partition by agg.table_name order by top.freq desc) = 1
    \\) p
    \\left join duckdb_tables() t on p.table_name = t.table_name
    \\where not exists (
    \\  select 1 from vizier.recommendation_store r
    \\  where r.kind = 'parquet_row_group_size'
    \\    and r.table_name = p.table_name
    \\    and r.status = 'pending'
    \\)
;

/// SQL to run the summary table advisor.
/// Detects repeated GROUP BY patterns and recommends precomputed rollup tables.
pub const summary_table_advisor_sql: [*:0]const u8 =
    \\insert into vizier.recommendation_store
    \\  (kind, table_name, columns_json, score, confidence, reason, sql_text,
    \\   estimated_gain, estimated_build_cost, estimated_maintenance_cost)
    \\select
    \\  'create_summary_table',
    \\  p.table_name,
    \\  '[' || string_agg('"' || p.column_name || '"', ',' order by p.freq desc) || ']',
    \\  least(sum(p.freq)::double / 10.0, 1.0),
    \\  case when sum(p.freq) >= 6 then 0.85 when sum(p.freq) >= 3 then 0.7 else 0.5 end,
    \\  'Table ' || p.table_name || ' has ' || sum(p.freq) || ' group-by aggregation(s) across '
    \\    || count(*) || ' column(s); a precomputed summary table may speed up repeated queries',
    \\  'create table vizier_summary_' || p.table_name || ' as select '
    \\    || string_agg(p.column_name, ', ' order by p.freq desc)
    \\    || ', count(*) as cnt from ' || p.table_name
    \\    || ' group by ' || string_agg(p.column_name, ', ' order by p.freq desc),
    \\  least(sum(p.freq)::double / 8.0, 1.0), 0.3, 0.1
    \\from (
    \\  select table_name, column_name, count(*) as freq
    \\  from vizier.workload_predicates
    \\  where predicate_kind = 'group_by'
    \\  group by table_name, column_name
    \\  having count(*) >= 2
    \\) p
    \\where not exists (
    \\  select 1 from vizier.recommendation_store r
    \\  where r.kind = 'create_summary_table'
    \\    and r.table_name = p.table_name
    \\    and r.status = 'pending'
    \\)
    \\group by p.table_name
;

/// SQL to run the join-path advisor.
/// Detects tables that frequently appear together in multi-table queries
/// and recommends sorting both sides by the join key for merge-join compatibility.
pub const join_path_advisor_sql: [*:0]const u8 =
    \\insert into vizier.recommendation_store
    \\  (kind, table_name, columns_json, score, confidence, reason, sql_text,
    \\   estimated_gain, estimated_build_cost, estimated_maintenance_cost)
    \\select
    \\  'sort_for_join',
    \\  p.table_name,
    \\  '["' || p.column_name || '"]',
    \\  least(p.freq::double / 8.0, 1.0),
    \\  case when p.freq >= 5 then 0.85 when p.freq >= 2 then 0.7 else 0.5 end,
    \\  'Column ' || p.column_name || ' on ' || p.table_name || ' is used in '
    \\    || p.freq || ' join predicate(s) across multi-table queries'
    \\    || '; sorting by this column may enable merge joins',
    \\  'create or replace table ' || p.table_name
    \\    || ' as select * from ' || p.table_name
    \\    || ' order by ' || p.column_name,
    \\  least(p.freq::double / 5.0, 1.0), 0.2, 0.0
    \\from (
    \\  select p1.table_name, p1.column_name, count(*) as freq
    \\  from vizier.workload_predicates p1
    \\  where p1.predicate_kind = 'equality'
    \\    and (select w.tables_json from vizier.workload_queries w
    \\         where w.query_signature = p1.query_signature) like '%,%'
    \\  group by p1.table_name, p1.column_name
    \\  having count(*) >= 2
    \\) p
    \\where not exists (
    \\  select 1 from vizier.recommendation_store r
    \\  where r.kind = 'sort_for_join'
    \\    and r.table_name = p.table_name
    \\    and r.columns_json = '["' || p.column_name || '"]'
    \\    and r.status = 'pending'
    \\)
;

/// SQL to run the no-action advisor.
/// Emits 'no_action' for tables that appear in the workload but have no
/// pending actionable recommendations — confirming they are well-optimized.
pub const no_action_advisor_sql: [*:0]const u8 =
    \\insert into vizier.recommendation_store
    \\  (kind, table_name, columns_json, score, confidence, reason, sql_text,
    \\   estimated_gain, estimated_build_cost, estimated_maintenance_cost)
    \\select
    \\  'no_action',
    \\  t.table_name,
    \\  '[]',
    \\  0.0,
    \\  1.0,
    \\  'Table ' || t.table_name || ' appears in the workload and has no pending recommendations — no changes needed',
    \\  '',
    \\  0.0, 0.0, 0.0
    \\from (
    \\  select distinct table_name
    \\  from vizier.workload_predicates
    \\) t
    \\where not exists (
    \\  select 1 from vizier.recommendation_store r
    \\  where r.table_name = t.table_name
    \\    and r.status = 'pending'
    \\    and r.kind != 'no_action'
    \\)
    \\  and not exists (
    \\    select 1 from vizier.recommendation_store r2
    \\    where r2.table_name = t.table_name
    \\      and r2.kind = 'no_action'
    \\      and r2.status = 'pending'
    \\  )
;

/// All advisor SQL statements in execution order.
/// no_action_advisor_sql must be last (it depends on other advisors having run).
pub const all_advisor_sqls = [_][*:0]const u8{
    index_advisor_sql,
    sort_advisor_sql,
    redundant_index_advisor_sql,
    parquet_sort_advisor_sql,
    parquet_partition_advisor_sql,
    parquet_row_group_advisor_sql,
    summary_table_advisor_sql,
    join_path_advisor_sql,
    no_action_advisor_sql,
};

/// SQL to clear stale recommendations (optional, called before re-analysis).
pub const clear_pending_sql: [*:0]const u8 =
    "delete from vizier.recommendation_store where status = 'pending'";

/// SQL to count pending recommendations.
pub const count_recommendations_sql: [*:0]const u8 =
    "select count(*)::bigint from vizier.recommendation_store where status = 'pending'";

/// SQL template to fetch a recommendation's SQL text by ID.
pub fn buildFetchRecommendationSql(buf: []u8, rec_id: i64) ?[*:0]const u8 {
    var fbs = std.io.fixedBufferStream(buf);
    const w = fbs.writer();
    w.print("select sql_text::varchar, kind::varchar, table_name::varchar from vizier.recommendation_store where recommendation_id = {d} and status = 'pending'\x00", .{rec_id}) catch return null;
    return @ptrCast(fbs.getWritten().ptr);
}

/// SQL template to mark a recommendation as applied.
pub fn buildMarkAppliedSql(buf: []u8, rec_id: i64) ?[*:0]const u8 {
    var fbs = std.io.fixedBufferStream(buf);
    const w = fbs.writer();
    w.print("update vizier.recommendation_store set status = 'applied' where recommendation_id = {d}\x00", .{rec_id}) catch return null;
    return @ptrCast(fbs.getWritten().ptr);
}

/// SQL template to log an applied action.
pub fn buildLogActionSql(buf: []u8, rec_id: i64, sql_text: []const u8, success: bool) ?[*:0]const u8 {
    var fbs = std.io.fixedBufferStream(buf);
    const w = fbs.writer();
    w.print("insert into vizier.applied_actions (recommendation_id, sql_text, success, notes) values ({d}, '", .{rec_id}) catch return null;
    // Escape single quotes
    for (sql_text) |c| {
        if (c == '\'') {
            w.writeAll("''") catch return null;
        } else {
            w.writeByte(c) catch return null;
        }
    }
    w.print("', {s}, '{s}')\x00", .{
        if (success) "true" else "false",
        if (success) "Applied successfully" else "Apply failed",
    }) catch return null;
    return @ptrCast(fbs.getWritten().ptr);
}

test "buildFetchRecommendationSql" {
    var buf: [512]u8 = undefined;
    const sql = buildFetchRecommendationSql(&buf, 42);
    try std.testing.expect(sql != null);
    try std.testing.expect(std.mem.indexOf(u8, std.mem.span(sql.?), "42") != null);
}

test "buildMarkAppliedSql" {
    var buf: [512]u8 = undefined;
    const sql = buildMarkAppliedSql(&buf, 7);
    try std.testing.expect(sql != null);
    try std.testing.expect(std.mem.indexOf(u8, std.mem.span(sql.?), "applied") != null);
}

test "buildLogActionSql escapes quotes" {
    var buf: [1024]u8 = undefined;
    const sql = buildLogActionSql(&buf, 1, "create index idx on t('col')", true);
    try std.testing.expect(sql != null);
    try std.testing.expect(std.mem.indexOf(u8, std.mem.span(sql.?), "''col''") != null);
}
