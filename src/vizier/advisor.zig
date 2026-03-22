const std = @import("std");

/// SQL to run the index advisor.
/// Finds columns with high-frequency equality/in_list predicates that have no existing index.
/// Inserts recommendations into vizier.recommendation_store.
pub const index_advisor_sql: [*:0]const u8 =
    \\insert into vizier.recommendation_store
    \\  (kind, table_name, columns_json, score, confidence, reason, sql_text)
    \\select
    \\  'create_index',
    \\  p.table_name,
    \\  '["' || p.column_name || '"]',
    \\  least(p.freq::double / 10.0, 1.0),
    \\  case when p.freq >= 5 then 0.9 when p.freq >= 2 then 0.7 else 0.5 end,
    \\  'Column ' || p.column_name || ' appears in ' || p.freq || ' selective predicate(s) on ' || p.table_name || ' but has no index',
    \\  'create index idx_' || p.table_name || '_' || p.column_name || ' on ' || p.table_name || '(' || p.column_name || ')'
    \\from (
    \\  select table_name, column_name, count(*) as freq
    \\  from vizier.workload_predicates
    \\  where predicate_kind in ('equality', 'in_list')
    \\  group by table_name, column_name
    \\) p
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
    \\  (kind, table_name, columns_json, score, confidence, reason, sql_text)
    \\select
    \\  'rewrite_sorted_table',
    \\  p.table_name,
    \\  '[' || string_agg('"' || p.column_name || '"', ',' order by p.freq desc) || ']',
    \\  least(sum(p.freq)::double / 20.0, 1.0),
    \\  case when sum(p.freq) >= 10 then 0.85 when sum(p.freq) >= 3 then 0.6 else 0.4 end,
    \\  'Table ' || p.table_name || ' has ' || sum(p.freq) || ' range/equality predicates across ' || count(*) || ' column(s); rewriting sorted may improve scan pruning',
    \\  'create or replace table ' || p.table_name || ' as select * from ' || p.table_name || ' order by ' || string_agg(p.column_name, ', ' order by p.freq desc)
    \\from (
    \\  select table_name, column_name, count(*) as freq
    \\  from vizier.workload_predicates
    \\  where predicate_kind in ('equality', 'range')
    \\  group by table_name, column_name
    \\  having count(*) >= 2
    \\) p
    \\where not exists (
    \\  select 1 from vizier.recommendation_store r
    \\  where r.kind = 'rewrite_sorted_table'
    \\    and r.table_name = p.table_name
    \\    and r.status = 'pending'
    \\)
    \\group by p.table_name
;

/// SQL to clear stale recommendations (optional, called before re-analysis).
pub const clear_pending_sql: [*:0]const u8 =
    "delete from vizier.recommendation_store where status = 'pending'"
;

/// SQL to count pending recommendations.
pub const count_recommendations_sql: [*:0]const u8 =
    "select count(*)::bigint from vizier.recommendation_store where status = 'pending'"
;

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
