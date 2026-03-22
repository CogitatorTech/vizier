const std = @import("std");
const duckdb = @import("duckdb");

// Vizier sub-modules
pub const schema = @import("vizier/schema.zig");
pub const capture = @import("vizier/capture.zig");
pub const extract = @import("vizier/extract.zig");
pub const inspect = @import("vizier/inspect.zig");
pub const advisor = @import("vizier/advisor.zig");
pub const sql_runner = @import("vizier/sql_runner.zig");

// ============================================================================
// Constants
// ============================================================================

pub const version: [*:0]const u8 = "0.1.0";

// ============================================================================
// C-exported functions called from extension.c
// ============================================================================

/// Initialize the vizier schema and metadata tables.
pub export fn zig_init_schema(db: duckdb.duckdb_database) callconv(.c) bool {
    return schema.initSchema(db);
}

/// Return the vizier version string.
pub export fn zig_get_version() callconv(.c) [*:0]const u8 {
    return version;
}

/// Normalize a SQL string (replace literals with ?, uppercase keywords).
/// Writes into a static buffer and returns the result. Returns the original
/// SQL if normalization fails.
pub export fn zig_normalize_sql(
    sql_ptr: [*:0]const u8,
    out_ptr: *[*]const u8,
    out_len: *usize,
) callconv(.c) void {
    const S = struct {
        var buf: [4096]u8 = undefined;
    };
    const sql_slice = std.mem.span(sql_ptr);
    const normalized = extract.normalizeSql(sql_slice, &S.buf);
    out_ptr.* = normalized.ptr;
    out_len.* = normalized.len;
}

/// Extract predicates/tables from a SQL string and store in metadata tables.
/// Called from flush_pending() in extension.c with the open connection.
pub export fn zig_extract_and_store(
    conn: duckdb.duckdb_connection,
    sig_ptr: [*:0]const u8,
    sql_ptr: [*:0]const u8,
) callconv(.c) bool {
    const sql_slice = std.mem.span(sql_ptr);
    const sig = std.mem.span(sig_ptr);
    const result = extract.extractFromSql(sql_slice);

    // Delete existing predicates for this signature
    var del_buf: [256]u8 = undefined;
    const del_sql = std.fmt.bufPrint(&del_buf, "delete from vizier.workload_predicates where query_signature = '{s}'\x00", .{sig}) catch return false;
    sql_runner.runOnConn(conn, @ptrCast(del_sql.ptr)) catch {};

    // Insert each predicate
    for (result.predicateSlice()) |pred| {
        var ins_buf: [1024]u8 = undefined;
        const ins_sql = std.fmt.bufPrint(&ins_buf, "insert into vizier.workload_predicates (query_signature, table_name, column_name, predicate_kind) values ('{s}', '{s}', '{s}', '{s}')\x00", .{
            sig,
            pred.table_name,
            pred.column_name,
            pred.kind.toStr(),
        }) catch continue;
        sql_runner.runOnConn(conn, @ptrCast(ins_sql.ptr)) catch {};
    }

    // Build JSON and update workload_queries
    var tables_buf: [2048]u8 = undefined;
    var cols_buf: [2048]u8 = undefined;
    const tables_json = extract.buildTablesJson(&result, &tables_buf);
    const columns_json = extract.buildColumnsJson(&result, &cols_buf);

    var upd_buf: [4096]u8 = undefined;
    const upd_sql = std.fmt.bufPrint(&upd_buf, "update vizier.workload_queries set tables_json = '{s}', columns_json = '{s}' where query_signature = '{s}'\x00", .{
        tables_json,
        columns_json,
        sig,
    }) catch return false;
    sql_runner.runOnConn(conn, @ptrCast(upd_sql.ptr)) catch {};

    return true;
}

// ---- Advisor orchestration ----

/// Run all advisors on the given connection.
/// Clears pending recommendations, then runs each advisor SQL.
/// Returns 0 on full success, -1 if clear failed, or the count of
/// individual advisor failures (> 0).
pub export fn zig_run_all_advisors(conn: duckdb.duckdb_connection) callconv(.c) i64 {
    // Clear old pending recommendations
    sql_runner.runOnConn(conn, advisor.clear_pending_sql) catch return -1;

    // Run each advisor in order (no_action must be last)
    var failures: i64 = 0;
    for (advisor.all_advisor_sqls) |sql| {
        sql_runner.runOnConn(conn, sql) catch {
            failures += 1;
        };
    }

    return failures;
}

pub export fn zig_get_count_recommendations_sql() callconv(.c) [*:0]const u8 {
    return advisor.count_recommendations_sql;
}
