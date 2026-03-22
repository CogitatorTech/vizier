const std = @import("std");
const duckdb = @import("duckdb");

// Vizier sub-modules
pub const schema = @import("vizier/schema.zig");
pub const capture = @import("vizier/capture.zig");
pub const extract = @import("vizier/extract.zig");
pub const inspect = @import("vizier/inspect.zig");
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

/// Extract predicates/tables from a SQL string and store in metadata tables.
/// Called from flush_pending() in extension.c with the open connection.
pub export fn zig_extract_and_store(
    conn: duckdb.duckdb_connection,
    query_hash: i64,
    sql_ptr: [*:0]const u8,
) callconv(.c) bool {
    const sql_slice = std.mem.span(sql_ptr);
    const result = extract.extractFromSql(sql_slice);

    // Delete existing predicates for this query hash
    var del_buf: [256]u8 = undefined;
    const del_sql = std.fmt.bufPrint(&del_buf, "DELETE FROM vizier.workload_predicates WHERE query_hash = {d}\x00", .{query_hash}) catch return false;
    sql_runner.runOnConn(conn, @ptrCast(del_sql.ptr)) catch {};

    // Insert each predicate
    for (result.predicateSlice()) |pred| {
        var ins_buf: [1024]u8 = undefined;
        const ins_sql = std.fmt.bufPrint(&ins_buf, "INSERT INTO vizier.workload_predicates (query_hash, table_name, column_name, predicate_kind) VALUES ({d}, '{s}', '{s}', '{s}')\x00", .{
            query_hash,
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
    const upd_sql = std.fmt.bufPrint(&upd_buf, "UPDATE vizier.workload_queries SET tables_json = '{s}', columns_json = '{s}' WHERE query_hash = {d}\x00", .{
        tables_json,
        columns_json,
        query_hash,
    }) catch return false;
    sql_runner.runOnConn(conn, @ptrCast(upd_sql.ptr)) catch {};

    return true;
}
