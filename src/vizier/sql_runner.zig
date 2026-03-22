const std = @import("std");
const duckdb = @import("duckdb");

/// Access the DuckDB extension API global (defined in extension.c via DUCKDB_EXTENSION_GLOBAL).
/// All DuckDB function calls in extension context go through this struct.
extern var duckdb_ext_api: duckdb.duckdb_ext_api_v1;

const DuckDBSuccess: c_uint = 0;

pub const RunError = error{
    ConnectFailed,
    QueryFailed,
};

/// Execute a SQL statement on an existing connection.
/// The caller owns the connection — this function does not open or close it.
pub fn runOnConn(conn: duckdb.duckdb_connection, sql: [*:0]const u8) RunError!void {
    var result: duckdb.duckdb_result = std.mem.zeroes(duckdb.duckdb_result);
    if (duckdb_ext_api.duckdb_query.?(conn, sql, &result) != DuckDBSuccess) {
        duckdb_ext_api.duckdb_destroy_result.?(&result);
        return RunError.QueryFailed;
    }
    duckdb_ext_api.duckdb_destroy_result.?(&result);
}

/// Execute a SQL statement against a duckdb_database handle.
/// Opens a connection, runs the query, cleans up.
pub fn run(db: duckdb.duckdb_database, sql: [*:0]const u8) RunError!void {
    var conn: duckdb.duckdb_connection = null;
    if (duckdb_ext_api.duckdb_connect.?(db, &conn) != DuckDBSuccess) {
        return RunError.ConnectFailed;
    }
    defer duckdb_ext_api.duckdb_disconnect.?(&conn);

    var result: duckdb.duckdb_result = std.mem.zeroes(duckdb.duckdb_result);
    if (duckdb_ext_api.duckdb_query.?(conn, sql, &result) != DuckDBSuccess) {
        duckdb_ext_api.duckdb_destroy_result.?(&result);
        return RunError.QueryFailed;
    }
    duckdb_ext_api.duckdb_destroy_result.?(&result);
}
