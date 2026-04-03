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
/// The caller owns the connection; this function does not open or close it.
pub fn runOnConn(conn: duckdb.duckdb_connection, sql: [*:0]const u8) RunError!void {
    var result: duckdb.duckdb_result = std.mem.zeroes(duckdb.duckdb_result);
    if (duckdb_ext_api.duckdb_query.?(conn, sql, &result) != DuckDBSuccess) {
        duckdb_ext_api.duckdb_destroy_result.?(&result);
        return RunError.QueryFailed;
    }
    duckdb_ext_api.duckdb_destroy_result.?(&result);
}

/// Execute a query on an existing connection and return the first varchar value
/// from column `col` of the first row. Writes into the caller-provided buffer.
/// Returns the slice on success, or null if the query fails or returns no rows.
pub fn queryFirstVarchar(conn: duckdb.duckdb_connection, sql: [*:0]const u8, col: u64, buf: []u8) ?[]const u8 {
    var result: duckdb.duckdb_result = std.mem.zeroes(duckdb.duckdb_result);
    if (duckdb_ext_api.duckdb_query.?(conn, sql, &result) != DuckDBSuccess) {
        duckdb_ext_api.duckdb_destroy_result.?(&result);
        return null;
    }
    defer duckdb_ext_api.duckdb_destroy_result.?(&result);

    var chunk = duckdb_ext_api.duckdb_fetch_chunk.?(result);
    if (chunk == null) return null;
    defer duckdb_ext_api.duckdb_destroy_data_chunk.?(&chunk);

    const row_count = duckdb_ext_api.duckdb_data_chunk_get_size.?(chunk);
    if (row_count == 0) return null;

    const vector = duckdb_ext_api.duckdb_data_chunk_get_vector.?(chunk, col);
    const raw_data = duckdb_ext_api.duckdb_vector_get_data.?(vector) orelse return null;
    const strings: [*]duckdb.duckdb_string_t = @ptrCast(@alignCast(raw_data));

    const str = &strings[0];
    const len = duckdb_ext_api.duckdb_string_t_length.?(str.*);
    const data = duckdb_ext_api.duckdb_string_t_data.?(str);
    if (data == null) return null;

    const src: []const u8 = @as([*]const u8, @ptrCast(data))[0..len];
    if (src.len > buf.len) return null;

    @memcpy(buf[0..src.len], src);
    return buf[0..src.len];
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
