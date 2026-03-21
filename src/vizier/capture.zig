const std = @import("std");
const duckdb = @import("duckdb");
const sql_runner = @import("sql_runner.zig");

pub const CaptureResult = struct {
    query_hash: i64,
    success: bool,
};

/// Hash a SQL string to produce a stable identifier.
pub fn hashQuery(sql: []const u8) u64 {
    return std.hash.Fnv1a_64.hash(sql);
}

/// Capture a query into vizier.workload_queries.
/// Inserts a new row or updates execution_count if the query_hash already exists.
pub fn captureQuery(db: duckdb.duckdb_database, sql_text: [*:0]const u8) CaptureResult {
    const sql_slice = std.mem.span(sql_text);
    const hash = hashQuery(sql_slice);
    const hash_i64: i64 = @bitCast(hash);

    // Build the INSERT ... ON CONFLICT SQL.
    // We need to escape single quotes in the SQL text.
    var buf: [8192]u8 = undefined;
    const insert_sql = buildInsertSql(&buf, hash_i64, sql_slice) orelse {
        return CaptureResult{ .query_hash = hash_i64, .success = false };
    };

    sql_runner.run(db, insert_sql) catch {
        return CaptureResult{ .query_hash = hash_i64, .success = false };
    };

    return CaptureResult{ .query_hash = hash_i64, .success = true };
}

/// Build the INSERT SQL with escaped quotes. Returns a sentinel-terminated slice or null on overflow.
fn buildInsertSql(buf: []u8, hash: i64, sql_text: []const u8) ?[*:0]const u8 {
    // We'll write the SQL manually to handle escaping
    var fbs = std.io.fixedBufferStream(buf);
    const writer = fbs.writer();

    writer.print(
        "INSERT INTO vizier.workload_queries (query_hash, normalized_sql, sample_sql) VALUES ({d}, '",
        .{hash},
    ) catch return null;

    // Escape single quotes in the SQL text
    for (sql_text) |c| {
        if (c == '\'') {
            writer.writeAll("''") catch return null;
        } else {
            writer.writeByte(c) catch return null;
        }
    }

    writer.writeAll("', '") catch return null;

    // Write sample_sql (same as normalized for now)
    for (sql_text) |c| {
        if (c == '\'') {
            writer.writeAll("''") catch return null;
        } else {
            writer.writeByte(c) catch return null;
        }
    }

    writer.print(
        "') ON CONFLICT (query_hash) DO UPDATE SET last_seen = now(), execution_count = workload_queries.execution_count + 1",
        .{},
    ) catch return null;

    // Null-terminate
    writer.writeByte(0) catch return null;

    const written = fbs.getWritten();
    // Return a pointer to the sentinel-terminated string
    return @ptrCast(written.ptr);
}

test "hashQuery is deterministic" {
    const sql = "SELECT * FROM foo WHERE id = 1";
    const h1 = hashQuery(sql);
    const h2 = hashQuery(sql);
    try std.testing.expectEqual(h1, h2);
}

test "hashQuery differs for different inputs" {
    const h1 = hashQuery("SELECT 1");
    const h2 = hashQuery("SELECT 2");
    try std.testing.expect(h1 != h2);
}

test "buildInsertSql handles single quotes" {
    var buf: [8192]u8 = undefined;
    const result = buildInsertSql(&buf, 42, "SELECT * FROM foo WHERE name = 'bar'");
    try std.testing.expect(result != null);
    const sql = std.mem.span(result.?);
    // Should contain escaped quotes
    try std.testing.expect(std.mem.indexOf(u8, sql, "''bar''") != null);
}
