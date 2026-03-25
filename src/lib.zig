const std = @import("std");
const duckdb = @import("duckdb");

// Vizier's components
pub const schema = @import("vizier/schema.zig");
pub const capture = @import("vizier/capture.zig");
pub const extract = @import("vizier/extract.zig");
pub const inspect = @import("vizier/inspect.zig");
pub const advisor = @import("vizier/advisor.zig");
pub const sql_runner = @import("vizier/sql_runner.zig");
pub const explain = @import("vizier/explain.zig");
pub const dashboard = @import("vizier/dashboard.zig");

// ============================================================================
// Constants
// ============================================================================

const build_options = @import("build_options");
pub const version: [*:0]const u8 = @ptrCast(build_options.version);

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

/// Escape single quotes in a string for safe SQL embedding.
/// Writes into the provided buffer and returns the escaped slice.
fn escapeSql(input: []const u8, buf: []u8) []const u8 {
    var i: usize = 0;
    for (input) |c| {
        if (c == '\'') {
            if (i + 2 > buf.len) return input; // fallback to unescaped if buffer full
            buf[i] = '\'';
            buf[i + 1] = '\'';
            i += 2;
        } else {
            if (i + 1 > buf.len) return input;
            buf[i] = c;
            i += 1;
        }
    }
    return buf[0..i];
}

/// Check if a table name exists in a comma-separated list of real table names.
fn tableExistsIn(name: []const u8, csv_list: []const u8) bool {
    var iter = std.mem.splitScalar(u8, csv_list, ',');
    while (iter.next()) |entry| {
        if (std.mem.eql(u8, entry, name)) return true;
    }
    return false;
}

/// Extract predicates/tables from a SQL string and store in metadata tables.
/// Called from flush_pending() in extension.c with the open connection.
///
/// Uses two extraction strategies:
/// 1. Tokenizer (always runs, works even if tables don't exist in the database)
/// 2. EXPLAIN-based (runs if tables exist, provides more accurate predicates and row estimates)
///
/// If EXPLAIN succeeds and produces predicates, its results replace the tokenizer output.
/// The tokenizer result is kept as a fallback when EXPLAIN fails.
pub export fn zig_extract_and_store(
    conn: duckdb.duckdb_connection,
    sig_ptr: [*:0]const u8,
    sql_ptr: [*:0]const u8,
) callconv(.c) bool {
    const sql_slice = std.mem.span(sql_ptr);
    const sig = std.mem.span(sig_ptr);

    // Strategy 1: tokenizer-based extraction (always works, no DB access needed)
    const tokenizer_result = extract.extractFromSql(sql_slice);

    // Strategy 2: EXPLAIN-based extraction (needs tables to exist in the database)
    var explain_scratch: [32768]u8 = undefined;
    const explain_result = explain.runExplain(conn, sql_slice, &explain_scratch);

    // Use EXPLAIN results if available and non-empty, otherwise fall back to tokenizer
    const result = if (explain_result.ok and explain_result.predicate_count > 0)
        explain_result.toExtractionResult()
    else
        tokenizer_result;

    const estimated_rows = explain_result.estimated_rows;
    const used_explain = explain_result.ok and explain_result.predicate_count > 0;

    // Build a set of real table names to filter out aliases.
    // Only applied when using the tokenizer fallback with real tables in the DB,
    // to catch alias leakage (n1, l1, etc.) without breaking capture of queries
    // that reference tables not yet created.
    var real_tables_buf: [4096]u8 = undefined;
    const real_tables_str = if (!used_explain)
        sql_runner.queryFirstVarchar(
            conn,
            "select string_agg(table_name, ',') from duckdb_tables() where schema_name not in ('vizier', 'information_schema')\x00",
            0,
            &real_tables_buf,
        ) orelse ""
    else
        @as([]const u8, "");

    // Delete existing predicates for this signature
    var del_buf: [256]u8 = undefined;
    const del_sql = std.fmt.bufPrint(&del_buf, "delete from vizier.workload_predicates where query_signature = '{s}'\x00", .{sig}) catch return false;
    sql_runner.runOnConn(conn, @ptrCast(del_sql.ptr)) catch {};

    // Insert each predicate (skip empty names, aliases, and escape for SQL safety)
    for (result.predicateSlice()) |pred| {
        if (pred.table_name.len == 0 or pred.column_name.len == 0) continue;

        // Skip predicates referencing non-existent tables (alias leakage)
        if (real_tables_str.len > 0 and !tableExistsIn(pred.table_name, real_tables_str)) continue;

        var esc_table_buf: [256]u8 = undefined;
        var esc_col_buf: [256]u8 = undefined;
        const esc_table = escapeSql(pred.table_name, &esc_table_buf);
        const esc_col = escapeSql(pred.column_name, &esc_col_buf);

        var ins_buf: [1024]u8 = undefined;
        const ins_sql = std.fmt.bufPrint(&ins_buf, "insert into vizier.workload_predicates (query_signature, table_name, column_name, predicate_kind) values ('{s}', '{s}', '{s}', '{s}')\x00", .{
            sig,
            esc_table,
            esc_col,
            pred.kind.toStr(),
        }) catch continue;
        sql_runner.runOnConn(conn, @ptrCast(ins_sql.ptr)) catch {};
    }

    // Build JSON and update workload_queries (including estimated_rows from EXPLAIN)
    var tables_buf: [2048]u8 = undefined;
    var cols_buf: [2048]u8 = undefined;
    const tables_json = extract.buildTablesJson(&result, &tables_buf);
    const columns_json = extract.buildColumnsJson(&result, &cols_buf);

    var upd_buf: [4096]u8 = undefined;
    const upd_sql = std.fmt.bufPrint(&upd_buf, "update vizier.workload_queries set tables_json = '{s}', columns_json = '{s}', estimated_rows = {d} where query_signature = '{s}'\x00", .{
        tables_json,
        columns_json,
        estimated_rows,
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

    // Prune recommendations below min_confidence threshold
    sql_runner.runOnConn(conn, advisor.prune_low_score_sql) catch {};

    return failures;
}

pub export fn zig_get_count_recommendations_sql() callconv(.c) [*:0]const u8 {
    return advisor.count_recommendations_sql;
}

// ---- Dashboard template accessors ----

pub export fn zig_get_dashboard_before() callconv(.c) [*:0]const u8 {
    return dashboard.template_before;
}

pub export fn zig_get_dashboard_after() callconv(.c) [*:0]const u8 {
    return dashboard.template_after;
}
