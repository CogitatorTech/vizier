const std = @import("std");
const duckdb = @import("duckdb");
const sql_runner = @import("sql_runner.zig");
const extract = @import("extract.zig");

extern var duckdb_ext_api: duckdb.duckdb_ext_api_v1;

// ============================================================================
// Types
// ============================================================================

pub const ExplainResult = struct {
    tables: [extract.MAX_TABLES]extract.TableRef = undefined,
    table_count: usize = 0,
    predicates: [extract.MAX_PREDICATES]extract.Predicate = undefined,
    predicate_count: usize = 0,
    estimated_rows: u64 = 0,
    ok: bool = false,

    pub fn toExtractionResult(self: *const ExplainResult) extract.ExtractionResult {
        var r = extract.ExtractionResult{};
        r.table_count = self.table_count;
        for (0..self.table_count) |i| {
            r.tables[i] = self.tables[i];
        }
        r.predicate_count = self.predicate_count;
        for (0..self.predicate_count) |i| {
            r.predicates[i] = self.predicates[i];
        }
        return r;
    }
};

// ============================================================================
// EXPLAIN execution
// ============================================================================

/// Run EXPLAIN on a query and parse the physical plan.
/// Returns an ExplainResult with ok=true on success, ok=false if EXPLAIN fails
/// (e.g., referenced tables do not exist).
pub fn runExplain(conn: duckdb.duckdb_connection, sql: []const u8, scratch: []u8) ExplainResult {
    var result = ExplainResult{};

    // Build "EXPLAIN <sql>" statement
    const prefix = "explain ";
    if (prefix.len + sql.len + 1 > scratch.len) return result;

    @memcpy(scratch[0..prefix.len], prefix);
    @memcpy(scratch[prefix.len .. prefix.len + sql.len], sql);
    scratch[prefix.len + sql.len] = 0;

    const explain_sql: [*:0]const u8 = @ptrCast(scratch[0 .. prefix.len + sql.len :0]);

    // Use a separate buffer region for the result
    const plan_buf_start = prefix.len + sql.len + 1;
    if (plan_buf_start >= scratch.len) return result;
    const plan_buf = scratch[plan_buf_start..];

    const plan_text = sql_runner.queryFirstVarchar(conn, explain_sql, 1, plan_buf) orelse return result;

    result = parsePlan(plan_text);
    result.ok = true;
    return result;
}

// ============================================================================
// Plan text parser
// ============================================================================

/// Parse a DuckDB physical plan text and extract tables, predicates, and estimated rows.
pub fn parsePlan(plan: []const u8) ExplainResult {
    var result = ExplainResult{};
    var lines = std.mem.splitScalar(u8, plan, '\n');

    var in_filters = false;
    var in_conditions = false;
    var in_table = false;
    var current_table: []const u8 = "";

    while (lines.next()) |raw_line| {
        const line = trimPlanLine(raw_line);
        if (line.len == 0) continue;

        // Separator lines (dashes)
        if (isSeparator(line)) {
            in_filters = false;
            in_conditions = false;
            in_table = false;
            continue;
        }

        // "Table:" header
        if (std.mem.eql(u8, line, "Table:")) {
            in_table = true;
            continue;
        }

        // Table name (line after "Table:")
        if (in_table) {
            in_table = false;
            const tname = extractTableName(line);
            if (tname.len > 0) {
                current_table = tname;
                addTable(&result, tname);
            }
            continue;
        }

        // Estimated rows (e.g., "~2 rows", "~1 row") — check before filters/conditions
        if (line.len > 1 and line[0] == '~') {
            in_filters = false;
            in_conditions = false;
            result.estimated_rows = @max(result.estimated_rows, parseEstimatedRows(line));
            continue;
        }

        // "Filters:" header
        if (std.mem.eql(u8, line, "Filters:")) {
            in_filters = true;
            continue;
        }

        // Filter line (e.g., "account_id=42", "ts>='2026-01-01'::DATE")
        if (in_filters) {
            parseFilterLine(&result, line, current_table);
            continue;
        }

        // "Conditions:" header (join conditions)
        if (std.mem.eql(u8, line, "Conditions:")) {
            in_conditions = true;
            continue;
        }

        // Join condition line (e.g., "account_id = id")
        if (in_conditions) {
            parseJoinCondition(&result, line, current_table);
            continue;
        }
    }

    return result;
}

// ============================================================================
// Line parsing helpers
// ============================================================================

/// Strip box-drawing characters, pipes, and whitespace from a plan line.
fn trimPlanLine(line: []const u8) []const u8 {
    var start: usize = 0;
    var end: usize = line.len;

    // Skip leading whitespace, box-drawing UTF-8 sequences, and pipe characters
    while (start < end) {
        if (line[start] == ' ' or line[start] == '|') {
            start += 1;
        } else if (start + 2 < end and line[start] == 0xe2 and (line[start + 1] == 0x94 or line[start + 1] == 0x95)) {
            // UTF-8 box-drawing characters: U+2500-U+257F (0xe2 0x94/0x95 xx)
            start += 3;
        } else {
            break;
        }
    }

    // Skip trailing whitespace, box-drawing, and pipe characters
    while (end > start) {
        if (line[end - 1] == ' ' or line[end - 1] == '|') {
            end -= 1;
        } else if (end >= start + 3 and line[end - 3] == 0xe2 and (line[end - 2] == 0x94 or line[end - 2] == 0x95)) {
            end -= 3;
        } else {
            break;
        }
    }

    if (start >= end) return "";
    return line[start..end];
}

/// Check if a line is a separator (all dashes, possibly with spaces).
/// Handles both ASCII dashes '-' and UTF-8 box-drawing dashes '─' (U+2500 = 0xe2 0x94 0x80).
fn isSeparator(line: []const u8) bool {
    if (line.len < 3) return false;
    var i: usize = 0;
    var dash_count: usize = 0;
    while (i < line.len) {
        if (line[i] == '-') {
            dash_count += 1;
            i += 1;
        } else if (line[i] == ' ') {
            i += 1;
        } else if (i + 2 < line.len and line[i] == 0xe2 and line[i + 1] == 0x94 and line[i + 2] == 0x80) {
            // UTF-8 box-drawing horizontal: ─ (U+2500)
            dash_count += 1;
            i += 3;
        } else {
            return false;
        }
    }
    return dash_count >= 3;
}

/// Extract table name from "memory.main.events" or "catalog.schema.table" format.
/// Returns just the table name (last component).
fn extractTableName(line: []const u8) []const u8 {
    // Find last dot
    var last_dot: ?usize = null;
    for (line, 0..) |c, i| {
        if (c == '.') last_dot = i;
    }
    if (last_dot) |d| {
        if (d + 1 < line.len) return line[d + 1 ..];
    }
    return line;
}

/// Parse a filter line like "account_id=42" or "ts>='2026-01-01'::DATE" or "account_id IS NOT NULL".
fn parseFilterLine(result: *ExplainResult, line: []const u8, table: []const u8) void {
    // Skip "IS NOT NULL" and "IS NULL" lines (not useful predicates for indexing)
    if (std.mem.indexOf(u8, line, "IS NOT NULL") != null) return;
    if (std.mem.indexOf(u8, line, "IS NULL") != null and std.mem.indexOf(u8, line, "=") == null) return;

    // Skip AND/OR connectors that appear on their own line
    if (std.mem.eql(u8, line, "AND") or std.mem.eql(u8, line, "OR")) return;

    // Find the operator to split column name from value
    const col_name = extractColumnFromFilter(line);
    if (col_name.len == 0) return;

    const kind = classifyFilterOp(line);
    addPredicate(result, table, col_name, kind);
}

/// Extract column name from a filter expression.
/// Handles: "account_id=42", "ts>='2026-01-01'", "name LIKE '%foo%'"
fn extractColumnFromFilter(line: []const u8) []const u8 {
    // Find first operator character
    for (line, 0..) |c, i| {
        if (c == '=' or c == '>' or c == '<' or c == '!') {
            if (i == 0) return "";
            return std.mem.trimRight(u8, line[0..i], " ");
        }
    }

    // Check for LIKE, IN, BETWEEN (keyword operators)
    if (std.mem.indexOf(u8, line, " LIKE ")) |i| return std.mem.trimRight(u8, line[0..i], " ");
    if (std.mem.indexOf(u8, line, " IN ")) |i| return std.mem.trimRight(u8, line[0..i], " ");
    if (std.mem.indexOf(u8, line, " BETWEEN ")) |i| return std.mem.trimRight(u8, line[0..i], " ");

    return "";
}

/// Classify the operator in a filter expression.
fn classifyFilterOp(line: []const u8) extract.PredicateKind {
    if (std.mem.indexOf(u8, line, " LIKE ") != null) return .like;
    if (std.mem.indexOf(u8, line, " IN ") != null) return .in_list;
    if (std.mem.indexOf(u8, line, " BETWEEN ") != null) return .range;
    // Check not-equal operators before range operators to avoid misclassifying <> as range
    if (std.mem.indexOf(u8, line, "<>") != null or std.mem.indexOf(u8, line, "!=") != null) return .equality;
    if (std.mem.indexOf(u8, line, ">=") != null or
        std.mem.indexOf(u8, line, "<=") != null or
        std.mem.indexOf(u8, line, ">") != null or
        std.mem.indexOf(u8, line, "<") != null)
        return .range;
    return .equality;
}

/// Parse a join condition line like "account_id = id".
fn parseJoinCondition(result: *ExplainResult, line: []const u8, table: []const u8) void {
    // Split on " = "
    if (std.mem.indexOf(u8, line, " = ")) |eq_pos| {
        const lhs = std.mem.trim(u8, line[0..eq_pos], " ");
        const rhs = std.mem.trim(u8, line[eq_pos + 3 ..], " ");
        if (lhs.len > 0) addPredicate(result, table, lhs, .equality);
        if (rhs.len > 0) addPredicate(result, table, rhs, .equality);
    }
}

/// Parse "~2 rows" or "~1 row" to a u64.
fn parseEstimatedRows(line: []const u8) u64 {
    // Skip '~' prefix
    var i: usize = 1;
    while (i < line.len and line[i] == ' ') : (i += 1) {}

    var n: u64 = 0;
    while (i < line.len and line[i] >= '0' and line[i] <= '9') : (i += 1) {
        n = n *| 10 +| (line[i] - '0');
    }
    return n;
}

// ============================================================================
// Result builders
// ============================================================================

fn addTable(result: *ExplainResult, name: []const u8) void {
    // Deduplicate
    for (result.tables[0..result.table_count]) |t| {
        if (std.mem.eql(u8, t.name, name)) return;
    }
    if (result.table_count >= extract.MAX_TABLES) return;
    result.tables[result.table_count] = .{ .name = name, .alias = null };
    result.table_count += 1;
}

fn addPredicate(result: *ExplainResult, table: []const u8, column: []const u8, kind: extract.PredicateKind) void {
    // Deduplicate
    for (result.predicates[0..result.predicate_count]) |p| {
        if (std.mem.eql(u8, p.table_name, table) and
            std.mem.eql(u8, p.column_name, column) and
            p.kind == kind) return;
    }
    if (result.predicate_count >= extract.MAX_PREDICATES) return;
    result.predicates[result.predicate_count] = .{
        .table_name = table,
        .column_name = column,
        .kind = kind,
    };
    result.predicate_count += 1;
}

// ============================================================================
// Tests
// ============================================================================

test "parse simple SEQ_SCAN with filters" {
    const plan =
        \\┌───────────────────────────┐
        \\│          SEQ_SCAN         │
        \\│    ────────────────────   │
        \\│           Table:          │
        \\│     memory.main.events    │
        \\│                           │
        \\│          Filters:         │
        \\│       account_id=42       │
        \\│   ts>='2026-01-01'::DATE  │
        \\│                           │
        \\│          ~2 rows          │
        \\└───────────────────────────┘
    ;

    const result = parsePlan(plan);
    try std.testing.expectEqual(@as(usize, 1), result.table_count);
    try std.testing.expectEqualStrings("events", result.tables[0].name);
    try std.testing.expectEqual(@as(usize, 2), result.predicate_count);
    try std.testing.expectEqualStrings("account_id", result.predicates[0].column_name);
    try std.testing.expectEqual(extract.PredicateKind.equality, result.predicates[0].kind);
    try std.testing.expectEqualStrings("ts", result.predicates[1].column_name);
    try std.testing.expectEqual(extract.PredicateKind.range, result.predicates[1].kind);
    try std.testing.expectEqual(@as(u64, 2), result.estimated_rows);
}

test "parse join with conditions" {
    const plan =
        \\┌───────────────────────────┐
        \\│         HASH_JOIN         │
        \\│    ────────────────────   │
        \\│      Join Type: INNER     │
        \\│                           │
        \\│        Conditions:        │
        \\│      account_id = id      │
        \\│                           │
        \\│          ~0 rows          │
        \\└─────────────┬─────────────┘
        \\┌─────────────┴─────────────┐
        \\│          SEQ_SCAN         │
        \\│    ────────────────────   │
        \\│           Table:          │
        \\│     memory.main.events    │
        \\│                           │
        \\│          Filters:         │
        \\│       account_id=42       │
        \\│                           │
        \\│           ~1 row          │
        \\└───────────────────────────┘
    ;

    const result = parsePlan(plan);
    try std.testing.expectEqual(@as(usize, 1), result.table_count);
    try std.testing.expect(result.predicate_count >= 2);
    // Should have join condition predicates + filter predicate
}

test "parse estimated rows" {
    try std.testing.expectEqual(@as(u64, 2), parseEstimatedRows("~2 rows"));
    try std.testing.expectEqual(@as(u64, 1), parseEstimatedRows("~1 row"));
    try std.testing.expectEqual(@as(u64, 10000), parseEstimatedRows("~10000 rows"));
}

test "extractTableName from qualified name" {
    try std.testing.expectEqualStrings("events", extractTableName("memory.main.events"));
    try std.testing.expectEqualStrings("accounts", extractTableName("memory.main.accounts"));
    try std.testing.expectEqualStrings("events", extractTableName("events"));
}

test "classifyFilterOp" {
    try std.testing.expectEqual(extract.PredicateKind.equality, classifyFilterOp("account_id=42"));
    try std.testing.expectEqual(extract.PredicateKind.range, classifyFilterOp("ts>='2026-01-01'::DATE"));
    try std.testing.expectEqual(extract.PredicateKind.range, classifyFilterOp("amount<100"));
    try std.testing.expectEqual(extract.PredicateKind.like, classifyFilterOp("name LIKE '%foo%'"));
}

test "isSeparator handles UTF-8 box dashes" {
    try std.testing.expect(isSeparator("────────────────────"));
    try std.testing.expect(isSeparator("-------------------"));
    try std.testing.expect(isSeparator("── ── ── ── ── ──"));
    try std.testing.expect(!isSeparator("Table:"));
    try std.testing.expect(!isSeparator("ab"));
    try std.testing.expect(!isSeparator("Filters:"));
}

test "classifyFilterOp handles not-equal" {
    try std.testing.expectEqual(extract.PredicateKind.equality, classifyFilterOp("status<>'active'"));
    try std.testing.expectEqual(extract.PredicateKind.equality, classifyFilterOp("status!='active'"));
    try std.testing.expectEqual(extract.PredicateKind.range, classifyFilterOp("amount>100"));
}

test "extractColumnFromFilter" {
    try std.testing.expectEqualStrings("account_id", extractColumnFromFilter("account_id=42"));
    try std.testing.expectEqualStrings("ts", extractColumnFromFilter("ts>='2026-01-01'::DATE"));
    try std.testing.expectEqualStrings("name", extractColumnFromFilter("name LIKE '%foo%'"));
}
