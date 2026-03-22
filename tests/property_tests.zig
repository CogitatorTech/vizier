const std = @import("std");
const testing = std.testing;
const minish = @import("minish");
const gen = minish.gen;

// Import vizier modules under test (provided via build.zig)
const extract = @import("vizier/extract");
const capture = @import("vizier/capture");
const inspect = @import("vizier/inspect");

// ============================================================================
// Property: tokenizer never crashes on arbitrary input
// ============================================================================

fn prop_tokenizer_does_not_crash(input: []const u8) !void {
    // The tokenizer should handle any input without panicking.
    // It may produce zero tokens for garbage input, but must not crash.
    const result = extract.tokenize(input);
    // Must always produce at least an EOF token
    try testing.expect(result.count >= 1);
    // Last token must be EOF
    try testing.expect(result.tokens[result.count - 1].kind == .eof);
}

test "property: tokenizer handles arbitrary ASCII input" {
    try minish.check(
        testing.allocator,
        gen.string(.{ .min_len = 0, .max_len = 256 }),
        prop_tokenizer_does_not_crash,
        .{ .num_runs = 500, .seed = 42 },
    );
}

// ============================================================================
// Property: extractFromSql never crashes on arbitrary input
// ============================================================================

fn prop_extract_does_not_crash(input: []const u8) !void {
    const result = extract.extractFromSql(input);
    // Must produce bounded results
    try testing.expect(result.table_count <= 32);
    try testing.expect(result.predicate_count <= 64);
}

test "property: extraction handles arbitrary ASCII input" {
    try minish.check(
        testing.allocator,
        gen.string(.{ .min_len = 0, .max_len = 512 }),
        prop_extract_does_not_crash,
        .{ .num_runs = 500, .seed = 42 },
    );
}

// ============================================================================
// Property: hash is deterministic (same input -> same output)
// ============================================================================

fn prop_hash_deterministic(input: []const u8) !void {
    const h1 = capture.hashQuery(input);
    const h2 = capture.hashQuery(input);
    try testing.expectEqual(h1, h2);
}

test "property: hash is deterministic for arbitrary strings" {
    try minish.check(
        testing.allocator,
        gen.string(.{ .min_len = 0, .max_len = 256 }),
        prop_hash_deterministic,
        .{ .num_runs = 500, .seed = 42 },
    );
}

// ============================================================================
// Property: JSON builders never crash and produce valid brackets
// ============================================================================

fn prop_tables_json_valid(input: []const u8) !void {
    const result = extract.extractFromSql(input);
    var buf: [4096]u8 = undefined;
    const json = extract.buildTablesJson(&result, &buf);
    // Must start with [ and end with ]
    try testing.expect(json.len >= 2);
    try testing.expectEqual(@as(u8, '['), json[0]);
    try testing.expectEqual(@as(u8, ']'), json[json.len - 1]);
}

test "property: tables JSON is well-formed for arbitrary SQL" {
    try minish.check(
        testing.allocator,
        gen.string(.{ .min_len = 0, .max_len = 256 }),
        prop_tables_json_valid,
        .{ .num_runs = 300, .seed = 42 },
    );
}

fn prop_columns_json_valid(input: []const u8) !void {
    const result = extract.extractFromSql(input);
    var buf: [4096]u8 = undefined;
    const json = extract.buildColumnsJson(&result, &buf);
    try testing.expect(json.len >= 2);
    try testing.expectEqual(@as(u8, '['), json[0]);
    try testing.expectEqual(@as(u8, ']'), json[json.len - 1]);
}

test "property: columns JSON is well-formed for arbitrary SQL" {
    try minish.check(
        testing.allocator,
        gen.string(.{ .min_len = 0, .max_len = 256 }),
        prop_columns_json_valid,
        .{ .num_runs = 300, .seed = 42 },
    );
}

// ============================================================================
// Property: parseTableRef always returns non-empty table name
// ============================================================================

fn prop_parse_table_ref_non_empty(input: []const u8) !void {
    if (input.len == 0) return; // skip empty
    // Filter out inputs with only dots or whitespace
    for (input) |c| {
        if (std.ascii.isAlphanumeric(c) or c == '_') {
            const ref = inspect.parseTableRef(input);
            try testing.expect(ref.table.len > 0);
            try testing.expect(ref.schema.len > 0);
            return;
        }
    }
}

test "property: parseTableRef produces non-empty parts for valid identifiers" {
    try minish.check(
        testing.allocator,
        gen.string(.{ .min_len = 1, .max_len = 64 }),
        prop_parse_table_ref_non_empty,
        .{ .num_runs = 300, .seed = 42 },
    );
}

// ============================================================================
// Property: extraction of well-formed SQL-like strings
// ============================================================================

// Generate strings that look like simple SQL: "select * from <word> where <word> = <number>"
fn prop_simple_sql_extracts_table(input: []const u8) !void {
    // Build a simple SQL from the random input as a table name
    // Only use alphanumeric chars for the table name
    var table_name_buf: [64]u8 = undefined;
    var len: usize = 0;
    for (input) |c| {
        if (len >= 63) break;
        if (std.ascii.isAlphabetic(c)) {
            table_name_buf[len] = c;
            len += 1;
        }
    }
    if (len == 0) return; // skip if no valid chars

    const table_name = table_name_buf[0..len];

    // Build SQL
    var sql_buf: [256]u8 = undefined;
    const sql = std.fmt.bufPrint(&sql_buf, "select * from {s} where id = 1", .{table_name}) catch return;

    const result = extract.extractFromSql(sql);

    // Should extract at least one table
    try testing.expect(result.table_count >= 1);

    // The first table should match our generated name (unless it's a keyword)
    if (!extract.isKeyword(table_name)) {
        try testing.expect(result.table_count == 1);
    }
}

test "property: well-formed SELECT always extracts a table" {
    try minish.check(
        testing.allocator,
        gen.string(.{ .min_len = 1, .max_len = 32 }),
        prop_simple_sql_extracts_table,
        .{ .num_runs = 300, .seed = 42 },
    );
}
