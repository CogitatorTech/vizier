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

// ============================================================================
// Property: normalization never crashes and always produces output
// ============================================================================

fn prop_normalize_does_not_crash(input: []const u8) !void {
    var buf: [4096]u8 = undefined;
    const result = extract.normalizeSql(input, &buf);
    _ = result;
}

test "property: normalization handles arbitrary input" {
    try minish.check(
        testing.allocator,
        gen.string(.{ .min_len = 0, .max_len = 512 }),
        prop_normalize_does_not_crash,
        .{ .num_runs = 500, .seed = 42 },
    );
}

// ============================================================================
// Property: extraction result predicates always reference known tables
// ============================================================================

fn prop_predicates_reference_extracted_tables(input: []const u8) !void {
    const result = extract.extractFromSql(input);
    const tables = result.tableSlice();
    for (result.predicateSlice()) |pred| {
        if (pred.table_name.len == 0) continue;
        // Each predicate's table should either match a table name or an alias
        var found = false;
        for (tables) |t| {
            if (std.mem.eql(u8, pred.table_name, t.name)) {
                found = true;
                break;
            }
            if (t.alias) |a| {
                if (std.mem.eql(u8, pred.table_name, a)) {
                    found = true;
                    break;
                }
            }
        }
        // If no tables extracted, predicate uses defaultTable which may be empty
        if (tables.len == 0) continue;
        // The predicate table should be from the extracted tables (or resolved alias)
        try testing.expect(found);
    }
}

test "property: predicates reference extracted tables" {
    try minish.check(
        testing.allocator,
        gen.string(.{ .min_len = 10, .max_len = 256 }),
        prop_predicates_reference_extracted_tables,
        .{ .num_runs = 500, .seed = 42 },
    );
}

// ============================================================================
// Property: hash distribution (no constant output)
// ============================================================================

fn prop_hash_not_constant(input: []const u8) !void {
    if (input.len < 2) return;
    const h1 = capture.hashQuery(input);
    const h2 = capture.hashQuery(input[1..]);
    // Different inputs should (almost certainly) produce different hashes
    // This won't always hold due to collisions, but with 64-bit hashes
    // the probability is negligible for 500 runs
    if (!std.mem.eql(u8, input, input[1..])) {
        try testing.expect(h1 != h2);
    }
}

test "property: hash varies for different inputs" {
    try minish.check(
        testing.allocator,
        gen.string(.{ .min_len = 2, .max_len = 128 }),
        prop_hash_not_constant,
        .{ .num_runs = 500, .seed = 42 },
    );
}

// ============================================================================
// Property: extraction of JOIN queries always finds at least 2 tables
// ============================================================================

fn prop_join_extracts_multiple_tables(input: []const u8) !void {
    // Build a JOIN query with random table names
    var name1_buf: [32]u8 = undefined;
    var name2_buf: [32]u8 = undefined;
    var n1: usize = 0;
    var n2: usize = 0;

    for (input) |c| {
        if (n1 < 31 and std.ascii.isAlphabetic(c)) {
            name1_buf[n1] = c;
            n1 += 1;
        }
    }
    // Use second half for name2
    const mid = input.len / 2;
    for (input[mid..]) |c| {
        if (n2 < 31 and std.ascii.isAlphabetic(c)) {
            name2_buf[n2] = c;
            n2 += 1;
        }
    }
    if (n1 == 0 or n2 == 0) return;
    if (std.mem.eql(u8, name1_buf[0..n1], name2_buf[0..n2])) return;

    const t1 = name1_buf[0..n1];
    const t2 = name2_buf[0..n2];

    var sql_buf: [256]u8 = undefined;
    const sql = std.fmt.bufPrint(&sql_buf, "select * from {s} join {s} on {s}.id = {s}.id", .{ t1, t2, t1, t2 }) catch return;

    const result = extract.extractFromSql(sql);

    // Skip if either name is a SQL keyword (would not be parsed as table)
    if (extract.isKeyword(t1) or extract.isKeyword(t2)) return;

    try testing.expect(result.table_count >= 2);
}
