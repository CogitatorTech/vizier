const std = @import("std");

// ============================================================================
// Table reference parsing
// ============================================================================

pub const TableRef = struct {
    schema: []const u8,
    table: []const u8,
};

/// Parse a table reference like "schema.table" or just "table" (defaults to "main").
pub fn parseTableRef(input: []const u8) TableRef {
    if (std.mem.indexOfScalar(u8, input, '.')) |dot_pos| {
        return .{
            .schema = input[0..dot_pos],
            .table = input[dot_pos + 1 ..],
        };
    }
    return .{
        .schema = "main",
        .table = input,
    };
}

// ============================================================================
// Tests
// ============================================================================

test "parseTableRef with schema" {
    const ref = parseTableRef("main.events");
    try std.testing.expectEqualStrings("main", ref.schema);
    try std.testing.expectEqualStrings("events", ref.table);
}

test "parseTableRef without schema" {
    const ref = parseTableRef("events");
    try std.testing.expectEqualStrings("main", ref.schema);
    try std.testing.expectEqualStrings("events", ref.table);
}

test "parseTableRef custom schema" {
    const ref = parseTableRef("my_schema.my_table");
    try std.testing.expectEqualStrings("my_schema", ref.schema);
    try std.testing.expectEqualStrings("my_table", ref.table);
}
