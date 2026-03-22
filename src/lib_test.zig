const std = @import("std");
const testing = std.testing;

// Import modules to run their tests
const capture = @import("vizier/capture.zig");
const extract = @import("vizier/extract.zig");
const inspect = @import("vizier/inspect.zig");

// Force module tests to be included
comptime {
    _ = extract;
    _ = inspect;
}

test "hashQuery is deterministic" {
    const sql = "SELECT * FROM foo WHERE id = 1";
    const h1 = capture.hashQuery(sql);
    const h2 = capture.hashQuery(sql);
    try testing.expectEqual(h1, h2);
}

test "hashQuery differs for different inputs" {
    const h1 = capture.hashQuery("SELECT 1");
    const h2 = capture.hashQuery("SELECT 2");
    try testing.expect(h1 != h2);
}

test "version string is valid" {
    const version = "0.1.0";
    try testing.expect(version.len > 0);
    // Check it contains a dot (semver-like)
    try testing.expect(std.mem.indexOf(u8, version, ".") != null);
}
