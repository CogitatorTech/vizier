const std = @import("std");
const duckdb = @import("duckdb");

// Vizier sub-modules
pub const schema = @import("vizier/schema.zig");
pub const capture = @import("vizier/capture.zig");
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
