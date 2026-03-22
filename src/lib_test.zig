// Test harness — pulls in all inline tests from vizier modules.
// No tests defined here; all unit and regression tests live in their modules.

comptime {
    _ = @import("vizier/capture.zig");
    _ = @import("vizier/extract.zig");
    _ = @import("vizier/inspect.zig");
    _ = @import("vizier/advisor.zig");
}
