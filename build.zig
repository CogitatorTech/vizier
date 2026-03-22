const std = @import("std");

pub fn build(b: *std.Build) void {
    // Add standard options for target and optimization mode.
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{
        .preferred_optimize_mode = .ReleaseFast,
    });

    // Build options for DuckDB Extension configuration
    const extension_name = b.option([]const u8, "extension-name", "Extension name (default: vizier)") orelse "vizier";
    const extension_api_version = b.option([]const u8, "api-version", "DuckDB Extension API version (default: v1.2.0)") orelse "v1.2.0";
    const extension_version = b.option([]const u8, "extension-version", "Extension version (default: v0.1.0)") orelse "v0.1.0";
    const platform = b.option([]const u8, "platform", "Target platform (e.g., linux_amd64, linux_arm64)") orelse detectPlatform(target);

    // Resolve the extension-template-c dependency from build.zig.zon
    const ext_template_dep = b.dependency("extension-template-c", .{});
    const ext_template_capi_path = ext_template_dep.path("duckdb_capi");

    const duckdb_module = b.addModule("duckdb", .{
        .root_source_file = b.path("src/duckdb.zig"),
    });

    const root_module = b.createModule(.{
        .root_source_file = b.path("src/lib.zig"),
        .target = target,
        .optimize = optimize,
    });
    root_module.addImport("duckdb", duckdb_module);

    const lib = b.addLibrary(.{
        .name = extension_name,
        .root_module = root_module,
        .linkage = .dynamic,
    });

    const extension_filename = b.fmt("{s}.duckdb_extension", .{extension_name});
    lib.install_name = extension_filename;

    // Add the C source file that handles DuckDB API integration
    lib.addCSourceFile(.{
        .file = b.path("src/extension.c"),
        .flags = &.{"-std=c11"},
    });

    // Add include path for DuckDB headers (from build.zig.zon dependency)
    lib.addIncludePath(ext_template_capi_path);

    // Link libc (required for C code)
    lib.linkLibC();

    // Add C macro for extension name
    lib.root_module.addCMacro("DUCKDB_EXTENSION_NAME", extension_name);
    lib.root_module.addCMacro("DUCKDB_BUILD_LOADABLE_EXTENSION", "1");

    // Allow undefined symbols - they will be provided by DuckDB at runtime
    lib.linker_allow_shlib_undefined = true;

    // Install the library artifact
    const lib_install = b.addInstallArtifact(lib, .{});
    b.getInstallStep().dependOn(&lib_install.step);

    // Test configuration - use a separate test file that doesn't require DuckDB runtime
    const test_module = b.createModule(.{
        .root_source_file = b.path("src/lib_test.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_module.addImport("duckdb", duckdb_module);

    const lib_unit_tests = b.addTest(.{
        .root_module = test_module,
    });

    const run_lib_unit_tests = b.addRunArtifact(lib_unit_tests);

    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_lib_unit_tests.step);

    // Clean step - removes build artifacts and cache
    const clean_step = b.step("clean", "Remove build artifacts and cache");
    const clean_cmd = b.addSystemCommand(&[_][]const u8{
        "rm",
        "-rf",
        "zig-out",
        ".zig-cache",
    });
    clean_step.dependOn(&clean_cmd.step);

    // Detect the library file extension based on target OS
    const lib_filename = getLibFilename(b, target, extension_name);
    // Windows DLLs go to bin/, other platforms go to lib/
    const os_tag = target.result.os.tag;
    const lib_path = if (os_tag == .windows)
        b.getInstallPath(.bin, lib_filename)
    else
        b.getInstallPath(.lib, lib_filename);

    // Add metadata step - adds DuckDB extension metadata for proper loading (name, version, platform, API version)
    // Note: we will be using DuckDB extension API version (v1.2.0) so the extension will be compatible with DuckDB versions >= 1.2.0
    const add_metadata_step = b.step("add-metadata", "Add DuckDB extension metadata");
    const metadata_cmd = b.addSystemCommand(&[_][]const u8{
        "python3",
        "scripts/append_extension_metadata.py",
        "-l",
        lib_path,
        "-n",
        extension_name,
        "-o",
        b.getInstallPath(.lib, extension_filename),
        "-dv",
        extension_api_version,
        "-ev",
        extension_version,
        "-p",
        platform,
    });
    metadata_cmd.step.dependOn(b.getInstallStep());
    add_metadata_step.dependOn(&metadata_cmd.step);

    // Test extension with DuckDB step
    const test_ext_step = b.step("test-extension", "Test the extension with DuckDB");
    const test_load_cmd = b.fmt("LOAD 'zig-out/lib/{s}'; SELECT 'Extension loaded successfully' as status;", .{extension_filename});
    const test_ext_cmd = b.addSystemCommand(&[_][]const u8{
        "duckdb",
        "-unsigned",
        "-c",
        test_load_cmd,
    });
    test_ext_cmd.step.dependOn(&metadata_cmd.step);
    test_ext_step.dependOn(&test_ext_cmd.step);

    // Interactive DuckDB session with extension loaded
    const duckdb_step = b.step("duckdb", "Start interactive DuckDB session with extension loaded");
    const load_cmd = b.fmt("LOAD 'zig-out/lib/{s}'", .{extension_filename});
    const run_duckdb = b.addSystemCommand(&[_][]const u8{
        "duckdb",
        "-unsigned",
        "-cmd",
        load_cmd,
        "-cmd",
        "SELECT 'Extension loaded successfully!' as status",
    });
    run_duckdb.step.dependOn(&metadata_cmd.step);
    duckdb_step.dependOn(&run_duckdb.step);

    // Generate DuckDB Zig bindings from C API
    const gen_bindings_step = b.step("duckdb-translate", "Generate Zig bindings from DuckDB C API");
    const capi_dir = ext_template_capi_path.getPath(b);
    const ext_header = ext_template_dep.path("duckdb_capi/duckdb_extension.h").getPath(b);
    const translate_shell = b.fmt("zig translate-c -I {s} {s} > src/duckdb.zig", .{ capi_dir, ext_header });
    const translate_cmd = b.addSystemCommand(&[_][]const u8{
        "sh",
        "-c",
        translate_shell,
    });
    gen_bindings_step.dependOn(&translate_cmd.step);

    // Build all: build + add metadata
    const build_all_step = b.step("build-all", "Build extension and add metadata");
    build_all_step.dependOn(b.getInstallStep());
    build_all_step.dependOn(add_metadata_step);

    // The documentation step remains unchanged.
    const docs_step = b.step("docs", "Generate API documentation");
    const doc_install_path = "docs/api";
    const gen_docs_cmd = b.addSystemCommand(&[_][]const u8{
        b.graph.zig_exe,
        "build-lib",
        "src/lib.zig",
        "-femit-docs=" ++ doc_install_path,
    });
    const mkdir_cmd = b.addSystemCommand(&[_][]const u8{
        "mkdir", "-p", doc_install_path,
    });
    gen_docs_cmd.step.dependOn(&mkdir_cmd.step);
    docs_step.dependOn(&gen_docs_cmd.step);
}

fn detectPlatform(target: std.Build.ResolvedTarget) []const u8 {
    const os_tag = target.result.os.tag;
    const cpu_arch = target.result.cpu.arch;

    if (cpu_arch == .x86_64) {
        if (os_tag == .linux) return "linux_amd64";
        if (os_tag == .macos) return "osx_amd64";
        if (os_tag == .windows) return "windows_amd64";
    } else if (cpu_arch == .aarch64) {
        if (os_tag == .linux) return "linux_arm64";
        if (os_tag == .macos) return "osx_arm64";
    }

    return "unknown";
}

fn getLibExtension(target: std.Build.ResolvedTarget) []const u8 {
    const os_tag = target.result.os.tag;

    return switch (os_tag) {
        .windows => ".dll",
        .macos => ".dylib",
        else => ".so",
    };
}

fn getLibFilename(b: *std.Build, target: std.Build.ResolvedTarget, extension_name: []const u8) []const u8 {
    const lib_extension = getLibExtension(target);
    const os_tag = target.result.os.tag;

    // Note: Windows DLLs don't use "lib" prefix, but other platforms do
    if (os_tag == .windows) {
        return b.fmt("{s}{s}", .{ extension_name, lib_extension });
    } else {
        return b.fmt("lib{s}{s}", .{ extension_name, lib_extension });
    }
}
