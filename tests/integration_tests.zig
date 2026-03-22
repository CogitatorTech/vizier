const std = @import("std");
const testing = std.testing;

const extension_path = "zig-out/lib/vizier.duckdb_extension";

/// Run a SQL script in DuckDB with the extension loaded and return stdout.
fn runSql(allocator: std.mem.Allocator, sql: []const u8) ![]const u8 {
    var full_sql_buf: [8192]u8 = undefined;
    const full_sql = try std.fmt.bufPrint(&full_sql_buf, "load '{s}';\n{s}", .{ extension_path, sql });

    const result = try std.process.Child.run(.{
        .allocator = allocator,
        .argv = &.{ "duckdb", "-unsigned", "-noheader", "-csv", "-c", full_sql },
    });
    allocator.free(result.stderr);

    if (result.term.Exited != 0) {
        defer allocator.free(result.stdout);
        return error.DuckDBFailed;
    }

    return result.stdout;
}

fn expectContains(haystack: []const u8, needle: []const u8) !void {
    if (std.mem.indexOf(u8, haystack, needle) == null) {
        std.debug.print("\n--- expected to find ---\n{s}\n--- in output ---\n{s}\n---\n", .{ needle, haystack });
        return error.TestUnexpectedResult;
    }
}

// ============================================================================
// Integration tests
// ============================================================================

test "extension loads and version works" {
    const out = try runSql(testing.allocator, "select vizier_version();");
    defer testing.allocator.free(out);
    try expectContains(out, "0.1.0");
}

test "capture returns ok with query hash" {
    const out = try runSql(testing.allocator,
        \\select * from vizier_capture('select * from orders where id = 1');
    );
    defer testing.allocator.free(out);
    try expectContains(out, "ok");
}

test "flush persists captured queries" {
    const out = try runSql(testing.allocator,
        \\select * from vizier_capture('select * from t where x = 1');
        \\select * from vizier_capture('select * from t where y > 2');
        \\select * from vizier_flush();
        \\select count(*) from vizier.workload_queries;
    );
    defer testing.allocator.free(out);
    try expectContains(out, "ok");
    try expectContains(out, "2");
}

test "duplicate queries increment execution_count" {
    const out = try runSql(testing.allocator,
        \\select * from vizier_capture('select * from orders where id = 1');
        \\select * from vizier_capture('select * from orders where id = 1');
        \\select * from vizier_capture('select * from orders where id = 1');
        \\select * from vizier_flush();
        \\select execution_count from vizier.workload_queries;
    );
    defer testing.allocator.free(out);
    try expectContains(out, "3");
}

test "predicate extraction populates workload_predicates" {
    const out = try runSql(testing.allocator,
        \\select * from vizier_capture('select * from events where account_id = 42 and ts >= date ''2026-01-01''');
        \\select * from vizier_flush();
        \\select column_name, predicate_kind from vizier.workload_predicates order by column_name;
    );
    defer testing.allocator.free(out);
    try expectContains(out, "account_id");
    try expectContains(out, "equality");
    try expectContains(out, "ts");
    try expectContains(out, "range");
}

test "predicate extraction handles joins with aliases" {
    const out = try runSql(testing.allocator,
        \\select * from vizier_capture('select o.id from orders o join customers c on o.cid = c.id where o.amount > 100 and c.country = ''US''');
        \\select * from vizier_flush();
        \\select table_name, column_name, predicate_kind from vizier.workload_predicates order by table_name, column_name;
    );
    defer testing.allocator.free(out);
    try expectContains(out, "orders");
    try expectContains(out, "amount");
    try expectContains(out, "customers");
    try expectContains(out, "country");
    try expectContains(out, "equality");
}

test "tables_json and columns_json populated on flush" {
    const out = try runSql(testing.allocator,
        \\select * from vizier_capture('select * from products where category in (''a'', ''b'') and name like ''%phone%''');
        \\select * from vizier_flush();
        \\select tables_json, columns_json from vizier.workload_queries;
    );
    defer testing.allocator.free(out);
    try expectContains(out, "products");
    try expectContains(out, "products.category");
    try expectContains(out, "products.name");
}

test "inspect_table shows column info with predicate counts" {
    const out = try runSql(testing.allocator,
        \\create table test_t (id integer, name varchar, score double);
        \\insert into test_t values (1, 'a', 1.0);
        \\select * from vizier_capture('select * from test_t where name = ''x'' and score > 0.5');
        \\select * from vizier_flush();
        \\select column_name, column_type, predicate_count, predicate_kinds from vizier.inspect_table('test_t');
    );
    defer testing.allocator.free(out);
    try expectContains(out, "id");
    try expectContains(out, "INTEGER");
    try expectContains(out, "name");
    try expectContains(out, "VARCHAR");
    try expectContains(out, "score");
    try expectContains(out, "DOUBLE");
    try expectContains(out, "equality");
    try expectContains(out, "range");
}

test "workload_summary view orders by execution count" {
    const out = try runSql(testing.allocator,
        // Use structurally different queries (normalization replaces literals,
        // so "select 1" and "select 2" would hash the same)
        \\select * from vizier_capture('select * from a where x = 1');
        \\select * from vizier_capture('select * from b where y = 1');
        \\select * from vizier_capture('select * from a where x = 2');
        \\select * from vizier_flush();
        \\select sql, runs from vizier.workload_summary;
    );
    defer testing.allocator.free(out);
    try expectContains(out, "2");
    try expectContains(out, "1");
}

test "analyze generates index recommendations" {
    const out = try runSql(testing.allocator,
        \\select * from vizier_capture('select * from orders where customer_id = 42');
        \\select * from vizier_capture('select * from orders where customer_id = 10');
        \\select * from vizier_flush();
        \\select * from vizier_analyze();
        \\select kind, table_name, sql_text from vizier.recommendations where kind = 'create_index';
    );
    defer testing.allocator.free(out);
    try expectContains(out, "ok");
    try expectContains(out, "create_index");
    try expectContains(out, "customer_id");
    try expectContains(out, "create index");
}

test "analyze generates sort recommendations" {
    const out = try runSql(testing.allocator,
        \\select * from vizier_capture('select * from events where account_id = 1 and ts >= ''2026-01-01''');
        \\select * from vizier_capture('select * from events where account_id = 2 and ts >= ''2026-02-01''');
        \\select * from vizier_capture('select * from events where account_id = 3 and ts between ''2026-01-01'' and ''2026-12-31''');
        \\select * from vizier_flush();
        \\select * from vizier_analyze();
        \\select kind, table_name from vizier.recommendations where kind = 'rewrite_sorted_table';
    );
    defer testing.allocator.free(out);
    try expectContains(out, "rewrite_sorted_table");
    try expectContains(out, "events");
}

test "analyze is idempotent" {
    const out = try runSql(testing.allocator,
        \\select * from vizier_capture('select * from t where x = 1');
        \\select * from vizier_flush();
        \\select * from vizier_analyze();
        \\select * from vizier_analyze();
        \\select count(*) from vizier.recommendations;
    );
    defer testing.allocator.free(out);
    try expectContains(out, "ok");
}

test "apply executes recommendation and logs action" {
    const out = try runSql(testing.allocator,
        \\create table apply_test (id integer, x integer);
        \\select * from vizier_capture('select * from apply_test where x = 1');
        \\select * from vizier_capture('select * from apply_test where x = 2');
        \\select * from vizier_flush();
        \\select * from vizier_analyze();
        \\select * from vizier_apply(1);
        \\select status from vizier.recommendation_store where recommendation_id = 1;
        \\select success from vizier.applied_actions where recommendation_id = 1;
    );
    defer testing.allocator.free(out);
    try expectContains(out, "applied");
    try expectContains(out, "true");
    try expectContains(out, "create index");
}

test "apply non-existent recommendation returns not found" {
    const out = try runSql(testing.allocator,
        \\select * from vizier_apply(9999);
    );
    defer testing.allocator.free(out);
    try expectContains(out, "not found");
}

test "schema tables created on load" {
    const out = try runSql(testing.allocator,
        \\select table_name from information_schema.tables where table_schema = 'vizier' order by table_name;
    );
    defer testing.allocator.free(out);
    try expectContains(out, "applied_actions");
    try expectContains(out, "benchmark_results");
    try expectContains(out, "recommendation_store");
    try expectContains(out, "workload_predicates");
    try expectContains(out, "workload_queries");
    try expectContains(out, "workload_summary");
}
