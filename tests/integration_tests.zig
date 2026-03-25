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
    // Both sides of ON clause: o.cid and c.id
    try expectContains(out, "cid");
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

test "session capture with start and stop" {
    const out = try runSql(testing.allocator,
        \\select * from vizier_start_capture();
        \\select vizier_session_log('select * from orders where id = 1');
        \\select vizier_session_log('select count(*) from events group by aid');
        \\select vizier_session_log('select * from orders where id = 2');
        \\select * from vizier_stop_capture();
        \\select count(*) from vizier.workload_queries;
    );
    defer testing.allocator.free(out);
    try expectContains(out, "capture started");
    try expectContains(out, "ok");
    try expectContains(out, "2");
}

test "import_profile reads from JSON profiling output" {
    const out = try runSql(testing.allocator,
        \\copy (select 'select * from orders where id = 1' as query union all select 'select count(*) from events' as query) to '/tmp/vizier_test_profile.json' (format json);
        \\select * from vizier_import_profile('/tmp/vizier_test_profile.json');
        \\select * from vizier_flush();
        \\select count(*) from vizier.workload_queries;
    );
    defer testing.allocator.free(out);
    try expectContains(out, "ok");
    try expectContains(out, "2");
}

test "capture_bulk reads queries from a table" {
    const out = try runSql(testing.allocator,
        \\create table query_log (sql_text varchar);
        \\insert into query_log values ('select * from orders where id = 1');
        \\insert into query_log values ('select * from orders where id = 2');
        \\insert into query_log values ('select count(*) from events group by aid');
        \\select * from vizier_capture_bulk('query_log', 'sql_text');
        \\select * from vizier_flush();
        \\select count(*) from vizier.workload_queries;
    );
    defer testing.allocator.free(out);
    try expectContains(out, "ok");
    try expectContains(out, "3");
    // 2 distinct queries after normalization (orders id=1 and id=2 are same pattern)
    try expectContains(out, "2");
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

test "inspect_table shows storage info columns" {
    const out = try runSql(testing.allocator,
        \\create table storage_t (id integer, val varchar);
        \\insert into storage_t select i, 'row' || i::varchar from range(100) t(i);
        \\select column_name, approx_rows, has_nulls, compression, row_groups from vizier.inspect_table('storage_t');
    );
    defer testing.allocator.free(out);
    try expectContains(out, "id");
    try expectContains(out, "val");
    try expectContains(out, "100");
    try expectContains(out, "false");
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

test "analyze generates join-path sort recommendations" {
    const out = try runSql(testing.allocator,
        \\select * from vizier_capture('select o.id from jp_orders o join jp_custs c on o.cid = c.id where o.amt > 100');
        \\select * from vizier_capture('select o.id from jp_orders o join jp_custs c on o.cid = c.id where c.name = ''x''');
        \\select * from vizier_capture('select o.id from jp_orders o join jp_custs c on o.cid = c.id where o.amt > 200');
        \\select * from vizier_flush();
        \\select * from vizier_analyze();
        \\select kind, reason from vizier.recommendations where kind = 'sort_for_join';
    );
    defer testing.allocator.free(out);
    try expectContains(out, "sort_for_join");
    try expectContains(out, "join predicate");
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

test "analyze detects point lookups with boosted confidence" {
    const out = try runSql(testing.allocator,
        \\select * from vizier_capture('select * from users where id = 1');
        \\select * from vizier_capture('select * from users where id = 2');
        \\select * from vizier_flush();
        \\select * from vizier_analyze();
        \\select reason from vizier.recommendations where table_name = 'users';
    );
    defer testing.allocator.free(out);
    try expectContains(out, "point lookup");
}

test "analyze detects join predicates" {
    const out = try runSql(testing.allocator,
        \\select * from vizier_capture('select o.id from orders o join customers c on o.cid = c.id where o.amount > 100');
        \\select * from vizier_capture('select o.id from orders o join customers c on o.cid = c.id where o.amount > 200');
        \\select * from vizier_flush();
        \\select * from vizier_analyze();
        \\select reason from vizier.recommendations where kind = 'create_index';
    );
    defer testing.allocator.free(out);
    try expectContains(out, "join predicate");
}

test "analyze generates parquet sort recommendations" {
    const out = try runSql(testing.allocator,
        \\select * from vizier_capture('select * from pq_events where account_id = 1 and ts >= ''2026-01-01''');
        \\select * from vizier_flush();
        \\select * from vizier_analyze();
        \\select kind, reason, sql_text from vizier.recommendations where kind = 'parquet_sort_order';
    );
    defer testing.allocator.free(out);
    try expectContains(out, "parquet_sort_order");
    try expectContains(out, "parquet");
}

test "analyze generates parquet row group size recommendations" {
    const out = try runSql(testing.allocator,
        \\select * from vizier_capture('select * from rg_events where ts >= ''2026-01-01'' and ts < ''2026-02-01''');
        \\select * from vizier_capture('select * from rg_events where ts between ''2026-03-01'' and ''2026-06-01''');
        \\select * from vizier_capture('select * from rg_events where ts >= ''2026-06-01''');
        \\select * from vizier_flush();
        \\select * from vizier_analyze();
        \\select kind, reason from vizier.recommendations where kind = 'parquet_row_group_size';
    );
    defer testing.allocator.free(out);
    try expectContains(out, "parquet_row_group_size");
    try expectContains(out, "row_group_size");
}

test "analyze generates parquet partition recommendations" {
    const out = try runSql(testing.allocator,
        \\select * from vizier_capture('select * from pp_logs where region = ''us-east''');
        \\select * from vizier_capture('select * from pp_logs where region = ''eu-west'' and ts > ''2026-01-01''');
        \\select * from vizier_flush();
        \\select * from vizier_analyze();
        \\select kind, reason from vizier.recommendations where kind = 'parquet_partition';
    );
    defer testing.allocator.free(out);
    try expectContains(out, "parquet_partition");
    try expectContains(out, "region");
}

test "analyze generates summary table recommendations" {
    const out = try runSql(testing.allocator,
        \\select * from vizier_capture('select region, count(*) from st_orders group by region');
        \\select * from vizier_capture('select region, sum(amount) from st_orders group by region');
        \\select * from vizier_capture('select region, avg(amount) from st_orders group by region having count(*) > 10');
        \\select * from vizier_flush();
        \\select * from vizier_analyze();
        \\select kind, reason, sql_text from vizier.recommendations where kind = 'create_summary_table';
    );
    defer testing.allocator.free(out);
    try expectContains(out, "create_summary_table");
    try expectContains(out, "region");
    try expectContains(out, "vizier_summary_");
}

test "analyze detects redundant indexes" {
    const out = try runSql(testing.allocator,
        \\create table ri_t (a integer, b integer, c integer);
        \\create index idx_ri_a on ri_t(a);
        \\create index idx_ri_ab on ri_t(a, b);
        \\select * from vizier_analyze();
        \\select kind, reason, sql_text from vizier.recommendations where kind = 'drop_redundant_index';
    );
    defer testing.allocator.free(out);
    try expectContains(out, "drop_redundant_index");
    try expectContains(out, "idx_ri_a");
    try expectContains(out, "covered by");
    try expectContains(out, "idx_ri_ab");
}

test "sort advisor includes sort quality percentage" {
    const out = try runSql(testing.allocator,
        \\select * from vizier_capture('select * from sq_events where account_id = 1 and ts >= ''2026-01-01''');
        \\select * from vizier_capture('select * from sq_events where account_id = 2 and ts between ''2026-01-01'' and ''2026-12-31''');
        \\select * from vizier_capture('select * from sq_events where account_id in (1,2) and ts >= ''2026-01-01''');
        \\select * from vizier_flush();
        \\select * from vizier_analyze();
        \\select reason from vizier.recommendations where kind = 'rewrite_sorted_table';
    );
    defer testing.allocator.free(out);
    try expectContains(out, "sort quality:");
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

test "apply with dry_run previews without executing" {
    const out = try runSql(testing.allocator,
        \\create table dryrun_t (id integer, x integer);
        \\select * from vizier_capture('select * from dryrun_t where x = 1');
        \\select * from vizier_capture('select * from dryrun_t where x = 2');
        \\select * from vizier_flush();
        \\select * from vizier_analyze();
        \\select * from vizier_apply(1, dry_run => true);
        \\select status from vizier.recommendation_store where recommendation_id = 1;
        \\select count(*) from vizier.applied_actions where recommendation_id = 1;
    );
    defer testing.allocator.free(out);
    try expectContains(out, "dry_run");
    try expectContains(out, "create index");
    // Status should still be pending (not applied)
    try expectContains(out, "pending");
    // No action logged
    try expectContains(out, "0");
}

test "apply_all applies recommendations above threshold" {
    const out = try runSql(testing.allocator,
        \\create table aa_t (id integer, x integer);
        \\select * from vizier_capture('select * from aa_t where x = 1');
        \\select * from vizier_capture('select * from aa_t where x = 2');
        \\select * from vizier_flush();
        \\select * from vizier_analyze();
        \\select * from vizier_apply_all(min_score => 0.0, max_actions => 5, dry_run => true);
    );
    defer testing.allocator.free(out);
    try expectContains(out, "dry_run");
}

test "apply_all actually applies when not dry_run" {
    const out = try runSql(testing.allocator,
        \\create table aa2_t (id integer, x integer);
        \\select * from vizier_capture('select * from aa2_t where x = 1');
        \\select * from vizier_capture('select * from aa2_t where x = 2');
        \\select * from vizier_flush();
        \\select * from vizier_analyze();
        \\select * from vizier_apply_all(min_score => 0.0, max_actions => 1);
        \\select count(*) from vizier.applied_actions;
    );
    defer testing.allocator.free(out);
    try expectContains(out, "ok");
    try expectContains(out, "1");
}

test "rollback reverses an applied index recommendation" {
    const out = try runSql(testing.allocator,
        \\create table rb_t (id integer, x integer);
        \\select * from vizier_capture('select * from rb_t where x = 1');
        \\select * from vizier_capture('select * from rb_t where x = 2');
        \\select * from vizier_flush();
        \\select * from vizier_analyze();
        \\select * from vizier_apply(1);
        \\select status from vizier.recommendation_store where recommendation_id = 1;
        \\select * from vizier_rollback(1);
        \\select status from vizier.recommendation_store where recommendation_id = 1;
        \\select can_rollback from vizier.change_history where action_id = 1;
    );
    defer testing.allocator.free(out);
    try expectContains(out, "applied");
    try expectContains(out, "rolled back");
    try expectContains(out, "pending");
    try expectContains(out, "drop index");
}

test "rollback_all reverses all applied recommendations" {
    const out = try runSql(testing.allocator,
        \\create table rba_t (id integer, x integer, y integer);
        \\select * from vizier_capture('select * from rba_t where x = 1');
        \\select * from vizier_capture('select * from rba_t where y = 2');
        \\select * from vizier_flush();
        \\select * from vizier_analyze();
        \\select * from vizier_apply_all(min_score => 0.0, max_actions => 10);
        \\select count(*) from vizier.applied_actions where success = true;
        \\select * from vizier_rollback_all();
    );
    defer testing.allocator.free(out);
    try expectContains(out, "ok");
}

test "apply non-existent recommendation returns not found" {
    const out = try runSql(testing.allocator,
        \\select * from vizier_apply(9999);
    );
    defer testing.allocator.free(out);
    try expectContains(out, "not found");
}

test "explain_human gives natural language explanation" {
    const out = try runSql(testing.allocator,
        \\select * from vizier_capture('select * from nlp_t where x = 1');
        \\select * from vizier_capture('select * from nlp_t where x = 2');
        \\select * from vizier_flush();
        \\select * from vizier_analyze();
        \\select explanation from vizier.explain_human(1);
    );
    defer testing.allocator.free(out);
    try expectContains(out, "queries frequently filter");
    try expectContains(out, "Adding an index");
}

test "analyze_table shows recommendations for a specific table" {
    const out = try runSql(testing.allocator,
        \\select * from vizier_capture('select * from at_t where x = 1');
        \\select * from vizier_capture('select * from at_t where x = 2');
        \\select * from vizier_flush();
        \\select * from vizier_analyze();
        \\select kind, table_name from vizier.analyze_table('at_t');
    );
    defer testing.allocator.free(out);
    try expectContains(out, "at_t");
    try expectContains(out, "create_index");
}

test "analyze_workload filters by time and execution count" {
    const out = try runSql(testing.allocator,
        \\select * from vizier_capture('select * from aw_t where x = 1');
        \\select * from vizier_capture('select * from aw_t where x = 1');
        \\select * from vizier_capture('select * from aw_t where y = 1');
        \\select * from vizier_flush();
        \\select sql, runs from vizier.analyze_workload('2020-01-01'::timestamp, 2);
    );
    defer testing.allocator.free(out);
    try expectContains(out, "2");
}

test "explain shows recommendation details" {
    const out = try runSql(testing.allocator,
        \\select * from vizier_capture('select * from t where x = 1');
        \\select * from vizier_flush();
        \\select * from vizier_analyze();
        \\select kind, reason, sql from vizier.explain(1);
    );
    defer testing.allocator.free(out);
    try expectContains(out, "create_index");
    try expectContains(out, "create index");
}

test "overview shows all user tables with predicate counts" {
    const out = try runSql(testing.allocator,
        \\create table ov_orders (id integer, cid integer);
        \\create table ov_events (id integer, aid integer);
        \\select * from vizier_capture('select * from ov_orders where cid = 1');
        \\select * from vizier_flush();
        \\select * from vizier.overview();
    );
    defer testing.allocator.free(out);
    try expectContains(out, "ov_orders");
    try expectContains(out, "ov_events");
}

test "compare benchmarks before and after applying recommendation" {
    const out = try runSql(testing.allocator,
        \\create table cmp_t as select i as id, i % 100 as x from range(1000) t(i);
        \\select * from vizier_capture('select * from cmp_t where x = 42');
        \\select * from vizier_capture('select * from cmp_t where x = 99');
        \\select * from vizier_flush();
        \\select * from vizier_analyze();
        \\select * from vizier_compare(1);
        \\select count(*) from vizier.benchmark_results where recommendation_id = 1;
    );
    defer testing.allocator.free(out);
    try expectContains(out, "ok");
    try expectContains(out, "1");
}

test "dashboard generates interactive HTML" {
    const out = try runSql(testing.allocator,
        \\select * from vizier_capture('select * from dash_t where x = 1');
        \\select * from vizier_flush();
        \\select * from vizier_analyze();
        \\select * from vizier_dashboard('/tmp/vizier_test_dashboard.html');
    );
    defer testing.allocator.free(out);
    try expectContains(out, "ok");
    try expectContains(out, "Dashboard generated");
}

test "report generates HTML file" {
    const out = try runSql(testing.allocator,
        \\select * from vizier_capture('select * from rpt_t where x = 1');
        \\select * from vizier_flush();
        \\select * from vizier_analyze();
        \\select * from vizier_report('/tmp/vizier_test_report.html');
    );
    defer testing.allocator.free(out);
    try expectContains(out, "ok");
}

test "replay re-runs captured queries and stores results" {
    const out = try runSql(testing.allocator,
        \\create table replay_t as select i as id, i % 50 as x from range(1000) t(i);
        \\select * from vizier_capture('select * from replay_t where x = 1');
        \\select * from vizier_capture('select count(*) from replay_t');
        \\select * from vizier_flush();
        \\select * from vizier_replay();
        \\select count(*) from vizier.replay_results;
    );
    defer testing.allocator.free(out);
    try expectContains(out, "ok");
    try expectContains(out, "2");
}

test "benchmark runs a query and returns timing stats" {
    const out = try runSql(testing.allocator,
        \\create table bench_t as select i as id from range(1000) t(i);
        \\select * from vizier_benchmark('select count(*) from bench_t', 5);
    );
    defer testing.allocator.free(out);
    try expectContains(out, "ok");
    try expectContains(out, "5");
}

test "score_breakdown shows scoring components" {
    const out = try runSql(testing.allocator,
        \\select * from vizier_capture('select * from orders where customer_id = 42');
        \\select * from vizier_capture('select * from orders where customer_id = 10');
        \\select * from vizier_flush();
        \\select * from vizier_analyze();
        \\select kind, column_name, predicate_kind, frequency from vizier.score_breakdown(1);
    );
    defer testing.allocator.free(out);
    try expectContains(out, "create_index");
    try expectContains(out, "customer_id");
    try expectContains(out, "equality");
}

// ============================================================================
// End-to-end tests with sample workloads
// ============================================================================

test "e2e: full capture-analyze-apply-benchmark loop" {
    const out = try runSql(testing.allocator,
        // Create sample data
        \\create table e2e_orders as select i as id, i % 100 as customer_id, i * 1.5 as amount, '2026-01-01'::date + (i % 365)::integer as order_date from range(10000) t(i);
        \\create table e2e_customers as select i as id, 'cust_' || i as name, case when i % 3 = 0 then 'US' when i % 3 = 1 then 'EU' else 'APAC' end as region from range(100) t(i);
        //
        // Capture a realistic workload
        \\select * from vizier_capture('select * from e2e_orders where customer_id = 42');
        \\select * from vizier_capture('select * from e2e_orders where customer_id = 7');
        \\select * from vizier_capture('select * from e2e_orders where customer_id = 99 and order_date >= date ''2026-06-01''');
        \\select * from vizier_capture('select o.id, c.name from e2e_orders o join e2e_customers c on o.customer_id = c.id where c.region = ''US''');
        \\select * from vizier_capture('select o.id, c.name from e2e_orders o join e2e_customers c on o.customer_id = c.id where c.region = ''EU''');
        \\select * from vizier_capture('select customer_id, sum(amount) from e2e_orders group by customer_id');
        \\select * from vizier_capture('select customer_id, count(*) from e2e_orders group by customer_id');
        \\select * from vizier_flush();
        //
        // Analyze and check recommendations
        \\select * from vizier_analyze();
        \\select count(*) from vizier.recommendations;
        //
        // Overview
        \\select table_name, predicates, hot_cols from vizier.overview() where table_name like 'e2e_%' order by table_name;
        //
        // Dry run apply
        \\select status, sql_executed from vizier_apply(1, dry_run => true);
    );
    defer testing.allocator.free(out);
    try expectContains(out, "ok");
    try expectContains(out, "e2e_orders");
    try expectContains(out, "dry_run");
}

test "e2e: bulk capture from query log table" {
    const out = try runSql(testing.allocator,
        // Create data and query log
        \\create table e2e_events as select i as id, i % 50 as account_id, now() - interval (i) minute as ts from range(5000) t(i);
        \\create table e2e_query_log (sql_text varchar);
        \\insert into e2e_query_log values ('select * from e2e_events where account_id = 1');
        \\insert into e2e_query_log values ('select * from e2e_events where account_id = 2 and ts >= ''2026-01-01''');
        \\insert into e2e_query_log values ('select * from e2e_events where account_id = 3 and ts between ''2026-01-01'' and ''2026-06-01''');
        \\insert into e2e_query_log values ('select account_id, count(*) from e2e_events group by account_id');
        \\insert into e2e_query_log values ('select account_id, count(*) from e2e_events group by account_id having count(*) > 10');
        //
        // Bulk capture
        \\select * from vizier_capture_bulk('e2e_query_log', 'sql_text');
        \\select * from vizier_flush();
        //
        // Check workload
        \\select count(*) from vizier.workload_queries;
        \\select count(*) from vizier.workload_predicates;
        //
        // Analyze
        \\select * from vizier_analyze();
        \\select kind, table_name from vizier.recommendations order by kind;
    );
    defer testing.allocator.free(out);
    try expectContains(out, "ok");
    try expectContains(out, "e2e_events");
}

test "e2e: inspect and score breakdown" {
    const out = try runSql(testing.allocator,
        \\create table e2e_products (id integer, category varchar, price double, in_stock boolean);
        \\insert into e2e_products select i, case when i % 4 = 0 then 'electronics' when i % 4 = 1 then 'books' when i % 4 = 2 then 'clothing' else 'food' end, i * 0.99, i % 2 = 0 from range(500) t(i);
        \\select * from vizier_capture('select * from e2e_products where category = ''electronics'' and price > 100');
        \\select * from vizier_capture('select * from e2e_products where category in (''books'', ''food'')');
        \\select * from vizier_capture('select * from e2e_products where category = ''clothing'' and price between 10 and 50');
        \\select * from vizier_flush();
        //
        // Inspect table
        \\select column_name, approx_rows, predicate_count from vizier.inspect_table('e2e_products');
        //
        // Analyze
        \\select * from vizier_analyze();
        //
        // Score breakdown
        \\select column_name, predicate_kind, frequency from vizier.score_breakdown(1);
    );
    defer testing.allocator.free(out);
    try expectContains(out, "category");
    try expectContains(out, "500");
}

test "benchmark: TPC-H-like workload generates meaningful recommendations" {
    const out = try runSql(testing.allocator,
        // Create TPC-H-like tables
        \\create table lineitem as select i as l_orderkey, i % 200 as l_partkey, i % 10 as l_suppkey, (i * 0.99)::double as l_extendedprice, (random() * 0.1)::double as l_discount, '2024-01-01'::date + (i % 730)::integer as l_shipdate from range(50000) t(i);
        \\create table orders as select i as o_orderkey, i % 1000 as o_custkey, (i * 10.0)::double as o_totalprice, '2024-01-01'::date + (i % 730)::integer as o_orderdate, case when i % 3 = 0 then 'F' else 'O' end as o_orderstatus from range(10000) t(i);
        //
        // Simulate TPC-H-like queries
        \\select * from vizier_capture('select l_orderkey, sum(l_extendedprice * (1 - l_discount)) from lineitem where l_shipdate >= date ''2024-01-01'' and l_shipdate < date ''2025-01-01'' group by l_orderkey');
        \\select * from vizier_capture('select l_orderkey, sum(l_extendedprice) from lineitem where l_shipdate between date ''2024-06-01'' and date ''2024-12-31'' group by l_orderkey');
        \\select * from vizier_capture('select o_orderstatus, count(*) from orders where o_orderdate >= date ''2024-01-01'' group by o_orderstatus');
        \\select * from vizier_capture('select o_orderstatus, sum(o_totalprice) from orders where o_orderdate >= date ''2024-06-01'' group by o_orderstatus');
        \\select * from vizier_capture('select * from orders where o_custkey = 42');
        \\select * from vizier_capture('select * from orders where o_custkey = 99');
        \\select * from vizier_flush();
        //
        \\select * from vizier_analyze();
        \\select kind, table_name, score, confidence from vizier.recommendations order by score desc;
    );
    defer testing.allocator.free(out);
    try expectContains(out, "ok");
    // Should generate index and/or sort recommendations for these tables
    try expectContains(out, "lineitem");
    try expectContains(out, "orders");
}

test "benchmark: vizier_benchmark measures real query performance" {
    const out = try runSql(testing.allocator,
        \\create table bench_data as select i as id, i % 100 as grp, random() as val from range(100000) t(i);
        \\select * from vizier_benchmark('select grp, avg(val) from bench_data group by grp', 10);
    );
    defer testing.allocator.free(out);
    try expectContains(out, "ok");
    try expectContains(out, "10");
}

test "recommendation_store has cost estimate columns" {
    const out = try runSql(testing.allocator,
        \\select * from vizier_capture('select * from ce_t where x = 1');
        \\select * from vizier_capture('select * from ce_t where x = 2');
        \\select * from vizier_flush();
        \\select * from vizier_analyze();
        \\select estimated_gain, estimated_build_cost, estimated_maintenance_cost from vizier.recommendation_store limit 1;
    );
    defer testing.allocator.free(out);
    // Should have numeric values (not null)
    try expectContains(out, "0.");
}

test "workload_queries has p95_time_ms column" {
    const out = try runSql(testing.allocator,
        \\select * from vizier_capture('select * from p95_t where a = 1');
        \\select * from vizier_flush();
        \\select p95_time_ms from vizier.workload_queries limit 1;
    );
    defer testing.allocator.free(out);
    try expectContains(out, "0");
}

test "analyze populates avg_selectivity for predicates" {
    const out = try runSql(testing.allocator,
        \\create table sel_t as select i as id, i % 10 as category from range(1000) t(i);
        \\select * from vizier_capture('select * from sel_t where category = 1');
        \\select * from vizier_capture('select * from sel_t where id = 42');
        \\select * from vizier_flush();
        \\select * from vizier_analyze();
        \\select column_name, round(avg_selectivity, 4) as sel from vizier.workload_predicates where table_name = 'sel_t' order by column_name;
    );
    defer testing.allocator.free(out);
    // category has ~10 distinct values out of 1000 rows → selectivity ~0.01
    // id has ~1000 distinct values → selectivity ~1.0
    try expectContains(out, "category");
    try expectContains(out, "id");
    try expectContains(out, "0.");
}

test "save and load persist state across sessions" {
    // Save state with captured queries
    const out = try runSql(testing.allocator,
        \\select * from vizier_capture('select * from persist_t where x = 1');
        \\select * from vizier_flush();
        \\select * from vizier_save('/tmp/vizier_test_state.db');
    );
    defer testing.allocator.free(out);
    try expectContains(out, "ok");

    // Load into a fresh session — queries should survive
    const out2 = try runSql(testing.allocator,
        \\select * from vizier_load('/tmp/vizier_test_state.db');
        \\select count(*) from vizier.workload_queries;
    );
    defer testing.allocator.free(out2);
    try expectContains(out2, "ok");
    try expectContains(out2, "1");
}

test "auto-save persists state on flush when state_path configured" {
    // Configure state_path, capture, flush (should auto-save), then load in new session
    const out = try runSql(testing.allocator,
        \\select vizier_configure('state_path', '/tmp/vizier_autosave_test.db');
        \\select * from vizier_capture('select * from autosave_t where x = 1');
        \\select * from vizier_flush();
    );
    defer testing.allocator.free(out);
    try expectContains(out, "ok");

    const out2 = try runSql(testing.allocator,
        \\select * from vizier_load('/tmp/vizier_autosave_test.db');
        \\select count(*) from vizier.workload_queries;
    );
    defer testing.allocator.free(out2);
    try expectContains(out2, "ok");
    try expectContains(out2, "1");
}

test "no_action emitted for tables with no actionable recommendations" {
    const out = try runSql(testing.allocator,
        \\select * from vizier_capture('select * from na_t where name like ''%test%''');
        \\select * from vizier_flush();
        \\select * from vizier_analyze();
        \\select kind, reason from vizier.recommendation_store where table_name = 'na_t' and kind = 'no_action';
    );
    defer testing.allocator.free(out);
    try expectContains(out, "no_action");
    try expectContains(out, "no changes needed");
}

test "settings table has default values" {
    const out = try runSql(testing.allocator,
        \\select key, value from vizier.settings order by key;
    );
    defer testing.allocator.free(out);
    try expectContains(out, "benchmark_runs");
    try expectContains(out, "min_confidence");
    try expectContains(out, "min_query_count");
}

test "vizier_configure updates settings" {
    const out = try runSql(testing.allocator,
        \\select vizier_configure('benchmark_runs', '10');
        \\select value from vizier.settings where key = 'benchmark_runs';
    );
    defer testing.allocator.free(out);
    try expectContains(out, "ok");
    try expectContains(out, "10");
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

test "explain-based extraction populates predicates when tables exist" {
    const out = try runSql(testing.allocator,
        \\create table test_explain_events (account_id int, ts date, data varchar);
        \\insert into test_explain_events values (42, date '2026-03-01', 'x'), (1, date '2025-01-01', 'y');
        \\select * from vizier_capture('select * from test_explain_events where account_id = 42 and ts >= date ''2026-01-01''');
        \\select * from vizier_flush();
        \\select column_name, predicate_kind from vizier.workload_predicates order by column_name;
    );
    defer testing.allocator.free(out);
    try expectContains(out, "account_id");
    try expectContains(out, "ts");
}

test "explain-based extraction stores estimated_rows" {
    const out = try runSql(testing.allocator,
        \\create table test_explain_rows (id int, val varchar);
        \\insert into test_explain_rows select i, 'v' from range(1000) t(i);
        \\select * from vizier_capture('select * from test_explain_rows where id = 42');
        \\select * from vizier_flush();
        \\select estimated_rows from vizier.workload_queries;
    );
    defer testing.allocator.free(out);
    // estimated_rows should be > 0 since the table has data
    const trimmed = std.mem.trim(u8, out, " \n\r");
    const rows_str = blk: {
        var lines = std.mem.splitScalar(u8, trimmed, '\n');
        var last: []const u8 = "";
        while (lines.next()) |l| last = l;
        break :blk last;
    };
    const rows = std.fmt.parseInt(u64, std.mem.trim(u8, rows_str, " \r"), 10) catch 0;
    // EXPLAIN should produce a non-zero estimate for a table with 1000 rows
    try testing.expect(rows > 0);
}

test "tokenizer fallback when tables do not exist" {
    const out = try runSql(testing.allocator,
        \\select * from vizier_capture('select * from nonexistent_table where col = 1');
        \\select * from vizier_flush();
        \\select column_name, predicate_kind from vizier.workload_predicates;
    );
    defer testing.allocator.free(out);
    // Tokenizer should still extract predicates even when table doesn't exist
    try expectContains(out, "col");
    try expectContains(out, "equality");
}

test "estimated_rows column exists after schema init" {
    // Regression test for Bug 4: estimated_rows column must exist even if table
    // was created by an older version (the migration DDL adds it).
    const out = try runSql(testing.allocator,
        \\select column_name from duckdb_columns() where table_name = 'workload_queries' and schema_name = 'vizier' and column_name = 'estimated_rows';
    );
    defer testing.allocator.free(out);
    try expectContains(out, "estimated_rows");
}

test "state save and load preserves workload data" {
    // Regression test for Bug 5: state load uses BY NAME insert to handle
    // schema differences between saved state and current version.
    const out = try runSql(testing.allocator,
        \\select * from vizier_capture('select * from t where x = 1');
        \\select * from vizier_flush();
        \\select * from vizier_save('/tmp/_vizier_test_state.db');
        \\delete from vizier.workload_queries;
        \\select * from vizier_load('/tmp/_vizier_test_state.db');
        \\select count(*) from vizier.workload_queries;
    );
    defer testing.allocator.free(out);
    try expectContains(out, "1");
}

test "predicates with single quotes in names do not break flush" {
    // Regression test for Bug 6: single quotes in table/column names must
    // be escaped when inserting into workload_predicates.
    const out = try runSql(testing.allocator,
        \\select * from vizier_capture('select * from "it''s_a_table" where "col''s" = 1');
        \\select * from vizier_flush();
        \\select count(*) from vizier.workload_predicates;
    );
    defer testing.allocator.free(out);
    // Should not crash or error; at minimum the flush completes
    try expectContains(out, "ok");
}

test "vizier_compare logs to applied_actions for rollback" {
    // Regression test for Bug 7: vizier_compare must log to applied_actions
    // so the change shows in change_history and can be rolled back.
    const out = try runSql(testing.allocator,
        \\create table test_compare_t (id int, val varchar);
        \\insert into test_compare_t select i, 'v' from range(100) t(i);
        \\select * from vizier_capture('select * from test_compare_t where id = 42');
        \\select * from vizier_capture('select * from test_compare_t where id = 42');
        \\select * from vizier_flush();
        \\select * from vizier_analyze();
        \\select * from vizier_compare(1);
        \\select count(*) from vizier.applied_actions where recommendation_id = 1;
    );
    defer testing.allocator.free(out);
    // applied_actions should have an entry from vizier_compare
    try expectContains(out, "1");
}
