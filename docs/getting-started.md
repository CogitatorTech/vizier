# Getting Started

## Build from Source

Vizier requires Zig 0.15.2 and a DuckDB binary (version `1.2.0` or newer) for testing.

```bash
git clone --depth=1 https://github.com/CogitatorTech/vizier.git
cd vizier

# Build the extension
make build-all

# Run tests (optional; needs DuckDB in PATH)
make test

# Try Vizier interactively (needs DuckDB in PATH)
make duckdb
```

## Install from a Release

Open the [latest release](https://github.com/CogitatorTech/vizier/releases/latest), find the platform-specific `.duckdb_extension` asset, copy its download URL, and install that asset URL before `load vizier;`.

Choose the asset that matches your platform:

- Linux AMD64: `vizier-linux-amd64.duckdb_extension`
- Linux ARM64: `vizier-linux-arm64.duckdb_extension`
- Linux AMD64 (musl): `vizier-linux-amd64-musl.duckdb_extension`
- Linux ARM64 (musl): `vizier-linux-arm64-musl.duckdb_extension`
- macOS ARM64: `vizier-macos-arm64.duckdb_extension`
- Windows AMD64: `vizier-windows-amd64.duckdb_extension`
- Windows ARM64: `vizier-windows-arm64.duckdb_extension`
- FreeBSD AMD64: `vizier-freebsd-amd64.duckdb_extension`

Replace `v0.1.0` with the release tag you want:

```sql
install 'https://github.com/CogitatorTech/vizier/releases/download/v0.1.0/vizier-linux-amd64.duckdb_extension';
load vizier;
```

> Vizier is currently distributed as an unsigned extension.
> Start DuckDB with `-unsigned`, or enable unsigned extensions in your client before installing and loading Vizier.

## Loading the Extension

After choosing the correct `.duckdb_extension` asset from the release page for your platform:

```sql
install 'https://github.com/CogitatorTech/vizier/releases/download/v0.1.0/vizier-linux-amd64.duckdb_extension';
load vizier;
```

## Basic Workflow

Vizier follows a capture, analyze, and apply workflow.

### 1. Capture Queries

Feed Vizier the queries you run against your database:

```sql
select * from vizier_capture('select * from events where account_id = 42 and ts >= date ''2026-01-01''');
select * from vizier_capture('select count(*) from orders group by customer_id');
select * from vizier_capture('select * from events where account_id = 42 and ts >= date ''2026-01-01''');
```

Flush captured queries to metadata tables:

```sql
select * from vizier_flush();
```

### 2. Review the Workload

```sql
select * from vizier.workload_summary;
```

```
┌──────────────┬──────────────────────────────────────────────────────────────────────────┬───────┬─────────────────────┬────────┐
│     sig      │                                   sql                                    │ runs  │      last_seen      │ avg_ms │
├──────────────┼──────────────────────────────────────────────────────────────────────────┼───────┼─────────────────────┼────────┤
│ 8c9d7175aab0 │ SELECT * FROM events WHERE account_id = 42 AND ts >= DATE '2026-01-01'   │     2 │ 2026-03-22 16:44:06 │    0.0 │
│ 2af899d9dba6 │ SELECT count(*) FROM orders GROUP BY customer_id                         │     1 │ 2026-03-22 16:44:06 │    0.0 │
└──────────────┴──────────────────────────────────────────────────────────────────────────┴───────┴─────────────────────┴────────┘
```

### 3. Analyze and Get Recommendations

```sql
select * from vizier_analyze();
select * from vizier.recommendations;
```

Each recommendation includes a kind, target table, SQL to execute, score, and confidence level.

### 4. Apply a Recommendation

Preview first with a dry run:

```sql
select * from vizier_apply(1, dry_run = > true);
```

Then apply:

```sql
select * from vizier_apply(1);
```

### 5. Measure the Impact

```sql
select * from vizier_benchmark('select * from events where account_id = 42', 10);
select * from vizier_compare(1);
```

## Persistent State

By default, Vizier state is in-memory. To persist across sessions:

```sql
-- Initialize with a state file
select * from vizier_init('/path/to/vizier_state.db');

-- Or configure the path (auto-loads on next extension init)
select vizier_configure('state_path', '/path/to/vizier_state.db');
```

You can also export and import state between environments:

```sql
select * from vizier_save('/tmp/vizier_state.db');
select * from vizier_load('/tmp/vizier_state.db');
```

## Bulk Capture

If you have a table with query logs:

```sql
select * from vizier_capture_bulk('query_log', 'sql_text');
select * from vizier_flush();
```

## Session Capture

Capture all queries in a session without calling `vizier_capture()` per query:

```sql
select * from vizier_start_capture();
-- run your queries here
select * from vizier_stop_capture();
```

## Import from Profiling Output

If you have a DuckDB JSON profiling file:

```sql
select * from vizier_import_profile('/path/to/profile.json');
select * from vizier_flush();
```
