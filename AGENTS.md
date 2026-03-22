# AGENTS.md

This file provides guidance to coding agents collaborating on this repository.

## Mission

Vizier is a database advisor and finetuner for DuckDB.
It is implemented as a DuckDB extension in Zig and C.
Priorities, in order:

1. Correct, trustworthy recommendations backed by transparent scoring.
2. Clean SQL API that feels natural to DuckDB users.
3. Clear separation between C (DuckDB API glue) and Zig (business logic).
4. Maintainable and well-tested code.

## Core Rules

- Use English for code, comments, docs, and tests.
- Keep global mutable state minimal; currently limited to the capture buffer and persistent connection (`g_flush_conn`) in `extension.c`.
- Prefer small, focused changes over large refactoring.
- Add comments only when they clarify non-obvious behavior.
- Do not add features, error handling, or abstractions beyond what is needed for the current task.

## Repository Layout

- `src/extension.c`: C entry point, DuckDB function registration, capture/flush logic. Uses `add_pending_capture()` helper for all capture
  paths and `REGISTER_TABLE_FUNC_0` macro for no-parameter table function registration.
- `src/lib.zig`: Root Zig module with C-exported functions. Key exports: `zig_run_all_advisors(conn)` orchestrates all advisors,
  `zig_extract_and_store(conn, sig, sql)` populates predicates/JSON, `zig_normalize_sql()` for query normalization.
- `src/lib_test.zig`: Test harness that pulls in all inline module tests.
- `src/duckdb.zig`: Auto-generated DuckDB C API bindings (do not edit manually).
- `src/vizier/schema.zig`: DDL for metadata tables and views.
- `src/vizier/capture.zig`: Query hashing and INSERT SQL building.
- `src/vizier/extract.zig`: Heuristic SQL tokenizer and predicate/table extractor (pure Zig, no DB deps). Captures both sides of JOIN conditions.
- `src/vizier/sql_runner.zig`: Connect/query/disconnect wrapper using `duckdb_ext_api`. Also `runOnConn()` for reusing an existing connection.
- `src/vizier/inspect.zig`: Table reference parsing (pure Zig). The `inspect_table` macro is defined in `schema.zig`.
- `src/vizier/advisor.zig`: All advisor SQL queries (index, sort, redundant index, parquet layout, summary table, join-path, no-action),
  the `all_advisor_sqls` array for orchestration, and recommendation helper functions.
- `src/vizier/summary.zig`: Workload summary view SQL definition.
- `tests/property_tests.zig`: Property-based tests (using the Minish framework).
- `tests/integration_tests.zig`: Integration tests that spawn DuckDB and validate output.
- `tests/sql/`: Standalone SQL test files runnable with `duckdb -unsigned -c ".read file.sql"`.
- `benches/`: SQL-based benchmarks with timing metrics. Run via `make bench` or `./benches/run.sh`.
- `scripts/`: Build tooling (metadata appender script).
- `build.zig` / `build.zig.zon`: Zig build configuration and dependencies.
- `Makefile`: GNU Make wrapper around `zig build`.

## Architecture Constraints

### C / Zig Split

- `extension.c` owns the DuckDB entry point (`DUCKDB_EXTENSION_ENTRYPOINT`), function registration, and table function callbacks (bind/init/execute).
- Zig modules under `src/vizier/` own business logic: schema DDL, hashing, SQL construction, and advisor orchestration.
- C calls Zig via `extern` functions exported with `callconv(.c)`. Advisor execution is fully delegated to `zig_run_all_advisors(conn)`.
- Zig calls DuckDB API functions through the `duckdb_ext_api` global struct (not direct `extern fn` from `duckdb.zig`).

### DuckDB Extension API Limitations

These are hard constraints discovered during development:

- No new connections from table function callbacks after init. Opening `duckdb_connect` from callbacks causes segfaults or stale handles. Use the
  persistent connection (`g_flush_conn`) opened during init, or use SQL VIEWs/MACROs that run on the user's connection.
- `duckdb_value_*` functions are not in `duckdb_ext_api_v1`. Use `duckdb_fetch_chunk` + vector operations to read query results.
- Type mismatches cause segfaults.** When copying vector data between chunks, the source column types must match exactly (e.g., INT32 vs BIGINT).
  Use explicit casts in SQL queries if needed.
- `duckdb_query` executes only one statement.** Multi-statement strings silently ignore everything after the first semicolon.
- `access->get_database(info)` may return a temporary pointer. Heap-copy the `duckdb_database` value if storing it beyond the entry point scope.

### Capture / Flush Pattern

Queries are buffered in a global C array (`g_pending`), not written to DB immediately.
`vizier_flush()` uses a persistent connection (`g_flush_conn`) opened during extension init to write all pending captures.
`vizier.workload_summary` and `vizier.inspect_table()` are SQL VIEWs/MACROs, not table functions, because they run on the user's connection and avoid
snapshot isolation issues.

Multiple capture methods feed into the same `g_pending` buffer:

- `vizier_capture(sql)` — single query capture.
- `vizier_capture_bulk(table, column)` — reads SQL from a table column via `g_flush_conn`.
- `vizier_start_capture()` / `vizier_session_log(sql)` / `vizier_stop_capture()` — session-based capture using `vizier.session_log` table.
- `vizier_import_profile(path)` — reads from DuckDB JSON profiling output via `read_json_auto`.

## Zig and C Conventions

- Zig code uses the `duckdb` module (imported via `build.zig`) for type definitions.
- Zig code accesses DuckDB functions via `extern var duckdb_ext_api: duckdb.duckdb_ext_api_v1`.
- C code uses `-std=c11` and includes `duckdb_extension.h`.
- C formatting is handled by `clang-format`; Zig formatting by `zig fmt`.
- SQL keywords must be lowercase. Write `select`, `from`, `where`, `create table`, not `SELECT`, `FROM`, `WHERE`, `CREATE TABLE`. This applies to all
  SQL strings in Zig modules, C source, and generated recommendation SQL.

## Required Validation

Run `make test` for any change. This runs all three test suites:

| Target            | Command                 | What it runs                                                    |
|-------------------|-------------------------|-----------------------------------------------------------------|
| Unit + regression | `make test-unit`        | Inline `test` blocks in `src/vizier/*.zig`                      |
| Property-based    | `make test-property`    | `tests/property_tests.zig` with Minish                          |
| Integration       | `make test-integration` | `tests/integration_tests.zig` — spawns DuckDB, validates output |
| All               | `make test`             | Runs all of the above                                           |

For interactive exploration: `make duckdb`.

## First Contribution Flow

1. Read `src/extension.c` and the relevant `src/vizier/*.zig` module.
2. Implement the smallest possible change.
3. Add or update inline `test` blocks in the changed Zig module. Add integration tests in `tests/integration_tests.zig` if SQL behavior changed.
4. Run `make test`.
5. Verify interactively with `make duckdb` if needed.

Good first tasks:

- Add a new metadata column to a vizier table (update DDL in `schema.zig`).
- Add a new advisor in `advisor.zig` (define SQL, add to `all_advisor_sqls` array — it auto-registers).
- Add a new scalar function (follow the `vizier_version` pattern in `extension.c`).
- Add a new table macro (add SQL constant in `schema.zig`, register in `ddl_statements` array).

## Testing Expectations

- Unit/regression tests live as inline `test` blocks in the module they cover (`src/vizier/*.zig`). `src/lib_test.zig` is just a harness that
  pulls them in. No DuckDB runtime is required.
- Property-based tests live in `tests/property_tests.zig` using the Minish framework. Use these to fuzz parsers and verify invariants (like the
  "tokenizer never crashes on arbitrary input").
- Integration tests live in `tests/integration_tests.zig`. They spawn `duckdb` as a child process, load the extension, run SQL, and assert on the
  output. Add a test here for any new SQL-visible function.
- No SQL-facing change is complete without an integration test.

## Change Design Checklist

Before coding:

1. Identify whether the change touches C callbacks, Zig logic, or both.
2. Check if new DuckDB API functions are needed and verify they exist in `duckdb_ext_api_v1`.
3. Consider the capture or flush constraint if the change involves writing data from callbacks.

Before submitting:

1. `make test` passes (all three suites).
2. Docs updated if the API surface changed.

## Commit and PR Hygiene

- Keep commits scoped to one logical change.
- PR descriptions should include:
    1. Behavioral change summary.
    2. Tests added/updated.
    3. Interactive verification done (yes/no).
    4. Docs updated (yes/no).

Suggested PR checklist:

- [ ] Inline unit tests added/updated for logic changes
- [ ] Integration test added for new SQL-visible functions
- [ ] `make test` passes (unit + property + integration)
- [ ] Docs/README updated (if API surface changed)
