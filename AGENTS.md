# AGENTS.md

This file provides guidance to coding agents collaborating on this repository.

## Mission

Vizier is a physical design advisor for DuckDB, implemented as a DuckDB extension in Zig and C.
Priorities, in order:

1. Correct, trustworthy recommendations backed by transparent scoring.
2. Clean SQL API that feels natural to DuckDB users.
3. Clear separation between C (DuckDB API glue) and Zig (business logic).
4. Maintainable and well-tested code.

## Core Rules

- Use English for code, comments, docs, and tests.
- Keep global mutable state minimal; currently limited to the capture buffer and DB handle in `extension.c`.
- Prefer small, focused changes over large refactoring.
- Add comments only when they clarify non-obvious behavior.
- Do not add features, error handling, or abstractions beyond what is needed for the current task.

## Repository Layout

- `src/extension.c`: C entry point, DuckDB function registration, capture/flush logic.
- `src/lib.zig`: Root Zig module with C-exported functions.
- `src/lib_test.zig`: Unit tests (no DuckDB runtime required).
- `src/duckdb.zig`: Auto-generated DuckDB C API bindings (do not edit manually).
- `src/vizier/schema.zig`: DDL for metadata tables and views.
- `src/vizier/capture.zig`: Query hashing and INSERT SQL building.
- `src/vizier/sql_runner.zig`: Connect/query/disconnect wrapper using `duckdb_ext_api`.
- `src/vizier/summary.zig`: Workload summary view SQL definition.
- `scripts/`: Build tooling (metadata appender script).
- `build.zig` / `build.zig.zon`: Zig build configuration and dependencies.
- `Makefile`: GNU Make wrapper around `zig build`.

## Architecture Constraints

### C / Zig Split

- `extension.c` owns the DuckDB entry point (`DUCKDB_EXTENSION_ENTRYPOINT`), function registration, and table function callbacks (bind/init/execute).
- Zig modules under `src/vizier/` own business logic: schema DDL, hashing, SQL construction.
- C calls Zig via `extern` functions exported with `callconv(.c)`.
- Zig calls DuckDB API functions through the `duckdb_ext_api` global struct (not direct `extern fn` from `duckdb.zig`).

### DuckDB Extension API Limitations

These are hard constraints discovered during development:

- **No new connections from table function callbacks.** Opening `duckdb_connect` from bind/init/execute callbacks causes segfaults on subsequent
  calls. Use the in-memory buffer + flush pattern instead.
- **`duckdb_value_*` functions are not in `duckdb_ext_api_v1`.** Use `duckdb_fetch_chunk` + vector operations to read query results.
- **Type mismatches cause segfaults.** When copying vector data between chunks, the source column types must match exactly (e.g., INT32 vs BIGINT).
  Use explicit casts in SQL queries if needed.
- **`duckdb_query` executes only one statement.** Multi-statement strings silently ignore everything after the first semicolon.
- **`access->get_database(info)` may return a temporary pointer.** Heap-copy the `duckdb_database` value if storing it beyond the entry point scope.

### Capture / Flush Pattern

Queries are buffered in a global C array (`g_pending`), not written to DB immediately.
`vizier_flush()` opens a connection and writes all pending captures in one batch.
`vizier.workload_summary` is a SQL VIEW, not a table function, to avoid snapshot isolation issues.

## Zig and C Conventions

- Zig code uses the `duckdb` module (imported via `build.zig`) for type definitions.
- Zig code accesses DuckDB functions via `extern var duckdb_ext_api: duckdb.duckdb_ext_api_v1`.
- C code uses `-std=c11` and includes `duckdb_extension.h`.
- C formatting is handled by `clang-format`; Zig formatting by `zig fmt`.

## Required Validation

Run these checks for any change:

1. `make build-all` (builds extension with DuckDB metadata)
2. `make test` (unit tests)

For integration testing:

3. `make duckdb` then exercise the changed functionality interactively.

Quick smoke test:

```sql
LOAD vizier;

SELECT vizier_version();
SELECT * FROM vizier_capture_query('SELECT 1');
SELECT * FROM vizier_flush();
SELECT * FROM vizier.workload_summary;
```

## First Contribution Flow

1. Read `src/extension.c` and the relevant `src/vizier/*.zig` module.
2. Implement the smallest possible change.
3. Add or update tests in `src/lib_test.zig` or inline `test` blocks in Zig modules.
4. Run `make build-all && make test`.
5. Verify interactively with `make duckdb` if the change affects SQL-visible behavior.

Good first tasks:

- Add a new metadata column to a vizier table (update DDL in `schema.zig`).
- Improve query normalization in `capture.zig` (strip whitespace, lowercase keywords).
- Add a new scalar function (follow the `vizier_version` pattern in `extension.c`).

## Testing Expectations

- No SQL-facing change is complete without a test or interactive verification.
- Unit tests in Zig should not require DuckDB runtime; test pure logic (hashing, SQL building).
- Integration tests use `make test-extension` or manual `make duckdb` verification.

## Change Design Checklist

Before coding:

1. Identify whether the change touches C callbacks, Zig logic, or both.
2. Check if new DuckDB API functions are needed and verify they exist in `duckdb_ext_api_v1`.
3. Consider the capture/flush constraint if the change involves writing data from callbacks.

Before submitting:

1. `make build-all` succeeds.
2. `make test` passes.
3. Interactive smoke test passes if SQL behavior is changed.
4. Docs updated if the API surface changed.

## Commit and PR Hygiene

- Keep commits scoped to one logical change.
- PR descriptions should include:
    1. Behavioral change summary.
    2. Tests added/updated.
    3. Interactive verification done (yes/no).
    4. Docs updated (yes/no).

Suggested PR checklist:

- [ ] Tests added/updated for behavior changes
- [ ] `make build-all` passes
- [ ] `make test` passes
- [ ] Interactive smoke test done (if SQL behavior is changed)
- [ ] Docs/README updated (if API surface changed)
