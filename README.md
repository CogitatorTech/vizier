<div align="center">

<h2>Vizier</h2>

[![Tests](https://img.shields.io/github/actions/workflow/status/CogitatorTech/vizier/tests.yml?label=tests&style=flat&labelColor=282c34&logo=github)](https://github.com/CogitatorTech/vizier/actions/workflows/tests.yml)
[![Benchmarks](https://img.shields.io/github/actions/workflow/status/CogitatorTech/vizier/benchmarks.yml?label=benchmarks&style=flat&labelColor=282c34&logo=github)](https://github.com/CogitatorTech/vizier/actions/workflows/benchmarks.yml)
[![License](https://img.shields.io/badge/license-MIT-007ec6?label=license&style=flat&labelColor=282c34&logo=open-source-initiative)](https://github.com/CogitatorTech/vizier/blob/main/LICENSE)
[![Zig Version](https://img.shields.io/badge/Zig-0.15.2-orange?logo=zig&labelColor=282c34)](https://ziglang.org/download/)
[![Release](https://img.shields.io/github/release/CogitatorTech/vizier.svg?label=release&style=flat&labelColor=282c34&logo=github)](https://github.com/CogitatorTech/vizier/releases/latest)

A database advisor and finetuner for DuckDB

</div>

---


Vizier is a DuckDB extension that analyzes your query workload and recommends physical design changes like indexes, sort orders, Parquet layouts,
and summary tables to improve query performance.

When you have a DuckDB database, you are typically on your own to figure out things like:

- Which columns need indexes?
- Should I rewrite my table sorted differently?
- What sort order should my Parquet exports use?
- Are any of my indexes redundant?
- Which queries are my bottleneck?

Tools like [pg_qualstats](https://github.com/powa-team/pg_qualstats) (for PostgreSQL) and
[Database Engine Tuning Advisor](https://learn.microsoft.com/en-us/sql/relational-databases/performance/database-engine-tuning-advisor?view=sql-server-ver17)
(for SQL Server) solve these problems, but nothing equivalent exists for DuckDB.
Vizier aims to fill that gap.

### Features

- Fully cross-platform (Windows, Linux, macOS, and FreeBSD)
- Supports DuckDB version `1.2.0` or newer
- Query pattern and execution stat capture
- Physical design inspection (indexes, table sizes, cardinalities, and storage layout)
- Workload analysis with bottleneck detection
- Index, sort order, Parquet layout, and summary table recommendations
- Dry-run support and before/after benchmarking
- Rollback for applied recommendations
- Persistent state across sessions
- Static HTML report generation

See [ROADMAP.md](ROADMAP.md) for the list of implemented and planned features.

> [!IMPORTANT]
> This project is still in early development, so compatibility is not perfect.
> Bugs and breaking changes are also expected.
> Please use the [issues page](https://github.com/CogitatorTech/vizier/issues) to report bugs or request features.

---

### Quickstart

#### Install in DuckDB

Install Vizier from the hosted DuckDB extension repository:

```sql
install vizier from 'https://cogitatortech.github.io/vizier/extensions';
load vizier;
```

The hosted repository currently includes binaries for DuckDB `v1.2.0` to `v1.5.0`.

> [!IMPORTANT]
> Vizier is currently not available on DuckDB's community extensions repository and is distributed as an unsigned extension.
> So you need to start DuckDB with `-unsigned`, or enable unsigned extensions in your client before installing and loading Vizier.

#### Download from the Release Page

Open the [latest release](https://github.com/CogitatorTech/vizier/releases/latest) for release notes and platform-specific zip downloads:

- Linux AMD64: `vizier-linux-amd64.zip`
- Linux ARM64: `vizier-linux-arm64.zip`
- Linux AMD64 (musl): `vizier-linux-amd64-musl.zip`
- Linux ARM64 (musl): `vizier-linux-arm64-musl.zip`
- macOS ARM64: `vizier-macos-arm64.zip`
- Windows AMD64: `vizier-windows-amd64.zip`
- Windows ARM64: `vizier-windows-arm64.zip`
- FreeBSD AMD64: `vizier-freebsd-amd64.zip`

#### Build from Source

```bash
# Clone Vizier repository
git clone --depth=1 https://github.com/CogitatorTech/vizier.git
cd vizier

# Build the extension
make build-all

# Run all tests (optional; needs DuckDB in PATH)
make test

# Try out Vizier (needs DuckDB in PATH)
make duckdb
```

#### Simple Example

The example below assumes you install Vizier from the hosted extension repository.

```sql
install vizier from 'https://cogitatortech.github.io/vizier/extensions';
load vizier;

-- Capture queries from your workload
select * from vizier_capture('select * from events where account_id = 42 and ts >= date ''2026-01-01''');
select * from vizier_capture('select count(*) from orders group by customer_id');
select * from vizier_capture('select * from events where account_id = 42 and ts >= date ''2026-01-01''');

-- Persist captured queries to metadata tables
select * from vizier_flush();

-- View workload summary (ordered by frequency)
select * from vizier.workload_summary;
```

Example output:

```
┌──────────────┬────────────────────────────────────────────────────────────────────────┬───────┬─────────────────────┬────────┐
│     sig      │                                  sql                                   │ runs  │      last_seen      │ avg_ms │  
│   varchar    │                                varchar                                 │ int32 │       varchar       │ double │  
├──────────────┼────────────────────────────────────────────────────────────────────────┼───────┼─────────────────────┼────────┤  
│ 8c9d7175aab0 │ SELECT * FROM events WHERE account_id = 42 AND ts >= DATE '2026-01-01' │     2 │ 2026-03-22 16:44:06 │    0.0 │  
│ 2af899d9dba6 │ SELECT count(*) FROM orders GROUP BY customer_id                       │     1 │ 2026-03-22 16:44:06 │    0.0 │  
└──────────────┴────────────────────────────────────────────────────────────────────────┴───────┴─────────────────────┴────────┘  
```

---

### Documentation

Check out the project documentation [here](https://cogitatortech.github.io/vizier/).

---

### Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for details on how to make a contribution.

### License

This project is licensed under the MIT License (see [LICENSE](LICENSE)).
