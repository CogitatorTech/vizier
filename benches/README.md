## Benchmarks

This directory contains SQL-based benchmarks for measuring Vizier's performance.
Each benchmark is a standalone `.sql` file that loads the extension, generates data, and reports timing metrics.

### Running Benchmarks

```bash
make bench
```

Or directly:

```bash
./benches/run.sh
```

### Running a Single Benchmark

```bash
./benches/run.sh tpch           # Runs only tpch_workload.sql
./benches/run.sh capture        # Runs only capture_throughput.sql
./benches/run.sh advisor        # Runs only advisor_throughput.sql
```

### Benchmark Descriptions

| File                     | What it measures                                                                                         |
|--------------------------|----------------------------------------------------------------------------------------------------------|
| `tpch_workload.sql`      | End-to-end: TPC-H data (500K+ rows), 12 analytical queries, advisors, apply, before and after benchmarks |
| `advisor_throughput.sql` | Advisor speed: analyze time at 10 and 30 query workload sizes across 3 tables                            |
| `capture_throughput.sql` | Capture speed: 50 individual captures, 500 bulk captures, flush throughput                               |

### Metrics Reported

Each benchmark reports structured timing data:

- Operation timings via `epoch_ms()` deltas (like `capture 50 queries: 7 ms` and `analyze time: 20 ms`)
- Query benchmarks via `vizier_benchmark(sql, runs)` with `min_ms`, `avg_ms`, `p95_ms`, `max_ms` over N iterations
- Recommendation breakdowns by kind and count
- Before and after comparisons for applied recommendations (TPC-H benchmark)

### Adding a New Benchmark

1. Create a `.sql` file in this directory.
2. Start with `load 'zig-out/lib/vizier.duckdb_extension';`.
3. Use `select '>>> Description' as benchmark;` headers to label sections.
4. Measure timing with the `epoch_ms` pattern:
   ```sql
   create table _t1 as select epoch_ms(now()::timestamp) as t;
   -- ... Operation to measure ...
   select '  label: ' || (epoch_ms(now()::timestamp) - t)::varchar || ' ms' as result from _t1;
   ```
5. Use `vizier_benchmark(sql, runs)` for controlled query-level measurements.
6. The runner script (`run.sh`) picks up any `.sql` file in this directory automatically.
