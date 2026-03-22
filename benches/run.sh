#!/usr/bin/env bash
# Run all Vizier benchmarks.
# Usage: ./benches/run.sh [benchmark_name]
# Examples:
#   ./benches/run.sh              # Run all benchmarks
#   ./benches/run.sh tpch         # Run only tpch_workload.sql

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DUCKDB="${DUCKDB:-duckdb}"

if ! command -v "$DUCKDB" &>/dev/null; then
    echo "error: duckdb not found in PATH" >&2
    exit 1
fi

run_bench() {
    local file="$1"
    local name
    name="$(basename "$file" .sql)"
    echo "━━━ $name ━━━"
    local start
    start=$(date +%s%N)
    "$DUCKDB" -unsigned <"$file"
    local end
    end=$(date +%s%N)
    local elapsed_ms=$(( (end - start) / 1000000 ))
    echo "━━━ $name completed in ${elapsed_ms}ms ━━━"
    echo ""
}

filter="${1:-}"

for f in "$SCRIPT_DIR"/*.sql; do
    name="$(basename "$f" .sql)"
    if [ -n "$filter" ] && [[ "$name" != *"$filter"* ]]; then
        continue
    fi
    run_bench "$f"
done
