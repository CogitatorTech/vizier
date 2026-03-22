#!/usr/bin/env bash
# Run all Vizier benchmarks with structured output.
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

filter="${1:-}"
total_pass=0
total_fail=0

for f in "$SCRIPT_DIR"/*.sql; do
    name="$(basename "$f" .sql)"
    if [ -n "$filter" ] && [[ "$name" != *"$filter"* ]]; then
        continue
    fi

    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "  $name"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo ""

    if "$DUCKDB" -unsigned -column -c ".read $f" 2>&1; then
        total_pass=$((total_pass + 1))
    else
        echo "  FAILED"
        total_fail=$((total_fail + 1))
    fi
    echo ""
done

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Summary: $total_pass passed, $total_fail failed"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
[ "$total_fail" -eq 0 ]
