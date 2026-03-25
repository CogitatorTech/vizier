#!/usr/bin/env python3
"""Import dbt run results into Vizier.

Reads compiled SQL from manifest.json and execution timing from
run_results.json, then inserts them into Vizier's workload tables
via vizier_capture_bulk().

Usage:
    python scripts/import_dbt.py --db my_database.duckdb
    python scripts/import_dbt.py --db my_database.duckdb --target path/to/target
    python scripts/import_dbt.py --db my_database.duckdb --min-time 0.5
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Import dbt run results into Vizier workload tables."
    )
    parser.add_argument(
        "--db",
        required=True,
        help="Path to the DuckDB database file.",
    )
    parser.add_argument(
        "--target",
        default="target",
        help="Path to the dbt target directory (default: target).",
    )
    parser.add_argument(
        "--min-time",
        type=float,
        default=0.0,
        help="Only import models with execution_time above this threshold in seconds (default: 0).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the SQL that would be captured without executing.",
    )
    return parser.parse_args()


def load_json(path: Path) -> dict:
    with open(path) as f:
        return json.load(f)


def extract_models(
    manifest: dict, run_results: dict, min_time: float
) -> list[dict]:
    """Join manifest nodes with run results to get compiled SQL and timing."""
    nodes = manifest.get("nodes", {})
    models = []

    for result in run_results.get("results", []):
        uid = result.get("unique_id", "")
        status = result.get("status", "")
        exec_time = result.get("execution_time", 0.0)

        # Only import successful model runs
        if status != "success":
            continue
        if not uid.startswith("model."):
            continue
        if exec_time < min_time:
            continue

        node = nodes.get(uid)
        if node is None:
            continue

        # compiled_code (dbt >= 1.3) or compiled_sql (older)
        sql = node.get("compiled_code") or node.get("compiled_sql", "")
        if not sql.strip():
            continue

        models.append(
            {
                "unique_id": uid,
                "sql": sql.strip(),
                "execution_time_s": exec_time,
                "model_name": node.get("name", uid),
            }
        )

    return models


def build_import_sql(models: list[dict], db_path: str) -> str:
    """Build SQL to load dbt models into Vizier via a temp table and capture_bulk."""
    lines = []

    # Create a temp table with the captured queries
    lines.append(
        "create temporary table _dbt_import (model_name varchar, sql_text varchar, exec_time_s double);"
    )

    for m in models:
        escaped = m["sql"].replace("'", "''")
        name = m["model_name"].replace("'", "''")
        lines.append(
            f"insert into _dbt_import values ('{name}', '{escaped}', {m['execution_time_s']:.3f});"
        )

    # Use vizier_capture_bulk to import from the temp table
    lines.append(
        "select * from vizier_capture_bulk('_dbt_import', 'sql_text');"
    )
    lines.append("select * from vizier_flush();")

    # Clean up
    lines.append("drop table _dbt_import;")

    return "\n".join(lines)


def main() -> int:
    args = parse_args()

    target = Path(args.target)
    manifest_path = target / "manifest.json"
    results_path = target / "run_results.json"

    if not manifest_path.exists():
        print(f"error: {manifest_path} not found", file=sys.stderr)
        return 1
    if not results_path.exists():
        print(f"error: {results_path} not found", file=sys.stderr)
        return 1

    manifest = load_json(manifest_path)
    run_results = load_json(results_path)
    models = extract_models(manifest, run_results, args.min_time)

    if not models:
        print("no matching models found in dbt run results.")
        return 0

    print(f"found {len(models)} model(s) to import:")
    for m in models:
        print(f"  {m['model_name']} ({m['execution_time_s']:.2f}s)")

    import_sql = build_import_sql(models, args.db)

    if args.dry_run:
        print("\n--- SQL that would be executed ---")
        print(import_sql)
        return 0

    # Execute via duckdb CLI
    import subprocess

    result = subprocess.run(
        ["duckdb", "-unsigned", args.db, "-c", f"load vizier;\n{import_sql}"],
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        print(f"error: duckdb failed:\n{result.stderr}", file=sys.stderr)
        return 1

    print(f"imported {len(models)} dbt model(s) into vizier.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
