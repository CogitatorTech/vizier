#!/usr/bin/env python3

from __future__ import annotations

import argparse
import shutil
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Stage a DuckDB extension repository under a static site directory."
    )
    parser.add_argument(
        "--artifacts-root",
        required=True,
        help="Directory containing per-platform artifacts named <platform>/vizier.duckdb_extension",
    )
    parser.add_argument(
        "--site-root",
        required=True,
        help="MkDocs output directory where the extension repository will be created",
    )
    parser.add_argument(
        "--extension-name",
        default="vizier",
        help="Extension name used for the repository filename",
    )
    parser.add_argument(
        "--duckdb-versions",
        required=True,
        help="Comma-separated list of DuckDB versions to mirror, e.g. v1.4.3,v1.5.0",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    artifacts_root = Path(args.artifacts_root)
    site_root = Path(args.site_root)
    versions = [version.strip() for version in args.duckdb_versions.split(",") if version.strip()]

    extension_filename = f"{args.extension_name}.duckdb_extension"
    repository_root = site_root / "extensions"
    repository_root.mkdir(parents=True, exist_ok=True)

    platform_dirs = sorted(path for path in artifacts_root.iterdir() if path.is_dir())
    if not platform_dirs:
        raise SystemExit(f"no platform artifacts found under {artifacts_root}")

    for platform_dir in platform_dirs:
        source = platform_dir / extension_filename
        if not source.is_file():
            raise SystemExit(f"missing extension artifact: {source}")

        for version in versions:
            destination = repository_root / version / platform_dir.name / extension_filename
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, destination)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
