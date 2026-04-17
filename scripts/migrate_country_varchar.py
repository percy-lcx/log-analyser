#!/usr/bin/env python3
"""Rewrite parsed parquet partitions where `country` is stored as JSON.

Older partitions had `country` inferred as JSON by DuckDB's NDJSON reader
when every row's value was null. Mixing those with newer VARCHAR partitions
under `read_parquet(..., union_by_name=true)` triggers a JSON-parse failure
on real country codes (e.g. "JP"). This script casts JSON → VARCHAR in
place. Safe to re-run; VARCHAR and missing-column partitions are skipped.
"""
import os
import sys
from pathlib import Path
from typing import Optional

import duckdb

ROOT = Path(__file__).resolve().parents[1]
PARSED_DIR = ROOT / "data" / "parsed"


def country_type(con: duckdb.DuckDBPyConnection, path: Path) -> Optional[str]:
    posix = path.as_posix().replace("'", "''")
    rows = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{posix}')").fetchall()
    for name, ctype, *_ in rows:
        if name == "country":
            return ctype
    return None


def rewrite(con: duckdb.DuckDBPyConnection, path: Path) -> None:
    tmp = path.with_suffix(path.suffix + ".migrate.tmp")
    src = path.as_posix().replace("'", "''")
    dst = tmp.as_posix().replace("'", "''")
    con.execute(
        f"COPY (SELECT * REPLACE (CAST(country AS VARCHAR) AS country) "
        f"FROM read_parquet('{src}')) TO '{dst}' (FORMAT PARQUET)"
    )
    os.replace(tmp, path)


def main() -> int:
    if not PARSED_DIR.exists():
        print(f"no parsed dir at {PARSED_DIR}", file=sys.stderr)
        return 1

    files = sorted(PARSED_DIR.glob("date=*/access.parquet"))
    con = duckdb.connect(database=":memory:")

    migrated = skipped_varchar = skipped_missing = errors = 0
    for fp in files:
        try:
            ctype = country_type(con, fp)
        except Exception as e:
            print(f"[ERROR] describe {fp}: {e}", file=sys.stderr)
            errors += 1
            continue

        if ctype is None:
            skipped_missing += 1
            continue
        if ctype.upper() == "VARCHAR":
            skipped_varchar += 1
            continue
        if ctype.upper() != "JSON":
            print(f"[WARN] {fp.parent.name}: unexpected country type {ctype}, skipping")
            continue

        try:
            rewrite(con, fp)
            migrated += 1
            print(f"[OK] migrated {fp.parent.name}")
        except Exception as e:
            print(f"[ERROR] rewrite {fp}: {e}", file=sys.stderr)
            errors += 1

    print(
        f"\nsummary: migrated={migrated} "
        f"skipped_varchar={skipped_varchar} "
        f"skipped_missing_col={skipped_missing} "
        f"errors={errors}"
    )
    return 0 if errors == 0 else 2


if __name__ == "__main__":
    sys.exit(main())
