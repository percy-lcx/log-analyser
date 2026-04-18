#!/usr/bin/env python3
"""Backfill missing gsc_daily aggregates.

Runs _build_gsc_daily_aggregate for every date that has both parsed log data
and raw GSC data but no gsc_daily/date=*/part.parquet yet.

Use this after connecting GSC late — full rebuild.py is unnecessary because
every other aggregate table is already populated.
"""
from __future__ import annotations

import importlib.util
import sys
import time
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
DATA_GSC = ROOT / "data" / "gsc"
DATA_PARSED = ROOT / "data" / "parsed"
DATA_AGG = ROOT / "data" / "aggregates"
GSC_DAILY = DATA_AGG / "gsc_daily"


def load_ingest_module():
    spec = importlib.util.spec_from_file_location(
        "ingest", (ROOT / "scripts" / "ingest.py").as_posix()
    )
    mod = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(mod)
    return mod


def main() -> int:
    ingest = load_ingest_module()

    if not DATA_GSC.exists():
        print("No data/gsc directory — nothing to backfill.")
        return 0

    candidates = sorted(p.parent.name.split("=", 1)[1]
                        for p in DATA_GSC.glob("date=*/performance.parquet"))

    missing: list[str] = []
    for d in candidates:
        agg = GSC_DAILY / f"date={d}" / "part.parquet"
        if agg.exists():
            continue
        parsed = DATA_PARSED / f"date={d}" / "access.parquet"
        if not parsed.exists():
            print(f"[skip] {d}: no parsed log data")
            continue
        missing.append(d)

    if not missing:
        print("All gsc_daily aggregates already built.")
        return 0

    print(f"Backfilling gsc_daily for {len(missing)} date(s): "
          f"{missing[0]} -> {missing[-1]}")

    t0 = time.monotonic()
    for i, d in enumerate(missing, 1):
        parsed = DATA_PARSED / f"date={d}" / "access.parquet"
        conn = duckdb.connect(database=":memory:")
        conn.execute("PRAGMA threads=4;")
        conn.execute(
            f"CREATE OR REPLACE VIEW parsed AS "
            f"SELECT * FROM read_parquet('{parsed.as_posix()}');"
        )

        def out(name: str) -> Path:
            return DATA_AGG / name / f"date={d}" / "part.parquet"

        try:
            ingest._build_gsc_daily_aggregate(d, conn, out)
        finally:
            conn.close()

        print(f"[{i}/{len(missing)}] {d} done", flush=True)

    print(f"\nBackfill complete in {time.monotonic() - t0:.1f}s.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
