#!/usr/bin/env python3
"""
Sample no-locale rows from parsed parquet data to verify grouping accuracy.

Shows the top URLs driving the no-locale count so you can confirm they are
all structurally expected (infrastructure hits, locale homepages caught by the
Home regex, bot probes, etc.) rather than misclassified real content.

Usage:
    python scripts/sample_no_locale.py
    python scripts/sample_no_locale.py --top 50
    python scripts/sample_no_locale.py --date 2026-03-01   # single date
"""

import argparse
import sys
from pathlib import Path

try:
    import duckdb
except ImportError:
    sys.exit("duckdb not installed – run: pip install duckdb")

ROOT = Path(__file__).resolve().parents[1]
DATA_PARSED = ROOT / "data" / "parsed"


def find_parquet_files(date_filter: str | None) -> list[Path]:
    if not DATA_PARSED.exists():
        return []
    if date_filter:
        # Accept either data/parsed/YYYY-MM-DD.parquet or data/parsed/YYYY-MM-DD/*.parquet
        direct = DATA_PARSED / f"{date_filter}.parquet"
        if direct.exists():
            return [direct]
        date_dir = DATA_PARSED / date_filter
        if date_dir.is_dir():
            return sorted(date_dir.glob("*.parquet"))
        return []
    return sorted(DATA_PARSED.rglob("*.parquet"))


def run(top: int, date_filter: str | None) -> None:
    files = find_parquet_files(date_filter)
    if not files:
        loc = f"for date {date_filter}" if date_filter else f"under {DATA_PARSED}"
        print(f"No parquet files found {loc}.")
        print("Run the ingest pipeline first to generate parsed data.")
        return

    # Build a DuckDB union over all matched files.
    quoted = ", ".join(f"'{f}'" for f in files)
    parquet_expr = f"read_parquet([{quoted}])"

    con = duckdb.connect()

    # ── 1. Overall no-locale summary ──────────────────────────────────────────
    summary = con.execute(f"""
        SELECT
            COUNT(*)                                        AS total_rows,
            SUM(CASE WHEN locale = 'no-locale' THEN 1 END) AS no_locale_rows,
            ROUND(
                100.0 * SUM(CASE WHEN locale = 'no-locale' THEN 1 END) / COUNT(*),
                1
            )                                               AS pct
        FROM {parquet_expr}
    """).fetchone()

    total, no_locale, pct = summary
    print(f"\n── No-locale summary {'(' + date_filter + ')' if date_filter else '(all dates)'} ──────────────────────────────")
    print(f"  Total rows   : {total:>12,}")
    print(f"  no-locale    : {no_locale:>12,}  ({pct}%)")

    # ── 2. Breakdown by url_group ─────────────────────────────────────────────
    print(f"\n── no-locale rows by url_group ──────────────────────────────────────")
    rows = con.execute(f"""
        SELECT url_group, COUNT(*) AS n
        FROM {parquet_expr}
        WHERE locale = 'no-locale'
        GROUP BY url_group
        ORDER BY n DESC
    """).fetchall()
    for group, n in rows:
        bar = "█" * min(40, max(1, int(40 * n / no_locale))) if no_locale else ""
        print(f"  {group:<25} {n:>10,}  {bar}")

    # ── 3. Top paths ──────────────────────────────────────────────────────────
    print(f"\n── Top {top} no-locale paths ──────────────────────────────────────────")
    rows = con.execute(f"""
        SELECT path, url_group, COUNT(*) AS n
        FROM {parquet_expr}
        WHERE locale = 'no-locale'
        GROUP BY path, url_group
        ORDER BY n DESC
        LIMIT {top}
    """).fetchall()

    if not rows:
        print("  (no rows)")
        return

    col_w = max(len(r[0]) for r in rows)
    col_w = max(col_w, 4)
    print(f"  {'path':<{col_w}}  {'group':<22}  {'count':>10}")
    print(f"  {'-'*col_w}  {'-'*22}  {'-'*10}")
    for path, group, n in rows:
        print(f"  {path:<{col_w}}  {group:<22}  {n:>10,}")

    # ── 4. Unexpected hits (not infrastructure) ───────────────────────────────
    infra_groups = {"Home", "Robots", "Sitemaps", "Nuxt Assets", "API", "Static Assets"}
    unexpected = [(p, g, n) for p, g, n in rows if g not in infra_groups]
    if unexpected:
        print(f"\n⚠  Unexpected no-locale paths (not infrastructure) in top {top}:")
        for path, group, n in unexpected:
            print(f"   {path:<{col_w}}  group={group!r}  n={n:,}")
        print("   These may indicate misclassified real content – investigate.")
    else:
        print(f"\n✓ All top-{top} no-locale paths are infrastructure (expected).")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--top", type=int, default=30, help="Number of top paths to show (default: 30)")
    parser.add_argument("--date", metavar="YYYY-MM-DD", help="Filter to a single date")
    args = parser.parse_args()
    run(args.top, args.date)


if __name__ == "__main__":
    main()
