#!/usr/bin/env python3
"""
Patch existing top_urls_daily parquet partitions in-place so that every
path is reclassified with the current url_groups.yml rules.

Paths that were previously mis-classified (e.g. bare locale homepages
falling into "Other Content") are corrected.  If reclassifying a row
causes it to collide with another row that already has the correct group,
the two rows are merged by summing all numeric columns.

Usage:
    # patch every date found under the directory
    python scripts/patch_url_groups.py data/aggregates/top_urls_daily

    # patch a specific date range only
    python scripts/patch_url_groups.py data/aggregates/top_urls_daily \\
        --from 2026-01-01 --to 2026-03-07

    # preview changes without writing
    python scripts/patch_url_groups.py data/aggregates/top_urls_daily --dry-run
"""

import argparse
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import pyarrow as pa
import pyarrow.parquet as pq

from ingest import apply_url_grouping, load_url_grouping

# All numeric columns present in top_urls_daily that must be summed when rows merge.
_SUM_COLS = [
    "hits_total",
    "hits_bot",
    "hits_human",
    "s3xx",
    "s4xx",
    "s5xx",
    "parameterized_hits",
    "bytes_sent",
]

_DATE_RE = re.compile(r"date=(\d{4}-\d{2}-\d{2})")


def _reclassify(df, url_cfg):
    """Return a copy of *df* with url_group corrected, merging any newly-duplicate rows."""
    import pandas as pd

    df = df.copy()
    df["url_group"] = df["path"].apply(lambda p: apply_url_grouping(p, url_cfg)[0])

    # Merge rows that now share the same key so we don't create duplicates.
    key_cols = [c for c in ("date", "path", "url_group") if c in df.columns]
    sum_cols = [c for c in _SUM_COLS if c in df.columns]
    other_cols = [c for c in df.columns if c not in key_cols and c not in sum_cols]

    if not sum_cols:
        return df

    agg: dict = {c: "sum" for c in sum_cols}
    # For any unexpected extra columns, keep the first value (best effort).
    agg.update({c: "first" for c in other_cols})

    df = df.groupby(key_cols, as_index=False, sort=False).agg(agg)

    # Restore original column order.
    original_order = [c for c in sum_cols + other_cols if c in df.columns]
    df = df[key_cols + original_order]

    return df


def patch_file(parquet_path: Path, url_cfg, *, dry_run: bool = False) -> tuple[int, int]:
    """Reclassify url_group for one parquet partition.

    Returns *(rows_changed, rows_merged)* — rows_merged counts rows that were
    collapsed together after reclassification.
    """
    table = pq.read_table(parquet_path)
    df = table.to_pandas()

    if "url_group" not in df.columns or "path" not in df.columns:
        return 0, 0

    original_groups = df["url_group"].copy()
    new_df = _reclassify(df, url_cfg)

    # How many rows had their group changed?
    new_groups = new_df.set_index(list({c for c in ("date", "path") if c in new_df.columns}))["url_group"]
    old_groups = df.set_index(list({c for c in ("date", "path") if c in df.columns}))["url_group"]
    rows_changed = int((original_groups != df["path"].apply(lambda p: apply_url_grouping(p, url_cfg)[0])).sum())
    rows_merged = len(df) - len(new_df)

    if rows_changed == 0:
        return 0, 0

    if not dry_run:
        # Rebuild an Arrow table that matches the original schema as closely as possible.
        schema = table.schema
        new_table = pa.Table.from_pandas(new_df, preserve_index=False)

        # Cast each column back to the original Arrow type where possible.
        casted_arrays = []
        casted_names = []
        for field in schema:
            if field.name in new_table.schema.names:
                col = new_table.column(field.name)
                try:
                    col = col.cast(field.type)
                except (pa.ArrowInvalid, pa.ArrowNotImplementedError):
                    pass  # keep as-is if cast fails
                casted_arrays.append(col)
                casted_names.append(field.name)

        out_table = pa.table(dict(zip(casted_names, casted_arrays)))
        pq.write_table(out_table, parquet_path)

    return rows_changed, rows_merged


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Reclassify url_group in existing top_urls_daily parquet files."
    )
    ap.add_argument("agg_dir", help="Path to the top_urls_daily aggregates directory")
    ap.add_argument("--from", dest="date_from", metavar="YYYY-MM-DD",
                    help="Only patch dates >= this value")
    ap.add_argument("--to", dest="date_to", metavar="YYYY-MM-DD",
                    help="Only patch dates <= this value")
    ap.add_argument("--dry-run", action="store_true",
                    help="Report changes without writing any files")
    args = ap.parse_args()

    agg_dir = Path(args.agg_dir)
    if not agg_dir.exists():
        sys.exit(f"Directory not found: {agg_dir}")

    url_cfg = load_url_grouping()

    parquet_files = sorted(agg_dir.glob("date=*/part.parquet"))
    if not parquet_files:
        sys.exit(f"No date=*/part.parquet files found under {agg_dir}")

    # Apply optional date filter.
    if args.date_from or args.date_to:
        def _in_range(f: Path) -> bool:
            m = _DATE_RE.search(f.as_posix())
            if not m:
                return True
            d = m.group(1)
            if args.date_from and d < args.date_from:
                return False
            if args.date_to and d > args.date_to:
                return False
            return True
        parquet_files = [f for f in parquet_files if _in_range(f)]

    prefix = "[DRY RUN] " if args.dry_run else ""
    print(f"{prefix}Scanning {len(parquet_files)} partition(s) in {agg_dir} …")

    total_changed = total_merged = 0
    for f in parquet_files:
        changed, merged = patch_file(f, url_cfg, dry_run=args.dry_run)
        total_changed += changed
        total_merged += merged
        if changed:
            parts = [f"  {prefix}{f.parent.name}: {changed} row(s) reclassified"]
            if merged:
                parts.append(f", {merged} merged")
            print("".join(parts))

    if total_changed == 0:
        print("No rows needed reclassification.")
    else:
        verb = "Would update" if args.dry_run else "Updated"
        merge_note = f", {total_merged} merged into existing rows" if total_merged else ""
        print(f"\n{verb} {total_changed} row(s) across {len(parquet_files)} file(s){merge_note}.")
        if args.dry_run:
            print("Re-run without --dry-run to apply changes.")


if __name__ == "__main__":
    main()
