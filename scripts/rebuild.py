#!/usr/bin/env python3
import argparse
from datetime import datetime, timedelta
from pathlib import Path
import re
import sys
import os
import importlib.util

ROOT = Path(__file__).resolve().parents[1]
DATA_RAW = ROOT / "data" / "raw"
DATA_PARSED = ROOT / "data" / "parsed"
DATA_AGG = ROOT / "data" / "aggregates"

DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

print(f"[rebuild] pid={os.getpid()} argv={sys.argv}")


def load_ingest_module():
    ingest_path = ROOT / "scripts" / "ingest.py"
    spec = importlib.util.spec_from_file_location("ingest", ingest_path.as_posix())
    mod = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(mod)
    return mod


def date_range(start: str, end: str):
    ds = datetime.strptime(start, "%Y-%m-%d").date()
    de = datetime.strptime(end, "%Y-%m-%d").date()
    cur = ds
    while cur <= de:
        yield cur.strftime("%Y-%m-%d")
        cur = cur + timedelta(days=1)


def all_dates_from_raw_files(files, extract_date_from_filename):
    dates = set()
    for fp in files:
        d = extract_date_from_filename(fp.name)
        if d:
            dates.add(d)
    return sorted(dates)


def delete_dir_files(path: Path) -> None:
    if path.exists():
        for child in path.glob("*"):
            try:
                child.unlink()
            except IsADirectoryError:
                pass


def main():
    p = argparse.ArgumentParser(
        description="Rebuild parsed + aggregates. If no --from/--to, rebuild ALL dates found in data/raw/."
    )
    p.add_argument("--from", dest="date_from", required=False, help="YYYY-MM-DD")
    p.add_argument("--to", dest="date_to", required=False, help="YYYY-MM-DD")
    args = p.parse_args()

    ingest = load_ingest_module()

    ingest.ensure_dirs()
    ingest.init_manifest()

    bot_rules = ingest.load_bot_rules()
    url_cfg = ingest.load_url_grouping()
    files = ingest.list_raw_files()

    if not files:
        print("No files found in data/raw/")
        return

    print(f"[debug] raw files count: {len(files)} unique: {len(set([f.as_posix() for f in files]))}")

    # Decide which dates to rebuild
    if args.date_from or args.date_to:
        if not (args.date_from and args.date_to):
            print("If you use --from or --to, you must provide BOTH.", file=sys.stderr)
            sys.exit(1)
        if not DATE_RE.match(args.date_from) or not DATE_RE.match(args.date_to):
            print("Dates must be YYYY-MM-DD", file=sys.stderr)
            sys.exit(1)
        dates = list(date_range(args.date_from, args.date_to))
    else:
        dates = all_dates_from_raw_files(files, ingest.extract_date_from_filename)

    # Hard de-dupe (prevents repeated rebuild of same date even if something upstream duplicates)
    dates = sorted(set(dates))

    print(f"[debug] dates count: {len(dates)} unique: {len(set(dates))}")
    if len(dates) <= 200:
        print("[debug] dates:", dates)

    if not dates:
        print("No dated log files found. Filenames must include _access_YYYY-MM-DD_.")
        return

    print(f"Rebuilding {len(dates)} date(s).")

    agg_tables = [
        "daily",
        "hourly",
        "bot_daily",
        "locale_daily",
        "group_daily",
        "locale_group_daily",
        "top_urls_daily",
        "top_404_daily",
        "top_5xx_daily",
        "wasted_crawl_daily",
        "top_resource_waste_daily",
        "bot_urls_daily",

        # legacy
        "utm_chatgpt_daily",
        "utm_chatgpt_urls_daily",

        # NEW generic UTM
        "utm_sources_daily",
        "utm_source_urls_daily",
    ]

    for d in dates:
        day_files = ingest.gather_files_for_date(files, d)
        if not day_files:
            continue

        # Remove old parsed output for the date (safe)
        parsed_dir = DATA_PARSED / f"date={d}"
        delete_dir_files(parsed_dir)
        parsed_dir.mkdir(parents=True, exist_ok=True)

        # Remove old aggregates for the date (safe)
        for agg_name in agg_tables:
            agg_dir = DATA_AGG / agg_name / f"date={d}"
            delete_dir_files(agg_dir)
            agg_dir.mkdir(parents=True, exist_ok=True)

        print(f"\n[DATE {d}] Rebuilding parsed partition from {len(day_files)} file(s)")
        n_rows = ingest.build_parsed_for_date(d, day_files, bot_rules, url_cfg)
        print(f"[DATE {d}] Parsed rows: {n_rows}")

        print(f"[DATE {d}] Building aggregates")
        ingest.build_aggregates_for_date(d)
        print(f"[DATE {d}] Aggregates done")

    print("\nRebuild complete.")


if __name__ == "__main__":
    main()