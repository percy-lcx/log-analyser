#!/usr/bin/env python3
import argparse
from datetime import datetime, timedelta
from pathlib import Path
import re
import sys
import importlib.util


ROOT = Path(__file__).resolve().parents[1]
DATA_RAW = ROOT / "data" / "raw"
DATA_PARSED = ROOT / "data" / "parsed"
DATA_AGG = ROOT / "data" / "aggregates"

DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


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
                # should not happen in our layout, but safe fallback
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

    if not dates:
        print("No dated log files found. Filenames must include _access_YYYY-MM-DD_.")
        return

    print(f"Rebuilding {len(dates)} date(s).")

    for d in dates:
        day_files = ingest.gather_files_for_date(files, d)
        if not day_files:
            continue

        # Remove old outputs for the date (safe)
        parsed_dir = DATA_PARSED / f"date={d}"
        delete_dir_files(parsed_dir)
        parsed_dir.mkdir(parents=True, exist_ok=True)

        for agg_name in [
            "daily", "hourly", "bot_daily", "top_urls_daily", "top_404_daily", "top_5xx_daily",
            "wasted_crawl_daily", "top_resource_waste_daily"
        ]:
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
