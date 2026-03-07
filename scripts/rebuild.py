#!/usr/bin/env python3
import argparse
from datetime import datetime, timedelta
from pathlib import Path
import re
import sys
import os
import importlib.util
import multiprocessing
import time

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


AGG_TABLES = [
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


def rebuild_date(args_tuple):
    """Worker function for parallel date rebuild. Runs in a child process."""
    d, day_file_strs, bot_rules, referer_rules, url_cfg = args_tuple
    ingest = load_ingest_module()

    day_files = [Path(f) for f in day_file_strs]

    parsed_dir = DATA_PARSED / f"date={d}"
    delete_dir_files(parsed_dir)
    parsed_dir.mkdir(parents=True, exist_ok=True)

    for agg_name in AGG_TABLES:
        agg_dir = DATA_AGG / agg_name / f"date={d}"
        delete_dir_files(agg_dir)
        agg_dir.mkdir(parents=True, exist_ok=True)

    t0 = time.monotonic()
    print(f"\n[DATE {d}] Rebuilding parsed partition from {len(day_files)} file(s)", flush=True)
    n_rows = ingest.build_parsed_for_date(d, day_files, bot_rules, url_cfg, referer_rules)
    t_parsed = time.monotonic()
    print(f"[DATE {d}] Parsed rows: {n_rows} ({t_parsed - t0:.1f}s)", flush=True)

    ingest.build_aggregates_for_date(d)
    t_agg = time.monotonic()
    print(f"[DATE {d}] Aggregates done ({t_agg - t_parsed:.1f}s, total {t_agg - t0:.1f}s)", flush=True)

    return d, n_rows


def main():
    p = argparse.ArgumentParser(
        description="Rebuild parsed + aggregates. If no --from/--to, rebuild ALL dates found in data/raw/."
    )
    p.add_argument("--from", dest="date_from", required=False, help="YYYY-MM-DD")
    p.add_argument("--to", dest="date_to", required=False, help="YYYY-MM-DD")
    p.add_argument("--workers", dest="workers", type=int, default=None,
                   help="Parallel worker processes (default: CPU count)")
    args = p.parse_args()

    ingest = load_ingest_module()

    ingest.ensure_dirs()
    ingest.init_manifest()

    bot_rules = ingest.load_bot_rules()
    referer_rules = ingest.load_referer_rules()
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

    workers = args.workers or multiprocessing.cpu_count()
    print(f"Rebuilding {len(dates)} date(s) with {workers} worker(s).")

    work_items = []
    for d in dates:
        day_files = ingest.gather_files_for_date(files, d)
        if not day_files:
            continue
        # Pass file paths as strings — Path objects don't pickle on all platforms
        work_items.append((d, [f.as_posix() for f in day_files], bot_rules, referer_rules, url_cfg))

    if not work_items:
        print("No work to do.")
        return

    t_start = time.monotonic()

    if workers == 1 or len(work_items) == 1:
        for item in work_items:
            rebuild_date(item)
    else:
        with multiprocessing.Pool(processes=workers) as pool:
            pool.map(rebuild_date, work_items)

    elapsed = time.monotonic() - t_start
    print(f"\nRebuild complete. {len(work_items)} date(s) in {elapsed:.1f}s.")


if __name__ == "__main__":
    main()