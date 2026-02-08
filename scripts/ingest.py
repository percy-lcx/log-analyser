#!/usr/bin/env python3
import os
import re
import sys
import json
import yaml
import sqlite3
import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import duckdb


ROOT = Path(__file__).resolve().parents[1]
DATA_RAW = ROOT / "data" / "raw"
DATA_PARSED = ROOT / "data" / "parsed"
DATA_AGG = ROOT / "data" / "aggregates"
DETECTORS_DIR = ROOT / "detectors"
STATE_DIR = ROOT / "state"
MANIFEST_DB = STATE_DIR / "manifest.sqlite"


# Nginx combined-ish:
# $remote_addr - - [$time_local] "$request" $status $body_bytes_sent "$http_referer" "$http_user_agent"
NGINX_RE = re.compile(
    r'^(?P<ip>\S+)\s+\S+\s+\S+\s+\[(?P<time>[^\]]+)\]\s+'
    r'"(?P<request>[^"]*)"\s+'
    r'(?P<status>\d{3})\s+'
    r'(?P<bytes>\d+)\s+'
    r'"(?P<referer>[^"]*)"\s+'
    r'"(?P<ua>[^"]*)"\s*$'
)

# Filename pattern example:
# site.com_access_YYYY-MM-DD_021001(.log)
FNAME_DATE_RE = re.compile(r"_access_(\d{4}-\d{2}-\d{2})_")


@dataclass
class BotRule:
    family: str
    pattern: re.Pattern


@dataclass
class UrlRule:
    group: str
    match: str
    value: object  # str or list[str] depending on match type
    compiled: Optional[re.Pattern] = None


@dataclass
class UrlGroupingConfig:
    locales: set
    rules: List[UrlRule]
    section_map: Dict[str, str]
    fallback_group: str


def ensure_dirs() -> None:
    for p in [DATA_RAW, DATA_PARSED, DATA_AGG, DETECTORS_DIR, STATE_DIR]:
        p.mkdir(parents=True, exist_ok=True)


def load_yaml(path: Path) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_bot_rules() -> List[BotRule]:
    cfg = load_yaml(DETECTORS_DIR / "bots.yml")
    rules = []
    for r in cfg.get("rules", []):
        family = r["family"]
        pat = re.compile(r["pattern"], re.IGNORECASE)
        rules.append(BotRule(family=family, pattern=pat))
    return rules


def load_url_grouping() -> UrlGroupingConfig:
    cfg = load_yaml(DETECTORS_DIR / "url_groups.yml")
    locales = set(cfg.get("locales", []))
    rules: List[UrlRule] = []
    for r in cfg.get("rules", []):
        ur = UrlRule(group=r["group"], match=r["match"], value=r["value"])
        if ur.match == "regex":
            ur.compiled = re.compile(str(ur.value), re.IGNORECASE)
        rules.append(ur)
    section_map = {k.lower(): v for k, v in (cfg.get("section_map") or {}).items()}
    fallback = cfg.get("fallback_group", "Other Content")
    return UrlGroupingConfig(locales=locales, rules=rules, section_map=section_map, fallback_group=fallback)


def init_manifest() -> None:
    conn = sqlite3.connect(MANIFEST_DB)
    cur = conn.cursor()
    cur.execute("""
      CREATE TABLE IF NOT EXISTS ingested_files (
        path TEXT PRIMARY KEY,
        size_bytes INTEGER NOT NULL,
        mtime_epoch INTEGER NOT NULL,
        log_date TEXT NOT NULL,
        ingested_at_epoch INTEGER NOT NULL
      )
    """)
    conn.commit()
    conn.close()


def file_meta(path: Path) -> Tuple[int, int]:
    st = path.stat()
    return int(st.st_size), int(st.st_mtime)


def extract_date_from_filename(name: str) -> Optional[str]:
    m = FNAME_DATE_RE.search(name)
    return m.group(1) if m else None


def list_raw_files() -> List[Path]:
    return sorted([p for p in DATA_RAW.iterdir() if p.is_file()])


def manifest_get(path: Path) -> Optional[Tuple[int, int, str]]:
    conn = sqlite3.connect(MANIFEST_DB)
    cur = conn.cursor()
    cur.execute("SELECT size_bytes, mtime_epoch, log_date FROM ingested_files WHERE path = ?", (str(path),))
    row = cur.fetchone()
    conn.close()
    return row if row else None


def manifest_upsert(path: Path, size_bytes: int, mtime_epoch: int, log_date: str) -> None:
    conn = sqlite3.connect(MANIFEST_DB)
    cur = conn.cursor()
    cur.execute("""
      INSERT INTO ingested_files(path, size_bytes, mtime_epoch, log_date, ingested_at_epoch)
      VALUES(?,?,?,?,?)
      ON CONFLICT(path) DO UPDATE SET
        size_bytes=excluded.size_bytes,
        mtime_epoch=excluded.mtime_epoch,
        log_date=excluded.log_date,
        ingested_at_epoch=excluded.ingested_at_epoch
    """, (str(path), size_bytes, mtime_epoch, log_date, int(datetime.now(tz=timezone.utc).timestamp())))
    conn.commit()
    conn.close()


def classify_bot(ua: str, bot_rules: List[BotRule]) -> Tuple[bool, str]:
    ua_l = (ua or "").lower()
    for r in bot_rules:
        if r.pattern.search(ua_l):
            family = r.family
            return (family != "Browser/Other"), family
    return False, "Browser/Other"


def ext_of_path(path: str) -> Optional[str]:
    # extract extension from last path segment only
    seg = path.rsplit("/", 1)[-1]
    if "." not in seg:
        return None
    ext = seg.rsplit(".", 1)[-1].lower()
    return ext if ext else None


def apply_url_grouping(path: str, cfg: UrlGroupingConfig) -> Tuple[str, Optional[str], Optional[str]]:
    """
    Returns: (url_group, locale, section)
    """
    # 1) high priority rules
    p = path or "/"
    for r in cfg.rules:
        if r.match == "exact":
            if p == r.value:
                return r.group, None, None
        elif r.match == "prefix":
            if p.startswith(str(r.value)):
                return r.group, None, None
        elif r.match == "regex":
            if r.compiled and r.compiled.search(p):
                return r.group, None, None
        elif r.match == "ext":
            ex = ext_of_path(p)
            if ex and ex in set([str(x).lower() for x in r.value]):
                return r.group, None, None

    # 2) segment-based
    segs = [s for s in p.split("/") if s]
    if not segs:
        return "Home", None, None

    first = segs[0]
    locale = None
    section = None

    if first in cfg.locales:
        locale = first
        section_index = 1
        section = segs[section_index] if len(segs) > section_index else None
    else:
        section_index = 0
        section = first

    if section:
        subsection = segs[section_index + 1] if len(segs) > section_index + 1 else None
        if subsection:
            composite = f"{section}/{subsection}".lower()
            mapped = cfg.section_map.get(composite)
            if mapped:
                return mapped, locale, section
        mapped = cfg.section_map.get(section.lower())
        if mapped:
            return mapped, locale, section
    return cfg.fallback_group, locale, section


def parse_request(req: str) -> Tuple[Optional[str], str, Optional[str], bool]:
    """
    request line: "GET /path?x=y HTTP/1.1"
    returns (method, path_no_query, http_version, has_query)
    """
    if not req:
        return None, "/", None, False
    parts = req.split()
    if len(parts) < 2:
        return None, "/", None, False
    method = parts[0]
    target = parts[1]
    http_ver = parts[2] if len(parts) >= 3 else None

    if "?" in target:
        path = target.split("?", 1)[0]
        return method, path or "/", http_ver, True
    return method, target or "/", http_ver, False


def parse_time_local(time_s: str) -> Tuple[datetime, datetime]:
    # Example: 29/Jan/2026:02:07:29 +0800
    dt_local = datetime.strptime(time_s, "%d/%b/%Y:%H:%M:%S %z")
    dt_utc = dt_local.astimezone(timezone.utc)
    return dt_local, dt_utc


def status_class(status: int) -> int:
    if 200 <= status <= 299:
        return 2
    if 300 <= status <= 399:
        return 3
    if 400 <= status <= 499:
        return 4
    if 500 <= status <= 599:
        return 5
    return 0


def build_parsed_for_date(log_date: str, files: List[Path], bot_rules: List[BotRule], url_cfg: UrlGroupingConfig) -> int:
    rows = []
    bad = 0

    for fp in files:
        with open(fp, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                line = line.rstrip("\n")
                m = NGINX_RE.match(line)
                if not m:
                    bad += 1
                    continue

                ip = m.group("ip")
                time_s = m.group("time")
                req = m.group("request")
                status = int(m.group("status"))
                bytes_sent = int(m.group("bytes"))
                referer = m.group("referer")
                ua = m.group("ua")

                try:
                    ts_local, ts_utc = parse_time_local(time_s)
                except Exception:
                    bad += 1
                    continue

                method, path, http_ver, has_query = parse_request(req)

                if referer == "-" or referer == "":
                    referer = None

                is_bot, bot_family = classify_bot(ua, bot_rules)
                url_group, locale, section = apply_url_grouping(path, url_cfg)
                is_resource = url_group in ("Nuxt Assets", "Static Assets")

                rows.append({
                    "date": log_date,
                    "ts_local": ts_local,
                    "ts_utc": ts_utc.replace(tzinfo=None),  # store UTC as naive timestamp
                    "edge_ip": ip,
                    "method": method,
                    "path": path,
                    "http_version": http_ver,
                    "status": status,
                    "status_class": status_class(status),
                    "bytes_sent": bytes_sent,
                    "referer": referer,
                    "user_agent": ua,
                    "has_query": bool(has_query),
                    "is_parameterized": bool(has_query),
                    "locale": locale,
                    "section": section,
                    "url_group": url_group,
                    "is_resource": bool(is_resource),
                    "is_bot": bool(is_bot),
                    "bot_family": bot_family if is_bot else None,
                })

    if not rows:
        # still create an empty parquet so later steps can work predictably
        df = pd.DataFrame(columns=[
            "date","ts_local","ts_utc","edge_ip","method","path","http_version",
            "status","status_class","bytes_sent","referer","user_agent",
            "has_query","is_parameterized","locale","section","url_group","is_resource",
            "is_bot","bot_family"
        ])
    else:
        df = pd.DataFrame(rows)

    out_dir = DATA_PARSED / f"date={log_date}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "access.parquet"
    df.to_parquet(out_path, index=False)

    if bad > 0:
        print(f"[WARN] {log_date}: {bad} lines failed to parse", file=sys.stderr)

    return len(df)


def agg_write_one(conn: duckdb.DuckDBPyConnection, sql: str, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    # DuckDB COPY writes Parquet
    conn.execute(f"COPY ({sql}) TO '{out_path.as_posix()}' (FORMAT PARQUET);")


def build_aggregates_for_date(log_date: str) -> None:
    parsed_path = (DATA_PARSED / f"date={log_date}" / "access.parquet")
    if not parsed_path.exists():
        return

    conn = duckdb.connect(database=":memory:")
    conn.execute("PRAGMA threads=4;")
    conn.execute(f"CREATE OR REPLACE VIEW parsed AS SELECT * FROM read_parquet('{parsed_path.as_posix()}');")

    # daily (one row)
    daily_sql = """
    SELECT
      date,
      COUNT(*) AS hits,
      SUM(CASE WHEN is_bot THEN 1 ELSE 0 END) AS hits_bot,
      SUM(CASE WHEN NOT is_bot THEN 1 ELSE 0 END) AS hits_human,

      SUM(CASE WHEN status_class = 2 THEN 1 ELSE 0 END) AS s2xx,
      SUM(CASE WHEN status_class = 3 THEN 1 ELSE 0 END) AS s3xx,
      SUM(CASE WHEN status_class = 4 THEN 1 ELSE 0 END) AS s4xx,
      SUM(CASE WHEN status_class = 5 THEN 1 ELSE 0 END) AS s5xx,

      SUM(bytes_sent) AS bytes_sent,

      SUM(CASE WHEN is_resource THEN 1 ELSE 0 END) AS resource_hits,
      SUM(CASE WHEN is_resource THEN bytes_sent ELSE 0 END) AS resource_bytes_sent,
      SUM(CASE WHEN is_resource AND status_class = 4 THEN 1 ELSE 0 END) AS resource_4xx,
      SUM(CASE WHEN is_resource AND status_class = 5 THEN 1 ELSE 0 END) AS resource_5xx,

      SUM(CASE WHEN is_resource AND is_bot THEN 1 ELSE 0 END) AS resource_hits_bot,
      SUM(CASE WHEN is_resource AND is_bot AND status_class = 4 THEN 1 ELSE 0 END) AS resource_4xx_bot,
      SUM(CASE WHEN is_resource AND is_bot AND status_class = 5 THEN 1 ELSE 0 END) AS resource_5xx_bot
    FROM parsed
    GROUP BY date
    """

    # hourly (bucket by log time, not machine time)
    hourly_sql = """
    SELECT
      date,
      date_trunc('hour', ts_local) AS hour_local,
      COUNT(*) AS hits,
      SUM(CASE WHEN is_bot THEN 1 ELSE 0 END) AS hits_bot,
      SUM(CASE WHEN NOT is_bot THEN 1 ELSE 0 END) AS hits_human,

      SUM(CASE WHEN status_class = 2 THEN 1 ELSE 0 END) AS s2xx,
      SUM(CASE WHEN status_class = 3 THEN 1 ELSE 0 END) AS s3xx,
      SUM(CASE WHEN status_class = 4 THEN 1 ELSE 0 END) AS s4xx,
      SUM(CASE WHEN status_class = 5 THEN 1 ELSE 0 END) AS s5xx,

      SUM(bytes_sent) AS bytes_sent,

      SUM(CASE WHEN is_resource THEN 1 ELSE 0 END) AS resource_hits,
      SUM(CASE WHEN is_resource THEN bytes_sent ELSE 0 END) AS resource_bytes_sent,
      SUM(CASE WHEN is_resource AND status_class = 4 THEN 1 ELSE 0 END) AS resource_4xx,
      SUM(CASE WHEN is_resource AND status_class = 5 THEN 1 ELSE 0 END) AS resource_5xx,

      SUM(CASE WHEN is_resource AND is_bot THEN 1 ELSE 0 END) AS resource_hits_bot,
      SUM(CASE WHEN is_resource AND is_bot AND status_class = 4 THEN 1 ELSE 0 END) AS resource_4xx_bot,
      SUM(CASE WHEN is_resource AND is_bot AND status_class = 5 THEN 1 ELSE 0 END) AS resource_5xx_bot
    FROM parsed
    GROUP BY date, hour_local
    """

    bot_daily_sql = """
    SELECT
      date,
      COALESCE(bot_family, 'Unknown bot') AS bot_family,
      COUNT(*) AS hits,

      SUM(CASE WHEN status_class = 2 THEN 1 ELSE 0 END) AS s2xx,
      SUM(CASE WHEN status_class = 3 THEN 1 ELSE 0 END) AS s3xx,
      SUM(CASE WHEN status_class = 4 THEN 1 ELSE 0 END) AS s4xx,
      SUM(CASE WHEN status_class = 5 THEN 1 ELSE 0 END) AS s5xx,

      SUM(bytes_sent) AS bytes_sent,

      SUM(CASE WHEN is_resource THEN 1 ELSE 0 END) AS resource_hits,
      SUM(CASE WHEN is_resource AND status_class IN (3,4,5) THEN 1 ELSE 0 END) AS resource_errors,
      (SUM(CASE WHEN is_resource THEN 1 ELSE 0 END) * 1.0) / NULLIF(COUNT(*), 0) AS resource_hits_pct
    FROM parsed
    WHERE is_bot
    GROUP BY date, COALESCE(bot_family, 'Unknown bot')
    """

    locale_daily_sql = """
    SELECT
      date,
      COALESCE(locale, 'Unknown') AS locale,
      COUNT(*) AS hits,
      SUM(CASE WHEN is_bot THEN 1 ELSE 0 END) AS hits_bot,
      SUM(CASE WHEN NOT is_bot THEN 1 ELSE 0 END) AS hits_human,
      SUM(CASE WHEN status_class = 2 THEN 1 ELSE 0 END) AS s2xx,
      SUM(CASE WHEN status_class = 3 THEN 1 ELSE 0 END) AS s3xx,
      SUM(CASE WHEN status_class = 4 THEN 1 ELSE 0 END) AS s4xx,
      SUM(CASE WHEN status_class = 5 THEN 1 ELSE 0 END) AS s5xx,
      SUM(bytes_sent) AS bytes_sent,
      SUM(CASE WHEN is_resource THEN 1 ELSE 0 END) AS resource_hits,
      SUM(CASE WHEN is_resource AND is_bot THEN 1 ELSE 0 END) AS resource_hits_bot
    FROM parsed
    GROUP BY date, COALESCE(locale, 'Unknown')
    """

    group_daily_sql = """
    SELECT
      date,
      url_group,
      COUNT(*) AS hits,
      SUM(CASE WHEN is_bot THEN 1 ELSE 0 END) AS hits_bot,
      SUM(CASE WHEN NOT is_bot THEN 1 ELSE 0 END) AS hits_human,
      SUM(CASE WHEN status_class = 2 THEN 1 ELSE 0 END) AS s2xx,
      SUM(CASE WHEN status_class = 3 THEN 1 ELSE 0 END) AS s3xx,
      SUM(CASE WHEN status_class = 4 THEN 1 ELSE 0 END) AS s4xx,
      SUM(CASE WHEN status_class = 5 THEN 1 ELSE 0 END) AS s5xx,
      SUM(bytes_sent) AS bytes_sent,
      SUM(CASE WHEN is_resource THEN 1 ELSE 0 END) AS resource_hits,
      SUM(CASE WHEN is_resource AND is_bot THEN 1 ELSE 0 END) AS resource_hits_bot
    FROM parsed
    GROUP BY date, url_group
    """

    locale_group_daily_sql = """
    SELECT
      date,
      COALESCE(locale, 'Unknown') AS locale,
      url_group,
      COUNT(*) AS hits,
      SUM(CASE WHEN is_bot THEN 1 ELSE 0 END) AS hits_bot,
      SUM(CASE WHEN NOT is_bot THEN 1 ELSE 0 END) AS hits_human,
      SUM(CASE WHEN status_class = 2 THEN 1 ELSE 0 END) AS s2xx,
      SUM(CASE WHEN status_class = 3 THEN 1 ELSE 0 END) AS s3xx,
      SUM(CASE WHEN status_class = 4 THEN 1 ELSE 0 END) AS s4xx,
      SUM(CASE WHEN status_class = 5 THEN 1 ELSE 0 END) AS s5xx,
      SUM(bytes_sent) AS bytes_sent
    FROM parsed
    GROUP BY date, COALESCE(locale, 'Unknown'), url_group
    """

    top_urls_daily_sql = """
    SELECT
      date,
      path,
      url_group,
      COUNT(*) AS hits_total,
      SUM(CASE WHEN is_bot THEN 1 ELSE 0 END) AS hits_bot,
      SUM(CASE WHEN NOT is_bot THEN 1 ELSE 0 END) AS hits_human,
      SUM(CASE WHEN status_class = 3 THEN 1 ELSE 0 END) AS s3xx,
      SUM(CASE WHEN status_class = 4 THEN 1 ELSE 0 END) AS s4xx,
      SUM(CASE WHEN status_class = 5 THEN 1 ELSE 0 END) AS s5xx,
      SUM(CASE WHEN is_parameterized THEN 1 ELSE 0 END) AS parameterized_hits,
      SUM(bytes_sent) AS bytes_sent
    FROM parsed
    GROUP BY date, path, url_group
    """

    top_404_daily_sql = """
    SELECT
      date,
      path,
      url_group,
      COUNT(*) AS hits_404,
      SUM(CASE WHEN is_bot THEN 1 ELSE 0 END) AS hits_404_bot,
      SUM(bytes_sent) AS bytes_sent_404
    FROM parsed
    WHERE status = 404
    GROUP BY date, path, url_group
    """

    top_5xx_daily_sql = """
    SELECT
      date,
      path,
      url_group,
      COUNT(*) AS hits_5xx,
      SUM(CASE WHEN is_bot THEN 1 ELSE 0 END) AS hits_5xx_bot
    FROM parsed
    WHERE status_class = 5
    GROUP BY date, path, url_group
    """

    wasted_crawl_daily_sql = """
    SELECT
      date,
      path,
      url_group,
      COALESCE(bot_family, 'Unknown bot') AS bot_family,

      COUNT(*) AS bot_hits,

      SUM(CASE WHEN is_parameterized THEN 1 ELSE 0 END) AS parameterized_bot_hits,
      SUM(CASE WHEN status_class = 3 THEN 1 ELSE 0 END) AS redirect_bot_hits,
      SUM(CASE WHEN status_class IN (4,5) THEN 1 ELSE 0 END) AS error_bot_hits,

      SUM(CASE WHEN is_resource THEN 1 ELSE 0 END) AS resource_bot_hits,
      SUM(CASE WHEN is_resource AND is_parameterized THEN 1 ELSE 0 END) AS resource_parameterized_bot_hits,
      SUM(CASE WHEN is_resource AND status_class IN (3,4,5) THEN 1 ELSE 0 END) AS resource_error_bot_hits,

      (
        SUM(CASE WHEN is_parameterized THEN 1 ELSE 0 END) +
        SUM(CASE WHEN status_class = 3 THEN 1 ELSE 0 END) +
        SUM(CASE WHEN status_class IN (4,5) THEN 1 ELSE 0 END) +
        SUM(CASE WHEN is_resource AND is_parameterized THEN 1 ELSE 0 END) +
        SUM(CASE WHEN is_resource AND status_class IN (3,4,5) THEN 1 ELSE 0 END)
      ) AS waste_score,

      (
        (
          SUM(CASE WHEN is_parameterized THEN 1 ELSE 0 END) +
          SUM(CASE WHEN status_class = 3 THEN 1 ELSE 0 END) +
          SUM(CASE WHEN status_class IN (4,5) THEN 1 ELSE 0 END) +
          SUM(CASE WHEN is_resource AND is_parameterized THEN 1 ELSE 0 END) +
          SUM(CASE WHEN is_resource AND status_class IN (3,4,5) THEN 1 ELSE 0 END)
        ) + SUM(CASE WHEN is_resource THEN 1 ELSE 0 END)
      ) AS waste_score_strict
    FROM parsed
    WHERE is_bot
    GROUP BY date, path, url_group, COALESCE(bot_family, 'Unknown bot')
    """

    top_resource_waste_daily_sql = """
    SELECT
      date,
      path,
      COUNT(*) AS bot_hits,
      SUM(CASE WHEN status_class IN (3,4,5) THEN 1 ELSE 0 END) AS resource_error_bot_hits,
      SUM(CASE WHEN status = 404 THEN 1 ELSE 0 END) AS status_404_bot_hits,
      (SUM(CASE WHEN status_class IN (3,4,5) THEN 1 ELSE 0 END) + COUNT(*)) AS waste_score_strict
    FROM parsed
    WHERE is_bot AND is_resource
    GROUP BY date, path
    """

    def out(name: str) -> Path:
        return DATA_AGG / name / f"date={log_date}" / "part.parquet"

    agg_write_one(conn, daily_sql, out("daily"))
    agg_write_one(conn, hourly_sql, out("hourly"))
    agg_write_one(conn, bot_daily_sql, out("bot_daily"))
    agg_write_one(conn, locale_daily_sql, out("locale_daily"))
    agg_write_one(conn, group_daily_sql, out("group_daily"))
    agg_write_one(conn, locale_group_daily_sql, out("locale_group_daily"))
    agg_write_one(conn, top_urls_daily_sql, out("top_urls_daily"))
    agg_write_one(conn, top_404_daily_sql, out("top_404_daily"))
    agg_write_one(conn, top_5xx_daily_sql, out("top_5xx_daily"))
    agg_write_one(conn, wasted_crawl_daily_sql, out("wasted_crawl_daily"))
    agg_write_one(conn, top_resource_waste_daily_sql, out("top_resource_waste_daily"))

    conn.close()


def gather_files_for_date(files: List[Path], log_date: str) -> List[Path]:
    out = []
    for fp in files:
        d = extract_date_from_filename(fp.name)
        if d == log_date:
            out.append(fp)
    return sorted(out)


def ingest(dry_run: bool = False) -> None:
    ensure_dirs()
    init_manifest()
    bot_rules = load_bot_rules()
    url_cfg = load_url_grouping()

    files = list_raw_files()
    if not files:
        print("No files found in data/raw/")
        return

    affected_dates = set()

    for fp in files:
        d = extract_date_from_filename(fp.name)
        if not d:
            continue
        size_b, mtime = file_meta(fp)
        prev = manifest_get(fp)
        if prev is None or prev[0] != size_b or prev[1] != mtime:
            affected_dates.add(d)

    if not affected_dates:
        print("No new/changed raw files. Nothing to do.")
        return

    print(f"Affected dates: {', '.join(sorted(affected_dates))}")

    for d in sorted(affected_dates):
        day_files = gather_files_for_date(files, d)
        if not day_files:
            continue

        print(f"\n[DATE {d}] Rebuilding parsed partition from {len(day_files)} file(s)")
        if dry_run:
            continue

        n_rows = build_parsed_for_date(d, day_files, bot_rules, url_cfg)
        print(f"[DATE {d}] Parsed rows: {n_rows}")

        print(f"[DATE {d}] Building aggregates")
        build_aggregates_for_date(d)
        print(f"[DATE {d}] Aggregates done")

        # Update manifest for day files
        for fp in day_files:
            size_b, mtime = file_meta(fp)
            manifest_upsert(fp, size_b, mtime, d)

    print("\nIngest complete.")


def main():
    parser = argparse.ArgumentParser(description="Ingest raw nginx logs -> parsed parquet -> aggregates")
    parser.add_argument("--dry-run", action="store_true", help="Scan and print affected dates, but do not write output")
    args = parser.parse_args()
    ingest(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
