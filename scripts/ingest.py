#!/usr/bin/env python3
import os
import re
import subprocess
import time
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

from urllib.parse import parse_qs, unquote_plus


ROOT = Path(__file__).resolve().parents[1]
DATA_RAW = ROOT / "data" / "raw"
DATA_PARSED = ROOT / "data" / "parsed"
DATA_AGG = ROOT / "data" / "aggregates"
DETECTORS_DIR = ROOT / "detectors"
STATE_DIR = ROOT / "state"
MANIFEST_DB = STATE_DIR / "manifest.sqlite"

# Locale label to use when the URL does not start with a whitelisted locale segment.
NO_LOCALE_LABEL = "no-locale"

# Label used when a request has UTM params but no utm_source key.
NO_UTM_SOURCE_LABEL = "(none)"  # NEW


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
    locales: set  # normalized to lowercase
    rules: List[UrlRule]
    section_map: Dict[str, str]
    fallback_group: str
    locale_homepage_group: str = "Locale Homepage"
    locale_homepage_pattern: Optional[re.Pattern] = None  # fallback for unlisted locale-like segments


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


def load_referer_rules() -> List[BotRule]:
    cfg = load_yaml(DETECTORS_DIR / "bots.yml")
    rules = []
    for r in cfg.get("referer_rules", []):
        pat = re.compile(r["pattern"], re.IGNORECASE)
        rules.append(BotRule(family=r["family"], pattern=pat))
    return rules


def load_url_grouping() -> UrlGroupingConfig:
    cfg = load_yaml(DETECTORS_DIR / "url_groups.yml")

    # Normalize locales to lowercase for robust matching against URL path segments.
    locales = set([str(x).strip().lower() for x in cfg.get("locales", []) if str(x).strip()])

    rules: List[UrlRule] = []
    for r in cfg.get("rules", []):
        ur = UrlRule(group=r["group"], match=r["match"], value=r["value"])
        if ur.match == "regex":
            ur.compiled = re.compile(str(ur.value), re.IGNORECASE)
        rules.append(ur)

    section_map = {k.lower(): v for k, v in (cfg.get("section_map") or {}).items()}
    fallback = cfg.get("fallback_group", "Other Content")
    locale_homepage_group = cfg.get("locale_homepage_group", "Locale Homepage")
    raw_pattern = cfg.get("locale_homepage_pattern", "")
    locale_homepage_pattern = re.compile(raw_pattern, re.IGNORECASE) if raw_pattern else None
    return UrlGroupingConfig(locales=locales, rules=rules, section_map=section_map, fallback_group=fallback, locale_homepage_group=locale_homepage_group, locale_homepage_pattern=locale_homepage_pattern)


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
    seg = path.rsplit("/", 1)[-1]
    if "." not in seg:
        return None
    ext = seg.rsplit(".", 1)[-1].lower()
    return ext if ext else None


def normalize_path_only(path: str) -> str:
    p = (path or "/").split("?", 1)[0]
    if not p.startswith("/"):
        p = "/" + p
    return p.lower() or "/"


def apply_url_grouping(path: str, cfg: UrlGroupingConfig) -> Tuple[str, Optional[str], Optional[str]]:
    p = normalize_path_only(path)

    for r in cfg.rules:
        if r.match == "exact":
            if p == str(r.value).lower():
                return r.group, NO_LOCALE_LABEL, None
        elif r.match == "prefix":
            if p.startswith(str(r.value).lower()):
                return r.group, NO_LOCALE_LABEL, None
        elif r.match == "regex":
            if r.compiled and r.compiled.search(p):
                return r.group, NO_LOCALE_LABEL, None
        elif r.match == "ext":
            ex = ext_of_path(p)
            if ex and ex in set([str(x).lower() for x in r.value]):
                return r.group, NO_LOCALE_LABEL, None

    segs = [s for s in p.split("/") if s]
    if not segs:
        return "Home", NO_LOCALE_LABEL, None

    first = segs[0]
    locale: Optional[str] = None
    section: Optional[str] = None

    if first in cfg.locales:
        locale = first
        if len(segs) == 1:
            return cfg.locale_homepage_group, locale, None
        section_index = 1
        section = segs[section_index] if len(segs) > section_index else None
    elif len(segs) == 1 and cfg.locale_homepage_pattern and cfg.locale_homepage_pattern.fullmatch(first):
        # Single-segment path whose segment looks like a locale code but isn't
        # in the explicit whitelist (e.g. /en-gb, /ja, /zh-tw).  Classify as
        # Locale Homepage so these don't pollute Other Content.
        return cfg.locale_homepage_group, first, None
    else:
        locale = NO_LOCALE_LABEL
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


def parse_request(req: str) -> Tuple[Optional[str], str, Optional[str], bool, str, Optional[str]]:
    """
    request line: "GET /path?x=y HTTP/1.1"
    returns (method, path_no_query, http_version, has_query, request_target, query_string)
    """
    if not req:
        return None, "/", None, False, "/", None
    parts = req.split()
    if len(parts) < 2:
        return None, "/", None, False, "/", None

    method = parts[0]
    request_target = parts[1] or "/"
    http_ver = parts[2] if len(parts) >= 3 else None

    if "?" in request_target:
        path, qs = request_target.split("?", 1)
        path = path or "/"
        qs = qs if qs != "" else None
        return method, path, http_ver, True, request_target, qs

    return method, request_target, http_ver, False, request_target, None


def parse_time_local(time_s: str) -> Tuple[datetime, datetime]:
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


# ---------- NEW: UTM parsing helpers ----------

def _first_qs_value(qs: dict, key: str) -> Optional[str]:
    v = qs.get(key)
    if not v:
        return None
    s = v[0] if isinstance(v, list) else v
    if s is None:
        return None
    s = str(s).strip()
    return s if s != "" else None


def parse_utm_fields(query_string: Optional[str]) -> Tuple[bool, Optional[str], Optional[str], Optional[str], Optional[str], Optional[str], Optional[str]]:
    if not query_string:
        return False, None, None, None, None, None, None

    # Decode at the query-string level first (handles %27/%2527 etc.)
    qs_decoded = _safe_unquote_plus(query_string, passes=2)

    # parse_qs will still handle splitting/decoding reliably
    qs = parse_qs(qs_decoded, keep_blank_values=True)

    has_utm = any(k.lower().startswith("utm_") for k in qs.keys())
    if not has_utm:
        return False, None, None, None, None, None, None

    utm_source = normalize_utm_value(_first_qs_value(qs, "utm_source") or _first_qs_value(qs, "UTM_SOURCE"))
    utm_medium = normalize_utm_value(_first_qs_value(qs, "utm_medium") or _first_qs_value(qs, "UTM_MEDIUM"))
    utm_campaign = normalize_utm_value(_first_qs_value(qs, "utm_campaign") or _first_qs_value(qs, "UTM_CAMPAIGN"))
    utm_term = normalize_utm_value(_first_qs_value(qs, "utm_term") or _first_qs_value(qs, "UTM_TERM"))
    utm_content = normalize_utm_value(_first_qs_value(qs, "utm_content") or _first_qs_value(qs, "UTM_CONTENT"))

    utm_source_norm = utm_source.lower() if utm_source else None

    return has_utm, utm_source, utm_source_norm, utm_medium, utm_campaign, utm_term, utm_content

def _find_go_binary() -> Optional[str]:
    """Return an absolute path to the 'go' executable, or None."""
    import shutil
    for p in ["/usr/local/go/bin/go", "/usr/local/bin/go", "/usr/bin/go"]:
        if os.path.isfile(p) and os.access(p, os.X_OK):
            return p
    return shutil.which("go")


def _try_build_native_parser() -> Optional[Path]:
    """Compile the Go parser from source if Go is available. Returns binary path or None."""
    parser_dir = ROOT / "parser"
    candidate = parser_dir / "log-parser"
    if candidate.exists() and os.access(candidate, os.X_OK):
        return candidate
    if not (parser_dir / "main.go").exists():
        return None
    go_bin = _find_go_binary()
    if go_bin is None:
        print("[parser] Go toolchain not found — using Python parser.", flush=True)
        return None
    print("[parser] Compiling native parser (this runs once)...", flush=True)
    t0 = time.monotonic()
    try:
        result = subprocess.run(
            [go_bin, "build", "-o", str(candidate), "."],
            cwd=str(parser_dir),
            capture_output=True,
            text=True,
            timeout=120,
            env={**os.environ, "GONOSUMDB": "*"},
        )
    except subprocess.TimeoutExpired:
        print("[parser] Go build timed out — using Python parser.", flush=True)
        return None
    except Exception as exc:
        print(f"[parser] Go build error ({exc}) — using Python parser.", flush=True)
        return None
    elapsed = time.monotonic() - t0
    if result.returncode != 0:
        print(
            f"[parser] Go build failed (exit {result.returncode}) — using Python parser.\n"
            f"  stderr: {(result.stderr or '').strip()[:300]}",
            flush=True,
        )
        return None
    if not candidate.exists() or not os.access(candidate, os.X_OK):
        print("[parser] Go build appeared to succeed but binary not found — using Python parser.", flush=True)
        return None
    print(f"[parser] Native parser built in {elapsed:.1f}s \u2192 {candidate}", flush=True)
    return candidate


def _find_native_parser() -> Optional[Path]:
    """Return the path to the native log-parser binary, compiling it if needed."""
    candidate = ROOT / "parser" / "log-parser"
    if candidate.exists() and os.access(candidate, os.X_OK):
        return candidate
    return _try_build_native_parser()


def _build_parsed_native(log_date: str, files: List[Path], native_bin: Path) -> int:
    """Use the Go native parser binary to write the parquet file.

    The binary writes NDJSON; we read it here and convert to parquet so
    the schema matches what the rest of the pipeline (DuckDB) expects.
    Returns the number of rows written.
    """
    import tempfile

    out_dir = DATA_PARSED / f"date={log_date}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "access.parquet"

    bots_yml = DETECTORS_DIR / "bots.yml"
    urls_yml = DETECTORS_DIR / "url_groups.yml"

    with tempfile.NamedTemporaryFile(suffix=".ndjson", delete=False) as tmp:
        tmp_path = tmp.name

    try:
        cmd = [
            str(native_bin),
            "--date", log_date,
            "--out", tmp_path,
            "--bots", str(bots_yml),
            "--urls", str(urls_yml),
        ] + [str(f) for f in files]

        result = subprocess.run(cmd, capture_output=True, text=True)

        # Parser stats go to stderr (shown in rebuild output).
        if result.stderr:
            print(result.stderr, end="", flush=True)

        if result.returncode != 0:
            raise RuntimeError(
                f"native parser exited {result.returncode}: {result.stdout.strip()}"
            )

        # Row count is on stdout.
        try:
            n_rows = int(result.stdout.strip())
        except ValueError:
            n_rows = 0

        if n_rows == 0:
            # Write empty parquet with correct schema.
            df = pd.DataFrame(columns=[
                "date", "ts_local", "ts_utc", "edge_ip", "method", "path", "http_version",
                "status", "status_class", "bytes_sent", "referer", "user_agent",
                "has_query", "is_parameterized", "locale", "section", "url_group", "is_resource",
                "is_bot", "bot_family", "request_target", "query_string",
                "is_utm_chatgpt", "has_utm",
                "utm_source", "utm_source_norm", "utm_medium", "utm_campaign", "utm_term", "utm_content",
            ])
            df.to_parquet(out_path, index=False)
            return 0

        # Convert NDJSON → parquet with DuckDB — avoids the slow pandas read_json
        # round-trip. DuckDB's NDJSON reader is C++ and orders of magnitude faster.
        #
        # ts_local: ISO-8601 string with offset → TIMESTAMPTZ (stored as UTC).
        # ts_utc:   ISO-8601 string, no offset  → naive TIMESTAMP.
        conn = duckdb.connect(database=":memory:")
        try:
            conn.execute(f"""
                COPY (
                    SELECT
                        date,
                        ts_local::TIMESTAMPTZ  AS ts_local,
                        ts_utc::TIMESTAMP      AS ts_utc,
                        edge_ip,
                        method,
                        path,
                        http_version,
                        status,
                        status_class,
                        bytes_sent,
                        referer,
                        user_agent,
                        has_query,
                        is_parameterized,
                        locale,
                        section,
                        url_group,
                        is_resource,
                        is_bot,
                        bot_family,
                        request_target,
                        query_string,
                        is_utm_chatgpt,
                        has_utm,
                        utm_source,
                        utm_source_norm,
                        utm_medium,
                        utm_campaign,
                        utm_term,
                        utm_content
                    FROM read_ndjson('{tmp_path}', auto_detect=true)
                ) TO '{out_path.as_posix()}' (FORMAT PARQUET)
            """)
        finally:
            conn.close()
        return n_rows

    finally:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass


def build_parsed_for_date(log_date: str, files: List[Path], bot_rules: List[BotRule], url_cfg: UrlGroupingConfig, referer_rules: Optional[List[BotRule]] = None) -> int:
    # Try the native Go binary first; fall back to the Python implementation.
    native_bin = _find_native_parser()
    if native_bin:
        try:
            return _build_parsed_native(log_date, files, native_bin)
        except Exception as e:
            print(
                f"[DATE {log_date}] native parser failed ({e}), falling back to Python",
                flush=True,
            )

    t0 = time.monotonic()
    rows = []
    bad = 0

    # Per-date caches: most bots repeat the same UA and URL patterns heavily.
    ua_cache: Dict[str, Tuple[bool, str]] = {}
    path_cache: Dict[str, Tuple[str, Optional[str], Optional[str]]] = {}

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

                method, path, http_ver, has_query, request_target, query_string = parse_request(req)

                if referer == "-" or referer == "":
                    referer = None

                if ua not in ua_cache:
                    ua_cache[ua] = classify_bot(ua, bot_rules)
                is_bot, bot_family = ua_cache[ua]

                # Referer check is per-request (same UA can arrive with different referers).
                if not is_bot and referer and referer_rules:
                    for rr in referer_rules:
                        if rr.pattern.search(referer.lower()):
                            is_bot, bot_family = True, rr.family
                            break

                if path not in path_cache:
                    path_cache[path] = apply_url_grouping(path, url_cfg)
                url_group, locale, section = path_cache[path]

                # NEW: dynamic UTM extraction
                has_utm, utm_source, utm_source_norm, utm_medium, utm_campaign, utm_term, utm_content = parse_utm_fields(query_string)
                is_utm_chatgpt = (utm_source_norm == "chatgpt.com") if utm_source_norm else False

                is_resource = url_group in ("Nuxt Assets", "Static Assets")

                rows.append({
                    "date": log_date,
                    "ts_local": ts_local,
                    "ts_utc": ts_utc.replace(tzinfo=None),
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
                    "request_target": request_target,
                    "query_string": query_string,

                    # legacy + new UTM fields
                    "is_utm_chatgpt": bool(is_utm_chatgpt),
                    "has_utm": bool(has_utm),
                    "utm_source": utm_source,
                    "utm_source_norm": utm_source_norm,
                    "utm_medium": utm_medium,
                    "utm_campaign": utm_campaign,
                    "utm_term": utm_term,
                    "utm_content": utm_content,
                })

    if not rows:
        df = pd.DataFrame(columns=[
            "date", "ts_local", "ts_utc", "edge_ip", "method", "path", "http_version",
            "status", "status_class", "bytes_sent", "referer", "user_agent",
            "has_query", "is_parameterized", "locale", "section", "url_group", "is_resource",
            "is_bot", "bot_family", "request_target", "query_string",

            # legacy + new UTM columns
            "is_utm_chatgpt", "has_utm",
            "utm_source", "utm_source_norm", "utm_medium", "utm_campaign", "utm_term", "utm_content",
        ])
    else:
        df = pd.DataFrame(rows)

    out_dir = DATA_PARSED / f"date={log_date}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "access.parquet"
    df.to_parquet(out_path, index=False)

    elapsed = time.monotonic() - t0
    n = len(df)
    rate = int(n / elapsed) if elapsed > 0 else 0
    print(f"[DATE {log_date}] parse complete: {n} rows, {bad} bad lines, {elapsed:.1f}s ({rate:,} rows/s)")

    return n


def agg_write_one(conn: duckdb.DuckDBPyConnection, sql: str, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    conn.execute(f"COPY ({sql}) TO '{out_path.as_posix()}' (FORMAT PARQUET);")


def build_aggregates_for_date(log_date: str) -> None:
    parsed_path = (DATA_PARSED / f"date={log_date}" / "access.parquet")
    if not parsed_path.exists():
        return

    conn = duckdb.connect(database=":memory:")
    conn.execute("PRAGMA threads=4;")
    conn.execute(f"CREATE OR REPLACE VIEW parsed AS SELECT * FROM read_parquet('{parsed_path.as_posix()}');")

    # ----------------------------
    # existing aggregates unchanged
    # ----------------------------

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

    locale_daily_sql = f"""
    SELECT
      date,
      COALESCE(locale, '{NO_LOCALE_LABEL}') AS locale,
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
    GROUP BY date, COALESCE(locale, '{NO_LOCALE_LABEL}')
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

    locale_group_daily_sql = f"""
    SELECT
      date,
      COALESCE(locale, '{NO_LOCALE_LABEL}') AS locale,
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
    GROUP BY date, COALESCE(locale, '{NO_LOCALE_LABEL}'), url_group
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
      SUM(CASE WHEN status = 301 THEN 1 ELSE 0 END) AS s301,
      SUM(CASE WHEN status = 302 THEN 1 ELSE 0 END) AS s302,
      SUM(CASE WHEN status = 303 THEN 1 ELSE 0 END) AS s303,
      SUM(CASE WHEN status = 307 THEN 1 ELSE 0 END) AS s307,
      SUM(CASE WHEN status = 308 THEN 1 ELSE 0 END) AS s308,
      SUM(CASE WHEN status_class = 4 THEN 1 ELSE 0 END) AS s4xx,
      SUM(CASE WHEN status_class = 5 THEN 1 ELSE 0 END) AS s5xx,
      SUM(CASE WHEN is_parameterized THEN 1 ELSE 0 END) AS parameterized_hits,
      SUM(bytes_sent) AS bytes_sent
    FROM parsed
    GROUP BY date, path, url_group
    """

    top_4xx_daily_sql = """
    SELECT
      date,
      path,
      url_group,
      COUNT(*) AS hits_4xx,
      SUM(CASE WHEN is_bot THEN 1 ELSE 0 END) AS hits_4xx_bot,
      SUM(bytes_sent) AS bytes_sent_4xx,
      SUM(CASE WHEN status = 400 THEN 1 ELSE 0 END) AS s400,
      SUM(CASE WHEN status = 401 THEN 1 ELSE 0 END) AS s401,
      SUM(CASE WHEN status = 403 THEN 1 ELSE 0 END) AS s403,
      SUM(CASE WHEN status = 404 THEN 1 ELSE 0 END) AS s404,
      SUM(CASE WHEN status = 405 THEN 1 ELSE 0 END) AS s405,
      SUM(CASE WHEN status = 410 THEN 1 ELSE 0 END) AS s410,
      SUM(CASE WHEN status = 422 THEN 1 ELSE 0 END) AS s422,
      SUM(CASE WHEN status = 429 THEN 1 ELSE 0 END) AS s429
    FROM parsed
    WHERE status_class = 4
    GROUP BY date, path, url_group
    """

    top_5xx_daily_sql = """
    SELECT
      date,
      path,
      url_group,
      COUNT(*) AS hits_5xx,
      SUM(CASE WHEN is_bot THEN 1 ELSE 0 END) AS hits_5xx_bot,
      SUM(CASE WHEN status = 500 THEN 1 ELSE 0 END) AS s500,
      SUM(CASE WHEN status = 502 THEN 1 ELSE 0 END) AS s502,
      SUM(CASE WHEN status = 503 THEN 1 ELSE 0 END) AS s503,
      SUM(CASE WHEN status = 504 THEN 1 ELSE 0 END) AS s504
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

    human_urls_daily_sql = """
    SELECT
    date,
    path,
    url_group,
    COUNT(*) AS hits
    FROM parsed
    WHERE NOT is_bot
    GROUP BY date, path, url_group
    """

    bot_urls_daily_sql = """
    SELECT
    date,
    COALESCE(bot_family, 'Unknown bot') AS bot_family,
    path,
    url_group,
    COUNT(*) AS hits
    FROM parsed
    WHERE is_bot
    GROUP BY date, COALESCE(bot_family, 'Unknown bot'), path, url_group
    """

    # ----------------------------
    # legacy UTM chatgpt aggregates (kept, derived from utm_source_norm)
    # ----------------------------
    utm_chatgpt_daily_sql = """
    SELECT
    date,
    COUNT(*) AS hits,
    SUM(CASE WHEN is_bot THEN 1 ELSE 0 END) AS hits_bot,
    SUM(CASE WHEN NOT is_bot THEN 1 ELSE 0 END) AS hits_human
    FROM parsed
    WHERE utm_source_norm = 'chatgpt.com'
    GROUP BY date
    """

    utm_chatgpt_urls_daily_sql = """
    SELECT
    date,
    path,
    url_group,
    COUNT(*) AS hits
    FROM parsed
    WHERE utm_source_norm = 'chatgpt.com'
    GROUP BY date, path, url_group
    """

    # ----------------------------
    # NEW: generic UTM aggregates
    # ----------------------------

    # Note: We aggregate by normalized utm_source (lowercase), with a stable label for missing utm_source.
    utm_sources_daily_sql = f"""
    SELECT
      date,
      COALESCE(utm_source_norm, '{NO_UTM_SOURCE_LABEL}') AS utm_source,
      COUNT(*) AS hits,
      SUM(CASE WHEN is_bot THEN 1 ELSE 0 END) AS hits_bot,
      SUM(CASE WHEN NOT is_bot THEN 1 ELSE 0 END) AS hits_human
    FROM parsed
    WHERE has_utm
    GROUP BY date, COALESCE(utm_source_norm, '{NO_UTM_SOURCE_LABEL}')
    """

    utm_source_urls_daily_sql = f"""
    SELECT
      date,
      COALESCE(utm_source_norm, '{NO_UTM_SOURCE_LABEL}') AS utm_source,
      path,
      url_group,
      COUNT(*) AS hits,
      SUM(CASE WHEN is_bot THEN 1 ELSE 0 END) AS hits_bot,
      SUM(CASE WHEN NOT is_bot THEN 1 ELSE 0 END) AS hits_human
    FROM parsed
    WHERE has_utm
    GROUP BY date, COALESCE(utm_source_norm, '{NO_UTM_SOURCE_LABEL}'), path, url_group
    """

    def out(name: str) -> Path:
        return DATA_AGG / name / f"date={log_date}" / "part.parquet"

    # existing
    agg_write_one(conn, daily_sql, out("daily"))
    agg_write_one(conn, hourly_sql, out("hourly"))
    agg_write_one(conn, bot_daily_sql, out("bot_daily"))
    agg_write_one(conn, locale_daily_sql, out("locale_daily"))
    agg_write_one(conn, group_daily_sql, out("group_daily"))
    agg_write_one(conn, locale_group_daily_sql, out("locale_group_daily"))
    agg_write_one(conn, top_urls_daily_sql, out("top_urls_daily"))
    agg_write_one(conn, top_4xx_daily_sql, out("top_4xx_daily"))
    agg_write_one(conn, top_5xx_daily_sql, out("top_5xx_daily"))
    agg_write_one(conn, wasted_crawl_daily_sql, out("wasted_crawl_daily"))
    agg_write_one(conn, top_resource_waste_daily_sql, out("top_resource_waste_daily"))
    agg_write_one(conn, bot_urls_daily_sql, out("bot_urls_daily"))
    agg_write_one(conn, human_urls_daily_sql, out("human_urls_daily"))

    # legacy
    agg_write_one(conn, utm_chatgpt_daily_sql, out("utm_chatgpt_daily"))
    agg_write_one(conn, utm_chatgpt_urls_daily_sql, out("utm_chatgpt_urls_daily"))

    # NEW generic
    agg_write_one(conn, utm_sources_daily_sql, out("utm_sources_daily"))
    agg_write_one(conn, utm_source_urls_daily_sql, out("utm_source_urls_daily"))

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
    referer_rules = load_referer_rules()
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

    t_ingest_start = time.monotonic()

    for d in sorted(affected_dates):
        day_files = gather_files_for_date(files, d)
        if not day_files:
            continue

        print(f"\n[DATE {d}] Rebuilding parsed partition from {len(day_files)} file(s)")
        if dry_run:
            continue

        t0 = time.monotonic()
        n_rows = build_parsed_for_date(d, day_files, bot_rules, url_cfg, referer_rules)
        t_parsed = time.monotonic()
        print(f"[DATE {d}] Parsed rows: {n_rows} ({t_parsed - t0:.1f}s)", flush=True)

        build_aggregates_for_date(d)
        t_done = time.monotonic()
        print(f"[DATE {d}] Aggregates done ({t_done - t_parsed:.1f}s, total {t_done - t0:.1f}s)")

        for fp in day_files:
            size_b, mtime = file_meta(fp)
            manifest_upsert(fp, size_b, mtime, d)

    elapsed = time.monotonic() - t_ingest_start
    print(f"\nIngest complete. {len(affected_dates)} date(s) in {elapsed:.1f}s.")

def _safe_unquote_plus(s: str, passes: int = 2) -> str:
    """
    Decode percent-encoding and '+' to space. Do up to `passes` rounds to handle
    double-encoding like %2527 -> %27 -> '.
    """
    out = s or ""
    for _ in range(max(1, int(passes))):
        new = unquote_plus(out)
        if new == out:
            break
        out = new
    return out

def normalize_utm_value(s: Optional[str]) -> Optional[str]:
    """
    Canonicalize decoded UTM values for grouping.
    Fixes cases where utm_source ends with a decoded %27 (apostrophe).
    - trims whitespace
    - strips wrapping quotes/apostrophes/backticks from BOTH ends repeatedly
    - keeps internal apostrophes intact (only touches ends)
    """
    if s is None:
        return None

    s = str(s)
    s = _safe_unquote_plus(s, passes=2)  # handles %27 and %2527 safely
    s = s.strip()
    if not s:
        return None

    # Strip ONLY from ends, repeatedly
    strip_chars = " '\"\t\r\n`"
    prev = None
    while s != prev:
        prev = s
        s = s.strip(strip_chars)

    return s if s else None

def main():
    parser = argparse.ArgumentParser(description="Ingest raw nginx logs -> parsed parquet -> aggregates")
    parser.add_argument("--dry-run", action="store_true", help="Scan and print affected dates, but do not write output")
    args = parser.parse_args()
    ingest(dry_run=args.dry_run)


if __name__ == "__main__":
    main()