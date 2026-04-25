from __future__ import annotations

import os
import re
import threading
import time
from collections import OrderedDict
from pathlib import Path
import json
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import duckdb
from fastapi import FastAPI, Query, Request
from fastapi.responses import (
    HTMLResponse,
    StreamingResponse,
    PlainTextResponse,
    RedirectResponse,
    JSONResponse,
)
from fastapi.staticfiles import StaticFiles

import csv
import functools
import io
import itertools
from html import escape as html_escape
from urllib.parse import quote as url_quote

import yaml

import pandas as pd

_chart_uid_counter = itertools.count(1)


def _chart_uid() -> str:
    return f"ec{next(_chart_uid_counter)}"


def _echart_html(option: dict, height: int = 400, fns: Optional[Dict[str, str]] = None) -> str:
    """Wrap an ECharts option dict in a div + init script, ready to embed.

    fns: map of placeholder marker (e.g. "__FMT__") to a JS function literal.
    Each occurrence of "<marker>" in the JSON is replaced with the literal,
    letting callers inject formatter functions that JSON cannot represent.
    """
    uid = _chart_uid()
    opt_json = json.dumps(option, default=str)
    if fns:
        for marker, js in fns.items():
            opt_json = opt_json.replace(f'"{marker}"', js)
    # Defer init until the browser has done layout — inline scripts run during
    # parsing, when clientWidth can be 0 and ECharts would lock to that width.
    # ResizeObserver also handles sidebar collapse / window resize after init.
    return (
        f"<div class='card'>"
        f"<div id='{uid}' data-echart style='width:100%;height:{height}px'></div>"
        f"<script>(function(){{"
        f"var el=document.getElementById('{uid}');"
        f"if(!el||!window.echarts)return;"
        f"var opt={opt_json};"
        f"function init(){{"
        f"var c=echarts.getInstanceByDom(el)||echarts.init(el);"
        f"c.setOption(opt);c.resize();"
        f"if(window.ResizeObserver){{new ResizeObserver(function(){{c.resize();}}).observe(el);}}}}"
        f"if(document.readyState==='loading'){{document.addEventListener('DOMContentLoaded',init);}}"
        f"else{{requestAnimationFrame(init);}}"
        f"}})();</script>"
        f"</div>"
    )

ROOT = Path(__file__).resolve().parents[1]
AGG = ROOT / "data" / "aggregates"

CHART_BAR_LIMIT = 30

NON_CONTENT_GROUPS = {"Static Assets", "Nuxt Assets", "API", "Feeds", "Robots", "Sitemaps", "Well-Known"}


def fmt_bytes(n) -> str:
    """Format a byte count as a human-readable string (B / KB / MB / GB)."""
    try:
        n = float(n)
    except (TypeError, ValueError):
        return ""
    for unit in ("B", "KB", "MB", "GB"):
        if abs(n) < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"

app = FastAPI(title="Local Log Dashboard")

_STATIC_DIR = ROOT / "app" / "static"
if _STATIC_DIR.is_dir():
    app.mount("/static", StaticFiles(directory=str(_STATIC_DIR)), name="static")

from app.settings import router as settings_router
app.include_router(settings_router)


REPORT_CACHE_TTL_SECONDS = int(os.environ.get("REPORT_CACHE_TTL_SECONDS", "900"))
REPORT_CACHE_MAX_ENTRIES = int(os.environ.get("REPORT_CACHE_MAX_ENTRIES", "256"))


class _TTLCache:
    """Thread-safe TTL + LRU cache. Used for DuckDB query results and filesystem
    partition scans. Keys are arbitrary hashables; values are any Python object
    (we rely on callers not mutating cached results)."""

    def __init__(self, ttl_seconds: int, max_entries: int):
        self._ttl = max(0, ttl_seconds)
        self._max = max(1, max_entries)
        self._store: "OrderedDict[object, tuple[float, object]]" = OrderedDict()
        self._lock = threading.Lock()

    def get(self, key):
        if self._ttl == 0:
            return None
        now = time.monotonic()
        with self._lock:
            entry = self._store.get(key)
            if entry is None:
                return None
            expires_at, value = entry
            if expires_at < now:
                self._store.pop(key, None)
                return None
            self._store.move_to_end(key)
            return value

    def set(self, key, value):
        if self._ttl == 0:
            return
        expires_at = time.monotonic() + self._ttl
        with self._lock:
            self._store[key] = (expires_at, value)
            self._store.move_to_end(key)
            while len(self._store) > self._max:
                self._store.popitem(last=False)

    def clear(self):
        with self._lock:
            self._store.clear()


_REPORT_CACHE = _TTLCache(REPORT_CACHE_TTL_SECONDS, REPORT_CACHE_MAX_ENTRIES)


def list_partitions(table: str, date_from: Optional[str], date_to: Optional[str]) -> List[str]:
    """
    We store partitions as: data/aggregates/<table>/date=YYYY-MM-DD/part.parquet
    To keep this beginner-friendly, we scan the filesystem and filter by date string.
    """
    cache_key = ("list_partitions", table, date_from, date_to)
    cached = _REPORT_CACHE.get(cache_key)
    if cached is not None:
        return list(cached)
    base = AGG / table
    if not base.exists():
        _REPORT_CACHE.set(cache_key, [])
        return []
    out = []
    for p in base.glob("date=*/part.parquet"):
        date_dir = p.parent.name
        if not date_dir.startswith("date="):
            continue
        d = date_dir.split("=", 1)[1]
        if date_from and d < date_from:
            continue
        if date_to and d > date_to:
            continue
        out.append(p.as_posix())
    out.sort()
    _REPORT_CACHE.set(cache_key, out)
    return list(out)

def available_dates() -> List[str]:
    """Return sorted list of dates that have at least one aggregate partition."""
    cache_key = ("available_dates",)
    cached = _REPORT_CACHE.get(cache_key)
    if cached is not None:
        return list(cached)
    dates: set[str] = set()
    if not AGG.exists():
        _REPORT_CACHE.set(cache_key, [])
        return []
    for p in AGG.glob("*/date=*/part.parquet"):
        date_dir = p.parent.name
        if date_dir.startswith("date="):
            dates.add(date_dir.split("=", 1)[1])
    result = sorted(dates)
    _REPORT_CACHE.set(cache_key, result)
    return list(result)


STATUS_CODE_LABELS = {
    # 3xx
    "s301": "301 Moved Permanently",
    "s302": "302 Found (temporary redirect)",
    "s303": "303 See Other",
    "s307": "307 Temporary Redirect",
    "s308": "308 Permanent Redirect",
    # 4xx
    "s400": "400 Bad Request",
    "s401": "401 Unauthorized",
    "s403": "403 Forbidden",
    "s404": "404 Not Found",
    "s405": "405 Method Not Allowed",
    "s410": "410 Gone",
    "s422": "422 Unprocessable Entity",
    "s429": "429 Too Many Requests",
    # 5xx
    "s500": "500 Internal Server Error",
    "s502": "502 Bad Gateway",
    "s503": "503 Service Unavailable",
    "s504": "504 Gateway Timeout",
    # aggregate bands
    "s2xx": "2xx Success",
    "s3xx": "3xx Redirects",
    "s4xx": "4xx Client Errors",
    "s5xx": "5xx Server Errors",
}


def html_table(rows, columns, max_rows: int = 999, server_paginated: bool = False) -> str:
    """
    Styled HTML table. A shared JS controller auto-enhances every
    .sortable table on DOMContentLoaded: non-server-paginated tables get
    client-side pagination (25/50/100/200 presets) and full-dataset sort;
    server-paginated tables (e.g. /logs) flip ?sort=&order= on header click.
    """
    head = "".join(
        f"<th scope='col' data-col='{i}' data-sort-col='{c}' title='{STATUS_CODE_LABELS.get(c, '')}'>{c}</th>"
        for i, c in enumerate(columns)
    )

    body_rows = []
    for i, r in enumerate(rows):
        if i >= max_rows:
            break
        tds = "".join(f"<td>{'' if v is None else v}</td>" for v in r)
        body_rows.append(f"<tr>{tds}</tr>")
    body = "\n".join(body_rows)

    table_cls = "sortable server-paginated" if server_paginated else "sortable"
    table = (
        f"<table class='{table_cls}'>"
        f"<thead><tr>{head}</tr></thead>"
        f"<tbody>{body}</tbody>"
        "</table>"
    )
    return (
        "<div class='card'>"
        f"<div class='table-wrapper'>{table}</div>"
        "<div class='table-controls' data-table-controls hidden></div>"
        "</div>"
    )


_DB_CONN: Optional[duckdb.DuckDBPyConnection] = None
_DB_INIT_LOCK = threading.Lock()


def _get_db() -> duckdb.DuckDBPyConnection:
    global _DB_CONN
    if _DB_CONN is None:
        with _DB_INIT_LOCK:
            if _DB_CONN is None:
                conn = duckdb.connect(database=":memory:")
                conn.execute("PRAGMA threads=4;")
                conn.execute("PRAGMA enable_object_cache=true;")
                _DB_CONN = conn
    return _DB_CONN


def _wrap_with_parquet_cte(paths: List[str], sql: str) -> str:
    files_sql = "[" + ",".join("'" + p.replace("'", "''") + "'" for p in paths) + "]"
    sql_clean = sql.strip().rstrip(";").strip()
    return (
        f"WITH t AS (SELECT * FROM read_parquet({files_sql}, union_by_name=true)) "
        f"{sql_clean}"
    )


def run_query(paths: List[str], sql: str):
    if not paths:
        return [], []
    cache_key = ("run_query", tuple(paths), sql)
    cached = _REPORT_CACHE.get(cache_key)
    if cached is not None:
        cols, rows = cached
        return list(cols), list(rows)
    cur = _get_db().cursor()
    try:
        res = cur.execute(_wrap_with_parquet_cte(paths, sql))
        cols = [d[0] for d in res.description]
        rows = res.fetchall()
        _REPORT_CACHE.set(cache_key, (tuple(cols), tuple(rows)))
        return cols, rows
    finally:
        cur.close()


def _parse_int(value, default: int) -> int:
    """Parse a query param that may arrive as '' from an HTML form."""
    if not value:
        return default
    try:
        return int(value)
    except (ValueError, TypeError):
        return default


def _parse_status_list(value: Optional[str]) -> List[int]:
    """Parse comma-separated status codes, e.g. '404,500' -> [404, 500]."""
    if not value:
        return []
    codes = []
    for part in value.split(","):
        part = part.strip()
        try:
            codes.append(int(part))
        except ValueError:
            continue
    return codes


def _parse_csv_param(value: Optional[str]) -> List[str]:
    """Parse comma-separated query param into list of non-empty strings."""
    if not value:
        return []
    return [v.strip() for v in value.split(",") if v.strip()]


def _in_clause(column: str, values: List[str]) -> Optional[str]:
    """Return `col IN ('v1','v2',...)` SQL, or None when values is empty."""
    if not values:
        return None
    escaped = ",".join(f"'{sql_escape_string(v)}'" for v in values)
    return f"{column} IN ({escaped})"


def _fmt_filter_display(values: List[str]) -> str:
    """Render a selected-values list for titles (e.g. 'a, b')."""
    return ", ".join(values)


def _fmt_filter_param(values: List[str]) -> str:
    """Render a selected-values list back into a CSV query-param value."""
    return ",".join(values)


def _coerce_numeric(df: pd.DataFrame, cols: List[str]) -> None:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")


def _coerce_datetime(df: pd.DataFrame, col: str) -> None:
    if col in df.columns:
        # Works for 'YYYY-MM-DD' and for timestamps like '2026-02-18 10:00:00+08:00' etc.
        df[col] = pd.to_datetime(df[col], errors="coerce")


def _to_float_or_none(v):
    if v is None or (isinstance(v, float) and pd.isna(v)) or pd.isna(v):
        return None
    try:
        return float(v)
    except Exception:
        return None


def line_chart(rows, columns, x_col, y_cols, title, dual_axis=False):
    if not rows:
        return "<p>No data.</p>"

    df = pd.DataFrame(rows, columns=columns)
    x_series = df[x_col] if x_col in df.columns else pd.Series([])
    x = ["" if pd.isna(v) else str(v) for v in x_series.tolist()]

    status_colors = {
        "s2xx": "#03e200",
        "s3xx": "#fce839",
        "s4xx": "#ffb03d",
        "s5xx": "#ff2a07",
    }
    dual_axis_colors = ["#1f77b4", "#ff7f0e"]

    present_y_cols = [yc for yc in y_cols if yc in df.columns]
    use_dual = dual_axis and len(present_y_cols) == 2

    series = []
    for idx, yc in enumerate(present_y_cols):
        y = [_to_float_or_none(v) for v in df[yc].tolist()]
        color = status_colors.get(yc)
        if not color and use_dual:
            color = dual_axis_colors[idx]
        s = {
            "name": STATUS_CODE_LABELS.get(yc, yc),
            "type": "line",
            "data": y,
            "showSymbol": False,
            "connectNulls": False,
            "smooth": False,
        }
        if color:
            s["itemStyle"] = {"color": color}
            s["lineStyle"] = {"color": color}
        if use_dual and idx == 1:
            s["yAxisIndex"] = 1
        series.append(s)

    if use_dual:
        y_axis = [
            {"type": "value"},
            {"type": "value", "position": "right", "splitLine": {"show": False}},
        ]
        grid_right = 40
    else:
        y_axis = {"type": "value"}
        grid_right = 20

    option = {
        "title": {"text": title, "left": "center", "textStyle": {"fontSize": 14, "fontWeight": "normal"}},
        "tooltip": {"trigger": "axis"},
        "legend": {"top": 28, "type": "scroll"},
        "grid": {"left": 10, "right": grid_right, "top": 70, "bottom": 30, "containLabel": True},
        "xAxis": {"type": "category", "data": x, "boundaryGap": False},
        "yAxis": y_axis,
        "series": series,
    }
    return _echart_html(option)


# Tooltip formatter that reads the pre-formatted "fmt" string off each data point.
# Works for both axis-trigger (params is array) and item-trigger (params is single).
_BYTES_TOOLTIP_FN = (
    "function(p){var a=Array.isArray(p)?p[0]:p;"
    "var lbl=a.axisValueLabel||a.name||'';"
    "var fmt=(a.data&&a.data.fmt)||a.value;"
    "return lbl+'<br/>'+fmt;}"
)


def _bytes_data(values):
    """Build ECharts data array with pre-formatted byte strings on each point."""
    out = []
    for v in values:
        fv = _to_float_or_none(v)
        out.append({"value": fv, "fmt": fmt_bytes(fv) if fv is not None else ""})
    return out


def bytes_line_chart(rows, columns, x_col, y_col, title):
    """Line chart for a single byte-valued series; hover shows KB/MB/GB."""
    if not rows:
        return "<p>No data.</p>"
    df = pd.DataFrame(rows, columns=columns)
    x = ["" if pd.isna(v) else str(v) for v in df[x_col].tolist()]
    data = _bytes_data(df[y_col].tolist())
    option = {
        "title": {"text": title, "left": "center", "textStyle": {"fontSize": 14, "fontWeight": "normal"}},
        "tooltip": {"trigger": "axis", "formatter": "__BYTES_FMT__"},
        "grid": {"left": 10, "right": 20, "top": 50, "bottom": 30, "containLabel": True},
        "xAxis": {"type": "category", "data": x, "boundaryGap": False},
        "yAxis": {"type": "value"},
        "series": [{
            "name": y_col,
            "type": "line",
            "data": data,
            "showSymbol": False,
            "connectNulls": False,
        }],
    }
    return _echart_html(option, fns={"__BYTES_FMT__": _BYTES_TOOLTIP_FN})


def bytes_bar_chart(rows, columns, x_col, y_col, title):
    """Bar chart for a single byte-valued series; hover shows KB/MB/GB."""
    if not rows:
        return "<p>No data.</p>"
    df = pd.DataFrame(rows, columns=columns)
    x = ["" if v is None or pd.isna(v) else str(v) for v in df[x_col].tolist()]
    data = _bytes_data(df[y_col].tolist())
    option = {
        "title": {"text": title, "left": "center", "textStyle": {"fontSize": 14, "fontWeight": "normal"}},
        "tooltip": {"trigger": "axis", "formatter": "__BYTES_FMT__"},
        "grid": {"left": 10, "right": 20, "top": 50, "bottom": 30, "containLabel": True},
        "xAxis": {"type": "category", "data": x},
        "yAxis": {"type": "value"},
        "series": [{
            "name": y_col,
            "type": "bar",
            "data": data,
        }],
    }
    return _echart_html(option, fns={"__BYTES_FMT__": _BYTES_TOOLTIP_FN})


def bar_chart(rows, columns, x_col, y_col=None, title="", y_cols=None, barmode="group"):
    """Bar chart. Pass y_cols (list) for grouped/stacked multi-series; y_col for single series."""
    if not rows:
        return "<p>No data.</p>"

    df = pd.DataFrame(rows, columns=columns)
    x_series = df[x_col] if x_col in df.columns else pd.Series([])
    x = ["" if v is None or pd.isna(v) else str(v) for v in x_series.tolist()]

    series_cols = y_cols if y_cols else [y_col]
    stack_key = "total" if barmode == "stack" else None

    series = []
    for yc in series_cols:
        if yc not in df.columns:
            continue
        y = [_to_float_or_none(v) for v in df[yc].tolist()]
        s = {
            "name": STATUS_CODE_LABELS.get(yc, yc),
            "type": "bar",
            "data": y,
        }
        if stack_key:
            s["stack"] = stack_key
        series.append(s)

    option = {
        "title": {"text": title, "left": "center", "textStyle": {"fontSize": 14, "fontWeight": "normal"}},
        "tooltip": {"trigger": "axis", "axisPointer": {"type": "shadow"}},
        "legend": {"top": 28, "type": "scroll"},
        "grid": {"left": 10, "right": 20, "top": 70, "bottom": 30, "containLabel": True},
        "xAxis": {"type": "category", "data": x},
        "yAxis": {"type": "value"},
        "series": series,
    }
    return _echart_html(option)


def heatmap_chart(rows, columns, x_col, y_col, z_col, title):
    """Heatmap where x_col=columns, y_col=rows, z_col=values (e.g. locale × url_group)."""
    if not rows:
        return "<p>No data.</p>"

    df = pd.DataFrame(rows, columns=columns)
    pivot = df.pivot_table(index=y_col, columns=x_col, values=z_col, aggfunc="sum", fill_value=0)
    x_labels = [str(c) for c in pivot.columns]
    y_labels = [str(r) for r in pivot.index]

    data = []
    z_max = 0.0
    for yi, row in enumerate(pivot.values):
        for xi, v in enumerate(row):
            fv = 0.0 if (v is None or (isinstance(v, float) and pd.isna(v))) else float(v)
            data.append([xi, yi, fv])
            if fv > z_max:
                z_max = fv

    height = min(max(300, 30 * len(y_labels) + 80), 1800)
    option = {
        "title": {"text": title, "left": "center", "textStyle": {"fontSize": 14, "fontWeight": "normal"}},
        "tooltip": {"position": "top"},
        "grid": {"left": 10, "right": 20, "top": 50, "bottom": 60, "containLabel": True},
        "xAxis": {"type": "category", "data": x_labels, "splitArea": {"show": True}, "axisLabel": {"rotate": 30}},
        "yAxis": {"type": "category", "data": y_labels, "splitArea": {"show": True}},
        "visualMap": {
            "min": 0,
            "max": z_max if z_max > 0 else 1,
            "calculable": True,
            "orient": "horizontal",
            "left": "center",
            "bottom": 0,
            "inRange": {"color": ["#f7fbff", "#deebf7", "#9ecae1", "#4292c6", "#08519c", "#08306b"]},
        },
        "series": [{
            "name": z_col,
            "type": "heatmap",
            "data": data,
            "emphasis": {"itemStyle": {"shadowBlur": 10, "shadowColor": "rgba(0,0,0,0.3)"}},
        }],
    }
    return _echart_html(option, height=height)


def sql_escape_string(s: str) -> str:
    return s.replace("'", "''")


def sql_escape_like(s: str) -> str:
    # Escape LIKE/ILIKE wildcards so user input matches literally. Pair with
    # `ESCAPE '\'` on the SQL side. Order matters: backslash first.
    return s.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_").replace("'", "''")


def distinct_values(table: str, column: str, date_from: Optional[str], date_to: Optional[str]) -> List[str]:
    paths = list_partitions(table, date_from, date_to)
    if not paths:
        return []
    sql = f"""
    SELECT DISTINCT {column} AS value
    FROM t
    WHERE {column} IS NOT NULL
    ORDER BY value;
    """
    _, rows = run_query(paths, sql)
    return [r[0] for r in rows if r and r[0] is not None]


def distinct_parsed_values(column: str, date_from: Optional[str], date_to: Optional[str]) -> List[str]:
    """Return sorted distinct values for a column in parsed parquet files."""
    paths = list_parsed_partitions(date_from, date_to)
    if not paths:
        return []
    # `country` can be JSON-typed in old partitions (all-null days) and VARCHAR
    # in newer ones. union_by_name promotes the union to JSON; ORDER BY then
    # tries to JSON-parse values like "US" and fails. Cast to VARCHAR for it.
    select_expr = f"CAST({column} AS VARCHAR)" if column == "country" else column
    sql = f"""
    SELECT DISTINCT {select_expr} AS value
    FROM t
    WHERE {column} IS NOT NULL AND CAST({column} AS VARCHAR) != ''
    ORDER BY value;
    """
    _, rows = run_query(paths, sql)
    return [str(r[0]) for r in rows if r and r[0] is not None]


def no_data_notice() -> str:
    return "<p class='no-data'>No data found for the selected date range.</p>"


def viewer_preset_banner(preset: str, date_from: Optional[str], date_to: Optional[str],
                          description: str) -> str:
    """Inline banner pointing readers to the equivalent /logs?preset= view."""
    qs = f"preset={preset}"
    if date_from:
        qs += f"&from={date_from}"
    if date_to:
        qs += f"&to={date_to}"
    return (
        "<div style='background:#eff6ff;border:1px solid #bfdbfe;border-radius:6px;"
        "padding:10px 14px;margin:8px 0 16px 0;font-size:13px;color:#1e3a8a;'>"
        f"<strong>New:</strong> {description} "
        f"<a href='/logs?{qs}' style='color:#1d4ed8;font-weight:600;'>"
        "Open in the unified log viewer &rarr;</a></div>"
    )


def export_link(report: str, date_from, date_to, extra: str = "") -> str:
    return (
        f"<p><a href='/export?report={report}&from={date_from or ''}"
        f"&to={date_to or ''}{extra}'>Export CSV</a></p>"
    )


# ---------------------------------------------------------------------------
# Shared helpers for actionable insights
# ---------------------------------------------------------------------------

def _default_last_7(
    date_from: Optional[str],
    date_to: Optional[str],
    avail: List[str],
) -> Tuple[Optional[str], Optional[str]]:
    """If no range is supplied, default to the last 7 days of available data."""
    if date_from or date_to or not avail:
        return date_from, date_to
    end = datetime.strptime(avail[-1], "%Y-%m-%d")
    start = end - timedelta(days=6)
    earliest = datetime.strptime(avail[0], "%Y-%m-%d")
    if start < earliest:
        start = earliest
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")


def _compute_periods(
    date_from: Optional[str],
    date_to: Optional[str],
    avail: List[str],
) -> Tuple[str, str, str, str]:
    """Return (curr_from, curr_to, prev_from, prev_to) date strings.

    If a date range is given, the previous period is the same-length window
    immediately before.  If no range, use the latest 7 days of available data
    as current and the 7 days before that as previous.
    """
    if date_from and date_to:
        d_from = datetime.strptime(date_from, "%Y-%m-%d")
        d_to = datetime.strptime(date_to, "%Y-%m-%d")
        span = (d_to - d_from).days + 1
        prev_to = d_from - timedelta(days=1)
        prev_from = prev_to - timedelta(days=span - 1)
        return date_from, date_to, prev_from.strftime("%Y-%m-%d"), prev_to.strftime("%Y-%m-%d")

    if not avail:
        today = datetime.now().strftime("%Y-%m-%d")
        return today, today, today, today

    curr_to_dt = datetime.strptime(avail[-1], "%Y-%m-%d")
    curr_from_dt = curr_to_dt - timedelta(days=6)
    prev_to_dt = curr_from_dt - timedelta(days=1)
    prev_from_dt = prev_to_dt - timedelta(days=6)
    return (
        curr_from_dt.strftime("%Y-%m-%d"),
        curr_to_dt.strftime("%Y-%m-%d"),
        prev_from_dt.strftime("%Y-%m-%d"),
        prev_to_dt.strftime("%Y-%m-%d"),
    )


def _trend_arrow(current: float, previous: float, lower_is_better: bool = False) -> str:
    """Return an HTML snippet with a colored arrow and percent change."""
    if previous == 0:
        if current == 0:
            return "<span class='kpi-trend-flat'>&#8212; 0%</span>"
        return "<span class='kpi-trend-up'>&#9650; new</span>" if not lower_is_better else "<span class='kpi-trend-down-bad'>&#9650; new</span>"
    pct = (current - previous) / abs(previous) * 100
    if abs(pct) < 0.5:
        return f"<span class='kpi-trend-flat'>&#8212; {pct:+.0f}%</span>"
    if pct > 0:
        cls = "kpi-trend-up-bad" if lower_is_better else "kpi-trend-up-good"
        return f"<span class='{cls}'>&#9650; {pct:+.0f}%</span>"
    cls = "kpi-trend-down-good" if lower_is_better else "kpi-trend-down-bad"
    return f"<span class='{cls}'>&#9660; {pct:+.0f}%</span>"


def _fmt_number(n) -> str:
    """Format a number with commas."""
    try:
        return f"{int(n):,}"
    except (TypeError, ValueError):
        return str(n)


def _fmt_pct_ratio0(v):
    """Format a 0-1 ratio as a percentage with no decimals (e.g. 0.79 -> '79%')."""
    try:
        return f"{float(v) * 100:.0f}%"
    except (TypeError, ValueError):
        return v if v is not None else ""


def _fmt_pct_ratio2(v):
    """Format a 0-1 ratio as a percentage with 2 decimals (e.g. 0.0345 -> '3.45%')."""
    try:
        return f"{float(v) * 100:.2f}%"
    except (TypeError, ValueError):
        return v if v is not None else ""


def _fmt_1dp(v):
    """Format a number with 1 decimal place (e.g. 12.345 -> '12.3')."""
    try:
        return f"{float(v):.1f}"
    except (TypeError, ValueError):
        return v if v is not None else ""


_GSC_COL_FORMATTERS = {
    "avg_ctr": _fmt_pct_ratio2,
    "avg_position": _fmt_1dp,
}


def _apply_col_formatters(rows, cols, formatters):
    """Return rows with per-column formatters applied. None values pass through."""
    idx = {c: i for i, c in enumerate(cols)}
    fmts = [(idx[c], fn) for c, fn in formatters.items() if c in idx]
    if not fmts:
        return rows
    out = []
    for r in rows:
        r = list(r)
        for i, fn in fmts:
            if r[i] is not None:
                r[i] = fn(r[i])
        out.append(tuple(r))
    return out


def kpi_card(label: str, current, previous, fmt_fn=None, lower_is_better: bool = False) -> str:
    """Render a single KPI metric card with trend arrow."""
    fn = fmt_fn or _fmt_number
    try:
        c = float(current or 0)
        p = float(previous or 0)
    except (TypeError, ValueError):
        c, p = 0.0, 0.0
    arrow = _trend_arrow(c, p, lower_is_better)
    return (
        f"<div class='kpi-card'>"
        f"<div class='kpi-label'>{label}</div>"
        f"<div class='kpi-value'>{fn(c)}</div>"
        f"<div class='kpi-change'>{arrow}</div>"
        f"</div>"
    )


def issue_card(severity: str, title: str, detail: str, link: str) -> str:
    """Render a finding card with severity badge, description, and drill-down link."""
    return (
        f"<div class='issue-item issue-{severity}'>"
        f"<span class='issue-severity'>{severity.upper()}</span>"
        f"<div class='issue-body'>"
        f"<div class='issue-title'>{title}</div>"
        f"<div class='issue-detail'>{detail}</div>"
        f"</div>"
        f"<a class='issue-link' href='{link}'>View details &#8594;</a>"
        f"</div>"
    )


_BOTS_YML_PATH = ROOT / "detectors" / "bots.yml"


@functools.lru_cache(maxsize=1)
def _load_bots_yml() -> dict:
    try:
        with open(_BOTS_YML_PATH, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except FileNotFoundError:
        return {}


def bot_family_detection_blurb(family: str) -> str:
    """Return a human-readable description of how *family* is classified.

    Empty string when the family is absent from detectors/bots.yml, so the
    caller hides the snippet entirely.
    """
    cfg = _load_bots_yml()
    for r in cfg.get("referer_rules", []) or []:
        if r.get("family") == family:
            return f"Detected by referer match: {r.get('pattern', '')}"
    for r in cfg.get("rules", []) or []:
        if r.get("family") == family:
            pat = r.get("pattern", "")
            if pat == "^-$":
                return "Detected when User-Agent header is missing"
            return f"Detected by User-Agent regex: {pat}"
    return ""


def recommendations_section(items: List[Tuple[str, str, str]]) -> str:
    """Render a 'Recommended Actions' box.

    *items*: list of (action_type, description, code_suggestion).
    """
    if not items:
        return ""
    rows_html = ""
    for i, (action, desc, suggestion) in enumerate(items, 1):
        rows_html += (
            f"<div class='rec-item'>"
            f"<span class='rec-num'>{i}</span>"
            f"<div class='rec-body'>"
            f"<span class='rec-action'>{action}</span>"
            f"<span class='rec-desc'>{desc}</span>"
            + (f"<code class='rec-code'>{suggestion}</code>" if suggestion else "")
            + f"</div></div>"
        )
    return (
        f"<div class='recommendations-box'>"
        f"<div class='rec-heading'>Recommended Actions</div>"
        f"{rows_html}"
        f"</div>"
    )


def add_trend_columns(
    cols: List[str],
    rows: list,
    prev_cols: List[str],
    prev_rows: list,
    key_col: str,
    metric_cols: List[str],
) -> Tuple[List[str], list]:
    """Merge current and previous period data, adding change columns with arrows.

    Returns new (cols, rows) with additional columns named ``{metric}_chg``
    inserted after each metric column.
    """
    # Build lookup: key -> {metric: value}
    prev_idx = {c: i for i, c in enumerate(prev_cols)}
    key_i = prev_idx.get(key_col)
    if key_i is None:
        return cols, rows
    prev_map: Dict[str, dict] = {}
    for pr in prev_rows:
        k = pr[key_i]
        prev_map[k] = {m: pr[prev_idx[m]] for m in metric_cols if m in prev_idx}

    curr_idx = {c: i for i, c in enumerate(cols)}
    key_ci = curr_idx.get(key_col)
    if key_ci is None:
        return cols, rows

    # Build new column list with _chg columns interspersed
    new_cols: List[str] = []
    for c in cols:
        new_cols.append(c)
        if c in metric_cols:
            new_cols.append(f"{c}_chg")

    new_rows = []
    for row in rows:
        k = row[key_ci]
        prev_vals = prev_map.get(k, {})
        new_row: list = []
        for c, v in zip(cols, row):
            new_row.append(v)
            if c in metric_cols:
                try:
                    curr_v = float(v or 0)
                    prev_v = float(prev_vals.get(c, 0) or 0)
                except (TypeError, ValueError):
                    curr_v, prev_v = 0.0, 0.0
                new_row.append(_trend_arrow(curr_v, prev_v))
        new_rows.append(tuple(new_row))

    return new_cols, new_rows


def list_tables() -> List[str]:
    """Return aggregate table names that have at least one parquet partition."""
    if not AGG.exists():
        return []
    return sorted(
        p.name for p in AGG.iterdir()
        if p.is_dir() and any(p.glob("date=*/part.parquet"))
    )


GUIDED_INSIGHTS: List[Dict] = [
    # Traffic
    {
        "id": "busiest-days",
        "category": "Traffic",
        "question": "What were my busiest days?",
        "table": "daily",
        "sql": "SELECT date, hits, hits_human, hits_bot FROM t ORDER BY hits DESC LIMIT 20",
        "chart": "bar", "x": "date", "y": "hits",
    },
    {
        "id": "bot-vs-human",
        "category": "Traffic",
        "question": "How much of my traffic is bots vs humans?",
        "table": "daily",
        "sql": "SELECT date, hits_human, hits_bot FROM t ORDER BY date",
        "chart": "line", "x": "date", "y": ["hits_human", "hits_bot"],
    },
    {
        "id": "traffic-trend",
        "category": "Traffic",
        "question": "How has my traffic trended over time?",
        "table": "daily",
        "sql": "SELECT date, hits FROM t ORDER BY date",
        "chart": "line", "x": "date", "y": "hits",
    },
    {
        "id": "error-rate-trend",
        "category": "Traffic",
        "question": "Is my error rate getting worse over time?",
        "table": "daily",
        "sql": "SELECT date, ROUND(100.0*(s4xx+s5xx)/hits,1) AS error_pct FROM t WHERE hits > 0 ORDER BY date",
        "chart": "line", "x": "date", "y": "error_pct",
    },
    # Content
    {
        "id": "top-pages",
        "category": "Content",
        "question": "What are my most popular pages?",
        "table": "human_urls_daily",
        "sql": "SELECT path, SUM(hits) AS hits FROM t GROUP BY path ORDER BY hits DESC LIMIT 20",
        "chart": "bar", "x": "path", "y": "hits",
    },
    {
        "id": "top-url-groups",
        "category": "Content",
        "question": "Which sections of my site get the most traffic?",
        "table": "group_daily",
        "sql": "SELECT url_group, SUM(hits_human) AS human_hits FROM t GROUP BY url_group ORDER BY human_hits DESC LIMIT 20",
        "chart": "bar", "x": "url_group", "y": "human_hits",
    },
    {
        "id": "heaviest-pages",
        "category": "Content",
        "question": "Which pages have the largest response sizes?",
        "table": "top_urls_daily",
        "sql": "SELECT path, ROUND(SUM(bytes_sent)*1.0/SUM(hits_total),0) AS avg_bytes FROM t WHERE hits_total > 0 GROUP BY path ORDER BY avg_bytes DESC LIMIT 20",
        "chart": "bar", "x": "path", "y": "avg_bytes",
    },
    # Errors
    {
        "id": "top-404",
        "category": "Errors",
        "question": "Which pages are returning 404 errors?",
        "table": "top_404_daily",
        "sql": "SELECT path, SUM(hits_404) AS errors FROM t GROUP BY path ORDER BY errors DESC LIMIT 20",
        "chart": "bar", "x": "path", "y": "errors",
    },
    {
        "id": "top-5xx",
        "category": "Errors",
        "question": "Which pages have server errors (5xx)?",
        "table": "top_5xx_daily",
        "sql": "SELECT path, SUM(hits_5xx) AS errors FROM t GROUP BY path ORDER BY errors DESC LIMIT 20",
        "chart": "bar", "x": "path", "y": "errors",
    },
    {
        "id": "top-redirects",
        "category": "Errors",
        "question": "Which pages are causing the most redirects?",
        "table": "top_urls_daily",
        "sql": "SELECT path, SUM(s3xx) AS redirects FROM t GROUP BY path ORDER BY redirects DESC LIMIT 20",
        "chart": "bar", "x": "path", "y": "redirects",
    },
    # Bots
    {
        "id": "top-bots",
        "category": "Bots",
        "question": "Which bots are crawling my site the most?",
        "table": "bot_daily",
        "sql": "SELECT bot_family, SUM(hits) AS hits FROM t GROUP BY bot_family ORDER BY hits DESC LIMIT 20",
        "chart": "bar", "x": "bot_family", "y": "hits",
    },
    {
        "id": "wasted-bot-crawl",
        "category": "Bots",
        "question": "Which bots are wasting their crawl budget?",
        "table": "wasted_crawl_daily",
        "sql": "SELECT bot_family, SUM(bot_hits) AS hits, SUM(error_bot_hits) AS errors, ROUND(AVG(waste_score),2) AS avg_waste FROM t GROUP BY bot_family ORDER BY avg_waste DESC LIMIT 20",
        "chart": "bar", "x": "bot_family", "y": "avg_waste",
    },
    {
        "id": "bot-trend",
        "category": "Bots",
        "question": "How has bot traffic changed over time?",
        "table": "daily",
        "sql": "SELECT date, hits_bot FROM t ORDER BY date",
        "chart": "line", "x": "date", "y": "hits_bot",
    },
    {
        "id": "bot-error-rates",
        "category": "Bots",
        "question": "Which bots generate the most errors?",
        "table": "bot_daily",
        "sql": "SELECT bot_family, SUM(s4xx) AS s4xx, SUM(s5xx) AS s5xx, SUM(hits) AS hits FROM t GROUP BY bot_family ORDER BY s4xx+s5xx DESC LIMIT 20",
        "chart": "bar", "x": "bot_family", "y": "s4xx",
    },
    # Geography
    {
        "id": "top-locales",
        "category": "Geography",
        "question": "Where does my traffic come from?",
        "table": "locale_daily",
        "sql": "SELECT locale, SUM(hits_human) AS human_hits FROM t GROUP BY locale ORDER BY human_hits DESC LIMIT 20",
        "chart": "bar", "x": "locale", "y": "human_hits",
    },
    {
        "id": "locale-error-rates",
        "category": "Geography",
        "question": "Which locales have the highest error rates?",
        "table": "locale_daily",
        "sql": "SELECT locale, SUM(s4xx+s5xx) AS errors, SUM(hits) AS hits, ROUND(100.0*SUM(s4xx+s5xx)/SUM(hits),1) AS error_pct FROM t WHERE hits > 0 GROUP BY locale ORDER BY error_pct DESC LIMIT 20",
        "chart": "bar", "x": "locale", "y": "error_pct",
    },
    # Campaigns
    {
        "id": "utm-sources",
        "category": "Campaigns",
        "question": "Which campaigns are driving the most traffic?",
        "table": "utm_sources_daily",
        "sql": "SELECT utm_source, SUM(hits_human) AS hits FROM t GROUP BY utm_source ORDER BY hits DESC LIMIT 20",
        "chart": "bar", "x": "utm_source", "y": "hits",
    },
    {
        "id": "utm-top-pages",
        "category": "Campaigns",
        "question": "Which pages do campaign visitors land on?",
        "table": "utm_source_urls_daily",
        "sql": "SELECT path, SUM(hits_human) AS hits FROM t GROUP BY path ORDER BY hits DESC LIMIT 20",
        "chart": "bar", "x": "path", "y": "hits",
    },
]

_INSIGHT_BY_ID: Dict[str, Dict] = {i["id"]: i for i in GUIDED_INSIGHTS}
_INSIGHT_CATEGORIES: List[str] = list(dict.fromkeys(i["category"] for i in GUIDED_INSIGHTS))


def select_html(name: str, options: List[str], current: Optional[str], label: str) -> str:
    opts = ["<option value=''>All</option>"]
    for opt in options:
        selected = "selected" if current == opt else ""
        opts.append(f"<option value='{opt}' {selected}>{opt}</option>")
    return f"<label>{label}: <select name='{name}'>{''.join(opts)}</select></label>"


def multi_select_html(name: str, options: List[str], selected: List[str], label: str) -> str:
    """Render a checkbox-dropdown multi-select widget."""
    sel_set = set(selected)
    # Button label
    if not sel_set:
        btn_text = "All"
    elif len(sel_set) == 1:
        btn_text = next(iter(sel_set))
    else:
        btn_text = f"{len(sel_set)} selected"
    hidden_val = ",".join(selected)
    # Build checkbox list
    cbs = ""
    for opt in options:
        chk = "checked" if opt in sel_set else ""
        cbs += f"<label><input type='checkbox' value='{opt}' {chk}> {opt}</label>"
    header = (
        "<div class='ms-header'>"
        "<input type='text' class='ms-search' placeholder='Type to filter...' autocomplete='off'>"
        "<div class='ms-meta'>"
        "<span class='ms-count'></span>"
        "<div class='ms-actions'>"
        "<button type='button' class='ms-select-visible'>Select visible</button>"
        "<button type='button' class='ms-clear-all'>Clear all</button>"
        "</div>"
        "</div>"
        "</div>"
    )
    return (
        f"<div class='ms-wrap'>"
        f"<span class='ms-label'>{label}</span>"
        f"<button type='button' class='ms-toggle'>{btn_text} &#9662;</button>"
        f"<div class='ms-dropdown'>{header}<div class='ms-list'>{cbs}</div></div>"
        f"<input type='hidden' name='{name}' value='{hidden_val}'>"
        f"</div>"
    )


def tab_bar(tabs: List[Tuple[str, str]]) -> str:
    """Render a tab bar. tabs = [(id, label), ...]."""
    btns = "".join(
        f"<button class='tab-btn' data-tab='{tid}'>{label}</button>"
        for tid, label in tabs
    )
    return f"<div class='tab-bar'>{btns}</div>"


def tab_panel(tid: str, content: str) -> str:
    """Wrap content in a tab panel div."""
    return f"<div class='tab-panel' id='tab-{tid}'>{content}</div>"


def page(title: str, body: str) -> HTMLResponse:

    charts_cdn = "<script src='https://cdn.jsdelivr.net/npm/echarts@5.5.1/dist/echarts.min.js'></script>"

    nav_items = [
        ("Log Viewer", "/logs"),
        ("Executive Summary", "/reports/summary"),
        ("Locales", "/reports/locales"),
        ("Search Console", "/reports/gsc"),
        ("Settings", "/settings"),
    ]
    nav_links = "\n".join(
        f"<a href='{url}' class='topnav-link' data-path='{url}'>{name}</a>"
        for name, url in nav_items
    )

    # Lift the first <form class="filter-bar">…</form> out of body into the rail.
    # Route handlers keep emitting the form inline; page() repositions it so the
    # data lands above the fold. If no filter-bar is present, the rail is omitted.
    rail_html = ""
    m = re.search(r"<form[^>]*class=['\"]filter-bar['\"][^>]*>.*?</form>", body, re.DOTALL)
    if m:
        rail_html = m.group(0)
        body = body[:m.start()] + body[m.end():]
    has_rail = bool(rail_html)
    layout_class = "with-rail" if has_rail else "no-rail"
    rail_aside = (
        f"""<aside class="filter-rail" id="filterRail" data-collapsed="0">
            <div class="rail-header">
                <span class="rail-title">Filters</span>
                <button type="button" class="rail-toggle" id="railToggle"
                        aria-label="Collapse filters" title="Collapse filters">‹</button>
            </div>
            <div class="rail-body">{rail_html}</div>
        </aside>"""
        if has_rail else ""
    )

    css = """<style>
*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

body {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    background: #f1f5f9;
    color: #334155;
    display: block;
    min-height: 100vh;
}

/* ── Top nav ── */
.topnav {
    position: sticky;
    top: 0;
    z-index: 100;
    display: flex;
    align-items: stretch;
    background: #1e293b;
    color: #cbd5e1;
    height: 48px;
    padding: 0 18px;
    border-bottom: 1px solid #0f172a;
}
.topnav-brand {
    font-size: 13px;
    font-weight: 700;
    color: #f8fafc;
    text-transform: uppercase;
    letter-spacing: 0.5px;
    display: flex;
    align-items: center;
    padding-right: 22px;
    margin-right: 8px;
    border-right: 1px solid #334155;
    white-space: nowrap;
}
.topnav-brand span {
    font-size: 10px;
    color: #64748b;
    font-weight: 400;
    text-transform: none;
    letter-spacing: 0;
    margin-left: 6px;
}
.topnav-links { display: flex; gap: 2px; align-items: stretch; }
.topnav-link {
    display: flex;
    align-items: center;
    padding: 0 14px;
    color: #94a3b8;
    text-decoration: none;
    font-size: 13px;
    border-bottom: 2px solid transparent;
    transition: background 0.12s, color 0.12s, border-color 0.12s;
}
.topnav-link:hover { color: #e2e8f0; background: #334155; }
.topnav-link.active { color: #60a5fa; border-bottom-color: #3b82f6; font-weight: 600; }

/* ── Layout: top nav + (optional) filter rail + content ── */
.layout { display: flex; min-height: calc(100vh - 48px); align-items: stretch; }
.layout.no-rail .filter-rail { display: none; }

/* ── Filter rail (left) ── */
.filter-rail {
    width: 280px;
    min-width: 280px;
    background: #fff;
    border-right: 1px solid #e2e8f0;
    display: flex;
    flex-direction: column;
    position: sticky;
    top: 48px;
    align-self: flex-start;
    max-height: calc(100vh - 48px);
    overflow-y: auto;
    transition: width 0.18s, min-width 0.18s;
    z-index: 40;
}
.filter-rail[data-collapsed="1"] { width: 36px; min-width: 36px; overflow: hidden; }
.filter-rail[data-collapsed="1"] .rail-body,
.filter-rail[data-collapsed="1"] .rail-title { display: none; }
.filter-rail[data-collapsed="1"] .rail-toggle { transform: rotate(180deg); }
.rail-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 10px 12px;
    border-bottom: 1px solid #e2e8f0;
    position: sticky;
    top: 0;
    background: #fff;
    z-index: 1;
}
.rail-title {
    font-size: 11px;
    font-weight: 700;
    color: #475569;
    text-transform: uppercase;
    letter-spacing: 0.5px;
}
.rail-toggle {
    width: 22px;
    height: 22px;
    border: 1px solid #e2e8f0;
    background: #f8fafc;
    border-radius: 4px;
    cursor: pointer;
    color: #64748b;
    font-size: 14px;
    line-height: 1;
    display: inline-flex;
    align-items: center;
    justify-content: center;
    transition: transform 0.18s, border-color 0.12s, color 0.12s;
}
.rail-toggle:hover { border-color: #94a3b8; color: #1e293b; }
.rail-body { padding: 8px 10px 14px; }

/* ── Content area (right of rail or full-width) ── */
.content-area {
    flex: 1;
    min-width: 0;
    display: flex;
    flex-direction: column;
}
.topbar {
    background: #fff;
    border-bottom: 1px solid #e2e8f0;
    padding: 14px 24px;
    position: sticky;
    top: 48px;
    z-index: 50;
    display: flex;
    align-items: center;
}
.topbar h1 { font-size: 18px; font-weight: 700; color: #1e293b; }
.content { padding: 20px 24px; flex: 1; overflow: visible; }

/* Hide raw <br> separators between cards */
.content > br { display: none; }

/* ── Cards ── */
.card {
    background: #fff;
    border-radius: 8px;
    border: 1px solid #e2e8f0;
    padding: 18px 20px;
    margin-bottom: 16px;
    box-shadow: 0 1px 2px rgba(0,0,0,0.04);
    overflow: hidden;
}

/* ── Section headings ── */
.content h2 {
    font-size: 14px;
    font-weight: 700;
    color: #475569;
    text-transform: uppercase;
    letter-spacing: 0.5px;
    margin: 20px 0 10px;
}
.content h2:first-child { margin-top: 0; }

/* ── Forms (filter bars) ── */
.content form.filter-bar {
    background: #fff;
    border: 1px solid #e2e8f0;
    border-radius: 8px;
    padding: 14px 18px;
    margin-bottom: 14px;
    display: flex;
    flex-wrap: wrap;
    gap: 12px;
    align-items: flex-end;
    box-shadow: 0 1px 2px rgba(0,0,0,0.04);
}
.content form.filter-bar label {
    display: flex;
    flex-direction: column;
    gap: 4px;
    font-size: 11px;
    font-weight: 600;
    color: #64748b;
    text-transform: uppercase;
    letter-spacing: 0.4px;
}
.content form.filter-bar label:has(input[type=checkbox]) {
    flex-direction: row;
    align-items: center;
    gap: 6px;
    text-transform: none;
    letter-spacing: 0;
    font-size: 13px;
    color: #374151;
    font-weight: 500;
    padding-bottom: 2px;
}
.content form.filter-bar input:not([type=checkbox]):not([type=hidden]),
.content form.filter-bar select,
.content form.filter-bar .ms-toggle {
    box-sizing: border-box;
    width: 140px;
    height: 34px;
    padding: 0 10px;
    border: 1px solid #cbd5e1;
    border-radius: 6px;
    font-size: 13px;
    color: #334155;
    background: #fff;
    outline: none;
    transition: border-color 0.12s, box-shadow 0.12s;
}
.content form.filter-bar input[type=date] { width: 150px; }
.content form.filter-bar input[type=text][name=search] { width: 240px; }
.content form.filter-bar select[name=per_page] { width: 90px; }
.content form.filter-bar input:focus, .content form.filter-bar select:focus {
    border-color: #3b82f6;
    box-shadow: 0 0 0 3px rgba(59,130,246,0.15);
}
.content form.filter-bar input[type=checkbox] { width: 15px; height: 15px; cursor: pointer; accent-color: #3b82f6; }
.content form.filter-bar button[type=submit] { height: 34px; padding: 0 18px; }
.content form.filter-bar .clear-filters-btn { height: 34px; padding: 0 14px; display: inline-flex; align-items: center; background: #fff; border: 1px solid #cbd5e1; border-radius: 6px; color: #64748b; text-decoration: none; font-size: 13px; font-weight: 500; cursor: pointer; transition: border-color 0.12s, color 0.12s; }
.content form.filter-bar .clear-filters-btn:hover { border-color: #94a3b8; color: #334155; }
.content form.filter-bar .date-presets { display: flex; flex-wrap: wrap; gap: 6px; align-items: center; }
.content form.filter-bar .date-preset-btn { height: 34px; padding: 0 12px; background: #fff; border: 1px solid #cbd5e1; border-radius: 6px; color: #475569; font-size: 13px; font-weight: 500; cursor: pointer; transition: border-color 0.12s, color 0.12s, background 0.12s; }
.content form.filter-bar .date-preset-btn:hover { border-color: #3b82f6; color: #1d4ed8; background: #eff6ff; }

/* ── Filter-group wrappers inside .filter-bar ── */
.content form.filter-bar { flex-direction: column; align-items: stretch; gap: 0; padding: 4px 18px; }
.content form.filter-bar .filter-group { display: flex; flex-direction: column; gap: 6px; padding: 10px 0; border-top: 1px dashed #e2e8f0; }
.content form.filter-bar .filter-group:first-of-type { border-top: none; }
.content form.filter-bar .filter-group-label { font-size: 10px; font-weight: 700; color: #94a3b8; text-transform: uppercase; letter-spacing: 0.6px; }
.content form.filter-bar .filter-group-fields { display: flex; flex-wrap: wrap; gap: 12px; align-items: flex-end; }
.content form.filter-bar .filter-group-view .filter-group-fields { align-items: center; }
.content form.filter-bar .filter-group-view .filter-group-actions { margin-left: auto; display: inline-flex; gap: 8px; align-items: center; }
.content form.filter-bar > .clear-filters-btn { display: none; }
.content form.filter-bar .search-hint { flex-basis: 100%; font-size: 11px; color: #64748b; margin-top: 2px; }

/* ── Multi-select checkbox dropdown ── */
.ms-wrap { position: relative; display: flex; flex-direction: column; gap: 4px; }
.ms-label { font-size: 11px; font-weight: 600; color: #64748b; text-transform: uppercase; letter-spacing: 0.4px; }
.ms-toggle { text-align: left; cursor: pointer; }
.ms-toggle:hover { border-color: #94a3b8; }
.ms-dropdown { display: none; position: absolute; top: 100%; left: 0; z-index: 50; background: #fff; border: 1px solid #cbd5e1; border-radius: 6px; box-shadow: 0 4px 12px rgba(0,0,0,0.1); min-width: 220px; margin-top: 2px; flex-direction: column; }
.ms-dropdown.open { display: flex; }
.ms-dropdown .ms-header { padding: 8px 10px; border-bottom: 1px solid #e2e8f0; display: flex; flex-direction: column; gap: 6px; background: #f8fafc; border-radius: 6px 6px 0 0; }
.content form.filter-bar .ms-dropdown input.ms-search { box-sizing: border-box; width: 100%; height: 28px; padding: 0 8px; border: 1px solid #cbd5e1; border-radius: 4px; font-size: 13px; color: #334155; background: #fff; outline: none; }
.content form.filter-bar .ms-dropdown input.ms-search:focus { border-color: #3b82f6; box-shadow: 0 0 0 2px rgba(59,130,246,0.15); }
.ms-dropdown .ms-meta { display: flex; align-items: center; justify-content: space-between; gap: 8px; }
.ms-dropdown .ms-count { font-size: 11px; color: #64748b; font-weight: 500; }
.ms-dropdown .ms-actions { display: flex; gap: 6px; }
.content form.filter-bar .ms-dropdown .ms-actions button { background: none; border: none; padding: 2px 4px; height: auto; font-size: 11px; color: #3b82f6; cursor: pointer; text-transform: none; letter-spacing: 0; }
.content form.filter-bar .ms-dropdown .ms-actions button:hover { text-decoration: underline; background: none; }
.ms-dropdown .ms-list { max-height: 220px; overflow-y: auto; padding: 6px 0; }
.content form.filter-bar .ms-dropdown label { display: flex; flex-direction: row; align-items: center; gap: 6px; padding: 4px 12px; font-size: 13px; cursor: pointer; text-transform: none; font-weight: 400; color: #334155; letter-spacing: 0; }
.content form.filter-bar .ms-dropdown label:hover { background: #f1f5f9; }
.content form.filter-bar .ms-dropdown label.ms-hidden { display: none !important; }

/* ── Rail-flavored filter-bar (when lifted into .filter-rail) ── */
.filter-rail form.filter-bar {
    background: transparent;
    border: none;
    box-shadow: none;
    padding: 0;
    margin: 0;
    gap: 0;
    flex-direction: column;
    align-items: stretch;
}
.filter-rail form.filter-bar .filter-group {
    display: block;
    border: none;
    border-bottom: 1px solid #f1f5f9;
    padding: 2px 0;
    margin: 0;
}
.filter-rail form.filter-bar .filter-group:last-of-type { border-bottom: none; }
.filter-rail form.filter-bar summary.filter-group-label {
    list-style: none;
    cursor: pointer;
    padding: 8px 6px;
    display: flex;
    align-items: center;
    gap: 6px;
    font-size: 11px;
    font-weight: 700;
    color: #475569;
    text-transform: uppercase;
    letter-spacing: 0.5px;
    border-radius: 4px;
}
.filter-rail form.filter-bar summary.filter-group-label::-webkit-details-marker { display: none; }
.filter-rail form.filter-bar summary.filter-group-label::before {
    content: "▸";
    font-size: 10px;
    color: #94a3b8;
    transition: transform 0.12s;
    display: inline-block;
}
.filter-rail form.filter-bar details[open] > summary.filter-group-label::before { transform: rotate(90deg); }
.filter-rail form.filter-bar summary.filter-group-label:hover { background: #f8fafc; color: #1e293b; }
.filter-rail form.filter-bar .filter-group-fields {
    display: flex;
    flex-direction: column;
    align-items: stretch;
    gap: 8px;
    padding: 4px 6px 10px;
}
.filter-rail form.filter-bar label { width: 100%; }
.filter-rail form.filter-bar input:not([type=checkbox]):not([type=hidden]),
.filter-rail form.filter-bar select,
.filter-rail form.filter-bar .ms-toggle { width: 100%; }
.filter-rail form.filter-bar input[type=date],
.filter-rail form.filter-bar input[type=text][name=search],
.filter-rail form.filter-bar select[name=per_page] { width: 100%; }
.filter-rail form.filter-bar .date-presets { display: grid; grid-template-columns: 1fr 1fr; gap: 6px; }
.filter-rail form.filter-bar .date-preset-btn { padding: 0 8px; }
.filter-rail form.filter-bar .search-hint { font-size: 11px; color: #64748b; margin-top: 2px; }
/* Sticky view-group with full-width Apply */
.filter-rail form.filter-bar .filter-group-view {
    position: sticky;
    bottom: 0;
    background: #fff;
    border-top: 1px solid #e2e8f0;
    border-bottom: none;
    margin: 0 -10px;
    padding: 6px 10px 10px;
    z-index: 1;
}
.filter-rail form.filter-bar .filter-group-view .filter-group-actions {
    display: flex;
    gap: 8px;
    align-items: center;
    margin-left: 0;
    margin-top: 8px;
}
.filter-rail form.filter-bar .filter-group-view button[type=submit] { flex: 1; width: 100%; }
.filter-rail form.filter-bar .filter-group-view .clear-filters-btn { flex: 0 0 auto; }
/* Auto-injected "Clear filters" link sits at form bottom in rail; show + full-width */
.filter-rail form.filter-bar > .clear-filters-btn {
    display: block;
    width: 100%;
    text-align: center;
    margin-top: 10px;
    height: 32px;
    line-height: 32px;
    padding: 0;
}
/* Multi-select dropdowns inside the rail span the rail width */
.filter-rail form.filter-bar .ms-dropdown {
    left: 0;
    right: 0;
    min-width: 0;
}
.filter-rail form.filter-bar .ms-dropdown input.ms-search {
    box-sizing: border-box; width: 100%; height: 28px; padding: 0 8px;
    border: 1px solid #cbd5e1; border-radius: 4px; font-size: 13px;
    color: #334155; background: #fff; outline: none;
}
.filter-rail form.filter-bar .ms-dropdown input.ms-search:focus {
    border-color: #3b82f6; box-shadow: 0 0 0 2px rgba(59,130,246,0.15);
}
.filter-rail form.filter-bar .ms-dropdown .ms-actions button {
    background: none; border: none; padding: 2px 4px; height: auto;
    font-size: 11px; color: #3b82f6; cursor: pointer;
    text-transform: none; letter-spacing: 0;
}
.filter-rail form.filter-bar .ms-dropdown .ms-actions button:hover { text-decoration: underline; background: none; }
.filter-rail form.filter-bar .ms-dropdown label {
    display: flex; flex-direction: row; align-items: center; gap: 6px;
    padding: 4px 12px; font-size: 13px; cursor: pointer; text-transform: none;
    font-weight: 400; color: #334155; letter-spacing: 0; width: auto;
}
.filter-rail form.filter-bar .ms-dropdown label:hover { background: #f1f5f9; }
.filter-rail form.filter-bar .ms-dropdown label.ms-hidden { display: none !important; }

/* ── Buttons ── */
button[type=submit] {
    padding: 8px 18px;
    background: #3b82f6;
    color: #fff;
    border: none;
    border-radius: 6px;
    font-size: 13px;
    font-weight: 600;
    cursor: pointer;
    transition: background 0.12s;
    white-space: nowrap;
}
button[type=submit]:hover { background: #2563eb; }

/* ── Export links ── */
a[href^='/export'] {
    display: inline-flex;
    align-items: center;
    gap: 4px;
    padding: 5px 12px;
    background: #f8fafc;
    color: #475569;
    border: 1px solid #e2e8f0;
    border-radius: 6px;
    font-size: 12px;
    font-weight: 500;
    text-decoration: none;
    transition: background 0.12s, color 0.12s;
    margin-bottom: 14px;
    margin-right: 6px;
}
a[href^='/export']:hover { background: #e2e8f0; color: #1e293b; }
.content p:has(a[href^='/export']) { margin-bottom: 0; }

/* ── Tables ── */
.table-wrapper { overflow-x: auto; }
.table-wrapper table.sortable thead th {
    background: #f8fafc;
    box-shadow: 0 1px 0 #e2e8f0;
}
table.sortable {
    width: 100%;
    border-collapse: collapse;
    font-size: 13px;
}
table.sortable thead tr { background: #f8fafc; }
table.sortable th {
    padding: 9px 12px;
    text-align: left;
    font-size: 11px;
    font-weight: 700;
    color: #64748b;
    text-transform: uppercase;
    letter-spacing: 0.4px;
    border-bottom: 2px solid #e2e8f0;
    cursor: pointer;
    user-select: none;
    white-space: nowrap;
}
table.sortable th:hover { color: #1e293b; background: #f1f5f9; }
table.sortable th.sorted-asc::after  { content: " ▲"; color: #3b82f6; }
table.sortable th.sorted-desc::after { content: " ▼"; color: #3b82f6; }
table.sortable td {
    padding: 8px 12px;
    border-bottom: 1px solid #f1f5f9;
    color: #374151;
    max-width: 480px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
}
table.sortable tbody tr:hover { background: #f8fafc; }
table.sortable tbody tr:last-child td { border-bottom: none; }

/* ── Per-table pagination / sort controls ── */
.table-controls {
    display: flex; align-items: center; gap: 14px; flex-wrap: wrap;
    padding: 8px 12px; border-top: 1px solid #e2e8f0;
    font-size: 12px; color: #475569; background: #fafbfc;
    margin: 0 -20px -18px; padding-left: 20px; padding-right: 20px;
    border-radius: 0 0 8px 8px;
}
.table-controls[hidden] { display: none; }
.table-controls .tc-count { color: #64748b; }
.table-controls label { display: inline-flex; align-items: center; gap: 6px; font-weight: 500; color: #475569; }
.table-controls select,
.table-controls input[type=number] {
    padding: 3px 6px; border: 1px solid #cbd5e1; border-radius: 4px;
    font-size: 12px; color: #334155; background: #fff; outline: none;
}
.table-controls input[type=number] { width: 60px; }
.table-controls .tc-nav { display: inline-flex; gap: 4px; align-items: center; }
.table-controls button {
    padding: 3px 9px; border: 1px solid #cbd5e1; background: #fff;
    border-radius: 4px; cursor: pointer; font-size: 12px; color: #3b82f6;
}
.table-controls button:hover:not(:disabled) { background: #eff6ff; }
.table-controls button:disabled { color: #cbd5e1; cursor: default; }

/* ── No-data notice ── */
.no-data { color: #94a3b8; font-style: italic; padding: 8px 0; }

/* ── Report grid (index page) ── */
.report-grid {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
    gap: 10px;
}
.report-card {
    display: block;
    background: #fff;
    border: 1px solid #e2e8f0;
    border-radius: 8px;
    padding: 14px 16px;
    text-decoration: none;
    color: #1e293b;
    font-size: 13.5px;
    font-weight: 500;
    transition: border-color 0.12s, box-shadow 0.12s, color 0.12s;
    box-shadow: 0 1px 2px rgba(0,0,0,0.04);
}
.report-card:hover {
    border-color: #3b82f6;
    box-shadow: 0 0 0 2px rgba(59,130,246,0.15);
    color: #2563eb;
}
.report-card small { display: block; font-size: 11px; color: #94a3b8; font-weight: 400; margin-top: 3px; }
.index-tip {
    background: #eff6ff;
    border: 1px solid #bfdbfe;
    border-radius: 8px;
    padding: 12px 16px;
    font-size: 13px;
    color: #1e40af;
    margin-bottom: 16px;
}
.index-tip code { background: #dbeafe; padding: 1px 5px; border-radius: 4px; font-size: 12px; }

.insights-date-bar { display: flex; gap: 12px; align-items: flex-end; flex-wrap: wrap; margin-bottom: 20px; }
.insights-date-bar label { display: flex; flex-direction: column; font-size: 12px; font-weight: 600; color: #64748b; gap: 4px; }
.insights-date-bar input { padding: 5px 8px; border: 1px solid #cbd5e1; border-radius: 4px; font-size: 13px; }
.insights-date-bar button { padding: 6px 14px; background: #3b82f6; color: #fff; border: none; border-radius: 4px; font-size: 13px; cursor: pointer; }
.insights-date-bar button:hover { background: #2563eb; }
.insights-back { font-size: 13px; color: #3b82f6; text-decoration: none; margin-left: auto; align-self: center; }
.insights-back:hover { text-decoration: underline; }
.insights-section-title { font-size: 11px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.5px; color: #64748b; margin: 20px 0 8px; }
.insights-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(210px, 1fr)); gap: 10px; margin-bottom: 4px; }
.insight-card { background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 6px; padding: 12px 14px; font-size: 13px; color: #1e293b; text-decoration: none; display: block; line-height: 1.4; }
.insight-card:hover { background: #eff6ff; border-color: #93c5fd; color: #1d4ed8; }
.insight-result-title { font-size: 17px; font-weight: 700; color: #1e293b; margin-bottom: 16px; }

.query-meta { font-size: 12px; color: #64748b; margin-bottom: 10px; }
.query-error {
    background: #fef2f2;
    border: 1px solid #fecaca;
    border-radius: 8px;
    padding: 12px 16px;
    color: #991b1b;
    font-size: 13px;
    margin-bottom: 16px;
    white-space: pre-wrap;
    font-family: 'SFMono-Regular', Consolas, monospace;
}

/* ── KPI cards (Executive Summary) ── */
.kpi-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(170px, 1fr)); gap: 12px; margin-bottom: 20px; }
.kpi-card { background: #fff; border: 1px solid #e2e8f0; border-radius: 8px; padding: 16px 18px; box-shadow: 0 1px 2px rgba(0,0,0,0.04); }
.kpi-label { font-size: 11px; font-weight: 600; color: #64748b; text-transform: uppercase; letter-spacing: 0.4px; margin-bottom: 4px; }
.kpi-value { font-size: 24px; font-weight: 700; color: #1e293b; margin-bottom: 4px; }
.kpi-change { font-size: 12px; font-weight: 600; }
.kpi-trend-up-good { color: #16a34a; }
.kpi-trend-up-bad { color: #dc2626; }
.kpi-trend-down-good { color: #16a34a; }
.kpi-trend-down-bad { color: #dc2626; }
.kpi-trend-flat { color: #64748b; }

/* ── Issue cards (Executive Summary) ── */
.issue-list { display: flex; flex-direction: column; gap: 8px; margin-bottom: 20px; }
.issue-item { display: flex; align-items: flex-start; gap: 12px; background: #fff; border: 1px solid #e2e8f0; border-radius: 8px; padding: 12px 16px; border-left: 4px solid #94a3b8; }
.issue-high { border-left-color: #dc2626; }
.issue-medium { border-left-color: #f59e0b; }
.issue-low { border-left-color: #3b82f6; }
.issue-severity { font-size: 10px; font-weight: 700; padding: 2px 8px; border-radius: 4px; white-space: nowrap; flex-shrink: 0; margin-top: 2px; }
.issue-high .issue-severity { background: #fef2f2; color: #dc2626; }
.issue-medium .issue-severity { background: #fffbeb; color: #d97706; }
.issue-low .issue-severity { background: #eff6ff; color: #2563eb; }
.issue-body { flex: 1; min-width: 0; }
.issue-title { font-size: 13px; font-weight: 600; color: #1e293b; }
.issue-detail { font-size: 12px; color: #64748b; margin-top: 2px; }
.issue-link { font-size: 12px; color: #3b82f6; text-decoration: none; white-space: nowrap; flex-shrink: 0; align-self: center; }
.issue-link:hover { text-decoration: underline; }

/* ── Summary sections ── */
.summary-section { margin-bottom: 24px; }
.summary-section h2 { margin-bottom: 12px; }
.summary-period { font-size: 12px; color: #64748b; margin-bottom: 12px; }

/* ── Recommendations box ── */
.recommendations-box { background: #f0fdf4; border: 1px solid #bbf7d0; border-radius: 8px; padding: 16px 18px; margin-top: 16px; margin-bottom: 16px; }
.rec-heading { font-size: 13px; font-weight: 700; color: #166534; margin-bottom: 10px; text-transform: uppercase; letter-spacing: 0.3px; }
.rec-item { display: flex; gap: 10px; align-items: flex-start; margin-bottom: 8px; }
.rec-item:last-child { margin-bottom: 0; }
.rec-num { flex-shrink: 0; width: 22px; height: 22px; background: #16a34a; color: #fff; border-radius: 50%; font-size: 11px; font-weight: 700; display: flex; align-items: center; justify-content: center; margin-top: 1px; }
.rec-body { flex: 1; }
.rec-action { display: inline-block; font-size: 11px; font-weight: 700; color: #166534; background: #dcfce7; padding: 1px 6px; border-radius: 3px; margin-right: 6px; }
.rec-desc { font-size: 13px; color: #374151; }
.rec-code { display: block; margin-top: 4px; font-size: 12px; padding: 4px 8px; background: #ecfdf5; border: 1px solid #bbf7d0; border-radius: 4px; color: #166534; white-space: pre-wrap; word-break: break-all; }

/* ── Tab navigation ── */
.tab-bar {
    display: flex;
    gap: 0;
    border-bottom: 2px solid #e2e8f0;
    margin-bottom: 16px;
    overflow-x: auto;
}
.tab-btn {
    padding: 10px 18px;
    font-size: 13px;
    font-weight: 600;
    color: #64748b;
    background: transparent;
    border: none;
    border-bottom: 2px solid transparent;
    margin-bottom: -2px;
    cursor: pointer;
    white-space: nowrap;
    transition: color 0.12s, border-color 0.12s;
}
.tab-btn:hover { color: #1e293b; }
.tab-btn.active {
    color: #3b82f6;
    border-bottom-color: #3b82f6;
}
.tab-panel { display: none; }
.tab-panel.active { display: block; }

/* ── Empty state card ── */
.empty-state {
    background: #f8fafc;
    border: 1px dashed #cbd5e1;
    border-radius: 8px;
    padding: 40px 24px;
    text-align: center;
    color: #64748b;
    margin: 20px 0;
}
.empty-state h3 { font-size: 16px; font-weight: 700; color: #475569; margin-bottom: 8px; }
.empty-state p { font-size: 13px; margin-bottom: 12px; }
.empty-state a {
    display: inline-block;
    padding: 8px 18px;
    background: #3b82f6;
    color: #fff;
    border-radius: 6px;
    text-decoration: none;
    font-size: 13px;
    font-weight: 600;
}
.empty-state a:hover { background: #2563eb; }
</style>"""

    js = """<script>
(function() {
// Highlight the active top-nav link based on current path
(function() {
    var path = window.location.pathname;
    document.querySelectorAll('.topnav-link').forEach(function(a) {
        var p = a.getAttribute('data-path');
        if (path === p || path.startsWith(p + '/')) a.classList.add('active');
    });
})();

const BYTE_UNITS = { B: 1, KB: 1024, MB: 1048576, GB: 1073741824, TB: 1099511627776 };
function parseNumber(s) {
    const cleaned = s.replace(/[,\s]/g, "").replace(/%$/, "");
    if (!cleaned) return null;
    const byteMatch = cleaned.match(/^([\d.]+)(B|KB|MB|GB|TB)$/i);
    if (byteMatch) {
        const n = parseFloat(byteMatch[1]);
        const mult = BYTE_UNITS[byteMatch[2].toUpperCase()] ?? 1;
        return Number.isFinite(n) ? n * mult : null;
    }
    const n = Number(cleaned);
    return Number.isFinite(n) ? n : null;
}

const PER_PAGE_PRESETS = [25, 50, 100, 200];
const DEFAULT_PER_PAGE = 50;
const DATE_RE = /^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}(:\d{2})?)?$/;

function cellValue(tr, idx) {
    return (tr.cells[idx]?.innerText ?? "").trim();
}
function cmpValues(a, b) {
    const na = parseNumber(a), nb = parseNumber(b);
    if (na !== null && nb !== null) return na - nb;
    if (DATE_RE.test(a) && DATE_RE.test(b)) {
        const da = Date.parse(a), db = Date.parse(b);
        if (!Number.isNaN(da) && !Number.isNaN(db)) return da - db;
    }
    return a.localeCompare(b);
}

function enhanceTable(table) {
    if (table.dataset.enhanced === "1") return;
    table.dataset.enhanced = "1";

    const serverPaginated = table.classList.contains("server-paginated");
    const tbody = table.tBodies[0];
    if (!tbody) return;
    const headers = table.tHead ? Array.from(table.tHead.rows[0].cells) : [];

    if (serverPaginated) {
        // /logs: headers flip ?sort=&order= and reload. Reflect current
        // URL state in the header indicator; do not touch tbody/controls.
        const url = new URL(window.location);
        const curSort = url.searchParams.get("sort");
        const curOrder = (url.searchParams.get("order") || "desc").toLowerCase();
        headers.forEach(th => {
            const col = th.getAttribute("data-sort-col");
            if (col && col === curSort) {
                th.classList.add(curOrder === "asc" ? "sorted-asc" : "sorted-desc");
            }
            th.addEventListener("click", () => {
                if (!col) return;
                const u = new URL(window.location);
                const prevSort = u.searchParams.get("sort");
                const prevOrder = (u.searchParams.get("order") || "desc").toLowerCase();
                const nextOrder = (prevSort === col && prevOrder === "asc") ? "desc" : "asc";
                u.searchParams.set("sort", col);
                u.searchParams.set("order", nextOrder);
                u.searchParams.set("page", "1");
                window.location.assign(u.toString());
            });
        });
        return;
    }

    // Client-paginated path: cache full row set, re-render slice on sort/page change.
    const state = {
        rows: Array.from(tbody.rows),
        sortIdx: null,
        sortAsc: true,
        page: 1,
        perPage: DEFAULT_PER_PAGE,
    };

    headers.forEach((th, idx) => {
        th.addEventListener("click", () => {
            const asc = !(state.sortIdx === idx && state.sortAsc);
            state.sortIdx = idx;
            state.sortAsc = asc;
            state.rows.sort((ra, rb) => {
                const r = cmpValues(cellValue(ra, idx), cellValue(rb, idx));
                return asc ? r : -r;
            });
            headers.forEach(h => h.classList.remove("sorted-asc", "sorted-desc"));
            th.classList.add(asc ? "sorted-asc" : "sorted-desc");
            state.page = 1;
            render();
        });
    });

    const card = table.closest(".card");
    const controls = card ? card.querySelector("[data-table-controls]") : null;
    const total = state.rows.length;

    if (!controls || total <= PER_PAGE_PRESETS[0]) {
        // Small table: no controls, render once.
        render();
        return;
    }

    controls.hidden = false;
    const ppOptions = PER_PAGE_PRESETS.map(n => `<option value="${n}">${n}</option>`).join("");
    controls.innerHTML =
        `<span class="tc-count"></span>` +
        `<label>Per page <select class="tc-pp">${ppOptions}</select></label>` +
        `<span class="tc-nav">` +
            `<button type="button" data-nav="first" title="First">&laquo;</button>` +
            `<button type="button" data-nav="prev" title="Prev">&lsaquo;</button>` +
            `<label>Page <input type="number" class="tc-page" min="1" value="1"> of <span class="tc-total-pages"></span></label>` +
            `<button type="button" data-nav="next" title="Next">&rsaquo;</button>` +
            `<button type="button" data-nav="last" title="Last">&raquo;</button>` +
        `</span>`;

    const ppSel = controls.querySelector(".tc-pp");
    ppSel.value = String(state.perPage);
    ppSel.addEventListener("change", () => {
        const v = parseInt(ppSel.value, 10);
        state.perPage = Number.isFinite(v) && v > 0 ? v : DEFAULT_PER_PAGE;
        state.page = 1;
        render();
    });

    controls.querySelectorAll("[data-nav]").forEach(btn => {
        btn.addEventListener("click", () => {
            const tp = Math.max(1, Math.ceil(state.rows.length / state.perPage));
            const nav = btn.getAttribute("data-nav");
            if (nav === "first") state.page = 1;
            else if (nav === "prev") state.page = Math.max(1, state.page - 1);
            else if (nav === "next") state.page = Math.min(tp, state.page + 1);
            else if (nav === "last") state.page = tp;
            render();
        });
    });

    const pageInput = controls.querySelector(".tc-page");
    const commitPage = () => {
        const tp = Math.max(1, Math.ceil(state.rows.length / state.perPage));
        let v = parseInt(pageInput.value, 10);
        if (!Number.isFinite(v) || v < 1) v = 1;
        if (v > tp) v = tp;
        state.page = v;
        render();
    };
    pageInput.addEventListener("change", commitPage);
    pageInput.addEventListener("keydown", e => {
        if (e.key === "Enter") { e.preventDefault(); commitPage(); }
    });

    render();

    function render() {
        const total = state.rows.length;
        const tp = Math.max(1, Math.ceil(total / state.perPage));
        if (state.page > tp) state.page = tp;
        const start = (state.page - 1) * state.perPage;
        const end = Math.min(total, start + state.perPage);
        tbody.replaceChildren(...state.rows.slice(start, end));
        if (!controls || controls.hidden) return;
        controls.querySelector(".tc-count").textContent =
            `${total.toLocaleString()} rows · showing ${(start + 1).toLocaleString()}-${end.toLocaleString()}`;
        controls.querySelector(".tc-total-pages").textContent = tp.toLocaleString();
        pageInput.value = String(state.page);
        pageInput.max = String(tp);
        const firstBtn = controls.querySelector('[data-nav="first"]');
        const prevBtn = controls.querySelector('[data-nav="prev"]');
        const nextBtn = controls.querySelector('[data-nav="next"]');
        const lastBtn = controls.querySelector('[data-nav="last"]');
        firstBtn.disabled = prevBtn.disabled = state.page <= 1;
        nextBtn.disabled = lastBtn.disabled = state.page >= tp;
    }
}

function enhanceAllTables(root) {
    (root || document).querySelectorAll("table.sortable").forEach(enhanceTable);
}

// Resize every ECharts instance on the page when the window resizes.
// ECharts charts are fixed-size at init and don't auto-respond to layout changes.
function resizeAllECharts() {
    if (!window.echarts) return;
    document.querySelectorAll('[data-echart]').forEach(function(el) {
        var inst = echarts.getInstanceByDom(el);
        if (inst) inst.resize();
    });
}
window.addEventListener('resize', resizeAllECharts);

document.addEventListener("DOMContentLoaded", () => {
    enhanceAllTables();

    // Tab navigation
    (function() {
        var tabBtns = document.querySelectorAll('.tab-btn');
        if (!tabBtns.length) return;

        function executeScripts(container) {
            // innerHTML does NOT run <script> tags; replace each with a live script so
            // each chart's inline echarts.init(...) call runs after injection.
            container.querySelectorAll('script').forEach(function(oldScript) {
                var newScript = document.createElement('script');
                for (var i = 0; i < oldScript.attributes.length; i++) {
                    var attr = oldScript.attributes[i];
                    newScript.setAttribute(attr.name, attr.value);
                }
                newScript.text = oldScript.text;
                oldScript.parentNode.replaceChild(newScript, oldScript);
            });
        }

        function fragmentEndpointFor(tabId) {
            // Only /reports/bots uses lazy tab loading today. Other report pages
            // embed all panels eagerly, so no placeholder and no fetch.
            if (window.location.pathname === '/reports/bots') {
                return '/reports/bots/_tab/' + tabId;
            }
            return null;
        }

        function loadLazyPanel(panel) {
            var placeholder = panel.querySelector('[data-lazy-tab]');
            if (!placeholder) return Promise.resolve();
            var tabId = placeholder.getAttribute('data-lazy-tab');
            var endpoint = fragmentEndpointFor(tabId);
            if (!endpoint) return Promise.resolve();
            // Preserve current filter querystring; drop any existing tab= param.
            var params = new URLSearchParams(window.location.search);
            params.delete('tab');
            var qs = params.toString();
            var url = endpoint + (qs ? ('?' + qs) : '');
            placeholder.innerHTML = "<p>Loading…</p>";
            return fetch(url, { credentials: 'same-origin' })
                .then(function(r) { return r.text(); })
                .then(function(html) {
                    panel.innerHTML = html;
                    panel.removeAttribute('data-lazy-loaded-from');
                    panel.setAttribute('data-lazy-loaded', '1');
                    executeScripts(panel);
                    enhanceAllTables(panel);
                })
                .catch(function(err) {
                    panel.innerHTML = "<p style='color:#e74c3c;'>Failed to load: " + (err && err.message ? err.message : 'error') + "</p>";
                });
        }

        function activateTab(tabId) {
            document.querySelectorAll('.tab-btn').forEach(function(b) { b.classList.toggle('active', b.getAttribute('data-tab') === tabId); });
            document.querySelectorAll('.tab-panel').forEach(function(p) { p.classList.toggle('active', p.id === 'tab-' + tabId); });
            var url = new URL(window.location);
            url.searchParams.set('tab', tabId);
            window.history.replaceState(null, '', url.toString());
            var panel = document.getElementById('tab-' + tabId);
            if (panel && panel.querySelector('[data-lazy-tab]') && !panel.getAttribute('data-lazy-loaded')) {
                loadLazyPanel(panel);
            }
        }
        tabBtns.forEach(function(btn) {
            btn.addEventListener('click', function() { activateTab(btn.getAttribute('data-tab')); });
        });
        // Activate from URL param or first tab
        var params = new URLSearchParams(window.location.search);
        var activeTab = params.get('tab');
        if (!activeTab || !document.getElementById('tab-' + activeTab)) {
            activeTab = tabBtns[0].getAttribute('data-tab');
        }
        activateTab(activeTab);
    })();

    // Multi-select checkbox dropdown
    (function() {
        document.querySelectorAll('.ms-wrap').forEach(function(wrap) {
            var toggle = wrap.querySelector('.ms-toggle');
            var dropdown = wrap.querySelector('.ms-dropdown');
            var hidden = wrap.querySelector('input[type=hidden]');
            if (!toggle || !dropdown || !hidden) return;
            var search = dropdown.querySelector('.ms-search');
            var countEl = dropdown.querySelector('.ms-count');
            var selectVisibleBtn = dropdown.querySelector('.ms-select-visible');
            var clearAllBtn = dropdown.querySelector('.ms-clear-all');
            var list = dropdown.querySelector('.ms-list');
            var labels = list ? Array.from(list.querySelectorAll('label')) : [];
            var total = labels.length;

            function applyFilter(query) {
                var q = (query || '').trim().toLowerCase();
                var shown = 0;
                labels.forEach(function(lbl) {
                    var txt = lbl.textContent.trim().toLowerCase();
                    var match = !q || txt.indexOf(q) !== -1;
                    lbl.classList.toggle('ms-hidden', !match);
                    if (match) shown++;
                });
                if (countEl) countEl.textContent = q ? (shown + ' of ' + total) : String(total);
            }

            var filterTimer = null;
            function scheduleFilter() {
                if (filterTimer) clearTimeout(filterTimer);
                filterTimer = setTimeout(function() {
                    applyFilter(search ? search.value : '');
                }, 150);
            }

            toggle.addEventListener('click', function(e) {
                e.preventDefault();
                // Close other open dropdowns
                document.querySelectorAll('.ms-dropdown.open').forEach(function(d) {
                    if (d !== dropdown) d.classList.remove('open');
                });
                var willOpen = !dropdown.classList.contains('open');
                dropdown.classList.toggle('open');
                if (willOpen && search) {
                    search.value = '';
                    applyFilter('');
                    search.focus();
                }
            });

            function updateState() {
                var checked = dropdown.querySelectorAll('input[type=checkbox]:checked');
                var vals = Array.from(checked).map(function(cb) { return cb.value; });
                hidden.value = vals.join(',');
                if (vals.length === 0) toggle.textContent = 'All \u25BE';
                else if (vals.length === 1) toggle.textContent = vals[0] + ' \u25BE';
                else toggle.textContent = vals.length + ' selected \u25BE';
            }

            dropdown.querySelectorAll('input[type=checkbox]').forEach(function(cb) {
                cb.addEventListener('change', updateState);
            });

            if (search) {
                search.addEventListener('input', scheduleFilter);
                search.addEventListener('click', function(e) { e.stopPropagation(); });
            }
            if (selectVisibleBtn) {
                selectVisibleBtn.addEventListener('click', function(e) {
                    e.preventDefault();
                    e.stopPropagation();
                    labels.forEach(function(lbl) {
                        if (lbl.classList.contains('ms-hidden')) return;
                        var cb = lbl.querySelector('input[type=checkbox]');
                        if (cb && !cb.checked) cb.checked = true;
                    });
                    updateState();
                });
            }
            if (clearAllBtn) {
                clearAllBtn.addEventListener('click', function(e) {
                    e.preventDefault();
                    e.stopPropagation();
                    labels.forEach(function(lbl) {
                        var cb = lbl.querySelector('input[type=checkbox]');
                        if (cb && cb.checked) cb.checked = false;
                    });
                    updateState();
                });
            }

            applyFilter('');
        });

        // Close dropdowns when clicking outside
        document.addEventListener('click', function(e) {
            if (!e.target.closest('.ms-wrap')) {
                document.querySelectorAll('.ms-dropdown.open').forEach(function(d) {
                    d.classList.remove('open');
                });
            }
        });
    })();

    // Append "Clear filters" button to every filter bar — navigates to the
    // current page with no query string so every filter on the active report resets.
    (function() {
        document.querySelectorAll('form.filter-bar').forEach(function(form) {
            if (form.querySelector('.clear-filters-btn')) return;
            var a = document.createElement('a');
            a.className = 'clear-filters-btn';
            a.textContent = 'Clear filters';
            a.href = window.location.pathname;
            form.appendChild(a);
        });
    })();

    // Filter rail: collapse toggle (persisted) + per-group accordion state (persisted).
    (function() {
        var rail = document.getElementById('filterRail');
        var btn  = document.getElementById('railToggle');
        if (rail && btn) {
            if (localStorage.getItem('logdash.rail.collapsed') === '1') {
                rail.setAttribute('data-collapsed', '1');
            }
            btn.addEventListener('click', function() {
                var collapsed = rail.getAttribute('data-collapsed') === '1';
                rail.setAttribute('data-collapsed', collapsed ? '0' : '1');
                localStorage.setItem('logdash.rail.collapsed', collapsed ? '0' : '1');
                if (window.resizeAllECharts) setTimeout(resizeAllECharts, 220);
            });
        }
        document.querySelectorAll('.filter-rail details[data-group]').forEach(function(d) {
            var key = 'logdash.group.' + d.getAttribute('data-group');
            var saved = localStorage.getItem(key);
            if (saved === '1') d.open = true;
            else if (saved === '0') d.open = false;
            d.addEventListener('toggle', function() {
                localStorage.setItem(key, d.open ? '1' : '0');
            });
        });
    })();
});

// Date-range presets used by report filter bars and the log viewer form.
window.applyDatePreset = function(form, days) {
    const presets = form.querySelector('.date-presets');
    if (!presets) return;
    const minStr = presets.dataset.min;
    const maxStr = presets.dataset.max;
    if (!maxStr) return;
    const parse = function(s) {
        const parts = s.split('-').map(Number);
        return Date.UTC(parts[0], parts[1] - 1, parts[2]);
    };
    const fmt = function(t) {
        const d = new Date(t);
        const y = d.getUTCFullYear();
        const m = String(d.getUTCMonth() + 1).padStart(2, '0');
        const day = String(d.getUTCDate()).padStart(2, '0');
        return y + '-' + m + '-' + day;
    };
    const endT = parse(maxStr);
    let startT = endT - (days - 1) * 86400000;
    if (minStr) {
        const minT = parse(minStr);
        if (startT < minT) startT = minT;
    }
    form.elements['from'].value = fmt(startT);
    form.elements['to'].value = maxStr;
    if (typeof form.requestSubmit === 'function') {
        form.requestSubmit();
    } else {
        form.submit();
    }
};
})();
</script>"""

    html = f"""<!doctype html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>{title} — Log Dashboard</title>
    {charts_cdn}
    {css}
</head>
<body>
    <header class="topnav">
        <div class="topnav-brand">Log Dashboard<span>nginx analytics</span></div>
        <nav class="topnav-links">
            {nav_links}
        </nav>
    </header>
    <div class="layout {layout_class}">
        {rail_aside}
        <main class="content-area">
            <div class="topbar"><h1>{title}</h1></div>
            <div class="content">
                {body}
            </div>
        </main>
    </div>
    {js}
</body>
</html>"""
    return HTMLResponse(html)


def date_filters_html(date_from, date_to, available: List[str] = []):
    min_date = available[0] if available else ""
    max_date = available[-1] if available else ""
    available_json = json.dumps(available)
    presets_disabled = "" if max_date else " disabled"
    return f"""
    <form method="get" class="filter-bar" onsubmit="return validateDates(this, {available_json})">
    <label>From<input type="date" name="from" value="{date_from or ''}"
           min="{min_date}" max="{max_date}"></label>
    <label>To<input type="date" name="to" value="{date_to or ''}"
           min="{min_date}" max="{max_date}"></label>
    <div class="date-presets" data-min="{min_date}" data-max="{max_date}">
      <button type="button" class="date-preset-btn" onclick="applyDatePreset(this.form, 3)"{presets_disabled}>Last 3 days</button>
      <button type="button" class="date-preset-btn" onclick="applyDatePreset(this.form, 7)"{presets_disabled}>Last 7 days</button>
      <button type="button" class="date-preset-btn" onclick="applyDatePreset(this.form, 14)"{presets_disabled}>Last 14 days</button>
      <button type="button" class="date-preset-btn" onclick="applyDatePreset(this.form, 30)"{presets_disabled}>Last 30 days</button>
    </div>
    <button type="submit">Apply dates</button>
    </form>
    <script>
    function validateDates(form, available) {{
      const from = form.elements['from'].value;
      const to   = form.elements['to'].value;
      if (from && available.length && !available.includes(from)) {{
        alert('No data for ' + from + '. Available range: {min_date}\u2013{max_date}');
        return false;
      }}
      if (to && available.length && !available.includes(to)) {{
        alert('No data for ' + to + '. Available range: {min_date}\u2013{max_date}');
        return false;
      }}
      return true;
    }}
    </script>
    """


# ---------------------------------------------------------------------------
# Executive Summary — unified actionable overview
# ---------------------------------------------------------------------------

@app.get("/reports/summary", response_class=HTMLResponse)
def executive_summary(
    date_from: Optional[str] = Query(None, alias="from"),
    date_to: Optional[str] = Query(None, alias="to"),
):
    avail = available_dates()
    date_from, date_to = _default_last_7(date_from, date_to, avail)
    body = ""

    if not avail:
        return _lv2_report_shell(
            title="Executive Summary", nav_key="summary",
            date_from=date_from, date_to=date_to, avail=avail,
            body=no_data_notice(),
        )

    curr_from, curr_to, prev_from, prev_to = _compute_periods(date_from, date_to, avail)

    # ── KPI cards ──────────────────────────────────────────────────────
    kpi_sql = """
    SELECT
        COALESCE(SUM(hits), 0) AS hits,
        COALESCE(SUM(hits_human), 0) AS human,
        COALESCE(SUM(hits_bot), 0) AS bot,
        COALESCE(SUM(s4xx + s5xx), 0) AS errors,
        ROUND(100.0 * SUM(s4xx + s5xx) / NULLIF(SUM(hits), 0), 1) AS error_pct,
        COALESCE(SUM(bytes_sent), 0) AS bytes
    FROM t;
    """
    curr_paths = list_partitions("daily", curr_from, curr_to)
    prev_paths = list_partitions("daily", prev_from, prev_to)

    def _get_kpi(paths):
        if not paths:
            return (0, 0, 0, 0, 0.0, 0)
        _, rows = run_query(paths, kpi_sql)
        return rows[0] if rows else (0, 0, 0, 0, 0.0, 0)

    curr = _get_kpi(curr_paths)
    prev = _get_kpi(prev_paths)

    body += f"<div class='summary-period'>Comparing {curr_from} to {curr_to} vs previous period {prev_from} to {prev_to}</div>"
    body += "<div class='summary-section'>"
    body += "<h2>Key Metrics</h2>"
    body += "<div class='kpi-grid'>"
    body += kpi_card("Total Hits", curr[0], prev[0])
    body += kpi_card("Human Hits", curr[1], prev[1])
    body += kpi_card("Bot Hits", curr[2], prev[2], lower_is_better=True)
    body += kpi_card("Errors (4xx+5xx)", curr[3], prev[3], lower_is_better=True)
    body += kpi_card("Error Rate", curr[4], prev[4], fmt_fn=lambda v: f"{v:.1f}%", lower_is_better=True)
    body += kpi_card("Bytes Sent", curr[5], prev[5], fmt_fn=fmt_bytes)
    body += "</div></div>"

    # ── Top Issues ─────────────────────────────────────────────────────
    issues: List[str] = []

    # 1. Top 404s (human-facing)
    paths_4xx = list_partitions("top_4xx_daily", curr_from, curr_to)
    if paths_4xx:
        _, rows_404 = run_query(paths_4xx, """
            SELECT path, SUM(s404) AS errors,
                   SUM(hits_4xx - hits_4xx_bot) AS human_errors
            FROM t
            GROUP BY path
            HAVING SUM(s404) > 0
            ORDER BY human_errors DESC
            LIMIT 5;
        """)
        for r in rows_404:
            path, total, human = r[0], int(r[1] or 0), int(r[2] or 0)
            if human > 10:
                issues.append(issue_card(
                    "high" if human > 100 else "medium",
                    f"404 Not Found: {path}",
                    f"{human:,} human visitors affected ({total:,} total hits)",
                    f"/reports/status-codes?tab=client-errors&from={curr_from}&to={curr_to}",
                ))

    # 2. Top 5xx errors
    paths_5xx = list_partitions("top_5xx_daily", curr_from, curr_to)
    if paths_5xx:
        _, rows_5xx = run_query(paths_5xx, """
            SELECT path, SUM(hits_5xx) AS errors,
                   COUNT(DISTINCT date) AS days_affected
            FROM t
            GROUP BY path
            HAVING SUM(hits_5xx) > 0
            ORDER BY errors DESC
            LIMIT 5;
        """)
        for r in rows_5xx:
            path, errors, days = r[0], int(r[1] or 0), int(r[2] or 0)
            if errors > 5:
                sev = "high" if errors > 50 or days > 3 else "medium"
                issues.append(issue_card(
                    sev,
                    f"Server Error (5xx): {path}",
                    f"{errors:,} errors across {days} day{'s' if days != 1 else ''} — investigate server-side root cause",
                    f"/reports/status-codes?tab=server-errors&from={curr_from}&to={curr_to}",
                ))

    # 3. Wasteful bots
    paths_waste = list_partitions("wasted_crawl_daily", curr_from, curr_to)
    if paths_waste:
        _, rows_waste = run_query(paths_waste, """
            SELECT bot_family,
                   SUM(bot_hits) AS hits,
                   SUM(error_bot_hits) AS errors,
                   SUM(waste_score) AS waste
            FROM t
            GROUP BY bot_family
            ORDER BY waste DESC
            LIMIT 5;
        """)
        for r in rows_waste:
            family, hits, errors, waste = r[0], int(r[1] or 0), int(r[2] or 0), int(r[3] or 0)
            if waste > 50:
                err_pct = round(100.0 * errors / hits, 1) if hits else 0
                issues.append(issue_card(
                    "medium" if waste > 200 else "low",
                    f"Wasteful bot: {family}",
                    f"{hits:,} requests, {err_pct}% errors, waste score {waste:,}",
                    f"/reports/bots?tab=crawl-waste&from={curr_from}&to={curr_to}&bot={family}",
                ))

    # 4. Bot anomalies — new or spiking bots
    paths_bot_curr = list_partitions("bot_daily", curr_from, curr_to)
    paths_bot_prev = list_partitions("bot_daily", prev_from, prev_to)
    bot_sql = "SELECT bot_family, SUM(hits) AS hits FROM t GROUP BY bot_family;"
    if paths_bot_curr:
        _, curr_bots = run_query(paths_bot_curr, bot_sql)
        prev_bot_map: Dict[str, int] = {}
        if paths_bot_prev:
            _, prev_bots = run_query(paths_bot_prev, bot_sql)
            prev_bot_map = {r[0]: int(r[1] or 0) for r in prev_bots}
        for r in curr_bots:
            family, hits = r[0], int(r[1] or 0)
            prev_hits = prev_bot_map.get(family, 0)
            if prev_hits == 0 and hits > 50:
                issues.append(issue_card(
                    "medium",
                    f"New bot detected: {family}",
                    f"{hits:,} requests with no activity in previous period",
                    f"/reports/bots?from={curr_from}&to={curr_to}&bot={family}",
                ))
            elif prev_hits > 0 and hits > prev_hits * 3 and hits > 100:
                pct = round(100.0 * (hits - prev_hits) / prev_hits)
                issues.append(issue_card(
                    "low",
                    f"Bot traffic spike: {family}",
                    f"{hits:,} requests (+{pct}% vs previous period)",
                    f"/reports/bots?from={curr_from}&to={curr_to}&bot={family}",
                ))

    # 5. Error rate spikes (days with error_pct > 2x average)
    if curr_paths:
        _, daily_rows = run_query(curr_paths, """
            SELECT date, hits,
                   ROUND(100.0 * (s4xx + s5xx) / NULLIF(hits, 0), 1) AS error_pct
            FROM t
            WHERE hits > 0
            ORDER BY date;
        """)
        if daily_rows:
            pcts = [float(r[2] or 0) for r in daily_rows]
            avg_pct = sum(pcts) / len(pcts) if pcts else 0
            for r in daily_rows:
                day, hits, epct = r[0], int(r[1] or 0), float(r[2] or 0)
                if avg_pct > 0 and epct > avg_pct * 2 and epct > 5:
                    issues.append(issue_card(
                        "medium",
                        f"Error spike on {day}",
                        f"{epct:.1f}% error rate (avg: {avg_pct:.1f}%) across {hits:,} hits",
                        f"/reports/status-codes?tab=trends&from={curr_from}&to={curr_to}",
                    ))

    if issues:
        body += "<div class='summary-section'>"
        body += "<h2>Issues Requiring Attention</h2>"
        body += "<div class='issue-list'>"
        body += "".join(issues[:15])
        body += "</div></div>"
    else:
        body += "<div class='summary-section'><h2>Issues</h2><p class='no-data'>No issues found for this period — looking good!</p></div>"

    # ── Quick links to reports ─────────────────────────────────────────
    body += "<div class='summary-section'>"
    body += "<h2>Explore Reports</h2>"
    body += "<div class='report-grid'>"
    quick_links = [
        ("Status Codes", f"/reports/status-codes?from={curr_from}&to={curr_to}&tab=client-errors", "Fix broken pages"),
        ("Status Codes", f"/reports/status-codes?from={curr_from}&to={curr_to}&tab=server-errors", "Investigate server errors"),
        ("Bots", f"/reports/bots?from={curr_from}&to={curr_to}", "Review bot activity"),
        ("Bots", f"/reports/bots?from={curr_from}&to={curr_to}&tab=crawl-waste", "Reduce crawl waste"),
        ("Content", f"/reports/content?from={curr_from}&to={curr_to}&tab=human-paths", "Top pages for real users"),
        ("UTM Campaigns", f"/reports/utm?from={curr_from}&to={curr_to}", "Campaign performance"),
    ]
    for name, url, desc in quick_links:
        body += f"<a href='{url}' class='report-card'>{name}<small>{desc}</small></a>"
    body += "</div></div>"

    return _lv2_report_shell(
        title="Executive Summary", nav_key="summary",
        date_from=date_from, date_to=date_to, avail=avail,
        body=body,
    )


@app.get("/")
def index():
    return RedirectResponse(url="/logs", status_code=302)


@app.post("/internal/cache/clear")
def clear_report_cache():
    """Invalidate the report query cache. Call this after the parser publishes
    new aggregate partitions so users see fresh data without waiting for TTL."""
    _REPORT_CACHE.clear()
    return {"ok": True}


# ---------------------------------------------------------------------------
# Consolidated reports (Phase 4)
# ---------------------------------------------------------------------------


# ── /reports/traffic ──────────────────────────────────────────────────────

@app.get("/reports/traffic")
def traffic_report(
    date_from: Optional[str] = Query(None, alias="from"),
    date_to: Optional[str] = Query(None, alias="to"),
):
    return _resolve_log_preset("traffic", date_from=date_from, date_to=date_to)




# ── /reports/status-codes ─────────────────────────────────────────────────

@app.get("/reports/status-codes")
def status_codes_report(
    date_from: Optional[str] = Query(None, alias="from"),
    date_to: Optional[str] = Query(None, alias="to"),
    url_group: Optional[str] = None,
):
    return _resolve_log_preset("status-over-time", date_from=date_from, date_to=date_to,
                                extra_url_group=url_group)




# ── /reports/content ──────────────────────────────────────────────────────

@app.get("/reports/content")
def content_report(
    date_from: Optional[str] = Query(None, alias="from"),
    date_to: Optional[str] = Query(None, alias="to"),
    url_group: Optional[str] = None,
):
    return _resolve_log_preset("top-urls", date_from=date_from, date_to=date_to,
                                extra_url_group=url_group)




# ── /reports/locales ──────────────────────────────────────────────────────

@app.get("/reports/locales", response_class=HTMLResponse)
def locales(
    date_from: Optional[str] = Query(None, alias="from"),
    date_to: Optional[str] = Query(None, alias="to"),
    locale: Optional[str] = None,
    url_group: Optional[str] = None,
    content_only: bool = False,
    limit: str = "999",
):
    limit = _parse_int(limit, 999)
    locales_sel = _parse_csv_param(locale)
    url_groups = _parse_csv_param(url_group)
    locale_param = _fmt_filter_param(locales_sel)
    url_group_param = _fmt_filter_param(url_groups)
    locale_display = _fmt_filter_display(locales_sel)
    url_group_display = _fmt_filter_display(url_groups)
    avail = available_dates()
    date_from, date_to = _default_last_7(date_from, date_to, avail)
    body = ""

    # ── Locale Breakdown tab ──
    breakdown = ""
    paths = list_partitions("locale_daily", date_from, date_to)
    if not paths:
        breakdown = no_data_notice()
    else:
        sql_totals = """
        SELECT
          CASE WHEN locale IS NULL OR locale = 'Unknown' THEN 'no-locale' ELSE locale END AS locale,
          SUM(hits) AS hits, SUM(hits_bot) AS hits_bot, SUM(hits_human) AS hits_human,
          SUM(s4xx) AS s4xx, SUM(s5xx) AS s5xx,
          SUM(resource_hits) AS resource_hits, SUM(resource_hits_bot) AS resource_hits_bot
        FROM t GROUP BY 1 ORDER BY hits DESC;
        """
        cols1, rows1 = run_query(paths, sql_totals)

        curr_from, curr_to, prev_from, prev_to = _compute_periods(date_from, date_to, avail)
        prev_paths = list_partitions("locale_daily", prev_from, prev_to)
        if prev_paths:
            prev_cols1, prev_rows1 = run_query(prev_paths, sql_totals)
            cols1, rows1 = add_trend_columns(cols1, rows1, prev_cols1, prev_rows1,
                                             key_col="locale", metric_cols=["hits", "hits_human"])

        sql_non_resource = """
        SELECT
          CASE WHEN locale IS NULL OR locale = 'Unknown' THEN 'no-locale' ELSE locale END AS locale,
          (SUM(hits) - SUM(resource_hits)) AS hits_non_resource,
          (SUM(hits_bot) - SUM(resource_hits_bot)) AS hits_bot_non_resource,
          (SUM(hits_human) - (SUM(resource_hits) - SUM(resource_hits_bot))) AS hits_human_non_resource
        FROM t GROUP BY 1 ORDER BY hits_non_resource DESC;
        """
        cols2, rows2 = run_query(paths, sql_non_resource)

        breakdown += export_link("locales", date_from, date_to)
        chart_limit = 20
        breakdown += "<h2>All hits (includes Nuxt/static resources)</h2>"
        chart_cols1 = [c for c in cols1 if not c.endswith("_chg")]
        chart_rows1 = [tuple(v for c, v in zip(cols1, r) if not c.endswith("_chg")) for r in rows1]
        breakdown += bar_chart(chart_rows1[:chart_limit], chart_cols1, x_col="locale",
                               y_cols=["hits_human", "hits_bot"],
                               title=f"Top {chart_limit} locales — human vs bot", barmode="stack")
        breakdown += html_table(rows1, cols1)
        breakdown += "<h2>Non-resource hits (excludes Nuxt/static resources)</h2>"
        breakdown += bar_chart(rows2[:chart_limit], cols2, x_col="locale",
                               y_cols=["hits_human_non_resource", "hits_bot_non_resource"],
                               title=f"Top {chart_limit} locales (non-resource) — human vs bot", barmode="stack")
        breakdown += html_table(rows2, cols2)


    # ── Locale x Group Heatmap tab ──
    heatmap_content = ""
    paths_lg = list_partitions("locale_group_daily", date_from, date_to)
    if not paths_lg:
        heatmap_content = no_data_notice()
    else:
        lg_clauses = []
        for clause in (
            _in_clause("locale", locales_sel),
            _in_clause("url_group", url_groups),
        ):
            if clause:
                lg_clauses.append(clause)
        lg_where = ("WHERE " + " AND ".join(lg_clauses)) if lg_clauses else ""

        sql_lg = f"""
        SELECT locale, url_group, SUM(hits) AS hits, SUM(hits_bot) AS hits_bot,
               SUM(hits_human) AS hits_human, SUM(s4xx) AS s4xx, SUM(s5xx) AS s5xx
        FROM t {lg_where}
        GROUP BY locale, url_group ORDER BY hits DESC LIMIT {int(limit)};
        """
        cols_lg, rows_lg = run_query(paths_lg, sql_lg)

        locale_options = distinct_values("locale_daily", "locale", date_from, date_to)
        group_options = distinct_values("group_daily", "url_group", date_from, date_to)
        heatmap_content += f"""
        <form method="get" class="filter-bar">
        <input type="hidden" name="from" value="{date_from or ''}">
        <input type="hidden" name="to" value="{date_to or ''}">
        <input type="hidden" name="tab" value="heatmap">
        {multi_select_html("locale", locale_options, locales_sel, "Locale")}
        {multi_select_html("url_group", group_options, url_groups, "URL group")}
        <label>Limit: <input name="limit" value="{limit}" size="6"></label>
        <label style="margin-left:1em"><input type="checkbox" name="content_only" value="true"{"checked" if content_only else ""}> Content only (heatmap)</label>
        <button type="submit">Apply</button>
        </form>
        """
        heatmap_content += export_link("locale-groups", date_from, date_to,
                                       extra=f"&locale={locale_param}&url_group={url_group_param}&limit={limit}")

        url_group_idx = cols_lg.index("url_group") if "url_group" in cols_lg else None
        heatmap_rows = rows_lg
        if content_only and url_group_idx is not None:
            heatmap_rows = [r for r in rows_lg if r[url_group_idx] not in NON_CONTENT_GROUPS]
        title_suffix = ""
        if locales_sel and url_groups:
            title_suffix = f" — locale={locale_display}, url_group={url_group_display}"
        elif locales_sel:
            title_suffix = f" — locale={locale_display}"
        elif url_groups:
            title_suffix = f" — url_group={url_group_display}"
        title_hm = "Hits heatmap — locale × URL group" + title_suffix + (" (content only)" if content_only else "")
        heatmap_content += heatmap_chart(heatmap_rows, cols_lg, x_col="url_group", y_col="locale",
                                         z_col="hits", title=title_hm)
        heatmap_content += html_table(rows_lg, cols_lg, max_rows=min(int(limit), 500))

    tabs = tab_bar([("breakdown", "Locale Breakdown"), ("heatmap", "Locale × Group Heatmap")])
    body += tabs
    body += tab_panel("breakdown", breakdown)
    body += tab_panel("heatmap", heatmap_content)
    return _lv2_report_shell(
        title="Locales", nav_key="reports",
        date_from=date_from, date_to=date_to, avail=avail,
        body=body,
    )



# ── /reports/bots ─────────────────────────────────────────────────────────

_BOTS_VALID_TABS = {"bot-families", "ai-crawlers", "crawl-waste", "resource-waste"}














@app.get("/reports/bots")
def bots_report(
    date_from: Optional[str] = Query(None, alias="from"),
    date_to: Optional[str] = Query(None, alias="to"),
    url_group: Optional[str] = None,
    tab: Optional[str] = None,
):
    tab_to_preset = {
        "crawl-waste": "crawl-waste",
        "resource-waste": "crawl-waste",
        "categories": "bot-categories",
        "ai-crawlers": "bot-families",
        "families": "bot-families",
    }
    preset = tab_to_preset.get(tab or "", "bot-families")
    return _resolve_log_preset(preset, date_from=date_from, date_to=date_to,
                                extra_url_group=url_group)








# ── /reports/utm ──────────────────────────────────────────────────────────



@app.get("/reports/utm")
def utm_report(
    date_from: Optional[str] = Query(None, alias="from"),
    date_to: Optional[str] = Query(None, alias="to"),
    utm_source: Optional[str] = None,
):
    # When utm_source is given, jump straight to top-urls filtered by UTM instead
    # of the generic utm-sources breakdown — matches the old utm-urls view.
    if utm_source:
        params = {"mode": "group", "group_by": "path", "utm_source": utm_source}
        if date_from:
            params["from"] = date_from
        if date_to:
            params["to"] = date_to
        qs = "&".join(f"{k}={v}" for k, v in params.items())
        return RedirectResponse(url=f"/logs?{qs}", status_code=302)
    return _resolve_log_preset("utm-sources", date_from=date_from, date_to=date_to)




# ── /reports/gsc ─────────────────────────────────────────────────────────

@app.get("/reports/gsc", response_class=HTMLResponse)
def gsc_report(
    date_from: Optional[str] = Query(None, alias="from"),
    date_to: Optional[str] = Query(None, alias="to"),
    tab: Optional[str] = None,
    limit: int = 200,
):
    gsc_paths = list_partitions("gsc_daily", date_from, date_to)
    avail = available_dates()
    date_from, date_to = _default_last_7(date_from, date_to, avail)
    body = ""

    # Check if GSC is connected by looking for gsc_daily data
    if not gsc_paths:
        # Check if data/gsc has any parquet files at all
        gsc_data_dir = ROOT / "data" / "gsc"
        has_any_gsc = gsc_data_dir.exists() and any(gsc_data_dir.glob("date=*/performance.parquet"))
        if not has_any_gsc:
            body += (
                "<div class='empty-state'>"
                "<h3>Google Search Console</h3>"
                "<p>Connect Google Search Console to see crawl-to-search performance data.</p>"
                "<p>This report will show crawl efficiency analysis, including pages with high crawl frequency "
                "but zero impressions, pages with impressions but no recent crawl, and crawl frequency vs. position correlations.</p>"
                "<a href='/settings/gsc' style='display:inline-block;margin-top:12px;padding:8px 16px;"
                "background:#3b82f6;color:#fff;border-radius:6px;text-decoration:none;font-size:13px;'>Connect Search Console</a>"
                "</div>"
            )
        else:
            body += (
                "<div class='empty-state'>"
                "<h3>No aggregates for this date range</h3>"
                "<p>Raw GSC data exists for other dates, but the <code>gsc_daily</code> "
                "aggregate has not been built for the range you selected.</p>"
                "<p>Run <code>python scripts/backfill_gsc_daily.py</code> to fill gaps, "
                "or adjust the date filter above to a range that already has aggregates.</p>"
                "</div>"
            )
        return _lv2_report_shell(
            title="Search Console", nav_key="reports",
            date_from=date_from, date_to=date_to, avail=avail,
            body=body,
        )

    # ── Tab: Performance Overview ──────────────────────────────────────
    perf = ""

    # KPI cards
    curr_from, curr_to, prev_from, prev_to = _compute_periods(date_from, date_to, avail)
    curr_paths = list_partitions("gsc_daily", curr_from, curr_to)
    prev_paths = list_partitions("gsc_daily", prev_from, prev_to)

    kpi_sql = """
    SELECT
        SUM(clicks) AS total_clicks,
        SUM(impressions) AS total_impressions,
        AVG(CASE WHEN impressions > 0 THEN ctr ELSE NULL END) AS avg_ctr,
        AVG(CASE WHEN impressions > 0 THEN position ELSE NULL END) AS avg_position
    FROM t
    WHERE has_impressions
    """
    _, curr_kpi = run_query(curr_paths, kpi_sql)
    _, prev_kpi = run_query(prev_paths, kpi_sql)

    c = curr_kpi[0] if curr_kpi else (0, 0, 0, 0)
    p = prev_kpi[0] if prev_kpi else (0, 0, 0, 0)

    def fmt_pct(v):
        try:
            return f"{float(v)*100:.1f}%"
        except (TypeError, ValueError):
            return "0.0%"

    def _format_gsc_rows(rows, cols):
        return _apply_col_formatters(rows, cols, _GSC_COL_FORMATTERS)

    perf += "<div class='kpi-row'>"
    perf += kpi_card("Total Clicks", c[0], p[0])
    perf += kpi_card("Total Impressions", c[1], p[1])
    perf += kpi_card("Avg CTR", c[2], p[2], fmt_fn=fmt_pct)
    perf += kpi_card("Avg Position", c[3], p[3], fmt_fn=_fmt_1dp, lower_is_better=True)
    perf += "</div>"

    # Clicks & impressions over time
    trend_sql = """
    SELECT
        date,
        SUM(clicks) AS clicks,
        SUM(impressions) AS impressions,
        AVG(CASE WHEN impressions > 0 THEN position ELSE NULL END) AS avg_position
    FROM t
    WHERE has_impressions
    GROUP BY date
    ORDER BY date
    """
    cols_t, rows_t = run_query(gsc_paths, trend_sql)
    perf += line_chart(rows_t, cols_t, "date", ["clicks", "impressions"], "Clicks & Impressions Over Time", dual_axis=True)
    perf += line_chart(rows_t, cols_t, "date", ["avg_position"], "Average Position Over Time")

    # Top pages by clicks
    top_clicks_sql = f"""
    SELECT
        page,
        SUM(clicks) AS clicks,
        SUM(impressions) AS impressions,
        AVG(ctr) AS avg_ctr,
        AVG(position) AS avg_position,
        SUM(crawl_hits) AS crawl_hits
    FROM t
    WHERE has_impressions
    GROUP BY page
    ORDER BY clicks DESC
    LIMIT {int(limit)}
    """
    cols_tc, rows_tc = run_query(gsc_paths, top_clicks_sql)
    perf += (
        f"<p><a href='/export?report=gsc-top-clicks&from={date_from or ''}&to={date_to or ''}"
        f"&limit={int(limit)}'>Export CSV</a></p>"
    )
    perf += "<h2>Top Pages by Clicks</h2>"
    perf += html_table(_format_gsc_rows(rows_tc, cols_tc), cols_tc, max_rows=min(int(limit), 500))

    # Top queries by impressions (from raw GSC data, not the joined aggregate)
    gsc_data_dir = ROOT / "data" / "gsc"
    gsc_raw_paths = []
    if gsc_data_dir.exists():
        for p_dir in gsc_data_dir.glob("date=*/performance.parquet"):
            d = p_dir.parent.name.split("=", 1)[1]
            if date_from and d < date_from:
                continue
            if date_to and d > date_to:
                continue
            gsc_raw_paths.append(p_dir.as_posix())
    gsc_raw_paths.sort()

    if gsc_raw_paths:
        query_sql = f"""
        SELECT
            query,
            SUM(clicks) AS clicks,
            SUM(impressions) AS impressions,
            AVG(ctr) AS avg_ctr,
            AVG(position) AS avg_position
        FROM t
        WHERE query IS NOT NULL
        GROUP BY query
        ORDER BY impressions DESC
        LIMIT {int(limit)}
        """
        cols_q, rows_q = run_query(gsc_raw_paths, query_sql)
        perf += "<h2>Top Queries by Impressions</h2>"
        perf += html_table(_format_gsc_rows(rows_q, cols_q), cols_q, max_rows=min(int(limit), 500))

    # ── Tab: Crawl-to-Index Efficiency ─────────────────────────────────
    eff = ""

    # High crawl, zero impressions
    high_crawl_sql = f"""
    SELECT
        page,
        SUM(crawl_hits) AS crawl_hits,
        SUM(bot_hits) AS bot_hits,
        SUM(impressions) AS impressions
    FROM t
    WHERE crawl_hits > 0
    GROUP BY page
    HAVING SUM(impressions) = 0
    ORDER BY crawl_hits DESC
    LIMIT {int(limit)}
    """
    cols_hc, rows_hc = run_query(gsc_paths, high_crawl_sql)
    eff += "<h2>High Crawl, Zero Impressions</h2>"
    eff += "<p style='font-size:13px;color:#64748b;margin-bottom:8px;'>Pages actively crawled but receiving no search impressions. Potential crawl budget waste.</p>"
    eff += (
        f"<p><a href='/export?report=gsc-high-crawl-no-impressions&from={date_from or ''}&to={date_to or ''}"
        f"&limit={int(limit)}'>Export CSV</a></p>"
    )
    if rows_hc:
        eff += html_table(rows_hc, cols_hc, max_rows=min(int(limit), 500))
    else:
        eff += "<p style='color:#94a3b8;'>No pages found with crawl activity but zero impressions.</p>"

    # Impressions but no recent crawl
    no_crawl_sql = f"""
    SELECT
        page,
        SUM(impressions) AS impressions,
        SUM(clicks) AS clicks,
        AVG(position) AS avg_position,
        SUM(crawl_hits) AS crawl_hits
    FROM t
    WHERE impressions > 0
    GROUP BY page
    HAVING SUM(crawl_hits) = 0
    ORDER BY impressions DESC
    LIMIT {int(limit)}
    """
    cols_nc, rows_nc = run_query(gsc_paths, no_crawl_sql)
    eff += "<h2>Impressions but No Recent Crawl</h2>"
    eff += "<p style='font-size:13px;color:#64748b;margin-bottom:8px;'>Pages with search impressions but no crawl activity in the selected period. May have stale cached content.</p>"
    eff += (
        f"<p><a href='/export?report=gsc-impressions-no-crawl&from={date_from or ''}&to={date_to or ''}"
        f"&limit={int(limit)}'>Export CSV</a></p>"
    )
    if rows_nc:
        eff += html_table(_format_gsc_rows(rows_nc, cols_nc), cols_nc, max_rows=min(int(limit), 500))
    else:
        eff += "<p style='color:#94a3b8;'>No pages found with impressions but zero crawl activity.</p>"

    # Crawl frequency vs position scatter plot
    scatter_sql = f"""
    SELECT
        page,
        SUM(crawl_hits) AS crawl_hits,
        AVG(position) AS avg_position,
        SUM(impressions) AS impressions,
        SUM(clicks) AS clicks
    FROM t
    WHERE crawl_hits > 0 AND impressions > 0
    GROUP BY page
    ORDER BY crawl_hits DESC
    LIMIT {min(int(limit), 500)}
    """
    cols_sc, rows_sc = run_query(gsc_paths, scatter_sql)
    eff += "<h2>Crawl Frequency vs Position</h2>"
    eff += "<p style='font-size:13px;color:#64748b;margin-bottom:8px;'>Scatter plot showing relationship between crawl frequency and average search position per page.</p>"
    if rows_sc:
        df_sc = pd.DataFrame(rows_sc, columns=cols_sc)
        impressions = [float(v or 0) for v in df_sc["impressions"].tolist()]
        max_imp = max(impressions) if impressions else 1.0
        clicks = [float(v) if v is not None else 0.0 for v in df_sc["clicks"].tolist()]
        max_clicks = max(clicks) if clicks else 1.0
        scatter_data = []
        for page_v, x_v, y_v, imp_v, click_v in zip(
            df_sc["page"].tolist(),
            df_sc["crawl_hits"].tolist(),
            df_sc["avg_position"].tolist(),
            impressions,
            clicks,
        ):
            xf = float(x_v) if x_v is not None else 0.0
            yf = float(y_v) if y_v is not None else 0.0
            size = max(4, min(20, int(imp_v / max(1.0, max_imp) * 20)))
            scatter_data.append({
                "value": [xf, yf, click_v],
                "page": str(page_v),
                "imp": imp_v,
                "symbolSize": size,
            })
        scatter_fn = (
            "function(p){var d=p.data;"
            "return d.page+'<br/>Crawl hits: '+d.value[0]+"
            "'<br/>Avg position: '+d.value[1].toFixed(1)+"
            "'<br/>Impressions: '+d.imp+'<br/>Clicks: '+d.value[2];}"
        )
        scatter_option = {
            "title": {
                "text": "Crawl Hits vs Avg Position (bubble size = impressions, color = clicks)",
                "left": "center",
                "textStyle": {"fontSize": 14, "fontWeight": "normal"},
            },
            "tooltip": {"trigger": "item", "formatter": "__SCATTER_FMT__"},
            "grid": {"left": 50, "right": 60, "top": 60, "bottom": 50, "containLabel": True},
            "xAxis": {"type": "value", "name": "Crawl Hits", "nameLocation": "middle", "nameGap": 30},
            "yAxis": {"type": "value", "name": "Avg Position (lower is better)", "nameLocation": "middle", "nameGap": 40, "inverse": True},
            "visualMap": {
                "min": 0,
                "max": max_clicks if max_clicks > 0 else 1,
                "dimension": 2,
                "calculable": True,
                "orient": "vertical",
                "right": 10,
                "top": "middle",
                "text": ["Clicks", ""],
                "inRange": {"color": ["#deebf7", "#9ecae1", "#4292c6", "#08519c", "#08306b"]},
            },
            "series": [{
                "type": "scatter",
                "data": scatter_data,
                "itemStyle": {"borderColor": "#475569", "borderWidth": 0.5},
            }],
        }
        eff += _echart_html(scatter_option, height=500, fns={"__SCATTER_FMT__": scatter_fn})
    else:
        eff += "<p style='color:#94a3b8;'>Not enough data for scatter plot (need pages with both crawl and impressions).</p>"

    tabs = tab_bar([
        ("performance", "Performance Overview"),
        ("efficiency", "Crawl-to-Index Efficiency"),
    ])
    body += tabs
    body += tab_panel("performance", perf)
    body += tab_panel("efficiency", eff)
    body += (
        "<p style='margin-top:16px;font-size:13px;'>"
        "Looking to split bot crawls into <strong>strategic vs non-strategic</strong>? "
        f"See <a href='/reports/strategic-crawl?from={date_from or ''}&to={date_to or ''}'>"
        "Strategic Crawl</a>."
        "</p>"
    )
    return _lv2_report_shell(
        title="Search Console", nav_key="reports",
        date_from=date_from, date_to=date_to, avail=avail,
        body=body,
    )


# ── /reports/strategic-crawl ─────────────────────────────────────────────
# Joins per-bot crawl (from logs) with GSC performance data to classify each
# crawl as strategic (toward an indexed/ranking URL) or non-strategic (crawl
# budget spent on pages GSC has never seen ranking).
#
# Requires the gsc_bot_daily aggregate — produced by
# scripts/ingest.py::_build_gsc_daily_aggregate. Dates without GSC data are
# silently absent from the aggregate.

_STRATEGIC_CLASS_LABELS = [
    ("strategic_top10",          "Strategic · top 10"),
    ("strategic_indexed",        "Strategic · indexed"),
    ("strategic_ranked_low",     "Strategic · ranked, 0 impressions"),
    ("non_strategic_unindexed",  "Non-strategic · no GSC signal"),
]
_STRATEGIC_CLASS_ORDER = {k: i for i, (k, _) in enumerate(_STRATEGIC_CLASS_LABELS)}


@app.get("/reports/strategic-crawl", response_class=HTMLResponse)
def strategic_crawl_report(
    date_from: Optional[str] = Query(None, alias="from"),
    date_to: Optional[str] = Query(None, alias="to"),
    bot_family: str = Query("Googlebot"),
    limit: int = 100,
):
    paths = list_partitions("gsc_bot_daily", date_from, date_to)
    avail = available_dates()
    date_from, date_to = _default_last_7(date_from, date_to, avail)
    body = ""

    if not paths:
        body += (
            "<div class='empty-state'>"
            "<h3>Strategic Crawl</h3>"
            "<p>This report requires the <code>gsc_bot_daily</code> aggregate, which "
            "joins per-bot crawl data with Google Search Console performance.</p>"
            "<p>To build it:</p>"
            "<ol>"
            "<li>Connect Google Search Console (see <a href='/settings/gsc'>Settings → Search Console</a>).</li>"
            "<li>Run <code>python scripts/backfill_gsc_daily.py</code> to build the aggregate "
            "for historical dates.</li>"
            "</ol>"
            "</div>"
        )
        return _lv2_report_shell(
            title="Strategic Crawl", nav_key="reports",
            date_from=date_from, date_to=date_to, avail=avail,
            body=body,
        )

    # Bot selector – which bot_family to slice on.
    _, bot_rows = run_query(
        paths,
        "SELECT bot_family, SUM(crawl_hits) AS hits FROM t GROUP BY bot_family ORDER BY hits DESC LIMIT 30",
    )
    bot_options = [str(r[0]) for r in bot_rows if r[0]]
    if bot_family not in bot_options and bot_options:
        # Fall back to Googlebot if present, else the bot with the most crawls.
        bot_family = "Googlebot" if "Googlebot" in bot_options else bot_options[0]
    esc_bot = sql_escape_string(bot_family)

    body += (
        "<form method='get' style='margin-bottom:12px;font-size:13px;'>"
        f"<input type='hidden' name='from' value='{html_escape(date_from or '', quote=True)}'>"
        f"<input type='hidden' name='to' value='{html_escape(date_to or '', quote=True)}'>"
        "<label>Bot family "
        "<select name='bot_family' onchange='this.form.submit()' "
        "style='border:1px solid var(--border);border-radius:4px;padding:3px 6px;'>"
    )
    for b in bot_options:
        sel = " selected" if b == bot_family else ""
        body += f"<option value='{html_escape(b, quote=True)}'{sel}>{html_escape(b)}</option>"
    body += "</select></label></form>"

    bot_filter_sql = f"bot_family = '{esc_bot}'"

    # KPI row: total crawls + counts by strategic_class.
    kpi_sql = f"""
    SELECT
        SUM(crawl_hits) AS total_crawl,
        SUM(CASE WHEN strategic_class IN ('strategic_top10','strategic_indexed','strategic_ranked_low') THEN crawl_hits ELSE 0 END) AS strategic_crawl,
        SUM(CASE WHEN strategic_class = 'strategic_top10' THEN crawl_hits ELSE 0 END) AS top10_crawl,
        SUM(CASE WHEN strategic_class = 'non_strategic_unindexed' THEN crawl_hits ELSE 0 END) AS non_strategic_crawl,
        COUNT(DISTINCT page) AS unique_pages,
        SUM(CASE WHEN is_indexed THEN crawl_hits ELSE 0 END) AS indexed_crawl
    FROM t
    WHERE {bot_filter_sql}
    """
    _, kpi_rows = run_query(paths, kpi_sql)
    k = kpi_rows[0] if kpi_rows else (0, 0, 0, 0, 0, 0)
    total = int(k[0] or 0)
    strategic = int(k[1] or 0)
    top10 = int(k[2] or 0)
    non_strategic = int(k[3] or 0)
    unique_pages = int(k[4] or 0)

    def pct_of_total(n: int) -> str:
        return f"{(100.0 * n / total):.1f}%" if total else "—"

    body += "<div class='kpi-row'>"
    body += kpi_card(f"Total {html_escape(bot_family)} crawls", total, None)
    body += kpi_card(f"Strategic ({pct_of_total(strategic)})", strategic, None)
    body += kpi_card(f"Top-10 ranking ({pct_of_total(top10)})", top10, None)
    body += kpi_card(f"Non-strategic ({pct_of_total(non_strategic)})", non_strategic, None)
    body += kpi_card("Unique pages crawled", unique_pages, None)
    body += "</div>"

    # Class breakdown bar chart.
    class_sql = f"""
    SELECT strategic_class, SUM(crawl_hits) AS hits, COUNT(DISTINCT page) AS pages
    FROM t
    WHERE {bot_filter_sql}
    GROUP BY strategic_class
    """
    cols_c, rows_c = run_query(paths, class_sql)
    # Relabel + order for display.
    relabeled: List[Tuple[str, int, int]] = []
    for r in rows_c:
        key = str(r[0] or "non_strategic_unindexed")
        label = dict(_STRATEGIC_CLASS_LABELS).get(key, key)
        relabeled.append((label, int(r[1] or 0), int(r[2] or 0)))
    relabeled.sort(key=lambda x: _STRATEGIC_CLASS_ORDER.get(
        next((k for k, v in _STRATEGIC_CLASS_LABELS if v == x[0]), ""), 99
    ))
    body += "<h2>Crawl volume by strategic class</h2>"
    if relabeled:
        chart_rows = [[lab, hits] for lab, hits, _p in relabeled]
        body += bar_chart(chart_rows, ["strategic_class", "crawl_hits"],
                          x_col="strategic_class", y_col="crawl_hits",
                          title=f"{bot_family} crawl hits by class")
        body += html_table(
            [[lab, f"{hits:,}", pct_of_total(hits), f"{pages:,}"] for lab, hits, pages in relabeled],
            ["Class", "Crawl hits", "% of total", "Unique pages"],
            max_rows=20,
        )
    else:
        body += "<p class='no-data'>No classified crawls for this selection.</p>"

    # Top non-strategic URLs — these are the pages burning crawl budget.
    non_strat_sql = f"""
    SELECT page, SUM(crawl_hits) AS crawl_hits, SUM(err_hits) AS err_hits,
           SUM(param_hits) AS param_hits
    FROM t
    WHERE {bot_filter_sql} AND strategic_class = 'non_strategic_unindexed'
    GROUP BY page
    ORDER BY crawl_hits DESC
    LIMIT {int(limit)}
    """
    cols_ns, rows_ns = run_query(paths, non_strat_sql)
    body += "<h2>Top non-strategic URLs</h2>"
    body += (
        "<p style='font-size:13px;color:#64748b;margin-bottom:8px;'>"
        f"URLs {html_escape(bot_family)} crawls most often that have no GSC impressions "
        "and no GSC ranking position. Typical candidates for noindex / blocking / canonicalization.</p>"
    )
    if rows_ns:
        body += html_table(rows_ns, cols_ns, max_rows=min(int(limit), 500))
    else:
        body += "<p class='no-data'>No non-strategic URLs in this range.</p>"

    # Top strategic URLs — where crawl effort is paying off.
    strat_sql = f"""
    SELECT page, SUM(crawl_hits) AS crawl_hits, SUM(impressions) AS impressions,
           SUM(clicks) AS clicks, AVG(position) AS avg_position
    FROM t
    WHERE {bot_filter_sql}
      AND strategic_class IN ('strategic_top10', 'strategic_indexed')
    GROUP BY page
    ORDER BY crawl_hits DESC
    LIMIT {int(limit)}
    """
    cols_s, rows_s = run_query(paths, strat_sql)
    body += "<h2>Top strategic URLs</h2>"
    body += (
        "<p style='font-size:13px;color:#64748b;margin-bottom:8px;'>"
        f"Most-crawled URLs that are indexed and/or ranking. Crawl budget well spent.</p>"
    )
    if rows_s:
        body += html_table(rows_s, cols_s, max_rows=min(int(limit), 500))
    else:
        body += "<p class='no-data'>No strategic URLs in this range.</p>"

    # Non-strategic crawl by path_segment_1 — which site areas are bleeding budget?
    seg_sql = f"""
    SELECT
        COALESCE(list_filter(string_split(page, '/'), x -> x <> '')[1], '(root)') AS path_segment_1,
        SUM(crawl_hits) AS crawl_hits,
        COUNT(DISTINCT page) AS unique_pages
    FROM t
    WHERE {bot_filter_sql} AND strategic_class = 'non_strategic_unindexed'
    GROUP BY 1
    ORDER BY crawl_hits DESC
    LIMIT 30
    """
    cols_seg, rows_seg = run_query(paths, seg_sql)
    body += "<h2>Non-strategic crawl by path segment</h2>"
    body += (
        "<p style='font-size:13px;color:#64748b;margin-bottom:8px;'>"
        "Which top-level site areas absorb the most non-strategic crawl. "
        "Use this to prioritize cleanup.</p>"
    )
    if rows_seg:
        body += bar_chart(rows_seg, cols_seg, x_col="path_segment_1",
                          y_col="crawl_hits",
                          title="Non-strategic crawl by segment 1")
        body += html_table(rows_seg, cols_seg, max_rows=30)
    else:
        body += "<p class='no-data'>No non-strategic crawl to break down.</p>"

    return _lv2_report_shell(
        title="Strategic Crawl", nav_key="reports",
        date_from=date_from, date_to=date_to, avail=avail,
        body=body,
    )


# ── Legacy route redirects ────────────────────────────────────────────────

def _preset_redirect(preset: str, date_from: Optional[str], date_to: Optional[str],
                     extra: Optional[Dict[str, str]] = None) -> RedirectResponse:
    parts = [f"preset={preset}"]
    if date_from: parts.append(f"from={date_from}")
    if date_to: parts.append(f"to={date_to}")
    if extra:
        for k, v in extra.items():
            if v:
                parts.append(f"{k}={v}")
    return RedirectResponse(url=f"/logs?{'&'.join(parts)}", status_code=301)


@app.get("/reports/crawl-volume")
def _redirect_crawl_volume(date_from: Optional[str] = Query(None, alias="from"), date_to: Optional[str] = Query(None, alias="to")):
    return _preset_redirect("traffic", date_from, date_to)

@app.get("/reports/status-over-time")
def _redirect_status_over_time(date_from: Optional[str] = Query(None, alias="from"), date_to: Optional[str] = Query(None, alias="to")):
    return _preset_redirect("status-over-time", date_from, date_to)

@app.get("/reports/top-urls")
def _redirect_top_urls(date_from: Optional[str] = Query(None, alias="from"), date_to: Optional[str] = Query(None, alias="to")):
    return _preset_redirect("top-urls", date_from, date_to)

@app.get("/reports/top-404")
def _redirect_top_404(date_from: Optional[str] = Query(None, alias="from"), date_to: Optional[str] = Query(None, alias="to")):
    return _preset_redirect("top-404", date_from, date_to)

@app.get("/reports/top-4xx")
def _redirect_top_4xx(date_from: Optional[str] = Query(None, alias="from"), date_to: Optional[str] = Query(None, alias="to")):
    return _preset_redirect("top-4xx", date_from, date_to)

@app.get("/reports/top-5xx")
def _redirect_top_5xx(date_from: Optional[str] = Query(None, alias="from"), date_to: Optional[str] = Query(None, alias="to")):
    return _preset_redirect("top-5xx", date_from, date_to)

@app.get("/reports/top-3xx")
def _redirect_top_3xx(date_from: Optional[str] = Query(None, alias="from"), date_to: Optional[str] = Query(None, alias="to")):
    return _preset_redirect("top-3xx", date_from, date_to)

@app.get("/reports/url-bytes")
def _redirect_url_bytes(date_from: Optional[str] = Query(None, alias="from"), date_to: Optional[str] = Query(None, alias="to")):
    # url-bytes = top paths sorted by bytes. Not a named preset; build directly.
    parts = ["mode=group", "group_by=path", "sort_by=bytes", "include_assets=false"]
    if date_from: parts.append(f"from={date_from}")
    if date_to: parts.append(f"to={date_to}")
    return RedirectResponse(url=f"/logs?{'&'.join(parts)}", status_code=301)

@app.get("/reports/url-groups")
def _redirect_url_groups(date_from: Optional[str] = Query(None, alias="from"), date_to: Optional[str] = Query(None, alias="to")):
    return _preset_redirect("url-groups", date_from, date_to)

@app.get("/reports/locale-groups")
def _redirect_locale_groups(date_from: Optional[str] = Query(None, alias="from"), date_to: Optional[str] = Query(None, alias="to")):
    # /reports/locales is a survivor — keep pointing there.
    qs = "&".join(f"{k}={v}" for k, v in [("from", date_from), ("to", date_to)] if v)
    return RedirectResponse(url=f"/reports/locales?tab=heatmap{'&' + qs if qs else ''}", status_code=301)

@app.get("/reports/wasted-crawl")
def _redirect_wasted_crawl(date_from: Optional[str] = Query(None, alias="from"), date_to: Optional[str] = Query(None, alias="to")):
    return _preset_redirect("crawl-waste", date_from, date_to)

@app.get("/reports/top-resource-waste")
def _redirect_top_resource_waste(date_from: Optional[str] = Query(None, alias="from"), date_to: Optional[str] = Query(None, alias="to")):
    # Closest preset is crawl-waste (both rank bot paths by waste).
    return _preset_redirect("crawl-waste", date_from, date_to)

@app.get("/reports/human-urls")
def _redirect_human_urls(date_from: Optional[str] = Query(None, alias="from"), date_to: Optional[str] = Query(None, alias="to")):
    # top-urls preset already forces is_bot=false.
    return _preset_redirect("top-urls", date_from, date_to)

@app.get("/reports/bot-urls")
def _redirect_bot_urls(bot: Optional[str] = None, date_from: Optional[str] = Query(None, alias="from"), date_to: Optional[str] = Query(None, alias="to")):
    parts = ["mode=group", "group_by=path", "is_bot=true"]
    if bot: parts.append(f"bot_family={bot}")
    if date_from: parts.append(f"from={date_from}")
    if date_to: parts.append(f"to={date_to}")
    return RedirectResponse(url=f"/logs?{'&'.join(parts)}", status_code=301)

@app.get("/reports/utm-chatgpt")
def _redirect_utm_chatgpt(date_from: Optional[str] = Query(None, alias="from"), date_to: Optional[str] = Query(None, alias="to")):
    parts = ["mode=group", "group_by=path", "utm_source=chatgpt.com"]
    if date_from: parts.append(f"from={date_from}")
    if date_to: parts.append(f"to={date_to}")
    return RedirectResponse(url=f"/logs?{'&'.join(parts)}", status_code=302)

@app.get("/reports/referer-flow")
def _redirect_referer_flow(date_from: Optional[str] = Query(None, alias="from"), date_to: Optional[str] = Query(None, alias="to")):
    return _preset_redirect("internal-nav", date_from, date_to)

# ----------------------------
# Legacy redirect: Guided Insights → Log Viewer
# ----------------------------

@app.get("/insights")
def insights_page():
    return RedirectResponse(url="/logs", status_code=301)


@app.get("/query", response_class=RedirectResponse)
def query_redirect():
    return RedirectResponse(url="/", status_code=301)


@app.get("/export")
def export(
    report: str,
    date_from: Optional[str] = Query(None, alias="from"),
    date_to: Optional[str] = Query(None, alias="to"),
    url_group: Optional[str] = None,
    locale: Optional[str] = None,
    limit: int = 100000,
):
    report = report.strip().lower()
    url_groups = _parse_csv_param(url_group)
    locales_sel = _parse_csv_param(locale)

    def stream_csv(sql: str, paths: List[str], filename: str, col_formatters: Optional[Dict[str, Any]] = None):
        if not paths:
            return PlainTextResponse("No data found for the selected date range.", status_code=404)

        wrapped_sql = _wrap_with_parquet_cte(paths, sql)
        cur = _get_db().cursor()

        def gen():
            try:
                res = cur.execute(wrapped_sql)
                cols = [d[0] for d in res.description]

                fmts: List[Tuple[int, Any]] = []
                if col_formatters:
                    idx = {c: i for i, c in enumerate(cols)}
                    fmts = [(idx[c], fn) for c, fn in col_formatters.items() if c in idx]

                s = io.StringIO()
                w = csv.writer(s)
                w.writerow(cols)
                yield s.getvalue().encode("utf-8-sig")
                s.seek(0)
                s.truncate(0)

                while True:
                    batch = res.fetchmany(1000)
                    if not batch:
                        break
                    for row in batch:
                        if fmts:
                            row = list(row)
                            for i, fn in fmts:
                                if row[i] is not None:
                                    row[i] = fn(row[i])
                        w.writerow(row)
                    yield s.getvalue().encode("utf-8")
                    s.seek(0)
                    s.truncate(0)
            finally:
                cur.close()

        headers = {
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Cache-Control": "no-store",
        }
        return StreamingResponse(gen(), media_type="text/csv; charset=utf-8", headers=headers)

    # Existing exports unchanged…

    if report == "locales":
        paths = list_partitions("locale_daily", date_from, date_to)
        sql = """
        SELECT locale,
                SUM(hits) AS hits,
                SUM(hits_bot) AS hits_bot,
                SUM(hits_human) AS hits_human,
                SUM(s4xx) AS s4xx,
                SUM(s5xx) AS s5xx,
                SUM(resource_hits) AS resource_hits,
                SUM(resource_hits_bot) AS resource_hits_bot
        FROM t
        GROUP BY locale
        ORDER BY hits DESC;
        """
        return stream_csv(sql, paths, "locales.csv")

    if report == "locale-groups":
        paths = list_partitions("locale_group_daily", date_from, date_to)
        clauses = []
        for clause in (
            _in_clause("locale", locales_sel),
            _in_clause("url_group", url_groups),
        ):
            if clause:
                clauses.append(clause)
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        sql = f"""
        SELECT locale,
                url_group,
                SUM(hits) AS hits,
                SUM(hits_bot) AS hits_bot,
                SUM(hits_human) AS hits_human,
                SUM(s4xx) AS s4xx,
                SUM(s5xx) AS s5xx
        FROM t
        {where}
        GROUP BY locale, url_group
        ORDER BY hits DESC
        LIMIT {int(limit)};
        """
        return stream_csv(sql, paths, "locale_groups.csv")

    if report == "gsc-top-clicks":
        paths = list_partitions("gsc_daily", date_from, date_to)
        sql = f"""
        SELECT page, SUM(clicks) AS clicks, SUM(impressions) AS impressions,
               AVG(ctr) AS avg_ctr, AVG(position) AS avg_position,
               SUM(crawl_hits) AS crawl_hits
        FROM t WHERE has_impressions
        GROUP BY page ORDER BY clicks DESC LIMIT {int(limit)};
        """
        return stream_csv(sql, paths, "gsc_top_clicks.csv", col_formatters=_GSC_COL_FORMATTERS)

    if report == "gsc-high-crawl-no-impressions":
        paths = list_partitions("gsc_daily", date_from, date_to)
        sql = f"""
        SELECT page, SUM(crawl_hits) AS crawl_hits, SUM(bot_hits) AS bot_hits,
               SUM(impressions) AS impressions
        FROM t WHERE crawl_hits > 0
        GROUP BY page HAVING SUM(impressions) = 0
        ORDER BY crawl_hits DESC LIMIT {int(limit)};
        """
        return stream_csv(sql, paths, "gsc_high_crawl_no_impressions.csv")

    if report == "gsc-impressions-no-crawl":
        paths = list_partitions("gsc_daily", date_from, date_to)
        sql = f"""
        SELECT page, SUM(impressions) AS impressions, SUM(clicks) AS clicks,
               AVG(position) AS avg_position, SUM(crawl_hits) AS crawl_hits
        FROM t WHERE impressions > 0
        GROUP BY page HAVING SUM(crawl_hits) = 0
        ORDER BY impressions DESC LIMIT {int(limit)};
        """
        return stream_csv(sql, paths, "gsc_impressions_no_crawl.csv", col_formatters=_GSC_COL_FORMATTERS)

    return PlainTextResponse(
        "Unknown report. Try report=locales, locale-groups, gsc-top-clicks, "
        "gsc-high-crawl-no-impressions, gsc-impressions-no-crawl. "
        "For log-viewer exports (rows/group/timeseries), use /export/logs with the same filter params.",
        status_code=400,
    )


@app.get("/export/logs")
def export_logs(
    date_from: Optional[str] = Query(None, alias="from"),
    date_to: Optional[str] = Query(None, alias="to"),
    search: Optional[str] = Query(None),
    search_mode: Optional[str] = Query(None),
    search_field: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    status_class: Optional[str] = Query(None),
    method: Optional[str] = Query(None),
    is_bot: Optional[str] = Query(None),
    bot_family: Optional[str] = Query(None),
    bot_category: Optional[str] = Query(None),
    url_group: Optional[str] = Query(None),
    locale: Optional[str] = Query(None),
    country: Optional[str] = Query(None),
    referer_type: Optional[str] = Query(None),
    utm_source: Optional[str] = Query(None),
    include_assets: bool = Query(True),
    content_only: bool = Query(False),
    mode: str = Query("rows"),
    group_by: Optional[str] = Query(None),
    group_by_2: Optional[str] = Query(None),
    sort_by: str = Query("hits"),
    bucket: Optional[str] = Query(None),
    stack_by: str = Query(""),
    limit: int = Query(10000),
):
    """CSV export for Rows / Group / Timeseries modes of /logs."""
    status_codes = _parse_status_list(status)
    status_classes = [int(c) for c in _parse_csv_param(status_class) if c.isdigit()]
    methods = _parse_csv_param(method)
    bot_families = _parse_csv_param(bot_family)
    bot_categories = _parse_csv_param(bot_category)
    url_groups = _parse_csv_param(url_group)
    locales_sel = _parse_csv_param(locale)
    countries = _parse_csv_param(country)
    referer_types = _parse_csv_param(referer_type)
    utm_sources = _parse_csv_param(utm_source)
    if search_mode not in LOG_SEARCH_MODE_VALUES:
        search_mode = "contains"
    if search_field not in LOG_SEARCH_FIELD_COLS:
        search_field = "any"
    if mode not in {m for m, _ in LOG_MODE_OPTIONS}:
        mode = "rows"

    paths = list_parsed_partitions(date_from, date_to)
    if not paths:
        return PlainTextResponse("No data in the selected range.", status_code=404)

    has_country_col = False
    try:
        _cols, _probe = run_query(paths, "SELECT column_name FROM (DESCRIBE t) WHERE column_name = 'country'")
        has_country_col = bool(_probe)
    except Exception:
        has_country_col = False

    if mode == "group" and sort_by == "waste_score":
        is_bot = "true"

    clauses = _build_log_where_clauses(
        status_codes=status_codes, status_classes=status_classes, methods=methods,
        bot_families=bot_families, bot_categories=bot_categories,
        url_groups=url_groups, locales=locales_sel, countries=countries,
        referer_types=referer_types, utm_sources=utm_sources,
        is_bot=is_bot, include_assets=include_assets, content_only=content_only,
        search=search, search_mode=search_mode, search_field=search_field,
        has_country_col=has_country_col,
    )
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""

    def stream_sql(sql: str, filename: str) -> StreamingResponse:
        wrapped = _wrap_with_parquet_cte(paths, sql)
        cur = _get_db().cursor()

        def gen():
            try:
                res = cur.execute(wrapped)
                cols = [d[0] for d in res.description]
                s = io.StringIO()
                w = csv.writer(s)
                w.writerow(cols)
                yield s.getvalue().encode("utf-8-sig")
                s.seek(0); s.truncate(0)
                while True:
                    batch = res.fetchmany(1000)
                    if not batch:
                        break
                    for row in batch:
                        w.writerow(row)
                    yield s.getvalue().encode("utf-8")
                    s.seek(0); s.truncate(0)
            finally:
                cur.close()

        return StreamingResponse(gen(), media_type="text/csv; charset=utf-8",
                                 headers={"Content-Disposition": f'attachment; filename="{filename}"',
                                          "Cache-Control": "no-store"})

    if mode == "group":
        valid_cols = {c for c, _ in LOG_GROUP_BY_COLUMNS}
        if not group_by or group_by not in valid_cols:
            return PlainTextResponse("Group mode requires a valid group_by.", status_code=400)
        group_cols = [group_by]
        select_parts = [f"{_group_col_expr(group_by, has_country_col)} AS {group_by}"]
        if group_by_2 and group_by_2 in valid_cols and group_by_2 != group_by:
            group_cols.append(group_by_2)
            select_parts.append(f"{_group_col_expr(group_by_2, has_country_col)} AS {group_by_2}")
        waste_enabled = sort_by == "waste_score" and is_bot == "true"
        metrics = [
            "COUNT(*) AS hits",
            "SUM(CASE WHEN is_bot THEN 1 ELSE 0 END) AS hits_bot",
            "SUM(CASE WHEN NOT is_bot THEN 1 ELSE 0 END) AS hits_human",
            "SUM(bytes_sent) AS bytes_total",
            "COUNT(DISTINCT edge_ip) AS unique_ips",
            "SUM(CASE WHEN status_class = 4 THEN 1 ELSE 0 END) AS s4xx",
            "SUM(CASE WHEN status_class = 5 THEN 1 ELSE 0 END) AS s5xx",
        ]
        if waste_enabled:
            metrics.append(f"({_WASTE_SCORE_SQL}) AS waste_score")
            order_expr = "waste_score DESC, hits DESC"
        elif sort_by == "bytes":
            order_expr = "bytes_total DESC"
        elif sort_by == "unique_ips":
            order_expr = "unique_ips DESC"
        else:
            order_expr = "hits DESC"
        sql = (
            f"SELECT {', '.join(select_parts + metrics)} FROM t {where} "
            f"GROUP BY {', '.join(group_cols)} ORDER BY {order_expr} LIMIT {int(limit)};"
        )
        return stream_sql(sql, f"logs_group_{group_by}.csv")

    if mode == "timeseries":
        single_day = date_from and date_to and date_from == date_to
        b = bucket if bucket in ("hour", "day") else ("hour" if single_day else "day")
        time_expr = "date_trunc('hour', ts_utc)" if b == "hour" else "CAST(ts_utc AS DATE)"
        if not stack_by:
            sql = f"""
            SELECT {time_expr} AS time_bucket,
                   COUNT(*) FILTER (WHERE status >= 200 AND status < 300) AS s2xx,
                   COUNT(*) FILTER (WHERE status >= 300 AND status < 400) AS s3xx,
                   COUNT(*) FILTER (WHERE status >= 400 AND status < 500) AS s4xx,
                   COUNT(*) FILTER (WHERE status >= 500 AND status < 600) AS s5xx,
                   COUNT(*) AS hits
            FROM t {where} GROUP BY time_bucket ORDER BY time_bucket;
            """
            return stream_sql(sql, f"logs_timeseries_{b}.csv")
        stack_expr = _group_col_expr(stack_by, has_country_col=False)
        top_keys_sql = f"SELECT {stack_expr} AS k FROM t {where} GROUP BY k ORDER BY COUNT(*) DESC LIMIT {LOG_STACK_LIMIT}"
        sql = f"""
        SELECT {time_expr} AS time_bucket,
               CASE WHEN {stack_expr} IN ({top_keys_sql}) THEN {stack_expr} ELSE 'Other' END AS stack_k,
               COUNT(*) AS hits
        FROM t {where}
        GROUP BY time_bucket, stack_k
        ORDER BY time_bucket, stack_k;
        """
        return stream_sql(sql, f"logs_timeseries_{b}_by_{stack_by}.csv")

    # Rows mode — export raw filtered rows (capped by limit for safety).
    country_sel = "CAST(country AS VARCHAR) AS country" if has_country_col else "NULL AS country"
    sql = f"""
    SELECT ts_utc, edge_ip, method, path, status, bytes_sent,
           is_bot, bot_family, bot_category, user_agent, url_group, locale,
           referer, referer_type, utm_source_norm, {country_sel}
    FROM t {where}
    ORDER BY ts_utc DESC
    LIMIT {int(limit)};
    """
    return stream_sql(sql, "logs_rows.csv")


# ---------------------------------------------------------------------------
# Log Viewer — browse individual parsed log rows
# ---------------------------------------------------------------------------

PARSED_DIR = ROOT / "data" / "parsed"


def list_parsed_partitions(date_from: Optional[str], date_to: Optional[str]) -> List[str]:
    """Return sorted list of parquet paths in data/parsed/date=YYYY-MM-DD/."""
    if not PARSED_DIR.exists():
        return []
    out = []
    for p in PARSED_DIR.glob("date=*/access.parquet"):
        date_dir = p.parent.name
        if not date_dir.startswith("date="):
            continue
        d = date_dir.split("=", 1)[1]
        if date_from and d < date_from:
            continue
        if date_to and d > date_to:
            continue
        out.append(p.as_posix())
    return sorted(out)


def available_parsed_dates() -> List[str]:
    """Return sorted list of dates that have parsed parquet files."""
    if not PARSED_DIR.exists():
        return []
    dates: set[str] = set()
    for p in PARSED_DIR.glob("date=*/access.parquet"):
        date_dir = p.parent.name
        if date_dir.startswith("date="):
            dates.add(date_dir.split("=", 1)[1])
    return sorted(dates)


# ── Unified log viewer helpers ──────────────────────────────────────────

LOG_GROUP_BY_COLUMNS = [
    ("path", "Path"),
    ("path_segment_1", "Path segment 1 (e.g. /en)"),
    ("path_segment_2", "Path segment 2 (e.g. /en/products)"),
    ("path_depth", "Path depth"),
    ("url_group", "URL group"),
    ("bot_family", "Bot family"),
    ("bot_category", "Bot category"),
    ("status", "Status code"),
    ("status_class", "Status class"),
    ("locale", "Locale"),
    ("country", "Country"),
    ("referer_type", "Referer type"),
    ("referer_path", "Referer path (internal)"),
    ("utm_source_norm", "UTM source"),
    ("method", "HTTP method"),
    ("is_bot", "Bot vs human"),
]

LOG_STACK_BY_OPTIONS = [
    ("", "(none)"),
    ("is_bot", "Bot vs human"),
    ("status_class", "Status class"),
    ("bot_family", "Bot family"),
    ("url_group", "URL group"),
    ("path_segment_1", "Path segment 1"),
    ("method", "HTTP method"),
    ("referer_type", "Referer type"),
    ("country", "Country"),
]

# SQL list of non-empty lowercased path segments, stripping query/fragment.
# Used to derive path_segment_N and path_depth at query time.
# e.g. "/en/products/widget?x=1" -> ['en', 'products', 'widget'].
_PATH_SEG_LIST_SQL = (
    "list_filter("
    "string_split("
    "LOWER(regexp_replace(COALESCE(path, ''), '[?#].*$', ''))"
    ", '/'"
    "), x -> x <> ''"
    ")"
)

LOG_SORT_BY_OPTIONS = [
    ("hits", "Hits"),
    ("bytes", "Bytes"),
    ("unique_ips", "Unique IPs"),
    ("waste_score", "Crawl waste score (bots only)"),
]

LOG_MODE_OPTIONS = [("rows", "Rows"), ("group", "Group"), ("timeseries", "Timeseries")]

# ── Search field + match-mode registries ──
# Each entry maps search_field → (human label, list of SQL column expressions).
# "any" fans out across four columns (OR-combined, or AND-combined for not_contains);
# scoped values target a single expression. referer_host is derived on the fly.
LOG_SEARCH_FIELDS: List[Tuple[str, str, List[str]]] = [
    ("any", "Any (path, IP, UA, referer)", ["path", "edge_ip", "user_agent", "referer"]),
    ("path", "Path", ["path"]),
    ("ip", "IP", ["edge_ip"]),
    ("user_agent", "User agent", ["user_agent"]),
    ("user_agent_family", "User-agent family", ["COALESCE(bot_family, '')"]),
    ("referer", "Referer", ["referer"]),
    ("referer_host", "Referer host",
     ["COALESCE(regexp_extract(referer, '^https?://([^/]+)', 1), '')"]),
]
LOG_SEARCH_FIELD_COLS: Dict[str, List[str]] = {k: cols for k, _, cols in LOG_SEARCH_FIELDS}

# Search modes + human labels. Values persisted in the URL — do not rename.
LOG_SEARCH_MODES: List[Tuple[str, str]] = [
    ("contains", "contains"),
    ("not_contains", "does not contain"),
    ("equals", "equals"),
    ("starts_with", "starts with"),
    ("ends_with", "ends with"),
    ("regex", "regex"),
]
LOG_SEARCH_MODE_VALUES = {v for v, _ in LOG_SEARCH_MODES}

# Default columns shown in /logs Rows mode when the user hasn't pinned a set via
# ?columns=. Every other schema column is available via the Columns picker.
LOG_DEFAULT_COLUMNS: List[str] = [
    "ts_utc", "method", "path", "status", "bytes_sent", "edge_ip",
    "country", "locale", "is_bot", "bot_family", "user_agent",
    "url_group", "referer", "referer_type", "utm_source_norm",
]

LOG_STACK_LIMIT = 8

# Waste score formula, ported verbatim from scripts/ingest.py wasted_crawl_daily.
# Meaningful only when is_bot = true; the UI forces is_bot=true when this is selected.
_WASTE_SCORE_SQL = (
    "SUM(CAST(is_parameterized AS BIGINT)) "
    "+ SUM(CASE WHEN status_class = 3 THEN 1 ELSE 0 END) "
    "+ SUM(CASE WHEN status_class IN (4,5) THEN 1 ELSE 0 END) "
    "+ SUM(CASE WHEN is_resource AND is_parameterized THEN 1 ELSE 0 END) "
    "+ SUM(CASE WHEN is_resource AND status_class IN (3,4,5) THEN 1 ELSE 0 END)"
)


# ── Search-operator parser (used by logviewer-v2) ──────────────────────────
# Tokens understood by the unified search input. Keys that are URL-safe are
# passed through to _build_log_where_clauses via the *_include / *_exclude sets
# so operator filters and popover-set filters share one predicate path.
_SEARCH_OP_KEYS = {
    "status":        "status",
    "status_class":  "status_class",
    "method":        "method",
    "is":            "is_bot",          # is:bot / is:human
    "path":          "path",
    "bot_family":    "bot_family",
    "bot_category":  "bot_category",
    "country":       "country",
    "locale":        "locale",
    "referer_type":  "referer_type",
    "utm_source":    "utm_source",
    "utm":           "utm_source",
    "url_group":     "url_group",
}


def _parse_search_operators(q: Optional[str]) -> Tuple[str, Dict[str, List[str]], Dict[str, List[str]], Optional[str]]:
    """Split a raw search input into (free_text, includes, excludes, forced_mode).

    Operator syntax: `key:value`, `-key:value` negates. `status:4xx` shorthand
    maps to status_class. `is:bot|human` maps to is_bot. A lone `/regex/`
    token sets forced_mode="regex" on the free-text search.

    Values remain strings; the caller dedupes and types them via the existing
    filter-parsing helpers. Unknown operators fall through to free text.
    """
    includes: Dict[str, List[str]] = {}
    excludes: Dict[str, List[str]] = {}
    free_tokens: List[str] = []
    forced_mode: Optional[str] = None

    if not q:
        return "", includes, excludes, forced_mode

    for tok in q.split():
        if not tok:
            continue
        neg = False
        t = tok
        if t.startswith("-") and ":" in t:
            neg = True
            t = t[1:]
        if ":" in t:
            key, _, val = t.partition(":")
            key_lower = key.lower()
            target = _SEARCH_OP_KEYS.get(key_lower)
            if target and val:
                bucket = excludes if neg else includes
                if target == "status" and len(val) >= 3 and val[-2:].lower() == "xx" and val[:-2].isdigit():
                    bucket.setdefault("status_class", []).append(val[:-2])
                    continue
                if target == "is_bot":
                    vl = val.lower()
                    if vl in ("bot", "true", "1"):
                        bucket.setdefault("is_bot", []).append("true")
                    elif vl in ("human", "false", "0"):
                        bucket.setdefault("is_bot", []).append("false")
                    continue
                bucket.setdefault(target, []).append(val)
                continue
        # bare /regex/ token → force regex mode for the free-text search
        if len(tok) >= 2 and tok.startswith("/") and tok.endswith("/"):
            forced_mode = "regex"
            free_tokens.append(tok[1:-1])
            continue
        free_tokens.append(tok)

    return " ".join(free_tokens), includes, excludes, forced_mode


def _build_log_where_clauses(
    *,
    status_codes: List[int],
    status_classes: List[int],
    methods: List[str],
    bot_families: List[str],
    bot_categories: List[str],
    url_groups: List[str],
    locales: List[str],
    countries: List[str],
    referer_types: List[str],
    utm_sources: List[str],
    is_bot: Optional[str],
    include_assets: bool,
    content_only: bool,
    search: Optional[str],
    search_mode: str,
    search_field: str,
    has_country_col: bool,
    # ── Phase 2 extensions: explicit excludes + path prefix filters ──
    excludes: Optional[Dict[str, List[str]]] = None,
    paths_include: Optional[List[str]] = None,
    paths_exclude: Optional[List[str]] = None,
    # ── Inline path rule-builder (contains / not_contains / regex, AND or OR) ──
    path_rules: Optional[List[Tuple[str, str]]] = None,
    path_rules_join: str = "and",
) -> List[str]:
    clauses: List[str] = []
    if status_codes:
        if len(status_codes) == 1:
            clauses.append(f"status = {status_codes[0]}")
        else:
            clauses.append(f"status IN ({','.join(str(c) for c in status_codes)})")
    if status_classes:
        valid = [c for c in status_classes if c in (1, 2, 3, 4, 5)]
        if valid:
            if len(valid) == 1:
                clauses.append(f"status_class = {valid[0]}")
            else:
                clauses.append(f"status_class IN ({','.join(str(c) for c in valid)})")
    for clause in (
        _in_clause("method", methods),
        _in_clause("bot_family", bot_families),
        _in_clause("bot_category", bot_categories),
        _in_clause("url_group", url_groups),
        _in_clause("locale", locales),
        _in_clause("referer_type", referer_types),
        _in_clause("utm_source_norm", utm_sources),
    ):
        if clause:
            clauses.append(clause)
    if is_bot == "true":
        clauses.append("is_bot = true")
    elif is_bot == "false":
        clauses.append("is_bot = false")
    if has_country_col:
        country_clause = _in_clause("CAST(country AS VARCHAR)", countries)
        if country_clause:
            clauses.append(country_clause)
    if not include_assets:
        clauses.append("(is_resource IS NULL OR is_resource = false)")
    if content_only:
        non_content = ",".join(f"'{sql_escape_string(g)}'" for g in sorted(NON_CONTENT_GROUPS))
        clauses.append(f"url_group NOT IN ({non_content})")
    if search:
        search_cols = LOG_SEARCH_FIELD_COLS.get(search_field, LOG_SEARCH_FIELD_COLS["any"])
        clause = _build_search_clause(search, search_mode, search_cols)
        if clause:
            clauses.append(clause)

    # ── Path prefix filters (from search operator `path:/foo`) ──
    for p in (paths_include or []):
        esc = sql_escape_string(p)
        clauses.append(f"path LIKE '{esc}%'")
    for p in (paths_exclude or []):
        esc = sql_escape_string(p)
        clauses.append(f"path NOT LIKE '{esc}%'")

    # ── Inline path rule-builder ──
    # Per-rule path-only fragments combined with AND or OR. Wraps in parens so
    # the join scope is *only* over the builder rows; the rest of the WHERE
    # stays AND-joined (prefix filters above remain AND-applied).
    if path_rules:
        rule_parts: List[str] = []
        for op, value in path_rules:
            if not value:
                continue
            if op == "contains":
                like_esc = sql_escape_like(value)
                rule_parts.append(f"path ILIKE '%{like_esc}%' ESCAPE '\\'")
            elif op == "not_contains":
                like_esc = sql_escape_like(value)
                rule_parts.append(f"(path IS NULL OR path NOT ILIKE '%{like_esc}%' ESCAPE '\\')")
            elif op == "regex":
                pat = value if value.startswith("(?") else f"(?i){value}"
                pat_esc = sql_escape_string(pat)
                rule_parts.append(f"regexp_matches(path, '{pat_esc}')")
        if rule_parts:
            joiner = " OR " if path_rules_join == "or" else " AND "
            clauses.append("(" + joiner.join(rule_parts) + ")")

    # ── Negation (exclude_*) clauses from search operators / popover ──
    if excludes:
        ex_status = [int(v) for v in excludes.get("status", []) if str(v).isdigit()]
        if ex_status:
            clauses.append(f"(status IS NULL OR status NOT IN ({','.join(str(c) for c in ex_status)}))")
        ex_status_class = [int(v) for v in excludes.get("status_class", []) if str(v).isdigit() and int(v) in (1, 2, 3, 4, 5)]
        if ex_status_class:
            clauses.append(f"(status_class IS NULL OR status_class NOT IN ({','.join(str(c) for c in ex_status_class)}))")
        for ex_key, col in (
            ("method", "method"),
            ("bot_family", "bot_family"),
            ("bot_category", "bot_category"),
            ("url_group", "url_group"),
            ("locale", "locale"),
            ("referer_type", "referer_type"),
            ("utm_source", "utm_source_norm"),
        ):
            vals = excludes.get(ex_key, [])
            if vals:
                escaped = ",".join(f"'{sql_escape_string(v)}'" for v in vals)
                clauses.append(f"({col} IS NULL OR {col} NOT IN ({escaped}))")
        ex_country = excludes.get("country", [])
        if ex_country and has_country_col:
            escaped = ",".join(f"'{sql_escape_string(v)}'" for v in ex_country)
            clauses.append(f"(country IS NULL OR CAST(country AS VARCHAR) NOT IN ({escaped}))")
        ex_is_bot = excludes.get("is_bot", [])
        if "true" in ex_is_bot:
            clauses.append("(is_bot IS NULL OR is_bot = false)")
        if "false" in ex_is_bot:
            clauses.append("is_bot = true")

    return clauses


def _build_search_clause(value: str, mode: str, cols: List[str]) -> str:
    """Build a SQL predicate for the Log Viewer Search field.

    Per-column predicate depends on `mode`. Multiple cols are OR-combined for
    matching modes (contains, equals, starts_with, ends_with, regex) and
    AND-combined for `not_contains` (so the value is absent from every col).
    Regex defaults to case-insensitive via a leading (?i); users override with
    an inline (?-i) or any other leading (? group — those are preserved as-is.
    """
    esc = sql_escape_string(value)
    if mode == "regex":
        pat = value if value.startswith("(?") else f"(?i){value}"
        pat_esc = sql_escape_string(pat)
        parts = [f"regexp_matches({col}, '{pat_esc}')" for col in cols]
        return "(" + " OR ".join(parts) + ")"
    if mode == "not_contains":
        parts = [f"{col} NOT ILIKE '%{esc}%'" for col in cols]
        return "(" + " AND ".join(parts) + ")"
    if mode == "equals":
        parts = [f"LOWER({col}) = LOWER('{esc}')" for col in cols]
        return "(" + " OR ".join(parts) + ")"
    if mode == "starts_with":
        parts = [f"{col} ILIKE '{esc}%'" for col in cols]
        return "(" + " OR ".join(parts) + ")"
    if mode == "ends_with":
        parts = [f"{col} ILIKE '%{esc}'" for col in cols]
        return "(" + " OR ".join(parts) + ")"
    # contains (default)
    parts = [f"{col} ILIKE '%{esc}%'" for col in cols]
    return "(" + " OR ".join(parts) + ")"


def _group_col_expr(col: str, has_country_col: bool) -> str:
    """SQL expression for a group_by column (handles NULLs and country casting)."""
    if col == "country":
        if not has_country_col:
            return "NULL"
        return "COALESCE(CAST(country AS VARCHAR), '(none)')"
    if col == "is_bot":
        return "CASE WHEN is_bot THEN 'Bot' ELSE 'Human' END"
    if col == "status_class":
        return "COALESCE(CAST(status_class AS VARCHAR), '')"
    if col == "status":
        return "CAST(status AS VARCHAR)"
    if col == "path_segment_1":
        return f"COALESCE({_PATH_SEG_LIST_SQL}[1], '(root)')"
    if col == "path_segment_2":
        return f"COALESCE({_PATH_SEG_LIST_SQL}[2], '(none)')"
    if col == "path_depth":
        return f"CAST(len({_PATH_SEG_LIST_SQL}) AS VARCHAR)"
    # String-valued cols
    return f"COALESCE({col}, '(none)')"


def _render_log_group_mode(
    paths, where: str, *,
    group_by: str, group_by_2: Optional[str], sort_by: str, limit: int,
    has_country_col: bool, is_bot: Optional[str],
) -> str:
    valid_cols = {c for c, _ in LOG_GROUP_BY_COLUMNS}
    if group_by not in valid_cols:
        return "<p class='no-data'>Pick a valid Group by column.</p>"
    if group_by == "country" and not has_country_col:
        return "<p class='no-data'>No country column in these partitions.</p>"
    if group_by_2 and group_by_2 not in valid_cols:
        group_by_2 = None
    if group_by_2 == group_by:
        group_by_2 = None
    if group_by_2 == "country" and not has_country_col:
        group_by_2 = None

    select_parts: List[str] = [f"{_group_col_expr(group_by, has_country_col)} AS {group_by}"]
    group_cols: List[str] = [group_by]
    if group_by_2:
        select_parts.append(f"{_group_col_expr(group_by_2, has_country_col)} AS {group_by_2}")
        group_cols.append(group_by_2)

    waste_enabled = sort_by == "waste_score" and is_bot == "true"
    metrics_parts = [
        "COUNT(*) AS hits",
        "SUM(CASE WHEN is_bot THEN 1 ELSE 0 END) AS hits_bot",
        "SUM(CASE WHEN NOT is_bot THEN 1 ELSE 0 END) AS hits_human",
        "SUM(bytes_sent) AS bytes_total",
        "COUNT(DISTINCT edge_ip) AS unique_ips",
        "SUM(CASE WHEN status_class = 4 THEN 1 ELSE 0 END) AS s4xx",
        "SUM(CASE WHEN status_class = 5 THEN 1 ELSE 0 END) AS s5xx",
    ]
    if waste_enabled:
        metrics_parts.append(f"({_WASTE_SCORE_SQL}) AS waste_score")

    if waste_enabled:
        order_expr = "waste_score DESC, hits DESC"
    elif sort_by == "bytes":
        order_expr = "bytes_total DESC"
    elif sort_by == "unique_ips":
        order_expr = "unique_ips DESC"
    else:
        order_expr = "hits DESC"

    group_sql = ", ".join(group_cols)
    select_sql = ",\n    ".join(select_parts + metrics_parts)
    sql = f"""
    SELECT {select_sql}
    FROM t
    {where}
    GROUP BY {group_sql}
    ORDER BY {order_expr}
    LIMIT {int(limit)};
    """
    cols, rows = run_query(paths, sql)
    if not rows:
        return no_data_notice()

    # Charts: a bar chart on the top N by the primary group column.
    chart_rows = rows[:CHART_BAR_LIMIT]
    chart_title = f"Top {min(len(rows), CHART_BAR_LIMIT)} by {group_by}"
    if waste_enabled:
        chart_html = bar_chart(chart_rows, cols, x_col=group_by, y_col="waste_score",
                               title=f"{chart_title} (waste score)")
    elif sort_by == "bytes":
        chart_html = bytes_bar_chart(chart_rows, cols, x_col=group_by, y_col="bytes_total",
                                     title=f"{chart_title} (bytes)")
    else:
        chart_html = bar_chart(chart_rows, cols, x_col=group_by,
                               y_cols=["hits_human", "hits_bot"],
                               title=f"{chart_title} — human vs bot", barmode="stack")

    # Format byte column for the table.
    try:
        bytes_idx = cols.index("bytes_total")
    except ValueError:
        bytes_idx = -1
    display_rows = []
    for r in rows:
        row = list(r)
        if bytes_idx >= 0:
            row[bytes_idx] = fmt_bytes(row[bytes_idx])
        display_rows.append(row)

    return chart_html + html_table(display_rows, cols, max_rows=int(limit))


def _render_log_timeseries_mode(
    paths, where: str, *, date_from: Optional[str], date_to: Optional[str],
    bucket: str, stack_by: str,
) -> str:
    single_day = date_from and date_to and date_from == date_to
    if bucket not in ("hour", "day"):
        bucket = "hour" if single_day else "day"
    if bucket == "hour":
        time_expr = "date_trunc('hour', ts_utc)"
        gran = "hourly"
    else:
        time_expr = "CAST(ts_utc AS DATE)"
        gran = "daily"

    valid_stack = {k for k, _ in LOG_STACK_BY_OPTIONS}
    if stack_by not in valid_stack:
        stack_by = ""

    if not stack_by:
        sql = f"""
        SELECT {time_expr} AS time_bucket,
               COUNT(*) FILTER (WHERE status >= 200 AND status < 300) AS s2xx,
               COUNT(*) FILTER (WHERE status >= 300 AND status < 400) AS s3xx,
               COUNT(*) FILTER (WHERE status >= 400 AND status < 500) AS s4xx,
               COUNT(*) FILTER (WHERE status >= 500 AND status < 600) AS s5xx
        FROM t
        {where}
        GROUP BY time_bucket
        ORDER BY time_bucket;
        """
        cols, rows = run_query(paths, sql)
        if not rows:
            return no_data_notice()
        return line_chart(rows, cols, x_col="time_bucket",
                          y_cols=["s2xx", "s3xx", "s4xx", "s5xx"],
                          title=f"Requests over time ({gran}, by status class)")

    stack_expr = _group_col_expr(stack_by, has_country_col=False)
    top_keys_sql = f"""
    SELECT {stack_expr} AS k
    FROM t {where}
    GROUP BY k
    ORDER BY COUNT(*) DESC
    LIMIT {LOG_STACK_LIMIT}
    """
    sql = f"""
    SELECT {time_expr} AS time_bucket,
           CASE WHEN {stack_expr} IN ({top_keys_sql})
                THEN {stack_expr} ELSE 'Other' END AS stack_k,
           COUNT(*) AS hits
    FROM t
    {where}
    GROUP BY time_bucket, stack_k
    ORDER BY time_bucket, stack_k;
    """
    cols, rows = run_query(paths, sql)
    if not rows:
        return no_data_notice()

    df = pd.DataFrame(rows, columns=cols)
    pivot = df.pivot_table(index="time_bucket", columns="stack_k",
                           values="hits", aggfunc="sum", fill_value=0).sort_index()
    y_cols = [str(c) for c in pivot.columns]
    out_cols = ["time_bucket"] + y_cols
    out_rows = [[idx] + [int(v) for v in row] for idx, row in zip(pivot.index, pivot.values)]
    return line_chart(out_rows, out_cols, x_col="time_bucket", y_cols=y_cols,
                      title=f"Requests over time ({gran}, stacked by {stack_by})")


# Preset registry for /logs?preset=<name>. Values are canonical query params
# the preset expands to. Dates passed by the caller win over preset dates
# (so bookmarks retain the user's chosen range).
LOG_PRESETS: Dict[str, Dict[str, str]] = {
    # ── Group presets ─────────────────────────────────────────────────────
    "top-4xx": {"mode": "group", "group_by": "path", "status_class": "4",
                "sort_by": "hits", "include_assets": "false"},
    "top-5xx": {"mode": "group", "group_by": "path", "status_class": "5",
                "sort_by": "hits", "include_assets": "false"},
    "top-3xx": {"mode": "group", "group_by": "path", "status_class": "3",
                "sort_by": "hits", "include_assets": "false"},
    "top-404": {"mode": "group", "group_by": "path", "status": "404",
                "sort_by": "hits", "include_assets": "false"},
    "top-urls": {"mode": "group", "group_by": "path", "is_bot": "false",
                 "include_assets": "false"},
    "url-groups": {"mode": "group", "group_by": "url_group"},
    "bot-families": {"mode": "group", "group_by": "bot_family", "is_bot": "true"},
    "bot-categories": {"mode": "group", "group_by": "bot_category", "is_bot": "true"},
    "crawl-waste": {"mode": "group", "group_by": "path", "is_bot": "true",
                    "sort_by": "waste_score"},
    "googlebot-segments": {
        "mode": "group", "group_by": "path_segment_1",
        "is_bot": "true", "bot_family": "Googlebot",
        "include_assets": "false",
    },
    "googlebot-subsegments": {
        "mode": "group", "group_by": "path_segment_1",
        "group_by_2": "path_segment_2",
        "is_bot": "true", "bot_family": "Googlebot",
        "include_assets": "false",
    },
    "googlebot-depth": {
        "mode": "group", "group_by": "path_depth",
        "is_bot": "true", "bot_family": "Googlebot",
        "include_assets": "false",
    },
    "bot-segments": {
        "mode": "group", "group_by": "path_segment_1",
        "is_bot": "true", "include_assets": "false",
    },
    "heavy-urls": {"mode": "group", "group_by": "path", "sort_by": "bytes",
                   "include_assets": "false"},
    "top-countries": {"mode": "group", "group_by": "country"},
    "top-locales": {"mode": "group", "group_by": "locale"},
    "utm-sources": {"mode": "group", "group_by": "utm_source_norm"},
    "internal-nav": {"mode": "group", "group_by": "referer_path",
                     "group_by_2": "path", "referer_type": "Internal"},
    "referer-types": {"mode": "group", "group_by": "referer_type"},
    # ── Timeseries presets ────────────────────────────────────────────────
    "traffic": {"mode": "timeseries", "bucket": "day", "stack_by": "is_bot"},
    "hourly-traffic": {"mode": "timeseries", "bucket": "hour", "stack_by": "is_bot"},
    "status-over-time": {"mode": "timeseries", "bucket": "day",
                          "stack_by": "status_class"},
    "errors-over-time": {"mode": "timeseries", "bucket": "day",
                          "stack_by": "status_class", "status_class": "4,5"},
    "methods-over-time": {"mode": "timeseries", "bucket": "day", "stack_by": "method"},
    "url-groups-over-time": {"mode": "timeseries", "bucket": "day",
                              "stack_by": "url_group"},
    "bot-families-over-time": {"mode": "timeseries", "bucket": "day",
                                "stack_by": "bot_family", "is_bot": "true"},
    "countries-over-time": {"mode": "timeseries", "bucket": "day", "stack_by": "country"},
    "referers-over-time": {"mode": "timeseries", "bucket": "day",
                            "stack_by": "referer_type"},
}


# Chip groups for the Insights strip above the Log Viewer filter bar.
# Each entry is (preset_key, short_label). Keys must exist in LOG_PRESETS.
LOG_PRESET_CHIP_GROUPS: List[Tuple[str, List[Tuple[str, str]]]] = [
    ("Traffic", [
        ("traffic", "Daily traffic"),
        ("hourly-traffic", "Hourly traffic"),
        ("status-over-time", "Status over time"),
        ("methods-over-time", "Methods over time"),
    ]),
    ("Errors", [
        ("top-4xx", "Top 4xx"),
        ("top-5xx", "Top 5xx"),
        ("top-404", "Top 404"),
        ("top-3xx", "Top 3xx"),
        ("errors-over-time", "Errors over time"),
    ]),
    ("URLs", [
        ("top-urls", "Top content URLs"),
        ("url-groups", "URL groups"),
        ("heavy-urls", "Heaviest URLs"),
        ("url-groups-over-time", "URL groups over time"),
    ]),
    ("Bots", [
        ("bot-families", "Bot families"),
        ("bot-categories", "Bot categories"),
        ("crawl-waste", "Crawl waste"),
        ("bot-families-over-time", "Bot families over time"),
        ("googlebot-segments", "Googlebot top path segments"),
        ("googlebot-subsegments", "Googlebot segment 1 × 2"),
        ("googlebot-depth", "Googlebot by path depth"),
        ("bot-segments", "All bots by path segment"),
    ]),
    ("Geo", [
        ("top-countries", "Top countries"),
        ("top-locales", "Top locales"),
        ("countries-over-time", "Countries over time"),
    ]),
    ("Acquisition", [
        ("utm-sources", "UTM sources"),
        ("referer-types", "Referer types"),
        ("internal-nav", "Internal nav"),
        ("referers-over-time", "Referers over time"),
    ]),
]


def _resolve_log_preset(
    name: str,
    *,
    date_from: Optional[str],
    date_to: Optional[str],
    extra_status: Optional[str] = None,
    extra_url_group: Optional[str] = None,
) -> RedirectResponse:
    params = dict(LOG_PRESETS.get(name, {}))
    if not params:
        return RedirectResponse(url="/logs", status_code=302)
    if date_from:
        params["from"] = date_from
    if date_to:
        params["to"] = date_to
    # Allow status/url_group to be layered on top of a preset (e.g. top-urls + filter).
    if extra_status and "status" not in params and "status_class" not in params:
        params["status"] = extra_status
    if extra_url_group:
        params["url_group"] = extra_url_group
    # Carry the preset key forward under a distinct name so the render path
    # can highlight it in the Insights dropdown without re-entering the
    # redirect loop (the handler only redirects on ?preset=, not on
    # ?active_preset=).
    params["active_preset"] = name
    qs = "&".join(f"{k}={v}" for k, v in params.items())
    return RedirectResponse(url=f"/logs?{qs}", status_code=302)


def _search_hint_text(field: str, mode: str) -> str:
    """Plain-English caption of what the current Field + Match selection matches."""
    field_phrases = {
        "any": "path, IP, UA, or referer",
        "path": "Path",
        "ip": "IP",
        "user_agent": "User agent",
        "user_agent_family": "User-agent family",
        "referer": "Referer",
        "referer_host": "Referer host",
    }
    phrase = field_phrases.get(field, field_phrases["any"])
    is_any = field == "any"

    if mode == "regex":
        tail = " — case-insensitive by default; prepend (?-i) for case-sensitive."
        if is_any:
            return f"Matches if any of {phrase} matches the pattern{tail}"
        return f"{phrase} matches the pattern{tail}"

    verb_map = {
        "contains": "contains",
        "not_contains": "does not contain",
        "equals": "equals",
        "starts_with": "starts with",
        "ends_with": "ends with",
    }
    verb = verb_map.get(mode, "contains")
    tail = " — case-insensitive."
    if is_any:
        quantifier = "none of" if mode == "not_contains" else "any of"
        return f"Matches if {quantifier} {phrase} {verb} the text{tail}"
    return f"{phrase} {verb} the text{tail}"


def _render_insights_strip(
    date_from: Optional[str],
    date_to: Optional[str],
    mode: str = "rows",
    active_preset: Optional[str] = None,
) -> str:
    """Single-line 'Jump to view ▾' dropdown (Group + Timeseries modes only).

    The dropdown remembers the current view: when `active_preset` matches a
    chip, its label becomes the summary text ("Jump to: Top 404 ▾") and it's
    highlighted in the menu. Dates in the URL are preserved.

    Returns "" in Rows mode — presets only make sense for aggregated views.
    """
    if mode not in ("group", "timeseries"):
        return ""

    date_qs = ""
    if date_from:
        date_qs += f"&from={html_escape(date_from, quote=True)}"
    if date_to:
        date_qs += f"&to={html_escape(date_to, quote=True)}"

    label_for: Dict[str, str] = {}
    for _, chips in LOG_PRESET_CHIP_GROUPS:
        for key, label in chips:
            label_for[key] = label

    active_label = label_for.get(active_preset or "")
    summary_text = (
        f"Jump to: <strong>{html_escape(active_label)}</strong>"
        if active_label else "Jump to a view"
    )

    parts: List[str] = ["<div class='insights-jump'>"]
    parts.append("<span class='ij-lead'>Insights</span>")
    parts.append("<details>")
    parts.append(f"<summary>{summary_text}</summary>")
    parts.append("<div class='ij-menu' role='menu'>")

    for category, chips in LOG_PRESET_CHIP_GROUPS:
        parts.append("<div class='ij-col'>")
        parts.append(f"<div class='ij-cat'>{html_escape(category)}</div>")
        for preset_key, label in chips:
            cls = "ij-item active" if preset_key == active_preset else "ij-item"
            href = f"/logs?preset={html_escape(preset_key, quote=True)}{date_qs}"
            parts.append(f"<a class='{cls}' href='{html_escape(href, quote=True)}'>{html_escape(label)}</a>")
        parts.append("</div>")

    parts.append("</div>")   # .ij-menu
    parts.append("</details>")

    if active_preset:
        clear_href = f"/logs?{date_qs.lstrip('&')}" if date_qs else "/logs"
        parts.append(
            f"<a class='ij-clear' href='{html_escape(clear_href, quote=True)}'>Clear view</a>"
        )

    parts.append("</div>")   # .insights-jump
    return "".join(parts)


# ── Log viewer v2 (htmx redesign) ───────────────────────────────────────────
# SAVED_VIEWS are hardcoded predefined views — user CRUD is Phase 5. Each view
# is a canonical set of filter params; when `view=<id>` is present, the handler
# merges these defaults with any explicit overrides in the URL.
LV2_SAVED_VIEWS: List[Dict[str, Any]] = [
    {"id": "all",      "label": "All traffic",  "icon": "▦", "filters": {}},
    {"id": "errors",   "label": "Errors only",  "icon": "!", "filters": {"status_class": "4,5"}},
    {"id": "top-404",  "label": "404s",         "icon": "✕", "filters": {"status": "404"}},
    {"id": "ai-bots",  "label": "AI crawlers",  "icon": "✦", "filters": {"bot_category": "AI", "is_bot": "true"}},
    {"id": "humans",   "label": "Humans only",  "icon": "☺", "filters": {"is_bot": "false"}},
    {"id": "paid",     "label": "Paid traffic", "icon": "$", "filters": {"utm_source": "google_ads,newsletter,chatgpt"}},
]

LV2_COLUMN_LABELS: Dict[str, str] = {
    "ts_utc": "Timestamp", "ts_local": "Time (local)", "method": "Method",
    "path": "Path", "status": "Status", "status_class": "Status class",
    "bytes_sent": "Bytes", "edge_ip": "IP", "country": "Country",
    "locale": "Locale", "is_bot": "Is bot", "bot_family": "Bot family",
    "bot_category": "Bot category", "user_agent": "User agent",
    "url_group": "URL group", "referer": "Referer", "referer_host": "Referer host",
    "referer_path": "Referer path", "referer_type": "Referer type",
    "utm_source_norm": "UTM source", "utm_medium": "UTM medium",
    "utm_campaign": "UTM campaign", "utm_term": "UTM term", "utm_content": "UTM content",
    "is_parameterized": "Parameterized?", "is_resource": "Resource?",
    "query_string": "Query", "request_target": "Request",
}


_LV2_STATIC_DIR = Path(__file__).resolve().parent / "static"


def _lv2_static_ver(name: str) -> str:
    try:
        return str(int((_LV2_STATIC_DIR / name).stat().st_mtime))
    except OSError:
        return "0"


def _lv2_icon(name: str, cls: str = "") -> str:
    extra = f" class='{cls}'" if cls else ""
    return f"<svg{extra} width='14' height='14' aria-hidden='true'><use href='/static/icons.svg#{name}'/></svg>"


def _lv2_build_qs(params: Dict[str, Any], **overrides: Any) -> str:
    """Build a query string from params dict, applying overrides (None drops).

    List/tuple values emit one `k=v` pair per element so repeated query params
    (e.g. `pf=contains:/api&pf=regex:^v\\d`) roundtrip without collapsing.
    """
    merged = dict(params)
    for k, v in overrides.items():
        if v is None:
            merged.pop(k, None)
        else:
            merged[k] = v
    parts = []
    for k, v in merged.items():
        if v is None or v == "":
            continue
        if isinstance(v, (list, tuple)):
            for item in v:
                if item is None or item == "":
                    continue
                # URL-encode list items so reserved chars (&, +, ?, #) survive
                # the query-string round trip. `:` and `/` left readable.
                parts.append(f"{k}={url_quote(str(item), safe=':/')}")
        else:
            parts.append(f"{k}={html_escape(str(v), quote=True)}")
    return "&".join(parts)


def _lv2_apply_view(view_id: Optional[str], current: Dict[str, Optional[str]]) -> Dict[str, Optional[str]]:
    """Merge a saved view's defaults onto the current filter dict (URL overrides win)."""
    if not view_id:
        return current
    match = next((v for v in LV2_SAVED_VIEWS if v["id"] == view_id), None)
    if not match:
        return current
    merged = dict(current)
    for k, v in match["filters"].items():
        if not merged.get(k):
            merged[k] = v
    return merged


def _lv2_topnav(active: str) -> str:
    """Render the sticky top nav. Reports is a dropdown; other items are plain links."""
    reports_children = [
        ("Locales", "/reports/locales"),
        ("Search Console", "/reports/gsc"),
        ("Strategic Crawl", "/reports/strategic-crawl"),
    ]
    reports_active = "active" if active == "reports" else ""
    reports_items = "".join(
        f"<a href='{url}'>{html_escape(label)}</a>"
        for label, url in reports_children
    )
    reports_dropdown = (
        f"<div class='nav-item-dropdown'>"
        f"<button type='button' class='nav-link {reports_active}' "
        "data-nav-dropdown='nav-dd-reports' aria-haspopup='true' aria-expanded='false'>"
        "Reports"
        f"<span class='caret'>{_lv2_icon('chevron')}</span>"
        "</button>"
        f"<div class='nav-dropdown' id='nav-dd-reports' role='menu'>{reports_items}</div>"
        "</div>"
    )

    plain_items = [
        ("logs", "Logs", "/logs"),
        ("summary", "Summary", "/reports/summary"),
    ]
    plain_links = "".join(
        f"<a href='{url}' class='nav-link{' active' if active == key else ''}'>{html_escape(label)}</a>"
        for key, label, url in plain_items
    )
    settings_link = (
        f"<a href='/settings' class='nav-link{' active' if active == 'settings' else ''}'>Settings</a>"
    )

    return (
        "<nav class='topnav'>"
        "<div class='brand'><span class='brand-mark'></span><span>log analyser</span></div>"
        f"<div class='nav-links'>{plain_links}{reports_dropdown}{settings_link}</div>"
        "<div class='nav-util'>"
        "<span class='nav-pill' title='Data freshness'><span class='dot'></span>live</span>"
        f"<button class='btn btn-icon btn-ghost' data-popover-trigger='kb-help-open' title='Shortcuts (?)' aria-label='Keyboard shortcuts'>{_lv2_icon('keyboard')}</button>"
        "</div>"
        "</nav>"
    )


def _lv2_kb_help() -> str:
    rows = [
        ("<kbd class='kbd'>/</kbd>", "Focus search"),
        ("<kbd class='kbd'>?</kbd>", "Show this help"),
        ("<kbd class='kbd'>esc</kbd>", "Close drawer / popover"),
        ("<kbd class='kbd'>j</kbd> / <kbd class='kbd'>k</kbd>", "Next / previous row"),
        ("<kbd class='kbd'>g</kbd>", "Toggle chart"),
    ]
    row_html = "".join(f"<div class='kh-row'><span>{lbl}</span><span>{k}</span></div>" for k, lbl in rows)
    return (
        "<div class='kb-help' id='kb-help' data-kb-close>"
        "<div class='kb-help-panel'>"
        "<h3>Keyboard shortcuts</h3>"
        f"{row_html}"
        "<div class='kh-close'><button class='btn btn-sm' data-kb-close>Close</button></div>"
        "</div></div>"
    )


def _lv2_page_head(title: str, count_text: str, mode: str, params: Dict[str, Any]) -> str:
    modes = [("rows", "Rows"), ("group", "Group"), ("timeseries", "Timeseries")]
    seg_links = "".join(
        f"<a data-mode-val='{mv}' class='{'active' if mode == mv else ''}' "
        f"href='/logs?{_lv2_build_qs(params, mode=mv, page=None, active_preset=None)}' "
        f"hx-get='/logs?{_lv2_build_qs(params, mode=mv, page=None, active_preset=None)}' "
        f"hx-target='#results' hx-push-url='true' hx-swap='innerHTML'>{html_escape(ml)}</a>"
        for mv, ml in modes
    )
    export_qs = _lv2_build_qs(params, mode=None, page=None, per_page=None)
    return (
        "<div class='page-head'>"
        f"<div><div class='page-title'>{html_escape(title)} <small>{count_text}</small></div></div>"
        "<div class='page-actions'>"
        f"<div class='seg' role='tablist' data-mode-seg>{seg_links}</div>"
        f"<a class='btn btn-sm' href='/export/logs?{export_qs}' title='Export'>{_lv2_icon('download')}<span>Export</span></a>"
        "</div>"
        "</div>"
    )


def _lv2_views_strip(active_view: Optional[str], date_from: Optional[str], date_to: Optional[str]) -> str:
    date_qs = ""
    if date_from:
        date_qs += f"&from={html_escape(date_from, quote=True)}"
    if date_to:
        date_qs += f"&to={html_escape(date_to, quote=True)}"
    chips = []
    for v in LV2_SAVED_VIEWS:
        href = f"/logs?view={v['id']}{date_qs}"
        active = " active" if active_view == v["id"] else ""
        chips.append(
            f"<a class='view-chip{active}' href='{href}' "
            f"hx-get='{href}' hx-target='#results' hx-push-url='true' hx-swap='innerHTML'>"
            f"<span class='view-icon'>{html_escape(v['icon'])}</span>"
            f"{html_escape(v['label'])}</a>"
        )
    return (
        "<div class='views-strip'>"
        "<span class='views-label'>Views</span>"
        f"{''.join(chips)}"
        "</div>"
    )


def _lv2_more_filters_popover(
    *,
    params: Dict[str, Any],
    filter_opts: Dict[str, List[str]],
    filter_selected: Dict[str, List[str]],
    is_bot: Optional[str],
    has_country_col: bool,
) -> Tuple[str, int]:
    """Render the two-pane More filters popover. Returns (html, active_count)."""

    def checklist(field: str, options: List[str], selected_vals: List[str]) -> str:
        items = []
        for opt in options:
            checked = "checked" if opt in selected_vals else ""
            items.append(
                f"<label><input type='checkbox' data-mfp-field='{html_escape(field)}' "
                f"data-filter-value='{html_escape(opt, quote=True)}' {checked}> "
                f"<span>{html_escape(opt)}</span></label>"
            )
        list_html = f"<div class='pop-checklist'>{''.join(items)}</div>"
        if len(options) <= 6:
            return list_html
        placeholder = html_escape(field.replace("_", " "), quote=True)
        return (
            "<div class='pop-checklist-wrap'>"
            f"<input type='text' class='pop-checklist-search' "
            f"placeholder='Filter {placeholder}…' "
            "autocomplete='off' spellcheck='false'>"
            f"{list_html}"
            "<div class='pop-checklist-meta' data-checklist-meta></div>"
            "</div>"
        )

    # Panes (category → list of (label, field, options, selected_list))
    panes: List[Tuple[str, str, List[Tuple[str, str, List[str], List[str]]]]] = [
        ("request", "Request", [
            ("Status code", "status",       filter_opts.get("status",      []), filter_selected.get("status", [])),
            ("Method",      "method",       filter_opts.get("method",      []), filter_selected.get("method", [])),
        ]),
        ("traffic", "Traffic", [
            ("Bot family",   "bot_family",   filter_opts.get("bot_family",   []), filter_selected.get("bot_family", [])),
            ("Bot category", "bot_category", filter_opts.get("bot_category", []), filter_selected.get("bot_category", [])),
            ("URL group",    "url_group",    filter_opts.get("url_group",    []), filter_selected.get("url_group", [])),
        ]),
        ("geo", "Geo & language", [
            ("Locale",  "locale",  filter_opts.get("locale",  []), filter_selected.get("locale", [])),
        ] + ([
            ("Country", "country", filter_opts.get("country", []), filter_selected.get("country", [])),
        ] if has_country_col else [])),
        ("acquisition", "Acquisition", [
            ("Referer type", "referer_type", filter_opts.get("referer_type", []), filter_selected.get("referer_type", [])),
            ("UTM source",   "utm_source",   filter_opts.get("utm_source",   []), filter_selected.get("utm_source", [])),
        ]),
    ]

    # Count active filters per pane (for badges) and overall.
    total_active = sum(len(sel) for _, _, fields in panes for _, _, _, sel in fields)
    if is_bot:
        total_active += 1

    # Nav buttons
    nav_buttons = []
    for idx, (pane_id, pane_label, fields) in enumerate(panes):
        count = sum(len(sel) for _, _, _, sel in fields)
        if pane_id == "traffic" and is_bot:
            count += 1
        badge = f"<span class='count'>{count}</span>" if count else ""
        active = "active" if idx == 0 else ""
        nav_buttons.append(
            f"<button type='button' class='{active}' data-mfp-tab='{pane_id}'>"
            f"<span>{html_escape(pane_label)}</span>{badge}</button>"
        )

    # Pane bodies
    pane_bodies = []
    for idx, (pane_id, pane_label, fields) in enumerate(panes):
        sections = []
        if pane_id == "traffic":
            # is_bot radio group (single-value)
            opts = [("", "All traffic"), ("true", "Bots only"), ("false", "Humans only")]
            btns = []
            for v, lbl in opts:
                active = "active" if (is_bot or "") == v else ""
                btns.append(
                    f"<button type='button' class='filter-chip-btn solid{(' '+active) if active else ''}' "
                    f"data-mfp-single='is_bot' data-filter-value='{html_escape(v, quote=True)}' "
                    f"style='margin-right:4px;'>{html_escape(lbl)}</button>"
                )
            sections.append(
                "<div style='margin-bottom:14px;'>"
                "<div style='font-size:11px;text-transform:uppercase;font-weight:600;color:var(--ink-3);margin-bottom:6px;'>Bot vs human</div>"
                f"<div style='display:flex;flex-wrap:wrap;'>{''.join(btns)}</div>"
                "</div>"
            )
        for label, field, options, selected_vals in fields:
            if not options:
                continue
            sections.append(
                "<div style='margin-bottom:14px;'>"
                f"<div style='font-size:11px;text-transform:uppercase;font-weight:600;color:var(--ink-3);margin-bottom:6px;'>{html_escape(label)}</div>"
                + checklist(field, options, selected_vals) +
                "</div>"
            )
        display = "block" if idx == 0 else "none"
        if not sections:
            sections.append(
                "<div style='font-size:12px;color:var(--ink-3);padding:8px 0;'>No values in this range.</div>"
            )
        pane_bodies.append(
            f"<div class='mfp-pane' data-mfp-pane='{pane_id}' style='display:{display};'>"
            f"{''.join(sections)}</div>"
        )

    # Clear-all href: drops every multi-filter + is_bot, keeps dates/mode/columns.
    keep_keys = {"from", "to", "mode", "chart", "columns", "view"}
    clear_qs = _lv2_build_qs({k: v for k, v in params.items() if k in keep_keys})

    popover_html = (
        "<div class='popover mfp' id='pop-add-filter'>"
        "<div class='mfp-head'>"
        "<div>"
        "<h4 style='font-size:13px;font-weight:600;color:var(--ink-1);'>All filters</h4>"
        "<p style='font-size:12px;color:var(--ink-3);margin-top:2px;'>Tick what you want, then click Apply.</p>"
        "</div>"
        f"<button type='button' class='mfp-close btn btn-icon btn-ghost' data-mfp-close aria-label='Close'>{_lv2_icon('x')}</button>"
        "</div>"
        "<div class='mfp-body'>"
        f"<div class='mfp-nav'>{''.join(nav_buttons)}</div>"
        f"<div class='mfp-main'>{''.join(pane_bodies)}</div>"
        "</div>"
        "<div class='mfp-foot'>"
        f"<span data-mfp-active-count>{total_active} active</span>"
        "<div style='display:flex;gap:6px;'>"
        f"<a class='btn btn-sm' href='/logs?{clear_qs}' "
        f"hx-get='/logs?{clear_qs}' hx-target='#results' hx-push-url='true' hx-swap='innerHTML' "
        "data-mfp-close>Clear all</a>"
        "<button type='button' class='btn btn-sm btn-primary' data-mfp-apply>Apply</button>"
        "</div>"
        "</div>"
        "</div>"
    )
    return popover_html, total_active


def _lv2_filter_bar(
    *,
    search: Optional[str],
    date_from: Optional[str],
    date_to: Optional[str],
    show_chart: bool,
    schema_cols: List[str],
    visible_cols: List[str],
    params: Dict[str, Any],
    filter_opts: Optional[Dict[str, List[str]]] = None,
    filter_selected: Optional[Dict[str, List[str]]] = None,
    is_bot: Optional[str] = None,
    has_country_col: bool = False,
    avail_dates: Optional[List[str]] = None,
) -> str:
    # Search input
    search_val = html_escape(search or "", quote=True)
    hidden_kvs = []
    for hk, hv in params.items():
        if hk in ("search", "page") or hv in (None, ""):
            continue
        if isinstance(hv, (list, tuple)):
            for item in hv:
                if item in (None, ""):
                    continue
                hidden_kvs.append(
                    f"<input type='hidden' name='{html_escape(hk)}' value='{html_escape(str(item), quote=True)}'>"
                )
        else:
            hidden_kvs.append(
                f"<input type='hidden' name='{html_escape(hk)}' value='{html_escape(str(hv), quote=True)}'>"
            )
    hidden_inputs_html = "".join(hidden_kvs)
    search_html = (
        "<div class='search-input'>"
        f"{_lv2_icon('search')}"
        "<form method='get' action='/logs' style='display:flex;flex:1;min-width:0;'>"
        f"{hidden_inputs_html}"
        f"<input type='text' name='search' value='{search_val}' "
        "placeholder='Search or filter   e.g.  status:404  is:bot  path:/api' "
        "spellcheck='false' autocomplete='off'>"
        "</form>"
        "<span class='sx-kbd'>/</span>"
        "</div>"
    )

    avail_from = avail_dates[0] if avail_dates else None
    avail_to = avail_dates[-1] if avail_dates else None

    # Presets anchor on the latest available date (not today's wall clock), so
    # each window always lands on real data. Falls back to today if nothing
    # is ingested yet.
    try:
        preset_anchor = (
            datetime.fromisoformat(avail_to).date()
            if avail_to
            else datetime.utcnow().date()
        )
    except ValueError:
        preset_anchor = datetime.utcnow().date()

    # Date range + presets
    def preset_link(days: Optional[int], label: str, is_active: bool) -> str:
        if days is None:
            qs = _lv2_build_qs(params, **{"from": None, "to": None, "page": None})
        else:
            frm = (preset_anchor - timedelta(days=days - 1)).isoformat()
            to = preset_anchor.isoformat()
            qs = _lv2_build_qs(params, **{"from": frm, "to": to, "page": None})
        cls = "active" if is_active else ""
        days_attr = f" data-preset-days='{days}'" if days is not None else ""
        return (
            f"<a class='{cls}' href='/logs?{qs}' "
            f"hx-get='/logs?{qs}' hx-target='#results' hx-push-url='true' hx-swap='innerHTML'"
            f"{days_attr}>"
            f"{html_escape(label)}</a>"
        )

    # Compute active preset by span in days (anchored on the latest data day).
    active_preset_days: Optional[int] = None
    if date_from and date_to:
        try:
            f_d = datetime.fromisoformat(date_from).date()
            t_d = datetime.fromisoformat(date_to).date()
            span = (t_d - f_d).days + 1
            if t_d == preset_anchor and span in {1, 3, 7, 14, 30}:
                active_preset_days = span
        except ValueError:
            active_preset_days = None
    is_all_time = bool(
        avail_from and avail_to
        and date_from == avail_from and date_to == avail_to
    )
    range_label = "All time"
    if date_from and date_to and not is_all_time:
        if date_from == date_to:
            range_label = date_from
        else:
            range_label = f"{date_from} → {date_to}"

    preset_btns = "".join([
        preset_link(1, "24h", active_preset_days == 1),
        preset_link(3, "3d", active_preset_days == 3),
        preset_link(7, "7d", active_preset_days == 7),
        preset_link(14, "14d", active_preset_days == 14),
        preset_link(30, "30d", active_preset_days == 30),
    ])
    # Hidden inputs for the custom-range form so other filters survive submit.
    dr_hidden_parts: List[str] = []
    for hk, hv in params.items():
        if hk in ("from", "to", "page") or hv in (None, ""):
            continue
        if isinstance(hv, (list, tuple)):
            for item in hv:
                if item in (None, ""):
                    continue
                dr_hidden_parts.append(
                    f"<input type='hidden' name='{html_escape(hk)}' value='{html_escape(str(item), quote=True)}'>"
                )
        else:
            dr_hidden_parts.append(
                f"<input type='hidden' name='{html_escape(hk)}' value='{html_escape(str(hv), quote=True)}'>"
            )
    dr_hidden_html = "".join(dr_hidden_parts)
    if avail_from and avail_to:
        dr_clear_qs = _lv2_build_qs(
            params, **{"from": avail_from, "to": avail_to, "page": None}
        )
    else:
        dr_clear_qs = _lv2_build_qs(params, **{"from": None, "to": None, "page": None})
    _dr_input_style = (
        "border:1px solid var(--border);border-radius:4px;padding:5px 8px;"
        "font-size:13px;background:var(--surface);color:var(--ink-1);width:100%;"
    )
    avail_from_attr = f" data-avail-from='{html_escape(avail_from or '', quote=True)}'" if avail_from else ""
    avail_to_attr = f" data-avail-to='{html_escape(avail_to or '', quote=True)}'" if avail_to else ""
    date_range_html = (
        f"<div class='date-range' data-date-range{avail_from_attr}{avail_to_attr}>"
        "<span class='dr-trigger' data-popover-trigger='pop-date-range' "
        "role='button' tabindex='0' "
        "style='display:inline-flex;align-items:center;gap:6px;cursor:pointer;'>"
        f"{_lv2_icon('calendar')}"
        f"<span class='dr-label'>{html_escape(range_label)}</span>"
        "</span>"
        f"<div class='dr-presets'>{preset_btns}</div>"
        "<div class='popover' id='pop-date-range' "
        "style='top:38px; left:0; min-width:260px;'>"
        "<div class='popover-title'>Custom date range</div>"
        "<form method='get' action='/logs'>"
        f"{dr_hidden_html}"
        "<div class='pop-row'><label>From</label>"
        f"<input type='date' name='from' value='{html_escape(date_from or '', quote=True)}' "
        f"style='{_dr_input_style}'></div>"
        "<div class='pop-row'><label>To</label>"
        f"<input type='date' name='to' value='{html_escape(date_to or '', quote=True)}' "
        f"style='{_dr_input_style}'></div>"
        "<div class='pop-actions'>"
        f"<a href='/logs?{dr_clear_qs}' class='btn btn-sm btn-ghost' "
        f"hx-get='/logs?{dr_clear_qs}' hx-target='#results' "
        "hx-push-url='true' hx-swap='innerHTML'>All time</a>"
        "<button type='submit' class='btn btn-sm btn-primary'>Apply</button>"
        "</div>"
        "</form>"
        "</div>"
        "</div>"
    )

    # Add filter button (badge count when filters active)
    mfp_html, mfp_count = _lv2_more_filters_popover(
        params=params,
        filter_opts=filter_opts or {},
        filter_selected=filter_selected or {},
        is_bot=is_bot,
        has_country_col=has_country_col,
    )
    count_badge = f"<span class='count' style='background:var(--accent);color:#fff;border-radius:999px;padding:0 6px;font-size:10px;min-width:16px;text-align:center;'>{mfp_count}</span>" if mfp_count else ""
    add_filter_btn = (
        "<button type='button' class='filter-chip-btn' data-popover-trigger='pop-add-filter'>"
        f"{_lv2_icon('plus')}Add filter{count_badge}</button>"
    )

    # Columns popover
    col_items = []
    for c in schema_cols:
        checked = "checked" if c in visible_cols else ""
        lbl = LV2_COLUMN_LABELS.get(c, c)
        col_items.append(
            f"<label><input type='checkbox' data-col-checkbox value='{html_escape(c)}' {checked}> "
            f"<span>{html_escape(lbl)}</span></label>"
        )
    columns_popover = (
        "<div class='popover-anchor'>"
        "<button type='button' class='filter-chip-btn' data-popover-trigger='pop-columns'>"
        f"{_lv2_icon('columns')}Columns</button>"
        "<div class='popover' id='pop-columns' style='top:38px; right:0;'>"
        "<div class='popover-title'>Visible columns</div>"
        f"<div class='pop-checklist'>{''.join(col_items)}</div>"
        "</div>"
        "</div>"
    )

    # Chart toggle — JS-driven (reads current URL at click time). The server
    # can't encode a correct hx-get here because the filter bar lives outside
    # #results and doesn't re-render on partial swaps; a baked URL goes stale
    # after the first click. logviewer.js handles data-chart-toggle clicks and
    # keeps the checkbox glyph in sync via htmx:afterSettle.
    chart_checked = "checked" if show_chart else ""
    chart_toggle = (
        "<a class='filter-chip-btn solid chart-toggle' href='#' data-chart-toggle "
        "style='margin-left:auto;text-decoration:none;'>"
        f"<input type='checkbox' {chart_checked} tabindex='-1' aria-hidden='true' "
        "style='pointer-events:none;margin:0;'> Chart</a>"
    )

    # Path filter rule-builder (inline disclosure under the search row)
    pf_entries_raw = params.get("pf") or []
    if isinstance(pf_entries_raw, str):
        pf_entries_raw = [pf_entries_raw]
    pf_join_val = "or" if str(params.get("pf_join", "")).lower() == "or" else "and"
    pf_open = bool(pf_entries_raw)

    def _pf_row_html(op: str = "contains", val: str = "") -> str:
        op_safe = op if op in ("contains", "not_contains", "regex") else "contains"
        opts = [
            ("contains", "Contains"),
            ("not_contains", "Does not contain"),
            ("regex", "Regex"),
        ]
        opt_html = "".join(
            f"<option value='{ov}'{' selected' if ov == op_safe else ''}>{ol}</option>"
            for ov, ol in opts
        )
        return (
            "<div class='pf-row' data-pf-row>"
            f"<select aria-label='Operator' data-pf-op>{opt_html}</select>"
            f"<input type='text' aria-label='Value' data-pf-val placeholder='value or regex' "
            f"value='{html_escape(val, quote=True)}' spellcheck='false' autocomplete='off'>"
            "<button type='button' class='pf-remove' data-pf-remove aria-label='Remove rule'>×</button>"
            "</div>"
        )

    pf_rows_html = ""
    for entry in pf_entries_raw:
        if not isinstance(entry, str) or ":" not in entry:
            continue
        op_part, _, val_part = entry.partition(":")
        if op_part not in ("contains", "not_contains", "regex") or not val_part:
            continue
        pf_rows_html += _pf_row_html(op_part, val_part)
    if not pf_rows_html:
        pf_rows_html = _pf_row_html()

    pf_count = len(pf_entries_raw)
    pf_count_badge = (
        f"<span class='count' style='background:var(--accent);color:#fff;border-radius:999px;"
        f"padding:0 6px;font-size:10px;min-width:16px;text-align:center;margin-left:4px;'>{pf_count}</span>"
        if pf_count else ""
    )
    pf_open_attr = " data-pf-open" if pf_open else ""
    pf_template_row = _pf_row_html().replace("'", "&#39;")
    path_filter_html = (
        f"<div class='path-filter'{pf_open_attr} data-pf-template='{pf_template_row}'>"
        "<div class='path-filter-head'>"
        "<button type='button' class='filter-chip-btn' data-pf-toggle aria-expanded='"
        f"{'true' if pf_open else 'false'}'>"
        f"Path filter{pf_count_badge}<span style='margin-left:4px;'>▾</span></button>"
        "<span class='pf-label' style='margin-left:8px;'>Match</span>"
        "<div class='pf-join-seg' role='radiogroup' aria-label='Match mode'>"
        f"<button type='button' data-pf-join='and' class='{'active' if pf_join_val == 'and' else ''}' "
        f"aria-pressed='{'true' if pf_join_val == 'and' else 'false'}'>ALL</button>"
        f"<button type='button' data-pf-join='or' class='{'active' if pf_join_val == 'or' else ''}' "
        f"aria-pressed='{'true' if pf_join_val == 'or' else 'false'}'>ANY</button>"
        "</div>"
        "<button type='button' class='filter-chip-btn' data-pf-add>+ Add rule</button>"
        "<button type='button' class='filter-chip-btn solid' data-pf-apply>Apply</button>"
        "<button type='button' class='filter-chip-btn' data-pf-clear>Clear</button>"
        "</div>"
        "<div class='path-filter-body'>"
        f"<div class='pf-rules' data-pf-rules>{pf_rows_html}</div>"
        "</div>"
        "</div>"
    )

    return (
        "<div class='filter-bar'>"
        f"{search_html}"
        f"{date_range_html}"
        f"<div class='popover-anchor'>{add_filter_btn}{mfp_html}</div>"
        f"{columns_popover}"
        f"{chart_toggle}"
        "</div>"
        f"{path_filter_html}"
    )


# Short chip labels by canonical field name.
_LV2_CHIP_LABELS = {
    "status": "status", "status_class": "status_class", "method": "method",
    "is_bot": "is", "bot_family": "bot_family", "bot_category": "bot_category",
    "url_group": "url_group", "locale": "locale", "country": "country",
    "referer_type": "referer_type", "utm_source": "utm", "path": "path",
    "search": "search", "view": "view",
}

# URL param name that stores values for each canonical field. `is_bot` takes
# the raw "true"/"false"; others are CSV lists.
_LV2_URL_PARAM_BY_FIELD = {
    "status": "status", "status_class": "status_class", "method": "method",
    "is_bot": "is_bot", "bot_family": "bot_family", "bot_category": "bot_category",
    "url_group": "url_group", "locale": "locale", "country": "country",
    "referer_type": "referer_type", "utm_source": "utm_source",
}


def _lv2_strip_search_operator(raw_search: str, field: str, value: str, negated: bool) -> str:
    """Return `raw_search` with the first matching `[-]key:value` token removed.

    Handles both `utm:` and `utm_source:` shortcuts, and the `status:4xx` →
    status_class shorthand. If nothing matches, returns the input unchanged.
    """
    tokens = (raw_search or "").split()
    # Candidate token surface forms to try.
    candidates: List[str] = []
    neg = "-" if negated else ""
    if field == "is_bot":
        candidates.append(f"{neg}is:{'bot' if value == 'true' else 'human'}")
    elif field == "status_class":
        candidates.append(f"{neg}status:{value}xx")
        candidates.append(f"{neg}status_class:{value}")
    elif field == "utm_source":
        candidates.append(f"{neg}utm:{value}")
        candidates.append(f"{neg}utm_source:{value}")
    else:
        candidates.append(f"{neg}{field}:{value}")
    cand_lower = {c.lower() for c in candidates}
    out: List[str] = []
    dropped = False
    for tok in tokens:
        if not dropped and tok.lower() in cand_lower:
            dropped = True
            continue
        out.append(tok)
    return " ".join(out)


def _lv2_active_chips(
    params: Dict[str, Any],
    schema_cols: List[str],
    *,
    op_includes: Optional[Dict[str, List[str]]] = None,
    op_excludes: Optional[Dict[str, List[str]]] = None,
    raw_search: str = "",
) -> str:
    """Emit a removable chip for each active filter. Empty string when nothing active."""
    op_includes = op_includes or {}
    op_excludes = op_excludes or {}

    # URL-origin chip fields (param_key == field name). Values are CSV or raw.
    URL_CHIP_FIELDS = [
        "status", "status_class", "method", "is_bot", "bot_family", "bot_category",
        "url_group", "locale", "country", "referer_type", "utm_source", "view",
    ]

    chips: List[str] = []

    def _append_chip(display_key: str, display_val: str, sep: str, remove_qs: str, tooltip: str) -> None:
        trunc = display_val if len(display_val) <= 32 else display_val[:32] + "…"
        chips.append(
            f"<span class='chip'>"
            f"<span class='chip-key'>{html_escape(display_key)}</span>"
            f"<span class='chip-sep'>{sep}</span>"
            f"<span class='chip-val' title='{html_escape(tooltip, quote=True)}'>{html_escape(trunc)}</span>"
            f"<a class='chip-x' href='/logs?{remove_qs}' "
            f"hx-get='/logs?{remove_qs}' hx-target='#results' hx-push-url='true' hx-swap='innerHTML' "
            f"aria-label='Remove {html_escape(display_key)}'>×</a>"
            f"</span>"
        )

    # URL-origin chips
    for field in URL_CHIP_FIELDS:
        val = params.get(field)
        if val in (None, ""):
            continue
        display_key = _LV2_CHIP_LABELS.get(field, field)
        remove_qs = _lv2_build_qs(params, **{field: None, "page": None})
        _append_chip(display_key, str(val), "=", remove_qs, str(val))

    # Free-text search chip
    if params.get("search"):
        display_val = str(params["search"])
        remove_qs = _lv2_build_qs(params, search=None, page=None)
        _append_chip("search", display_val, "~", remove_qs, display_val)

    # Operator-origin include chips (filter came from `status:404` etc.)
    for field, vals in op_includes.items():
        if field == "path":
            label = "path"
        else:
            label = _LV2_CHIP_LABELS.get(field, field)
        for v in vals:
            # Skip values that are already represented as URL-origin chips.
            url_field_name = _LV2_URL_PARAM_BY_FIELD.get(field)
            if url_field_name:
                url_val = params.get(url_field_name)
                if url_val and v in str(url_val).split(","):
                    continue
            new_search = _lv2_strip_search_operator(raw_search, field, v, negated=False)
            remove_params = dict(params)
            if new_search:
                remove_params["search"] = new_search
            else:
                remove_params.pop("search", None)
            remove_qs = _lv2_build_qs(remove_params, page=None)
            display_val = "bot" if field == "is_bot" and v == "true" else ("human" if field == "is_bot" and v == "false" else v)
            sep = "=" if field != "path" else "^"
            _append_chip(label, display_val, sep, remove_qs, v)

    # Operator-origin exclude chips (`-utm:direct`)
    for field, vals in op_excludes.items():
        if field == "path":
            label = "path"
        else:
            label = _LV2_CHIP_LABELS.get(field, field)
        for v in vals:
            new_search = _lv2_strip_search_operator(raw_search, field, v, negated=True)
            remove_params = dict(params)
            if new_search:
                remove_params["search"] = new_search
            else:
                remove_params.pop("search", None)
            remove_qs = _lv2_build_qs(remove_params, page=None)
            display_val = "bot" if field == "is_bot" and v == "true" else ("human" if field == "is_bot" and v == "false" else v)
            _append_chip(label, display_val, "≠", remove_qs, v)

    # ── Path rule-builder chips (pf=<op>:<value>) ──
    pf_entries = params.get("pf")
    if pf_entries:
        if not isinstance(pf_entries, (list, tuple)):
            pf_entries = [pf_entries]
        _PF_SEP = {"contains": "~", "not_contains": "!~", "regex": "re"}
        for idx, entry in enumerate(pf_entries):
            if not isinstance(entry, str) or ":" not in entry:
                continue
            op_part, _, val_part = entry.partition(":")
            sep = _PF_SEP.get(op_part)
            if not sep or not val_part:
                continue
            # Remove only this index from the list (preserves duplicates).
            remaining = [e for i, e in enumerate(pf_entries) if i != idx]
            remove_params = dict(params)
            if remaining:
                remove_params["pf"] = remaining
                if len(remaining) <= 1:
                    remove_params.pop("pf_join", None)
            else:
                remove_params.pop("pf", None)
                remove_params.pop("pf_join", None)
            remove_qs = _lv2_build_qs(remove_params, page=None)
            _append_chip("path", val_part, sep, remove_qs, val_part)

    if not chips:
        return ""
    clear_qs = _lv2_build_qs(
        {k: v for k, v in params.items() if k in ("from", "to", "mode", "chart", "columns")}
    )
    return (
        "<div class='chips-row'>"
        "<span class='chips-label'>Filters</span>"
        f"{''.join(chips)}"
        f"<a class='clear-all' href='/logs?{clear_qs}' "
        f"hx-get='/logs?{clear_qs}' hx-target='#results' hx-push-url='true' hx-swap='innerHTML'>Clear all</a>"
        "</div>"
    )


def _lv2_status_pill(status: Any) -> str:
    try:
        s = int(status)
    except (TypeError, ValueError):
        return html_escape(str(status) if status is not None else "")
    cls = "status-2xx" if 200 <= s < 300 else "status-3xx" if 300 <= s < 400 else "status-4xx" if 400 <= s < 500 else "status-5xx" if 500 <= s < 600 else ""
    return f"<span class='status-pill {cls}'>{s}</span>"


def _lv2_method_pill(method: Any) -> str:
    if method is None:
        return ""
    m = html_escape(str(method))
    return f"<span class='method method-{m}'>{m}</span>"


def _lv2_agent_cell(is_bot: Any, bot_family: Any, user_agent: Any) -> str:
    is_bot_truthy = bool(is_bot) and str(is_bot).lower() not in ("false", "0", "")
    if is_bot_truthy:
        label = str(bot_family) if bot_family else "Bot"
        icon = "✦"
        cls = "bot"
    else:
        label = "Human"
        icon = "☺"
        cls = "human"
    ua_title = html_escape(str(user_agent or ""), quote=True)
    return (
        f"<span class='agent-cell {cls}' title='{ua_title}'>"
        f"<span class='agent-icon'>{icon}</span>"
        f"<span class='agent-label'>{html_escape(label)}</span>"
        "</span>"
    )


def _lv2_cell(col: str, val: Any) -> str:
    if val is None or val == "":
        return ""
    if col == "status":
        return _lv2_status_pill(val)
    if col == "method":
        return _lv2_method_pill(val)
    if col == "bytes_sent":
        try:
            return html_escape(fmt_bytes(val))
        except Exception:
            return html_escape(str(val))
    if col == "is_bot":
        return "Bot" if val and str(val).lower() not in ("false", "0") else "Human"
    s = str(val)
    if col in ("ts_utc", "ts_local", "date"):
        s = s[:19]
    if len(s) > 80 and col in {"user_agent", "referer", "referer_path", "path", "request_target", "query_string", "utm_term", "utm_content"}:
        s = s[:80] + "…"
    return html_escape(s)


def _lv2_results_card(
    *,
    rows: List[Any],
    cols: List[str],
    visible_cols: List[str],
    sort: str,
    order: str,
    page_num: int,
    per_page: int,
    total: int,
    params: Dict[str, Any],
) -> str:
    # Build combined row: col -> idx in tuple
    col_idx = {c: i for i, c in enumerate(cols)}

    # Build the agent column by combining is_bot + bot_family + user_agent.
    def agent_html(row: Any) -> str:
        ib = row[col_idx["is_bot"]] if "is_bot" in col_idx else None
        bf = row[col_idx["bot_family"]] if "bot_family" in col_idx else None
        ua = row[col_idx["user_agent"]] if "user_agent" in col_idx else ""
        return _lv2_agent_cell(ib, bf, ua)

    # Header with sortable links
    head_cells = []
    for c in visible_cols:
        label = LV2_COLUMN_LABELS.get(c, c)
        is_sorted = c == sort
        sorted_cls = " sorted" if is_sorted else ""
        next_order = "asc" if is_sorted and order == "desc" else "desc"
        qs = _lv2_build_qs(params, sort=c, order=next_order, page=None)
        arrow = "▼" if is_sorted and order == "desc" else "▲" if is_sorted else "↕"
        head_cells.append(
            f"<th class='col{sorted_cls}' data-column='{html_escape(c)}'>"
            f"<a href='/logs?{qs}' hx-get='/logs?{qs}' hx-target='#results' hx-push-url='true' hx-swap='innerHTML'>"
            f"{html_escape(label)}<span class='sort-arrow'>{arrow}</span></a></th>"
        )

    # Body rows — each row gets a composite id for the drawer endpoint. We key
    # on ts_utc (full precision) + date so /logs/detail can look up that single
    # row via WHERE ts_utc = … LIMIT 1.
    body_rows = []
    for r in rows:
        cells = []
        ts_raw = r[col_idx["ts_utc"]] if "ts_utc" in col_idx else None
        ts_str = str(ts_raw) if ts_raw is not None else ""
        date_str = ts_str[:10] if ts_str else ""
        path_raw = r[col_idx["path"]] if "path" in col_idx else None
        ip_raw = r[col_idx["edge_ip"]] if "edge_ip" in col_idx else None
        row_attrs = ""
        if ts_str and date_str:
            qs_parts = [
                f"date={url_quote(date_str, safe='')}",
                f"ts={url_quote(ts_str, safe='')}",
            ]
            if path_raw is not None:
                qs_parts.append(f"path={url_quote(str(path_raw), safe='')}")
            if ip_raw is not None:
                qs_parts.append(f"ip={url_quote(str(ip_raw), safe='')}")
            href = "/logs/detail?" + "&".join(qs_parts)
            row_attrs = (
                f" data-row-ts='{html_escape(ts_str, quote=True)}' "
                f"data-row-date='{html_escape(date_str, quote=True)}' "
                f"hx-get='{html_escape(href, quote=True)}' hx-target='#drawer' hx-swap='innerHTML' "
                "hx-trigger='click' role='button' tabindex='0'"
            )
        for c in visible_cols:
            idx = col_idx.get(c)
            val = r[idx] if idx is not None else None
            if c == "user_agent" and "is_bot" in col_idx:
                cells.append(f"<td>{agent_html(r)}</td>")
                continue
            cell_html = _lv2_cell(c, val)
            cls = ""
            if c in ("ts_utc", "ts_local", "date"):
                cls = "col-time"
            elif c == "path":
                cls = "col-path"
            elif c == "bytes_sent":
                cls = "col-num"
            elif c == "edge_ip":
                cls = "col-ip"
            elif c in ("referer", "referer_host", "referer_path", "user_agent", "query_string"):
                cls = "col-mono"
            td_class = f" class='{cls}'" if cls else ""
            cells.append(f"<td{td_class}>{cell_html}</td>")
        body_rows.append(f"<tr{row_attrs}>{''.join(cells)}</tr>")

    if not body_rows:
        body_rows.append(f"<tr><td colspan='{len(visible_cols)}'><div class='empty'>No rows match these filters.</div></td></tr>")

    # Pager
    total_pages = max(1, (total + per_page - 1) // per_page) if per_page > 0 else 1
    page_num = min(max(1, page_num), total_pages)
    first_row = (page_num - 1) * per_page + 1 if total > 0 else 0
    last_row = min(page_num * per_page, total)

    def page_link(p: int, label: str, active: bool = False, disabled: bool = False) -> str:
        if disabled:
            return f"<span class='disabled'>{label}</span>"
        qs = _lv2_build_qs(params, page=p)
        cls = " class='active'" if active else ""
        return (
            f"<a{cls} href='/logs?{qs}' hx-get='/logs?{qs}' hx-target='#results' "
            f"hx-push-url='true' hx-swap='innerHTML'>{label}</a>"
        )

    # Per-page selector
    per_page_opts = "".join(
        f"<option value='{n}'{' selected' if per_page == n else ''}>{n}</option>"
        for n in (25, 50, 100, 200)
    )
    per_page_qs_base = _lv2_build_qs(params, per_page="__N__", page=None)
    per_page_select = (
        "<label>Rows "
        "<select onchange=\"var u='/logs?'+this.value;"
        "if(window.htmx){window.htmx.ajax('GET',u,{target:'#results',swap:'innerHTML'});"
        "window.history.pushState({},'',u);}else{window.location.href=u;}\">"
        + "".join(
            f"<option value='{per_page_qs_base.replace('__N__', str(n))}'"
            f"{' selected' if per_page == n else ''}>{n}</option>"
            for n in (25, 50, 100, 200)
        )
        + "</select></label>"
    )

    # Build page number buttons (show up to 7 around current)
    page_btns = []
    start = max(1, page_num - 3)
    end = min(total_pages, page_num + 3)
    if start > 1:
        page_btns.append(page_link(1, "1"))
        if start > 2:
            page_btns.append("<span>…</span>")
    for p in range(start, end + 1):
        page_btns.append(page_link(p, str(p), active=(p == page_num)))
    if end < total_pages:
        if end < total_pages - 1:
            page_btns.append("<span>…</span>")
        page_btns.append(page_link(total_pages, str(total_pages)))

    pager = (
        "<div class='pager'>"
        f"<span>Showing <strong>{first_row:,}–{last_row:,}</strong> of <strong>{total:,}</strong></span>"
        "<div class='pager-right'>"
        f"{per_page_select}"
        f"{page_link(page_num - 1, '‹', disabled=page_num <= 1)}"
        f"<div class='pager-pages'>{''.join(page_btns)}</div>"
        f"{page_link(page_num + 1, '›', disabled=page_num >= total_pages)}"
        "</div>"
        "</div>"
    )

    toolbar = (
        "<div class='table-toolbar'>"
        f"<div class='tt-count'><strong>{total:,}</strong> rows</div>"
        "<div class='tt-right'>Click a row for details · <kbd class='kbd'>j</kbd> / <kbd class='kbd'>k</kbd> to navigate</div>"
        "</div>"
    )

    return (
        "<div class='table-wrap'>"
        f"{toolbar}"
        "<div class='table-scroll'>"
        "<table class='log-table'><thead><tr>"
        f"{''.join(head_cells)}"
        "</tr></thead><tbody>"
        f"{''.join(body_rows)}"
        "</tbody></table>"
        "</div>"
        f"{pager}"
        "</div>"
    )


# ── Phase 4 report shell ────────────────────────────────────────────────
# Wraps existing report bodies (/reports/summary, /reports/locales, /reports/gsc)
# in the new lv2 chrome. The body itself is untouched — the helpers
# kpi_card/issue_card/recommendations_section/html_table/*_chart keep rendering
# their existing markup; only the chrome and styling change.
def _lv2_report_shell(
    *,
    title: str,
    nav_key: str,
    date_from: Optional[str],
    date_to: Optional[str],
    avail: List[str],
    body: str,
    subtitle: str = "",
    extra_actions_html: str = "",
) -> HTMLResponse:
    # Params dict for the date presets (active state + hx-get targets)
    params: Dict[str, Any] = {}
    if date_from:
        params["from"] = date_from
    if date_to:
        params["to"] = date_to

    def preset_link(days: int, label: str, is_active: bool) -> str:
        today = datetime.utcnow().date()
        frm = (today - timedelta(days=days - 1)).isoformat()
        to = today.isoformat()
        qs = _lv2_build_qs(params, **{"from": frm, "to": to})
        cls = "active" if is_active else ""
        return f"<a class='{cls}' href='?{qs}'>{html_escape(label)}</a>"

    # Detect active preset
    active_days: Optional[int] = None
    if date_from and date_to:
        try:
            f_d = datetime.fromisoformat(date_from).date()
            t_d = datetime.fromisoformat(date_to).date()
            span = (t_d - f_d).days + 1
            if span in {1, 3, 7, 14, 30}:
                active_days = span
        except ValueError:
            active_days = None

    range_label = f"{date_from} → {date_to}" if date_from and date_to and date_from != date_to else (date_from or "All time")
    min_d = avail[0] if avail else ""
    max_d = avail[-1] if avail else ""
    date_bar = (
        "<div class='filter-bar'>"
        "<div class='date-range'>"
        f"{_lv2_icon('calendar')}"
        f"<span class='dr-label'>{html_escape(range_label)}</span>"
        "<div class='dr-presets'>"
        f"{preset_link(3, '3d', active_days == 3)}"
        f"{preset_link(7, '7d', active_days == 7)}"
        f"{preset_link(14, '14d', active_days == 14)}"
        f"{preset_link(30, '30d', active_days == 30)}"
        "</div>"
        "</div>"
        # Custom-range inputs (plain form; reloads page)
        "<form method='get' style='display:flex;gap:6px;align-items:center;'>"
        f"<label style='font-size:12px;color:var(--ink-3);display:inline-flex;gap:4px;align-items:center;'>From <input type='date' name='from' value='{html_escape(date_from or '', quote=True)}' min='{html_escape(min_d, quote=True)}' max='{html_escape(max_d, quote=True)}' style='border:1px solid var(--border);border-radius:4px;padding:3px 6px;font-size:12px;'></label>"
        f"<label style='font-size:12px;color:var(--ink-3);display:inline-flex;gap:4px;align-items:center;'>To <input type='date' name='to' value='{html_escape(date_to or '', quote=True)}' min='{html_escape(min_d, quote=True)}' max='{html_escape(max_d, quote=True)}' style='border:1px solid var(--border);border-radius:4px;padding:3px 6px;font-size:12px;'></label>"
        "<button type='submit' class='btn btn-sm'>Apply</button>"
        "</form>"
        "</div>"
    )

    page_head_html = (
        "<div class='page-head'>"
        f"<div><div class='page-title'>{html_escape(title)}"
        + (f" <small>{html_escape(subtitle)}</small>" if subtitle else "")
        + "</div></div>"
        f"<div class='page-actions'>{extra_actions_html}</div>"
        "</div>"
    )

    body_html = page_head_html + date_bar + f"<div id='results'>{body}</div>"
    return _lv2_page(f"Log analyser — {title}", nav_key, body_html)


# ── Phase 3 chart emitter ────────────────────────────────────────────────
# Area SVG over time for one of several metrics (requests / bytes / bots /
# error-rate). The card exposes t0/t1 (epoch ms of the rendered range) and
# the active metric on the root div so the JS island can translate pointer-x
# into a timestamp for brushing and format the hover tooltip.
_CHART_METRIC_OPTIONS: List[Tuple[str, str]] = [
    ("requests", "Requests"),
    ("status", "Status class"),
    ("bytes", "Bytes sent"),
    ("bots", "Humans vs bots"),
    ("errors", "Error rate"),
]


def _fmt_bytes_short(n: float) -> str:
    """Human-friendly byte label (1.2 KB / 3.4 MB / …). 0 → '0 B'."""
    n = float(n or 0)
    if n <= 0:
        return "0 B"
    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    i = 0
    while n >= 1024 and i < len(units) - 1:
        n /= 1024.0
        i += 1
    if n >= 100 or i == 0:
        return f"{int(round(n))} {units[i]}"
    if n >= 10:
        return f"{n:.1f} {units[i]}"
    return f"{n:.2f} {units[i]}"


def _lv2_chart_card(
    *,
    paths: List[str],
    where: str,
    date_from: Optional[str],
    date_to: Optional[str],
    bt0: Optional[int],
    bt1: Optional[int],
    params: Dict[str, Any],
    metric: str = "requests",
) -> str:
    # Compute range endpoints as unix ms. Partitions are keyed by local date
    # but ts_utc can span wider; querying MIN/MAX of the filtered data gives a
    # reliable range. Brush overrides when set.
    if bt0 is not None and bt1 is not None:
        t0_ms, t1_ms = sorted((int(bt0), int(bt1)))
    else:
        mm_sql = f"SELECT EPOCH_MS(MIN(ts_utc)), EPOCH_MS(MAX(ts_utc)) FROM t {where};"
        try:
            _, mm = run_query(paths, mm_sql)
            t0_ms = int(mm[0][0]) if mm and mm[0][0] is not None else 0
            t1_ms = int(mm[0][1]) if mm and mm[0][1] is not None else 0
        except Exception:
            return ""
        if not t0_ms or not t1_ms:
            return (
                "<div class='chart-card'>"
                "<div class='chart-head'><div class='chart-title'><h3>Requests over time</h3>"
                "<span class='ch-total'>0 requests</span></div></div>"
                "<div class='empty' style='padding:40px;'>No data in range.</div>"
                "</div>"
            )

    if t1_ms <= t0_ms:
        t1_ms = t0_ms + 60_000

    N_BUCKETS = 60
    bucket_ms = max(1, (t1_ms - t0_ms) / N_BUCKETS)

    # Always compute over the UN-BRUSHED range so the overlay can show the
    # brushed region against the full chart. We reuse the same WHERE clauses
    # passed in, but remove any brush clause the caller included.
    chart_where = where  # already excludes brush because caller builds WHERE from brush-aware clauses

    # Per-metric SQL. Each variant returns one row per bucket; the `select_cols`
    # text names the value columns the renderer expects below.
    bucket_expr = f"CAST(FLOOR((EPOCH_MS(ts_utc) - {t0_ms}) / {bucket_ms:.6f}) AS BIGINT) AS bucket"
    if metric == "bytes":
        select_cols = f"{bucket_expr}, SUM(COALESCE(bytes_sent, 0)) AS v"
    elif metric == "status":
        select_cols = (
            f"{bucket_expr}, "
            "COUNT(*) FILTER (WHERE status >= 200 AND status < 300) AS s2xx, "
            "COUNT(*) FILTER (WHERE status >= 300 AND status < 400) AS s3xx, "
            "COUNT(*) FILTER (WHERE status >= 400 AND status < 500) AS s4xx, "
            "COUNT(*) FILTER (WHERE status >= 500 AND status < 600) AS s5xx"
        )
    elif metric == "bots":
        select_cols = (
            f"{bucket_expr}, "
            "COUNT(*) FILTER (WHERE is_bot = true) AS v_bot, "
            "COUNT(*) FILTER (WHERE is_bot IS NOT TRUE) AS v_human"
        )
    elif metric == "errors":
        select_cols = (
            f"{bucket_expr}, "
            "COUNT(*) FILTER (WHERE status >= 400 AND status < 600) AS err, "
            "COUNT(*) AS tot"
        )
    else:  # "requests"
        select_cols = f"{bucket_expr}, COUNT(*) AS v"

    chart_sql = f"""
    SELECT {select_cols}
    FROM t {chart_where}
    GROUP BY bucket
    HAVING bucket >= 0 AND bucket < {N_BUCKETS}
    ORDER BY bucket;
    """
    try:
        _, rows = run_query(paths, chart_sql)
    except Exception:
        return ""

    # Normalise query results into a list of layers per bucket. Layers stack
    # bottom-up so index 0 is the bottom area.
    # layer_info: list of (label, fill, stroke)
    if metric == "bytes":
        layer_info = [("Bytes", "#c9c4b0", "#857f67")]
        buckets: List[List[float]] = [[0.0] for _ in range(N_BUCKETS)]
        for br, v in rows:
            i = max(0, min(N_BUCKETS - 1, int(br)))
            buckets[i] = [float(v or 0)]
        # errors_den holds raw denominators for tooltip context; unused here.
        errors_den = [0] * N_BUCKETS
    elif metric == "status":
        layer_info = [
            ("2xx", "#b7d1b5", "#3d7048"),
            ("3xx", "#a9bfde", "#3a5a86"),
            ("4xx", "#e6c888", "#8a6824"),
            ("5xx", "#d6a4a4", "#8a3a3a"),
        ]
        buckets = [[0.0, 0.0, 0.0, 0.0] for _ in range(N_BUCKETS)]
        for br, s2, s3, s4, s5 in rows:
            i = max(0, min(N_BUCKETS - 1, int(br)))
            buckets[i] = [float(s2 or 0), float(s3 or 0), float(s4 or 0), float(s5 or 0)]
        errors_den = [0] * N_BUCKETS
    elif metric == "bots":
        layer_info = [
            ("Humans", "#b7d1b5", "#3d7048"),
            ("Bots", "#a9bfde", "#3a5a86"),
        ]
        buckets = [[0.0, 0.0] for _ in range(N_BUCKETS)]
        for br, v_bot, v_human in rows:
            i = max(0, min(N_BUCKETS - 1, int(br)))
            buckets[i] = [float(v_human or 0), float(v_bot or 0)]
        errors_den = [0] * N_BUCKETS
    elif metric == "errors":
        layer_info = [("Error rate", "#e6c2c2", "#8a3a3a")]
        buckets = [[0.0] for _ in range(N_BUCKETS)]
        errors_den = [0] * N_BUCKETS
        for br, err, tot in rows:
            i = max(0, min(N_BUCKETS - 1, int(br)))
            t = int(tot or 0)
            e = int(err or 0)
            pct = (e * 100.0 / t) if t > 0 else 0.0
            buckets[i] = [pct]
            errors_den[i] = t
    else:  # "requests"
        layer_info = [("Requests", "#b7d1b5", "#3d7048")]
        buckets = [[0.0] for _ in range(N_BUCKETS)]
        for br, v in rows:
            i = max(0, min(N_BUCKETS - 1, int(br)))
            buckets[i] = [float(v or 0)]
        errors_den = [0] * N_BUCKETS

    totals = [sum(b) for b in buckets]
    if metric == "errors":
        # Fixed 0–100% scale so "27%" means the same across time ranges.
        y_max = 100.0
        has_data = any(d > 0 for d in errors_den)
    else:
        y_max = max(totals) if totals else 0
        has_data = y_max > 0

    metric_title = {
        "requests": "Requests over time",
        "status": "Status class over time",
        "bytes": "Bytes sent over time",
        "bots": "Humans vs bots over time",
        "errors": "Error rate over time",
    }.get(metric, "Requests over time")

    if not has_data:
        dropdown_empty = _lv2_chart_metric_dropdown(params, metric)
        return (
            "<div class='chart-card'>"
            "<div class='chart-head'>"
            f"<div class='chart-title'><h3>{html_escape(metric_title)}</h3>"
            "<span class='ch-total'>no data</span></div>"
            f"{dropdown_empty}"
            "</div>"
            "<div class='empty' style='padding:40px;'>No data in range.</div>"
            "</div>"
        )

    # SVG layout
    L, R, T, B = 34, 12, 10, 22
    W, H = 1200, 160
    plot_w = W - L - R
    plot_h = H - T - B

    def x_at(i: int) -> float:
        return L + (i / (N_BUCKETS - 1)) * plot_w

    def y_at(v: float) -> float:
        return T + (1 - v / y_max) * plot_h

    n_layers = len(layer_info)
    area_fills = [li[1] for li in layer_info]
    area_strokes = [li[2] for li in layer_info]
    legend_labels = [li[0] for li in layer_info]

    # Stacked cumulative: for each bucket, cumulative totals from the bottom.
    # Areas drawn bottom-up, so layer 0 is at the bottom.
    lower = [0.0] * N_BUCKETS
    paths_svg: List[str] = []
    for layer in range(n_layers):
        upper = [lower[i] + buckets[i][layer] for i in range(N_BUCKETS)]
        pts_top = [f"{x_at(i):.2f},{y_at(upper[i]):.2f}" for i in range(N_BUCKETS)]
        pts_bot = [f"{x_at(i):.2f},{y_at(lower[i]):.2f}" for i in reversed(range(N_BUCKETS))]
        d = "M" + " L".join(pts_top + pts_bot) + " Z"
        paths_svg.append(
            f"<path d='{d}' fill='{area_fills[layer]}' fill-opacity='0.95' "
            f"stroke='{area_strokes[layer]}' stroke-opacity='0.4' stroke-width='1'/>"
        )
        lower = upper

    # Y-axis grid lines at 25/50/75/100% of y_max. Label formatting follows
    # the metric (counts, bytes, or percentage).
    def _fmt_axis(v: float) -> str:
        if metric == "bytes":
            return _fmt_bytes_short(v)
        if metric == "errors":
            return f"{int(round(v))}%"
        return f"{int(round(v)):,}"

    grid_html: List[str] = []
    for frac in (0.25, 0.5, 0.75, 1.0):
        yv = y_max * frac
        y = y_at(yv)
        grid_html.append(f"<line x1='{L}' x2='{W - R}' y1='{y:.2f}' y2='{y:.2f}' stroke='#ecebe7' stroke-dasharray='2,2'/>")
        grid_html.append(f"<text x='{L - 6}' y='{y + 3:.2f}' font-size='10' fill='#97938d' text-anchor='end'>{html_escape(_fmt_axis(yv))}</text>")

    # Brush overlay — rectangle showing the currently-brushed window
    brush_html = ""
    if bt0 is not None and bt1 is not None and t1_ms > t0_ms:
        a, b = sorted((int(bt0), int(bt1)))
        xa = L + ((a - t0_ms) / (t1_ms - t0_ms)) * plot_w
        xb = L + ((b - t0_ms) / (t1_ms - t0_ms)) * plot_w
        xa = max(L, min(W - R, xa))
        xb = max(L, min(W - R, xb))
        brush_html = (
            f"<rect x='{xa:.2f}' y='{T}' width='{max(1, xb-xa):.2f}' height='{plot_h:.2f}' "
            "fill='rgba(47,92,197,0.08)' stroke='#2f5cc5' stroke-dasharray='3,3' "
            "stroke-width='1' data-brush-overlay/>"
        )

    # X-axis date labels (5 equally-spaced ticks)
    from datetime import timezone
    tick_labels: List[str] = []
    for i in range(5):
        frac = i / 4
        tms = t0_ms + frac * (t1_ms - t0_ms)
        dt = datetime.fromtimestamp(tms / 1000.0, tz=timezone.utc)
        span_hours = (t1_ms - t0_ms) / 3_600_000
        if span_hours <= 48:
            label = dt.strftime("%H:%M")
        else:
            label = dt.strftime("%b %d")
        tick_labels.append(label)
    xaxis_html = "".join(f"<span>{html_escape(lbl)}</span>" for lbl in tick_labels)

    # Legend with per-series totals. For error-rate the "total" concept is
    # a weighted average, not a sum — show error count / total requests instead.
    def _fmt_legend_val(layer: int, val: float) -> str:
        if metric == "bytes":
            return _fmt_bytes_short(val)
        return f"{int(round(val)):,}"

    if metric == "errors":
        err_sum = sum(b[0] * errors_den[i] / 100.0 for i, b in enumerate(buckets))
        den_sum = sum(errors_den)
        avg_pct = (err_sum * 100.0 / den_sum) if den_sum > 0 else 0.0
        legend_html = (
            f"<span><span class='lg-swatch' style='background:{area_fills[0]};outline:1px solid {area_strokes[0]}33;'></span>"
            f"{html_escape(legend_labels[0])} "
            f"<strong style='color:var(--ink-1);margin-left:2px;'>{avg_pct:.1f}%</strong> "
            f"<span style='color:var(--ink-4);margin-left:2px;'>({int(round(err_sum)):,} of {den_sum:,})</span>"
            "</span>"
        )
    else:
        series_totals = [sum(b[layer] for b in buckets) for layer in range(n_layers)]
        legend_html = "".join(
            f"<span><span class='lg-swatch' style='background:{area_fills[i]};outline:1px solid {area_strokes[i]}33;'></span>"
            f"{html_escape(legend_labels[i])} <strong style='color:var(--ink-1);margin-left:2px;'>{html_escape(_fmt_legend_val(i, series_totals[i]))}</strong></span>"
            for i in range(n_layers)
        )

    # Per-bucket invisible hitboxes. Attribute names vary by metric so the
    # client can render the right tooltip payload.
    hit_w = plot_w / N_BUCKETS
    hitbox_html: List[str] = []
    for i in range(N_BUCKETS):
        hx = L + i * hit_w
        b0 = int(round(t0_ms + i * bucket_ms))
        b1 = int(round(t0_ms + (i + 1) * bucket_ms))
        cx = x_at(i)
        if metric == "bots":
            v_human, v_bot = buckets[i]
            extra = f"data-human='{int(round(v_human))}' data-bot='{int(round(v_bot))}'"
        elif metric == "status":
            s2, s3, s4, s5 = buckets[i]
            extra = (
                f"data-s2='{int(round(s2))}' data-s3='{int(round(s3))}' "
                f"data-s4='{int(round(s4))}' data-s5='{int(round(s5))}'"
            )
        elif metric == "errors":
            pct = buckets[i][0]
            den = errors_den[i]
            num = int(round(pct * den / 100.0))
            extra = f"data-pct='{pct:.2f}' data-err='{num}' data-den='{den}'"
        elif metric == "bytes":
            extra = f"data-v='{int(round(buckets[i][0]))}'"
        else:  # requests
            extra = f"data-v='{int(round(buckets[i][0]))}'"
        hitbox_html.append(
            f"<rect data-chart-hitbox x='{hx:.2f}' y='{T}' "
            f"width='{hit_w:.4f}' height='{plot_h:.2f}' "
            f"data-idx='{i}' data-cx='{cx:.2f}' "
            f"data-t0='{b0}' data-t1='{b1}' {extra}/>"
        )

    # Header summary text under the title. Counts for count-like metrics,
    # total bytes for bytes, weighted average for error rate.
    if metric == "bytes":
        total_val = sum(b[0] for b in buckets)
        summary_html = f"<strong>{html_escape(_fmt_bytes_short(total_val))}</strong> sent · {N_BUCKETS} buckets"
    elif metric == "bots":
        tot = int(sum(totals))
        summary_html = f"<strong>{tot:,}</strong> requests · {N_BUCKETS} buckets"
    elif metric == "errors":
        den_sum = sum(errors_den)
        err_sum = int(round(sum(b[0] * errors_den[i] / 100.0 for i, b in enumerate(buckets))))
        avg_pct = (err_sum * 100.0 / den_sum) if den_sum > 0 else 0.0
        summary_html = f"<strong>{avg_pct:.1f}%</strong> avg · {err_sum:,} of {den_sum:,}"
    else:
        tot = int(sum(totals))
        summary_html = f"<strong>{tot:,}</strong> requests · {N_BUCKETS} buckets"

    # Clear-brush chip when active
    brush_chip = ""
    if bt0 is not None and bt1 is not None:
        clear_qs = _lv2_build_qs(params, bt0=None, bt1=None, page=None)
        brush_chip = (
            f"<span class='chart-selection'>brushed window "
            f"<a href='/logs?{clear_qs}' hx-get='/logs?{clear_qs}' hx-target='#results' "
            "hx-push-url='true' hx-swap='innerHTML' style='color:var(--accent-ink);'>×</a>"
            "</span>"
        )

    dropdown_html = _lv2_chart_metric_dropdown(params, metric)

    return (
        "<div class='chart-card' data-lv2-chart "
        f"data-metric='{html_escape(metric, quote=True)}' "
        f"data-t0='{t0_ms}' data-t1='{t1_ms}' "
        f"data-bt0='{bt0 if bt0 is not None else ''}' data-bt1='{bt1 if bt1 is not None else ''}' "
        f"data-plot-l='{L}' data-plot-r='{R}' data-plot-t='{T}' data-plot-h='{plot_h}'>"
        "<div class='chart-head'>"
        "<div class='chart-title'>"
        f"<h3>{html_escape(metric_title)}</h3>"
        f"<span class='ch-total'>{summary_html}</span>"
        f"{brush_chip}"
        "</div>"
        f"<div class='chart-legend'>{legend_html}</div>"
        f"{dropdown_html}"
        "</div>"
        f"<svg class='chart-svg' viewBox='0 0 {W} {H}' preserveAspectRatio='none'>"
        f"{''.join(grid_html)}"
        f"{''.join(paths_svg)}"
        f"{brush_html}"
        f"<line data-chart-hover-line x1='0' x2='0' y1='{T}' y2='{T + plot_h}'/>"
        f"{''.join(hitbox_html)}"
        "</svg>"
        f"<div class='chart-xaxis' style='padding-left:{L}px;padding-right:{R}px;'>{xaxis_html}</div>"
        "<div class='chart-tooltip' data-chart-tooltip aria-hidden='true'></div>"
        "</div>"
    )


def _lv2_chart_metric_dropdown(params: Dict[str, Any], current: str) -> str:
    """Right-aligned <select> in the chart header. Each option pre-builds the
    target URL with chart_metric swapped; JS reads data-href on change and
    navigates via htmx (falls back to full reload)."""
    options = []
    for key, label in _CHART_METRIC_OPTIONS:
        qs_val = None if key == "requests" else key
        href = "/logs?" + _lv2_build_qs(params, chart_metric=qs_val, page=None)
        selected = " selected" if key == current else ""
        options.append(
            f"<option value='{key}' data-href='{html_escape(href, quote=True)}'{selected}>{html_escape(label)}</option>"
        )
    return (
        "<label class='chart-metric' style='display:inline-flex;align-items:center;gap:6px;font-size:11.5px;color:var(--ink-3);'>"
        "<span>Metric</span>"
        f"<select data-chart-metric-sel class='chart-metric-sel'>{''.join(options)}</select>"
        "</label>"
    )


def _lv2_page(title: str, active_nav: str, body: str) -> HTMLResponse:
    """Full page shell for log viewer v2 routes. No sidebar; htmx + CSS via /static."""
    html = (
        "<!doctype html>"
        "<html lang='en'>"
        "<head>"
        "<meta charset='utf-8'>"
        "<meta name='viewport' content='width=device-width, initial-scale=1'>"
        f"<title>{html_escape(title)}</title>"
        "<link rel='preconnect' href='https://fonts.googleapis.com'>"
        "<link rel='preconnect' href='https://fonts.gstatic.com' crossorigin>"
        "<link rel='stylesheet' href='https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap'>"
        f"<link rel='stylesheet' href='/static/logviewer.css?v={_lv2_static_ver('logviewer.css')}'>"
        "<script src='https://unpkg.com/htmx.org@1.9.12'></script>"
        "<script src='https://cdn.jsdelivr.net/npm/echarts@5.5.1/dist/echarts.min.js'></script>"
        "</head>"
        f"<body data-app='logviewer-v2'>"
        f"{_lv2_topnav(active_nav)}"
        f"<main class='page'>{body}</main>"
        "<div class='drawer-backdrop' id='drawer-backdrop'></div>"
        "<aside class='drawer' id='drawer' aria-hidden='true'></aside>"
        "<div class='popover-backdrop' id='popover-backdrop'></div>"
        f"{_lv2_kb_help()}"
        f"<script src='/static/logviewer.js?v={_lv2_static_ver('logviewer.js')}' defer></script>"
        "</body></html>"
    )
    return HTMLResponse(html)


@app.get("/logs", response_class=HTMLResponse)
def log_viewer(
    request: Request,
    date_from: Optional[str] = Query(None, alias="from"),
    date_to: Optional[str] = Query(None, alias="to"),
    page_num: int = Query(1, alias="page"),
    per_page: int = Query(100, alias="per_page"),
    search: Optional[str] = Query(None),
    search_mode: Optional[str] = Query(None),
    search_field: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    status_class: Optional[str] = Query(None),
    method: Optional[str] = Query(None),
    is_bot: Optional[str] = Query(None),
    bot_family: Optional[str] = Query(None),
    bot_category: Optional[str] = Query(None),
    url_group: Optional[str] = Query(None),
    locale: Optional[str] = Query(None),
    country: Optional[str] = Query(None),
    referer_type: Optional[str] = Query(None),
    utm_source: Optional[str] = Query(None),
    include_assets: bool = Query(True),
    content_only: bool = Query(False),
    sort: str = Query("ts_utc"),
    order: str = Query("desc"),
    chart: Optional[str] = Query(None),
    mode: str = Query("rows"),
    group_by: Optional[str] = Query(None),
    group_by_2: Optional[str] = Query(None),
    sort_by: str = Query("hits"),
    bucket: Optional[str] = Query(None),
    stack_by: str = Query(""),
    limit: int = Query(500),
    preset: Optional[str] = Query(None),
    columns: Optional[str] = Query(None),
    view: Optional[str] = Query(None),
    bt0: Optional[int] = Query(None),
    bt1: Optional[int] = Query(None),
    chart_metric: Optional[str] = Query(None),
    active_preset: Optional[str] = Query(None),
    pf: List[str] = Query(default_factory=list),
    pf_join: str = Query("and"),
):
    if preset:
        return _resolve_log_preset(
            preset,
            date_from=date_from, date_to=date_to,
            extra_status=status, extra_url_group=url_group,
        )

    # Apply saved-view defaults (URL-explicit values always win).
    if view:
        _view_cfg = next((v for v in LV2_SAVED_VIEWS if v["id"] == view), None)
        if _view_cfg:
            vf = _view_cfg["filters"]
            if not status and vf.get("status"): status = vf["status"]
            if not status_class and vf.get("status_class"): status_class = vf["status_class"]
            if not is_bot and vf.get("is_bot"): is_bot = vf["is_bot"]
            if not bot_family and vf.get("bot_family"): bot_family = vf["bot_family"]
            if not bot_category and vf.get("bot_category"): bot_category = vf["bot_category"]
            if not url_group and vf.get("url_group"): url_group = vf["url_group"]
            if not utm_source and vf.get("utm_source"): utm_source = vf["utm_source"]

    status_codes = _parse_status_list(status)
    status_classes = [int(c) for c in _parse_csv_param(status_class) if c.isdigit()]
    methods = _parse_csv_param(method)
    bot_families = _parse_csv_param(bot_family)
    bot_categories = _parse_csv_param(bot_category)
    url_groups = _parse_csv_param(url_group)
    locales = _parse_csv_param(locale)
    countries = _parse_csv_param(country)
    referer_types = _parse_csv_param(referer_type)
    utm_sources = _parse_csv_param(utm_source)
    show_chart = chart == "1"
    chart_metric = (chart_metric or "requests").lower()
    if chart_metric not in {"requests", "status", "bytes", "bots", "errors"}:
        chart_metric = "requests"
    if search_mode not in LOG_SEARCH_MODE_VALUES:
        search_mode = "contains"
    if search_field not in LOG_SEARCH_FIELD_COLS:
        search_field = "any"
    if mode not in {m for m, _ in LOG_MODE_OPTIONS}:
        mode = "rows"

    avail = available_parsed_dates()

    # Default to latest available date if no range specified
    if not date_from and not date_to and avail:
        date_from = avail[-1]
        date_to = avail[-1]

    paths = list_parsed_partitions(date_from, date_to)

    # Check if 'country' column exists in the parquet files
    has_country_col = False
    if paths:
        try:
            _cols, _ = run_query(paths, "SELECT column_name FROM (DESCRIBE t) WHERE column_name = 'country'")
            has_country_col = bool(_)
        except Exception:
            has_country_col = False

    # Probe the unioned parquet schema so the Rows-mode picker knows every available column.
    schema_cols: List[str] = []
    if paths:
        try:
            schema_cols, _ = run_query(paths, "SELECT * FROM t LIMIT 0")
        except Exception:
            schema_cols = []

    # Resolve the set of visible columns for Rows mode.
    # `columns` param is a comma-separated list; empty/missing → defaults.
    requested_cols = [c.strip() for c in (columns or "").split(",") if c.strip()]
    if requested_cols:
        visible_cols = [c for c in requested_cols if c in schema_cols]
    else:
        visible_cols = [c for c in LOG_DEFAULT_COLUMNS if c in schema_cols]
    if not visible_cols and schema_cols:
        visible_cols = list(schema_cols)

    # ── Populate filter dropdown options ──
    status_opts = distinct_parsed_values("status", date_from, date_to) if paths else []
    bot_family_opts = distinct_parsed_values("bot_family", date_from, date_to) if paths else []
    bot_category_opts = distinct_parsed_values("bot_category", date_from, date_to) if paths else []
    url_group_opts = distinct_parsed_values("url_group", date_from, date_to) if paths else []
    locale_opts = distinct_parsed_values("locale", date_from, date_to) if paths else []
    country_opts = distinct_parsed_values("country", date_from, date_to) if paths and has_country_col else []
    referer_type_opts = distinct_parsed_values("referer_type", date_from, date_to) if paths else []
    utm_source_opts = distinct_parsed_values("utm_source_norm", date_from, date_to) if paths else []

    # ── Parse search operators (Phase 2) ──
    # Split operator tokens out of the raw search string, merge their values
    # into the existing filter lists for SQL. The raw `search` param stays in
    # the URL so links roundtrip; chips render the effective filter state.
    raw_search = search or ""
    cleaned_free_text, op_includes, op_excludes, forced_mode = _parse_search_operators(raw_search)
    if forced_mode:
        search_mode = forced_mode

    def _merge_ints(url_list: List[int], key: str) -> List[int]:
        extra = [int(v) for v in op_includes.get(key, []) if str(v).isdigit()]
        seen = set(url_list)
        out = list(url_list)
        for v in extra:
            if v not in seen:
                seen.add(v)
                out.append(v)
        return out

    def _merge_strs(url_list: List[str], key: str) -> List[str]:
        extra = op_includes.get(key, [])
        seen = set(url_list)
        out = list(url_list)
        for v in extra:
            if v not in seen:
                seen.add(v)
                out.append(v)
        return out

    status_codes = _merge_ints(status_codes, "status")
    status_classes = _merge_ints(status_classes, "status_class")
    methods = _merge_strs(methods, "method")
    bot_families = _merge_strs(bot_families, "bot_family")
    bot_categories = _merge_strs(bot_categories, "bot_category")
    url_groups = _merge_strs(url_groups, "url_group")
    locales = _merge_strs(locales, "locale")
    countries = _merge_strs(countries, "country")
    referer_types = _merge_strs(referer_types, "referer_type")
    utm_sources = _merge_strs(utm_sources, "utm_source")
    if not is_bot and op_includes.get("is_bot"):
        is_bot = op_includes["is_bot"][0]
    paths_include = list(op_includes.get("path", []))
    paths_exclude = list(op_excludes.get("path", []))
    # `search` passed to the SQL builder is the free-text leftover only.
    effective_search = cleaned_free_text

    # ── Parse path rule-builder params (pf=<op>:<value>, pf_join=and|or) ──
    # Rules with an unknown op, empty value, oversized regex, or invalid regex
    # are dropped silently; the cleaned list is what re-enters ui_params.
    _PF_OPS = {"contains", "not_contains", "regex"}
    _PF_REGEX_MAX = 200
    path_rules: List[Tuple[str, str]] = []
    pf_clean: List[str] = []
    for entry in (pf or []):
        if not entry:
            continue
        op_part, sep, val_part = entry.partition(":")
        if not sep:
            continue
        op_norm = op_part.strip().lower()
        if op_norm not in _PF_OPS or not val_part:
            continue
        if op_norm == "regex":
            if len(val_part) > _PF_REGEX_MAX:
                continue
            try:
                re.compile(val_part)
            except re.error:
                continue
        path_rules.append((op_norm, val_part))
        pf_clean.append(f"{op_norm}:{val_part}")
    pf_join_norm = "or" if (pf_join or "").lower() == "or" else "and"

    # ── Build canonical ui_params dict (single source of truth for URLs) ──
    show_chart = chart == "1"
    ui_params: Dict[str, Any] = {}
    if date_from: ui_params["from"] = date_from
    if date_to: ui_params["to"] = date_to
    if view: ui_params["view"] = view
    if search: ui_params["search"] = search
    if search_mode and search_mode != "contains": ui_params["search_mode"] = search_mode
    if search_field and search_field != "any": ui_params["search_field"] = search_field
    if status_codes: ui_params["status"] = ",".join(str(c) for c in status_codes)
    if status_classes: ui_params["status_class"] = ",".join(str(c) for c in status_classes)
    if methods: ui_params["method"] = ",".join(methods)
    if is_bot: ui_params["is_bot"] = is_bot
    if bot_families: ui_params["bot_family"] = ",".join(bot_families)
    if bot_categories: ui_params["bot_category"] = ",".join(bot_categories)
    if url_groups: ui_params["url_group"] = ",".join(url_groups)
    if locales: ui_params["locale"] = ",".join(locales)
    if countries: ui_params["country"] = ",".join(countries)
    if referer_types: ui_params["referer_type"] = ",".join(referer_types)
    if utm_sources: ui_params["utm_source"] = ",".join(utm_sources)
    if not include_assets: ui_params["include_assets"] = "false"
    if content_only: ui_params["content_only"] = "true"
    if mode != "rows": ui_params["mode"] = mode
    if show_chart: ui_params["chart"] = "1"
    if show_chart and chart_metric != "requests": ui_params["chart_metric"] = chart_metric
    if active_preset: ui_params["active_preset"] = active_preset
    if sort and sort != "ts_utc": ui_params["sort"] = sort
    if order and order != "desc": ui_params["order"] = order
    if per_page != 100: ui_params["per_page"] = str(per_page)
    if pf_clean:
        ui_params["pf"] = pf_clean
        if pf_join_norm == "or" and len(pf_clean) > 1:
            ui_params["pf_join"] = "or"
    default_col_set = {c for c in LOG_DEFAULT_COLUMNS if c in schema_cols}
    if visible_cols and set(visible_cols) != default_col_set:
        ui_params["columns"] = ",".join(visible_cols)

    # Shell chrome (topnav/views/filter-bar) is OUTSIDE #results and not swapped.
    chrome_html = (
        _lv2_page_head("Logs", "", mode, ui_params)
        + _lv2_views_strip(view, date_from, date_to)
        + _lv2_filter_bar(
            search=search,
            date_from=date_from,
            date_to=date_to,
            show_chart=show_chart,
            schema_cols=schema_cols,
            visible_cols=visible_cols,
            params=ui_params,
            filter_opts={
                "status":       status_opts,
                "method":       ["GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS", "PATCH"],
                "bot_family":   bot_family_opts,
                "bot_category": bot_category_opts,
                "url_group":    url_group_opts,
                "locale":       locale_opts,
                "country":      country_opts,
                "referer_type": referer_type_opts,
                "utm_source":   utm_source_opts,
            },
            filter_selected={
                "status":       [str(c) for c in status_codes],
                "method":       list(methods),
                "bot_family":   list(bot_families),
                "bot_category": list(bot_categories),
                "url_group":    list(url_groups),
                "locale":       list(locales),
                "country":      list(countries),
                "referer_type": list(referer_types),
                "utm_source":   list(utm_sources),
            },
            is_bot=is_bot,
            has_country_col=has_country_col,
            avail_dates=avail,
        )
    )

    # ── Early-return when there's no data at all ──
    if not paths:
        results_inner = (
            _render_insights_strip(date_from, date_to, mode=mode, active_preset=active_preset)
            + _lv2_active_chips(ui_params, schema_cols)
            + "<div class='lv2-card'><div class='empty'>No ingested data for this range. "
              "Adjust the date or ingest more partitions.</div></div>"
        )
        if request.headers.get("HX-Request") == "true":
            return HTMLResponse(results_inner)
        body_html = chrome_html + f"<div id='results'>{results_inner}</div>"
        return _lv2_page("Log analyser — Logs", "logs", body_html)

    # Special-case: waste_score sort forces is_bot=true (formula only meaningful for bots).
    if mode == "group" and sort_by == "waste_score":
        is_bot = "true"

    clauses = _build_log_where_clauses(
        status_codes=status_codes,
        status_classes=status_classes,
        methods=methods,
        bot_families=bot_families,
        bot_categories=bot_categories,
        url_groups=url_groups,
        locales=locales,
        countries=countries,
        referer_types=referer_types,
        utm_sources=utm_sources,
        is_bot=is_bot,
        include_assets=include_assets,
        content_only=content_only,
        search=effective_search,
        search_mode=search_mode,
        search_field=search_field,
        has_country_col=has_country_col,
        excludes=op_excludes,
        paths_include=paths_include,
        paths_exclude=paths_exclude,
        path_rules=path_rules,
        path_rules_join=pf_join_norm,
    )
    # Chart WHERE (no brush — chart shows the full requested range with a brush
    # overlay), vs. results WHERE (brush narrows to the selected window).
    chart_where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    if bt0 is not None and bt1 is not None:
        a, b = sorted((int(bt0), int(bt1)))
        clauses = clauses + [f"EPOCH_MS(ts_utc) >= {a} AND EPOCH_MS(ts_utc) <= {b}"]
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    if bt0 is not None:
        ui_params["bt0"] = str(bt0)
    if bt1 is not None:
        ui_params["bt1"] = str(bt1)

    # ── Build the results fragment body (goes inside #results) ──
    chips_html = _lv2_active_chips(ui_params, schema_cols,
                                   op_includes=op_includes, op_excludes=op_excludes,
                                   raw_search=raw_search)
    # Insights dropdown lives INSIDE #results so htmx mode toggles re-render
    # it alongside the mode-specific body (it's hidden in Rows mode).
    insights_html = _render_insights_strip(
        date_from, date_to, mode=mode, active_preset=active_preset,
    )

    if mode == "group":
        if not group_by:
            inner = insights_html + chips_html + "<div class='lv2-card'><div class='empty'>Pick a Group by column to aggregate.</div></div>"
        else:
            note = ""
            if sort_by == "waste_score":
                note = "<div class='lv2-note'>Waste-score sort forces <em>Bots only</em>.</div>"
            inner = insights_html + chips_html + note + "<div class='lv2-card'>" + _render_log_group_mode(
                paths, where,
                group_by=group_by, group_by_2=group_by_2, sort_by=sort_by, limit=int(limit),
                has_country_col=has_country_col, is_bot=is_bot,
            ) + "</div>"
    elif mode == "timeseries":
        inner = insights_html + chips_html + "<div class='lv2-card'>" + _render_log_timeseries_mode(
            paths, where,
            date_from=date_from, date_to=date_to,
            bucket=(bucket or ""), stack_by=stack_by,
        ) + "</div>"
    else:
        # Rows mode
        chart_html = ""
        if show_chart:
            chart_html = _lv2_chart_card(
                paths=paths, where=chart_where,
                date_from=date_from, date_to=date_to,
                bt0=bt0, bt1=bt1, params=ui_params,
                metric=chart_metric,
            )

        # Validate sort column against the live schema.
        sort_col = sort if sort in schema_cols else "ts_utc"
        sort_dir = "ASC" if order.lower() == "asc" else "DESC"

        per_page = min(max(per_page, 25), 200)
        page_num = max(page_num, 1)
        offset = (page_num - 1) * per_page

        count_sql = f"SELECT COUNT(*) FROM t {where};"
        _, count_rows = run_query(paths, count_sql)
        total = count_rows[0][0] if count_rows else 0

        # `ts_local` is TIMESTAMPTZ and needs casting for DuckDB to materialise
        # without pytz; `country` historically casted too.
        def _col_select(name: str) -> str:
            if name == "ts_local":
                return "CAST(ts_local AS VARCHAR) AS ts_local"
            if name == "country":
                return "CAST(country AS VARCHAR) AS country"
            return name
        # Always fetch ts_utc + path + edge_ip so rows expose a stable key to
        # the drawer (ts_utc alone is second-precision, so multiple requests
        # share it; path/ip disambiguate them).
        fetch_cols = list(visible_cols)
        for needed in ("ts_utc", "path", "edge_ip"):
            if needed in schema_cols and needed not in fetch_cols:
                fetch_cols = [needed] + fetch_cols
        select_list = ", ".join(_col_select(c) for c in fetch_cols)
        data_sql = f"""
        SELECT {select_list}
        FROM t
        {where}
        ORDER BY {sort_col} {sort_dir}
        LIMIT {per_page} OFFSET {offset};
        """
        cols, rows = run_query(paths, data_sql)

        results_card_html = _lv2_results_card(
            rows=rows,
            cols=cols,
            visible_cols=visible_cols,
            sort=sort_col,
            order=order,
            page_num=page_num,
            per_page=per_page,
            total=total,
            params=ui_params,
        )
        inner = chips_html + chart_html + results_card_html

    if request.headers.get("HX-Request") == "true":
        return HTMLResponse(inner)

    # Full page: replace the count text in page-head now that we know totals
    if mode == "rows":
        try:
            count_text = f"{total:,} matching requests"
        except Exception:
            count_text = ""
    else:
        count_text = ""
    chrome_html = (
        _lv2_page_head("Logs", count_text, mode, ui_params)
        + _lv2_views_strip(view, date_from, date_to)
        + _lv2_filter_bar(
            search=search,
            date_from=date_from,
            date_to=date_to,
            show_chart=show_chart,
            schema_cols=schema_cols,
            visible_cols=visible_cols,
            params=ui_params,
            filter_opts={
                "status":       status_opts,
                "method":       ["GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS", "PATCH"],
                "bot_family":   bot_family_opts,
                "bot_category": bot_category_opts,
                "url_group":    url_group_opts,
                "locale":       locale_opts,
                "country":      country_opts,
                "referer_type": referer_type_opts,
                "utm_source":   utm_source_opts,
            },
            filter_selected={
                "status":       [str(c) for c in status_codes],
                "method":       list(methods),
                "bot_family":   list(bot_families),
                "bot_category": list(bot_categories),
                "url_group":    list(url_groups),
                "locale":       list(locales),
                "country":      list(countries),
                "referer_type": list(referer_types),
                "utm_source":   list(utm_sources),
            },
            is_bot=is_bot,
            has_country_col=has_country_col,
            avail_dates=avail,
        )
    )
    body_html = chrome_html + f"<div id='results'>{inner}</div>"
    return _lv2_page("Log analyser — Logs", "logs", body_html)


# ── Autocomplete for the search input (Phase 2) ────────────────────────────
# Distinct-value scans are the expensive part, so cache by (field, from, to)
# with a 5-minute TTL. Prefix filtering happens in-memory after lookup.
_LV2_AUTOCOMPLETE_FIELDS = {
    "status", "method", "bot_family", "bot_category", "url_group",
    "locale", "country", "referer_type", "utm_source_norm",
}
_LV2_AC_TTL = 300.0
_LV2_AC_CACHE: Dict[Tuple[str, str, str], Tuple[float, List[str]]] = {}


def _lv2_autocomplete_values(field: str, date_from: Optional[str], date_to: Optional[str]) -> List[str]:
    key = (field, date_from or "", date_to or "")
    now = time.monotonic()
    hit = _LV2_AC_CACHE.get(key)
    if hit and (now - hit[0]) < _LV2_AC_TTL:
        return hit[1]
    vals = distinct_parsed_values(field, date_from, date_to)
    _LV2_AC_CACHE[key] = (now, vals)
    return vals


@app.get("/logs/autocomplete")
def log_autocomplete(
    field: str = Query(...),
    q: str = Query(""),
    date_from: Optional[str] = Query(None, alias="from"),
    date_to: Optional[str] = Query(None, alias="to"),
    limit: int = Query(10, ge=1, le=50),
):
    """JSON suggestions for a filter field, prefix-matched by `q`."""
    # Accept `utm` as a short alias for utm_source_norm
    real_field = "utm_source_norm" if field in ("utm", "utm_source") else field
    if real_field not in _LV2_AUTOCOMPLETE_FIELDS:
        return JSONResponse({"field": field, "suggestions": []})
    values = _lv2_autocomplete_values(real_field, date_from, date_to)
    needle = (q or "").strip().lower()
    if needle:
        suggestions = [v for v in values if needle in v.lower()][:limit]
    else:
        suggestions = values[:limit]
    return JSONResponse({"field": field, "suggestions": suggestions})


# ── Drawer fragment (Phase 3) ──────────────────────────────────────────────
_LV2_DRAWER_SECTIONS = [
    ("Request",   ["method", "path", "url_group", "request_target", "query_string", "is_parameterized", "is_resource"]),
    ("Response",  ["status", "status_class", "bytes_sent"]),
    ("Client",    ["edge_ip", "country", "locale", "user_agent", "is_bot", "bot_family", "bot_category"]),
    ("Referer",   ["referer", "referer_host", "referer_path", "referer_type"]),
    ("Attribution", ["utm_source_norm", "utm_medium", "utm_campaign", "utm_term", "utm_content"]),
]


def _lv2_drawer_fragment(entry: Any, cols: List[str], date: str, row_num: Optional[int] = None) -> str:
    col_idx = {c: i for i, c in enumerate(cols)}

    def get(col: str) -> Any:
        i = col_idx.get(col)
        return entry[i] if i is not None else None

    status_val = get("status")
    method_val = get("method")
    bytes_val = get("bytes_sent")
    ts_val = get("ts_utc")
    is_bot_val = get("is_bot")
    bot_family_val = get("bot_family")
    ua_val = get("user_agent")

    summary_items = [
        ("Status", _lv2_status_pill(status_val) if status_val is not None else "—"),
        ("Method", _lv2_method_pill(method_val) if method_val is not None else "—"),
        ("Bytes",  html_escape(fmt_bytes(bytes_val)) if bytes_val is not None else "—"),
        ("Time (UTC)", f"<span class='mono'>{html_escape(str(ts_val)[:19]) if ts_val is not None else '—'}</span>"),
        ("Agent", _lv2_agent_cell(is_bot_val, bot_family_val, ua_val)),
    ]
    summary_html = "".join(
        f"<div class='ds-item'><span class='ds-label'>{html_escape(lbl)}</span><span class='ds-value'>{val}</span></div>"
        for lbl, val in summary_items
    )

    # KV sections
    body_sections: List[str] = []
    for section_title, field_list in _LV2_DRAWER_SECTIONS:
        kvs = []
        for field in field_list:
            if field not in col_idx:
                continue
            raw = get(field)
            if raw is None or raw == "":
                continue
            if field == "bytes_sent":
                display = html_escape(f"{raw} ({fmt_bytes(raw)})")
            elif field == "status":
                display = _lv2_status_pill(raw)
            elif field == "method":
                display = _lv2_method_pill(raw)
            elif field == "is_bot":
                display = "Bot" if raw and str(raw).lower() not in ("false", "0") else "Human"
            else:
                s = str(raw)
                display = html_escape(s)
            kvs.append(
                f"<div class='kv'>"
                f"<span class='k'>{html_escape(field)}</span>"
                f"<span class='v'>{display}</span>"
                "</div>"
            )
        if kvs:
            body_sections.append(f"<h4>{html_escape(section_title)}</h4>" + "".join(kvs))

    # Quick filter buttons (path + ip). Inline `onclick` handlers run with
    # `document` in the scope chain, so `URL` resolves to `document.URL` (a
    # string) and `new URL(...)` throws "URL is not a constructor". Route
    # through helpers exposed on `window` from logviewer.js, and pass the
    # value via `data-qf-value` so we don't have to JS-escape it inline.
    path_val = get("path")
    ip_val = get("edge_ip")
    quick_filters: List[str] = []

    if path_val:
        quick_filters.append(
            "<button type='button' class='qf' "
            f"data-qf-value='{html_escape(str(path_val), quote=True)}' "
            "onclick=\"window.lvApplyFilter&amp;&amp;window.lvApplyFilter('path',this.dataset.qfValue)\">"
            "<span class='qf-icon'>+</span>Filter path</button>"
        )
    if ip_val:
        quick_filters.append(
            "<button type='button' class='qf' "
            f"data-qf-value='{html_escape(str(ip_val), quote=True)}' "
            "onclick=\"window.lvApplyFilter&amp;&amp;window.lvApplyFilter('edge_ip',this.dataset.qfValue)\">"
            "<span class='qf-icon'>+</span>Filter this IP</button>"
        )
    if ua_val:
        quick_filters.append(
            "<button type='button' class='qf' "
            f"data-qf-value='{html_escape(str(ua_val), quote=True)}' "
            "onclick=\"window.lvCopyText&amp;&amp;window.lvCopyText(this.dataset.qfValue,this)\">"
            "<span class='qf-icon'>⧉</span>Copy UA</button>"
        )

    quick_filters_html = (
        f"<div class='quick-filters'>{''.join(quick_filters)}</div>" if quick_filters else ""
    )

    # Raw JSON dump (truncate very long user-agent)
    raw_items = {}
    for c, v in zip(cols, entry):
        if v is None:
            continue
        if hasattr(v, "isoformat"):
            raw_items[c] = v.isoformat()
        elif isinstance(v, (int, float, bool, str)):
            raw_items[c] = v
        else:
            raw_items[c] = str(v)
    raw_json = html_escape(json.dumps(raw_items, default=str, indent=2))

    subtitle = html_escape(date)
    if row_num is not None:
        subtitle += f" · row {row_num}"
    return (
        "<div class='drawer-head'>"
        f"<h3>Request detail <small style='color:var(--ink-3);font-weight:500;margin-left:8px;font-size:12px;'>{subtitle}</small></h3>"
        "<div class='dh-nav'>"
        "<button data-drawer-nav='prev' title='Previous (k)'>‹</button>"
        "<button data-drawer-nav='next' title='Next (j)'>›</button>"
        "<button data-drawer-close style='margin-left:4px;' title='Close (esc)'>×</button>"
        "</div>"
        "</div>"
        f"<div class='drawer-summary'>{summary_html}</div>"
        "<div class='drawer-body'>"
        f"{''.join(body_sections)}"
        f"{quick_filters_html}"
        f"<h4>Raw record</h4><pre class='ua'>{raw_json}</pre>"
        "</div>"
    )


@app.get("/logs/detail", response_class=HTMLResponse)
def log_detail(
    request: Request,
    date: str = Query(...),
    row: Optional[int] = Query(None),
    ts: Optional[str] = Query(None),
    path: Optional[str] = Query(None),
    ip: Optional[str] = Query(None),
):
    """Show all fields for a single log entry, keyed by `ts` (preferred) or `row`.

    When called with HX-Request: true, returns only the drawer-inner HTML
    (head + summary + body) for target="#drawer" hx-swap="innerHTML". For full
    navigation (shareable deep link) it falls back to the legacy full page.
    """
    # Parquet partitions are keyed by local date while ts_utc is UTC, so a row
    # tagged with date=D may actually live in the neighboring partition. Widen
    # the lookup by ±1 day so drawer hits find the row regardless.
    try:
        d = datetime.fromisoformat(date).date()
        from_d = (d - timedelta(days=1)).isoformat()
        to_d = (d + timedelta(days=1)).isoformat()
        paths = list_parsed_partitions(from_d, to_d)
    except ValueError:
        paths = list_parsed_partitions(date, date)
    is_htmx = request.headers.get("HX-Request") == "true"

    if not paths:
        if is_htmx:
            return HTMLResponse(
                "<div class='drawer-head'><h3>Log detail</h3>"
                "<button data-drawer-close>×</button></div>"
                "<div class='drawer-body'><div class='empty'>No data for this date.</div></div>"
            )
        return page("Log Detail", "<p class='no-data'>No data for this date.</p>")

    # Select every schema column, but cast types DuckDB can't materialise to
    # Python natively (TIMESTAMPTZ, ENUM).
    detail_schema_cols, _ = run_query(paths, "SELECT * FROM t LIMIT 0")
    def _detail_col(c: str) -> str:
        if c == "ts_local":
            return "CAST(ts_local AS VARCHAR) AS ts_local"
        if c == "country":
            return "CAST(country AS VARCHAR) AS country"
        return c
    select_list = ", ".join(_detail_col(c) for c in detail_schema_cols)
    if ts:
        esc_ts = sql_escape_string(ts)
        extra_where = ""
        if path is not None and "path" in detail_schema_cols:
            extra_where += f" AND path = '{sql_escape_string(path)}'"
        if ip is not None and "edge_ip" in detail_schema_cols:
            extra_where += f" AND edge_ip = '{sql_escape_string(ip)}'"
        sql = f"""
        SELECT {select_list}
        FROM t
        WHERE (CAST(ts_utc AS VARCHAR) = '{esc_ts}'
               OR CAST(ts_utc AS VARCHAR) LIKE '{esc_ts}%')
              {extra_where}
        ORDER BY ts_utc
        LIMIT 1;
        """
    else:
        offset = max(0, int(row or 0))
        sql = f"""
        SELECT {select_list}
        FROM t
        LIMIT 1 OFFSET {offset};
        """
    cols, rows = run_query(paths, sql)
    if not rows:
        if is_htmx:
            return HTMLResponse(
                "<div class='drawer-head'><h3>Log detail</h3>"
                "<button data-drawer-close>×</button></div>"
                "<div class='drawer-body'><div class='empty'>Row not found.</div></div>"
            )
        return page("Log Detail", "<p class='no-data'>Row not found.</p>")

    if is_htmx:
        return HTMLResponse(_lv2_drawer_fragment(rows[0], cols, date, int(row) if row is not None else None))

    # Legacy full-page fallback (kept for deep-linking)
    entry = rows[0]
    items = ""
    for col_name, val in zip(cols, entry):
        display_val = val if val is not None else "<em style='color:#94a3b8'>null</em>"
        if col_name == "bytes_sent" and val is not None:
            display_val = f"{val} ({fmt_bytes(val)})"
        items += (
            f"<tr>"
            f"<td style='font-weight:600;color:#1e293b;padding:6px 16px 6px 0;white-space:nowrap;vertical-align:top;font-family:monospace;font-size:12px;'>{col_name}</td>"
            f"<td style='padding:6px 0;word-break:break-all;font-size:13px;'>{display_val}</td>"
            f"</tr>"
        )
    body = f"<p style='margin-bottom:12px;'><a href='/logs?from={date}&to={date}' style='color:#3b82f6;text-decoration:none;font-size:13px;'>&larr; Back to logs</a></p>"
    body += f"<div class='card'><table style='width:100%;border-collapse:collapse;'>{items}</table></div>"
    return page(f"Log Detail — {date}", body)