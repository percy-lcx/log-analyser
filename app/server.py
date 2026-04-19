from __future__ import annotations

import os
import threading
import time
from collections import OrderedDict
from pathlib import Path
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import duckdb
from fastapi import FastAPI, Query
from fastapi.responses import (
    HTMLResponse,
    StreamingResponse,
    PlainTextResponse,
    RedirectResponse,
)

import csv
import functools
import io
import itertools
from html import escape as html_escape

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
        ("Traffic", "/reports/traffic"),
        ("Status Codes", "/reports/status-codes"),
        ("Content", "/reports/content"),
        ("Locales", "/reports/locales"),
        ("Bots", "/reports/bots"),
        ("Referer Flow", "/reports/referer-flow"),
        ("UTM Campaigns", "/reports/utm"),
        ("Search Console", "/reports/gsc"),
        ("Settings", "/settings"),
    ]
    nav_links = "\n".join(
        f"<a href='{url}' class='nav-link' data-path='{url}'>{name}</a>"
        for name, url in nav_items
    )

    css = """<style>
*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

body {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    background: #f1f5f9;
    color: #334155;
    display: flex;
    min-height: 100vh;
}

/* ── Sidebar ── */
.sidebar {
    width: 210px;
    min-width: 210px;
    background: #1e293b;
    color: #cbd5e1;
    display: flex;
    flex-direction: column;
    position: fixed;
    top: 0; left: 0; bottom: 0;
    overflow-y: auto;
    z-index: 100;
}
.sidebar-logo {
    padding: 18px 16px;
    font-size: 13px;
    font-weight: 700;
    color: #f8fafc;
    border-bottom: 1px solid #334155;
    letter-spacing: 0.5px;
    text-transform: uppercase;
    line-height: 1.3;
}
.sidebar-logo span { display: block; font-size: 10px; font-weight: 400; color: #64748b; text-transform: none; margin-top: 2px; letter-spacing: 0; }
.sidebar-section {
    padding: 14px 16px 4px;
    font-size: 10px;
    color: #475569;
    text-transform: uppercase;
    letter-spacing: 1px;
    font-weight: 600;
}
.nav-link {
    display: block;
    padding: 7px 16px;
    color: #94a3b8;
    text-decoration: none;
    font-size: 13px;
    border-left: 3px solid transparent;
    transition: background 0.12s, color 0.12s;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
}
.nav-link:hover { background: #334155; color: #e2e8f0; border-left-color: #475569; }
.nav-link.active { background: #0f172a; color: #60a5fa; border-left-color: #3b82f6; font-weight: 600; }

/* ── Main content ── */
.main {
    margin-left: 210px;
    flex: 1;
    display: flex;
    flex-direction: column;
    min-height: 100vh;
    min-width: 0;
}
.topbar {
    background: #fff;
    border-bottom: 1px solid #e2e8f0;
    padding: 14px 24px;
    position: sticky;
    top: 0;
    z-index: 50;
}
.topbar { display: flex; align-items: center; }
.topbar h1 { font-size: 18px; font-weight: 700; color: #1e293b; }
.content { padding: 20px 24px; flex: 1; overflow: hidden; }

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
// Highlight the active nav link based on current path
var path = window.location.pathname;
document.querySelectorAll('.nav-link').forEach(function(a) {
    if (a.getAttribute('data-path') === path) a.classList.add('active');
});

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
});
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
    <nav class="sidebar">
        <div class="sidebar-logo">Log Dashboard<span>nginx analytics</span></div>
        <div class="sidebar-section">Reports</div>
        {nav_links}
    </nav>
    <div class="main">
        <div class="topbar"><h1>{title}</h1></div>
        <div class="content">
            {body}
        </div>
    </div>
    {js}
</body>
</html>"""
    return HTMLResponse(html)


def date_filters_html(date_from, date_to, available: List[str] = []):
    min_date = available[0] if available else ""
    max_date = available[-1] if available else ""
    available_json = json.dumps(available)
    return f"""
    <form method="get" class="filter-bar" onsubmit="return validateDates(this, {available_json})">
    <label>From<input type="date" name="from" value="{date_from or ''}"
           min="{min_date}" max="{max_date}"></label>
    <label>To<input type="date" name="to" value="{date_to or ''}"
           min="{min_date}" max="{max_date}"></label>
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
    body = date_filters_html(date_from, date_to, avail)

    if not avail:
        return page("Executive Summary", body + no_data_notice())

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

    return page("Executive Summary", body)


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

@app.get("/reports/traffic", response_class=HTMLResponse)
def traffic_report(
    date_from: Optional[str] = Query(None, alias="from"),
    date_to: Optional[str] = Query(None, alias="to"),
):
    avail = available_dates()
    date_from, date_to = _default_last_7(date_from, date_to, avail)
    body = date_filters_html(date_from, date_to, avail)

    # ── Overview tab ──
    paths_daily = list_partitions("daily", date_from, date_to)
    overview = ""
    if not paths_daily:
        overview = no_data_notice()
    else:
        sql_daily = """
        SELECT date, hits, hits_bot, hits_human, bytes_sent, resource_hits, resource_hits_bot
        FROM t ORDER BY date;
        """
        cols, rows = run_query(paths_daily, sql_daily)

        # KPIs
        curr_from, curr_to, prev_from, prev_to = _compute_periods(date_from, date_to, avail)
        _kpi_sql = """
        SELECT COALESCE(SUM(hits),0), COALESCE(SUM(hits_human),0),
               COALESCE(SUM(hits_bot),0), COALESCE(SUM(bytes_sent),0)
        FROM t;
        """
        curr_p = list_partitions("daily", curr_from, curr_to)
        prev_p = list_partitions("daily", prev_from, prev_to)
        def _kpi(p):
            if not p: return (0,0,0,0)
            _, r = run_query(p, _kpi_sql)
            return r[0] if r else (0,0,0,0)
        c, p = _kpi(curr_p), _kpi(prev_p)
        overview += "<div class='kpi-grid'>"
        overview += kpi_card("Total Hits", c[0], p[0])
        overview += kpi_card("Human Hits", c[1], p[1])
        overview += kpi_card("Bot Hits", c[2], p[2], lower_is_better=True)
        overview += kpi_card("Bytes Sent", c[3], p[3], fmt_fn=fmt_bytes)
        overview += "</div>"

        overview += export_link("daily", date_from, date_to)
        overview += line_chart(rows, cols, x_col="date",
                               y_cols=["hits", "hits_bot", "hits_human"],
                               title="Daily Traffic Volume")
        overview += bytes_line_chart(rows, cols, x_col="date", y_col="bytes_sent",
                                     title="Daily Bytes Sent")
        overview += html_table(rows, cols)

    # ── Hourly Patterns tab ──
    paths_hourly = list_partitions("hourly", date_from, date_to)
    hourly = ""
    if not paths_hourly:
        hourly = no_data_notice()
    else:
        sql_hourly = """
        SELECT hour_local AS hour, SUM(hits) AS hits, SUM(hits_bot) AS hits_bot, SUM(hits_human) AS hits_human
        FROM t GROUP BY hour_local ORDER BY hour_local;
        """
        cols_h, rows_h = run_query(paths_hourly, sql_hourly)
        hourly += export_link("hourly", date_from, date_to)
        hourly += bar_chart(rows_h, cols_h, x_col="hour",
                            y_cols=["hits_human", "hits_bot"],
                            title="Traffic by Hour of Day", barmode="stack")
        hourly += html_table(rows_h, cols_h)

    tabs = tab_bar([("overview", "Overview"), ("hourly", "Hourly Patterns")])
    body += tabs
    body += tab_panel("overview", overview)
    body += tab_panel("hourly", hourly)
    return page("Traffic", body)


# ── /reports/status-codes ─────────────────────────────────────────────────

@app.get("/reports/status-codes", response_class=HTMLResponse)
def status_codes_report(
    date_from: Optional[str] = Query(None, alias="from"),
    date_to: Optional[str] = Query(None, alias="to"),
    include_assets: bool = False,
    url_group: Optional[str] = None,
    limit: str = "500",
):
    limit = _parse_int(limit, 500)
    url_groups = _parse_csv_param(url_group)
    avail = available_dates()
    date_from, date_to = _default_last_7(date_from, date_to, avail)
    body = date_filters_html(date_from, date_to, avail)

    # Shared filter form
    groups_4xx = distinct_values("top_4xx_daily", "url_group", date_from, date_to)
    groups_5xx = distinct_values("top_5xx_daily", "url_group", date_from, date_to)
    groups_urls = distinct_values("top_urls_daily", "url_group", date_from, date_to)
    all_groups = sorted(set(groups_4xx + groups_5xx + groups_urls))
    checked = "checked" if include_assets else ""
    body += "<form method='get' class='filter-bar'>"
    body += f"<input type='hidden' name='from' value='{date_from or ''}'>"
    body += f"<input type='hidden' name='to' value='{date_to or ''}'>"
    body += multi_select_html("url_group", all_groups, url_groups, "URL group")
    body += f" <label><input type='checkbox' name='include_assets' value='true' {checked}> Include assets</label>"
    body += f" <label>Limit: <input name='limit' value='{int(limit)}' size='6'></label>"
    body += " <button type='submit'>Apply</button></form>"

    url_group_param = _fmt_filter_param(url_groups)
    url_group_display = _fmt_filter_display(url_groups)

    # Shared WHERE clause for asset/group filters
    def _where():
        clauses = []
        if not include_assets:
            clauses.append("url_group NOT IN ('Nuxt Assets','Static Assets')")
        group_clause = _in_clause("url_group", url_groups)
        if group_clause:
            clauses.append(group_clause)
        return ("WHERE " + " AND ".join(clauses)) if clauses else ""

    where = _where()

    # ── Trends tab ──
    trends = ""
    # When url_group/include_assets filters are active, re-aggregate from
    # top_urls_daily (which carries url_group) so the trend chart honors them.
    # Otherwise use the cheaper pre-aggregated `daily` partition.
    if where:
        paths_trend = list_partitions("top_urls_daily", date_from, date_to)
    else:
        paths_trend = list_partitions("daily", date_from, date_to)
    if not paths_trend:
        trends = no_data_notice()
    else:
        if where:
            sql_status = f"""
            SELECT date,
                   SUM(hits_total) - SUM(s3xx) - SUM(s4xx) - SUM(s5xx) AS s2xx,
                   SUM(s3xx) AS s3xx, SUM(s4xx) AS s4xx, SUM(s5xx) AS s5xx,
                   SUM(hits_total) AS hits
            FROM t {where}
            GROUP BY date ORDER BY date;
            """
        else:
            sql_status = """
            SELECT date, s2xx, s3xx, s4xx, s5xx, hits FROM t ORDER BY date;
            """
        cols_s, rows_s = run_query(paths_trend, sql_status)
        trends += export_link("daily-status", date_from, date_to)
        title_trend = "Status Codes Over Time"
        if url_groups:
            title_trend += f" — url_group={url_group_display}"
        trends += line_chart(rows_s, cols_s, x_col="date",
                             y_cols=["s2xx", "s3xx", "s4xx", "s5xx"],
                             title=title_trend)
        trends += html_table(rows_s, cols_s)

    # ── Redirects (3xx) tab ──
    redirects = ""
    paths_3xx = list_partitions("top_urls_daily", date_from, date_to)
    if not paths_3xx:
        redirects = no_data_notice()
    else:
        daily_3xx_sql = f"""
        SELECT date, SUM(s3xx) AS s3xx,
               SUM(s301) AS s301, SUM(s302) AS s302, SUM(s303) AS s303,
               SUM(s307) AS s307, SUM(s308) AS s308
        FROM t {where} GROUP BY date ORDER BY date;
        """
        cols_d3, rows_d3 = run_query(paths_3xx, daily_3xx_sql)

        sql_3xx = f"""
        SELECT path, url_group,
               SUM(s3xx) AS s3xx, SUM(s301) AS s301, SUM(s302) AS s302,
               SUM(s303) AS s303, SUM(s307) AS s307, SUM(s308) AS s308,
               SUM(hits_total) AS hits_total,
               ROUND(100.0 * SUM(s3xx) / NULLIF(SUM(hits_total), 0), 1) AS redirect_pct
        FROM t {where}
        GROUP BY path, url_group HAVING SUM(s3xx) > 0
        ORDER BY s3xx DESC LIMIT {int(limit)};
        """
        cols_3, rows_3 = run_query(paths_3xx, sql_3xx)

        redirects += export_link("top-3xx", date_from, date_to,
                                 extra=f"&include_assets={'true' if include_assets else 'false'}"
                                       + (f"&url_group={url_group_param}" if url_groups else "")
                                       + f"&limit={int(limit)}")
        redirects += "<h2>Redirect trend (daily)</h2>"
        redirects += line_chart(rows_d3, cols_d3, x_col="date",
                                y_cols=["s301", "s302", "s303", "s307", "s308"],
                                title="3xx responses by day (per code)")
        redirects += html_table(rows_d3, cols_d3, max_rows=500)
        if rows_3:
            redirects += "<h2>Top redirecting URLs</h2>"
            redirects += bar_chart(rows_3[:CHART_BAR_LIMIT], cols_3, x_col="path",
                                   y_cols=["s301", "s302", "s303", "s307", "s308"],
                                   title=f"Top {CHART_BAR_LIMIT} redirecting URLs by code", barmode="stack")
        redirects += html_table(rows_3, cols_3, max_rows=min(int(limit), 500))

    # ── Client Errors (4xx) tab ──
    client_errors = ""
    paths_4xx = list_partitions("top_4xx_daily", date_from, date_to)
    if not paths_4xx:
        client_errors = no_data_notice()
    else:
        daily_4xx_sql = f"""
        SELECT date,
            SUM(hits_4xx) AS hits_4xx, SUM(hits_4xx_bot) AS hits_4xx_bot,
            (SUM(hits_4xx) - SUM(hits_4xx_bot)) AS hits_4xx_non_bot,
            SUM(s400) AS s400, SUM(s401) AS s401, SUM(s403) AS s403,
            SUM(s404) AS s404, SUM(s405) AS s405, SUM(s410) AS s410,
            SUM(s422) AS s422, SUM(s429) AS s429
        FROM t {where} GROUP BY date ORDER BY date;
        """
        cols_d4, rows_d4 = run_query(paths_4xx, daily_4xx_sql)

        sql_4xx = f"""
        SELECT path, url_group,
            SUM(hits_4xx) AS hits_4xx, SUM(hits_4xx_bot) AS hits_4xx_bot,
            (SUM(hits_4xx) - SUM(hits_4xx_bot)) AS hits_4xx_non_bot,
            SUM(bytes_sent_4xx) AS bytes_sent_4xx,
            SUM(s400) AS s400, SUM(s401) AS s401, SUM(s403) AS s403,
            SUM(s404) AS s404, SUM(s405) AS s405, SUM(s410) AS s410,
            SUM(s422) AS s422, SUM(s429) AS s429
        FROM t {where}
        GROUP BY path, url_group ORDER BY hits_4xx DESC LIMIT {int(limit)};
        """
        cols_4, rows_4 = run_query(paths_4xx, sql_4xx)

        client_errors += export_link("top-4xx", date_from, date_to,
                                     extra=f"&include_assets={'true' if include_assets else 'false'}"
                                           + (f"&url_group={url_group_param}" if url_groups else "")
                                           + f"&limit={int(limit)}")
        client_errors += "<h2>4xx trend (daily)</h2>"
        client_errors += line_chart(rows_d4, cols_d4, x_col="date",
                                    y_cols=["s400", "s401", "s403", "s404", "s405", "s410", "s422", "s429"],
                                    title="4xx responses by day (per code)")
        client_errors += html_table(rows_d4, cols_d4, max_rows=500)
        if rows_4:
            client_errors += "<h2>Top 4xx URLs</h2>"
            client_errors += bar_chart(rows_4[:CHART_BAR_LIMIT], cols_4, x_col="path",
                                       y_cols=["s400", "s401", "s403", "s404", "s405", "s410", "s422", "s429"],
                                       title=f"Top {CHART_BAR_LIMIT} 4xx URLs by code", barmode="stack")
        client_errors += html_table(rows_4, cols_4, max_rows=min(int(limit), 500))

        # 4xx recommendations
        if rows_4:
            recs_4xx: List[Tuple[str, str, str]] = []
            path_i = cols_4.index("path") if "path" in cols_4 else 0
            non_bot_i = cols_4.index("hits_4xx_non_bot") if "hits_4xx_non_bot" in cols_4 else 4
            s404_i = cols_4.index("s404") if "s404" in cols_4 else None
            for r in rows_4[:15]:
                path_val = r[path_i]
                human_4xx = int(r[non_bot_i] or 0)
                s404_val = int(r[s404_i] or 0) if s404_i is not None else 0
                if s404_val > 50 and human_4xx > 20:
                    recs_4xx.append(("Redirect", f"{path_val} returns 404 for {human_4xx:,} human visitors",
                                     f"Set up a 301 redirect from {path_val} to the correct page"))
                elif human_4xx > 100 and s404_val == 0:
                    recs_4xx.append(("Investigate", f"{path_val} returns other 4xx for {human_4xx:,} human visitors", ""))
            if recs_4xx:
                client_errors += recommendations_section(recs_4xx[:5])

    # ── Server Errors (5xx) tab ──
    server_errors = ""
    paths_5xx = list_partitions("top_5xx_daily", date_from, date_to)
    if not paths_5xx:
        server_errors = no_data_notice()
    else:
        daily_5xx_sql = f"""
        SELECT date,
            SUM(hits_5xx) AS hits_5xx, SUM(hits_5xx_bot) AS hits_5xx_bot,
            (SUM(hits_5xx) - SUM(hits_5xx_bot)) AS hits_5xx_non_bot,
            SUM(s500) AS s500, SUM(s502) AS s502, SUM(s503) AS s503, SUM(s504) AS s504
        FROM t {where} GROUP BY date ORDER BY date;
        """
        cols_d5, rows_d5 = run_query(paths_5xx, daily_5xx_sql)

        sql_5xx = f"""
        SELECT path, url_group,
            SUM(hits_5xx) AS hits_5xx, SUM(hits_5xx_bot) AS hits_5xx_bot,
            (SUM(hits_5xx) - SUM(hits_5xx_bot)) AS hits_5xx_non_bot,
            SUM(s500) AS s500, SUM(s502) AS s502, SUM(s503) AS s503, SUM(s504) AS s504
        FROM t {where}
        GROUP BY path, url_group ORDER BY hits_5xx DESC LIMIT {int(limit)};
        """
        cols_5, rows_5 = run_query(paths_5xx, sql_5xx)

        server_errors += export_link("top-5xx", date_from, date_to,
                                     extra=f"&include_assets={'true' if include_assets else 'false'}"
                                           + (f"&url_group={url_group_param}" if url_groups else "")
                                           + f"&limit={int(limit)}")
        server_errors += "<h2>5xx trend (daily)</h2>"
        server_errors += line_chart(rows_d5, cols_d5, x_col="date",
                                    y_cols=["s500", "s502", "s503", "s504"],
                                    title="5xx responses by day (per code)")
        server_errors += html_table(rows_d5, cols_d5, max_rows=500)
        if rows_5:
            server_errors += "<h2>Top 5xx URLs</h2>"
            server_errors += bar_chart(rows_5[:CHART_BAR_LIMIT], cols_5, x_col="path",
                                       y_cols=["s500", "s502", "s503", "s504"],
                                       title=f"Top {CHART_BAR_LIMIT} 5xx URLs by code", barmode="stack")
        server_errors += html_table(rows_5, cols_5, max_rows=min(int(limit), 500))

        # 5xx recommendations
        recurrence_sql = f"""
        SELECT path, COUNT(DISTINCT date) AS days_affected, SUM(hits_5xx) AS total
        FROM t {where}
        GROUP BY path HAVING COUNT(DISTINCT date) > 1
        ORDER BY days_affected DESC, total DESC LIMIT 5;
        """
        _, recur_rows = run_query(paths_5xx, recurrence_sql)
        recs_5xx: List[Tuple[str, str, str]] = []
        for r in recur_rows:
            path_val, days, total = r[0], int(r[1] or 0), int(r[2] or 0)
            recs_5xx.append(("Investigate",
                             f"{path_val} has returned 5xx errors on {days} different days ({total:,} total)",
                             "Check server logs for this path to identify the root cause"))
        if recs_5xx:
            server_errors += recommendations_section(recs_5xx)

    tabs = tab_bar([("trends", "Trends Over Time"), ("redirects", "Redirects (3xx)"),
                    ("client-errors", "Client Errors (4xx)"), ("server-errors", "Server Errors (5xx)")])
    body += tabs
    body += tab_panel("trends", trends)
    body += tab_panel("redirects", redirects)
    body += tab_panel("client-errors", client_errors)
    body += tab_panel("server-errors", server_errors)
    return page("Status Codes", body)


# ── /reports/content ──────────────────────────────────────────────────────

@app.get("/reports/content", response_class=HTMLResponse)
def content_report(
    date_from: Optional[str] = Query(None, alias="from"),
    date_to: Optional[str] = Query(None, alias="to"),
    include_assets: bool = False,
    url_group: Optional[str] = None,
    limit: str = "500",
):
    limit = _parse_int(limit, 500)
    url_groups = _parse_csv_param(url_group)
    avail = available_dates()
    date_from, date_to = _default_last_7(date_from, date_to, avail)
    body = date_filters_html(date_from, date_to, avail)

    # Shared filter form
    groups = distinct_values("top_urls_daily", "url_group", date_from, date_to)
    checked = "checked" if include_assets else ""
    body += "<form method='get' class='filter-bar'>"
    body += f"<input type='hidden' name='from' value='{date_from or ''}'>"
    body += f"<input type='hidden' name='to' value='{date_to or ''}'>"
    body += multi_select_html("url_group", groups, url_groups, "URL group")
    body += f" <label><input type='checkbox' name='include_assets' value='true' {checked}> Include assets</label>"
    body += f" <label>Limit: <input name='limit' value='{int(limit)}' size='6'></label>"
    body += " <button type='submit'>Apply</button></form>"

    url_group_param = _fmt_filter_param(url_groups)
    url_group_display = _fmt_filter_display(url_groups)

    clauses = []
    if not include_assets:
        clauses.append("url_group NOT IN ('Nuxt Assets','Static Assets')")
    group_clause = _in_clause("url_group", url_groups)
    if group_clause:
        clauses.append(group_clause)
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""

    # ── Top URLs tab ──
    top_urls_content = ""
    paths_urls = list_partitions("top_urls_daily", date_from, date_to)
    if not paths_urls:
        top_urls_content = no_data_notice()
    else:
        sql_top = f"""
        SELECT path, url_group,
               SUM(hits_total) AS hits_total, SUM(hits_bot) AS hits_bot,
               SUM(s3xx) AS s3xx, SUM(s4xx) AS s4xx, SUM(s5xx) AS s5xx,
               SUM(parameterized_hits) AS parameterized_hits
        FROM t {where}
        GROUP BY path, url_group ORDER BY hits_total DESC LIMIT {int(limit)};
        """
        cols_t, rows_t = run_query(paths_urls, sql_top)
        top_urls_content += export_link("top-urls", date_from, date_to,
                                        extra=f"&include_assets={'true' if include_assets else 'false'}"
                                              + (f"&url_group={url_group_param}" if url_groups else "")
                                              + f"&limit={int(limit)}")
        if rows_t:
            top_urls_content += bar_chart(rows_t[:CHART_BAR_LIMIT], cols_t, x_col="path",
                                          y_cols=["hits_total", "hits_bot"],
                                          title=f"Top {CHART_BAR_LIMIT} URLs — total vs bot", barmode="group")
        top_urls_content += html_table(rows_t, cols_t)

    # ── URL Groups tab ──
    url_groups_content = ""
    paths_groups = list_partitions("group_daily", date_from, date_to)
    if not paths_groups:
        url_groups_content = no_data_notice()
    else:
        sql_grp = """
        SELECT url_group, SUM(hits) AS hits, SUM(hits_bot) AS hits_bot,
               SUM(hits_human) AS hits_human, SUM(s4xx) AS s4xx, SUM(s5xx) AS s5xx,
               SUM(resource_hits) AS resource_hits, SUM(resource_hits_bot) AS resource_hits_bot
        FROM t GROUP BY url_group ORDER BY hits DESC;
        """
        cols_g, rows_g = run_query(paths_groups, sql_grp)
        url_groups_content += export_link("url-groups", date_from, date_to)
        url_groups_content += bar_chart(rows_g, cols_g, x_col="url_group",
                                        y_cols=["hits_human", "hits_bot"],
                                        title="URL groups — human vs bot", barmode="stack")
        url_groups_content += html_table(rows_g, cols_g)

    # ── Page Size tab ──
    page_size_content = ""
    if not paths_urls:
        page_size_content = no_data_notice()
    else:
        sql_bytes = f"""
        SELECT path, url_group,
               SUM(bytes_sent) AS bytes_total, SUM(hits_total) AS hits_total,
               ROUND(SUM(bytes_sent) / NULLIF(SUM(hits_total), 0)) AS avg_bytes_per_hit
        FROM t {where}
        GROUP BY path, url_group HAVING SUM(bytes_sent) > 0
        ORDER BY avg_bytes_per_hit DESC LIMIT {int(limit)};
        """
        cols_b, rows_b = run_query(paths_urls, sql_bytes)
        page_size_content += export_link("url-bytes", date_from, date_to,
                                         extra=f"&include_assets={'true' if include_assets else 'false'}"
                                               + (f"&url_group={url_group_param}" if url_groups else "")
                                               + f"&limit={int(limit)}")
        if rows_b:
            page_size_content += bytes_bar_chart(rows_b[:CHART_BAR_LIMIT], cols_b,
                                                 x_col="path", y_col="avg_bytes_per_hit",
                                                 title=f"Top {CHART_BAR_LIMIT} URLs by avg response size")
            # Format byte columns
            bytes_total_idx = cols_b.index("bytes_total")
            avg_idx = cols_b.index("avg_bytes_per_hit")
            display_rows_b = [
                tuple(fmt_bytes(v) if i in (bytes_total_idx, avg_idx) else v for i, v in enumerate(r))
                for r in rows_b
            ]
            page_size_content += html_table(display_rows_b, cols_b, max_rows=min(int(limit), 500))
        else:
            page_size_content += no_data_notice()

    # ── Human Paths tab ──
    human_content = ""
    paths_human = list_partitions("human_urls_daily", date_from, date_to)
    if not paths_human:
        human_content = no_data_notice()
    else:
        human_clauses = []
        if not include_assets:
            human_clauses.append("url_group NOT IN ('Nuxt Assets','Static Assets')")
        human_group_clause = _in_clause("url_group", url_groups)
        if human_group_clause:
            human_clauses.append(human_group_clause)
        human_where = ("WHERE " + " AND ".join(human_clauses)) if human_clauses else ""
        sql_human = f"""
        SELECT url_group, path, SUM(hits) AS hits
        FROM t {human_where}
        GROUP BY url_group, path ORDER BY hits DESC LIMIT {int(limit)};
        """
        cols_h, rows_h = run_query(paths_human, sql_human)
        human_content += export_link("human-urls", date_from, date_to,
                                     extra=f"&include_assets={'true' if include_assets else 'false'}"
                                           + (f"&url_group={url_group_param}" if url_groups else "")
                                           + f"&limit={int(limit)}")
        chart_title = f"Top {CHART_BAR_LIMIT} human URLs" + (f" — {url_group_display}" if url_groups else "")
        human_content += bar_chart(rows_h[:CHART_BAR_LIMIT], cols_h, x_col="path", y_col="hits",
                                   title=chart_title)
        human_content += html_table(rows_h, cols_h)

    tabs = tab_bar([("top-urls", "Top URLs"), ("url-groups", "URL Groups"),
                    ("page-size", "Page Size"), ("human-paths", "Human Paths")])
    body += tabs
    body += tab_panel("top-urls", top_urls_content)
    body += tab_panel("url-groups", url_groups_content)
    body += tab_panel("page-size", page_size_content)
    body += tab_panel("human-paths", human_content)
    return page("Content", body)


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
    body = date_filters_html(date_from, date_to, avail)

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
    return page("Locales", body)



# ── /reports/bots ─────────────────────────────────────────────────────────

_BOTS_VALID_TABS = {"bot-families", "ai-crawlers", "crawl-waste", "resource-waste"}


def _bots_tab_families(
    date_from: Optional[str],
    date_to: Optional[str],
    bots: List[str],
    include_assets: bool,
    categories: List[str],
    limit: int,
    avail: List[str],
) -> str:
    paths_summary = list_partitions("bot_daily", date_from, date_to)
    if not paths_summary:
        return no_data_notice()

    bot_families_content = ""
    cat_clause = _in_clause("bot_category", categories)
    cat_where = f"WHERE {cat_clause}" if cat_clause else ""
    cat_and = f"AND {cat_clause}" if cat_clause else ""
    sql_summary = f"""
    SELECT bot_family, SUM(hits) AS hits, SUM(resource_hits) AS resource_hits,
           AVG(resource_hits_pct) AS resource_hits_pct,
           SUM(s4xx) AS s4xx, SUM(s5xx) AS s5xx
    FROM t {cat_where} GROUP BY bot_family ORDER BY hits DESC;
    """
    cols_bf, rows_bf = run_query(paths_summary, sql_summary)

    bot_display = _fmt_filter_display(bots)
    if bots:
        bot_in = _in_clause("bot_family", bots)
        trend_sql = f"""
        SELECT date, bot_family, SUM(hits) AS hits
        FROM t WHERE {bot_in} {cat_and}
        GROUP BY date, bot_family ORDER BY date, bot_family;
        """
        trend_heading = f"Activity over time — {bot_display}"
        trend_title = f"Bot hits over time — {bot_display}"
    else:
        top_families = [r[0] for r in rows_bf[:10]]
        trend_sql = None
        if top_families:
            families_in = ", ".join(f"'{sql_escape_string(f)}'" for f in top_families)
            trend_sql = f"""
            SELECT date, bot_family, SUM(hits) AS hits
            FROM t WHERE bot_family IN ({families_in}) {cat_and}
            GROUP BY date, bot_family ORDER BY date, bot_family;
            """
            trend_heading = "Activity over time (top 10 families)"
            trend_title = "Bot hits over time (top 10 families)"

    if trend_sql:
        cols_bt, rows_bt = run_query(paths_summary, trend_sql)
        if rows_bt:
            df_t = pd.DataFrame(rows_bt, columns=cols_bt)
            df_pivot = df_t.pivot_table(index="date", columns="bot_family", values="hits", fill_value=0).reset_index()
            pivot_cols = list(df_pivot.columns)
            pivot_rows = [tuple(r) for r in df_pivot.itertuples(index=False, name=None)]
            y_families = [c for c in pivot_cols if c != "date"]
            bot_families_content += f"<h3>{trend_heading}</h3>"
            bot_families_content += line_chart(pivot_rows, pivot_cols, x_col="date", y_cols=y_families,
                                               title=trend_title)

    curr_from_b, curr_to_b, prev_from_b, prev_to_b = _compute_periods(date_from, date_to, avail)
    prev_paths_b = list_partitions("bot_daily", prev_from_b, prev_to_b)
    if prev_paths_b:
        prev_cols_b, prev_rows_b = run_query(prev_paths_b, sql_summary)
        cols_bf, rows_bf = add_trend_columns(cols_bf, rows_bf, prev_cols_b, prev_rows_b,
                                             key_col="bot_family", metric_cols=["hits", "s4xx", "s5xx"])

    chart_cols_b = [c for c in cols_bf if not c.endswith("_chg")]
    chart_rows_b = [tuple(v for c, v in zip(cols_bf, r) if not c.endswith("_chg")) for r in rows_bf]
    bot_families_content += "<h3>Total hits by bot family</h3>"
    bot_families_content += bar_chart(chart_rows_b, chart_cols_b, x_col="bot_family", y_col="hits",
                                      title="Bot hits by family")

    bot_idx = cols_bf.index("bot_family")
    category_param = _fmt_filter_param(categories)
    def _bot_link_qs(family):
        parts = [f"bot={family}"]
        if date_from: parts.append(f"from={date_from}")
        if date_to:   parts.append(f"to={date_to}")
        if categories: parts.append(f"category={category_param}")
        if include_assets: parts.append("include_assets=true")
        parts.append(f"limit={int(limit)}")
        return "&".join(parts)
    linked_rows = [
        tuple(
            f"<a href='/reports/bots?{_bot_link_qs(v)}&tab=bot-families#urls'>{v}</a>" if i == bot_idx else v
            for i, v in enumerate(r)
        )
        for r in rows_bf
    ]
    bot_families_content += export_link("bots", date_from, date_to)
    bot_families_content += html_table(linked_rows, cols_bf)

    bot_recs: List[Tuple[str, str, str]] = []
    _ci = {c: i for i, c in enumerate(cols_bf)}
    for r in rows_bf:
        family = r[_ci["bot_family"]]
        hits_val = int(r[_ci["hits"]] or 0)
        resource_pct = float(r[_ci["resource_hits_pct"]] or 0)
        s4xx_val = int(r[_ci["s4xx"]] or 0)
        s5xx_val = int(r[_ci["s5xx"]] or 0)
        if hits_val == 0:
            continue
        error_rate = (s4xx_val + s5xx_val) / hits_val
        if error_rate > 0.5 and hits_val > 50:
            bot_recs.append(("Block", f"{family} has {error_rate:.0%} error rate across {hits_val:,} requests",
                             bot_family_detection_blurb(family)))
        elif resource_pct > 80 and hits_val > 100:
            bot_recs.append(("Restrict", f"{family} spends {resource_pct:.0f}% of crawl on static assets ({hits_val:,} hits)",
                             bot_family_detection_blurb(family)))
    if bot_recs:
        bot_families_content += recommendations_section(bot_recs[:5])

    # Prefer skinnier bot_top_urls_daily (top-50 per family) for unfiltered previews.
    # Falls back to the full bot_urls_daily when drilling into a specific bot or when
    # the skinnier aggregate hasn't been built yet.
    url_limit = int(limit) if bots else 20
    use_top_urls = (not bots) and url_limit <= 50
    paths_urls = None
    if use_top_urls:
        paths_urls = list_partitions("bot_top_urls_daily", date_from, date_to)
    if not paths_urls:
        paths_urls = list_partitions("bot_urls_daily", date_from, date_to)
    url_title = f"URLs crawled by {bot_display}" if bots else "URLs crawled — preview (click a bot above for full breakdown)"
    bot_families_content += f"<h2 id='urls'>{url_title}</h2>"
    if not paths_urls:
        bot_families_content += no_data_notice()
    else:
        url_clauses = []
        bot_clause = _in_clause("bot_family", bots)
        if bot_clause:
            url_clauses.append(bot_clause)
        if not include_assets:
            url_clauses.append("url_group NOT IN ('Nuxt Assets','Static Assets')")
        url_where = ("WHERE " + " AND ".join(url_clauses)) if url_clauses else ""
        sql_urls = f"""
        SELECT bot_family, url_group, path, SUM(hits) AS hits
        FROM t {url_where}
        GROUP BY bot_family, url_group, path ORDER BY hits DESC LIMIT {url_limit};
        """
        cols_u, rows_u = run_query(paths_urls, sql_urls)
        if bots:
            bot_families_content += export_link("bot-urls", date_from, date_to,
                                                extra=f"&include_assets={'true' if include_assets else 'false'}&bot={_fmt_filter_param(bots)}&limit={int(limit)}")
            bot_families_content += bar_chart(rows_u[:CHART_BAR_LIMIT], cols_u, x_col="path", y_col="hits",
                                              title=f"Top {CHART_BAR_LIMIT} URLs crawled by {bot_display}")
        else:
            bot_families_content += "<p>Showing top 20 across all bots. Select a bot above for the full breakdown.</p>"
        bot_families_content += html_table(rows_u, cols_u)

    return bot_families_content


def _bots_tab_ai_crawlers(
    date_from: Optional[str],
    date_to: Optional[str],
) -> str:
    paths_ai = list_partitions("ai_crawler_daily", date_from, date_to)
    if not paths_ai:
        return (
            "<div class='empty-state'>"
            "<h3>AI Crawlers</h3>"
            "<p>No AI crawler data available for the selected date range.</p>"
            "<p>Run ingestion and rebuild aggregates to populate this tab.</p>"
            "</div>"
        )

    ai_crawlers_content = ""
    ai_trend_sql = """
    SELECT date, bot_family, SUM(total_hits) AS hits
    FROM t GROUP BY date, bot_family ORDER BY date, bot_family;
    """
    cols_ait, rows_ait = run_query(paths_ai, ai_trend_sql)
    if rows_ait:
        df_ai_t = pd.DataFrame(rows_ait, columns=cols_ait)
        df_ai_pivot = df_ai_t.pivot_table(index="date", columns="bot_family", values="hits", fill_value=0).reset_index()
        ai_pivot_cols = list(df_ai_pivot.columns)
        ai_pivot_rows = [tuple(r) for r in df_ai_pivot.itertuples(index=False, name=None)]
        ai_y_families = [c for c in ai_pivot_cols if c != "date"]
        ai_crawlers_content += "<h3>AI Crawler Volume Trends</h3>"
        ai_crawlers_content += line_chart(ai_pivot_rows, ai_pivot_cols, x_col="date", y_cols=ai_y_families,
                                          title="Daily hits per AI crawler")

    ai_summary_sql = """
    SELECT bot_family,
           SUM(total_hits) AS total_hits,
           SUM(unique_urls) AS unique_urls,
           SUM(bytes_sent) AS bytes_sent,
           AVG(overlap_with_googlebot) AS avg_overlap
    FROM t GROUP BY bot_family ORDER BY total_hits DESC;
    """
    cols_ais, rows_ais = run_query(paths_ai, ai_summary_sql)
    if rows_ais:
        ai_crawlers_content += "<h3>Googlebot Overlap</h3>"
        ai_crawlers_content += "<p>Percentage of URLs also crawled by Googlebot on the same day. "
        ai_crawlers_content += "High overlap suggests the AI crawler follows similar signals; low overlap may indicate scraping behavior.</p>"
        ai_crawlers_content += "<div style='display:flex;flex-wrap:wrap;gap:1rem;margin:1rem 0;'>"
        ci = {c: i for i, c in enumerate(cols_ais)}
        for r in rows_ais:
            family = r[ci["bot_family"]]
            overlap = float(r[ci["avg_overlap"]] or 0)
            total = int(r[ci["total_hits"]] or 0)
            urls = int(r[ci["unique_urls"]] or 0)
            ai_crawlers_content += (
                f"<div style='border:1px solid #ddd;border-radius:8px;padding:1rem;min-width:180px;'>"
                f"<strong>{family}</strong><br>"
                f"<span style='font-size:1.5em;color:{'#2ecc71' if overlap > 0.5 else '#e67e22' if overlap > 0.2 else '#e74c3c'};'>"
                f"{overlap:.0%}</span> overlap<br>"
                f"<small>{total:,} hits &middot; {urls:,} unique URLs</small>"
                f"</div>"
            )
        ai_crawlers_content += "</div>"

        ai_crawlers_content += "<h3>AI Crawler Summary</h3>"
        ai_crawlers_content += export_link("ai-crawlers", date_from, date_to)
        ai_crawlers_content += html_table(rows_ais, cols_ais)

    # Prefer the skinnier bot_top_urls_daily here: this panel only shows the
    # top 20 URLs per AI family, which fits in the top-50 pre-rank.
    paths_bot_urls = list_partitions("bot_top_urls_daily", date_from, date_to)
    if not paths_bot_urls:
        paths_bot_urls = list_partitions("bot_urls_daily", date_from, date_to)
    if paths_bot_urls:
        ai_families_sql = "SELECT DISTINCT bot_family FROM t;"
        _, ai_fam_rows = run_query(paths_ai, ai_families_sql)
        ai_family_names = [r[0] for r in ai_fam_rows] if ai_fam_rows else []

        if ai_family_names:
            families_in = ", ".join(f"'{sql_escape_string(f)}'" for f in ai_family_names)
            content_sql = f"""
            SELECT bot_family, url_group, path, SUM(hits) AS hits
            FROM t WHERE bot_family IN ({families_in})
            GROUP BY bot_family, url_group, path
            ORDER BY bot_family, hits DESC;
            """
            cols_ct, rows_ct = run_query(paths_bot_urls, content_sql)
            if rows_ct:
                ai_crawlers_content += "<h3>Content Targeting (top 20 URLs per crawler)</h3>"
                from collections import defaultdict
                by_family = defaultdict(list)
                fi = {c: i for i, c in enumerate(cols_ct)}
                for r in rows_ct:
                    by_family[r[fi["bot_family"]]].append(r)
                for fam in ai_family_names:
                    fam_rows = by_family.get(fam, [])[:20]
                    if fam_rows:
                        ai_crawlers_content += f"<h4>{fam}</h4>"
                        ai_crawlers_content += html_table(fam_rows, cols_ct)

    return ai_crawlers_content


def _bots_tab_crawl_waste(
    date_from: Optional[str],
    date_to: Optional[str],
    bots: List[str],
    include_assets: bool,
    strict: bool,
    url_groups: List[str],
    limit: int,
) -> str:
    paths_waste = list_partitions("wasted_crawl_daily", date_from, date_to)
    if not paths_waste:
        return no_data_notice()

    crawl_waste_content = ""
    score_col = "waste_score_strict" if strict else "waste_score"
    w_clauses = []
    for clause in (
        _in_clause("bot_family", bots),
        _in_clause("url_group", url_groups),
    ):
        if clause:
            w_clauses.append(clause)
    if not include_assets:
        w_clauses.append("url_group NOT IN ('Nuxt Assets','Static Assets')")
    w_where = ("WHERE " + " AND ".join(w_clauses)) if w_clauses else ""

    sql_waste = f"""
    SELECT bot_family, url_group, path,
           SUM(bot_hits) AS bot_hits,
           SUM(parameterized_bot_hits) AS parameterized_bot_hits,
           SUM(redirect_bot_hits) AS redirect_bot_hits,
           SUM(error_bot_hits) AS error_bot_hits,
           SUM(resource_bot_hits) AS resource_bot_hits,
           SUM(resource_parameterized_bot_hits) AS resource_parameterized_bot_hits,
           SUM(resource_error_bot_hits) AS resource_error_bot_hits,
           SUM({score_col}) AS waste_score
    FROM t {w_where}
    GROUP BY bot_family, url_group, path
    ORDER BY waste_score DESC, bot_hits DESC LIMIT {int(limit)};
    """
    cols_w, rows_w = run_query(paths_waste, sql_waste)

    trend_w_sql = f"""
    SELECT date, SUM({score_col}) AS waste_score, SUM(bot_hits) AS bot_hits
    FROM t {w_where} GROUP BY date ORDER BY date;
    """
    cols_wt, rows_wt = run_query(paths_waste, trend_w_sql)

    crawl_waste_content += (
        f"<p><a href='/export?report=wasted-crawl&from={date_from or ''}&to={date_to or ''}"
        f"&strict={'true' if strict else 'false'}"
        f"&include_assets={'true' if include_assets else 'false'}"
        f"{'&bot=' + _fmt_filter_param(bots) if bots else ''}"
        f"{'&url_group=' + _fmt_filter_param(url_groups) if url_groups else ''}"
        f"&limit={int(limit)}'>Export CSV</a></p>"
    )
    crawl_waste_content += "<h2>Waste score trend (daily)</h2>"
    crawl_waste_content += line_chart(rows_wt, cols_wt, x_col="date",
                                      y_cols=["waste_score", "bot_hits"],
                                      title="Waste score over time")
    crawl_waste_content += "<h2>Top wasted paths</h2>"
    crawl_waste_content += bar_chart(rows_w, cols_w, x_col="path", y_col="waste_score",
                                     title="Highest waste score (top paths)")
    crawl_waste_content += html_table(rows_w, cols_w)

    if rows_w:
        recs_waste: List[Tuple[str, str, str]] = []
        bot_waste: Dict[str, Tuple[int, int]] = {}
        for r in rows_w:
            family = r[0]
            bot_hits = int(r[3] or 0)
            error_hits = int(r[6] or 0)
            waste = int(r[-1] or 0)
            prev = bot_waste.get(family, (0, 0))
            bot_waste[family] = (prev[0] + waste, prev[1] + bot_hits)
        for family, (total_waste, total_hits) in sorted(bot_waste.items(), key=lambda x: -x[1][0])[:3]:
            if total_waste > 50:
                recs_waste.append(("Rate-limit", f"{family} generates {total_waste:,} waste score across {total_hits:,} requests",
                                   f"User-agent: {family}\nCrawl-delay: 10"))
        for r in rows_w[:5]:
            path_val = r[2]
            error_hits = int(r[6] or 0)
            if error_hits > 100:
                recs_waste.append(("Fix or remove", f"{path_val} causes {error_hits:,} bot errors",
                                   "Fix the underlying error or return 410 Gone"))
        if recs_waste:
            crawl_waste_content += recommendations_section(recs_waste[:6])

    return crawl_waste_content


def _bots_tab_resource_waste(
    date_from: Optional[str],
    date_to: Optional[str],
    limit: int,
) -> str:
    paths_rw = list_partitions("top_resource_waste_daily", date_from, date_to)
    if not paths_rw:
        return no_data_notice()

    resource_waste_content = ""
    sql_rw = f"""
    SELECT path, SUM(bot_hits) AS bot_hits,
           SUM(resource_error_bot_hits) AS resource_error_bot_hits,
           SUM(status_404_bot_hits) AS status_404_bot_hits,
           SUM(waste_score_strict) AS waste_score_strict
    FROM t GROUP BY path
    ORDER BY waste_score_strict DESC, bot_hits DESC LIMIT {int(limit)};
    """
    cols_rw, rows_rw = run_query(paths_rw, sql_rw)
    resource_waste_content += export_link("top-resource-waste", date_from, date_to, extra=f"&limit={int(limit)}")
    resource_waste_content += bar_chart(rows_rw, cols_rw, x_col="path", y_col="waste_score_strict",
                                        title="Top resource waste (strict)")
    resource_waste_content += html_table(rows_rw, cols_rw)
    return resource_waste_content


def _bots_render_tab(
    tab_id: str,
    date_from: Optional[str],
    date_to: Optional[str],
    bots: List[str],
    include_assets: bool,
    strict: bool,
    url_groups: List[str],
    categories: List[str],
    limit: int,
    avail: List[str],
) -> str:
    if tab_id == "bot-families":
        return _bots_tab_families(date_from, date_to, bots, include_assets, categories, limit, avail)
    if tab_id == "ai-crawlers":
        return _bots_tab_ai_crawlers(date_from, date_to)
    if tab_id == "crawl-waste":
        return _bots_tab_crawl_waste(date_from, date_to, bots, include_assets, strict, url_groups, limit)
    if tab_id == "resource-waste":
        return _bots_tab_resource_waste(date_from, date_to, limit)
    return no_data_notice()


def _lazy_tab_placeholder(tab_id: str) -> str:
    return (
        f"<div class='lazy-tab-placeholder' data-lazy-tab='{tab_id}' "
        "style='padding:2rem;text-align:center;color:#64748b;'>"
        "<p>Loading…</p></div>"
    )


@app.get("/reports/bots", response_class=HTMLResponse)
def bots_report(
    date_from: Optional[str] = Query(None, alias="from"),
    date_to: Optional[str] = Query(None, alias="to"),
    bot: Optional[str] = None,
    include_assets: bool = False,
    strict: bool = False,
    url_group: Optional[str] = None,
    category: Optional[str] = None,
    limit: str = "500",
    tab: Optional[str] = None,
):
    limit = _parse_int(limit, 500)
    bots_sel = _parse_csv_param(bot)
    url_groups = _parse_csv_param(url_group)
    categories = _parse_csv_param(category)
    avail = available_dates()
    date_from, date_to = _default_last_7(date_from, date_to, avail)
    body = date_filters_html(date_from, date_to, avail)

    # Shared filter form
    bots_list = distinct_values("bot_urls_daily", "bot_family", date_from, date_to)
    waste_bots = distinct_values("wasted_crawl_daily", "bot_family", date_from, date_to)
    waste_groups = distinct_values("wasted_crawl_daily", "url_group", date_from, date_to)
    bot_categories = distinct_values("bot_daily", "bot_category", date_from, date_to)
    checked_assets = "checked" if include_assets else ""
    checked_strict = "checked" if strict else ""
    body += f"""
    <form method="get" class="filter-bar">
    <input type="hidden" name="from" value="{date_from or ''}">
    <input type="hidden" name="to" value="{date_to or ''}">
    {multi_select_html("bot", sorted(set(bots_list + waste_bots)), bots_sel, "Bot family")}
    {multi_select_html("category", sorted(bot_categories), categories, "Bot category")}
    {multi_select_html("url_group", waste_groups, url_groups, "URL group")}
    <label><input type="checkbox" name="include_assets" value="true" {checked_assets}> Include assets</label>
    <label><input type="checkbox" name="strict" value="true" {checked_strict}> Strict scoring</label>
    <label>Limit: <input name="limit" value="{int(limit)}" size="6"></label>
    <button type="submit">Apply</button>
    </form>
    """

    active_tab = tab if tab in _BOTS_VALID_TABS else "bot-families"

    body += tab_bar([("bot-families", "Bot Families"), ("ai-crawlers", "AI Crawlers"),
                     ("crawl-waste", "Crawl Waste"), ("resource-waste", "Resource Waste")])
    # Only the active tab is rendered eagerly; the other three render a
    # placeholder that the client fetches on demand from /reports/bots/_tab/*.
    for tid in ("bot-families", "ai-crawlers", "crawl-waste", "resource-waste"):
        if tid == active_tab:
            content = _bots_render_tab(tid, date_from, date_to, bots_sel, include_assets,
                                       strict, url_groups, categories, limit, avail)
        else:
            content = _lazy_tab_placeholder(tid)
        body += tab_panel(tid, content)
    return page("Bots", body)


@app.get("/reports/bots/_tab/{tab_id}", response_class=HTMLResponse)
def bots_tab_fragment(
    tab_id: str,
    date_from: Optional[str] = Query(None, alias="from"),
    date_to: Optional[str] = Query(None, alias="to"),
    bot: Optional[str] = None,
    include_assets: bool = False,
    strict: bool = False,
    url_group: Optional[str] = None,
    category: Optional[str] = None,
    limit: str = "500",
):
    if tab_id not in _BOTS_VALID_TABS:
        return HTMLResponse(no_data_notice(), status_code=404)
    parsed_limit = _parse_int(limit, 500)
    bots_sel = _parse_csv_param(bot)
    url_groups = _parse_csv_param(url_group)
    categories = _parse_csv_param(category)
    avail = available_dates()
    date_from, date_to = _default_last_7(date_from, date_to, avail)
    html = _bots_render_tab(tab_id, date_from, date_to, bots_sel, include_assets,
                            strict, url_groups, categories, parsed_limit, avail)
    return HTMLResponse(html)


# ── /reports/referer-flow ─────────────────────────────────────────────────

@app.get("/reports/referer-flow", response_class=HTMLResponse)
def referer_flow_report(
    date_from: Optional[str] = Query(None, alias="from"),
    date_to: Optional[str] = Query(None, alias="to"),
    limit: int = 500,
):
    avail = available_dates()
    date_from, date_to = _default_last_7(date_from, date_to, avail)
    body = date_filters_html(date_from, date_to, avail)

    paths_rf = list_partitions("referer_flow_daily", date_from, date_to)

    # Check if 'referer_path' column exists in the parquet files
    has_referer_path = False
    if paths_rf:
        try:
            _, _r = run_query(paths_rf, "SELECT column_name FROM (DESCRIBE t) WHERE column_name = 'from_path'")
            has_referer_path = bool(_r)
        except Exception:
            pass

    # ── Internal Navigation tab ──
    internal_nav = ""
    if not paths_rf or not has_referer_path:
        internal_nav = no_data_notice()
    else:
        sql_nav = f"""
        SELECT from_path, to_path, SUM(hit_count) AS hits
        FROM t
        WHERE referer_type = 'Internal' AND from_path IS NOT NULL
        GROUP BY from_path, to_path
        ORDER BY hits DESC LIMIT {int(limit)};
        """
        cols_n, rows_n = run_query(paths_rf, sql_nav)
        internal_nav += export_link("referer-flow-internal", date_from, date_to, extra=f"&limit={int(limit)}")
        if rows_n:
            internal_nav += "<h2>Top internal navigation paths (from → to)</h2>"
            internal_nav += html_table(rows_n, cols_n, max_rows=min(int(limit), 500))
        else:
            internal_nav += no_data_notice()

    # ── Entry Points tab ──
    entry_points = ""
    if not paths_rf:
        entry_points = no_data_notice()
    else:
        sql_entry = f"""
        SELECT referer_type, to_path AS landing_page, SUM(hit_count) AS hits
        FROM t
        GROUP BY referer_type, to_path
        ORDER BY hits DESC LIMIT {int(limit)};
        """
        cols_e, rows_e = run_query(paths_rf, sql_entry)
        entry_points += export_link("referer-flow-entry", date_from, date_to, extra=f"&limit={int(limit)}")
        if rows_e:
            entry_points += "<h2>Top landing pages by referer type</h2>"
            entry_points += html_table(rows_e, cols_e, max_rows=min(int(limit), 500))
        else:
            entry_points += no_data_notice()

    # ── Referer Types tab ──
    referer_types = ""
    if not paths_rf:
        referer_types = no_data_notice()
    else:
        sql_types = """
        SELECT referer_type, SUM(hit_count) AS hits
        FROM t GROUP BY referer_type ORDER BY hits DESC;
        """
        cols_rt, rows_rt = run_query(paths_rf, sql_types)
        referer_types += export_link("referer-flow-types", date_from, date_to)
        if rows_rt:
            referer_types += bar_chart(rows_rt, cols_rt, x_col="referer_type", y_col="hits",
                                       title="Traffic by Referer Type")
            referer_types += html_table(rows_rt, cols_rt)
        else:
            referer_types += no_data_notice()

    tabs = tab_bar([("internal-nav", "Internal Navigation"), ("entry-points", "Entry Points"),
                    ("referer-types", "Referer Types")])
    body += tabs
    body += tab_panel("internal-nav", internal_nav)
    body += tab_panel("entry-points", entry_points)
    body += tab_panel("referer-types", referer_types)
    return page("Referer Flow", body)


# ── /reports/utm ──────────────────────────────────────────────────────────

def utm_traffic_where(traffic: str) -> str:
    t = (traffic or "all").strip().lower()
    if t == "human":
        return "hits_human"
    if t == "bot":
        return "hits_bot"
    return "hits"


@app.get("/reports/utm", response_class=HTMLResponse)
def utm_report(
    date_from: Optional[str] = Query(None, alias="from"),
    date_to: Optional[str] = Query(None, alias="to"),
    utm_source: Optional[str] = None,
    group_by: str = "path",
    traffic: str = "all",
    limit: str = "200",
):
    limit = _parse_int(limit, 200)
    utm_sources = _parse_csv_param(utm_source)
    utm_source_param = _fmt_filter_param(utm_sources)
    utm_source_display = _fmt_filter_display(utm_sources)
    sources_table = "utm_sources_daily"
    urls_table = "utm_source_urls_daily"
    sources_paths = list_partitions(sources_table, date_from, date_to)
    urls_paths = list_partitions(urls_table, date_from, date_to)
    avail = available_dates()
    date_from, date_to = _default_last_7(date_from, date_to, avail)
    body = date_filters_html(date_from, date_to, avail)

    if not sources_paths:
        body += (
            "<p><strong>UTM report not available yet.</strong></p>"
            "<p>This page requires aggregate tables: <code>utm_sources_daily</code> and <code>utm_source_urls_daily</code>.</p>"
        )
        return page("UTM Campaigns", body)

    utm_options = distinct_values(sources_table, "utm_source", date_from, date_to)
    group_by = (group_by or "path").strip().lower()
    if group_by not in ("url_group", "path"):
        group_by = "path"
    traffic = (traffic or "all").strip().lower()
    if traffic not in ("all", "human", "bot"):
        traffic = "all"

    body += f"""
    <form method="get" class="filter-bar">
      <input type="hidden" name="from" value="{date_from or ''}">
      <input type="hidden" name="to" value="{date_to or ''}">
      {multi_select_html("utm_source", utm_options, utm_sources, "UTM source")}
      <label>Group by:
        <select name="group_by">
          <option value="url_group" {"selected" if group_by == "url_group" else ""}>url_group</option>
          <option value="path" {"selected" if group_by == "path" else ""}>path</option>
        </select>
      </label>
      <label>Traffic:
        <select name="traffic">
          <option value="all" {"selected" if traffic == "all" else ""}>all</option>
          <option value="human" {"selected" if traffic == "human" else ""}>human</option>
          <option value="bot" {"selected" if traffic == "bot" else ""}>bot</option>
        </select>
      </label>
      <label>Limit: <input name="limit" value="{int(limit)}" size="6"></label>
      <button type="submit">Apply</button>
    </form>
    """

    metric = utm_traffic_where(traffic)

    # ── Source Volume tab ──
    source_vol = ""
    sql_top_sources = f"""
    SELECT utm_source, SUM({metric}) AS hits
    FROM t GROUP BY utm_source ORDER BY hits DESC LIMIT {int(limit)};
    """
    colsS, rowsS = run_query(sources_paths, sql_top_sources)
    source_vol += (
        "<p>"
        f"<a href='/export?report=utm-sources&from={date_from or ''}&to={date_to or ''}&traffic={traffic}&limit={int(limit)}'>Export sources CSV</a>"
        "</p>"
    )
    source_vol += "<h2>Top UTM sources</h2>"
    source_vol += bar_chart(rowsS, colsS, x_col="utm_source", y_col="hits",
                            title=f"Top UTM sources ({traffic})")
    source_vol += html_table(rowsS, colsS, max_rows=min(int(limit), 500))

    if utm_sources:
        src_in = _in_clause("utm_source", utm_sources)
        sql_daily = f"""
        SELECT date, SUM({metric}) AS hits
        FROM t WHERE {src_in}
        GROUP BY date ORDER BY date;
        """
        cols_ud, rows_ud = run_query(sources_paths, sql_daily)
        source_vol += f"<h2>Daily hits — {utm_source_display}</h2>"
        source_vol += (
            f"<p><a href='/export?report=utm&from={date_from or ''}&to={date_to or ''}&utm_source={utm_source_param}&traffic={traffic}'>Export daily CSV</a></p>"
        )
        source_vol += line_chart(rows_ud, cols_ud, x_col="date", y_cols=["hits"],
                                 title=f"UTM {utm_source_display} — Daily hits ({traffic})")
        source_vol += html_table(rows_ud, cols_ud)

    # ── Landing Pages tab ──
    landing = ""
    if not utm_sources:
        landing = "<p>Select a UTM source from the filter above to see landing pages.</p>"
    elif not urls_paths:
        landing = "<p><em>No URL distribution data (missing utm_source_urls_daily partitions).</em></p>"
    else:
        src_in = _in_clause("utm_source", utm_sources)
        dim = "url_group" if group_by == "url_group" else "path"
        sql_urls = f"""
        SELECT {dim} AS dimension, SUM({metric}) AS hits
        FROM t WHERE {src_in} AND {dim} IS NOT NULL
        GROUP BY {dim} ORDER BY hits DESC LIMIT {int(limit)};
        """
        cols_ul, rows_ul = run_query(urls_paths, sql_urls)
        landing += (
            f"<p><a href='/export?report=utm-urls&from={date_from or ''}&to={date_to or ''}&utm_source={utm_source_param}&group_by={group_by}&traffic={traffic}&limit={int(limit)}'>Export URLs CSV</a></p>"
        )
        landing += f"<h2>Top landing {dim} — {utm_source_display}</h2>"
        landing += bar_chart(rows_ul, cols_ul, x_col="dimension", y_col="hits",
                             title=f"Top landing {dim} — {utm_source_display} ({traffic})")
        landing += html_table(rows_ul, cols_ul, max_rows=min(int(limit), 500))

    tabs = tab_bar([("source-volume", "Source Volume"), ("landing-pages", "Landing Pages")])
    body += tabs
    body += tab_panel("source-volume", source_vol)
    body += tab_panel("landing-pages", landing)
    return page("UTM Campaigns", body)


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
    body = date_filters_html(date_from, date_to, avail)

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
        return page("Search Console", body)

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

    def fmt_pos(v):
        try:
            return f"{float(v):.1f}"
        except (TypeError, ValueError):
            return "0.0"

    def fmt_pct2(v):
        try:
            return f"{float(v)*100:.2f}%"
        except (TypeError, ValueError):
            return "0.00%"

    _gsc_col_formatters = {
        "avg_ctr": fmt_pct2,
        "avg_position": fmt_pos,
    }

    def _format_gsc_rows(rows, cols):
        idx = {c: i for i, c in enumerate(cols)}
        fmts = [(idx[c], fn) for c, fn in _gsc_col_formatters.items() if c in idx]
        if not fmts:
            return rows
        out = []
        for r in rows:
            r = list(r)
            for i, fn in fmts:
                if r[i] is not None:
                    r[i] = fn(r[i])
            out.append(r)
        return out

    perf += "<div class='kpi-row'>"
    perf += kpi_card("Total Clicks", c[0], p[0])
    perf += kpi_card("Total Impressions", c[1], p[1])
    perf += kpi_card("Avg CTR", c[2], p[2], fmt_fn=fmt_pct)
    perf += kpi_card("Avg Position", c[3], p[3], fmt_fn=fmt_pos, lower_is_better=True)
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
    return page("Search Console", body)


# ── Legacy route redirects ────────────────────────────────────────────────

@app.get("/reports/crawl-volume")
def _redirect_crawl_volume(date_from: Optional[str] = Query(None, alias="from"), date_to: Optional[str] = Query(None, alias="to")):
    qs = "&".join(f"{k}={v}" for k, v in [("from", date_from), ("to", date_to)] if v)
    return RedirectResponse(url=f"/reports/traffic?tab=overview{'&' + qs if qs else ''}", status_code=301)

@app.get("/reports/status-over-time")
def _redirect_status_over_time(date_from: Optional[str] = Query(None, alias="from"), date_to: Optional[str] = Query(None, alias="to")):
    qs = "&".join(f"{k}={v}" for k, v in [("from", date_from), ("to", date_to)] if v)
    return RedirectResponse(url=f"/reports/status-codes?tab=trends{'&' + qs if qs else ''}", status_code=301)

@app.get("/reports/top-urls")
def _redirect_top_urls(date_from: Optional[str] = Query(None, alias="from"), date_to: Optional[str] = Query(None, alias="to")):
    qs = "&".join(f"{k}={v}" for k, v in [("from", date_from), ("to", date_to)] if v)
    return RedirectResponse(url=f"/reports/content?tab=top-urls{'&' + qs if qs else ''}", status_code=301)

@app.get("/reports/top-404")
def _redirect_top_404(date_from: Optional[str] = Query(None, alias="from"), date_to: Optional[str] = Query(None, alias="to")):
    qs = "&".join(f"{k}={v}" for k, v in [("from", date_from), ("to", date_to)] if v)
    return RedirectResponse(url=f"/reports/status-codes?tab=client-errors{'&' + qs if qs else ''}", status_code=301)

@app.get("/reports/top-4xx")
def _redirect_top_4xx(date_from: Optional[str] = Query(None, alias="from"), date_to: Optional[str] = Query(None, alias="to")):
    qs = "&".join(f"{k}={v}" for k, v in [("from", date_from), ("to", date_to)] if v)
    return RedirectResponse(url=f"/reports/status-codes?tab=client-errors{'&' + qs if qs else ''}", status_code=301)

@app.get("/reports/top-5xx")
def _redirect_top_5xx(date_from: Optional[str] = Query(None, alias="from"), date_to: Optional[str] = Query(None, alias="to")):
    qs = "&".join(f"{k}={v}" for k, v in [("from", date_from), ("to", date_to)] if v)
    return RedirectResponse(url=f"/reports/status-codes?tab=server-errors{'&' + qs if qs else ''}", status_code=301)

@app.get("/reports/top-3xx")
def _redirect_top_3xx(date_from: Optional[str] = Query(None, alias="from"), date_to: Optional[str] = Query(None, alias="to")):
    qs = "&".join(f"{k}={v}" for k, v in [("from", date_from), ("to", date_to)] if v)
    return RedirectResponse(url=f"/reports/status-codes?tab=redirects{'&' + qs if qs else ''}", status_code=301)

@app.get("/reports/url-bytes")
def _redirect_url_bytes(date_from: Optional[str] = Query(None, alias="from"), date_to: Optional[str] = Query(None, alias="to")):
    qs = "&".join(f"{k}={v}" for k, v in [("from", date_from), ("to", date_to)] if v)
    return RedirectResponse(url=f"/reports/content?tab=page-size{'&' + qs if qs else ''}", status_code=301)

@app.get("/reports/url-groups")
def _redirect_url_groups(date_from: Optional[str] = Query(None, alias="from"), date_to: Optional[str] = Query(None, alias="to")):
    qs = "&".join(f"{k}={v}" for k, v in [("from", date_from), ("to", date_to)] if v)
    return RedirectResponse(url=f"/reports/content?tab=url-groups{'&' + qs if qs else ''}", status_code=301)

@app.get("/reports/locale-groups")
def _redirect_locale_groups(date_from: Optional[str] = Query(None, alias="from"), date_to: Optional[str] = Query(None, alias="to")):
    qs = "&".join(f"{k}={v}" for k, v in [("from", date_from), ("to", date_to)] if v)
    return RedirectResponse(url=f"/reports/locales?tab=heatmap{'&' + qs if qs else ''}", status_code=301)

@app.get("/reports/wasted-crawl")
def _redirect_wasted_crawl(date_from: Optional[str] = Query(None, alias="from"), date_to: Optional[str] = Query(None, alias="to")):
    qs = "&".join(f"{k}={v}" for k, v in [("from", date_from), ("to", date_to)] if v)
    return RedirectResponse(url=f"/reports/bots?tab=crawl-waste{'&' + qs if qs else ''}", status_code=301)

@app.get("/reports/top-resource-waste")
def _redirect_top_resource_waste(date_from: Optional[str] = Query(None, alias="from"), date_to: Optional[str] = Query(None, alias="to")):
    qs = "&".join(f"{k}={v}" for k, v in [("from", date_from), ("to", date_to)] if v)
    return RedirectResponse(url=f"/reports/bots?tab=resource-waste{'&' + qs if qs else ''}", status_code=301)

@app.get("/reports/human-urls")
def _redirect_human_urls(date_from: Optional[str] = Query(None, alias="from"), date_to: Optional[str] = Query(None, alias="to")):
    qs = "&".join(f"{k}={v}" for k, v in [("from", date_from), ("to", date_to)] if v)
    return RedirectResponse(url=f"/reports/content?tab=human-paths{'&' + qs if qs else ''}", status_code=301)

@app.get("/reports/bot-urls")
def _redirect_bot_urls(bot: Optional[str] = None, date_from: Optional[str] = Query(None, alias="from"), date_to: Optional[str] = Query(None, alias="to")):
    parts = []
    if bot: parts.append(f"bot={bot}")
    if date_from: parts.append(f"from={date_from}")
    if date_to: parts.append(f"to={date_to}")
    parts.append("tab=bot-families")
    return RedirectResponse(url=f"/reports/bots?{'&'.join(parts)}#urls", status_code=301)

@app.get("/reports/utm-chatgpt")
def _redirect_utm_chatgpt(date_from: Optional[str] = Query(None, alias="from"), date_to: Optional[str] = Query(None, alias="to")):
    return RedirectResponse(url=f"/reports/utm?from={date_from or ''}&to={date_to or ''}&utm_source=chatgpt.com", status_code=302)

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
    include_assets: bool = False,
    strict: bool = False,
    bot: Optional[str] = None,
    url_group: Optional[str] = None,
    locale: Optional[str] = None,
    # NEW for UTM
    utm_source: Optional[str] = None,
    group_by: str = "url_group",
    traffic: str = "all",
    limit: int = 100000,
):
    report = report.strip().lower()
    bots_sel = _parse_csv_param(bot)
    url_groups = _parse_csv_param(url_group)
    locales_sel = _parse_csv_param(locale)
    utm_sources = _parse_csv_param(utm_source)

    def stream_csv(sql: str, paths: List[str], filename: str):
        if not paths:
            return PlainTextResponse("No data found for the selected date range.", status_code=404)

        wrapped_sql = _wrap_with_parquet_cte(paths, sql)
        cur = _get_db().cursor()

        def gen():
            try:
                res = cur.execute(wrapped_sql)
                cols = [d[0] for d in res.description]

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

    if report == "daily":
        paths = list_partitions("daily", date_from, date_to)
        sql = "SELECT * FROM t ORDER BY date;"
        return stream_csv(sql, paths, "daily.csv")

    if report == "daily-status":
        paths = list_partitions("daily", date_from, date_to)
        sql = "SELECT date, s2xx, s3xx, s4xx, s5xx, hits FROM t ORDER BY date;"
        return stream_csv(sql, paths, "daily_status.csv")

    if report == "top-urls":
        paths = list_partitions("top_urls_daily", date_from, date_to)
        clauses = []
        if not include_assets:
            clauses.append("url_group NOT IN ('Nuxt Assets','Static Assets')")
        group_clause = _in_clause("url_group", url_groups)
        if group_clause:
            clauses.append(group_clause)
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        sql = f"""
        SELECT path, url_group,
                SUM(hits_total) AS hits_total,
                SUM(hits_bot) AS hits_bot,
                SUM(s3xx) AS s3xx,
                SUM(s4xx) AS s4xx,
                SUM(s5xx) AS s5xx,
                SUM(parameterized_hits) AS parameterized_hits
        FROM t
        {where}
        GROUP BY path, url_group
        ORDER BY hits_total DESC
        LIMIT {int(limit)};
        """
        return stream_csv(sql, paths, "top_urls.csv")

    if report in ("top-404", "top-4xx"):
        paths = list_partitions("top_4xx_daily", date_from, date_to)
        clauses = []
        if not include_assets:
            clauses.append("url_group NOT IN ('Nuxt Assets','Static Assets')")
        group_clause = _in_clause("url_group", url_groups)
        if group_clause:
            clauses.append(group_clause)
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        sql = f"""
        SELECT path, url_group,
                SUM(hits_4xx) AS hits_4xx,
                SUM(hits_4xx_bot) AS hits_4xx_bot,
                SUM(s400) AS s400,
                SUM(s401) AS s401,
                SUM(s403) AS s403,
                SUM(s404) AS s404,
                SUM(s405) AS s405,
                SUM(s410) AS s410,
                SUM(s422) AS s422,
                SUM(s429) AS s429
        FROM t
        {where}
        GROUP BY path, url_group
        ORDER BY hits_4xx DESC
        LIMIT {int(limit)};
        """
        return stream_csv(sql, paths, "top_4xx.csv")

    if report == "top-5xx":
        paths = list_partitions("top_5xx_daily", date_from, date_to)
        clauses = []
        if not include_assets:
            clauses.append("url_group NOT IN ('Nuxt Assets','Static Assets')")
        group_clause = _in_clause("url_group", url_groups)
        if group_clause:
            clauses.append(group_clause)
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        sql = f"""
        SELECT path, url_group,
                SUM(hits_5xx) AS hits_5xx,
                SUM(hits_5xx_bot) AS hits_5xx_bot,
                SUM(s500) AS s500,
                SUM(s502) AS s502,
                SUM(s503) AS s503,
                SUM(s504) AS s504
        FROM t
        {where}
        GROUP BY path, url_group
        ORDER BY hits_5xx DESC
        LIMIT {int(limit)};
        """
        return stream_csv(sql, paths, "top_5xx.csv")

    if report == "top-3xx":
        paths = list_partitions("top_urls_daily", date_from, date_to)
        clauses = []
        if not include_assets:
            clauses.append("url_group NOT IN ('Nuxt Assets','Static Assets')")
        group_clause = _in_clause("url_group", url_groups)
        if group_clause:
            clauses.append(group_clause)
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        sql = f"""
        SELECT path, url_group,
               SUM(s3xx) AS s3xx,
               SUM(s301) AS s301, SUM(s302) AS s302, SUM(s303) AS s303,
               SUM(s307) AS s307, SUM(s308) AS s308,
               SUM(hits_total) AS hits_total,
               ROUND(100.0 * SUM(s3xx) / NULLIF(SUM(hits_total), 0), 1) AS redirect_pct
        FROM t
        {where}
        GROUP BY path, url_group
        HAVING SUM(s3xx) > 0
        ORDER BY s3xx DESC
        LIMIT {int(limit)};
        """
        return stream_csv(sql, paths, "top_3xx.csv")

    if report == "url-bytes":
        paths = list_partitions("top_urls_daily", date_from, date_to)
        clauses = []
        if not include_assets:
            clauses.append("url_group NOT IN ('Nuxt Assets','Static Assets')")
        group_clause = _in_clause("url_group", url_groups)
        if group_clause:
            clauses.append(group_clause)
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        sql = f"""
        SELECT path, url_group,
               SUM(bytes_sent) AS bytes_total,
               SUM(hits_total) AS hits_total,
               ROUND(SUM(bytes_sent) / NULLIF(SUM(hits_total), 0)) AS avg_bytes_per_hit
        FROM t
        {where}
        GROUP BY path, url_group
        HAVING SUM(bytes_sent) > 0
        ORDER BY avg_bytes_per_hit DESC
        LIMIT {int(limit)};
        """
        return stream_csv(sql, paths, "url_bytes.csv")

    if report == "bots":
        paths = list_partitions("bot_daily", date_from, date_to)
        sql = f"""
        SELECT bot_family,
                SUM(hits) AS hits,
                SUM(resource_hits) AS resource_hits,
                AVG(resource_hits_pct) AS avg_resource_pct,
                SUM(s4xx) AS s4xx,
                SUM(s5xx) AS s5xx
        FROM t
        GROUP BY bot_family
        ORDER BY hits DESC
        LIMIT {int(limit)};
        """
        return stream_csv(sql, paths, "bots.csv")

    if report == "ai-crawlers":
        paths = list_partitions("ai_crawler_daily", date_from, date_to)
        sql = f"""
        SELECT bot_family,
                SUM(total_hits) AS total_hits,
                SUM(unique_urls) AS unique_urls,
                SUM(bytes_sent) AS bytes_sent,
                AVG(overlap_with_googlebot) AS avg_overlap
        FROM t
        GROUP BY bot_family
        ORDER BY total_hits DESC
        LIMIT {int(limit)};
        """
        return stream_csv(sql, paths, "ai_crawlers.csv")

    if report == "wasted-crawl":
        paths = list_partitions("wasted_crawl_daily", date_from, date_to)
        score_col = "waste_score_strict" if strict else "waste_score"
        clauses = []
        bot_clause = _in_clause("bot_family", bots_sel)
        if bot_clause:
            clauses.append(bot_clause)
        if not include_assets:
            clauses.append("url_group NOT IN ('Nuxt Assets','Static Assets')")
        group_clause = _in_clause("url_group", url_groups)
        if group_clause:
            clauses.append(group_clause)
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        score_col = "waste_score_strict" if strict else "waste_score"

        sql = f"""
        SELECT bot_family, url_group, path,
            SUM(bot_hits) AS bot_hits,
            SUM(parameterized_bot_hits) AS parameterized_bot_hits,
            SUM(redirect_bot_hits) AS redirect_bot_hits,
            SUM(error_bot_hits) AS error_bot_hits,
            SUM(resource_bot_hits) AS resource_bot_hits,
            SUM(resource_parameterized_bot_hits) AS resource_parameterized_bot_hits,
            SUM(resource_error_bot_hits) AS resource_error_bot_hits,
            SUM({score_col}) AS waste_score
        FROM t
        {where}
        GROUP BY bot_family, url_group, path
        ORDER BY waste_score DESC, bot_hits DESC
        LIMIT {int(limit)};
        """
        return stream_csv(sql, paths, "wasted_crawl.csv")

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

    if report == "url-groups":
        paths = list_partitions("group_daily", date_from, date_to)
        sql = """
        SELECT url_group,
                SUM(hits) AS hits,
                SUM(hits_bot) AS hits_bot,
                SUM(hits_human) AS hits_human,
                SUM(s4xx) AS s4xx,
                SUM(s5xx) AS s5xx,
                SUM(resource_hits) AS resource_hits,
                SUM(resource_hits_bot) AS resource_hits_bot
        FROM t
        GROUP BY url_group
        ORDER BY hits DESC;
        """
        return stream_csv(sql, paths, "url_groups.csv")

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

    if report == "top-resource-waste":
        paths = list_partitions("top_resource_waste_daily", date_from, date_to)
        sql = f"""
        SELECT path,
                SUM(bot_hits) AS bot_hits,
                SUM(resource_error_bot_hits) AS resource_error_bot_hits,
                SUM(status_404_bot_hits) AS status_404_bot_hits,
                SUM(waste_score_strict) AS waste_score_strict
        FROM t
        GROUP BY path
        ORDER BY waste_score_strict DESC
        LIMIT {int(limit)};
        """
        return stream_csv(sql, paths, "top_resource_waste.csv")

    if report == "bot-urls":
        paths = list_partitions("bot_urls_daily", date_from, date_to)

        clauses = []
        bot_clause = _in_clause("bot_family", bots_sel)
        if bot_clause:
            clauses.append(bot_clause)
        if not include_assets:
            clauses.append("url_group NOT IN ('Nuxt Assets','Static Assets')")
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""

        sql = f"""
            SELECT path, url_group, SUM(hits) AS hits
            FROM t
            {where}
            GROUP BY path, url_group
            ORDER BY hits DESC
            LIMIT {int(limit)};
        """
        return stream_csv(sql, paths, "bot_urls.csv")

    # NEW: generic UTM exports
    metric = utm_traffic_where(traffic)

    if report == "utm-sources":
        paths = list_partitions("utm_sources_daily", date_from, date_to)
        sql = f"""
        SELECT utm_source, SUM({metric}) AS hits
        FROM t
        GROUP BY utm_source
        ORDER BY hits DESC
        LIMIT {int(limit)};
        """
        return stream_csv(sql, paths, "utm_sources.csv")

    if report == "utm":
        paths = list_partitions("utm_sources_daily", date_from, date_to)
        if not utm_sources:
            return PlainTextResponse("Missing utm_source parameter.", status_code=400)
        src_in = _in_clause("utm_source", utm_sources)
        sql = f"""
        SELECT date, SUM({metric}) AS hits
        FROM t
        WHERE {src_in}
        GROUP BY date
        ORDER BY date;
        """
        filename_slug = "_".join(utm_sources) if len(utm_sources) <= 3 else f"{len(utm_sources)}sources"
        return stream_csv(sql, paths, f"utm_{filename_slug}_daily.csv")

    if report == "utm-urls":
        paths = list_partitions("utm_source_urls_daily", date_from, date_to)
        if not utm_sources:
            return PlainTextResponse("Missing utm_source parameter.", status_code=400)

        gb = (group_by or "url_group").strip().lower()
        if gb not in ("url_group", "path"):
            gb = "url_group"
        dim = "url_group" if gb == "url_group" else "path"

        src_in = _in_clause("utm_source", utm_sources)
        sql = f"""
        SELECT {dim} AS dimension, SUM({metric}) AS hits
        FROM t
        WHERE {src_in}
          AND {dim} IS NOT NULL
        GROUP BY {dim}
        ORDER BY hits DESC
        LIMIT {int(limit)};
        """
        filename_slug = "_".join(utm_sources) if len(utm_sources) <= 3 else f"{len(utm_sources)}sources"
        return stream_csv(sql, paths, f"utm_{filename_slug}_{dim}.csv")

    if report == "hourly":
        paths = list_partitions("hourly", date_from, date_to)
        sql = """
        SELECT hour, SUM(hits) AS hits, SUM(hits_bot) AS hits_bot, SUM(hits_human) AS hits_human
        FROM t GROUP BY hour ORDER BY hour;
        """
        return stream_csv(sql, paths, "hourly.csv")

    if report == "human-urls":
        paths = list_partitions("human_urls_daily", date_from, date_to)
        clauses = []
        if not include_assets:
            clauses.append("url_group NOT IN ('Nuxt Assets','Static Assets')")
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        sql = f"""
        SELECT url_group, path, SUM(hits) AS hits
        FROM t {where}
        GROUP BY url_group, path ORDER BY hits DESC LIMIT {int(limit)};
        """
        return stream_csv(sql, paths, "human_urls.csv")

    if report == "referer-flow-internal":
        paths = list_partitions("referer_flow_daily", date_from, date_to)
        has_rp = False
        if paths:
            try:
                _, _r = run_query(paths, "SELECT column_name FROM (DESCRIBE t) WHERE column_name = 'referer_path'")
                has_rp = bool(_r)
            except Exception:
                pass
        if not has_rp:
            return PlainTextResponse("Internal navigation data not available for this date range.", status_code=404)
        sql = f"""
        SELECT referer_path AS from_path, path AS to_path, SUM(hits) AS hits
        FROM t WHERE referer_type = 'Internal' AND referer_path IS NOT NULL
        GROUP BY referer_path, path ORDER BY hits DESC LIMIT {int(limit)};
        """
        return stream_csv(sql, paths, "referer_flow_internal.csv")

    if report == "referer-flow-entry":
        paths = list_partitions("referer_flow_daily", date_from, date_to)
        sql = f"""
        SELECT referer_type, path AS landing_page, SUM(hits) AS hits
        FROM t GROUP BY referer_type, path ORDER BY hits DESC LIMIT {int(limit)};
        """
        return stream_csv(sql, paths, "referer_flow_entry.csv")

    if report == "referer-flow-types":
        paths = list_partitions("referer_flow_daily", date_from, date_to)
        sql = """
        SELECT referer_type, SUM(hits) AS hits
        FROM t GROUP BY referer_type ORDER BY hits DESC;
        """
        return stream_csv(sql, paths, "referer_flow_types.csv")

    # GSC exports
    if report == "gsc-top-clicks":
        paths = list_partitions("gsc_daily", date_from, date_to)
        sql = f"""
        SELECT page, SUM(clicks) AS clicks, SUM(impressions) AS impressions,
               AVG(ctr) AS avg_ctr, AVG(position) AS avg_position,
               SUM(crawl_hits) AS crawl_hits
        FROM t WHERE has_impressions
        GROUP BY page ORDER BY clicks DESC LIMIT {int(limit)};
        """
        return stream_csv(sql, paths, "gsc_top_clicks.csv")

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
        return stream_csv(sql, paths, "gsc_impressions_no_crawl.csv")

    return PlainTextResponse(
        "Unknown report. Try report=daily, daily-status, hourly, locales, url-groups, locale-groups, "
        "top-urls, top-3xx, top-4xx, top-5xx, url-bytes, bots, wasted-crawl, top-resource-waste, "
        "bot-urls, human-urls, utm-sources, utm, utm-urls, referer-flow-internal, referer-flow-entry, "
        "referer-flow-types, gsc-top-clicks, gsc-high-crawl-no-impressions, gsc-impressions-no-crawl",
        status_code=400,
    )


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


@app.get("/logs", response_class=HTMLResponse)
def log_viewer(
    date_from: Optional[str] = Query(None, alias="from"),
    date_to: Optional[str] = Query(None, alias="to"),
    page_num: int = Query(1, alias="page"),
    per_page: int = Query(100, alias="per_page"),
    search: Optional[str] = Query(None),
    search_mode: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    method: Optional[str] = Query(None),
    is_bot: Optional[str] = Query(None),
    bot_family: Optional[str] = Query(None),
    url_group: Optional[str] = Query(None),
    locale: Optional[str] = Query(None),
    country: Optional[str] = Query(None),
    sort: str = Query("ts_utc"),
    order: str = Query("desc"),
    chart: Optional[str] = Query(None),
):
    status_codes = _parse_status_list(status)
    methods = _parse_csv_param(method)
    bot_families = _parse_csv_param(bot_family)
    url_groups = _parse_csv_param(url_group)
    locales = _parse_csv_param(locale)
    countries = _parse_csv_param(country)
    show_chart = chart == "1"
    if search_mode not in ("contains", "not_contains", "regex"):
        search_mode = "contains"

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

    # ── Populate filter dropdown options ──
    status_opts = distinct_parsed_values("status", date_from, date_to) if paths else []
    bot_family_opts = distinct_parsed_values("bot_family", date_from, date_to) if paths else []
    url_group_opts = distinct_parsed_values("url_group", date_from, date_to) if paths else []
    locale_opts = distinct_parsed_values("locale", date_from, date_to) if paths else []
    country_opts = distinct_parsed_values("country", date_from, date_to) if paths and has_country_col else []

    # ── Filter form ──
    body = ""

    # Date filters
    min_date = avail[0] if avail else ""
    max_date = avail[-1] if avail else ""
    body += "<form method='get' class='filter-bar'>"
    body += f"<label>From<input type='date' name='from' value='{date_from or ''}' min='{min_date}' max='{max_date}'></label>"
    body += f"<label>To<input type='date' name='to' value='{date_to or ''}' min='{min_date}' max='{max_date}'></label>"
    # Search with mode selector
    body += f"<label>Search<input type='text' name='search' value='{search or ''}' placeholder='path, IP, UA, referer'></label>"
    body += "<label>Mode<select name='search_mode'>"
    for val, lbl in [("contains", "contains"), ("not_contains", "does not contain"), ("regex", "regex")]:
        sel = "selected" if search_mode == val else ""
        body += f"<option value='{val}' {sel}>{lbl}</option>"
    body += "</select></label>"

    # Multi-select filters
    method_opts = ["GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS", "PATCH"]
    body += multi_select_html("status", status_opts, [str(c) for c in status_codes], "Status")
    body += multi_select_html("method", method_opts, methods, "Method")

    # Bot filter
    body += "<label>Bot<select name='is_bot'><option value=''>All</option>"
    for val, lbl in [("true", "Bots only"), ("false", "Humans only")]:
        sel = "selected" if is_bot == val else ""
        body += f"<option value='{val}' {sel}>{lbl}</option>"
    body += "</select></label>"

    # Column filters (multi-select)
    body += multi_select_html("bot_family", bot_family_opts, bot_families, "Bot family")
    body += multi_select_html("url_group", url_group_opts, url_groups, "URL group")
    body += multi_select_html("locale", locale_opts, locales, "Locale")
    if has_country_col:
        body += multi_select_html("country", country_opts, countries, "Country")

    body += f"<label>Per page<select name='per_page'>"
    for pp in [25, 50, 100, 200]:
        sel = "selected" if per_page == pp else ""
        body += f"<option value='{pp}' {sel}>{pp}</option>"
    body += "</select></label>"

    chart_checked = "checked" if show_chart else ""
    body += f" <label><input type='checkbox' name='chart' value='1' {chart_checked}> Show chart</label>"

    body += " <button type='submit'>Apply</button></form>"

    if not paths:
        return page("Log Viewer", body + no_data_notice())

    # ── Build SQL query ──
    clauses: list[str] = []
    if status_codes:
        if len(status_codes) == 1:
            clauses.append(f"status = {status_codes[0]}")
        else:
            clauses.append(f"status IN ({','.join(str(c) for c in status_codes)})")
    for clause in (
        _in_clause("method", methods),
        _in_clause("bot_family", bot_families),
        _in_clause("url_group", url_groups),
        _in_clause("locale", locales),
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
    if search:
        esc = sql_escape_string(search)
        search_cols = ["path", "edge_ip", "user_agent", "referer"]
        if search_mode == "not_contains":
            clauses.append(
                " AND ".join(f"{col} NOT ILIKE '%{esc}%'" for col in search_cols)
            )
        elif search_mode == "regex":
            clauses.append(
                "(" + " OR ".join(f"regexp_matches({col}, '{esc}')" for col in search_cols) + ")"
            )
        else:
            clauses.append(
                "(" + " OR ".join(f"{col} ILIKE '%{esc}%'" for col in search_cols) + ")"
            )

    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""

    # ── Chart aggregation (when requested) ──
    chart_html = ""
    if show_chart and paths:
        single_day = date_from and date_to and date_from == date_to
        if single_day:
            time_expr = "date_trunc('hour', ts_utc)"
            granularity_label = "hourly"
        else:
            time_expr = "CAST(ts_utc AS DATE)"
            granularity_label = "daily"
        chart_sql = f"""
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
        chart_cols, chart_rows = run_query(paths, chart_sql)
        chart_html = line_chart(
            chart_rows, chart_cols, x_col="time_bucket",
            y_cols=["s2xx", "s3xx", "s4xx", "s5xx"],
            title=f"Requests over time ({granularity_label})",
        )

    # Validate sort column
    allowed_sort = {"ts_utc", "edge_ip", "method", "path", "status", "bytes_sent", "is_bot", "bot_family", "user_agent", "url_group", "locale"}
    if has_country_col:
        allowed_sort.add("country")
    sort_col = sort if sort in allowed_sort else "ts_utc"
    sort_dir = "ASC" if order.lower() == "asc" else "DESC"

    # Clamp pagination
    per_page = min(max(per_page, 25), 200)
    page_num = max(page_num, 1)
    offset = (page_num - 1) * per_page

    # Count total rows
    count_sql = f"SELECT COUNT(*) FROM t {where};"
    _, count_rows = run_query(paths, count_sql)
    total = count_rows[0][0] if count_rows else 0

    # Fetch page
    country_col = "CAST(country AS VARCHAR) AS country" if has_country_col else "NULL AS country"
    data_sql = f"""
    SELECT ts_utc, edge_ip, method, path, status, bytes_sent,
           is_bot, bot_family, user_agent, url_group, locale, {country_col}
    FROM t
    {where}
    ORDER BY {sort_col} {sort_dir}
    LIMIT {per_page} OFFSET {offset};
    """
    cols, rows = run_query(paths, data_sql)

    # Format rows for display
    display_rows = []
    for r in rows:
        row = list(r)
        # ts_utc (idx 0): format nicely
        if row[0] is not None:
            row[0] = str(row[0])[:19]
        # bytes_sent (idx 5): human-readable
        row[5] = fmt_bytes(row[5])
        # is_bot (idx 6): label
        row[6] = "Bot" if row[6] else "Human"
        # user_agent (idx 8): truncate
        if row[8] and len(str(row[8])) > 80:
            row[8] = str(row[8])[:80] + "\u2026"
        display_rows.append(row)

    # ── Pagination info ──
    total_pages = max(1, (total + per_page - 1) // per_page)
    page_num = min(page_num, total_pages)

    # Build base query string for pagination links
    params: dict[str, str] = {}
    if date_from:
        params["from"] = date_from
    if date_to:
        params["to"] = date_to
    if search:
        params["search"] = search
    if search_mode and search_mode != "contains":
        params["search_mode"] = search_mode
    if status_codes:
        params["status"] = ",".join(str(c) for c in status_codes)
    if methods:
        params["method"] = ",".join(methods)
    if is_bot:
        params["is_bot"] = is_bot
    if bot_families:
        params["bot_family"] = ",".join(bot_families)
    if url_groups:
        params["url_group"] = ",".join(url_groups)
    if locales:
        params["locale"] = ",".join(locales)
    if countries:
        params["country"] = ",".join(countries)
    if per_page != 100:
        params["per_page"] = str(per_page)
    if sort != "ts_utc":
        params["sort"] = sort
    if order != "desc":
        params["order"] = order
    if show_chart:
        params["chart"] = "1"

    def page_link(p: int, label: str) -> str:
        qs = "&".join(f"{k}={v}" for k, v in params.items())
        return f"<a href='/logs?page={p}&{qs}' style='padding:4px 10px;border:1px solid #cbd5e1;border-radius:4px;text-decoration:none;color:#3b82f6;font-size:13px;margin:0 2px;'>{label}</a>"

    hidden_inputs = "".join(
        f"<input type='hidden' name='{k}' value='{html_escape(v)}'>"
        for k, v in params.items() if k != "page"
    )
    jump_form = (
        "<form method='get' action='/logs' style='display:inline-flex;align-items:center;gap:4px;margin:0;'>"
        f"{hidden_inputs}"
        "<label style='font-size:12px;color:#64748b;text-transform:none;letter-spacing:0;font-weight:500;'>Go to page "
        f"<input type='number' name='page' value='{page_num}' min='1' max='{total_pages}' "
        "style='width:64px;padding:3px 6px;border:1px solid #cbd5e1;border-radius:4px;font-size:12px;'>"
        "</label>"
        "<button type='submit' style='padding:3px 10px;border:1px solid #cbd5e1;border-radius:4px;"
        "background:#f8fafc;color:#3b82f6;font-size:12px;cursor:pointer;'>Go</button>"
        "</form>"
    )

    pagination = f"<div style='display:flex;align-items:center;gap:8px;margin:12px 0;flex-wrap:wrap;'>"
    pagination += f"<span style='font-size:13px;color:#64748b;'>{total:,} rows &middot; page {page_num} of {total_pages:,}</span>"
    if page_num > 1:
        pagination += page_link(1, "&laquo; First")
        pagination += page_link(page_num - 1, "&lsaquo; Prev")
    pagination += jump_form
    if page_num < total_pages:
        pagination += page_link(page_num + 1, "Next &rsaquo;")
        pagination += page_link(total_pages, "Last &raquo;")
    pagination += "</div>"

    if chart_html:
        body += chart_html
    body += pagination
    body += html_table(display_rows, cols, max_rows=per_page, server_paginated=True)
    body += pagination

    return page("Log Viewer", body)


@app.get("/logs/detail", response_class=HTMLResponse)
def log_detail(
    date: str = Query(...),
    row: int = Query(...),
):
    """Show all fields for a single log entry identified by date + row offset."""
    paths = list_parsed_partitions(date, date)
    if not paths:
        return page("Log Detail", "<p class='no-data'>No data for this date.</p>")

    sql = f"""
    SELECT *
    FROM t
    LIMIT 1 OFFSET {max(0, int(row))};
    """
    cols, rows = run_query(paths, sql)
    if not rows:
        return page("Log Detail", "<p class='no-data'>Row not found.</p>")

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