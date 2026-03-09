from __future__ import annotations

import os
from pathlib import Path
from typing import List, Optional

import duckdb
from fastapi import FastAPI, Query
from fastapi.responses import (
    HTMLResponse,
    StreamingResponse,
    PlainTextResponse,
    RedirectResponse,
)

import csv
import io

import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

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


def list_partitions(table: str, date_from: Optional[str], date_to: Optional[str]) -> List[str]:
    """
    We store partitions as: data/aggregates/<table>/date=YYYY-MM-DD/part.parquet
    To keep this beginner-friendly, we scan the filesystem and filter by date string.
    """
    base = AGG / table
    if not base.exists():
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
    return sorted(out)

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


def html_table(rows, columns, max_rows: int = 999) -> str:
    """
    Styled HTML table with client-side sorting.
    Click a header to sort; click again to reverse.
    """
    head = "".join(
        f"<th scope='col' data-col='{i}' title='{STATUS_CODE_LABELS.get(c, '')}'>{c}</th>"
        for i, c in enumerate(columns)
    )

    body_rows = []
    for i, r in enumerate(rows):
        if i >= max_rows:
            break
        tds = "".join(f"<td>{'' if v is None else v}</td>" for v in r)
        body_rows.append(f"<tr>{tds}</tr>")
    body = "\n".join(body_rows)

    table = (
        "<table class='sortable'>"
        f"<thead><tr>{head}</tr></thead>"
        f"<tbody>{body}</tbody>"
        "</table>"
    )
    return f"<div class='card'><div class='table-wrapper'>{table}</div></div>"


def run_query(paths: List[str], sql: str):
    if not paths:
        return [], []
    conn = duckdb.connect(database=":memory:")
    conn.execute("PRAGMA threads=4;")

    files_sql = "[" + ",".join("'" + p.replace("'", "''") + "'" for p in paths) + "]"
    conn.execute(f"CREATE OR REPLACE VIEW t AS SELECT * FROM read_parquet({files_sql});")

    res = conn.execute(sql)
    cols = [d[0] for d in res.description]
    rows = res.fetchall()
    conn.close()
    return cols, rows


def _coerce_numeric(df: pd.DataFrame, cols: List[str]) -> None:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")


def _coerce_datetime(df: pd.DataFrame, col: str) -> None:
    if col in df.columns:
        # Works for 'YYYY-MM-DD' and for timestamps like '2026-02-18 10:00:00+08:00' etc.
        df[col] = pd.to_datetime(df[col], errors="coerce")


def line_chart(rows, columns, x_col, y_cols, title):
    if not rows:
        return "<p>No data.</p>"

    df = pd.DataFrame(rows, columns=columns)

    # x: stringify dates/timestamps so JSON is simple and Plotly parses it reliably
    x_series = df[x_col] if x_col in df.columns else pd.Series([])
    x = [None if pd.isna(v) else str(v) for v in x_series.tolist()]

    fig = go.Figure()

    status_colors = {
        "s2xx": "#03e200",
        "s3xx": "#fce839",
        "s4xx": "#ffb03d",
        "s5xx": "#ff2a07",
    }


    # y: force float conversion; invalid -> None (so Plotly will gap)
    for yc in y_cols:
        if yc not in df.columns:
            continue
        y_raw = df[yc].tolist()
        y = []
        for v in y_raw:
            if v is None or (isinstance(v, float) and pd.isna(v)) or pd.isna(v):
                y.append(None)
            else:
                try:
                    y.append(float(v))
                except Exception:
                    y.append(None)

        color = status_colors.get(yc)
        fig.add_trace(go.Scatter(x=x, y=y, mode="lines", name=STATUS_CODE_LABELS.get(yc, yc), line=dict(color=color) if color else {}))

    fig.update_layout(height=400, margin=dict(l=20, r=20, t=40, b=20), title=title)
    chart_html = fig.to_html(full_html=False, include_plotlyjs=False)
    return f"<div class='card'>{chart_html}</div>"


def bar_chart(rows, columns, x_col, y_col=None, title="", y_cols=None, barmode="group"):
    """Bar chart. Pass y_cols (list) for grouped/stacked multi-series; y_col for single series."""
    if not rows:
        return "<p>No data.</p>"

    df = pd.DataFrame(rows, columns=columns)

    x_series = df[x_col] if x_col in df.columns else pd.Series([])
    x = ["" if v is None or pd.isna(v) else str(v) for v in x_series.tolist()]

    series_cols = y_cols if y_cols else [y_col]

    fig = go.Figure()
    for yc in series_cols:
        if yc not in df.columns:
            continue
        y_raw = df[yc].tolist()
        y = []
        for v in y_raw:
            if v is None or (isinstance(v, float) and pd.isna(v)) or pd.isna(v):
                y.append(None)
            else:
                try:
                    y.append(float(v))
                except Exception:
                    y.append(None)
        fig.add_trace(go.Bar(name=STATUS_CODE_LABELS.get(yc, yc), x=x, y=y))

    fig.update_layout(
        height=400,
        margin=dict(l=20, r=20, t=40, b=20),
        title=title,
        barmode=barmode,
    )
    chart_html = fig.to_html(full_html=False, include_plotlyjs=False)
    return f"<div class='card'>{chart_html}</div>"


def heatmap_chart(rows, columns, x_col, y_col, z_col, title):
    """Heatmap where x_col=columns, y_col=rows, z_col=values (e.g. locale × url_group)."""
    if not rows:
        return "<p>No data.</p>"

    df = pd.DataFrame(rows, columns=columns)

    pivot = df.pivot_table(index=y_col, columns=x_col, values=z_col, aggfunc="sum", fill_value=0)
    z = [[0.0 if (v is None or (isinstance(v, float) and pd.isna(v))) else float(v) for v in row] for row in pivot.values]
    x_labels = [str(c) for c in pivot.columns]
    y_labels = [str(r) for r in pivot.index]

    fig = go.Figure(data=go.Heatmap(
        z=z, x=x_labels, y=y_labels,
        colorscale="Blues", hoverongaps=False,
    ))
    fig.update_layout(
        height=min(max(300, 30 * len(y_labels) + 80), 1800),
        margin=dict(l=20, r=20, t=40, b=60),
        title=title,
    )
    chart_html = fig.to_html(full_html=False, include_plotlyjs=False)
    return f"<div class='card'>{chart_html}</div>"


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


def no_data_notice() -> str:
    return "<p class='no-data'>No data found for the selected date range.</p>"


def list_tables() -> List[str]:
    """Return aggregate table names that have at least one parquet partition."""
    if not AGG.exists():
        return []
    return sorted(
        p.name for p in AGG.iterdir()
        if p.is_dir() and any(p.glob("date=*/part.parquet"))
    )


TABLE_DESCRIPTIONS = {
    "daily": "Daily totals — hits, status codes, bytes sent",
    "locale_daily": "Daily hits broken down by locale",
    "group_daily": "Daily hits broken down by URL group",
    "locale_group_daily": "Daily hits by locale × URL group",
    "bot_daily": "Daily hits by bot family",
    "top_urls_daily": "Top requested paths per day",
    "top_4xx_daily": "Top 4xx paths per day (all client errors, broken down by code)",
    "top_5xx_daily": "Top 5xx paths per day",
    "wasted_crawl_daily": "Wasted bot crawl by bot and path",
    "top_resource_waste_daily": "Resource waste score by path",
    "bot_urls_daily": "URLs crawled by specific bots",
    "human_urls_daily": "URLs visited by human traffic",
    "utm_sources_daily": "UTM source hit counts by day",
    "utm_source_urls_daily": "UTM source URL distribution",
}


def select_html(name: str, options: List[str], current: Optional[str], label: str) -> str:
    opts = ["<option value=''>All</option>"]
    for opt in options:
        selected = "selected" if current == opt else ""
        opts.append(f"<option value='{opt}' {selected}>{opt}</option>")
    return f"<label>{label}: <select name='{name}'>{''.join(opts)}</select></label>"


def page(title: str, body: str) -> HTMLResponse:

    plotly_cdn = "<script src='https://cdn.plot.ly/plotly-latest.min.js'></script>"

    nav_items = [
        ("Home", "/"),
        ("Crawl volume", "/reports/crawl-volume"),
        ("Status over time", "/reports/status-over-time"),
        ("Locale breakdown", "/reports/locales"),
        ("URL group breakdown", "/reports/url-groups"),
        ("Locale x URL group", "/reports/locale-groups"),
        ("Top URLs", "/reports/top-urls"),
        ("Top 3xx", "/reports/top-3xx"),
        ("Top 4xx", "/reports/top-4xx"),
        ("Top 5xx", "/reports/top-5xx"),
        ("URL byte size", "/reports/url-bytes"),
        ("Bot traffic", "/reports/bots"),
        ("Wasted crawl", "/reports/wasted-crawl"),
        ("Top resource waste", "/reports/top-resource-waste"),
        ("Human URLs", "/reports/human-urls"),
        ("UTM sources", "/reports/utm"),
        ("SQL Query", "/query"),
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
}
.topbar {
    background: #fff;
    border-bottom: 1px solid #e2e8f0;
    padding: 14px 24px;
    position: sticky;
    top: 0;
    z-index: 50;
}
.topbar h1 { font-size: 18px; font-weight: 700; color: #1e293b; }
.content { padding: 20px 24px; flex: 1; }

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
.content form {
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
.content form label {
    display: flex;
    flex-direction: column;
    gap: 4px;
    font-size: 11px;
    font-weight: 600;
    color: #64748b;
    text-transform: uppercase;
    letter-spacing: 0.4px;
}
.content form label:has(input[type=checkbox]) {
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
.content form input:not([type=checkbox]):not([type=hidden]),
.content form select {
    padding: 7px 10px;
    border: 1px solid #cbd5e1;
    border-radius: 6px;
    font-size: 13px;
    color: #334155;
    background: #fff;
    min-width: 110px;
    outline: none;
    transition: border-color 0.12s, box-shadow 0.12s;
}
.content form input:focus, .content form select:focus {
    border-color: #3b82f6;
    box-shadow: 0 0 0 3px rgba(59,130,246,0.15);
}
.content form input[type=checkbox] { width: 15px; height: 15px; cursor: pointer; accent-color: #3b82f6; }

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

/* ── SQL Query page ── */
.content form textarea {
    width: 100%;
    font-family: 'SFMono-Regular', Consolas, 'Liberation Mono', monospace;
    font-size: 13px;
    line-height: 1.5;
    padding: 10px 12px;
    border: 1px solid #cbd5e1;
    border-radius: 6px;
    resize: vertical;
    color: #1e293b;
    background: #f8fafc;
    outline: none;
    transition: border-color 0.12s, box-shadow 0.12s;
    min-height: 130px;
}
.content form textarea:focus {
    border-color: #3b82f6;
    box-shadow: 0 0 0 3px rgba(59,130,246,0.15);
    background: #fff;
}
/* Make the textarea label span full width inside the flex form */
label.sql-label { width: 100%; flex: 0 0 100%; }

.schema-pills { display: flex; flex-wrap: wrap; gap: 5px; margin: 12px 0 4px; }
.schema-pill {
    background: #eff6ff;
    border: 1px solid #bfdbfe;
    color: #1d4ed8;
    border-radius: 4px;
    padding: 2px 8px;
    font-size: 11px;
    font-family: 'SFMono-Regular', Consolas, monospace;
    cursor: pointer;
    user-select: none;
    transition: background 0.1s;
}
.schema-pill:hover { background: #dbeafe; }
.schema-pill small { color: #60a5fa; margin-left: 3px; }

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
.table-hint {
    background: #f8fafc;
    border: 1px solid #e2e8f0;
    border-radius: 8px;
    padding: 16px 20px;
    margin-bottom: 16px;
}
.table-hint table { font-size: 12px; border-collapse: collapse; width: 100%; }
.table-hint td { padding: 5px 12px 5px 0; color: #374151; vertical-align: top; }
.table-hint td:first-child { font-family: monospace; color: #2563eb; font-weight: 600; white-space: nowrap; }
.table-hint td:last-child { color: #64748b; }
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
    const cleaned = s.replace(/[,\s]/g, "");
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

function sortTable(table, colIndex, asc) {
    const tbody = table.tBodies[0];
    if (!tbody) return;
    const rows = Array.from(tbody.rows);
    rows.sort((ra, rb) => {
        const a = (ra.cells[colIndex]?.innerText ?? "").trim();
        const b = (rb.cells[colIndex]?.innerText ?? "").trim();
        const na = parseNumber(a);
        const nb = parseNumber(b);
        if (na !== null && nb !== null) return asc ? (na - nb) : (nb - na);
        return asc ? a.localeCompare(b) : b.localeCompare(a);
    });
    for (const r of rows) tbody.appendChild(r);
}

function attachSortable(table) {
    const headers = table.tHead ? Array.from(table.tHead.rows[0].cells) : [];
    headers.forEach((th, idx) => {
        th.addEventListener("click", () => {
            const currentlyAsc = th.classList.contains("sorted-asc");
            const asc = !currentlyAsc;
            headers.forEach(h => h.classList.remove("sorted-asc", "sorted-desc"));
            th.classList.add(asc ? "sorted-asc" : "sorted-desc");
            sortTable(table, idx, asc);
        });
    });
}

document.addEventListener("DOMContentLoaded", () => {
    document.querySelectorAll("table.sortable").forEach(attachSortable);

    // Click a schema pill to insert the column name at the textarea cursor
    document.querySelectorAll(".schema-pill").forEach(function(pill) {
        pill.addEventListener("click", function() {
            var ta = document.querySelector("textarea[name='sql']");
            if (!ta) return;
            var col = this.dataset.col;
            var start = ta.selectionStart, end = ta.selectionEnd;
            ta.value = ta.value.slice(0, start) + col + ta.value.slice(end);
            ta.selectionStart = ta.selectionEnd = start + col.length;
            ta.focus();
        });
    });
});
})();
</script>"""

    html = f"""<!doctype html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>{title} — Log Dashboard</title>
    {plotly_cdn}
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


def date_filters_html(date_from, date_to):
    return f"""
    <form method="get">
    <label>From<input type="date" name="from" value="{date_from or ''}"></label>
    <label>To<input type="date" name="to" value="{date_to or ''}"></label>
    <button type="submit">Apply dates</button>
    </form>
    """


@app.get("/", response_class=HTMLResponse)
def index():
    items = [
        ("Crawl volume", "/reports/crawl-volume", "Daily hit counts — total, bot and human"),
        ("Status over time", "/reports/status-over-time", "2xx / 3xx / 4xx / 5xx trends by day"),
        ("Locale breakdown", "/reports/locales", "Hit distribution by locale"),
        ("URL group breakdown", "/reports/url-groups", "Hits aggregated by URL group"),
        ("Locale x URL group", "/reports/locale-groups", "Cross-tab of locale and URL group"),
        ("Top URLs", "/reports/top-urls", "Most-requested paths, filterable by group"),
        ("Top 3xx redirects", "/reports/top-3xx", "URLs that redirect visitors, broken down by code"),
        ("Top 4xx errors", "/reports/top-4xx", "Paths returning 4xx, broken down by code (400/401/403/404…)"),
        ("Top 5xx errors", "/reports/top-5xx", "Paths returning 5xx, broken down by code (500/502/503/504)"),
        ("URL byte size", "/reports/url-bytes", "Which URLs have the largest average response size"),
        ("Bot traffic", "/reports/bots", "Bot families summary + per-bot URL drill-down"),
        ("Wasted crawl", "/reports/wasted-crawl", "Bot requests that yielded no value"),
        ("Top resource waste", "/reports/top-resource-waste", "Paths with highest waste score"),
        ("Human URLs", "/reports/human-urls", "Top paths visited by real users"),
        ("UTM sources", "/reports/utm", "Traffic from UTM-tagged campaigns"),
        ("SQL Query", "/query", "Ad-hoc SQL against any aggregate table"),
    ]
    cards = "".join(
        f"<a href='{url}' class='report-card'>{name}<small>{desc}</small></a>"
        for name, url, desc in items
    )
    tip = (
        "<div class='index-tip'>"
        "Tip: use the date picker on any report page to filter by date range. "
        "Each report also has an <strong>Export CSV</strong> link for raw data downloads."
        "</div>"
    )
    return page("Log Dashboard", tip + "<div class='report-grid'>" + cards + "</div>")


# ----------------------------
# Existing reports unchanged…
# ----------------------------

@app.get("/reports/locales", response_class=HTMLResponse)
def locales(date_from: Optional[str] = Query(None, alias="from"), date_to: Optional[str] = Query(None, alias="to")):
    paths = list_partitions("locale_daily", date_from, date_to)
    if not paths:
        body = date_filters_html(date_from, date_to) + no_data_notice()
        return page("Locale breakdown", body)

    sql_totals = """
    SELECT
      CASE
        WHEN locale IS NULL OR locale = 'Unknown' THEN 'no-locale'
        ELSE locale
      END AS locale,
      SUM(hits) AS hits,
      SUM(hits_bot) AS hits_bot,
      SUM(hits_human) AS hits_human,
      SUM(s4xx) AS s4xx,
      SUM(s5xx) AS s5xx,
      SUM(resource_hits) AS resource_hits,
      SUM(resource_hits_bot) AS resource_hits_bot
    FROM t
    GROUP BY 1
    ORDER BY hits DESC;
    """
    cols1, rows1 = run_query(paths, sql_totals)

    sql_non_resource = """
    SELECT
      CASE
        WHEN locale IS NULL OR locale = 'Unknown' THEN 'no-locale'
        ELSE locale
      END AS locale,
      (SUM(hits) - SUM(resource_hits)) AS hits_non_resource,
      (SUM(hits_bot) - SUM(resource_hits_bot)) AS hits_bot_non_resource,
      (SUM(hits_human) - (SUM(resource_hits) - SUM(resource_hits_bot))) AS hits_human_non_resource
    FROM t
    GROUP BY 1
    ORDER BY hits_non_resource DESC;
    """
    cols2, rows2 = run_query(paths, sql_non_resource)

    body = date_filters_html(date_from, date_to)
    body += f"<p><a href='/export?report=locales&from={date_from or ''}&to={date_to or ''}'>Export CSV</a></p>"

    chart_limit = 20
    body += "<h2>All hits (includes Nuxt/static resources)</h2>"
    body += bar_chart(
        rows1[:chart_limit], cols1,
        x_col="locale", y_col="hits_human",
        y_cols=["hits_human", "hits_bot"],
        title=f"Top {chart_limit} locales — human vs bot",
        barmode="stack",
    )
    body += "<br>"
    body += html_table(rows1, cols1)

    body += "<h2>Non-resource hits (excludes Nuxt/static resources)</h2>"
    body += bar_chart(
        rows2[:chart_limit], cols2,
        x_col="locale", y_col="hits_human_non_resource",
        y_cols=["hits_human_non_resource", "hits_bot_non_resource"],
        title=f"Top {chart_limit} locales (non-resource) — human vs bot",
        barmode="stack",
    )
    body += "<br>"
    body += html_table(rows2, cols2)

    return page("Locale breakdown", body)


@app.get("/reports/url-groups", response_class=HTMLResponse)
def url_groups(date_from: Optional[str] = Query(None, alias="from"), date_to: Optional[str] = Query(None, alias="to")):
    paths = list_partitions("group_daily", date_from, date_to)
    if not paths:
        body = date_filters_html(date_from, date_to) + no_data_notice()
        return page("URL group breakdown", body)
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
    cols, rows = run_query(paths, sql)
    body = date_filters_html(date_from, date_to)
    body += f"<p><a href='/export?report=url-groups&from={date_from or ''}&to={date_to or ''}'>Export CSV</a></p>"
    body += bar_chart(
        rows, cols,
        x_col="url_group", y_col="hits_human",
        y_cols=["hits_human", "hits_bot"],
        title="URL groups — human vs bot",
        barmode="stack",
    )
    body += "<br>"
    body += html_table(rows, cols)
    return page("URL group breakdown", body)


@app.get("/reports/locale-groups", response_class=HTMLResponse)
def locale_groups(
    date_from: Optional[str] = Query(None, alias="from"),
    date_to: Optional[str] = Query(None, alias="to"),
    locale: Optional[str] = None,
    url_group: Optional[str] = None,
    content_only: bool = False,
    limit: int = 999,
):
    paths = list_partitions("locale_group_daily", date_from, date_to)
    if not paths:
        body = date_filters_html(date_from, date_to) + no_data_notice()
        return page("Locale x URL group", body)
    clauses = []
    if locale:
        clauses.append(f"locale = '{sql_escape_string(locale)}'")
    if url_group:
        clauses.append(f"url_group = '{sql_escape_string(url_group)}'")
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
    cols, rows = run_query(paths, sql)
    locale_options = distinct_values("locale_daily", "locale", date_from, date_to)
    group_options = distinct_values("group_daily", "url_group", date_from, date_to)
    body = date_filters_html(date_from, date_to)
    body += f"""
    <form method="get">
    <input type="hidden" name="from" value="{date_from or ''}">
    <input type="hidden" name="to" value="{date_to or ''}">
    {select_html("locale", locale_options, locale, "Locale")}
    {select_html("url_group", group_options, url_group, "URL group")}
    <label>Limit: <input name="limit" value="{limit}" size="6"></label>
    <label style="margin-left:1em"><input type="checkbox" name="content_only" value="true"{"checked" if content_only else ""}> Content only (heatmap)</label>
    <button type="submit">Apply</button>
    </form>
    """
    body += f"<p><a href='/export?report=locale-groups&from={date_from or ''}&to={date_to or ''}&locale={locale or ''}&url_group={url_group or ''}&limit={limit}'>Export CSV</a></p>"

    url_group_idx = cols.index("url_group") if "url_group" in cols else None

    # Only show heatmap when not filtered to a single locale or url_group (would be a 1-row/col matrix)
    if not locale and not url_group:
        heatmap_rows = rows
        if content_only and url_group_idx is not None:
            heatmap_rows = [r for r in rows if r[url_group_idx] not in NON_CONTENT_GROUPS]
        title = "Hits heatmap — locale × URL group" + (" (content only)" if content_only else "")
        body += heatmap_chart(heatmap_rows, cols, x_col="url_group", y_col="locale", z_col="hits", title=title)
        body += "<br>"
    body += html_table(rows, cols, max_rows=min(int(limit), 500))
    return page("Locale x URL group", body)


@app.get("/reports/crawl-volume", response_class=HTMLResponse)
def crawl_volume(date_from: Optional[str] = Query(None, alias="from"), date_to: Optional[str] = Query(None, alias="to")):
    paths = list_partitions("daily", date_from, date_to)
    sql = """
    SELECT date, hits, hits_bot, hits_human, bytes_sent, resource_hits, resource_hits_bot
    FROM t
    ORDER BY date;
    """
    cols, rows = run_query(paths, sql)
    chart_html = line_chart(
        rows,
        cols,
        x_col="date",
        y_cols=["hits", "hits_bot", "hits_human"],
        title="Daily Crawl Volume"
    )
    bytes_chart = line_chart(rows, cols, x_col="date", y_cols=["bytes_sent"], title="Daily bytes sent")
    body = date_filters_html(date_from, date_to)
    body += f"<p><a href='/export?report=daily&from={date_from or ''}&to={date_to or ''}'>Export CSV</a></p>"
    body += chart_html
    body += "<br>"
    body += bytes_chart
    body += "<br>"
    body += html_table(rows, cols)
    return page("Crawl volume (daily)", body)


@app.get("/reports/status-over-time", response_class=HTMLResponse)
def status_over_time(date_from: Optional[str] = Query(None, alias="from"), date_to: Optional[str] = Query(None, alias="to")):
    paths = list_partitions("daily", date_from, date_to)
    sql = """
    SELECT date, s2xx, s3xx, s4xx, s5xx, hits
    FROM t
    ORDER BY date;
    """
    cols, rows = run_query(paths, sql)
    chart_html = line_chart(
        rows,
        cols,
        x_col="date",
        y_cols=["s2xx", "s3xx", "s4xx", "s5xx"],
        title="Status Codes Over Time"
    )
    body = date_filters_html(date_from, date_to)
    body += f"<p><a href='/export?report=daily-status&from={date_from or ''}&to={date_to or ''}'>Export CSV</a></p>"
    body += chart_html
    body += "<br>"
    body += html_table(rows, cols)
    return page("Status codes over time (daily)", body)

# ----------------------------
# Missing report pages (fix 404)
# ----------------------------

def export_link(report: str, date_from: Optional[str], date_to: Optional[str], extra: str = "") -> str:
    qs = []
    qs.append(f"report={report}")
    if date_from:
        qs.append(f"from={date_from}")
    if date_to:
        qs.append(f"to={date_to}")
    if extra:
        qs.append(extra.lstrip("&"))
    return f"<p><a href='/export?{'&'.join(qs)}'>&#8595; Export CSV</a></p>"


@app.get("/reports/top-urls", response_class=HTMLResponse)
def top_urls(
    date_from: Optional[str] = Query(None, alias="from"),
    date_to: Optional[str] = Query(None, alias="to"),
    include_assets: bool = False,
    url_group: Optional[str] = None,
    limit: int = 500,
):
    paths = list_partitions("top_urls_daily", date_from, date_to)
    body = date_filters_html(date_from, date_to)

    # filters
    groups = distinct_values("top_urls_daily", "url_group", date_from, date_to)
    body += "<form method='get'>"
    body += f"<input type='hidden' name='from' value='{date_from or ''}'>"
    body += f"<input type='hidden' name='to' value='{date_to or ''}'>"
    body += select_html("url_group", groups, url_group, "URL group")
    checked = "checked" if include_assets else ""
    body += f" <label><input type='checkbox' name='include_assets' value='true' {checked}> Include assets</label>"
    body += f" <label>Limit: <input name='limit' value='{int(limit)}' size='6'></label>"
    body += " <button type='submit'>Apply</button></form>"

    if not paths:
        return page("Top URLs", body + no_data_notice())

    clauses = []
    if not include_assets:
        clauses.append("url_group NOT IN ('Nuxt Assets','Static Assets')")
    if url_group:
        clauses.append(f"url_group = '{sql_escape_string(url_group)}'")
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
    cols, rows = run_query(paths, sql)
    body += export_link(
        "top-urls", date_from, date_to,
        extra=f"&include_assets={'true' if include_assets else 'false'}"
              + (f"&url_group={url_group}" if url_group else "")
              + f"&limit={int(limit)}"
    )
    chart_limit = CHART_BAR_LIMIT
    if rows:
        body += bar_chart(
            rows[:chart_limit], cols,
            x_col="path", y_col="hits_total",
            y_cols=["hits_total", "hits_bot"],
            title=f"Top {chart_limit} URLs — total vs bot",
            barmode="group",
        )
        body += "<br>"
    body += html_table(rows, cols)
    return page("Top URLs", body)


@app.get("/reports/top-404")
def top_404_redirect(
    date_from: Optional[str] = Query(None, alias="from"),
    date_to: Optional[str] = Query(None, alias="to"),
    include_assets: bool = False,
    url_group: Optional[str] = None,
    limit: int = 500,
):
    """Redirects old /reports/top-404 links to /reports/top-4xx."""
    parts = []
    if date_from:      parts.append(f"from={date_from}")
    if date_to:        parts.append(f"to={date_to}")
    if include_assets: parts.append("include_assets=true")
    if url_group:      parts.append(f"url_group={url_group}")
    parts.append(f"limit={limit}")
    return RedirectResponse(url=f"/reports/top-4xx?{'&'.join(parts)}", status_code=301)


@app.get("/reports/top-4xx", response_class=HTMLResponse)
def top_4xx(
    date_from: Optional[str] = Query(None, alias="from"),
    date_to: Optional[str] = Query(None, alias="to"),
    include_assets: bool = False,
    url_group: Optional[str] = None,
    limit: int = 500,
):
    paths = list_partitions("top_4xx_daily", date_from, date_to)
    body = date_filters_html(date_from, date_to)

    groups = distinct_values("top_4xx_daily", "url_group", date_from, date_to)
    body += "<form method='get'>"
    body += f"<input type='hidden' name='from' value='{date_from or ''}'>"
    body += f"<input type='hidden' name='to' value='{date_to or ''}'>"
    body += select_html("url_group", groups, url_group, "URL group")
    checked = "checked" if include_assets else ""
    body += f" <label><input type='checkbox' name='include_assets' value='true' {checked}> Include assets</label>"
    body += f" <label>Limit: <input name='limit' value='{int(limit)}' size='6'></label>"
    body += " <button type='submit'>Apply</button></form>"

    if not paths:
        return page("Top 4xx", body + no_data_notice())

    clauses = []
    if not include_assets:
        clauses.append("url_group NOT IN ('Nuxt Assets','Static Assets')")
    if url_group:
        clauses.append(f"url_group = '{sql_escape_string(url_group)}'")
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""

    daily_sql = f"""
    SELECT
        date,
        SUM(hits_4xx)     AS hits_4xx,
        SUM(hits_4xx_bot) AS hits_4xx_bot,
        (SUM(hits_4xx) - SUM(hits_4xx_bot)) AS hits_4xx_non_bot,
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
    GROUP BY date
    ORDER BY date;
    """
    cols_d, rows_d = run_query(paths, daily_sql)

    sql = f"""
    SELECT path, url_group,
        SUM(hits_4xx)     AS hits_4xx,
        SUM(hits_4xx_bot) AS hits_4xx_bot,
        (SUM(hits_4xx) - SUM(hits_4xx_bot)) AS hits_4xx_non_bot,
        SUM(bytes_sent_4xx) AS bytes_sent_4xx,
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
    cols, rows = run_query(paths, sql)

    body += export_link(
        "top-4xx", date_from, date_to,
        extra=f"&include_assets={'true' if include_assets else 'false'}"
              + (f"&url_group={url_group}" if url_group else "")
              + f"&limit={int(limit)}"
    )

    body += "<h2>4xx trend (daily)</h2>"
    body += line_chart(rows_d, cols_d, x_col="date",
                       y_cols=["s400", "s401", "s403", "s404", "s405", "s410", "s422", "s429"],
                       title="4xx responses by day (per code)")
    body += "<br>"
    body += html_table(rows_d, cols_d, max_rows=500)

    if rows:
        body += "<h2>Top 4xx URLs</h2>"
        body += bar_chart(rows[:CHART_BAR_LIMIT], cols, x_col="path",
                          y_cols=["s400", "s401", "s403", "s404", "s405", "s410", "s422", "s429"],
                          title=f"Top {CHART_BAR_LIMIT} 4xx URLs by code", barmode="stack")
        body += "<br>"

    body += html_table(rows, cols, max_rows=min(int(limit), 500))
    return page("Top 4xx", body)

@app.get("/reports/top-5xx", response_class=HTMLResponse)
def top_5xx(
    date_from: Optional[str] = Query(None, alias="from"),
    date_to: Optional[str] = Query(None, alias="to"),
    include_assets: bool = False,
    url_group: Optional[str] = None,
    limit: int = 500,
):
    paths = list_partitions("top_5xx_daily", date_from, date_to)
    body = date_filters_html(date_from, date_to)

    groups = distinct_values("top_5xx_daily", "url_group", date_from, date_to)
    body += "<form method='get'>"
    body += f"<input type='hidden' name='from' value='{date_from or ''}'>"
    body += f"<input type='hidden' name='to' value='{date_to or ''}'>"
    body += select_html("url_group", groups, url_group, "URL group")
    checked = "checked" if include_assets else ""
    body += f" <label><input type='checkbox' name='include_assets' value='true' {checked}> Include assets</label>"
    body += f" <label>Limit: <input name='limit' value='{int(limit)}' size='6'></label>"
    body += " <button type='submit'>Apply</button></form>"

    if not paths:
        return page("Top 5xx", body + no_data_notice())

    clauses = []
    if not include_assets:
        clauses.append("url_group NOT IN ('Nuxt Assets','Static Assets')")
    if url_group:
        clauses.append(f"url_group = '{sql_escape_string(url_group)}'")
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""

    daily_sql = f"""
    SELECT
        date,
        SUM(hits_5xx)     AS hits_5xx,
        SUM(hits_5xx_bot) AS hits_5xx_bot,
        (SUM(hits_5xx) - SUM(hits_5xx_bot)) AS hits_5xx_non_bot,
        SUM(s500) AS s500,
        SUM(s502) AS s502,
        SUM(s503) AS s503,
        SUM(s504) AS s504
    FROM t
    {where}
    GROUP BY date
    ORDER BY date;
    """
    cols_d, rows_d = run_query(paths, daily_sql)

    sql = f"""
    SELECT path, url_group,
        SUM(hits_5xx)     AS hits_5xx,
        SUM(hits_5xx_bot) AS hits_5xx_bot,
        (SUM(hits_5xx) - SUM(hits_5xx_bot)) AS hits_5xx_non_bot,
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
    cols, rows = run_query(paths, sql)

    body += export_link(
        "top-5xx", date_from, date_to,
        extra=f"&include_assets={'true' if include_assets else 'false'}"
              + (f"&url_group={url_group}" if url_group else "")
              + f"&limit={int(limit)}"
    )

    body += "<h2>5xx trend (daily)</h2>"
    body += line_chart(rows_d, cols_d, x_col="date",
                       y_cols=["s500", "s502", "s503", "s504"],
                       title="5xx responses by day (per code)")
    body += "<br>"
    body += html_table(rows_d, cols_d, max_rows=500)

    if rows:
        body += "<h2>Top 5xx URLs</h2>"
        body += bar_chart(rows[:CHART_BAR_LIMIT], cols, x_col="path",
                          y_cols=["s500", "s502", "s503", "s504"],
                          title=f"Top {CHART_BAR_LIMIT} 5xx URLs by code", barmode="stack")
        body += "<br>"

    body += html_table(rows, cols, max_rows=min(int(limit), 500))
    return page("Top 5xx", body)


@app.get("/reports/top-3xx", response_class=HTMLResponse)
def top_3xx(
    date_from: Optional[str] = Query(None, alias="from"),
    date_to: Optional[str] = Query(None, alias="to"),
    include_assets: bool = False,
    url_group: Optional[str] = None,
    limit: int = 500,
):
    paths = list_partitions("top_urls_daily", date_from, date_to)
    body = date_filters_html(date_from, date_to)

    groups = distinct_values("top_urls_daily", "url_group", date_from, date_to)
    body += "<form method='get'>"
    body += f"<input type='hidden' name='from' value='{date_from or ''}'>"
    body += f"<input type='hidden' name='to' value='{date_to or ''}'>"
    body += select_html("url_group", groups, url_group, "URL group")
    checked = "checked" if include_assets else ""
    body += f" <label><input type='checkbox' name='include_assets' value='true' {checked}> Include assets</label>"
    body += f" <label>Limit: <input name='limit' value='{int(limit)}' size='6'></label>"
    body += " <button type='submit'>Apply</button></form>"

    if not paths:
        return page("Top 3xx (redirects)", body + no_data_notice())

    clauses = []
    if not include_assets:
        clauses.append("url_group NOT IN ('Nuxt Assets','Static Assets')")
    if url_group:
        clauses.append(f"url_group = '{sql_escape_string(url_group)}'")
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""

    daily_sql = f"""
    SELECT date,
           SUM(s3xx) AS s3xx,
           SUM(s301)  AS s301,
           SUM(s302)  AS s302,
           SUM(s303)  AS s303,
           SUM(s307)  AS s307,
           SUM(s308)  AS s308
    FROM t
    {where}
    GROUP BY date
    ORDER BY date;
    """
    cols_d, rows_d = run_query(paths, daily_sql)

    sql = f"""
    SELECT path, url_group,
           SUM(s3xx)  AS s3xx,
           SUM(s301)  AS s301,
           SUM(s302)  AS s302,
           SUM(s303)  AS s303,
           SUM(s307)  AS s307,
           SUM(s308)  AS s308,
           SUM(hits_total) AS hits_total,
           ROUND(100.0 * SUM(s3xx) / NULLIF(SUM(hits_total), 0), 1) AS redirect_pct
    FROM t
    {where}
    GROUP BY path, url_group
    HAVING SUM(s3xx) > 0
    ORDER BY s3xx DESC
    LIMIT {int(limit)};
    """
    cols, rows = run_query(paths, sql)

    body += export_link(
        "top-3xx", date_from, date_to,
        extra=f"&include_assets={'true' if include_assets else 'false'}"
              + (f"&url_group={url_group}" if url_group else "")
              + f"&limit={int(limit)}"
    )

    body += "<h2>Redirect trend (daily)</h2>"
    body += line_chart(rows_d, cols_d, x_col="date", y_cols=["s301", "s302", "s303", "s307", "s308"], title="3xx responses by day (per code)")
    body += "<br>"
    body += html_table(rows_d, cols_d, max_rows=500)

    if rows:
        body += "<h2>Top redirecting URLs</h2>"
        body += bar_chart(rows[:CHART_BAR_LIMIT], cols, x_col="path",
                          y_cols=["s301", "s302", "s303", "s307", "s308"],
                          title=f"Top {CHART_BAR_LIMIT} redirecting URLs by code", barmode="stack")
        body += "<br>"

    body += html_table(rows, cols, max_rows=min(int(limit), 500))
    return page("Top 3xx (redirects)", body)


@app.get("/reports/url-bytes", response_class=HTMLResponse)
def url_bytes(
    date_from: Optional[str] = Query(None, alias="from"),
    date_to: Optional[str] = Query(None, alias="to"),
    include_assets: bool = False,
    url_group: Optional[str] = None,
    limit: int = 500,
):
    paths = list_partitions("top_urls_daily", date_from, date_to)
    body = date_filters_html(date_from, date_to)

    groups = distinct_values("top_urls_daily", "url_group", date_from, date_to)
    body += "<form method='get'>"
    body += f"<input type='hidden' name='from' value='{date_from or ''}'>"
    body += f"<input type='hidden' name='to' value='{date_to or ''}'>"
    body += select_html("url_group", groups, url_group, "URL group")
    checked = "checked" if include_assets else ""
    body += f" <label><input type='checkbox' name='include_assets' value='true' {checked}> Include assets</label>"
    body += f" <label>Limit: <input name='limit' value='{int(limit)}' size='6'></label>"
    body += " <button type='submit'>Apply</button></form>"

    if not paths:
        return page("URL byte size", body + no_data_notice())

    clauses = []
    if not include_assets:
        clauses.append("url_group NOT IN ('Nuxt Assets','Static Assets')")
    if url_group:
        clauses.append(f"url_group = '{sql_escape_string(url_group)}'")
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""

    sql = f"""
    SELECT path, url_group,
           SUM(bytes_sent)                                            AS bytes_total,
           SUM(hits_total)                                            AS hits_total,
           ROUND(SUM(bytes_sent) / NULLIF(SUM(hits_total), 0))       AS avg_bytes_per_hit
    FROM t
    {where}
    GROUP BY path, url_group
    HAVING SUM(bytes_sent) > 0
    ORDER BY avg_bytes_per_hit DESC
    LIMIT {int(limit)};
    """
    cols, rows = run_query(paths, sql)

    body += export_link(
        "url-bytes", date_from, date_to,
        extra=f"&include_assets={'true' if include_assets else 'false'}"
              + (f"&url_group={url_group}" if url_group else "")
              + f"&limit={int(limit)}"
    )

    if rows:
        body += "<h2>Largest pages by average response size</h2>"
        body += bar_chart(
            rows[:CHART_BAR_LIMIT], cols,
            x_col="path", y_col="avg_bytes_per_hit",
            title=f"Top {CHART_BAR_LIMIT} URLs by avg response size",
        )
        body += "<br>"

    # Format byte columns for readability before rendering the table
    bytes_total_idx = cols.index("bytes_total")
    avg_idx = cols.index("avg_bytes_per_hit")
    display_rows = [
        tuple(
            fmt_bytes(v) if i in (bytes_total_idx, avg_idx) else v
            for i, v in enumerate(r)
        )
        for r in rows
    ]
    body += html_table(display_rows, cols, max_rows=min(int(limit), 500))
    return page("URL byte size", body)


@app.get("/reports/bots", response_class=HTMLResponse)
def bots(
    date_from: Optional[str] = Query(None, alias="from"),
    date_to: Optional[str] = Query(None, alias="to"),
    bot: Optional[str] = None,
    include_assets: bool = False,
    limit: int = 500,
):
    bots_list = distinct_values("bot_urls_daily", "bot_family", date_from, date_to)
    body = date_filters_html(date_from, date_to)
    checked_assets = "checked" if include_assets else ""
    body += f"""
    <form method="get">
    <input type="hidden" name="from" value="{date_from or ''}">
    <input type="hidden" name="to" value="{date_to or ''}">
    {select_html("bot", bots_list, bot, "Bot family")}
    <label><input type="checkbox" name="include_assets" value="true" {checked_assets}> Include assets</label>
    <label style="margin-left:1em">Limit: <input name="limit" value="{int(limit)}" size="6"></label>
    <button type="submit">Apply</button>
    </form>
    """

    # ── Summary section (bot_daily) ──────────────────────────────────────────
    paths_summary = list_partitions("bot_daily", date_from, date_to)
    body += "<h2>Bot activity summary</h2>"
    if not paths_summary:
        body += no_data_notice()
    else:
        sql_summary = """
        SELECT bot_family,
               SUM(hits) AS hits,
               SUM(resource_hits) AS resource_hits,
               AVG(resource_hits_pct) AS resource_hits_pct,
               SUM(s4xx) AS s4xx,
               SUM(s5xx) AS s5xx
        FROM t
        GROUP BY bot_family
        ORDER BY hits DESC;
        """
        cols, rows = run_query(paths_summary, sql_summary)

        # Trend: top-10 bots over time
        top_families = [r[0] for r in rows[:10]]
        families_in = ", ".join(f"'{sql_escape_string(f)}'" for f in top_families)
        trend_sql = f"""
        SELECT date, bot_family, SUM(hits) AS hits
        FROM t
        WHERE bot_family IN ({families_in})
        GROUP BY date, bot_family
        ORDER BY date, bot_family;
        """
        cols_t, rows_t = run_query(paths_summary, trend_sql)
        if rows_t:
            df_t = pd.DataFrame(rows_t, columns=cols_t)
            df_pivot = df_t.pivot_table(index="date", columns="bot_family", values="hits", fill_value=0).reset_index()
            pivot_cols = list(df_pivot.columns)
            pivot_rows = [tuple(r) for r in df_pivot.itertuples(index=False, name=None)]
            y_families = [c for c in pivot_cols if c != "date"]
            body += "<h3>Activity over time (top 10 families)</h3>"
            body += line_chart(pivot_rows, pivot_cols, x_col="date", y_cols=y_families, title="Bot hits over time (top 10 families)")
            body += "<br>"

        body += "<h3>Total hits by bot family</h3>"
        body += bar_chart(rows, cols, x_col="bot_family", y_col="hits", title="Bot hits by family")
        body += "<br>"

        # Bot family names are links that pre-select the bot in the URL section below
        bot_idx = cols.index("bot_family")
        def _bot_link_qs(family):
            parts = [f"bot={family}"]
            if date_from: parts.append(f"from={date_from}")
            if date_to:   parts.append(f"to={date_to}")
            if include_assets: parts.append("include_assets=true")
            parts.append(f"limit={int(limit)}")
            return "&".join(parts)
        linked_rows = [
            tuple(
                f"<a href='/reports/bots?{_bot_link_qs(v)}#urls'>{v}</a>" if i == bot_idx else v
                for i, v in enumerate(r)
            )
            for r in rows
        ]
        body += export_link("bots", date_from, date_to)
        body += html_table(linked_rows, cols)

    # ── URL drill-down section (bot_urls_daily) ───────────────────────────────
    paths_urls = list_partitions("bot_urls_daily", date_from, date_to)
    url_title = f"URLs crawled by {bot}" if bot else "URLs crawled — preview (click a bot above for full breakdown)"
    body += f"<h2 id='urls'>{url_title}</h2>"

    if not paths_urls:
        body += no_data_notice()
    else:
        clauses = []
        if bot:
            clauses.append(f"bot_family = '{sql_escape_string(bot)}'")
        if not include_assets:
            clauses.append("url_group NOT IN ('Nuxt Assets','Static Assets')")
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        url_limit = int(limit) if bot else 20

        sql_urls = f"""
        SELECT bot_family, url_group, path,
               SUM(hits) AS hits
        FROM t
        {where}
        GROUP BY bot_family, url_group, path
        ORDER BY hits DESC
        LIMIT {url_limit};
        """
        cols_u, rows_u = run_query(paths_urls, sql_urls)

        if bot:
            body += export_link(
                "bot-urls", date_from, date_to,
                extra=f"&include_assets={'true' if include_assets else 'false'}"
                      + f"&bot={bot}&limit={int(limit)}"
            )
            chart_limit = CHART_BAR_LIMIT
            body += bar_chart(rows_u[:chart_limit], cols_u, x_col="path", y_col="hits",
                              title=f"Top {chart_limit} URLs crawled by {bot}")
            body += "<br>"
        else:
            body += "<p>Showing top 20 across all bots. Select a bot above for the full breakdown.</p>"

        body += html_table(rows_u, cols_u)

    return page("Bot traffic", body)


@app.get("/reports/wasted-crawl", response_class=HTMLResponse)
def wasted_crawl(
    date_from: Optional[str] = Query(None, alias="from"),
    date_to: Optional[str] = Query(None, alias="to"),
    strict: bool = False,
    include_assets: bool = False,
    bot: Optional[str] = None,
    url_group: Optional[str] = None,
    limit: int = 500,
):
    paths = list_partitions("wasted_crawl_daily", date_from, date_to)
    body = date_filters_html(date_from, date_to)

    # Filters: these columns DO exist here
    bots_list = distinct_values("wasted_crawl_daily", "bot_family", date_from, date_to)
    groups = distinct_values("wasted_crawl_daily", "url_group", date_from, date_to)

    body += "<form method='get'>"
    body += f"<input type='hidden' name='from' value='{date_from or ''}'>"
    body += f"<input type='hidden' name='to' value='{date_to or ''}'>"
    body += select_html("bot", bots_list, bot, "Bot family")
    body += select_html("url_group", groups, url_group, "URL group")
    checked_assets = "checked" if include_assets else ""
    checked_strict = "checked" if strict else ""
    body += f" <label><input type='checkbox' name='include_assets' value='true' {checked_assets}> Include assets</label>"
    body += f" <label><input type='checkbox' name='strict' value='true' {checked_strict}> Strict scoring</label>"
    body += f" <label>Limit: <input name='limit' value='{int(limit)}' size='6'></label>"
    body += " <button type='submit'>Apply</button></form>"

    if not paths:
        return page("Wasted crawl", body + no_data_notice())

    score_col = "waste_score_strict" if strict else "waste_score"

    clauses = []
    if bot:
        clauses.append(f"bot_family = '{sql_escape_string(bot)}'")
    if url_group:
        clauses.append(f"url_group = '{sql_escape_string(url_group)}'")
    if not include_assets:
        clauses.append("url_group NOT IN ('Nuxt Assets','Static Assets')")
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""

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
    cols, rows = run_query(paths, sql)

    # Trend: total waste score per day
    trend_sql = f"""
    SELECT date, SUM({score_col}) AS waste_score, SUM(bot_hits) AS bot_hits
    FROM t
    {where}
    GROUP BY date
    ORDER BY date;
    """
    cols_t, rows_t = run_query(paths, trend_sql)
    trend_html = line_chart(rows_t, cols_t, x_col="date", y_cols=["waste_score", "bot_hits"], title="Waste score over time")

    chart_html = bar_chart(rows, cols, x_col="path", y_col="waste_score", title="Highest waste score (top paths)")

    body += (
        f"<p><a href='/export?report=wasted-crawl&from={date_from or ''}&to={date_to or ''}"
        f"&strict={'true' if strict else 'false'}"
        f"&include_assets={'true' if include_assets else 'false'}"
        f"{'&bot=' + bot if bot else ''}"
        f"{'&url_group=' + url_group if url_group else ''}"
        f"&limit={int(limit)}'>Export CSV</a></p>"
    )
    body += "<h2>Waste score trend (daily)</h2>"
    body += trend_html
    body += "<br>"
    body += "<h2>Top wasted paths</h2>"
    body += chart_html
    body += "<br>"
    body += html_table(rows, cols)
    return page("Wasted crawl", body)


@app.get("/reports/top-resource-waste", response_class=HTMLResponse)
def top_resource_waste(
    date_from: Optional[str] = Query(None, alias="from"),
    date_to: Optional[str] = Query(None, alias="to"),
    limit: int = 500,
):
    paths = list_partitions("top_resource_waste_daily", date_from, date_to)
    body = date_filters_html(date_from, date_to)

    body += "<form method='get'>"
    body += f"<input type='hidden' name='from' value='{date_from or ''}'>"
    body += f"<input type='hidden' name='to' value='{date_to or ''}'>"
    body += f" <label>Limit: <input name='limit' value='{int(limit)}' size='6'></label>"
    body += " <button type='submit'>Apply</button></form>"

    if not paths:
        return page("Top resource waste", body + no_data_notice())

    sql = f"""
    SELECT path,
           SUM(bot_hits) AS bot_hits,
           SUM(resource_error_bot_hits) AS resource_error_bot_hits,
           SUM(status_404_bot_hits) AS status_404_bot_hits,
           SUM(waste_score_strict) AS waste_score_strict
    FROM t
    GROUP BY path
    ORDER BY waste_score_strict DESC, bot_hits DESC
    LIMIT {int(limit)};
    """
    cols, rows = run_query(paths, sql)

    # Optional chart, same style as crawl-volume
    chart_html = bar_chart(rows, cols, x_col="path", y_col="waste_score_strict", title="Top resource waste (strict)")

    body += f"<p><a href='/export?report=top-resource-waste&from={date_from or ''}&to={date_to or ''}&limit={int(limit)}'>Export CSV</a></p>"
    body += chart_html
    body += "<br>"
    body += html_table(rows, cols)
    return page("Top resource waste", body)

@app.get("/reports/human-urls", response_class=HTMLResponse)
def human_urls(
    date_from: Optional[str] = Query(None, alias="from"),
    date_to: Optional[str] = Query(None, alias="to"),
    include_assets: bool = False,
    limit: int = 500,
):
    paths = list_partitions("human_urls_daily", date_from, date_to)
    body = date_filters_html(date_from, date_to)

    groups = distinct_values("human_urls_daily", "url_group", date_from, date_to)

    body += "<form method='get'>"
    body += f"<input type='hidden' name='from' value='{date_from or ''}'>"
    body += f"<input type='hidden' name='to' value='{date_to or ''}'>"
    checked_assets = "checked" if include_assets else ""
    body += f" <label><input type='checkbox' name='include_assets' value='true' {checked_assets}> Include assets</label>"
    body += f" <label>Limit: <input name='limit' value='{int(limit)}' size='6'></label>"
    body += " <button type='submit'>Apply</button></form>"

    if not paths:
        return page("Human URLs", body + no_data_notice())

    clauses = []
    if not include_assets:
        clauses.append("url_group NOT IN ('Nuxt Assets','Static Assets')")
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""

    sql = f"""
    SELECT url_group, path,
        SUM(hits) AS hits
    FROM t
    {where}
    GROUP BY url_group, path
    ORDER BY hits DESC
    LIMIT {int(limit)};
    """
    cols, rows = run_query(paths, sql)
    body += export_link(
        "human-urls", date_from, date_to,
        extra=f"&include_assets={'true' if include_assets else 'false'}"
              + f"&limit={int(limit)}"
    )
    chart_limit = CHART_BAR_LIMIT
    body += bar_chart(rows[:chart_limit], cols, x_col="path", y_col="hits", title=f"Top {chart_limit} human URLs")
    body += "<br>"
    body += html_table(rows, cols)
    return page("Human URLs", body)

@app.get("/reports/bot-urls")
def bot_urls_redirect(
    bot: Optional[str] = None,
    include_assets: bool = False,
    limit: int = 500,
    date_from: Optional[str] = Query(None, alias="from"),
    date_to: Optional[str] = Query(None, alias="to"),
):
    """Redirects to the combined /reports/bots page, preserving all query params."""
    parts = []
    if bot:           parts.append(f"bot={bot}")
    if date_from:     parts.append(f"from={date_from}")
    if date_to:       parts.append(f"to={date_to}")
    if include_assets: parts.append("include_assets=true")
    parts.append(f"limit={limit}")
    qs = "&".join(parts)
    return RedirectResponse(url=f"/reports/bots?{qs}#urls", status_code=301)

# ----------------------------
# NEW: Generic UTM report
# Requires new aggregate tables:
#   - utm_sources_daily (date, utm_source, hits, hits_human, hits_bot)
#   - utm_source_urls_daily (date, utm_source, url_group, path, hits[, hits_human, hits_bot])
# ----------------------------

def utm_traffic_where(traffic: str) -> str:
    """
    traffic:
      - all (default): use hits
      - human: use hits_human
      - bot: use hits_bot
    We return the metric column name to use.
    """
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
    group_by: str = "url_group",   # url_group | path
    traffic: str = "all",          # all | human | bot
    limit: int = 200,
):
    # Table names for new generic UTM aggregates
    sources_table = "utm_sources_daily"
    urls_table = "utm_source_urls_daily"

    sources_paths = list_partitions(sources_table, date_from, date_to)
    urls_paths = list_partitions(urls_table, date_from, date_to)

    body = date_filters_html(date_from, date_to)

    if not sources_paths:
        body += (
            "<p><strong>UTM report not available yet.</strong></p>"
            "<p>This page requires new aggregate tables: <code>utm_sources_daily</code> and <code>utm_source_urls_daily</code>.</p>"
        )
        return page("UTM (all sources)", body)

    # selector options
    utm_options = distinct_values(sources_table, "utm_source", date_from, date_to)

    # controls
    group_by = (group_by or "url_group").strip().lower()
    if group_by not in ("url_group", "path"):
        group_by = "url_group"

    traffic = (traffic or "all").strip().lower()
    if traffic not in ("all", "human", "bot"):
        traffic = "all"

    body += f"""
    <form method="get">
      <input type="hidden" name="from" value="{date_from or ''}">
      <input type="hidden" name="to" value="{date_to or ''}">
      {select_html("utm_source", utm_options, utm_source, "UTM source")}
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

    # export links
    body += (
        "<p>"
        f"<a href='/export?report=utm-sources&from={date_from or ''}&to={date_to or ''}&traffic={traffic}&limit={int(limit)}'>Export sources CSV</a>"
        " | "
        f"<a href='/export?report=utm&from={date_from or ''}&to={date_to or ''}&utm_source={utm_source or ''}&traffic={traffic}'>Export daily CSV</a>"
        " | "
        f"<a href='/export?report=utm-urls&from={date_from or ''}&to={date_to or ''}&utm_source={utm_source or ''}&group_by={group_by}&traffic={traffic}&limit={int(limit)}'>Export URLs CSV</a>"
        "</p>"
    )

    metric = utm_traffic_where(traffic)

    # Overview: top sources (always shown)
    sql_top_sources = f"""
    SELECT utm_source, SUM({metric}) AS hits
    FROM t
    GROUP BY utm_source
    ORDER BY hits DESC
    LIMIT {int(limit)};
    """
    colsS, rowsS = run_query(sources_paths, sql_top_sources)
    body += "<h2>Top UTM sources</h2>"
    body += bar_chart(rowsS, colsS, x_col="utm_source", y_col="hits", title=f"Top UTM sources ({traffic})")
    body += "<br>"
    body += html_table(rowsS, colsS, max_rows=min(int(limit), 500))

    # If no specific source chosen, stop here (overview-only)
    if not utm_source:
        body += "<p>Select a UTM source above to see trends and landing pages.</p>"
        return page("UTM (all sources)", body)

    src_esc = sql_escape_string(utm_source)

    # Trend: selected source over time
    sql_daily = f"""
    SELECT date, SUM({metric}) AS hits
    FROM t
    WHERE utm_source = '{src_esc}'
    GROUP BY date
    ORDER BY date;
    """
    cols1, rows1 = run_query(sources_paths, sql_daily)

    body += "<h2>Daily hits</h2>"
    body += line_chart(rows1, cols1, x_col="date", y_cols=["hits"], title=f"UTM {utm_source} — Daily hits ({traffic})")
    body += "<br>"
    body += html_table(rows1, cols1)

    # Distribution: landing pages / groups for selected source
    if not urls_paths:
        body += "<h2>Top landing</h2>"
        body += "<p><em>No URL distribution data (missing utm_source_urls_daily partitions).</em></p>"
        return page("UTM (all sources)", body)

    dim = "url_group" if group_by == "url_group" else "path"

    # Some implementations may only have 'hits'. We default to SUM(hits) if the chosen metric isn't present.
    # (If you add hits_human/hits_bot to the urls table, this will work as-is.)
    sql_urls = f"""
    SELECT {dim} AS dimension, SUM({metric}) AS hits
    FROM t
    WHERE utm_source = '{src_esc}'
      AND {dim} IS NOT NULL
    GROUP BY {dim}
    ORDER BY hits DESC
    LIMIT {int(limit)};
    """
    cols2, rows2 = run_query(urls_paths, sql_urls)

    body += f"<h2>Top landing {dim}</h2>"
    body += bar_chart(rows2, cols2, x_col="dimension", y_col="hits", title=f"Top landing {dim} — {utm_source} ({traffic})")
    body += "<br>"
    body += html_table(rows2, cols2, max_rows=min(int(limit), 500))

    return page("UTM (all sources)", body)


# Legacy convenience: redirect old report to new one
@app.get("/reports/utm-chatgpt", response_class=HTMLResponse)
def utm_chatgpt(
    date_from: Optional[str] = Query(None, alias="from"),
    date_to: Optional[str] = Query(None, alias="to"),
):
    # Keep legacy URL but send to the generic UTM report
    target = f"/reports/utm?from={date_from or ''}&to={date_to or ''}&utm_source=chatgpt.com"
    return RedirectResponse(url=target, status_code=302)


# ----------------------------
# SQL Query page
# ----------------------------

@app.get("/query", response_class=HTMLResponse)
def query_page(
    date_from: Optional[str] = Query(None, alias="from"),
    date_to: Optional[str] = Query(None, alias="to"),
    table: Optional[str] = None,
    sql: Optional[str] = None,
    limit: int = Query(500, ge=1, le=5000),
):
    import time as _time

    tables = list_tables()

    # ── Table selector ──────────────────────────────────────────────
    table_options = "<option value=''>— select a table —</option>"
    for t in tables:
        sel = "selected" if t == table else ""
        desc = TABLE_DESCRIPTIONS.get(t, "")
        table_options += f"<option value='{t}' {sel}>{t}</option>"

    # ── Schema pills (if a valid table + date range selected) ───────
    schema_html = ""
    safe_table = table if table in tables else None
    if safe_table:
        schema_paths = list_partitions(safe_table, date_from, date_to)
        if schema_paths:
            try:
                col_info, _ = run_query(schema_paths, "DESCRIBE t")
                # col_info columns: column_name, column_type, null, key, default, extra
                pills = "".join(
                    f"<span class='schema-pill' data-col='{r[0]}' title='{r[1]}'>"
                    f"{r[0]}<small>{r[1]}</small></span>"
                    for r in col_info
                )
                schema_html = (
                    f"<div style='margin-bottom:4px;font-size:11px;color:#64748b;font-weight:600;text-transform:uppercase;letter-spacing:0.4px'>"
                    f"Columns — click to insert</div>"
                    f"<div class='schema-pills'>{pills}</div>"
                )
            except Exception:
                pass

    # ── Table hint on landing (no table selected) ───────────────────
    hint_html = ""
    if not safe_table and tables:
        rows_hint = "".join(
            f"<tr><td>{t}</td><td>{TABLE_DESCRIPTIONS.get(t, '')}</td></tr>"
            for t in tables
        )
        hint_html = (
            "<div class='table-hint'>"
            "<div style='font-size:12px;font-weight:700;color:#475569;text-transform:uppercase;letter-spacing:0.4px;margin-bottom:10px'>Available tables</div>"
            f"<table><tbody>{rows_hint}</tbody></table>"
            "</div>"
        )

    # ── Default SQL ──────────────────────────────────────────────────
    default_sql = sql or (f"SELECT *\nFROM t\nLIMIT {limit}" if safe_table else "-- Select a table above, then write your SQL here.\n-- The table data is exposed as view \"t\".\n\nSELECT *\nFROM t\nLIMIT 100")

    # ── Export URL (shown only when there are results to export) ─────
    import urllib.parse as _urlparse
    export_qs = _urlparse.urlencode({
        "table": table or "",
        "from": date_from or "",
        "to": date_to or "",
        "sql": sql or "",
        "limit": limit,
    })
    export_link_html = (
        f"<a href='/query/export?{export_qs}'>&#8595; Export CSV</a>"
        if safe_table and sql else ""
    )

    form_html = f"""
    <form method="get">
        <label>Table
            <select name="table" onchange="this.form.submit()">
                {table_options}
            </select>
        </label>
        <label>From<input type="date" name="from" value="{date_from or ''}"></label>
        <label>To<input type="date" name="to" value="{date_to or ''}"></label>
        <label>Limit<input type="number" name="limit" value="{limit}" min="1" max="5000" style="width:80px"></label>
        {schema_html}
        <label class="sql-label">SQL
            <textarea name="sql" spellcheck="false">{default_sql}</textarea>
        </label>
        <button type="submit">&#9654; Run query</button>
        {export_link_html}
    </form>
    """

    # ── Execute ──────────────────────────────────────────────────────
    result_html = ""
    if safe_table and sql and sql.strip():
        paths = list_partitions(safe_table, date_from, date_to)
        if not paths:
            result_html = no_data_notice()
        else:
            try:
                t0 = _time.monotonic()
                cols, rows = run_query(paths, sql)
                elapsed = _time.monotonic() - t0
                n = len(rows)
                result_html = (
                    f"<p class='query-meta'>{n:,} row{'s' if n != 1 else ''} &middot; {elapsed:.2f}s</p>"
                    + html_table(rows, cols, max_rows=limit)
                )
            except Exception as exc:
                result_html = f"<div class='query-error'>{exc}</div>"

    body = hint_html + form_html + result_html
    return page("SQL Query", body)


@app.get("/query/export")
def query_export(
    table: Optional[str] = None,
    date_from: Optional[str] = Query(None, alias="from"),
    date_to: Optional[str] = Query(None, alias="to"),
    sql: Optional[str] = None,
    limit: int = Query(100_000, ge=1, le=500_000),
):
    tables = list_tables()
    safe_table = table if table in tables else None
    if not safe_table or not sql or not sql.strip():
        return PlainTextResponse("Missing table or sql parameter.", status_code=400)

    paths = list_partitions(safe_table, date_from, date_to)
    if not paths:
        return PlainTextResponse("No data found for the selected date range.", status_code=404)

    sql_clean = sql.strip().rstrip(";")
    conn = duckdb.connect(database=":memory:")
    conn.execute("PRAGMA threads=4;")
    files_sql = "[" + ",".join("'" + p.replace("'", "''") + "'" for p in paths) + "]"
    conn.execute(f"CREATE OR REPLACE VIEW t AS SELECT * FROM read_parquet({files_sql});")

    def gen():
        try:
            cur = conn.execute(sql_clean)
            cols = [d[0] for d in cur.description]
            buf = io.StringIO()
            w = csv.writer(buf)
            w.writerow(cols)
            yield buf.getvalue().encode("utf-8-sig")
            buf.seek(0); buf.truncate(0)
            while True:
                batch = cur.fetchmany(1000)
                if not batch:
                    break
                for row in batch:
                    w.writerow(row)
                yield buf.getvalue().encode("utf-8")
                buf.seek(0); buf.truncate(0)
        finally:
            conn.close()

    filename = f"query_{safe_table}.csv"
    headers = {
        "Content-Disposition": f'attachment; filename="{filename}"',
        "Cache-Control": "no-store",
    }
    return StreamingResponse(gen(), media_type="text/csv; charset=utf-8", headers=headers)


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

    def stream_csv(sql: str, paths: List[str], filename: str):
        if not paths:
            return PlainTextResponse("No data found for the selected date range.", status_code=404)

        sql_clean = sql.strip().rstrip(";").strip()

        conn = duckdb.connect(database=":memory:")
        conn.execute("PRAGMA threads=4;")
        files_sql = "[" + ",".join("'" + p.replace("'", "''") + "'" for p in paths) + "]"
        conn.execute(f"CREATE OR REPLACE VIEW t AS SELECT * FROM read_parquet({files_sql});")

        def gen():
            try:
                cur = conn.execute(sql_clean)
                cols = [d[0] for d in cur.description]

                s = io.StringIO()
                w = csv.writer(s)
                w.writerow(cols)
                yield s.getvalue().encode("utf-8-sig")
                s.seek(0)
                s.truncate(0)

                while True:
                    batch = cur.fetchmany(1000)
                    if not batch:
                        break
                    for row in batch:
                        w.writerow(row)
                    yield s.getvalue().encode("utf-8")
                    s.seek(0)
                    s.truncate(0)
            finally:
                conn.close()

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
        if url_group:
            clauses.append(f"url_group = '{sql_escape_string(url_group)}'")
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
        if url_group:
            clauses.append(f"url_group = '{sql_escape_string(url_group)}'")
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
        if url_group:
            clauses.append(f"url_group = '{sql_escape_string(url_group)}'")
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
        if url_group:
            clauses.append(f"url_group = '{sql_escape_string(url_group)}'")
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
        if url_group:
            clauses.append(f"url_group = '{sql_escape_string(url_group)}'")
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

    if report == "wasted-crawl":
        paths = list_partitions("wasted_crawl_daily", date_from, date_to)
        score_col = "waste_score_strict" if strict else "waste_score"
        clauses = []
        if bot:
            clauses.append(f"bot_family = '{sql_escape_string(bot)}'")
        if not include_assets:
            clauses.append("url_group NOT IN ('Nuxt Assets','Static Assets')")
        if url_group:
            clauses.append(f"url_group = '{sql_escape_string(url_group)}'")
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
        if locale:
            clauses.append(f"locale = '{sql_escape_string(locale)}'")
        if url_group:
            clauses.append(f"url_group = '{sql_escape_string(url_group)}'")
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
        if bot:
            clauses.append(f"bot_family = '{sql_escape_string(bot)}'")
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
        if not utm_source:
            return PlainTextResponse("Missing utm_source parameter.", status_code=400)
        src_esc = sql_escape_string(utm_source)
        sql = f"""
        SELECT date, SUM({metric}) AS hits
        FROM t
        WHERE utm_source = '{src_esc}'
        GROUP BY date
        ORDER BY date;
        """
        return stream_csv(sql, paths, f"utm_{utm_source}_daily.csv")

    if report == "utm-urls":
        paths = list_partitions("utm_source_urls_daily", date_from, date_to)
        if not utm_source:
            return PlainTextResponse("Missing utm_source parameter.", status_code=400)

        gb = (group_by or "url_group").strip().lower()
        if gb not in ("url_group", "path"):
            gb = "url_group"
        dim = "url_group" if gb == "url_group" else "path"

        src_esc = sql_escape_string(utm_source)
        sql = f"""
        SELECT {dim} AS dimension, SUM({metric}) AS hits
        FROM t
        WHERE utm_source = '{src_esc}'
          AND {dim} IS NOT NULL
        GROUP BY {dim}
        ORDER BY hits DESC
        LIMIT {int(limit)};
        """
        return stream_csv(sql, paths, f"utm_{utm_source}_{dim}.csv")

    return PlainTextResponse(
        "Unknown report. Try report=daily, daily-status, locales, url-groups, locale-groups, top-urls, top-3xx, top-4xx, top-5xx, url-bytes, bots, wasted-crawl, top-resource-waste, bot-urls, utm-sources, utm, utm-urls",
        status_code=400,
    )