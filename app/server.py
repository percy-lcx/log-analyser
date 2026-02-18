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


def html_table(rows, columns, max_rows: int = 999) -> str:
    """
    Simple HTML table with client-side sorting (no deps).
    Click a header to sort; click again to reverse.
    """
    head = "".join(
        f"<th scope='col' data-col='{i}'>{c}</th>"
        for i, c in enumerate(columns)
    )

    body_rows = []
    for i, r in enumerate(rows):
        if i >= max_rows:
            break
        tds = "".join(f"<td>{'' if v is None else v}</td>" for v in r)
        body_rows.append(f"<tr>{tds}</tr>")
    body = "\n".join(body_rows)

    return (
        "<table class='sortable' border='1' cellpadding='6' cellspacing='0'>"
        f"<thead><tr>{head}</tr></thead>"
        f"<tbody>{body}</tbody>"
        "</table>"
    )


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

        fig.add_trace(go.Scatter(x=x, y=y, mode="lines", name=yc))

    fig.update_layout(height=400, margin=dict(l=20, r=20, t=40, b=20), title=title)
    return fig.to_html(full_html=False, include_plotlyjs=False)


def bar_chart(rows, columns, x_col, y_col, title):
    if not rows:
        return "<p>No data.</p>"

    df = pd.DataFrame(rows, columns=columns)

    x_series = df[x_col] if x_col in df.columns else pd.Series([])
    x = ["" if v is None or pd.isna(v) else str(v) for v in x_series.tolist()]

    y_raw = df[y_col].tolist() if y_col in df.columns else []
    y = []
    for v in y_raw:
        if v is None or (isinstance(v, float) and pd.isna(v)) or pd.isna(v):
            y.append(None)
        else:
            try:
                y.append(float(v))
            except Exception:
                y.append(None)

    fig = go.Figure(data=[go.Bar(x=x, y=y)])
    fig.update_layout(height=400, margin=dict(l=20, r=20, t=40, b=20), title=title)
    return fig.to_html(full_html=False, include_plotlyjs=False)


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
    return "<p><em>No data found for the selected date range.</em></p>"


def select_html(name: str, options: List[str], current: Optional[str], label: str) -> str:
    opts = ["<option value=''>All</option>"]
    for opt in options:
        selected = "selected" if current == opt else ""
        opts.append(f"<option value='{opt}' {selected}>{opt}</option>")
    return f"<label>{label}: <select name='{name}'>{''.join(opts)}</select></label>"


def page(title: str, body: str) -> HTMLResponse:

    plotly_cdn = "<script src='https://cdn.plot.ly/plotly-latest.min.js'></script>"

    css = """
    <style>
    table.sortable th { cursor: pointer; user-select: none; }
    table.sortable th.sorted-asc::after  { content: " ▲"; }
    table.sortable th.sorted-desc::after { content: " ▼"; }
    </style>
    """

    js = """
    <script>
    (function() {
    function parseNumber(s) {
        const cleaned = s.replace(/[,\\s]/g, "");
        if (!cleaned) return null;
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

        if (na !== null && nb !== null) {
            return asc ? (na - nb) : (nb - na);
        }

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
    });
    })();
    </script>
    """

    html = f"""
    <html>
    <head>
        <meta charset="utf-8">
        <title>{title}</title>
        {plotly_cdn}
        {css}
    </head>
    <body style="font-family: Arial, sans-serif; padding: 20px;">
        <h1>{title}</h1>
        <p><a href="/">Home</a></p>
        {body}
        {js}
    </body>
    </html>
    """
    return HTMLResponse(html)


def date_filters_html(date_from, date_to):
    return f"""
    <form method="get">
    <label>From: <input name="from" value="{date_from or ''}" placeholder="YYYY-MM-DD"></label>
    <label>To: <input name="to" value="{date_to or ''}" placeholder="YYYY-MM-DD"></label>
    <button type="submit">Apply</button>
    </form>
    """


@app.get("/", response_class=HTMLResponse)
def index():
    items = [
        ("Crawl volume", "/reports/crawl-volume"),
        ("Status over time", "/reports/status-over-time"),
        ("Locale breakdown", "/reports/locales"),
        ("URL group breakdown", "/reports/url-groups"),
        ("Locale x URL group", "/reports/locale-groups"),
        ("Top URLs", "/reports/top-urls"),
        ("Top 404", "/reports/top-404"),
        ("Top 5xx", "/reports/top-5xx"),
        ("Bots", "/reports/bots"),
        ("Wasted crawl", "/reports/wasted-crawl"),
        ("Top resource waste", "/reports/top-resource-waste"),
        ("Bot URLs (Googlebot)", "/reports/bot-urls?bot=Googlebot"),
        ("UTM (all sources)", "/reports/utm"),
        ("UTM: chatgpt.com (legacy)", "/reports/utm-chatgpt"),
    ]
    links = "<ul>" + "".join(f"<li><a href='{u}'>{n}</a></li>" for n, u in items) + "</ul>"
    help_txt = """
    <p>Tip: Use query parameters like <code>?from=2026-01-01&amp;to=2026-01-31</code>.</p>
    <p>CSV export: add <code>/export?report=...</code> (links are on each page).</p>
    """
    return page("Local Log Dashboard", links + help_txt)


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

    body += "<h2>All hits (includes Nuxt/static resources)</h2>"
    body += html_table(rows1, cols1)

    body += "<h2>Non-resource hits (excludes Nuxt/static resources)</h2>"
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
    body += html_table(rows, cols)
    return page("URL group breakdown", body)


@app.get("/reports/locale-groups", response_class=HTMLResponse)
def locale_groups(
    date_from: Optional[str] = Query(None, alias="from"),
    date_to: Optional[str] = Query(None, alias="to"),
    locale: Optional[str] = None,
    url_group: Optional[str] = None,
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
    <button type="submit">Apply</button>
    </form>
    """
    body += f"<p><a href='/export?report=locale-groups&from={date_from or ''}&to={date_to or ''}&locale={locale or ''}&url_group={url_group or ''}&limit={limit}'>Export CSV</a></p>"
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
    body = date_filters_html(date_from, date_to)
    body += f"<p><a href='/export?report=daily&from={date_from or ''}&to={date_to or ''}'>Export CSV</a></p>"
    body += chart_html
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
    return f"<p><a href='/export?{'&'.join(qs)}'>Export CSV</a></p>"


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
    body += html_table(rows, cols)
    return page("Top URLs", body)


@app.get("/reports/top-404", response_class=HTMLResponse)
def top_404(
    date_from: Optional[str] = Query(None, alias="from"),
    date_to: Optional[str] = Query(None, alias="to"),
    include_assets: bool = False,
    url_group: Optional[str] = None,
    limit: int = 500,
):
    paths = list_partitions("top_404_daily", date_from, date_to)
    body = date_filters_html(date_from, date_to)

    groups = distinct_values("top_404_daily", "url_group", date_from, date_to)
    body += "<form method='get'>"
    body += f"<input type='hidden' name='from' value='{date_from or ''}'>"
    body += f"<input type='hidden' name='to' value='{date_to or ''}'>"
    body += select_html("url_group", groups, url_group, "URL group")
    checked = "checked" if include_assets else ""
    body += f" <label><input type='checkbox' name='include_assets' value='true' {checked}> Include assets</label>"
    body += f" <label>Limit: <input name='limit' value='{int(limit)}' size='6'></label>"
    body += " <button type='submit'>Apply</button></form>"

    if not paths:
        return page("Top 404", body + no_data_notice())

    clauses = []
    if not include_assets:
        clauses.append("url_group NOT IN ('Nuxt Assets','Static Assets')")
    if url_group:
        clauses.append(f"url_group = '{sql_escape_string(url_group)}'")
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""

    sql = f"""
    SELECT path, url_group,
        SUM(hits_404) AS hits_404,
        SUM(hits_404_bot) AS hits_404_bot,
        SUM(bytes_sent_404) AS bytes_sent_404
    FROM t
    {where}
    GROUP BY path, url_group
    ORDER BY hits_404 DESC
    LIMIT {int(limit)};
    """
    cols, rows = run_query(paths, sql)
    body += export_link(
        "top-404", date_from, date_to,
        extra=f"&include_assets={'true' if include_assets else 'false'}"
              + (f"&url_group={url_group}" if url_group else "")
              + f"&limit={int(limit)}"
    )
    body += html_table(rows, cols)
    return page("Top 404", body)


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

    sql = f"""
    SELECT path, url_group,
        SUM(hits_5xx) AS hits_5xx,
        SUM(hits_5xx_bot) AS hits_5xx_bot
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
    body += html_table(rows, cols)
    return page("Top 5xx", body)


@app.get("/reports/bots", response_class=HTMLResponse)
def bots(date_from: Optional[str] = Query(None, alias="from"),
         date_to: Optional[str] = Query(None, alias="to")):

    paths = list_partitions("bot_daily", date_from, date_to)
    body = date_filters_html(date_from, date_to)

    if not paths:
        return page("Bots", body + no_data_notice())

    sql = """
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
    cols, rows = run_query(paths, sql)

    chart_html = bar_chart(
        rows,
        cols,
        x_col="bot_family",
        y_col="hits",
        title="Bot hits by family"
    )

    body += f"<p><a href='/export?report=bots&from={date_from or ''}&to={date_to or ''}'>Export CSV</a></p>"
    body += chart_html
    body += "<br>"
    body += html_table(rows, cols)
    return page("Bots", body)


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

    chart_html = bar_chart(rows, cols, x_col="path", y_col="waste_score", title="Highest waste score (top paths)")

    body += (
        f"<p><a href='/export?report=wasted-crawl&from={date_from or ''}&to={date_to or ''}"
        f"&strict={'true' if strict else 'false'}"
        f"&include_assets={'true' if include_assets else 'false'}"
        f"{'&bot=' + bot if bot else ''}"
        f"{'&url_group=' + url_group if url_group else ''}"
        f"&limit={int(limit)}'>Export CSV</a></p>"
    )
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


@app.get("/reports/bot-urls", response_class=HTMLResponse)
def bot_urls(
    date_from: Optional[str] = Query(None, alias="from"),
    date_to: Optional[str] = Query(None, alias="to"),
    bot: Optional[str] = None,
    include_assets: bool = False,
    limit: int = 500,
):
    paths = list_partitions("bot_urls_daily", date_from, date_to)
    body = date_filters_html(date_from, date_to)

    bots_list = distinct_values("bot_urls_daily", "bot_family", date_from, date_to)
    groups = distinct_values("bot_urls_daily", "url_group", date_from, date_to)

    body += "<form method='get'>"
    body += f"<input type='hidden' name='from' value='{date_from or ''}'>"
    body += f"<input type='hidden' name='to' value='{date_to or ''}'>"
    body += select_html("bot", bots_list, bot, "Bot family")
    checked_assets = "checked" if include_assets else ""
    body += f" <label><input type='checkbox' name='include_assets' value='true' {checked_assets}> Include assets</label>"
    body += f" <label>Limit: <input name='limit' value='{int(limit)}' size='6'></label>"
    body += " <button type='submit'>Apply</button></form>"

    if not paths:
        return page("Bot URLs", body + no_data_notice())

    clauses = []
    if bot:
        clauses.append(f"bot_family = '{sql_escape_string(bot)}'")
    if not include_assets:
        clauses.append("url_group NOT IN ('Nuxt Assets','Static Assets')")
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""

    sql = f"""
    SELECT bot_family, url_group, path,
        SUM(hits) AS hits
    FROM t
    {where}
    GROUP BY bot_family, url_group, path
    ORDER BY hits DESC
    LIMIT {int(limit)};
    """
    cols, rows = run_query(paths, sql)
    body += export_link(
        "bot-urls", date_from, date_to,
        extra=f"&include_assets={'true' if include_assets else 'false'}"
              + (f"&bot={bot}" if bot else "")
              + f"&limit={int(limit)}"
    )
    body += html_table(rows, cols)
    return page("Bot URLs", body)

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

    if report == "top-404":
        paths = list_partitions("top_404_daily", date_from, date_to)
        clauses = []
        if not include_assets:
            clauses.append("url_group NOT IN ('Nuxt Assets','Static Assets')")
        if url_group:
            clauses.append(f"url_group = '{sql_escape_string(url_group)}'")
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        sql = f"""
        SELECT path, url_group,
                SUM(hits_404) AS hits_404,
                SUM(hits_404_bot) AS hits_404_bot
        FROM t
        {where}
        GROUP BY path, url_group
        ORDER BY hits_404 DESC
        LIMIT {int(limit)};
        """
        return stream_csv(sql, paths, "top_404.csv")

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
                SUM(hits_5xx_bot) AS hits_5xx_bot
        FROM t
        {where}
        GROUP BY path, url_group
        ORDER BY hits_5xx DESC
        LIMIT {int(limit)};
        """
        return stream_csv(sql, paths, "top_5xx.csv")

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
        "Unknown report. Try report=daily, daily-status, locales, url-groups, locale-groups, top-urls, top-404, top-5xx, bots, wasted-crawl, top-resource-waste, bot-urls, utm-sources, utm, utm-urls",
        status_code=400,
    )