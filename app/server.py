from __future__ import annotations

import os
from pathlib import Path
from typing import List, Optional

import duckdb
from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, StreamingResponse, PlainTextResponse

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
        # Extract YYYY-MM-DD from "date=YYYY-MM-DD"
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


# def html_table(rows, columns, max_rows: int = 200) -> str:
#     # Simple HTML table (no extra deps)
#     head = "".join(f"<th>{c}</th>" for c in columns)
#     body_rows = []
#     for i, r in enumerate(rows):
#         if i >= max_rows:
#             break
#         tds = "".join(f"<td>{'' if v is None else v}</td>" for v in r)
#         body_rows.append(f"<tr>{tds}</tr>")
#     body = "\n".join(body_rows)
#     return f"<table border='1' cellpadding='6' cellspacing='0'><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table>"

def html_table(rows, columns, max_rows: int = 200) -> str:
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

    # Build a DuckDB list literal: ['file1','file2',...]
    files_sql = "[" + ",".join("'" + p.replace("'", "''") + "'" for p in paths) + "]"
    conn.execute(f"CREATE OR REPLACE VIEW t AS SELECT * FROM read_parquet({files_sql});")

    res = conn.execute(sql)
    cols = [d[0] for d in res.description]
    rows = res.fetchall()
    conn.close()
    return cols, rows


def sql_escape_string(s: str) -> str:
    # DuckDB uses SQL single quotes; escape single quote by doubling it.
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

# def page(title: str, body: str) -> HTMLResponse:
#     html = f"""
#     <html>
#       <head><title>{title}</title></head>
#       <body style="font-family: Arial, sans-serif; padding: 20px;">
#         <h1>{title}</h1>
#         <p><a href="/">Home</a></p>
#         {body}
#       </body>
#     </html>
#     """
#     return HTMLResponse(html)

def page(title: str, body: str) -> HTMLResponse:
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
        // Remove commas and spaces; keep digits, dot, minus.
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

          // Fallback to string compare
          return asc ? a.localeCompare(b) : b.localeCompare(a);
        });

        // Re-append in new order
        for (const r of rows) tbody.appendChild(r);
      }

      function attachSortable(table) {
        const headers = table.tHead ? Array.from(table.tHead.rows[0].cells) : [];
        headers.forEach((th, idx) => {
          th.addEventListener("click", () => {
            const currentlyAsc = th.classList.contains("sorted-asc");
            const asc = !currentlyAsc;

            // Clear indicators for this table
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
    ]
    links = "<ul>" + "".join(f"<li><a href='{u}'>{n}</a></li>" for n, u in items) + "</ul>"
    help_txt = """
    <p>Tip: Use query parameters like <code>?from=2026-01-01&amp;to=2026-01-31</code>.</p>
    <p>CSV export: add <code>/export?report=...</code> (links are on each page).</p>
    """
    return page("Local Log Dashboard", links + help_txt)


def date_filters_html(date_from, date_to):
    return f"""
    <form method="get">
      <label>From: <input name="from" value="{date_from or ''}" placeholder="YYYY-MM-DD"></label>
      <label>To: <input name="to" value="{date_to or ''}" placeholder="YYYY-MM-DD"></label>
      <button type="submit">Apply</button>
    </form>
    """


@app.get("/reports/locales", response_class=HTMLResponse)
def locales(date_from: Optional[str] = Query(None, alias="from"), date_to: Optional[str] = Query(None, alias="to")):
    paths = list_partitions("locale_daily", date_from, date_to)
    if not paths:
        body = date_filters_html(date_from, date_to) + no_data_notice()
        return page("Locale breakdown", body)
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
    cols, rows = run_query(paths, sql)
    body = date_filters_html(date_from, date_to)
    body += f"<p><a href='/export?report=locales&from={date_from or ''}&to={date_to or ''}'>Export CSV</a></p>"
    body += html_table(rows, cols)
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
    limit: int = 200,
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
    body = date_filters_html(date_from, date_to)
    body += f"<p><a href='/export?report=daily&from={date_from or ''}&to={date_to or ''}'>Export CSV</a></p>"
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
    body = date_filters_html(date_from, date_to)
    body += f"<p><a href='/export?report=daily-status&from={date_from or ''}&to={date_to or ''}'>Export CSV</a></p>"
    body += html_table(rows, cols)
    return page("Status codes over time (daily)", body)


@app.get("/reports/top-urls", response_class=HTMLResponse)
def top_urls(
    date_from: Optional[str] = Query(None, alias="from"),
    date_to: Optional[str] = Query(None, alias="to"),
    include_assets: bool = False,
    url_group: Optional[str] = None,
    limit: int = 200
):
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
    cols, rows = run_query(paths, sql)
    group_options = distinct_values("group_daily", "url_group", date_from, date_to)
    body = date_filters_html(date_from, date_to)
    body += f"""
    <form method="get">
      <input type="hidden" name="from" value="{date_from or ''}">
      <input type="hidden" name="to" value="{date_to or ''}">
      <label><input type="checkbox" name="include_assets" value="true" {"checked" if include_assets else ""}> Include assets</label>
      {select_html("url_group", group_options, url_group, "URL group")}
      <label>Limit: <input name="limit" value="{limit}" size="6"></label>
      <button type="submit">Apply</button>
    </form>
    """
    body += f"<p><a href='/export?report=top-urls&from={date_from or ''}&to={date_to or ''}&include_assets={str(include_assets).lower()}&url_group={url_group or ''}&limit={limit}'>Export CSV</a></p>"
    body += html_table(rows, cols, max_rows=min(int(limit), 500))
    return page("Top URLs", body)


@app.get("/reports/top-404", response_class=HTMLResponse)
def top_404(
    date_from: Optional[str] = Query(None, alias="from"),
    date_to: Optional[str] = Query(None, alias="to"),
    include_assets: bool = False,
    url_group: Optional[str] = None,
    limit: int = 200
):
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
    cols, rows = run_query(paths, sql)
    group_options = distinct_values("group_daily", "url_group", date_from, date_to)
    body = date_filters_html(date_from, date_to)
    body += f"""
    <form method="get">
      <input type="hidden" name="from" value="{date_from or ''}">
      <input type="hidden" name="to" value="{date_to or ''}">
      <label><input type="checkbox" name="include_assets" value="true" {"checked" if include_assets else ""}> Include assets</label>
      {select_html("url_group", group_options, url_group, "URL group")}
      <label>Limit: <input name="limit" value="{limit}" size="6"></label>
      <button type="submit">Apply</button>
    </form>
    """
    body += f"<p><a href='/export?report=top-404&from={date_from or ''}&to={date_to or ''}&include_assets={str(include_assets).lower()}&url_group={url_group or ''}&limit={limit}'>Export CSV</a></p>"
    body += html_table(rows, cols, max_rows=min(int(limit), 500))
    return page("Top 404 URLs", body)


@app.get("/reports/top-5xx", response_class=HTMLResponse)
def top_5xx(
    date_from: Optional[str] = Query(None, alias="from"),
    date_to: Optional[str] = Query(None, alias="to"),
    include_assets: bool = False,
    url_group: Optional[str] = None,
    limit: int = 200
):
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
    cols, rows = run_query(paths, sql)
    group_options = distinct_values("group_daily", "url_group", date_from, date_to)
    body = date_filters_html(date_from, date_to)
    body += f"""
    <form method="get">
      <input type="hidden" name="from" value="{date_from or ''}">
      <input type="hidden" name="to" value="{date_to or ''}">
      <label><input type="checkbox" name="include_assets" value="true" {"checked" if include_assets else ""}> Include assets</label>
      {select_html("url_group", group_options, url_group, "URL group")}
      <label>Limit: <input name="limit" value="{limit}" size="6"></label>
      <button type="submit">Apply</button>
    </form>
    """
    body += f"<p><a href='/export?report=top-5xx&from={date_from or ''}&to={date_to or ''}&include_assets={str(include_assets).lower()}&url_group={url_group or ''}&limit={limit}'>Export CSV</a></p>"
    body += html_table(rows, cols, max_rows=min(int(limit), 500))
    return page("Top 5xx URLs", body)


@app.get("/reports/bots", response_class=HTMLResponse)
def bots(date_from: Optional[str] = Query(None, alias="from"), date_to: Optional[str] = Query(None, alias="to"), limit: int = 200):
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
    cols, rows = run_query(paths, sql)
    body = date_filters_html(date_from, date_to)
    body += f"<p><a href='/export?report=bots&from={date_from or ''}&to={date_to or ''}&limit={limit}'>Export CSV</a></p>"
    body += html_table(rows, cols, max_rows=min(int(limit), 500))
    return page("Bots summary", body)


@app.get("/reports/wasted-crawl", response_class=HTMLResponse)
def wasted_crawl(
    date_from: Optional[str] = Query(None, alias="from"),
    date_to: Optional[str] = Query(None, alias="to"),
    strict: bool = False,
    bot: Optional[str] = None,
    include_assets: bool = False,
    url_group: Optional[str] = None,
    limit: int = 200,
):
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

    sql = f"""
      SELECT path, url_group, bot_family,
             SUM(bot_hits) AS bot_hits,
             SUM(parameterized_bot_hits) AS parameterized_bot_hits,
             SUM(redirect_bot_hits) AS redirect_bot_hits,
             SUM(error_bot_hits) AS error_bot_hits,
             SUM(resource_bot_hits) AS resource_bot_hits,
             SUM(resource_error_bot_hits) AS resource_error_bot_hits,
             SUM({score_col}) AS waste_score
      FROM t
      {where}
      GROUP BY path, url_group, bot_family
      ORDER BY waste_score DESC
      LIMIT {int(limit)};
    """
    cols, rows = run_query(paths, sql)
    group_options = distinct_values("group_daily", "url_group", date_from, date_to)
    body = date_filters_html(date_from, date_to)
    body += f"""
    <form method="get">
      <input type="hidden" name="from" value="{date_from or ''}">
      <input type="hidden" name="to" value="{date_to or ''}">
      <label><input type="checkbox" name="strict" value="true" {"checked" if strict else ""}> Strict (count all resources)</label>
      <label><input type="checkbox" name="include_assets" value="true" {"checked" if include_assets else ""}> Include assets in list</label>
      {select_html("url_group", group_options, url_group, "URL group")}
      <label>Bot family (optional): <input name="bot" value="{bot or ''}" placeholder="e.g. Googlebot"></label>
      <label>Limit: <input name="limit" value="{limit}" size="6"></label>
      <button type="submit">Apply</button>
    </form>
    """
    body += f"<p><a href='/export?report=wasted-crawl&from={date_from or ''}&to={date_to or ''}&strict={str(strict).lower()}&include_assets={str(include_assets).lower()}&url_group={url_group or ''}&bot={bot or ''}&limit={limit}'>Export CSV</a></p>"
    body += html_table(rows, cols, max_rows=min(int(limit), 500))
    return page("Wasted crawl", body)


@app.get("/reports/top-resource-waste", response_class=HTMLResponse)
def top_resource_waste(
    date_from: Optional[str] = Query(None, alias="from"),
    date_to: Optional[str] = Query(None, alias="to"),
    limit: int = 200,
):
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
    cols, rows = run_query(paths, sql)
    body = date_filters_html(date_from, date_to)
    body += f"<p><a href='/export?report=top-resource-waste&from={date_from or ''}&to={date_to or ''}&limit={limit}'>Export CSV</a></p>"
    body += html_table(rows, cols, max_rows=min(int(limit), 500))
    return page("Top resource waste", body)


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
    limit: int = 100000
):
    # Map report name -> query + table source
    report = report.strip().lower()

    def stream_csv(sql: str, paths: List[str], filename: str):
        if not paths:
            return PlainTextResponse("No data found for the selected date range.", status_code=404)
        conn = duckdb.connect(database=":memory:")
        conn.execute("PRAGMA threads=4;")
        files_sql = "[" + ",".join("'" + p.replace("'", "''") + "'" for p in paths) + "]"
        conn.execute(f"CREATE OR REPLACE VIEW t AS SELECT * FROM read_parquet({files_sql});")


        # Stream by writing to an in-memory string generator via DuckDB COPY TO STDOUT
        def gen():
            yield conn.execute(f"COPY ({sql}) TO STDOUT (HEADER, DELIMITER ',');").fetchone()[0]

        headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
        return StreamingResponse(gen(), media_type="text/csv", headers=headers)

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
        sql = f"""
          SELECT path, url_group, bot_family,
                 SUM(bot_hits) AS bot_hits,
                 SUM(parameterized_bot_hits) AS parameterized_bot_hits,
                 SUM(redirect_bot_hits) AS redirect_bot_hits,
                 SUM(error_bot_hits) AS error_bot_hits,
                 SUM(resource_bot_hits) AS resource_bot_hits,
                 SUM(resource_error_bot_hits) AS resource_error_bot_hits,
                 SUM({score_col}) AS waste_score
          FROM t
          {where}
          GROUP BY path, url_group, bot_family
          ORDER BY waste_score DESC
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

    return PlainTextResponse(
        "Unknown report. Try report=daily, daily-status, locales, url-groups, locale-groups, top-urls, top-404, top-5xx, bots, wasted-crawl, top-resource-waste",
        status_code=400,
    )
