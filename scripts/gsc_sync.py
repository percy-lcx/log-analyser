#!/usr/bin/env python3
"""
Google Search Console sync script.

Usage:
    python scripts/gsc_sync.py                             # daily sync (last 3 days)
    python scripts/gsc_sync.py --backfill                  # full historical pull (~16 months)
    python scripts/gsc_sync.py --from YYYY-MM-DD --to YYYY-MM-DD  # specific date range
"""
import argparse
import json
import os
import sqlite3
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
STATE_DIR = ROOT / "state"
MANIFEST_DB = STATE_DIR / "manifest.sqlite"
CREDENTIALS_PATH = STATE_DIR / "gsc_credentials.json"
DATA_GSC = ROOT / "data" / "gsc"
DATA_PARSED = ROOT / "data" / "parsed"
DATA_AGG = ROOT / "data" / "aggregates"

# GSC API limits
MAX_ROWS_PER_REQUEST = 25000
RATE_LIMIT_PER_MINUTE = 200
SCOPES = ["https://www.googleapis.com/auth/webmasters.readonly"]


# ---------------------------------------------------------------------------
# SQLite helpers — token and sync state storage
# ---------------------------------------------------------------------------

def init_gsc_tables():
    conn = sqlite3.connect(MANIFEST_DB)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS gsc_tokens (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            access_token TEXT,
            refresh_token TEXT,
            expiry TEXT,
            property_url TEXT,
            token_uri TEXT,
            client_id TEXT,
            client_secret TEXT
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS gsc_sync_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sync_date TEXT NOT NULL,
            status TEXT NOT NULL,
            records_pulled INTEGER DEFAULT 0,
            error_message TEXT,
            started_at TEXT NOT NULL,
            completed_at TEXT
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS gsc_sync_state (
            date TEXT PRIMARY KEY,
            status TEXT NOT NULL,
            records INTEGER DEFAULT 0,
            synced_at TEXT
        )
    """)
    conn.commit()
    conn.close()


def get_stored_tokens() -> dict | None:
    conn = sqlite3.connect(MANIFEST_DB)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("SELECT * FROM gsc_tokens WHERE id = 1")
    row = cur.fetchone()
    conn.close()
    if row and row["refresh_token"]:
        return dict(row)
    return None


def save_tokens(access_token: str, refresh_token: str, expiry: str,
                property_url: str, token_uri: str,
                client_id: str, client_secret: str):
    conn = sqlite3.connect(MANIFEST_DB)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO gsc_tokens (id, access_token, refresh_token, expiry,
                                property_url, token_uri, client_id, client_secret)
        VALUES (1, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            access_token=excluded.access_token,
            refresh_token=excluded.refresh_token,
            expiry=excluded.expiry,
            property_url=excluded.property_url,
            token_uri=excluded.token_uri,
            client_id=excluded.client_id,
            client_secret=excluded.client_secret
    """, (access_token, refresh_token, expiry, property_url,
          token_uri, client_id, client_secret))
    conn.commit()
    conn.close()


def update_property_url(url: str):
    conn = sqlite3.connect(MANIFEST_DB)
    cur = conn.cursor()
    cur.execute("UPDATE gsc_tokens SET property_url = ? WHERE id = 1", (url,))
    conn.commit()
    conn.close()


def delete_tokens():
    conn = sqlite3.connect(MANIFEST_DB)
    cur = conn.cursor()
    cur.execute("DELETE FROM gsc_tokens WHERE id = 1")
    conn.commit()
    conn.close()


def log_sync(sync_date: str, status: str, records: int = 0, error: str = None,
             started_at: str = None, completed_at: str = None):
    conn = sqlite3.connect(MANIFEST_DB)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO gsc_sync_log (sync_date, status, records_pulled, error_message,
                                  started_at, completed_at)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (sync_date, status, records, error,
          started_at or datetime.now(tz=timezone.utc).isoformat(),
          completed_at))
    conn.commit()
    conn.close()


def update_sync_state(date: str, status: str, records: int = 0):
    conn = sqlite3.connect(MANIFEST_DB)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO gsc_sync_state (date, status, records, synced_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(date) DO UPDATE SET
            status=excluded.status,
            records=excluded.records,
            synced_at=excluded.synced_at
    """, (date, status, records, datetime.now(tz=timezone.utc).isoformat()))
    conn.commit()
    conn.close()


def get_sync_state(date: str) -> dict | None:
    conn = sqlite3.connect(MANIFEST_DB)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("SELECT * FROM gsc_sync_state WHERE date = ?", (date,))
    row = cur.fetchone()
    conn.close()
    return dict(row) if row else None


def get_last_sync_log(limit: int = 10) -> list:
    conn = sqlite3.connect(MANIFEST_DB)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("""
        SELECT * FROM gsc_sync_log ORDER BY id DESC LIMIT ?
    """, (limit,))
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


def get_gsc_stats() -> dict:
    conn = sqlite3.connect(MANIFEST_DB)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*), SUM(records) FROM gsc_sync_state WHERE status = 'done'")
    row = cur.fetchone()
    conn.close()
    return {"dates_synced": row[0] or 0, "total_records": row[1] or 0}


# ---------------------------------------------------------------------------
# OAuth 2.0 flow
# ---------------------------------------------------------------------------

def build_credentials_from_stored(token_data: dict):
    """Build a google.oauth2.credentials.Credentials from stored token data."""
    from google.oauth2.credentials import Credentials

    expiry = None
    if token_data.get("expiry"):
        try:
            expiry = datetime.fromisoformat(token_data["expiry"]).replace(tzinfo=None)
        except (ValueError, TypeError):
            pass

    creds = Credentials(
        token=token_data["access_token"],
        refresh_token=token_data["refresh_token"],
        token_uri=token_data["token_uri"],
        client_id=token_data["client_id"],
        client_secret=token_data["client_secret"],
        scopes=SCOPES,
    )
    if expiry:
        creds.expiry = expiry
    return creds


def refresh_credentials(creds):
    """Refresh credentials and persist the new tokens."""
    from google.auth.transport.requests import Request
    creds.refresh(Request())
    save_tokens(
        access_token=creds.token,
        refresh_token=creds.refresh_token,
        expiry=creds.expiry.isoformat() if creds.expiry else "",
        property_url=get_stored_tokens()["property_url"],
        token_uri=creds.token_uri,
        client_id=creds.client_id,
        client_secret=creds.client_secret,
    )
    return creds


def get_authenticated_service():
    """Return an authenticated Search Console API service object."""
    from googleapiclient.discovery import build as build_service

    token_data = get_stored_tokens()
    if not token_data:
        print("No GSC tokens found. Run the auth flow first.")
        print("  python scripts/gsc_sync.py --auth")
        sys.exit(1)

    creds = build_credentials_from_stored(token_data)

    # Auto-refresh if expired
    if creds.expired and creds.refresh_token:
        print("[gsc] Refreshing expired access token...")
        creds = refresh_credentials(creds)

    service = build_service("searchconsole", "v1", credentials=creds)
    return service, token_data["property_url"]


def run_auth_flow():
    """Interactive OAuth 2.0 authorization flow."""
    from google_auth_oauthlib.flow import InstalledAppFlow

    if not CREDENTIALS_PATH.exists():
        print(f"Error: OAuth client credentials file not found at {CREDENTIALS_PATH}")
        print("Download your OAuth 2.0 Client ID JSON from Google Cloud Console")
        print(f"and save it to: {CREDENTIALS_PATH}")
        sys.exit(1)

    flow = InstalledAppFlow.from_client_secrets_file(
        str(CREDENTIALS_PATH), scopes=SCOPES,
        redirect_uri="http://localhost"
    )

    auth_url, _ = flow.authorization_url(prompt="consent", access_type="offline")
    print("\n=== Google Search Console Authorization ===")
    print(f"\n1. Open this URL in your browser:\n\n   {auth_url}\n")
    print("2. After approving, your browser will redirect to http://localhost/?code=...")
    print("   It will show a connection-refused page - that is expected.")
    print("   Copy the 'code' value from the URL bar (between ?code= and &scope).")

    auth_code = input("\n3. Paste the authorization code here: ").strip()
    if not auth_code:
        print("No code provided. Aborting.")
        sys.exit(1)

    flow.fetch_token(code=auth_code)
    creds = flow.credentials

    # List available properties and let user pick
    from googleapiclient.discovery import build as build_service
    service = build_service("searchconsole", "v1", credentials=creds)
    sites = service.sites().list().execute()
    site_list = sites.get("siteEntry", [])

    if not site_list:
        print("No Search Console properties found for this account.")
        sys.exit(1)

    print("\nAvailable properties:")
    for i, site in enumerate(site_list, 1):
        print(f"  {i}. {site['siteUrl']} ({site.get('permissionLevel', 'unknown')})")

    choice = input(f"\nSelect property (1-{len(site_list)}): ").strip()
    try:
        idx = int(choice) - 1
        property_url = site_list[idx]["siteUrl"]
    except (ValueError, IndexError):
        print("Invalid selection. Aborting.")
        sys.exit(1)

    # Read client config for token_uri etc.
    with open(CREDENTIALS_PATH) as f:
        client_config = json.load(f)
    client_info = client_config.get("installed") or client_config.get("web", {})

    save_tokens(
        access_token=creds.token,
        refresh_token=creds.refresh_token,
        expiry=creds.expiry.isoformat() if creds.expiry else "",
        property_url=property_url,
        token_uri=client_info.get("token_uri", "https://oauth2.googleapis.com/token"),
        client_id=client_info.get("client_id", ""),
        client_secret=client_info.get("client_secret", ""),
    )

    print(f"\nAuthorization complete! Connected to: {property_url}")
    print("Tokens saved. You can now run: python scripts/gsc_sync.py")


# ---------------------------------------------------------------------------
# Data pull
# ---------------------------------------------------------------------------

def _api_call_with_backoff(request_fn, max_retries=5):
    """Execute an API call with exponential backoff on rate limit (429) errors."""
    from googleapiclient.errors import HttpError
    for attempt in range(max_retries):
        try:
            return request_fn()
        except HttpError as e:
            if e.resp.status == 429:
                wait = 2 ** (attempt + 1)
                print(f"  Rate limited (429). Waiting {wait}s before retry {attempt+1}/{max_retries}...")
                time.sleep(wait)
            else:
                raise
    raise Exception(f"API call failed after {max_retries} retries due to rate limiting")


def pull_date(service, property_url: str, date_str: str) -> pd.DataFrame:
    """Pull GSC search analytics for a single date. Returns a DataFrame."""
    all_rows = []

    # Pull by-page data
    start_row = 0
    while True:
        request_body = {
            "startDate": date_str,
            "endDate": date_str,
            "dimensions": ["page", "date"],
            "rowLimit": MAX_ROWS_PER_REQUEST,
            "startRow": start_row,
        }
        response = _api_call_with_backoff(
            lambda: service.searchanalytics().query(
                siteUrl=property_url, body=request_body
            ).execute()
        )
        rows = response.get("rows", [])
        if not rows:
            break
        for row in rows:
            all_rows.append({
                "date": row["keys"][1],
                "page": row["keys"][0],
                "query": None,
                "clicks": row.get("clicks", 0),
                "impressions": row.get("impressions", 0),
                "ctr": round(row.get("ctr", 0.0), 4),
                "position": round(row.get("position", 0.0), 2),
            })
        if len(rows) < MAX_ROWS_PER_REQUEST:
            break
        start_row += MAX_ROWS_PER_REQUEST

    # Pull by-query data
    start_row = 0
    while True:
        request_body = {
            "startDate": date_str,
            "endDate": date_str,
            "dimensions": ["query", "page", "date"],
            "rowLimit": MAX_ROWS_PER_REQUEST,
            "startRow": start_row,
        }
        response = _api_call_with_backoff(
            lambda: service.searchanalytics().query(
                siteUrl=property_url, body=request_body
            ).execute()
        )
        rows = response.get("rows", [])
        if not rows:
            break
        for row in rows:
            all_rows.append({
                "date": row["keys"][2],
                "page": row["keys"][1],
                "query": row["keys"][0],
                "clicks": row.get("clicks", 0),
                "impressions": row.get("impressions", 0),
                "ctr": round(row.get("ctr", 0.0), 4),
                "position": round(row.get("position", 0.0), 2),
            })
        if len(rows) < MAX_ROWS_PER_REQUEST:
            break
        start_row += MAX_ROWS_PER_REQUEST

    if not all_rows:
        return pd.DataFrame(columns=["date", "page", "query", "clicks",
                                      "impressions", "ctr", "position"])

    return pd.DataFrame(all_rows)


def save_gsc_parquet(df: pd.DataFrame, date_str: str):
    """Write GSC data to date-partitioned Parquet."""
    out_dir = DATA_GSC / f"date={date_str}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "performance.parquet"
    df.to_parquet(out_path, index=False)
    return out_path


def rebuild_gsc_daily_for(date_str: str) -> bool:
    # The gsc_daily aggregate joins fresh GSC data against parsed logs. If the
    # parsed partition isn't there yet (logs ingested later), skip silently —
    # the next rebuild.py run will pick it up.
    parsed = DATA_PARSED / f"date={date_str}" / "access.parquet"
    if not parsed.exists():
        return False

    import importlib.util
    import duckdb

    spec = importlib.util.spec_from_file_location(
        "ingest", (ROOT / "scripts" / "ingest.py").as_posix()
    )
    ingest = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(ingest)

    conn = duckdb.connect(database=":memory:")
    try:
        conn.execute("PRAGMA threads=4;")
        conn.execute(
            f"CREATE OR REPLACE VIEW parsed AS "
            f"SELECT * FROM read_parquet('{parsed.as_posix()}');"
        )

        def out(name: str) -> Path:
            return DATA_AGG / name / f"date={date_str}" / "part.parquet"

        ingest._build_gsc_daily_aggregate(date_str, conn, out)
    finally:
        conn.close()
    return True


# ---------------------------------------------------------------------------
# Sync logic
# ---------------------------------------------------------------------------

def sync_dates(date_list: list[str], force: bool = False):
    """Sync GSC data for a list of dates."""
    init_gsc_tables()
    service, property_url = get_authenticated_service()

    total_records = 0
    synced = 0
    errors = 0

    for date_str in date_list:
        # Check if already synced (skip unless force)
        if not force:
            state = get_sync_state(date_str)
            if state and state["status"] == "done":
                print(f"  [{date_str}] Already synced ({state['records']} records). Skipping.")
                continue

        started_at = datetime.now(tz=timezone.utc).isoformat()
        print(f"  [{date_str}] Pulling data...", end="", flush=True)

        try:
            df = pull_date(service, property_url, date_str)
            n = len(df)
            if n > 0:
                save_gsc_parquet(df, date_str)
                try:
                    rebuild_gsc_daily_for(date_str)
                except Exception as agg_err:
                    print(f" (gsc_daily rebuild failed: {agg_err})", end="")
            update_sync_state(date_str, "done", n)
            log_sync(date_str, "success", n, started_at=started_at,
                     completed_at=datetime.now(tz=timezone.utc).isoformat())
            total_records += n
            synced += 1
            print(f" {n} records.")
        except Exception as e:
            update_sync_state(date_str, "error", 0)
            log_sync(date_str, "error", 0, str(e), started_at=started_at,
                     completed_at=datetime.now(tz=timezone.utc).isoformat())
            errors += 1
            print(f" ERROR: {e}")

    print(f"\nSync complete: {synced} date(s) synced, {total_records} total records, {errors} error(s).")


def daily_sync():
    """Sync the last 3 days (accounting for GSC data delay)."""
    today = datetime.now(tz=timezone.utc).date()
    # GSC data is typically 2-3 days delayed. Pull days 2-4 ago.
    dates = []
    for offset in range(2, 5):
        d = today - timedelta(days=offset)
        dates.append(d.strftime("%Y-%m-%d"))
    print(f"[gsc] Daily sync: {dates}")
    sync_dates(dates, force=True)


def backfill_sync():
    """Pull all available GSC data (up to ~16 months)."""
    today = datetime.now(tz=timezone.utc).date()
    start = today - timedelta(days=490)  # ~16 months
    end = today - timedelta(days=2)

    dates = []
    cur = start
    while cur <= end:
        dates.append(cur.strftime("%Y-%m-%d"))
        cur += timedelta(days=1)

    print(f"[gsc] Backfill sync: {len(dates)} dates from {dates[0]} to {dates[-1]}")
    sync_dates(dates, force=False)  # don't re-pull already-synced dates


def range_sync(from_date: str, to_date: str):
    """Sync a specific date range."""
    start = datetime.strptime(from_date, "%Y-%m-%d").date()
    end = datetime.strptime(to_date, "%Y-%m-%d").date()

    dates = []
    cur = start
    while cur <= end:
        dates.append(cur.strftime("%Y-%m-%d"))
        cur += timedelta(days=1)

    print(f"[gsc] Range sync: {len(dates)} dates from {from_date} to {to_date}")
    sync_dates(dates, force=True)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Google Search Console data sync")
    parser.add_argument("--auth", action="store_true",
                        help="Run OAuth 2.0 authorization flow")
    parser.add_argument("--backfill", action="store_true",
                        help="Pull all available historical data (~16 months)")
    parser.add_argument("--from", dest="date_from",
                        help="Start date (YYYY-MM-DD)")
    parser.add_argument("--to", dest="date_to",
                        help="End date (YYYY-MM-DD)")
    parser.add_argument("--disconnect", action="store_true",
                        help="Remove stored tokens")
    parser.add_argument("--status", action="store_true",
                        help="Show connection status")

    args = parser.parse_args()

    STATE_DIR.mkdir(parents=True, exist_ok=True)
    DATA_GSC.mkdir(parents=True, exist_ok=True)
    init_gsc_tables()

    if args.auth:
        run_auth_flow()
    elif args.disconnect:
        delete_tokens()
        print("GSC tokens removed. Disconnected.")
    elif args.status:
        token_data = get_stored_tokens()
        if token_data:
            stats = get_gsc_stats()
            print(f"Connected to: {token_data['property_url']}")
            print(f"Dates synced: {stats['dates_synced']}")
            print(f"Total records: {stats['total_records']}")
        else:
            print("Not connected.")
    elif args.backfill:
        backfill_sync()
    elif args.date_from and args.date_to:
        range_sync(args.date_from, args.date_to)
    elif args.date_from or args.date_to:
        print("Both --from and --to are required for range sync.", file=sys.stderr)
        sys.exit(1)
    else:
        daily_sync()


if __name__ == "__main__":
    main()
