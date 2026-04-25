#!/usr/bin/env python3
"""
Per-profile saved /logs view CRUD, persisted in state/manifest.sqlite.

A saved filter snapshots the full /logs query-string state under a name,
scoped to a profile. The captured params are stored as a JSON object
(scalar params as strings, multi-value params like `pf` as lists).
"""

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

ROOT = Path(__file__).resolve().parents[1]
STATE_DIR = ROOT / "state"
MANIFEST_DB = STATE_DIR / "manifest.sqlite"

_DDL = """\
CREATE TABLE IF NOT EXISTS saved_filters (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    profile_id  INTEGER NOT NULL,
    name        TEXT NOT NULL,
    params_json TEXT NOT NULL DEFAULT '{}',
    created_at  TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at  TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(profile_id, name)
)
"""


def _now_iso() -> str:
    return datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _conn() -> sqlite3.Connection:
    return sqlite3.connect(MANIFEST_DB)


def init_table() -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    conn = _conn()
    conn.execute(_DDL)
    conn.commit()
    conn.close()


def list_for_profile(profile_id: int) -> List[Dict[str, Any]]:
    init_table()
    conn = _conn()
    conn.row_factory = sqlite3.Row
    cur = conn.execute(
        "SELECT id, name, params_json, created_at, updated_at "
        "FROM saved_filters WHERE profile_id = ? ORDER BY id DESC",
        (profile_id,),
    )
    rows = []
    for r in cur.fetchall():
        d = dict(r)
        try:
            d["params"] = json.loads(d.pop("params_json") or "{}")
        except json.JSONDecodeError:
            d["params"] = {}
        rows.append(d)
    conn.close()
    return rows


def get_by_id(filter_id: int, profile_id: int) -> Optional[Dict[str, Any]]:
    init_table()
    conn = _conn()
    conn.row_factory = sqlite3.Row
    cur = conn.execute(
        "SELECT id, name, params_json, created_at, updated_at "
        "FROM saved_filters WHERE id = ? AND profile_id = ?",
        (filter_id, profile_id),
    )
    row = cur.fetchone()
    conn.close()
    if row is None:
        return None
    d = dict(row)
    try:
        d["params"] = json.loads(d.pop("params_json") or "{}")
    except json.JSONDecodeError:
        d["params"] = {}
    return d


def create(profile_id: int, name: str, params: Dict[str, Any]) -> int:
    """Create a saved filter. Raises sqlite3.IntegrityError on duplicate name."""
    init_table()
    now = _now_iso()
    payload = json.dumps(params, ensure_ascii=False, sort_keys=True)
    conn = _conn()
    try:
        conn.execute(
            "INSERT INTO saved_filters (profile_id, name, params_json, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (profile_id, name, payload, now, now),
        )
        new_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        conn.commit()
    finally:
        conn.close()
    return new_id


def rename(filter_id: int, profile_id: int, new_name: str) -> bool:
    """Rename a saved filter. Raises sqlite3.IntegrityError on duplicate name."""
    init_table()
    now = _now_iso()
    conn = _conn()
    try:
        cur = conn.execute(
            "UPDATE saved_filters SET name = ?, updated_at = ? "
            "WHERE id = ? AND profile_id = ?",
            (new_name, now, filter_id, profile_id),
        )
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


def delete(filter_id: int, profile_id: int) -> bool:
    init_table()
    conn = _conn()
    try:
        cur = conn.execute(
            "DELETE FROM saved_filters WHERE id = ? AND profile_id = ?",
            (filter_id, profile_id),
        )
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()
