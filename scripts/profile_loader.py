#!/usr/bin/env python3
"""
Database-backed site profile management.

Profiles store URL grouping rules, locale whitelists, and section mappings
in SQLite (state/manifest.sqlite) instead of reading from YAML at runtime.
"""

import json
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

ROOT = Path(__file__).resolve().parents[1]
STATE_DIR = ROOT / "state"
MANIFEST_DB = STATE_DIR / "manifest.sqlite"
DETECTORS_DIR = ROOT / "detectors"

_PROFILES_DDL = """\
CREATE TABLE IF NOT EXISTS profiles (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    name             TEXT NOT NULL UNIQUE,
    is_active        INTEGER NOT NULL DEFAULT 0,
    locale_whitelist TEXT NOT NULL DEFAULT '[]',
    bcp47_fallback   INTEGER NOT NULL DEFAULT 1,
    url_group_rules  TEXT NOT NULL DEFAULT '[]',
    section_mappings TEXT NOT NULL DEFAULT '{}',
    settings         TEXT NOT NULL DEFAULT '{}',
    created_at       TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at       TEXT NOT NULL DEFAULT (datetime('now'))
)
"""

_ALLOWED_UPDATE_FIELDS = frozenset(
    {"locale_whitelist", "url_group_rules", "section_mappings", "settings", "bcp47_fallback"}
)


# ---------------------------------------------------------------------------
# Schema bootstrap
# ---------------------------------------------------------------------------

def init_profiles_table() -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(MANIFEST_DB)
    conn.execute(_PROFILES_DDL)
    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _now_iso() -> str:
    return datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _conn() -> sqlite3.Connection:
    return sqlite3.connect(MANIFEST_DB)


def _row_to_url_grouping_config(row: dict):
    """Convert a profile DB row (as dict) into a UrlGroupingConfig."""
    from ingest import UrlGroupingConfig, UrlRule

    locales_raw = json.loads(row["locale_whitelist"])
    locales = set(str(x).strip().lower() for x in locales_raw if str(x).strip())

    rules_raw = json.loads(row["url_group_rules"])
    rules = []
    for r in rules_raw:
        ur = UrlRule(group=r["group"], match=r["match"], value=r["value"])
        if ur.match == "regex":
            ur.compiled = re.compile(str(ur.value), re.IGNORECASE)
        rules.append(ur)

    section_map_raw = json.loads(row["section_mappings"])
    section_map = {k.lower(): v for k, v in section_map_raw.items()}

    settings = json.loads(row.get("settings") or "{}")
    fallback = settings.get("fallback_group", "Other Content")
    locale_homepage_group = settings.get("locale_homepage_group", "Locale Homepage")
    raw_pattern = settings.get("locale_homepage_pattern", "")
    locale_homepage_pattern = re.compile(raw_pattern, re.IGNORECASE) if raw_pattern else None

    return UrlGroupingConfig(
        locales=locales,
        rules=rules,
        section_map=section_map,
        fallback_group=fallback,
        locale_homepage_group=locale_homepage_group,
        locale_homepage_pattern=locale_homepage_pattern,
    )


def _load_url_grouping_from_yaml():
    """Fallback: load from YAML the same way ingest.py used to."""
    import yaml
    from ingest import UrlGroupingConfig, UrlRule

    yaml_path = DETECTORS_DIR / "url_groups.yml"
    with open(yaml_path) as f:
        cfg = yaml.safe_load(f)

    locales = set(str(x).strip().lower() for x in cfg.get("locales", []) if str(x).strip())

    rules = []
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

    return UrlGroupingConfig(
        locales=locales,
        rules=rules,
        section_map=section_map,
        fallback_group=fallback,
        locale_homepage_group=locale_homepage_group,
        locale_homepage_pattern=locale_homepage_pattern,
    )


# ---------------------------------------------------------------------------
# Public API — profile loading
# ---------------------------------------------------------------------------

def get_active_profile():
    """Return the active profile as a UrlGroupingConfig.

    Falls back to loading from YAML if no active profile exists in the
    database (migration safety).
    """
    init_profiles_table()
    conn = _conn()
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("SELECT * FROM profiles WHERE is_active = 1 LIMIT 1")
    row = cur.fetchone()
    conn.close()

    if row is None:
        return _load_url_grouping_from_yaml()

    return _row_to_url_grouping_config(dict(row))


def get_active_profile_raw() -> Optional[Dict[str, Any]]:
    """Return the active profile as a raw dict, or None."""
    init_profiles_table()
    conn = _conn()
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("SELECT * FROM profiles WHERE is_active = 1 LIMIT 1")
    row = cur.fetchone()
    conn.close()
    if row is None:
        return None
    return dict(row)


# ---------------------------------------------------------------------------
# Public API — CRUD
# ---------------------------------------------------------------------------

def list_profiles() -> List[Dict[str, Any]]:
    """Return all profiles (id, name, is_active, created_at, updated_at)."""
    init_profiles_table()
    conn = _conn()
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("SELECT id, name, is_active, created_at, updated_at FROM profiles ORDER BY id")
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


def get_profile_by_id(profile_id: int) -> Optional[Dict[str, Any]]:
    """Return full profile data including JSON fields, or None."""
    conn = _conn()
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("SELECT * FROM profiles WHERE id = ?", (profile_id,))
    row = cur.fetchone()
    conn.close()
    return dict(row) if row else None


def create_profile(
    name: str,
    locale_whitelist: str = "[]",
    bcp47_fallback: int = 1,
    url_group_rules: str = "[]",
    section_mappings: str = "{}",
    settings: str = "{}",
    active: bool = False,
) -> int:
    """Create a new profile. Returns the new profile ID.

    If active=True, deactivates all other profiles first.
    JSON fields should be passed as serialized JSON strings.
    """
    init_profiles_table()
    now = _now_iso()
    conn = _conn()
    try:
        if active:
            conn.execute("UPDATE profiles SET is_active = 0, updated_at = ?", (now,))
        conn.execute(
            """INSERT INTO profiles
               (name, is_active, locale_whitelist, bcp47_fallback,
                url_group_rules, section_mappings, settings, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (name, 1 if active else 0, locale_whitelist, bcp47_fallback,
             url_group_rules, section_mappings, settings, now, now),
        )
        profile_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        conn.commit()
    finally:
        conn.close()
    return profile_id


def clone_profile(source_id: int, new_name: str) -> int:
    """Clone an existing profile under a new name. Returns new profile ID."""
    source = get_profile_by_id(source_id)
    if source is None:
        raise ValueError(f"Source profile id={source_id} not found")
    return create_profile(
        name=new_name,
        locale_whitelist=source["locale_whitelist"],
        bcp47_fallback=source["bcp47_fallback"],
        url_group_rules=source["url_group_rules"],
        section_mappings=source["section_mappings"],
        settings=source["settings"],
        active=False,
    )


def rename_profile(profile_id: int, new_name: str) -> None:
    """Rename a profile."""
    now = _now_iso()
    conn = _conn()
    try:
        cur = conn.execute(
            "UPDATE profiles SET name = ?, updated_at = ? WHERE id = ?",
            (new_name, now, profile_id),
        )
        if cur.rowcount == 0:
            raise ValueError(f"Profile id={profile_id} not found")
        conn.commit()
    finally:
        conn.close()


def delete_profile(profile_id: int) -> None:
    """Delete a profile. Raises ValueError if it is the active profile."""
    conn = _conn()
    try:
        row = conn.execute(
            "SELECT is_active FROM profiles WHERE id = ?", (profile_id,)
        ).fetchone()
        if row is None:
            raise ValueError(f"Profile id={profile_id} not found")
        if row[0]:
            raise ValueError("Cannot delete the active profile")
        conn.execute("DELETE FROM profiles WHERE id = ?", (profile_id,))
        conn.commit()
    finally:
        conn.close()


def set_active_profile(profile_id: int) -> None:
    """Set a profile as active, deactivating all others."""
    now = _now_iso()
    conn = _conn()
    try:
        row = conn.execute("SELECT id FROM profiles WHERE id = ?", (profile_id,)).fetchone()
        if row is None:
            raise ValueError(f"Profile id={profile_id} not found")
        conn.execute("UPDATE profiles SET is_active = 0, updated_at = ?", (now,))
        conn.execute(
            "UPDATE profiles SET is_active = 1, updated_at = ? WHERE id = ?",
            (now, profile_id),
        )
        conn.commit()
    finally:
        conn.close()


def update_profile(profile_id: int, field: str, value: Any) -> None:
    """Update a single field on a profile.

    Allowed fields: locale_whitelist, url_group_rules, section_mappings,
    settings, bcp47_fallback.
    """
    if field not in _ALLOWED_UPDATE_FIELDS:
        raise ValueError(f"Field '{field}' is not updatable. Allowed: {_ALLOWED_UPDATE_FIELDS}")
    now = _now_iso()
    conn = _conn()
    try:
        cur = conn.execute(
            f"UPDATE profiles SET {field} = ?, updated_at = ? WHERE id = ?",
            (value, now, profile_id),
        )
        if cur.rowcount == 0:
            raise ValueError(f"Profile id={profile_id} not found")
        conn.commit()
    finally:
        conn.close()
