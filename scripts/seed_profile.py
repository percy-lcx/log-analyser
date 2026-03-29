#!/usr/bin/env python3
"""
Seed a site profile from detectors/url_groups.yml into the SQLite database.

Creates a "Default" profile (or custom --name) with is_active=1.
Idempotent: skips if the profile already exists unless --force is given.

Usage:
    python scripts/seed_profile.py [--name NAME] [--force]
"""

import argparse
import json
import sqlite3
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from profile_loader import (
    MANIFEST_DB,
    create_profile,
    delete_profile,
    init_profiles_table,
    list_profiles,
    set_active_profile,
)


def load_yaml(path: Path) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


def seed(name: str = "Default", force: bool = False, yaml_path: Path | None = None) -> None:
    yaml_path = yaml_path or (ROOT / "detectors" / "url_groups.yml")
    cfg = load_yaml(yaml_path)

    # Serialize locale whitelist
    locales_raw = cfg.get("locales", [])
    locale_whitelist = json.dumps([str(x).strip() for x in locales_raw if str(x).strip()])

    # Serialize URL group rules (preserve order = priority)
    rules_raw = cfg.get("rules", [])
    url_group_rules = json.dumps([
        {"group": r["group"], "match": r["match"], "value": r["value"]}
        for r in rules_raw
    ])

    # Serialize section mappings
    section_mappings = json.dumps(cfg.get("section_map") or {})

    # Serialize extra settings
    settings = json.dumps({
        "fallback_group": cfg.get("fallback_group", "Other Content"),
        "locale_homepage_group": cfg.get("locale_homepage_group", "Locale Homepage"),
        "locale_homepage_pattern": cfg.get("locale_homepage_pattern", ""),
    })

    init_profiles_table()

    # Check for existing profile with this name
    existing = [p for p in list_profiles() if p["name"] == name]
    if existing:
        if not force:
            print(f"Profile '{name}' already exists (id={existing[0]['id']}). Use --force to overwrite.")
            return
        # Force: delete existing (deactivate first if active)
        pid = existing[0]["id"]
        if existing[0]["is_active"]:
            # Temporarily allow deletion by deactivating
            conn = sqlite3.connect(MANIFEST_DB)
            conn.execute("UPDATE profiles SET is_active = 0 WHERE id = ?", (pid,))
            conn.commit()
            conn.close()
        delete_profile(pid)
        print(f"Deleted existing profile '{name}' (id={pid}).")

    profile_id = create_profile(
        name=name,
        locale_whitelist=locale_whitelist,
        bcp47_fallback=1,
        url_group_rules=url_group_rules,
        section_mappings=section_mappings,
        settings=settings,
        active=True,
    )
    print(f"Created profile '{name}' (id={profile_id}) with is_active=1.")
    print(f"  Locales: {len(json.loads(locale_whitelist))}")
    print(f"  URL rules: {len(json.loads(url_group_rules))}")
    print(f"  Section mappings: {len(json.loads(section_mappings))}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Seed a site profile from url_groups.yml")
    parser.add_argument("--name", default="Default", help="Profile name (default: Default)")
    parser.add_argument("--force", action="store_true", help="Overwrite if profile already exists")
    parser.add_argument("--yaml", type=Path, default=None, help="Path to url_groups.yml (default: detectors/url_groups.yml)")
    args = parser.parse_args()
    seed(name=args.name, force=args.force, yaml_path=args.yaml)


if __name__ == "__main__":
    main()
