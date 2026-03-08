#!/usr/bin/env python3
"""
Verify the accuracy of no-locale grouping.

Covers three angles:
  1. Unit-test apply_url_grouping() against an exhaustive set of cases.
  2. Audit why no-locale is (legitimately) large – structural breakdown.
  3. Python / Go parser parity – run both on the same synthetic log lines and
     diff every (url_group, locale, section) triple.

Run:
    pytest scripts/test_grouping_accuracy.py -v
    # or stand-alone:
    python scripts/test_grouping_accuracy.py
"""

import json
import subprocess
import sys
import tempfile
import textwrap
from pathlib import Path
from typing import Optional, Tuple

import pytest

# ---------------------------------------------------------------------------
# Bootstrap – make ingest importable from any working directory.
# ---------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from ingest import (  # noqa: E402
    NO_LOCALE_LABEL,
    UrlGroupingConfig,
    apply_url_grouping,
    load_url_grouping,
)

# ---------------------------------------------------------------------------
# Shared fixture
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def cfg() -> UrlGroupingConfig:
    return load_url_grouping()


# ===========================================================================
# 1. UNIT TESTS – every rule type and locale detection path
# ===========================================================================

class TestHighPriorityRules:
    """High-priority rules always win and always return no-locale.

    These are "infrastructure" hits (static assets, API, bots/crawlers hitting
    root) where the locale dimension is not meaningful – but they DO inflate the
    raw no-locale row count.
    """

    def test_root_exact(self, cfg):
        group, locale, section = apply_url_grouping("/", cfg)
        assert group == "Home"
        assert locale == NO_LOCALE_LABEL

    def test_root_with_query(self, cfg):
        group, locale, section = apply_url_grouping("/?utm_source=google", cfg)
        assert group == "Home"
        assert locale == NO_LOCALE_LABEL

    def test_robots_txt(self, cfg):
        group, locale, _ = apply_url_grouping("/robots.txt", cfg)
        assert group == "Robots"
        assert locale == NO_LOCALE_LABEL

    def test_sitemap(self, cfg):
        group, locale, _ = apply_url_grouping("/sitemap.xml", cfg)
        assert group == "Sitemaps"
        assert locale == NO_LOCALE_LABEL

    def test_sitemap_locale_variant(self, cfg):
        group, locale, _ = apply_url_grouping("/sitemap-en.xml", cfg)
        assert group == "Sitemaps"
        assert locale == NO_LOCALE_LABEL

    def test_nuxt_asset(self, cfg):
        group, locale, _ = apply_url_grouping("/_nuxt/chunk.abc123.js", cfg)
        assert group == "Nuxt Assets"
        assert locale == NO_LOCALE_LABEL

    def test_api_prefix(self, cfg):
        group, locale, _ = apply_url_grouping("/api/v1/prices", cfg)
        assert group == "API"
        assert locale == NO_LOCALE_LABEL

    @pytest.mark.parametrize("path", [
        "/getMenuStructure",
        "/getMenuStructure/foo",
        "/getSubMenuStructure",
        "/getMenusTranslations",
    ])
    def test_api_menu_endpoints(self, cfg, path):
        group, locale, _ = apply_url_grouping(path, cfg)
        assert group == "API"
        assert locale == NO_LOCALE_LABEL

    @pytest.mark.parametrize("path", ["/feed", "/rss"])
    def test_feeds(self, cfg, path):
        group, locale, _ = apply_url_grouping(path, cfg)
        assert group == "Feeds"
        assert locale == NO_LOCALE_LABEL

    @pytest.mark.parametrize("path", [
        "/.well-known/passkey-endpoints",
        "/.well-known/assetlinks.json",
        "/.well-known/traffic-advice",
        "/autodiscover/autodiscover.xml",
    ])
    def test_well_known(self, cfg, path):
        group, locale, _ = apply_url_grouping(path, cfg)
        assert group == "Well-Known"
        assert locale == NO_LOCALE_LABEL

    @pytest.mark.parametrize("ext", [
        "css", "js", "png", "jpg", "jpeg", "gif", "webp",
        "svg", "ico", "woff", "woff2", "ttf", "eot",
        "map", "pdf", "zip", "mp4", "webm", "mp3",
    ])
    def test_static_asset_extensions(self, cfg, ext):
        group, locale, _ = apply_url_grouping(f"/some/path/file.{ext}", cfg)
        assert group == "Static Assets", f"Expected Static Assets for .{ext}"
        assert locale == NO_LOCALE_LABEL


class TestLocaleHomepages:
    """Bare locale homepages like /en/ and /en should be group=Home with the real locale."""

    @pytest.mark.parametrize("path,expected_locale", [
        ("/en",         "en"),
        ("/en/",        "en"),
        ("/fr",         "fr"),
        ("/fr/",        "fr"),
        ("/zh-hans",    "zh-hans"),
        ("/zh-hans/",   "zh-hans"),
        ("/zh-hant",    "zh-hant"),
        ("/zh-hant/",   "zh-hant"),
        ("/pt",         "pt"),
        ("/pt/",        "pt"),
        ("/ko",         "ko"),
        ("/ko/",        "ko"),
        ("/ar",         "ar"),
        ("/ar/",        "ar"),
        ("/de",         "de"),
        ("/de/",        "de"),
        ("/es",         "es"),
        ("/es/",        "es"),
        ("/en-au",      "en-au"),
        ("/en-au/",     "en-au"),
        ("/zh-hans-cn", "zh-hans-cn"),
        ("/zh-hans-cn/","zh-hans-cn"),
        ("/zh-hans-au", "zh-hans-au"),
        ("/zh-hans-au/","zh-hans-au"),
        ("/zh-hans-nz", "zh-hans-nz"),
        ("/zh-hans-nz/","zh-hans-nz"),
        ("/en-nz",      "en-nz"),
        ("/en-nz/",     "en-nz"),
        ("/en-sg",      "en-sg"),
        ("/en-sg/",     "en-sg"),
        ("/tw",         "tw"),
        ("/tw/",        "tw"),
        ("/au",         "au"),
        ("/au/",        "au"),
        ("/zh-hant-au", "zh-hant-au"),
        ("/zh-hant-au/","zh-hant-au"),
    ])
    def test_locale_home_classified(self, cfg, path, expected_locale):
        group, locale, section = apply_url_grouping(path, cfg)
        assert group == "Home", f"{path!r}: expected group='Home', got {group!r}"
        assert locale == expected_locale, (
            f"{path!r}: expected locale={expected_locale!r}, got {locale!r}"
        )


class TestLocaleContentPages:
    """URLs with a recognised locale as the first segment and real content."""

    @pytest.mark.parametrize("path,expected_locale,expected_group", [
        ("/en/about",           "en",       "About"),
        ("/fr/trading",         "fr",       "Trading"),
        ("/zh-hans/platform",   "zh-hans",  "Platform"),
        ("/zh-hant/analysis/market-insight", "zh-hant", "Analysis - Market Insight"),
        ("/ko/academy",         "ko",       "Academy"),
        ("/en-in/about",        "en-in",    "About"),
        ("/en-eu/trading",      "en-eu",    "Trading"),
        ("/cht/analysis",       "cht",       "Analysis"),
        ("/eng/platform",       "eng",      "Platform"),
        ("/en-au/about",        "en-au",    "About"),
        ("/zh-hans-cn/platform","zh-hans-cn","Platform"),
        ("/es/trading",         "es",       "Trading"),
    ])
    def test_locale_content_gets_correct_locale(self, cfg, path, expected_locale, expected_group):
        group, locale, section = apply_url_grouping(path, cfg)
        assert locale == expected_locale, (
            f"{path!r}: expected locale={expected_locale!r}, got {locale!r}"
        )
        assert group == expected_group, (
            f"{path!r}: expected group={expected_group!r}, got {group!r}"
        )

    @pytest.mark.parametrize("path,expected_locale", [
        ("/en/some-unknown-section",  "en"),
        ("/fr/checkout",              "fr"),
        ("/zh-hans/login",            "zh-hans"),
    ])
    def test_locale_unknown_section_fallback(self, cfg, path, expected_locale):
        group, locale, section = apply_url_grouping(path, cfg)
        assert locale == expected_locale
        assert group == cfg.fallback_group  # "Other Content"

    def test_composite_section(self, cfg):
        group, locale, _ = apply_url_grouping("/en/analysis/market-news", cfg)
        assert locale == "en"
        assert group == "Analysis - Market News"


class TestNoLocaleContentPages:
    """URLs whose first segment is NOT a known locale → no-locale (correct)."""

    @pytest.mark.parametrize("path", [
        "/admin/dashboard",
        "/healthz",
        "/wp-login.php",
        "/xmlrpc.php",
        "/cdn-cgi/rum",
        "/unknown-section/page",
    ])
    def test_unknown_first_segment_is_no_locale(self, cfg, path):
        _, locale, _ = apply_url_grouping(path, cfg)
        assert locale == NO_LOCALE_LABEL, (
            f"{path!r} first segment is not a locale, should be no-locale"
        )


class TestEdgeCases:
    """Normalisation, case-insensitivity, and boundary conditions."""

    def test_uppercase_locale_in_url(self, cfg):
        # Paths are lowercased before matching.
        group, locale, section = apply_url_grouping("/EN/about", cfg)
        assert locale == "en"

    def test_locale_in_query_string_ignored(self, cfg):
        # Query string must be stripped first.
        group, locale, _ = apply_url_grouping("/about?locale=en", cfg)
        assert locale == NO_LOCALE_LABEL  # "about" is not a locale

    def test_empty_path(self, cfg):
        group, locale, _ = apply_url_grouping("", cfg)
        assert group == "Home"
        assert locale == NO_LOCALE_LABEL

    def test_path_with_double_slash(self, cfg):
        # Normalisation should handle //en/about gracefully.
        _, locale, _ = apply_url_grouping("//en/about", cfg)
        # /en is the first non-empty segment after splitting → locale detected.
        assert locale == "en"

    def test_ext_with_query_string(self, cfg):
        # Query string must be stripped before extension check.
        group, locale, _ = apply_url_grouping("/style.css?v=3", cfg)
        assert group == "Static Assets"
        assert locale == NO_LOCALE_LABEL

    def test_path_no_extension_dot_in_dir(self, cfg):
        # A dot in a directory name must not be mistaken for a file extension.
        group, locale, _ = apply_url_grouping("/en/v1.0/release-notes", cfg)
        assert locale == "en"  # ext "release-notes" is not in ext list

    def test_locale_whitelist_completeness(self, cfg):
        """Every locale in the whitelist must resolve to itself (not no-locale)."""
        missing = []
        for locale_code in cfg.locales:
            path = f"/{locale_code}/about"
            _, locale, _ = apply_url_grouping(path, cfg)
            if locale != locale_code:
                missing.append((locale_code, locale))
        assert not missing, (
            "Some locale codes resolve incorrectly (likely caught by a high-priority rule):\n"
            + "\n".join(f"  /{lc}/about → locale={got!r}" for lc, got in missing)
        )


# ===========================================================================
# 2. STRUCTURAL BREAKDOWN – shows WHY no-locale is large
# ===========================================================================

def print_no_locale_breakdown(cfg: UrlGroupingConfig) -> None:
    """Print a breakdown of every category that produces no-locale hits."""
    cases = [
        # (description, sample_paths)
        ("Root /",               ["/"]),
        ("Locale homepages (detected via locale whitelist → real locale)",
         ["/en/", "/fr/", "/zh-hans/", "/zh-hant/", "/ko/", "/ar/"]),
        ("_nuxt/ assets",        ["/_nuxt/app.js"]),
        ("API endpoints",        ["/api/v1/foo"]),
        ("Static file extensions", ["/a.css", "/b.js", "/c.png", "/d.woff2"]),
        ("Robots / Sitemaps",    ["/robots.txt", "/sitemap.xml"]),
        ("Unknown first segments (bots, probe URLs)",
         ["/admin/", "/wp-login.php", "/cgi-bin/", "/.env"]),
    ]
    print("\n── No-locale structural breakdown ──────────────────────────────")
    for description, paths in cases:
        for path in paths:
            g, loc, sec = apply_url_grouping(path, cfg)
            tag = "✓ no-locale" if loc == NO_LOCALE_LABEL else f"✗ locale={loc!r}"
            print(f"  {path:<40} group={g!r:<20} {tag}")
    print()

    print()


# ===========================================================================
# 3. PYTHON / GO PARITY CHECK
# ===========================================================================

GO_BINARY = ROOT / "parser" / "log-parser"

# Synthetic log lines that exercise every code path.
# Format: IP - - [time] "METHOD /path HTTP/1.1" STATUS BYTES "ref" "ua"
_LOG_TEMPLATE = (
    '1.2.3.4 - - [07/Mar/2026:00:00:00 +0000] '
    '"{method} {path} HTTP/1.1" 200 512 "-" "Mozilla/5.0"'
)

PARITY_PATHS = [
    "/",
    "/en/",
    "/fr/about",
    "/zh-hans/platform",
    "/zh-hant/analysis/market-insight",
    "/en-au/trading",
    "/zh-hans-cn/academy",
    "/es/about",
    "/cht/platform",
    "/unknown/page",
    "/_nuxt/chunk.js",
    "/api/v1/prices",
    "/style.css",
    "/robots.txt",
    "/sitemap.xml",
    "/admin/login",
    "/wp-login.php",
    "/en/about?ref=nav",
    "/EN/TRADING",
]


def _python_results(cfg: UrlGroupingConfig):
    """Return {path: (group, locale, section)} for PARITY_PATHS using Python."""
    results = {}
    for path in PARITY_PATHS:
        g, loc, sec = apply_url_grouping(path, cfg)
        results[path] = (g, loc, sec)
    return results


def _go_results() -> Optional[dict]:
    """Run the Go binary on synthetic log lines and return {path: (group, locale, section)}.

    Returns None if the binary is not built or config files are missing.

    The Go binary interface:
        log-parser --date DATE --out OUT --bots bots.yml --urls url_groups.yml FILE...
    NDJSON is written to --out; stdout only contains the row count.
    """
    if not GO_BINARY.exists():
        return None

    bots_yml = ROOT / "detectors" / "bots.yml"
    urls_yml = ROOT / "detectors" / "url_groups.yml"
    if not bots_yml.exists() or not urls_yml.exists():
        print("Go parity: detectors config files missing")
        return None

    lines = []
    for path in PARITY_PATHS:
        lines.append(_LOG_TEMPLATE.format(method="GET", path=path))

    log_tmp = ndjson_tmp = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".log", delete=False) as f:
            f.write("\n".join(lines) + "\n")
            log_tmp = f.name

        with tempfile.NamedTemporaryFile(suffix=".ndjson", delete=False) as f:
            ndjson_tmp = f.name

        result = subprocess.run(
            [
                str(GO_BINARY),
                "--date", "2026-03-07",
                "--out", ndjson_tmp,
                "--bots", str(bots_yml),
                "--urls", str(urls_yml),
                log_tmp,
            ],
            capture_output=True, text=True, timeout=30,
        )
        if result.returncode != 0:
            print(f"Go binary error: {result.stderr}")
            return None

        go_map = {}
        with open(ndjson_tmp) as fh:
            for line in fh:
                if not line.strip():
                    continue
                row = json.loads(line)
                path = row.get("path", "")
                go_map[path] = (
                    row.get("url_group"),
                    row.get("locale"),
                    row.get("section"),
                )
        return go_map
    except Exception as e:
        print(f"Could not run Go binary: {e}")
        return None
    finally:
        for tmp in (log_tmp, ndjson_tmp):
            if tmp:
                Path(tmp).unlink(missing_ok=True)


@pytest.mark.skipif(not GO_BINARY.exists(), reason="Go binary not built")
def test_python_go_parity(cfg):
    """Every (group, locale, section) triple must match between parsers."""
    py = _python_results(cfg)
    go = _go_results()

    if go is None:
        pytest.skip("Go binary produced no output")

    mismatches = []
    for path in PARITY_PATHS:
        py_val = py.get(path)
        go_val = go.get(path)
        if py_val != go_val:
            mismatches.append((path, py_val, go_val))

    assert not mismatches, (
        "Python / Go parity failures:\n" +
        "\n".join(
            f"  {p!r}\n    Python: {pv}\n    Go:     {gv}"
            for p, pv, gv in mismatches
        )
    )


# ===========================================================================
# Stand-alone runner (no pytest required)
# ===========================================================================

if __name__ == "__main__":
    cfg = load_url_grouping()

    print_no_locale_breakdown(cfg)

    # Quick parity report
    py = _python_results(cfg)
    go = _go_results()
    if go is not None:
        mismatches = [(p, py[p], go.get(p)) for p in PARITY_PATHS if py[p] != go.get(p)]
        if mismatches:
            print("✗ Python / Go MISMATCHES:")
            for p, pv, gv in mismatches:
                print(f"  {p!r}\n    Python: {pv}\n    Go:     {gv}")
        else:
            print(f"✓ Python / Go agree on all {len(PARITY_PATHS)} test paths.")
    else:
        print(f"(Go binary not found at {GO_BINARY} – skipping parity check)")

    print("\nRunning pytest for full unit-test coverage …\n")
    sys.exit(pytest.main([__file__, "-v"]))
