# Log analyser

Local FastAPI dashboard over parsed access logs (DuckDB on parquet partitions).

## Run

- `uvicorn app.server:app --reload` — dev server on `:8000`. Usually already running.
- `python3` (not `python`) — system has no `python` shim.
- After ingesting, hit `POST /internal/cache/clear` to drop the report TTL cache.

## Layout

- `app/server.py` — single ~7000-line FastAPI module. Renders HTML as Python f-strings inline (no template engine). Most edits land here.
- `app/settings.py` — `/settings/*` sub-router; mounted from server.py. Imports first and inserts `scripts/` onto `sys.path`, so server.py can `from profile_loader import …` without its own path setup.
- `app/static/logviewer.{js,css}` — single-IIFE JS island + CSS for `/logs`. Vanilla JS (no framework), uses htmx for navigation.
- `scripts/` — CLI entrypoints + library modules importable from `app/` once settings.py loads. `profile_loader.py` and `saved_filters.py` own the per-profile sqlite layer.
- `state/manifest.sqlite` — single sqlite holding `profiles`, `saved_filters`, `gsc_*`, `ingested_files`. **All per-user/per-profile config goes here**, not in JSON files.
- `data/raw/` (input logs), `data/parsed/` (parquet partitions queried by DuckDB), `data/aggregates/`, `data/gsc/`.
- `parser/` — Go binary (`log-parser`) that produces `data/parsed/`.
- `detectors/` — yaml configs (url groups, bots, etc.).

## /logs conventions

- URL state IS the source of truth. Filters/columns/sort/path-rules are query params.
- Multi-value params (e.g. `pf`) emit repeated `k=v` pairs — handled by `_lv2_build_qs`.
- Active profile lookup: `from profile_loader import get_active_profile_raw` → dict with `id`.
- htmx swaps target `#results`. The **filter bar (and saved-views, path-filter rows) live OUTSIDE `#results`** — they don't repaint on swap. New chrome must follow this convention or it'll stop reflecting state after navigation.
- Route handler in `app/server.py` is `@app.get("/logs")` → `def log_viewer(...)`. Filter bar built by `_lv2_filter_bar`; saved views via `_render_saved_views_box`; path-filter inline in the same function.
- HX-Request branch returns just `results_inner`; full GETs return `chrome_html + #results`.

## Style

- Terse code, no docstrings beyond a one-liner where context isn't obvious. No multi-paragraph comments. No "WHAT the code does" comments — only WHY when non-obvious.
- No backwards-compat shims, dead-code comments, or unused `_var` renames. Delete if unused.
- No emoji in code or commits unless explicitly requested.
- Commits: focus on the why; format like recent `git log` (capitalised verb summary, body explains rationale + non-obvious behaviour).
- The user wants commits going **straight to `main`** in this session — no feature branches.
