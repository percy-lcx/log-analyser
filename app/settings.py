"""
Settings UI — FastAPI router for profile management, URL group editing,
locale/section configuration, and log file management.
"""
from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse

ROOT = Path(__file__).resolve().parents[1]
DATA_RAW = ROOT / "data" / "raw"
STATE_DIR = ROOT / "state"
MANIFEST_DB = STATE_DIR / "manifest.sqlite"

# Lazy import so we don't create circular deps at module level
sys.path.insert(0, str(ROOT / "scripts"))

router = APIRouter()

# ---------------------------------------------------------------------------
# In-memory task tracking for async ingest / rebuild jobs
# ---------------------------------------------------------------------------
_tasks: Dict[str, Dict[str, Any]] = {}


def _new_task(kind: str) -> str:
    tid = uuid.uuid4().hex[:12]
    _tasks[tid] = {"kind": kind, "status": "running", "detail": "", "pid": None}
    return tid


# ---------------------------------------------------------------------------
# Profile API endpoints
# ---------------------------------------------------------------------------

@router.get("/api/settings/profiles")
def api_list_profiles():
    from profile_loader import list_profiles
    return list_profiles()


@router.post("/api/settings/profiles")
async def api_create_profile(request: Request):
    body = await request.json()
    name = body.get("name", "").strip()
    if not name:
        return JSONResponse({"error": "Name is required"}, status_code=400)
    clone_from = body.get("clone_from")
    try:
        from profile_loader import create_profile, clone_profile
        if clone_from:
            pid = clone_profile(int(clone_from), name)
        else:
            pid = create_profile(name=name)
        return {"id": pid, "name": name}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=400)


@router.put("/api/settings/profiles/{profile_id}")
async def api_rename_profile(profile_id: int, request: Request):
    body = await request.json()
    new_name = body.get("name", "").strip()
    if not new_name:
        return JSONResponse({"error": "Name is required"}, status_code=400)
    try:
        from profile_loader import rename_profile
        rename_profile(profile_id, new_name)
        return {"ok": True}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=400)


@router.delete("/api/settings/profiles/{profile_id}")
def api_delete_profile(profile_id: int):
    try:
        from profile_loader import delete_profile
        delete_profile(profile_id)
        return {"ok": True}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=400)


@router.post("/api/settings/profiles/{profile_id}/activate")
def api_activate_profile(profile_id: int):
    try:
        from profile_loader import set_active_profile
        set_active_profile(profile_id)
        return {"ok": True}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=400)


# ---------------------------------------------------------------------------
# URL Group Rule endpoints
# ---------------------------------------------------------------------------

@router.get("/api/settings/url-groups")
def api_get_url_groups():
    from profile_loader import get_active_profile_raw
    p = get_active_profile_raw()
    if p is None:
        return {"rules": [], "profile_id": None}
    return {"rules": json.loads(p["url_group_rules"]), "profile_id": p["id"]}


@router.put("/api/settings/url-groups")
async def api_save_url_groups(request: Request):
    body = await request.json()
    rules = body.get("rules", [])
    from profile_loader import get_active_profile_raw, update_profile
    p = get_active_profile_raw()
    if p is None:
        return JSONResponse({"error": "No active profile"}, status_code=400)
    update_profile(p["id"], "url_group_rules", json.dumps(rules))
    return {"ok": True}


@router.post("/api/settings/url-groups/preview")
async def api_preview_url_group(request: Request):
    body = await request.json()
    sample_path = body.get("path", "/")
    from profile_loader import get_active_profile
    from ingest import apply_url_grouping
    cfg = get_active_profile()
    url_group, locale, section = apply_url_grouping(sample_path, cfg)
    return {"path": sample_path, "url_group": url_group, "locale": locale, "section": section}


# ---------------------------------------------------------------------------
# Locale endpoints
# ---------------------------------------------------------------------------

@router.get("/api/settings/locales")
def api_get_locales():
    from profile_loader import get_active_profile_raw
    p = get_active_profile_raw()
    if p is None:
        return {"locales": [], "bcp47_fallback": True, "profile_id": None}
    return {
        "locales": json.loads(p["locale_whitelist"]),
        "bcp47_fallback": bool(p["bcp47_fallback"]),
        "profile_id": p["id"],
    }


@router.put("/api/settings/locales")
async def api_save_locales(request: Request):
    body = await request.json()
    locales = body.get("locales", [])
    bcp47 = body.get("bcp47_fallback", True)
    from profile_loader import get_active_profile_raw, update_profile
    p = get_active_profile_raw()
    if p is None:
        return JSONResponse({"error": "No active profile"}, status_code=400)
    update_profile(p["id"], "locale_whitelist", json.dumps(locales))
    update_profile(p["id"], "bcp47_fallback", 1 if bcp47 else 0)
    return {"ok": True}


# ---------------------------------------------------------------------------
# Section endpoints
# ---------------------------------------------------------------------------

@router.get("/api/settings/sections")
def api_get_sections():
    from profile_loader import get_active_profile_raw
    p = get_active_profile_raw()
    if p is None:
        return {"sections": {}, "profile_id": None}
    return {"sections": json.loads(p["section_mappings"]), "profile_id": p["id"]}


@router.put("/api/settings/sections")
async def api_save_sections(request: Request):
    body = await request.json()
    sections = body.get("sections", {})
    from profile_loader import get_active_profile_raw, update_profile
    p = get_active_profile_raw()
    if p is None:
        return JSONResponse({"error": "No active profile"}, status_code=400)
    update_profile(p["id"], "section_mappings", json.dumps(sections))
    return {"ok": True}


# ---------------------------------------------------------------------------
# Log management endpoints
# ---------------------------------------------------------------------------

def _log_file_status() -> List[Dict[str, Any]]:
    """List files in data/raw/ with ingestion status from manifest."""
    import sqlite3
    DATA_RAW.mkdir(parents=True, exist_ok=True)
    files = sorted(DATA_RAW.iterdir()) if DATA_RAW.exists() else []
    conn = sqlite3.connect(MANIFEST_DB)
    cur = conn.cursor()
    result = []
    for f in files:
        if not f.is_file():
            continue
        st = f.stat()
        cur.execute(
            "SELECT size_bytes, mtime_epoch FROM ingested_files WHERE path = ?",
            (str(f),),
        )
        row = cur.fetchone()
        if row and row[0] == int(st.st_size) and row[1] == int(st.st_mtime):
            status = "ingested"
        elif row:
            status = "changed"
        else:
            status = "pending"
        result.append({
            "name": f.name,
            "path": str(f),
            "size": int(st.st_size),
            "mtime": datetime.fromtimestamp(st.st_mtime, tz=timezone.utc).isoformat(),
            "status": status,
        })
    conn.close()
    return result


@router.get("/api/settings/logs")
def api_list_logs():
    return _log_file_status()


@router.post("/api/settings/ingest")
async def api_trigger_ingest(request: Request):
    body = await request.json() if (await request.body()) else {}
    file_list = body.get("file_list", [])
    tid = _new_task("ingest")
    cmd = [sys.executable, str(ROOT / "scripts" / "ingest.py")]
    if file_list:
        cmd += file_list
    try:
        proc = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
            cwd=str(ROOT),
        )
        _tasks[tid]["pid"] = proc.pid
        import threading

        def _watch(t_id, p):
            out = p.communicate()[0] or ""
            _tasks[t_id]["detail"] = out
            _tasks[t_id]["status"] = "done" if p.returncode == 0 else "error"

        threading.Thread(target=_watch, args=(tid, proc), daemon=True).start()
    except Exception as e:
        _tasks[tid]["status"] = "error"
        _tasks[tid]["detail"] = str(e)
    return {"task_id": tid}


@router.get("/api/settings/ingest/{task_id}/status")
def api_ingest_status(task_id: str):
    t = _tasks.get(task_id)
    if not t:
        return JSONResponse({"error": "Unknown task"}, status_code=404)
    return {"task_id": task_id, "status": t["status"], "detail": t["detail"]}


@router.post("/api/settings/rebuild")
async def api_trigger_rebuild(request: Request):
    body = await request.json() if (await request.body()) else {}
    from_date = body.get("from_date", "")
    to_date = body.get("to_date", "")
    tid = _new_task("rebuild")
    cmd = [sys.executable, str(ROOT / "scripts" / "rebuild.py")]
    if from_date:
        cmd += ["--from", from_date]
    if to_date:
        cmd += ["--to", to_date]
    try:
        proc = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
            cwd=str(ROOT),
        )
        _tasks[tid]["pid"] = proc.pid
        import threading

        def _watch(t_id, p):
            out = p.communicate()[0] or ""
            _tasks[t_id]["detail"] = out
            _tasks[t_id]["status"] = "done" if p.returncode == 0 else "error"

        threading.Thread(target=_watch, args=(tid, proc), daemon=True).start()
    except Exception as e:
        _tasks[tid]["status"] = "error"
        _tasks[tid]["detail"] = str(e)
    return {"task_id": tid}


@router.get("/api/settings/rebuild/{task_id}/status")
def api_rebuild_status(task_id: str):
    t = _tasks.get(task_id)
    if not t:
        return JSONResponse({"error": "Unknown task"}, status_code=404)
    return {"task_id": task_id, "status": t["status"], "detail": t["detail"]}


# ===================================================================
# HTML Pages — reuse the page() shell from server.py via import
# ===================================================================

def _settings_page(title: str, body: str) -> HTMLResponse:
    """Wrapper that imports page() from the main server module."""
    from app.server import page
    return page(title, body)


def _fmt_bytes(n) -> str:
    try:
        n = float(n)
    except (TypeError, ValueError):
        return ""
    for unit in ("B", "KB", "MB", "GB"):
        if abs(n) < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


# ---------------------------------------------------------------------------
# /settings — hub page
# ---------------------------------------------------------------------------

@router.get("/settings", response_class=HTMLResponse)
def settings_hub():
    cards = [
        ("Profiles", "/settings/profiles", "Create, clone, rename and activate site profiles"),
        ("URL Groups", "/settings/url-groups", "Edit URL grouping rules for the active profile"),
        ("Locales", "/settings/locales", "Manage locale whitelist and BCP 47 fallback"),
        ("Sections", "/settings/sections", "Define section mappings for content classification"),
        ("Log Files", "/settings/logs", "Browse log files, trigger ingestion and rebuild"),
    ]
    html = "<div class='report-grid'>"
    for name, url, desc in cards:
        html += f"<a href='{url}' class='report-card'>{name}<small>{desc}</small></a>"
    html += "</div>"
    return _settings_page("Settings", html)


# ---------------------------------------------------------------------------
# /settings/profiles
# ---------------------------------------------------------------------------

@router.get("/settings/profiles", response_class=HTMLResponse)
def settings_profiles():
    from profile_loader import list_profiles
    profiles = list_profiles()

    rows = ""
    for p in profiles:
        active = p["is_active"]
        badge = "<span style='color:#16a34a;font-weight:600;'>Active</span>" if active else ""
        disable_del = "disabled title='Cannot delete active profile'" if active else ""
        disable_act = "disabled" if active else ""
        rows += f"""<tr data-id='{p["id"]}'>
            <td>{p["id"]}</td>
            <td class='profile-name'>{p["name"]}</td>
            <td>{badge}</td>
            <td>{p["created_at"]}</td>
            <td>{p["updated_at"]}</td>
            <td style='white-space:nowrap;'>
                <button class='btn-sm btn-blue' onclick='activateProfile({p["id"]})' {disable_act}>Activate</button>
                <button class='btn-sm' onclick='cloneProfile({p["id"]}, "{p["name"]}")'>Clone</button>
                <button class='btn-sm' onclick='renameProfile({p["id"]}, "{p["name"]}")'>Rename</button>
                <button class='btn-sm btn-red' onclick='deleteProfile({p["id"]})' {disable_del}>Delete</button>
            </td>
        </tr>"""

    body = f"""
    <p style='margin-bottom:12px;'><a href='/settings' style='color:#3b82f6;text-decoration:none;font-size:13px;'>&larr; Back to Settings</a></p>

    <div class='card' style='margin-bottom:16px;'>
        <div style='display:flex;gap:10px;align-items:center;flex-wrap:wrap;'>
            <input id='newName' placeholder='New profile name' style='padding:7px 10px;border:1px solid #cbd5e1;border-radius:6px;font-size:13px;min-width:180px;'>
            <button class='btn-sm btn-blue' onclick='createProfile()'>Create Blank</button>
            <span style='color:#94a3b8;font-size:12px;'>or clone from an existing profile using Clone button below</span>
        </div>
    </div>

    <div class='card'>
        <div class='table-wrapper'>
            <table class='sortable' id='profilesTable'>
                <thead><tr>
                    <th>ID</th><th>Name</th><th>Status</th><th>Created</th><th>Updated</th><th>Actions</th>
                </tr></thead>
                <tbody>{rows}</tbody>
            </table>
        </div>
    </div>

    <style>
    .btn-sm {{ padding:5px 12px; border:1px solid #cbd5e1; border-radius:5px; font-size:12px; cursor:pointer; background:#fff; color:#374151; }}
    .btn-sm:hover {{ background:#f1f5f9; }}
    .btn-sm:disabled {{ opacity:0.4; cursor:not-allowed; }}
    .btn-blue {{ background:#3b82f6; color:#fff; border-color:#3b82f6; }}
    .btn-blue:hover {{ background:#2563eb; }}
    .btn-red {{ color:#dc2626; border-color:#fca5a5; }}
    .btn-red:hover {{ background:#fef2f2; }}
    </style>

    <script>
    async function apiCall(url, opts={{}}) {{
        const res = await fetch(url, {{
            headers: {{'Content-Type': 'application/json'}},
            ...opts
        }});
        return res.json();
    }}
    function reload() {{ location.reload(); }}

    async function createProfile() {{
        const name = document.getElementById('newName').value.trim();
        if (!name) return alert('Enter a profile name');
        await apiCall('/api/settings/profiles', {{method:'POST', body:JSON.stringify({{name}})}});
        reload();
    }}
    async function cloneProfile(id, currentName) {{
        const name = prompt('Name for the cloned profile:', currentName + ' (copy)');
        if (!name) return;
        await apiCall('/api/settings/profiles', {{method:'POST', body:JSON.stringify({{name, clone_from:id}})}});
        reload();
    }}
    async function renameProfile(id, currentName) {{
        const name = prompt('New name:', currentName);
        if (!name || name === currentName) return;
        await apiCall('/api/settings/profiles/' + id, {{method:'PUT', body:JSON.stringify({{name}})}});
        reload();
    }}
    async function deleteProfile(id) {{
        if (!confirm('Delete this profile?')) return;
        const r = await apiCall('/api/settings/profiles/' + id, {{method:'DELETE'}});
        if (r.error) return alert(r.error);
        reload();
    }}
    async function activateProfile(id) {{
        await apiCall('/api/settings/profiles/' + id + '/activate', {{method:'POST'}});
        reload();
    }}
    </script>
    """
    return _settings_page("Site Profiles", body)


# ---------------------------------------------------------------------------
# /settings/url-groups — URL group rule editor with drag-reorder + live preview
# ---------------------------------------------------------------------------

@router.get("/settings/url-groups", response_class=HTMLResponse)
def settings_url_groups():
    from profile_loader import get_active_profile_raw
    p = get_active_profile_raw()
    rules_json = json.dumps(json.loads(p["url_group_rules"])) if p else "[]"
    profile_name = p["name"] if p else "(none)"

    body = f"""
    <p style='margin-bottom:12px;'><a href='/settings' style='color:#3b82f6;text-decoration:none;font-size:13px;'>&larr; Back to Settings</a></p>

    <div id='warningBanner' class='card' style='display:none;background:#fffbeb;border-color:#fbbf24;margin-bottom:14px;'>
        <span style='color:#92400e;font-size:13px;font-weight:600;'>Rules changed — rebuild aggregates to apply.</span>
    </div>

    <p style='font-size:13px;color:#64748b;margin-bottom:14px;'>Active profile: <strong>{profile_name}</strong></p>

    <div class='card' style='margin-bottom:16px;'>
        <h2 style='margin-bottom:12px;'>Live Preview</h2>
        <div style='display:flex;gap:10px;align-items:center;flex-wrap:wrap;'>
            <input id='previewPath' placeholder='Enter a sample path, e.g. /en/trading/forex' style='padding:7px 10px;border:1px solid #cbd5e1;border-radius:6px;font-size:13px;flex:1;min-width:250px;'>
            <div id='previewResult' style='font-size:13px;color:#374151;'></div>
        </div>
    </div>

    <div class='card'>
        <div style='display:flex;justify-content:space-between;align-items:center;margin-bottom:12px;'>
            <h2 style='margin:0;'>URL Group Rules</h2>
            <div style='display:flex;gap:8px;'>
                <button class='btn-sm' onclick='addRule()'>+ Add Rule</button>
                <button class='btn-sm btn-blue' onclick='saveRules()'>Save</button>
            </div>
        </div>
        <div class='table-wrapper' style='max-height:none;'>
            <table id='rulesTable' style='width:100%;border-collapse:collapse;font-size:13px;'>
                <thead><tr style='background:#f8fafc;'>
                    <th style='padding:8px 6px;width:30px;'></th>
                    <th style='padding:8px 6px;'>Label</th>
                    <th style='padding:8px 6px;width:120px;'>Match Type</th>
                    <th style='padding:8px 6px;'>Pattern / Value</th>
                    <th style='padding:8px 6px;width:50px;'></th>
                </tr></thead>
                <tbody id='rulesBody'></tbody>
            </table>
        </div>
    </div>

    <style>
    .btn-sm {{ padding:5px 12px; border:1px solid #cbd5e1; border-radius:5px; font-size:12px; cursor:pointer; background:#fff; color:#374151; }}
    .btn-sm:hover {{ background:#f1f5f9; }}
    .btn-blue {{ background:#3b82f6; color:#fff; border-color:#3b82f6; }}
    .btn-blue:hover {{ background:#2563eb; }}
    #rulesTable td {{ padding:6px; border-bottom:1px solid #f1f5f9; vertical-align:middle; }}
    #rulesTable input, #rulesTable select {{
        padding:5px 8px; border:1px solid #cbd5e1; border-radius:4px; font-size:13px; width:100%;
    }}
    .drag-handle {{ cursor:grab; color:#94a3b8; font-size:16px; text-align:center; user-select:none; }}
    .drag-handle:active {{ cursor:grabbing; }}
    tr.dragging {{ opacity:0.4; background:#eff6ff; }}
    tr.drag-over {{ border-top:2px solid #3b82f6; }}
    .del-btn {{ background:none; border:none; color:#dc2626; cursor:pointer; font-size:16px; padding:2px 6px; }}
    .del-btn:hover {{ background:#fef2f2; border-radius:4px; }}
    </style>

    <script>
    let rules = {rules_json};
    let dirty = false;

    function renderRules() {{
        const tbody = document.getElementById('rulesBody');
        tbody.innerHTML = '';
        rules.forEach((r, i) => {{
            const tr = document.createElement('tr');
            tr.draggable = true;
            tr.dataset.idx = i;
            const val = Array.isArray(r.value) ? r.value.join(', ') : (r.value || '');
            tr.innerHTML = `
                <td class='drag-handle' title='Drag to reorder'>&#9776;</td>
                <td><input value="${{escHtml(r.group)}}" onchange="updateRule(${{i}},'group',this.value)"></td>
                <td><select onchange="updateRule(${{i}},'match',this.value)">
                    <option value='exact' ${{r.match==='exact'?'selected':''}}>exact</option>
                    <option value='prefix' ${{r.match==='prefix'?'selected':''}}>prefix</option>
                    <option value='regex' ${{r.match==='regex'?'selected':''}}>regex</option>
                    <option value='ext' ${{r.match==='ext'?'selected':''}}>extension</option>
                </select></td>
                <td><input value="${{escHtml(val)}}" onchange="updateRule(${{i}},'value',this.value)" placeholder="${{r.match==='ext'?'css, js, png':'pattern'}}"></td>
                <td><button class='del-btn' onclick='removeRule(${{i}})' title='Delete'>&times;</button></td>
            `;
            // Drag events
            tr.addEventListener('dragstart', e => {{ e.dataTransfer.setData('text/plain', i); tr.classList.add('dragging'); }});
            tr.addEventListener('dragend', () => {{ tr.classList.remove('dragging'); document.querySelectorAll('.drag-over').forEach(el=>el.classList.remove('drag-over')); }});
            tr.addEventListener('dragover', e => {{ e.preventDefault(); tr.classList.add('drag-over'); }});
            tr.addEventListener('dragleave', () => tr.classList.remove('drag-over'));
            tr.addEventListener('drop', e => {{
                e.preventDefault();
                tr.classList.remove('drag-over');
                const from = parseInt(e.dataTransfer.getData('text/plain'));
                const to = parseInt(tr.dataset.idx);
                if (from !== to) {{
                    const item = rules.splice(from, 1)[0];
                    rules.splice(to, 0, item);
                    markDirty();
                    renderRules();
                }}
            }});
            tbody.appendChild(tr);
        }});
    }}

    function escHtml(s) {{ const d = document.createElement('div'); d.textContent = s; return d.innerHTML.replace(/"/g, '&quot;'); }}

    function updateRule(i, field, val) {{
        if (field === 'value' && rules[i].match === 'ext') {{
            rules[i].value = val.split(',').map(s => s.trim()).filter(Boolean);
        }} else {{
            rules[i][field] = val;
        }}
        markDirty();
    }}

    function addRule() {{
        rules.push({{group: '', match: 'prefix', value: ''}});
        markDirty();
        renderRules();
    }}

    function removeRule(i) {{
        rules.splice(i, 1);
        markDirty();
        renderRules();
    }}

    function markDirty() {{
        dirty = true;
        document.getElementById('warningBanner').style.display = 'block';
    }}

    async function saveRules() {{
        const res = await fetch('/api/settings/url-groups', {{
            method: 'PUT',
            headers: {{'Content-Type': 'application/json'}},
            body: JSON.stringify({{rules}})
        }});
        const data = await res.json();
        if (data.ok) {{
            alert('Rules saved.');
        }} else {{
            alert('Error: ' + (data.error || 'Unknown'));
        }}
    }}

    // Live preview (debounced)
    let previewTimer;
    document.getElementById('previewPath').addEventListener('input', function() {{
        clearTimeout(previewTimer);
        const path = this.value;
        if (!path) {{ document.getElementById('previewResult').textContent = ''; return; }}
        previewTimer = setTimeout(async () => {{
            const res = await fetch('/api/settings/url-groups/preview', {{
                method: 'POST',
                headers: {{'Content-Type': 'application/json'}},
                body: JSON.stringify({{path}})
            }});
            const d = await res.json();
            document.getElementById('previewResult').innerHTML =
                `<strong>${{d.url_group}}</strong> &middot; locale: ${{d.locale || '—'}} &middot; section: ${{d.section || '—'}}`;
        }}, 300);
    }});

    renderRules();
    </script>
    """
    return _settings_page("URL Group Rules", body)


# ---------------------------------------------------------------------------
# /settings/locales — locale whitelist editor
# ---------------------------------------------------------------------------

@router.get("/settings/locales", response_class=HTMLResponse)
def settings_locales():
    from profile_loader import get_active_profile_raw
    p = get_active_profile_raw()
    locales_json = p["locale_whitelist"] if p else "[]"
    bcp47 = bool(p["bcp47_fallback"]) if p else True
    profile_name = p["name"] if p else "(none)"

    body = f"""
    <p style='margin-bottom:12px;'><a href='/settings' style='color:#3b82f6;text-decoration:none;font-size:13px;'>&larr; Back to Settings</a></p>
    <p style='font-size:13px;color:#64748b;margin-bottom:14px;'>Active profile: <strong>{profile_name}</strong></p>

    <div class='card' style='margin-bottom:16px;'>
        <h2 style='margin-bottom:10px;'>Locale Whitelist</h2>
        <p style='font-size:12px;color:#64748b;margin-bottom:12px;'>
            Locale codes that appear as the first path segment (e.g. /en/, /zh-hans/).
            Click a tag to remove it. Type below to add.
        </p>
        <div id='localeTags' style='display:flex;flex-wrap:wrap;gap:6px;margin-bottom:12px;'></div>
        <div style='display:flex;gap:8px;align-items:center;'>
            <input id='localeInput' placeholder='Add locale code (e.g. en-gb)' style='padding:7px 10px;border:1px solid #cbd5e1;border-radius:6px;font-size:13px;width:200px;'
                   onkeydown="if(event.key==='Enter'){{ event.preventDefault(); addLocale(); }}">
            <button class='btn-sm' onclick='addLocale()'>Add</button>
        </div>
    </div>

    <div class='card' style='margin-bottom:16px;'>
        <label style='display:flex;align-items:center;gap:8px;font-size:13px;cursor:pointer;'>
            <input type='checkbox' id='bcp47Toggle' {'checked' if bcp47 else ''}
                   style='width:16px;height:16px;accent-color:#3b82f6;'>
            <span><strong>BCP 47 fallback</strong> — treat unrecognised single-segment paths matching BCP 47 pattern as locale homepages</span>
        </label>
    </div>

    <button class='btn-sm btn-blue' onclick='saveLocales()' style='margin-bottom:16px;'>Save</button>

    <style>
    .btn-sm {{ padding:5px 12px; border:1px solid #cbd5e1; border-radius:5px; font-size:12px; cursor:pointer; background:#fff; color:#374151; }}
    .btn-sm:hover {{ background:#f1f5f9; }}
    .btn-blue {{ background:#3b82f6; color:#fff; border-color:#3b82f6; }}
    .btn-blue:hover {{ background:#2563eb; }}
    .locale-tag {{
        display:inline-flex; align-items:center; gap:4px;
        padding:4px 10px; background:#eff6ff; border:1px solid #bfdbfe;
        border-radius:20px; font-size:12px; color:#1e40af; cursor:pointer;
    }}
    .locale-tag:hover {{ background:#dbeafe; border-color:#93c5fd; }}
    .locale-tag .x {{ color:#3b82f6; font-weight:700; margin-left:2px; }}
    </style>

    <script>
    let locales = {locales_json};

    function renderLocales() {{
        const c = document.getElementById('localeTags');
        c.innerHTML = '';
        locales.forEach((loc, i) => {{
            const tag = document.createElement('span');
            tag.className = 'locale-tag';
            tag.innerHTML = loc + ' <span class="x">&times;</span>';
            tag.title = 'Click to remove';
            tag.onclick = () => {{ locales.splice(i, 1); renderLocales(); }};
            c.appendChild(tag);
        }});
        if (locales.length === 0) {{
            c.innerHTML = '<span style="color:#94a3b8;font-size:12px;">No locales defined</span>';
        }}
    }}

    function addLocale() {{
        const inp = document.getElementById('localeInput');
        const val = inp.value.trim().toLowerCase();
        if (!val) return;
        if (locales.includes(val)) {{ alert('Already in the list'); return; }}
        locales.push(val);
        inp.value = '';
        renderLocales();
    }}

    async function saveLocales() {{
        const bcp47 = document.getElementById('bcp47Toggle').checked;
        const res = await fetch('/api/settings/locales', {{
            method: 'PUT',
            headers: {{'Content-Type': 'application/json'}},
            body: JSON.stringify({{locales, bcp47_fallback: bcp47}})
        }});
        const d = await res.json();
        if (d.ok) alert('Saved.');
        else alert('Error: ' + (d.error || 'Unknown'));
    }}

    renderLocales();
    </script>
    """
    return _settings_page("Locale Whitelist", body)


# ---------------------------------------------------------------------------
# /settings/sections — section mapping editor
# ---------------------------------------------------------------------------

@router.get("/settings/sections", response_class=HTMLResponse)
def settings_sections():
    from profile_loader import get_active_profile_raw
    p = get_active_profile_raw()
    sections_json = p["section_mappings"] if p else "{}"
    profile_name = p["name"] if p else "(none)"

    body = f"""
    <p style='margin-bottom:12px;'><a href='/settings' style='color:#3b82f6;text-decoration:none;font-size:13px;'>&larr; Back to Settings</a></p>
    <p style='font-size:13px;color:#64748b;margin-bottom:14px;'>Active profile: <strong>{profile_name}</strong></p>

    <div class='card'>
        <div style='display:flex;justify-content:space-between;align-items:center;margin-bottom:12px;'>
            <h2 style='margin:0;'>Section Mappings</h2>
            <div style='display:flex;gap:8px;'>
                <button class='btn-sm' onclick='addSection()'>+ Add Mapping</button>
                <button class='btn-sm btn-blue' onclick='saveSections()'>Save</button>
            </div>
        </div>
        <p style='font-size:12px;color:#64748b;margin-bottom:12px;'>
            Map URL path segments to section labels. Use <code>segment</code> for primary sections
            and <code>segment/subsegment</code> for composite sections (e.g. <code>analysis/market-news</code>).
        </p>
        <div class='table-wrapper' style='max-height:none;'>
            <table style='width:100%;border-collapse:collapse;font-size:13px;' id='sectionsTable'>
                <thead><tr style='background:#f8fafc;'>
                    <th style='padding:8px 6px;'>Path Segment(s)</th>
                    <th style='padding:8px 6px;'>Section Label</th>
                    <th style='padding:8px 6px;width:50px;'></th>
                </tr></thead>
                <tbody id='sectionsBody'></tbody>
            </table>
        </div>
    </div>

    <style>
    .btn-sm {{ padding:5px 12px; border:1px solid #cbd5e1; border-radius:5px; font-size:12px; cursor:pointer; background:#fff; color:#374151; }}
    .btn-sm:hover {{ background:#f1f5f9; }}
    .btn-blue {{ background:#3b82f6; color:#fff; border-color:#3b82f6; }}
    .btn-blue:hover {{ background:#2563eb; }}
    #sectionsTable td {{ padding:6px; border-bottom:1px solid #f1f5f9; }}
    #sectionsTable input {{
        padding:5px 8px; border:1px solid #cbd5e1; border-radius:4px; font-size:13px; width:100%;
    }}
    .del-btn {{ background:none; border:none; color:#dc2626; cursor:pointer; font-size:16px; padding:2px 6px; }}
    .del-btn:hover {{ background:#fef2f2; border-radius:4px; }}
    </style>

    <script>
    let sections = {sections_json};
    // Convert object to array of [key, value] for editing
    let entries = Object.entries(sections).sort((a,b) => a[0].localeCompare(b[0]));

    function renderSections() {{
        const tbody = document.getElementById('sectionsBody');
        tbody.innerHTML = '';
        entries.forEach((e, i) => {{
            const tr = document.createElement('tr');
            tr.innerHTML = `
                <td><input value="${{escHtml(e[0])}}" onchange="entries[${{i}}][0]=this.value"></td>
                <td><input value="${{escHtml(e[1])}}" onchange="entries[${{i}}][1]=this.value"></td>
                <td><button class='del-btn' onclick='entries.splice(${{i}},1);renderSections();' title='Delete'>&times;</button></td>
            `;
            tbody.appendChild(tr);
        }});
    }}

    function escHtml(s) {{ const d = document.createElement('div'); d.textContent = s; return d.innerHTML.replace(/"/g, '&quot;'); }}

    function addSection() {{
        entries.push(['', '']);
        renderSections();
        // Focus the new key input
        const inputs = document.querySelectorAll('#sectionsBody tr:last-child input');
        if (inputs[0]) inputs[0].focus();
    }}

    async function saveSections() {{
        const obj = {{}};
        for (const [k, v] of entries) {{
            const key = k.trim();
            if (key) obj[key] = v.trim();
        }}
        const res = await fetch('/api/settings/sections', {{
            method: 'PUT',
            headers: {{'Content-Type': 'application/json'}},
            body: JSON.stringify({{sections: obj}})
        }});
        const d = await res.json();
        if (d.ok) alert('Saved.');
        else alert('Error: ' + (d.error || 'Unknown'));
    }}

    renderSections();
    </script>
    """
    return _settings_page("Section Mappings", body)


# ---------------------------------------------------------------------------
# /settings/logs — log file manager + ingest / rebuild triggers
# ---------------------------------------------------------------------------

@router.get("/settings/logs", response_class=HTMLResponse)
def settings_logs():
    files = _log_file_status()

    rows_html = ""
    for f in files:
        status = f["status"]
        status_color = {"ingested": "#16a34a", "pending": "#f59e0b", "changed": "#3b82f6"}.get(status, "#64748b")
        rows_html += f"""<tr>
            <td><input type='checkbox' class='file-cb' value='{f["name"]}' data-path='{f["path"]}'></td>
            <td style='font-family:monospace;font-size:12px;'>{f["name"]}</td>
            <td>{_fmt_bytes(f["size"])}</td>
            <td>{f["mtime"][:19]}</td>
            <td><span style='color:{status_color};font-weight:600;font-size:12px;'>{status}</span></td>
        </tr>"""

    body = f"""
    <p style='margin-bottom:12px;'><a href='/settings' style='color:#3b82f6;text-decoration:none;font-size:13px;'>&larr; Back to Settings</a></p>

    <div class='card' style='margin-bottom:16px;'>
        <h2 style='margin-bottom:12px;'>Log Files</h2>
        <div class='table-wrapper' style='max-height:500px;'>
            <table class='sortable' style='width:100%;'>
                <thead><tr>
                    <th style='width:30px;'><input type='checkbox' id='selectAll'></th>
                    <th>Filename</th><th>Size</th><th>Last Modified</th><th>Status</th>
                </tr></thead>
                <tbody>{rows_html}</tbody>
            </table>
        </div>
    </div>

    <div class='card' style='margin-bottom:16px;'>
        <h2 style='margin-bottom:10px;'>Ingestion</h2>
        <div style='display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin-bottom:10px;'>
            <button class='btn-sm btn-blue' id='btnIngestSelected' onclick='runIngest("selected")'>Ingest Selected</button>
            <button class='btn-sm btn-blue' id='btnIngestAll' onclick='runIngest("all")'>Ingest All Pending</button>
        </div>
        <div id='ingestProgress' style='font-size:13px;color:#64748b;'></div>
    </div>

    <div class='card'>
        <h2 style='margin-bottom:10px;'>Rebuild Aggregates</h2>
        <div style='display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin-bottom:10px;'>
            <label style='font-size:12px;font-weight:600;color:#64748b;display:flex;flex-direction:column;gap:4px;'>
                FROM
                <input type='date' id='rebuildFrom' style='padding:5px 8px;border:1px solid #cbd5e1;border-radius:4px;font-size:13px;'>
            </label>
            <label style='font-size:12px;font-weight:600;color:#64748b;display:flex;flex-direction:column;gap:4px;'>
                TO
                <input type='date' id='rebuildTo' style='padding:5px 8px;border:1px solid #cbd5e1;border-radius:4px;font-size:13px;'>
            </label>
            <button class='btn-sm btn-blue' id='btnRebuild' onclick='runRebuild()' style='align-self:flex-end;'>Rebuild</button>
        </div>
        <div id='rebuildProgress' style='font-size:13px;color:#64748b;'></div>
    </div>

    <style>
    .btn-sm {{ padding:5px 12px; border:1px solid #cbd5e1; border-radius:5px; font-size:12px; cursor:pointer; background:#fff; color:#374151; }}
    .btn-sm:hover {{ background:#f1f5f9; }}
    .btn-sm:disabled {{ opacity:0.4; cursor:not-allowed; }}
    .btn-blue {{ background:#3b82f6; color:#fff; border-color:#3b82f6; }}
    .btn-blue:hover {{ background:#2563eb; }}
    </style>

    <script>
    // Select-all checkbox
    document.getElementById('selectAll').addEventListener('change', function() {{
        document.querySelectorAll('.file-cb').forEach(cb => cb.checked = this.checked);
    }});

    let activeTask = null;

    function setButtonsDisabled(disabled) {{
        ['btnIngestSelected','btnIngestAll','btnRebuild'].forEach(id => {{
            const el = document.getElementById(id);
            if (el) el.disabled = disabled;
        }});
    }}

    async function runIngest(mode) {{
        const payload = {{}};
        if (mode === 'selected') {{
            const selected = Array.from(document.querySelectorAll('.file-cb:checked')).map(cb => cb.dataset.path);
            if (selected.length === 0) {{ alert('No files selected'); return; }}
            payload.file_list = selected;
        }}
        setButtonsDisabled(true);
        const el = document.getElementById('ingestProgress');
        el.textContent = 'Starting ingestion...';
        try {{
            const res = await fetch('/api/settings/ingest', {{
                method: 'POST',
                headers: {{'Content-Type': 'application/json'}},
                body: JSON.stringify(payload)
            }});
            const data = await res.json();
            activeTask = data.task_id;
            pollTask('/api/settings/ingest/' + data.task_id + '/status', el);
        }} catch(err) {{
            el.textContent = 'Error: ' + err.message;
            setButtonsDisabled(false);
        }}
    }}

    async function runRebuild() {{
        const from_date = document.getElementById('rebuildFrom').value;
        const to_date = document.getElementById('rebuildTo').value;
        setButtonsDisabled(true);
        const el = document.getElementById('rebuildProgress');
        el.textContent = 'Starting rebuild...';
        try {{
            const res = await fetch('/api/settings/rebuild', {{
                method: 'POST',
                headers: {{'Content-Type': 'application/json'}},
                body: JSON.stringify({{from_date, to_date}})
            }});
            const data = await res.json();
            activeTask = data.task_id;
            pollTask('/api/settings/rebuild/' + data.task_id + '/status', el);
        }} catch(err) {{
            el.textContent = 'Error: ' + err.message;
            setButtonsDisabled(false);
        }}
    }}

    function pollTask(url, el) {{
        const iv = setInterval(async () => {{
            try {{
                const res = await fetch(url);
                const d = await res.json();
                if (d.status === 'running') {{
                    const lines = (d.detail || '').trim().split('\\n');
                    el.textContent = 'Running... ' + (lines[lines.length - 1] || '');
                }} else {{
                    clearInterval(iv);
                    const ok = d.status === 'done';
                    el.innerHTML = (ok
                        ? '<span style="color:#16a34a;font-weight:600;">Completed.</span>'
                        : '<span style="color:#dc2626;font-weight:600;">Error.</span>')
                        + '<pre style="margin-top:6px;font-size:11px;max-height:200px;overflow:auto;background:#f8fafc;padding:8px;border-radius:4px;border:1px solid #e2e8f0;">'
                        + escHtml(d.detail || '') + '</pre>';
                    setButtonsDisabled(false);
                    activeTask = null;
                }}
            }} catch(err) {{
                clearInterval(iv);
                el.textContent = 'Poll error: ' + err.message;
                setButtonsDisabled(false);
            }}
        }}, 1500);
    }}

    function escHtml(s) {{ const d = document.createElement('div'); d.textContent = s; return d.innerHTML; }}
    </script>
    """
    return _settings_page("Log File Manager", body)
