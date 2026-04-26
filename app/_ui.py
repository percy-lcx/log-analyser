"""Shared server-rendered chrome: icons, topnav, page-head."""
from __future__ import annotations

from html import escape as _esc
from typing import Optional


def iconHtml(name: str, cls: str = "", size: int = 14) -> str:
    extra = f" class='{cls}'" if cls else ""
    return (
        f"<svg{extra} width='{size}' height='{size}' aria-hidden='true'>"
        f"<use href='/static/icons.svg#{name}'/></svg>"
    )


def topnav(active: str, *, last_ingest_label: str, date_label: Optional[str]) -> str:
    """Sticky v2 top nav. `active` is one of: logs, summary, reports, settings."""
    reports_children = [
        ("Locales", "/reports/locales"),
        ("Search Console", "/reports/gsc"),
        ("Strategic Crawl", "/reports/strategic-crawl"),
    ]
    reports_active = "active" if active == "reports" else ""
    reports_items = "".join(
        f"<a href='{url}'>{_esc(label)}</a>" for label, url in reports_children
    )
    reports_dropdown = (
        f"<div class='nav-item-dropdown'>"
        f"<button type='button' class='nav-link {reports_active}' "
        "data-nav-dropdown='nav-dd-reports' aria-haspopup='true' aria-expanded='false'>"
        f"Reports<span class='caret'>{iconHtml('chevron')}</span>"
        "</button>"
        f"<div class='nav-dropdown' id='nav-dd-reports' role='menu'>{reports_items}</div>"
        "</div>"
    )

    plain_items = [
        ("logs", "Logs", "/logs"),
        ("summary", "Summary", "/reports/summary"),
    ]
    plain_links = "".join(
        f"<a href='{url}' class='nav-link{' active' if active == key else ''}'>{_esc(label)}</a>"
        for key, label, url in plain_items
    )
    settings_link = (
        f"<a href='/settings' class='nav-link{' active' if active == 'settings' else ''}'>Settings</a>"
    )

    pills: list[str] = []
    pill_text = last_ingest_label.strip() if last_ingest_label else ""
    if pill_text:
        pills.append(
            f"<span class='nav-pill' title='Last ingest'><span class='dot'></span>{_esc(pill_text)}</span>"
        )
    else:
        pills.append("<span class='nav-pill' title='Data freshness'><span class='dot'></span>live</span>")
    if date_label:
        pills.append(f"<span class='nav-pill' title='Date range'>{_esc(date_label)}</span>")

    return (
        "<nav class='topnav'>"
        "<div class='brand'><span class='brand-mark'></span><span>log analyser</span></div>"
        f"<div class='nav-links'>{plain_links}{reports_dropdown}{settings_link}</div>"
        "<div class='nav-util'>"
        f"{''.join(pills)}"
        f"<button class='btn btn-icon btn-ghost' data-popover-trigger='kb-help-open' "
        f"title='Shortcuts (?)' aria-label='Keyboard shortcuts'>{iconHtml('keyboard')}</button>"
        "</div>"
        "</nav>"
    )


def page_head(title: str, subtitle: Optional[str] = None, actions_html: str = "") -> str:
    sub = f" <small>{_esc(subtitle)}</small>" if subtitle else ""
    return (
        "<div class='page-head'>"
        f"<div class='page-title'>{_esc(title)}{sub}</div>"
        f"<div class='page-actions'>{actions_html}</div>"
        "</div>"
    )


def notice(variant: str, text: str, *, link: Optional[tuple[str, str]] = None) -> str:
    """Inline message banner. variant in {'info','warn','err'}. `link` is (label, href)."""
    cls = {"info": "notice-info", "warn": "notice-warn", "err": "notice-err"}.get(variant, "notice-info")
    link_html = ""
    if link:
        label, href = link
        link_html = f" <a class='notice-link' href='{_esc(href, quote=True)}'>{_esc(label)}</a>"
    return f"<div class='notice {cls}'><span>{text}{link_html}</span></div>"
