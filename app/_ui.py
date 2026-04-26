"""Shared server-rendered chrome: icons, topnav, page-head."""
from __future__ import annotations

from html import escape as _esc
from typing import Any, Callable, Optional


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


def filter_popover(
    *,
    popover_id: str,
    title: str,
    description: str = "",
    categories: list,
    popover_attrs: str = "",
    close_attr: str = "data-mfp-close",
    extras_html: str = "",
    footer_left_html: str = "",
    footer_right_html: str = "",
    section_actions_attr_fn=None,
) -> str:
    """Generic filter popover (.mfp shell, flat single-column body).

    `categories` items: {id, label, options, selected, search_enabled,
                          treat_unselected_as_all? (default False)}.
    Caller wires JS via popover_attrs/close_attr/footer_*_html — the helper
    emits structure only.
    """
    sections: list[str] = []
    for cat in categories:
        cid = cat["id"]
        clabel = cat.get("label", cid)
        options = cat.get("options", [])
        selected = cat.get("selected", []) or []
        search_enabled = bool(cat.get("search_enabled")) or len(options) > 6
        treat_all = bool(cat.get("treat_unselected_as_all"))
        sel_set = set(selected)
        items: list[str] = []
        for opt in options:
            opt_str = str(opt)
            opt_safe = _esc(opt_str, quote=True)
            opt_text = _esc(opt_str)
            checked = " checked" if (treat_all and not sel_set) or opt in sel_set else ""
            items.append(
                f"<label><input type='checkbox' data-pop-field='{_esc(cid, quote=True)}' "
                f"value='{opt_safe}'{checked}> <span>{opt_text}</span></label>"
            )
        list_html = (
            f"<div class='pop-checklist' data-pop-checklist "
            f"data-pop-field-list='{_esc(cid, quote=True)}'>{''.join(items)}</div>"
        )
        search_html = ""
        if search_enabled and options:
            search_html = (
                f"<input type='text' class='pop-checklist-search' "
                f"placeholder='Filter {_esc(clabel, quote=True)}…' "
                "autocomplete='off' spellcheck='false'>"
            )
        actions_extra = section_actions_attr_fn(cid) if section_actions_attr_fn else ""
        section_actions = (
            "<div class='mfp-section-actions'>"
            f"<button type='button' class='mfp-mini' data-pop-field='{_esc(cid, quote=True)}' "
            f"data-pop-select-all{(' ' + actions_extra) if actions_extra else ''}>Select all</button>"
            f"<button type='button' class='mfp-mini' data-pop-field='{_esc(cid, quote=True)}' "
            f"data-pop-clear-field{(' ' + actions_extra) if actions_extra else ''}>Clear</button>"
            "</div>"
        )
        sections.append(
            "<div class='mfp-section'>"
            "<div class='mfp-section-head'>"
            f"<div class='mfp-section-label'>{_esc(clabel)}</div>"
            f"{section_actions}"
            "</div>"
            "<div class='pop-checklist-wrap'>"
            f"{search_html}"
            f"{list_html}"
            "<div class='pop-checklist-meta' data-pop-checklist-meta></div>"
            "</div>"
            "</div>"
        )

    desc_html = f"<p>{_esc(description)}</p>" if description else ""
    return (
        f"<div class='popover mfp' id='{_esc(popover_id, quote=True)}'"
        f"{(' ' + popover_attrs) if popover_attrs else ''}>"
        "<div class='mfp-head'>"
        f"<div><h4>{_esc(title)}</h4>{desc_html}</div>"
        f"<button type='button' class='mfp-close btn btn-icon btn-ghost' "
        f"{close_attr} aria-label='Close'>{iconHtml('x')}</button>"
        "</div>"
        "<div class='mfp-body mfp-flat'>"
        f"{''.join(sections)}"
        f"{extras_html}"
        "</div>"
        "<div class='mfp-foot'>"
        f"{footer_left_html}"
        "<div class='mfp-foot-right'>"
        f"{footer_right_html}"
        "</div>"
        "</div>"
        "</div>"
    )


def status_pill_html(value: Any) -> str:
    """HTTP status code → coloured pill. Non-numeric values pass through escaped."""
    if value is None or value == "":
        return ""
    try:
        s = int(value)
    except (TypeError, ValueError):
        return _esc(str(value))
    if 200 <= s < 300:
        cls = "status-2xx"
    elif 300 <= s < 400:
        cls = "status-3xx"
    elif 400 <= s < 500:
        cls = "status-4xx"
    elif 500 <= s < 600:
        cls = "status-5xx"
    else:
        cls = ""
    return f"<span class='status-pill {cls}'>{s}</span>"


def method_pill_html(value: Any) -> str:
    if value is None or value == "":
        return ""
    m = _esc(str(value))
    return f"<span class='method method-{m}'>{m}</span>"


_PILLIFY_COLS = {
    "status": status_pill_html,
    "method": method_pill_html,
    "s2xx": status_pill_html,
    "s3xx": status_pill_html,
    "s4xx": status_pill_html,
    "s5xx": status_pill_html,
}


def render_table(
    rows: list,
    columns: list,
    *,
    sortable: bool = True,
    server_paginated: bool = False,
    column_renderers: Optional[dict[str, Callable[[Any], str]]] = None,
    empty_text: str = "No data.",
    max_rows: Optional[int] = None,
    count_label: Optional[str] = None,
    toolbar_right_html: str = "",
    pager_html: str = "",
    row_attrs_fn: Optional[Callable[[Any, int], str]] = None,
) -> str:
    """v2 table component.

    Auto-detects status/method-style columns by name unless `column_renderers`
    overrides. `column_renderers` keys are column names; values take the cell
    value and return HTML. Sticky header by default; `sortable=True` adds
    `.sort-arrow` affordance and the JS controller hooks up sort + paging.
    """
    cols = list(columns)
    renderers: dict[str, Callable[[Any], str]] = dict(_PILLIFY_COLS)
    if column_renderers:
        renderers.update(column_renderers)

    if not rows:
        return (
            "<div class='table-wrap'>"
            f"<div class='empty'>{_esc(empty_text)}</div>"
            "</div>"
        )

    head_cells = []
    for i, c in enumerate(cols):
        sort_attr = f" data-sort-col='{_esc(str(c), quote=True)}'" if sortable else ""
        arrow = "<span class='sort-arrow'>↕</span>" if sortable else ""
        head_cells.append(
            f"<th scope='col' data-col='{i}'{sort_attr}>{_esc(str(c))}{arrow}</th>"
        )
    thead = "<thead><tr>" + "".join(head_cells) + "</tr></thead>"

    visible = rows if max_rows is None else rows[: max(0, int(max_rows))]
    body_rows = []
    for idx, r in enumerate(visible):
        cells = []
        for c, v in zip(cols, r):
            if v is None:
                cells.append("<td></td>")
                continue
            renderer = renderers.get(c)
            if renderer is not None:
                cells.append(f"<td>{renderer(v)}</td>")
            elif isinstance(v, str):
                cells.append(f"<td>{v}</td>")
            else:
                cells.append(f"<td>{_esc(str(v))}</td>")
        attrs = row_attrs_fn(r, idx) if row_attrs_fn else ""
        prefix = (" " + attrs) if attrs else ""
        body_rows.append(f"<tr{prefix}>" + "".join(cells) + "</tr>")
    tbody = "<tbody>" + "".join(body_rows) + "</tbody>"

    cls_parts = ["log-table"]
    if sortable:
        cls_parts.append("sortable")
    if server_paginated:
        cls_parts.append("server-paginated")
    table_cls = " ".join(cls_parts)

    if count_label is None:
        count = len(rows)
        if max_rows is not None and count > int(max_rows):
            count_label = f"<strong>{int(max_rows):,}</strong> of {count:,} rows"
        else:
            count_label = f"<strong>{count:,}</strong> {'row' if count == 1 else 'rows'}"

    toolbar = (
        "<div class='table-toolbar'>"
        f"<div class='tt-count'>{count_label}</div>"
        f"<div class='tt-right'>{toolbar_right_html}</div>"
        "</div>"
    )

    return (
        "<div class='table-wrap'>"
        f"{toolbar}"
        f"<div class='table-scroll'><table class='{table_cls}'>{thead}{tbody}</table></div>"
        f"{pager_html}"
        "</div>"
    )


def date_range_popover(
    *,
    popover_id: str,
    date_from: Optional[str],
    date_to: Optional[str],
    avail_from: Optional[str] = None,
    avail_to: Optional[str] = None,
    hidden_inputs_html: str = "",
    clear_href: str = "",
    clear_label: str = "All time",
    form_action: str = "",
    extra_styles: str = "",
) -> str:
    """The standard custom date-range popover. Mounted via data-popover-trigger."""
    fmin = f" min='{_esc(avail_from, quote=True)}'" if avail_from else ""
    fmax = f" max='{_esc(avail_to, quote=True)}'" if avail_to else ""
    style_attr = f" style='{extra_styles}'" if extra_styles else ""
    action_attr = f" action='{_esc(form_action, quote=True)}'" if form_action else ""
    clear_button = ""
    if clear_href:
        clear_button = (
            f"<a href='{_esc(clear_href, quote=True)}' class='btn btn-sm btn-ghost' "
            f"hx-get='{_esc(clear_href, quote=True)}' hx-target='#results' "
            f"hx-push-url='true' hx-swap='innerHTML'>{_esc(clear_label)}</a>"
        )
    return (
        f"<div class='popover' id='{_esc(popover_id, quote=True)}'{style_attr}>"
        "<div class='popover-title'>Custom date range</div>"
        f"<form method='get'{action_attr}>"
        f"{hidden_inputs_html}"
        "<div class='pop-row'><label>From</label>"
        f"<input class='input' type='date' name='from' "
        f"value='{_esc(date_from or '', quote=True)}'{fmin}{fmax}></div>"
        "<div class='pop-row'><label>To</label>"
        f"<input class='input' type='date' name='to' "
        f"value='{_esc(date_to or '', quote=True)}'{fmin}{fmax}></div>"
        "<div class='pop-actions'>"
        f"{clear_button}"
        "<button type='submit' class='btn btn-sm btn-primary'>Apply</button>"
        "</div>"
        "</form>"
        "</div>"
    )
