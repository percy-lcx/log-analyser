// Log viewer v2 — Phase 1 + 2 JS island.
// Phase 1: `/` focus search, `?` help modal, Esc close, popover toggles,
//   column-checkbox submit-on-change, close-popover-on-htmx-settle.
// Phase 2: search autocomplete, More filters popover (pane switch + checkbox
//   URL sync + single-value chips), active-state sync after htmx settles.
(function () {
  'use strict';

  function isTypingTarget(el) {
    if (!el) return false;
    var tag = (el.tagName || '').toLowerCase();
    return tag === 'input' || tag === 'textarea' || tag === 'select' || el.isContentEditable;
  }

  function setPopoverBackdrop(on) {
    var b = document.getElementById('popover-backdrop');
    if (!b) return;
    b.classList.toggle('is-open', !!on);
  }

  function closeAllPopovers() {
    document.querySelectorAll('.popover.is-open').forEach(function (p) {
      p.classList.remove('is-open');
    });
    setPopoverBackdrop(false);
  }

  function togglePopover(id) {
    var p = document.getElementById(id);
    if (!p) return;
    var wasOpen = p.classList.contains('is-open');
    closeAllPopovers();
    if (!wasOpen) {
      p.classList.add('is-open');
      // Modal popovers (.mfp) get the backdrop; regular popovers don't.
      setPopoverBackdrop(p.classList.contains('mfp'));
    }
  }

  function navigate(url) {
    if (window.htmx) {
      window.htmx.ajax('GET', url, { target: '#results', swap: 'innerHTML' });
      window.history.pushState({}, '', url);
    } else {
      window.location.href = url;
    }
  }

  // ── Popover open/close ────────────────────────────────────────────────
  function closeAllNavDropdowns() {
    document.querySelectorAll('.nav-dropdown.is-open').forEach(function (d) {
      d.classList.remove('is-open');
    });
    document.querySelectorAll('[data-nav-dropdown][aria-expanded="true"]').forEach(function (b) {
      b.setAttribute('aria-expanded', 'false');
    });
  }

  document.addEventListener('click', function (e) {
    var trigger = e.target.closest('[data-popover-trigger]');
    if (trigger) {
      e.preventDefault();
      togglePopover(trigger.getAttribute('data-popover-trigger'));
      return;
    }
    var navTrigger = e.target.closest('[data-nav-dropdown]');
    if (navTrigger) {
      e.preventDefault();
      var id = navTrigger.getAttribute('data-nav-dropdown');
      var dd = document.getElementById(id);
      if (!dd) return;
      var wasOpen = dd.classList.contains('is-open');
      closeAllNavDropdowns();
      if (!wasOpen) {
        dd.classList.add('is-open');
        navTrigger.setAttribute('aria-expanded', 'true');
      }
      return;
    }
    if (!e.target.closest('.nav-dropdown') && !e.target.closest('[data-nav-dropdown]')) {
      closeAllNavDropdowns();
    }
    if (!e.target.closest('.popover') && !e.target.closest('[data-popover-trigger]')) {
      closeAllPopovers();
    }
  });

  // ── Escape: close any open overlays ───────────────────────────────────
  document.addEventListener('keydown', function (e) {
    if (e.key === 'Escape') {
      closeAllPopovers();
      closeAllNavDropdowns();
      var hint = document.querySelector('.search-hint');
      if (hint) hint.style.display = '';
      return;
    }
    if (isTypingTarget(e.target)) {
      // In search input: handle arrow-nav + Enter inside autocomplete dropdown
      var inp = e.target.closest('.search-input input');
      if (inp) handleSearchKey(e, inp);
    }
  });

  // ── Chart toggle: flip `chart` in the URL at click time ─────────────
  // (The anchor can't carry a baked hx-get URL because the filter bar lives
  // outside #results and doesn't re-render on partial swaps.)
  document.addEventListener('click', function (e) {
    var a = e.target.closest('[data-chart-toggle]');
    if (!a) return;
    e.preventDefault();
    var url = new URL(window.location.href);
    if (url.searchParams.get('chart') === '1') url.searchParams.delete('chart');
    else                                        url.searchParams.set('chart', '1');
    url.searchParams.delete('page');
    navigate(url.pathname + url.search);
  });

  // Keep the checkbox glyph in sync with the URL after every partial swap.
  document.body.addEventListener('htmx:afterSettle', function () {
    var on = new URL(window.location.href).searchParams.get('chart') === '1';
    var toggle = document.querySelector('[data-chart-toggle]');
    if (!toggle) return;
    var cb = toggle.querySelector('input[type="checkbox"]');
    if (cb) cb.checked = on;
  });

  // ── Filter-bar sync: chrome sits outside #results so we must re-align
  //    the date label, preset .active, from/to inputs, and relative-preset
  //    hrefs with the URL after every htmx swap.
  function syncDateRangeUi() {
    var url = new URL(window.location.href);
    var from = url.searchParams.get('from') || '';
    var to = url.searchParams.get('to') || '';
    var dr = document.querySelector('[data-date-range]');
    if (!dr) return;
    var availFrom = dr.getAttribute('data-avail-from') || '';
    var availTo = dr.getAttribute('data-avail-to') || '';

    // Date label
    var isAllTime = availFrom && availTo && from === availFrom && to === availTo;
    var label;
    if (!from && !to) label = 'All time';
    else if (isAllTime) label = 'All time';
    else if (from && to) label = (from === to) ? from : (from + ' → ' + to);
    else label = 'All time';
    var labelEl = dr.querySelector('.dr-label');
    if (labelEl) labelEl.textContent = label;

    // Popover from/to inputs
    var pop = document.getElementById('pop-date-range');
    if (pop) {
      var fromInp = pop.querySelector('input[name="from"][type="date"]');
      var toInp = pop.querySelector('input[name="to"][type="date"]');
      if (fromInp) fromInp.value = from;
      if (toInp) toInp.value = to;
    }

    // Preset anchor = latest available data day (falls back to today).
    var anchorIso;
    if (availTo) {
      anchorIso = availTo;
    } else {
      var now = new Date();
      anchorIso = now.getUTCFullYear() + '-' +
        String(now.getUTCMonth() + 1).padStart(2, '0') + '-' +
        String(now.getUTCDate()).padStart(2, '0');
    }

    // Active preset based on span ending on the anchor.
    var activeDays = null;
    if (from && to) {
      try {
        var fd = new Date(from + 'T00:00:00Z');
        var td = new Date(to + 'T00:00:00Z');
        var spanDays = Math.round((td - fd) / 86400000) + 1;
        if (to === anchorIso && [1, 3, 7, 14, 30].indexOf(spanDays) !== -1) {
          activeDays = spanDays;
        }
      } catch (e) { /* fall through — no active */ }
    }
    dr.querySelectorAll('[data-preset-days]').forEach(function (a) {
      var d = parseInt(a.getAttribute('data-preset-days'), 10);
      a.classList.toggle('active', d === activeDays);
    });

    // Rebuild hrefs on presets + All-time so they carry the current URL's
    // non-date filters forward (chrome isn't re-rendered between swaps).
    function buildQsWithDates(fromVal, toVal) {
      var u = new URL(url.pathname + url.search, window.location.origin);
      if (fromVal) u.searchParams.set('from', fromVal); else u.searchParams.delete('from');
      if (toVal) u.searchParams.set('to', toVal); else u.searchParams.delete('to');
      u.searchParams.delete('page');
      return u.pathname + u.search;
    }
    var anchorParts = anchorIso.split('-').map(function (s) { return parseInt(s, 10); });
    var anchorUtc = Date.UTC(anchorParts[0], anchorParts[1] - 1, anchorParts[2]);
    dr.querySelectorAll('[data-preset-days]').forEach(function (a) {
      var d = parseInt(a.getAttribute('data-preset-days'), 10);
      var fromUtc = new Date(anchorUtc - (d - 1) * 86400000);
      var fromIso = fromUtc.getUTCFullYear() + '-' +
        String(fromUtc.getUTCMonth() + 1).padStart(2, '0') + '-' +
        String(fromUtc.getUTCDate()).padStart(2, '0');
      var href = buildQsWithDates(fromIso, anchorIso);
      a.setAttribute('href', href);
      a.setAttribute('hx-get', href);
    });
    // "All time" link inside the popover
    if (pop && availFrom && availTo) {
      var allLink = pop.querySelector('a.btn-ghost[hx-target="#results"]');
      if (allLink) {
        var allHref = buildQsWithDates(availFrom, availTo);
        allLink.setAttribute('href', allHref);
        allLink.setAttribute('hx-get', allHref);
      }
    }

    // Let htmx rescan the new attributes.
    if (window.htmx && window.htmx.process) {
      dr.querySelectorAll('[hx-get]').forEach(function (el) { window.htmx.process(el); });
      if (pop) pop.querySelectorAll('[hx-get]').forEach(function (el) { window.htmx.process(el); });
    }
  }
  document.body.addEventListener('htmx:afterSettle', syncDateRangeUi);
  document.addEventListener('DOMContentLoaded', syncDateRangeUi);

  // ── Columns popover checkbox sync ─────────────────────────────────────
  // Keep the columns popover open across the htmx swap so the user can toggle
  // multiple columns without re-opening it.
  var keepColumnsPopoverOpen = false;
  document.addEventListener('change', function (e) {
    var cb = e.target.closest('[data-col-checkbox]');
    if (!cb) return;
    var form = cb.closest('.popover');
    var checked = form.querySelectorAll('[data-col-checkbox]:checked');
    var cols = Array.from(checked).map(function (el) { return el.value; });
    var url = new URL(window.location.href);
    if (cols.length) url.searchParams.set('columns', cols.join(','));
    else             url.searchParams.delete('columns');
    url.searchParams.delete('page');
    keepColumnsPopoverOpen = true;
    navigate(url.pathname + url.search);
  });

  // ── Path filter rule-builder ──────────────────────────────────────────
  // Inline disclosure under the search input. Add/remove rule rows locally;
  // Apply collects state, rewrites the URL, and routes through navigate().
  function pfRoot() { return document.querySelector('.path-filter'); }

  document.addEventListener('click', function (e) {
    var root = pfRoot();
    if (!root) return;

    var toggle = e.target.closest('[data-pf-toggle]');
    if (toggle && root.contains(toggle)) {
      var open = root.hasAttribute('data-pf-open');
      if (open) {
        root.removeAttribute('data-pf-open');
        toggle.setAttribute('aria-expanded', 'false');
      } else {
        root.setAttribute('data-pf-open', '');
        toggle.setAttribute('aria-expanded', 'true');
      }
      return;
    }

    var joinBtn = e.target.closest('[data-pf-join]');
    if (joinBtn && root.contains(joinBtn)) {
      e.preventDefault();
      root.querySelectorAll('[data-pf-join]').forEach(function (b) {
        var on = b === joinBtn;
        b.classList.toggle('active', on);
        b.setAttribute('aria-pressed', on ? 'true' : 'false');
      });
      return;
    }

    if (e.target.closest('[data-pf-add]')) {
      e.preventDefault();
      var tmpl = root.getAttribute('data-pf-template') || '';
      var rules = root.querySelector('[data-pf-rules]');
      if (!tmpl || !rules) return;
      var holder = document.createElement('div');
      holder.innerHTML = tmpl;
      var row = holder.firstElementChild;
      if (row) {
        rules.appendChild(row);
        var inp = row.querySelector('[data-pf-val]');
        if (inp) inp.focus();
      }
      return;
    }

    var rm = e.target.closest('[data-pf-remove]');
    if (rm && root.contains(rm)) {
      e.preventDefault();
      var row = rm.closest('[data-pf-row]');
      if (row) row.remove();
      return;
    }

    if (e.target.closest('[data-pf-clear]')) {
      e.preventDefault();
      var url = new URL(window.location.href);
      url.searchParams.delete('pf');
      url.searchParams.delete('pf_join');
      url.searchParams.delete('page');
      navigate(url.pathname + url.search);
      return;
    }

    if (e.target.closest('[data-pf-apply]')) {
      e.preventDefault();
      var u = new URL(window.location.href);
      u.searchParams.delete('pf');
      u.searchParams.delete('pf_join');
      u.searchParams.delete('page');
      var rows = root.querySelectorAll('[data-pf-row]');
      var added = 0;
      rows.forEach(function (r) {
        var op = (r.querySelector('[data-pf-op]') || {}).value || '';
        var val = (r.querySelector('[data-pf-val]') || {}).value || '';
        val = val.trim();
        if (!val) return;
        if (op !== 'contains' && op !== 'not_contains' && op !== 'regex') return;
        u.searchParams.append('pf', op + ':' + val);
        added++;
      });
      var join = (root.querySelector('[data-pf-join].active') || {}).getAttribute
        ? root.querySelector('[data-pf-join].active').getAttribute('data-pf-join')
        : 'and';
      if (join === 'or' && added > 1) u.searchParams.set('pf_join', 'or');
      navigate(u.pathname + u.search);
      return;
    }
  });

  // Enter key inside a rule's value input → Apply
  document.addEventListener('keydown', function (e) {
    if (e.key !== 'Enter') return;
    var inp = e.target && e.target.closest && e.target.closest('[data-pf-val]');
    if (!inp) return;
    var root = pfRoot();
    if (!root) return;
    e.preventDefault();
    var apply = root.querySelector('[data-pf-apply]');
    if (apply) apply.click();
  });

  // ── More filters: pane switching (category nav) ───────────────────────
  document.addEventListener('click', function (e) {
    var tabBtn = e.target.closest('[data-mfp-tab]');
    if (!tabBtn) return;
    var popover = tabBtn.closest('.mfp');
    if (!popover) return;
    var target = tabBtn.getAttribute('data-mfp-tab');
    popover.querySelectorAll('[data-mfp-tab]').forEach(function (b) {
      b.classList.toggle('active', b.getAttribute('data-mfp-tab') === target);
    });
    popover.querySelectorAll('[data-mfp-pane]').forEach(function (p) {
      p.style.display = (p.getAttribute('data-mfp-pane') === target) ? 'block' : 'none';
    });
  });

  // ── More filters: single-value buttons (is_bot) buffer locally ────────
  // Clicking toggles active class within the popover; nothing is applied
  // until the Apply button fires.
  document.addEventListener('click', function (e) {
    var btn = e.target.closest('[data-mfp-single]');
    if (!btn) return;
    var popover = btn.closest('.mfp');
    if (!popover) return;
    e.preventDefault();
    var field = btn.getAttribute('data-mfp-single');
    popover.querySelectorAll('[data-mfp-single="' + field + '"]').forEach(function (b) {
      b.classList.remove('active');
    });
    btn.classList.add('active');
  });

  // ── More filters: Apply → collect state → build URL → navigate ────────
  document.addEventListener('click', function (e) {
    var btn = e.target.closest('[data-mfp-apply]');
    if (!btn) return;
    e.preventDefault();
    var popover = btn.closest('.mfp');
    if (!popover) return;
    var url = new URL(window.location.href);

    // Multi-value fields (checkbox groups keyed by data-mfp-field)
    var multiFields = new Set();
    popover.querySelectorAll('[data-mfp-field]').forEach(function (cb) {
      multiFields.add(cb.getAttribute('data-mfp-field'));
    });
    multiFields.forEach(function (f) {
      var checked = popover.querySelectorAll(
        'input[data-mfp-field="' + f + '"]:checked'
      );
      var vals = Array.from(checked).map(function (el) {
        return el.getAttribute('data-filter-value');
      });
      if (vals.length) url.searchParams.set(f, vals.join(','));
      else             url.searchParams.delete(f);
    });

    // Single-value fields (is_bot)
    var singleFields = new Set();
    popover.querySelectorAll('[data-mfp-single]').forEach(function (b) {
      singleFields.add(b.getAttribute('data-mfp-single'));
    });
    singleFields.forEach(function (f) {
      var activeBtn = popover.querySelector('[data-mfp-single="' + f + '"].active');
      if (activeBtn) {
        var v = activeBtn.getAttribute('data-filter-value');
        if (v) url.searchParams.set(f, v);
        else   url.searchParams.delete(f);
      }
    });

    url.searchParams.delete('page');
    closeAllPopovers();
    navigate(url.pathname + url.search);
  });

  // ── More filters: close button (X in header) ──────────────────────────
  document.addEventListener('click', function (e) {
    var closer = e.target.closest('[data-mfp-close]');
    if (!closer) return;
    // Don't swallow anchor navigation (Clear-all is also [data-mfp-close])
    var popover = closer.closest('.mfp');
    if (popover) popover.classList.remove('is-open');
    setPopoverBackdrop(false);
  });

  // ── More filters: per-section search (type-to-filter checkboxes) ──────
  var checklistFilterTimers = new WeakMap();
  function applyChecklistFilter(input) {
    var wrap = input.closest('.pop-checklist-wrap');
    if (!wrap) return;
    var list = wrap.querySelector('.pop-checklist');
    var meta = wrap.querySelector('[data-checklist-meta]');
    if (!list) return;
    var q = (input.value || '').trim().toLowerCase();
    var labels = list.querySelectorAll('label');
    var shown = 0;
    labels.forEach(function (lbl) {
      var txt = (lbl.textContent || '').trim().toLowerCase();
      var match = !q || txt.indexOf(q) !== -1;
      lbl.classList.toggle('ms-hidden', !match);
      if (match) shown++;
    });
    if (meta) meta.textContent = q ? (shown + ' of ' + labels.length) : '';
  }
  document.addEventListener('input', function (e) {
    var input = e.target.closest('.pop-checklist-search');
    if (!input) return;
    var prev = checklistFilterTimers.get(input);
    if (prev) clearTimeout(prev);
    checklistFilterTimers.set(input, setTimeout(function () {
      applyChecklistFilter(input);
    }, 120));
  });

  // ── Search autocomplete ───────────────────────────────────────────────
  // When the user types inside the search input, detect the current token
  // (by cursor position), parse its `key:value` form, and fetch /logs/autocomplete.
  var acState = { items: [], activeIdx: -1, tokenStart: 0, tokenEnd: 0 };
  var acTimer = null;

  function currentTokenAt(input) {
    var val = input.value;
    var pos = input.selectionStart || 0;
    var before = val.slice(0, pos);
    var after  = val.slice(pos);
    var startIdx = before.search(/\S+$/);
    if (startIdx < 0) startIdx = pos;
    var endIdx = pos + (after.match(/^\S*/) || [''])[0].length;
    return { text: val.slice(startIdx, endIdx), start: startIdx, end: endIdx };
  }

  var FIELD_ALIAS = {
    status: 'status', method: 'method', bot_family: 'bot_family',
    bot_category: 'bot_category', url_group: 'url_group',
    locale: 'locale', country: 'country',
    referer_type: 'referer_type', utm: 'utm_source', utm_source: 'utm_source',
  };

  var SYNTAX_HINTS = [
    { op: 'status:404',   desc: 'only a specific code' },
    { op: 'status:4xx',   desc: 'all 4xx errors (status_class)' },
    { op: 'method:GET',   desc: 'filter by method' },
    { op: 'is:bot',       desc: 'bot traffic only' },
    { op: 'is:human',     desc: 'human traffic only' },
    { op: 'path:/api',    desc: 'path prefix match' },
    { op: 'bot_family:GPTBot', desc: 'filter by bot family' },
    { op: 'utm:chatgpt',  desc: 'UTM source (utm_source alias)' },
    { op: '-utm:direct',  desc: 'negate — exclude this value' },
    { op: '/regex/',      desc: 'free-text regex on path/UA/referer' },
  ];

  function renderSyntaxHints(hint, query) {
    var qLower = (query || '').toLowerCase();
    var rows = SYNTAX_HINTS
      .filter(function (h) { return !qLower || h.op.toLowerCase().includes(qLower); })
      .slice(0, 8)
      .map(function (h, i) {
        return (
          "<div class='sh-row' data-sh-idx='" + i + "' data-sh-insert='" + h.op + "'>" +
            "<code>" + h.op + "</code>" +
            "<span class='sh-desc'>" + h.desc + "</span>" +
          "</div>"
        );
      }).join('');
    hint.innerHTML = (
      "<div class='sh-title'>Filter syntax</div>" +
      (rows || "<div class='sh-row'><span class='sh-desc'>No matching operator.</span></div>")
    );
    hint.style.display = 'block';
    acState.items = Array.from(hint.querySelectorAll('[data-sh-idx]'));
    acState.activeIdx = acState.items.length ? 0 : -1;
    updateHintActive();
  }

  function renderValueSuggestions(hint, field, values, tokenStart, tokenEnd) {
    var rows = values.map(function (v, i) {
      var insert = field + ':' + v;
      return (
        "<div class='sh-row' data-sh-idx='" + i + "' " +
        "data-sh-insert='" + insert + "' data-sh-field='" + field + "' data-sh-value='" + v + "'>" +
          "<code>" + insert + "</code>" +
          "<span class='sh-desc'>" + values.length + " matching</span>" +
        "</div>"
      );
    }).join('');
    hint.innerHTML = (
      "<div class='sh-title'>Suggestions for " + field + "</div>" +
      (rows || "<div class='sh-row'><span class='sh-desc'>No matches.</span></div>")
    );
    hint.style.display = 'block';
    acState.items = Array.from(hint.querySelectorAll('[data-sh-idx]'));
    acState.activeIdx = acState.items.length ? 0 : -1;
    acState.tokenStart = tokenStart;
    acState.tokenEnd = tokenEnd;
    updateHintActive();
  }

  function updateHintActive() {
    acState.items.forEach(function (row, i) {
      row.classList.toggle('active', i === acState.activeIdx);
    });
    if (acState.activeIdx >= 0 && acState.items[acState.activeIdx]) {
      acState.items[acState.activeIdx].scrollIntoView({ block: 'nearest' });
    }
  }

  function applyHintRow(input, row) {
    var insert = row.getAttribute('data-sh-insert') || '';
    var tok = currentTokenAt(input);
    var val = input.value;
    var next = val.slice(0, tok.start) + insert + val.slice(tok.end);
    input.value = next;
    var newPos = tok.start + insert.length;
    input.setSelectionRange(newPos, newPos);
    // Collapse hint; user typically submits next
    var hint = document.querySelector('.search-hint');
    if (hint) hint.style.display = 'none';
  }

  function handleSearchInput(input) {
    var hint = document.querySelector('.search-hint');
    if (!hint) return;
    var tok = currentTokenAt(input);
    var text = tok.text;
    // No token yet → just show syntax hints
    if (!text) {
      renderSyntaxHints(hint, '');
      return;
    }
    // Looks like `key:value` prefix → hit autocomplete
    var colon = text.indexOf(':');
    if (colon > 0) {
      var rawKey = text.slice(0, colon).replace(/^-/, '').toLowerCase();
      var field = FIELD_ALIAS[rawKey];
      if (field) {
        var value = text.slice(colon + 1);
        // Debounce
        clearTimeout(acTimer);
        acTimer = setTimeout(function () {
          var url = new URL(window.location.href);
          var qs = new URLSearchParams({ field: field, q: value, limit: '10' });
          var from = url.searchParams.get('from');
          var to   = url.searchParams.get('to');
          if (from) qs.set('from', from);
          if (to)   qs.set('to', to);
          fetch('/logs/autocomplete?' + qs.toString())
            .then(function (r) { return r.json(); })
            .then(function (j) {
              renderValueSuggestions(hint, rawKey, j.suggestions || [], tok.start, tok.end);
            })
            .catch(function () { /* silent */ });
        }, 120);
        return;
      }
    }
    // Fallback: syntax hints filtered by whatever was typed
    renderSyntaxHints(hint, text);
  }

  function handleSearchKey(e, input) {
    if (e.key === 'ArrowDown') {
      if (!acState.items.length) return;
      e.preventDefault();
      acState.activeIdx = (acState.activeIdx + 1) % acState.items.length;
      updateHintActive();
    } else if (e.key === 'ArrowUp') {
      if (!acState.items.length) return;
      e.preventDefault();
      acState.activeIdx = (acState.activeIdx - 1 + acState.items.length) % acState.items.length;
      updateHintActive();
    } else if (e.key === 'Enter') {
      if (acState.activeIdx >= 0 && acState.items[acState.activeIdx]) {
        e.preventDefault();
        applyHintRow(input, acState.items[acState.activeIdx]);
      }
      // If Enter without active selection, let the <form> submit
    } else if (e.key === 'Tab' && acState.activeIdx >= 0 && acState.items[acState.activeIdx]) {
      e.preventDefault();
      applyHintRow(input, acState.items[acState.activeIdx]);
    }
  }

  // Delegate input listener (search box may re-render after htmx swap)
  document.addEventListener('input', function (e) {
    var inp = e.target.closest('.search-input input');
    if (!inp) return;
    handleSearchInput(inp);
  });

  // Focus → render initial syntax hints
  document.addEventListener('focusin', function (e) {
    var inp = e.target.closest('.search-input input');
    if (!inp) return;
    handleSearchInput(inp);
  });

  // Click on a suggestion row → apply
  document.addEventListener('mousedown', function (e) {
    var row = e.target.closest('.search-hint [data-sh-idx]');
    if (!row) return;
    e.preventDefault();
    var inp = document.querySelector('.search-input input');
    if (inp) applyHintRow(inp, row);
  });

  // ── Drawer ────────────────────────────────────────────────────────────
  function openDrawer() {
    var d = document.getElementById('drawer');
    var bd = document.getElementById('drawer-backdrop');
    if (d) d.classList.add('open');
    if (bd) bd.classList.add('open');
  }
  function closeDrawer() {
    var d = document.getElementById('drawer');
    var bd = document.getElementById('drawer-backdrop');
    if (d) d.classList.remove('open');
    if (bd) bd.classList.remove('open');
    // Clear selected-row highlight
    document.querySelectorAll('.log-table tbody tr.selected').forEach(function (tr) {
      tr.classList.remove('selected');
    });
  }
  window.closeDrawer = closeDrawer; // reachable from inline onclick in fragment

  // Helpers for the drawer's quick-filter buttons. We can't put `new URL(...)`
  // inside the inline `onclick` because inline-handler scope chains through
  // `document` — `URL` resolves to the `document.URL` string property and
  // throws "URL is not a constructor". Calling these via `window.lv…(…)` works
  // because property access bypasses the shadowed identifier lookup.
  window.lvApplyFilter = function (field, value) {
    if (value == null) return;
    var u = new URL(window.location.href);
    u.searchParams.set('search', field + ':' + value);
    u.searchParams.delete('page');
    if (window.htmx) {
      window.htmx.ajax('GET', u.pathname + u.search, { target: '#results', swap: 'innerHTML' });
      window.history.pushState({}, '', u.toString());
    } else {
      window.location.href = u.toString();
    }
    closeDrawer();
  };

  window.lvCopyText = function (text, btn) {
    if (text == null) return;
    function ok() { if (btn) { btn.textContent = 'Copied'; } }
    function fail() { if (btn) { btn.textContent = 'Copy failed'; } }
    if (window.navigator && navigator.clipboard && navigator.clipboard.writeText) {
      navigator.clipboard.writeText(String(text)).then(ok, fail);
      return;
    }
    // Fallback for non-secure contexts: hidden textarea + execCommand
    try {
      var ta = document.createElement('textarea');
      ta.value = String(text);
      ta.setAttribute('readonly', '');
      ta.style.position = 'absolute';
      ta.style.left = '-9999px';
      document.body.appendChild(ta);
      ta.select();
      var success = document.execCommand && document.execCommand('copy');
      document.body.removeChild(ta);
      if (success) ok(); else fail();
    } catch (e) { fail(); }
  };

  // Close the Insights dropdown when a click lands outside it.
  document.addEventListener('click', function (e) {
    document.querySelectorAll('.insights-jump details[open]').forEach(function (d) {
      if (!d.contains(e.target)) d.open = false;
    });
  });

  // View / preset chips are baked into the page chrome and not re-rendered
  // when filters change via htmx. Before the chip's click reaches htmx or the
  // browser, copy from/to from the current URL onto the chip's target so the
  // user's live date range wins over the stale rendered one.
  document.addEventListener('click', function (e) {
    var a = e.target && e.target.closest && e.target.closest('a.view-chip, a.ij-item');
    if (!a) return;
    var href = a.getAttribute('href');
    if (!href) return;
    var curr = new URLSearchParams(window.location.search);
    var from = curr.get('from');
    var to = curr.get('to');
    if (!from && !to) return;
    try {
      var u = new URL(href, window.location.origin);
      if (from) u.searchParams.set('from', from); else u.searchParams.delete('from');
      if (to) u.searchParams.set('to', to); else u.searchParams.delete('to');
      var newHref = u.pathname + u.search;
      a.setAttribute('href', newHref);
      if (a.hasAttribute('hx-get')) a.setAttribute('hx-get', newHref);
    } catch (_) { /* ignore malformed URLs */ }
  }, true);

  // Keep the Rows/Group/Timeseries segmented control in sync with the URL.
  // The control lives outside #results, so it isn't re-rendered on swap — we
  // re-apply the .active class from the current location.
  function syncModeSeg() {
    var seg = document.querySelector('[data-mode-seg]');
    if (!seg) return;
    var mode = new URLSearchParams(window.location.search).get('mode') || 'rows';
    seg.querySelectorAll('a[data-mode-val]').forEach(function (a) {
      a.classList.toggle('active', a.getAttribute('data-mode-val') === mode);
    });
  }

  // When an htmx swap targets #drawer, open the drawer and highlight the row.
  document.body.addEventListener('htmx:afterSwap', function (evt) {
    if (keepColumnsPopoverOpen) {
      // Close every popover except the columns popover so the user can keep
      // toggling visible columns without re-opening it.
      document.querySelectorAll('.popover.is-open').forEach(function (p) {
        if (p.id !== 'pop-columns') p.classList.remove('is-open');
      });
      setPopoverBackdrop(false);
      keepColumnsPopoverOpen = false;
    } else {
      closeAllPopovers();
    }
    if (evt.target && evt.target.id === 'drawer') {
      openDrawer();
    }
    syncModeSeg();
  });

  // Close clicks: data-drawer-close button, or backdrop click
  document.addEventListener('click', function (e) {
    if (e.target.closest('[data-drawer-close]')) {
      e.preventDefault();
      closeDrawer();
      return;
    }
    if (e.target.id === 'drawer-backdrop') {
      closeDrawer();
    }
  });

  // Row click highlighting (complement to row's hx-get)
  document.addEventListener('click', function (e) {
    var tr = e.target.closest('.log-table tbody tr[data-row-ts]');
    if (!tr) return;
    document.querySelectorAll('.log-table tbody tr.selected').forEach(function (o) {
      if (o !== tr) o.classList.remove('selected');
    });
    tr.classList.add('selected');
  });

  // j / k row navigation
  function navigateRow(direction) {
    var rows = Array.from(document.querySelectorAll('.log-table tbody tr[data-row-ts]'));
    if (!rows.length) return;
    var selected = document.querySelector('.log-table tbody tr.selected[data-row-ts]');
    var idx = selected ? rows.indexOf(selected) : -1;
    var next = direction > 0 ? rows[idx + 1] || rows[0] : rows[idx - 1] || rows[rows.length - 1];
    if (!next) return;
    if (selected) selected.classList.remove('selected');
    next.classList.add('selected');
    next.scrollIntoView({ block: 'nearest' });
    if (window.htmx) {
      // Trigger the row's hx-get (fires against #drawer)
      next.click();
    }
  }

  // Drawer head prev/next buttons
  document.addEventListener('click', function (e) {
    var btn = e.target.closest('[data-drawer-nav]');
    if (!btn) return;
    e.preventDefault();
    navigateRow(btn.getAttribute('data-drawer-nav') === 'next' ? 1 : -1);
  });

  // ── Chart metric dropdown: change → navigate to the option's data-href ──
  document.addEventListener('change', function (e) {
    var sel = e.target && e.target.closest && e.target.closest('[data-chart-metric-sel]');
    if (!sel) return;
    var opt = sel.options[sel.selectedIndex];
    if (!opt) return;
    var href = opt.getAttribute('data-href');
    if (href) navigate(href);
  });

  // ── Chart hover tooltip: mouse over .chart-svg hitboxes → show details ──
  (function wireChartHover() {
    function fmtNum(n) { return (n || 0).toLocaleString(); }
    function fmtBytes(n) {
      n = Number(n || 0);
      if (n <= 0) return '0 B';
      var units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB'];
      var i = 0;
      while (n >= 1024 && i < units.length - 1) { n /= 1024; i++; }
      if (n >= 100 || i === 0) return Math.round(n) + ' ' + units[i];
      if (n >= 10) return n.toFixed(1) + ' ' + units[i];
      return n.toFixed(2) + ' ' + units[i];
    }
    function fmtRange(t0, t1) {
      var d0 = new Date(t0);
      var d1 = new Date(t1);
      var spanMin = (t1 - t0) / 60000;
      var opts = spanMin >= 1440
        ? { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' }
        : { hour: '2-digit', minute: '2-digit' };
      var head = spanMin >= 1440
        ? ''
        : d0.toLocaleDateString(undefined, { month: 'short', day: 'numeric' }) + ' · ';
      return head +
        d0.toLocaleTimeString(undefined, opts).replace(',', '') +
        ' – ' +
        d1.toLocaleTimeString(undefined, opts).replace(',', '');
    }
    function findHit(e) {
      var svg = e.target.closest('.chart-svg');
      if (!svg) return null;
      var card = svg.closest('[data-lv2-chart]');
      if (!card) return null;
      var hit = e.target.closest('[data-chart-hitbox]');
      if (!hit) {
        // Mouse may be over the area paths — resolve bucket by x position.
        var vb = svg.viewBox.baseVal;
        var rect = svg.getBoundingClientRect();
        var ratio = vb.width / rect.width;
        var x = (e.clientX - rect.left) * ratio;
        var hits = svg.querySelectorAll('[data-chart-hitbox]');
        if (!hits.length) return null;
        var best = null, bestDx = Infinity;
        for (var i = 0; i < hits.length; i++) {
          var hx = parseFloat(hits[i].getAttribute('x'));
          var hw = parseFloat(hits[i].getAttribute('width'));
          var mid = hx + hw / 2;
          var dx = Math.abs(mid - x);
          if (dx < bestDx) { bestDx = dx; best = hits[i]; }
        }
        hit = best;
      }
      if (!hit) return null;
      return { svg: svg, card: card, hit: hit };
    }
    function showTooltip(ctx, clientX) {
      var card = ctx.card;
      var svg = ctx.svg;
      var hit = ctx.hit;
      var tip = card.querySelector('[data-chart-tooltip]');
      var line = svg.querySelector('[data-chart-hover-line]');
      if (!tip) return;
      var t0 = parseInt(hit.getAttribute('data-t0') || '0', 10);
      var t1 = parseInt(hit.getAttribute('data-t1') || '0', 10);
      var metric = card.getAttribute('data-metric') || 'requests';
      var body = '';
      if (metric === 'bytes') {
        var v = parseInt(hit.getAttribute('data-v') || '0', 10);
        body =
          '<div class="tt-row"><span class="tt-label"><span class="tt-sw" style="background:#c9c4b0"></span>Bytes sent</span>' +
          '<span class="tt-val">' + fmtBytes(v) + '</span></div>';
      } else if (metric === 'status') {
        var STATUS_SERIES = [
          { key: 's2', label: '2xx', fill: '#b7d1b5' },
          { key: 's3', label: '3xx', fill: '#a9bfde' },
          { key: 's4', label: '4xx', fill: '#e6c888' },
          { key: 's5', label: '5xx', fill: '#d6a4a4' },
        ];
        var counts = {};
        var total = 0;
        STATUS_SERIES.forEach(function (s) {
          counts[s.key] = parseInt(hit.getAttribute('data-' + s.key) || '0', 10);
          total += counts[s.key];
        });
        var rows = STATUS_SERIES.map(function (s) {
          return '<div class="tt-row"><span class="tt-label"><span class="tt-sw" style="background:' +
            s.fill + '"></span>' + s.label + '</span>' +
            '<span class="tt-val">' + fmtNum(counts[s.key]) + '</span></div>';
        }).join('');
        body = rows + '<div class="tt-total"><span>Total</span><span>' + fmtNum(total) + '</span></div>';
      } else if (metric === 'bots') {
        var human = parseInt(hit.getAttribute('data-human') || '0', 10);
        var bot = parseInt(hit.getAttribute('data-bot') || '0', 10);
        var tot = human + bot;
        var botPct = tot > 0 ? (bot * 100 / tot) : 0;
        body =
          '<div class="tt-row"><span class="tt-label"><span class="tt-sw" style="background:#b7d1b5"></span>Humans</span>' +
          '<span class="tt-val">' + fmtNum(human) + '</span></div>' +
          '<div class="tt-row"><span class="tt-label"><span class="tt-sw" style="background:#a9bfde"></span>Bots</span>' +
          '<span class="tt-val">' + fmtNum(bot) + '</span></div>' +
          '<div class="tt-total"><span>Bot share</span><span>' + botPct.toFixed(1) + '%</span></div>';
      } else if (metric === 'errors') {
        var pct = parseFloat(hit.getAttribute('data-pct') || '0');
        var err = parseInt(hit.getAttribute('data-err') || '0', 10);
        var den = parseInt(hit.getAttribute('data-den') || '0', 10);
        if (den <= 0) {
          body = '<div class="tt-row"><span class="tt-label">No requests</span><span class="tt-val">—</span></div>';
        } else {
          body =
            '<div class="tt-row"><span class="tt-label"><span class="tt-sw" style="background:#e6c2c2"></span>Error rate</span>' +
            '<span class="tt-val">' + pct.toFixed(1) + '%</span></div>' +
            '<div class="tt-total"><span>Errors / total</span><span>' + fmtNum(err) + ' / ' + fmtNum(den) + '</span></div>';
        }
      } else {
        var n = parseInt(hit.getAttribute('data-v') || '0', 10);
        body =
          '<div class="tt-row"><span class="tt-label"><span class="tt-sw" style="background:#b7d1b5"></span>Requests</span>' +
          '<span class="tt-val">' + fmtNum(n) + '</span></div>';
      }
      tip.innerHTML = '<div class="tt-time">' + fmtRange(t0, t1) + '</div>' + body;

      // Position tooltip relative to card, centered on bucket.
      var cardRect = card.getBoundingClientRect();
      var svgRect = svg.getBoundingClientRect();
      var vb = svg.viewBox.baseVal;
      var ratio = svgRect.width / vb.width;
      var cx = parseFloat(hit.getAttribute('data-cx') || '0') * ratio;
      var leftInCard = (svgRect.left - cardRect.left) + cx;
      // Clamp so tooltip stays inside card.
      var tipW = tip.offsetWidth || 180;
      var minL = tipW / 2 + 4;
      var maxL = cardRect.width - tipW / 2 - 4;
      leftInCard = Math.max(minL, Math.min(maxL, leftInCard));
      var topInCard = (svgRect.top - cardRect.top) + parseFloat(card.getAttribute('data-plot-t') || '10');
      tip.style.left = leftInCard + 'px';
      tip.style.top = topInCard + 'px';
      tip.classList.add('is-visible');
      tip.setAttribute('aria-hidden', 'false');

      // Move vertical hover line to bucket center.
      if (line) {
        var cxSvg = parseFloat(hit.getAttribute('data-cx') || '0');
        line.setAttribute('x1', String(cxSvg));
        line.setAttribute('x2', String(cxSvg));
        svg.classList.add('is-hovering');
      }
    }
    function hideTooltip(card) {
      if (!card) return;
      var tip = card.querySelector('[data-chart-tooltip]');
      if (tip) {
        tip.classList.remove('is-visible');
        tip.setAttribute('aria-hidden', 'true');
      }
      var svg = card.querySelector('.chart-svg');
      if (svg) svg.classList.remove('is-hovering');
    }
    document.addEventListener('mousemove', function (e) {
      var ctx = findHit(e);
      if (!ctx) return;
      showTooltip(ctx, e.clientX);
    });
    document.addEventListener('mouseleave', function (e) {
      var card = e.target && e.target.closest && e.target.closest('[data-lv2-chart]');
      if (card) hideTooltip(card);
    }, true);
    document.addEventListener('mouseout', function (e) {
      var card = e.target.closest('[data-lv2-chart]');
      if (!card) return;
      var to = e.relatedTarget;
      if (!to || !card.contains(to)) hideTooltip(card);
    });
  })();

  // ── Report pages: tab switching (.tab-btn + .tab-panel#tab-<id>) ──────
  (function wireTabs() {
    function activate(tabId) {
      document.querySelectorAll('.tab-btn').forEach(function (b) {
        b.classList.toggle('active', b.getAttribute('data-tab') === tabId);
      });
      document.querySelectorAll('.tab-panel').forEach(function (p) {
        p.classList.toggle('active', p.id === 'tab-' + tabId);
      });
      var url = new URL(window.location);
      url.searchParams.set('tab', tabId);
      window.history.replaceState(null, '', url.toString());
    }
    function init() {
      var btns = document.querySelectorAll('.tab-btn');
      if (!btns.length) return;
      btns.forEach(function (b) {
        b.addEventListener('click', function () {
          activate(b.getAttribute('data-tab'));
        });
      });
      var params = new URLSearchParams(window.location.search);
      var current = params.get('tab');
      if (!current || !document.getElementById('tab-' + current)) {
        current = btns[0].getAttribute('data-tab');
      }
      activate(current);
    }
    if (document.readyState === 'loading') {
      document.addEventListener('DOMContentLoaded', init);
    } else {
      init();
    }
  })();

  // ── Multi-select dropdown (.ms-wrap) — used by /reports/locales ───────
  (function wireMultiSelect() {
    function init(wrap) {
      if (wrap.__lv2Wired) return;
      wrap.__lv2Wired = true;
      var toggle = wrap.querySelector('.ms-toggle');
      var dropdown = wrap.querySelector('.ms-dropdown');
      var hidden = wrap.querySelector('input[type=hidden]');
      if (!toggle || !dropdown || !hidden) return;
      var search = dropdown.querySelector('.ms-search');
      var countEl = dropdown.querySelector('.ms-count');
      var list = dropdown.querySelector('.ms-list');
      var labels = list ? Array.from(list.querySelectorAll('label')) : [];
      var total = labels.length;

      function applyFilter(q) {
        q = (q || '').trim().toLowerCase();
        var shown = 0;
        labels.forEach(function (lbl) {
          var txt = lbl.textContent.trim().toLowerCase();
          var match = !q || txt.indexOf(q) !== -1;
          lbl.classList.toggle('ms-hidden', !match);
          if (match) shown++;
        });
        if (countEl) countEl.textContent = q ? (shown + ' of ' + total) : String(total);
      }
      function updateState() {
        var checked = dropdown.querySelectorAll('input[type=checkbox]:checked');
        var vals = Array.from(checked).map(function (cb) { return cb.value; });
        hidden.value = vals.join(',');
        if (vals.length === 0) toggle.textContent = 'All \u25BE';
        else if (vals.length === 1) toggle.textContent = vals[0] + ' \u25BE';
        else toggle.textContent = vals.length + ' selected \u25BE';
      }

      toggle.addEventListener('click', function (e) {
        e.preventDefault();
        document.querySelectorAll('.ms-dropdown.open').forEach(function (d) {
          if (d !== dropdown) d.classList.remove('open');
        });
        var willOpen = !dropdown.classList.contains('open');
        dropdown.classList.toggle('open');
        if (willOpen && search) {
          search.value = '';
          applyFilter('');
          search.focus();
        }
      });
      dropdown.querySelectorAll('input[type=checkbox]').forEach(function (cb) {
        cb.addEventListener('change', updateState);
      });
      if (search) {
        search.addEventListener('input', function () { applyFilter(search.value); });
        search.addEventListener('click', function (e) { e.stopPropagation(); });
      }
      var selBtn = dropdown.querySelector('.ms-select-visible');
      if (selBtn) selBtn.addEventListener('click', function (e) {
        e.preventDefault(); e.stopPropagation();
        labels.forEach(function (lbl) {
          if (lbl.classList.contains('ms-hidden')) return;
          var cb = lbl.querySelector('input[type=checkbox]');
          if (cb && !cb.checked) cb.checked = true;
        });
        updateState();
      });
      var clrBtn = dropdown.querySelector('.ms-clear-all');
      if (clrBtn) clrBtn.addEventListener('click', function (e) {
        e.preventDefault(); e.stopPropagation();
        labels.forEach(function (lbl) {
          var cb = lbl.querySelector('input[type=checkbox]');
          if (cb && cb.checked) cb.checked = false;
        });
        updateState();
      });
      applyFilter('');
    }
    function initAll() {
      document.querySelectorAll('.ms-wrap').forEach(init);
    }
    document.addEventListener('click', function (e) {
      if (!e.target.closest('.ms-wrap')) {
        document.querySelectorAll('.ms-dropdown.open').forEach(function (d) {
          d.classList.remove('open');
        });
      }
    });
    if (document.readyState === 'loading') {
      document.addEventListener('DOMContentLoaded', initAll);
    } else {
      initAll();
    }
  })();

  // ── Saved views (per-profile saved /logs query state) ────────────────
  // Capture set must mirror server-side _SF_CAPTURE_PARAMS in app/server.py.
  var SV_CAPTURE = [
    'from','to','search','search_mode','search_field',
    'status','status_class','method',
    'is_bot','bot_family','bot_category',
    'url_group','locale','country','referer_type','utm_source',
    'include_assets','content_only',
    'sort','order','mode',
    'group_by','group_by_2','sort_by','bucket','stack_by','limit',
    'columns','view','bt0','bt1',
    'chart','chart_metric','per_page',
    'pf','pf_join'
  ];

  function svRoot() { return document.querySelector('[data-saved-views]'); }

  function svCaptureCurrentParams() {
    var u = new URL(window.location.href);
    var out = {};
    SV_CAPTURE.forEach(function (k) {
      var vals = u.searchParams.getAll(k);
      if (!vals.length) return;
      // Multi-value keys (currently `pf`) become arrays; everything else is scalar.
      if (k === 'pf') out[k] = vals;
      else            out[k] = vals[0];
    });
    return out;
  }

  function svBuildHrefFromParams(params) {
    var u = new URL('/logs', window.location.origin);
    Object.keys(params || {}).forEach(function (k) {
      var v = params[k];
      if (v === null || v === undefined || v === '') return;
      if (Array.isArray(v)) {
        v.forEach(function (item) {
          if (item === null || item === undefined || item === '') return;
          u.searchParams.append(k, String(item));
        });
      } else {
        u.searchParams.set(k, String(v));
      }
    });
    return u.pathname + u.search;
  }

  function svEscapeHtml(s) {
    return String(s == null ? '' : s).replace(/[&<>"']/g, function (c) {
      return ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[c];
    });
  }

  function svRenderItem(item) {
    var lines = item.lines && item.lines.length
      ? item.lines.map(function (l) { return '<li>' + svEscapeHtml(l) + '</li>'; }).join('')
      : "<li class='sv-line-empty'>(no filters — defaults)</li>";
    var href = svBuildHrefFromParams(item.params || {});
    var name = item.name || '';
    return (
      "<li class='sv-item' data-sv-id='" + Number(item.id) + "' " +
      "data-sv-href='" + svEscapeHtml(href) + "'>" +
        "<div class='sv-row-top'>" +
          "<span class='sv-name' data-sv-name>" + svEscapeHtml(name) + "</span>" +
          "<form class='sv-rename-form' data-sv-rename-form hidden>" +
            "<input type='text' data-sv-rename-input value='" + svEscapeHtml(name) + "' maxlength='80' required>" +
            "<button type='submit' class='filter-chip-btn solid'>Save</button>" +
            "<button type='button' class='filter-chip-btn' data-sv-rename-cancel>Cancel</button>" +
            "<span class='sv-rename-msg' data-sv-rename-msg></span>" +
          "</form>" +
          "<div class='sv-actions'>" +
            "<button type='button' class='filter-chip-btn solid' data-sv-apply>Apply</button>" +
            "<button type='button' class='filter-chip-btn' data-sv-rename>Rename</button>" +
            "<button type='button' class='filter-chip-btn' data-sv-delete>Delete</button>" +
          "</div>" +
        "</div>" +
        "<ul class='sv-lines'>" + lines + "</ul>" +
      "</li>"
    );
  }

  function svRenderList(items) {
    var root = svRoot();
    if (!root) return;
    var listEl = root.querySelector('[data-sv-list]');
    if (!listEl) return;
    if (!items || !items.length) {
      listEl.innerHTML = "<li class='sv-empty'>No saved views yet — click “Save current view” to add one.</li>";
    } else {
      listEl.innerHTML = items.map(svRenderItem).join('');
    }
    // Update the count badge in the toggle button.
    var toggle = root.querySelector('[data-sv-toggle]');
    if (toggle) {
      var existing = toggle.querySelector('.count');
      if (existing) existing.remove();
      if (items && items.length) {
        var badge = document.createElement('span');
        badge.className = 'count';
        badge.style.cssText = 'background:var(--accent);color:#fff;border-radius:999px;padding:0 6px;font-size:10px;min-width:16px;text-align:center;margin-left:4px;display:inline-block;';
        badge.textContent = String(items.length);
        // Insert before the trailing chevron span.
        var chevron = toggle.querySelector('span:last-child');
        if (chevron) toggle.insertBefore(badge, chevron);
        else toggle.appendChild(badge);
      }
    }
  }

  function svRefreshList() {
    return fetch('/api/saved-filters', { headers: { 'Accept': 'application/json' } })
      .then(function (r) { return r.ok ? r.json() : { items: [] }; })
      .then(function (j) { svRenderList(j.items || []); })
      .catch(function () { /* leave existing render in place */ });
  }

  function svSetSaveMsg(text, isError) {
    var root = svRoot();
    if (!root) return;
    var el = root.querySelector('[data-sv-save-msg]');
    if (!el) return;
    el.textContent = text || '';
    el.classList.toggle('is-error', !!isError);
  }

  document.addEventListener('click', function (e) {
    var root = svRoot();
    if (!root) return;

    var toggle = e.target.closest('[data-sv-toggle]');
    if (toggle && root.contains(toggle)) {
      e.preventDefault();
      var body = root.querySelector('[data-sv-body]');
      if (!body) return;
      var open = !body.hasAttribute('hidden');
      if (open) {
        body.setAttribute('hidden', '');
        toggle.setAttribute('aria-expanded', 'false');
      } else {
        body.removeAttribute('hidden');
        toggle.setAttribute('aria-expanded', 'true');
      }
      return;
    }

    var openSave = e.target.closest('[data-sv-save-open]');
    if (openSave && root.contains(openSave)) {
      e.preventDefault();
      var form = root.querySelector('[data-sv-save-form]');
      if (!form) return;
      form.removeAttribute('hidden');
      svSetSaveMsg('', false);
      var nameInp = form.querySelector('[data-sv-name]');
      if (nameInp) { nameInp.value = ''; nameInp.focus(); }
      return;
    }

    var cancelSave = e.target.closest('[data-sv-save-cancel]');
    if (cancelSave && root.contains(cancelSave)) {
      e.preventDefault();
      var form2 = root.querySelector('[data-sv-save-form]');
      if (form2) form2.setAttribute('hidden', '');
      svSetSaveMsg('', false);
      return;
    }

    var applyBtn = e.target.closest('[data-sv-apply]');
    if (applyBtn && root.contains(applyBtn)) {
      e.preventDefault();
      var item = applyBtn.closest('[data-sv-id]');
      if (!item) return;
      var href = item.getAttribute('data-sv-href') || '/logs';
      navigate(href);
      return;
    }

    var renameBtn = e.target.closest('[data-sv-rename]');
    if (renameBtn && root.contains(renameBtn)) {
      e.preventDefault();
      var item0 = renameBtn.closest('[data-sv-id]');
      if (!item0) return;
      var form0 = item0.querySelector('[data-sv-rename-form]');
      var nameSpan0 = item0.querySelector('[data-sv-name]');
      var actions0 = item0.querySelector('.sv-actions');
      if (!form0) return;
      form0.removeAttribute('hidden');
      if (nameSpan0) nameSpan0.setAttribute('hidden', '');
      if (actions0) actions0.setAttribute('hidden', '');
      var msg0 = form0.querySelector('[data-sv-rename-msg]');
      if (msg0) { msg0.textContent = ''; msg0.classList.remove('is-error'); }
      var inp0 = form0.querySelector('[data-sv-rename-input]');
      if (inp0) { inp0.focus(); inp0.select(); }
      return;
    }

    var renameCancel = e.target.closest('[data-sv-rename-cancel]');
    if (renameCancel && root.contains(renameCancel)) {
      e.preventDefault();
      var item1 = renameCancel.closest('[data-sv-id]');
      if (!item1) return;
      var form1 = item1.querySelector('[data-sv-rename-form]');
      var nameSpan1 = item1.querySelector('[data-sv-name]');
      var actions1 = item1.querySelector('.sv-actions');
      if (form1) form1.setAttribute('hidden', '');
      if (nameSpan1) nameSpan1.removeAttribute('hidden');
      if (actions1) actions1.removeAttribute('hidden');
      // Reset input value to the current name in case the user typed.
      var inp1 = form1 && form1.querySelector('[data-sv-rename-input]');
      if (inp1 && nameSpan1) inp1.value = nameSpan1.textContent || '';
      return;
    }

    var delBtn = e.target.closest('[data-sv-delete]');
    if (delBtn && root.contains(delBtn)) {
      e.preventDefault();
      var item2 = delBtn.closest('[data-sv-id]');
      if (!item2) return;
      var name = (item2.querySelector('.sv-name') || {}).textContent || 'this view';
      if (!window.confirm('Delete saved view “' + name + '”?')) return;
      var id = item2.getAttribute('data-sv-id');
      fetch('/api/saved-filters/' + encodeURIComponent(id), { method: 'DELETE' })
        .then(function (r) {
          if (r.ok) svRefreshList();
          else      window.alert('Failed to delete saved view.');
        });
      return;
    }
  });

  document.addEventListener('submit', function (e) {
    // Rename form (per-item)
    var renameForm = e.target.closest('[data-sv-rename-form]');
    if (renameForm) {
      e.preventDefault();
      var renameItem = renameForm.closest('[data-sv-id]');
      if (!renameItem) return;
      var renameInp = renameForm.querySelector('[data-sv-rename-input]');
      var renameMsg = renameForm.querySelector('[data-sv-rename-msg]');
      var newName = renameInp ? (renameInp.value || '').trim() : '';
      function setRenameMsg(t, isErr) {
        if (!renameMsg) return;
        renameMsg.textContent = t || '';
        renameMsg.classList.toggle('is-error', !!isErr);
      }
      if (!newName) {
        setRenameMsg('Name is required', true);
        if (renameInp) renameInp.focus();
        return;
      }
      var renameId = renameItem.getAttribute('data-sv-id');
      setRenameMsg('Saving…', false);
      fetch('/api/saved-filters/' + encodeURIComponent(renameId), {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json', 'Accept': 'application/json' },
        body: JSON.stringify({ name: newName })
      }).then(function (r) {
        return r.json().then(function (j) { return { ok: r.ok, status: r.status, body: j }; });
      }).then(function (res) {
        if (!res.ok) {
          setRenameMsg((res.body && res.body.error) || 'Rename failed', true);
          return;
        }
        svRefreshList();
      }).catch(function () {
        setRenameMsg('Network error', true);
      });
      return;
    }

    // Save (new view) form
    var form = e.target.closest('[data-sv-save-form]');
    if (!form) return;
    e.preventDefault();
    var root = svRoot();
    if (!root) return;
    var nameInp = form.querySelector('[data-sv-name]');
    var name = nameInp ? (nameInp.value || '').trim() : '';
    if (!name) {
      svSetSaveMsg('Name is required', true);
      if (nameInp) nameInp.focus();
      return;
    }
    var params = svCaptureCurrentParams();
    svSetSaveMsg('Saving…', false);
    fetch('/api/saved-filters', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Accept': 'application/json' },
      body: JSON.stringify({ name: name, params: params })
    }).then(function (r) {
      return r.json().then(function (j) { return { ok: r.ok, status: r.status, body: j }; });
    }).then(function (res) {
      if (!res.ok) {
        svSetSaveMsg((res.body && res.body.error) || 'Save failed', true);
        return;
      }
      svSetSaveMsg('Saved.', false);
      form.setAttribute('hidden', '');
      // Open the dropdown so the user sees their new entry.
      var body = root.querySelector('[data-sv-body]');
      if (body) body.removeAttribute('hidden');
      var toggle = root.querySelector('[data-sv-toggle]');
      if (toggle) toggle.setAttribute('aria-expanded', 'true');
      svRefreshList();
    }).catch(function () {
      svSetSaveMsg('Network error', true);
    });
  });

  // ── Heatmap filter popover (/reports/locales heatmap tab) ─────────────
  // Self-contained. Trigger toggles the modal + a backdrop. Apply collects
  // checked values per data-pop-field, writes them into the hidden form's
  // data-hm-input mirrors, and submits the form (plain GET — no htmx here).
  (function () {
    var trigger  = document.querySelector('[data-hm-toggle]');
    var popover  = document.querySelector('[data-hm-popover]');
    var backdrop = document.querySelector('[data-hm-backdrop]');
    var form     = document.querySelector('[data-hm-form]');
    if (!trigger || !popover || !backdrop || !form) return;

    function setOpen(open) {
      popover.classList.toggle('is-open', open);
      backdrop.classList.toggle('is-open', open);
      trigger.setAttribute('aria-expanded', open ? 'true' : 'false');
      if (open) {
        var firstSearch = popover.querySelector('.pop-checklist-search');
        if (firstSearch) firstSearch.focus();
      }
    }

    trigger.addEventListener('click', function (e) {
      e.preventDefault();
      // Stop bubbling so the global "outside-click closes popovers" handler
      // (logviewer.js click delegate) doesn't immediately re-close us. That
      // delegate only spares clicks inside `.popover` or `[data-popover-trigger]`,
      // and our trigger uses neither.
      e.stopPropagation();
      setOpen(!popover.classList.contains('is-open'));
    });

    popover.querySelectorAll('[data-hm-close]').forEach(function (btn) {
      btn.addEventListener('click', function (e) {
        e.preventDefault();
        setOpen(false);
      });
    });

    backdrop.addEventListener('click', function () { setOpen(false); });

    document.addEventListener('keydown', function (e) {
      if (e.key === 'Escape' && popover.classList.contains('is-open')) {
        setOpen(false);
      }
    });

    // Type-to-filter — debounced 120ms, mirrors the .ms-wrap pattern.
    popover.querySelectorAll('.pop-checklist-wrap').forEach(function (wrap) {
      var search = wrap.querySelector('.pop-checklist-search');
      var list   = wrap.querySelector('[data-pop-checklist]');
      var meta   = wrap.querySelector('[data-pop-checklist-meta]');
      if (!search || !list) return;
      var labels = Array.from(list.querySelectorAll('label'));
      var total  = labels.length;
      function applyFilter(q) {
        q = (q || '').trim().toLowerCase();
        var shown = 0;
        labels.forEach(function (lbl) {
          var match = !q || lbl.textContent.trim().toLowerCase().indexOf(q) !== -1;
          lbl.classList.toggle('ms-hidden', !match);
          if (match) shown++;
        });
        if (meta) meta.textContent = q ? (shown + ' of ' + total + ' shown') : '';
      }
      var timer = null;
      search.addEventListener('input', function () {
        if (timer) clearTimeout(timer);
        timer = setTimeout(function () { applyFilter(search.value); }, 120);
      });
    });

    // Per-field Select all (respects current type-filter — only ticks visible
    // labels) and Clear (un-ticks every checkbox in the field).
    popover.querySelectorAll('[data-pop-select-all]').forEach(function (btn) {
      btn.addEventListener('click', function (e) {
        e.preventDefault();
        var field = btn.getAttribute('data-pop-field');
        var list = popover.querySelector('[data-pop-field-list="' + field + '"]');
        if (!list) return;
        list.querySelectorAll('label').forEach(function (lbl) {
          if (lbl.classList.contains('ms-hidden')) return;
          var cb = lbl.querySelector('input[type=checkbox]');
          if (cb) cb.checked = true;
        });
      });
    });
    popover.querySelectorAll('[data-pop-clear-field]').forEach(function (btn) {
      btn.addEventListener('click', function (e) {
        e.preventDefault();
        var field = btn.getAttribute('data-pop-field');
        popover.querySelectorAll('[data-pop-field="' + field + '"]').forEach(function (cb) {
          cb.checked = false;
        });
      });
    });

    // Footer "Clear" — un-ticks every checkbox in the popover (all fields).
    var clearAllBtn = popover.querySelector('[data-hm-clear-all]');
    if (clearAllBtn) {
      clearAllBtn.addEventListener('click', function (e) {
        e.preventDefault();
        popover.querySelectorAll('[data-pop-field]').forEach(function (cb) {
          cb.checked = false;
        });
      });
    }

    // Apply: read checked state per data-pop-field, set hidden inputs, submit.
    var applyBtn = popover.querySelector('[data-hm-apply]');
    if (applyBtn) {
      applyBtn.addEventListener('click', function (e) {
        e.preventDefault();
        var fields = new Set();
        popover.querySelectorAll('[data-pop-field]').forEach(function (cb) {
          fields.add(cb.getAttribute('data-pop-field'));
        });
        fields.forEach(function (field) {
          var inp = form.querySelector('[data-hm-input="' + field + '"]');
          if (!inp) return;
          var checked = popover.querySelectorAll(
            '[data-pop-field="' + field + '"]:checked'
          );
          var value = '';
          if (field === 'content_only') {
            value = checked.length ? 'true' : '';
          } else {
            var total = popover.querySelectorAll(
              '[data-pop-field="' + field + '"]'
            ).length;
            // 0 OR all checked → no narrowing.
            if (checked.length > 0 && checked.length < total) {
              value = Array.from(checked).map(function (cb) { return cb.value; }).join(',');
            }
          }
          inp.value = value;
          // Skip the input entirely when empty so the URL stays clean.
          inp.disabled = (value === '');
        });
        form.submit();
      });
    }
  })();

  // Re-run htmx process on drawer inserts (it happens via hx-swap on tr)
  document.body.addEventListener('htmx:beforeRequest', function () {
    // no-op placeholder — kept for future hooks
  });
})();
