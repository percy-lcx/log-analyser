// Shared client-side helpers loaded on every page (logs, reports, settings).
// Page-specific behavior still lives in logviewer.js for /logs and /reports/*.
// Single click-outside + Escape handlers below; logviewer.js delegates to
// the same data-popover-trigger contract so behavior stays consistent.
(function () {
  'use strict';

  function closeAllAnchored() {
    document.querySelectorAll('.popover.is-open[data-anchored]').forEach(function (p) {
      p.classList.remove('is-open');
    });
  }

  function position(trigger, popover, placement) {
    var r = trigger.getBoundingClientRect();
    var top = r.bottom + 4;
    var left = r.left;
    if (placement === 'bottom-end') {
      left = r.right - popover.offsetWidth;
    } else if (placement === 'top-start') {
      top = r.top - popover.offsetHeight - 4;
    }
    popover.style.position = 'fixed';
    popover.style.top = top + 'px';
    popover.style.left = Math.max(8, left) + 'px';
  }

  // attachPopover(triggerEl, popoverEl, {placement}) — wires up an anchored
  // popover. Returns {open, close, toggle}. Uses the shared close-on-outside
  // listener registered below.
  window.attachPopover = function (trigger, popover, opts) {
    if (!trigger || !popover) return null;
    var placement = (opts && opts.placement) || 'bottom-start';
    popover.setAttribute('data-anchored', '');
    function open() {
      closeAllAnchored();
      popover.classList.add('is-open');
      position(trigger, popover, placement);
    }
    function close() { popover.classList.remove('is-open'); }
    function toggle() {
      if (popover.classList.contains('is-open')) close(); else open();
    }
    trigger.addEventListener('click', function (e) {
      e.preventDefault(); e.stopPropagation();
      toggle();
    });
    return { open: open, close: close, toggle: toggle };
  };

  document.addEventListener('click', function (e) {
    if (e.target.closest('.popover[data-anchored]')) return;
    closeAllAnchored();
  });
  document.addEventListener('keydown', function (e) {
    if (e.key === 'Escape') closeAllAnchored();
  });

  // ── Table enhancer (sort + client-side pagination) ─────────────────────
  // Hooks every table.log-table.sortable. Server-paginated tables only get
  // their header click handler (toggles ?sort=&order=); client tables cache
  // rows and slice on sort/page change. Pager controls render into a sibling
  // .pager element if rows exceed PER_PAGE_PRESETS[0].
  var BYTE_UNITS = { B: 1, KB: 1024, MB: 1048576, GB: 1073741824, TB: 1099511627776 };
  var DATE_RE = /^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}(:\d{2})?)?$/;
  var PER_PAGE_PRESETS = [25, 50, 100, 200];
  var DEFAULT_PER_PAGE = 50;

  function parseNumber(s) {
    var cleaned = (s || '').replace(/[,\s]/g, '').replace(/%$/, '');
    if (!cleaned) return null;
    var bm = cleaned.match(/^([\d.]+)(B|KB|MB|GB|TB)$/i);
    if (bm) {
      var n = parseFloat(bm[1]);
      var mult = BYTE_UNITS[bm[2].toUpperCase()] || 1;
      return Number.isFinite(n) ? n * mult : null;
    }
    var v = Number(cleaned);
    return Number.isFinite(v) ? v : null;
  }
  function cellValue(tr, idx) {
    var c = tr.cells[idx];
    return ((c && c.innerText) || '').trim();
  }
  function cmpValues(a, b) {
    var na = parseNumber(a), nb = parseNumber(b);
    if (na !== null && nb !== null) return na - nb;
    if (DATE_RE.test(a) && DATE_RE.test(b)) {
      var da = Date.parse(a), db = Date.parse(b);
      if (!Number.isNaN(da) && !Number.isNaN(db)) return da - db;
    }
    return a.localeCompare(b);
  }
  function enhanceTable(table) {
    if (table.dataset.enhanced === '1') return;
    table.dataset.enhanced = '1';
    var tbody = table.tBodies[0];
    if (!tbody) return;
    var headers = table.tHead ? Array.from(table.tHead.rows[0].cells) : [];
    var serverPaginated = table.classList.contains('server-paginated');
    if (serverPaginated) {
      var url = new URL(window.location);
      var curSort = url.searchParams.get('sort');
      var curOrder = (url.searchParams.get('order') || 'desc').toLowerCase();
      headers.forEach(function (th) {
        var col = th.getAttribute('data-sort-col');
        if (col && col === curSort) th.classList.add('sorted');
        th.addEventListener('click', function () {
          if (!col) return;
          var u = new URL(window.location);
          var prevSort = u.searchParams.get('sort');
          var prevOrder = (u.searchParams.get('order') || 'desc').toLowerCase();
          var nextOrder = (prevSort === col && prevOrder === 'asc') ? 'desc' : 'asc';
          u.searchParams.set('sort', col);
          u.searchParams.set('order', nextOrder);
          u.searchParams.set('page', '1');
          window.location.assign(u.toString());
        });
      });
      return;
    }
    var state = {
      rows: Array.from(tbody.rows),
      sortIdx: null,
      sortAsc: true,
      page: 1,
      perPage: DEFAULT_PER_PAGE,
    };
    headers.forEach(function (th, idx) {
      th.addEventListener('click', function () {
        var asc = !(state.sortIdx === idx && state.sortAsc);
        state.sortIdx = idx;
        state.sortAsc = asc;
        state.rows.sort(function (ra, rb) {
          var r = cmpValues(cellValue(ra, idx), cellValue(rb, idx));
          return asc ? r : -r;
        });
        headers.forEach(function (h) { h.classList.remove('sorted'); });
        th.classList.add('sorted');
        var arrow = th.querySelector('.sort-arrow');
        if (arrow) arrow.textContent = asc ? '▲' : '▼';
        state.page = 1;
        render();
      });
    });
    var wrap = table.closest('.table-wrap');
    var pager = wrap ? wrap.querySelector('.pager[data-pager-client]') : null;
    var total = state.rows.length;
    if (total <= PER_PAGE_PRESETS[0] || !wrap) {
      render();
      return;
    }
    if (!pager) {
      pager = document.createElement('div');
      pager.className = 'pager';
      pager.setAttribute('data-pager-client', '');
      wrap.appendChild(pager);
    }
    var ppOptions = PER_PAGE_PRESETS.map(function (n) {
      return '<option value="' + n + '">' + n + '</option>';
    }).join('');
    pager.innerHTML =
      '<span class="pager-count"></span>' +
      '<div class="pager-right">' +
        '<label>Rows <select class="tc-pp">' + ppOptions + '</select></label>' +
        '<button type="button" data-nav="first" title="First">«</button>' +
        '<button type="button" data-nav="prev" title="Prev">‹</button>' +
        '<span class="pager-pages"><label>Page <input type="number" class="tc-page" min="1" value="1"> of <span class="tc-total-pages"></span></label></span>' +
        '<button type="button" data-nav="next" title="Next">›</button>' +
        '<button type="button" data-nav="last" title="Last">»</button>' +
      '</div>';
    var ppSel = pager.querySelector('.tc-pp');
    ppSel.value = String(state.perPage);
    ppSel.addEventListener('change', function () {
      var v = parseInt(ppSel.value, 10);
      state.perPage = Number.isFinite(v) && v > 0 ? v : DEFAULT_PER_PAGE;
      state.page = 1;
      render();
    });
    pager.querySelectorAll('[data-nav]').forEach(function (btn) {
      btn.addEventListener('click', function () {
        var tp = Math.max(1, Math.ceil(state.rows.length / state.perPage));
        var nav = btn.getAttribute('data-nav');
        if (nav === 'first') state.page = 1;
        else if (nav === 'prev') state.page = Math.max(1, state.page - 1);
        else if (nav === 'next') state.page = Math.min(tp, state.page + 1);
        else if (nav === 'last') state.page = tp;
        render();
      });
    });
    var pageInput = pager.querySelector('.tc-page');
    function commitPage() {
      var tp = Math.max(1, Math.ceil(state.rows.length / state.perPage));
      var v = parseInt(pageInput.value, 10);
      if (!Number.isFinite(v) || v < 1) v = 1;
      if (v > tp) v = tp;
      state.page = v;
      render();
    }
    pageInput.addEventListener('change', commitPage);
    pageInput.addEventListener('keydown', function (e) {
      if (e.key === 'Enter') { e.preventDefault(); commitPage(); }
    });
    render();
    function render() {
      var t = state.rows.length;
      var tp = Math.max(1, Math.ceil(t / state.perPage));
      if (state.page > tp) state.page = tp;
      var start = (state.page - 1) * state.perPage;
      var end = Math.min(t, start + state.perPage);
      tbody.replaceChildren.apply(tbody, state.rows.slice(start, end));
      if (!pager) return;
      pager.querySelector('.pager-count').textContent =
        t.toLocaleString() + ' rows · showing ' + (start + 1).toLocaleString() + '–' + end.toLocaleString();
      pager.querySelector('.tc-total-pages').textContent = tp.toLocaleString();
      pageInput.value = String(state.page);
      pageInput.max = String(tp);
      pager.querySelector('[data-nav="first"]').disabled = state.page <= 1;
      pager.querySelector('[data-nav="prev"]').disabled = state.page <= 1;
      pager.querySelector('[data-nav="next"]').disabled = state.page >= tp;
      pager.querySelector('[data-nav="last"]').disabled = state.page >= tp;
    }
  }
  function enhanceAllTables(root) {
    (root || document).querySelectorAll('table.log-table.sortable').forEach(enhanceTable);
  }
  window.enhanceAllTables = enhanceAllTables;
  document.addEventListener('DOMContentLoaded', function () { enhanceAllTables(); });
  if (document.body) {
    document.body.addEventListener('htmx:afterSettle', function (e) {
      enhanceAllTables(e.target || document);
    });
  }
})();
