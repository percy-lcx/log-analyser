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
})();
