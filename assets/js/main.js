/* ============================================================
   AirportAccidents.com — main.js
   Shared interactions across all pages
   ============================================================ */

(function () {
  'use strict';

  /* ── STICKY BAR ─────────────────────────────────────────── */
  function initStickyBar() {
    const bar = document.getElementById('sticky-bar');
    if (!bar) return;
    const threshold = window.innerHeight * 0.85;
    let visible = false;
    function update() {
      const should = window.scrollY > threshold;
      if (should === visible) return;
      visible = should;
      bar.classList.toggle('sticky-bar--visible', visible);
    }
    window.addEventListener('scroll', update, { passive: true });
    update();
  }

  /* ── SMOOTH SCROLL ──────────────────────────────────────── */
  function initSmoothScroll() {
    document.addEventListener('click', function (e) {
      const link = e.target.closest('a[href^="#"]');
      if (!link) return;
      const target = document.querySelector(link.getAttribute('href'));
      if (!target) return;
      e.preventDefault();
      const navH = document.querySelector('nav') ? 72 : 0;
      const top = target.getBoundingClientRect().top + window.scrollY - navH;
      window.scrollTo({ top, behavior: 'smooth' });
    });
  }

  /* ── NAV SCROLL STATE ───────────────────────────────────── */
  function initNavScroll() {
    const nav = document.querySelector('nav');
    if (!nav) return;
    function update() {
      nav.classList.toggle('nav--scrolled', window.scrollY > 24);
    }
    window.addEventListener('scroll', update, { passive: true });
    update();
  }

  /* ── MOBILE NAV ─────────────────────────────────────────── */
  function initMobileNav() {
    const toggle = document.getElementById('nav-toggle');
    const menu = document.getElementById('nav-menu');
    if (!toggle || !menu) return;
    toggle.addEventListener('click', function () {
      const open = menu.classList.toggle('nav-menu--open');
      toggle.setAttribute('aria-expanded', String(open));
      document.body.style.overflow = open ? 'hidden' : '';
    });
    document.addEventListener('click', function (e) {
      if (!menu.contains(e.target) && !toggle.contains(e.target)) {
        menu.classList.remove('nav-menu--open');
        toggle.setAttribute('aria-expanded', 'false');
        document.body.style.overflow = '';
      }
    });
    document.addEventListener('keydown', function (e) {
      if (e.key === 'Escape') {
        menu.classList.remove('nav-menu--open');
        toggle.setAttribute('aria-expanded', 'false');
        document.body.style.overflow = '';
      }
    });
  }

  /* ── LEAD FORMS ─────────────────────────────────────────── */
  function initLeadForms() {
    document.querySelectorAll('.lead-form').forEach(function (form) {

      // "Other airport" reveals text input
      const airportSelect = form.querySelector('[data-role="airport-select"]');
      if (airportSelect) {
        airportSelect.addEventListener('change', function () {
          let txt = form.querySelector('[data-role="airport-text"]');
          if (this.value === 'other') {
            if (!txt) {
              txt = document.createElement('input');
              txt.type = 'text';
              txt.placeholder = 'Type your airport name or city...';
              txt.setAttribute('data-role', 'airport-text');
              txt.className = 'form__input';
              this.closest('.form__group').appendChild(txt);
            }
            txt.style.display = 'block';
            txt.focus();
          } else if (txt) {
            txt.style.display = 'none';
          }
        });
      }

      // Phone auto-format
      const phoneInput = form.querySelector('[data-role="phone"]');
      if (phoneInput) {
        phoneInput.addEventListener('input', function () {
          let d = this.value.replace(/\D/g, '').slice(0, 10);
          if (d.length >= 7)      this.value = '(' + d.slice(0,3) + ') ' + d.slice(3,6) + '-' + d.slice(6);
          else if (d.length >= 4) this.value = '(' + d.slice(0,3) + ') ' + d.slice(3);
          else if (d.length > 0)  this.value = '(' + d;
        });
      }

      // Submit
      form.addEventListener('submit', function (e) {
        e.preventDefault();
        const btn = form.querySelector('[data-role="submit"]');
        const phone = phoneInput ? phoneInput.value.replace(/\D/g, '') : '0000000000';
        if (phoneInput && phone.length < 10) {
          phoneInput.classList.add('form__input--error');
          phoneInput.focus();
          return;
        }
        if (phoneInput) phoneInput.classList.remove('form__input--error');

        const payload = {
          airport:       form.dataset.airport || '',
          accidentType:  form.dataset.accidentType || '',
          phone:         phone,
          timestamp:     new Date().toISOString(),
          page:          window.location.pathname,
        };
        form.querySelectorAll('select,input,textarea').forEach(function (el) {
          if (el.name) payload[el.name] = el.value;
        });

        if (btn) { btn.textContent = 'Submitting...'; btn.disabled = true; }

        fetch('/api/leads', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload),
        })
        .catch(function () {})
        .finally(function () { showFormSuccess(form); });
      });
    });
  }

  function showFormSuccess(form) {
    const success = form.querySelector('[data-role="success"]');
    if (success) {
      const fields = form.querySelector('[data-role="fields"]');
      if (fields) fields.style.display = 'none';
      success.style.display = 'block';
    } else {
      form.innerHTML =
        '<div class="form__success">' +
        '<div class="form__success-icon">✓</div>' +
        '<h3 class="form__success-title">Case Review Submitted</h3>' +
        '<p class="form__success-text">An attorney will review your case and contact you within 24 hours.</p>' +
        '</div>';
    }
  }

  /* ── PHONE CLICK TRACKING ───────────────────────────────── */
  function initPhoneTracking() {
    document.querySelectorAll('a[href^="tel:"]').forEach(function (link) {
      link.addEventListener('click', function () {
        if (typeof gtag !== 'undefined') {
          gtag('event', 'phone_click', { event_category: 'engagement', event_label: window.location.pathname });
        }
      });
    });
  }

  /* ── INIT ────────────────────────────────────────────────── */
  function init() {
    initStickyBar();
    initSmoothScroll();
    initNavScroll();
    initMobileNav();
    initLeadForms();
    initPhoneTracking();
  }

  document.readyState === 'loading'
    ? document.addEventListener('DOMContentLoaded', init)
    : init();

}());

// ── FAQ Accordion ──────────────────────────────────────────────────────────
(function() {
  document.querySelectorAll('.faq-item__btn').forEach(function(btn) {
    btn.addEventListener('click', function() {
      var expanded = btn.getAttribute('aria-expanded') === 'true';
      var bodyId   = btn.getAttribute('aria-controls');
      var body     = document.getElementById(bodyId);
      btn.setAttribute('aria-expanded', !expanded);
      if (body) body.hidden = expanded;
    });
  });
})();
