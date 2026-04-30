/* =============================================================================
   AIRPORTACCIDENTS.COM — MAIN.JS
   Shared interactions for all pages. No dependencies.
   ============================================================================= */

(function () {
  'use strict';

  /* --- Sticky bar on scroll ------------------------------------------------ */
  const stickyBar = document.querySelector('.sticky-bar');
  if (stickyBar) {
    const threshold = window.innerHeight * 0.85;
    const toggleSticky = () => {
      stickyBar.style.display = window.scrollY > threshold ? 'flex' : 'none';
    };
    window.addEventListener('scroll', toggleSticky, { passive: true });
  }

  /* --- Smooth scroll for all anchor links ---------------------------------- */
  document.querySelectorAll('a[href^="#"]').forEach(function (anchor) {
    anchor.addEventListener('click', function (e) {
      const target = document.querySelector(this.getAttribute('href'));
      if (target) {
        e.preventDefault();
        const offset = 72; // nav height
        const top = target.getBoundingClientRect().top + window.scrollY - offset;
        window.scrollTo({ top: top, behavior: 'smooth' });
      }
    });
  });

  /* --- Form: show text input if "other" airport selected ------------------- */
  const airportSelects = document.querySelectorAll('[data-airport-select]');
  airportSelects.forEach(function (select) {
    select.addEventListener('change', function () {
      const group = this.closest('.form-group');
      const existing = group.querySelector('.airport-text-input');
      if (this.value === 'other') {
        if (!existing) {
          const input = document.createElement('input');
          input.type = 'text';
          input.placeholder = 'Type your airport name or city...';
          input.className = 'form-group__input airport-text-input';
          input.style.marginTop = '8px';
          group.appendChild(input);
          input.focus();
        }
      } else if (existing) {
        existing.remove();
      }
    });
  });

  /* --- Form submission (stub — replace with real endpoint) ----------------- */
  const forms = document.querySelectorAll('[data-lead-form]');
  forms.forEach(function (form) {
    form.addEventListener('submit', function (e) {
      e.preventDefault();
      const btn = form.querySelector('.form__submit');
      if (btn) {
        btn.textContent = 'Submitting...';
        btn.disabled = true;
      }
      // TODO: replace with real fetch() to CRM endpoint
      setTimeout(function () {
        form.innerHTML = `
          <div style="text-align:center;padding:32px 0;">
            <div style="font-size:32px;margin-bottom:12px;">✓</div>
            <p style="font-size:16px;font-weight:700;color:#0A1628;margin-bottom:8px;">Case Review Submitted</p>
            <p style="font-size:13px;color:#5C6478;line-height:1.6;">An attorney will contact you within 24 hours. Check your phone for a confirmation text.</p>
          </div>
        `;
      }, 1200);
    });
  });

  /* --- Nav active state ---------------------------------------------------- */
  const currentPath = window.location.pathname;
  document.querySelectorAll('.nav__link').forEach(function (link) {
    if (link.getAttribute('href') === currentPath) {
      link.setAttribute('aria-current', 'page');
      link.style.color = 'var(--color-gold-300)';
    }
  });

})();
