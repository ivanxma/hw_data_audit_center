'use strict';
(() => {
  document.querySelectorAll('form').forEach(form => {
    const mode = form.querySelector('select[name="mode"]');
    if (!mode) return;
    const update = () => form.querySelectorAll('[data-capture-field]').forEach(label => {
      const selected = label.dataset.captureField === mode.value;
      label.hidden = !selected;
      const input = label.querySelector('input,textarea');
      input.disabled = !selected;
      input.required = selected;
    });
    mode.addEventListener('change', update);
    update();
  });
})();
