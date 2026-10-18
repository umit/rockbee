(() => {
  const preview = document.querySelector('[data-console-preview]');
  if (!preview) return;

  const tabs = [...preview.querySelectorAll('[data-console-tab]')];
  const panels = [...preview.querySelectorAll('[data-console-panel]')];

  const select = (name, focus = false) => {
    tabs.forEach((tab) => {
      const selected = tab.dataset.consoleTab === name;
      tab.setAttribute('aria-selected', String(selected));
      tab.tabIndex = selected ? 0 : -1;
      if (selected && focus) tab.focus();
    });
    panels.forEach((panel) => {
      panel.hidden = panel.dataset.consolePanel !== name;
    });
  };

  tabs.forEach((tab, index) => {
    tab.addEventListener('click', () => select(tab.dataset.consoleTab));
    tab.addEventListener('keydown', (event) => {
      if (!['ArrowLeft', 'ArrowRight', 'Home', 'End'].includes(event.key)) return;
      event.preventDefault();
      const nextIndex = event.key === 'Home' ? 0 : event.key === 'End' ? tabs.length - 1 : (index + (event.key === 'ArrowRight' ? 1 : -1) + tabs.length) % tabs.length;
      select(tabs[nextIndex].dataset.consoleTab, true);
    });
  });
})();
