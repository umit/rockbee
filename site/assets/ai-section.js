"use strict";
const aiTabs = [...document.querySelectorAll('[data-ai-tab]')];
function showAiTab(tab, moveFocus = false) {
  aiTabs.forEach(item => {
    const selected = item === tab;
    item.setAttribute('aria-selected', String(selected));
    item.tabIndex = selected ? 0 : -1;
    document.getElementById(item.getAttribute('aria-controls')).hidden = !selected;
  });
  if (moveFocus) tab.focus();
}
aiTabs.forEach((tab, index) => {
  tab.addEventListener('click', () => showAiTab(tab));
  tab.addEventListener('keydown', event => {
    let next;
    if (event.key === 'ArrowRight') next = (index + 1) % aiTabs.length;
    if (event.key === 'ArrowLeft') next = (index + aiTabs.length - 1) % aiTabs.length;
    if (event.key === 'Home') next = 0;
    if (event.key === 'End') next = aiTabs.length - 1;
    if (next !== undefined) { event.preventDefault(); showAiTab(aiTabs[next], true); }
  });
});
