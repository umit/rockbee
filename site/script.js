"use strict";

// Presentation controls only. The examples do not connect to a live database.
const apiTabs = [...document.querySelectorAll('[data-api]')];
function activateApi(tab, focus = false) {
  apiTabs.forEach(item => {
    const selected = item === tab;
    item.setAttribute('aria-selected', String(selected));
    item.tabIndex = selected ? 0 : -1;
    document.getElementById(item.getAttribute('aria-controls')).hidden = !selected;
  });
  if (focus) tab.focus();
}
apiTabs.forEach((tab, index) => {
  tab.addEventListener('click', () => activateApi(tab));
  tab.addEventListener('keydown', event => {
    let next;
    if (event.key === 'ArrowRight') next = (index + 1) % apiTabs.length;
    if (event.key === 'ArrowLeft') next = (index + apiTabs.length - 1) % apiTabs.length;
    if (event.key === 'Home') next = 0;
    if (event.key === 'End') next = apiTabs.length - 1;
    if (next !== undefined) { event.preventDefault(); activateApi(apiTabs[next], true); }
  });
});

let readers = 1;
const pool = document.getElementById('reader-pool');
const add = document.getElementById('add-reader');
const remove = document.getElementById('remove-reader');
function renderReaders() {
  pool.replaceChildren();
  for (let i = 1; i <= readers; i++) {
    const node = document.createElement('div');
    node.className = 'compute-node';
    const icon = document.createElement('span');
    icon.className = 'node-icon'; icon.setAttribute('aria-hidden', 'true'); icon.textContent = '↗';
    const label = document.createElement('b'); label.textContent = `Reader ${String(i).padStart(2, '0')}`;
    const note = document.createElement('small'); note.textContent = 'GET + SQL · local cache';
    node.append(icon, label, note); pool.append(node);
  }
  document.getElementById('reader-count').textContent = `${readers} reader${readers > 1 ? 's' : ''}`;
  document.getElementById('scale-note').textContent = readers === 1
    ? 'One writer, one reader. Separate processes, shared durable data.'
    : `One writer, ${readers} readers. Each reader has its own cache; the durable dataset stays shared.`;
  add.disabled = readers >= 3;
  remove.disabled = readers <= 1;
}
add.addEventListener('click', () => { if (readers < 3) { readers++; renderReaders(); } });
remove.addEventListener('click', () => { if (readers > 1) { readers--; renderReaders(); } });

const paths = {
  write: ['WRITE → DURABLE ACK', 'Client → gateway → writer. SlateDB persists the mutation in object storage before the writer acknowledges success. Readers catch up independently; the writer publishes the SQL projection in the background.'],
  get: ['GET → SLATEDB READER', 'Client → gateway → reader. SlateDB looks up the key or document ID using that reader’s current view, local cache, and shared durable files. The original value comes back through the gateway. A recent write may not be visible yet.'],
  sql: ['SQL → WRITER EXPORT + READER QUERY', 'The writer uses DuckDB to produce and publish Parquet in the background. When you call query, the gateway sends your SELECT to a reader, where DuckDB reads that published projection and returns JSON rows. SQL visibility follows publication and can lag both durable writes and GET.']
};
const pathButtons = [...document.querySelectorAll('button[data-path]')];
pathButtons.forEach(button => button.addEventListener('click', () => {
  document.querySelector('.architecture-plate').dataset.path = button.dataset.path;
  document.getElementById('sql-pipeline').hidden = button.dataset.path !== 'sql';
  const [label, description] = paths[button.dataset.path];
  document.getElementById('path-label').textContent = label;
  document.getElementById('path-description').textContent = description;
  pathButtons.forEach(item => item.setAttribute('aria-pressed', String(item === button)));
}));

const status = document.getElementById('copy-status');
document.querySelectorAll('[data-copy]').forEach(button => button.addEventListener('click', async () => {
  const source = document.getElementById(button.dataset.copy);
  try {
    if (!navigator.clipboard || !window.isSecureContext) throw new Error('Clipboard unavailable');
    await navigator.clipboard.writeText(source.textContent);
    button.textContent = 'Copied ✓';
    status.textContent = 'Example copied to clipboard.';
  } catch {
    const selection = window.getSelection();
    const range = document.createRange();
    range.selectNodeContents(source);
    selection.removeAllRanges(); selection.addRange(range);
    button.textContent = 'Selected';
    status.textContent = 'Code selected. Use your keyboard to copy.';
  }
  window.setTimeout(() => { button.textContent = 'Copy ↗'; }, 2200);
}));
