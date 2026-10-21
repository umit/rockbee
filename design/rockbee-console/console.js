/* Rockbee Console preview: sample data and browser-local settings only. */
(() => {
  'use strict';
  const $ = (s, r = document) => r.querySelector(s);
  const $$ = (s, r = document) => [...r.querySelectorAll(s)];
  const main = $('#main-content'), inspector = $('#inspector'), dialog = $('#dialog'), toast = $('#toast');
  const themeKey = 'rockbee-console-theme', isMac = /Mac|iPhone|iPad/.test(navigator.platform);
  const esc = (v) => String(v ?? '').replace(/[&<>'"]/g, c => ({ '&':'&amp;','<':'&lt;','>':'&gt;',"'":'&#39;','"':'&quot;' }[c]));
  const copy = v => JSON.parse(JSON.stringify(v)), pretty = v => JSON.stringify(v, null, 2);
  const icon = (n, z = 16) => window.rockbeeIcons?.[n] || `<svg width="${z}" height="${z}" viewBox="0 0 24 24" aria-hidden="true"><path fill="none" stroke="currentColor" stroke-width="1.8" d="M5 12h14m-7-7 7 7-7 7"/></svg>`;
  const btn = (text, action, style = 'quiet', extra = '') => `<button type="button" class="button ${style} ${extra}" data-action="${action}">${text}</button>`;
  const ibtn = (label, action, svg) => `<button type="button" class="button quiet icon-button" data-action="${action}" aria-label="${label}" title="${label}">${svg}</button>`;
  const fixtures = {
    products: [
      {id:'prod_001',name:'Atlas Mechanical Keyboard',brand:'Nexus',price:149,stock:18,status:'active',attributes:{switch:'Tactile',layout:'ANSI',wireless:true}}, {id:'prod_002',name:'Studio Wireless Headphones',brand:'Sonic',price:249,stock:7,status:'active',attributes:{colour:'Midnight',batteryHours:38,codec:'AAC'}}, {id:'prod_003',name:'Halo Desk Lamp',brand:'Lumen',price:89,stock:31,status:'active',attributes:{finish:'Sand',lumens:800,dimmable:true}}, {id:'prod_004',name:'Orbit Ergonomic Mouse',brand:'Nexus',price:79,stock:0,status:'archived',attributes:{handedness:'Right',sensor:'4K',buttons:6}}, {id:'prod_005',name:'Arc Monitor Stand',brand:'Form',price:119,stock:14,status:'active',attributes:{material:'Aluminium',widthCm:52,cableTray:true}}, {id:'prod_006',name:'Quiet Keys 65',brand:'Nexus',price:129,stock:23,status:'draft',attributes:{switch:'Linear',layout:'ISO',hotSwap:true}}, {id:'prod_007',name:'Field Notes Set',brand:'Paper & Co.',price:18,stock:64,status:'active',attributes:{sheets:192,paper:'Dot grid',recycled:true}}, {id:'prod_008',name:'Lift Laptop Riser',brand:'Form',price:69,stock:12,status:'active',attributes:{material:'Steel',maxKg:8,foldable:true}}, {id:'prod_009',name:'Focus Timer',brand:'Lumen',price:42,stock:9,status:'active',attributes:{display:'E-ink',presets:[25,50],sound:false}}, {id:'prod_010',name:'Cove Cable Kit',brand:'Nexus',price:24,stock:48,status:'active',attributes:{pieces:12,colour:'Graphite',reusable:true}}, {id:'prod_011',name:'Cloud Desk Mat',brand:'Form',price:55,stock:5,status:'draft',attributes:{size:'XL',surface:'Vegan leather',waterproof:true}}, {id:'prod_012',name:'Tone USB Microphone',brand:'Sonic',price:159,stock:16,status:'active',attributes:{pattern:'Cardioid',sampleRateKhz:48,standIncluded:true}}
    ],
    agent_runs: [{id:'run_8c92',agent:'Catalog writer',status:'completed',durationMs:1840,input:{products:12},output:{drafts:4},startedAt:'2026-10-18T09:42:15Z'},{id:'run_8c91',agent:'Inventory reviewer',status:'completed',durationMs:920,input:{threshold:10},output:{flagged:3},startedAt:'2026-10-18T09:31:02Z'},{id:'run_8c90',agent:'Catalog writer',status:'failed',durationMs:412,input:{products:1},output:{error:'sample validation error'},startedAt:'2026-10-18T09:18:44Z'}],
    prompt_versions: [{id:'prm_catalog_03',name:'Catalog enrichment',version:3,status:'active',model:'gpt-5',variables:['product','tone'],createdAt:'2026-10-17T14:10:00Z'},{id:'prm_catalog_02',name:'Catalog enrichment',version:2,status:'archived',model:'gpt-5',variables:['product'],createdAt:'2026-10-14T11:22:00Z'},{id:'prm_inventory_01',name:'Inventory summary',version:1,status:'draft',model:'gpt-5-mini',variables:['items'],createdAt:'2026-10-18T08:16:00Z'}]
  };
  const labels = {products:'Products',agent_runs:'Agent runs',prompt_versions:'Prompt versions'};
  const cols = {products:['id','name','brand','price','stock','status'],agent_runs:['id','agent','status','durationMs','startedAt'],prompt_versions:['id','name','version','status','model']};
  const presets = {
    products:`SELECT id, value->>'name' AS name, value->>'brand' AS brand,\n       TRY_CAST(value->>'price' AS DOUBLE) AS price,\n       TRY_CAST(value->>'stock' AS INTEGER) AS stock, value->>'status' AS status\nFROM products\nWHERE value->>'status' = 'active'\nORDER BY TRY_CAST(value->>'price' AS DOUBLE) DESC\nLIMIT 12;`,
    runs:`SELECT id, value->>'agent' AS agent, value->>'status' AS status,\n       TRY_CAST(value->>'durationMs' AS INTEGER) AS durationMs, value->>'startedAt' AS startedAt\nFROM agent_runs\nORDER BY value->>'startedAt' DESC\nLIMIT 20;`,
    versions:`SELECT id, value->>'name' AS name, TRY_CAST(value->>'version' AS INTEGER) AS version,\n       value->>'status' AS status, value->>'model' AS model\nFROM prompt_versions\nORDER BY value->>'createdAt' DESC\nLIMIT 20;`
  };
  const presetTables = {products:'products',runs:'agent_runs',versions:'prompt_versions'};
  const norm = q => q.trim().replace(/;$/, '').replace(/\s+/g,' ').toLowerCase();
  const approved = Object.fromEntries(Object.entries(presets).map(([k,v]) => [norm(v),k]));
  const utf8 = new TextEncoder();
  const utf8Decoder = new TextDecoder('utf-8', {fatal:true});
  const kvFixtures = [
    {key:'cart:customer-42', bytes:utf8.encode('{\n  "customerId": "customer-42",\n  "items": [{"sku": "prod_001", "quantity": 1}, {"sku": "prod_003", "quantity": 2}],\n  "currency": "USD"\n}')},
    {key:'config:checkout', bytes:utf8.encode('{"currency":"USD","taxInclusive":false,"capture":"automatic"}')},
    {key:'blob:sample', bytes:new Uint8Array([0, 255, 16, 128, 66, 101, 101, 0, 1])}
  ];
  const copyKvEntries = entries => entries.map(entry => ({key:entry.key, bytes:new Uint8Array(entry.bytes)}));
  const state = {
    tables:copy(fixtures), table:'products', filter:'', view:'kv', selected:null, source:null,
    sql:presets.products, result:null, history:[], kvEntries:copyKvEntries(kvFixtures),
    kvSelectedKey:'cart:customer-42', kvTab:'json', sqlDrafts:{}
  };

  function flash(message) { toast.hidden=false; toast.textContent=message; toast.classList.add('visible'); clearTimeout(flash.timer); flash.timer=setTimeout(()=>{toast.classList.remove('visible');toast.hidden=true;},2800); }
  function empty(name,title,body) { return `<div class="empty-state"><div class="empty-icon">${icon(name,24)}</div><strong>${title}</strong><span>${body}</span></div>`; }
  function syntaxJsonText(raw) { const token=/"(?:\\.|[^"\\])*"|\btrue\b|\bfalse\b|\bnull\b|-?\d+(?:\.\d+)?/g; let out='',at=0; for(const m of raw.matchAll(token)){const t=m[0],i=m.index;out+=esc(raw.slice(at,i));const kind=t[0]==='"'?(/^\s*:/.test(raw.slice(i+t.length))?'syntax-key':'syntax-string'):t==='null'?'syntax-null':/^(true|false)$/.test(t)?'syntax-keyword':'syntax-number';out+=`<span class="${kind}">${esc(t)}</span>`;at=i+t.length;}return out+esc(raw.slice(at)); }
  function syntaxJson(value) { return syntaxJsonText(pretty(value)); }
  function cell(v,key) { if(key==='status')return `<span class="status-tag ${esc(v)}">${esc(v)}</span>`;if(typeof v==='number')return `<span class="cell-number">${key==='price'?'$'+v.toFixed(2):v.toLocaleString()}</span>`;return `<span class="${key==='id'?'cell-id mono':'cell-primary'}">${esc(v)}</span>`; }
  function chrome() {
    const viewNames = {sql: 'SQL workspace', kv: 'Key-value lookup', services: 'Services', history: 'Query history'};
    $('#breadcrumb').textContent = state.view === 'data' ? labels[state.table] : viewNames[state.view];
    $('#workspace-name').textContent = localStorage.getItem('rockbee-workspace') || 'Local development';
    const tablesWorkspace = state.view === 'data' || state.view === 'sql';
    const tablesNavigation = $('#tables-navigation');
    if (tablesNavigation) tablesNavigation.hidden = !tablesWorkspace;
    $$('.main-nav [data-view]').forEach(button => {
      button.classList.toggle('active', button.dataset.view === (tablesWorkspace ? 'data' : state.view));
    });
  }
  function tables(){const list=$('#table-list');if(!list)return;list.innerHTML=Object.keys(labels).map(k=>`<button type="button" class="table-link ${state.table===k&&(state.view==='data'||state.view==='sql')?'active':''}" data-table="${k}"><span>${icon('file')}</span>${labels[k]}<span class="table-count">${state.tables[k].length}</span></button>`).join('');}
  function inspectorView() {
    $$('[data-row-id]').forEach(row => row.classList.toggle('selected', state.source === 'data' && row.dataset.rowId === state.selected?.id));
    $$('[data-sql-row]').forEach(row => row.classList.toggle('selected', state.source === 'sql' && state.result?.rows[Number(row.dataset.sqlRow)] === state.selected));
    inspector.hidden = !state.selected;
    if (!state.selected) return;
    const editable = state.source === 'data';
    inspector.innerHTML = `
      <div class="inspector-head">
        <div><span class="inspector-label">${editable ? 'Sample document' : 'Query result'}</span><strong class="mono">${esc(state.selected.id || 'result')}</strong></div>
        <div>${ibtn('Copy JSON', 'copy', icon('copy'))}${ibtn('Close inspector', 'close-inspector', icon('close'))}</div>
      </div>
      <div class="inspector-body"><pre class="json-code">${syntaxJson(state.selected)}</pre></div>
      <footer class="inspector-footer">${editable
        ? btn(`${icon('edit')} Edit`, 'edit', 'quiet small') + btn(`${icon('trash')} Delete`, 'delete', 'danger small')
        : '<span class="muted">Read-only query result</span>'}</footer>`;
  }
  function workspaceTabs() {
    return `<div class="workspace-tabs" role="tablist" aria-label="${esc(labels[state.table])} workspace"><button type="button" data-view="data" class="${state.view === 'data' ? 'active' : ''}" role="tab" aria-selected="${state.view === 'data'}">Documents</button><button type="button" data-view="sql" class="${state.view === 'sql' ? 'active' : ''}" role="tab" aria-selected="${state.view === 'sql'}">SQL editor</button></div>`;
  }

  function dataCell(row, column) {
    return Object.hasOwn(row, column) ? cell(row[column], column) : '<span class="muted">—</span>';
  }

  function data() {
    const rows = state.tables[state.table].filter(row => !state.filter || pretty(row).toLowerCase().includes(state.filter.toLowerCase()));
    const columns = cols[state.table];
    main.innerHTML = `<section class="view-stack"><header class="page-heading"><div class="page-title-group"><p class="page-eyebrow">JSON TABLES</p><h1>${labels[state.table]}</h1><p class="page-description">Flexible JSON documents, addressed by ID.</p></div><div class="page-actions">${btn(`${icon('download')} Download`,'download','quiet small')}${btn(`${icon('plus')} New document`,'create','primary small')}</div></header>${workspaceTabs()}<div class="toolbar"><label class="search-field">${icon('search')}<input id="table-filter" value="${esc(state.filter)}" placeholder="Filter visible documents" aria-label="Filter visible documents"></label><div class="toolbar-right"><span class="pill blue">${rows.length} documents</span></div></div><div class="data-card"><div class="table-scroll"><table class="data-table"><thead><tr><th class="row-index">#</th>${columns.map(column=>`<th>${column} <span class="muted">${column==='price'||column==='stock'?'number':'string'}</span></th>`).join('')}</tr></thead><tbody>${rows.length?rows.map((row,index)=>`<tr tabindex="0" role="button" data-row-id="${esc(row.id)}"><td class="row-index">${index+1}</td>${columns.map(column=>`<td>${dataCell(row,column)}</td>`).join('')}</tr>`).join(''):`<tr><td colspan="${columns.length+1}">${empty('search','No matching documents','Change the filter or reset the preview.')}</td></tr>`}</tbody></table></div><footer class="table-footer"><span>Documents are selected by exact ID in Rockbee.</span><span>${rows.length} visible</span></footer></div></section>`;
  }
  function sqlHighlight(query) {
    const tokens = /'(?:''|[^'])*'|--[^\n]*|\b(?:SELECT|FROM|WHERE|ORDER|BY|LIMIT|AS|TRY_CAST|DOUBLE|INTEGER|DESC)\b|\b\d+(?:\.\d+)?\b/gi;
    let output = '';
    let cursor = 0;
    for (const match of query.matchAll(tokens)) {
      const token = match[0];
      const kind = token.startsWith("'") ? 'syntax-string'
        : token.startsWith('--') ? 'syntax-null'
        : /^\d/.test(token) ? 'syntax-number' : 'syntax-keyword';
      output += esc(query.slice(cursor, match.index));
      output += `<span class="${kind}">${esc(token)}</span>`;
      cursor = match.index + token.length;
    }
    return output + esc(query.slice(cursor));
  }
  function syncEditor(){const input=$('#sql-input'),hi=$('#sql-highlight'),gut=$('#sql-gutter');if(!input)return;hi.innerHTML=sqlHighlight(input.value)+'\n';gut.innerHTML=input.value.split('\n').map((_,i)=>i+1).join('<br>');hi.scrollTop=input.scrollTop;hi.scrollLeft=input.scrollLeft;gut.scrollTop=input.scrollTop;}
  function sql(){const r=state.result,rows=r?.rows||[],cs=r?.columns||[];main.innerHTML=`<section class="view-stack"><header class="page-heading"><div class="page-title-group"><p class="page-eyebrow">JSON TABLES</p><h1>${labels[state.table]} SQL editor</h1><p class="page-description">Query the selected pinned JSON table with SQL.</p></div><div class="page-actions"><span class="pill amber">SELECT only</span>${btn(`${icon('run')} Run query`,'run-sql','primary small')}</div></header>${workspaceTabs()}<div class="editor-card"><div class="editor-tabs"><button class="editor-tab active" type="button">Query</button><span class="muted">${isMac?'⌘↵':'Ctrl ↵'} to run</span></div><div class="editor-toolbar"><span>Sample presets</span><div class="segmented"><button type="button" data-preset="products">Products</button><button type="button" data-preset="runs">Runs</button><button type="button" data-preset="versions">Versions</button></div></div><div class="editor-box"><div class="editor-gutter" id="sql-gutter"></div><pre class="editor-highlight" id="sql-highlight" aria-hidden="true"></pre><textarea id="sql-input" class="editor-input highlighted" spellcheck="false" aria-label="SQL query">${esc(state.sql)}</textarea></div><footer class="editor-footer"><span>SELECT results are bounded to 1,000 rows / 512 KiB.</span><span>Only the supplied preview queries can run here.</span></footer></div>${r?`<section class="data-card"><header class="results-heading"><div><h2>${esc(r.title)}</h2><p class="muted">${esc(r.note)}</p></div><span class="pill green">${rows.length} rows</span></header>${rows.length?`<div class="table-scroll"><table class="data-table"><thead><tr>${cs.map(c=>`<th>${c}</th>`).join('')}</tr></thead><tbody>${rows.map((row,i)=>`<tr tabindex="0" role="button" data-sql-row="${i}">${cs.map(c=>`<td>${dataCell(row,c)}</td>`).join('')}</tr>`).join('')}</tbody></table></div>`:empty('info','No result',r.note)}</section>`:''}</section>`;syncEditor();}
  function rowsFor(preset) {
    // Preview documents may omit the fields used by the supplied sample queries.
    const date = value => typeof value === 'string' ? value : '';
    if (preset === 'products') {
      return state.tables.products
        .filter(row => row.status === 'active')
        .sort((a, b) => b.price - a.price)
        .slice(0, 12)
        .map(({id, name, brand, price, stock, status}) => ({id, name, brand, price, stock, status}));
    }
    if (preset === 'runs') {
      return state.tables.agent_runs.slice()
        .sort((a, b) => date(b.startedAt).localeCompare(date(a.startedAt)))
        .slice(0, 20)
        .map(({id, agent, status, durationMs, startedAt}) => ({id, agent, status, durationMs, startedAt}));
    }
    return state.tables.prompt_versions.slice()
      .sort((a, b) => date(b.createdAt).localeCompare(date(a.createdAt)))
      .slice(0, 20)
      .map(({id, name, version, status, model}) => ({id, name, version, status, model}));
  }
  function selectTable(table) {
    if (table === state.table) return;
    state.sqlDrafts[state.table] = state.sql;
    state.table = table;
    const preset = Object.keys(presetTables).find(name => presetTables[name] === table);
    state.sql = state.sqlDrafts[table] ?? presets[preset];
    state.result = null;
    state.selected = null;
  }

  function run() {
    state.sql = $('#sql-input')?.value ?? state.sql;
    const preset = Object.hasOwn(approved, norm(state.sql)) ? approved[norm(state.sql)] : null;
    if (!preset || presetTables[preset] !== state.table) {
      state.result = {title:'Preview limitation', note:'Run a supplied sample query for the selected table. Other SQL requires a Rockbee connection.', columns:[], rows:[]};
      sql();
      return;
    }
    const rows = rowsFor(preset);
    state.result = {title:`${labels[state.table]} result`, note:'Sample result for the supplied query.', columns:cols[state.table], rows};
    state.history.unshift({id:crypto.randomUUID(), query:state.sql, table:state.table, at:new Date().toLocaleTimeString([], {hour:'2-digit', minute:'2-digit'})});
    state.history = state.history.slice(0, 20);
    state.selected = null;
    render();
    flash('Sample query added to browser history');
  }
  function selectedKvEntry() {
    return state.kvEntries.find(entry => entry.key === state.kvSelectedKey) || null;
  }

  function decodeUtf8(bytes) {
    try { return utf8Decoder.decode(bytes); } catch { return null; }
  }

  function hexValue(bytes) {
    return [...bytes].map(byte => byte.toString(16).padStart(2, '0')).join(' ');
  }

  function kvDisplay(entry) {
    const text = decodeUtf8(entry.bytes);
    if (state.kvTab === 'hex') return esc(hexValue(entry.bytes));
    if (text === null) return esc('This byte value is not valid UTF-8. Use Hex to inspect or copy it losslessly.');
    if (state.kvTab === 'json') {
      try { return syntaxJson(JSON.parse(text)); }
      catch { return esc('This UTF-8 value is not valid JSON. Use Text to inspect its original bytes.'); }
    }
    return esc(text);
  }

  function kvWorkbench() {
    const entry = selectedKvEntry();
    const pins = kvFixtures.map(sample => {
      const type = sample.key === 'blob:sample' ? 'binary' : 'JSON';
      return `<button type="button" data-kv="${esc(sample.key)}" class="kv-pin ${sample.key === state.kvSelectedKey ? 'active' : ''}"><code class="kv-pin-title">${esc(sample.key)}</code><small class="kv-pin-meta">${type} · ${sample.bytes.length} bytes</small></button>`;
    }).join('');
    const detail = entry ? `<section class="kv-detail"><div class="kv-lookup"><label class="search-field">${icon('key')}<input id="kv-key" value="${esc(entry.key)}" placeholder="Enter an exact key" aria-label="Exact key"></label>${btn('Get value','get-kv','primary small')}</div><div class="value-tabs" role="tablist" aria-label="Value representation"><button type="button" data-kv-tab="json" class="${state.kvTab === 'json' ? 'active' : ''}" role="tab" aria-selected="${state.kvTab === 'json'}">JSON</button><button type="button" data-kv-tab="text" class="${state.kvTab === 'text' ? 'active' : ''}" role="tab" aria-selected="${state.kvTab === 'text'}">Text</button><button type="button" data-kv-tab="hex" class="${state.kvTab === 'hex' ? 'active' : ''}" role="tab" aria-selected="${state.kvTab === 'hex'}">Hex</button></div><div class="value-toolbar"><div><span class="muted">Exact key</span><strong class="mono">${esc(entry.key)}</strong></div><span class="muted">${entry.bytes.length} bytes</span><div>${ibtn('Copy value','copy-kv',icon('copy'))}${ibtn('Edit entry','edit-kv',icon('edit'))}${ibtn('Delete entry','delete-kv',icon('trash'))}</div></div><div class="value-body"><pre class="json-code">${kvDisplay(entry)}</pre></div></section>` : `<section class="kv-detail"><div class="kv-lookup"><label class="search-field">${icon('key')}<input id="kv-key" value="${esc(state.kvSelectedKey)}" placeholder="Enter an exact key" aria-label="Exact key"></label>${btn('Get value','get-kv','primary small')}</div><div class="kv-empty">${empty('search','No local value for this exact key','Use a pinned sample or create a local sample entry.')}</div></section>`;
    return `<div class="kv-workbench"><aside class="kv-pins"><p class="page-eyebrow kv-pins-heading">PINNED SAMPLE KEYS</p><p class="muted">Your saved keys, one click away.</p>${pins}</aside>${detail}</div>`;
  }

  function kv() {
    main.innerHTML = `<section class="view-stack"><header class="page-heading"><div class="page-title-group"><p class="page-eyebrow">RAW STORAGE</p><h1>Key-value</h1><p class="page-description">Work with values by their exact key.</p></div><div class="page-actions">${btn(`${icon('plus')} New entry`,'create-kv','small')}</div></header>${kvWorkbench()}</section>`;
  }

  function getKv() {
    const key = $('#kv-key')?.value ?? '';
    state.kvSelectedKey = key;
    state.kvTab = 'json';
    kv();
  }

  function kvForm(mode) {
    const entry = mode === 'edit' ? selectedKvEntry() : null;
    const text = entry ? decodeUtf8(entry.bytes) : '';
    open(`<form method="dialog"><header class="dialog-head"><div><h2 class="dialog-title">${mode === 'edit' ? 'Edit local entry' : 'New local entry'}</h2><p class="dialog-description">Values written here are encoded as UTF-8 and remain in this browser preview.</p></div>${ibtn('Close dialog','cancel',icon('close'))}</header><div class="dialog-body"><label class="field"><span class="field-label">Exact key</span><input id="kv-entry-key" class="form-input mono" value="${esc(entry?.key || '')}"></label><label class="field"><span class="field-label">Text value</span><textarea id="kv-entry-value" class="form-textarea mono" rows="10">${esc(text ?? '')}</textarea></label>${text === null ? '<p class="form-error">The existing binary value cannot be edited as text. Replace it with an explicit UTF-8 value.</p>' : ''}<p id="kv-form-error" class="form-error" hidden></p></div><footer class="dialog-footer">${btn('Cancel','cancel')}${btn(mode === 'edit' ? 'Save entry' : 'Create entry','save-kv','primary')}</footer></form>`,'#kv-entry-key');
    dialog.dataset.kvMode = mode;
    dialog.dataset.kvOriginalKey = entry?.key || '';
    $('#kv-entry-key', dialog).readOnly = mode === 'edit';
  }

  function saveKv() {
    const key = $('#kv-entry-key', dialog).value;
    const value = $('#kv-entry-value', dialog).value;
    const originalKey = dialog.dataset.kvOriginalKey;
    if (!key.length || !value.length) {
      const error = $('#kv-form-error', dialog);
      error.textContent = 'Key and value must each contain at least one byte.';
      error.hidden = false;
      return;
    }
    const duplicate = state.kvEntries.some(entry => entry.key === key && entry.key !== originalKey);
    if (duplicate) {
      const error = $('#kv-form-error', dialog);
      error.textContent = 'That exact key already exists in this local preview.';
      error.hidden = false;
      return;
    }
    const next = {key, bytes:utf8.encode(value)};
    const index = state.kvEntries.findIndex(entry => entry.key === originalKey);
    if (dialog.dataset.kvMode === 'edit' && index >= 0) state.kvEntries.splice(index, 1, next);
    else state.kvEntries.push(next);
    state.kvSelectedKey = key;
    state.kvTab = 'text';
    close();
    kv();
    flash('Local UTF-8 entry saved');
  }
  function services(){main.innerHTML=`<section class="view-stack"><header class="page-heading"><div class="page-title-group"><h1>Services</h1><p class="page-description">Illustrative endpoint counters for this preview.</p></div><span class="pill amber">Sample</span></header><div class="service-topology"><div class="topology-node">Client</div><div class="topology-link">${icon('arrow')}</div><div class="topology-node">Gateway</div><div class="topology-link">${icon('arrow')}</div><div class="topology-node">Writer / readers</div><div class="topology-link">${icon('arrow')}</div><div class="topology-node">Shared object storage</div></div><div class="service-grid">${[['Gateway','Routes reads and mutations','gateway-01','12','1,284','0'],['Writer','Acknowledges durable mutations','writer-01','3','96','0'],['Reader','Serves eventual reads','reader-02','8','1,188','2']].map(x=>`<article class="service-card"><header class="service-heading"><h2>${x[0]}</h2><span class="pill blue">sample</span></header><p>${x[1]}</p><dl class="service-meta"><div><dt>Endpoint</dt><dd class="mono">${x[2]}</dd></div><div><dt>Role</dt><dd>${x[0].toLowerCase()}</dd></div></dl><div class="service-metrics"><div><span class="metric-value">${x[3]}</span><span class="metric-label">connections</span></div><div><span class="metric-value">${x[4]}</span><span class="metric-label">requests</span></div><div><span class="metric-value">${x[5]}</span><span class="metric-label">errors</span></div></div></article>`).join('')}</div><p class="info-note">Sample counters describe individual service endpoints.</p></section>`;}
  function history(){main.innerHTML=`<section class="view-stack"><header class="page-heading"><div class="page-title-group"><h1>Query history</h1><p class="page-description">Queries run in this browser session.</p></div>${state.history.length?btn('Clear history','clear-history','quiet small'):''}</header><div class="history-list">${state.history.length?state.history.map(x=>`<button type="button" class="history-item" data-history="${x.id}"><code class="history-query">${esc(x.query)}</code><span class="history-meta">Executed ${x.at}</span>${icon('arrow')}</button>`).join(''):`<div class="data-card">${empty('history','No queries yet','Run a supplied SQL query to see it here.')}</div>`}</div></section>`;}
  function render(){tables();chrome();({data,sql,kv,services,history})[state.view]();inspectorView();}
  function nav(view){close();state.view=view;state.selected=null;state.source=null;document.body.classList.remove('sidebar-open');const activeView=(view==='data'||view==='sql')?'data':view;$$('[data-view]').forEach(x=>x.classList.toggle('active',x.dataset.view===activeView));render();}
  function open(content,focus='input,textarea,button'){dialog.innerHTML=content;dialog.showModal();setTimeout(()=>$(focus,dialog)?.focus(),0);}
  function close(){if(dialog.open)dialog.close();}
  function form(mode){const existing=mode==='create'?{}:(state.selected||{}),body=mode==='create'?'{\n  "name": "New sample document"\n}':pretty(existing);open(`<form method="dialog"><header class="dialog-head"><div><h2 class="dialog-title">${mode==='create'?'New sample document':'Edit sample document'}</h2><p class="dialog-description">Changes remain in this browser preview.</p></div>${ibtn('Close dialog','cancel',icon('close'))}</header><div class="dialog-body"><label class="field"><span class="field-label">Document ID</span><input id="doc-id" class="form-input mono" value="${esc(existing.id||'')}" ${mode==='edit'?'readonly':''}></label><label class="field"><span class="field-label">JSON object</span><textarea id="doc-json" class="form-textarea mono" rows="14">${esc(body)}</textarea></label><p id="form-error" class="form-error" hidden></p></div><footer class="dialog-footer">${btn('Cancel','cancel')}${btn(mode==='create'?'Create document':'Save document','save','primary')}</footer></form>`,'#doc-json');dialog.dataset.mode=mode;}
  function save(){let d;try{d=JSON.parse($('#doc-json',dialog).value);if(!d||Array.isArray(d)||typeof d!=='object')throw Error('JSON must be an object');}catch(e){const x=$('#form-error',dialog);x.textContent=e.message==='JSON must be an object'?e.message:'Enter valid JSON for one object.';x.hidden=false;return;}const a=state.tables[state.table];if(dialog.dataset.mode==='create'){let id=$('#doc-id',dialog).value.trim(),n=a.length+1,p=state.table==='products'?'prod':state.table==='agent_runs'?'run':'prm';while(!id&&a.some(x=>x.id===`${p}_sample_${String(n).padStart(2,'0')}`))n++;d.id=id||`${p}_sample_${String(n).padStart(2,'0')}`;if(a.some(x=>x.id===d.id)){const x=$('#form-error',dialog);x.textContent='That sample document ID already exists.';x.hidden=false;return;}a.unshift(d);}else{d.id=state.selected.id;a.splice(a.findIndex(x=>x.id===d.id),1,d);}state.selected=d;state.source='data';close();render();flash('Sample document saved locally');}
  function reset(){Object.assign(state,{tables:copy(fixtures),table:'products',sqlDrafts:{},filter:'',selected:null,source:null,sql:presets.products,result:null,history:[],kvEntries:copyKvEntries(kvFixtures),kvSelectedKey:'cart:customer-42',kvTab:'json'});render();flash('Preview data and history reset');}
  async function clip(text,msg){if(!navigator.clipboard?.writeText){flash('Copy is unavailable in this browser.');return;}try{await navigator.clipboard.writeText(text);flash(msg);}catch{flash('Copy is unavailable in this browser.');}}
  document.addEventListener('click', e => {
    const target = e.target.closest('[data-action],[data-view],[data-table],[data-row-id],[data-sql-row],[data-preset],[data-kv],[data-kv-tab],[data-history]');
    if (!target) return;
    if (target.dataset.view) return nav(target.dataset.view);
    if (target.dataset.table) { selectTable(target.dataset.table); state.filter = ''; return nav('data'); }
    if (target.dataset.rowId) { state.selected = state.tables[state.table].find(row => row.id === target.dataset.rowId); state.source = 'data'; return inspectorView(); }
    if (target.dataset.sqlRow !== undefined) { state.selected = state.result.rows[+target.dataset.sqlRow]; state.source = 'sql'; return inspectorView(); }
    if (target.dataset.preset) { selectTable(presetTables[target.dataset.preset]); state.sql = presets[target.dataset.preset]; state.result = null; state.selected = null; return render(); }
    if (target.dataset.kv) { state.kvSelectedKey = target.dataset.kv; state.kvTab = decodeUtf8(selectedKvEntry()?.bytes || new Uint8Array()) === null ? 'hex' : 'json'; return kv(); }
    if (target.dataset.kvTab) { state.kvTab = target.dataset.kvTab; return kv(); }
    if (target.dataset.history) { const item = state.history.find(entry => entry.id === target.dataset.history); selectTable(item.table); state.sql = item.query; state.result = null; return nav('sql'); }
    switch (target.dataset.action) {
      case 'create': form('create'); break;
      case 'edit': form('edit'); break;
      case 'delete': open(`<div><header class="dialog-head"><div><h2 class="dialog-title">Delete sample document?</h2><p class="dialog-description">This removes ${esc(state.selected.id)} from this browser preview.</p></div></header><footer class="dialog-footer">${btn('Cancel','cancel')}${btn('Delete document','confirm-delete','danger')}</footer></div>`,'[data-action="confirm-delete"]'); break;
      case 'confirm-delete': state.tables[state.table] = state.tables[state.table].filter(row => row.id !== state.selected.id); state.selected = null; close(); render(); flash('Sample document deleted locally'); break;
      case 'create-kv': kvForm('create'); break;
      case 'edit-kv': kvForm('edit'); break;
      case 'delete-kv': open(`<div><header class="dialog-head"><div><h2 class="dialog-title">Delete local entry?</h2><p class="dialog-description">This removes ${esc(state.kvSelectedKey)} from this browser preview.</p></div></header><footer class="dialog-footer">${btn('Cancel','cancel')}${btn('Delete entry','confirm-delete-kv','danger')}</footer></div>`,'[data-action="confirm-delete-kv"]'); break;
      case 'confirm-delete-kv': state.kvEntries = state.kvEntries.filter(entry => entry.key !== state.kvSelectedKey); state.kvSelectedKey = ''; close(); kv(); flash('Local entry deleted'); break;
      case 'save': save(); break;
      case 'save-kv': saveKv(); break;
      case 'cancel': close(); break;
      case 'close-inspector': state.selected = null; inspectorView(); break;
      case 'copy': clip(pretty(state.selected),'Sample JSON copied'); break;
      case 'copy-kv': { const entry = selectedKvEntry(); const text = entry && decodeUtf8(entry.bytes); if (!entry) break; if (state.kvTab === 'hex') clip(hexValue(entry.bytes), 'Hex value copied'); else if (text === null) flash('Binary values can only be copied as hex.'); else clip(text, 'UTF-8 value copied'); break; }
      case 'download': { const anchor=document.createElement('a'),url=URL.createObjectURL(new Blob([pretty(state.tables[state.table].filter(row=>!state.filter||pretty(row).toLowerCase().includes(state.filter.toLowerCase())) )],{type:'application/json'}));anchor.href=url;anchor.download=`${state.table}-sample.json`;anchor.click();URL.revokeObjectURL(url);break; }
      case 'run-sql': run(); break;
      case 'get-kv': getKv(); break;
      case 'focus-kv': $('#kv-key')?.focus(); break;
      case 'clear-history': state.history=[]; history(); break;
    }
  });
  document.addEventListener('input',e=>{if(e.target.id==='table-filter'){const[a,b]=[e.target.selectionStart,e.target.selectionEnd];state.filter=e.target.value;data();const x=$('#table-filter');x.focus();x.setSelectionRange(a,b);}if(e.target.id==='sql-input'){state.sql=e.target.value;syncEditor();}if(e.target.id==='command-search')$$('.command-item',dialog).forEach(x=>x.hidden=!x.textContent.toLowerCase().includes(e.target.value.toLowerCase()));});
  document.addEventListener('scroll',e=>{if(e.target.id==='sql-input')syncEditor();},true);
  function visibleCommandItems() {
    return $$('.command-item', dialog).filter(item => !item.hidden);
  }

  function moveCommandFocus(direction) {
    const items = visibleCommandItems();
    if (!items.length) return;
    const current = items.indexOf(document.activeElement);
    const next = current < 0
      ? (direction > 0 ? 0 : items.length - 1)
      : (current + direction + items.length) % items.length;
    items[next].focus();
  }

  function commandPalette() {
    const commands = {kv:'Key-value',data:'Tables',sql:'SQL editor',services:'Services',history:'Query history'};
    open(`<div class="command-dialog"><header class="dialog-head"><div><h2 class="dialog-title">Go to</h2><p class="dialog-description">Navigate the preview</p></div></header><label class="search-field">${icon('search')}<input id="command-search" placeholder="Search views" aria-label="Search views"></label><div class="command-list">${Object.entries(commands).map(([view, name])=>`<button type="button" class="command-item" data-view="${view}"><strong>${name}</strong></button>`).join('')}</div></div>`,'#command-search');
  }

  document.addEventListener('keydown', e => {
    if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === 'k') {
      e.preventDefault();
      if (!dialog.open) commandPalette();
    } else if ((e.target.id === 'command-search' || e.target.closest('.command-item')) && (e.key === 'ArrowDown' || e.key === 'ArrowUp')) {
      e.preventDefault();
      moveCommandFocus(e.key === 'ArrowDown' ? 1 : -1);
    } else if (e.target.id === 'command-search' && e.key === 'Enter') {
      e.preventDefault();
      visibleCommandItems()[0]?.click();
    } else if ((e.metaKey || e.ctrlKey) && e.key === 'Enter' && state.view === 'sql' && !dialog.open) {
      e.preventDefault();
      run();
    } else if (e.key === 'Escape' && !dialog.open && !inspector.hidden) {
      state.selected = null;
      inspectorView();
    } else if (e.key === 'Enter' && e.target.id === 'kv-key') {
      e.preventDefault();
      getKv();
    } else if ((e.key === 'Enter' || e.key === ' ') && e.target.matches('[data-row-id]')) {
      e.preventDefault();
      e.target.click();
    } else if ((e.key === 'Enter' || e.key === ' ') && e.target.matches('[data-sql-row]')) {
      e.preventDefault();
      e.target.click();
    }
  });

  $('#connection-trigger')?.addEventListener('click',()=>open(`<form method="dialog"><header class="dialog-head"><div><h2 class="dialog-title">Preview connection</h2><p class="dialog-description">These settings only personalize the preview. No gateway is contacted.</p></div></header><div class="dialog-body"><label class="field"><span class="field-label">Workspace name</span><input id="preview-workspace" class="form-input" value="${esc(localStorage.getItem('rockbee-workspace')||'Preview workspace')}"></label><label class="field"><span class="field-label">Gateway address</span><input id="gateway-address" class="form-input mono" value="${esc(localStorage.getItem('rockbee-gateway')||'127.0.0.1:7100')}"></label></div><footer class="dialog-footer">${btn('Cancel','cancel')}${btn('Save preview settings','save-settings','primary')}</footer></form>`));
  document.addEventListener('click',e=>{if(e.target.closest('[data-action="save-settings"]')){localStorage.setItem('rockbee-workspace',$('#preview-workspace',dialog).value.trim()||'Preview workspace');localStorage.setItem('rockbee-gateway',$('#gateway-address',dialog).value.trim()||'127.0.0.1:7100');close();chrome();flash('Preview settings saved locally');}});
  $('#command-trigger')?.addEventListener('click', commandPalette);
  $('#preview-reset')?.addEventListener('click',reset);$('#sidebar-toggle')?.addEventListener('click',()=>document.body.classList.toggle('sidebar-open'));
  $('#theme-toggle')?.addEventListener('click',()=>{const v=document.documentElement.dataset.theme==='dark'?'light':'dark';document.documentElement.dataset.theme=v;localStorage.setItem(themeKey,v);});document.documentElement.dataset.theme=localStorage.getItem(themeKey)||'light';render();
})();
