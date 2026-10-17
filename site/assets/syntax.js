/* Rockbee code highlighting. PrismJS 1.30.0 is vendored alongside this file. */
(() => {
  'use strict';
  const prism = window.Prism;
  if (!prism || !prism.tokenize) return;
  const states = new WeakMap();
  const pending = new Set();
  let scheduled = false;
  const aliases = { sh: 'bash', shell: 'bash', shellscript: 'bash', rs: 'rust' };
  const supported = new Set(['rust', 'sql', 'json', 'bash', 'toml']);

  function language(node, text) {
    if (node.closest('[data-no-highlight]')) return null;
    for (let el = node; el && el.tagName !== 'BODY'; el = el.parentElement) {
      const match = /(?:^|\s)lang(?:uage)?-([\w-]+)/i.exec(el.className || '');
      if (match) return supported.has(aliases[match[1]] || match[1]) ? (aliases[match[1]] || match[1]) : null;
      if (el.tagName === 'PRE') break;
    }
    const source = text.trim();
    if (!source || source.length > 100000) return null;
    if (/^(?:\/\/[^\n]*\n\s*)*(?:use\s+[\w:]+|(?:pub\s+)?(?:async\s+)?fn\s|#\[|let\s+(?:mut\s+)?[\w]+\s*[=:])/.test(source)) return 'rust';
    if (/^(?:--[^\n]*\n\s*)*(?:SELECT\s|WITH\s)/i.test(source)) return 'sql';
    if (/^[{[]/.test(source)) {
      try { const value = JSON.parse(source.replace(/^\s*\/\/[^\n]*$/gm, '')); if (value && typeof value === 'object') return 'json'; } catch (_) { /* Human output stays plain. */ }
    }
    if (/^\[[\w.-]+\]\s*(?:\n|$)/.test(source) && /^\s*[\w.-]+\s*=/m.test(source)) return 'toml';
    if (/^(?:#[^\n]*\n\s*)*(?:\$\s*)?(?:git\s+clone\s|cd\s|cargo\s|docker\s|make(?:\s|$)|export\s+[A-Z_]+=|\.\/target\/)/.test(source)) return 'bash';
    if (/^(?:\/\/[^\n]*\n\s*)*(?:client|products|table|write)\.[\w]+\(/.test(source)) return 'rust';
    return null;
  }

  // Composite the actual code background through transparent ancestors.
  function theme(node) {
    let rgb = [0, 0, 0], alpha = 0;
    for (let el = node; el && alpha < .999; el = el.parentElement) {
      const color = getComputedStyle(el).backgroundColor;
      const parts = color.match(/[\d.]+/g);
      if (!parts || parts.length < 3) continue;
      const a = parts.length > 3 ? Number(parts[3]) : 1;
      for (let i = 0; i < 3; i++) rgb[i] += Number(parts[i]) * a * (1 - alpha);
      alpha += a * (1 - alpha);
    }
    rgb = rgb.map(channel => (channel + 255 * (1 - alpha)) / 255);
    const linear = rgb.map(channel => channel <= .04045 ? channel / 12.92 : ((channel + .055) / 1.055) ** 2.4);
    return linear[0] * .2126 + linear[1] * .7152 + linear[2] * .0722 > .3 ? 'light' : 'dark';
  }

  // Use DOM text nodes: Prism's HTML encoder replaces NBSP, which would change copy.
  function append(parent, token) {
    if (typeof token === 'string') { parent.appendChild(document.createTextNode(token)); return; }
    if (Array.isArray(token)) { token.forEach(item => append(parent, item)); return; }
    const span = document.createElement('span');
    span.classList.add('token', token.type);
    const names = token.alias ? (Array.isArray(token.alias) ? token.alias : [token.alias]) : [];
    names.forEach(name => span.classList.add(name));
    append(span, token.content);
    parent.appendChild(span);
  }

  function highlight(pre) {
    if (!pre.isConnected) return;
    const node = pre.querySelector('code') || pre;
    const text = node.textContent;
    const lang = language(node, text);
    const currentTheme = theme(node);
    const previous = states.get(node);
    if (previous && previous.text === text && previous.lang === lang && previous.html === node.innerHTML) {
      if (lang) node.dataset.syntaxTheme = currentTheme;
      return;
    }
    if (lang && prism.languages[lang]) {
      const fragment = document.createDocumentFragment();
      append(fragment, prism.tokenize(text, prism.languages[lang]));
      node.replaceChildren(fragment);
      node.classList.add('syntax-highlight');
      node.dataset.syntaxLanguage = lang;
      node.dataset.syntaxTheme = currentTheme;
    } else if (node.classList.contains('syntax-highlight')) {
      node.replaceChildren(document.createTextNode(text));
      node.classList.remove('syntax-highlight');
      delete node.dataset.syntaxLanguage;
      delete node.dataset.syntaxTheme;
    }
    states.set(node, { text, lang, html: node.innerHTML });
  }

  function queue(node) {
    const element = node.nodeType === 1 ? node : node.parentElement;
    if (!element) return;
    const pre = element.closest('pre');
    if (pre) pending.add(pre);
    element.querySelectorAll('pre').forEach(item => pending.add(item));
    if (!scheduled && pending.size) {
      scheduled = true;
      requestAnimationFrame(() => { scheduled = false; const batch = [...pending]; pending.clear(); batch.forEach(highlight); });
    }
  }
  function start() {
    queue(document.body);
    new MutationObserver(records => {
      records.forEach(record => {
        queue(record.target);
        if (record.type === 'childList') record.addedNodes.forEach(queue);
      });
    }).observe(document.body, { subtree: true, childList: true, characterData: true, attributes: true, attributeFilter: ['class', 'data-language', 'data-no-highlight'] });
  }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', start, { once: true });
  else start();
})();
