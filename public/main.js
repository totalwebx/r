/* global QRCode, Chart, io */

const API_BASE = '';
const $ = (id) => document.getElementById(id);

const state = {
  me: { username: null, credit: 0, rate: null },

  clients: [],
  accounts: [],

  logs: { sent: [], delivered: [], seen: [], failed: [], counts: { sent: 0, delivered: 0, seen: 0, failed: 0 } },

  // DataTable state
  dt: {
    page: 1,
    pageSize: 50,
    sortKey: 'at',
    sortDir: 'desc',
  },

  // QR modal
  qrTimer: null,
  lastQrText: null,
  lastQrClient: null,

  // Sender messages
  msgCounter: 0,

  // Chats
  chats: [],
  selectedChat: null,

  // Quotas
  quotas: {},
  lastTotalTargets: 0,

  // Charts
  charts: {
    timeline: null,
    topSent: null,
    topDelivered: null,
    funnel: null,
    lastFetchAt: 0,
  },

  // Realtime progress
  job: { jobId: null, total: 0, done: 0, ok: 0, failed: 0, status: 'idle' },
};

// ---------------- Utils ----------------
function escapeHtml(s) {
  return String(s ?? '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#039;');
}
function setText(id, txt) { const el = $(id); if (el) el.textContent = txt; }

function parseNumbers(text) {
  return String(text || '')
    .split(/[\n,; \t]+/g)
    .map((s) => s.trim())
    .filter(Boolean);
}
function toMoroccoJid(phone) {
  const digits = String(phone || '').replace(/\D/g, '');
  if (!digits) return '';
  if (digits.startsWith('212')) return digits + '@c.us';
  if (digits.startsWith('0')) return '212' + digits.slice(1) + '@c.us';
  return '212' + digits + '@c.us';
}
function randToken() { return Math.random().toString(36).slice(2, 8); }
function applyTemplate(text, ctx) {
  return String(text || '')
    .replaceAll('{{number}}', ctx.number)
    .replaceAll('{{jid}}', ctx.jid)
    .replaceAll('{{index}}', String(ctx.index))
    .replaceAll('{{rand}}', ctx.rand);
}
function fmtTime(iso) {
  try { return iso ? new Date(iso).toLocaleString() : ''; }
  catch { return iso || ''; }
}
function clampInt(v, fallback, min, max) {
  const n = parseInt(v, 10);
  if (!Number.isFinite(n)) return fallback;
  return Math.min(Math.max(n, min), max);
}
function toIsoFromLocalInput(v) {
  if (!v) return null;
  const d = new Date(v);
  const t = d.getTime();
  if (!Number.isFinite(t)) return null;
  return d.toISOString();
}
function withinRangeISO(iso, fromIso, toIso) {
  const t = Date.parse(iso);
  if (!Number.isFinite(t)) return false;
  const f = fromIso ? Date.parse(fromIso) : -Infinity;
  const to = toIso ? Date.parse(toIso) : Infinity;
  return t >= f && t <= to;
}

// ---------------- Navigation ----------------
function setActiveNav(which) {
  const tabs = ['Sender','Generator','Chats','Logs','Charts','Accounts'];
  tabs.forEach((k) => { const b = $('nav'+k); if (b) b.classList.remove('active'); });
  const activeBtn = $('nav'+which);
  if (activeBtn) activeBtn.classList.add('active');

  ['sender','generator','chats','logs','charts','accounts'].forEach((p) => {
    const el = $('page-'+p);
    if (el) el.classList.remove('active');
  });
  const page = $('page-'+which.toLowerCase());
  if (page) page.classList.add('active');

  if (which === 'Sender') {
    setText('pageTitle', 'Sender');
    setText('pageSub', 'Send with one account or distribute across all accounts automatically.');
    updatePreview();
    updateTotalTargetsUI();
    refreshQuotasUI();
    updateSafeHint();
  }
  if (which === 'Generator') {
    setText('pageTitle', 'Generator');
    setText('pageSub', 'Create phone numbers from a prefix and push them to Sender.');
  }
  if (which === 'Chats') {
    setText('pageTitle', 'Chats');
    setText('pageSub', 'All conversations (saved + unsaved) and CRM categories.');
    refreshChats();
  }
  if (which === 'Logs') {
    setText('pageTitle', 'Logs');
    setText('pageSub', 'Data table logs with filters and export.');
    loadLogs();
  }
  if (which === 'Charts') {
    setText('pageTitle', 'Charts');
    setText('pageSub', 'Analytics by time range and account activity.');
    refreshCharts();
  }
  if (which === 'Accounts') {
    setText('pageTitle', 'Account Management');
    setText('pageSub', 'Rename / reconnect / delete. Accounts persist after restart.');
    loadAccounts();
  }
}

$('navSender').onclick = () => setActiveNav('Sender');
$('navGenerator').onclick = () => setActiveNav('Generator');
$('navChats').onclick = () => setActiveNav('Chats');
$('navLogs').onclick = () => setActiveNav('Logs');
$('navCharts').onclick = () => setActiveNav('Charts');
$('navAccounts').onclick = () => setActiveNav('Accounts');

// ---------------- Auth / Me / Logout ----------------
async function loadMe() {
  try {
    const r = await fetch(`${API_BASE}/api/me`);
    const d = await r.json();
    if (!r.ok || !d.success) return;
    state.me.username = d.username;
    state.me.credit = d.credit ?? 0;
    state.me.rate = d.rate || null;
    setText('meUser', d.username || '—');
    setText('meCredit', `$${Number(d.credit ?? 0).toFixed(2)}`);
    if (d.rate) {
      setText('billingHint', `Billing: 1000 delivered = $${d.rate.dollarsPer1000Delivered} (=$${d.rate.costPerDelivered.toFixed(2)}/delivered).`);
    }
  } catch {}
}

$('logoutBtn').onclick = async () => {
  try { await fetch(`${API_BASE}/api/logout`, { method: 'POST' }); } catch {}
  window.location.href = '/login';
};

// ---------------- Safe Mode presets ----------------
const SAFE_PRESETS = {
  chill: { perClientMaxPerMinute: 10, warmupMinutes: 12, safeExtraDelayFrom: 2, safeExtraDelayTo: 5, autoEnable: true, label: 'Chill' },
  normal: { perClientMaxPerMinute: 18, warmupMinutes: 6, safeExtraDelayFrom: 1, safeExtraDelayTo: 3, autoEnable: true, label: 'Normal' },
  aggressive: { perClientMaxPerMinute: 30, warmupMinutes: 3, safeExtraDelayFrom: 0, safeExtraDelayTo: 1, autoEnable: true, label: 'Aggressive' },
};

function getReadyClientIds() {
  return state.clients.filter((c) => c.ready && !c.coolingDown).map((c) => c.id);
}

function applySafePreset(key) {
  const p = SAFE_PRESETS[key] || SAFE_PRESETS.normal;
  if (p.autoEnable) $('safeMode').checked = true;
  $('perClientMaxPerMinute').value = String(p.perClientMaxPerMinute);
  $('warmupMinutes').value = String(p.warmupMinutes);
  $('safeExtraDelayFrom').value = String(p.safeExtraDelayFrom);
  $('safeExtraDelayTo').value = String(p.safeExtraDelayTo);
  updateSafeHint();
}

function updateSafeHint() {
  const enabled = $('safeMode').checked;
  const preset = $('safePreset').value;
  const ready = getReadyClientIds();

  const perMin = clampInt($('perClientMaxPerMinute').value, 18, 1, 999);
  const warm = clampInt($('warmupMinutes').value, 6, 0, 999);
  const exFrom = clampInt($('safeExtraDelayFrom').value, 1, 0, 999);
  const exTo = clampInt($('safeExtraDelayTo').value, 3, 0, 999);

  const totalPerMin = perMin * Math.max(1, ready.length);
  const exStr = exFrom === exTo ? `${exFrom}s` : `${exFrom}-${exTo}s`;

  const safeHint = $('safeHint');
  if (!safeHint) return;

  safeHint.textContent =
    (enabled ? `🛡️ Safe Mode ON (${SAFE_PRESETS[preset]?.label || preset})` : `⚠️ Safe Mode OFF`) +
    `\nReady accounts: ${ready.length}` +
    `\nCap per account: ${perMin}/min` +
    `\nTotal theoretical cap: ~${totalPerMin}/min (before delays)` +
    `\nWarm-up: ${warm} min` +
    `\nExtra delay: ${exStr}` +
    `\nAuto: cooldown + failover on errors/disconnect.`;
}

function maybeDowngradeAggressiveIfOneAccount() {
  const ready = getReadyClientIds();
  if (ready.length <= 1 && $('safePreset').value === 'aggressive') {
    $('safePreset').value = 'normal';
    applySafePreset('normal');
    $('safeHint').textContent += `\n\nℹ️ Aggressive downgraded to Normal (only 1 ready account).`;
  }
}

// Safe mode UI bindings
$('safePreset').onchange = () => { applySafePreset($('safePreset').value); maybeDowngradeAggressiveIfOneAccount(); };
$('safeMode').onchange = () => updateSafeHint();
$('perClientMaxPerMinute').oninput = () => updateSafeHint();
$('warmupMinutes').oninput = () => updateSafeHint();
$('safeExtraDelayFrom').oninput = () => updateSafeHint();
$('safeExtraDelayTo').oninput = () => updateSafeHint();

// ---------------- QR Modal ----------------
function openQrModal(clientId) {
  $('qrModal').classList.remove('hidden');
  $('qrModal').setAttribute('aria-hidden', 'false');
  $('qrClient').textContent = clientId || '—';
  state.lastQrClient = clientId || null;
}
function closeQrModal() {
  $('qrModal').classList.add('hidden');
  $('qrModal').setAttribute('aria-hidden', 'true');
  clearInterval(state.qrTimer);
  state.qrTimer = null;
  state.lastQrText = null;
  state.lastQrClient = null;
}
function setQrMeta(clientId, timeIso) {
  $('qrClient').textContent = clientId || '—';
  $('qrTime').textContent = timeIso ? fmtTime(timeIso) : '—';
}
function renderQr(qrText) {
  const box = $('qrBox');
  box.innerHTML = '';
  if (!qrText) { box.innerHTML = `<div class="hint">Waiting for QR…</div>`; return; }
  state.lastQrText = qrText;
  // eslint-disable-next-line no-undef
  new QRCode(box, { text: qrText, width: 360, height: 360, correctLevel: QRCode.CorrectLevel.M });
}
async function fetchQr(clientId) {
  try {
    const res = await fetch(`${API_BASE}/qr/${encodeURIComponent(clientId)}`);
    const data = await res.json();
    if (!res.ok || !data.success) return null;
    return data;
  } catch { return null; }
}
async function startQrPolling(clientId) {
  if (!clientId) return;
  openQrModal(clientId);
  renderQr(null);

  const first = await fetchQr(clientId);
  if (first) {
    setQrMeta(clientId, first.lastQrAt);
    if (first.ready) { closeQrModal(); checkHealth(); return; }
    renderQr(first.qr);
  }

  clearInterval(state.qrTimer);
  state.qrTimer = setInterval(async () => {
    const d = await fetchQr(clientId);
    if (!d) return;
    setQrMeta(clientId, d.lastQrAt);
    if (d.ready) { closeQrModal(); checkHealth(); return; }
    if (d.qr && d.qr !== state.lastQrText) renderQr(d.qr);
  }, 2000);
}

$('qrCloseBtn').onclick = closeQrModal;
$('qrRefreshBtn').onclick = () => startQrPolling(state.lastQrClient || $('clientSelect').value);
$('qrCopyBtn').onclick = async () => {
  if (!state.lastQrText) return alert('No QR text yet.');
  try { await navigator.clipboard.writeText(state.lastQrText); alert('QR text copied'); }
  catch { alert('Copy blocked by browser'); }
};
$('qrModal').addEventListener('click', (e) => { if (e.target && e.target.id === 'qrModal') closeQrModal(); });

$('connectBtn').onclick = () => startQrPolling($('clientSelect').value);

// ---------------- Clients / Health / Add Account ----------------
async function loadClients() {
  try {
    const res = await fetch(`${API_BASE}/clients`);
    const data = await res.json();
    if (!res.ok || !data.success) return;

    state.clients = data.clients;

    const sel = $('clientSelect');
    const old = sel.value;
    sel.innerHTML = '';
    data.clients.forEach((c) => {
      const opt = document.createElement('option');
      opt.value = c.id;
      const nm = c.name ? `${c.name} (${c.id})` : c.id;
      const badge = c.ready ? ' ✅' : (c.hasQr ? ' 🔳' : ' ⚠️');
      const cool = c.coolingDown ? ' 🧊' : '';
      opt.textContent = `${nm}${badge}${cool}`;
      sel.appendChild(opt);
    });
    if (old && data.clients.some((x) => x.id === old)) sel.value = old;

    // Logs filter client dropdown
    const logClient = $('logClient');
    if (logClient) {
      const prev = logClient.value;
      logClient.innerHTML = `<option value="">All accounts</option>`;
      data.clients.forEach((c) => {
        const opt = document.createElement('option');
        opt.value = c.id;
        opt.textContent = c.name ? `${c.name} (${c.id})` : c.id;
        logClient.appendChild(opt);
      });
      logClient.value = prev;
    }
  } catch {}
}

async function checkHealth() {
  const dot = $('dot');
  const text = $('healthText');
  text.textContent = 'Checking…';
  dot.className = 'dot';

  try {
    const res = await fetch(`${API_BASE}/health`);
    const data = await res.json();

    await loadClients();
    refreshQuotasUI();
    maybeDowngradeAggressiveIfOneAccount();
    updateSafeHint();

    const current = $('clientSelect').value || (state.clients[0]?.id ?? '');
    const c = state.clients.find((x) => x.id === current);

    if (res.ok && data.ok) {
      if (c && c.ready && !c.coolingDown) { dot.className = 'dot ok'; text.textContent = `Connected (${current})`; }
      else if (c && c.ready && c.coolingDown) { dot.className = 'dot'; text.textContent = `Cooling down (${current})`; }
      else { dot.className = 'dot'; text.textContent = c ? `Not ready (${current})` : 'Not ready'; }
    } else {
      dot.className = 'dot bad'; text.textContent = 'API error';
    }
  } catch {
    dot.className = 'dot bad';
    text.textContent = 'Offline';
  }
}

$('refreshBtn').onclick = () => checkHealth();
$('clientSelect').onchange = () => checkHealth();

$('addClientBtn').onclick = async () => {
  const status = $('addClientStatus');
  status.textContent = 'Adding account...';

  const desiredId = ($('newClientId').value || '').trim();

  try {
    const res = await fetch(`${API_BASE}/clients/add`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ id: desiredId || undefined }),
    });
    const data = await res.json();
    if (!res.ok || !data.success) {
      status.textContent = '❌ ' + (data.error || 'Failed to add');
      return;
    }

    status.textContent = data.created ? `✅ Added ${data.id}. Opening QR...` : `ℹ️ ${data.id} exists. Opening QR...`;
    $('newClientId').value = '';

    await loadClients();
    $('clientSelect').value = data.id;
    refreshQuotasUI();
    updateSafeHint();
    startQrPolling(data.id);
  } catch {
    status.textContent = '❌ API error (server offline?)';
  }
};

// ---------------- Sender: Messages UI ----------------
function addMsg(value = '') {
  state.msgCounter++;
  const id = state.msgCounter;

  const wrap = document.createElement('div');
  wrap.className = 'msgItem';
  wrap.dataset.id = String(id);
  wrap.innerHTML = `
    <div class="msgHead">
      <div>Message #${id}</div>
      <button class="small danger" data-remove="${id}">Remove</button>
    </div>
    <textarea class="msgText" rows="3" placeholder="Write message...">${escapeHtml(value)}</textarea>
    <div class="hint">Variables: <span class="mono">{{number}}</span> <span class="mono">{{jid}}</span> <span class="mono">{{index}}</span> <span class="mono">{{rand}}</span></div>
  `;

  $('messages').appendChild(wrap);
  wrap.querySelector('.msgText').addEventListener('input', updatePreview);
  wrap.querySelector('[data-remove]').addEventListener('click', () => removeMsg(id));
  updatePreview();
}
function removeMsg(id) {
  const el = document.querySelector(`.msgItem[data-id="${id}"]`);
  if (el) el.remove();
  updatePreview();
}
function clearMsgs() { $('messages').innerHTML = ''; state.msgCounter = 0; updatePreview(); }
function addExample() { addMsg('Hello {{number}} 👋\nReply with: beauty / cats / electro\nRef: {{rand}}'); }
function getMessages() {
  return Array.from(document.querySelectorAll('.msgText'))
    .map((t) => t.value.trim())
    .filter(Boolean);
}
$('addMsgBtn').onclick = () => addMsg();
$('addExampleBtn').onclick = () => addExample();
$('clearMsgsBtn').onclick = () => clearMsgs();

// Preview
function updatePreview() {
  const nums = parseNumbers($('numbers').value);
  const first = nums[0] || '0642284241';
  const msgs = getMessages();
  const ctx = { number: first, jid: toMoroccoJid(first), index: 1, rand: randToken() };

  if (!msgs.length) { $('preview').textContent = '(Add a message to preview…)'; return; }
  $('preview').textContent = applyTemplate(msgs[0], ctx);
}
$('numbers').addEventListener('input', () => { updatePreview(); updateTotalTargetsUI(); refreshQuotasUI(); });
$('limit').addEventListener('input', () => { updateTotalTargetsUI(); refreshQuotasUI(); });

// ---------------- Sender: Quotas logic ----------------
function computeTotalTargets() {
  const all = parseNumbers($('numbers').value);
  const lim = clampInt($('limit').value, 0, 0, 999999);
  return (lim > 0) ? Math.min(all.length, lim) : all.length;
}
function updateTotalTargetsUI() {
  const total = computeTotalTargets();
  state.lastTotalTargets = total;
  $('totalTargets').value = String(total);
}

function equalSplit(total, ids) {
  const out = {};
  if (!ids.length) return out;
  const base = Math.floor(total / ids.length);
  let rem = total - base * ids.length;
  for (const id of ids) { out[id] = base + (rem > 0 ? 1 : 0); rem--; }
  return out;
}
function normalizeQuotas(total) {
  const ids = getReadyClientIds();
  const q = { ...state.quotas };

  for (const k of Object.keys(q)) if (!ids.includes(k)) delete q[k];
  for (const id of ids) if (q[id] == null) q[id] = 0;

  const sum = ids.reduce((a, id) => a + (parseInt(q[id] || 0, 10) || 0), 0);
  if (sum === 0 && total > 0) { state.quotas = equalSplit(total, ids); return; }
  state.quotas = q;
  rebalanceQuotas(total, null);
}
function rebalanceQuotas(total, changedId) {
  const ids = getReadyClientIds();
  if (!ids.length) return;

  for (const id of ids) {
    const v = parseInt(state.quotas[id] || 0, 10);
    state.quotas[id] = Number.isFinite(v) ? Math.max(0, v) : 0;
  }

  let sum = ids.reduce((a, id) => a + state.quotas[id], 0);
  if (sum === total) return;

  const others = ids.filter((x) => x !== changedId);
  const order = others.length ? others : ids;

  while (sum < total) {
    for (const id of order) { if (sum >= total) break; state.quotas[id] += 1; sum += 1; }
    if (!order.length) break;
  }
  while (sum > total) {
    for (let i = order.length - 1; i >= 0; i--) {
      if (sum <= total) break;
      const id = order[i];
      if (state.quotas[id] > 0) { state.quotas[id] -= 1; sum -= 1; }
    }
    if (sum > total) {
      for (const id of ids) {
        if (sum <= total) break;
        if (state.quotas[id] > 0) { state.quotas[id]--; sum--; }
      }
    }
  }
}

function refreshQuotasUI() {
  const mode = $('sendMode').value;
  const box = $('quotaBox');
  const list = $('quotaList');
  const hint = $('quotaHint');

  updateTotalTargetsUI();

  if (mode !== 'all') { box.classList.add('hidden'); return; }

  const total = state.lastTotalTargets;
  const ids = getReadyClientIds();
  if (!ids.length) {
    box.classList.remove('hidden');
    list.innerHTML = `<div class="hint">No ready accounts. Connect one WhatsApp first.</div>`;
    hint.textContent = '';
    return;
  }

  normalizeQuotas(total);
  box.classList.remove('hidden');
  list.innerHTML = '';

  for (const id of ids) {
    const c = state.clients.find((x) => x.id === id);
    const nm = c?.name ? c.name : id;
    const row = document.createElement('div');
    row.className = 'quotaRow';
    row.innerHTML = `
      <div>
        <div class="nm">${escapeHtml(nm)}</div>
        <div class="meta mono">${escapeHtml(id)} • ready</div>
      </div>
      <div>
        <div class="hint" style="margin-top:0;">Limit</div>
        <input class="quotaInput" data-id="${escapeHtml(id)}" type="number" min="0" value="${state.quotas[id] || 0}" />
      </div>
      <div>
        <div class="hint" style="margin-top:0;">%</div>
        <input disabled value="${total > 0 ? Math.round((state.quotas[id] * 100) / total) : 0}%" />
      </div>
    `;
    list.appendChild(row);
  }

  hint.textContent = `Total targets: ${total}. Total quotas: ${ids.reduce((a, id) => a + (state.quotas[id] || 0), 0)}.`;

  list.querySelectorAll('.quotaInput').forEach((inp) => {
    inp.addEventListener('input', () => {
      const id = inp.dataset.id;
      const totalTargets = computeTotalTargets();
      const v = clampInt(inp.value, 0, 0, totalTargets);
      state.quotas[id] = v;
      rebalanceQuotas(totalTargets, id);
      refreshQuotasUI();
    });
  });
}

$('sendMode').onchange = () => refreshQuotasUI();
$('equalSplitBtn').onclick = () => {
  const total = computeTotalTargets();
  const ids = getReadyClientIds();
  state.quotas = equalSplit(total, ids);
  refreshQuotasUI();
};

// ---------------- Generator ----------------
function onlyDigits(s) { return String(s || '').replace(/\D/g, ''); }
function padLeft(num, width) {
  const s = String(num);
  if (s.length >= width) return s;
  return '0'.repeat(width - s.length) + s;
}
function randInt(min, max) { return Math.floor(Math.random() * (max - min + 1)) + min; }

$('genBtn').onclick = () => {
  const status = $('genStatus');
  const out = $('out');

  const prefix = onlyDigits($('prefix').value);
  const totalLen = parseInt($('totalLen').value, 10);
  const count = parseInt($('count').value, 10);
  const mode = $('mode').value;
  const start = parseInt($('start').value, 10) || 0;

  if (!prefix) { status.textContent = '❌ Prefix is empty'; return; }
  if (!Number.isFinite(totalLen) || totalLen < 1) { status.textContent = '❌ Total length must be >= 1'; return; }
  if (!Number.isFinite(count) || count < 1) { status.textContent = '❌ Count must be >= 1'; return; }
  if (prefix.length > totalLen) { status.textContent = `❌ Prefix longer than total length`; return; }

  const suffixLen = totalLen - prefix.length;
  const lines = [];
  const seen = new Set();

  if (mode === 'sequential') {
    for (let i = 0; i < count; i++) {
      const n = start + i;
      const suffix = suffixLen > 0 ? padLeft(n, suffixLen) : '';
      lines.push(prefix + suffix);
    }
  } else {
    const maxComb = suffixLen > 0 ? Math.pow(10, suffixLen) : 1;
    const maxUnique = Math.min(count, maxComb);
    while (lines.length < maxUnique) {
      const n = suffixLen > 0 ? randInt(0, maxComb - 1) : 0;
      const suffix = suffixLen > 0 ? padLeft(n, suffixLen) : '';
      const phone = prefix + suffix;
      if (!seen.has(phone)) { seen.add(phone); lines.push(phone); }
    }
  }

  out.value = lines.join('\n');
  status.textContent = `✅ Generated ${lines.length} number(s)`;
};

$('copyGenBtn').onclick = async () => {
  const status = $('genStatus');
  const out = $('out');
  if (!out.value.trim()) { status.textContent = 'ℹ️ Nothing to copy'; return; }
  try { await navigator.clipboard.writeText(out.value); status.textContent = '✅ Copied'; }
  catch { status.textContent = '❌ Copy failed'; }
};
$('clearGenBtn').onclick = () => { $('out').value = ''; $('genStatus').textContent = ''; };

function fillFromGenerator() {
  const generated = $('out').value || '';
  if (!generated.trim()) return alert('No generated numbers yet.');
  $('numbers').value = generated;
  setActiveNav('Sender');
  updatePreview();
  updateTotalTargetsUI();
  refreshQuotasUI();
}
$('useGeneratedBtn').onclick = () => fillFromGenerator();
$('useGeneratedBtn2').onclick = () => fillFromGenerator();

// ---------------- Sender: Send ----------------
$('sendBtn').onclick = async () => {
  const status = $('status');
  const result = $('result');

  const numbersRaw = $('numbers').value;
  const numbers = parseNumbers(numbersRaw);
  const msgs = getMessages();

  if (!numbers.length) { status.textContent = '❌ Please enter at least one number.'; return; }

  const photo = $('photo').files[0];
  const sendContactEnabled = $('sendContactEnabled').checked;

  if (!msgs.length && !photo && !sendContactEnabled) {
    status.textContent = '❌ Add a message OR choose a photo OR enable contact card.';
    return;
  }
  if (sendContactEnabled) {
    const cn = $('contactName').value.trim();
    const cp = $('contactPhone').value.trim();
    if (!cn || !cp) { status.textContent = '❌ Contact name and phone required when contact enabled.'; return; }
  }

  const sendMode = $('sendMode').value;
  const form = new FormData();

  form.append('sendMode', sendMode);
  form.append('clientId', $('clientSelect').value || '');
  if (sendMode === 'all') {
    const total = computeTotalTargets();
    normalizeQuotas(total);
    form.append('perClientLimit', JSON.stringify(state.quotas || {}));
  }

  form.append('numbers', numbersRaw);
  form.append('messages', JSON.stringify(msgs));
  form.append('delayFrom', $('delayFrom').value);
  form.append('delayTo', $('delayTo').value);
  form.append('limit', $('limit').value);

  if (photo) form.append('photo', photo);

  form.append('sendContactEnabled', sendContactEnabled ? '1' : '0');
  form.append('contactName', $('contactName').value.trim());
  form.append('contactPhone', $('contactPhone').value.trim());

  // Safe mode fields
  form.append('safeMode', $('safeMode').checked ? '1' : '0');
  form.append('perClientMaxPerMinute', $('perClientMaxPerMinute').value);
  form.append('warmupMinutes', $('warmupMinutes').value);
  form.append('safeExtraDelayFrom', $('safeExtraDelayFrom').value);
  form.append('safeExtraDelayTo', $('safeExtraDelayTo').value);

  status.textContent = 'Sending… (watch progress bar)';
  result.textContent = '';

  try {
    const res = await fetch(`${API_BASE}/send`, { method: 'POST', body: form });
    const data = await res.json();

    if (!res.ok) {
      status.textContent = '❌ ' + (data.error || 'Request failed');
      result.textContent = JSON.stringify(data, null, 2);
      return;
    }

    status.textContent = `✅ Done (failovers: ${data.failoverCount ?? 0})`;
    result.textContent = JSON.stringify(data, null, 2);

    // refresh
    loadLogs();
    loadAccounts();
    checkHealth();
    loadMe();
  } catch {
    status.textContent = '❌ API error (server offline?)';
  }
};

// ---------------- Logs: load + DataTable render ----------------
async function loadLogs() {
  try {
    const res = await fetch(`${API_BASE}/logs`);
    const data = await res.json();
    if (!res.ok || !data.success) return;

    state.logs = data;

    setText('kSent', data.counts.sent);
    setText('kDelivered', data.counts.delivered);
    setText('kSeen', data.counts.seen);
    setText('kFailed', data.counts.failed);

    state.dt.page = 1;
    renderLogsTable();
  } catch {}
}

function getLogsCurrentType() {
  return $('logTab').value || 'sent';
}

function getFilteredSortedLogs() {
  const type = getLogsCurrentType();
  const q = ($('logQ').value || '').toLowerCase().trim();
  const client = $('logClient').value || '';
  const fromIso = toIsoFromLocalInput($('logFrom').value);
  const toIso = toIsoFromLocalInput($('logTo').value);

  let items = (state.logs[type] || []).slice();

  if (client) items = items.filter((x) => (x.clientId || '') === client);
  if (q) items = items.filter((x) => String(x.number || '').toLowerCase().includes(q));
  if (fromIso || toIso) items = items.filter((x) => withinRangeISO(x.at, fromIso, toIso));

  const { sortKey, sortDir } = state.dt;
  items.sort((a, b) => {
    const va = a[sortKey] ?? '';
    const vb = b[sortKey] ?? '';
    const cmp = String(va).localeCompare(String(vb));
    return sortDir === 'asc' ? cmp : -cmp;
  });

  return items;
}

function renderLogsTable() {
  const body = $('logBody');
  body.innerHTML = '';

  const items = getFilteredSortedLogs();
  const pageSize = state.dt.pageSize;
  const totalPages = Math.max(1, Math.ceil(items.length / pageSize));

  state.dt.page = Math.min(Math.max(1, state.dt.page), totalPages);

  const start = (state.dt.page - 1) * pageSize;
  const slice = items.slice(start, start + pageSize);

  for (const it of slice) {
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td>${escapeHtml(fmtTime(it.at))}</td>
      <td class="mono">${escapeHtml(it.number || '')}</td>
      <td class="mono">${escapeHtml(it.clientId || '')}</td>
    `;
    body.appendChild(tr);
  }

  $('logPageInfo').textContent = `Page ${state.dt.page} / ${totalPages} • Rows ${items.length}`;
}

$('refreshLogsBtn').onclick = () => loadLogs();
$('logQ').oninput = () => { state.dt.page = 1; renderLogsTable(); };
$('logTab').onchange = () => { state.dt.page = 1; renderLogsTable(); };
$('logClient').onchange = () => { state.dt.page = 1; renderLogsTable(); };
$('logFrom').onchange = () => { state.dt.page = 1; renderLogsTable(); };
$('logTo').onchange = () => { state.dt.page = 1; renderLogsTable(); };

$('logPageSize').onchange = () => {
  state.dt.pageSize = clampInt($('logPageSize').value, 50, 1, 500);
  state.dt.page = 1;
  renderLogsTable();
};

$('logPrev').onclick = () => { state.dt.page = Math.max(1, state.dt.page - 1); renderLogsTable(); };
$('logNext').onclick = () => { state.dt.page = state.dt.page + 1; renderLogsTable(); };

$('logClearFilter').onclick = () => {
  $('logQ').value = '';
  $('logClient').value = '';
  $('logFrom').value = '';
  $('logTo').value = '';
  state.dt.page = 1;
  renderLogsTable();
};

function exportLogsCsv() {
  const items = getFilteredSortedLogs();
  const rows = [['Time','Number','Client']];
  for (const it of items) rows.push([it.at || '', it.number || '', it.clientId || '']);

  const csv = rows.map((r) =>
    r.map((x) => `"${String(x).replaceAll('"','""')}"`).join(',')
  ).join('\n');

  const blob = new Blob([csv], { type: 'text/csv;charset=utf-8' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = `logs_${getLogsCurrentType()}_${Date.now()}.csv`;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}
$('logExport').onclick = exportLogsCsv;

// sortable headers
document.querySelectorAll('table.dt thead th[data-sort]').forEach((th) => {
  th.addEventListener('click', () => {
    const key = th.dataset.sort;
    if (state.dt.sortKey === key) state.dt.sortDir = (state.dt.sortDir === 'asc' ? 'desc' : 'asc');
    else { state.dt.sortKey = key; state.dt.sortDir = 'desc'; }
    renderLogsTable();
  });
});

// ---------------- Charts ----------------
function destroyChart(c) { try { if (c) c.destroy(); } catch {} }

function setDefaultChartRange() {
  // default: last 7 days
  const now = new Date();
  const from = new Date(now.getTime() - 7 * 24 * 3600 * 1000);

  const toLocal = (d) => {
    const pad = (n) => String(n).padStart(2,'0');
    return `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}`;
  };

  if (!$('chartFrom').value) $('chartFrom').value = toLocal(from);
  if (!$('chartTo').value) $('chartTo').value = toLocal(now);
}

async function fetchTimeSeries() {
  const group = $('chartGroup').value || 'day';
  const fromIso = toIsoFromLocalInput($('chartFrom').value);
  const toIso = toIsoFromLocalInput($('chartTo').value);

  const url = `${API_BASE}/stats/timeseries?group=${encodeURIComponent(group)}&from=${encodeURIComponent(fromIso||'')}&to=${encodeURIComponent(toIso||'')}`;
  const res = await fetch(url);
  const data = await res.json();
  if (!res.ok || !data.success) throw new Error(data.error || 'stats error');
  return data;
}
async function fetchTopClients(type) {
  const fromIso = toIsoFromLocalInput($('chartFrom').value);
  const toIso = toIsoFromLocalInput($('chartTo').value);
  const url = `${API_BASE}/stats/clients?type=${encodeURIComponent(type)}&from=${encodeURIComponent(fromIso||'')}&to=${encodeURIComponent(toIso||'')}`;
  const res = await fetch(url);
  const data = await res.json();
  if (!res.ok || !data.success) throw new Error(data.error || 'clients stats error');
  return data;
}

function buildTimelineChart(series) {
  const ctx = $('chartTimeline').getContext('2d');

  const allKeys = new Set();
  ['sent','delivered','seen','failed'].forEach((k) => (series[k] || []).forEach((p) => allKeys.add(p.t)));
  const labels = Array.from(allKeys).sort((a,b)=>String(a).localeCompare(String(b)));

  const mapTo = (arr) => {
    const m = new Map(arr.map((x)=>[x.t, x.v]));
    return labels.map((t)=>m.get(t) || 0);
  };

  destroyChart(state.charts.timeline);
  state.charts.timeline = new Chart(ctx, {
    type: 'line',
    data: {
      labels,
      datasets: [
        { label: 'Sent', data: mapTo(series.sent || []) },
        { label: 'Delivered', data: mapTo(series.delivered || []) },
        { label: 'Seen', data: mapTo(series.seen || []) },
        { label: 'Failed', data: mapTo(series.failed || []) },
      ]
    },
    options: {
      responsive: true,
      plugins: { legend: { labels: { color: '#eaf0ff' } } },
      scales: {
        x: { ticks: { color: 'rgba(234,240,255,.75)' }, grid: { color: 'rgba(255,255,255,.06)' } },
        y: { ticks: { color: 'rgba(234,240,255,.75)' }, grid: { color: 'rgba(255,255,255,.06)' } },
      }
    }
  });
}

function buildTopChart(canvasId, title, list, topN) {
  const ctx = $(canvasId).getContext('2d');

  const filtered = (list || []).slice(0, topN);
  const labels = filtered.map((x) => x.clientId);
  const values = filtered.map((x) => x.v);

  destroyChart(state.charts[title]);
  state.charts[title] = new Chart(ctx, {
    type: 'bar',
    data: { labels, datasets: [{ label: title, data: values }] },
    options: {
      responsive: true,
      plugins: { legend: { display: false } },
      scales: {
        x: { ticks: { color: 'rgba(234,240,255,.75)' }, grid: { color: 'rgba(255,255,255,.06)' } },
        y: { ticks: { color: 'rgba(234,240,255,.75)' }, grid: { color: 'rgba(255,255,255,.06)' } },
      }
    }
  });
}

function buildFunnel(sentCount, deliveredCount, seenCount, failedCount) {
  const ctx = $('chartFunnel').getContext('2d');
  destroyChart(state.charts.funnel);
  state.charts.funnel = new Chart(ctx, {
    type: 'bar',
    data: {
      labels: ['Sent', 'Delivered', 'Seen', 'Failed'],
      datasets: [{ label: 'Count', data: [sentCount, deliveredCount, seenCount, failedCount] }]
    },
    options: {
      responsive: true,
      plugins: { legend: { display: false } },
      scales: {
        x: { ticks: { color: 'rgba(234,240,255,.75)' }, grid: { color: 'rgba(255,255,255,.06)' } },
        y: { ticks: { color: 'rgba(234,240,255,.75)' }, grid: { color: 'rgba(255,255,255,.06)' } },
      }
    }
  });
}

async function refreshCharts() {
  try {
    setDefaultChartRange();
    $('chartHint').textContent = 'Loading charts...';

    const topN = clampInt($('topClients').value, 8, 1, 50);

    const ts = await fetchTimeSeries();
    buildTimelineChart(ts.series);

    const topSent = await fetchTopClients('sent');
    buildTopChart('chartTopSent', 'topSent', topSent.byClient, topN);

    const topDel = await fetchTopClients('delivered');
    buildTopChart('chartTopDelivered', 'topDelivered', topDel.byClient, topN);

    // funnel totals within range: sum from timeseries arrays
    const sum = (arr) => (arr || []).reduce((a, x) => a + (x.v || 0), 0);
    buildFunnel(
      sum(ts.series.sent),
      sum(ts.series.delivered),
      sum(ts.series.seen),
      sum(ts.series.failed),
    );

    state.charts.lastFetchAt = Date.now();
    $('chartHint').textContent = `✅ Updated: ${new Date().toLocaleString()}`;
  } catch (e) {
    $('chartHint').textContent = '❌ ' + (e?.message || 'Failed to load charts');
  }
}

$('refreshChartsBtn').onclick = () => refreshCharts();
$('chartGroup').onchange = () => refreshCharts();
$('chartFrom').onchange = () => refreshCharts();
$('chartTo').onchange = () => refreshCharts();
$('topClients').oninput = () => {
  // fast update without refetch if possible
  refreshCharts();
};

// ---------------- Progress bar (realtime) ----------------
function setProgress(job) {
  const total = job.total || 0;
  const done = job.done || 0;
  const ok = job.ok || 0;
  const failed = job.failed || 0;

  const pct = total > 0 ? Math.round((done * 100) / total) : 0;

  $('jobId').textContent = job.jobId || '—';
  $('jobCounts').textContent = `${done} / ${total}`;
  $('jobPct').textContent = `${pct}%`;
  $('progressFill').style.width = `${pct}%`;

  const status = job.status || 'idle';
  const meta =
    status === 'start' ? 'Job started…' :
    status === 'update' ? 'Sending…' :
    status === 'finish' ? 'Job finished ✅' :
    status === 'stopped' ? 'Stopped ⚠️' :
    'No active job';

  $('jobMeta').textContent = meta;
  $('jobStats').textContent = `OK: ${ok} • Failed: ${failed}`;
}

// ---------------- Chats ----------------
function setChatPanelVisible(visible) {
  if (visible) { $('chatPanelEmpty').classList.add('hidden'); $('chatPanel').classList.remove('hidden'); }
  else { $('chatPanelEmpty').classList.remove('hidden'); $('chatPanel').classList.add('hidden'); }
}
function renderChatList() {
  const q = ($('chatSearch').value || '').toLowerCase().trim();
  const list = $('chatList');
  list.innerHTML = '';

  let items = state.chats.slice();
  if (q) {
    items = items.filter((c) =>
      String(c.name || '').toLowerCase().includes(q) ||
      String(c.number || '').toLowerCase().includes(q) ||
      String(c.category || '').toLowerCase().includes(q)
    );
  }

  for (const c of items) {
    const div = document.createElement('div');
    div.className = 'chatItem' + (state.selectedChat?.chatId === c.chatId ? ' active' : '');

    const cat = c.category ? `<span class="tagPill">${escapeHtml(c.category)}</span>` : '';
    const unread = (c.unreadCount || 0) > 0 ? `<span class="tagPill unread">Unread ${c.unreadCount}</span>` : '';
    const mine = c.isMyContact ? `<span class="tagPill mine">Contact</span>` : `<span class="tagPill">Unsaved</span>`;

    div.innerHTML = `
      <div class="chatRow">
        <div>
          <div class="chatName">${escapeHtml(c.name || 'Unknown')}</div>
          <div class="chatSub mono">${escapeHtml(c.number || '')} • ${escapeHtml(fmtTime(c.lastTimestamp) || '')}</div>
          <div class="chatSub">${escapeHtml(c.lastText || '')}</div>
        </div>
        <div class="chatBadges">
          ${mine}
          ${cat}
          ${unread}
        </div>
      </div>
    `;
    div.onclick = () => selectChat(c.chatId);
    list.appendChild(div);
  }
}
function selectChat(chatId) {
  const c = state.chats.find((x) => x.chatId === chatId);
  if (!c) return;

  state.selectedChat = c;
  setChatPanelVisible(true);

  setText('selName', c.name || 'Unknown');
  setText('selNumber', c.number || '');
  setText('selChatId', c.chatId || '');
  $('selUnread').textContent = String(c.unreadCount || 0);

  $('selPreview').textContent =
    (c.lastText ? `Last: ${c.lastText}\n` : '') +
    (c.lastTimestamp ? `Updated: ${fmtTime(c.lastTimestamp)}` : '') +
    `\nSaved contact: ${c.isMyContact ? 'YES' : 'NO'}`;

  $('selCategory').value = c.category || '';
  $('selNotes').value = c.notes || '';
  $('chatReply').value = '';
  $('chatStatus').textContent = '';

  renderChatList();
}

async function refreshChats() {
  const btn = $('refreshChatsBtn');
  if (!btn) return;

  btn.textContent = 'Loading...';
  btn.disabled = true;

  try {
    const clientId = $('clientSelect').value || '';
    const onlyReplied = $('onlyReplied').value || '0';
    const onlyContacts = $('onlyContacts').value || '0';
    const limit = $('chatLimit').value || '80';

    const url =
      `${API_BASE}/chats/list?clientId=${encodeURIComponent(clientId)}` +
      `&onlyReplied=${encodeURIComponent(onlyReplied)}` +
      `&onlyContacts=${encodeURIComponent(onlyContacts)}` +
      `&limit=${encodeURIComponent(limit)}`;

    const res = await fetch(url);
    const data = await res.json();

    if (!res.ok || !data.success) {
      alert(data.error || 'Failed to load chats. Is this account connected?');
      return;
    }

    state.chats = data.chats || [];

    if (state.selectedChat) {
      const still = state.chats.find((x) => x.chatId === state.selectedChat.chatId);
      state.selectedChat = still || null;
    }

    setChatPanelVisible(!!state.selectedChat);
    renderChatList();
    if (state.selectedChat) selectChat(state.selectedChat.chatId);
  } catch {
    alert('API error while loading chats');
  } finally {
    btn.textContent = 'Refresh chats';
    btn.disabled = false;
  }
}

$('refreshChatsBtn').onclick = () => refreshChats();
$('chatSearch').oninput = () => renderChatList();
$('onlyReplied').onchange = () => refreshChats();
$('onlyContacts').onchange = () => refreshChats();
$('chatLimit').onchange = () => refreshChats();

$('quickCats').addEventListener('click', (e) => {
  const cat = e.target?.dataset?.cat;
  if (!cat) return;
  $('selCategory').value = cat;
});

$('saveCategoryBtn').onclick = async () => {
  if (!state.selectedChat) return;
  const status = $('chatStatus');
  status.textContent = 'Saving...';

  const clientId = $('clientSelect').value || '';
  const chatId = state.selectedChat.chatId;
  const category = ($('selCategory').value || '').trim();
  const notes = ($('selNotes').value || '').trim();

  if (!category) { status.textContent = '❌ Category is required'; return; }

  try {
    const res = await fetch(`${API_BASE}/chats/category`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ clientId, chatId, category, notes }),
    });
    const data = await res.json();

    if (!res.ok || !data.success) { status.textContent = '❌ ' + (data.error || 'Failed'); return; }

    status.textContent = '✅ Saved';
    const idx = state.chats.findIndex((x) => x.chatId === chatId);
    if (idx >= 0) { state.chats[idx].category = category; state.chats[idx].notes = notes; }
    state.selectedChat.category = category;
    state.selectedChat.notes = notes;

    renderChatList();
  } catch {
    status.textContent = '❌ API error';
  }
};

$('copyChatBtn').onclick = async () => {
  if (!state.selectedChat) return;
  const num = state.selectedChat.number || '';
  try { await navigator.clipboard.writeText(num); $('chatStatus').textContent = '✅ Copied'; }
  catch { $('chatStatus').textContent = '❌ Copy failed'; }
};

$('sendChatBtn').onclick = async () => {
  if (!state.selectedChat) return;

  const status = $('chatStatus');
  const clientId = $('clientSelect').value || '';
  const chatId = state.selectedChat.chatId;
  const message = ($('chatReply').value || '').trim();

  if (!message) { status.textContent = '❌ Write a reply'; return; }
  status.textContent = 'Sending...';

  try {
    const res = await fetch(`${API_BASE}/chats/send`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ clientId, chatId, message }),
    });

    const data = await res.json();

    if (!res.ok || !data.success) { status.textContent = '❌ ' + (data.error || 'Send failed'); return; }

    status.textContent = `✅ Sent (client: ${data.clientId})`;
    $('chatReply').value = '';
    refreshChats();
  } catch {
    status.textContent = '❌ API error';
  }
};

// ---------------- Account Management ----------------
$('refreshAccountsBtn').onclick = () => loadAccounts();

async function loadAccounts() {
  const status = $('accountsStatus');
  status.textContent = 'Loading accounts...';

  try {
    const res = await fetch(`${API_BASE}/accounts`);
    const data = await res.json();

    if (!res.ok || !data.success) {
      status.textContent = '❌ ' + (data.error || 'Failed to load accounts');
      return;
    }

    state.accounts = data.accounts || [];
    status.textContent = `✅ Loaded ${state.accounts.length} account(s)`;
    renderAccounts();
  } catch {
    status.textContent = '❌ API error';
  }
}

function renderAccounts() {
  const grid = $('accountsGrid');
  grid.innerHTML = '';

  for (const a of state.accounts) {
    const readyPill = a.ready
      ? `<span class="tagPill mine">Ready</span>`
      : `<span class="tagPill unread">${a.coolingDown ? 'Cooling' : 'Not ready'}</span>`;

    const el = document.createElement('div');
    el.className = 'accCard';
    el.innerHTML = `
      <div class="accTop">
        <div>
          <div class="accName">${escapeHtml(a.name || a.id)}</div>
          <div class="accId mono">${escapeHtml(a.id)}</div>
        </div>
        <div>${readyPill}</div>
      </div>

      <div class="accMeta">
        <div>Added: <span class="mono">${escapeHtml(fmtTime(a.createdAt) || '—')}</span></div>
      </div>

      <div class="accStats">
        <div class="sbox"><div class="n">${a.stats?.sent ?? 0}</div><div class="l">Sent</div></div>
        <div class="sbox"><div class="n">${a.stats?.delivered ?? 0}</div><div class="l">Delivered</div></div>
        <div class="sbox"><div class="n">${a.stats?.seen ?? 0}</div><div class="l">Seen</div></div>
        <div class="sbox"><div class="n">${a.stats?.failed ?? 0}</div><div class="l">Failed</div></div>
      </div>

      <label>Display name</label>
      <input class="accNameInput" data-id="${escapeHtml(a.id)}" value="${escapeHtml(a.name || a.id)}" />

      <div class="accBtns">
        <button class="small primary" data-act="rename" data-id="${escapeHtml(a.id)}">Save name</button>
        <button class="small ghost" data-act="reconnect" data-id="${escapeHtml(a.id)}">Scan again</button>
        <button class="small danger" data-act="delete" data-id="${escapeHtml(a.id)}">Delete</button>
      </div>

      <div class="hint accMsg" data-msg="${escapeHtml(a.id)}"></div>
    `;

    grid.appendChild(el);
  }

  grid.querySelectorAll('button[data-act]').forEach((btn) => {
    btn.addEventListener('click', async () => {
      const act = btn.dataset.act;
      const id = btn.dataset.id;
      const msgEl = grid.querySelector(`.accMsg[data-msg="${CSS.escape(id)}"]`);
      if (!id) return;

      if (act === 'rename') {
        const input = grid.querySelector(`.accNameInput[data-id="${CSS.escape(id)}"]`);
        const name = (input?.value || '').trim();
        if (!name) { msgEl.textContent = '❌ Name required'; return; }

        msgEl.textContent = 'Saving...';
        try {
          const res = await fetch(`${API_BASE}/clients/rename`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ id, name }),
          });
          const data = await res.json();
          if (!res.ok || !data.success) { msgEl.textContent = '❌ ' + (data.error || 'Failed'); return; }
          msgEl.textContent = '✅ Saved';
          await loadClients();
          refreshQuotasUI();
          updateSafeHint();
        } catch { msgEl.textContent = '❌ API error'; }
      }

      if (act === 'reconnect') {
        const ok = confirm(`Scan again for ${id}? This will reset session and require QR scan.`);
        if (!ok) return;

        msgEl.textContent = 'Reconnecting...';
        try {
          const res = await fetch(`${API_BASE}/clients/reconnect`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ id }),
          });
          const data = await res.json();
          if (!res.ok || !data.success) { msgEl.textContent = '❌ ' + (data.error || 'Failed'); return; }

          msgEl.textContent = '✅ Session reset. Opening QR...';
          await loadClients();
          $('clientSelect').value = id;
          refreshQuotasUI();
          updateSafeHint();
          startQrPolling(id);
        } catch { msgEl.textContent = '❌ API error'; }
      }

      if (act === 'delete') {
        const ok = confirm(`Delete ${id}?`);
        if (!ok) return;

        const delSess = confirm(`Also delete saved session for ${id}? (If YES, you'll need to scan again if re-added)`);

        msgEl.textContent = 'Deleting...';
        try {
          const res = await fetch(`${API_BASE}/clients/delete`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ id, deleteSession: delSess ? '1' : '0' }),
          });
          const data = await res.json();
          if (!res.ok || !data.success) { msgEl.textContent = '❌ ' + (data.error || 'Failed'); return; }

          msgEl.textContent = '✅ Deleted';
          await loadClients();
          refreshQuotasUI();
          updateSafeHint();
          await loadAccounts();
        } catch { msgEl.textContent = '❌ API error'; }
      }
    });
  });
}

// ---------------- Charts + Logs realtime refresh logic ----------------
let chartsDirty = false;
let chartsTimer = null;

function markChartsDirty() {
  chartsDirty = true;
  if (chartsTimer) return;
  chartsTimer = setTimeout(async () => {
    chartsTimer = null;
    if (!chartsDirty) return;
    chartsDirty = false;
    // Only refresh charts if charts page is active OR recent fetch is old
    const isChartsActive = $('page-charts').classList.contains('active');
    const old = Date.now() - state.charts.lastFetchAt > 15_000;
    if (isChartsActive || old) await refreshCharts();
  }, 1500);
}

// ---------------- Realtime Socket.IO ----------------
function initSocket() {
  try {
    const socket = io({ transports: ['websocket', 'polling'] });

    socket.on('connect', () => {
      // console.log('socket connected');
    });

    socket.on('connect_error', () => {
      // If auth fails, redirect to login
      // Some browsers send "unauthorized"
      // We do a safe check after.
      loadMe();
    });

    socket.on('me', (data) => {
      if (!data) return;
      state.me.username = data.username || state.me.username;
      if (typeof data.credit === 'number') state.me.credit = data.credit;
      setText('meUser', state.me.username || '—');
      setText('meCredit', `$${Number(state.me.credit ?? 0).toFixed(2)}`);
      if (data.rate) setText('billingHint', `Billing: 1000 delivered = $${data.rate.dollarsPer1000Delivered} (=$${data.rate.costPerDelivered.toFixed(2)}/delivered).`);
    });

    socket.on('credit_update', (data) => {
      if (!data) return;
      // same user
      if (data.username && state.me.username && data.username !== state.me.username) return;
      if (typeof data.credit === 'number') {
        state.me.credit = data.credit;
        setText('meCredit', `$${Number(state.me.credit ?? 0).toFixed(2)}`);
      }
    });

    socket.on('send_progress', (job) => {
      if (!job) return;
      // only show my jobs
      if (job.username && state.me.username && job.username !== state.me.username) return;
      state.job = job;
      setProgress(job);
      // progress changes affect charts over time, but not too often
    });

    socket.on('log_event', (evt) => {
      if (!evt || !evt.type) return;

      // append in-memory logs
      const t = evt.type;
      if (!state.logs[t]) return;
      state.logs[t].push({ at: evt.at, number: evt.number, clientId: evt.clientId });

      // update counts
      if (!state.logs.counts) state.logs.counts = { sent: 0, delivered: 0, seen: 0, failed: 0 };
      state.logs.counts[t] = (state.logs.counts[t] || 0) + 1;

      // update KPI instantly
      if (t === 'sent') setText('kSent', state.logs.counts.sent);
      if (t === 'delivered') setText('kDelivered', state.logs.counts.delivered);
      if (t === 'seen') setText('kSeen', state.logs.counts.seen);
      if (t === 'failed') setText('kFailed', state.logs.counts.failed);

      // if logs page is open, re-render the table (cheap)
      const isLogsActive = $('page-logs').classList.contains('active');
      if (isLogsActive) renderLogsTable();

      // mark charts dirty
      markChartsDirty();
    });

    socket.on('client_update', async () => {
      // update clients list quickly
      await loadClients();
      refreshQuotasUI();
      updateSafeHint();
    });

    socket.on('accounts_update', async () => {
      // if accounts page open, refresh it
      const isAcc = $('page-accounts').classList.contains('active');
      if (isAcc) await loadAccounts();
      await loadClients();
      refreshQuotasUI();
    });

  } catch (e) {
    // ignore
  }
}

// ---------------- Charts bindings ----------------
$('refreshChartsBtn').onclick = () => refreshCharts();
$('chartGroup').onchange = () => refreshCharts();
$('chartFrom').onchange = () => refreshCharts();
$('chartTo').onchange = () => refreshCharts();
$('topClients').oninput = () => refreshCharts();

// ---------------- Basic init for default message + preview ----------------
function addDefaultMessage() {
  if ($('messages').children.length === 0) addMsg();
}

function computeInitialClientSelection() {
  const sel = $('clientSelect');
  if (!sel.value && sel.options.length) sel.value = sel.options[0].value;
}

// ---------------- Init ----------------
(async function init() {
  // default safe preset
  applySafePreset('normal');

  addDefaultMessage();
  setChatPanelVisible(false);

  await loadMe();
  await loadClients();
  computeInitialClientSelection();
  await checkHealth();

  updatePreview();
  updateTotalTargetsUI();
  refreshQuotasUI();
  updateSafeHint();

  // logs/charts initial
  await loadLogs();
  setDefaultChartRange();
  // charts only when open (but we can preload once)
  // await refreshCharts();

  initSocket();
})();
