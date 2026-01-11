'use strict';

const express = require('express');
const cors = require('cors');
const multer = require('multer');
const fs = require('fs');
const path = require('path');
const http = require('http');
const session = require('express-session');
const { Server } = require('socket.io');

const { Client, LocalAuth, MessageMedia } = require('whatsapp-web.js');

process.on('uncaughtException', (err) => console.error('🔥 uncaughtException:', err));
process.on('unhandledRejection', (err) => console.error('🔥 unhandledRejection:', err));

const app = express();
const server = http.createServer(app);

// ---------------- Socket.IO (session-auth) ----------------
const sessionMiddleware = session({
  secret: 'wa-dashboard-secret-change-me',
  resave: false,
  saveUninitialized: false,
  cookie: { httpOnly: true, sameSite: 'lax' },
});

const io = new Server(server, {
  cors: { origin: true, credentials: true },
});

io.use((socket, next) => {
  sessionMiddleware(socket.request, {}, next);
});
io.use((socket, next) => {
  const sess = socket.request.session;
  if (sess && sess.user && sess.user.username) return next();
  return next(new Error('unauthorized'));
});

// Emit helper (to all authed sockets)
function ioEmit(event, payload) {
  try { io.emit(event, payload); } catch {}
}

io.on('connection', (socket) => {
  const u = socket.request.session.user;
  console.log(`🔌 socket connected: ${u?.username || 'unknown'}`);

  // send initial credit + user info
  if (u?.username) {
    const me = getUser(u.username);
    socket.emit('me', { username: me?.username, credit: me?.credit ?? 0, rate: billingRate() });
  }

  socket.on('disconnect', () => console.log('🔌 socket disconnected'));
});

// ---------------- Folders ----------------
const LOG_DIR = path.join(__dirname, 'logs');
const DATA_DIR = path.join(__dirname, 'data');
if (!fs.existsSync(LOG_DIR)) fs.mkdirSync(LOG_DIR);
if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR);

const FILES = {
  sent: path.join(LOG_DIR, 'sent.txt'),
  failed: path.join(LOG_DIR, 'failed.txt'),
  delivered: path.join(LOG_DIR, 'delivered.txt'),
  seen: path.join(LOG_DIR, 'seen.txt'),
};

const ACCOUNTS_FILE = path.join(DATA_DIR, 'accounts.json');
const TAGS_FILE = path.join(DATA_DIR, 'categories.json');
const USERS_FILE = path.join(DATA_DIR, 'users.json');

// ---------------- Express ----------------
app.use(cors({ origin: true, credentials: true }));
app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true }));
app.use(sessionMiddleware);

// ---------------- Auth ----------------
function readUsersFile() {
  try {
    if (!fs.existsSync(USERS_FILE)) return { users: [] };
    return JSON.parse(fs.readFileSync(USERS_FILE, 'utf8') || '{"users":[]}');
  } catch {
    return { users: [] };
  }
}
function writeUsersFile(obj) {
  try { fs.writeFileSync(USERS_FILE, JSON.stringify(obj, null, 2), 'utf8'); } catch {}
}
function getUser(username) {
  const db = readUsersFile();
  return (db.users || []).find((u) => u.username === username) || null;
}
function updateUser(username, patch) {
  const db = readUsersFile();
  const users = db.users || [];
  const idx = users.findIndex((u) => u.username === username);
  if (idx < 0) return null;
  users[idx] = { ...users[idx], ...patch };
  db.users = users;
  writeUsersFile(db);
  return users[idx];
}
function requireAuth(req, res, next) {
  if (req.session && req.session.user && req.session.user.username) return next();
  // allow login endpoints
  if (req.path === '/api/login' || req.path === '/api/logout' || req.path === '/login') return next();
  // allow socket.io transport
  if (req.path.startsWith('/socket.io')) return next();
  // redirect browser, json for API
  if (req.path.startsWith('/api') || req.path.startsWith('/send') || req.path.startsWith('/clients') || req.path.startsWith('/accounts') || req.path.startsWith('/logs') || req.path.startsWith('/qr') || req.path.startsWith('/chats') || req.path.startsWith('/stats')) {
    return res.status(401).json({ success: false, error: 'Unauthorized' });
  }
  return res.redirect('/login');
}

// Protect everything (except /login + auth endpoints)
app.use(requireAuth);

// Minimal login page (no extra files needed)
app.get('/login', (req, res) => {
  res.type('html').send(`
<!doctype html><html><head><meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Login</title>
<style>
  body{margin:0;height:100vh;display:grid;place-items:center;font-family:system-ui;background:#0b1020;color:#eaf0ff}
  .card{width:min(420px,92vw);background:rgba(255,255,255,.04);border:1px solid rgba(255,255,255,.10);border-radius:16px;padding:18px;box-shadow:0 16px 60px rgba(0,0,0,.45)}
  h1{margin:0 0 6px 0;font-size:18px}
  .hint{opacity:.75;font-size:12px;margin-bottom:14px;line-height:1.4}
  input,button{width:100%;padding:12px;border-radius:14px;border:1px solid rgba(255,255,255,.10);background:rgba(255,255,255,.05);color:#eaf0ff;outline:none}
  button{cursor:pointer;margin-top:10px;background:rgba(109,124,255,.18);border-color:rgba(109,124,255,.45)}
  .err{margin-top:10px;font-size:12px;color:#ffd166}
</style></head><body>
  <div class="card">
    <h1>WhatsApp Dashboard</h1>
    <div class="hint">Sign in to access sender, logs, charts, and accounts.</div>
    <input id="u" placeholder="Username" autocomplete="username"/>
    <input id="p" placeholder="Password" type="password" autocomplete="current-password" style="margin-top:10px"/>
    <button onclick="go()">Login</button>
    <div class="err" id="err"></div>
  </div>
<script>
  async function go(){
    const username=document.getElementById('u').value.trim();
    const password=document.getElementById('p').value.trim();
    const err=document.getElementById('err'); err.textContent='';
    try{
      const r=await fetch('/api/login',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({username,password})});
      const d=await r.json();
      if(!r.ok) {err.textContent=d.error||'Login failed'; return;}
      location.href='/';
    }catch(e){err.textContent='Server offline?';}
  }
</script>
</body></html>
`);
});

app.post('/api/login', (req, res) => {
  const username = String(req.body?.username || '').trim();
  const password = String(req.body?.password || '').trim();
  const u = getUser(username);
  if (!u || u.password !== password) return res.status(401).json({ success: false, error: 'Invalid credentials' });
  req.session.user = { username };
  return res.json({ success: true, username, credit: u.credit ?? 0 });
});

app.post('/api/logout', (req, res) => {
  req.session.destroy(() => res.json({ success: true }));
});

app.get('/api/me', (req, res) => {
  const username = req.session?.user?.username;
  const u = username ? getUser(username) : null;
  if (!u) return res.status(401).json({ success: false, error: 'Unauthorized' });
  res.json({ success: true, username: u.username, credit: u.credit ?? 0, rate: billingRate() });
});

// Serve dashboard static AFTER auth middleware
app.use(express.static(path.join(__dirname, 'public')));

// ---------------- Billing ----------------
// Requirement: 1000 delivered messages = 10 ($)
function billingRate() {
  return {
    deliveredPer10$: 1000,
    dollarsPer1000Delivered: 10,
    costPerDelivered: 10 / 1000, // 0.01
  };
}
function canAffordMore(username) {
  const u = getUser(username);
  const credit = Number(u?.credit ?? 0);
  return credit > 0;
}
function maxTargetsFromCredit(username) {
  const u = getUser(username);
  const credit = Number(u?.credit ?? 0);
  const cost = billingRate().costPerDelivered;
  // conservative: allow at most credit/cost sends (even if not all deliver)
  return Math.max(0, Math.floor(credit / cost));
}
function chargeDelivered(username, count = 1) {
  const u = getUser(username);
  if (!u) return null;
  const credit = Number(u.credit ?? 0);
  const cost = billingRate().costPerDelivered * count;
  const next = Math.max(0, +(credit - cost).toFixed(2));
  const updated = updateUser(username, { credit: next });
  ioEmit('credit_update', { username, credit: updated?.credit ?? next });
  return updated;
}

// ---------------- Helpers ----------------
function nowStamp() { return new Date().toISOString(); }
function nowMs() { return Date.now(); }
function sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }
function clampInt(n, fallback, min, max) {
  const x = Number.parseInt(n, 10);
  if (Number.isNaN(x)) return fallback;
  return Math.min(Math.max(x, min), max);
}
function randomInt(min, max) { return Math.floor(Math.random() * (max - min + 1)) + min; }

// ---------------- ID safety (fix Invalid clientId crash) ----------------
function isValidClientId(id) { return /^[A-Za-z0-9_-]+$/.test(String(id || '')); }
function sanitizeClientId(id) {
  const raw = String(id || '').trim();
  let s = raw.replace(/[^A-Za-z0-9_-]/g, '_');
  s = s.replace(/^_+|_+$/g, '');
  if (!s) s = 'wa';
  if (s.length > 48) s = s.slice(0, 48);
  return s;
}
function assertSafeClientId(id) {
  const safe = sanitizeClientId(id);
  if (!isValidClientId(safe)) return null;
  return safe;
}

// ---------------- Multer ----------------
const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 15 * 1024 * 1024 },
});

// ---------------- Accounts storage ----------------
function readAccounts() {
  try {
    if (!fs.existsSync(ACCOUNTS_FILE)) return {};
    return JSON.parse(fs.readFileSync(ACCOUNTS_FILE, 'utf8') || '{}') || {};
  } catch {
    return {};
  }
}
function writeAccounts(obj) {
  try { fs.writeFileSync(ACCOUNTS_FILE, JSON.stringify(obj, null, 2), 'utf8'); }
  catch (e) { console.error('writeAccounts error:', e?.message || e); }
}
function ensureAccountMeta(id) {
  const acc = readAccounts();
  if (!acc[id]) {
    acc[id] = { id, name: id, createdAt: nowStamp() };
    writeAccounts(acc);
  }
  return acc[id];
}
function generateNextClientId() {
  const acc = readAccounts();
  const ids = new Set(Object.keys(acc));
  let i = 1;
  while (ids.has(`wa${i}`)) i++;
  return `wa${i}`;
}
function migrateAccountsToSafeIds() {
  const acc = readAccounts();
  const out = {};
  const used = new Set();

  const safeUnique = (base) => {
    let s = base;
    let i = 2;
    while (used.has(s)) { s = `${base}_${i}`; i++; }
    used.add(s);
    return s;
  };

  for (const [id, meta] of Object.entries(acc)) {
    const safeBase = assertSafeClientId(id) || 'wa';
    const safeId = safeUnique(safeBase);

    out[safeId] = {
      id: safeId,
      name: String(meta?.name || safeId),
      createdAt: meta?.createdAt || nowStamp(),
    };

    if (safeId !== id) console.warn(`⚠️ Migrated invalid clientId "${id}" -> "${safeId}"`);
  }

  writeAccounts(out);
  return out;
}

// ---------------- Logs (compact) ----------------
function appendLine(filePath, line) {
  try { fs.appendFileSync(filePath, line.trim() + '\n', 'utf8'); }
  catch (e) { console.error('log append error:', e?.message || e); }
}
function logCompact(type, whenIso, number, clientId) {
  const fileMap = { sent: FILES.sent, failed: FILES.failed, delivered: FILES.delivered, seen: FILES.seen };
  const fp = fileMap[type];
  if (!fp) return;
  const line = `${whenIso} | ${number} | client=${clientId}`;
  appendLine(fp, line);

  // realtime logs
  ioEmit('log_event', { type, at: whenIso, number, clientId });
}
function readLogCompact(filePath) {
  if (!fs.existsSync(filePath)) return [];
  const content = fs.readFileSync(filePath, 'utf8');
  return content
    .split('\n')
    .map((l) => l.trim())
    .filter(Boolean)
    .map((line) => {
      const parts = line.split('|').map((p) => p.trim());
      const at = parts[0] || '';
      const number = parts[1] || '';
      const m = (parts[2] || '').match(/client=([A-Za-z0-9_\-]+)/);
      const clientId = m ? m[1] : null;
      return { at, number, clientId };
    });
}
function computeStatsByClient() {
  const acc = readAccounts();
  const stats = {};
  for (const id of Object.keys(acc)) stats[id] = { sent: 0, delivered: 0, seen: 0, failed: 0 };

  const mapAdd = (arr, field) => {
    for (const it of arr) {
      if (!it.clientId) continue;
      if (!stats[it.clientId]) stats[it.clientId] = { sent: 0, delivered: 0, seen: 0, failed: 0 };
      stats[it.clientId][field]++;
    }
  };

  mapAdd(readLogCompact(FILES.sent), 'sent');
  mapAdd(readLogCompact(FILES.delivered), 'delivered');
  mapAdd(readLogCompact(FILES.seen), 'seen');
  mapAdd(readLogCompact(FILES.failed), 'failed');

  return stats;
}

// ---------------- Phone helpers ----------------
function parseNumbers(text) {
  return String(text || '')
    .split(/[\n,; \t]+/g)
    .map((s) => s.trim())
    .filter(Boolean);
}

/**
 * Worldwide WhatsApp JID builder.
 *
 * WhatsApp expects digits only + "@c.us" (E.164 digits).
 * Accepted inputs:
 *  - "+447700900123"  => "447700900123@c.us"
 *  - "447700900123"   => "447700900123@c.us" (already E.164)
 *  - "07700900123"    => needs defaultCountryCode (e.g. "44")
 *  - "06..." (Morocco local) => needs defaultCountryCode "212"
 *
 * If you pass local numbers without defaultCountryCode, returns null (safer than guessing).
 */
function toWorldwideJid(phone, defaultCountryCode = null) {
  let s = String(phone || '').trim();
  if (!s) return null;

  const hasPlus = s.startsWith('+');
  const digits = s.replace(/\D/g, '');
  if (!digits) return null;

  // If user included '+', assume E.164 and use as-is.
  if (hasPlus) {
    if (digits.length < 8 || digits.length > 15) return null;
    return `${digits}@c.us`;
  }

  // If starts with 00 (international prefix), convert to E.164
  if (digits.startsWith('00')) {
    const d = digits.slice(2);
    if (d.length < 8 || d.length > 15) return null;
    return `${d}@c.us`;
  }

  // If looks like E.164 already, accept (no leading 0)
  if (digits.length >= 8 && digits.length <= 15 && !digits.startsWith('0')) {
    return `${digits}@c.us`;
  }

  // Otherwise treat as local (leading zero most likely)
  if (!defaultCountryCode) return null;

  const cc = String(defaultCountryCode).replace(/\D/g, '');
  if (!cc) return null;

  const national = digits.replace(/^0+/, '');
  const e164 = `${cc}${national}`;
  if (e164.length < 8 || e164.length > 15) return null;

  return `${e164}@c.us`;
}

function safeJsonParseArray(s) {
  try {
    const v = JSON.parse(s || '[]');
    return Array.isArray(v) ? v : [];
  } catch {
    return [];
  }
}
function parseMapJson(s) {
  try {
    const v = JSON.parse(s || '{}');
    if (!v || typeof v !== 'object') return {};
    return v;
  } catch {
    return {};
  }
}

// ---------------- vCard ----------------
function buildVCard(name, phoneDigits) {
  const safeName = String(name || 'Contact').replace(/\r?\n/g, ' ').trim() || 'Contact';
  const safePhone = String(phoneDigits || '').replace(/\D/g, '');
  return [
    'BEGIN:VCARD',
    'VERSION:3.0',
    `FN:${safeName}`,
    `TEL;type=CELL;type=VOICE:${safePhone}`,
    'END:VCARD',
  ].join('\n');
}

// ---------------- Multi-client manager ----------------
const clients = new Map();

function getClientIdsInOrder() { return Array.from(clients.keys()); }
function isCoolingDown(st) {
  const until = st?.runtime?.cooldownUntil || 0;
  return until > nowMs();
}
function getReadyClients() {
  return Array.from(clients.values()).filter((c) => c.ready && !isCoolingDown(c));
}
function nextReadyClient(afterId) {
  const ids = getClientIdsInOrder();
  if (!ids.length) return null;
  const readySet = new Set(getReadyClients().map((c) => c.id));
  if (!readySet.size) return null;

  if (!afterId || !ids.includes(afterId)) {
    for (const id of ids) if (readySet.has(id)) return clients.get(id);
    return null;
  }

  const startIdx = ids.indexOf(afterId);
  for (let step = 1; step <= ids.length; step++) {
    const id = ids[(startIdx + step) % ids.length];
    if (readySet.has(id)) return clients.get(id);
  }
  return null;
}

function pickClientForSend(preferredId) {
  if (preferredId && clients.has(preferredId)) {
    const st = clients.get(preferredId);
    if (st.ready && !isCoolingDown(st)) return st;
  }
  const ready = getReadyClients();
  return ready[0] || null;
}

// Safe mode per-account limiter
function canSendNow(st, safeModeCfg) {
  if (!st || !safeModeCfg?.enabled) return true;
  if (isCoolingDown(st)) return false;

  const now = nowMs();
  const perMin = safeModeCfg.perClientMaxPerMinute;

  if (!st.runtime.windowStartMs || now - st.runtime.windowStartMs >= 60_000) {
    st.runtime.windowStartMs = now;
    st.runtime.sentInWindow = 0;
  }

  let factor = 1.0;
  if (safeModeCfg.warmupMinutes > 0) {
    const elapsed = (now - st.runtime.safeWarmupStartMs) / 60000;
    const t = Math.min(Math.max(elapsed / safeModeCfg.warmupMinutes, 0), 1);
    factor = 0.35 + 0.65 * t;
  }

  const allowed = Math.max(1, Math.floor(perMin * factor));
  return st.runtime.sentInWindow < allowed;
}
function noteSent(st) {
  const now = nowMs();
  if (!st.runtime.windowStartMs || now - st.runtime.windowStartMs >= 60_000) {
    st.runtime.windowStartMs = now;
    st.runtime.sentInWindow = 0;
  }
  st.runtime.sentInWindow += 1;
  st.runtime.consecutiveErrors = 0;
}
function riskErrorCooldownMs(errMsg) {
  const s = (errMsg || '').toLowerCase();
  const risky =
    s.includes('rate') ||
    s.includes('too many') ||
    s.includes('spam') ||
    s.includes('banned') ||
    s.includes('temporarily') ||
    s.includes('blocked') ||
    s.includes('limit');
  if (!risky) return 0;
  return 8 * 60_000;
}
function noteError(st, errMsg) {
  if (!st) return;
  st.runtime.consecutiveErrors = (st.runtime.consecutiveErrors || 0) + 1;
  const baseCd = riskErrorCooldownMs(errMsg);
  if (baseCd > 0) {
    const extra = Math.min(st.runtime.consecutiveErrors * 60_000, 10 * 60_000);
    st.runtime.cooldownUntil = nowMs() + baseCd + extra;
    console.warn(`🧊 [${st.id}] cooldown until ${new Date(st.runtime.cooldownUntil).toLocaleTimeString()} (reason: ${errMsg})`);
  }
}

async function reinitializeClient(state) {
  if (!state || state.initializing) return;
  state.initializing = true;

  try { try { await state.client.destroy(); } catch {} } catch {}

  const wait = state.backoffMs || 1500;
  const nextWait = Math.min(wait * 2, 60_000);

  console.log(`🔄 [${state.id}] Re-initializing in ${wait}ms...`);
  await sleep(wait);

  try {
    state.client.initialize();
    state.backoffMs = nextWait;
  } catch (e) {
    console.error(`❌ [${state.id}] initialize() failed:`, e?.message || e);
  } finally {
    state.initializing = false;
  }
}

// delete LocalAuth session folder
function removeLocalAuthSession(clientId) {
  const authDir = path.join(__dirname, '.wwebjs_auth', `session-${clientId}`);
  try { if (fs.existsSync(authDir)) fs.rmSync(authDir, { recursive: true, force: true }); }
  catch (e) { console.error('removeLocalAuthSession error:', e?.message || e); }
}

// ---- credit charging hook per delivered (ack=2) ----
const msgOwner = new Map(); // msgId -> username

function createClient(clientIdInput) {
  const safeId = assertSafeClientId(clientIdInput);
  if (!safeId) {
    console.error(`❌ createClient refused invalid id: "${clientIdInput}"`);
    return null;
  }
  if (clients.has(safeId)) return clients.get(safeId);

  ensureAccountMeta(safeId);

  const state = {
    id: safeId,
    ready: false,
    qr: null,
    lastQrAt: null,
    lastDisconnect: null,
    lastReadyAt: null,
    initializing: false,
    backoffMs: 1500,
    sentIndex: new Map(), // msgId -> { number, chatId, clientId }
    runtime: {
      cooldownUntil: 0,
      windowStartMs: 0,
      sentInWindow: 0,
      safeWarmupStartMs: nowMs(),
      consecutiveErrors: 0,
    },
    client: null,
  };

  let client;
  try {
    client = new Client({
      authStrategy: new LocalAuth({ clientId: safeId }),
      puppeteer: {
        headless: true,
        args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage'],
      },
    });
  } catch (e) {
    console.error(`❌ Failed to construct Client for "${safeId}":`, e?.message || e);
    return null;
  }

  client.on('qr', (qr) => {
    state.qr = qr;
    state.lastQrAt = nowStamp();
    state.ready = false;
    console.log(`🧩 [${safeId}] QR ready (dashboard shows it)`);
    ioEmit('client_update', { id: safeId, event: 'qr', lastQrAt: state.lastQrAt });
  });

  client.on('authenticated', () => {
    console.log(`🔐 [${safeId}] authenticated (session saved)`);
    ioEmit('client_update', { id: safeId, event: 'authenticated' });
  });

  client.on('ready', () => {
    state.ready = true;
    state.qr = null;
    state.backoffMs = 1500;
    state.lastReadyAt = nowStamp();
    state.runtime.cooldownUntil = 0;
    state.runtime.safeWarmupStartMs = nowMs();
    console.log(`✅ [${safeId}] ready`);
    ioEmit('client_update', { id: safeId, event: 'ready', lastReadyAt: state.lastReadyAt });
  });

  client.on('auth_failure', (msg) => {
    state.ready = false;
    console.error(`❌ [${safeId}] auth_failure:`, msg);
    ioEmit('client_update', { id: safeId, event: 'auth_failure', msg });
  });

  client.on('disconnected', (reason) => {
    state.ready = false;
    state.lastDisconnect = `${nowStamp()} | ${reason}`;
    console.error(`⚠️ [${safeId}] disconnected:`, reason);
    ioEmit('client_update', { id: safeId, event: 'disconnected', reason, lastDisconnect: state.lastDisconnect });
    reinitializeClient(state);
  });

  client.on('message_ack', (msg, ack) => {
    const id = msg?.id?._serialized;
    if (!id) return;

    const meta = state.sentIndex.get(id);
    if (!meta) return;

    if (ack === 2) {
      logCompact('delivered', nowStamp(), meta.number, safeId);

      // charge credit to the user who initiated this message
      const username = msgOwner.get(id);
      if (username) {
        chargeDelivered(username, 1);
        msgOwner.delete(id);
      }

      ioEmit('delivered_update', { clientId: safeId, number: meta.number });
    }
    if (ack === 3) {
      logCompact('seen', nowStamp(), meta.number, safeId);
      ioEmit('seen_update', { clientId: safeId, number: meta.number });
    }
  });

  state.client = client;
  clients.set(safeId, state);

  try { client.initialize(); }
  catch (e) {
    console.error(`❌ [${safeId}] initial initialize() failed:`, e?.message || e);
    reinitializeClient(state);
  }

  return state;
}

// ---------------- Boot accounts ----------------
(function bootLoadAccounts() {
  const acc = migrateAccountsToSafeIds();
  const ids = Object.keys(acc);
  if (!ids.length) {
    const id = 'wa1';
    ensureAccountMeta(id);
    createClient(id);
    return;
  }
  for (const id of ids) createClient(id);
})();

// ---------------- API: clients/health/qr ----------------
app.get('/clients', (req, res) => {
  const acc = readAccounts();
  const list = Array.from(clients.values()).map((c) => ({
    id: c.id,
    name: acc[c.id]?.name || c.id,
    createdAt: acc[c.id]?.createdAt || null,
    ready: c.ready,
    hasQr: !!c.qr,
    lastQrAt: c.lastQrAt,
    lastDisconnect: c.lastDisconnect,
    lastReadyAt: c.lastReadyAt,
    coolingDown: isCoolingDown(c),
    cooldownUntil: c.runtime?.cooldownUntil ? new Date(c.runtime.cooldownUntil).toISOString() : null,
  }));
  res.json({ success: true, clients: list });
});

app.get('/health', (req, res) => {
  const list = Array.from(clients.values()).map((c) => ({
    id: c.id,
    ready: c.ready,
    hasQr: !!c.qr,
    coolingDown: isCoolingDown(c),
  }));
  res.json({ ok: true, clients: list, any_ready: list.some((x) => x.ready && !x.coolingDown) });
});

app.get('/qr/:clientId', (req, res) => {
  const idRaw = String(req.params.clientId || '').trim();
  const id = assertSafeClientId(idRaw);
  if (!id) return res.status(400).json({ success: false, error: 'Invalid clientId' });

  const st = clients.get(id);
  if (!st) return res.status(404).json({ success: false, error: 'client not found' });

  res.json({ success: true, clientId: st.id, ready: st.ready, lastQrAt: st.lastQrAt, qr: st.qr });
});

// ---------------- API: Account management ----------------
app.get('/accounts', (req, res) => {
  const acc = readAccounts();
  const stats = computeStatsByClient();
  const out = Object.keys(acc).map((id) => ({
    id,
    name: acc[id]?.name || id,
    createdAt: acc[id]?.createdAt || null,
    ready: clients.get(id)?.ready || false,
    coolingDown: clients.get(id) ? isCoolingDown(clients.get(id)) : false,
    stats: stats[id] || { sent: 0, delivered: 0, seen: 0, failed: 0 },
  }));
  out.sort((a, b) => String(a.createdAt || '').localeCompare(String(b.createdAt || '')));
  res.json({ success: true, accounts: out });
});

app.post('/clients/add', (req, res) => {
  let idRaw = String(req.body?.id || '').trim();
  let name = String(req.body?.name || '').trim();

  let id = idRaw ? assertSafeClientId(idRaw) : null;
  if (!id) id = generateNextClientId();
  if (!name) name = id;

  const acc = readAccounts();
  const existed = !!acc[id];

  if (!acc[id]) {
    acc[id] = { id, name, createdAt: nowStamp() };
    writeAccounts(acc);
  }

  const st = createClient(id);
  if (!st) return res.status(500).json({ success: false, error: 'Failed to create client' });

  ioEmit('accounts_update', { type: 'add', id });
  res.json({ success: true, created: !existed, id, sanitizedFrom: idRaw && idRaw !== id ? idRaw : null });
});

app.post('/clients/rename', (req, res) => {
  const idRaw = String(req.body?.id || '').trim();
  const id = assertSafeClientId(idRaw);
  const name = String(req.body?.name || '').trim();
  if (!id) return res.status(400).json({ success: false, error: 'Invalid id' });
  if (!name) return res.status(400).json({ success: false, error: 'name required' });

  const acc = readAccounts();
  if (!acc[id]) return res.status(404).json({ success: false, error: 'account not found' });

  acc[id].name = name;
  writeAccounts(acc);

  ioEmit('accounts_update', { type: 'rename', id, name });
  res.json({ success: true, id, name });
});

app.post('/clients/reconnect', async (req, res) => {
  const idRaw = String(req.body?.id || '').trim();
  const id = assertSafeClientId(idRaw);
  if (!id) return res.status(400).json({ success: false, error: 'Invalid id' });

  const st = clients.get(id);
  if (!st) return res.status(404).json({ success: false, error: 'client not found' });

  try {
    try { await st.client.destroy(); } catch {}
    removeLocalAuthSession(id);

    clients.delete(id);
    const st2 = createClient(id);
    if (!st2) return res.status(500).json({ success: false, error: 'Failed to recreate client' });

    ioEmit('accounts_update', { type: 'reconnect', id });
    return res.json({ success: true, id, message: 'Session cleared. Scan QR again.' });
  } catch (e) {
    return res.status(500).json({ success: false, error: e?.message || 'reconnect failed' });
  }
});

app.post('/clients/delete', async (req, res) => {
  const idRaw = String(req.body?.id || '').trim();
  const id = assertSafeClientId(idRaw);
  const deleteSession = String(req.body?.deleteSession || '0') === '1';
  if (!id) return res.status(400).json({ success: false, error: 'Invalid id' });

  const acc = readAccounts();
  if (!acc[id]) return res.status(404).json({ success: false, error: 'account not found' });

  const st = clients.get(id);
  if (st) {
    try { await st.client.destroy(); } catch {}
    clients.delete(id);
  }
  if (deleteSession) removeLocalAuthSession(id);

  delete acc[id];
  writeAccounts(acc);

  ioEmit('accounts_update', { type: 'delete', id });
  res.json({ success: true, id, deleted: true, deleteSession });
});

// ---------------- API: logs ----------------
app.get('/logs', (req, res) => {
  try {
    const sent = readLogCompact(FILES.sent);
    const delivered = readLogCompact(FILES.delivered);
    const seen = readLogCompact(FILES.seen);
    const failed = readLogCompact(FILES.failed);

    res.json({
      success: true,
      counts: { sent: sent.length, delivered: delivered.length, seen: seen.length, failed: failed.length },
      sent, delivered, seen, failed,
      compact: true,
    });
  } catch (e) {
    res.status(500).json({ success: false, error: e?.message || 'Failed to read logs' });
  }
});

// ---------------- Stats APIs (for Charts page) ----------------
function withinRangeISO(iso, fromIso, toIso) {
  const t = Date.parse(iso);
  if (!Number.isFinite(t)) return false;
  const f = fromIso ? Date.parse(fromIso) : -Infinity;
  const to = toIso ? Date.parse(toIso) : Infinity;
  return t >= f && t <= to;
}
function groupKey(iso, group) {
  const d = new Date(iso);
  if (group === 'hour') return `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')} ${String(d.getHours()).padStart(2,'0')}:00`;
  return `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')}`;
}
function aggByDate(items, from, to, group = 'day') {
  const map = new Map();
  for (const it of items) {
    if (!withinRangeISO(it.at, from, to)) continue;
    const k = groupKey(it.at, group);
    map.set(k, (map.get(k) || 0) + 1);
  }
  const out = Array.from(map.entries()).map(([k, v]) => ({ t: k, v }));
  out.sort((a, b) => String(a.t).localeCompare(String(b.t)));
  return out;
}
function aggByClient(items, from, to) {
  const map = new Map();
  for (const it of items) {
    if (!withinRangeISO(it.at, from, to)) continue;
    const c = it.clientId || 'unknown';
    map.set(c, (map.get(c) || 0) + 1);
  }
  const out = Array.from(map.entries()).map(([clientId, v]) => ({ clientId, v }));
  out.sort((a, b) => b.v - a.v);
  return out;
}

app.get('/stats/timeseries', (req, res) => {
  const from = String(req.query.from || '').trim() || null;
  const to = String(req.query.to || '').trim() || null;
  const group = String(req.query.group || 'day').trim(); // day|hour

  const sent = readLogCompact(FILES.sent);
  const delivered = readLogCompact(FILES.delivered);
  const seen = readLogCompact(FILES.seen);
  const failed = readLogCompact(FILES.failed);

  res.json({
    success: true,
    group,
    from,
    to,
    series: {
      sent: aggByDate(sent, from, to, group),
      delivered: aggByDate(delivered, from, to, group),
      seen: aggByDate(seen, from, to, group),
      failed: aggByDate(failed, from, to, group),
    }
  });
});

app.get('/stats/clients', (req, res) => {
  const from = String(req.query.from || '').trim() || null;
  const to = String(req.query.to || '').trim() || null;
  const type = String(req.query.type || 'sent').trim(); // sent|delivered|seen|failed

  const mapFile = {
    sent: FILES.sent,
    delivered: FILES.delivered,
    seen: FILES.seen,
    failed: FILES.failed
  };
  const fp = mapFile[type] || FILES.sent;
  const arr = readLogCompact(fp);

  res.json({ success: true, from, to, type, byClient: aggByClient(arr, from, to) });
});

// ---------------- Chats ----------------
app.get('/chats/list', async (req, res) => {
  const clientIdRaw = String(req.query.clientId || '').trim();
  const clientId = clientIdRaw ? assertSafeClientId(clientIdRaw) : null;

  const onlyReplied = String(req.query.onlyReplied || '0') === '1';
  const onlyContacts = String(req.query.onlyContacts || '0') === '1';
  const limit = clampInt(req.query.limit, 80, 1, 250);

  const st = clientId ? pickClientForSend(clientId) : pickClientForSend(null);
  if (!st) return res.status(503).json({ success: false, error: 'No ready clients (scan QR)' });

  try {
    const chats = await st.client.getChats();
    let people = chats
      .filter((c) => !c.isGroup)
      .filter((c) => String(c.id?._serialized || '').endsWith('@c.us'));

    if (onlyReplied) people = people.filter((c) => (c.unreadCount || 0) > 0);

    let tags = {};
    try {
      if (fs.existsSync(TAGS_FILE)) tags = JSON.parse(fs.readFileSync(TAGS_FILE, 'utf8') || '{}') || {};
    } catch {}

    const out = [];
    for (const c of people) {
      const chatId = c.id?._serialized;
      const user = c.id?.user || '';
      const name = c.name || c.pushname || user || 'Unknown';
      const lastTs = c.timestamp || c.lastMessage?.timestamp || 0;

      let lastText = '';
      try {
        const lm = c.lastMessage;
        if (lm) lastText = String(lm.body || '').slice(0, 120);
      } catch {}

      let isMyContact = false;
      try {
        const contact = await c.getContact();
        isMyContact = !!contact?.isMyContact;
      } catch {}

      if (onlyContacts && !isMyContact) continue;

      const t = tags[chatId] || null;

      out.push({
        chatId,
        number: user,
        name,
        isMyContact,
        unreadCount: c.unreadCount || 0,
        lastTimestamp: lastTs ? new Date(lastTs * 1000).toISOString() : null,
        lastText,
        category: t?.category || '',
        notes: t?.notes || '',
      });

      if (out.length >= limit) break;
    }

    out.sort((a, b) => String(b.lastTimestamp || '').localeCompare(String(a.lastTimestamp || '')));
    res.json({ success: true, clientId: st.id, chats: out.slice(0, limit) });
  } catch (e) {
    res.status(500).json({ success: false, error: e?.message || 'Failed to fetch chats' });
  }
});

app.post('/chats/category', async (req, res) => {
  const chatId = String(req.body?.chatId || '').trim();
  const category = String(req.body?.category || '').trim();
  const notes = String(req.body?.notes || '').trim();

  if (!chatId) return res.status(400).json({ success: false, error: 'chatId required' });
  if (!category) return res.status(400).json({ success: false, error: 'category required' });

  let tags = {};
  try {
    if (fs.existsSync(TAGS_FILE)) tags = JSON.parse(fs.readFileSync(TAGS_FILE, 'utf8') || '{}') || {};
  } catch {}
  tags[chatId] = { category, notes, updatedAt: nowStamp() };
  try { fs.writeFileSync(TAGS_FILE, JSON.stringify(tags, null, 2), 'utf8'); } catch {}

  ioEmit('chats_update', { type: 'category', chatId, category, notes });
  res.json({ success: true, chatId, category, notes });
});

app.post('/chats/send', async (req, res) => {
  const clientIdRaw = String(req.body?.clientId || '').trim();
  const clientId = clientIdRaw ? assertSafeClientId(clientIdRaw) : null;

  const chatId = String(req.body?.chatId || '').trim();
  const message = String(req.body?.message || '').trim();
  if (!chatId) return res.status(400).json({ success: false, error: 'chatId required' });
  if (!message) return res.status(400).json({ success: false, error: 'message required' });

  let st = clientId ? pickClientForSend(clientId) : pickClientForSend(null);
  if (!st) return res.status(503).json({ success: false, error: 'No ready WhatsApp client. Scan QR.' });

  const MAX_ATTEMPTS = 3;
  let attempt = 0;
  while (attempt < MAX_ATTEMPTS) {
    attempt++;
    try {
      const m = await st.client.sendMessage(chatId, message);
      return res.json({ success: true, clientId: st.id, msgId: m?.id?._serialized || null });
    } catch (e) {
      const msg = e?.message || String(e);
      noteError(st, msg);
      const nxt = nextReadyClient(st.id);
      if (!nxt) return res.status(500).json({ success: false, error: msg, clientId: st.id });
      st = nxt;
    }
  }
  res.status(500).json({ success: false, error: 'send failed after retries' });
});

// ---------------- Sender / Distribution ----------------
function applyTemplate(text, ctx) {
  return String(text || '')
    .replaceAll('{{number}}', ctx.number)
    .replaceAll('{{jid}}', ctx.jid)
    .replaceAll('{{index}}', String(ctx.index))
    .replaceAll('{{rand}}', ctx.rand);
}

function buildDistribution(numbers, readyClientIds, perClientLimit) {
  const total = numbers.length;
  const ids = readyClientIds.slice();
  if (!ids.length) return { assignments: {}, limits: {} };

  const limits = {};
  let sum = 0;
  for (const id of ids) {
    const n = clampInt(perClientLimit[id], 0, 0, total);
    limits[id] = n;
    sum += n;
  }

  if (sum === 0) {
    const base = Math.floor(total / ids.length);
    let rem = total - base * ids.length;
    for (const id of ids) {
      limits[id] = base + (rem > 0 ? 1 : 0);
      rem--;
    }
    sum = total;
  }

  while (sum < total) {
    for (const id of ids) {
      if (sum >= total) break;
      limits[id] += 1;
      sum += 1;
    }
  }
  while (sum > total) {
    for (let i = ids.length - 1; i >= 0; i--) {
      if (sum <= total) break;
      const id = ids[i];
      if (limits[id] > 0) { limits[id] -= 1; sum -= 1; }
    }
    if (sum > total) break;
  }

  const assignments = {};
  for (const id of ids) assignments[id] = [];

  let idx = 0;
  for (const id of ids) {
    const take = limits[id];
    for (let k = 0; k < take && idx < total; k++) {
      assignments[id].push(numbers[idx]);
      idx++;
    }
  }
  while (idx < total) { assignments[ids[0]].push(numbers[idx]); idx++; }

  return { assignments, limits };
}

function readSafeModeCfg(req) {
  const enabled = String(req.body.safeMode || '0') === '1';
  const perClientMaxPerMinute = clampInt(req.body.perClientMaxPerMinute, 18, 1, 120);
  const warmupMinutes = clampInt(req.body.warmupMinutes, 6, 0, 60);
  const extraDelayFrom = clampInt(req.body.safeExtraDelayFrom, 1, 0, 3600);
  const extraDelayTo = clampInt(req.body.safeExtraDelayTo, 3, 0, 3600);
  return {
    enabled,
    perClientMaxPerMinute,
    warmupMinutes,
    extraDelayFrom,
    extraDelayTo: Math.max(extraDelayFrom, extraDelayTo),
  };
}

// ---------------- Sending Jobs (for realtime progress bar) ----------------
const jobs = new Map(); // jobId -> { username,total,done,ok,failed,startedAt,finishedAt }
function newJob(username, total) {
  const jobId = `job_${Date.now()}_${Math.random().toString(36).slice(2,8)}`;
  const job = { jobId, username, total, done: 0, ok: 0, failed: 0, startedAt: nowStamp(), finishedAt: null };
  jobs.set(jobId, job);
  ioEmit('send_progress', { ...job, status: 'start' });
  return job;
}
function updateJob(job) {
  ioEmit('send_progress', { ...job, status: 'update' });
}
function finishJob(job) {
  job.finishedAt = nowStamp();
  ioEmit('send_progress', { ...job, status: 'finish' });
}

// ---------------- /send (with credit safety + realtime progress) ----------------
app.post('/send', upload.single('photo'), async (req, res) => {
  const username = req.session?.user?.username;
  if (!username) return res.status(401).json({ success: false, error: 'Unauthorized' });

  if (!canAffordMore(username)) {
    return res.status(402).json({ success: false, error: 'No credit left. Please top up.' });
  }

  const requestedClientIdRaw = String(req.body.clientId || '').trim();
  const requestedClientId = requestedClientIdRaw ? assertSafeClientId(requestedClientIdRaw) : null;

  const sendMode = String(req.body.sendMode || 'single').trim(); // single | all
  const safeModeCfg = readSafeModeCfg(req);

  const numbersAll = parseNumbers(req.body.numbers);

  // NEW: default country code used ONLY for local-format numbers (leading 0 etc)
  // For worldwide support, prefer sending E.164 like +14155552671
  const defaultCountryCode = String(req.body.defaultCountryCode || '').replace(/\D/g, '').trim() || null;

  const limit = clampInt(req.body.limit, 0, 0, 50000);
  let numbers = limit > 0 ? numbersAll.slice(0, limit) : numbersAll;

  let delayFrom = clampInt(req.body.delayFrom, 0, 0, 3600);
  let delayTo = clampInt(req.body.delayTo, 0, 0, 3600);
  if (delayTo < delayFrom) [delayFrom, delayTo] = [delayTo, delayFrom];

  const messages = safeJsonParseArray(req.body.messages)
    .map((m) => String(m || '').trim())
    .filter(Boolean);

  const sendContactEnabled = String(req.body.sendContactEnabled || '0') === '1';
  const contactName = String(req.body.contactName || '').trim();
  const contactPhone = String(req.body.contactPhone || '').trim();

  if (!numbers.length) return res.status(400).json({ success: false, error: 'Provide at least one number' });
  if (!messages.length && !req.file && !sendContactEnabled) {
    return res.status(400).json({ success: false, error: 'Provide message or photo or contact card' });
  }
  if (sendContactEnabled && (!contactName || !contactPhone)) {
    return res.status(400).json({ success: false, error: 'Contact name and phone required' });
  }

  // Credit conservative limiter:
  const allowedByCredit = maxTargetsFromCredit(username);
  if (allowedByCredit <= 0) {
    return res.status(402).json({ success: false, error: 'No credit left.' });
  }
  if (numbers.length > allowedByCredit) {
    numbers = numbers.slice(0, allowedByCredit);
  }

  // media
  let media = null;
  if (req.file) {
    const base64 = req.file.buffer.toString('base64');
    media = new MessageMedia(req.file.mimetype, base64, req.file.originalname);
  }

  let vcardMedia = null;
  if (sendContactEnabled) {
    const vcard = buildVCard(contactName, contactPhone);
    const b64 = Buffer.from(vcard, 'utf8').toString('base64');
    vcardMedia = new MessageMedia('text/vcard', b64, `${contactName}.vcf`);
  }

  const readyClients = getReadyClients().map((c) => c.id);
  if (!readyClients.length) {
    return res.status(503).json({ success: false, error: 'No WhatsApp client ready (or all cooling down). Scan QR / wait cooldown.' });
  }

  // batches
  let batches = {};
  let limitsUsed = {};

  if (sendMode === 'all') {
    const perClientLimit = parseMapJson(req.body.perClientLimit);
    const dist = buildDistribution(numbers, readyClients, perClientLimit);
    batches = dist.assignments;
    limitsUsed = dist.limits;
  } else {
    const preferred = (requestedClientId && readyClients.includes(requestedClientId)) ? requestedClientId : readyClients[0];
    batches = { [preferred]: numbers.slice() };
    limitsUsed = { [preferred]: numbers.length };
  }

  const plan = {
    user: username,
    sendMode,
    requestedClientId,
    numbers_total: numbersAll.length,
    numbers_processing: numbers.length,
    delayFromSeconds: delayFrom,
    delayToSeconds: delayTo,
    messages_count: messages.length,
    hasPhoto: !!media,
    sendContactEnabled,
    defaultCountryCode, // NEW
    readyClients,
    perClientLimitUsed: limitsUsed,
    safeMode: safeModeCfg,
    creditBefore: getUser(username)?.credit ?? 0,
    billing: billingRate(),
  };

  const job = newJob(username, numbers.length);

  const results = [];
  let failoverCount = 0;

  async function maybeSafeWait(st) {
    if (!safeModeCfg.enabled) return st;

    while (!canSendNow(st, safeModeCfg)) {
      const nxt = nextReadyClient(st.id);
      if (nxt && nxt.id !== st.id && canSendNow(nxt, safeModeCfg)) return nxt;
      await sleep(1000);
    }
    return st;
  }

  async function sendToOneNumber(st, raw, indexGlobal) {
    const chatId = toWorldwideJid(raw, defaultCountryCode);
    const per = { number: raw, chatId, success: true, clientUsed: null, pickedIndex: null, actions: [] };
    const logAction = (ok, action, detail) => per.actions.push({ at: nowStamp(), ok, action, detail: detail || null });

    if (!chatId) {
      per.success = false;
      logCompact('failed', nowStamp(), raw, st?.id || 'none');
      logAction(false, 'invalid_number', defaultCountryCode ? `defaultCC=${defaultCountryCode}` : 'missing_defaultCC_or_invalid_E164');
      return per;
    }

    const ctx = { number: raw, jid: chatId, index: indexGlobal, rand: Math.random().toString(36).slice(2, 8) };

    let pickedMsg = '';
    if (messages.length) {
      const idx = Math.floor(Math.random() * messages.length);
      per.pickedIndex = idx;
      pickedMsg = applyTemplate(messages[idx], ctx);
    }

    const MAX_ATTEMPTS = 4;
    let attempt = 0;

    while (attempt < MAX_ATTEMPTS) {
      attempt++;

      if (!st || !st.ready || isCoolingDown(st)) {
        const nxt = nextReadyClient(st?.id);
        if (!nxt) {
          per.success = false;
          logCompact('failed', nowStamp(), raw, 'none');
          logAction(false, 'no_ready_clients');
          return per;
        }
        failoverCount++;
        st = nxt;
        logAction(true, 'failover_switch', `switched_to=${st.id}`);
      }

      const st2 = await maybeSafeWait(st);
      if (st2 && st2.id !== st.id) {
        failoverCount++;
        st = st2;
        logAction(true, 'safe_switch', `switched_to=${st.id}`);
      }

      per.clientUsed = st.id;

      try {
        // optional contact card first
        if (vcardMedia) {
          await st.client.sendMessage(chatId, vcardMedia);
          logAction(true, 'sent_contact_card');
          noteSent(st);
          if (safeModeCfg.enabled) {
            const extra = randomInt(safeModeCfg.extraDelayFrom, safeModeCfg.extraDelayTo);
            if (extra > 0) { logAction(true, 'safe_extra_delay', `${extra}s`); await sleep(extra * 1000); }
          }
        }

        if (media) {
          const m = await st.client.sendMessage(chatId, media, { caption: pickedMsg || undefined });
          const msgId = m?.id?._serialized;
          if (msgId) {
            st.sentIndex.set(msgId, { number: raw, chatId, clientId: st.id });
            msgOwner.set(msgId, username); // important for billing on delivered
          }

          logCompact('sent', nowStamp(), raw, st.id);
          logAction(true, 'sent_photo', msgId ? `msgId=${msgId}` : '');
          noteSent(st);
        } else if (pickedMsg) {
          const m = await st.client.sendMessage(chatId, pickedMsg);
          const msgId = m?.id?._serialized;
          if (msgId) {
            st.sentIndex.set(msgId, { number: raw, chatId, clientId: st.id });
            msgOwner.set(msgId, username);
          }

          logCompact('sent', nowStamp(), raw, st.id);
          logAction(true, 'sent_text', msgId ? `msgId=${msgId}` : '');
          noteSent(st);
        } else {
          // only vCard
          logCompact('sent', nowStamp(), raw, st.id);
          logAction(true, 'sent_only_contact');
          noteSent(st);
        }

        return per;
      } catch (e) {
        const msg = e?.message || String(e);
        logAction(false, 'send_error', msg);
        noteError(st, msg);

        const low = msg.toLowerCase();
        if (low.includes('not ready') || low.includes('disconnected')) st.ready = false;

        const nxt = nextReadyClient(st.id);
        if (nxt) {
          failoverCount++;
          st = nxt;
          logAction(true, 'failover_switch', `switched_to=${st.id}`);
          continue;
        }

        per.success = false;
        logCompact('failed', nowStamp(), raw, per.clientUsed || 'none');
        return per;
      }
    }

    per.success = false;
    logCompact('failed', nowStamp(), raw, per.clientUsed || 'none');
    return per;
  }

  // Run batches sequentially (stable)
  let globalIndex = 0;

  try {
    for (const [batchClientId, nums] of Object.entries(batches)) {
      let st = pickClientForSend(batchClientId) || pickClientForSend(null);

      for (const raw of nums) {
        // if user credit ended mid-job, stop immediately
        if (!canAffordMore(username)) {
          const stopMsg = 'Credit ended. Job stopped.';
          ioEmit('send_progress', { ...job, status: 'stopped', reason: stopMsg });
          finishJob(job);
          return res.status(402).json({ success: false, error: stopMsg, plan, jobId: job.jobId, partial: results });
        }

        globalIndex++;
        const per = await sendToOneNumber(st, raw, globalIndex);
        results.push(per);

        job.done++;
        if (per.success) job.ok++; else job.failed++;
        updateJob(job);

        // global delay
        const d = randomInt(delayFrom, delayTo);
        if (d > 0) await sleep(d * 1000);

        // safe mode extra delay (global)
        if (safeModeCfg.enabled) {
          const extra = randomInt(safeModeCfg.extraDelayFrom, safeModeCfg.extraDelayTo);
          if (extra > 0) await sleep(extra * 1000);
        }
      }
    }

    finishJob(job);

    const me = getUser(username);
    res.json({
      success: true,
      plan: { ...plan, creditAfter: me?.credit ?? 0 },
      jobId: job.jobId,
      failoverCount,
      count: results.length,
      results,
    });
  } catch (e) {
    finishJob(job);
    res.status(500).json({ success: false, error: e?.message || 'send failed', plan, jobId: job.jobId, partial: results });
  }
});

// ---------------- Start ----------------
const PORT = 3000;
server.listen(PORT, () => {
  console.log(`🚀 Dashboard running on http://localhost:${PORT}`);
  console.log(`🔐 Login: http://localhost:${PORT}/login`);
  console.log(`✅ Accounts persist in data/accounts.json and auto-connect via saved sessions.`);
  console.log(`📡 Socket.IO realtime enabled (logs + progress + credit).`);
});
