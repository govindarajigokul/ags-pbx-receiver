// AGS PBX recording receiver.
//
// Accepts call recordings pushed by pbx-uploader/sd_pbx_uploader.py running on each
// resort's PBX, and stores them on disk with their call details. Nothing else yet:
// transcription / AI processing will read from here later.
//
//   POST /v1/pbx/recordings            raw audio body, Bearer <PBX token>   (from the PBX)
//   GET  /v1/pbx/recordings            list, Bearer <ADMIN_TOKEN>  ?source=&since=&limit=
//   GET  /v1/pbx/recordings/:id/audio  stream one file, Bearer <ADMIN_TOKEN>
//   GET  /health
//
// No dependencies: Node 20+ standard library only.
import { createServer } from 'node:http';
import { createHash, randomUUID, timingSafeEqual } from 'node:crypto';
import { createReadStream, createWriteStream, existsSync, mkdirSync, readFileSync, appendFileSync, renameSync, unlinkSync, statSync } from 'node:fs';
import { join, dirname } from 'node:path';

const PORT = Number(process.env.PORT || 3000);
const DATA_DIR = process.env.DATA_DIR || './data';
const MAX_BYTES = Number(process.env.MAX_BYTES || 25 * 1024 * 1024);
const ADMIN_TOKEN = process.env.ADMIN_TOKEN || '';
// "resort-a:token1,resort-b:token2" — one token per PBX, the name tags every recording.
const SOURCES = parseSources(process.env.PBX_TOKENS || '');

if (SOURCES.length === 0) console.warn('PBX_TOKENS is empty: every upload will be refused');
if (!ADMIN_TOKEN) console.warn('ADMIN_TOKEN is empty: list/download endpoints are disabled');

const INDEX = join(DATA_DIR, 'index.jsonl');
const TMP = join(DATA_DIR, 'tmp');
mkdirSync(TMP, { recursive: true });

/** @type {Map<string, object>} id -> record */
const byId = new Map();
/** `${source}:${sha256}` -> id, for idempotent re-uploads */
const bySha = new Map();
loadIndex();

function parseSources(s) {
  return s.split(',').map((p) => p.trim()).filter(Boolean).map((p) => {
    const i = p.indexOf(':');
    return { name: p.slice(0, i).trim(), token: Buffer.from(p.slice(i + 1).trim()) };
  }).filter((x) => x.name && x.token.length);
}

function loadIndex() {
  if (!existsSync(INDEX)) return;
  for (const line of readFileSync(INDEX, 'utf8').split('\n')) {
    if (!line.trim()) continue;
    try {
      const r = JSON.parse(line);
      byId.set(r.id, r);
      bySha.set(`${r.source}:${r.sha256}`, r.id);
    } catch { /* a torn last line after a crash: skip it */ }
  }
  console.log(`loaded ${byId.size} recordings from index`);
}

function bearer(req) {
  const h = req.headers.authorization || '';
  return h.startsWith('Bearer ') ? Buffer.from(h.slice(7).trim()) : null;
}

function same(a, b) {
  return a.length === b.length && timingSafeEqual(a, b);
}

function sourceFor(req) {
  const t = bearer(req);
  return t ? SOURCES.find((s) => same(s.token, t))?.name ?? null : null;
}

function isAdmin(req) {
  const t = bearer(req);
  return !!(ADMIN_TOKEN && t && same(Buffer.from(ADMIN_TOKEN), t));
}

function send(res, code, body) {
  res.writeHead(code, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify(body));
}

const EXT = { 'audio/wav': 'wav', 'audio/x-wav': 'wav', 'audio/wave': 'wav', 'audio/mpeg': 'mp3', 'audio/gsm': 'gsm' };
const safe = (v, n = 64) => (v == null ? null : String(v).replace(/[^\w.:+\- /]/g, '').slice(0, n) || null);

// Answering before the body is read makes the proxy cut the upload mid-stream, and the PBX
// then logs a TLS error instead of the real status. Drain small bodies first; close on big ones.
function reject(req, res, code, body) {
  if (Number(req.headers['content-length'] || 0) > MAX_BYTES) {
    res.setHeader('Connection', 'close');
    return send(res, code, body);
  }
  req.resume();
  req.on('end', () => send(res, code, body));
}

async function receive(req, res) {
  const source = sourceFor(req);
  if (!source) return reject(req, res, 401, { error: 'bad token' });

  const type = String(req.headers['content-type'] || '').split(';')[0].trim().toLowerCase();
  const ext = EXT[type];
  if (!ext) return reject(req, res, 415, { error: `unsupported content-type ${type}` });
  const declared = Number(req.headers['content-length'] || 0);
  if (declared > MAX_BYTES) return reject(req, res, 413, { error: 'too large' });

  // Stream to a temp file while hashing, then move into place: a dropped upload leaves nothing behind.
  const tmp = join(TMP, randomUUID());
  const hash = createHash('sha256');
  let size = 0;
  const out = createWriteStream(tmp);
  try {
    await new Promise((resolve, reject) => {
      req.on('data', (chunk) => {
        size += chunk.length;
        if (size > MAX_BYTES) { req.destroy(); reject(Object.assign(new Error('too large'), { code: 413 })); return; }
        hash.update(chunk);
        if (!out.write(chunk)) { req.pause(); out.once('drain', () => req.resume()); }
      });
      req.on('end', resolve);
      req.on('error', reject);
      req.on('aborted', () => reject(new Error('aborted')));
    });
    await new Promise((resolve, reject) => out.end((e) => (e ? reject(e) : resolve())));
  } catch (e) {
    out.destroy();
    try { unlinkSync(tmp); } catch {}
    if (e.code === 413) return send(res, 413, { error: 'too large' });
    return; // client went away
  }

  const sha256 = hash.digest('hex');
  const claimed = req.headers['x-pbx-sha256'];
  if (claimed && claimed !== sha256) {
    unlinkSync(tmp);
    return send(res, 422, { error: 'sha256 mismatch' });
  }
  if (size === 0) {
    unlinkSync(tmp);
    return send(res, 422, { error: 'empty body' });
  }

  const dupe = bySha.get(`${source}:${sha256}`);
  if (dupe) {
    unlinkSync(tmp);
    return send(res, 409, { id: dupe, duplicate: true });
  }

  const started = safe(req.headers['x-pbx-started'], 19); // PBX local time "YYYY-MM-DD HH:MM:SS"
  const day = /^\d{4}-\d{2}-\d{2}/.test(started || '') ? started.slice(0, 10).split('-') : new Date().toISOString().slice(0, 10).split('-');
  const id = randomUUID();
  const rel = join('recordings', source, ...day, `${id}.${ext}`);
  const dest = join(DATA_DIR, rel);
  mkdirSync(dirname(dest), { recursive: true });
  renameSync(tmp, dest);

  const duration = Number(req.headers['x-pbx-duration']);
  const record = {
    id,
    source,
    receivedAt: new Date().toISOString(),
    file: rel.split('\\').join('/'),
    contentType: type,
    size,
    sha256,
    pbxPath: safe(req.headers['x-pbx-path'], 255),
    kind: safe(req.headers['x-pbx-kind'], 16),       // exten | in | out | q | g ...
    from: safe(req.headers['x-pbx-from'], 32),
    to: safe(req.headers['x-pbx-to'], 32),
    startedAt: started,
    uniqueid: safe(req.headers['x-pbx-uniqueid'], 40),
    durationSecs: Number.isFinite(duration) ? duration : null,
    uploader: safe(req.headers['user-agent'], 80),
  };
  appendFileSync(INDEX, JSON.stringify(record) + '\n');
  byId.set(id, record);
  bySha.set(`${source}:${sha256}`, id);
  console.log(`stored ${source} ${record.pbxPath} (${size} bytes)`);
  send(res, 201, { id });
}

function list(req, res, url) {
  if (!isAdmin(req)) return send(res, 401, { error: 'bad token' });
  const source = url.searchParams.get('source');
  const since = url.searchParams.get('since'); // ISO receivedAt
  const limit = Math.min(Number(url.searchParams.get('limit') || 100), 1000);
  const rows = [];
  for (const r of [...byId.values()].reverse()) {
    if (source && r.source !== source) continue;
    if (since && r.receivedAt <= since) continue;
    rows.push(r);
    if (rows.length >= limit) break;
  }
  send(res, 200, { count: rows.length, recordings: rows });
}

function audio(req, res, id) {
  if (!isAdmin(req)) return send(res, 401, { error: 'bad token' });
  const r = byId.get(id);
  const path = r && join(DATA_DIR, r.file);
  if (!r || !existsSync(path)) return send(res, 404, { error: 'not found' });
  res.writeHead(200, { 'Content-Type': r.contentType, 'Content-Length': statSync(path).size });
  createReadStream(path).pipe(res);
}

const server = createServer((req, res) => {
  const url = new URL(req.url, 'http://x');
  const p = url.pathname.replace(/\/+$/, '');
  try {
    if (p === '/health') return send(res, 200, { ok: true, recordings: byId.size });
    if (p === '/v1/pbx/recordings' && req.method === 'POST') return void receive(req, res).catch((e) => {
      console.error(e);
      if (!res.headersSent) send(res, 500, { error: 'internal' });
    });
    if (p === '/v1/pbx/recordings' && req.method === 'GET') return list(req, res, url);
    const m = p.match(/^\/v1\/pbx\/recordings\/([0-9a-f-]{36})\/audio$/);
    if (m && req.method === 'GET') return audio(req, res, m[1]);
    send(res, 404, { error: 'not found' });
  } catch (e) {
    console.error(e);
    if (!res.headersSent) send(res, 500, { error: 'internal' });
  }
});
server.requestTimeout = 10 * 60_000; // slow resort uplinks
server.listen(PORT, () => console.log(`ags-pbx-receiver listening on :${PORT}, data in ${DATA_DIR}`));
