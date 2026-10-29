'use strict';
// Toon Brawl relay — lets viewers anywhere play against a streamer running Toonlight Brawl.
// It only passes messages between the streamer's app (the "host") and players' browsers. The player page itself is
// fetched from a connected streamer's Toonlight Brawl (only the player page and drawing code), so this one file is
// the whole relay. No accounts, nothing stored. Run it on any Node 18+ host with WebSockets:  node relay.cjs
// (PORT from the environment, default 8080). See docs/BRAWL-RELAY.md in the app for step-by-step hosting.
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { EventEmitter } = require('events');
// ---- a tiny WebSocket server (same as the app's app/ws.cjs), inlined so the relay is one file ----
const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';

class Conn extends EventEmitter {
  constructor(socket, req, { maxMessage = 1 << 20 } = {}) {
    super(); this.socket = socket; this.req = req; this.max = maxMessage; this.buf = Buffer.alloc(0); this.frag = null; this.open = true;
    this.url = new URL(req.url, 'http://x'); this.ip = (req.headers['x-forwarded-for'] || socket.remoteAddress || '').toString().split(',')[0].trim();
    socket.setNoDelay(true);
    this.seen = Date.now();
    socket.on('data', (d) => { this.seen = Date.now(); this._data(d); });
    socket.on('close', () => this._closed());
    socket.on('error', () => this._closed());
  }
  _closed() { if (!this.open) return; this.open = false; this.emit('close'); }
  _data(d) {
    this.buf = this.buf.length ? Buffer.concat([this.buf, d]) : d;
    while (this.buf.length >= 2) {
      const b0 = this.buf[0], b1 = this.buf[1], fin = !!(b0 & 0x80), op = b0 & 0x0f, masked = !!(b1 & 0x80);
      let len = b1 & 0x7f, off = 2;
      if (len === 126) { if (this.buf.length < 4) return; len = this.buf.readUInt16BE(2); off = 4; }
      else if (len === 127) { if (this.buf.length < 10) return; const hi = this.buf.readUInt32BE(2); if (hi) return this.close(1009); len = this.buf.readUInt32BE(6); off = 10; }
      if (len > this.max) return this.close(1009);
      const need = off + (masked ? 4 : 0) + len; if (this.buf.length < need) return;
      let payload = this.buf.subarray(off + (masked ? 4 : 0), need);
      if (masked) { const m = this.buf.subarray(off, off + 4); payload = Buffer.from(payload); for (let i = 0; i < payload.length; i++) payload[i] ^= m[i & 3]; }
      this.buf = this.buf.subarray(need);
      if (op === 0x8) { this.close(1000); return; }
      if (op === 0x9) { this._send(0xA, payload); continue; }
      if (op === 0xA) continue;
      if (op === 0x1 || op === 0x2) { if (fin) this.emit('message', payload.toString('utf8')); else this.frag = [payload]; }
      else if (op === 0x0 && this.frag) { this.frag.push(payload); if (this.frag.reduce((a, b) => a + b.length, 0) > this.max) return this.close(1009); if (fin) { const m = Buffer.concat(this.frag).toString('utf8'); this.frag = null; this.emit('message', m); } }
    }
  }
  _send(op, data) {
    if (!this.open) return false; const len = data.length; let head;
    if (len < 126) { head = Buffer.from([0x80 | op, len]); }
    else if (len < 65536) { head = Buffer.alloc(4); head[0] = 0x80 | op; head[1] = 126; head.writeUInt16BE(len, 2); }
    else { head = Buffer.alloc(10); head[0] = 0x80 | op; head[1] = 127; head.writeUInt32BE(0, 2); head.writeUInt32BE(len, 6); }
    try { this.socket.write(Buffer.concat([head, data])); return true; } catch { return false; }
  }
  // skip a frame when the client is slow, so a phone on bad Wi-Fi never builds up lag
  send(str) { if (!this.open) return false; if (this.socket.writableLength > (this.lagLimit || 256 * 1024)) return false; return this._send(0x1, Buffer.from(String(str), 'utf8')); }
  json(o) { return this.send(JSON.stringify(o)); }
  ping() { return this._send(0x9, Buffer.alloc(0)); }
  close(code = 1000) { if (!this.open) return; const b = Buffer.alloc(2); b.writeUInt16BE(code, 0); this._send(0x8, b); this.open = false; try { this.socket.end(); } catch { } this.emit('close'); }
}

// accept(req, socket) → Conn, or null when it isn't a valid WebSocket upgrade
const ALL = new Set(); // every open connection, for the heartbeat
function accept(req, socket, opts = {}) {
  const key = req.headers['sec-websocket-key'];
  if (!key || String(req.headers.upgrade || '').toLowerCase() !== 'websocket') { try { socket.destroy(); } catch { } return null; }
  const accept = crypto.createHash('sha1').update(key + GUID).digest('base64');
  socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n' + `Sec-WebSocket-Accept: ${accept}\r\n\r\n`);
  const c = new Conn(socket, req, opts); ALL.add(c); c.on('close', () => ALL.delete(c)); return c;
}
// heartbeat: hosting sites drop quiet WebSockets after a minute or two without telling anyone, so ping every
// connection every 20 s and close the ones that stopped answering (both ends then reconnect by themselves)
setInterval(() => { const now = Date.now(); for (const c of ALL) { if (now - c.seen > 65000) { try { c.socket.destroy(); } catch { } c._closed(); } else c.ping(); } }, 20000).unref?.();


const PORT = Number(process.env.PORT) || 8080;
const PUB = path.join(__dirname, 'public');
const LIMITS = { rooms: 500, playersPerRoom: 30, perIp: 12, hostMsg: 4 << 20, playerMsg: 8 << 10, inputsPerSec: 120 };
const MIME = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8', '.json': 'application/json', '.png': 'image/png', '.jpg': 'image/jpeg', '.ttf': 'font/ttf', '.css': 'text/css; charset=utf-8' };
const rooms = new Map(); // room → { host, secret, players: Map(pid → p), n }
const perIp = new Map();
const ROOM_RE = /^[a-z0-9]{4,12}$/;
const log = (...a) => console.log(new Date().toISOString(), ...a);

function serve(req, res) {
  const u = new URL(req.url, 'http://x');
  if (u.pathname === '/health') { res.writeHead(200, { 'Content-Type': 'application/json' }); return res.end(JSON.stringify({ ok: true, rooms: rooms.size })); }
  if (u.pathname === '/' || u.pathname === '/play') { res.writeHead(302, { Location: '/brawl/play.html' + (u.search || '') }); return res.end(); }
  let rel; try { rel = decodeURIComponent(u.pathname).replace(/^\/+/, ''); } catch { res.writeHead(400); return res.end(); }
  if (rel.includes('..') || !FILE_RE.test(rel) || !MIME[path.extname(rel)]) { res.writeHead(404); return res.end(); }
  const send = (buf, type, ver) => { res.writeHead(200, { 'Content-Type': type || MIME[path.extname(rel)], 'Cache-Control': 'no-cache', 'X-Content-Type-Options': 'nosniff', 'X-Brawl-Version': ver || '' }); res.end(buf); };
  // a public/ folder next to relay.cjs wins (optional); otherwise ask a connected streamer's app for the file
  const f = path.join(PUB, rel);
  fs.readFile(f, (err, buf) => { if (!err && f.startsWith(PUB + path.sep)) return send(buf); fromHost(rel).then(r => r ? send(r.buf, r.type, r.ver) : (res.writeHead(503, { 'Content-Type': 'text/plain' }), res.end('No streamer is online right now, so the player page is not available. Try again when the stream is live.'))); });
}
// ---- player page files come from a streamer's app (only these folders; the app checks the same list) ----
const FILE_RE = /^(brawl\/[\w./-]+\.(js|html|png|jpg)|(renderer|animation|wardrobe)\/[\w./-]+\.js|characters\/[\w-]+\.js|assets\/fonts\/ToonlightToon\.ttf)$/;
const cache = new Map(); let cacheBytes = 0, reqN = 0; const pending = new Map();
function pickHost() { let best = null; for (const r of rooms.values()) if (r.host && r.host.open && (!best || r.since > best.since)) best = r; return best; }
function fromHost(rel) {
  const room = pickHost(); if (!room) return Promise.resolve(null);
  const hit = cache.get(rel); if (hit && hit.ver === room.ver) return Promise.resolve(hit);
  return new Promise((resolve) => {
    const id = ++reqN; const t = setTimeout(() => { pending.delete(id); resolve(null); }, 15000); t.unref?.();
    pending.set(id, (o) => { clearTimeout(t); pending.delete(id); if (!o || !o.ok) return resolve(null); const buf = Buffer.from(o.b64, 'base64'); const e = { buf, type: o.type, ver: room.ver };
      if (buf.length < 4 << 20) { const old = cache.get(rel); if (old) cacheBytes -= old.buf.length; cache.set(rel, e); cacheBytes += buf.length; while (cacheBytes > 60 << 20) { const k = cache.keys().next().value; cacheBytes -= cache.get(k).buf.length; cache.delete(k); } }
      resolve(e); });
    room.host.json({ t: 'file', id, path: rel });
  });
}
function newCode(room) { const A = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'; let c; do { c = Array.from(crypto.randomBytes(4), b => A[b % A.length]).join(''); } while ([...room.players.values()].some(p => p.code === c)); return c; }
const sameSecret = (a, b) => typeof a === 'string' && typeof b === 'string' && a.length === b.length && crypto.timingSafeEqual(Buffer.from(a), Buffer.from(b));

function onHost(conn, roomId, secret) {
  let room = rooms.get(roomId);
  if (room && !sameSecret(secret, room.secret)) { conn.json({ t: 'error', why: 'That room is in use by someone else.' }); return conn.close(4003); }
  if (!room) { if (rooms.size >= LIMITS.rooms) return conn.close(4029); room = { secret, host: null, players: new Map(), n: 0 }; rooms.set(roomId, room); }
  if (room.host && room.host.open) room.host.close(4000);
  room.host = conn; room.since = Date.now(); room.ver = room.ver || ''; conn.lagLimit = 8 << 20; log('host up', roomId);
  conn.json({ t: 'relay', ok: true, room: roomId });
  for (const p of room.players.values()) if (p.conn) p.conn.json({ t: 'hostback' });
  for (const [pid, p] of room.players) conn.json({ t: 'pjoin', pid, name: p.name, code: p.code, via: 'relay', away: !p.conn });
  conn.on('message', (m) => {
    if (m.charCodeAt(0) === 76 /* L */) { const s = m.slice(1); for (const p of room.players.values()) if (p.ready && p.conn) p.conn.send(s); return; }
    let o; try { o = JSON.parse(m); } catch { return; }
    if (o.t === 'filedata') { const cb = pending.get(o.id); if (cb) cb(o); return; }
    if (o.t === 'hello') { room.ver = String(o.version || ''); return; }
    if (o.t === 'ping') { conn.json({ t: 'pong', at: o.at }); return; }
    if (o.t === 'to') { const p = room.players.get(o.pid); if (p && o.msg) { if (o.msg.t === 'accept') p.ready = true; if (p.conn) p.conn.json(o.msg); } }
    else if (o.t === 'kick') { const p = room.players.get(o.pid); if (p) { if (p.conn) { p.conn.json({ t: 'bye', why: o.why || 'removed' }); p.kicked = true; p.conn.close(4001); } room.players.delete(o.pid); } }
  });
  conn.on('close', () => {
    if (room.host !== conn) return; room.host = null; log('host gone', roomId);
    // players wait three minutes for the streamer's app to come back, then the room closes
    for (const p of room.players.values()) if (p.conn) p.conn.json({ t: 'hostaway' });
    setTimeout(() => { if (!room.host) { for (const p of room.players.values()) { try { if (p.conn) { p.conn.json({ t: 'bye', why: 'The streamer went offline.' }); p.conn.close(4004); } } catch { } } rooms.delete(roomId); } }, 180000).unref?.();
  });
}
function onPlayer(conn, roomId, name, resume) {
  const room = rooms.get(roomId);
  if (!room || !room.host) { conn.json({ t: 'error', why: 'The streamer is not online right now.' }); return conn.close(4004); }
  // a player whose connection dropped comes back with their key and keeps their code and place
  let pid = null, p = null;
  if (resume) for (const [id, q] of room.players) if (q.key === resume) { pid = id; p = q; break; }
  if (p) { if (p.conn && p.conn !== conn) { p.replaced = true; p.conn.close(4000); } clearTimeout(p.goneT); p.conn = conn; conn.json({ t: 'code', code: p.code, key: p.key, via: 'relay', resumed: true }); room.host.json({ t: 'pback', pid }); }
  else {
    if (room.players.size >= LIMITS.playersPerRoom) { conn.json({ t: 'error', why: 'Too many players are connected. Try again in a bit.' }); return conn.close(4029); }
    pid = 'R' + (++room.n); p = { conn, name: String(name || '').replace(/[^\w .-]/g, '').slice(0, 24), code: newCode(room), key: crypto.randomBytes(12).toString('hex'), ready: false, n: 0, t0: Date.now() };
    room.players.set(pid, p); conn.json({ t: 'code', code: p.code, key: p.key, via: 'relay' }); room.host.json({ t: 'pjoin', pid, name: p.name, code: p.code, via: 'relay' });
  }
  conn.on('message', (m) => {
    const now = Date.now(); if (now - p.t0 >= 1000) { p.t0 = now; p.n = 0; } if (++p.n > LIMITS.inputsPerSec) return;
    let o; try { o = JSON.parse(m); } catch { return; }
    if (o.t === 'in') { if (room.host) room.host.json({ t: 'pin', pid, l: !!o.l, r: !!o.r, u: !!o.u, d: !!o.d, press: Array.isArray(o.press) ? o.press.slice(0, 6).map(String) : [] }); }
    else if (o.t === 'ping') conn.json({ t: 'pong', at: o.at });
  });
  conn.on('close', () => {
    if (p.conn !== conn) return; p.conn = null;
    if (p.kicked) { room.players.delete(pid); if (room.host) room.host.json({ t: 'pleave', pid }); return; }
    // keep their place for two minutes in case they come straight back
    p.goneT = setTimeout(() => { if (!p.conn && room.players.get(pid) === p) { room.players.delete(pid); if (room.host) room.host.json({ t: 'pleave', pid }); } }, 120000); p.goneT.unref?.();
  });
}
const server = http.createServer(serve);
server.on('upgrade', (req, socket) => {
  const u = new URL(req.url, 'http://x'), ip = (req.headers['x-forwarded-for'] || socket.remoteAddress || '').toString().split(',')[0].trim();
  const n = perIp.get(ip) || 0; if (n >= LIMITS.perIp) { socket.destroy(); return; }
  const room = String(u.searchParams.get('room') || '').toLowerCase();
  if (!ROOM_RE.test(room) || (u.pathname !== '/host' && u.pathname !== '/play')) { socket.destroy(); return; }
  const isHost = u.pathname === '/host', secret = String(u.searchParams.get('secret') || '');
  if (isHost && secret.length < 16) { socket.destroy(); return; }
  const conn = accept(req, socket, { maxMessage: isHost ? LIMITS.hostMsg : LIMITS.playerMsg }); if (!conn) return;
  perIp.set(ip, n + 1); conn.on('close', () => { const k = (perIp.get(ip) || 1) - 1; if (k <= 0) perIp.delete(ip); else perIp.set(ip, k); });
  if (isHost) onHost(conn, room, secret); else onPlayer(conn, room, u.searchParams.get('name'), String(u.searchParams.get('resume') || '').slice(0, 64));
});
if (require.main === module) server.listen(PORT, () => log(`Toon Brawl relay on port ${PORT}`));
module.exports = { server, rooms };
