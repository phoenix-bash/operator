// ============================================================================
// relay.js  -  v4.8
//
// Differences vs v4.7:
//   - Keepalive: every agent and operator socket gets {"op":"ping"} every 25s.
//     Idle WebSockets were being dropped by whatever sits between the agent and
//     the relay, and neither end was told, so the device vanished from the list
//     until the laptop was touched. The agent already answers {"op":"pong"}.
//   - /debug now carries a "recent" ring buffer of the last 60 connect and
//     disconnect events. It answers "is the agent flapping, or is it just not
//     running?" without reading the log stream. IPs are masked to three octets.
//
// Differences vs v4.6:
//   - Bootstrap endpoints for machines that cannot self-update yet:
//       GET /agent.exe          the built Agent.exe
//       GET /agent.sha256       its SHA-256, for the installer to verify
//       GET /install-agent.ps1  the installer script
//       GET /diag-agent.ps1     the read-only diagnostic script
//     All three require the relay token (X-Relay-Token header, or ?token=).
//   - Query strings are stripped from HTTP logs: /ws?token=... was writing the
//     relay token into the Render log stream.
//
// Differences vs v4.5:
//   - The operator console is served by this process at "/", so the UI and the
//     relay ship as one Render service. No separate static host, no hardcoded
//     relay URL in the page: the relay injects { ws, token } into the HTML it
//     serves, and the page falls back to its own origin when nothing is
//     injected (local `node relay.js`).
//   - The HTML is served with cache-control: no-store, so a redeploy is picked
//     up on reload instead of being shadowed by a cached copy.
//
// Differences vs v4.4:
//   - Agent self-update: forwards {op:"upd"} (begin/chunk/end/abort) down and
//     {op:"updstate"} progress back up. The relay only routes the bytes; the
//     agent verifies size and SHA-256 before touching its install path.
//
// Differences vs v4.3:
//   - Camera stream support: forwards {op:"camera"} / {op:"cameraOnce"} to the
//     agent, {op:"cam"} state echoes and {op:"camframe"} JPEG frames back.
//   - Per-agent camera stream state (camOn/quality/maxW/intervalMs) echoed to
//     operators on attach, and force-stopped when nobody is watching.
//
// Differences vs v4.2:
//   - Agents are keyed by a unique per-connection id (sid), not by hostname.
//     Duplicate copies on one host can no longer shadow each other, which was
//     the reason "stop" reached only one agent while another kept streaming.
//   - Relay tracks per-agent stream state (streamOn/quality/maxW/intervalMs)
//     and echoes it to operators on attach.
//   - Safety net: a stream is stopped when no operator is watching the agent
//     (operator detach / attach switch / disconnect / agent death).
//   - Forwards {op:"shot"} (stream state), {op:"micPeak"} and {op:"micDiag"}.
//   - All v4.2 fixes retained (pcm forwarding, heartbeat, graceful shutdown).
//
// Deploy: Render Web Service (Node 20+), console included
//   Build:  npm install
//   Start:  node relay.js
//   Env:    RELAY_TOKEN=<32+ hex>   (required)
//   PORT is injected by Render (10000). Do not set it manually.
//
// Endpoints:
//   GET /health   -> tiny JSON status (cron keeps service warm)
//   GET /debug    -> full state, per-agent counters
//   GET /         -> operator console, with relay config injected
//   WS  /ws       -> agent + operator channels (?role=&token=)
// ============================================================================

'use strict';

const http  = require('http');
const fs    = require('fs');
const path  = require('path');
const os    = require('os');
const { WebSocketServer } = require('ws');
const { randomUUID } = require('crypto');

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------
const BOOT_TS = new Date().toISOString();
const TOKEN   = process.env.RELAY_TOKEN;
const PORT    = process.env.PORT || 10000;
const HOST    = process.env.HOST || '0.0.0.0';

// Binary the relay hands out to agents that cannot self-update yet, plus the
// bootstrap script that installs it. Overridable so the test harness can serve
// a patched build without touching the real one.
const AGENT_EXE    = process.env.AGENT_EXE || path.join(__dirname, 'Agent.exe');
const INSTALL_PS1  = path.join(__dirname, 'install-agent.ps1');
const DIAG_PS1     = path.join(__dirname, 'diag-agent.ps1');

// The token may arrive as a header (preferred, never logged) or a query param
// (kept for convenience, stripped from logs above).
function tokenOk(req, url) {
  const h = req.headers['x-relay-token'];
  if (typeof h === 'string' && h === TOKEN) return true;
  return url.searchParams.get('token') === TOKEN;
}

// ---------------------------------------------------------------------------
// Logging
// ---------------------------------------------------------------------------
function now() { return new Date().toISOString(); }
function tag(lvl) { return `[${now()}] ${lvl.padEnd(5)}`; }
function info(...a)  { console.log(tag('INFO'),  ...a); }
function warn(...a)  { console.log(tag('WARN'),  ...a); }
function error(...a) { console.log(tag('ERROR'), ...a); }
function debug(...a) { if (process.env.RELAY_DEBUG) console.log(tag('DEBUG'), ...a); }

// ---------------------------------------------------------------------------
// Boot banner
// ---------------------------------------------------------------------------
info('relay v4.8 booting');
info('  node     ', process.version);
info('  pid      ', process.pid);
info('  boot_ts  ', BOOT_TS);
info('  host     ', os.hostname());
info('  PORT     ', PORT);
info('  HOST     ', HOST);
info('  token    ', TOKEN ? `set (${TOKEN.length} chars)` : 'MISSING');
info('  debug    ', process.env.RELAY_DEBUG ? 'on' : 'off');

if (!TOKEN || TOKEN.length < 16) {
  error('RELAY_TOKEN env var required (>=16 chars). Refusing to start.');
  process.exit(1);
}

// ---------------------------------------------------------------------------
// Safe send (declared with const so strict mode is happy)
// ---------------------------------------------------------------------------
const wsSafe = (sock, payload) => {
  if (!sock || sock.readyState !== 1) return false;
  try { sock.send(payload); return true; }
  catch (e) { warn('ws send failed:', e.message); return false; }
};

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------
// sid (unique per connection) -> { sid, id, ws, info, lastSeen, counters,
//                                  streamOn, shotQuality, shotMaxW, shotIntervalMs,
//                                  camOn, camQuality, camMaxW, camIntervalMs }
const agents    = new Map();
const operators = new Set();   // Set of op objects: { ws, id, attachedTo }
let   opSeq     = 0;

// ---------------------------------------------------------------------------
// Recent events
//
// A ring buffer of connect/disconnect events, exposed at /debug. Without it the
// only way to answer "is the agent flapping or just never running?" is to read
// the Render log stream, which is awkward to correlate. With it, /debug alone
// shows the pattern: tight bursts mean a reconnect loop, wide gaps mean the
// machine is asleep or the agent is not starting.
//
// IPs are masked to three octets because /debug is unauthenticated.
// ---------------------------------------------------------------------------
const recentEvents = [];
const RECENT_MAX = 60;
function noteEvent(evt) {
  recentEvents.push({ t: new Date().toISOString(), ...evt });
  if (recentEvents.length > RECENT_MAX) recentEvents.shift();
}
function maskIp(ip) {
  const s = String(ip || '?');
  return s.includes('.') ? s.replace(/\.\d+$/, '.x') : s;
}

const stats = {
  httpRequests:    0,
  wsConnects:      0,
  wsRejects:       0,
  framesIn:        0,
  framesForwarded: 0,
  camFramesIn:     0,
  camFramesForwarded: 0,
  outsIn:          0,
  outsForwarded:   0,
  logsIn:          0,
  logsForwarded:   0,
  pcmIn:           0,
  pcmForwarded:    0,
  cmdsForwarded:   0,
  forcedStops:     0,
  drops:           0
};

// ---------------------------------------------------------------------------
// HTTP
// ---------------------------------------------------------------------------
const server = http.createServer((req, res) => {
  const t0 = Date.now();
  stats.httpRequests++;

  const ip = (req.headers['x-forwarded-for'] || req.socket.remoteAddress || '?')
             .toString().split(',')[0].trim();
  const ua = (req.headers['user-agent'] || '-').slice(0, 60);
  // Never log the query string: it can carry the relay token.
  const route = req.url.split('?')[0];
  info(`http ${req.method} ${route} ip=${ip} ua="${ua}"`);

  res.on('finish', () => {
    info(`http ${req.method} ${route} -> ${res.statusCode} (${Date.now()-t0}ms)`);
  });

  let url = null;
  try { url = new URL(req.url, 'http://relay'); } catch { /* malformed */ }

  if (req.url === '/health') {
    res.writeHead(200, {'content-type':'application/json'});
    res.end(JSON.stringify({
      ok: true,
      boot: BOOT_TS,
      uptime_s: Math.floor(process.uptime()),
      agents: agents.size,
      operators: operators.size,
      stats,
      ts: Date.now()
    }));
    return;
  }

  if (req.url === '/debug') {
    const ag = [];
    for (const a of agents.values()) ag.push({
      sid: a.sid,
      id: a.id,
      info: a.info,
      lastSeen: a.lastSeen,
      age_s: Math.floor((Date.now()-a.lastSeen)/1000),
      framesSent: a.framesSent,
      camFramesSent: a.camFramesSent,
      outsSent: a.outsSent,
      logsSent: a.logsSent,
      pcmSent: a.pcmSent,
      streamOn: a.streamOn,
      shotQuality: a.shotQuality,
      shotMaxW: a.shotMaxW,
      shotIntervalMs: a.shotIntervalMs,
      camOn: a.camOn,
      camQuality: a.camQuality,
      camMaxW: a.camMaxW,
      camIntervalMs: a.camIntervalMs
    });
    const op = [];
    for (const o of operators) op.push({
      id: o.id,
      attachedTo: o.attachedTo,
      state: o.ws.readyState
    });
    res.writeHead(200, {'content-type':'application/json'});
    res.end(JSON.stringify({
      ok: true, boot: BOOT_TS, agents: ag, operators: op, stats,
      recent: recentEvents,
      ts: Date.now()
    }, null, 2));
    return;
  }

  if (req.url === '/favicon.ico') {
    res.writeHead(204);
    res.end();
    return;
  }

  if (route === '/agent.exe' || route === '/agent.sha256' ||
      route === '/install-agent.ps1' || route === '/diag-agent.ps1') {
    if (!url || !tokenOk(req, url)) {
      res.writeHead(401, {'content-type':'text/plain'});
      res.end('unauthorized\n');
      return;
    }
    try {
      if (route === '/install-agent.ps1' || route === '/diag-agent.ps1') {
        const file = route === '/diag-agent.ps1' ? DIAG_PS1 : INSTALL_PS1;
        const ps1 = fs.readFileSync(file, 'utf8');
        res.writeHead(200, {
          'content-type': 'text/plain; charset=utf-8',
          'cache-control': 'no-store, must-revalidate'
        });
        res.end(ps1);
        return;
      }
      const bin = fs.readFileSync(AGENT_EXE);
      if (route === '/agent.sha256') {
        res.writeHead(200, {
          'content-type': 'text/plain; charset=utf-8',
          'cache-control': 'no-store, must-revalidate'
        });
        res.end(require('crypto').createHash('sha256').update(bin).digest('hex') + '\n');
        return;
      }
      res.writeHead(200, {
        'content-type': 'application/octet-stream',
        'content-length': bin.length,
        'content-disposition': 'attachment; filename="Agent.exe"',
        'cache-control': 'no-store, must-revalidate'
      });
      res.end(bin);
    } catch (e) {
      error(`${route} unavailable:`, e.message);
      res.writeHead(404, {'content-type':'text/plain'});
      res.end('not found\n');
    }
    return;
  }

  if (route === '/' || route === '/index.html') {
    try {
      const html = fs.readFileSync(path.join(__dirname, 'operator.html'), 'utf8');
      // The page ships with the relay, so it never carries a relay URL or token
      // of its own. Hand both to it here; the client falls back to its own
      // origin when this block is absent.
      const cfg = '<script>window.__RELAY_CFG__=' +
                  JSON.stringify({ ws: null, token: TOKEN }).replace(/</g, '\\u003c') +
                  ';</script>';
      const page = html.includes('</head>')
        ? html.replace('</head>', cfg + '</head>')
        : cfg + html;
      res.writeHead(200, {
        'content-type': 'text/html; charset=utf-8',
        'cache-control': 'no-store, must-revalidate'
      });
      res.end(page);
    } catch (e) {
      error('operator.html unavailable:', e.message);
      res.writeHead(500, {'content-type':'text/plain; charset=utf-8'});
      res.end('operator console missing on this deploy (operator.html)\n');
    }
    return;
  }

  res.writeHead(404, {'content-type':'text/plain'});
  res.end('not found');
});

// ---------------------------------------------------------------------------
// WSS
// ---------------------------------------------------------------------------
const wss = new WebSocketServer({ server, path: '/ws' });
wss.on('error', (e) => error('wss error:', e.message));

wss.on('connection', (ws, req) => {
  stats.wsConnects++;

  let url;
  try { url = new URL(req.url, 'ws://x'); }
  catch { warn('ws connect with bad url', req.url); ws.close(4000, 'bad url'); return; }

  const tok  = url.searchParams.get('token') || '';
  const role = url.searchParams.get('role') || '';
  const ip = (req.headers['x-forwarded-for'] || req.socket.remoteAddress || '?')
             .toString().split(',')[0].trim();

  info(`ws connect role=${role||'?'} ip=${ip} token=${tok ? `${tok.length}c` : 'none'}`);
  noteEvent({ ev: 'ws', role: role || '?', ip: maskIp(ip) });

  if (tok !== TOKEN) {
    stats.wsRejects++;
    warn(`ws REJECT bad token role=${role} ip=${ip}`);
    noteEvent({ ev: 'reject', why: 'token', role, ip: maskIp(ip) });
    ws.close(4001, 'bad token');
    return;
  }
  if (role === 'agent')         onAgent(ws, req);
  else if (role === 'operator') onOperator(ws, req);
  else { stats.wsRejects++; warn(`ws REJECT bad role="${role}"`); noteEvent({ ev: 'reject', why: 'role', role }); ws.close(4002, 'bad role'); }
});

// ---------------------------------------------------------------------------
// Agent
// ---------------------------------------------------------------------------
function onAgent(ws, req) {
  let sid = null;

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw.toString()); }
    catch { warn('agent sent bad JSON, dropped'); return; }

    if (msg.op === 'hello') {
      const reported = String(msg.id || 'agent');
      sid = randomUUID();
      agents.set(sid, {
        sid,
        id: reported,
        ws, info: msg.info || {}, lastSeen: Date.now(), lastPong: Date.now(),
        framesSent: 0, outsSent: 0, logsSent: 0, pcmSent: 0,
        camFramesSent: 0,
        streamOn: false, shotQuality: 80, shotMaxW: 0, shotIntervalMs: 500,
        camOn: false, camQuality: 80, camMaxW: 0, camIntervalMs: 500
      });
      const twins = [...agents.values()].filter(a => a.id === reported).length - 1;
      noteEvent({ ev: 'agent-in', sid: sid.slice(0, 8), id: reported });
      info(`agent ONLINE sid=${sid} id=${reported} host=${msg.info?.hostname || '?'} ` +
           `user=${msg.info?.user || '?'}${twins ? ` (duplicate id, ${twins} other copy)` : ''}`);
      broadcastAgentList();
      autoAttachIfLone();
      return;
    }

    if (!sid || !agents.has(sid)) {
      warn(`agent msg before hello, dropped op=${msg.op}`);
      return;
    }
    const a = agents.get(sid);
    a.lastSeen = Date.now();
    if (msg.op === 'pong') a.lastPong = Date.now();

    if (msg.op === 'frame')    stats.framesIn++;
    if (msg.op === 'camframe') stats.camFramesIn++;
    if (msg.op === 'out')      stats.outsIn++;
    if (msg.op === 'log')      stats.logsIn++;
    if (msg.op === 'pcm')      stats.pcmIn++;
    if (msg.op === 'frame')    a.framesSent++;
    if (msg.op === 'camframe') a.camFramesSent++;
    if (msg.op === 'out')      a.outsSent++;
    if (msg.op === 'log')      a.logsSent++;
    if (msg.op === 'pcm')      a.pcmSent++;

    if (msg.op === 'out' || msg.op === 'frame' || msg.op === 'camframe' ||
        msg.op === 'pcm' || msg.op === 'pong' || msg.op === 'log' ||
        msg.op === 'shot' || msg.op === 'cam' || msg.op === 'updstate' ||
        msg.op === 'micPeak' || msg.op === 'micDiag') {
      // Keep relay-side stream state in sync with what the agent reports.
      if (msg.op === 'shot') {
        a.streamOn       = msg.on === true;
        a.shotQuality    = Number(msg.quality ?? a.shotQuality);
        a.shotMaxW       = Number(msg.maxW ?? a.shotMaxW);
        a.shotIntervalMs = Number(msg.intervalMs ?? a.shotIntervalMs);
      }
      if (msg.op === 'cam') {
        a.camOn          = msg.on === true;
        a.camQuality     = Number(msg.quality ?? a.camQuality);
        a.camMaxW        = Number(msg.maxW ?? a.camMaxW);
        a.camIntervalMs  = Number(msg.intervalMs ?? a.camIntervalMs);
      }
      let forwarded = 0;
      for (const op of operators) {
        if (op.attachedTo === sid) {
          if (wsSafe(op.ws, JSON.stringify({ ...msg, id: sid }))) forwarded++;
        }
      }
      if (msg.op === 'frame')    stats.framesForwarded    += forwarded;
      if (msg.op === 'camframe') stats.camFramesForwarded += forwarded;
      if (msg.op === 'out')      stats.outsForwarded      += forwarded;
      if (msg.op === 'log')      stats.logsForwarded      += forwarded;
      if (msg.op === 'pcm')      stats.pcmForwarded       += forwarded;

      const n = a.framesSent + a.camFramesSent + a.outsSent + a.logsSent + a.pcmSent;
      if (forwarded === 0 && n % 20 === 1) {
        stats.drops++;
        warn(`DROP agent=${sid} op=${msg.op} (no operator attached, total dropped=${stats.drops})`);
      }
    }
  });

  ws.on('close', (code, reason) => {
    if (!sid) return;
    const a = agents.get(sid);
    noteEvent({ ev: 'agent-out', sid: sid.slice(0, 8), id: a?.id || '?', code });
    info(`agent OFFLINE sid=${sid} id=${a?.id || '?'} code=${code} reason=${reason || '-'}`);
    agents.delete(sid);
    // Detach every operator pointing at the dead agent, and tell them why.
    for (const op of operators) {
      if (op.attachedTo === sid) {
        op.attachedTo = null;
        wsSafe(op.ws, JSON.stringify({ op: 'agentGone', id: sid }));
      }
    }
    broadcastAgentList();
  });
  ws.on('error', (e) => warn(`agent ws error sid=${sid||'?'}: ${e.message}`));
}

// ---------------------------------------------------------------------------
// Operator
// ---------------------------------------------------------------------------
function onOperator(ws, req) {
  const op = { ws, id: ++opSeq, attachedTo: null };
  operators.add(op);

  noteEvent({ ev: 'op-in', op: op.id });
  info(`operator CONNECT id=op${op.id} total=${operators.size}`);

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw.toString()); }
    catch { warn(`operator op${op.id} bad JSON`); return; }

    debug(`op${op.id} -> ${msg.op}${msg.id ? ' id='+msg.id : ''}`);

    if (msg.op === 'list') {
      wsSafe(ws, JSON.stringify({ op: 'agents', list: listAgents() }));
      autoAttachIfLone();
      return;
    }
    if (msg.op === 'attach') {
      const key = resolveAgent(msg.id);
      const prev = op.attachedTo;
      op.attachedTo = key;
      info(`operator op${op.id} attached -> ${key || 'detached'}`);
      if (prev && prev !== key) stopStreamIfIdle(prev);
      wsSafe(ws, JSON.stringify({ op: 'attached', id: key }));
      // Tell the UI what the agent's streams are actually doing right now.
      const a = key ? agents.get(key) : null;
      if (a) {
        wsSafe(ws, JSON.stringify({
          op: 'shot', on: !!a.streamOn, quality: a.shotQuality,
          maxW: a.shotMaxW, intervalMs: a.shotIntervalMs
        }));
        wsSafe(ws, JSON.stringify({
          op: 'cam', on: !!a.camOn, quality: a.camQuality,
          maxW: a.camMaxW, intervalMs: a.camIntervalMs
        }));
      }
      return;
    }
    if (msg.op === 'in' || msg.op === 'screenshot' ||
        msg.op === 'screenshotOnce' || msg.op === 'camera' ||
        msg.op === 'cameraOnce' || msg.op === 'mic' ||
        msg.op === 'upd' || msg.op === 'kill') {
      if (!op.attachedTo) {
        warn(`operator op${op.id} ${msg.op} with no target`);
        wsSafe(ws, JSON.stringify({ op: 'err', reason: 'no target' }));
        return;
      }
      const a = agents.get(op.attachedTo);
      if (!a) {
        warn(`operator op${op.id} ${msg.op} -> agent ${op.attachedTo} not present`);
        wsSafe(ws, JSON.stringify({ op: 'err', reason: 'agent offline' }));
        return;
      }
      if (!wsSafe(a.ws, JSON.stringify(msg))) {
        warn(`operator op${op.id} ${msg.op} -> agent send FAILED`);
        wsSafe(ws, JSON.stringify({ op: 'err', reason: 'agent send failed' }));
        return;
      }
      stats.cmdsForwarded++;
      if (msg.op === 'screenshot') {
        a.streamOn = msg.on === true;
        if (a.streamOn) {
          if (Number.isFinite(Number(msg.quality)))    a.shotQuality    = Number(msg.quality);
          if (Number.isFinite(Number(msg.maxW)))       a.shotMaxW       = Number(msg.maxW);
          if (Number.isFinite(Number(msg.intervalMs))) a.shotIntervalMs = Number(msg.intervalMs);
        }
        info(`operator op${op.id} -> agent ${a.sid} stream ${a.streamOn ? 'ON' : 'OFF'}` +
             (a.streamOn ? ` q=${a.shotQuality} maxW=${a.shotMaxW || 'native'} iv=${a.shotIntervalMs}ms` : ''));
      } else if (msg.op === 'camera') {
        a.camOn = msg.on === true;
        if (a.camOn) {
          if (Number.isFinite(Number(msg.quality)))    a.camQuality    = Number(msg.quality);
          if (Number.isFinite(Number(msg.maxW)))       a.camMaxW       = Number(msg.maxW);
          if (Number.isFinite(Number(msg.intervalMs))) a.camIntervalMs = Number(msg.intervalMs);
        }
        info(`operator op${op.id} -> agent ${a.sid} camera ${a.camOn ? 'ON' : 'OFF'}` +
             (a.camOn ? ` q=${a.camQuality} maxW=${a.camMaxW || 'native'} iv=${a.camIntervalMs}ms` : ''));
      } else {
        info(`operator op${op.id} -> agent ${a.sid} : ${msg.op}`);
      }
      return;
    }
    warn(`operator op${op.id} unknown op=${msg.op}`);
  });

  ws.on('close', (code, reason) => {
    noteEvent({ ev: 'op-out', op: op.id, code });
    info(`operator DISCONNECT id=op${op.id} code=${code} reason=${reason || '-'} total=${operators.size - 1}`);
    operators.delete(op);
    // Nobody is watching any more: stop the agent from burning bandwidth.
    stopStreamIfIdle(op.attachedTo);
  });
  ws.on('error', (e) => warn(`operator op${op.id} ws error: ${e.message}`));
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
function listAgents() {
  const out = [];
  const seen = new Map();
  for (const a of agents.values()) {
    const n = (seen.get(a.id) || 0) + 1;
    seen.set(a.id, n);
    out.push({
      id: a.sid,                                   // attach target (unique)
      name: n > 1 ? `${a.id}#${n}` : a.id,         // display label
      info: a.info,
      lastSeen: a.lastSeen,
      streaming: !!a.streamOn,
      camStreaming: !!a.camOn
    });
  }
  return out;
}

// Accept both a sid (current UI) and a bare hostname (older cached UI).
function resolveAgent(key) {
  if (!key) return null;
  const k = String(key);
  if (agents.has(k)) return k;
  for (const a of agents.values()) if (a.id === k) return a.sid;
  return null;
}

// Stop an agent's streams when no operator is attached to it any more.
function stopStreamIfIdle(sid) {
  if (!sid) return;
  for (const op of operators) if (op.attachedTo === sid) return;
  const a = agents.get(sid);
  if (!a) return;
  let stopped = false;
  if (a.streamOn &&
      wsSafe(a.ws, JSON.stringify({ op: 'screenshot', on: false }))) {
    a.streamOn = false;
    stopped = true;
  }
  if (a.camOn &&
      wsSafe(a.ws, JSON.stringify({ op: 'camera', on: false }))) {
    a.camOn = false;
    stopped = true;
  }
  if (stopped) {
    stats.forcedStops++;
    info(`streams OFF (no operators watching) agent=${sid} id=${a.id}`);
  }
}

function broadcastAgentList() {
  const payload = JSON.stringify({ op: 'agents', list: listAgents() });
  let n = 0;
  for (const op of operators) if (wsSafe(op.ws, payload)) n++;
  debug(`broadcast agents list to ${n}/${operators.size} operators`);
}

function autoAttachIfLone() {
  if (agents.size !== 1) return;
  const only = [...agents.values()][0];
  for (const op of operators) {
    if (!op.attachedTo) {
      op.attachedTo = only.sid;
      wsSafe(op.ws, JSON.stringify({ op: 'attached', id: only.sid }));
      wsSafe(op.ws, JSON.stringify({
        op: 'shot', on: !!only.streamOn, quality: only.shotQuality,
        maxW: only.shotMaxW, intervalMs: only.shotIntervalMs
      }));
      wsSafe(op.ws, JSON.stringify({
        op: 'cam', on: !!only.camOn, quality: only.camQuality,
        maxW: only.camMaxW, intervalMs: only.camIntervalMs
      }));
      info(`auto-attach operator op${op.id} -> ${only.sid} (${only.id})`);
    }
  }
}

// ---------------------------------------------------------------------------
// Keepalive
//
// An idle agent socket carries no traffic at all. Anything between the agent
// and the relay that drops idle WebSockets — a cloud proxy, a laptop Wi-Fi
// adapter going into power save — then kills the session without telling
// either end, and the device silently disappears from the list. A ping every
// 25s keeps bytes flowing and gives a half-open socket something to fail on,
// so the agent's own 5s reconnect loop can take over.
//
// The agent answers {"op":"pong"} and the relay already accepts that. The
// timeout lives in the reaper below rather than here, so a slow pong costs a
// whole interval of slack before anything is dropped.
// ---------------------------------------------------------------------------
const PING_MS = Number(process.env.RELAY_PING_MS || 25000);
setInterval(() => {
  const ping = JSON.stringify({ op: 'ping' });
  for (const a of agents.values()) wsSafe(a.ws, ping);
  for (const op of operators)      wsSafe(op.ws, ping);
}, PING_MS).unref();

// Reap agents that stop answering. wsSafe cannot detect this on its own: a
// half-open TCP socket still reports readyState OPEN, and send() happily
// buffers into the kernel, so a dead agent would stay in the list forever and
// every command aimed at it would vanish. The agent answers {"op":"pong"}, so
// two silent intervals is a reliable signal. Operators are exempt on purpose:
// the console never answers a ping, so the same rule would drop live tabs.
// The floor is two ping intervals, so a healthy agent can never be reaped
// merely because the timeout was configured shorter than the ping cadence.
const AGENT_PONG_TIMEOUT = Math.max(PING_MS * 2,
  Number(process.env.AGENT_PONG_TIMEOUT_MS || 60000));
setInterval(() => {
  const now = Date.now();
  for (const a of agents.values()) {
    const last = a.lastPong || a.lastSeen || now;
    if (now - last > AGENT_PONG_TIMEOUT) {
      warn(`agent ${a.sid} id=${a.id} silent for ${Math.round((now - last) / 1000)}s - terminating`);
      try { a.ws.terminate(); } catch {}
    }
  }
}, Math.max(1000, Math.min(10000, PING_MS))).unref();

// ---------------------------------------------------------------------------
// Heartbeat
// ---------------------------------------------------------------------------
setInterval(() => {
  info(`heartbeat agents=${agents.size} operators=${operators.size} ` +
       `frames=${stats.framesIn}/${stats.framesForwarded} ` +
       `cam=${stats.camFramesIn}/${stats.camFramesForwarded} ` +
       `outs=${stats.outsIn}/${stats.outsForwarded} ` +
       `pcm=${stats.pcmIn}/${stats.pcmForwarded} ` +
       `logs=${stats.logsIn}/${stats.logsForwarded} ` +
       `cmds=${stats.cmdsForwarded} forcedStops=${stats.forcedStops} drops=${stats.drops}`);
}, 60000).unref();

// ---------------------------------------------------------------------------
// Shutdown
// ---------------------------------------------------------------------------
function shutdown(sig) {
  info(`received ${sig}, shutting down`);
  try { wss.close(); } catch {}
  try { server.close(); } catch {}
  setTimeout(() => process.exit(0), 1000);
}
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT',  () => shutdown('SIGINT'));
process.on('uncaughtException', (e) => { error('UNCAUGHT:', e.stack || e.message); });
process.on('unhandledRejection', (e) => { error('UNHANDLED REJECTION:', e?.stack || String(e)); });

// ---------------------------------------------------------------------------
// Listen
// ---------------------------------------------------------------------------
// A failed bind used to leave the process alive and not listening, which on a
// host like Render means a zombie that never serves anything. Exit instead so
// the platform restarts the service.
server.on('error', (e) => {
  error('server error:', e.message);
  process.exit(1);
});

server.listen(PORT, HOST, () => {
  info(`relay v4.8 LIVE on ${HOST}:${PORT}`);
  info('endpoints:  /health  /debug  /ws  / (console)  /agent.exe  /install-agent.ps1  /diag-agent.ps1');
});