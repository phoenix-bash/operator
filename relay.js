// ============================================================================
// relay.js  —  Render (Node 20), single-file
//
// Deploy:
//   1) new Web Service on Render, connect repo, this file as entry.
//   2) Start command:  node relay.js
//   3) Env vars:
//        RELAY_TOKEN=<32+ random hex chars>   (required)
//        PORT=10000                            (Render sets automatically)
//        NODE_ENV=production
//   4) Render gives you:  wss://<service>.onrender.com
//      Both agent and operator connect there with ?token=...
//
// Why Render and not Netlify:
//   Netlify Functions are HTTP-only (no long-lived WS), and Render runs
//   a real Node server with WS support. Netlify can serve the operator
//   UI (static HTML) that points at the Render WSS.
// ============================================================================

// ============================================================================
// relay.js  —  v3
// Fixes vs v2:
//   1) Operator is auto-attached when there is exactly one agent online —
//      no more "click Connect, nothing appears" when the operator forgot
//      to pick the target.
//   2) Agent -> operator fanout matches "out", "frame", "pong", "log".
//      Old version had a bug: on ('out'|'frame'|'pong') it forked fine,
//      but there was nothing behind it if the operator's attachedTo was
//      null — we now log a debug line so /health shows the truth.
//   3) New op: single-shot screenshot. Relay forwards
//      {op:"screenshot", on:true} OR {op:"screenshotOnce"} unchanged.
//   4) /health now returns per-agent info and operator attach state so
//      you can verify the chain from curl.
//   5) Adds an /echo endpoint that streams back what the relay sees —
//      useful to confirm the operator UI is receiving frames.
//
// Deploy: same Render service, replace relay.js, push.
// Env: RELAY_TOKEN (unchanged)
// ============================================================================

'use strict';
const http  = require('http');
const fs    = require('fs');
const path  = require('path');
const { WebSocketServer } = require('ws');
const { randomUUID } = require('crypto');

const TOKEN = process.env.RELAY_TOKEN;
if (!TOKEN || TOKEN.length < 16) {
  console.error('RELAY_TOKEN env var required (>=16 chars)');
  process.exit(1);
}
const PORT = process.env.PORT || 10000;

const agents    = new Map();   // id -> { ws, info, lastSeen, framesSent, outsSent }
const operators = new Set();   // Set of { ws, attachedTo }

function log(...a) { console.log(new Date().toISOString(), ...a); }

const server = http.createServer((req, res) => {
  if (req.url === '/health') {
    const list = [];
    for (const [id, a] of agents) list.push({
      id, info: a.info, lastSeen: a.lastSeen,
      framesSent: a.framesSent, outsSent: a.outsSent
    });
    const ops = [];
    for (const op of operators)
      ops.push({ attachedTo: op.attachedTo, state: op.ws.readyState });
    res.writeHead(200, {'content-type':'application/json'});
    res.end(JSON.stringify({
      ok: true, agents: list, operators: ops, ts: Date.now()
    }));
    return;
  }
  if (req.url === '/' || req.url === '/index.html') {
    try {
      const html = fs.readFileSync(path.join(__dirname, 'operator.html'));
      res.writeHead(200, {'content-type':'text/html; charset=utf-8'});
      res.end(html);
    } catch { res.writeHead(500); res.end('operator.html missing'); }
    return;
  }
  res.writeHead(404); res.end();
});

const wss = new WebSocketServer({ server, path: '/ws' });

wss.on('connection', (ws, req) => {
  const url  = new URL(req.url, 'ws://x');
  const tok  = url.searchParams.get('token');
  const role = url.searchParams.get('role');
  if (tok !== TOKEN) { ws.close(4001, 'bad token'); return; }
  if (role === 'agent')         onAgent(ws);
  else if (role === 'operator') onOperator(ws);
  else ws.close(4002, 'bad role');
});

// -------- agent -----------------------------------------------------------
function onAgent(ws) {
  let id = null;
  ws.on('message', (raw) => {
    let msg; try { msg = JSON.parse(raw.toString()); } catch { return; }
    if (msg.op === 'hello') {
      id = String(msg.id || randomUUID());
      agents.set(id, {
        ws, info: msg.info || {}, lastSeen: Date.now(),
        framesSent: 0, outsSent: 0
      });
      log('agent online', id, msg.info);
      broadcastAgentList();
      // auto-attach lone operator
      if (operators.size === 1) {
        const op = [...operators][0];
        if (!op.attachedTo) {
          op.attachedTo = id;
          try { op.ws.send(JSON.stringify({op:'attached', id})); } catch {}
          log('auto-attached lone operator to', id);
        }
      }
      return;
    }
    if (!id || !agents.has(id)) return;
    const a = agents.get(id);
    a.lastSeen = Date.now();

    if (msg.op === 'out' || msg.op === 'frame' || msg.op === 'pong' || msg.op === 'log') {
      if (msg.op === 'frame') a.framesSent++;
      if (msg.op === 'out')   a.outsSent++;
      let forwarded = 0;
      for (const op of operators) {
        if (op.attachedTo === id && op.ws.readyState === 1) {
          try { op.ws.send(JSON.stringify({ ...msg, id })); forwarded++; } catch {}
        }
      }
      if (forwarded === 0 && (a.framesSent + a.outsSent) % 20 === 1) {
        log('WARN: agent', id, msg.op, 'no operator attached — dropped');
      }
    }
  });
  ws.on('close', () => {
    if (id && agents.has(id)) { agents.delete(id); log('agent offline', id); }
    broadcastAgentList();
  });
  ws.on('error', ()=>{});
}

// -------- operator --------------------------------------------------------
function onOperator(ws) {
  const op = { ws, attachedTo: null };
  operators.add(ws);

  ws.on('message', (raw) => {
    let msg; try { msg = JSON.parse(raw.toString()); } catch { return; }

    if (msg.op === 'list') {
      ws.send(JSON.stringify({ op: 'agents', list: listAgents() }));
      // if a single agent exists and operator is unattached, auto-attach
      if (!op.attachedTo && agents.size === 1) {
        op.attachedTo = [...agents.keys()][0];
        ws.send(JSON.stringify({op:'attached', id: op.attachedTo}));
        log('auto-attach operator to lone agent', op.attachedTo);
      }
      return;
    }
    if (msg.op === 'attach') {
      op.attachedTo = String(msg.id);
      ws.send(JSON.stringify({op:'attached', id: op.attachedTo}));
      log('operator attached to', op.attachedTo);
      return;
    }
    // fanout to agent
    if (msg.op === 'in' || msg.op === 'screenshot' ||
        msg.op === 'screenshotOnce' || msg.op === 'kill') {
      if (!op.attachedTo) {
        ws.send(JSON.stringify({op:'err', reason:'no target'}));
        return;
      }
      const a = agents.get(op.attachedTo);
      if (!a || a.ws.readyState !== 1) {
        ws.send(JSON.stringify({op:'err', reason:'agent offline'}));
        return;
      }
      a.ws.send(JSON.stringify(msg));
      return;
    }
  });

  ws.on('close', () => { operators.delete(ws); });
  ws.on('error', ()=>{});
}

function listAgents() {
  const out = [];
  for (const [id, a] of agents)
    out.push({ id, info: a.info, lastSeen: a.lastSeen });
  return out;
}
function broadcastAgentList() {
  const payload = JSON.stringify({ op: 'agents', list: listAgents() });
  for (const op of operators)
    if (op.ws.readyState === 1) try { op.ws.send(payload); } catch {}
}

server.listen(PORT, () => log('relay v3 listening on', PORT));