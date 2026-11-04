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

'use strict';
const http  = require('http');
const { WebSocketServer } = require('ws');
const { randomUUID } = require('crypto');

const TOKEN = process.env.RELAY_TOKEN;
if (!TOKEN || TOKEN.length < 16) {
  console.error('RELAY_TOKEN env var required (>=16 chars)');
  process.exit(1);
}
const PORT = process.env.PORT || 10000;

// -------- in-memory state -------------------------------------------------
// agents:      id -> { ws, info, lastSeen }
// operators:   ws -> { ws, attachedTo }
const agents    = new Map();
const operators = new Set();

function log(...a) { console.log(new Date().toISOString(), ...a); }

// -------- HTTP server (health + operator static) --------------------------
const server = http.createServer((req, res) => {
  if (req.url === '/' || req.url === '/health') {
    res.writeHead(200, {'content-type':'application/json'});
    res.end(JSON.stringify({
      ok: true, agents: agents.size, operators: operators.size,
      ts: Date.now()
    }));
    return;
  }
  res.writeHead(404); res.end();
});

// -------- WSS -------------------------------------------------------------
const wss = new WebSocketServer({ server, path: '/ws' });

wss.on('connection', (ws, req) => {
  const url  = new URL(req.url, 'ws://x');
  const tok  = url.searchParams.get('token');
  const role = url.searchParams.get('role'); // "agent" | "operator"

  if (tok !== TOKEN) {
    ws.close(4001, 'bad token');
    return;
  }
  if (role === 'agent')    onAgent(ws);
  else if (role === 'operator') onOperator(ws);
  else ws.close(4002, 'bad role');
});

// -------- agent side ------------------------------------------------------
function onAgent(ws) {
  let id = null;

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch { return; }

    if (msg.op === 'hello') {
      id = String(msg.id || randomUUID());
      agents.set(id, { ws, info: msg.info || {}, lastSeen: Date.now() });
      log('agent online', id, msg.info);
      broadcastAgentList();
      return;
    }
    if (!id || !agents.has(id)) return;
    agents.get(id).lastSeen = Date.now();

    // Forward to any operator attached to this id
    if (msg.op === 'out' || msg.op === 'frame' || msg.op === 'pong') {
      for (const op of operators) {
        if (op.attachedTo === id && op.ws.readyState === 1) {
          op.ws.send(JSON.stringify({ ...msg, id }));
        }
      }
    }
  });

  ws.on('close', () => {
    if (id && agents.has(id)) { agents.delete(id); log('agent offline', id); }
    broadcastAgentList();
  });
  ws.on('error', () => {});
}

// -------- operator side ---------------------------------------------------
function onOperator(ws) {
  const op = { ws, attachedTo: null };
  operators.add(ws);

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch { return; }

    if (msg.op === 'list') {
      ws.send(JSON.stringify({ op: 'agents', list: listAgents() }));
      return;
    }
    if (msg.op === 'attach') {
      op.attachedTo = String(msg.id);
      log('operator attached', op.attachedTo);
      return;
    }
    if (msg.op === 'in' || msg.op === 'screenshot' || msg.op === 'kill') {
      if (!op.attachedTo) return;
      const a = agents.get(op.attachedTo);
      if (!a || a.ws.readyState !== 1) return;
      a.ws.send(JSON.stringify(msg));
      return;
    }
  });

  ws.on('close', () => { operators.delete(ws); });
  ws.on('error', () => {});
}

function listAgents() {
  const out = [];
  for (const [id, a] of agents) {
    out.push({ id, info: a.info, lastSeen: a.lastSeen });
  }
  return out;
}
function broadcastAgentList() {
  const payload = JSON.stringify({ op: 'agents', list: listAgents() });
  for (const op of operators) {
    if (op.ws.readyState === 1) op.ws.send(payload);
  }
}

server.listen(PORT, () => log('relay listening on', PORT));