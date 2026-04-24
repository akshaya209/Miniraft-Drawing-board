'use strict';
/**
 * Gateway server — FIXED
 *
 * Root causes fixed:
 *
 * BUG 1 (PRIMARY): discoverLeader() trusted a follower's stale .leaderId field,
 *   pointing currentLeaderUrl at the dead old leader. Strokes silently failed.
 *   FIX: Only trust nodes that SELF-REPORT role === 'leader'. Poll all replicas
 *        in parallel so a dead replica timeout does not block discovery.
 *
 * BUG 2: flushQueue() silently discarded strokes when the flush failed.
 *   FIX: Re-queue failed strokes at front so they are retried on next leader.
 *
 * BUG 3: forwardToLeader() on failure just threw without clearing the stale
 *   currentLeaderUrl, so every subsequent stroke kept hitting the dead URL.
 *   FIX: On any forward failure, clear currentLeaderUrl and trigger immediate
 *        re-poll. Stroke is re-queued, not dropped.
 *
 * BUG 4: Poll interval was 1000ms and sequential (800ms timeout x3 = 2.4s worst).
 *   FIX: Parallel polling + 500ms interval.
 */

const express   = require('express');
const http      = require('http');
const WebSocket = require('ws');
const axios     = require('axios');

const PORT = parseInt(process.env.PORT, 10) || 4000;

const REPLICAS = [
  { id: 'replica-1', url: 'http://replica1:5001' },
  { id: 'replica-2', url: 'http://replica2:5002' },
  { id: 'replica-3', url: 'http://replica3:5003' },
];

const LEADER_POLL_INTERVAL = 500;
const MAX_QUEUE_SIZE       = 200;

const app    = express();
const server = http.createServer(app);
const wss    = new WebSocket.Server({ server });

app.use(express.json({ limit: '2mb' }));
app.use((req, res, next) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.sendStatus(204);
  next();
});

// ── State ──────────────────────────────────────────────────────────────────────

let currentLeaderUrl  = null;
let currentLeaderId   = null;
let currentTerm       = 0;
let strokeQueue       = [];
let committedStrokes  = [];
let clients           = new Set();
let pollInFlight      = false;

// ── Utilities ──────────────────────────────────────────────────────────────────

function broadcast(msg) {
  const json = JSON.stringify(msg);
  for (const ws of clients) {
    if (ws.readyState === WebSocket.OPEN) ws.send(json);
  }
}

function broadcastStatus() {
  broadcast({ type: 'cluster_status', leaderId: currentLeaderId, term: currentTerm });
}

function clearLeader(reason) {
  if (currentLeaderId) {
    console.log(`[gateway] leader cleared (${reason})`);
    currentLeaderUrl = null;
    currentLeaderId  = null;
    broadcastStatus();
  }
}

// ── Leader discovery ───────────────────────────────────────────────────────────
//
// FIXED: Poll ALL replicas in PARALLEL. Only trust a replica that SELF-REPORTS
// role === 'leader'. Never trust a follower's .leaderId hint — that field is
// not cleared atomically after a crash and will point to the dead node for up
// to one full election timeout window.

async function discoverLeader() {
  if (pollInFlight) return;
  pollInFlight = true;
  try {
    const results = await Promise.allSettled(
      REPLICAS.map(async (replica) => {
        const resp = await axios.get(`${replica.url}/status`, { timeout: 800 });
        return { replica, data: resp.data };
      })
    );

    let newLeaderUrl = null;
    let newLeaderId  = null;
    let newTerm      = -1;

    for (const r of results) {
      if (r.status !== 'fulfilled') continue;
      const { replica, data } = r.value;
      // Only accept a replica that claims to BE the leader
      if (data.role === 'leader' && data.term >= newTerm) {
        newLeaderUrl = replica.url;
        newLeaderId  = data.id;
        newTerm      = data.term;
      }
    }

    if (newLeaderId) {
      if (newLeaderId !== currentLeaderId || newTerm > currentTerm) {
        console.log(`[gateway] leader found: ${newLeaderId} term=${newTerm}`);
        currentLeaderUrl = newLeaderUrl;
        currentLeaderId  = newLeaderId;
        currentTerm      = newTerm;
        broadcastStatus();
        flushQueue();
      }
    } else {
      clearLeader('no self-reporting leader found in poll');
    }
  } finally {
    pollInFlight = false;
  }
}

setInterval(discoverLeader, LEADER_POLL_INTERVAL);
discoverLeader();

// ── Forward to leader ──────────────────────────────────────────────────────────
//
// FIXED: On any network failure, immediately clear currentLeaderUrl and trigger
// a re-poll. The caller is responsible for re-queuing the payload.

async function forwardToLeader(payload) {
  if (!currentLeaderUrl) {
    return { queued: true };
  }

  const path = payload.type === 'clear' ? '/clear' : '/stroke';
  const url  = `${currentLeaderUrl}${path}`;

  try {
    const resp = await axios.post(url, payload, { timeout: 3000 });
    return resp.data;
  } catch (e) {
    const status = e.response?.status;
    const body   = e.response?.data;

    // 503 with a leaderId hint: the replica is alive but not the leader
    if (status === 503 && body?.leaderId) {
      const hint = REPLICAS.find(r => r.id === body.leaderId);
      if (hint) {
        console.log(`[gateway] leader hint from replica: ${body.leaderId}`);
        currentLeaderUrl = hint.url;
        currentLeaderId  = body.leaderId;
        broadcastStatus();
        try {
          const hintUrl = `${hint.url}${path}`;
          const resp2 = await axios.post(hintUrl, payload, { timeout: 3000 });
          return resp2.data;
        } catch (e2) {
          console.warn(`[gateway] hint leader also failed: ${e2.message}`);
        }
      }
    }

    // Any failure: clear stale leader and trigger immediate async re-poll
    console.warn(`[gateway] forward to ${currentLeaderId} failed: ${e.message}`);
    clearLeader('forward failure');
    discoverLeader();  // fire-and-forget re-poll
    throw e;
  }
}

// ── Queue flush ────────────────────────────────────────────────────────────────
//
// FIXED: On failure, restore the item to the front of the queue and stop.
// Previously the entire queue was splice(0)'d before the try/catch, so failed
// items were permanently lost.

async function flushQueue() {
  if (!currentLeaderUrl || strokeQueue.length === 0) return;
  console.log(`[gateway] flushing ${strokeQueue.length} queued strokes`);

  while (strokeQueue.length > 0 && currentLeaderUrl) {
    const item = strokeQueue.shift();
    try {
      await forwardToLeader(item.payload);
    } catch (e) {
      console.warn(`[gateway] flush item failed — re-queuing: ${e.message}`);
      strokeQueue.unshift(item);  // restore to front, order preserved
      break;
    }
  }
}

// ── WebSocket handling ─────────────────────────────────────────────────────────

wss.on('connection', (ws) => {
  clients.add(ws);
  console.log(`[gateway] client connected total=${clients.size}`);

  ws.send(JSON.stringify({
    type:     'init',
    strokes:  committedStrokes,
    leaderId: currentLeaderId,
    term:     currentTerm,
  }));

  ws.on('message', async (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    // ── Stroke ──────────────────────────────────────────────────────────────
    if (msg.type === 'stroke') {
      const payload = { ...msg.data };

      if (!currentLeaderUrl) {
        if (strokeQueue.length < MAX_QUEUE_SIZE) strokeQueue.push({ payload });
        ws.send(JSON.stringify({ type: 'queued', id: msg.id }));
        return;
      }

      try {
        const result = await forwardToLeader(payload);
        ws.send(JSON.stringify({ type: 'ack', id: msg.id, index: result.index }));
      } catch (e) {
        // FIXED: re-queue instead of dropping
        if (strokeQueue.length < MAX_QUEUE_SIZE) strokeQueue.push({ payload });
        ws.send(JSON.stringify({ type: 'queued', id: msg.id }));
      }
      return;
    }

    // ── Clear ───────────────────────────────────────────────────────────────
    if (msg.type === 'clear') {
      const payload = { type: 'clear' };

      if (!currentLeaderUrl) {
        if (strokeQueue.length < MAX_QUEUE_SIZE) strokeQueue.push({ payload });
        ws.send(JSON.stringify({ type: 'queued', id: msg.id }));
        return;
      }

      try {
        await forwardToLeader(payload);
        ws.send(JSON.stringify({ type: 'ack', id: msg.id }));
      } catch (e) {
        if (strokeQueue.length < MAX_QUEUE_SIZE) strokeQueue.push({ payload });
        ws.send(JSON.stringify({ type: 'queued', id: msg.id }));
      }
      return;
    }
  });

  ws.on('close', () => {
    clients.delete(ws);
    console.log(`[gateway] client disconnected total=${clients.size}`);
  });

  ws.on('error', (err) => {
    console.error(`[gateway] ws error: ${err.message}`);
    clients.delete(ws);
  });
});

// ── Replica callbacks ──────────────────────────────────────────────────────────

/**
 * POST /commit — called by the leader when an entry is committed.
 */
app.post('/commit', (req, res) => {
  const { type, stroke, index } = req.body;
  res.sendStatus(200);

  if (type === 'clear') {
    committedStrokes = [];
    broadcast({ type: 'clear' });
    console.log('[gateway] broadcast clear');
    return;
  }

  if (type === 'committed_stroke' && stroke) {
    if (index && committedStrokes.some(s => s._index === index)) return;
    const enriched = { ...stroke, _index: index };
    committedStrokes.push(enriched);
    broadcast({ type: 'stroke', data: stroke, index });
    console.log(`[gateway] broadcast stroke index=${index} clients=${clients.size}`);
  }
});

/**
 * POST /leader — new leader announces itself immediately on election.
 * Provides faster detection than waiting for the next poll cycle.
 */
app.post('/leader', (req, res) => {
  const { leaderId, term } = req.body;
  res.sendStatus(200);

  if (!leaderId || term < currentTerm) return;

  const leaderReplica = REPLICAS.find(r => r.id === leaderId);
  if (!leaderReplica) return;

  if (leaderId !== currentLeaderId || term > currentTerm) {
    console.log(`[gateway] new leader announced: ${leaderId} term=${term}`);
    currentLeaderUrl = leaderReplica.url;
    currentLeaderId  = leaderId;
    currentTerm      = term;
    broadcastStatus();
    flushQueue();
  }
});

/**
 * POST /resync-log — called by a newly elected leader to replace the gateway's
 * committed stroke cache. This handles the case where the previous leader died
 * before sending /commit callbacks for some entries, leaving the gateway cache
 * incomplete. Without this, new clients connecting after failover get a stale
 * canvas init state.
 */
app.post('/resync-log', (req, res) => {
  const { strokes, term, leaderId } = req.body;
  res.sendStatus(200);

  if (!Array.isArray(strokes)) return;
  if (term < currentTerm) return;  // reject stale resync from old term

  console.log(`[gateway] log resynced from ${leaderId} term=${term}: ${strokes.length} strokes`);
  // Rebuild the committed strokes cache from the authoritative leader log.
  // Preserve existing _index metadata if present, add sequential indices otherwise.
  committedStrokes = strokes.map((s, i) => ({ ...s, _index: s._index || (i + 1) }));
});

/**
 * GET /status — gateway health.
 */
app.get('/status', (req, res) => {
  res.json({
    leaderId:    currentLeaderId,
    leaderUrl:   currentLeaderUrl,
    term:        currentTerm,
    clients:     clients.size,
    queuedItems: strokeQueue.length,
    logLength:   committedStrokes.length,
  });
});

// ── Boot ───────────────────────────────────────────────────────────────────────

server.listen(PORT, () => {
  console.log(`[gateway] listening on :${PORT}`);
});
