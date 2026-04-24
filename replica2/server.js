'use strict';
/**
 * Replica server — exposes RAFT RPC and drawing command endpoints.
 */

const express  = require('express');
const RaftNode = require('./raft');

const NODE_ID = process.env.NODE_ID;
const PORT    = parseInt(process.env.PORT, 10);

if (!NODE_ID || !PORT) {
  console.error('NODE_ID and PORT env vars are required');
  process.exit(1);
}

const app  = express();
const raft = new RaftNode(NODE_ID);

app.use((req, res, next) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.sendStatus(204);
  next();
});
app.use(express.json({ limit: '2mb' }));

// ── RAFT RPCs ──────────────────────────────────────────────────────────────────

app.post('/request-vote', (req, res) => {
  const result = raft.handleRequestVote(req.body);
  res.json(result);
});

app.post('/append-entries', (req, res) => {
  const result = raft.handleAppendEntries(req.body);
  res.json(result);
});

// ── SYNC-LOG — only leader responds with authoritative data ───────────────────

app.get('/sync-log', (req, res) => {
  const data = raft.syncLogResponse();
  if (!data) {
    return res.status(503).json({ error: 'not the leader' });
  }
  res.json(data);
});

// ── STATUS & LOG ───────────────────────────────────────────────────────────────

app.get('/status', (req, res) => {
  res.json(raft.statusSnapshot());
});

app.get('/log', (req, res) => {
  res.json({ strokes: raft.committedStrokes() });
});

// ── DRAWING COMMANDS (accepted by leader, forwarded by gateway) ───────────────

app.post('/stroke', async (req, res) => {
  const stroke = req.body;
  if (!stroke || !stroke.points) {
    return res.status(400).json({ error: 'Invalid stroke data' });
  }
  try {
    const entry = await raft.propose(stroke);
    res.json({ ok: true, index: entry.index });
  } catch (e) {
    console.warn(`[${NODE_ID}] /stroke failed: ${e.message}`);
    res.status(503).json({ error: e.message, leaderId: raft.leaderId });
  }
});

app.post('/clear', async (req, res) => {
  try {
    const entry = await raft.propose({ type: 'clear' });
    res.json({ ok: true, index: entry.index });
  } catch (e) {
    console.warn(`[${NODE_ID}] /clear failed: ${e.message}`);
    res.status(503).json({ error: e.message, leaderId: raft.leaderId });
  }
});

// ── STARTUP ────────────────────────────────────────────────────────────────────

setTimeout(async () => {
  console.log(`[${NODE_ID}] Attempting initial log sync from leader…`);
  const synced = await raft.syncFromLeader();
  if (!synced) {
    console.log(`[${NODE_ID}] No leader found for sync — starting fresh`);
  }
}, 2000);

app.listen(PORT, () => {
  console.log(`[${NODE_ID}] listening on :${PORT}`);
});
