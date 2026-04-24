'use strict';
/**
 * raft.js — Correct Mini-RAFT consensus engine
 *
 * Fixes implemented:
 *  - commitIndex is mutable, stored as integer (not derived from array length)
 *  - nextIndex / matchIndex use absolute log indices (1-based)
 *  - AppendEntries validates prevLogIndex + prevLogTerm before appending
 *  - Log entries are never overwritten once committed
 *  - Majority commit: leader counts matchIndex >= N for quorum
 *  - propose() is concurrency-safe via a mutex queue
 *  - Split-vote: randomised backoff before re-election
 *  - sync-log only served by current leader
 *  - Follower advances commitIndex from leaderCommit properly
 */

const axios = require('axios');

const ELECTION_TIMEOUT_MIN = 500;
const ELECTION_TIMEOUT_MAX = 900;
const HEARTBEAT_INTERVAL   = 150;
const GATEWAY_URL          = process.env.GATEWAY_URL || 'http://gateway:4000';

function buildPeers(selfId) {
  const all = [
    { id: 'replica-1', url: 'http://replica1:5001' },
    { id: 'replica-2', url: 'http://replica2:5002' },
    { id: 'replica-3', url: 'http://replica3:5003' },
  ];
  return all.filter(p => p.id !== selfId);
}

class RaftNode {
  constructor(nodeId) {
    this.nodeId      = nodeId;
    this.peers       = buildPeers(nodeId);

    // ── Persistent state (in-memory for demo) ──────────────────────────────────
    this.currentTerm = 0;
    this.votedFor    = null;
    this.log         = [];   // Array of { index, term, data }  (1-based index stored inside)

    // ── Volatile state ─────────────────────────────────────────────────────────
    this.commitIndex = 0;    // index of highest log entry known to be committed
    this.lastApplied = 0;    // index of highest log entry applied to state machine

    // ── Leader volatile state ──────────────────────────────────────────────────
    this.nextIndex   = {};   // peer → next log index to send  (initialised on election)
    this.matchIndex  = {};   // peer → highest log index known replicated on peer

    // ── Role ───────────────────────────────────────────────────────────────────
    this.role        = 'follower';
    this.leaderId    = null;
    this.votes       = 0;

    // ── Concurrency guard for propose() ───────────────────────────────────────
    this._proposeLock = Promise.resolve();

    this._electionTimer  = null;
    this._heartbeatTimer = null;

    this._resetElectionTimer();
    console.log(`[${this.nodeId}] started as follower term=0`);
  }

  // ────────────────────────────────────────────────────────────────────────────
  // HELPERS
  // ────────────────────────────────────────────────────────────────────────────

  _quorum() {
    // majority of (peers + self)
    return Math.floor((this.peers.length + 1) / 2) + 1;
  }

  _lastLogIndex() {
    return this.log.length;   // log is 0-based array, entries have 1-based .index
  }

  _lastLogTerm() {
    if (this.log.length === 0) return 0;
    return this.log[this.log.length - 1].term;
  }

  _entryAt(index) {
    // index is 1-based
    if (index <= 0 || index > this.log.length) return null;
    return this.log[index - 1];
  }

  // ────────────────────────────────────────────────────────────────────────────
  // TIMERS
  // ────────────────────────────────────────────────────────────────────────────

  _randomTimeout() {
    return ELECTION_TIMEOUT_MIN +
      Math.floor(Math.random() * (ELECTION_TIMEOUT_MAX - ELECTION_TIMEOUT_MIN));
  }

  _resetElectionTimer() {
    clearTimeout(this._electionTimer);
    this._electionTimer = setTimeout(() => this._startElection(), this._randomTimeout());
  }

  _stopElectionTimer() {
    clearTimeout(this._electionTimer);
    this._electionTimer = null;
  }

  _startHeartbeatTimer() {
    clearInterval(this._heartbeatTimer);
    this._heartbeatTimer = setInterval(() => this._sendHeartbeats(), HEARTBEAT_INTERVAL);
  }

  _stopHeartbeatTimer() {
    clearInterval(this._heartbeatTimer);
    this._heartbeatTimer = null;
  }

  // ────────────────────────────────────────────────────────────────────────────
  // ELECTIONS
  // ────────────────────────────────────────────────────────────────────────────

  _startElection() {
    this.role        = 'candidate';
    this.currentTerm += 1;
    this.votedFor    = this.nodeId;
    this.votes       = 1;
    this.leaderId    = null;

    console.log(`[${this.nodeId}] ⚡ Election started term=${this.currentTerm}`);

    // Reset election timer for split-vote retry with randomised backoff
    this._resetElectionTimer();

    const req = {
      term:         this.currentTerm,
      candidateId:  this.nodeId,
      lastLogIndex: this._lastLogIndex(),
      lastLogTerm:  this._lastLogTerm(),
    };

    for (const peer of this.peers) {
      axios.post(`${peer.url}/request-vote`, req, { timeout: 500 })
        .then(resp => this._handleVoteResponse(resp.data))
        .catch(() => { /* peer unreachable */ });
    }
  }

  _handleVoteResponse(resp) {
    if (this.role !== 'candidate') return;
    if (resp.term > this.currentTerm) {
      this._becomeFollower(resp.term);
      return;
    }
    if (resp.voteGranted) {
      this.votes++;
      console.log(`[${this.nodeId}] vote received votes=${this.votes}/${this.peers.length + 1}`);
      if (this.votes >= this._quorum()) {
        this._becomeLeader();
      }
    }
  }

  handleRequestVote(req) {
    const { term, candidateId, lastLogIndex, lastLogTerm } = req;

    if (term > this.currentTerm) {
      this._becomeFollower(term);
    }

    let voteGranted = false;

    const termOk   = term >= this.currentTerm;
    const notVoted = this.votedFor === null || this.votedFor === candidateId;
    // Log up-to-date check (§5.4.1)
    const logOk    = lastLogTerm > this._lastLogTerm() ||
                     (lastLogTerm === this._lastLogTerm() && lastLogIndex >= this._lastLogIndex());

    if (termOk && notVoted && logOk) {
      voteGranted   = true;
      this.votedFor = candidateId;
      this._resetElectionTimer();
      console.log(`[${this.nodeId}] voted for ${candidateId} term=${this.currentTerm}`);
    }

    return { term: this.currentTerm, voteGranted };
  }

  // ────────────────────────────────────────────────────────────────────────────
  // STATE TRANSITIONS
  // ────────────────────────────────────────────────────────────────────────────

  _becomeLeader() {
    if (this.role === 'leader') return;
    this.role     = 'leader';
    this.leaderId = this.nodeId;
    this._stopElectionTimer();

    // Initialise nextIndex to lastLogIndex + 1, matchIndex to 0
    for (const peer of this.peers) {
      this.nextIndex[peer.id]  = this._lastLogIndex() + 1;
      this.matchIndex[peer.id] = 0;
    }

    console.log(`[${this.nodeId}] 👑 Became LEADER term=${this.currentTerm} logLen=${this._lastLogIndex()}`);

    // Notify gateway of new leadership first (fast path: unblocks the stroke queue)
    this._notifyGatewayLeader();

    // Re-sync the full committed log to the gateway. The previous leader may have
    // died before sending some /commit callbacks, leaving the gateway's cache
    // incomplete. New clients connecting after failover would receive a stale init
    // log from the gateway. Sending all committed entries ensures the gateway cache
    // is complete. Fire-and-forget; failures are logged but do not block election.
    this._resyncGatewayLog();

    this._startHeartbeatTimer();
    this._sendHeartbeats();
  }

  _becomeFollower(term) {
    const wasLeader = this.role === 'leader';
    this.role        = 'follower';
    this.currentTerm = term;
    this.votedFor    = null;
    this._stopHeartbeatTimer();
    this._resetElectionTimer();
    if (wasLeader) console.log(`[${this.nodeId}] stepped down term=${this.currentTerm}`);
  }

  // ────────────────────────────────────────────────────────────────────────────
  // APPEND ENTRIES — LEADER SIDE
  // ────────────────────────────────────────────────────────────────────────────

  _sendHeartbeats() {
    if (this.role !== 'leader') return;
    for (const peer of this.peers) {
      this._replicateToPeer(peer);
    }
  }

  _buildAppendEntriesReq(peerId) {
    const nextIdx  = this.nextIndex[peerId] || 1;
    const prevIdx  = nextIdx - 1;
    const prevTerm = prevIdx > 0 ? (this._entryAt(prevIdx)?.term || 0) : 0;
    // Send all entries from nextIndex to end of log
    const entries  = this.log.slice(nextIdx - 1);   // log is 0-based array

    return {
      term:         this.currentTerm,
      leaderId:     this.nodeId,
      prevLogIndex: prevIdx,
      prevLogTerm:  prevTerm,
      entries,
      leaderCommit: this.commitIndex,
    };
  }

  _replicateToPeer(peer) {
    const req = this._buildAppendEntriesReq(peer.id);
    axios.post(`${peer.url}/append-entries`, req, { timeout: 500 })
      .then(resp => this._handleAppendResponse(peer, resp.data, req.entries.length, req.prevLogIndex))
      .catch(() => { /* peer unreachable — will retry on next heartbeat */ });
  }

  _handleAppendResponse(peer, resp, entriesSent, prevLogIndex) {
    if (this.role !== 'leader') return;

    if (resp.term > this.currentTerm) {
      this._becomeFollower(resp.term);
      return;
    }

    if (resp.success) {
      if (entriesSent > 0) {
        // matchIndex = prevLogIndex + entriesSent (absolute)
        const newMatch = prevLogIndex + entriesSent;
        if (newMatch > (this.matchIndex[peer.id] || 0)) {
          this.matchIndex[peer.id] = newMatch;
          this.nextIndex[peer.id]  = newMatch + 1;
          console.log(`[${this.nodeId}] peer ${peer.id} matchIndex=${newMatch}`);
          this._tryAdvanceCommitIndex();
        }
      }
    } else {
      // Decrement nextIndex and retry (log inconsistency back-off)
      // Use hint from follower if provided, else decrement by 1
      if (resp.conflictIndex && resp.conflictIndex > 0) {
        this.nextIndex[peer.id] = resp.conflictIndex;
      } else {
        this.nextIndex[peer.id] = Math.max(1, (this.nextIndex[peer.id] || 1) - 1);
      }
      console.log(`[${this.nodeId}] peer ${peer.id} rejected, nextIndex=${this.nextIndex[peer.id]}`);
    }
  }

  /**
   * Advance commitIndex: find the highest N > commitIndex such that
   * a majority of matchIndex[i] >= N AND log[N].term == currentTerm
   */
  _tryAdvanceCommitIndex() {
    if (this.role !== 'leader') return;

    for (let n = this._lastLogIndex(); n > this.commitIndex; n--) {
      const entry = this._entryAt(n);
      if (!entry || entry.term !== this.currentTerm) continue;

      // Count how many nodes (including self) have this entry
      let count = 1;  // self
      for (const peer of this.peers) {
        if ((this.matchIndex[peer.id] || 0) >= n) count++;
      }

      if (count >= this._quorum()) {
        const oldCommit = this.commitIndex;
        this.commitIndex = n;
        console.log(`[${this.nodeId}] ✅ commitIndex advanced ${oldCommit}→${n}`);
        this._applyCommitted();
        break;
      }
    }
  }

  // ────────────────────────────────────────────────────────────────────────────
  // APPEND ENTRIES — FOLLOWER SIDE
  // ────────────────────────────────────────────────────────────────────────────

  handleAppendEntries(req) {
    const { term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit } = req;

    // Rule 1: Reply false if term < currentTerm
    if (term < this.currentTerm) {
      return { term: this.currentTerm, success: false };
    }

    // Valid AppendEntries from current or new leader
    if (term > this.currentTerm || this.role === 'candidate') {
      this._becomeFollower(term);
    }
    this.leaderId = leaderId;
    this._resetElectionTimer();

    // Rule 2: prevLogIndex/prevLogTerm consistency check
    if (prevLogIndex > 0) {
      const prevEntry = this._entryAt(prevLogIndex);
      if (!prevEntry) {
        // We don't have that entry yet
        return {
          term: this.currentTerm,
          success: false,
          conflictIndex: this._lastLogIndex() + 1,
        };
      }
      if (prevEntry.term !== prevLogTerm) {
        // Conflict: find first index of conflicting term for fast back-off
        const conflictTerm = prevEntry.term;
        let conflictIndex  = prevLogIndex;
        for (let i = prevLogIndex - 1; i >= 1; i--) {
          if (this._entryAt(i)?.term !== conflictTerm) {
            conflictIndex = i + 1;
            break;
          }
        }
        return { term: this.currentTerm, success: false, conflictIndex };
      }
    }

    // Rule 3 & 4: Append new entries (delete conflicting, then append)
    if (entries && entries.length > 0) {
      for (let i = 0; i < entries.length; i++) {
        const logIdx = prevLogIndex + 1 + i;  // 1-based index in our log

        // CRITICAL: never truncate committed entries
        if (logIdx <= this.commitIndex) {
          // Already committed — verify it matches (should always match in correct RAFT)
          continue;
        }

        const existing = this._entryAt(logIdx);
        if (existing && existing.term !== entries[i].term) {
          // Conflict at logIdx — truncate from here
          console.log(`[${this.nodeId}] truncating log at index ${logIdx}`);
          this.log = this.log.slice(0, logIdx - 1);
        }

        if (!this._entryAt(logIdx)) {
          this.log.push(entries[i]);
        }
      }
      console.log(`[${this.nodeId}] AppendEntries: appended ${entries.length} entries, logLen=${this.log.length}`);
    }

    // Rule 5: Advance commitIndex
    if (leaderCommit > this.commitIndex) {
      const newCommit = Math.min(leaderCommit, this._lastLogIndex());
      if (newCommit > this.commitIndex) {
        this.commitIndex = newCommit;
        this._applyCommitted();
      }
    }

    return { term: this.currentTerm, success: true };
  }

  // ────────────────────────────────────────────────────────────────────────────
  // STATE MACHINE APPLICATION
  // ────────────────────────────────────────────────────────────────────────────

  _applyCommitted() {
    while (this.lastApplied < this.commitIndex) {
      this.lastApplied++;
      const entry = this._entryAt(this.lastApplied);
      if (entry) {
        console.log(`[${this.nodeId}] applied entry index=${this.lastApplied} term=${entry.term}`);
        // Only leader notifies gateway (to avoid duplicate broadcasts)
        if (this.role === 'leader') {
          this._notifyGateway(entry);
        }
      }
    }
  }

  // ────────────────────────────────────────────────────────────────────────────
  // PROPOSE — concurrency-safe via serialised promise chain
  // ────────────────────────────────────────────────────────────────────────────

  propose(data) {
    // Serialise: each propose waits for the previous to finish
    this._proposeLock = this._proposeLock.then(() => this._doPropose(data));
    return this._proposeLock;
  }

  async _doPropose(data) {
    if (this.role !== 'leader') {
      throw new Error(`Not leader. Current leader: ${this.leaderId || 'unknown'}`);
    }

    const index = this._lastLogIndex() + 1;
    const entry = { index, term: this.currentTerm, data };
    this.log.push(entry);
    console.log(`[${this.nodeId}] proposing index=${index} term=${this.currentTerm}`);

    // Replicate to all peers immediately (in parallel)
    const replicatePromises = this.peers.map(peer => this._replicateEntryToPeer(peer, entry));
    const results = await Promise.allSettled(replicatePromises);

    let acks = 1;  // count self
    for (const r of results) {
      if (r.status === 'fulfilled' && r.value === true) acks++;
    }

    if (acks >= this._quorum()) {
      this.matchIndex[this.nodeId] = index; // self
      this.commitIndex = Math.max(this.commitIndex, index);
      this._applyCommitted();
      console.log(`[${this.nodeId}] ✅ committed index=${index} acks=${acks}/${this.peers.length + 1}`);
      return entry;
    } else {
      // Roll back: remove the uncommitted entry from log
      if (this.log[this.log.length - 1]?.index === index) {
        this.log.pop();
      }
      throw new Error(`Failed to reach quorum: ${acks}/${this.peers.length + 1}`);
    }
  }

  async _replicateEntryToPeer(peer, entry) {
    const prevIdx  = entry.index - 1;
    const prevTerm = prevIdx > 0 ? (this._entryAt(prevIdx)?.term || 0) : 0;
    try {
      const resp = await axios.post(`${peer.url}/append-entries`, {
        term:         this.currentTerm,
        leaderId:     this.nodeId,
        prevLogIndex: prevIdx,
        prevLogTerm:  prevTerm,
        entries:      [entry],
        leaderCommit: this.commitIndex,
      }, { timeout: 1000 });

      if (resp.data.success) {
        this.matchIndex[peer.id] = Math.max(this.matchIndex[peer.id] || 0, entry.index);
        this.nextIndex[peer.id]  = entry.index + 1;
        return true;
      }
      // Handle back-off
      if (resp.data.conflictIndex) {
        this.nextIndex[peer.id] = resp.data.conflictIndex;
      } else {
        this.nextIndex[peer.id] = Math.max(1, entry.index - 1);
      }
      return false;
    } catch {
      return false;
    }
  }

  // ────────────────────────────────────────────────────────────────────────────
  // GATEWAY NOTIFICATIONS
  // ────────────────────────────────────────────────────────────────────────────

  async _notifyGateway(entry) {
    try {
      await axios.post(`${GATEWAY_URL}/commit`, {
        type:  entry.data?.type === 'clear' ? 'clear' : 'committed_stroke',
        stroke: entry.data,
        index:  entry.index,
      }, { timeout: 1000 });
    } catch (e) {
      console.warn(`[${this.nodeId}] gateway notify failed: ${e.message}`);
    }
  }

  async _notifyGatewayLeader() {
    try {
      await axios.post(`${GATEWAY_URL}/leader`, {
        leaderId: this.nodeId,
        term:     this.currentTerm,
      }, { timeout: 1000 });
    } catch { /* gateway may not be up yet */ }
  }

  /**
   * Re-send ALL committed entries to the gateway after a leadership change.
   *
   * Why this is necessary:
   *   The gateway builds its committedStrokes cache purely from /commit callbacks
   *   sent by the leader. If the previous leader crashed mid-flight, some entries
   *   may have been committed (replicated to a majority) but the /commit callback
   *   never reached the gateway. The gateway cache is then incomplete.
   *
   *   When a new client connects, the gateway sends this stale cache in the 'init'
   *   message — the client misses those entries and draws an incorrect canvas state.
   *
   *   The fix: new leader sends a /resync-log request with all committed entries.
   *   The gateway replaces its cache from this authoritative source.
   */
  async _resyncGatewayLog() {
    if (this.commitIndex === 0) return;
    const committedEntries = this.log
      .slice(0, this.commitIndex)
      .map(e => e.data)
      .filter(d => d && d.type !== 'clear');
    try {
      await axios.post(`${GATEWAY_URL}/resync-log`, {
        strokes:  committedEntries,
        term:     this.currentTerm,
        leaderId: this.nodeId,
      }, { timeout: 2000 });
      console.log(`[${this.nodeId}] gateway log resynced: ${committedEntries.length} committed strokes`);
    } catch (e) {
      console.warn(`[${this.nodeId}] gateway resync failed: ${e.message}`);
    }
  }

  // ────────────────────────────────────────────────────────────────────────────
  // SYNC-LOG — catch-up for restarted nodes (only leader responds)
  // ────────────────────────────────────────────────────────────────────────────

  syncLogResponse() {
    // Only leader should respond with authoritative log
    if (this.role !== 'leader') return null;
    return {
      entries:     this.log,
      commitIndex: this.commitIndex,
      term:        this.currentTerm,
      leaderId:    this.nodeId,
    };
  }

  async syncFromLeader() {
    for (const peer of this.peers) {
      try {
        const resp = await axios.get(`${peer.url}/sync-log`, { timeout: 2000 });
        if (resp.data && resp.data.entries && resp.data.entries.length > 0) {
          this.log         = resp.data.entries.slice();
          this.commitIndex = resp.data.commitIndex || 0;
          this.lastApplied = this.commitIndex;
          this.currentTerm = Math.max(this.currentTerm, resp.data.term || 0);
          console.log(`[${this.nodeId}] 🔄 synced ${this.log.length} entries from ${peer.id} commitIndex=${this.commitIndex}`);
          return true;
        }
      } catch { /* try next peer */ }
    }
    return false;
  }

  // ────────────────────────────────────────────────────────────────────────────
  // STATUS
  // ────────────────────────────────────────────────────────────────────────────

  statusSnapshot() {
    return {
      id:          this.nodeId,
      role:        this.role,
      term:        this.currentTerm,
      leaderId:    this.leaderId,
      commitIndex: this.commitIndex,
      logLength:   this._lastLogIndex(),
      votedFor:    this.votedFor,
    };
  }

  // Committed entries as strokes (for /log endpoint)
  committedStrokes() {
    return this.log
      .slice(0, this.commitIndex)
      .map(e => e.data)
      .filter(d => d && d.type !== 'clear');
  }
}

module.exports = RaftNode;
