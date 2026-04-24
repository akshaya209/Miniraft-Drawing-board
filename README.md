# MiniRaft-Final

A correct, fault-tolerant distributed drawing board built on the RAFT consensus protocol.

## Quick Start

```bash
docker-compose up --build
```

Then open:
- **Drawing Board**: http://localhost:3000
- **Gateway status**: http://localhost:4000/status
- **Replica 1 status**: http://localhost:5001/status
- **Replica 2 status**: http://localhost:5002/status
- **Replica 3 status**: http://localhost:5003/status

## Architecture

```
Browser ──WebSocket──▶ Gateway :4000
                           │
              ┌────────────┼────────────┐
              ▼            ▼            ▼
         Replica1:5001  Replica2:5002  Replica3:5003
              │            │            │
              └────────────┴────────────┘
                    RAFT consensus
```

## RAFT Correctness

### Fixed Issues

| Bug | Fix |
|-----|-----|
| `commitIndex` derived from array length | Mutable `commitIndex` integer, separately tracked |
| `matchIndex` incremental delta | Absolute values: `prevLogIndex + entriesSent` |
| No prevLogIndex/prevLogTerm validation | Full consistency check before appending |
| Committed entries could be overwritten | Guard: `if (logIdx <= commitIndex) continue` |
| Majority check missing for commit | `_tryAdvanceCommitIndex()` counts quorum over `matchIndex` |
| Split vote no backoff | `_resetElectionTimer()` with full random range on retry |
| propose() had race conditions | Serialised via `_proposeLock` promise chain |
| sync-log from any replica | Only leader returns data; 503 otherwise |
| leaderCommit ignored by follower | Follower advances `commitIndex = min(leaderCommit, lastLogIndex)` |

### Key Guarantees

- **Leader completeness**: only up-to-date candidates win elections
- **Log matching**: prevLogIndex + prevLogTerm checked on every AppendEntries
- **Commit safety**: only entries from current term are committed (no stale-term commits)
- **No lost strokes**: gateway queues strokes during elections, flushes on new leader

## Fault Tolerance

### Kill a follower
```bash
docker stop replica2
# Draw on canvas — works fine (quorum = 2 of 3)
docker start replica2
# replica2 syncs from leader automatically
```

### Kill the leader
```bash
docker stop replica1   # if replica1 is leader
# Within 500-900ms, replica2 or replica3 wins election
# Gateway re-discovers leader within ~1s
# Strokes drawn during election are queued and flushed
```

## Service Layout

```
MiniRaft-Final/
├── docker-compose.yml
├── gateway/
│   ├── Dockerfile
│   ├── package.json
│   └── server.js          # WebSocket + HTTP gateway
├── replica1/
│   ├── Dockerfile
│   ├── package.json
│   ├── raft.js            # RAFT consensus engine
│   └── server.js          # HTTP server exposing RAFT RPCs
├── replica2/              # identical to replica1
├── replica3/              # identical to replica1
└── frontend/
    ├── Dockerfile         # nginx
    └── index.html         # Canvas drawing app
```
## Screenshots
<img width="1470" height="814" alt="image" src="https://github.com/user-attachments/assets/cc4ef00e-5ea3-4757-8895-4476758f0625" />
## Consistency maintained even when a follower or leader is stopped
<img width="1459" height="730" alt="image" src="https://github.com/user-attachments/assets/7126e0e7-1626-407f-8a8c-63c10b0486bb" />
## Gateway status
<img width="834" height="71" alt="image" src="https://github.com/user-attachments/assets/3e219d4a-16eb-4025-bda3-2b1e334d2aac" />
## 

