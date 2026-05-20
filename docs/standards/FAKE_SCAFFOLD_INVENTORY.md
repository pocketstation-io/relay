# Fake / Scaffold Inventory — relay

This file lists every component in this repo that is currently mocked, stubbed, hardcoded, deferred, or otherwise not production-grade.

**It is a living document.** Every PR that adds a scaffold appends a row. Every PR that replaces a scaffold burns the row down (delete the row in the same PR that replaces it).

**Rule:** if a component is fake but not in this file, the PR that added it failed the production bar. Reviewers block PRs that introduce un-inventoried fakes.

---

## Status column meaning

```
SCAFFOLD    Empty placeholder, returns Default::default() or similar
MOCK        Functional fake — tests pass against it, real impl absent
STUB        Throws unimplemented! or returns hardcoded value
PARTIAL     Real implementation, missing significant behavior
DEFERRED    Intentionally postponed; ADR or phase plan justifies it
```

---

## Active inventory

| Component | Status | File | What's missing | Replace by | Blocked on |
|---|---|---|---|---|---|
| fake-source Opus payload | MOCK | relay/cmd/fake-source/main.go | Real libopus encoding; current repeats 0xAB byte | Phase 3 | libopus dependency + audio-core integration |
| In-process token store (relay) | MOCK | internal/room/room.go Manager | Persistent store; rooms lost on restart | Phase 2 | DB dependency decision |
| Pion WriteRTP connected bench | PARTIAL | internal/room/forward_bench_test.go | Bench with connected Pion pair; current uses disconnected track (0 alloc pre-send path, not SRTP path) | Phase 2 | Stable bench harness for Pion pair setup |
| TURN configuration | DEFERRED | cmd/relay-server/main.go | Production TURN credentials; STUN-only fails behind symmetric NAT | Phase 2 | TURN provider decision |
| SFrame E2EE | DEFERRED | relay + SDKs | Frame-layer encryption per RFC 9605 | Phase 3 | ADR for per-platform insertion point |
| latency_estimate_ms metric | PARTIAL | internal/metrics/metrics.go | Per-packet latency measurement requires clock sync (ADR-006) | Phase 2 | ADR-006 resolution |

---

## Phase 1 burns (completed 2026-05-20)

These Phase 1 rows from the pre-hardening inventory have been replaced and burned:

| Component | Replaced by | PR / task |
|---|---|---|
| Fake-source publisher (_to add_) | `cmd/fake-source/main.go` — real Pion WebRTC PUBLISH with synthetic Opus RTP | P1-PROD-003 |
| Token authority (_to add_) | relay owns Phase 1 room creation; relay `/v1/rooms` issues JWTs accepted by relay `/v1/signal`; documented in `README.md` | P1-PROD-002 |

---

## Permanent (intentional) scaffolds

These never become production — they exist for testing and development.

| Component | File | Purpose |
|---|---|---|
| discardListener bench mock | internal/room/forward_bench_test.go | Isolates room dispatch from Pion WriteRTP allocation |
| loopback Pion API | test/integration/relay_test.go | CI-safe WebRTC testing without real network |
| soak loopback setup | test/soak/soak_test.go | Long-running CI-safe soak without external STUN |

---

## How to use this file in a PR

When introducing a scaffold:
1. Add the row before the code lands.
2. Be specific about "what's missing" — "real implementation" is not enough.
3. Pick a "replace by" phase. If it's unknown, mark `DEFERRED` and link the ADR or issue tracking the decision.

When replacing a scaffold:
1. Delete the row in the same PR that lands the real implementation.
2. The PR description references the row being removed.

When reviewing:
1. Block any PR that introduces a fake component without adding to the table.
2. Block any PR that claims to "complete" a scaffold but doesn't burn down the row.
3. Block phase exit if the table has rows whose "replace by" matches the current phase.
