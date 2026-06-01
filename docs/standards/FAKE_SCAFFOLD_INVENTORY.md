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
| In-process token store (relay) | DEFERRED | internal/room/room.go Manager | Persistent store; rooms lost on restart | Phase 3 | DB / Durable Objects deployment infra decision not made; carry to Phase 3 when deployment target is chosen |
| TURN configuration | DEFERRED | cmd/relay-server/main.go | Production TURN credentials; STUN-only fails behind symmetric NAT | Phase 3 | TURN provider decision pending; see docs/architecture/ for open decision |
| SFrame E2EE | DEFERRED | relay + SDKs | Frame-layer encryption per RFC 9605 | Phase 3 | ADR for per-platform insertion point |
| latency_estimate_ms metric | DEFERRED | internal/metrics/metrics.go | Per-packet latency measurement requires clock sync | Phase 3 | ADR-006 (clock sync) deferred to Phase 3 |

---

## Phase 2 burns (completed 2026-05-21)

| Component | Replaced by | Finding |
|---|---|---|
| Pion WriteRTP connected bench | `BenchmarkWriteRTPFanoutConnected_{1,10,50}` in `internal/room/forward_bench_test.go` — real loopback Pion pair, DTLS/SRTP path | SRTP: ~7.7 µs/op, 11 allocs/op at fanout 1 (~297x vs disconnected); linear scaling; within Phase 2 budget. ADR-009 decision: acceptable. See `benches/results/phase2-baseline.txt`. |

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

## Phase 5 additions — status as of 2026-06-01

| Artifact | Status | Location | Replace by | Notes |
|---|---|---|---|---|
| SFrame E2EE relay endpoint | PARTIAL | internal/signaling/messages.go | Phase 5 | ADR-014; KEY_EXCHANGE forwarding done; per-frame test pending |
| RTCP adaptive codec feedback | PENDING | internal/rtcp/ + signaling CODEC_HINT | Phase 5 | ADR-021; signaling type done; RTCP→CODEC_HINT logic not wired |
| Capture-to-cloud benchmark CLI | PENDING | cmd/benchmark/ | Phase 5 | ADR-020; /v1/echo done; cmd/benchmark CLI not yet implemented |

## Phase 5 burns (completed 2026-06-01)

| Artifact | Burned by | Evidence |
|---|---|---|
| Embedded TURN server (pion/turn) | ADR-023 impl | internal/turn/; cmd/relay-server/main.go; 8 tests pass; on main |
| ICE-TCP mux | ADR-023 impl | server.Config.ICETCPMux; ICE_TCP_PORT env var |
| Relay echo endpoint | ADR-020 partial | /v1/echo WebSocket; HTTP test PASS |
| KEY_EXCHANGE signaling type | ADR-014 partial | internal/signaling/messages.go; handleKeyExchange() in server |
| CODEC_HINT signaling type | ADR-021 partial | internal/signaling/messages.go; CodecHintPayload struct |
| BenchmarkWriteRTPFanoutConnected_200 | P5-PROD-002 | internal/room/forward_bench_test.go; result pending local run |
| ICE failure integration tests | P5-PROD-003 | test/integration/ice_failure_test.go; execution pending local run |
| Binary smoke test CI job | P5-PROD-004 | .github/workflows/ci.yml smoke job |
| Phase 2 soak 50L×30min | P2-C1 | soak/results/phase2-baseline.txt; delta=-1, RSS 13%, 0 drops |

## Phase 6 additions (added 2026-05-23)

| Artifact | Status | Location | Replace by | Notes |
|---|---|---|---|---|
| WebTransport endpoint | PENDING | internal/webtransport/ | Phase 6 | ADR-016; /v1/wt; quic-go; ICE-free |
| Multi-region routing | PENDING | Phase 6 scaling | Phase 6 | Fly.io edge nodes; EU/US/APAC |

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
