# AGENTS.md — pocketstation-io/relay

## Source of truth

Before editing, read:

1. `docs/architecture/PocketStation-v2.3.md`
2. `docs/REPO_CONTRACT.md`
3. Relevant ADRs in `docs/adr/`
4. The assigned GitHub issue

## Phase status

**Phase 1: COMPLETE as of 2026-05-20. Audit: CONDITIONAL PASS.**

**Phase 2: INTAKE — see `RELAY_PHASE2_QUEUE.md` for the full task list.**

### Phase 1 — What is done

- Full signaling protocol: PUBLISH, SUBSCRIBE, ICE, SDP_ANSWER, LEAVE, ROOM_STATE, ERROR (all 7 types).
- JWT auth: room-scoped HS256 tokens (golang-jwt/v5), source and listener roles, configurable TTL. See ADR-014.
- RTP forwarding: forwardLoop, atomic packet/byte counters, goroutine teardown on error or done signal.
- Room lifecycle: Source/Listener interfaces, Manager with GetOrCreate/Get/Delete, idempotent Close.
- Integration tests: TestGiven_RelayRoom_When_TokenUsedForSignal_Then_Accepted (token flow end-to-end), TestGiven_SourcePublishing_When_ListenerSubscribes_Then_RTPForwarded (RTP E2E).
- Soak test (5 min, race-clean): goroutine delta=-3, RSS growth=0.0%.
- Fake-source binary: `cmd/fake-source/`.
- CI: unit + integration run in under 15 s with `-short`; soak runs only on push to main (ADR-016).

### Phase 1 — Token authority

Relay owns all room creation and JWT issuance in Phase 1. The relay's POST /v1/rooms is the single source of truth for tokens. api-server issues opaque hex tokens that are not valid for relay signaling. See ADR-014. This will change in Phase 2 (api-server upgraded to share the JWT secret).

### Phase 2 — Intake list

See `RELAY_PHASE2_QUEUE.md` for the full table. Summary:

- Copy-on-write listener slice (ADR-005): replace sync.RWMutex per packet with atomic.Pointer (v2.3 §26.2, §15).
- Source reconnect (ICE restart): relay survives source disconnect + reconnect without losing listeners (v2.3 §15).
- Listener reconnect: listener reconnects to an active room without session interruption.
- Rate limiting: max rooms per IP, max listeners per room (v2.3 §9 Phase 2).
- Room expiry: auto-close after N hours of inactivity (v2.3 §9 Phase 2).
- Graceful shutdown: SIGTERM drain — relay stops accepting new rooms, drains active sessions (v2.3 §15).
- SLO instrumentation: session completion, transport latency, source publish success (v2.3 §13.5).
- latency_estimate_ms metric: clock-sync-based per-session latency estimation (ADR-006, v2.3 §13.2).
- api-server JWT compatibility: api-server upgraded to call auth.Sign with shared POCKETSTATION_JWT_SECRET; cross-service integration test required before Phase 2 exit (ADR-014/ADR-015).
- relay→api-server source_active push: relay POSTs source_active event to api-server on source connect/disconnect (v2.3 §12.2).
- Connected WriteRTP bench (ADR-009): measure real Pion WriteRTP allocation against live tracks, not discardListener mock.
- ICE failure / SIGTERM / room-delete failure mode tests (Audit F3).

## Phase gate

This repo activated in **Phase 1**. Phase 1 is complete (2026-05-20, CONDITIONAL PASS). Work now targets **Phase 2**.

If an issue is not listed in RELAY_PHASE2_QUEUE.md, do not implement it here unless the issue has `phase-exception-approved`.

## Rules

- One issue = one branch = one PR.
- Do not edit unrelated repos.
- Do not create `pocketstation-io/protocol` before Phase 2.
- Signaling message types live in this repo until Phase 2 protocol repo creation. Do not extract them early.
- Do not change v2.3 architecture unless explicitly assigned.
- Do not add dependencies without approval.
- Do not bypass CI.

## Hot-path rules (non-negotiable)

- No logging in forwardLoop or any RTP path.
- No global mutable state.
- No locks held during WriteRTP calls (Phase 2 will use copy-on-write per ADR-005).
- No heap allocation on the forwarding hot path beyond what ADR-009 measurement justifies.

## Engineering Standards

Before code changes, every agent must read:

- `docs/standards/STAFF_ENGINEERING_BAR.md`
- `docs/standards/STRUCTURE_NAMING_STYLE_THINKING.md`
- `docs/standards/PRODUCTION_ENGINEERING_BAR.md`
- `docs/REPO_CONTRACT.md`
- relevant ADRs
- current phase progress file
- `FAKE_SCAFFOLD_INVENTORY.md`

All code follows the structure, naming, documentation, test naming,
comment style, and thinking process defined there.

Every non-trivial implementation documents:
- invariant
- ownership model
- failure behavior
- test coverage
- phase scope
- what is intentionally not implemented

Every PR that introduces a fake/mock/scaffold adds a row to
FAKE_SCAFFOLD_INVENTORY.md. Every PR that replaces one burns
the row down.

## Phase 5 intake gates

- [ ] ADR-023 Embedded TURN: pion/turn running in relay process on UDP/3478 + TURNS/TLS/443; ICE-TCP mux on 443; HMAC-SHA1 auth validated
- [ ] ADR-023 Embedded TURN soak: Phase 2 soak (50L × 30min) repeated with TCP-only ICE constraint; goroutine delta and RSS within ±20% of phase2-baseline
- [ ] ADR-014 SFrame E2EE: relay forwards encrypted frames without decrypting; test verifies relay cannot read audio content
- [ ] ADR-021 RTCP adaptive codec: CODEC_HINT sent within 2 RTT of entering a loss tier; ICE restart at > 15% loss
- [ ] ADR-020 Benchmark: /v1/echo endpoint implemented; cmd/benchmark CLI produces P95 ≤ 120ms in-process
- [x] Phase 2 soak target (50 listeners, 30 min) — COMPLETE 2026-06-01; goroutine delta=-1, RSS 13%, 89,435 pkts, 0 drops
- [ ] SFrame key exchange added to signaling protocol as KEY_EXCHANGE message type

## Phase 6 intake gates

- [ ] ADR-016 WebTransport: /v1/wt endpoint serves audio alongside /v1/signal WebRTC
- [ ] Multi-region Fly.io deployment (Phase 6 scaling, §9.5 BuildGuide)
- [ ] RTCP: parse per-listener RR for diarization speaker_id SSE events (ADR-018)