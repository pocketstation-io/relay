# AGENTS.md — pocketstation-io/relay

## Source of truth

Before editing, read:

1. `docs/architecture/PocketStation-v2.3.md`
2. `docs/REPO_CONTRACT.md`
3. Relevant ADRs in `docs/adr/`
4. The assigned GitHub issue

## Phase status

**Phase 1: COMPLETE as of 2026-05-20. Audit: CONDITIONAL PASS.**

**Phase 2: IN PROGRESS — see `RELAY_PHASE2_QUEUE.md` for the full task list. Core items shipped; SLO instrumentation + latency_estimate_ms still open.**

**Live production relay:** `wss://pocketstation-relay.fly.dev` (3 regions: iad/fra/nrt). Deployed 2026-06-01.

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

- [x] ADR-023 Embedded TURN: pion/turn in relay process; ICE-TCP mux; HMAC-SHA1 auth — COMPLETE 2026-06-01; 8 tests pass; on main
- [x] ADR-023 api-server TURN credential issuance: POST /v1/rooms returns ice_servers — COMPLETE 2026-06-01; 7 tests pass; on main
- [x] Phase 2 soak target (50 listeners, 30 min) — COMPLETE 2026-06-01; delta=-1, RSS 13%, 89,435 pkts, 0 drops
- [x] KEY_EXCHANGE message type added to signaling — COMPLETE 2026-06-01; handleKeyExchange() forwards without decrypting
- [x] CODEC_HINT message type added to signaling — COMPLETE 2026-06-01; CodecHintPayload struct defined
- [x] ADR-020 /v1/echo endpoint implemented — COMPLETE 2026-06-01; HTTP test PASS; WebSocket test BLOCKED [SANDBOX]
- [x] BenchmarkWriteRTPFanoutConnected_200 added to bench file — COMPLETE 2026-06-01; real result verified 2026-06-02: P50=18.9ms P95=21ms 50 subscribers 5000/5000 pkts 0 drops
- [x] ICE failure tests written — COMPLETE 2026-06-01; TestGiven_SourceIceFails + TestGiven_BothIceFail in test/integration/; executed against live relay 2026-06-02
- [x] Binary subprocess smoke test wired in CI — COMPLETE 2026-06-01; smoke job in .github/workflows/ci.yml; running
- [x] Webhook events (session_started/ended/utterance_detected) — COMPLETE 2026-06-02
- [x] Public broadcast channels (GET /v1/channels) — COMPLETE 2026-06-02
- [x] Playwright E2E: 6/6 tests pass against live relay — COMPLETE 2026-06-02
- [x] Multi-region Fly.io deployment (iad/fra/nrt) — COMPLETE 2026-06-01; live at wss://pocketstation-relay.fly.dev
- [ ] ADR-021 RTCP adaptive codec: CODEC_HINT sent within 2 RTT of entering a loss tier; ICE restart at > 15% loss
- [ ] ADR-014 SFrame E2EE: relay-side per-frame bypass test (KEY_EXCHANGE forwarding done; per-frame AES-GCM test pending)

## Phase 6 intake gates

- [ ] ADR-016 WebTransport: /v1/wt endpoint serves audio alongside /v1/signal WebRTC
- [x] Multi-region Fly.io deployment — COMPLETE 2026-06-01 (moved from Phase 6; deployed ahead of schedule)
- [ ] RTCP: parse per-listener RR for diarization speaker_id SSE events (ADR-018)