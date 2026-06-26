# RELAY_PHASE2_QUEUE.md

Phase 2 intake list for `pocketstation-io/relay`. All items are derived from v2.3 §15 Phase 2 deliverables and audit follow-ups. No item here is in scope for Phase 1.

Before picking up any item, read AGENTS.md, the relevant ADR, and the assigned issue.

---

| Task | Maps to v2.3 | ADR | Priority | Blocked on | Status |
|---|---|---|---|---|---|
| Copy-on-write listener slice | §26.2, §15 Phase 2 | RELAY-005 | P1 | none | DONE 2026-05-20 |
| Source reconnect (ICE restart) | §15 Phase 2 | — | P1 | Phase 2 start | DONE 2026-05-20 |
| Rate limiting (max rooms/IP, max listeners/room) | §9 Phase 2, §15 | — | P1 | Phase 2 start | DONE 2026-05-20 |
| Room expiry (auto-close on inactivity) | §9 Phase 2 | — | P1 | none | DONE 2026-05-20 |
| Graceful shutdown (SIGTERM + drain) | §15 Phase 2 | — | P1 | none | DONE 2026-05-20 |
| SLO instrumentation (session completion, transport latency, source publish) | §13.5 | — | P1 | RELAY-006 | OPEN |
| latency_estimate_ms metric (RELAY-006 clock sync) | §13.2 | RELAY-006 | P2 | RELAY-006 resolution | OPEN |
| api-server JWT compatibility test | RELAY-014 | RELAY-015 | P1 | api-server Phase 2 | DONE 2026-05-21 |
| relay→api-server source_active push | §12.2 | — | P2 | api-server Phase 2 | DONE 2026-05-21 |
| Connected WriteRTP bench (RELAY-009) | §26.6 | RELAY-009 | P2 | Phase 2 start | DONE 2026-05-21 |
| ICE failure / SIGTERM / room-delete failure mode tests | Audit F3 | — | P2 | Phase 2 start | DONE 2026-06-01 |
| NAT1To1IPs + ICE-TCP mux (port 8081) | §9 Phase 2 | RELAY-023 | P1 | — | DONE 2026-06-01 |
| CODEC_HINT message type + frame_ms field | §26.4 | — | P2 | — | DONE 2026-06-01 |
| KEY_EXCHANGE forwarding (SFrame E2EE relay bypass) | RELAY-014 | — | P2 | — | DONE 2026-06-01 |
| Webhook events (session_started / session_ended / utterance_detected) | §12.2 | — | P2 | api-server | DONE 2026-06-02 |
| Public broadcast channels (GET /v1/channels) | §12.2 | — | P2 | — | DONE 2026-06-02 |
| Playwright E2E (6 tests vs live relay) | §15 | — | P1 | live relay | DONE 2026-06-02 |

---

Priority definitions:

- P1: required for Phase 2 exit criteria.
- P2: improves quality, observability, or correctness but does not block Phase 2 exit.
