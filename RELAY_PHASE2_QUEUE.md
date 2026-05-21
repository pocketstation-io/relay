# RELAY_PHASE2_QUEUE.md

Phase 2 intake list for `pocketstation-io/relay`. All items are derived from v2.3 §15 Phase 2 deliverables and audit follow-ups. No item here is in scope for Phase 1.

Before picking up any item, read AGENTS.md, the relevant ADR, and the assigned issue.

---

| Task | Maps to v2.3 | ADR | Priority | Blocked on | Status |
|---|---|---|---|---|---|
| Copy-on-write listener slice | §26.2, §15 Phase 2 | ADR-005 | P1 | none | DONE 2026-05-20 |
| Source reconnect (ICE restart) | §15 Phase 2 | — | P1 | Phase 2 start | DONE 2026-05-20 |
| Rate limiting (max rooms/IP, max listeners/room) | §9 Phase 2, §15 | — | P1 | Phase 2 start | DONE 2026-05-20 |
| Room expiry (auto-close on inactivity) | §9 Phase 2 | — | P1 | none | DONE 2026-05-20 |
| Graceful shutdown (SIGTERM + drain) | §15 Phase 2 | — | P1 | none | DONE 2026-05-20 |
| SLO instrumentation (session completion, transport latency, source publish) | §13.5 | — | P1 | ADR-006 | OPEN |
| latency_estimate_ms metric (ADR-006 clock sync) | §13.2 | ADR-006 | P2 | ADR-006 resolution | OPEN |
| api-server JWT compatibility test | ADR-014 | ADR-015 | P1 | api-server Phase 2 | DONE 2026-05-21 |
| relay→api-server source_active push | §12.2 | — | P2 | api-server Phase 2 | DONE 2026-05-21 |
| Connected WriteRTP bench (ADR-009) | §26.6 | ADR-009 | P2 | Phase 2 start | DONE 2026-05-21 |
| ICE failure / SIGTERM / room-delete failure mode tests | Audit F3 | — | P2 | Phase 2 start | OPEN |

---

Priority definitions:

- P1: required for Phase 2 exit criteria.
- P2: improves quality, observability, or correctness but does not block Phase 2 exit.
