# Phase 1 Progress — relay

## Active branch: test/floodtest-100m-smoke

---

## Queue

- [x] Stereo Opus SDP (Stage B) — stereo=1;sprop-stereo=1 in MediaEngine fmtp
- [x] Padding fix — clear RTP padding bit on forward (fixes ~47% browser concealment)
- [x] Stage D core — RFC 2198 RED codec + redListener + MediaEngine registration
- [x] Stage D browser fix — app-web-receiver SDP munge to include audio/red in subscribe offer
- [x] Room → GraphSession migration — COMPLETE (audited 2026-07-02; see below)
- [x] AudioBus semantics on relay forwarding path — COMPLETE (AudioBus type, BusID, BusMix)
- [x] source_id / bus_id in signaling — COMPLETE (ClientMessage.BusID, SessionID/RoomID compat)
- [x] /v1/sessions control plane API — COMPLETE (POST, GET latency/health/packet-log/events, WHIP/WHEP)
- [ ] Stage D Gate D — prove loss recovery with packet-loss-injection harness (RELAY_ENABLE_RED=1)
- [ ] /v1/graphs endpoint — graph template listing (Phase 2+ scope; not started)

---

## Phase 1 Audit — GraphSession Migration (2026-07-02)

**Audited state vs. AGENTS.md claim ("0%"):**

The AGENTS.md and BUILD_GUIDE were written before Waves 13/13b/13c were committed.
The relay is fully v3.0-vocabulary. All claimed "not started" items are done.

| Item | Status | Evidence |
|---|---|---|
| RelaySession core type | ✅ DONE | `internal/graph/relay_session.go` |
| AudioBus (per-bus forwarding) | ✅ DONE | `AudioBus`, `BusID`, `BusMix`, `BusRole` in relay_session.go |
| BusSubscription interface | ✅ DONE | `BusSubscription` replaces old Listener |
| POST /v1/sessions | ✅ DONE | server.go:192 |
| GET /v1/sessions/{id}/latency | ✅ DONE | server.go:193 |
| GET /v1/sessions/{id}/health | ✅ DONE | server.go:194 — per-bus media watchdog |
| GET /v1/sessions/{id}/packet-log | ✅ DONE | server.go:195 — A3/A4 benchmark endpoint |
| GET /v1/sessions/{id}/events | ✅ DONE | server.go:196 — SSE presence stream |
| WHIP / WHEP (RFC 9725) | ✅ DONE | server.go:201-202 |
| v2.3 /v1/rooms alias | ✅ DONE | server.go:207 — backward compat, kept until SDKs migrate |
| session_id in signaling | ✅ DONE | ClientMessage.SessionID + RoomID alias |
| bus_id in signaling | ✅ DONE | ClientMessage.BusID |
| SessionRegistry | ✅ DONE | `graph.SessionRegistry`, replaces old room registry |
| Per-bus media watchdog | ✅ DONE | Wave 13 — LastRTPAge, DefaultMediaStallThresholdMs |

**What is NOT done:**
- Stage D Gate D — requires packet-loss injection with RELAY_ENABLE_RED=1 (real path needed)
- `/v1/graphs` — graph template listing endpoint; deferred to Phase 2+ (no template registry yet)

---

## Completed

### Fly UDP reply-path fix (2026-07-13)

- Bind the shared ICE UDP mux to `fly-global-services` on Fly instead of `0.0.0.0`.
- Real macOS exact-process proof connected over the deployed IAD relay and sent 499 RTP packets in 10 seconds.
- Bounded relay race suites passed for `cmd/relay-server`, `internal/server`, `internal/graph`, and `internal/downlink`.

### Task 1.1 — Room → GraphSession migration (DONE, audited 2026-07-02)

**Waves:** 13, 13b, 13c (pre-existing commits on test/floodtest-100m-smoke)
**v3.0 vocabulary fully implemented:**

- `graph.RelaySession` — the central forwarding unit; owns named AudioBuses
- `graph.AudioBus` — one named forwarding lane (voice / music / agent_voice / events)
- `graph.BusSubscription` — write side of audio delivery (replaces `room.Listener`)
- `graph.SessionRegistry` — room registry with RegistryConfig, inactivity timeouts, CloseAll
- `graph.BusRole` with `LatencyRank()` and `ReliabilityRank()` impl methods (LAW 8)
- BusMix (`"mix"`) — virtual BusID subscribing to all active buses (relay.out("mix") semantics)
- `/v1/sessions` canonical endpoints with `/v1/rooms` backward-compat aliases
- `ClientMessage.SessionID` (v3.0) + `RoomID` (v2.3 alias) via `EffectiveSessionID()`
- `ClientMessage.BusID` — names the AudioBus for PUBLISH/SUBSCRIBE
- Per-bus media watchdog: WebSocket liveness ≠ media liveness (Corrected Audit §6)
- Packet log ring-buffer (A3/A4 benchmark endpoint, 1000-entry window)
- SSE presence events at `/v1/sessions/{id}/events`
- WHIP/WHEP (RFC 9725) ingest/egress at `/v1/sessions/{id}/whip|whep`

### Stage D — Opus RED (RFC 2198) loss resilience (PARTIAL — Gate D pending)

**Branch:** test/floodtest-100m-smoke
**Commits:** relay 62bbaec · app-web-receiver 758f6be
**Date:** 2026-06-27

**What was built:**
- `internal/red/red.go` — RFC 2198 encoder + parser (spec-exact, round-trip tested)
- `internal/server/red_listener.go` — AudioBus decorator; wraps each forwarded
  Opus frame with the previous one as redundancy (1-deep, ~20ms redundancy window)
- `session.go` — MediaEngine registers audio/red PT 63 fmtp "111/111" ahead of Opus
  when RELAY_ENABLE_RED=1; listener track created as audio/red; AddListener wrapped
  with redListener decorator
- `app-web-receiver/src/webrtc.ts` — addRedToSdp() munges subscribe offer to inject
  PT 63 before setLocalDescription (the actual root cause of the prior revert)

**Code standard:**
- CODE_PROTOCOL LAW 1: TimestampOffsetSamples uint32 (unit suffix)
- CODE_PROTOCOL LAW 2: struct fields column-aligned
- LAW 16: all tests given_when_then named
- go test -race ./internal/... -short: ALL PASS
- go vet ./...: CLEAN

**Status:** PARTIAL — gated by RELAY_ENABLE_RED env var (default off).
Gate D not yet proven. See FAKE_SCAFFOLD_INVENTORY.md S-red-01.

**Staff Bar Self-Check:**
- Smallest correct design: yes — env var gate keeps verified stereo path untouched
- Tests: 5 RED codec tests + 1 redListener test; all given_when_then
- Hot-path safe: redListener allocates per-packet (copy prevPayload + Encode output);
  acceptable on the relay forwarding path which is not the audio callback hot path
- Public API changed: no (env var gated)
- New dependency: no
- Phase scope: Phase 1 relay, user-directed exception to Phase 0 priority
- Unsafe added: no
- Remaining risk: Gate D requires real packet loss; RED is no-op on lossless localhost
