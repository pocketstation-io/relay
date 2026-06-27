# Phase 1 Progress — relay

## Active branch: test/floodtest-100m-smoke

---

## Queue

- [x] Stereo Opus SDP (Stage B) — stereo=1;sprop-stereo=1 in MediaEngine fmtp
- [x] Padding fix — clear RTP padding bit on forward (fixes ~47% browser concealment)
- [x] Stage D core — RFC 2198 RED codec + redListener + MediaEngine registration
- [x] Stage D browser fix — app-web-receiver SDP munge to include audio/red in subscribe offer
- [ ] Stage D Gate D — prove loss recovery with packet-loss-injection harness (RELAY_ENABLE_RED=1)
- [ ] Room → GraphSession migration (Phase 1 primary task — not started)
- [ ] AudioBus semantics on relay forwarding path
- [ ] source_id / bus_id in signaling
- [ ] /v1/graphs and /v1/sessions control plane API

---

## Completed

### Stage D — Opus RED (RFC 2198) loss resilience

**Branch:** test/floodtest-100m-smoke
**Commits:** relay 62bbaec · app-web-receiver 758f6be
**Date:** 2026-06-27
**Phase-exception:** user-directed; Phase 0 audio-graph crate is the canonical next task

**What was built:**
- `internal/red/red.go` — RFC 2198 encoder + parser (spec-exact, round-trip tested)
- `internal/server/red_listener.go` — room.Listener decorator; wraps each forwarded
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
