# Phase 1 Progress — relay

## Active branch: test/floodtest-100m-smoke

---

## Queue

- [x] Stereo Opus SDP (Stage B) — stereo=1;sprop-stereo=1 in MediaEngine fmtp
- [x] Padding fix — clear RTP padding bit on forward (fixes ~47% browser concealment)
- [x] Stage D core — RFC 2198 RED codec + redListener + MediaEngine registration
- [x] Stage D browser fix — web-receiver SDP munge to include audio/red in subscribe offer
- [x] Room → GraphSession migration — COMPLETE (audited 2026-07-02; see below)
- [x] AudioBus semantics on relay forwarding path — COMPLETE (AudioBus type, BusID, BusMix)
- [x] source_id / bus_id in signaling — COMPLETE (ClientMessage.BusID, SessionID/RoomID compat)
- [x] /v1/sessions control plane API — COMPLETE (POST, GET latency/health/packet-log/events, WHIP/WHEP)
- [x] Stage D Gate D — real browser + Linux `tc netem` loss proof (RELAY_ENABLE_RED=1)
- [x] Forward RTP reorder hardening — bounded 40 ms gap-only hold with preserved loss gaps
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
- Stage D default policy — longer repeated clean/loss/jitter A/B is required before rollout
- `/v1/graphs` — graph template listing endpoint; deferred to Phase 2+ (no template registry yet)

---

## Completed

### Main workflow reproducibility (SAFE-TO-TEST, 2026-07-26)

The first W11 merge exposed two pre-existing workflow defects that pull
requests could not observe:

- The smoke job asked `go build` to compile two commands without `-o`, then
  attempted to execute root-level binaries that Go had not produced.
- The deploy setup action installed `flyctl`, while the deploy step invoked the
  absent `fly` alias.

The workflow now builds each smoke binary to the runner temporary directory,
uses a cleanup trap for the relay process, and invokes `flyctl` by its installed
name. Shutdown, integration, smoke, and short-soak jobs run on pull requests as
well as `main`, so a green PR now exercises the same component gates that run
after merge. Deployment remains a `main`-only real external action and requires
the repository-scoped `FLY_API_TOKEN` secret.

This is CI/deployment orchestration only. No relay runtime, media-plane,
threshold, product path, scaffold, mock, fallback, or loopback claim changes.

### Cadence catch-up bound (SAFE-TO-TEST, 2026-07-26)

The W11 clean-candidate gate reproduced an intermittent cadence-pacer write
interval below its existing 15 ms lower bound: 13.920834 ms in the acceptance
run and 13.402625 ms in a focused 100-repetition run. The experimental cadence
pacer kept due times anchored to the original RTP timeline after a late
scheduler wake, so a following write could consume the accumulated timing debt
as a subscriber-visible burst.

The cadence path now limits catch-up to 20% of the current RTP frame spacing.
This preserves a 16 ms minimum for the default 20 ms profile and an 8 ms
minimum for the optional 10 ms profile. Recovery packets, explicit timeline
resets, and the production forward-pacer path are unchanged. This follows the
bounded elapsed-time/budget pattern used by libwebrtc pacing while retaining
PocketStation's RTP-timestamp schedule.

Verification:

- Focused cadence scheduler, catch-up, and histogram tests passed 100
  repetitions while retaining the original 15–35 ms raw-spacing contract.
- Histogram bucket behavior is tested separately with deterministic inputs;
  scheduler oversleep can no longer fail an unrelated exact-bucket assertion.
- `go vet ./...`: PASS.
- `go test -short ./...`: PASS.
- `go test -race ./...`: PASS on the formatting/workflow follow-up; the soak
  package completed in 312.288 seconds.
- The full 14-gate `pocketstation-lab` candidate and independent verifier:
  PASS.
- Remote CI exposed five pre-existing files that were not canonical `gofmt`
  output. The merge candidate applies only the formatter's mechanical changes
  to those files so the repository-wide format gate is reproducible locally.
- The same CI audit found the explicit session-latency step still referenced
  the removed `internal/room` package. It now runs real canonical
  `/v1/sessions/{id}/latency` success and not-found regressions in
  `internal/server`; the repository-wide race suite still covers every package.

**Status:** `SAFE-TO-MERGE` locally. The repository-local format, vet,
short-test, focused endpoint, and full race gates pass. The exact remote head
must still be green before merge.

### Staff Bar Self-Check — cadence catch-up bound

- Smallest correct design: yes — one cadence-relative lower bound in the
  experimental pacing path
- Tests added or updated: yes — deterministic helper/histogram coverage and
  100 repetitions of the original wall-clock regression, plus canonical
  session-latency endpoint coverage replacing a stale CI target
- Hot-path safe: yes — no allocation, lock, blocking call, logging, panic, or
  asynchronous work was added to packet scheduling
- Public API changed: no
- New dependency: no
- Phase scope respected: yes — this fixes a W11 acceptance failure in existing
  relay behavior
- Unsafe added: no
- Remaining risk: remote CI must pass on the exact formatting follow-up head

### Forward RTP reorder hardening (PARTIAL, 2026-07-18)

The forward downlink previously serialized RTP in arrival order. Under
20 ms ± 10 ms controlled jitter, short ingress reordering therefore became
subscriber-visible sequence and timestamp discontinuities. The downlink now
uses bounded fixed-size pending storage: ordered packets forward immediately,
a detected gap waits at most 40 ms, recovered packets release in order, and a
true loss keeps its RTP gap for downstream NACK semantics.

Focused verification passed 100 repetitions, the downlink/server race suites,
and `go vet ./internal/downlink`. Post-fix short stress evidence reduced output
sequence discontinuities from approximately 68–71% of sent packets to
approximately 0.2%; the valid final 10-minute clean PocketStation cell reported
zero sequence discontinuities, gap timeouts, queue/stale drops, blocked writes,
or write errors across 30,243 packets.

**Status:** `PARTIAL`. The defect and bounded correction are real and tested,
but the remaining acceptance target is a valid repeated long jitter panel with
near-zero discontinuities and no clean-path latency or concealment regression.
Current comparative interpretation belongs in
`../../docs/reports/INTERACTIVE_TRANSPORT_FINDINGS_2026-07-18.md`.

### Source-hop media-padding normalization (REAL, 2026-07-20)

Str0m publisher packets can arrive through Pion with the RTP padding flag still
set on packets that contain Opus media. Forwarding that source-hop flag into the
independently encrypted subscriber hop caused the mono browser receiver to lose
approximately every second packet even though relay enqueue and send counters
were exact. The downlink now clears source-hop media padding before the write,
keeps pure padding explicit, and reports the normalization count.

The regression is covered in the forward-pacer unit suite. A 15-second real
device run on the corrected relay delivered 695 application and 696 microphone
packets with zero browser packet loss; evidence:
`pocketstation-lab/artifacts/product-proof/w7-browser-mono-diagnostic-15s-pass2-2026-07-20/`.
The earlier clean-checkout artifact remains useful negative evidence because it
reproduced the unfixed mono failure while relay ingress stayed exact.

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

### Stage D — Opus RED (RFC 2198) loss resilience (REAL-PROVEN / DEFAULT-OFF)

**Branch:** test/floodtest-100m-smoke
**Commits:** relay 62bbaec · web-receiver 758f6be
**Date:** 2026-06-27

**What was built:**
- `internal/red/red.go` — RFC 2198 encoder + parser (spec-exact, round-trip tested)
- `internal/server/red_listener.go` — AudioBus decorator; wraps each forwarded
  Opus frame with the previous one as redundancy (1-deep, ~20ms redundancy window)
- `session.go` — MediaEngine registers audio/red PT 63 fmtp "111/111" only for
  subscriber/WHEP egress when RELAY_ENABLE_RED=1; publisher/WHIP ingress stays
  Opus-only; listener track is wrapped with the redListener decorator
- `web-receiver/src/webrtc.ts` — addRedToSdp() munges subscribe offer to inject
  PT 63 before setLocalDescription (the actual root cause of the prior revert)

**Code standard:**
- CODE_PROTOCOL LAW 1: TimestampOffsetSamples uint32 (unit suffix)
- CODE_PROTOCOL LAW 2: struct fields column-aligned
- LAW 16: all tests given_when_then named
- go test -race ./internal/... -short: ALL PASS
- go vet ./...: CLEAN

**Status:** REAL-PROVEN / DEFAULT-OFF — gated by `RELAY_ENABLE_RED`. RED
encoding, role-specific negotiation, and pure-padding handling are real and
tested. The 15-minute matched diagnostic remained `NOT_PROVEN`: only the clean
pair was valid, and both PocketStation RED (180.73 ms) and LiveKit (184.66 ms)
exceeded the 170 ms end-to-end target. The LiveKit loss cell and both jitter
cells were measurement-invalid. Separate 10-minute RED-off/on jitter runs were
also invalid because of collector stalls and therefore do not establish RED's
latency cost. They did expose forward-mode writer stalls, stale drops, and
extensive RTP output sequence/timestamp discontinuities under reordered
ingress. A later hardened 10-minute PocketStation jitter cell was valid but
still failed at 255.93 ms source-to-playout p95 with one stale pacer drop. Keep
RED default-off until the collector completes a valid repeated long A/B and the
remaining loss-resilience and playout-tail gaps are resolved. Forward RTP
continuity is now explicitly bounded as documented above. Evidence:
`pocketstation-lab/artifacts/bench/red-livekit-diagnostic-15m-2026-07-18/` and
`pocketstation-lab/artifacts/bench/jitter-collector-hardened-10m-2026-07-18/`.
See FAKE_SCAFFOLD_INVENTORY.md S-red-01.

**Staff Bar Self-Check:**
- Smallest correct design: yes — env var gate keeps verified stereo path untouched
- Tests: RED codec, role-specific negotiation, pure-padding, and integration
  coverage; all given_when_then
- Hot-path safe: redListener allocates per-packet (copy prevPayload + Encode output);
  acceptable on the relay forwarding path which is not the audio callback hot path
- Public API changed: no (env var gated)
- New dependency: no
- Phase scope: Phase 1 relay, user-directed exception to Phase 0 priority
- Unsafe added: no
- Remaining risk: collector validity belongs to `pocketstation-lab`; RTP
  pacing, residual continuity, loss resilience, and writer stalls belong to the
  relay media plane. A valid repeated long clean/loss/jitter A/B is still
  required. No default-policy or LiveKit superiority claim is authorized.
