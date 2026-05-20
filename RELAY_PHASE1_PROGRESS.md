# RELAY_PHASE1_PROGRESS.md

Phase 1 relay MVP. Tracks what is done, what is partial, and what is intentionally deferred.

---

## What is done and tested

- **Signaling messages** (`internal/signaling/messages.go`): all 7 types defined (PUBLISH, SUBSCRIBE, ICE, LEAVE, SDP_ANSWER, ROOM_STATE, ERROR) with typed ClientMessage/ServerMessage structs.
- **JWT helper** (`internal/auth/token.go`): room-scoped token, source/listener roles, configurable TTL, HS256 signing. Tests in `token_test.go`.
- **Room lifecycle** (`internal/room/room.go`): Source/Listener interfaces, SetSource, AddListener, RemoveListener, ListenerCount, SourceActive, Close (idempotent via sync.Once), Manager with GetOrCreate/Get/Delete. Tests in `room_test.go`.
- **RTP forwarding scaffold** (`internal/room/room.go`): forwardLoop reads from Source, snapshots listeners under read lock, writes to each Listener. Counters (PacketCount, ByteCount) are atomic. Goroutine teardown: returns on ReadRTP error or done channel close.
- **WebSocket signaling skeleton** (`cmd/relay-server/main.go`): all message types dispatched; session struct with wmu-serialised writes; cleanup deferred.
- **Pion v4 peer connection skeleton** (`cmd/relay-server/main.go`): PC created on PUBLISH/SUBSCRIBE after JWT verification; OnTrack wires source to room; TrackLocalStaticRTP wired for listeners; SDP offer/answer exchange; ICE candidate forwarding via OnICECandidate.
- **ADR-009 benchmark** (`internal/room/forward_bench_test.go`): benchmarks dispatch loop with 1/10/100 mock listeners; documents that real Pion WriteRTP measurement is required before Phase 1 exit.

## What is partial (and what would finish it)

- **SDP offer absent**: if a client sends PUBLISH/SUBSCRIBE without an SDP offer, SetRemoteDescription will fail with a Pion error and the session sends an sdp_error. A dedicated validation step before SetRemoteDescription would give a clearer error. Deferred; low priority for MVP.
- **Room cleanup on empty**: rooms are never deleted automatically when the last peer leaves. Manager.Delete exists but is not called on last-peer-leave. For Phase 1 MVP this is acceptable; production would need a ref-counted cleanup pass.
- **STUN server**: `POCKETSTATION_STUN` env var defaults to `stun:stun.l.google.com:19302`. No TURN server configured; relay works in LAN/same-network tests only. TURN is Phase 2.

## What is fake/mock/scaffold

- **ADR-009 benchmark uses mock Listener** (`discardListener`), not `*webrtc.TrackLocalStaticRTP`. The allocation profile of Pion's WriteRTP is therefore unmeasured. No zero-alloc claim is made.
- **ADR-010 jitter buffer**: not implemented. RTP packets are forwarded immediately with no adaptive buffering. ADR-010 defers this to measurement data.

## What is blocked

- `go test ./...` and `go test -race ./...` could not be run: the Go binary is not installed in the agent's shell environment. The `go.sum` file is also absent and must be generated with `go mod tidy` before the first build. All code was written to compile correctly but has not been machine-verified.

## What needs human decision

- ADR-009: a human must run `go test -bench=BenchmarkWriteRTP -benchmem ./internal/room/` against real Pion tracks before the relay makes any latency or allocation claims.
- ADR-010: adaptive jitter buffer target depth needs a measurement plan for Phase 1.

---

### Staff Bar Self-Check — Task 1: relay compiles cleanly

- Smallest correct design: yes — removed stray uuid import, replaced with crypto/rand UUID v4 generator in-process; no new dependency.
- Tests added or updated: not applicable (compilation fix only).
- Hot-path safe: yes — newID() is called only at room creation and session open, not on the RTP path.
- Public API changed: no.
- New dependency: no — removed one (uuid).
- Phase scope respected: yes.
- Unsafe added: no.
- Remaining risk: go.sum absent; `go mod tidy` required before first build.

---

### Staff Bar Self-Check — Task 2: signaling messages

- Smallest correct design: yes — typed constants and two flat structs; no over-engineering.
- Tests added or updated: not applicable (types only; covered by integration via signaling handler tests in future).
- Hot-path safe: not applicable (message types).
- Public API changed: no (new package, no prior consumers).
- New dependency: no.
- Phase scope respected: yes — signaling types live in relay repo until Phase 2 per AGENTS.md.
- Unsafe added: no.
- Remaining risk: none.

---

### Staff Bar Self-Check — Task 3: room lifecycle

- Smallest correct design: yes — Source/Listener interfaces introduced to allow unit testing without Pion; Manager.Delete added; no speculative abstractions.
- Tests added or updated: yes — 15 tests in room_test.go covering create, add/remove listeners, forwarding, counter increment, EOF cleanup, Manager CRUD.
- Hot-path safe: yes — forwardLoop uses atomic counters; only allocates a snapshot slice per forwarded packet (consistent with ADR-009 pending measurement).
- Public API changed: yes — SetSource now takes `room.Source` interface instead of `*webrtc.TrackRemote`; AddListener now takes `(peerID string, l room.Listener)`. Both are new in Phase 1; no downstream consumers yet.
- New dependency: no — removed pion/webrtc from room.go; now only pion/rtp.
- Phase scope respected: yes.
- Unsafe added: no.
- Remaining risk: listener write errors are silently discarded (`_ = l.WriteRTP(pkt)`); a future phase should log or meter these.

---

### Staff Bar Self-Check — Task 4: JWT helper

- Smallest correct design: yes — thin wrapper over golang-jwt/v5; no custom claims beyond RoomID and Role.
- Tests added or updated: yes — 6 tests in token_test.go (happy path, wrong secret, expired, listener role, malformed, empty).
- Hot-path safe: not applicable.
- Public API changed: no.
- New dependency: no.
- Phase scope respected: yes.
- Unsafe added: no.
- Remaining risk: none.

---

### Staff Bar Self-Check — Task 5: WebSocket signaling skeleton

- Smallest correct design: yes — per-session struct with wmu for concurrent writes, all 7 message types dispatched, cleanup deferred.
- Tests added or updated: no — WebSocket handler requires integration test setup; deferred to Phase 1 integration test pass.
- Hot-path safe: not applicable (signaling path, not audio path).
- Public API changed: no.
- New dependency: no.
- Phase scope respected: yes.
- Unsafe added: no.
- Remaining risk: no limit on concurrent sessions; production needs a connection limit and rate limiter.

---

### Staff Bar Self-Check — Task 6: Pion v4 peer connection skeleton

- Smallest correct design: yes — minimal Pion setup: STUN config, OnICECandidate wiring, OnTrack for source, AddTrack for listener, SDP offer/answer exchange.
- Tests added or updated: no — requires live WebRTC; deferred.
- Hot-path safe: not applicable.
- Public API changed: no.
- New dependency: no (pion/webrtc/v4 already in go.mod).
- Phase scope respected: yes.
- Unsafe added: no.
- Remaining risk: no ICE state machine error handling; Pion errors after SetLocalDescription are not surfaced to the client. Phase 1 MVP; acceptable.

---

### Staff Bar Self-Check — Task 7: RTP forwarding scaffold

- Smallest correct design: yes — forwardLoop is a simple read/snapshot/write loop; done channel provides cooperative exit; source clears on error via deferred function.
- Tests added or updated: yes — TestForwardLoop_* tests in room_test.go.
- Hot-path safe: yes — no locks held during WriteRTP; one allocation per forwarded packet for the snapshot slice.
- Public API changed: no.
- New dependency: no.
- Phase scope respected: yes.
- Unsafe added: no.
- Remaining risk: snapshot slice allocation per packet. ADR-009 measurement pending.

---

### Staff Bar Self-Check — Task 8: WriteRTP benchmark (ADR-009)

- Smallest correct design: yes — three benchmark functions (1/10/100 listeners) + discardListener; mirrors forwardLoop dispatch exactly.
- Tests added or updated: yes — benchmark in forward_bench_test.go.
- Hot-path safe: not applicable (benchmark).
- Public API changed: no.
- New dependency: no.
- Phase scope respected: yes.
- Unsafe added: no.
- Remaining risk: discardListener is not *webrtc.TrackLocalStaticRTP. Pion WriteRTP allocation is unmeasured. This is explicitly noted in the benchmark file and progress file.

---

### Staff Bar Self-Check — Task 9: tests

- Smallest correct design: yes — GWT structure throughout; white-box tests via package room for mock injection; black-box tests for auth.
- Tests added or updated: yes — 15 room tests + 6 auth tests.
- Hot-path safe: not applicable.
- Public API changed: no.
- New dependency: no.
- Phase scope respected: yes.
- Unsafe added: no.
- Remaining risk: tests not machine-verified (Go not installed in agent shell). Require `go mod tidy && go test ./... && go test -race ./...` before merge.

---

### Staff Bar Self-Check — Task 10: final audit

- gofmt: NOT RUN — Go binary not available in agent shell. Must be run by human before merge.
- go vet: NOT RUN — same reason.
- go test ./...: NOT RUN — same reason.
- go test -race ./...: NOT RUN — same reason.
- go.sum: ABSENT — `go mod tidy` required.
- Remaining risk: all code requires human-run tooling verification before merge.
