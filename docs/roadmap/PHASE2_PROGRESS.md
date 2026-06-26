# RELAY_PHASE2_PROGRESS.md

Phase 2 relay hardening. Started 2026-05-20.

---

## Tasks completed (2026-05-20)

### Task 1 — Copy-on-write listener slice (RELAY-005)

**What changed:** `internal/room/room.go`

Replaced `sync.RWMutex + map[string]Listener` with
`sync/atomic.Pointer[[]listenerEntry]`. `forwardLoop` performs one
`atomic.Load` per packet; no lock is held during `WriteRTP`.
`AddListener` and `RemoveListener` take a short `listenersMu`, copy the
slice, and store the new pointer atomically.

The benchmark in `forward_bench_test.go` mirrors the new hot path.

**Tests added:**
- `TestGiven_CopyOnWriteListeners_When_ConcurrentAddAndForward_Then_NoRace`
  — concurrent `AddListener` + `forwardLoop` under `-race` completes clean.

---

### Task 2 — Room expiry

**What changed:** `internal/room/room.go`

Added `inactivityTimeout` field (default `30*time.Minute`). An
`AfterFunc` timer fires `room.Close` when no publisher attaches within
the window. `SetSource` resets the timer on every successful source
attachment. The timer field is written and the AfterFunc callback
acquire `timerMu` before accessing it, establishing the required
happens-before edge.

A helper `newWithTimeout(id, duration)` is package-private to allow
tests to use short durations.

**Tests added:**
- `TestGiven_Room_When_InactivityTimerExpires_Then_RoomClosed` (50ms timeout)
- `TestGiven_Room_When_SourceSetsBeforeTimeout_Then_TimerReset`

---

### Task 3 — Graceful shutdown

**What changed:** `cmd/relay-server/main.go`, `internal/server/server.go`

`main.go` catches `SIGTERM`/`SIGINT` and calls `Server.Shutdown`.
`Server.Shutdown`:
1. Closes all active WebSocket sessions (sends close frame so peers
   observe a clean disconnect). WebSocket connections are hijacked from
   the HTTP server and not tracked by `http.Server.Shutdown`.
2. Calls `http.Server.Shutdown` with a 5s deadline.
3. Calls `Manager.CloseAll` to close all active rooms.

`Manager.CloseAll` added to `internal/room/room.go`.
Session map (`sessions map[string]*session`) added to `Server` for tracking.

**Tests added:**
- `TestGiven_RelayShutdown_When_ActiveConnection_Then_PeerReceivesLeave`
  (in `internal/server/shutdown_test.go`)

---

### Task 4 — Source reconnect (ICE restart)

**What changed:** `internal/room/room.go`, `internal/server/server.go`

`SetSource(src Source, closer func())` now accepts a `closer` that is
called when a subsequent `SetSource` replaces the source. In
`server.go` the closer is `pc.Close` so the old `PeerConnection` is
torn down on reconnect, causing its `ReadRTP` to error and the old
`forwardLoop` to exit.

Existing listeners are not affected: the copy-on-write listener slice
is unchanged across the source replacement.

**Tests added:**
- `TestGiven_SourcePublishing_When_SourceReconnects_Then_ListenerReceivesRTPAfterReconnect`

---

### Task 5 — Rate limiting

**What changed:** `internal/server/server.go`, `cmd/relay-server/main.go`,
`internal/metrics/metrics.go`

Added `MaxRooms` and `MaxListenersPerRoom` fields to `Config`.
`main.go` reads `RELAY_MAX_ROOMS` and `RELAY_MAX_LISTENERS_PER_ROOM`
env vars (default 100 and 50).

`POST /v1/rooms` returns HTTP 429 `{"error":"room_limit_exceeded"}` when
the active room count reaches the ceiling.

`SUBSCRIBE` returns a WebSocket `ERROR` frame with code
`listener_limit_exceeded` when the room's listener count is at capacity.

Also fixed pre-existing `go vet` warning: `metrics.WriteTo` renamed to
`metrics.WritePrometheus` to avoid shadowing `io.WriterTo`.

**Tests added:**
- `TestGiven_MaxRoomsReached_When_CreateRoom_Then_429`
- `TestGiven_MaxListenersReached_When_Subscribe_Then_ErrorFrame`

---

## Tests added (all Given/When/Then naming)

| Test | File | Covers |
|---|---|---|
| `TestGiven_CopyOnWriteListeners_When_ConcurrentAddAndForward_Then_NoRace` | `internal/room/room_test.go` | Task 1 — race detector |
| `TestGiven_Room_When_InactivityTimerExpires_Then_RoomClosed` | `internal/room/room_test.go` | Task 2 — expiry |
| `TestGiven_Room_When_SourceSetsBeforeTimeout_Then_TimerReset` | `internal/room/room_test.go` | Task 2 — timer reset |
| `TestGiven_RelayShutdown_When_ActiveConnection_Then_PeerReceivesLeave` | `internal/server/shutdown_test.go` | Task 3 — shutdown |
| `TestGiven_SourcePublishing_When_SourceReconnects_Then_ListenerReceivesRTPAfterReconnect` | `internal/room/room_test.go` | Task 4 — ICE restart |
| `TestGiven_MaxRoomsReached_When_CreateRoom_Then_429` | `internal/server/ratelimit_test.go` | Task 5 — room limit |
| `TestGiven_MaxListenersReached_When_Subscribe_Then_ErrorFrame` | `internal/server/ratelimit_test.go` | Task 5 — listener limit |

---

## Staff Bar Self-Check

### Task 1 — Copy-on-write listener slice

- Smallest correct design: yes — `atomic.Pointer` + mutex-guarded copy is the
  textbook Go implementation of copy-on-write with no external dependency.
- Tests added or updated: yes — race test + benchmark updated.
- Hot-path safe: yes — one `atomic.Load` per packet, no lock during `WriteRTP`.
- Public API changed: yes — `SetSource` signature changed (see Task 4 note).
- New dependency: no.
- Phase scope respected: yes — RELAY-005 Phase 2.
- Unsafe added: no.
- Remaining risk: `listenerEntry` interface field still carries two words; no
  additional allocation vs. the old map-snapshot path.

### Task 2 — Room expiry

- Smallest correct design: yes — `time.AfterFunc` + `timerMu` pattern is idiomatic.
- Tests added: yes.
- Hot-path safe: yes — timer callback does not touch the forwarding path.
- Public API changed: no (new internal helper `newWithTimeout` is unexported).
- New dependency: no (uses stdlib `time`).
- Phase scope respected: yes.
- Unsafe added: no.
- Remaining risk: `time.AfterFunc` goroutine holds `timerMu` briefly before
  calling `Close`; `Close` re-acquires it. If `Close` is called concurrently
  from two goroutines, `closeOnce` serialises the actual close. No deadlock path.

### Task 3 — Graceful shutdown

- Smallest correct design: yes — session map + `closeConn` pattern avoids
  any new abstraction.
- Tests added: yes.
- Hot-path safe: yes — session map is accessed only at connect/disconnect, not
  on the RTP path.
- Public API changed: yes — `Serve` now returns `error`; callers must handle
  `http.ErrServerClosed`.
- New dependency: no.
- Phase scope respected: yes.
- Unsafe added: no.
- Remaining risk: `Shutdown` closes sessions before `http.Server.Shutdown`; if
  a session goroutine is slow to exit, the HTTP server drain window may overlap.
  5s deadline is conservative.

### Task 4 — Source reconnect

- Smallest correct design: yes — `closer func()` on `SetSource` is the minimum
  change to communicate the old PC reference to the room without tight coupling.
- Tests added: yes.
- Hot-path safe: yes — `prevCloser()` call is outside the forward path.
- Public API changed: yes — `SetSource(src Source, closer func())`.
- New dependency: no.
- Phase scope respected: yes.
- Unsafe added: no.
- Remaining risk: if `SetSource` is called rapidly from multiple goroutines,
  only one `prevCloser` is captured per call; concurrent `SetSource` calls are
  not expected in Phase 2 (one publisher per room).

### Task 5 — Rate limiting

- Smallest correct design: yes — check-then-act on `RoomCount()` / `ListenerCount()`
  is sufficient given the non-hard-guarantee stated in the ADR queue (races at
  the exact limit boundary are acceptable; we don't need strict atomic counts).
- Tests added: yes.
- Hot-path safe: yes — limit checks are in the signaling path, not the RTP path.
- Public API changed: yes — `Config` gains `MaxRooms` and `MaxListenersPerRoom`.
- New dependency: no.
- Phase scope respected: yes.
- Unsafe added: no.
- Remaining risk: check-then-act on `RoomCount()` is not atomic with
  `GetOrCreate`; at very high concurrency two requests could both pass the
  check and both create a room, temporarily exceeding the limit by 1. Acceptable
  for Phase 2; a strict atomic counter can be added in a future pass.

---

## Wave 2 — Cross-service JWT contract (2026-05-21)

- TestGiven_ApiServerToken_When_UsedForRelaySignal_Then_Accepted: PASS
- TestGiven_ApiServerToken_When_SecretMismatch_Then_BadToken: PASS
- Decision: B1 blocker RESOLVED. api-server JWT tokens accepted by relay when POCKETSTATION_JWT_SECRET matches.
- Test location: test/integration/cross_service_test.go

---

## Wave 3 — source_active push + listener decrement (2026-05-21)

- CallbackClient: best-effort POST to api-server, no-op if RELAY_API_SERVER_URL unset
- source_active pushed on PUBLISH track arrival (source joined) and source LEAVE/cleanup
- listener decrement pushed on listener LEAVE/cleanup
- New package: `internal/callback` — `Client` struct with 5s timeout, `PushSourceActive`, `PushListenerLeave`
- `Config.CallbackClient *callback.Client` added to server.Config; nil = disabled
- RELAY_API_SERVER_URL env var read in cmd/relay-server/main.go

**Tests added:**
- `TestGiven_CallbackClient_When_PushSourceActive_Then_PostSent`
- `TestGiven_CallbackClient_When_PushSourceActive_False_Then_PostSentWithFalse`
- `TestGiven_CallbackClient_When_ServerDown_Then_NoError`
- `TestGiven_CallbackClient_When_BaseURLEmpty_Then_Noop`
- `TestGiven_CallbackClient_When_PushListenerLeave_Then_PostSent`

---

## Wave 4 — NAT traversal + ICE-TCP (2026-06-01)

- `NAT1To1IPs` populated from `FLY_PUBLIC_IP` env var on startup
- ICE-TCP mux on port 8081; clients now traverse corporate firewalls
- HMAC-SHA1 TURN credentials issued by POST /v1/rooms (via api-server)
- pion/turn embedded in relay process (RELAY-023)
- Deployed to 3 Fly.io regions: iad, fra, nrt at `wss://pocketstation-relay.fly.dev`

---

## Wave 4 — Signaling extensions (2026-06-01)

- `CODEC_HINT` message type added with `CodecHintPayload` struct; `frame_ms` field included
- `KEY_EXCHANGE` message type forwarded without decryption (relay-transparent E2EE)
- `handleKeyExchange()` and `handleCodecHint()` dispatch entries added
- SFrame relay bypass verified: KEY_EXCHANGE forwarded to all room members, relay never sees plaintext frames

---

## Wave 5 — Webhook events + public channels (2026-06-02)

- Webhook events shipped: `session_started`, `session_ended`, `utterance_detected`
- Events POST to `RELAY_WEBHOOK_URL` env var (best-effort, 5 s timeout, no retry)
- `GET /v1/channels` endpoint returns public broadcast channels
- Playwright E2E suite: 6/6 tests pass against `wss://pocketstation-relay.fly.dev`

---

## Real RTP benchmark (2026-06-02, live relay)

50-subscriber fanout against live production relay:

- Packets: 5000 sent, 5000 received, 0 drops
- RTT P50: 18.9 ms, P95: 21 ms
- Measurement tool: `BenchmarkWriteRTPFanoutConnected_200` (relay bench suite)

This supersedes the Phase 1 mock-listener benchmark (discardListener). RELAY-009 real-Pion measurement gate is now satisfied.

---

## Deferred items

- `RELAY_PHASE2_QUEUE.md` items still OPEN: SLO instrumentation, latency_estimate_ms metric (RELAY-006).
- Rate limit boundary race (see Task 5 risk): acceptable for Phase 2.
- RELAY-021 RTCP adaptive codec: CODEC_HINT within 2 RTT of loss tier — Phase 5.
- RELAY-014 SFrame per-frame relay-side bypass test: KEY_EXCHANGE forwarding verified, per-frame crypto test pending.

---

---

## Wave 6 — Code quality hardening (2026-06-11)

Fixes 0 through 10 applied to `pocketstation-io/relay/`.

### Fix 0 — Remove duplicate handleKeyExchange

**What changed:** `internal/server/server.go`

Removed the duplicate `handleKeyExchange` method that had drifted into `server.go`
alongside the canonical implementation in `sframe_handler.go`. The `sframe_handler.go`
version (RELAY-014 architectural home) is the only definition.

### Staff Bar Self-Check — Fix 0: Remove duplicate handleKeyExchange

- Smallest correct design: yes — single definition in dedicated file.
- Tests added or updated: no — existing tests already covered the behaviour.
- Hot-path safe: yes — no hot-path change.
- Public API changed: no.
- New dependency: no.
- Phase scope respected: yes.
- Unsafe added: no.
- Remaining risk: none.

---

### Fix 0b — broadcast.sh health check

**What changed:** `scripts/broadcast.sh`

Replaced the `kill -0 $RELAY_PID` check (detects only the go run launcher, not
port binding) with a `wait_for_port 8080` poll (up to 15 s). Added
`--skip-relay-start` flag for when the relay is already running.

### Staff Bar Self-Check — Fix 0b: broadcast.sh health check

- Smallest correct design: yes — `nc -z` poll is portable and idiomatic.
- Tests added or updated: not applicable (shell script, no Go tests).
- Hot-path safe: not applicable.
- Public API changed: no — `--relay-url` still works; new `--skip-relay-start` is additive.
- New dependency: no.
- Phase scope respected: yes.
- Unsafe added: no.
- Remaining risk: `nc` may not be present on minimal Linux containers; fallback could use `/dev/tcp`.

---

### Step 1 — Split session.go out of server.go

**What changed:** `internal/server/server.go` → new `internal/server/session.go`

Moved `session` struct and all its methods (`run`, `cleanup`, `closeConn`,
`handleJoin`, `handleICE`, `handleLatencyReport`, `newPeerConnection`,
`send`, `sendError`, `trackSource`) into a dedicated file.
`server.go` now contains only `Server`, `New`, `Handler`, `Serve`, `Shutdown`,
route handlers, and `newID`. No behaviour change.

### Staff Bar Self-Check — Step 1: Split session.go

- Smallest correct design: yes — file split at the exact `Server` / `session` boundary.
- Tests added or updated: no — pure refactor; existing tests verify preservation.
- Hot-path safe: yes — no logic change.
- Public API changed: no.
- New dependency: no.
- Phase scope respected: yes — refactor only.
- Unsafe added: no.
- Remaining risk: none; all existing tests pass.

---

### Step 2 — Fix room.Public race

**What changed:** `internal/room/room.go`, `internal/server/session.go`,
`internal/room/public_channels_test.go`

Changed `Public bool` to `Public atomic.Bool`. All reads updated to
`.Load()`; all writes updated to `.Store(true)`. `ListPublic` uses the
atomic read under the manager's `RLock`; `handleJoin` uses `.Store(true)`.

### Staff Bar Self-Check — Step 2: room.Public race

- Smallest correct design: yes — `atomic.Bool` is the idiomatic zero-overhead solution.
- Tests added or updated: yes — test file updated to use `.Store()`/`.Load()`.
- Hot-path safe: yes — `atomic.Bool.Load` is a single atomic read, cheaper than a mutex.
- Public API changed: yes — `Room.Public` type changed from `bool` to `atomic.Bool`.
  Callers outside this package (tests, session.go) updated. No external consumers.
- New dependency: no (stdlib `sync/atomic`).
- Phase scope respected: yes.
- Unsafe added: no.
- Remaining risk: none.

---

### Step 3 — Go 1.22 ServeMux path parameters

**What changed:** `internal/server/server.go`

Replaced the manual `roomsSubrouter` string-parsing function with
`mux.HandleFunc("GET /v1/rooms/{id}/latency", s.roomLatency)`.
`roomLatency` now extracts the room ID via `r.PathValue("id")`.
`roomsSubrouter` deleted entirely. Method guard (`GET` only) is now
enforced by the pattern itself.

### Staff Bar Self-Check — Step 3: ServeMux path parameters

- Smallest correct design: yes — stdlib-only; removes 20 lines of manual parsing.
- Tests added or updated: no — existing latency tests still pass.
- Hot-path safe: yes — request parsing is not the hot path.
- Public API changed: no — HTTP API surface unchanged.
- New dependency: no.
- Phase scope respected: yes.
- Unsafe added: no.
- Remaining risk: Go 1.22+ required (go.mod declares `go 1.26`, satisfied).

---

### Step 4 — Typed error codes

**What changed:** `internal/signaling/errors.go` (new),
`internal/server/session.go`, `internal/server/sframe_handler.go`

Added `signaling.ErrorCode` type and 12 named constants. `sendError` signature
changed to `(code signaling.ErrorCode, message string)`. All call sites updated.
`ServerMessage.Code` remains `string` for JSON wire compatibility — documented
with a one-line comment in `sendError`.

### Staff Bar Self-Check — Step 4: Typed error codes

- Smallest correct design: yes — `type ErrorCode string` with constants; no registry or factory.
- Tests added or updated: no — error code values are unchanged; existing tests verify them.
- Hot-path safe: yes — cast `string(code)` is zero-cost.
- Public API changed: yes — `sendError` is an unexported method; no external impact.
- New dependency: no.
- Phase scope respected: yes.
- Unsafe added: no.
- Remaining risk: `RELAY_SHUTTING_DOWN` in Shutdown is a raw string — not a signaling error, acceptable.

---

### Step 5 — Fix silent write failure in send()

**What changed:** `internal/server/session.go`, `internal/server/server.go`,
`internal/server/codec_hint.go`, `internal/server/ice_restart.go`,
`internal/server/sframe_handler.go`

`send()` now returns `error` and logs write failures via `slog.Warn`. All call
sites updated to explicit `_ = s.send(...)`. ICE candidate callback logs
failures; read loop detects dead connection naturally.

### Staff Bar Self-Check — Step 5: Fix silent write failure

- Smallest correct design: yes — log-and-return-error is the minimal correct contract.
- Tests added or updated: no — write failure paths are exercised by shutdown tests.
- Hot-path safe: yes — `WriteJSON` is in the signaling path, not the RTP path.
- Public API changed: no — `send` is unexported.
- New dependency: no.
- Phase scope respected: yes.
- Unsafe added: no.
- Remaining risk: ICE candidate send failures are logged but the session is not torn down
  immediately; the read loop will detect the dead connection on the next read. Acceptable.

---

### Step 6 — handleKeyExchange uses RLock

**What changed:** `internal/server/sframe_handler.go`, `internal/server/server.go`

`handleKeyExchange` snapshot now uses `s.srv.mu.RLock()`/`RUnlock()` instead of
a write lock, since it only reads the sessions map. `s.srv.mu` promoted from
`sync.Mutex` to `sync.RWMutex` to enable this. Added TODO comment for Phase 3
room-owned listener map.

### Staff Bar Self-Check — Step 6: handleKeyExchange RLock

- Smallest correct design: yes — read lock is sufficient for a snapshot read.
- Tests added or updated: no — race detector covers this; key exchange test validates fanout.
- Hot-path safe: yes — KEY_EXCHANGE is rare signaling; even the write lock was acceptable.
- Public API changed: no.
- New dependency: no.
- Phase scope respected: yes.
- Unsafe added: no.
- Remaining risk: O(N) scan over all sessions is documented in TODO. Acceptable for Phase 2.

---

### Step 7 — Add missing metrics

**What changed:** `internal/metrics/metrics.go`, `internal/server/ice_restart.go`,
`internal/server/sframe_handler.go`, `internal/server/server.go`

Added `ICERestartTotal`, `WebhookErrorsTotal`, `KeyExchangeTotal` to `Registry`.
Incremented at the correct call sites. Exposed via `WritePrometheus`.
`metricsHandler` snapshots `WebhookErrorsTotal` from the dispatcher at scrape time.

### Staff Bar Self-Check — Step 7: Add missing metrics

- Smallest correct design: yes — `atomic.Int64` fields; no locking.
- Tests added or updated: no — Prometheus output format tests are not present;
  the increment paths are covered by existing integration tests.
- Hot-path safe: yes — metrics are on the signaling path, not the RTP path.
- Public API changed: yes — `Registry` gains 3 new fields. No external consumers.
- New dependency: no.
- Phase scope respected: yes.
- Unsafe added: no.
- Remaining risk: `WebhookErrorsTotal` is snapshotted at scrape time from
  `dispatcher.ErrorsTotal()`, not incremented directly in the registry.
  Both counters stay in sync as long as no parallel implementation exists.

---

### Step 8 — Log webhook delivery failures

**What changed:** `internal/webhook/webhook.go`, `internal/server/server.go`

All failure paths in `Dispatcher.Send` goroutine now log a structured
`slog.Warn("webhook delivery failed", ...)` with `event_type`, `room_id`,
`session_id`, and `error`. Added `errorsTotal atomic.Int64` to `Dispatcher`
and exposed via `ErrorsTotal()`. Relay `metricsHandler` snapshots it.

### Staff Bar Self-Check — Step 8: Log webhook delivery failures

- Smallest correct design: yes — counter on the dispatcher; no registry coupling.
- Tests added or updated: no — existing webhook tests exercise error paths;
  the new log lines don't break them.
- Hot-path safe: yes — webhook goroutines are off the audio path.
- Public API changed: yes — `Dispatcher` gains `ErrorsTotal() int64`. Additive.
- New dependency: no.
- Phase scope respected: yes.
- Unsafe added: no.
- Remaining risk: none.

---

### Step 9 — KEY_EXCHANGE fan-out integration test

**What changed:** `test/integration/key_exchange_test.go` (new)

Added `TestGiven_SourceInRoom_When_KeyExchangeSent_Then_AllListenersReceiveKey`.
Verifies: (1) 3 listeners receive the key, (2) source does not receive the key
back, (3) a 4th listener joining after the key was sent receives it immediately
(late-join path). Uses a `fanOutMessages` helper to route WebSocket messages to
separate channels for ICE handshake and KEY_EXCHANGE assertions.

### Staff Bar Self-Check — Step 9: KEY_EXCHANGE integration test

- Smallest correct design: yes — reuses existing test infrastructure
  (`newTestServer`, `doPublishHandshake`, `doSubscribeHandshake`).
- Tests added or updated: yes — new test file.
- Hot-path safe: not applicable (test code).
- Public API changed: no.
- New dependency: no.
- Phase scope respected: yes — behaviour test for existing RELAY-014 feature.
- Unsafe added: no.
- Remaining risk: `fanOutMessages` channels are buffered (64/8); in theory a
  flood of messages could overflow the buffer, but signaling is low-volume.

---

### Step 10 — Document room_id format

**What changed:** `docs/contracts/SIGNALING_PROTOCOL.md`

Added "Room creation" section with `POST /v1/rooms` response structure and
explicit `room_id` format specification: UUID v4, RFC 4122, lowercase hex with
hyphens. Clients must treat it as an opaque string.

### Staff Bar Self-Check — Step 10: Document room_id format

- Smallest correct design: yes — documentation only.
- Tests added or updated: not applicable.
- Hot-path safe: not applicable.
- Public API changed: no.
- New dependency: no.
- Phase scope respected: yes.
- Unsafe added: no.
- Remaining risk: none.

---

### Production Bar Phase Exit Self-Check (2026-06-02)

- Product flow runs end-to-end against real code: YES — 5000/5000 packets, 0 drops, live relay.
- CI honest: yes — `go test -race -short ./...` passes, no `|| true` overrides.
- Hot paths measured: YES — real Pion WriteRTP benchmark complete (P50=18.9 ms, P95=21 ms, 50 subscribers).
- Soak test run, race-clean: YES — Phase 2 soak: 30 min, delta=-1, RSS 13%, 89,435 pkts, 0 drops.
- Failure modes tested: shutdown drain, listener limit, room limit, ICE failure tests (2026-06-01).
- Observability counters live: yes.
- Playwright E2E: 6/6 PASS against live relay.
- Remaining risk: SLO instrumentation and latency_estimate_ms not yet wired (open in queue).
