# RELAY_PHASE2_PROGRESS.md

Phase 2 relay hardening. Started 2026-05-20.

---

## Tasks completed (2026-05-20)

### Task 1 — Copy-on-write listener slice (ADR-005)

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
- Phase scope respected: yes — ADR-005 Phase 2.
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

## Deferred items

- `RELAY_PHASE2_QUEUE.md` items still OPEN: SLO instrumentation, latency metric,
  api-server JWT compat, source_active push, Connected WriteRTP bench, failure
  mode tests.
- `Serve` return-value signature change: `cmd/relay-server/main.go` updated;
  no other callers (tests use `Handler()` not `Serve()`).
- Rate limit boundary race (see Task 5 risk): acceptable for Phase 2.

---

### Production Bar Phase Exit Self-Check (partial — Phase 2 not complete)

Tasks 1–5 are the hardening pass. Full phase exit requires remaining P1 items.

- Product flow runs end-to-end against real code: yes (integration tests pass).
- CI honest: yes — `go test -race -short ./...` passes, no `|| true` overrides.
- Hot paths measured: partial — ADR-009 benchmark present; real Pion WriteRTP
  measurement deferred to Connected WriteRTP bench task.
- Soak test run, race-clean: yes (`test/soak` passes with `-short`).
- Failure modes tested: shutdown drain, listener limit, room limit.
- Observability counters live: yes (same as Phase 1).
- Remaining risk: see deferred items above.
