# RELAY-014 — Phase 1 Token Authority

## Status

Accepted 2026-05-20.

## Context

Both `pocketstation-io/relay` and `pocketstation-io/api-server` expose `POST /v1/rooms`. They produce incompatible token formats:

- relay issues HS256 JWT tokens signed with `POCKETSTATION_JWT_SECRET` via `golang-jwt/v5`. Tokens carry `room_id` and `role` claims with a configurable TTL.
- api-server issues opaque 64-character hex strings. These are not JWTs.

The relay's `/v1/signal` WebSocket handler validates every incoming token as a JWT. Presenting an api-server hex token causes `jwt.ErrTokenMalformed`, which the relay rejects with a `bad_token` error and closes the WebSocket connection.

There is no ambiguity in the failure mode: the formats are structurally incompatible at the parse step, before any claim check.

## Decision

For Phase 1, the relay owns all room creation and JWT issuance. The relay's `POST /v1/rooms` is the single authoritative source of tokens for relay signaling.

- Clients and the fake-source tool connect directly to the relay for room creation.
- api-server is a control-plane stub in Phase 1. Its tokens are not valid for relay signaling and are not presented to relay endpoints.
- This incompatibility is documented Phase 1 behavior, not a bug.

## Options considered

**Option A — Accept both formats at relay /v1/signal (selected against).**
Relay would need to detect the token format (JWT prefix vs hex length), branch on format, and maintain two validation paths. This couples relay to api-server's token format and adds complexity before the formats are stable.

**Option B — api-server issues JWT tokens in Phase 1 (selected against).**
Would require api-server to import `auth.Sign` and share the secret before the cross-service contract is designed. This leaks relay implementation details into api-server and creates an untested dependency in Phase 1.

**Option C — Relay owns token issuance for Phase 1; api-server upgraded in Phase 2 (selected).**
Cleanest boundary. Phase 1 has a single authoritative token issuer. Phase 2 upgrades api-server to call `auth.Sign(sharedSecret, id, role, ttl)` with the same `POCKETSTATION_JWT_SECRET`. A cross-service integration test is required before Phase 2 exit. Documented as RELAY-015 on the api-server side.

## Consequences

- Any client presenting an api-server hex token to relay `/v1/signal` receives a `bad_token` error. This is documented Phase 1 behavior.
- The relay's `POST /v1/rooms` endpoint is the canonical room creation path for all Phase 1 clients, including the fake-source binary.
- Phase 2 must not exit until api-server JWT upgrade is complete and a cross-service integration test (`TestGiven_RelayRoom_When_ApiServerToken_Then_Accepted` or equivalent) passes. See RELAY-015 in api-server.
- Reversal requires the Phase 2 api-server JWT upgrade to be complete and the cross-service integration test to pass.

## Test / measurement plan

- `TestGiven_RelayRoom_When_TokenUsedForSignal_Then_Accepted` in `test/integration/relay_test.go` verifies the full token flow: POST /v1/rooms → JWT → WebSocket PUBLISH with that token → SDP_ANSWER received.
- Auth unit tests in `internal/auth/token_test.go` (6 tests) cover: happy path, wrong secret, expired token, listener role, malformed token, empty token.

## Phase 2 resolution

Resolved 2026-05-21. The cross-service integration test now gates Phase 2 exit:

- `TestGiven_ApiServerToken_When_UsedForRelaySignal_Then_Accepted` — confirms that a token
  minted by api-server (HS256, `Claims{RoomID, Role}`, shared `POCKETSTATION_JWT_SECRET`)
  is accepted by relay `/v1/signal` and results in an SDP_ANSWER.
- `TestGiven_ApiServerToken_When_SecretMismatch_Then_BadToken` — confirms that a
  mismatched-secret token is rejected with ERROR code `bad_token`.

Both tests are in `test/integration/cross_service_test.go` and pass with `-race`.
The reversal trigger below is satisfied.

## Reversal trigger

Phase 2 api-server JWT upgrade is complete and a cross-service integration test (relay token issuance by api-server, accepted by relay `/v1/signal`) passes in CI.
