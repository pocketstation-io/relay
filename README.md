# relay

**Organization:** `pocketstation-io`  
**Repository:** `pocketstation-io/relay`  
**v2.3 tier:** Tier 3 — Server Services  
**Current phase:** Phase 2 (activated Phase 1; Phase 1 COMPLETE 2026-05-20)  
**Language/package:** Go + Pion v4  
**Release strategy:** SemVer independent; Docker image on tag

This is an independently releasable PocketStation v2.3 repository folder. It is not meant to be merged permanently into a monorepo.

Agents must respect the phase gate in `docs/REPO_CONTRACT.md`.

---

## Phase 1 Token Authority Decision

**Decision (2026-05-20): relay owns room creation and JWT issuance for Phase 1.**

The relay's `POST /v1/rooms` endpoint creates rooms and issues two JWTs (HS256, shared
secret via `POCKETSTATION_JWT_SECRET`):

- `source_token` — role `source`, TTL 15 minutes
- `listener_token` — role `listener`, TTL 2 hours

These tokens are accepted by the relay's `POST /v1/signal` WebSocket endpoint via
`internal/auth.Verify`.

The api-server (`POST /v1/rooms` on port 8090) issues opaque hex bearer tokens that are
**not** accepted by the relay's JWT verifier. In Phase 1 the api-server is a separate
control-plane stub. Clients and the fake-source tool connect directly to the relay for
room creation.

**Phase 2 resolution:** api-server will be updated to call `auth.Sign` with the shared
secret and issue compatible JWTs, making it a true front-end proxy for the relay. This
will be validated by an integration test that calls api-server's `/v1/rooms` and uses the
returned token with relay's `/v1/signal`.

**Integration test:** `test/integration/relay_test.go`
`TestGiven_RelayRoom_When_TokenUsedForSignal_Then_Accepted` proves that a token issued
by `POST relay/v1/rooms` is accepted by `POST relay/v1/signal`.
