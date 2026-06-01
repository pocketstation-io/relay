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

---

## Deployment

### Multi-region Fly.io deploy (Phase 6)

The relay runs on Fly.io edge nodes across three regions: EU, US, and APAC. The origin
audio relay is hosted on Hetzner CX23; Fly.io handles the 30+ city edge layer with
pay-per-second billing.

**Region topology** (documented in `fly.regions.toml`):

| Region | Fly.io codes             |
|--------|--------------------------|
| EU     | `fra` (Frankfurt), `ams` (Amsterdam) |
| US     | `iad` (Ashburn), `lax` (Los Angeles) |
| APAC   | `nrt` (Tokyo), `sin` (Singapore) |

**Prerequisites:**

- `flyctl` installed (`brew install flyctl` or https://fly.io/docs/getting-started/installing-flyctl/)
- `FLY_API_TOKEN` set in your environment
- Authenticated: `fly auth login`

**Initial deploy:**

```bash
fly launch --no-deploy   # first-time only, creates the app
./scripts/deploy-multi-region.sh
```

`deploy-multi-region.sh` runs three commands in sequence:

1. `fly deploy --remote-only` — builds and pushes the Docker image via Fly builders
2. `fly scale count 2 --region fra,iad,nrt` — places two instances in each primary region
3. `fly status` — confirms all instances are running

**Continuous deployment:**

`.github/workflows/deploy.yml` triggers on every push to `main`. It installs `flyctl`
and runs `fly deploy --remote-only` using the `FLY_API_TOKEN` repository secret.

Set the secret once:

```bash
fly secrets set FLY_API_TOKEN="$(fly auth token)"
# or via GitHub: Settings → Secrets → Actions → New repository secret
```

**Environment variables** required at runtime (set with `fly secrets set`):

| Variable | Description |
|----------|-------------|
| `POCKETSTATION_JWT_SECRET` | HS256 shared secret for room JWTs |
| `RELAY_API_SERVER_URL` | Optional — api-server callback URL |
| `TURN_PUBLIC_IP` | Public IP for embedded TURN (omit for STUN-only mode) |

**Adding a region:**

```bash
fly regions add sin
fly scale count 2 --region sin
```
