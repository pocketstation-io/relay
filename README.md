# PocketStation Relay

Move live audio across a network without losing which source it came from.

PocketStation Relay is an open-source, source-aware WebRTC audio plane. It
keeps application audio, microphones, and generated audio on independently
addressable `AudioBus` paths, so a receiver can subscribe to one source or an
intentional mix instead of inheriting an anonymous combined track.

Use Relay when a PocketStation Session needs to deliver live audio to a browser,
a remote process, or another machine. Capture, processing, recording, and model
execution remain outside this service.

```text
PocketStation Session
  ├── application stem ──→ application AudioBus ──┐
  ├── microphone stem  ──→ microphone AudioBus  ──┼─→ browser or native receiver
  └── generated audio  ──→ agent AudioBus       ──┘
```

## Why Relay exists

General-purpose conferencing systems usually optimize around participants and
playout tracks. PocketStation starts from a different unit: a named audio source
with identity, attachment generation, and capture-clock continuity.

Relay gives that model a bounded network path:

- one authenticated publisher can attach a finite set of named buses;
- each incoming WebRTC stream is bound to exactly one declared `AudioBus`;
- subscribers choose a bus or the intentional `mix` view;
- a bus keeps stable meaning when its transient SSRC or source attachment
  changes;
- pacing, RTP continuity, NACK repair, and transport observations are handled
  in the media plane;
- admission limits reject excess work before unbounded queues form.

Relay does not claim to invent WebRTC, WHIP, RTP forwarding, or named tracks.
Its value is the coherent source-aware contract across those mechanisms.

## Run it locally

Requirements: Go 1.26 or newer and a UDP-capable local network.

```bash
export POCKETSTATION_JWT_SECRET="replace-this-development-secret"
go run ./cmd/relay-server
```

The server listens on `http://localhost:4800` by default.

```bash
curl -sf http://localhost:4800/healthz
curl -sf http://localhost:4800/metrics
curl -sS -X POST http://localhost:4800/v1/sessions
```

Creating a Session returns:

- `session_id` — the stable RelaySession identity;
- `source_token` — permission to publish;
- `subscriber_token` — permission to receive;
- `join_url` — an opaque browser invitation;
- `ice_servers` — when embedded TURN is configured.

Publishers normally connect through the
[`pocketstation-relay`](https://crates.io/crates/pocketstation-relay) Rust
connector. Browser receivers resolve the invitation, open `/v1/signal`, and
subscribe to the selected bus. The complete public wire contract is in
[`SIGNALING_PROTOCOL.md`](docs/contracts/SIGNALING_PROTOCOL.md).

## How it works

```text
authenticated attachment
  → RelaySession
      → named AudioBus
          → source generation + SSRC + capture clock
          → bounded BusSubscription fan-out
              → RTP translation + pacing + repair
                  → native or browser receiver
```

The control and data planes have different responsibilities:

| Plane | Owns |
|---|---|
| Control | Session creation, credentials, invitations, admission, signaling, callbacks, health, metrics |
| Media | source attachment, named buses, bounded subscriptions, RTP/RTCP continuity, pacing, repair |

A complete RelaySession stays on one relay instance. Cross-node Session
splitting and live multi-region migration are not part of the current contract.

## Public interfaces

| Interface | Purpose |
|---|---|
| `POST /v1/sessions` | Create a RelaySession and credentials |
| `POST /v1/sessions/{id}/invitations` | Create a receiver invitation after the source is active |
| `GET /v1/join/{code}` | Resolve an invitation into subscriber credentials |
| `GET /v1/signal` | WebSocket signaling and source-aware Session events |
| `POST /v1/sessions/{id}/whip` | Standards-based WebRTC ingest |
| `POST /v1/sessions/{id}/whep` | HTTP-based WebRTC egress |
| `PATCH /v1/connections/{id}` | Trickle ICE for an HTTP-signaled connection |
| `DELETE /v1/connections/{id}` | Close an HTTP-signaled connection |
| `GET /healthz` | Liveness and shutdown state |
| `GET /metrics` | Prometheus text exposition |

The WebSocket path carries PocketStation-specific capabilities such as
multi-bus publication, Session state, codec hints, latency reports, and opaque
SFrame key forwarding. WHIP/WHEP provide a narrower interoperable HTTP path.

## Deploy it

Relay is self-hosted software. There is no public PocketStation-hosted Relay
endpoint in this repository.

For a reachable deployment, configure:

1. a strong `POCKETSTATION_JWT_SECRET` shared only with the credential issuer;
2. `PUBLIC_RELAY_URL` and `PUBLIC_RECEIVER_URL` for invitations;
3. `RELAY_PUBLIC_IPS` or `RELAY_PUBLIC_IPS=auto` where NAT requires advertised
   public candidates;
4. embedded TURN or an equivalent reachable ICE path for restrictive networks;
5. explicit admission limits sized from measured capacity;
6. `ALLOWED_ORIGINS` when browser signaling must be origin-restricted.

The insecure JWT fallback and open-origin behavior are development defaults,
not production settings. Fly.io configuration is included as one deployment
example; the binary is not tied to Fly.

## Configuration

| Variable | Purpose | Default |
|---|---|---|
| `PORT` | HTTP/WebSocket listen port | `4800` |
| `POCKETSTATION_JWT_SECRET` | HS256 credential secret | insecure development fallback |
| `PUBLIC_RELAY_URL` | Public relay origin used in invitations | inferred from request |
| `PUBLIC_RECEIVER_URL` | Browser receiver origin | server default |
| `ALLOWED_ORIGINS` | Comma-separated WebSocket/browser origins | all origins |
| `RELAY_MAX_ROOMS` | Maximum active RelaySessions | bounded package default |
| `RELAY_MAX_LISTENERS_PER_ROOM` | Maximum subscribers per RelaySession; legacy variable name | bounded package default |
| `RELAY_MAX_BUSES_PER_SESSION` | Maximum retained buses per RelaySession | `16` |
| `MAX_ROOMS_PER_IP_PER_MINUTE` | Session-creation admission limit per client IP | bounded package default |
| `RELAY_MAX_CONCURRENT_HANDSHAKES` | Handshakes allowed to hold media resources | `128` |
| `RELAY_MAX_CONCURRENT_CALLBACKS` | Concurrent control-plane callback deliveries | `32` |
| `ROOM_EXPIRY_MINUTES` | Inactive Session expiry; legacy variable name | `30` |
| `SOURCE_RECONNECT_WINDOW_SEC` | Source reconnection window | `60` |
| `ICE_UDP_PORT` | Shared ICE UDP port; zero selects an ephemeral port | `0` |
| `ICE_TCP_PORT` | Shared ICE TCP port; zero disables it | `0` |
| `RELAY_PUBLIC_IPS` | Comma-separated advertised IPs or `auto` | unset |
| `TURN_PUBLIC_IP` | Enables embedded TURN and advertises this IP | unset |
| `TURN_UDP_PORT` / `TURN_TCP_PORT` | Embedded TURN ports | `3478` |
| `TURN_TLS_PORT` | Embedded TURNS port; zero disables TLS | `0` |
| `RELAY_ENABLE_RED` | Enable RFC 2198 RED wrapping | disabled |
| `RELAY_API_SERVER_URL` | Optional control-plane callback origin | disabled |
| `WEBHOOK_URL` | Optional lifecycle webhook target | disabled |
| `SHUTDOWN_GRACE_PERIOD_SEC` | Graceful shutdown deadline | `30` |

## Operational guarantees and limits

Relay is designed around finite ownership:

- Session, bus, subscriber, handshake, callback, packet, and retransmission
  resources are bounded;
- reconnects create a new source attachment generation instead of silently
  reusing transient transport identity;
- shutdown stops admission before closing active peers;
- health and transport metrics are available without entering the media path;
- library and server logs never constitute the source of protocol truth.

Current limitations are equally explicit:

- full PocketStation per-frame `FrameLineage` is not yet serialized to every
  remote receiver;
- one RelaySession is owned by one process;
- WHEP remains an evolving ecosystem contract;
- production scale, remote-device behavior, and competitive performance must
  be established by workload-specific evidence, not inferred from unit tests.

## Repository map

```text
cmd/relay-server/       process composition and lifecycle
cmd/relay-test-source/  local interoperability client
internal/auth/          credentials and roles
internal/admission/     finite admission controls
internal/signaling/     private Go projection of the public wire contract
internal/session/       RelaySession, AudioBus, source and subscriptions
internal/media/         clock lineage, downlink, pacing, repair and RED
internal/server/        HTTP, WebSocket, WebRTC and WHIP/WHEP composition
internal/metrics/       transport and capacity observations
test/                   integration, stress and bounded soak cells
```

`pocketstation-io/protocol` owns cross-language schemas.
[`pocketstation-io/connectors`](https://github.com/pocketstation-io/connectors)
owns the Rust client connector. PocketStation Core remains provider-neutral.

## Verify a change

```bash
test -z "$(gofmt -l .)"
go vet ./...
go test -short ./...
go test -race ./...
scripts/check-code-protocol.sh
```

The race suite includes bounded local stress/soak cells. Long-duration evidence
is run only for an explicitly frozen endurance candidate.

## Evidence boundary

- The relay transport is implemented and tested on its named path.
- Same-host Pion, browser, and CI runs are component or loopback evidence.
- A cloud deployment demonstrates cross-network reachability, not automatic
  production readiness or multi-region failover.
- Performance comparisons require equivalent codecs, packet durations, network
  conditions, receiver layers, and measured units.
- End-to-end product claims require PocketStation Lab artifacts that identify
  exact commits, devices, and network conditions.

Relay is intentionally narrower than a conferencing platform: it is the
source-aware network continuation of a PocketStation Session.
