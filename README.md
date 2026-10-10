# PocketStation Relay

PocketStation Relay provides Session access, readable join links and finite,
source-aware WebRTC audio delivery. It carries independent application,
microphone, caller and generated-audio buses to native or browser receivers.
Capture, recording and model connectors remain separate owners. Relay owns
Session access rules; managed control supplies persistence and orchestration.

```text
authenticated source attachment
              ↓
       one RelaySession
              ↓
    named AudioBus + generation
              ↓
 finite BusSubscription fan-out
              ↓
 RTP continuity, pacing, repair, and observations
```

An `AudioBus` keeps a stable semantic identity while a transient publisher
attachment, SSRC, and source generation may change. A subscriber selects one
bus or the declared `mix` output.

## Standalone and managed operation

Standalone is the default. Relay includes Session creation, owner and per-bus
credentials, readable links, one-use grant redemption, observation events and
revocation. Memory requires no database. SQLite retains logical state across
restarts when `POCKETSTATION_STORAGE=sqlite` and `POCKETSTATION_SQLITE_PATH` are
set. Stored observations are cleared until live media reports again.

In managed operation, the control plane invokes the same Relay-owned `access`
module with its selected storage adapter. It does not implement a second grant
or signing policy. The separate media process uses authenticated internal calls:

```text
RELAY_AUTHORITY_MODE=control-plane
RELAY_API_SERVER_URL=https://control.example.com
PUBLIC_CONTROL_PLANE_URL=https://control.example.com
POCKETSTATION_JWT_SECRET=<shared Session signing and verification secret>
POCKETSTATION_INTERNAL_SECRET=<shared internal request secret>
```

`RELAY_API_SERVER_URL` stays internal. `GET /.well-known/pocketstation` advertises
`schema_version: 1` and the public Session-service `authority_url` in both
modes. Standalone advertises `PUBLIC_RELAY_URL`; managed operation advertises
`PUBLIC_CONTROL_PLANE_URL` and retains the deprecated `control_plane_url` field.
A configured public address must use HTTPS, except HTTP loopback development.

Both modes use one credential profile. New Sessions pin the Relay issuer;
explicitly migrated Sessions can retain their recorded legacy issuer. The media
server fetches the trusted Session profile before validating signature, issuer,
audience, type, role, bus scope and incarnation. It acquires an ordered media
placement term before allocating transport state. A displaced process cannot
replace the newer process's observations.

Managed media rejects local Session/join mutations: clients use the advertised
Session service. A missing Session or unavailable service prevents admission.
`CONTROL_AUTHORITY_TIMEOUT_MS` can reduce the default five-second admission
bound. Reconciliation detects deletion after admission and closes media.
`/healthz` reports process liveness; `/readyz` also checks the Session service.

Readable names such as `rice-river` carry no permission by themselves. A shared
URL includes the opaque credential in its fragment, and redemption uses POST.
There is no separate invitation signing secret or name-based permission.
See [Session access and storage](docs/access-service.md) for retry, expiry,
renewal, recovery, adapter guarantees and finite capacity.

Deploy endpoints you own. Historical PocketStation Fly demonstration endpoints
are rate-limited and do not establish a hosted-service guarantee or SLA.

## Publish named buses

A source capability lists every bus the publisher may attach. One signaling
connection can declare multiple independent tracks:

```json
{
  "type": "PUBLISH",
  "token": "<source capability>",
  "publish_buses": [
    {"stream_id":"application","bus_id":"application"},
    {"stream_id":"microphone","bus_id":"microphone"}
  ],
  "sdp_offer": "..."
}
```

Every declared bus must be inside the token scope. Stream IDs and bus IDs must
be unique. Relay rejects ambiguous, oversized, malformed, or out-of-scope
declarations before media attachment.

For one track, send an explicit `bus_id` instead.

## Receive a bus

A subscriber capability contains exactly one `bus_id`. Use it with WebSocket
signaling or WHEP:

```http
POST /v1/sessions/{session_id}/whep?bus=application
Authorization: Bearer <subscriber capability>
Content-Type: application/sdp
```

The URL bus cannot exceed the token scope. `mix` is a declared virtual output;
it is not an unrestricted wildcard.

## Control-state reconciliation

Relay sends one complete state document for every accepted attachment change:

```json
{
  "contract_version": 1,
  "session_id": "16d2491c-86ef-4a86-9ba7-af1d2d246244",
  "relay_epoch": "f78124e8-...",
  "revision": 7,
  "writer_term": 1,
  "observed_at": "2026-08-21T17:45:00Z",
  "buses": [
    {"bus_id":"application","role":"application","source_active":true,"source_generation":2}
  ],
  "subscriptions": [
    {"subscriber_id":"a64a0c10-...","bus_id":"application"}
  ]
}
```

The callback is authenticated and size-limited. A full snapshot replaces all
Relay-owned state, so duplicate delivery is safe and callback loss is repaired
by periodic reconciliation. Reconciliation resends the current revision; it
does not manufacture a new transition.

State-change notification uses an atomic, nonblocking handoff. It does not add
a lock, allocation, network call, or log operation to RTP forwarding.

## Run locally

You need Go 1.26 or newer and a host that permits UDP.

Control-plane mode:

```bash
POCKETSTATION_JWT_SECRET=development-source-secret-32-bytes \
POCKETSTATION_INTERNAL_SECRET=development-internal-secret-32-bytes \
RELAY_AUTHORITY_MODE=control-plane \
RELAY_API_SERVER_URL=http://127.0.0.1:4801 \
PUBLIC_CONTROL_PLANE_URL=http://127.0.0.1:4801 \
go run ./cmd/relay-server
```

Standalone mode:

```bash
POCKETSTATION_JWT_SECRET=development-source-secret-32-bytes \
PUBLIC_RELAY_URL=http://127.0.0.1:4800 \
PUBLIC_RECEIVER_URL=http://127.0.0.1:4173 \
go run ./cmd/relay-server
```

The deterministic `relay-test-source` fixture can publish one named test bus
with explicit credentials:

```bash
go run ./cmd/relay-test-source -- \
  --relay http://127.0.0.1:4800 \
  --session <session_id> \
  --bus application \
  --token <source_capability> \
  --duration 3s
```

When both `--session` and `--token` are omitted, the fixture creates a temporary
standalone Session. Managed operation requires both. It emits valid synthetic
Opus for transport verification; it is not physical capture evidence.

### Optional ICE-TCP

To expose Relay's existing IPv4 ICE-TCP listener, add these variables to the
chosen startup command and permit both ports in the deployment:

```bash
ICE_UDP_PORT=4802 ICE_TCP_PORT=4803 go run ./cmd/relay-server
```

Keep the authentication and Session-service variables from the selected mode above.
In this source checkout, the default Relay-owned peer settings gather UDP
candidates and passive TCP4 host candidates when `ICE_TCP_PORT` is enabled.
Without a TCP mux, Pion's default UDP network types remain unchanged. A caller
that supplies a `SettingEngine` or signaling `API` retains its network policy;
it must enable TCP explicitly if needed.

For a known Pion TCP mux bound to an IPv4 listener, those default settings reject
IPv6 TCP requests before they reach the mux. UDP6 remains enabled. Custom and
multiport muxes keep their own behavior and interfaces.

ICE-TCP is a separate WebRTC listener, not HTTPS, WSS or an HTTP CONNECT proxy.
It does not enable TURN. TCP6 support is not qualified. Local candidate tests qualify configuration
only; deployed connectivity, browser audio decoding and off-host receivers
require their own evidence. See [ICE configuration](docs/reference/configuration.md).

## Configure capacity before accepting traffic

The server defaults are development values, not sizing recommendations:

| Resource | Environment variable | Default |
|---|---|---:|
| active RelaySessions | `RELAY_MAX_ROOMS` | 100 |
| subscribers per RelaySession | `RELAY_MAX_SUBSCRIBERS_PER_SESSION` | 50 |
| AudioBuses per RelaySession | `RELAY_MAX_BUSES_PER_SESSION` | 16 |
| concurrent signaling and WHIP/WHEP handshakes | `RELAY_MAX_CONCURRENT_HANDSHAKES` | 128 |
| standalone RelaySession creation per client IP per minute | `MAX_ROOMS_PER_IP_PER_MINUTE` | 10 |
| inactive RelaySession lifetime | `ROOM_EXPIRY_MINUTES` | 30 minutes |
| publisher reconnect window | `SOURCE_RECONNECT_WINDOW_SEC` | 60 seconds |

Each receiver pacer holds 32 RTP packets and discards packets older than 120
milliseconds. Relay counts queue-full and age-related drops. When an admission
limit is reached, it rejects new work with an HTTP or signaling error instead
of growing resource use without a limit.

Set these values from the expected audience, host memory, CPU, bandwidth, and
TURN allocation budget. Read
[capacity and recovery](docs/operations/capacity-and-recovery.md) for metrics,
reconnect behavior, shutdown, and the measurements required for a network
claim.

## Secure a public deployment

- Store JWT, internal synchronization, TURN, and webhook secrets in the
  deployment secret manager.
- Serve public HTTP and signaling endpoints over TLS.
- Set `ALLOWED_ORIGINS` to the exact HTTPS origins hosting browser receivers.
- Restrict `/metrics` with a private network, proxy, or ingress policy.
  Session diagnostic endpoints require an exact-Session owner or subscriber
  capability; packet logs also enforce bus scope.
- Treat single-use invitation codes as credentials until redeemed or expired.
- Plan coordinated secret rotation around the remaining capability lifetime.

WebRTC uses DTLS-SRTP. Optional SFrame messages provide separate end-to-end
media encryption only when both clients implement the same key exchange.

Read [security](docs/operations/security.md) before exposing Relay publicly.

## Finite work

Relay bounds:

- RelaySessions and AudioBuses per Session;
- subscriptions per Session;
- concurrent signaling and WHIP/WHEP handshakes;
- pending invitations in standalone mode;
- control-state notifications;
- callback duration and response size;
- control-plane admission duration;
- packet queues, repair caches, and packet age.

When capacity is unavailable, Relay rejects new work before allocating media
resources. It returns an explicit capacity response and does not grow an unbounded
retry or callback queue. Operators should set limits for their own budget and
expected audience; the repository's `fly.toml` intentionally describes only a
small demonstration deployment.

Deployment resource settings are not a billing ceiling. Measure host usage and
check your provider’s current prices and network charges before deployment.

For deployment instructions and endpoint references, see the
[Relay documentation](docs/README.md).

## Observe a RelaySession

`GET /metrics` exposes Prometheus metrics for active RelaySessions,
subscribers, forwarded and dropped RTP packets, handshakes, ICE restarts,
callbacks, webhooks, and key exchange.

Inspect one affected RelaySession with:

```text
GET /v1/sessions/{id}/health
GET /v1/sessions/{id}/latency
GET /v1/sessions/{id}/media-debug
GET /v1/sessions/{id}/packet-log?bus={bus_id}&limit=100
```

The packet-log endpoint returns at most 1,000 records. Restrict these endpoints
before public deployment.

These observations describe Relay and WebRTC behavior. They do not prove the
exact sample a loudspeaker played or what a person heard.

## Published limits

Repository and same-host tests establish component and local integration
behavior. They do not establish every NAT topology, cross-network latency,
multi-region behavior, or production load.

PocketStation's Fly deployment is a small, rate-limited environment used by
the installed Python example. It is not Relay's default configuration, a
hosted product, or an SLA. Operate Relay and the Control Plane under endpoints
and limits you control for an application deployment.

## Continue from the task you have

| Task | Guide |
|---|---|
| Start a local Relay | [Run Relay locally](docs/getting-started/run-relay.md) |
| Configure every environment variable | [Configuration](docs/reference/configuration.md) |
| Secure credentials and browser access | [Security](docs/operations/security.md) |
| Set capacity and recover from failures | [Capacity and recovery](docs/operations/capacity-and-recovery.md) |
| Implement a client | [WebSocket signaling](docs/reference/signaling.md) |
| Inspect HTTP, WHIP, WHEP, and diagnostics | [HTTP and WebRTC](docs/reference/http-and-webrtc.md) |

## Verify a change

```bash
scripts/check-code-protocol.sh
go test -race ./...
```

CI must pass before Fly deploys. Deployment checks out the exact successful CI
revision and records it in the OCI image. A successful local or same-host test
does not establish WAN/TURN or multi-region performance.

See [the signaling specification](docs/contracts/SIGNALING_PROTOCOL.md) for the wire
protocol and failure model.

## License

PocketStation Relay is available under the MIT license.
