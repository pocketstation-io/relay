# PocketStation Relay

Stream independently named application, microphone, caller, or generated-audio
buses to native and browser receivers over WebRTC.

```text
authenticated publisher
        │
        ├─ application AudioBus ── selected receivers
        ├─ microphone AudioBus ─── selected receivers
        └─ assistant AudioBus ──── selected receivers
```

Relay forwards live media. It does not capture desktop audio, run models, write
recordings, or store durable application state. PocketStation Core captures and
routes audio. The Control Plane creates RelaySessions and receiver invitations
when a deployment uses control-plane mode.

An `AudioBus` is a name such as `application`, `microphone`, or `assistant`.
The name stays stable when a publisher reconnects; `source_generation`
identifies the new attachment. A receiver is authorized for one bus or for a
declared `mix` output.

## Start Relay locally

You need Go 1.26 or newer and UDP access on the host.

Generate development credentials and start standalone mode:

```bash
export POCKETSTATION_JWT_SECRET="$(openssl rand -hex 32)"
export RELAY_INVITATION_SECRET="$(openssl rand -hex 32)"
export RELAY_AUTHORITY_MODE="standalone"
go run ./cmd/relay-server
```

Relay listens on `http://127.0.0.1:4800` by default. In another terminal:

```bash
curl --fail http://127.0.0.1:4800/healthz
```

The response is `ok`. This confirms that the HTTP server is accepting work; it
does not confirm ICE connectivity or media delivery.

Publish three seconds of synthetic Opus with the repository test source:

```bash
go run ./cmd/relay-test-source -- \
  --relay http://127.0.0.1:4800 \
  --duration 3s
```

The command creates a temporary RelaySession and prints the session, bus, and
credentials. The fixture checks transport behavior. It is not desktop capture
or physical-device evidence.

Stop Relay with `Ctrl-C`. The default process shutdown deadline is 30 seconds;
HTTP work receives a five-second drain interval inside that deadline.

Continue with the [local setup guide](docs/getting-started/run-relay.md) when a
receiver or Control Plane will join the test.

## Choose how RelaySessions are created

Relay supports two explicit operating modes.

### Control-plane mode

Use this mode when an application service owns RelaySession creation,
required-bus readiness, receiver invitations, and scoped credentials.

```bash
export RELAY_AUTHORITY_MODE="control-plane"
export RELAY_API_SERVER_URL="https://control.example.com"
export POCKETSTATION_JWT_SECRET="<shared capability secret>"
export POCKETSTATION_INTERNAL_SECRET="<state synchronization secret>"
go run ./cmd/relay-server
```

The Control Plane signs source and receiver capabilities. Relay verifies their
issuer, audience, token type, role, expiry, RelaySession ID, and AudioBus scope.
Relay sends complete attachment-state snapshots back to the Control Plane after
changes and at the configured reconciliation interval.

Relay rejects local RelaySession and invitation creation endpoints in this
mode.

### Standalone mode

Use standalone mode for a self-contained deployment or protocol development.
Relay creates temporary RelaySessions and single-use invitations itself. It
uses `RELAY_INVITATION_SECRET` to sign receiver capabilities.

Credentials from one mode are not accepted in the other. Choose one mode for a
deployment; do not configure fallback verification with the other issuer or
secret.

## Publish named buses

A source capability lists the buses a publisher may attach. One signaling
connection can publish several WebRTC tracks:

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

Every bus must be included in the capability. Stream IDs and bus IDs must be
unique. Relay rejects missing credentials, ambiguous declarations, oversized
messages, malformed SDP, and buses outside the token scope before attaching
media.

Rust applications can use the
[`pocketstation-relay` Connector](https://github.com/pocketstation-io/connectors/tree/main/relay)
instead of implementing signaling, Opus, RTP, and WebRTC publication.

## Receive one selected bus

A receiver capability identifies exactly one RelaySession and AudioBus. Use it
with WebSocket signaling or WHEP:

```http
POST /v1/sessions/{session_id}/whep?bus=application
Authorization: Bearer <receiver capability>
Content-Type: application/sdp
```

The bus in the URL must match the capability. `mix` is an explicitly declared
output, not permission to subscribe to every source.

For all HTTP endpoints and authentication requirements, read
[HTTP and WebRTC](docs/reference/http-and-webrtc.md). For every signaling
message, field limit, and error code, read
[WebSocket signaling](docs/reference/signaling.md).

## Keep the Control Plane synchronized

Relay sends one complete state document after an accepted attachment change:

```json
{
  "contract_version": 1,
  "session_id": "16d2491c-86ef-4a86-9ba7-af1d2d246244",
  "relay_epoch": "f78124e8-...",
  "revision": 7,
  "observed_at": "2026-08-21T17:45:00Z",
  "buses": [
    {"bus_id":"application","role":"application","source_active":true,"source_generation":2}
  ],
  "subscriptions": [
    {"subscriber_id":"a64a0c10-...","bus_id":"application"}
  ]
}
```

A later complete snapshot replaces earlier Relay-owned state. Duplicate or
reordered revisions are safe to acknowledge without replaying mutations. A new
`relay_epoch` identifies a Relay process restart.

State-change notification enters a fixed-capacity mailbox without a lock,
allocation, network request, or log call in RTP forwarding. If a notification
is missed, periodic reconciliation sends the latest complete state again.

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
- Restrict `/metrics` and RelaySession diagnostic endpoints with a private
  network, proxy, or ingress policy; Relay does not add administrator auth to
  them.
- Treat single-use invitation codes as credentials until redeemed or expired.
- Plan coordinated secret rotation around the remaining capability lifetime.

WebRTC uses DTLS-SRTP. Optional SFrame messages provide separate end-to-end
media encryption only when both clients implement the same key exchange.

Read [security](docs/operations/security.md) before exposing Relay publicly.

## Configure ICE and TURN

Set `ICE_UDP_PORT` and expose that UDP port for a public Relay. Use
`RELAY_PUBLIC_IPS` when the server must advertise a public address. Optional
ICE-TCP uses `ICE_TCP_PORT`.

Set `TURN_PUBLIC_IP` and a 32-byte-or-longer `TURN_SHARED_SECRET` to enable the
embedded TURN server. Expose the selected TURN listeners and the complete UDP
allocation port range. Authentication can succeed while media still fails if
firewalls block allocation ports.

The complete environment-variable reference is in
[Relay configuration](docs/reference/configuration.md).

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
go test -race -short ./...
go test -race ./internal/server ./test/integration
```

CI must pass before deployment. The deploy workflow checks out the successful
revision and records it in the OCI image. Local or same-host tests do not
establish WAN/TURN or multi-region performance.

## License

PocketStation Relay is available under the MIT license.
