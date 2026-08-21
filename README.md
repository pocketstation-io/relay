# PocketStation Relay

PocketStation Relay is a bounded, source-aware WebRTC audio plane. It carries
independent application, microphone, caller, and generated-audio buses to
native or browser receivers without taking ownership of capture, recording,
models, or durable Session state.

```text
authenticated source attachment
              ↓
       one RelaySession
              ↓
    named AudioBus + generation
              ↓
 bounded BusSubscription fan-out
              ↓
 RTP continuity, pacing, repair, and observations
```

An `AudioBus` keeps a stable semantic identity while a transient publisher
attachment, SSRC, and source generation may change. A subscriber selects one
bus or the declared `mix` output.

## Authority modes

Production uses control-plane authority:

```text
control plane creates the Session and capabilities
Relay validates those capabilities and owns live attachments
Relay sends authenticated, revisioned full-state snapshots back
```

Set:

```text
RELAY_AUTHORITY_MODE=control-plane
RELAY_API_SERVER_URL=https://pocketstation-api.fly.dev
POCKETSTATION_JWT_SECRET=<shared capability verification secret>
POCKETSTATION_INTERNAL_SECRET=<shared state-synchronization secret>
```

In this mode Relay rejects its local Session and invitation mutation routes.
Subscriber capabilities are signed by the control plane and validated by Relay
with the same strict issuer, audience, token-type, role, and bus-scope profile.

`RELAY_AUTHORITY_MODE=standalone` is an explicit self-hosted mode. Relay then
creates its own Sessions and single-use invitations. It uses the independent
`RELAY_INVITATION_SECRET` for subscriber capabilities. Credentials from one
authority mode are not accepted by the other.

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
  "observed_at": "2026-08-21T17:45:00Z",
  "buses": [
    {"bus_id":"application","role":"application","source_active":true,"source_generation":2}
  ],
  "subscriptions": [
    {"subscriber_id":"a64a0c10-...","bus_id":"application"}
  ]
}
```

The callback is authenticated and bounded. A full snapshot replaces all
Relay-owned state, so duplicate delivery is safe and callback loss is repaired
by periodic reconciliation. Reconciliation resends the current revision; it
does not manufacture a new transition.

State-change notification uses an atomic, nonblocking handoff. It does not add
a lock, allocation, network call, or log operation to RTP forwarding.

## Run locally

Control-plane mode:

```bash
POCKETSTATION_JWT_SECRET=development-source-secret-32-bytes \
POCKETSTATION_INTERNAL_SECRET=development-internal-secret-32-bytes \
RELAY_AUTHORITY_MODE=control-plane \
RELAY_API_SERVER_URL=http://127.0.0.1:4801 \
go run ./cmd/relay-server
```

Standalone mode:

```bash
POCKETSTATION_JWT_SECRET=development-source-secret-32-bytes \
RELAY_INVITATION_SECRET=development-receiver-secret-32-bytes \
RELAY_AUTHORITY_MODE=standalone \
go run ./cmd/relay-server
```

The deterministic `relay-test-source` fixture can publish one named test bus:

```bash
go run ./cmd/relay-test-source -- \
  --relay http://127.0.0.1:4800 \
  --session <session_id> \
  --bus application \
  --token <source_capability> \
  --duration 3s
```

It emits valid synthetic Opus for transport verification. It is not physical
capture evidence.

## Finite work

Relay bounds:

- RelaySessions and AudioBuses per Session;
- subscriptions per Session;
- concurrent signaling and WHIP/WHEP handshakes;
- pending invitations in standalone mode;
- control-state notifications;
- callback duration and response size;
- packet queues, repair caches, and packet age.

When capacity is unavailable, Relay rejects new work before allocating a media
path. It does not grow an unbounded retry or callback queue.

## Verify a change

```bash
scripts/check-code-protocol.sh
go test -race -short ./...
go test -race ./internal/server ./test/integration
```

CI must pass before Fly deploys. Deployment checks out the exact successful CI
revision and records it in the OCI image. A successful local or same-host test
does not establish WAN/TURN or multi-region performance.

See [the signaling contract](docs/contracts/SIGNALING_PROTOCOL.md) for the wire
protocol and failure model.
