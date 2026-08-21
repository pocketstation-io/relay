# Relay signaling contract

This document defines how a publisher or subscriber joins one PocketStation
`RelaySession`. It describes transport signaling only. Capture, graph
execution, recording, models, and durable Session ownership are outside Relay.

## Contract profile

| Property | Value |
|---|---|
| WebSocket endpoint | `/v1/signal` |
| Ingest endpoint | `POST /v1/sessions/{id}/whip` |
| Egress endpoint | `POST /v1/sessions/{id}/whep` |
| Media | Opus over WebRTC/SRTP |
| Session term | `RelaySession` |
| Named media lane | `AudioBus` |
| Receiver attachment | `BusSubscription` |
| Capability algorithm | HS256 only |
| Capability audience | `pocketstation-relay` |
| Capability type | `pks-relay-capability+jwt` |

All JSON messages have a finite size. Unknown message types, invalid state
transitions, excess capacity, and out-of-scope buses fail explicitly.

## Capability authority

In `control-plane` mode, Relay accepts capabilities issued by
`pocketstation-control-plane` and signed with `POCKETSTATION_JWT_SECRET`.
Relay's local Session and invitation endpoints return `409
control_plane_authority_required`.

In `standalone` mode, Relay accepts capabilities issued by
`pocketstation-relay`. Source and subscriber credentials use distinct configured
secrets.

Every capability requires:

- an exact HS256 algorithm;
- `typ: pks-relay-capability+jwt`;
- the configured issuer;
- `aud: pocketstation-relay`;
- an exact role-specific subject;
- `jti`, `iat`, `nbf`, and `exp`;
- one `session_id`;
- a source `bus_ids` set or one subscriber `bus_id`, never both.

## Publisher handshake

The first WebSocket message is `PUBLISH`:

```json
{
  "type": "PUBLISH",
  "token": "<source capability>",
  "bus_id": "application",
  "sdp_offer": "v=0..."
}
```

For a multi-track publisher, replace `bus_id` with `publish_buses`:

```json
{
  "type": "PUBLISH",
  "token": "<source capability>",
  "publish_buses": [
    {"stream_id":"application","bus_id":"application"},
    {"stream_id":"microphone","bus_id":"microphone"}
  ],
  "sdp_offer": "v=0..."
}
```

`bus_id` and `publish_buses` are mutually exclusive. A multi-bus declaration
contains 1–16 bindings. Every stream ID and bus ID is finite, portable, unique,
and inside the source capability.

Relay answers with `SDP_ANSWER`, then both peers exchange `ICE` messages:

```json
{"type":"SDP_ANSWER","sdp_answer":"v=0..."}
```

```json
{"type":"ICE","candidate":"candidate:..."}
```

The `stream_id` in `publish_buses` must match the WebRTC stream ID received for
that track. An undeclared or repeated track is rejected.

## Subscriber handshake

The first message is `SUBSCRIBE`:

```json
{
  "type": "SUBSCRIBE",
  "token": "<subscriber capability>",
  "bus_id": "application",
  "sdp_offer": "v=0..."
}
```

If `bus_id` is omitted, Relay uses the exact bus in the capability. Supplying a
different bus fails. A `mix` capability receives the Session's declared mixed
output.

Relay registers a `BusSubscription` only after the WebRTC connection is
connected. Pending handshakes do not count as active subscriptions.

## WHIP and WHEP

WHIP and WHEP use the same capability and bus-scope rules as WebSocket
signaling.

```http
POST /v1/sessions/{session_id}/whip?bus=application
Authorization: Bearer <source capability>
Content-Type: application/sdp
```

```http
POST /v1/sessions/{session_id}/whep?bus=application
Authorization: Bearer <subscriber capability>
Content-Type: application/sdp
```

The response is `201 Created`, contains the SDP answer, and returns a
connection resource in `Location`. Use `PATCH` for trickle ICE and `DELETE` for
bounded teardown.

The Session ID in the path must equal the capability Session ID. The selected
bus must be inside the capability.

## Session state messages

Relay may send a transport-facing `SESSION_STATE` message:

```json
{
  "type": "SESSION_STATE",
  "session_id": "16d2491c-86ef-4a86-9ba7-af1d2d246244",
  "source_active": true,
  "subscription_count": 2,
  "codec": "opus"
}
```

This message is a convenience observation for connected signaling peers. It is
not the authoritative control-plane record. The authoritative record is the
revisioned full-state callback described below.

## Control-plane synchronization

For every source attachment, source detachment, subscription attachment, or
subscription removal, Relay advances one revision and queues a complete state
snapshot:

```http
PUT /v1/internal/sessions/{session_id}/relay-state
X-PocketStation-Internal-Secret: <shared secret>
Content-Type: application/json
```

```json
{
  "contract_version": 1,
  "session_id": "16d2491c-86ef-4a86-9ba7-af1d2d246244",
  "relay_epoch": "f78124e8-4be8-451b-8b44-3238faf7e802",
  "revision": 7,
  "observed_at": "2026-08-21T17:45:00Z",
  "buses": [
    {"bus_id":"application","role":"application","source_active":true,"source_generation":2},
    {"bus_id":"microphone","role":"microphone","source_active":true,"source_generation":1}
  ],
  "subscriptions": [
    {"subscriber_id":"af37ddaa-...","bus_id":"application"}
  ]
}
```

The callback is a complete replacement, not a delta. Delivery uses a bounded
nonblocking mailbox and a finite HTTP deadline. A periodic pass resends the
latest unchanged revision. This makes lost delivery recoverable and duplicate
delivery idempotent.

`relay_epoch` changes when the Relay process restarts. Revisions are monotonic
within an epoch. The control plane remembers retired epochs so a delayed
snapshot from an older process cannot regress current state.

## Errors

WebSocket errors use:

```json
{"type":"ERROR","code":"bad_token","message":"..."}
```

Stable categories include:

- `bad_token` — invalid issuer, audience, type, signature, time, role, Session,
  or bus scope;
- `role_mismatch` — a source token used to subscribe or subscriber token used
  to publish;
- `bad_request` — malformed or ambiguous declaration;
- `room_limit_exceeded` — retained wire code for RelaySession admission until a
  protocol-major revision changes it;
- `listener_limit_exceeded` — retained wire code for subscription admission
  until a protocol-major revision changes it;
- `sdp_error` and `ice_error` — negotiation failures.

The retained error strings are wire compatibility values, not public type or
documentation vocabulary.

## Lifecycle

`LEAVE` requests clean teardown. Connection loss also removes the attachment.
Source reconnect retains `AudioBus` identity and advances
`source_generation`. Relay shutdown stops control synchronization, closes
signaling peers and WebRTC connections, and waits within the configured grace
period.

Relay callbacks and signaling messages are observations. They do not replace
PocketStation Core lineage. Complete per-frame Core lineage over a remote
transport requires an additional versioned metadata contract.
