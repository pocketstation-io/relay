# PocketStation Relay signaling contract

This document defines the public control contract between a Relay client and
PocketStation Relay. It covers Session creation, browser invitations, WebSocket
signaling, named `AudioBus` publication, WHIP/WHEP signaling, authentication,
errors, and compatibility behavior.

The contract is intentionally smaller than the implementation. Internal Go
types, package paths, and transport workers may change without changing this
wire API.

## The model in one minute

```text
create RelaySession
  → receive source and subscriber credentials
  → publisher attaches one or more named AudioBuses
  → subscriber selects one bus or the intentional mix
  → WebRTC carries audio; signaling carries lifecycle and control
```

The stable identity is the `session_id` plus a named `bus_id`. SSRCs, ICE
candidates, PeerConnections, and individual source attachments are transient.

## Origins and transports

Examples use `relay.example.com` as the Relay origin.

| Purpose | Transport | Endpoint |
|---|---|---|
| Create Session | HTTPS + JSON | `POST /v1/sessions` |
| Create invitation | HTTPS + JSON | `POST /v1/sessions/{id}/invitations` |
| Resolve invitation | HTTPS + JSON | `GET /v1/join/{code}` |
| PocketStation signaling | WebSocket + JSON text frames | `wss://relay.example.com/v1/signal` |
| WebRTC ingest | WHIP over HTTPS | `POST /v1/sessions/{id}/whip` |
| WebRTC egress | WHEP-style HTTPS | `POST /v1/sessions/{id}/whep` |
| Trickle ICE | HTTPS SDP fragment | `PATCH /v1/connections/{connection_id}` |
| Close HTTP-signaled peer | HTTPS | `DELETE /v1/connections/{connection_id}` |

WebSocket credentials are carried inside the first `PUBLISH` or `SUBSCRIBE`
message. They are not query parameters. WHIP/WHEP credentials use
`Authorization: Bearer <token>`.

## Create a RelaySession

```http
POST /v1/sessions HTTP/1.1
Host: relay.example.com
```

Successful response:

```json
{
  "session_id": "550e8400-e29b-41d4-a716-446655440000",
  "source_token": "<JWT>",
  "subscriber_token": "<JWT>",
  "join_code": "<opaque-value>",
  "join_url": "https://receiver.example.com/?join=<opaque-value>&relay=https%3A%2F%2Frelay.example.com",
  "ice_servers": []
}
```

Clients must treat `session_id`, `join_code`, tokens, and URLs as opaque. The
current server generates UUID-shaped identifiers, but their internal format is
not a client contract.

`ice_servers` is present only when the Relay is configured to advertise ICE
servers. Credentials and invitation responses must not be cached or logged.

For compatibility, current responses also include `room_id`, `listener_token`,
and `qr_url`. New clients use the Session/subscriber names.

### Create an invitation after publication starts

```http
POST /v1/sessions/{session_id}/invitations HTTP/1.1
Authorization: Bearer <source_token>
```

The source credential must target the path Session, and that Session must have
an active source. Success is `201 Created`:

```json
{
  "session_id": "550e8400-e29b-41d4-a716-446655440000",
  "join_code": "<opaque-value>",
  "join_url": "https://receiver.example.com/?join=<opaque-value>&relay=https%3A%2F%2Frelay.example.com"
}
```

Expected failures include `403` for invalid authority, `404` for an unknown
Session, and `409` while the source is not active.

### Resolve an invitation

```http
GET /v1/join/{join_code} HTTP/1.1
```

Success returns a short-lived subscriber credential and the canonical signal
URL:

```json
{
  "session_id": "550e8400-e29b-41d4-a716-446655440000",
  "subscriber_token": "<JWT>",
  "signal_url": "wss://relay.example.com/v1/signal",
  "ice_servers": []
}
```

Invitation responses use `Cache-Control: no-store`. Expired, unknown, or
orphaned invitations return `404`.

## Credentials and authority

Relay credentials are HS256 JWTs issued by the Relay or its trusted control
plane. The effective claims are:

```json
{
  "session_id": "<RelaySession>",
  "bus_id": "<optional AudioBus scope>",
  "role": "source | subscriber",
  "iat": 0,
  "exp": 0
}
```

Rules:

- `source` may publish and create an invitation for its Session;
- `subscriber` may receive;
- an optional `bus_id` narrows the credential to one bus;
- the token's Session and bus scope are authoritative;
- expired, malformed, or wrongly scoped credentials fail closed.

`room_id` and role `listener` are accepted compatibility aliases. New issuers
and clients use `session_id` and `subscriber`.

## WebSocket framing and lifecycle

- WebSocket protocol: RFC 6455.
- Endpoint: `/v1/signal`.
- Application messages: one JSON object per text frame.
- Maximum inbound message size: 64 KiB.
- Server ping interval: 30 seconds.
- Read timeout without traffic or pong: 90 seconds.
- Maximum queued ICE candidates before a remote description: 64.
- Unknown message types receive an `ERROR` with code `unknown_type`.

The first application message must establish the role:

```text
CONNECTED
  ├── PUBLISH   → publishing peer
  └── SUBSCRIBE → subscribing peer

publishing/subscribing peer
  ├── ICE
  ├── SDP_ANSWER when Relay offered
  ├── role-specific control messages
  └── LEAVE or socket close → teardown
```

Sending `PUBLISH` or `SUBSCRIBE` twice on one connection fails with
`already_joined`.

## JSON envelopes

Client to Relay:

```json
{
  "type": "PUBLISH",
  "session_id": "<optional compatibility/fallback identity>",
  "graph_id": "<optional graph label>",
  "bus_id": "<single-bus selection>",
  "publish_buses": [
    {"stream_id": "application-stream", "bus_id": "application"}
  ],
  "token": "<JWT>",
  "sdp_offer": "<optional SDP>",
  "sdp_answer": "<optional SDP>",
  "candidate": "<optional ICE candidate>",
  "sframe_key": "<optional opaque base64>",
  "latency_report": null,
  "public": false
}
```

Relay to client:

```json
{
  "type": "SESSION_STATE",
  "session_id": "<RelaySession>",
  "bus_id": "<optional AudioBus>",
  "sdp_offer": "<optional SDP>",
  "sdp_answer": "<optional SDP>",
  "candidate": "<optional ICE candidate>",
  "source_active": true,
  "subscription_count": 1,
  "codec": "opus",
  "code": "<optional stable error code>",
  "message": "<optional diagnostic>",
  "sframe_key": "<optional opaque base64>",
  "codec_hint": null,
  "use_turn": false
}
```

Fields not relevant to a message type are omitted. Clients must branch on
`type`, not on field presence.

## Publish audio

### Single bus

```json
{
  "type": "PUBLISH",
  "token": "<source_token>",
  "bus_id": "application",
  "sdp_offer": "<offer>"
}
```

Bus selection order is message `bus_id`, token `bus_id`, then the legacy
default `voice`. A message bus cannot escape a bus-scoped credential.

### Multiple independent buses

One publisher PeerConnection can attach up to 16 audio streams:

```json
{
  "type": "PUBLISH",
  "token": "<source_token>",
  "publish_buses": [
    {"stream_id": "app-stream", "bus_id": "application"},
    {"stream_id": "mic-stream", "bus_id": "microphone"}
  ],
  "sdp_offer": "<offer containing both stream IDs>"
}
```

The declaration is transactional and finite:

- `publish_buses` contains 1–16 bindings;
- `stream_id` and `bus_id` are each 1–64 ASCII characters from
  `A-Z`, `a-z`, `0-9`, `.`, `_`, and `-`;
- stream IDs are unique;
- bus IDs are unique;
- every arriving WebRTC track must use a declared stream ID;
- each declared stream may attach once;
- `bus_id` and `publish_buses` cannot appear together;
- a bus-scoped source token can publish only that bus.

This mapping prevents two tracks from aliasing or replacing each other inside
one publisher lifecycle.

## Subscribe to audio

```json
{
  "type": "SUBSCRIBE",
  "token": "<subscriber_token>",
  "bus_id": "application",
  "sdp_offer": "<offer>"
}
```

Selection order is message `bus_id`, token `bus_id`, then `mix`.
`publish_buses` is invalid for a subscriber. Subscriber capacity is reserved
during the handshake and becomes an active `BusSubscription` only after the
PeerConnection reaches `connected`.

## SDP and ICE exchange

Clients may offer or ask Relay to offer.

### Client offers

```text
client  → PUBLISH or SUBSCRIBE with sdp_offer
relay   → SDP_ANSWER
both    ↔ ICE
relay   → SESSION_STATE
```

### Relay offers

```text
client  → PUBLISH or SUBSCRIBE without sdp_offer
relay   → SDP_OFFER
client  → SDP_ANSWER
both    ↔ ICE
relay   → SESSION_STATE
```

ICE messages carry the candidate string:

```json
{"type": "ICE", "candidate": "candidate:..."}
```

Candidates received before the remote description are held in the finite
pending-candidate buffer.

## Lifecycle and control messages

### `SESSION_STATE`

Sent after join and whenever source/subscriber state changes:

```json
{
  "type": "SESSION_STATE",
  "session_id": "<RelaySession>",
  "source_active": true,
  "subscription_count": 2,
  "codec": "opus"
}
```

Clients should accept deprecated `ROOM_STATE` from older relays. The current
Relay sends `SESSION_STATE`.

### `LEAVE`

```json
{"type": "LEAVE"}
```

Requests graceful teardown. Closing the WebSocket also tears down the peer and
releases its source/subscription ownership.

### `KEY_EXCHANGE`

A source may forward opaque SFrame key material:

```json
{"type": "KEY_EXCHANGE", "sframe_key": "<base64>"}
```

Relay does not decrypt the value. It retains the current opaque string in the
RelaySession, forwards it to active subscribers, and sends it to later
subscribers. Subscriber-originated key exchange fails with `role_mismatch`.
Applications remain responsible for key generation, rotation, recipient trust,
and media encryption semantics.

### `LATENCY_REPORT`

After joining, either role may report measured segments:

```json
{
  "type": "LATENCY_REPORT",
  "latency_report": {
    "session_id": "<RelaySession>",
    "capture_ms": 1.2,
    "encode_ms": 0.8,
    "relay_rtt_ms": 12.4,
    "jitter_buffer_ms": 20.0,
    "decode_ms": 0.7,
    "packet_loss_pct": 0.2,
    "clock_drift_ppm": 3.1
  }
}
```

Values are observations supplied by the client. Relay does not reinterpret
them as independently measured end-to-end latency.

### `CODEC_HINT`

Relay may send an advisory encoder profile derived from receiver feedback:

```json
{
  "type": "CODEC_HINT",
  "codec_hint": {
    "bitrate_kbps": 64,
    "complexity": 8,
    "fec": true,
    "dtx": false,
    "frame_ms": 20
  }
}
```

No acknowledgement is required. Publishers apply supported fields
best-effort; unsupported hints may be ignored.

### `ICE_RESTART`

Relay may ask the source to renegotiate a degraded ICE path:

```json
{"type": "ICE_RESTART", "use_turn": true}
```

`use_turn` reports whether Relay is configured to offer TURN for the next
negotiation. The message is a request, not proof that reconnection succeeded.

## Errors

WebSocket protocol failures use:

```json
{
  "type": "ERROR",
  "code": "bad_token",
  "message": "diagnostic text"
}
```

Clients branch on `code`. `message` is diagnostic text and is not stable.

| Code | Meaning |
|---|---|
| `bad_token` | Credential is invalid, expired, or wrongly scoped |
| `already_joined` | This socket already owns a peer lifecycle |
| `role_mismatch` | Credential role cannot perform the operation |
| `pc_error` | PeerConnection creation failed |
| `sdp_error` | SDP state or content is invalid |
| `ice_error` | ICE candidate/state handling failed |
| `track_error` | Track creation, mapping, or attachment failed |
| `listener_limit_exceeded` | RelaySession subscriber capacity is exhausted |
| `room_limit_exceeded` | Relay process Session capacity is exhausted; legacy code name |
| `not_joined` | Operation requires an established Session role |
| `bad_request` | Message fields violate the contract |
| `unknown_type` | Message `type` is unsupported |

An `ERROR` does not imply that a client may retry blindly. Retry policy belongs
to the adapter and must respect the code, operation stage, deadline, and
credential lifetime.

## WHIP and WHEP

WHIP ingest follows RFC 9725:

```http
POST /v1/sessions/{session_id}/whip?bus=application HTTP/1.1
Authorization: Bearer <source_token>
Content-Type: application/sdp

<SDP offer>
```

WHEP-style egress uses the corresponding subscriber credential:

```http
POST /v1/sessions/{session_id}/whep?bus=application HTTP/1.1
Authorization: Bearer <subscriber_token>
Content-Type: application/sdp

<SDP offer>
```

Success is `201 Created`, `Content-Type: application/sdp`, an SDP answer body,
and a `Location: /v1/connections/{connection_id}` resource. Configured ICE
servers are returned as `Link` headers.

Use the returned resource for trickle ICE and teardown:

```http
PATCH /v1/connections/{connection_id}
Content-Type: application/trickle-ice-sdpfrag

DELETE /v1/connections/{connection_id}
```

WHIP/WHEP exposes a narrower interoperable path. PocketStation-specific
multi-bus declaration and control messages use `/v1/signal`.

## Security and operational invariants

- Run WebSocket and HTTP signaling over TLS outside local development.
- Never place source/subscriber tokens or SFrame keys in URLs or logs.
- Configure `ALLOWED_ORIGINS` for browser deployments.
- Treat `join_code` as a bearer secret until it expires.
- Apply Session, subscriber, bus, and concurrent-handshake limits before media
  allocation.
- Keep retry loops finite and inside a caller-owned startup deadline.
- Do not infer delivery from signaling success; readiness requires an active
  WebRTC connection and registered source/subscription.
- Do not infer complete PocketStation `FrameLineage` from bus identity. Remote
  per-frame lineage requires its own versioned metadata contract.

## Compatibility policy

Primary vocabulary is:

```text
RelaySession · session_id · subscriber · subscriber_token · SESSION_STATE
```

The following aliases remain for existing clients:

```text
/v1/rooms · room_id · listener · listener_token · ROOM_STATE
```

New code must not introduce additional room/listener vocabulary. Removing an
accepted alias or changing field meaning requires a new compatibility revision,
fixtures for old and new clients, and an explicit migration path.

## Implementation conformance

The authoritative Go projections are:

| Contract concern | Implementation |
|---|---|
| Message envelopes | `internal/signaling/messages.go` |
| Stable error codes | `internal/signaling/errors.go` |
| Credential claims | `internal/auth/token.go` |
| WebSocket lifecycle | `internal/server/signal_peer.go` |
| Join and role validation | `internal/server/peer_join.go` |
| Multi-bus mapping | `internal/server/publish_buses.go` |
| Invitations | `internal/server/invitations.go` |
| WHIP/WHEP resources | `internal/server/whip.go`, `internal/server/whip_resource.go` |

Every protocol change must update these projections, this contract, positive
and negative compatibility tests, and cross-language fixtures together.
