# Relay signaling specification

This document defines how a publisher or subscriber joins one PocketStation
`RelaySession`. It describes transport signaling only. Capture, graph
execution, recording and models are outside Relay. Session access rules belong
to Relay’s shared `access` module; storage is injected by the deployment.

## Protocol profile

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

## Capability verification

Both standalone and managed operation use the same Relay-owned access service.
New Sessions pin issuer `pocketstation-relay`. An explicitly migrated Session
may retain its recorded legacy issuer and migration cutoff; callers cannot
select another issuer. `POCKETSTATION_JWT_SECRET` is the shared signing key.
There is no separate standalone invitation secret.

Managed media rejects local Session and invitation mutations with `409
control_plane_authority_required`; clients use the advertised `authority_url`.
Before allocating media, Relay fetches the trusted Session profile:

```http
GET /v1/internal/sessions/{session_id}/authority
X-PocketStation-Internal-Secret: <shared secret>
```

The profile binds issuer, incarnation, declared buses and the current media
placement term. The capability must match that profile. A missing Session
rejects admission; timeout, transport or other access-service failures fail closed.
Standalone performs the equivalent checks through its local shared service.

The media process acquires its ordered placement with authenticated
`POST /v1/internal/sessions/{session_id}/relay-writer`, supplying `relay_epoch`
and `expected_term`. The returned `writer_term` fences later snapshots.
These operations occur outside RTP forwarding.

Every capability requires:

- an exact HS256 algorithm;
- `typ: pks-relay-capability+jwt`;
- the issuer pinned by the trusted Session record;
- `aud: pocketstation-relay`;
- an exact role-specific subject;
- `jti`, `iat`, `nbf`, and `exp`;
- one `session_id` and its Session-service incarnation (with explicit migration rules
  for pre-migration legacy credentials);
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
finite teardown.

The Session ID in the URL must equal the capability Session ID. The selected
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
  "writer_term": 1,
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

The callback is a complete replacement, not a delta. Delivery uses a fixed-capacity
nonblocking mailbox and a finite HTTP deadline. A periodic pass resends the
latest unchanged revision. This makes lost delivery recoverable and duplicate
delivery idempotent.

`relay_epoch` changes when the Relay process restarts. Revisions are monotonic
within an epoch. The shared access service requires the current ordered writer
term and epoch, so a delayed older process cannot replace current observations.
Observation time is taken from the storage transaction clock.

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
- `session_not_active` — the access service has no active Session for
  the capability;
- `authority_unavailable` — Relay could not verify the Session access record
  within the finite admission deadline;
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
transport requires an additional versioned metadata specification.

## Capability and established-connection lifetime

Capability expiry is an admission deadline. Relay checks the complete token
profile when a WebSocket or WHIP/WHEP attachment starts; it does not terminate
an already admitted media connection merely because that token later expires.
Reconnection requires a currently valid capability. This avoids introducing a
second media lifecycle clock into RTP forwarding.

A control-plane outage rejects new attachments. Existing admitted media continues
while reconciliation retries; an unavailable access service is not interpreted as a
Session deletion. An authenticated `404` from state reconciliation ends the
RelaySession, closes its publisher and subscriber transports, removes WHIP/WHEP
resources, and stops forwarding. The signal and HTTP transport owners observe
Session completion outside the media loop.

Deletion is asynchronous: detection depends on the reconciliation interval,
queued callbacks and finite callback deadlines. There is no instantaneous
revocation promise. The local single-Session regression measures closure within
three seconds with a 100 ms reconciliation interval and a responsive access service;
this is not a multi-Session outage or WAN latency bound.
