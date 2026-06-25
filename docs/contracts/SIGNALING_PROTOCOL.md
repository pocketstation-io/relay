# Signaling Protocol Contract

> Canonical reference for all WebSocket signaling messages exchanged between
> PocketStation clients and the relay. Source of truth: `internal/signaling/messages.go`.

---

## Transport

- **Protocol**: WebSocket (RFC 6455), JSON messages, text frames
- **Endpoint**: `wss://<relay>/ws?token=<jwt>`
- **Token**: HS256 JWT issued by api-server (`internal/auth/token.go`). Encodes:
  `room_id`, `role` (`source` | `listener`), `exp`.
- **Framing**: one JSON object per frame; each direction is independent.
- **Keepalive**: relay sends WebSocket `PING` every 30 s; clients must respond
  with `PONG` (browsers do this automatically). Read deadline resets to 90 s on
  each pong. See `internal/server/keepalive.go`.

---

## Message structure

Every message carries a `type` field. Unknown types are silently discarded.

### ClientMessage (client → relay)

```json
{
  "type":           "<MessageType>",
  "room_id":        "<string, omitempty>",
  "token":          "<string, omitempty>",
  "sdp_offer":      "<string, omitempty>",
  "candidate":      "<string, omitempty>",
  "sframe_key":     "<base64, omitempty>",
  "latency_report": { ... },
  "public":         false
}
```

### ServerMessage (relay → client)

```json
{
  "type":           "<MessageType>",
  "sdp_answer":     "<string, omitempty>",
  "candidate":      "<string, omitempty>",
  "source_active":  false,
  "listener_count": 0,
  "codec":          "<string, omitempty>",
  "code":           "<string, omitempty>",
  "message":        "<string, omitempty>",
  "sframe_key":     "<base64, omitempty>",
  "codec_hint":     { ... },
  "use_turn":       false
}
```

---

## Room creation

### POST /v1/rooms — response

```json
{
  "room_id":        "<string>",
  "source_token":   "<JWT>",
  "listener_token": "<JWT>",
  "qr_url":         "/listen?room=<room_id>",
  "ice_servers":    [ ... ]
}
```

#### room_id format

`room_id` is a UUID v4 string (RFC 4122, lowercase hex with hyphens, e.g.
`550e8400-e29b-41d4-a716-446655440000`). Clients must treat it as an opaque
string and must not parse its structure.

The relay mints `room_id` values using `crypto/rand` with version 4 bits set
(`b[6] & 0x0f | 0x40`) and variant bits set (`b[8] & 0x3f | 0x80`).

`ice_servers` is only present in the response when the relay has embedded TURN
configured (RELAY-023). When absent, clients should fall back to their own STUN
configuration.

---

## Message types

### PUBLISH (client → relay)

Source initiates a WebRTC session in a room.

| Field     | Required | Value             |
|-----------|----------|-------------------|
| type      | yes      | `"PUBLISH"`       |
| room_id   | yes      | Room ID from JWT  |
| token     | yes      | Source JWT        |
| sdp_offer | yes      | WebRTC SDP offer  |

The relay validates the JWT role (`source`), creates or joins the room, and
replies with `SDP_ANSWER` followed by zero or more `ICE` messages.

---

### SUBSCRIBE (client → relay)

Listener initiates a WebRTC session in a room.

| Field     | Required | Value             |
|-----------|----------|-------------------|
| type      | yes      | `"SUBSCRIBE"`     |
| room_id   | yes      | Room ID from JWT  |
| token     | yes      | Listener JWT      |
| sdp_offer | yes      | WebRTC SDP offer  |

The relay validates the JWT role (`listener`) and replies with `SDP_ANSWER`.
If the room has a stored SFrame key, the relay immediately sends `KEY_EXCHANGE`
with the stored key so late joiners receive it without waiting for the source
to re-issue it.

---

### SDP_ANSWER (relay → client)

| Field      | Value              |
|------------|--------------------|
| type       | `"SDP_ANSWER"`     |
| sdp_answer | WebRTC SDP answer  |

Sent after PUBLISH or SUBSCRIBE completes signaling.

---

### ICE (bidirectional)

WebRTC ICE candidate trickle. Direction depends on `type` field context.

| Field     | Value                         |
|-----------|-------------------------------|
| type      | `"ICE"`                       |
| candidate | JSON-encoded ICE candidate    |

---

### KEY_EXCHANGE (source → relay → listeners)

SFrame end-to-end encryption key distribution (RELAY-014).

| Field      | Value                           |
|------------|---------------------------------|
| type       | `"KEY_EXCHANGE"`                |
| sframe_key | base64-encoded SFrame key bytes |

**Invariant**: only a `source` role session may send `KEY_EXCHANGE`. The relay:
1. Rejects listener-sourced `KEY_EXCHANGE` with `role_mismatch` error.
2. Forwards the message verbatim to all listener sessions in the room.
3. Persists the key in the room object so late-joining listeners receive it
   immediately on SUBSCRIBE.

The relay never decrypts or reads key material — it copies `sframe_key` as an
opaque base64 string. See `internal/server/sframe_handler.go`.

---

### LEAVE (client → relay)

Graceful session teardown.

| Field | Value     |
|-------|-----------|
| type  | `"LEAVE"` |

The relay tears down the WebRTC PeerConnection, decrements listener count
(if listener), marks source inactive (if source), and closes the WebSocket.

**Gap**: `sdk-python` `client.py:105` has a `# Phase 5 TODO: send LEAVE` —
this means Python sessions do not currently send LEAVE on context-manager exit.
See `FAKE_SCAFFOLD_INVENTORY.md` entry `HIDDEN-003`.

---

### CODEC_HINT (relay → source)

Adaptive bitrate hint based on RTCP Receiver Reports (RELAY-021).

| Field                      | Value                        |
|----------------------------|------------------------------|
| type                       | `"CODEC_HINT"`               |
| codec_hint.bitrate_kbps    | Target Opus bitrate (kbps)   |
| codec_hint.complexity      | Opus complexity (0–10)       |
| codec_hint.fec             | Enable in-band FEC           |
| codec_hint.dtx             | Enable DTX                   |
| codec_hint.frame_ms        | Opus frame duration (10 or 20 ms) |

All fields are advisory; no acknowledgement is expected. Source applies them
best-effort on the next encode call.

**Status**: signaling types implemented; RTCP RR → CODEC_HINT wiring is PARTIAL
(see `FAKE_SCAFFOLD_INVENTORY.md`).

---

### ICE_RESTART (relay → source)

Triggered when sustained packet loss >15% for 3 consecutive RTCP reports
indicates ICE path degradation (spec §10.4).

| Field    | Value          |
|----------|----------------|
| type     | `"ICE_RESTART"` |
| use_turn | true if relay has embedded TURN configured (RELAY-023) |

Source calls `RTCPeerConnection.restartIce()` on receipt. When `use_turn=true`,
source should prefer TURN relay candidates on the next ICE negotiation.

---

### ERROR (relay → client)

| Field   | Value                    |
|---------|--------------------------|
| type    | `"ERROR"`                |
| code    | Machine-readable code    |
| message | Human-readable message   |

Known error codes:

| Code              | Trigger                                             |
|-------------------|-----------------------------------------------------|
| `bad_token`       | JWT invalid, expired, or wrong room                 |
| `role_mismatch`   | Operation not permitted for this session's role     |
| `not_joined`      | Operation requires PUBLISH/SUBSCRIBE first          |
| `room_full`       | Room has reached max listener count                 |
| `rate_limited`    | Room creation rate limit exceeded for this IP       |

---

### ROOM_STATE (relay → client)

Sent on join and on listener-count change.

| Field          | Value                    |
|----------------|--------------------------|
| type           | `"ROOM_STATE"`           |
| source_active  | Whether source is live   |
| listener_count | Current listener count   |
| codec          | Negotiated codec name    |

---

## Message flow diagrams

### Source connect and stream

```
Source                Relay
  |                     |
  |--- PUBLISH -------->|  (SDP offer + JWT)
  |<-- SDP_ANSWER ------|
  |<-> ICE (trickle) -->|
  |                     |
  |--- KEY_EXCHANGE ---->|  (optional; if SFrame enabled)
  |                     |
  |--- [RTP audio] ---->|  (WebRTC; not signaling)
  |                     |
  |--- LEAVE ---------->|
```

### Listener connect

```
Listener              Relay
  |                     |
  |--- SUBSCRIBE ------->|  (SDP offer + JWT)
  |<-- SDP_ANSWER -------|
  |<-> ICE (trickle) --->|
  |<-- KEY_EXCHANGE -----|  (if source key is cached)
  |<-- ROOM_STATE -------|
  |                     |
  |<-- [RTP audio] ------|  (WebRTC; not signaling)
```

---

## Implementation pointers

| Concern            | File                                          |
|--------------------|-----------------------------------------------|
| Message types/structs | `internal/signaling/messages.go`           |
| Dispatch loop      | `internal/server/server.go` `session.run()`   |
| SFrame forwarding  | `internal/server/sframe_handler.go`           |
| Keepalive          | `internal/server/keepalive.go`                |
| ICE restart logic  | `internal/server/ice_restart.go`              |
| CODEC_HINT logic   | `internal/server/codec_hint.go`               |
| Auth / JWT         | `internal/auth/`                              |
