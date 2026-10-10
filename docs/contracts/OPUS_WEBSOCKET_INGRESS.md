# Opus WebSocket ingress

Relay accepts authenticated Opus messages over `pks-opus-v1`. A client connects
to `GET /v1/sessions/{id}/audio?bus={bus_id}` with its source capability in the
`Authorization: Bearer` header and requests the `pks-opus-v1` subprotocol.
Each connection publishes exactly one declared AudioBus. The Session and bus
must match the capability. Relay sends `READY` only after attaching the
SourceSession; its JSON includes `bus_id` and `source_generation`.

Binary messages contain a 40-byte, big-endian header followed by one Opus packet:

| Byte offset | Field |
|---:|---|
| 0 | Four-byte `PKSO` magic. |
| 4 | Version `1`. |
| 5 | Channels, `1` or `2`. |
| 6 | Two reserved zero bytes. |
| 8 | Unsigned 64-bit sequence. |
| 16 | Unsigned 32-bit RTP timestamp at 48,000 Hz. |
| 20 | Unsigned 16-bit duration, exactly 960 samples. |
| 22 | Unsigned 16-bit payload length, 1 through 1,275 bytes. |
| 24 | Unsigned 64-bit capture monotonic time in nanoseconds. |
| 32 | Unsigned 64-bit publisher output generation. |
| 40 | Opus payload. |

The largest message is 1,315 bytes. Messages must preserve sequence and RTP
timestamp continuity. Relay validates and paces encoded Opus into the existing
AudioBus; it does not decode PCM, mix stems or run model inference. Browser
subscriptions continue to use WebRTC.

The connection closes after capability expiry, 15 seconds without an incoming
message, replacement, shutdown, or invalid pacing/message data. Static source
credentials need a fresh authenticated publisher when they expire; management
renewal alone does not update an existing media attachment. See the
[implementation](../../internal/media/ingress/opus.go) for the exact parser and
[server adapter](../../internal/server/audio_ingress.go) for admission and closure.
The Protocol repository owns the shared wire specification; this document
describes Relay's implemented support without assuming a separately published
Protocol document is available.

A healthy `/healthz`, successful WebSocket upgrade, source packet count or
loopback test cannot establish hosted browser decode or phone audio.
