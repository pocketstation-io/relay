# HTTP and WebRTC endpoints

| Method and endpoint | Use and access |
|---|---|
| `GET /.well-known/pocketstation` | Discover the public Session `authority_url`. |
| `GET /healthz` | Process liveness. |
| `GET /readyz` | Readiness including the access service. |
| `GET /metrics` | Operational metrics; restrict network access. |
| `GET /v1/sessions/{id}/audio?bus={bus_id}` with upgrade | Encoded Opus ingress with source Bearer capability and `pks-opus-v1`. |
| `GET /v1/signal` with upgrade | WebRTC signaling; capability required in handshake. |
| `GET /v1/echo` with upgrade | Timestamp echo, not a media quality measurement. |
| `POST /v1/sessions/{id}/whip` | SDP ingest with scoped source Bearer capability. |
| `POST /v1/sessions/{id}/whep` | SDP receive with exact-bus subscriber capability. |
| `PATCH /v1/connections/{id}` | Trickle ICE for an admitted connection resource. |
| `DELETE /v1/connections/{id}` | Close that connection resource. |
| `GET /v1/sessions/{id}/health` | Exact-Session owner/subscriber observation. |
| `GET /v1/sessions/{id}/latency` | Exact-Session owner/subscriber observation. |
| `GET /v1/sessions/{id}/media-debug` | Exact-Session owner/subscriber observation. |
| `GET /v1/sessions/{id}/packet-log` | Owner/subscriber observation with bus scope. |

The discovered access service mounts the same domain in both deployments:

| Method and endpoint | Use |
|---|---|
| `POST /v1/sessions` | Create a Session with declared buses and an owner capability. |
| `POST /v1/sessions/{id}/renew` | Renew with a still-valid owner capability. |
| `DELETE /v1/sessions/{id}` | Owner deletion, including grants and retry receipts. |
| `POST /v1/sessions/{id}/invitations` | Owner creates a one-use, exact-bus share grant. |
| `POST /v1/join` | Redeem opaque `join_code` in JSON body. |
| `POST /v1/join/{words}` | Locate by words and redeem matching `join_code` in body. |
| `GET` or `HEAD /v1/join/{locator}` | Preview only; never consume or grant access. |

Managed media rejects local domain mutations; use discovery's `authority_url`.
Standalone mounts domain routes beside media. Refer to [access and storage](../access-service.md)
for retry keys, lifetime, name policy and recovery, and the
[signaling specification](signaling.md) for exact transport messages.
