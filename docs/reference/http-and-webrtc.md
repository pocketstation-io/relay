# HTTP and WebRTC endpoints

Relay exposes one HTTP server for health, metrics, signaling, WHIP, WHEP, and
RelaySession diagnostics. Authentication requirements depend on the operation
and operating mode.

| Method and endpoint | Use |
|---|---|
| `GET /healthz` | HTTP process health. |
| `GET /metrics` | Prometheus text metrics. |
| `GET /v1/signal` with WebSocket upgrade | Publisher and subscriber signaling. |
| `GET /v1/echo` with WebSocket upgrade | Timestamp echo used by transport measurements. |
| `POST /v1/sessions/{id}/whip` | Publish one WebRTC session with SDP. |
| `POST /v1/sessions/{id}/whep` | Subscribe to one AudioBus with SDP. |
| `PATCH /v1/connections/{connection_id}` | Add trickled ICE candidates. |
| `DELETE /v1/connections/{connection_id}` | Close a WHIP or WHEP resource. |
| `GET /v1/sessions/{id}/health` | AudioBus health for one RelaySession. |
| `GET /v1/sessions/{id}/latency` | RelaySession latency observations. |
| `GET /v1/sessions/{id}/media-debug` | Source clock and downlink observations. |
| `GET /v1/sessions/{id}/packet-log` | Packet log up to the configured record limit for one named bus. |

Standalone mode additionally enables:

| Method and endpoint | Use |
|---|---|
| `POST /v1/sessions` | Create a temporary RelaySession and capabilities. |
| `POST /v1/sessions/{id}/invitations` | Create a single-use invitation. |
| `GET /v1/join/{code}` | Resolve one invitation code. |

Control-plane mode rejects local RelaySession and invitation mutation. Use the
Control Plane API to create sessions and credentials in that mode.

For exact WebSocket message fields, limits, and error codes, read the
[signaling protocol](signaling.md).
