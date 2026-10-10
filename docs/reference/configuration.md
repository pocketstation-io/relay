# Configure Relay

Relay reads configuration from environment variables at process start. Legacy media integer settings fall back on invalid input; domain storage,
name policy, Session TTL, required secrets and mode-specific URLs are validated.
Check startup diagnostics rather than assuming every variable uses the same parser.

## HTTP and authentication

| Variable | Default | Use |
|---|---:|---|
| `PORT` | `4800` | HTTP, WebSocket signaling, WHIP, WHEP, health, and metrics listener. |
| `RELAY_AUTHORITY_MODE` | `standalone` | `control-plane` or `standalone`. |
| `POCKETSTATION_JWT_SECRET` | required | Signs/verifies Session capabilities; at least 32 bytes. |
| `RELAY_API_SERVER_URL` | required in control-plane mode | Internal access-service URL for admission, media placement and state updates. |
| `POCKETSTATION_INTERNAL_SECRET` | required by the configured callback client | Authenticates Relay state updates to the Control Plane. |
| `ALLOWED_ORIGINS` | any origin | Comma-separated browser origins accepted by the signaling WebSocket. Native clients without an `Origin` header still require a capability. |
| `PUBLIC_RELAY_URL` | `http://localhost:$PORT` | Public Relay URL returned in invitations. |
| `PUBLIC_CONTROL_PLANE_URL` | required for managed discovery | Public URL of the shared access service mounted by control. |
| `PUBLIC_RECEIVER_URL` | unset | Browser receiver URL used to build join links. |
| `WEBHOOK_URL` | unset | Optional event delivery destination with payload and request-time limits. |

## Session capacity and time

| Variable | Default | Use |
|---|---:|---|
| `RELAY_MAX_ROOMS` | `100` | Maximum active RelaySessions. |
| `RELAY_MAX_SUBSCRIBERS_PER_SESSION` | `50` | Maximum active subscribers for one RelaySession. |
| `RELAY_MAX_BUSES_PER_SESSION` | `16` | Maximum named AudioBuses retained by one RelaySession. |
| `RELAY_MAX_CONCURRENT_HANDSHAKES` | `128` | Maximum signaling and WHIP/WHEP handshakes awaiting media allocation. |
| `RELAY_MAX_INVITATIONS` | `400` | Maximum active standalone grants. |
| `MAX_ROOMS_PER_IP_PER_MINUTE` | `10` | Standalone RelaySession creation rate per client IP. |
| `ROOM_EXPIRY_MINUTES` | `30` | Time an inactive RelaySession remains before closing. |
| `SOURCE_RECONNECT_WINDOW_SEC` | `60` | Time subscribers remain attached after the last publisher disconnects. |
| `CONTROL_RECONCILE_INTERVAL_SEC` | `5` | Interval between complete Control Plane state reconciliations. |
| `SHUTDOWN_GRACE_PERIOD_SEC` | `30` | Process shutdown deadline. The HTTP server uses a five-second drain within it. |

The values checked into `fly.toml` are intentionally smaller for the shared
demonstration deployment. They are not production recommendations.

## Standalone storage and naming

| Variable | Default | Use |
|---|---|---|
| `POCKETSTATION_STORAGE` | `memory` | `memory` or durable local `sqlite`. |
| `POCKETSTATION_SQLITE_PATH` | required for SQLite | Owner-writable database filename. |
| `POCKETSTATION_WRITER_ID` | `standalone-relay` | Logical access-service writer group. |
| `POCKETSTATION_SESSION_TTL_SECONDS` | `7200` | Logical Session lifetime; bounds owner renewal expiry. |
| `POCKETSTATION_NAME_POLICY` | `auto` | Automatic two-to-three-word fallback or fixed integer `2`–`15`. |
| `POCKETSTATION_MAX_RECEIPTS` | `400` | Retained retry receipt capacity. |
| `POCKETSTATION_INVITATION_RESOLVE_RATE_PER_MINUTE` | `120` | Redemption admission per client address. |
| `CONTROL_AUTHORITY_TIMEOUT_MS` | `5000` | Managed admission deadline, capped at five seconds. |

See [access and storage](../access-service.md) for atomicity, restart and expiry semantics.

## ICE and public addresses

| Variable | Default | Use |
|---|---:|---|
| `ICE_UDP_PORT` | operating-system assigned | One multiplexed UDP port for Relay WebRTC peers. Set and expose a fixed port in production. |
| `ICE_TCP_PORT` | disabled | Optional IPv4 TCP listener for passive ICE-TCP host candidates alongside UDP. |
| `RELAY_PUBLIC_IPS` | unset | Comma-separated public IPs advertised as host candidates. `auto` uses supported host discovery. |
| `RELAY_ENABLE_RED` | disabled | Set to `1` to enable the negotiated RED listener behavior. |

When `RELAY_PUBLIC_IPS` is set outside Fly.io, Relay binds the UDP mux to the
first advertised IP so the packet source matches the ICE candidate. On Fly.io,
it uses the platform's `fly-global-services` address. Explicit loopback binding
or advertisement enables loopback ICE candidates for local development only;
ordinary production configuration does not gain loopback candidates.

The existing `ICE_TCP_PORT` listener binds `tcp4`. In this source checkout, both
default Relay-owned peer constructors (WebSocket signaling and WHIP/WHEP)
enable UDP4, UDP6 and TCP4 when that mux is configured. No TCP mux leaves Pion's
default UDP4/UDP6 selection intact. Injected `SettingEngine` network types stay
authoritative; signaling also preserves an injected `API`. Callers using those
overrides must enable TCP themselves. The configured TCP listener does not
imply TCP6 support.

Locked Pion can request IPv6 TCP candidates while UDP6 is enabled. Relay's
default settings therefore guard the known Pion `TCPMuxDefault` when its actual
listener address is IPv4: IPv6 or malformed local-interface requests are
rejected before delegation. UDP6 gathering is unchanged. Unknown and multiport
muxes retain their identity and optional interfaces; injected settings keep
their original mux. No global IP filter or extra listener is introduced.

For example, add `ICE_UDP_PORT=4802 ICE_TCP_PORT=4803` to the authenticated
startup recipe and expose UDP 4802 plus TCP 4803. Public candidate addresses
still follow `RELAY_PUBLIC_IPS`; a listener alone does not configure a public
address, firewall or NAT mapping. ICE-TCP does not share the HTTP/WSS listener,
provide HTTP CONNECT proxying or enable TURN. Finite local production-peer
candidate gathering is `LOOPBACK-ONLY` configuration evidence, not deployed
connectivity, decoded audio or phone qualification.

## Embedded TURN

| Variable | Default | Use |
|---|---:|---|
| `TURN_PUBLIC_IP` | disabled | Public TURN address. `auto` uses supported host discovery. |
| `TURN_SHARED_SECRET` | required when TURN is enabled | Generates short-lived TURN credentials; at least 32 bytes. |
| `TURN_UDP_PORT` | `3478` | STUN and TURN over UDP. |
| `TURN_TCP_PORT` | `3478` | TURN over TCP; set `0` to disable. |
| `TURN_TLS_PORT` | disabled | TURN over TLS; enabling the port also requires a deployment that terminates TLS correctly. |
| `TURN_RELAY_MIN_PORT` | operating-system assigned | First UDP allocation port when a finite range is configured. |
| `TURN_RELAY_MAX_PORT` | operating-system assigned | Last UDP allocation port. |
| `TURN_REALM` | `pocketstation.io` | TURN authentication realm. |

Expose the complete allocation range. A configured TURN listener with blocked
allocation ports will authenticate clients but fail media relay.
