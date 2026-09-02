# Configure Relay

Relay reads configuration from environment variables at process start. Invalid
non-negative integer values are rejected in favor of the documented default;
required secrets and mode-specific URLs fail startup.

## HTTP and authentication

| Variable | Default | Use |
|---|---:|---|
| `PORT` | `4800` | HTTP, WebSocket signaling, WHIP, WHEP, health, and metrics listener. |
| `RELAY_AUTHORITY_MODE` | `control-plane` | `control-plane` or `standalone`. |
| `POCKETSTATION_JWT_SECRET` | required | Verifies source capabilities; at least 32 bytes. |
| `RELAY_INVITATION_SECRET` | required in standalone mode | Signs and verifies subscriber capabilities; at least 32 bytes. |
| `RELAY_API_SERVER_URL` | required in control-plane mode | Base URL for Relay state updates. |
| `POCKETSTATION_INTERNAL_SECRET` | required by the configured callback client | Authenticates Relay state updates to the Control Plane. |
| `ALLOWED_ORIGINS` | any origin | Comma-separated browser origins accepted by the signaling WebSocket. Native clients without an `Origin` header still require a capability. |
| `PUBLIC_RELAY_URL` | unset | Public Relay URL returned in invitations. |
| `PUBLIC_RECEIVER_URL` | unset | Browser receiver URL used to build join links. |
| `WEBHOOK_URL` | unset | Optional bounded event delivery destination. |

## Session capacity and time

| Variable | Default | Use |
|---|---:|---|
| `RELAY_MAX_ROOMS` | `100` | Maximum active RelaySessions. |
| `RELAY_MAX_SUBSCRIBERS_PER_SESSION` | `50` | Maximum active subscribers for one RelaySession. |
| `RELAY_MAX_BUSES_PER_SESSION` | `16` | Maximum named AudioBuses retained by one RelaySession. |
| `RELAY_MAX_CONCURRENT_HANDSHAKES` | `128` | Maximum signaling and WHIP/WHEP handshakes awaiting media allocation. |
| `RELAY_MAX_INVITATIONS` | four times `RELAY_MAX_ROOMS` | Maximum pending standalone invitations. |
| `MAX_ROOMS_PER_IP_PER_MINUTE` | `10` | Standalone RelaySession creation rate per client IP. |
| `ROOM_EXPIRY_MINUTES` | `30` | Time an inactive RelaySession remains before closing. |
| `SOURCE_RECONNECT_WINDOW_SEC` | `60` | Time subscribers remain attached after the last publisher disconnects. |
| `CONTROL_RECONCILE_INTERVAL_SEC` | `5` | Interval between complete Control Plane state reconciliations. |
| `SHUTDOWN_GRACE_PERIOD_SEC` | `30` | Process shutdown deadline. The HTTP server uses a five-second drain within it. |

The values checked into `fly.toml` are intentionally smaller for the shared
demonstration deployment. They are not production recommendations.

## ICE and public addresses

| Variable | Default | Use |
|---|---:|---|
| `ICE_UDP_PORT` | operating-system assigned | One multiplexed UDP port for Relay WebRTC peers. Set and expose a fixed port in production. |
| `ICE_TCP_PORT` | disabled | Optional TCP port for ICE-TCP. |
| `RELAY_PUBLIC_IPS` | unset | Comma-separated public IPs advertised as host candidates. `auto` uses supported host discovery. |
| `RELAY_ENABLE_RED` | disabled | Set to `1` to enable the negotiated RED listener behavior. |

When `RELAY_PUBLIC_IPS` is set outside Fly.io, Relay binds the UDP mux to the
first advertised IP so the packet source matches the ICE candidate. On Fly.io,
it uses the platform's `fly-global-services` address.

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
