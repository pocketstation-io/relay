# PocketStation Relay

`pocketstation-io/relay` is PocketStation's bounded, source-aware WebRTC audio
plane. It keeps application, microphone, and generated-audio buses independently
addressable while preserving source attachment, reconnect generation, SSRC,
and capture-clock continuity for native and browser receivers.

It does not capture audio, compile the PocketStation pipeline, run connectors,
record stems, or own durable product Session state.

## Local run

```bash
POCKETSTATION_JWT_SECRET=development-only-secret \
  go run ./cmd/relay-server
```

The local default is `http://localhost:4800`.

Health, metrics, and canonical Session creation:

```bash
curl -sf http://localhost:4800/healthz
curl -sf http://localhost:4800/metrics
curl -sS -X POST http://localhost:4800/v1/sessions
```

The Session response includes `session_id`, `source_token`, and
`subscriber_token`. The relay WebSocket is `/v1/signal`; WHIP and WHEP are
available under `/v1/sessions/{id}/whip` and `/v1/sessions/{id}/whep`.

The control-plane can also issue compatible Session credentials and TURN
metadata. Relay callbacks target its canonical internal Session/subscriber
paths. `RELAY_API_SERVER_URL` is the retained environment-variable spelling for
that control-plane callback base URL.

## Architecture and ownership

The media path is:

```text
authenticated source attachment
  → RelaySession
      → named AudioBus
          → source generation / SSRC / capture clock
          → bounded BusSubscription
              → pacing / continuity / repair
                  → native or browser receiver
```

The relay owns WebRTC signaling adaptation, SDP/ICE, WHIP/WHEP, RTP/RTCP,
Opus/RED negotiation, pacing, continuity, repair, embedded TURN/ICE-TCP, and
transport telemetry.

The individual WebRTC, WHIP/WHEP, RTP, bounded-queue, and clock-mapping
mechanisms are established systems techniques. Relay's product boundary is
their source-aware composition: stable semantic buses survive transient
transport identities and remain distinct for AI consumption, browser playout,
and downstream recording. Complete PocketStation `FrameLineage` delivery to
every remote receiver is not claimed until that separate protocol proof exists.

`pocketstation-io/protocol` owns cross-language wire schemas.
`internal/signaling` is the relay's package-private Go adapter.
`pocketstation-bench` owns neutral transport measurement tools, and
`pocketstation-lab` owns cross-repository product-proof orchestration.

See [the repository contract](docs/REPO_CONTRACT.md) and
[the architecture authority note](docs/architecture/pocketstation-v3.0.md).

## Compatibility

Current primary vocabulary is Session/subscriber:

```text
/v1/sessions · session_id · subscriber_token · SESSION_STATE
```

Selected `/v1/rooms`, `room_id`, `listener_token`, and `ROOM_STATE` surfaces
remain as explicitly tested wire aliases. They are compatibility debt, not the
primary API.

## Configuration

| Variable | Purpose | Local default |
|---|---|---|
| `PORT` | HTTP/WebSocket listen port | `4800` |
| `POCKETSTATION_JWT_SECRET` | HS256 Session credential secret | insecure development fallback |
| `RELAY_API_SERVER_URL` | Optional control-plane callback base URL | disabled |
| `RELAY_MAX_ROOMS` | Maximum active RelaySessions | package default |
| `RELAY_MAX_LISTENERS_PER_ROOM` | Maximum subscribers per RelaySession; legacy variable spelling | package default |
| `RELAY_MAX_BUSES_PER_SESSION` | Maximum retained named AudioBuses per RelaySession | `16` |
| `RELAY_MAX_CONCURRENT_HANDSHAKES` | Maximum WebSocket and WHIP/WHEP handshakes awaiting media allocation | `128` |
| `RELAY_MAX_CONCURRENT_CALLBACKS` | Maximum concurrent control-plane callback deliveries | `32` |
| `ROOM_EXPIRY_MINUTES` | Inactive RelaySession expiry; legacy variable spelling | `30` |
| `SOURCE_RECONNECT_WINDOW_SEC` | Source reconnection window | `60` |
| `ICE_UDP_PORT` | Shared Pion UDP mux port; zero selects an ephemeral local port | `0` |
| `ICE_TCP_PORT` | Enables shared ICE-TCP mux | disabled |
| `RELAY_PUBLIC_IPS` | Advertised public IPs or `auto` | unset |
| `TURN_PUBLIC_IP` | Enables the embedded TURN server | unset |
| `TURN_UDP_PORT` | TURN UDP port | `3478` |
| `TURN_TCP_PORT` | TURN TCP port | `3478` |
| `TURN_TLS_PORT` | TURNS port; zero disables TLS | `0` |
| `RELAY_ENABLE_RED` | Opt in to RFC 2198 RED wrapping | disabled |

Fly uses port `8080` through deployment configuration; that does not change the
local binary default.

## Verification

```bash
test -z "$(gofmt -l .)"
go vet ./...
go test -short ./...
go test -race ./...
```

The full race suite includes bounded local soak cells. Use `-short` for fast
iteration. Explicit long-soak environment flags are reserved for one frozen
candidate after preflights pass.

## Claim boundary

- Phase 1 relay transport is `REAL`.
- Same-host Pion and CI results are component or `LOOPBACK-ONLY` evidence.
- The Fly deployment is cross-network calibration evidence, not proof of
  production readiness or multi-region session migration.
- Competitive latency and quality claims require PocketStation Bench artifacts.
- Relay does not claim general performance superiority over LiveKit or another
  SFU; comparisons must use equivalent codec, impairment, and receiver planes.
- End-to-end product claims require immutable PocketStation Lab artifacts with
  exact repository commits, devices, and network conditions.
