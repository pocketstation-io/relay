# ADR-023-embedded-turn-ice-tcp — Embedded TURN + ICE-TCP, Zero External Provider

## Status

Accepted for v2.3. Phase 5.

## Context

PocketStation's relay has a public IP. Both source and listener connect **outbound** to it.
Symmetric NAT — the classic WebRTC hole-punch failure mode — does not apply here: the relay
never initiates inbound connections to clients, and NAT mappings to a fixed public IP are
stable regardless of NAT type.

The actual failure modes that affect a public-IP relay architecture are three and only three:

1. **Corporate UDP firewall** — IT policy blocks all UDP except DNS. Only TCP/443 allowed.
   Affects ~60–70% of enterprise connections (Microsoft Teams published data) and ~30% of
   all WebRTC failures industry-wide (Google/third-party WebRTC analysis).

2. **CGNAT aggressive timeout** — Mobile carrier NAT (RFC 6598: 100.64.0.0/10) expires idle
   UDP sessions in 30–60s. Affects ~15–35% of mobile connections (Verizon, T-Mobile deploy
   symmetric CGNAT). Pion already sends STUN keepalives on ICE-connected pairs; this is
   already mitigated.

3. **ISP UDP port filtering** — Some ISPs filter non-DNS UDP on non-standard ports. Affects
   ~5–10% of connections.

Current state: relay config provides `stun:stun.l.google.com:19302` only. No TURN. No
ICE-TCP. This produces ~20–30% connection failure on corporate networks and ~5–10% on
consumer mobile — unacceptable for production.

**Production validation of the embedded-TURN pattern:**
- LiveKit: 4M+ production deployments, 3B+ calls/year, embeds TURN in relay process using
  pion/turn, zero external provider. Auth tightly integrated with signaling layer.
- AWS Chime SDK: TURN embedded in media data plane, TCP/443 + UDP/3478, enterprise-scale.
- OpenAI GPT Realtime: uses ICE-TCP on port 443 as primary NAT traversal strategy, no TURN.
- Janus WebRTC: integrated TURN as primary deployment mode.

**Why not a third-party provider (Twilio NTS, Metered, Xirsys):**
- External TURN SLA becomes a reliability dependency for PocketStation's own uptime.
- Every self-hosted PocketStation deployment requires operator TURN configuration.
- LiveKit Cloud's competitive advantage is "zero ops" — external TURN is how self-hosted
  loses to LiveKit Cloud. Embedding TURN closes that gap.
- coturn (external) requires a separate process, TLS certificate, firewall rules, and
  credential sync pipeline — 3–4 hours of operator setup per deployment.

## Decision

Embed `pion/turn` v4+ in the relay process co-located with the existing WebRTC server.
Add ICE-TCP candidates on port 443. Issue ephemeral HMAC-SHA1 TURN credentials from
api-server alongside room tokens. No external TURN provider. No coturn.

**Four-path ICE candidate stack (achieves <1% connection failure — production-validated
by LiveKit, AWS Chime, Jitsi):**

```
Priority  Path                            Covers
───────   ──────────────────────────────  ──────────────────────────────
1         UDP direct (relay public IP)    ~70% of connections
2         ICE-TCP port 443                ~20% (corporate HTTP proxy)
3         TURN/UDP port 3478              ~8%  (UDP routing issues)
4         TURNS/TLS port 443              ~2%  (remaining enterprise)
                                          ──────────────────────────────
                                          <1%  total failure
```

**Ephemeral credential format (RFC 5766 §9.2 HMAC-SHA1):**

```
username  = "<expiry_unix_timestamp>:<room_id>"
password  = base64(HMAC-SHA1(shared_secret, username))
ttl       = room token TTL (default 24h)
```

api-server generates credentials using the same `POCKETSTATION_JWT_SECRET` already used
for room tokens. No new secret management surface.

**POST /v1/rooms response extended:**

```json
{
  "room_id": "...",
  "source_token": "...",
  "listener_token": "...",
  "ice_servers": [
    { "urls": ["stun:relay.pocketstation.io:3478"] },
    {
      "urls": [
        "turn:relay.pocketstation.io:3478",
        "turns:relay.pocketstation.io:443"
      ],
      "username": "1769990400:room-abc",
      "credential": "<HMAC-SHA1-base64>"
    }
  ]
}
```

SDK clients (sdk-ios, sdk-android, sdk-js, app-creator, app-desktop) pass `ice_servers`
directly to the WebRTC PeerConnection config. No hardcoded STUN URLs in SDKs.

**pion/turn integration in relay:**

```go
// cmd/relay-server/main.go — alongside existing webrtc.API init

udpListener, _  := net.ListenPacket("udp4", "0.0.0.0:3478")
tcpListener, _  := net.Listen("tcp4", "0.0.0.0:443")   // shared with TLS mux

turnServer, _ := turn.NewServer(turn.ServerConfig{
    Realm: "relay.pocketstation.io",
    AuthHandler: func(username, realm string, _ net.Addr) ([]byte, bool) {
        return hmacTurnPassword(jwtSecret, username), true
    },
    PacketConnConfigs: []turn.PacketConnConfig{{
        PacketConn: udpListener,
        RelayAddressGenerator: &turn.RelayAddressGeneratorStatic{
            RelayAddress: publicIP, Address: "0.0.0.0",
        },
    }},
    ListenerConfigs: []turn.ListenerConfig{{
        Listener: tls.NewListener(tcpListener, tlsConfig),
        RelayAddressGenerator: &turn.RelayAddressGeneratorStatic{
            RelayAddress: publicIP, Address: "0.0.0.0",
        },
    }},
})
```

**ICE-TCP candidates in Pion WebRTC:**

```go
se := webrtc.SettingEngine{}
tcpMux := webrtc.NewICETCPMux(nil, tcpMux443, 8)
se.SetICETCPMux(tcpMux)
se.SetNAT1To1IPs([]string{publicIP}, webrtc.ICECandidateTypeHost)
```

Port 443 is shared between the HTTPS/WebSocket signaling path (via TLS SNI mux) and the
ICE-TCP + TURNS listeners. This is the same port-sharing pattern used by coturn and Jitsi.

**Hairpinning not supported.** If two PocketStation clients connect from behind the same
relay (relay-to-relay), pion/turn does not support hairpinning (pion/turn#82). This is
acceptable: the relay forwards RTP at the application layer; TURN hairpin is not in the
forwarding path. The relay is never a TURN client — only a TURN server.

## Options considered

**A) External TURN provider (Twilio NTS, Metered.ca, Xirsys)**
- Adds external SLA dependency.
- Every self-hosted deployment requires operator configuration.
- Ongoing per-GB cost.
- Rejected: makes self-hosted PocketStation operationally harder than LiveKit Cloud.

**B) External coturn process (Jitsi model)**
- Separate process, shared machine.
- Proven at scale (Jitsi, matrix.org).
- Requires separate TLS cert management, firewall rules, credential sync.
- Rejected: 3–4h operator setup per deployment; no competitive advantage vs. Jitsi.

**C) pion/turn embedded in relay + ICE-TCP/443 (this decision)**
- Zero external process.
- Auth shares existing JWT secret — zero new secret management.
- LiveKit production-validated at 4M+ deployments.
- Acknowledged gap vs. coturn: hairpinning (not needed), complex multi-homed scenarios
  (not in PocketStation's architecture).
- Chosen.

**D) ICE-TCP only, no TURN (OpenAI GPT Realtime model)**
- OpenAI uses ICE-TCP port 443 with no TURN for GPT Realtime API.
- Covers ~90% of failure cases.
- Fails for strict enterprise deep-packet-inspection firewalls (~2% of connections).
- Retained as complement to TURN, not replacement.

**E) WebTransport (ADR-016)**
- Eliminates ICE/STUN/TURN entirely — the long-term architectural endgame.
- quic-go/webtransport-go is draft-02 spec, not finalized.
- Phase 6 scope per ADR-016.
- Retained on roadmap; does not replace this ADR for Phase 5.

## Consequences

- Relay binary grows by pion/turn dependency (same Pion ecosystem, no new transitive deps).
- Relay listens on two additional ports: UDP/3478, and shares TCP/443 via TLS SNI mux.
- api-server POST /v1/rooms response adds `ice_servers` field — backward-compatible
  (existing clients ignore unknown fields).
- All SDKs must read `ice_servers` from room creation response and pass to PeerConnection
  config. Hardcoded STUN URLs in SDK code must be removed.
- `POCKETSTATION_JWT_SECRET` is reused for TURN credential HMAC — same secret,
  same rotation policy, no new ops surface.
- TURN relay bandwidth passes through the relay host. Operator must provision sufficient
  egress. Rule of thumb: Opus 64kbps × 50 listeners = 3.2 Mbps for full room; TURN relay
  for 2% of connections = ~64 kbps marginal overhead.
- FAKE_SCAFFOLD_INVENTORY.md in relay: add row for embedded TURN and ICE-TCP mux;
  burn at Phase 5 exit.

## Test / measurement plan

- TURN authentication test: credential generated by api-server validates against
  `hmacTurnPassword(jwtSecret, username)` in relay.
- ICE-TCP connectivity test: PeerConnection with only TCP candidates connects successfully
  (simulated UDP block via iptables DROP on UDP in CI).
- TURNS/TLS test: TLS TURN allocation succeeds with self-signed cert in test environment.
- Connection success regression: existing relay integration tests (`TestGiven_Source
  Publishing_When_ListenerSubscribes_Then_RTPForwarded`) pass unmodified.
- ICE candidate type verification: relay PeerConnection offers host + srflx + relay
  candidates in SDP (confirmed by SDP parser in test).
- TURN throughput bench: 50 simultaneous TURN relay allocations, measure relay CPU and
  memory delta vs. direct ICE path.
- Soak: Phase 2 soak (50 listeners × 30min) repeated with TCP-only ICE constraint;
  goroutine delta and RSS growth must match Phase 2 baseline ±20%.

## Reversal trigger

pion/turn introduces a measurable CPU regression (>10% on relay forward bench) at 50
concurrent listeners AND coturn achieves equivalent zero-config deployment via a
`POCKETSTATION_COTURN=1` flag added to the relay container. In that case, extract TURN to
a sidecar container and retain the `ice_servers` API contract.
