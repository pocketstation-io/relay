# Multi-Region WebRTC SFU Scaling Patterns

**Status:** Scaling roadmap -- not immediate work.
**Current approach:** Single-region deployment (Tier 4). This is correct for current scale.
**Last updated:** 2026-07-01

---

## 1. Problem Statement

WebRTC SFU deployments face a fundamental split-transport problem when scaling
beyond a single region: WebSocket signaling (TCP) and UDP media can route to
different machines under anycast or geographic load balancing.

The core failure mode:

1. A client connects via WebSocket to machine A in `iad` (TCP, routed by DNS
   or anycast).
2. Machine A returns ICE candidates advertising the cluster's anycast IP.
3. The client sends STUN binding requests and DTLS ClientHello packets over
   UDP to the same anycast IP.
4. UDP packets arrive at machine B in `cdg` (different region, different
   machine -- UDP anycast routing is based on BGP path cost, not application
   state).
5. Machine B has no PeerConnection context for this client. The packets are
   silently dropped.
6. The client sees ICE failure after 5-30 seconds of retries.

This is not a theoretical concern. It is the primary reason WebRTC deployments
on shared anycast infrastructure (including fly.io) fail intermittently, and
why every production-scale WebRTC service has had to solve transport co-location
explicitly.

### Why This Matters for PocketStation

PocketStation's relay is a Go/Pion SFU. The relay currently runs as a single
fly.io machine in `iad`. As usage grows, users in Europe and Asia-Pacific will
experience 100-200ms of additional round-trip latency on every audio frame.
Scaling to multiple regions requires solving the signaling/media co-location
problem without building a private global network.

---

## 2. Industry Patterns (4 Tiers)

### Tier 1: Private Backbone + Edge PoPs

**Who uses this:** Google Meet, Agora (SD-RTN), Meta (Messenger/Instagram calls).

**How it works:**

- Own the global network infrastructure: private fiber, custom BGP, peering
  agreements with every major ISP.
- Deploy edge PoPs (Points of Presence) in 50-200+ metros worldwide.
- Signaling and media are co-located by definition -- both land on the same
  edge PoP, connected to every other PoP via private backbone.
- Media is forwarded between PoPs over the private backbone with guaranteed
  latency and zero public-internet jitter.
- SRTP keys and ICE state never leave the private network.

**Characteristics:**

- PeerConnection establishment: <50ms globally.
- Packet loss between PoPs: effectively zero (private fiber, no congestion).
- Operational cost: tens of millions USD/year for network alone.
- Engineering team: 50-200+ dedicated networking engineers.

**Applicability to PocketStation:** None. Requires private fiber, custom BGP
announcements, and peering agreements that are not replicable by a small team.
This is documented for completeness and to explain why Google Meet "just works"
globally while self-hosted SFUs do not.

---

### Tier 2: Anycast + Just-in-Time Consensus (Cloudflare Calls)

**Who uses this:** Cloudflare Calls (production since 2024).

**How it works:**

- Single anycast IP for everything: HTTPS signaling, STUN, DTLS, SRTP.
- Accept that UDP packets will hit different servers in different PoPs --
  this is treated as a feature, not a bug.
- When a DTLS packet arrives at a server that does not own the PeerConnection:
  1. The server extracts the ICE ufrag from the STUN binding request (or the
     DTLS fingerprint from the ClientHello).
  2. It queries a distributed routing table to find which server owns this
     session.
  3. It fetches the negotiated DTLS parameters (keys, sequence numbers) from
     the owning server over the internal network.
  4. It handles the packet locally -- no proxy, no redirect. The edge server
     becomes a full participant in the DTLS session.
- NACK retransmission happens at the edge closest to the user, reducing
  retransmit RTT from cross-region to local.

**Characteristics:**

- PeerConnection establishment: 100-250ms globally (consensus round-trip).
- Requires 300+ PoPs with own anycast network to make this viable.
- Consensus protocol must handle: session migration, DTLS epoch rollover,
  SRTP rollover counter synchronization, ICE restart across PoPs.
- Engineering complexity: very high. Cloudflare built this on top of 10+
  years of anycast infrastructure investment.

**Applicability to PocketStation:** Theoretically possible over fly.io's
private network (`fdaa::/16`), but the complexity is disproportionate to the
benefit at current scale. Would require:

- Shared state store (Redis or NATS on fly.io) for ICE ufrag routing table.
- DTLS session migration or transparent proxy between relay instances.
- SRTP context sharing (master key, SSRC mapping, rollover counters).
- Consensus protocol with <100ms latency over WireGuard mesh.

**Estimated effort:** 4-8 weeks of dedicated engineering, assuming the
consensus protocol is kept simple (query-and-proxy rather than full session
migration). Risk: high. The edge cases around DTLS epoch transitions and
SRTP rollover are subtle and hard to test without a multi-region test harness.

---

### Tier 3: GeoDNS + Cascading SFUs

**Who uses this:** LiveKit Cloud, Jitsi (JVB + Oleo/Octo), Twilio,
Daily.co, Stream Video.

This is the most common production pattern for WebRTC SFUs that need global
reach without owning network infrastructure.

**How it works:**

1. Deploy SFU instances in multiple cloud regions (e.g., `us-east-1`,
   `eu-west-1`, `ap-northeast-1`).
2. GeoDNS (Route53 latency-based routing, Cloudflare geo-steering, or
   similar) routes each client to the nearest regional SFU based on the
   client's resolver location.
3. The client connects via WebSocket to its regional SFU. The same SFU
   provides ICE candidates with its own dedicated IP. Signaling and media
   are co-located because DNS routed the initial connection and the same
   server provides both.
4. Each regional SFU handles local clients' media (DTLS, SRTP, RTCP).
5. When a room spans multiple regions, SFUs cascade media between regions
   using an inter-SFU protocol over the cloud provider's private network
   or encrypted tunnels over the public internet.

**Inter-SFU protocol examples:**

| Service       | Protocol                          | Transport           |
|---------------|-----------------------------------|---------------------|
| LiveKit Cloud | FlatBuffers overlay, custom       | Encrypted TCP/UDP   |
|               | inter-server signaling            | over private network |
| Jitsi (Octo)  | RTP wrapping with custom Oleo     | UDP over private    |
|               | header (oleo-type, oleo-seq)      | network             |
| Stream Video  | Redis Streams with ~100ms polling | Redis pub/sub       |

**Characteristics:**

- PeerConnection establishment: 50-150ms (regional, no cross-region hop for
  initial setup).
- Cross-region media latency: cloud provider's inter-region latency (typically
  30-80ms US-to-EU, 100-180ms US-to-APAC).
- Room placement decision required: which region "owns" a room? Options:
  first-joiner's region, explicit API parameter, or multi-home with cascading.
- Each region needs its own dedicated IPv4 address for UDP.
- DNS TTL must be low enough (30-60s) for failover but high enough to avoid
  excessive DNS lookups.

**Applicability to PocketStation:** Most viable path for multi-region scaling.
The relay's existing RTP forwarding logic (`room.go` forwardLoop) can be
adapted for inter-relay cascading with modest changes. Needs:

- Separate fly.io app per target region, each with dedicated IPv4.
- GeoDNS via Cloudflare (free tier supports geo-based routing).
- Inter-relay cascading protocol over fly.io private network (`fdaa::/16`).
- Room placement logic (pin room to creation region initially; cascade later).
- Health checking and failover between regions.

**Estimated effort:** 2-4 weeks for Phase S1 (regional dedicated IPs, no
cascading). Additional 3-5 weeks for Phase S2 (inter-relay cascading).

---

### Tier 4: Single Region (Current)

**Who uses this:** Most self-hosted LiveKit, Janus, mediasoup, and Pion
deployments. This is the default for any WebRTC SFU that has not explicitly
solved multi-region.

**How it works:**

1. Deploy SFU in one region (e.g., `iad` on fly.io).
2. All signaling (WebSocket) and media (UDP) hit the same machine.
3. TURN server (embedded or external) handles NAT traversal for clients
   behind symmetric NATs or restrictive firewalls.
4. Accept higher latency for geographically distant users.

**Characteristics:**

- PeerConnection establishment: 50-100ms for nearby clients, 150-400ms for
  distant clients.
- No split-transport problem -- everything lands on the same machine.
- No inter-SFU protocol needed.
- No room placement logic needed.
- Scale ceiling: ~100 concurrent rooms on a single machine (depends on
  machine size and per-room participant count).
- Operational complexity: minimal.

**Applicability to PocketStation:** This is the current approach. It is the
correct choice for the current stage: benchmarks, development, early users,
and fewer than 100 concurrent rooms. Moving to Tier 3 is warranted only when
users in multiple continents report unacceptable latency or when room count
exceeds single-machine capacity.

---

## 3. Fly.io-Specific Constraints

PocketStation's relay runs on fly.io. The following constraints are specific
to fly.io's networking model and directly affect multi-region scaling
decisions.

### UDP Routing

- **No `fly-replay` for UDP.** `fly-replay` is a Layer 7 HTTP concept. It
  works by having the edge proxy re-issue an HTTP request to a specific
  machine. UDP has no equivalent mechanism -- there is no HTTP request to
  replay.
- **UDP anycast is inconsistent across regions.** Fly.io uses eBPF-based
  4-tuple flow tracking for UDP. Once a flow is established (same src IP,
  src port, dst IP, dst port), packets are pinned to the same machine. But
  the initial packet's routing is based on BGP path cost, not geographic
  proximity or application state.
- **Dedicated IPv4 required for UDP.** Shared anycast IPv4 addresses route
  UDP unpredictably. A dedicated IPv4 per app (or per machine) is required
  for reliable UDP routing.
- **Internal port must match external port for UDP.** Unlike TCP services
  where fly.io can map external port 443 to internal port 8080, UDP services
  must listen on the same port externally and internally.
- **`fly-global-services` bind address.** UDP services must bind to the
  `fly-global-services` address (not `0.0.0.0`) to receive traffic through
  fly.io's edge network.

### WireGuard Mesh (Private Network)

- Fly.io's private network (`fdaa::/16`) uses WireGuard tunnels between
  machines.
- WireGuard adds ~72 bytes of overhead per UDP packet (WireGuard header +
  outer UDP header + outer IP header).
- Effective MTU over the private network is ~1300 bytes (1372 minus headers),
  which is below the standard 1400-byte RTP packet size used by most WebRTC
  implementations.
- This means inter-relay cascading over the private network must either:
  - Fragment RTP packets (adds complexity and latency).
  - Use a smaller MTU for inter-relay traffic (requires careful PMTU
    discovery).
  - Use TCP for inter-relay cascading (adds latency but avoids fragmentation).

### Prior Art on Fly.io

- The `bekriebel/livekit-flydotio` project attempted to run LiveKit on
  fly.io. It was abandoned due to persistent UDP connectivity issues --
  specifically, STUN/DTLS packets arriving at machines with no PeerConnection
  context. The project's README documents the failure mode in detail.
- Fly.io's own documentation acknowledges that WebRTC is a difficult use
  case on their platform and recommends dedicated IPv4 addresses and
  single-region deployments for reliability.

### Per-Port UDP Routing (2025)

- Fly.io introduced per-port UDP routing in 2025, allowing different UDP
  ports to route to different machines.
- This does not solve the cross-region problem: it only controls which
  machine within a region receives traffic on a given port. The initial
  routing to a region is still based on BGP anycast.

---

## 4. PocketStation Scaling Roadmap

This roadmap is sequenced by need. Each phase should be triggered by observed
user pain or capacity limits, not by speculative scaling.

### Phase Current: Single Region (Tier 4)

**Status:** Active. This is the current production configuration.

**Configuration:**

- Single fly.io machine in `iad` (Ashburn, Virginia).
- WebSocket signaling on port 443 (TCP, TLS terminated by fly.io edge).
- UDP media on port 3478 (dedicated IPv4).
- Embedded TURN server on TCP 3478 for NAT traversal.
- All signaling and media co-located on the same machine.

**Capacity:** ~100 concurrent rooms (estimated, depends on participant count
and audio complexity per room).

**Sufficient for:** Benchmarks, development, demos, early adopters, and any
deployment where all users are in North America or latency tolerance is
>200ms.

**Estimated effort:** Zero -- this is the current state.

**Trigger to move to Phase S1:** Users in Europe or Asia-Pacific report
>200ms one-way audio latency, or room count approaches single-machine
capacity.

---

### Phase S1: Regional Dedicated IPs

**Goal:** Reduce latency for users in multiple continents by running
independent relay instances in each target region.

**Architecture:**

- Separate fly.io app per target region:
  - `pocketstation-relay-iad` (US East)
  - `pocketstation-relay-ams` (EU West)
  - `pocketstation-relay-nrt` (Asia-Pacific)
- Each app gets its own dedicated IPv4 address.
- GeoDNS via Cloudflare (free tier supports geo-based routing rules):
  - `relay.pocketstation.io` resolves to `iad` IP for North American
    resolvers, `ams` IP for European resolvers, `nrt` IP for Asian
    resolvers.
- No inter-relay cascading. Rooms are pinned to their creation region.
  A room created by a user in Europe lives on `pocketstation-relay-ams`
  for its entire lifetime. If a US user joins that room, they connect
  to `ams` (higher latency for that user, but the room is consistent).

**Room placement logic:**

- Room creation API accepts optional `region` parameter.
- Default: region of the creating user (determined by GeoDNS).
- API server returns the region-specific relay URL in the room creation
  response.
- All subsequent joins for that room use the region-specific URL, not the
  GeoDNS hostname.

**Limitations:**

- Cross-region rooms have high latency for users far from the room's region.
- No media cascading -- all media transits through the room's home region.
- Room migration between regions is not supported.

**Capacity:** ~1000 concurrent rooms per region (3000 total across 3
regions).

**Sufficient for:** Regional user bases where most rooms are
geographically clustered (e.g., a European team using European rooms).

**Estimated effort:** 2-4 weeks.

- Week 1: Fly.io multi-app configuration, dedicated IPv4 per app,
  deployment automation.
- Week 2: Cloudflare GeoDNS setup, room creation API changes for region
  parameter, client SDK changes to use region-specific URLs.
- Weeks 3-4 (if needed): Monitoring, health checks, failover logic,
  documentation.

**Trigger to move to Phase S2:** Rooms regularly span continents (e.g., a
US broadcaster with European subscribers), making single-region pinning
unacceptable.

---

### Phase S2: Inter-Relay Cascading

**Goal:** Support rooms that span multiple regions with low latency for
all participants, regardless of their geographic location.

**Architecture:**

- All Phase S1 infrastructure remains.
- Inter-relay cascading protocol over fly.io private network (`fdaa::/16`):
  - When a subscriber in `ams` joins a room owned by `iad`, the `ams`
    relay opens a cascading connection to the `iad` relay.
  - The `iad` relay forwards the room's mixed audio stream to `ams` over
    the WireGuard mesh.
  - The `ams` relay decrypts, re-encrypts (with the subscriber's SRTP
    keys), and forwards to the local subscriber.
- Room state shared via lightweight coordination service:
  - NATS JetStream or Redis (deployed as a fly.io app in a central region).
  - Stores: room-to-region mapping, participant roster, cascade topology.
  - Not on the media hot path -- used only for room join/leave signaling.

**Inter-relay protocol design:**

- Reuse existing RTP forwarding logic from `room.go` forwardLoop.
- Wrap RTP packets in a thin cascade header:
  - Room ID (16 bytes, UUID).
  - Source relay ID (4 bytes).
  - Sequence number (4 bytes, for cascade-level loss detection).
- Transport: UDP over `fdaa::/16` with MTU capped at 1280 bytes to
  account for WireGuard overhead.
- Fallback: TCP over `fdaa::/16` if UDP fragmentation causes issues.

**Cascade topology:**

- Star topology initially: one "home" relay per room, all other relays
  cascade from it.
- Full mesh between relays deferred -- adds O(n^2) complexity for
  marginal latency improvement with small relay counts.

**Capacity:** ~10,000+ concurrent rooms across all regions.

**Sufficient for:** Global user base with cross-region rooms.

**Estimated effort:** 3-5 weeks (on top of Phase S1).

- Week 1: Inter-relay protocol design and implementation (cascade header,
  UDP transport over `fdaa::/16`).
- Week 2: Room state coordination (NATS/Redis deployment, room-to-region
  mapping, cascade establishment on subscriber join).
- Week 3: Integration testing with multi-region fly.io deployment, latency
  measurement, loss handling.
- Weeks 4-5 (if needed): Edge cases (relay failure mid-cascade, room
  migration, cascade teardown on last subscriber leave), monitoring, alerts.

**Trigger to move to Phase S3:** Relay density exceeds what GeoDNS can
handle (>20 regions), or packet-level routing optimization becomes critical.

---

### Phase S3: Edge Consensus (If Needed)

**Goal:** True anycast deployment where any UDP packet can arrive at any
relay and be handled correctly.

**Architecture:**

- Cloudflare-style just-in-time consensus for ICE/DTLS state.
- Any relay can handle any packet by querying the session owner for
  negotiated parameters.
- Eliminates the need for GeoDNS and region pinning entirely.

**Why this might never be needed:**

- Phase S2 (cascading SFUs) handles the vast majority of multi-region
  use cases.
- Edge consensus adds significant complexity (DTLS session migration,
  SRTP rollover counter synchronization, split-brain handling).
- At the scale where edge consensus becomes necessary (>20 regions,
  >100,000 concurrent rooms), a managed service (LiveKit Cloud, Cloudflare
  Calls) is likely more cost-effective than building and maintaining a
  custom consensus protocol.

**Estimated effort:** 6-10 weeks, with high risk of edge-case bugs in
DTLS/SRTP state synchronization.

**Recommendation:** Evaluate managed services before investing in Phase S3.
If PocketStation reaches the scale where Phase S2 is insufficient, the
engineering and operational cost of Phase S3 likely exceeds the cost of
using a managed WebRTC infrastructure provider for the transport layer
while keeping PocketStation's audio processing pipeline as the
differentiator.

---

## 5. Decision Matrix

| Pattern               | Complexity | Latency              | Scale Ceiling        | Cost  | When to Use                          |
|-----------------------|------------|----------------------|----------------------|-------|--------------------------------------|
| Single Region         | Low        | High for distant     | ~100 rooms           | $     | Now through early growth             |
| Regional Dedicated IPs| Medium     | Low (regional)       | ~1000 rooms/region   | $$    | When users span continents           |
| Cascading SFUs        | High       | Low (global)         | ~10,000+ rooms       | $$$   | When cross-region rooms are needed   |
| Edge Consensus        | Very High  | Lowest               | Unlimited            | $$$$  | Consider managed service instead     |

**Key decision factors:**

- **Do not pre-scale.** Each phase should be triggered by observed user
  pain, not anticipated growth. Premature multi-region deployment adds
  operational complexity without proportional benefit.
- **Latency vs. complexity tradeoff.** Phase S1 (regional IPs) solves 80%
  of the latency problem with 20% of the complexity. Phase S2 (cascading)
  solves the remaining 20% but doubles the operational surface area.
- **Managed service escape hatch.** At every phase, evaluate whether a
  managed service (LiveKit Cloud for transport, Cloudflare Calls for edge
  routing) is more cost-effective than building custom infrastructure. The
  audio processing pipeline -- PocketStation's differentiator -- is
  independent of the transport layer.

---

## 6. Current Status

**Deployment:** Single fly.io machine, `iad` region, dedicated IPv4.

**Scaling phase:** Tier 4 (Single Region). This is the correct choice for
the current stage of the project.

**No multi-region work is planned or in progress.** This document exists to
capture the scaling patterns and decision framework so that when
multi-region becomes necessary, the team does not have to re-derive these
patterns from first principles.

**Next action:** None until a scaling trigger is observed (see Phase S1
trigger above).

---

## 7. References

1. **LiveKit distributed architecture.**
   LiveKit documentation on multi-region deployment and inter-node
   communication. https://docs.livekit.io

2. **Cloudflare Calls: anycast WebRTC.**
   Cloudflare blog post describing the just-in-time consensus architecture
   for handling WebRTC over anycast. https://developers.cloudflare.com

3. **Jitsi Oleo/Octo cascading protocol.**
   WebRTCHacks coverage of Jitsi's inter-JVB cascading protocol for
   multi-region Oleo deployments. https://webrtchacks.com

4. **Fly.io UDP networking.**
   Fly.io documentation on UDP services, dedicated IPv4, and the
   `fly-global-services` bind address. https://fly.io

5. **Fly.io community: WebRTC/UDP routing.**
   Community threads documenting WebRTC deployment challenges on fly.io,
   including UDP anycast inconsistencies and `fly-replay` limitations.
   https://community.fly.io

6. **bekriebel/livekit-flydotio.**
   Abandoned project attempting to run LiveKit on fly.io. Documents the
   UDP routing failure mode in detail. https://github.com/bekriebel/livekit-flydotio

7. **RFC 8445: Interactive Connectivity Establishment (ICE).**
   IETF specification for ICE, relevant to understanding why STUN/DTLS
   packets must reach the correct server. https://www.rfc-editor.org/rfc/rfc8445
