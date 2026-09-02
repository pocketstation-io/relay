# Set capacity and recover from failures

Relay rejects new work when a finite limit is reached. It does not expand an
unbounded session, handshake, invitation, callback, repair, or packet queue.

## Size admission limits together

Set these values from the expected concurrent audience and host budget:

- active RelaySessions;
- named AudioBuses per RelaySession;
- subscribers per RelaySession;
- concurrent signaling and WHIP/WHEP handshakes;
- pending invitations;
- RelaySession creation rate; and
- TURN allocation port count.

When admission fails, clients receive an explicit HTTP or signaling failure.
Apply bounded retry with jitter in the client only for failures that can
recover. Invalid credentials, invalid bus scope, and malformed signaling need
configuration or code changes, not retry.

## Monitor the process

`GET /healthz` confirms the HTTP server is responding. Scrape `GET /metrics`
for:

- active RelaySessions and subscribers;
- forwarded and dropped RTP packets;
- active and rejected handshakes;
- ICE restarts;
- callback and webhook failures or dropped notifications; and
- successful key exchanges.

Use the RelaySession endpoints for one affected session:

```text
GET /v1/sessions/{id}/health
GET /v1/sessions/{id}/latency
GET /v1/sessions/{id}/media-debug
GET /v1/sessions/{id}/packet-log?bus={bus_id}&limit=100
```

The packet-log limit is capped at 1,000 records. Restrict these endpoints before
a public deployment.

## Handle publisher loss

When a publisher disconnects, Relay keeps subscribers attached for the finite
source reconnect window. A replacement publisher increments the source
generation. Receivers and the Control Plane can distinguish the new attachment
from uninterrupted media.

After the reconnect window or RelaySession expiry, create or resolve a new
RelaySession according to the application's ownership rules. Do not reuse an
expired capability indefinitely.

## Shut down cleanly

On `SIGTERM` or `SIGINT`, Relay:

1. stops periodic Control Plane reconciliation;
2. tells signaling peers that Relay is shutting down;
3. closes signaling connections;
4. gives HTTP work a finite drain interval;
5. closes active RelaySessions and media peers; and
6. stops admission-limit workers.

Clients should reconnect only within their own finite retry budget. A restart
can change the Relay epoch; the next complete Control Plane snapshot replaces
the previous Relay-owned state.

## Qualify the network you claim

A same-host test proves only same-host behavior. A public-network result must
record the selected ICE candidate pair, whether TURN was used, packet loss,
reconnect behavior, and receiver evidence. Latency results must name the two
timestamps, their clocks, the unit, p50, p95, p99, maximum, sample count, host,
region, and load.
