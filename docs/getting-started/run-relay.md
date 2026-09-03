# Run Relay locally

This guide starts one Relay process for development. Use standalone mode when
you want Relay to create temporary RelaySessions and invitations itself. Use
control-plane mode when testing the production credential model.

## Prerequisites

- Go 1.26 or newer;
- UDP access on the host for WebRTC media; and
- two independent secrets containing at least 32 bytes each.

## Start standalone mode

Generate development secrets and start Relay:

```bash
export POCKETSTATION_JWT_SECRET="$(openssl rand -hex 32)"
export RELAY_INVITATION_SECRET="$(openssl rand -hex 32)"
export RELAY_AUTHORITY_MODE="standalone"
go run ./cmd/relay-server
```

Relay listens on `http://127.0.0.1:4800` by default. Check the process:

```bash
curl --fail http://127.0.0.1:4800/healthz
```

The response is `ok`. This check confirms that the HTTP server is running. It
does not confirm that ICE connectivity or media forwarding works.

## Publish the test bus

The repository test source can create a temporary RelaySession and publish
synthetic Opus:

```bash
go run ./cmd/relay-test-source -- \
  --relay http://127.0.0.1:4800 \
  --duration 3s
```

Use the session, bus, and credentials printed by the program with a compatible
receiver. The fixture verifies transport behavior. It is not desktop capture
or physical-device evidence.

## Start control-plane mode

Run a compatible Control Plane, then set:

```bash
export POCKETSTATION_JWT_SECRET="$(openssl rand -hex 32)"
export POCKETSTATION_INTERNAL_SECRET="$(openssl rand -hex 32)"
export RELAY_AUTHORITY_MODE="control-plane"
export RELAY_API_SERVER_URL="http://127.0.0.1:4801"
go run ./cmd/relay-server
```

In this mode, Relay rejects its local RelaySession and invitation creation
endpoints. The Control Plane issues source and subscriber capabilities and
receives complete Relay state snapshots.

## Test a public deployment

A public deployment needs reachable ICE-UDP, and normally TURN for networks
that cannot establish a direct candidate pair. Set a public IP and expose the
configured ports before making a remote-network claim. A local run with no
public IP is only local connectivity evidence.

Continue with [configuration](../reference/configuration.md), [security](../operations/security.md),
and [capacity and recovery](../operations/capacity-and-recovery.md).
