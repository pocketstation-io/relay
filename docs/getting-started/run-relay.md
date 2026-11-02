# Run Relay locally

Use Go 1.26 and a host that permits UDP. Standalone is complete and is the
binary default; managed operation uses the same credential rules.

## Start standalone

```bash
export POCKETSTATION_JWT_SECRET="$(openssl rand -hex 32)"
export RELAY_AUTHORITY_MODE=standalone
export PUBLIC_RELAY_URL=http://127.0.0.1:4800
export PUBLIC_RECEIVER_URL=http://127.0.0.1:4173
export RELAY_PUBLIC_IPS=127.0.0.1
go run ./cmd/relay-server
```

The receiver is a separate application; setting its URL does not start it.
The explicit loopback address enables local ICE candidates. This setup is
LOOPBACK-ONLY and cannot serve remote receivers.

```bash
curl --fail http://127.0.0.1:4800/healthz
curl --fail http://127.0.0.1:4800/readyz
curl --fail http://127.0.0.1:4800/.well-known/pocketstation
```

Liveness and readiness do not establish media delivery. Use a supported SDK or
`pks` to create a Session, declare its AudioBuses and share a scoped link. Readable
words locate a grant; its opaque fragment credential is required to redeem it.
Keep owner capabilities and complete share links out of logs.

To retain logical Sessions across restart, additionally set
`POCKETSTATION_STORAGE=sqlite` and `POCKETSTATION_SQLITE_PATH` to an owner-writable
local database file. Restart does not restore WebRTC connections.

The deterministic `relay-test-source` accepts an existing Session and its
owner/source capability. In standalone mode it can create a temporary Session
when both arguments are omitted. It emits synthetic Opus, not captured audio. See
its `--help` and the [root example](../../README.md#run-locally).

## Managed composition

Start the control executable with the same signing key and a shared internal
secret, then configure the separate media Relay:

```bash
export RELAY_AUTHORITY_MODE=control-plane
export RELAY_API_SERVER_URL=http://127.0.0.1:4801
export PUBLIC_CONTROL_PLANE_URL=http://127.0.0.1:4801
# Set POCKETSTATION_INTERNAL_SECRET to the same secret as control.
go run ./cmd/relay-server
```

Clients use discovery's `authority_url` for Session operations. Control invokes
Relay’s shared access module; media performs authenticated admission and state
updates. An unavailable access service denies new admission while established
media follows the documented [lifecycle](../reference/signaling.md).

## Public networks

Use HTTPS, reachable ICE addresses and UDP ports, and TURN where needed. Do not
advertise loopback to remote clients. A WAN claim needs a separately recorded
candidate pair, network conditions and receiver measurements; local tests do
not establish it.
