# Secure credentials and browser access

Relay accepts scoped, expiring capabilities. A source capability authorizes one
RelaySession and a finite list of AudioBus IDs. A subscriber capability
authorizes exactly one RelaySession and one AudioBus.

## Keep the two operating modes separate

In control-plane mode:

- the Control Plane signs source and subscriber capabilities;
- Relay verifies the control-plane issuer, Relay audience, token type, role,
  expiration, token ID, RelaySession ID, and AudioBus scope; and
- Relay sends authenticated complete state snapshots to the Control Plane.

In standalone mode, Relay issues both capability types itself. Credentials from
one mode are not accepted in the other.

Choose the mode once per deployment. Do not add fallback verification with the
other issuer or secret.

## Store secrets outside source code

Provide signing and internal secrets through the deployment secret manager.
Do not place them in `fly.toml`, container images, example programs, browser
JavaScript, logs, or metrics.

Rotate a secret by coordinating the issuer and Relay. Because capabilities are
short-lived JWTs, a rotation plan must account for active sessions and the
remaining token lifetime. Relay currently accepts one configured secret for
each role; overlapping key rotation requires deployment coordination rather
than hidden multi-key acceptance.

## Restrict browsers

Set `ALLOWED_ORIGINS` to the exact HTTPS origins that host your receiver. The
origin check protects browser WebSocket use; it does not replace capability
verification. Native clients commonly omit `Origin` and must still present a
valid capability.

Serve public signaling and receiver pages over TLS. Do not send capabilities in
logs or analytics URLs. Invitation codes are single-use in standalone mode and
should still be treated as credentials until consumed or expired.

## Expose diagnostics deliberately

`/metrics` and RelaySession diagnostic endpoints contain operational state.
The current server does not add an administrator authentication scheme to
those HTTP endpoints. Restrict them with a private network, service proxy, or
ingress policy before exposing Relay publicly.

Packet logs and media diagnostics must not become a substitute for media
encryption or retention policy. Limit access and retention according to the
application's security requirements.

## Treat native media encryption separately

WebRTC protects transport with DTLS-SRTP. Optional SFrame messages provide a
separate end-to-end media encryption mechanism when both clients implement the
same key exchange. Deployment security still includes credential issuance,
TLS, network access, process isolation, and secret rotation.
