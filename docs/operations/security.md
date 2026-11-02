# Secure credentials and browser access

Both modes use the same Relay-owned Session policy. New credentials bind the
Relay issuer, Session incarnation, audience, role and exact bus scope. Explicit
legacy migrations retain their recorded issuer with a cutoff; there is no
caller-selected issuer fallback. See the [signaling specification](../reference/signaling.md).

Owner credentials manage a Session and its declared buses. Granular publisher
credentials permit only their listed buses; a subscriber credential permits
one bus. A readable name grants nothing by itself. Share links carry the opaque
one-use credential in their fragment; redemption sends it in a POST body.
Treat both opaque and readable link forms as credentials. Word count and the
legacy visibility label do not change authorization.

Store signing and internal-request secrets outside code, images and browser
JavaScript. Keep capabilities and full links out of logs and analytics. Renewal
must occur before the returned owner expiry. Rotation requires coordinated
issuer/media configuration; automatic overlapping-key rotation is not promised.

Set `ALLOWED_ORIGINS` to exact receiver origins and serve public endpoints over
TLS. The WebSocket origin check does not replace capability verification.
Native clients can omit `Origin` but still require a valid capability.

Session `/health`, `/latency`, `/media-debug` and `/packet-log` require a valid
owner or subscriber capability for that exact Session; media-only publishers
are rejected. Packet-log access also enforces bus scope. `/metrics` has no
administrator authentication scheme; restrict it with network/ingress policy.
Internal routes require the configured internal secret.

WebRTC uses DTLS-SRTP. SFrame signaling alone does not establish end-to-end
media encryption: both endpoints must implement and qualify it. The current
browser receiver must not be represented as SFrame-qualified.
