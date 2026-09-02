# PocketStation Relay documentation

Start with the deployment you want to run. The reference pages are for clients
and operators that need exact message fields or configuration values.

## Get started

- [Run Relay locally](getting-started/run-relay.md)

## Deploy and operate

- [Configure Relay](reference/configuration.md)
- [Secure credentials and browser access](operations/security.md)
- [Set capacity and recover from failures](operations/capacity-and-recovery.md)

## Reference

- [WebSocket signaling messages](reference/signaling.md)
- [HTTP, WebSocket, WHIP, and WHEP endpoints](reference/http-and-webrtc.md)

Relay forwards live media. It does not capture desktop audio, run models,
write recordings, issue durable application state, or prove what a receiver's
loudspeaker played. PocketStation Core and the language SDKs own capture and
source-aware routing. The Control Plane owns RelaySession creation and scoped
credentials in a production deployment.
