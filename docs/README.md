# PocketStation Relay documentation

- [Run Relay locally](getting-started/run-relay.md)
- [Configure Relay](reference/configuration.md)
- [Secure credentials and browser access](operations/security.md)
- [Set capacity and recover from failures](operations/capacity-and-recovery.md)
- [Session access, naming and storage](access-service.md)
- [WebSocket signaling](reference/signaling.md)
- [HTTP and WebRTC endpoints](reference/http-and-webrtc.md)

Relay owns the complete Session/join domain and live media transport. Standalone
mounts both; managed control mounts the same Relay-owned access module and
supplies persistence and operations. Memory is the default, SQLite is optional,
and the control composition also supports PostgreSQL. No database is required
for development. Relay does not capture audio, run model connectors or write
multistem recordings. Those remain Core, SDK and connector responsibilities.
