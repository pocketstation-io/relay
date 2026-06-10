# RELAY_PHASE1_QUEUE.md

Phase 1 relay MVP task queue. One item per commit. Checked off when the commit lands and the progress entry is written.

- [x] 1. Make current relay compile cleanly (remove stray uuid import)
- [x] 2. Define signaling messages inside relay repo (PUBLISH, SUBSCRIBE, ICE, LEAVE, SDP_ANSWER, ROOM_STATE, ERROR)
- [x] 3. Implement room lifecycle (create, source join, listener join, leave, cleanup)
- [x] 4. Implement JWT helper (room-scoped token, source/listener roles, short expiry)
- [x] 5. Implement WebSocket signaling skeleton (all message types dispatched)
- [x] 6. Implement Pion v4 peer connection skeleton (SDP offer/answer, ICE exchange)
- [x] 7. Implement RTP forwarding scaffold (forwardLoop with done channel, listener snapshot)
- [x] 8. Add WriteRTP allocation/mutation benchmark — RELAY-009
- [x] 9. Add tests for room lifecycle and token validation
- [x] 10. Final relay audit (gofmt, go test ./..., go test -race ./...)
