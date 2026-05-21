# ADR-009-pion-writertp-profile — Pion WriteRTP Allocation Profile

## Status
Accepted. Phase 1 baseline measurements taken 2026-05-20 (see `benches/results/phase1-baseline.txt`). Disconnected-track WriteRTP: 0 alloc/op at ~26 ns/op. Connected-track (SRTP path) measured 2026-05-21 (see `benches/results/phase2-baseline.txt` and "Phase 2 Measurement" section below). No zero-allocation claim is made for the connected relay path; SRTP overhead is accepted as within budget for Phase 2 scale targets.

## Context
PocketStation v2.3 requires this ADR before implementation lands. See `docs/architecture/pocketstation-v2.3.md`.

## Decision
Benchmark whether WriteRTP mutates packets or allocates per listener. No claim of zero-allocation relay until measured.

## Options considered

See v2.3 §26 for the complete option list.

## Consequences

- Agents must follow this decision until a new ADR supersedes it.
- Tests/benchmarks must verify the decision in the relevant phase.

## Test / measurement plan

- Add unit tests for correctness.
- Add benchmark where performance matters.
- Add soak/load tests where reliability matters.

## Phase 2 Measurement — Connected Path

Benchmark: `BenchmarkWriteRTPFanoutConnected` — loopback Pion pair (SetNAT1To1IPs + aggressive ICE timeouts), real DTLS/SRTP, no external network. Platform: darwin/arm64 (Apple M5), Go 1.26.3. Full output in `benches/results/phase2-baseline.txt`.

| Fanout | ns/op | allocs/op | B/op |
|--------|-------|-----------|------|
| 1      | 7728  | 11        | 329  |
| 10     | 82983 | 120       | 3591 |
| 50     | 392058 | 602      | 17929 |

Key finding: SRTP overhead vs disconnected path (~26 ns/op, 0 alloc) is ~297x slower and 11 allocs/call at fanout 1. Allocation originates inside `pion/srtp` and `pion/webrtc` — not in relay dispatch code. Scaling is linear with listener count (no unexpected overhead). At 50 pkts/sec (20 ms Opus cadence) and fanout 50, relay dispatch costs ~19.6 ms CPU/sec per source (~2% of one core).

Decision: **acceptable** — within Phase 2 budget. No relay-level optimization required. Re-evaluate at fanout >100 or when Phase 3 adds SFrame E2EE per-packet cost.

## Reversal trigger

Measured Phase 0/1 data shows this decision breaks latency, reliability, safety, or developer usability targets.
