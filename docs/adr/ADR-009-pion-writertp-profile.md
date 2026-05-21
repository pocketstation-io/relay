# ADR-009-pion-writertp-profile — Pion WriteRTP Allocation Profile

## Status
Accepted. Phase 1 baseline measurements taken 2026-05-20 (see `benches/results/phase1-baseline.txt`). Disconnected-track WriteRTP: 0 alloc/op at ~26 ns/op. Connected-track (SRTP path) measurement deferred to Phase 2 (see RELAY_PHASE2_QUEUE.md — "Connected WriteRTP bench"). No zero-allocation claim is made for the connected relay path.

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

## Reversal trigger

Measured Phase 0/1 data shows this decision breaks latency, reliability, safety, or developer usability targets.
