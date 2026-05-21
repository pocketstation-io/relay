# ADR-016 — CI Soak Policy

## Status

Accepted 2026-05-20.

## Context

The Phase 1 soak test (`TestSoak` in `test/soak/soak_test.go`) runs for 5 minutes with 1 publisher and 1 in-process subscriber, sampling goroutine count and RSS at t=0, t=1min, and t=5min. It asserts no goroutine leak (delta <= 10) and no unbounded RSS growth (< 20% from t=1min to t=5min). The race detector is active.

Running this test inline in every PR CI job adds more than 5 minutes to every build. At the Phase 1 contribution rate, this is a significant friction cost. However, the soak test provides value that unit and integration tests do not: it catches goroutine leaks and steady-state RSS growth that only manifest after sustained load.

## Decision

Unit CI steps use the `-short` flag. `TestSoak` checks `testing.Short()` at its first line and calls `t.Skip()` if set, so it is suppressed automatically in all `-short` runs.

Soak runs as a separate CI job (`soak`) triggered only on push to main, not on pull requests. The soak job uses `-timeout 10m` to bound worst-case CI time.

```
PR CI (go job):   go test -short ./...  +  go test -race -short ./...
main CI (soak job): go test -race -timeout 10m -run TestSoak ./test/soak/
```

The separation is implemented in `.github/workflows/ci.yml` with `if: github.ref == 'refs/heads/main'` on the soak job.

## Options considered

**Option A — Run soak inline in every PR job (selected against).**
Simple configuration, but adds 5+ minutes to every PR build. Discourages small commits and fast iteration.

**Option B — Run soak on a scheduled cron (e.g., nightly) (selected against).**
Decouples soak entirely from the merge event. A goroutine regression introduced in a Monday commit may not be caught until Tuesday. Merge-time detection is preferable.

**Option C — Separate soak job on push to main (selected).**
PRs get fast CI (unit + integration, approximately 15 s). Soak still runs within one merge cycle of any main-branch change. Goroutine and RSS regressions are caught before the next release tag.

## Consequences

- PRs see unit and integration test results only. Goroutine leak and RSS regressions introduced in a PR are caught at merge time (on push to main), not at PR open time. This is a deliberate tradeoff.
- The soak job must not be disabled or made non-blocking without a new ADR replacing this one.
- Any new long-running test added to the test suite must use `if testing.Short() { t.Skip() }` at its first line to remain compatible with PR CI.
- The soak results file (`soak/results/phase1-baseline.txt`) is the Phase 1 baseline. Regressions against it in main CI are treated as blocking.

## Test / measurement plan

Phase 1 soak baseline (2026-05-20, darwin/arm64, Apple M5, race-clean):

```
goroutines start=158 1min=149 5min=146 delta(1->5)=-3
rss_mb     start=14  1min=18  5min=18  growth(1->5)=0.0%
packets_forwarded measured via GET /metrics before result write
race_detector=clean
goroutine_leak=PASS
rss_growth=PASS
```

## Reversal trigger

Soak runtime drops below 60 s via test optimization, making the separate job unnecessary and the inline cost acceptable. A new ADR documents the change.
