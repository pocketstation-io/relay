package admission_test

import (
	"testing"
	"time"

	"github.com/pocketstation-io/relay/internal/admission"
)

// TestRateLimitAllowsUnderLimit verifies that the first N requests from the
// same IP within a window are all allowed.
func TestGivenRequestRateBelowLimitWhenCheckedThenRequestIsAllowed(t *testing.T) {
	// Given — a limiter with max 5 per minute.
	const max = 5
	l := admission.New(max, time.Minute)
	defer l.Stop()

	// When / Then — first 5 calls must be allowed.
	for i := 0; i < max; i++ {
		if !l.Allow("192.0.2.1") {
			t.Fatalf("call %d should be allowed, got denied", i+1)
		}
	}
}

// TestRateLimitBlocksOverLimit verifies that the (N+1)th request from the
// same IP within the window is denied.
func TestGivenRequestRateAboveLimitWhenCheckedThenRequestIsBlocked(t *testing.T) {
	// Given — a limiter with max 3 per minute.
	const max = 3
	l := admission.New(max, time.Minute)
	defer l.Stop()

	// When — exhaust the limit.
	for i := 0; i < max; i++ {
		l.Allow("10.0.0.1")
	}

	// Then — the next request is denied.
	if l.Allow("10.0.0.1") {
		t.Error("expected request to be denied after limit exhausted")
	}
}

// TestRateLimitResetAfterWindow verifies that once the sliding window expires,
// the IP is allowed again.
func TestGivenExhaustedRateLimitWhenWindowExpiresThenRequestIsAllowed(t *testing.T) {
	// Given — a limiter with max 2 per 50ms window.
	const max = 2
	window := 50 * time.Millisecond
	l := admission.New(max, window)
	defer l.Stop()

	ip := "172.16.0.1"

	// When — exhaust the limit.
	for i := 0; i < max; i++ {
		if !l.Allow(ip) {
			t.Fatalf("call %d should be allowed during initial window", i+1)
		}
	}
	if l.Allow(ip) {
		t.Error("expected denial at limit")
	}

	// Wait for the window to expire.
	time.Sleep(window + 10*time.Millisecond)

	// Then — requests are allowed again after the window resets.
	if !l.Allow(ip) {
		t.Error("expected Allow after window expiry")
	}
}

// TestRateLimitDistinctIPsAreIndependent verifies that different IPs do not
// share a counter.
func TestGivenDistinctAddressesWhenRateLimitedThenBudgetsRemainIndependent(t *testing.T) {
	// Given — a limiter that allows 1 per minute.
	l := admission.New(1, time.Minute)
	defer l.Stop()

	// When / Then — exhausting the limit for one IP does not affect another.
	if !l.Allow("1.2.3.4") {
		t.Error("first IP should be allowed")
	}
	if l.Allow("1.2.3.4") {
		t.Error("first IP should be denied after limit")
	}
	if !l.Allow("5.6.7.8") {
		t.Error("second IP should be allowed independently")
	}
}
