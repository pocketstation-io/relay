package admission

import (
	"testing"
	"time"
)

func TestGivenIPLimiterWhenCapacityIsReachedThenRequestsAndIdentitiesAreBounded(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	limiter := NewIPLimiter(2, time.Minute, 2)

	if allowed, _ := limiter.Allow("192.0.2.1:4000", now); !allowed {
		t.Fatal("first request was rejected")
	}
	if allowed, _ := limiter.Allow("192.0.2.1:4001", now); !allowed {
		t.Fatal("second request was rejected")
	}
	if allowed, retryAfter := limiter.Allow("192.0.2.1:4002", now); allowed || retryAfter != time.Minute {
		t.Fatalf("third request allowed=%v retry_after=%s", allowed, retryAfter)
	}

	if allowed, _ := limiter.Allow("192.0.2.2:4000", now); !allowed {
		t.Fatal("second identity was rejected")
	}
	if allowed, retryAfter := limiter.Allow("192.0.2.3:4000", now); allowed || retryAfter != time.Minute {
		t.Fatalf("identity above capacity allowed=%v retry_after=%s", allowed, retryAfter)
	}

	if allowed, _ := limiter.Allow("192.0.2.3:4000", now.Add(time.Minute)); !allowed {
		t.Fatal("expired identity entries were not pruned")
	}
}

func TestGivenNilLimiterWhenCheckedThenAdmissionIsAllowed(t *testing.T) {
	var limiter *IPLimiter
	if allowed, retryAfter := limiter.Allow("", time.Now()); !allowed || retryAfter != 0 {
		t.Fatalf("nil limiter allowed=%v retry_after=%s", allowed, retryAfter)
	}
}
