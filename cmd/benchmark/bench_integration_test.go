// bench_integration_test.go starts an in-process relay server and exercises
// the benchmark percentile logic so the test suite validates the tool works
// end-to-end without requiring an externally running relay.
package main

import (
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pocketstation-io/relay/internal/server"
)

// TestGiven_EchoEndpoint_When_BenchmarkRuns_Then_P95UnderGate verifies that the
// benchmark produces valid latency percentiles against a local relay echo
// endpoint and that P95 is well below the 50 ms gate on localhost.
func TestGiven_EchoEndpoint_When_BenchmarkRuns_Then_P95UnderGate(t *testing.T) {
	const (
		n       = 50
		p95gate = 50 * time.Millisecond
	)

	cfg := server.Config{JWTSecret: []byte("test-secret")}
	s := server.New(cfg)
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	u := "ws" + strings.TrimPrefix(ts.URL, "http") + "/v1/echo"

	conn, _, err := websocket.DefaultDialer.Dial(u, nil)
	if err != nil {
		t.Fatalf("dial /v1/echo: %v", err)
	}
	defer conn.Close()

	rtts := make([]time.Duration, 0, n)

	for i := 0; i < n; i++ {
		send := time.Now()
		msg := struct {
			SendTimestampNs int64 `json:"send_timestamp_ns"`
		}{SendTimestampNs: send.UnixNano()}

		if err := conn.WriteJSON(msg); err != nil {
			t.Fatalf("write message %d: %v", i, err)
		}

		var reply struct {
			SendTimestampNs int64 `json:"send_timestamp_ns"`
			RecvTimestampNs int64 `json:"recv_timestamp_ns"`
		}
		if err := conn.ReadJSON(&reply); err != nil {
			t.Fatalf("read message %d: %v", i, err)
		}
		if reply.SendTimestampNs != msg.SendTimestampNs {
			t.Errorf("send_timestamp_ns mismatch: got %d want %d", reply.SendTimestampNs, msg.SendTimestampNs)
		}
		rtt := time.Since(send)
		rtts = append(rtts, rtt)
	}

	sort.Slice(rtts, func(i, j int) bool { return rtts[i] < rtts[j] })

	p50 := percentile(rtts, 0.50)
	p95 := percentile(rtts, 0.95)
	p99 := percentile(rtts, 0.99)

	t.Logf("N=%d  P50=%v  P95=%v  P99=%v", n, p50, p95, p99)

	if p95 > p95gate {
		t.Errorf("P95 %v exceeds gate %v", p95, p95gate)
	}
}
