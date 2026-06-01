// benchmark measures round-trip latency against the relay /v1/echo WebSocket
// endpoint (ADR-020). It sends N messages, records the RTT for each, then
// reports P50/P95/P99 percentiles. The process exits non-zero if P95 exceeds
// the --p95-gate threshold.
//
// Usage:
//
//	benchmark [--url ws://localhost:8080/v1/echo] [--n 200] [--p95-gate 250ms]
package main

import (
	"flag"
	"fmt"
	"math"
	"os"
	"sort"
	"time"

	"github.com/gorilla/websocket"
)

const (
	defaultURL      = "ws://localhost:8080/v1/echo"
	defaultN        = 200
	defaultP95Gate  = 250 * time.Millisecond
)

func main() {
	url     := flag.String("url", defaultURL, "relay echo WebSocket URL")
	n       := flag.Int("n", defaultN, "number of round trips")
	p95gate := flag.Duration("p95-gate", defaultP95Gate, "fail if P95 RTT exceeds this")
	flag.Parse()

	if *n < 1 {
		fmt.Fprintln(os.Stderr, "benchmark: --n must be >= 1")
		os.Exit(1)
	}

	conn, _, err := websocket.DefaultDialer.Dial(*url, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "benchmark: dial %s: %v\n", *url, err)
		os.Exit(1)
	}
	defer conn.Close()

	rtts := make([]time.Duration, 0, *n)

	for i := 0; i < *n; i++ {
		send := time.Now()
		msg := struct {
			SendTimestampNs int64 `json:"send_timestamp_ns"`
		}{SendTimestampNs: send.UnixNano()}

		if err := conn.WriteJSON(msg); err != nil {
			fmt.Fprintf(os.Stderr, "benchmark: write message %d: %v\n", i, err)
			os.Exit(1)
		}

		var reply struct {
			SendTimestampNs int64 `json:"send_timestamp_ns"`
			RecvTimestampNs int64 `json:"recv_timestamp_ns"`
		}
		if err := conn.ReadJSON(&reply); err != nil {
			fmt.Fprintf(os.Stderr, "benchmark: read message %d: %v\n", i, err)
			os.Exit(1)
		}

		rtt := time.Since(send)
		rtts = append(rtts, rtt)
	}

	sort.Slice(rtts, func(i, j int) bool { return rtts[i] < rtts[j] })

	p50 := percentile(rtts, 0.50)
	p95 := percentile(rtts, 0.95)
	p99 := percentile(rtts, 0.99)

	fmt.Printf("N=%d  P50=%v  P95=%v  P99=%v\n", *n, p50, p95, p99)

	if p95 > *p95gate {
		fmt.Fprintf(os.Stderr, "FAIL: P95 %v exceeds gate %v\n", p95, *p95gate)
		os.Exit(1)
	}
	fmt.Println("PASS")
}

// percentile returns the value at the given percentile (0.0–1.0) from a
// pre-sorted slice of durations. Uses the nearest-rank method (ceiling).
func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(math.Ceil(float64(len(sorted))*p)) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}
