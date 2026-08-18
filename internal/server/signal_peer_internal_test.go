package server

import "testing"

func TestGivenPendingICEAtCapacityWhenAnotherCandidateArrivesThenItIsRejected(t *testing.T) {
	peer := &signalPeer{}
	for index := 0; index < maxPendingICECandidates; index++ {
		if !peer.queuePendingICE("candidate") {
			t.Fatalf("candidate %d rejected below capacity", index)
		}
	}
	if peer.queuePendingICE("overflow") {
		t.Fatal("candidate accepted above capacity")
	}
	if got := len(peer.pendingICE); got != maxPendingICECandidates {
		t.Fatalf("pending candidate count = %d, want %d", got, maxPendingICECandidates)
	}
}
