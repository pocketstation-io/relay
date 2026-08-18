package admission_test

import (
	"testing"

	"github.com/pocketstation-io/relay/internal/admission"
)

func TestGivenFullAdmissionGateWhenWorkArrivesThenItIsRejectedUntilRelease(t *testing.T) {
	gate := admission.NewGate(2)
	if !gate.TryAcquire() || !gate.TryAcquire() {
		t.Fatal("gate rejected work below its limit")
	}
	if gate.TryAcquire() {
		t.Fatal("gate accepted work above its limit")
	}

	gate.Release()
	if !gate.TryAcquire() {
		t.Fatal("gate did not restore capacity after release")
	}
	if got := gate.Active(); got != 2 {
		t.Fatalf("active = %d, want 2", got)
	}
}

func TestGivenClosedAdmissionGateWhenWorkArrivesThenItIsRejected(t *testing.T) {
	gate := admission.NewGate(0)
	if gate.TryAcquire() {
		t.Fatal("non-positive gate accepted work")
	}
	gate.Release()
	if got := gate.Active(); got != 0 {
		t.Fatalf("active = %d, want 0", got)
	}
}
