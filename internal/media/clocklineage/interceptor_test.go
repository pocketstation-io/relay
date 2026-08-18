package clocklineage

import (
	"testing"
	"time"

	"github.com/pion/interceptor"
	"github.com/pion/rtcp"
)

func TestGivenIncomingSenderReportWhenRTCPReadThenRemoteTimelineIsUpdated(t *testing.T) {
	registry := NewRegistry()
	factory := &InterceptorFactory{Registry: registry}
	interceptorInstance, err := factory.NewInterceptor("test")
	if err != nil {
		t.Fatalf("NewInterceptor: %v", err)
	}
	report := &rtcp.SenderReport{SSRC: 42, NTPTime: toNTP(time.Now()), RTPTime: 960}
	raw, err := report.Marshal()
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	reader := interceptorInstance.BindRTCPReader(interceptor.RTCPReaderFunc(func(buffer []byte, attributes interceptor.Attributes) (int, interceptor.Attributes, error) {
		copy(buffer, raw)
		return len(raw), attributes, nil
	}))
	buffer := make([]byte, 1500)
	if _, _, err = reader.Read(buffer, nil); err != nil {
		t.Fatalf("Read: %v", err)
	}

	snapshot, ok := registry.Remote(42).Snapshot()
	if !ok || snapshot.RTPTime != 960 || snapshot.ReportCount != 1 {
		t.Fatalf("remote snapshot = %+v, ok = %v", snapshot, ok)
	}
}

func TestGivenOutgoingSenderReportWhenRTCPWrittenThenLocalTimelineIsUpdated(t *testing.T) {
	registry := NewRegistry()
	factory := &InterceptorFactory{Registry: registry}
	interceptorInstance, err := factory.NewInterceptor("test")
	if err != nil {
		t.Fatalf("NewInterceptor: %v", err)
	}
	report := &rtcp.SenderReport{SSRC: 77, NTPTime: toNTP(time.Now()), RTPTime: 1920}
	writer := interceptorInstance.BindRTCPWriter(interceptor.RTCPWriterFunc(func(packets []rtcp.Packet, _ interceptor.Attributes) (int, error) {
		return len(packets), nil
	}))
	if _, err = writer.Write([]rtcp.Packet{report}, nil); err != nil {
		t.Fatalf("Write: %v", err)
	}

	snapshot, ok := registry.Local(77).Snapshot()
	if !ok || snapshot.RTPTime != 1920 || snapshot.ReportCount != 1 {
		t.Fatalf("local snapshot = %+v, ok = %v", snapshot, ok)
	}
}

func TestGivenOutgoingSenderReportAndCaptureMappingWhenRTCPWrittenThenClockIsNormalized(t *testing.T) {
	base := time.Now().UTC().Truncate(time.Millisecond)
	registry := NewRegistry()
	registry.Local(77).ObserveCapture(960, base, base)
	factory := &InterceptorFactory{Registry: registry}
	interceptorInstance, err := factory.NewInterceptor("test")
	if err != nil {
		t.Fatalf("NewInterceptor: %v", err)
	}
	report := &rtcp.SenderReport{SSRC: 77, NTPTime: toNTP(base.Add(time.Hour)), RTPTime: 1920}
	writer := interceptorInstance.BindRTCPWriter(interceptor.RTCPWriterFunc(func(packets []rtcp.Packet, _ interceptor.Attributes) (int, error) {
		return len(packets), nil
	}))
	if _, err = writer.Write([]rtcp.Packet{report}, nil); err != nil {
		t.Fatalf("Write: %v", err)
	}

	snapshot, ok := registry.Local(77).Snapshot()
	if !ok || !snapshot.Normalized {
		t.Fatalf("local snapshot = %+v, ok = %v", snapshot, ok)
	}
	want := base.Add(20 * time.Millisecond)
	if delta := ntpTime(snapshot.NTPTime).Sub(want); delta < -time.Microsecond || delta > time.Microsecond {
		t.Fatalf("normalized NTP delta = %v, want within 1us", delta)
	}
}
