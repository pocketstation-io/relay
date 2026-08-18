package clocklineage

import (
	"time"

	"github.com/pion/interceptor"
	"github.com/pion/rtcp"
)

// InterceptorFactory observes Sender Reports without consuming RTCP from Pion's
// RTPSender or RTPReceiver application readers.
type InterceptorFactory struct{ Registry *Registry }

// NewInterceptor creates one observer for a PeerConnection interceptor chain.
func (f *InterceptorFactory) NewInterceptor(string) (interceptor.Interceptor, error) {
	return &observer{NoOp: interceptor.NoOp{}, registry: f.Registry}, nil
}

type observer struct {
	interceptor.NoOp
	registry *Registry
}

func (o *observer) BindRTCPReader(reader interceptor.RTCPReader) interceptor.RTCPReader {
	return interceptor.RTCPReaderFunc(func(raw []byte, attributes interceptor.Attributes) (int, interceptor.Attributes, error) {
		n, attributes, err := reader.Read(raw, attributes)
		if err != nil {
			return n, attributes, err
		}
		if attributes == nil {
			attributes = make(interceptor.Attributes)
		}
		packets, parseErr := attributes.GetRTCPPackets(raw[:n])
		if parseErr != nil {
			return n, attributes, parseErr
		}
		now := time.Now()
		for _, packet := range packets {
			if report, ok := packet.(*rtcp.SenderReport); ok {
				if timeline := o.registry.observeRemote(report.SSRC); timeline != nil {
					timeline.Observe(report, now)
				}
			}
		}
		return n, attributes, nil
	})
}

func (o *observer) BindRTCPWriter(writer interceptor.RTCPWriter) interceptor.RTCPWriter {
	return interceptor.RTCPWriterFunc(func(packets []rtcp.Packet, attributes interceptor.Attributes) (int, error) {
		now := time.Now()
		for _, packet := range packets {
			if report, ok := packet.(*rtcp.SenderReport); ok {
				if timeline := o.registry.observeLocal(report.SSRC); timeline != nil {
					normalized := timeline.NormalizeSenderReport(report, now, 48_000)
					timeline.observe(report, now, normalized)
				}
			}
		}
		return writer.Write(packets, attributes)
	})
}

func (o *observer) BindRemoteStream(info *interceptor.StreamInfo, reader interceptor.RTPReader) interceptor.RTPReader {
	o.registry.Remote(info.SSRC)
	return reader
}

func (o *observer) UnbindRemoteStream(info *interceptor.StreamInfo) {
	o.registry.removeRemote(info.SSRC)
}

func (o *observer) BindLocalStream(info *interceptor.StreamInfo, writer interceptor.RTPWriter) interceptor.RTPWriter {
	o.registry.Local(info.SSRC)
	return writer
}

func (o *observer) UnbindLocalStream(info *interceptor.StreamInfo) { o.registry.removeLocal(info.SSRC) }
