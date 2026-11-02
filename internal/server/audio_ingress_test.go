package server

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"github.com/pocketstation-io/relay/access"
	"github.com/pocketstation-io/relay/internal/media/ingress"
)

func ingressTestFrame(sequence uint64, captureNs uint64) []byte {
	frame := make([]byte, ingress.HeaderBytes+3)
	copy(frame, "PKSO")
	frame[4] = 1
	frame[5] = 1
	binary.BigEndian.PutUint64(frame[8:16], sequence)
	binary.BigEndian.PutUint32(frame[16:20], uint32(sequence*960))
	binary.BigEndian.PutUint16(frame[20:22], 960)
	binary.BigEndian.PutUint16(frame[22:24], 3)
	binary.BigEndian.PutUint64(frame[24:32], captureNs)
	copy(frame[40:], []byte{0xf8, 0xff, 0xfe})
	return frame
}

// The WSS and SRTP endpoints are real local transports; the three-byte Opus
// fixtures test bus isolation, not physical capture or decoder fidelity.
func TestGivenWSSIngressWhenWebRTCSubscribesThenIndependentOpusBusesArrive(t *testing.T) {
	service := testAccessService(t, joinTestSecret, access.Config{})
	created, err := service.Create("application", "microphone")
	if err != nil {
		t.Fatal(err)
	}
	setting := webrtc.SettingEngine{}
	setting.SetNetworkTypes([]webrtc.NetworkType{webrtc.NetworkTypeUDP4})
	setting.SetIncludeLoopbackCandidate(true)
	setting.SetIPFilter(net.IP.IsLoopback)
	server := New(Config{JWTSecret: joinTestSecret, AccessService: service, SettingEngine: &setting, ICEServers: []webrtc.ICEServer{{}}})
	cleanupTestRelay(t, server)
	endpoint := httptest.NewTLSServer(server.Handler())
	t.Cleanup(endpoint.Close)
	certificates := x509.NewCertPool()
	certificates.AddCert(endpoint.Certificate())
	dialer := websocket.Dialer{Subprotocols: []string{opusIngressSubprotocol}, HandshakeTimeout: time.Second, TLSClientConfig: &tls.Config{RootCAs: certificates, MinVersion: tls.VersionTLS12}}
	api := webrtc.NewAPI(webrtc.WithSettingEngine(setting))
	type busConnection struct {
		bus     string
		conn    *websocket.Conn
		payload []byte
		packets chan []byte
	}
	buses := []busConnection{{bus: "application", payload: []byte{0xf8, 0xff, 0xfe}}, {bus: "microphone", payload: []byte{0xf8, 0xff, 0xfd}}}
	for index := range buses {
		bus := &buses[index]
		bus.conn, _, err = dialer.Dial("wss"+strings.TrimPrefix(endpoint.URL, "https")+"/v1/sessions/"+created.ID()+"/audio?bus="+bus.bus, http.Header{"Authorization": []string{"Bearer " + created.SourceToken()}})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = bus.conn.Close() })
		_ = bus.conn.SetReadDeadline(time.Now().Add(lifecycleWait))
		var ready struct {
			Type  string `json:"type"`
			BusID string `json:"bus_id"`
		}
		if err = bus.conn.ReadJSON(&ready); err != nil || ready.Type != "READY" || ready.BusID != bus.bus {
			t.Fatalf("WSS readiness: %+v err=%v", ready, err)
		}
		pc, err := api.NewPeerConnection(webrtc.Configuration{})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = pc.Close() })
		if _, err = pc.AddTransceiverFromKind(webrtc.RTPCodecTypeAudio, webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly}); err != nil {
			t.Fatal(err)
		}
		bus.packets = make(chan []byte, 64)
		pc.OnTrack(func(remote *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
			for {
				packet, _, err := remote.ReadRTP()
				if err != nil {
					return
				}
				select {
				case bus.packets <- append([]byte(nil), packet.Payload...):
				default:
				}
			}
		})
		offer, err := pc.CreateOffer(nil)
		if err != nil {
			t.Fatal(err)
		}
		gathered := webrtc.GatheringCompletePromise(pc)
		if err = pc.SetLocalDescription(offer); err != nil {
			t.Fatal(err)
		}
		select {
		case <-gathered:
		case <-time.After(lifecycleWait):
			t.Fatal("WHEP ICE gathering deadline")
		}
		token, err := service.IssueSubscriberTokenContext(context.Background(), created.ID(), bus.bus)
		if err != nil {
			t.Fatal(err)
		}
		request, _ := http.NewRequest(http.MethodPost, endpoint.URL+"/v1/sessions/"+created.ID()+"/whep?bus="+bus.bus, strings.NewReader(pc.LocalDescription().SDP))
		request.Header.Set("Authorization", "Bearer "+token)
		request.Header.Set("Content-Type", "application/sdp")
		client := endpoint.Client()
		client.Timeout = lifecycleWait
		response, err := client.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		answer, err := io.ReadAll(io.LimitReader(response.Body, 64*1024))
		_ = response.Body.Close()
		if err != nil || response.StatusCode != http.StatusCreated {
			t.Fatalf("WHEP status=%d err=%v", response.StatusCode, err)
		}
		if err = pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: string(answer)}); err != nil {
			t.Fatal(err)
		}
		waitLifecycle(t, "WHEP connected", func() bool { return pc.ConnectionState() == webrtc.PeerConnectionStateConnected })
	}
	for sequence := uint64(0); sequence < 15; sequence++ {
		for _, bus := range buses {
			frame := ingressTestFrame(sequence, sequence*20_000_000+1)
			copy(frame[ingress.HeaderBytes:], bus.payload)
			if err = bus.conn.WriteMessage(websocket.BinaryMessage, frame); err != nil {
				t.Fatal(err)
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	for _, bus := range buses {
		select {
		case payload := <-bus.packets:
			if !bytes.Equal(payload, bus.payload) {
				t.Fatalf("%s received another bus: %x", bus.bus, payload)
			}
		case <-time.After(lifecycleWait):
			t.Fatalf("no WebRTC Opus on %s", bus.bus)
		}
	}
}

func dialIngress(t *testing.T, endpoint *httptest.Server, sessionID, bus, token string) (*websocket.Conn, uint64) {
	t.Helper()
	dialer := websocket.Dialer{Subprotocols: []string{opusIngressSubprotocol}, HandshakeTimeout: time.Second}
	conn, response, err := dialer.Dial("ws"+strings.TrimPrefix(endpoint.URL, "http")+"/v1/sessions/"+sessionID+"/audio?bus="+bus, http.Header{"Authorization": []string{"Bearer " + token}})
	if err != nil {
		if response != nil {
			t.Fatalf("ingress status=%d err=%v", response.StatusCode, err)
		}
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	var ready struct {
		Type       string `json:"type"`
		BusID      string `json:"bus_id"`
		Generation uint64 `json:"source_generation"`
	}
	if err = conn.ReadJSON(&ready); err != nil {
		t.Fatal(err)
	}
	if conn.Subprotocol() != opusIngressSubprotocol || ready.Type != "READY" || ready.BusID != bus || ready.Generation == 0 {
		t.Fatalf("readiness=%#v protocol=%q", ready, conn.Subprotocol())
	}
	return conn, ready.Generation
}

type ingressPacketSink struct{ packets chan *rtp.Packet }

func (s *ingressPacketSink) WriteRTP(packet *rtp.Packet) error {
	select {
	case s.packets <- packet.Clone():
	default:
	}
	return nil
}

func awaitIngressPacket(t *testing.T, sink *ingressPacketSink) *rtp.Packet {
	t.Helper()
	select {
	case packet := <-sink.packets:
		return packet
	case <-time.After(2 * time.Second):
		t.Fatal("no forwarded Opus packet")
		return nil
	}
}

func TestGivenAuthenticatedIngressWhenPublishingAndReconnectingThenSeparateBusesPersist(t *testing.T) {
	server, service := newJoinTestServer(t)
	created, err := service.Create("application", "microphone")
	if err != nil {
		t.Fatal(err)
	}
	endpoint := httptest.NewServer(server.Handler())
	t.Cleanup(endpoint.Close)
	application, generation := dialIngress(t, endpoint, created.ID(), "application", created.SourceToken())
	microphone, _ := dialIngress(t, endpoint, created.ID(), "microphone", created.SourceToken())
	relaySession, found := server.relaySessions.Get(created.ID())
	if !found {
		t.Fatal("source not attached")
	}
	appSink := &ingressPacketSink{make(chan *rtp.Packet, 8)}
	micSink := &ingressPacketSink{make(chan *rtp.Packet, 8)}
	if err = relaySession.AddSubscription("app-receiver", "application", appSink); err != nil {
		t.Fatal(err)
	}
	if err = relaySession.AddSubscription("mic-receiver", "microphone", micSink); err != nil {
		t.Fatal(err)
	}
	if err = application.WriteMessage(websocket.BinaryMessage, ingressTestFrame(0, 1)); err != nil {
		t.Fatal(err)
	}
	if err = microphone.WriteMessage(websocket.BinaryMessage, ingressTestFrame(0, 1)); err != nil {
		t.Fatal(err)
	}
	appPacket := awaitIngressPacket(t, appSink)
	micPacket := awaitIngressPacket(t, micSink)
	if appPacket.SSRC == micPacket.SSRC || appPacket.SSRC == 0 {
		t.Fatal("independent source identity lost")
	}
	second, nextGeneration := dialIngress(t, endpoint, created.ID(), "application", created.SourceToken())
	if nextGeneration <= generation {
		t.Fatalf("generation %d did not replace %d", nextGeneration, generation)
	}
	if err = second.WriteMessage(websocket.BinaryMessage, ingressTestFrame(0, 1)); err != nil {
		t.Fatal(err)
	}
	reconnected := awaitIngressPacket(t, appSink)
	if reconnected.SSRC == appPacket.SSRC {
		t.Fatal("reconnect retained old source identity")
	}
	_ = application.SetReadDeadline(time.Now().Add(time.Second))
	if _, _, err = application.ReadMessage(); err == nil {
		t.Fatal("replaced source remains open")
	}
}

func TestGivenSourceScopeWhenIngressUnauthorizedThenNoAttachment(t *testing.T) {
	server, service := newJoinTestServer(t)
	created, err := service.Create("application")
	if err != nil {
		t.Fatal(err)
	}
	subscriberToken, err := service.IssueSubscriberTokenContext(context.Background(), created.ID(), "application")
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name, bus, token, protocol string
		status                     int
	}{
		{"no credential", "application", "", "pks-opus-v1", 401},
		{"wrong bus", "microphone", created.SourceToken(), "pks-opus-v1", 403},
		{"missing explicit bus", "", created.SourceToken(), "pks-opus-v1", 403},
		{"wrong protocol", "application", created.SourceToken(), "other", 400},
		{"subscriber role", "application", subscriberToken, "pks-opus-v1", 401},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest("GET", "/v1/sessions/"+created.ID()+"/audio?bus="+test.bus, nil)
			request.Header.Set("Authorization", "Bearer "+test.token)
			request.Header.Set("Sec-WebSocket-Protocol", test.protocol)
			response := httptest.NewRecorder()
			server.Handler().ServeHTTP(response, request)
			if response.Code != test.status {
				t.Fatalf("status=%d want=%d", response.Code, test.status)
			}
			if server.relaySessions.RoomCount() != 0 {
				t.Fatal("unauthorized source created RelaySession")
			}
		})
	}
}

func TestGivenIngressWhenMalformedOrAuthorityDeletedThenConnectionCloses(t *testing.T) {
	for _, mode := range []string{"malformed", "deleted"} {
		t.Run(mode, func(t *testing.T) {
			server, service := newJoinTestServer(t)
			created, err := service.Create("application")
			if err != nil {
				t.Fatal(err)
			}
			endpoint := httptest.NewServer(server.Handler())
			t.Cleanup(endpoint.Close)
			conn, _ := dialIngress(t, endpoint, created.ID(), "application", created.SourceToken())
			if mode == "malformed" {
				err = conn.WriteMessage(websocket.BinaryMessage, []byte("invalid"))
				if err != nil {
					t.Fatal(err)
				}
			} else {
				if deleted, err := service.DeleteContext(context.Background(), created.ID()); err != nil || !deleted {
					t.Fatalf("delete=%v err=%v", deleted, err)
				}
			}
			_ = conn.SetReadDeadline(time.Now().Add(4 * time.Second))
			_, _, err = conn.ReadMessage()
			if err == nil {
				t.Fatal("invalid source remained open")
			}
			if mode == "malformed" && !websocket.IsCloseError(err, websocket.CloseProtocolError) {
				t.Fatalf("malformed close=%v", err)
			}
		})
	}
}

func TestGivenIngressCapabilityWhenExpiredThenPolicyCloseEndsSource(t *testing.T) {
	service := testAccessService(t, joinTestSecret, access.Config{OwnerTokenTTL: 2 * time.Second})
	created, err := service.Create("application")
	if err != nil {
		t.Fatal(err)
	}
	server := New(Config{JWTSecret: joinTestSecret, AccessService: service})
	cleanupTestRelay(t, server)
	endpoint := httptest.NewServer(server.Handler())
	t.Cleanup(endpoint.Close)
	conn, _ := dialIngress(t, endpoint, created.ID(), "application", created.SourceToken())
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, _, err = conn.ReadMessage(); !websocket.IsCloseError(err, websocket.ClosePolicyViolation) {
		t.Fatalf("expiry close=%v", err)
	}
	relaySession, found := server.relaySessions.Get(created.ID())
	if !found {
		t.Fatal("expiry removed reconnect Session")
	}
	waitLifecycle(t, "expired source detached", func() bool { return !relaySession.BusSourceActive("application") })
}

func TestGivenIngressAuthorityMonitorUnavailableWhenCapabilityExpiresThenReaderCloses(t *testing.T) {
	// No monitor runs here, representing authority IO occupying that worker.
	// The reader must still enforce the expiry supplied by authenticated admission.
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		conn, err := upgrader.Upgrade(w, request, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		source := &opusWebSocketSource{conn: conn, done: make(chan struct{}), expiresAt: time.Now().Add(150 * time.Millisecond)}
		_, _ = source.ReadRTP()
	}))
	t.Cleanup(endpoint.Close)
	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(endpoint.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	if _, _, err = conn.ReadMessage(); !websocket.IsCloseError(err, websocket.ClosePolicyViolation) {
		t.Fatalf("reader did not enforce expiry without monitor: %v", err)
	}
}

func TestGivenIngressCapacityWhenOccupiedThenNewConnectionRejectedAndShutdownCloses(t *testing.T) {
	service := testAccessService(t, joinTestSecret, access.Config{})
	created, err := service.Create("application", "microphone")
	if err != nil {
		t.Fatal(err)
	}
	server := New(Config{JWTSecret: joinTestSecret, AccessService: service, MaxConcurrentHandshakes: 1})
	cleanupTestRelay(t, server)
	endpoint := httptest.NewServer(server.Handler())
	t.Cleanup(endpoint.Close)
	conn, _ := dialIngress(t, endpoint, created.ID(), "application", created.SourceToken())
	request := httptest.NewRequest("GET", "/v1/sessions/"+created.ID()+"/audio?bus=microphone", nil)
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)
	if response.Code != 503 {
		t.Fatalf("capacity status=%d", response.Code)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err = server.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	if _, _, err = conn.ReadMessage(); err == nil {
		t.Fatal("shutdown left ingress open")
	}
	request = httptest.NewRequest("GET", "/v1/sessions/"+created.ID()+"/audio?bus=microphone", nil)
	request.Header.Set("Authorization", "Bearer "+created.SourceToken())
	request.Header.Set("Sec-WebSocket-Protocol", opusIngressSubprotocol)
	response = httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("post-shutdown admission status=%d", response.Code)
	}
	if server.relaySessions.RoomCount() != 0 {
		t.Fatal("shutdown admitted a new RelaySession")
	}
}
