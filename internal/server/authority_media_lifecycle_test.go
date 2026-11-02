package server

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"github.com/pocketstation-io/relay/access"
	"github.com/pocketstation-io/relay/internal/notifications/callback"
	"github.com/pocketstation-io/relay/internal/signaling"
)

const lifecycleWait = 3 * time.Second

func waitLifecycle(t *testing.T, name string, predicate func() bool) {
	t.Helper()
	deadline := time.Now().Add(lifecycleWait)
	for time.Now().Before(deadline) {
		if predicate() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("deadline: " + name)
}

func lifecycleWS(t *testing.T, endpoint, token string, publish bool, pc *webrtc.PeerConnection) <-chan struct{} {
	t.Helper()
	connection, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(endpoint, "http")+"/v1/signal", nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	var writeMu sync.Mutex
	offer, err := pc.CreateOffer(nil)
	if err != nil {
		t.Fatal(err)
	}
	kind := signaling.TypeSubscribe
	if publish {
		kind = signaling.TypePublish
	}
	if err := connection.WriteJSON(signaling.ClientMessage{Type: kind, Token: token, BusID: "application", SDPOffer: offer.SDP}); err != nil {
		t.Fatal(err)
	}
	pc.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c != nil {
			writeMu.Lock()
			defer writeMu.Unlock()
			_ = connection.WriteJSON(signaling.ClientMessage{Type: signaling.TypeIce, Candidate: c.ToJSON().Candidate})
		}
	})
	if err := pc.SetLocalDescription(offer); err != nil {
		t.Fatal(err)
	}
	closed := make(chan struct{})
	go func() {
		defer close(closed)
		for {
			var message signaling.ServerMessage
			if err := connection.ReadJSON(&message); err != nil {
				return
			}
			switch message.Type {
			case signaling.TypeSDPAnswer:
				_ = pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: message.SDPAnswer})
			case signaling.TypeIce:
				_ = pc.AddICECandidate(webrtc.ICECandidateInit{Candidate: message.Candidate})
			}
		}
	}()
	return closed
}

// This uses real local ICE/SRTP/RTP and a deliberately synthetic authority server.
// It measures transport lifecycle, not physical capture or browser audio decode.
func TestGivenAdmittedMediaWhenAuthorityChangesThenContinuityAndDeletionFollowContract(t *testing.T) {
	for _, transport := range []string{"websocket", "whep"} {
		t.Run(transport, func(t *testing.T) {
			domain := testAccessService(t, joinTestSecret, access.Config{SubscriberTokenTTL: time.Second, PublisherTokenTTL: time.Minute})
			created, err := domain.Create("application")
			if err != nil {
				t.Fatal(err)
			}
			sessionID := created.ID()
			authorityMux := http.NewServeMux()
			domain.RegisterInternal(authorityMux, []byte(controlStateSyncTestSecret))
			var authorityStatus atomic.Int64
			authorityStatus.Store(http.StatusOK)
			authority := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("X-PocketStation-Internal-Secret") != controlStateSyncTestSecret {
					w.WriteHeader(http.StatusUnauthorized)
					return
				}
				if status := int(authorityStatus.Load()); status != http.StatusOK {
					w.WriteHeader(status)
					return
				}
				authorityMux.ServeHTTP(w, r)
			}))
			defer authority.Close()
			callbackClient, err := callback.NewClient(authority.URL, controlStateSyncTestSecret)
			if err != nil {
				t.Fatal(err)
			}
			setting := webrtc.SettingEngine{}
			setting.SetNetworkTypes([]webrtc.NetworkType{webrtc.NetworkTypeUDP4})
			setting.SetIncludeLoopbackCandidate(true)
			setting.SetIPFilter(net.IP.IsLoopback)
			service := New(Config{JWTSecret: joinTestSecret, AuthorityMode: "control-plane", CallbackClient: callbackClient, SettingEngine: &setting, ICEServers: []webrtc.ICEServer{{}}, ControlReconcileInterval: 100 * time.Millisecond})
			cleanupTestRelay(t, service)
			service.startControlStateSync()
			defer service.Shutdown(context.Background())
			endpoint := httptest.NewServer(service.Handler())
			defer endpoint.Close()
			api := webrtc.NewAPI(webrtc.WithSettingEngine(setting))
			publisher, err := api.NewPeerConnection(webrtc.Configuration{})
			if err != nil {
				t.Fatal(err)
			}
			defer publisher.Close()
			track, err := webrtc.NewTrackLocalStaticRTP(webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus}, "audio", "application")
			if err != nil {
				t.Fatal(err)
			}
			if _, err = publisher.AddTrack(track); err != nil {
				t.Fatal(err)
			}
			sourceToken, err := domain.IssuePublisherTokenContext(context.Background(), sessionID, "application")
			if err != nil {
				t.Fatal(err)
			}
			publisherClosed := lifecycleWS(t, endpoint.URL, sourceToken, true, publisher)
			waitLifecycle(t, "publisher connected", func() bool { return publisher.ConnectionState() == webrtc.PeerConnectionStateConnected })
			stop := make(chan struct{})
			defer close(stop)
			go func() {
				ticker := time.NewTicker(20 * time.Millisecond)
				defer ticker.Stop()
				var sequence uint16
				for {
					select {
					case <-stop:
						return
					case <-ticker.C:
						_ = track.WriteRTP(&rtp.Packet{Header: rtp.Header{Version: 2, PayloadType: 111, SequenceNumber: sequence, Timestamp: uint32(sequence) * 960, SSRC: 1234}, Payload: []byte{0xf8, 0xff, 0xfe}})
						sequence++
					}
				}
			}()
			subscriber, err := api.NewPeerConnection(webrtc.Configuration{})
			if err != nil {
				t.Fatal(err)
			}
			defer subscriber.Close()
			if _, err = subscriber.AddTransceiverFromKind(webrtc.RTPCodecTypeAudio, webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly}); err != nil {
				t.Fatal(err)
			}
			var packets atomic.Int64
			subscriber.OnTrack(func(remote *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
				for {
					if _, _, err := remote.ReadRTP(); err != nil {
						return
					}
					packets.Add(1)
				}
			})
			tokenIssued := time.Now()
			subscriberToken, err := domain.IssueSubscriberTokenContext(context.Background(), sessionID, "application")
			if err != nil {
				t.Fatal(err)
			}
			var subscriberClosed <-chan struct{}
			var resource string
			var ownedHTTPConnection *whipConn
			if transport == "websocket" {
				subscriberClosed = lifecycleWS(t, endpoint.URL, subscriberToken, false, subscriber)
			} else {
				offer, err := subscriber.CreateOffer(nil)
				if err != nil {
					t.Fatal(err)
				}
				gathered := webrtc.GatheringCompletePromise(subscriber)
				if err = subscriber.SetLocalDescription(offer); err != nil {
					t.Fatal(err)
				}
				select {
				case <-gathered:
				case <-time.After(lifecycleWait):
					t.Fatal("gather timeout")
				}
				request, _ := http.NewRequest(http.MethodPost, endpoint.URL+"/v1/sessions/"+sessionID+"/whep?bus=application", bytes.NewBufferString(subscriber.LocalDescription().SDP))
				request.Header.Set("Authorization", "Bearer "+subscriberToken)
				request.Header.Set("Content-Type", "application/sdp")
				response, err := (&http.Client{Timeout: lifecycleWait}).Do(request)
				if err != nil {
					t.Fatal(err)
				}
				answer, _ := io.ReadAll(response.Body)
				response.Body.Close()
				if response.StatusCode != http.StatusCreated {
					t.Fatalf("WHEP %d: %s", response.StatusCode, answer)
				}
				resource = strings.TrimPrefix(response.Header.Get("Location"), "/v1/connections/")
				stored, found := service.whipConns.Load(resource)
				if !found {
					t.Fatal("WHEP resource not retained before deletion")
				}
				ownedHTTPConnection = stored.(*whipConn)
				if err = subscriber.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: string(answer)}); err != nil {
					t.Fatal(err)
				}
			}
			waitLifecycle(t, "real RTP received", func() bool { return packets.Load() >= 5 })
			authorityStatus.Store(http.StatusServiceUnavailable)
			beforeOutage := packets.Load()
			waitLifecycle(t, "admitted media during authority outage", func() bool { return packets.Load() >= beforeOutage+10 })
			request, _ := http.NewRequest(http.MethodPost, endpoint.URL+"/v1/sessions/lifecycle-session/whip?bus=application", bytes.NewBufferString(""))
			request.Header.Set("Authorization", "Bearer "+sourceToken)
			request.Header.Set("Content-Type", "application/sdp")
			response, err := (&http.Client{Timeout: lifecycleWait}).Do(request)
			if err != nil {
				t.Fatal(err)
			}
			response.Body.Close()
			if response.StatusCode != http.StatusServiceUnavailable {
				t.Fatalf("outage admission = %d", response.StatusCode)
			}
			authorityStatus.Store(http.StatusOK)
			// JWT verifier permits five seconds of clock skew; observe after that window.
			if delay := time.Until(tokenIssued.Add(7 * time.Second)); delay > 0 {
				time.Sleep(delay)
			}
			beforeExpiry := packets.Load()
			waitLifecycle(t, "admitted media after capability expiry", func() bool { return packets.Load() >= beforeExpiry+10 })
			request, _ = http.NewRequest(http.MethodPost, endpoint.URL+"/v1/sessions/"+sessionID+"/whep?bus=application", bytes.NewBufferString(""))
			request.Header.Set("Authorization", "Bearer "+subscriberToken)
			request.Header.Set("Content-Type", "application/sdp")
			response, err = (&http.Client{Timeout: lifecycleWait}).Do(request)
			if err != nil {
				t.Fatal(err)
			}
			response.Body.Close()
			if response.StatusCode != http.StatusUnauthorized {
				t.Fatalf("expired admission = %d", response.StatusCode)
			}
			deletedAt := time.Now()
			authorityStatus.Store(http.StatusNotFound)
			waitLifecycle(t, "publisher signaling closed after deletion", func() bool {
				select {
				case <-publisherClosed:
					return true
				default:
					return false
				}
			})
			if transport == "websocket" {
				waitLifecycle(t, "subscriber signaling closed after deletion", func() bool {
					select {
					case <-subscriberClosed:
						return true
					default:
						return false
					}
				})
			} else {
				waitLifecycle(t, "WHEP owned connection removed", func() bool { _, found := service.whipConns.Load(resource); return !found })
				waitLifecycle(t, "WHEP transport closed", func() bool { return ownedHTTPConnection.pc.ConnectionState() == webrtc.PeerConnectionStateClosed })
			}
			waitLifecycle(t, "Session removed after deletion", func() bool { _, found := service.relaySessions.Get(sessionID); return !found })
			// Allow already queued packets to drain, then require no continued forwarding.
			time.Sleep(200 * time.Millisecond)
			afterDelete := packets.Load()
			time.Sleep(200 * time.Millisecond)
			if packets.Load() != afterDelete {
				t.Fatal("media continued after deletion cleanup")
			}
			t.Logf("transport=%s packets=%d outage_continuity=true expired_admission_denied=true existing_expiry_continuity=true deletion_cleanup_ms=%d", transport, packets.Load(), time.Since(deletedAt).Milliseconds())
		})
	}
}
