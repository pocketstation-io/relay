package server

import (
	"time"

	pionIce "github.com/pion/ice/v4"
	"github.com/pion/webrtc/v4"
	"github.com/pocketstation-io/relay/internal/notifications/callback"
	"github.com/pocketstation-io/relay/internal/notifications/webhook"
	"github.com/pocketstation-io/relay/internal/session"
)

const (
	defaultMaxRooms                = 100
	defaultMaxListenersPerRoom     = 50
	defaultMaxRoomsPerIPPerMinute  = 10
	defaultMaxConcurrentHandshakes = 128
)

// Config holds the parameters for creating a Server.
type Config struct {
	JWTSecret                []byte
	SubscriberJWTSecret      []byte
	SourceTokenIssuer        string
	AuthorityMode            string
	SettingEngine            *webrtc.SettingEngine
	API                      *webrtc.API
	MaxRooms                 int
	MaxSubscribersPerRoom    int
	MaxRoomsPerIPPerMinute   int
	MaxConcurrentHandshakes  int
	MaxInvitations           int
	CallbackClient           *callback.Client
	ControlReconcileInterval time.Duration
	RelayEpoch               string
	WebhookDispatcher        *webhook.Dispatcher
	ICEServers               []webrtc.ICEServer
	ClientICEServers         []webrtc.ICEServer
	ICETCPMux                pionIce.TCPMux
	ICEUDPMux                pionIce.UDPMux
	RegistryConfig           session.RegistryConfig
	UseTURN                  bool
	NAT1To1IPs               []string
	PublicReceiverURL        string
	PublicRelayURL           string
}
