package server

import (
	"context"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	pionIce "github.com/pion/ice/v4"
	"github.com/pion/webrtc/v4"
	"github.com/pocketstation-io/relay/internal/admission"
	"github.com/pocketstation-io/relay/internal/auth"
	"github.com/pocketstation-io/relay/internal/metrics"
	"github.com/pocketstation-io/relay/internal/notifications/callback"
	"github.com/pocketstation-io/relay/internal/notifications/webhook"
	"github.com/pocketstation-io/relay/internal/session"
)

const defaultPublicReceiverURL = "https://pocketstation-receiver.fly.dev"

// Server is the top-level relay server.
type Server struct {
	relaySessions            *session.SessionRegistry
	jwtSecret                []byte
	subscriberJWTSecret      []byte
	sourceTokenIssuer        string
	authorityMode            string
	settingEngine            *webrtc.SettingEngine
	api                      *webrtc.API
	Metrics                  *metrics.Registry
	callbackClient           *callback.Client
	relayEpoch               string
	controlReconcileInterval time.Duration
	controlStateChanges      chan *session.RelaySession
	controlSyncOnce          sync.Once
	controlSyncContext       context.Context
	controlSyncCancel        context.CancelFunc
	controlSyncWait          sync.WaitGroup
	webhookDispatcher        *webhook.Dispatcher
	iceServers               []webrtc.ICEServer // relay's own Pion PeerConnections
	clientICEServers         []webrtc.ICEServer // returned to clients in createRoom
	iceTCPMux                pionIce.TCPMux
	iceUDPMux                pionIce.UDPMux
	nat1to1IPs               []string

	maxRooms              int // set once at construction
	maxSubscribersPerRoom int // set once at construction
	maxInvitations        int
	handshakeAdmission    *admission.Gate

	// ipLimiter enforces per-IP room-creation rate limiting.
	// Nil when per-IP limiting is disabled (MaxRoomsPerIPPerMinute == -1).
	ipLimiter *admission.IPLimiter

	// mu guards httpServer and the active signaling-peer map.
	mu          sync.RWMutex
	httpServer  *http.Server
	signalPeers map[string]*signalPeer

	// codecHintStates and iceRestartStates are keyed by RelaySession ID.
	// sync.Map for concurrent access from multiple subscriber RTCP goroutines.
	codecHintStates  sync.Map
	iceRestartStates sync.Map

	// whipConns maps WHIP/WHEP connection IDs to their live PeerConnections.
	// Keyed by the opaque connID returned in the Location header (RFC 9725).
	// sync.Map: concurrent PATCH/DELETE from multiple HTTP goroutines.
	whipConns sync.Map

	// useTURN is set once from Config.UseTURN; propagated to ICE_RESTART msgs.
	useTURN bool

	publicReceiverURL string
	publicRelayURL    string
	joinMu            sync.Mutex
	joinInvites       map[string]joinInvite
}

// New creates a Server from cfg.
func New(cfg Config) *Server {
	maxRooms := cfg.MaxRooms
	if maxRooms <= 0 {
		maxRooms = defaultMaxRooms
	}
	maxSubs := cfg.MaxSubscribersPerRoom
	if maxSubs <= 0 {
		maxSubs = defaultMaxListenersPerRoom
	}
	maxHandshakes := cfg.MaxConcurrentHandshakes
	if maxHandshakes <= 0 {
		maxHandshakes = defaultMaxConcurrentHandshakes
	}
	maxInvitations := cfg.MaxInvitations
	if maxInvitations <= 0 {
		maxInvitations = maxRooms * 4
	}
	reconcileInterval := cfg.ControlReconcileInterval
	if reconcileInterval <= 0 {
		reconcileInterval = defaultControlReconcileInterval
	}
	relayEpoch := cfg.RelayEpoch
	if relayEpoch == "" {
		relayEpoch = newID()
	}
	subscriberSecret := cfg.SubscriberJWTSecret
	if len(subscriberSecret) == 0 {
		subscriberSecret = cfg.JWTSecret
	}
	sourceIssuer := cfg.SourceTokenIssuer
	if sourceIssuer == "" {
		sourceIssuer = auth.RelayIssuer
	}
	authorityMode := cfg.AuthorityMode
	if authorityMode == "" {
		authorityMode = "standalone"
	}

	var ipLim *admission.IPLimiter
	if cfg.MaxRoomsPerIPPerMinute != -1 {
		maxPerIP := cfg.MaxRoomsPerIPPerMinute
		if maxPerIP <= 0 {
			maxPerIP = defaultMaxRoomsPerIPPerMinute
		}
		ipLim = admission.New(int64(maxPerIP), time.Minute)
	}

	// Propagate MaxSubscriptions into the RegistryConfig so each RelaySession
	// enforces the same ceiling.
	regCfg := cfg.RegistryConfig
	regCfg.MaxSubscriptions = maxSubs

	publicReceiverURL := strings.TrimSpace(cfg.PublicReceiverURL)
	if publicReceiverURL == "" {
		publicReceiverURL = strings.TrimSpace(os.Getenv("PUBLIC_RECEIVER_URL"))
	}
	if publicReceiverURL == "" {
		publicReceiverURL = defaultPublicReceiverURL
	}
	publicRelayURL := strings.TrimSpace(cfg.PublicRelayURL)
	if publicRelayURL == "" {
		publicRelayURL = strings.TrimSpace(os.Getenv("PUBLIC_RELAY_URL"))
	}

	return &Server{
		relaySessions:            session.NewRegistryWithConfig(regCfg),
		jwtSecret:                cfg.JWTSecret,
		subscriberJWTSecret:      subscriberSecret,
		sourceTokenIssuer:        sourceIssuer,
		authorityMode:            authorityMode,
		settingEngine:            cfg.SettingEngine,
		api:                      cfg.API,
		Metrics:                  metrics.New(),
		callbackClient:           cfg.CallbackClient,
		relayEpoch:               relayEpoch,
		controlReconcileInterval: reconcileInterval,
		controlStateChanges:      make(chan *session.RelaySession, maxRooms),
		webhookDispatcher:        cfg.WebhookDispatcher,
		iceServers:               cfg.ICEServers,
		clientICEServers:         cfg.ClientICEServers,
		iceTCPMux:                cfg.ICETCPMux,
		iceUDPMux:                cfg.ICEUDPMux,
		nat1to1IPs:               cfg.NAT1To1IPs,
		maxRooms:                 maxRooms,
		maxSubscribersPerRoom:    maxSubs,
		maxInvitations:           maxInvitations,
		handshakeAdmission:       admission.NewGate(maxHandshakes),
		ipLimiter:                ipLim,
		signalPeers:              make(map[string]*signalPeer),
		useTURN:                  cfg.UseTURN,
		publicReceiverURL:        strings.TrimRight(publicReceiverURL, "/"),
		publicRelayURL:           strings.TrimRight(publicRelayURL, "/"),
		joinInvites:              make(map[string]joinInvite),
	}
}
