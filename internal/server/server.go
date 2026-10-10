package server

import (
	"context"
	"github.com/pocketstation-io/relay/access"
	accessAdmission "github.com/pocketstation-io/relay/access/admission"
	"github.com/pocketstation-io/relay/access/storage/memory"
	accessTurn "github.com/pocketstation-io/relay/access/turn"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	pionIce "github.com/pion/ice/v4"
	"github.com/pion/webrtc/v4"
	"github.com/pocketstation-io/relay/internal/admission"
	"github.com/pocketstation-io/relay/internal/metrics"
	"github.com/pocketstation-io/relay/internal/notifications/callback"
	"github.com/pocketstation-io/relay/internal/notifications/webhook"
	"github.com/pocketstation-io/relay/internal/session"
)

// Server is the top-level relay server.
type Server struct {
	accessService            *access.Service
	accessHandlerConfig      access.HandlerConfig
	accessInitError          error
	mediaWriterMu            sync.Mutex
	mediaWriters             map[string]*mediaWriter
	relaySessions            *session.SessionRegistry
	jwtSecret                []byte
	authorityMode            string
	settingEngine            *webrtc.SettingEngine
	api                      *webrtc.API
	Metrics                  *metrics.Registry
	callbackClient           *callback.Client
	relayEpoch               string
	controlAuthorityTimeout  time.Duration
	controlReconcileInterval time.Duration
	controlStateChanges      chan *session.RelaySession
	controlSyncOnce          sync.Once
	controlSyncContext       context.Context
	controlSyncCancel        context.CancelFunc
	controlSyncWait          sync.WaitGroup
	webhookDispatcher        *webhook.Dispatcher
	iceServers               []webrtc.ICEServer // relay's own Pion PeerConnections
	clientICEServers         []webrtc.ICEServer // returned in Session creation responses
	iceTCPMux                pionIce.TCPMux
	iceUDPMux                pionIce.UDPMux
	nat1to1IPs               []string

	maxRooms              int // set once at construction
	maxSubscribersPerRoom int // set once at construction
	maxInvitations        int
	handshakeAdmission    *admission.Gate

	// mu guards lifecycle admission, httpServer and signaling-peer state.
	mu           sync.RWMutex
	httpServer   *http.Server
	signalPeers  map[string]*signalPeer
	shuttingDown bool

	// codecHintStates and iceRestartStates are keyed by RelaySession ID.
	// sync.Map for concurrent access from multiple subscriber RTCP goroutines.
	codecHintStates  sync.Map
	iceRestartStates sync.Map

	// whipConns maps WHIP/WHEP connection IDs to their live PeerConnections.
	// Keyed by the opaque connID returned in the Location header (RFC 9725).
	// sync.Map: concurrent PATCH/DELETE from multiple HTTP goroutines.
	whipConns sync.Map

	// audioIngressConns contains authenticated, bounded WSS media sources.
	// Each retains a handshake admission slot until its source closes.
	audioIngressConns sync.Map

	// useTURN is set once from Config.UseTURN; propagated to ICE_RESTART msgs.
	useTURN bool

	publicReceiverURL     string
	publicRelayURL        string
	publicControlPlaneURL string
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
	authorityTimeout := cfg.ControlAuthorityTimeout
	if authorityTimeout <= 0 {
		authorityTimeout = defaultControlAuthorityTimeout
	} else if authorityTimeout > defaultControlAuthorityTimeout {
		authorityTimeout = defaultControlAuthorityTimeout
	}
	relayEpoch := cfg.RelayEpoch
	if relayEpoch == "" {
		relayEpoch = newID()
	}
	authorityMode := cfg.AuthorityMode
	if authorityMode == "" {
		authorityMode = "standalone"
	}

	// Propagate MaxSubscriptions into the RegistryConfig so each RelaySession
	// enforces the same ceiling.
	regCfg := cfg.RegistryConfig
	regCfg.MaxSubscriptions = maxSubs

	publicReceiverURL := strings.TrimSpace(cfg.PublicReceiverURL)
	if publicReceiverURL == "" {
		publicReceiverURL = strings.TrimSpace(os.Getenv("PUBLIC_RECEIVER_URL"))
	}
	publicRelayURL := strings.TrimSpace(cfg.PublicRelayURL)
	if publicRelayURL == "" {
		publicRelayURL = strings.TrimSpace(os.Getenv("PUBLIC_RELAY_URL"))
	}
	publicControlPlaneURL := strings.TrimSpace(cfg.PublicControlPlaneURL)
	if publicControlPlaneURL == "" {
		publicControlPlaneURL = strings.TrimSpace(os.Getenv("PUBLIC_CONTROL_PLANE_URL"))
	}

	service := cfg.AccessService
	var initErr error
	if authorityMode == "standalone" && service == nil {
		service, initErr = access.NewService(cfg.JWTSecret, memory.New(), access.Config{MaxSessions: maxRooms, MaxInvitations: maxInvitations})
	}
	handlerConfig := cfg.AccessHandlerConfig
	for _, ice := range cfg.ClientICEServers {
		credential, _ := ice.Credential.(string)
		handlerConfig.ICEServers = append(handlerConfig.ICEServers, accessTurn.ICEServer{URLs: append([]string(nil), ice.URLs...), Username: ice.Username, Credential: credential})
	}

	if handlerConfig.CreateAdmission == nil {
		limit := cfg.MaxRoomsPerIPPerMinute
		if limit == 0 {
			limit = defaultMaxRoomsPerIPPerMinute
		}
		handlerConfig.CreateAdmission = accessAdmission.NewIPLimiter(limit, time.Minute, 4096)
	}
	if handlerConfig.ResolveAdmission == nil {
		handlerConfig.ResolveAdmission = accessAdmission.NewIPLimiter(120, time.Minute, 4096)
	}
	return &Server{
		accessService: service, accessInitError: initErr, accessHandlerConfig: handlerConfig, mediaWriters: make(map[string]*mediaWriter),
		relaySessions:            session.NewRegistryWithConfig(regCfg),
		jwtSecret:                cfg.JWTSecret,
		authorityMode:            authorityMode,
		settingEngine:            cfg.SettingEngine,
		api:                      cfg.API,
		Metrics:                  metrics.New(),
		callbackClient:           cfg.CallbackClient,
		relayEpoch:               relayEpoch,
		controlAuthorityTimeout:  authorityTimeout,
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
		signalPeers:              make(map[string]*signalPeer),
		useTURN:                  cfg.UseTURN,
		publicReceiverURL:        strings.TrimRight(publicReceiverURL, "/"),
		publicRelayURL:           strings.TrimRight(publicRelayURL, "/"),
		publicControlPlaneURL:    strings.TrimRight(publicControlPlaneURL, "/"),
	}
}
