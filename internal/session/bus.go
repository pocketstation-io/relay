package session

// BusID names a forwarding lane within a RelaySession.
type BusID = string

// BusMix subscribes to all active buses.
const BusMix BusID = "mix"

// BusRole classifies an audio bus for forwarding priority and loss tolerance.
type BusRole uint8

const (
	BusRoleVoice BusRole = iota
	BusRoleMusic
	BusRoleAgentOutput
	BusRoleEvents
	BusRoleMonitor
)

func (role BusRole) LatencyRank() uint8 {
	switch role {
	case BusRoleVoice, BusRoleAgentOutput:
		return 0
	case BusRoleMusic:
		return 1
	case BusRoleEvents:
		return 2
	default:
		return 3
	}
}

func (role BusRole) ReliabilityRank() uint8 {
	switch role {
	case BusRoleVoice, BusRoleAgentOutput:
		return 0
	case BusRoleMusic:
		return 1
	case BusRoleEvents:
		return 2
	default:
		return 3
	}
}

func (role BusRole) String() string {
	switch role {
	case BusRoleVoice:
		return "voice"
	case BusRoleMusic:
		return "music"
	case BusRoleAgentOutput:
		return "agent_output"
	case BusRoleEvents:
		return "events"
	case BusRoleMonitor:
		return "monitor"
	default:
		return "unknown"
	}
}
