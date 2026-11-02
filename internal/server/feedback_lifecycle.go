package server

// Feedback state carries no authorization. Remove it once the Session is gone
// or this media writer is fenced; writer tombstones retain their own rules.
func (s *Server) forgetSessionFeedback(sessionID string) {
	s.codecHintStates.Delete(sessionID)
	s.iceRestartStates.Delete(sessionID)
}

// A peer admitted just before deletion can finish allocating feedback state
// after immediate teardown. The periodic sweep reclaims that late state too.
func (s *Server) pruneSessionFeedback() {
	s.codecHintStates.Range(func(key, _ any) bool {
		if _, live := s.relaySessions.Get(key.(string)); !live {
			s.codecHintStates.Delete(key)
		}
		return true
	})
	s.iceRestartStates.Range(func(key, _ any) bool {
		if _, live := s.relaySessions.Get(key.(string)); !live {
			s.iceRestartStates.Delete(key)
		}
		return true
	})
}
