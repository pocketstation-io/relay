package server

func (s *Server) dispatchCallback(deliver func()) {
	if deliver == nil {
		return
	}
	if !s.callbackAdmission.TryAcquire() {
		s.Metrics.CallbackDroppedTotal.Add(1)
		return
	}
	go func() {
		defer s.callbackAdmission.Release()
		deliver()
	}()
}
