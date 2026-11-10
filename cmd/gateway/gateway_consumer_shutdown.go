package main

import (
	"io"
	"log"
)

// gatewayConsumerSurface is resolved at cleanup time, after generation tasks join.
type gatewayConsumerSurface struct {
	listener      io.Closer
	reconstructor io.Closer
}

// gatewayConsumerShutdown owns only the HTTP and retained native-consumer tail.
// The lifecycle shell joins generation tasks before calling shutdown and cancels
// the control-plane context afterwards. Call shutdown once from that shell.
type gatewayConsumerShutdown struct {
	stopTransportWatch func()
	publishRetirement  func()
	drainHTTP          func() error
	surface            func() gatewayConsumerSurface
	deduplicator       io.Closer
	busObservability   io.Closer
	advertiser         io.Closer
	logf               func(string, ...any)
}

func (s gatewayConsumerShutdown) shutdown() {
	if s.stopTransportWatch != nil {
		s.stopTransportWatch()
	}
	if s.publishRetirement != nil {
		s.publishRetirement()
	}
	if s.drainHTTP != nil {
		s.logError("http server shutdown", s.drainHTTP())
	}
	var surface gatewayConsumerSurface
	if s.surface != nil {
		surface = s.surface()
	}
	s.close("broadcast listener close", surface.listener)
	s.close("deduplicator close", s.deduplicator)
	s.close("reconstructor close", surface.reconstructor)
	s.close("bus observability close", s.busObservability)
	s.close("mdns close", s.advertiser)
}

func (s gatewayConsumerShutdown) close(label string, resource io.Closer) {
	if resource != nil {
		s.logError(label, resource.Close())
	}
}

func (s gatewayConsumerShutdown) logError(label string, err error) {
	if err == nil {
		return
	}
	logf := s.logf
	if logf == nil {
		logf = log.Printf
	}
	logf("%s: %v", label, err)
}
