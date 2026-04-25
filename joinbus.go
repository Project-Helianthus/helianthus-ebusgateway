package ebusgateway

// Package-doc continued: JoinBus adapter.
//
// This file implements the JoinBus interface from ebusgo/protocol, wired
// over the PassiveTransactionReconstructor's subscription channel. It is
// the source-of-truth bridge for startup-admission warmup on join-capable
// direct transports (ENH, ENS, UDP-plain, TCP-plain). ebusd-tcp does NOT
// instantiate this adapter per AD13.

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
)

var ErrJoinBusInquiryUnsupported = errors.New("joinbus: inquiry not supported (InquiryEnabled=false)")

// TransportAdmissionPath is the admission-path dispatch decision for a given
// transport kind per the startup-admission-discovery plan's transport
// capability matrix (see plan AD11 and
// helianthus-docs-ebus/architecture/startup-admission-and-discovery.md §10).
type TransportAdmissionPath uint8

const (
	// TransportAdmissionJoinCapable denotes a direct transport on which the
	// gateway runs the Joiner warmup + JoinBus adapter before any
	// non-override active frame. Applies to ENH, ENS, UDP-plain, TCP-plain.
	TransportAdmissionJoinCapable TransportAdmissionPath = iota + 1

	// TransportAdmissionStaticFallback denotes the ebusd-tcp path, where the
	// gateway does NOT instantiate Joiner and uses the configured
	// ScanSource as admission-fallback per AD13.
	TransportAdmissionStaticFallback
)

type joinBusAdapter struct {
	reconstructor  *PassiveTransactionReconstructor
	name           string
	priority       PassiveSubscriberPriority
	buffer         int
	inquiryEnabled bool
}

// NewJoinBusAdapter returns a protocol.JoinBus subscribed to the given
// reconstructor with priority=NonCritical and default buffer.
func NewJoinBusAdapter(reconstructor *PassiveTransactionReconstructor, name string, inquiryEnabled bool) (protocol.JoinBus, error) {
	if reconstructor == nil {
		return nil, fmt.Errorf("joinbus: reconstructor is nil")
	}
	if name == "" {
		name = "startup_admission_joinbus"
	}
	return &joinBusAdapter{
		reconstructor:  reconstructor,
		name:           name,
		priority:       PassiveSubscriberNonCritical,
		buffer:         0,
		inquiryEnabled: inquiryEnabled,
	}, nil
}

func (a *joinBusAdapter) Listen(ctx context.Context, onFrame func(protocol.Frame)) error {
	if ctx == nil {
		ctx = context.Background()
	}

	subscription, err := a.reconstructor.Subscribe(a.name, a.priority, a.buffer)
	if err != nil {
		return fmt.Errorf("joinbus: subscribe: %w", err)
	}
	defer subscription.Close()

	events := subscription.Events()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event, ok := <-events:
			if !ok {
				return nil
			}
			forwardJoinBusEvent(event, onFrame)
		}
	}
}

func forwardJoinBusEvent(event PassiveClassifiedEvent, onFrame func(protocol.Frame)) {
	if onFrame == nil {
		return
	}

	switch event.Kind {
	case PassiveClassifiedEventBroadcastFrame, PassiveClassifiedEventMasterFrame:
		onFrame(event.Request)
	case PassiveClassifiedEventTransaction:
		onFrame(event.Request)
		if event.HasResponse {
			onFrame(event.Response)
		}
	}
}

func (a *joinBusAdapter) InquiryExistence(ctx context.Context) error {
	return ErrJoinBusInquiryUnsupported
}

// ClassifyTransportAdmission returns the admission path dispatch for the
// given TransportProtocol per the startup-admission-discovery plan's
// transport capability matrix. Unknown or empty values return (zero, error).
func ClassifyTransportAdmission(kind TransportProtocol) (TransportAdmissionPath, error) {
	switch kind {
	case TransportENH, TransportENS, TransportUDPPlain, TransportTCPPlain:
		return TransportAdmissionJoinCapable, nil
	case TransportEbusdTCP:
		return TransportAdmissionStaticFallback, nil
	case TransportAdapterDirect:
		return 0, fmt.Errorf("joinbus: adapter-direct is a multiplexer, classify its underlying transport")
	case "":
		return 0, fmt.Errorf("joinbus: empty transport protocol")
	default:
		return 0, fmt.Errorf("joinbus: unknown transport protocol %q", kind)
	}
}

// ResolveAdmissionPath returns the admission-path dispatch with adapter-direct
// special-cased to JoinCapable. Adapter-direct multiplexer mode always wraps a
// join-capable underlying transport (ENH or ENS in practice; UDP/TCP-plain are
// not configurations the multiplexer is built for). The JoinBus adapter
// subscribes to the same PassiveTransactionReconstructor regardless of
// multiplexer presence, so adapter-direct deployments MUST run Joiner.
//
// This helper exists because ClassifyTransportAdmission is intentionally pure
// (one transport at a time, no multiplexer-context awareness) and rejects
// adapter-direct as needing inner-transport unwrap. Callers that have access to
// the full Config (and therefore know about the multiplexer wrapper) should use
// this helper instead of ClassifyTransportAdmission directly.
//
// Returns the resolved admission path. The boolean indicates whether the
// adapter-direct special case fired (so the caller can log the multiplexer
// detection once, not twice). Empty/unknown protocols fall back to
// StaticFallback with the second return false.
//
// Resolves cruise-run #20 validation finding: startup_scan.go had its own
// ClassifyTransportAdmission calls that took the static-fallback path on
// adapter-direct, contradicting main.go's special-case. Centralising the
// logic here keeps all call sites in agreement.
func ResolveAdmissionPath(kind TransportProtocol) (path TransportAdmissionPath, adapterDirectSpecialCase bool) {
	if kind == TransportAdapterDirect {
		return TransportAdmissionJoinCapable, true
	}
	resolved, err := ClassifyTransportAdmission(kind)
	if err != nil {
		return TransportAdmissionStaticFallback, false
	}
	return resolved, false
}

// DefaultStartupAdmissionJoinConfig returns the JoinConfig used by the
// startup-admission-discovery plan for join-capable direct transports.
// See plan AD01/AD02/AD09 and
// helianthus-docs-ebus/architecture/startup-admission-and-discovery.md §2.2.3.
func DefaultStartupAdmissionJoinConfig() protocol.JoinConfig {
	return protocol.JoinConfig{
		ListenWarmup:       5 * time.Second,
		InquiryEnabled:     false,
		PersistLastGood:    true,
		PersistLastGoodSet: true,
	}
}
