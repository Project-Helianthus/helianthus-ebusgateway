package ebusgateway

// Package-doc continued: startup source-address selection bus adapter.
//
// This file implements the SourceAddressSelectionBus interface from
// ebusgo/protocol, wired over the PassiveTransactionReconstructor's
// subscription channel. It is the source-of-truth bridge for startup-admission
// warmup on source-selection-capable direct transports (ENH, ENS, UDP-plain, TCP-plain).
// ebusd-tcp does NOT instantiate this adapter per AD13.

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
)

var ErrSourceSelectionBusInquiryUnsupported = errors.New("source address selection: inquiry not supported (InquiryEnabled=false)")

// TransportAdmissionPath is the admission-path dispatch decision for a given
// transport kind per the startup-admission-discovery plan's transport
// capability matrix (see plan AD11 and
// helianthus-docs-ebus/architecture/startup-admission-and-discovery.md §10).
type TransportAdmissionPath uint8

const (
	// TransportAdmissionSourceSelectionCapable denotes a direct transport on which the
	// gateway runs the source-address selector warmup before any
	// non-override active frame. Applies to ENH, ENS, UDP-plain, TCP-plain.
	TransportAdmissionSourceSelectionCapable TransportAdmissionPath = iota + 1

	// TransportAdmissionStaticFallback denotes the ebusd-tcp path, where the
	// gateway does NOT instantiate source-address selection and uses the configured
	// ScanSource as admission-fallback per AD13.
	TransportAdmissionStaticFallback
)

type sourceSelectionBusAdapter struct {
	reconstructor  *PassiveTransactionReconstructor
	name           string
	priority       PassiveSubscriberPriority
	buffer         int
	inquiryEnabled bool
}

// NewSourceSelectionBusAdapter returns a protocol.SourceAddressSelectionBus subscribed to
// the given reconstructor with priority=NonCritical and default buffer.
func NewSourceSelectionBusAdapter(reconstructor *PassiveTransactionReconstructor, name string, inquiryEnabled bool) (protocol.SourceAddressSelectionBus, error) {
	if reconstructor == nil {
		return nil, fmt.Errorf("source address selection: reconstructor is nil")
	}
	if name == "" {
		name = "startup_admission_source_selection_bus"
	}
	return &sourceSelectionBusAdapter{
		reconstructor:  reconstructor,
		name:           name,
		priority:       PassiveSubscriberNonCritical,
		buffer:         0,
		inquiryEnabled: inquiryEnabled,
	}, nil
}

func (a *sourceSelectionBusAdapter) Listen(ctx context.Context, onFrame func(protocol.Frame)) error {
	if ctx == nil {
		ctx = context.Background()
	}

	subscription, err := a.reconstructor.Subscribe(a.name, a.priority, a.buffer)
	if err != nil {
		return fmt.Errorf("source address selection: subscribe: %w", err)
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
			forwardSourceSelectionBusEvent(event, onFrame)
		}
	}
}

func forwardSourceSelectionBusEvent(event PassiveClassifiedEvent, onFrame func(protocol.Frame)) {
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

func (a *sourceSelectionBusAdapter) InquiryExistence(ctx context.Context) error {
	return ErrSourceSelectionBusInquiryUnsupported
}

// ClassifyTransportAdmission returns the admission path dispatch for the
// given TransportProtocol per the startup-admission-discovery plan's
// transport capability matrix. Unknown or empty values return (zero, error).
func ClassifyTransportAdmission(kind TransportProtocol) (TransportAdmissionPath, error) {
	switch kind {
	case TransportENH, TransportENS, TransportUDPPlain, TransportTCPPlain:
		return TransportAdmissionSourceSelectionCapable, nil
	case TransportEbusdTCP:
		return TransportAdmissionStaticFallback, nil
	case TransportAdapterDirect:
		return 0, fmt.Errorf("source-selection bus: adapter-direct is a multiplexer, classify its underlying transport")
	case "":
		return 0, fmt.Errorf("source-selection bus: empty transport protocol")
	default:
		return 0, fmt.Errorf("source-selection bus: unknown transport protocol %q", kind)
	}
}

// ResolveAdmissionPath returns the admission-path dispatch with adapter-direct
// special-cased to JoinCapable. Adapter-direct multiplexer mode always wraps a
// source-selection-capable underlying transport (ENH or ENS in practice; UDP/TCP-plain are
// not configurations the multiplexer is built for). The source-address
// selection bus adapter
// subscribes to the same PassiveTransactionReconstructor regardless of
// multiplexer presence, so adapter-direct deployments MUST run source-address
// selection.
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
		return TransportAdmissionSourceSelectionCapable, true
	}
	resolved, err := ClassifyTransportAdmission(kind)
	if err != nil {
		return TransportAdmissionStaticFallback, false
	}
	return resolved, false
}

// DefaultStartupAdmissionSourceSelectionConfig returns the SourceAddressSelectionConfig
// used by the startup-admission-discovery plan for source-selection-capable direct
// transports.
// See plan AD01/AD02/AD09 and
// helianthus-docs-ebus/architecture/startup-admission-and-discovery.md §2.2.3.
func DefaultStartupAdmissionSourceSelectionConfig() protocol.SourceAddressSelectionConfig {
	return protocol.SourceAddressSelectionConfig{
		ListenWarmup:   5 * time.Second,
		InquiryEnabled: false,
	}
}

type noObservationSourceSelectionBus struct{}

func (noObservationSourceSelectionBus) Listen(context.Context, func(protocol.Frame)) error {
	return nil
}

func (noObservationSourceSelectionBus) InquiryExistence(context.Context) error {
	return ErrSourceSelectionBusInquiryUnsupported
}

// SelectDefaultStartupSourceAddress applies the docs-backed Helianthus
// source-selection policy without passive observations. This covers transports
// that cannot expose an observe-first lane: "auto" still means source
// selection, not source 0x00.
func SelectDefaultStartupSourceAddress(ctx context.Context) (protocol.SourceAddressSelection, error) {
	cfg := DefaultStartupAdmissionSourceSelectionConfig()
	cfg.ListenWarmup = time.Nanosecond
	cfg.InquiryEnabled = false
	selector := protocol.NewSourceAddressSelector(noObservationSourceSelectionBus{}, cfg)
	return selector.Select(ctx)
}
