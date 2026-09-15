package ebusgateway

// TransportRuntimeProtocol is the small, installation-independent vocabulary
// owned by the Gateway's runtime transport metric. It is intentionally not a
// native protocol identity or a source locator.
type TransportRuntimeProtocol string

const (
	TransportRuntimeProtocolModbusTCP TransportRuntimeProtocol = "modbus_tcp"
	TransportRuntimeProtocolEEBus     TransportRuntimeProtocol = "eebus"
)

// TransportRuntimeState describes the Gateway lifecycle view captured at a
// lifecycle transition. It does not claim peer/device connectivity.
type TransportRuntimeState string

const (
	TransportRuntimeStateDisabled TransportRuntimeState = "disabled"
	TransportRuntimeStateStarting TransportRuntimeState = "starting"
	TransportRuntimeStateReady    TransportRuntimeState = "ready"
	TransportRuntimeStateDegraded TransportRuntimeState = "degraded"
	TransportRuntimeStateRetired  TransportRuntimeState = "retired"
	TransportRuntimeStateUnknown  TransportRuntimeState = "unknown"
)

// TransportRuntimeOutcome is the finite availability result associated with a
// captured lifecycle state.
type TransportRuntimeOutcome string

const (
	TransportRuntimeOutcomeUnavailable TransportRuntimeOutcome = "unavailable"
	TransportRuntimeOutcomePending     TransportRuntimeOutcome = "pending"
	TransportRuntimeOutcomeAvailable   TransportRuntimeOutcome = "available"
	TransportRuntimeOutcomeUnknown     TransportRuntimeOutcome = "unknown"
)

const (
	TransportRuntimeReasonNotConfigured = "not_configured"
	TransportRuntimeReasonNone          = "none"
	TransportRuntimeReasonStartupFailed = "startup_failed"
	TransportRuntimeReasonShutdown      = "shutdown"
	TransportRuntimeReasonUnknown       = "unknown"
)

// TransportRuntimeStatus is the detached, Gateway-owned value rendered by
// /metrics. Only the fixed label vocabulary below may enter this structure.
type TransportRuntimeStatus struct {
	Protocol TransportRuntimeProtocol
	State    TransportRuntimeState
	Reason   string
	Outcome  TransportRuntimeOutcome
	// Revision is Gateway-internal ordering evidence. It is intentionally not
	// rendered as a Prometheus label.
	Revision uint64
}

func defaultTransportRuntimeStatus(protocol TransportRuntimeProtocol) TransportRuntimeStatus {
	return TransportRuntimeStatus{Protocol: protocol, State: TransportRuntimeStateDisabled, Reason: TransportRuntimeReasonNotConfigured, Outcome: TransportRuntimeOutcomeUnavailable}
}

func unknownTransportRuntimeStatus(protocol TransportRuntimeProtocol) TransportRuntimeStatus {
	return TransportRuntimeStatus{Protocol: protocol, State: TransportRuntimeStateUnknown, Reason: TransportRuntimeReasonUnknown, Outcome: TransportRuntimeOutcomeUnknown}
}

func normalizeTransportRuntimeStatus(status TransportRuntimeStatus) TransportRuntimeStatus {
	if status.Protocol != TransportRuntimeProtocolModbusTCP && status.Protocol != TransportRuntimeProtocolEEBus {
		return TransportRuntimeStatus{}
	}
	if !validTransportRuntimeState(status.State) || !validTransportRuntimeOutcome(status.Outcome) || !validTransportRuntimeReason(status.Reason) {
		unknown := unknownTransportRuntimeStatus(status.Protocol)
		unknown.Revision = status.Revision
		return unknown
	}
	return status
}

func validTransportRuntimeState(state TransportRuntimeState) bool {
	switch state {
	case TransportRuntimeStateDisabled, TransportRuntimeStateStarting, TransportRuntimeStateReady, TransportRuntimeStateDegraded, TransportRuntimeStateRetired, TransportRuntimeStateUnknown:
		return true
	default:
		return false
	}
}

func validTransportRuntimeOutcome(outcome TransportRuntimeOutcome) bool {
	switch outcome {
	case TransportRuntimeOutcomeUnavailable, TransportRuntimeOutcomePending, TransportRuntimeOutcomeAvailable, TransportRuntimeOutcomeUnknown:
		return true
	default:
		return false
	}
}

func validTransportRuntimeReason(reason string) bool {
	switch reason {
	case TransportRuntimeReasonNotConfigured, TransportRuntimeReasonNone, TransportRuntimeReasonStartupFailed, TransportRuntimeReasonShutdown, TransportRuntimeReasonUnknown:
		return true
	default:
		return false
	}
}

func writeTransportRuntimeMetrics(writer *prometheusWriter, statuses map[TransportRuntimeProtocol]TransportRuntimeStatus) {
	writer.writeHelp("helianthus_transport_runtime_status", "Gateway-owned, detached transport runtime lifecycle status.")
	writer.writeType("helianthus_transport_runtime_status", "gauge")
	for _, protocol := range []TransportRuntimeProtocol{TransportRuntimeProtocolModbusTCP, TransportRuntimeProtocolEEBus} {
		status, ok := statuses[protocol]
		if !ok {
			status = defaultTransportRuntimeStatus(protocol)
		}
		status = normalizeTransportRuntimeStatus(status)
		if status.Protocol == "" {
			status = unknownTransportRuntimeStatus(protocol)
		}
		writer.writeGaugeSample("helianthus_transport_runtime_status", 1, labelMap(
			"outcome", string(status.Outcome),
			"protocol", string(status.Protocol),
			"reason", status.Reason,
			"state", string(status.State),
		))
	}
}
