package main

import ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"

func modbusRuntimeTransportStatus(config ebusgateway.ModbusTCPConfig, started bool) ebusgateway.TransportRuntimeStatus {
	status := ebusgateway.TransportRuntimeStatus{Protocol: ebusgateway.TransportRuntimeProtocolModbusTCP}
	if !config.Enabled {
		status.State = ebusgateway.TransportRuntimeStateDisabled
		status.Reason = ebusgateway.TransportRuntimeReasonNotConfigured
		status.Outcome = ebusgateway.TransportRuntimeOutcomeUnavailable
		return status
	}
	if started {
		status.State = ebusgateway.TransportRuntimeStateReady
		status.Reason = ebusgateway.TransportRuntimeReasonNone
		status.Outcome = ebusgateway.TransportRuntimeOutcomeAvailable
		return status
	}
	status.State = ebusgateway.TransportRuntimeStateDegraded
	status.Reason = ebusgateway.TransportRuntimeReasonStartupFailed
	status.Outcome = ebusgateway.TransportRuntimeOutcomeUnavailable
	return status
}

func retiredModbusRuntimeTransportStatus() ebusgateway.TransportRuntimeStatus {
	return ebusgateway.TransportRuntimeStatus{
		Protocol: ebusgateway.TransportRuntimeProtocolModbusTCP,
		State:    ebusgateway.TransportRuntimeStateRetired,
		Reason:   ebusgateway.TransportRuntimeReasonShutdown,
		Outcome:  ebusgateway.TransportRuntimeOutcomeUnavailable,
	}
}

// publishGatewayTransportRetirement records the final bounded status before
// the control plane is closed. Native sidecar shutdown remains with its
// existing defer owners.
func publishGatewayTransportRetirement(store *ebusgateway.BusObservabilityStore, hasModbus bool, eebus *eebusRuntimeLifecycle) {
	if store == nil {
		return
	}
	if hasModbus {
		store.SetTransportRuntimeStatus(retiredModbusRuntimeTransportStatus())
	}
	if eebus != nil && eebus.Configured() {
		eebus.PublishTransportRetirement()
	}
}
