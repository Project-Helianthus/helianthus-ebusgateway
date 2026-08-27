package main

import (
	"strconv"
	"strings"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgateway/portal"
)

func auditNumber(value *uint64) string {
	if value == nil {
		return "-"
	}
	return strconv.FormatUint(*value, 10)
}

func portalFM5Interpretation(provider graphql.SemanticProvider) graphql.Fm5Interpretation {
	if provider == nil {
		return graphql.Fm5Interpretation{}
	}
	typed, ok := provider.(graphql.FM5InterpretationProvider)
	if !ok {
		return graphql.Fm5Interpretation{}
	}
	verdict := typed.FM5Interpretation()
	if verdict == (graphql.Fm5Interpretation{}) {
		return verdict
	}
	if verdict.Validate() != nil {
		return graphql.Fm5Interpretation{}
	}
	return verdict
}

func normalizeMountPath(path string, fallback string) string {
	normalized := strings.TrimSpace(path)
	if normalized == "" {
		normalized = fallback
	}
	if !strings.HasPrefix(normalized, "/") {
		normalized = "/" + normalized
	}
	if normalized != "/" {
		normalized = strings.TrimRight(normalized, "/")
	}
	if normalized == "/" {
		return fallback
	}
	return normalized
}

func mapPortalZones(zones []graphql.Zone) []portal.SemanticZone {
	if len(zones) == 0 {
		return nil
	}
	items := make([]portal.SemanticZone, 0, len(zones))
	for _, zone := range zones {
		items = append(items, portal.SemanticZone{
			ID:   zone.ID,
			Name: zone.Name,
			State: portal.SemanticZoneState{
				CurrentTempC:       cloneFloatPtr(zone.State.CurrentTempC),
				CurrentHumidityPct: cloneFloatPtr(zone.State.CurrentHumidityPct),
				HvacAction:         zone.State.HvacAction,
				SpecialFunction:    zone.State.SpecialFunction,
				HeatingDemandPct:   cloneFloatPtr(zone.State.HeatingDemandPct),
				ValvePositionPct:   cloneFloatPtr(zone.State.ValvePositionPct),
			},
			Config: portal.SemanticZoneConfig{
				OperatingMode:              zone.Config.OperatingMode,
				OperationModeChangeable:    cloneBoolPtr(zone.Config.OperationModeChangeable),
				SourceLabel:                cloneStringPtr(zone.Config.SourceLabel),
				Preset:                     zone.Config.Preset,
				TargetTempC:                cloneFloatPtr(zone.Config.TargetTempC),
				AllowedModes:               append([]string(nil), zone.Config.AllowedModes...),
				CircuitType:                zone.Config.CircuitType,
				AssociatedCircuit:          cloneIntPtr(zone.Config.AssociatedCircuit),
				RoomTemperatureZoneMapping: cloneIntPtr(zone.Config.RoomTemperatureZoneMapping),
			},
		})
	}
	return items
}

func mapPortalDHW(status *graphql.DhwStatus) *portal.SemanticDHW {
	if status == nil {
		return nil
	}
	return &portal.SemanticDHW{
		State: portal.SemanticDhwState{
			CurrentTempC:     cloneFloatPtr(status.State.CurrentTempC),
			OverrunActive:    cloneBoolPtr(status.State.OverrunActive),
			SpecialFunction:  status.State.SpecialFunction,
			HeatingDemandPct: cloneFloatPtr(status.State.HeatingDemandPct),
		},
		Config: portal.SemanticDhwConfig{
			OperatingMode:           status.Config.OperatingMode,
			OperationModeChangeable: cloneBoolPtr(status.Config.OperationModeChangeable),
			Preset:                  status.Config.Preset,
			TargetTempC:             cloneFloatPtr(status.Config.TargetTempC),
		},
	}
}

func mapPortalEnergyTotals(value *graphql.EnergyTotals) *portal.SemanticEnergyTotals {
	if value == nil {
		return nil
	}
	return &portal.SemanticEnergyTotals{
		Gas:      mapPortalEnergyChannel(value.Gas),
		Electric: mapPortalEnergyChannel(value.Electric),
		Solar:    mapPortalEnergyChannel(value.Solar),
	}
}

func mapPortalEnergyChannel(channel graphql.EnergyChannel) portal.SemanticEnergyChannel {
	return portal.SemanticEnergyChannel{
		DHW:     mapPortalEnergySeries(channel.DHW),
		Climate: mapPortalEnergySeries(channel.Climate),
	}
}

func mapPortalEnergySeries(series graphql.EnergySeries) portal.SemanticEnergySeries {
	out := portal.SemanticEnergySeries{
		Today:     series.Today,
		TodayMeta: mapPortalEnergyPointMeta(series.TodayMeta),
	}
	if len(series.Yearly) > 0 {
		out.Yearly = append([]float64(nil), series.Yearly...)
	}
	if len(series.Monthly) > 0 {
		out.Monthly = append([]float64(nil), series.Monthly...)
	}
	if len(series.YearlyMeta) > 0 {
		out.YearlyMeta = make([]portal.SemanticEnergyPointMeta, len(series.YearlyMeta))
		for i, meta := range series.YearlyMeta {
			out.YearlyMeta[i] = mapPortalEnergyPointMeta(meta)
		}
	}
	if len(series.MonthlyMeta) > 0 {
		out.MonthlyMeta = make([]portal.SemanticEnergyPointMeta, len(series.MonthlyMeta))
		for i, meta := range series.MonthlyMeta {
			out.MonthlyMeta[i] = mapPortalEnergyPointMeta(meta)
		}
	}
	return out
}

func mapPortalEnergyPointMeta(meta graphql.EnergyPointMeta) portal.SemanticEnergyPointMeta {
	return portal.SemanticEnergyPointMeta{
		FreshnessState:  string(meta.FreshnessState),
		Provenance:      string(meta.Provenance),
		LastObservedUTC: meta.LastObservedUTC,
		AgeSeconds:      meta.AgeSeconds,
		Stale:           meta.Stale,
	}
}

func mapPortalBoilerStatus(status *graphql.BoilerStatus) *portal.SemanticBoilerStatus {
	if status == nil {
		return nil
	}
	return &portal.SemanticBoilerStatus{
		State: portal.SemanticBoilerState{
			FlowTemperatureC:         cloneFloatPtr(status.State.FlowTemperatureC),
			ReturnTemperatureC:       cloneFloatPtr(status.State.ReturnTemperatureC),
			CentralHeatingPumpActive: cloneBoolPtr(status.State.CentralHeatingPumpActive),
			WaterPressureBar:         cloneFloatPtr(status.State.WaterPressureBar),
			ExternalPumpActive:       cloneBoolPtr(status.State.ExternalPumpActive),
			CirculationPumpActive:    cloneBoolPtr(status.State.CirculationPumpActive),
			GasValveActive:           cloneBoolPtr(status.State.GasValveActive),
			FlameActive:              cloneBoolPtr(status.State.FlameActive),
			DiverterValvePositionPct: cloneFloatPtr(status.State.DiverterValvePositionPct),
			FanSpeedRpm:              cloneIntPtr(status.State.FanSpeedRpm),
			TargetFanSpeedRpm:        cloneIntPtr(status.State.TargetFanSpeedRpm),
			IonisationVoltageUa:      cloneFloatPtr(status.State.IonisationVoltageUa),
			DhwWaterFlowLpm:          cloneFloatPtr(status.State.DhwWaterFlowLpm),
			DhwDemandActive:          cloneBoolPtr(status.State.DhwDemandActive),
			HeatingSwitchActive:      cloneBoolPtr(status.State.HeatingSwitchActive),
			StorageLoadPumpPct:       cloneFloatPtr(status.State.StorageLoadPumpPct),
			ModulationPct:            cloneFloatPtr(status.State.ModulationPct),
			PrimaryCircuitFlowLpm:    cloneFloatPtr(status.State.PrimaryCircuitFlowLpm),
			FlowTempDesiredC:         cloneFloatPtr(status.State.FlowTempDesiredC),
			DhwTempDesiredC:          cloneFloatPtr(status.State.DhwTempDesiredC),
			StateNumber:              cloneIntPtr(status.State.StateNumber),
			DhwTemperatureC:          cloneFloatPtr(status.State.DhwTemperatureC),
			DhwTargetTemperatureC:    cloneFloatPtr(status.State.DhwTargetTemperatureC),
		},
		Config: portal.SemanticBoilerConfig{
			DhwOperatingMode: cloneStringPtr(status.Config.DhwOperatingMode),
			FlowsetHcMaxC:    cloneFloatPtr(status.Config.FlowsetHcMaxC),
			FlowsetHwcMaxC:   cloneFloatPtr(status.Config.FlowsetHwcMaxC),
			PartloadHcKW:     cloneFloatPtr(status.Config.PartloadHcKW),
			PartloadHwcKW:    cloneFloatPtr(status.Config.PartloadHwcKW),
		},
		Diagnostics: portal.SemanticBoilerDiagnostics{
			HeatingStatusRaw:         cloneIntPtr(status.Diagnostics.HeatingStatusRaw),
			DhwStatusRaw:             cloneIntPtr(status.Diagnostics.DhwStatusRaw),
			CentralHeatingHours:      cloneFloatPtr(status.Diagnostics.CentralHeatingHours),
			DhwHours:                 cloneFloatPtr(status.Diagnostics.DhwHours),
			CentralHeatingStarts:     cloneIntPtr(status.Diagnostics.CentralHeatingStarts),
			DhwStarts:                cloneIntPtr(status.Diagnostics.DhwStarts),
			PumpHours:                cloneFloatPtr(status.Diagnostics.PumpHours),
			FanHours:                 cloneFloatPtr(status.Diagnostics.FanHours),
			DeactivationsIFC:         cloneIntPtr(status.Diagnostics.DeactivationsIFC),
			DeactivationsTemplimiter: cloneIntPtr(status.Diagnostics.DeactivationsTemplimiter),
		},
	}
}

func mapPortalSystemStatus(status *graphql.SystemStatus) *portal.SemanticSystemStatus {
	if status == nil {
		return nil
	}
	return &portal.SemanticSystemStatus{
		State: portal.SemanticSystemState{
			SystemOff:                    cloneBoolPtr(status.State.SystemOff),
			SystemWaterPressure:          cloneFloatPtr(status.State.SystemWaterPressure),
			SystemFlowTemperature:        cloneFloatPtr(status.State.SystemFlowTemperature),
			OutdoorTemperature:           cloneFloatPtr(status.State.OutdoorTemperature),
			OutdoorTemperatureAvg24h:     cloneFloatPtr(status.State.OutdoorTemperatureAvg24h),
			MaintenanceDue:               cloneBoolPtr(status.State.MaintenanceDue),
			HwcCylinderTemperatureTop:    cloneFloatPtr(status.State.HwcCylinderTemperatureTop),
			HwcCylinderTemperatureBottom: cloneFloatPtr(status.State.HwcCylinderTemperatureBottom),
		},
		Config: portal.SemanticSystemConfig{
			AdaptiveHeatingCurve:         cloneBoolPtr(status.Config.AdaptiveHeatingCurve),
			AlternativePoint:             cloneFloatPtr(status.Config.AlternativePoint),
			HeatingCircuitBivalencePoint: cloneFloatPtr(status.Config.HeatingCircuitBivalencePoint),
			DhwBivalencePoint:            cloneFloatPtr(status.Config.DhwBivalencePoint),
			HcEmergencyTemperature:       cloneFloatPtr(status.Config.HcEmergencyTemperature),
			HwcMaxFlowTempDesired:        cloneFloatPtr(status.Config.HwcMaxFlowTempDesired),
			MaxRoomHumidity:              cloneIntPtr(status.Config.MaxRoomHumidity),
		},
		Properties: portal.SemanticSystemProperties{
			SystemScheme:            cloneIntPtr(status.Properties.SystemScheme),
			ModuleConfigurationVR71: cloneIntPtr(status.Properties.ModuleConfigurationVR71),
		},
		GatewayBrand:  cloneStringPtr(status.GatewayBrand),
		GatewayVendor: cloneStringPtr(status.GatewayVendor),
	}
}

func mapPortalCircuits(circuits []graphql.CircuitStatus) []portal.SemanticCircuit {
	if len(circuits) == 0 {
		return nil
	}
	items := make([]portal.SemanticCircuit, 0, len(circuits))
	for _, circuit := range circuits {
		items = append(items, portal.SemanticCircuit{
			Index:       circuit.Index,
			CircuitType: circuit.CircuitType,
			HasMixer:    circuit.HasMixer,
			State: portal.SemanticCircuitState{
				PumpActive:       cloneBoolPtr(circuit.State.PumpActive),
				MixerPositionPct: cloneFloatPtr(circuit.State.MixerPositionPct),
				FlowTemperatureC: cloneFloatPtr(circuit.State.FlowTemperatureC),
				FlowSetpointC:    cloneFloatPtr(circuit.State.FlowSetpointC),
				CalcFlowTempC:    cloneFloatPtr(circuit.State.CalcFlowTempC),
				CircuitState:     circuit.State.CircuitState,
				Humidity:         cloneFloatPtr(circuit.State.Humidity),
				DewPoint:         cloneFloatPtr(circuit.State.DewPoint),
				PumpHours:        cloneFloatPtr(circuit.State.PumpHours),
				PumpStarts:       cloneIntPtr(circuit.State.PumpStarts),
			},
			Config: portal.SemanticCircuitConfig{
				HeatingCurve:    cloneFloatPtr(circuit.Config.HeatingCurve),
				FlowTempMaxC:    cloneFloatPtr(circuit.Config.FlowTempMaxC),
				FlowTempMinC:    cloneFloatPtr(circuit.Config.FlowTempMinC),
				SummerLimitC:    cloneFloatPtr(circuit.Config.SummerLimitC),
				FrostProtC:      cloneFloatPtr(circuit.Config.FrostProtC),
				RoomTempControl: circuit.Config.RoomTempControl,
				CoolingEnabled:  cloneBoolPtr(circuit.Config.CoolingEnabled),
			},
			ManagingDevice: portal.SemanticManagingDevice{
				Role:     string(circuit.ManagingDevice.Role),
				DeviceID: cloneStringPtr(circuit.ManagingDevice.DeviceID),
				Address:  cloneIntPtr(circuit.ManagingDevice.Address),
			},
		})
	}
	return items
}

func mapPortalRadioDevices(devices []graphql.RadioDevice) []portal.SemanticRadioDevice {
	if len(devices) == 0 {
		return nil
	}
	items := make([]portal.SemanticRadioDevice, 0, len(devices))
	for _, device := range devices {
		items = append(items, portal.SemanticRadioDevice{
			Group:                device.Group,
			Instance:             device.Instance,
			SlotMode:             device.SlotMode,
			DeviceConnected:      cloneBoolPtr(device.DeviceConnected),
			DeviceClassAddress:   cloneIntPtr(device.DeviceClassAddress),
			DeviceModel:          device.DeviceModel,
			FirmwareVersion:      cloneStringPtr(device.FirmwareVersion),
			HardwareIdentifier:   cloneIntPtr(device.HardwareIdentifier),
			RemoteControlAddress: cloneIntPtr(device.RemoteControlAddress),
			DevicePaired:         cloneBoolPtr(device.DevicePaired),
			ReceptionStrength:    cloneIntPtr(device.ReceptionStrength),
			ZoneAssignment:       cloneIntPtr(device.ZoneAssignment),
			RoomTemperatureC:     cloneFloatPtr(device.RoomTemperatureC),
			RoomHumidityPct:      cloneFloatPtr(device.RoomHumidityPct),
		})
	}
	return items
}

func mapPortalSolarStatus(status *graphql.SolarStatus) *portal.SemanticSolarStatus {
	if status == nil {
		return nil
	}
	return &portal.SemanticSolarStatus{
		CollectorTemperatureC: cloneFloatPtr(status.CollectorTemperatureC),
		ReturnTemperatureC:    cloneFloatPtr(status.ReturnTemperatureC),
		PumpActive:            cloneBoolPtr(status.PumpActive),
		CurrentYield:          cloneFloatPtr(status.CurrentYield),
		PumpHours:             cloneFloatPtr(status.PumpHours),
		SolarEnabled:          cloneBoolPtr(status.SolarEnabled),
		FunctionMode:          cloneBoolPtr(status.FunctionMode),
	}
}

func mapPortalCylinders(cylinders []graphql.CylinderStatus) []portal.SemanticCylinder {
	if len(cylinders) == 0 {
		return nil
	}
	items := make([]portal.SemanticCylinder, 0, len(cylinders))
	for _, cylinder := range cylinders {
		items = append(items, portal.SemanticCylinder{
			Index:             cylinder.Index,
			TemperatureC:      cloneFloatPtr(cylinder.TemperatureC),
			MaxSetpointC:      cloneFloatPtr(cylinder.MaxSetpointC),
			ChargeHysteresisC: cloneFloatPtr(cylinder.ChargeHysteresisC),
			ChargeOffsetC:     cloneFloatPtr(cylinder.ChargeOffsetC),
		})
	}
	return items
}

func mapPortalAdapterInfo(info *graphql.AdapterHardwareInfo) *portal.SemanticAdapterInfo {
	if info == nil {
		return nil
	}
	result := &portal.SemanticAdapterInfo{
		FirmwareVersion:    info.FirmwareVersion,
		FirmwareChecksum:   info.FirmwareChecksum,
		BootloaderVersion:  info.BootloaderVersion,
		BootloaderChecksum: info.BootloaderChecksum,
		HardwareID:         info.HardwareID,
		HardwareConfig:     info.HardwareConfig,
		Features:           info.Features,
		Jumpers:            info.Jumpers,
		IsWiFi:             info.IsWiFi,
		IsEthernet:         info.IsEthernet,
		VersionResponseLen: info.VersionResponseLen,
		InfoSupported:      info.InfoSupported,
		TemperatureC:       cloneFloatPtr(info.TemperatureC),
		SupplyVoltageMV:    cloneIntPtr(info.SupplyVoltageMV),
		BusVoltageMaxDV:    cloneIntPtr(info.BusVoltageMaxDV),
		BusVoltageMinDV:    cloneIntPtr(info.BusVoltageMinDV),
		ResetCause:         cloneStringPtr(info.ResetCause),
		WiFiRSSIDBm:        cloneIntPtr(info.WiFiRSSIDBm),
	}
	if info.JumperFlags != nil {
		result.JumperFlags = make([]string, len(info.JumperFlags))
		copy(result.JumperFlags, info.JumperFlags)
	}
	if info.ResetCauseCode != nil {
		code := int(*info.ResetCauseCode)
		result.ResetCauseCode = &code
	}
	if info.RestartCount != nil {
		count := int(*info.RestartCount)
		result.RestartCount = &count
	}
	if info.LastIdentityQuery != nil {
		result.LastIdentityQuery = info.LastIdentityQuery.Format(time.RFC3339)
	}
	if info.LastTelemetryQuery != nil {
		result.LastTelemetryQuery = info.LastTelemetryQuery.Format(time.RFC3339)
	}
	return result
}

// buildResponderCapabilityProvider composes the `meta.capabilities.responder`
// signal for the active transport per decision doc @ 567a6798 §4.2 + §5.
//
// Mapping (v1.1, locked):
//   - ENH / ENS → active.scope = "partial", surfaces = FF_03..FF_06,
//     refusal = nil. transports[] reports the same row as "supported"
//     and the ebusd-tcp row as perpetually "blocked".
//   - ebusd-tcp → active.scope = "none", surfaces = [],
//     refusal.code = "command_bridge_no_companion_listen". Consumers
//     apply fail-closed per §4.3 rule 4.
//   - Any other transport (udp-plain, tcp-plain, adapter-direct, empty,
//     or unrecognised string) → returns nil. The caller omits the
//     capability entirely and consumers fall back to §4.3 rule 1
//     (absence → scope=none, fail-closed). This is the only way to
//     preserve invariant I1 (exactly three rows at v1.1) AND I2
//     (active.transport MUST appear in transports[]) simultaneously,
//     because emitting a non-canonical active.transport literal would
//     violate I2 and widening transports[] with a fourth "unknown" row
//     would violate I1.
//
// The raw cfg.TransportConfig.Protocol is first canonicalised via
// ebusgateway.canonicalTransportProtocol (which handles the "ebusd"
// alias → "ebusd-tcp" and lowercases/trims whitespace). This ensures
// downstream envelope assertions see the canonical enum literal in
// every surface, matching the three fixed transports[] rows byte-for-
// byte.
//
// Invariants I1/I7 are enforced by always emitting exactly three
// transport rows (ENH, ENS, ebusd-tcp) in a fixed order. I2/I3 are
// enforced by deriving active.transport from the same canonical enum
// literals the rows use.
//
// Runtime-transport authority (Codex P1 on PR #509): the canonical
// protocol enum alone is NOT sufficient to decide active.scope. The
// adapter-direct URI mode (--address=adapter-direct://...) keeps
// TransportConfig.Protocol as the default "enh" string while the live
// transport instance is actually the adapter-direct mux's active path.
// That path implements transport.RawTransport but does NOT implement
// transport.ResponderTransport — it has no SendResponderBytes primitive.
// Reporting active.scope="partial" purely from the config string would
// over-advertise responder support for FF_03..FF_06 surfaces the gateway
// cannot actually emit. Accordingly the provider type-asserts the live
// transport instance against ebusgoTransport.ResponderTransport; when
// the canonical protocol is ENH/ENS but the instance does NOT satisfy
// the interface, we downgrade to scope="none" with
// refusal.code="transport_mux_bypass" AND rewrite both ENH and ENS
// rows in transports[] to state="blocked", scope="none",
// reason="transport_mux_bypass" so invariant I3 (active.scope ==
// matching row.scope) holds — the mux is a shared runtime wrapper
// above either upstream, so switching the canonical protocol would
// not restore responder emission. ebusd-tcp path is unchanged —
// it is forbidden from responder emission per M4b2 §3 regardless of
// what underlying transport type ebusd is wrapped by.
//
// actualTransport is the live instance returned by the bootstrap
// transport factory. A nil value means the caller has not yet wired a
// transport (legacy callers, some unit-test paths); in that case the
// provider falls back to protocol-only inference to preserve previous
// behaviour on paths that never exercise the adapter-direct mux.
