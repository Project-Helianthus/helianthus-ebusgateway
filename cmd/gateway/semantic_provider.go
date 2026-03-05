package main

import (
	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
)

type mcpSemanticProviderAdapter struct {
	provider graphql.SemanticProvider
}

func newMCPSemanticProvider(provider graphql.SemanticProvider) mcp.SemanticProvider {
	return mcpSemanticProviderAdapter{provider: provider}
}

func (adapter mcpSemanticProviderAdapter) Zones() []mcp.Zone {
	if adapter.provider == nil {
		return nil
	}
	zones := adapter.provider.Zones()
	if len(zones) == 0 {
		return nil
	}
	out := make([]mcp.Zone, len(zones))
	for i, zone := range zones {
		out[i] = mcp.Zone{
			ID:   zone.ID,
			Name: zone.Name,
			State: mcp.ZoneState{
				CurrentTempC:       cloneFloatPtr(zone.State.CurrentTempC),
				CurrentHumidityPct: cloneFloatPtr(zone.State.CurrentHumidityPct),
				HvacAction:         zone.State.HvacAction,
				SpecialFunction:    zone.State.SpecialFunction,
				HeatingDemandPct:   cloneFloatPtr(zone.State.HeatingDemandPct),
				ValvePositionPct:   cloneFloatPtr(zone.State.ValvePositionPct),
			},
			Config: mcp.ZoneConfig{
				OperatingMode:     zone.Config.OperatingMode,
				Preset:            zone.Config.Preset,
				TargetTempC:       cloneFloatPtr(zone.Config.TargetTempC),
				AllowedModes:      append([]string(nil), zone.Config.AllowedModes...),
				CircuitType:       zone.Config.CircuitType,
				AssociatedCircuit: cloneIntPtr(zone.Config.AssociatedCircuit),
			},
		}
	}
	return out
}

func (adapter mcpSemanticProviderAdapter) DHW() *mcp.DhwStatus {
	if adapter.provider == nil {
		return nil
	}
	status := adapter.provider.DHW()
	if status == nil {
		return nil
	}
	return &mcp.DhwStatus{
		State: mcp.DhwState{
			CurrentTempC:     cloneFloatPtr(status.State.CurrentTempC),
			SpecialFunction:  status.State.SpecialFunction,
			HeatingDemandPct: cloneFloatPtr(status.State.HeatingDemandPct),
		},
		Config: mcp.DhwConfig{
			OperatingMode: status.Config.OperatingMode,
			Preset:        status.Config.Preset,
			TargetTempC:   cloneFloatPtr(status.Config.TargetTempC),
		},
	}
}

func (adapter mcpSemanticProviderAdapter) Circuits() []mcp.CircuitStatus {
	if adapter.provider == nil {
		return nil
	}
	circuits := adapter.provider.Circuits()
	if len(circuits) == 0 {
		return nil
	}
	out := make([]mcp.CircuitStatus, len(circuits))
	for i, circuit := range circuits {
		out[i] = mcp.CircuitStatus{
			Index:       circuit.Index,
			CircuitType: circuit.CircuitType,
			HasMixer:    circuit.HasMixer,
			State: mcp.CircuitState{
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
			Config: mcp.CircuitConfig{
				HeatingCurve:    cloneFloatPtr(circuit.Config.HeatingCurve),
				FlowTempMaxC:    cloneFloatPtr(circuit.Config.FlowTempMaxC),
				FlowTempMinC:    cloneFloatPtr(circuit.Config.FlowTempMinC),
				SummerLimitC:    cloneFloatPtr(circuit.Config.SummerLimitC),
				FrostProtC:      cloneFloatPtr(circuit.Config.FrostProtC),
				RoomTempControl: circuit.Config.RoomTempControl,
				CoolingEnabled:  cloneBoolPtr(circuit.Config.CoolingEnabled),
			},
		}
	}
	return out
}

func (adapter mcpSemanticProviderAdapter) EnergyTotals() *mcp.EnergyTotals {
	if adapter.provider == nil {
		return nil
	}
	totals := adapter.provider.EnergyTotals()
	if totals == nil {
		return nil
	}
	return &mcp.EnergyTotals{
		Gas:      mapEnergyChannel(totals.Gas),
		Electric: mapEnergyChannel(totals.Electric),
		Solar:    mapEnergyChannel(totals.Solar),
	}
}

func (adapter mcpSemanticProviderAdapter) BoilerStatus() *mcp.BoilerStatus {
	if adapter.provider == nil {
		return nil
	}
	status := adapter.provider.BoilerStatus()
	if status == nil {
		return nil
	}
	out := &mcp.BoilerStatus{
		State: &mcp.BoilerState{
			FlowTemperatureC:         cloneFloatPtr(status.State.FlowTemperatureC),
			ReturnTemperatureC:       cloneFloatPtr(status.State.ReturnTemperatureC),
			CentralHeatingPumpActive: cloneBoolPtr(status.State.CentralHeatingPumpActive),
		},
		Diagnostics: &mcp.BoilerDiagnostics{
			HeatingStatusRaw: cloneIntPtr(status.Diagnostics.HeatingStatusRaw),
		},
	}
	return out
}

func cloneBoolPtr(value *bool) *bool {
	if value == nil {
		return nil
	}
	cp := *value
	return &cp
}

func cloneStringPtr(value *string) *string {
	if value == nil {
		return nil
	}
	cp := *value
	return &cp
}

func cloneIntPtr(value *int) *int {
	if value == nil {
		return nil
	}
	cp := *value
	return &cp
}

func mapEnergyChannel(channel graphql.EnergyChannel) mcp.EnergyChannel {
	return mcp.EnergyChannel{
		DHW:     mapEnergySeries(channel.DHW),
		Climate: mapEnergySeries(channel.Climate),
	}
}

func mapEnergySeries(series graphql.EnergySeries) mcp.EnergySeries {
	out := mcp.EnergySeries{Today: series.Today}
	if len(series.Yearly) > 0 {
		out.Yearly = make([]float64, len(series.Yearly))
		copy(out.Yearly, series.Yearly)
	}
	return out
}

func cloneFloatPtr(value *float64) *float64 {
	if value == nil {
		return nil
	}
	cp := *value
	return &cp
}
