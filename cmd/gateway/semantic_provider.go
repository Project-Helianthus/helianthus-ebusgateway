package main

import (
	"github.com/d3vi1/helianthus-ebusgateway/graphql"
	"github.com/d3vi1/helianthus-ebusgateway/mcp"
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
			ID:            zone.ID,
			Name:          zone.Name,
			OperatingMode: zone.OperatingMode,
			Preset:        zone.Preset,
			CurrentTempC:  cloneFloatPtr(zone.CurrentTempC),
			TargetTempC:   cloneFloatPtr(zone.TargetTempC),
			HeatingDemand: cloneFloatPtr(zone.HeatingDemand),
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
		OperatingMode: status.OperatingMode,
		Preset:        status.Preset,
		CurrentTempC:  cloneFloatPtr(status.CurrentTempC),
		TargetTempC:   cloneFloatPtr(status.TargetTempC),
		HeatingDemand: cloneFloatPtr(status.HeatingDemand),
	}
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
	copy := *value
	return &copy
}
