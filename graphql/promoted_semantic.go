package graphql

import "fmt"

// NewPromotedSemanticProvider composes a narrow, protocol-neutral overlay
// over the established semantic provider. Existing non-null values always win.
func NewPromotedSemanticProvider(base SemanticProvider, overlay func() PromotedSemanticOverlay) SemanticProvider {
	if base == nil || overlay == nil {
		return base
	}
	return promotedSemanticProvider{base: base, overlay: overlay}
}

type promotedSemanticProvider struct {
	base    SemanticProvider
	overlay func() PromotedSemanticOverlay
}

// ClonePromotedSemanticOverlay returns a detached snapshot suitable for
// concurrent consumer reads. The overlay contains no protocol identities.
func ClonePromotedSemanticOverlay(in PromotedSemanticOverlay) PromotedSemanticOverlay {
	out := PromotedSemanticOverlay{DHW: clonePromotedDHW(in.DHW), System: clonePromotedSystem(in.System)}
	if len(in.Zones) != 0 {
		out.Zones = make(map[string]PromotedZoneOverlay, len(in.Zones))
		for id, zone := range in.Zones {
			out.Zones[id] = clonePromotedZone(zone)
		}
	}
	return out
}

func clonePromotedZone(in PromotedZoneOverlay) PromotedZoneOverlay {
	in.CurrentTempC = copyFloat(in.CurrentTempC)
	in.TargetTempC = copyFloat(in.TargetTempC)
	in.OperatingMode = copyString(in.OperatingMode)
	in.OperationModeChangeable = copyBool(in.OperationModeChangeable)
	in.SourceLabel = copyString(in.SourceLabel)
	return in
}
func clonePromotedDHW(in PromotedDHWOverlay) PromotedDHWOverlay {
	in.CurrentTempC = copyFloat(in.CurrentTempC)
	in.TargetTempC = copyFloat(in.TargetTempC)
	in.OperatingMode = copyString(in.OperatingMode)
	in.OperationModeChangeable = copyBool(in.OperationModeChangeable)
	in.OverrunActive = copyBool(in.OverrunActive)
	return in
}
func clonePromotedSystem(in PromotedSystemOverlay) PromotedSystemOverlay {
	in.OutdoorTemperature = copyFloat(in.OutdoorTemperature)
	in.GatewayBrand = copyString(in.GatewayBrand)
	in.GatewayVendor = copyString(in.GatewayVendor)
	return in
}

func (p promotedSemanticProvider) Zones() []Zone {
	zones := p.base.Zones()
	overlay := p.overlay()
	for i := range zones {
		item, ok := overlay.Zones[zones[i].ID]
		if !ok {
			continue
		}
		if zones[i].State.CurrentTempC == nil {
			zones[i].State.CurrentTempC = copyFloat(item.CurrentTempC)
		}
		if zones[i].Config.TargetTempC == nil {
			zones[i].Config.TargetTempC = copyFloat(item.TargetTempC)
		}
		if zones[i].Config.OperatingMode == "" && item.OperatingMode != nil {
			zones[i].Config.OperatingMode = *item.OperatingMode
		}
		if zones[i].Config.OperationModeChangeable == nil {
			zones[i].Config.OperationModeChangeable = copyBool(item.OperationModeChangeable)
		}
		if zones[i].Config.SourceLabel == nil {
			zones[i].Config.SourceLabel = copyString(item.SourceLabel)
		}
	}
	return zones
}

func (p promotedSemanticProvider) DHW() *DhwStatus {
	status := p.base.DHW()
	if status == nil {
		status = &DhwStatus{}
	}
	o := p.overlay().DHW
	if status.State.CurrentTempC == nil {
		status.State.CurrentTempC = copyFloat(o.CurrentTempC)
	}
	if status.Config.TargetTempC == nil {
		status.Config.TargetTempC = copyFloat(o.TargetTempC)
	}
	if status.Config.OperatingMode == "" && o.OperatingMode != nil {
		status.Config.OperatingMode = *o.OperatingMode
	}
	if status.Config.OperationModeChangeable == nil {
		status.Config.OperationModeChangeable = copyBool(o.OperationModeChangeable)
	}
	if status.State.OverrunActive == nil {
		status.State.OverrunActive = copyBool(o.OverrunActive)
	}
	if status.State.CurrentTempC == nil && status.Config.TargetTempC == nil && status.Config.OperatingMode == "" && status.Config.OperationModeChangeable == nil && status.State.OverrunActive == nil {
		return p.base.DHW()
	}
	return status
}

func (p promotedSemanticProvider) System() *SystemStatus {
	status := p.base.System()
	if status == nil {
		status = &SystemStatus{}
	}
	o := p.overlay().System
	if status.State.OutdoorTemperature == nil {
		status.State.OutdoorTemperature = copyFloat(o.OutdoorTemperature)
	}
	if status.GatewayBrand == nil {
		status.GatewayBrand = copyString(o.GatewayBrand)
	}
	if status.GatewayVendor == nil {
		status.GatewayVendor = copyString(o.GatewayVendor)
	}
	if status.State.OutdoorTemperature == nil && status.GatewayBrand == nil && status.GatewayVendor == nil {
		return p.base.System()
	}
	return status
}

func (p promotedSemanticProvider) Circuits() []CircuitStatus        { return p.base.Circuits() }
func (p promotedSemanticProvider) RadioDevices() []RadioDevice      { return p.base.RadioDevices() }
func (p promotedSemanticProvider) FM5SemanticMode() Fm5SemanticMode { return p.base.FM5SemanticMode() }
func (p promotedSemanticProvider) Solar() *SolarStatus              { return p.base.Solar() }
func (p promotedSemanticProvider) Cylinders() []CylinderStatus      { return p.base.Cylinders() }
func (p promotedSemanticProvider) EnergyTotals() *EnergyTotals      { return p.base.EnergyTotals() }
func (p promotedSemanticProvider) BoilerStatus() *BoilerStatus      { return p.base.BoilerStatus() }
func (p promotedSemanticProvider) Schedules() *ScheduleStatus       { return p.base.Schedules() }
func (p promotedSemanticProvider) AdapterHardwareInfo() *AdapterHardwareInfo {
	return p.base.AdapterHardwareInfo()
}

// SemanticRegistryState remains an inventory of the owner-materialized eBUS
// registry. The promoted overlay is a public projection and is intentionally
// excluded from the owner-local M8 source-state surface.
func (p promotedSemanticProvider) SemanticRegistryState() (SemanticRegistrySnapshot, error) {
	owner, ok := p.base.(interface {
		SemanticRegistryState() (SemanticRegistrySnapshot, error)
	})
	if !ok {
		return SemanticRegistrySnapshot{}, fmt.Errorf("base semantic provider does not expose owner-local registry state")
	}
	return owner.SemanticRegistryState()
}

func copyFloat(value *float64) *float64 {
	if value == nil {
		return nil
	}
	copied := *value
	return &copied
}
func copyBool(value *bool) *bool {
	if value == nil {
		return nil
	}
	copied := *value
	return &copied
}
func copyString(value *string) *string {
	if value == nil {
		return nil
	}
	copied := *value
	return &copied
}
