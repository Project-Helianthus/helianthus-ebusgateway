package main

import (
	"context"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
	"github.com/Project-Helianthus/helianthus-ebusreg/vaillant/productids"
)

// refreshBoilerAddress applies the registry's boiler-class heat source without
// a root discovery pass; it reads the registry only and sends nothing. While
// the fallback discovery is suspended it keeps a heat source identified after
// ABSENT on the state and configuration ticks.
func (p *vaillantSemanticPoller) refreshBoilerAddress(ctx context.Context) {
	boilerAddress := p.findBoilerAddress()
	p.mu.Lock()
	prevBoilerAddress := p.boilerAddress
	p.boilerAddress = boilerAddress
	p.mu.Unlock()
	p.refreshHeatSourcePathReason()
	if boilerAddress != prevBoilerAddress && boilerAddress != 0 && boilerAddress != 0x08 {
		log.Printf("semantic_boiler_discovery address=0x%02x source=nonstandard", boilerAddress)
	}
	if boilerAddress != prevBoilerAddress && boilerAddress != 0 {
		p.enqueueBoilerStatusPriming(ctx)
	}
}

type vaillantBoilerSnapshot struct {
	FlowTemperatureC         *float64
	ReturnTemperatureC       *float64
	CentralHeatingPumpActive *bool
	WaterPressureBar         *float64
	ExternalPumpActive       *bool
	CirculationPumpActive    *bool
	GasValveActive           *bool
	FlameActive              *bool
	DiverterValvePositionPct *float64
	FanSpeedRpm              *int
	TargetFanSpeedRpm        *int
	IonisationVoltageUa      *float64
	DhwWaterFlowLpm          *float64
	DhwDemandActive          *bool
	HeatingSwitchActive      *bool
	StorageLoadPumpPct       *float64
	ModulationPct            *float64
	PrimaryCircuitFlowLpm    *float64
	FlowTempDesiredC         *float64
	DhwTempDesiredC          *float64
	StateNumber              *int
	DhwTemperatureC          *float64
	DhwTargetTemperatureC    *float64
	DhwOperatingMode         *int
	FlowsetHcMaxC            *float64
	FlowsetHwcMaxC           *float64
	PartloadHcKW             *float64
	PartloadHwcKW            *float64
	HeatingStatusRaw         *int
	DhwStatusRaw             *int
	CentralHeatingHours      *float64
	DhwHours                 *float64
	CentralHeatingStarts     *int
	DhwStarts                *int
	PumpHours                *float64
	FanHours                 *float64
	DeactivationsIFC         *int
	DeactivationsTemplimiter *int
	InstallerMenuCode        *int
	PhoneNumber              *string
	HoursTillService         *int
}

type boilerStatusField uint8

// Circuit registers (GG=0x02) are not boiler fields: circuit 0's pump status
// (RR=0x001B) and circuit state (RR=0x001E) are published only on the circuit
// surface, so no boiler status field is defined for them.
const (
	boilerStatusFieldFlowTemperature boilerStatusField = iota
)

type boilerStatusRegisterDecoder uint8

const (
	boilerStatusRegisterDecoderFloat32 boilerStatusRegisterDecoder = iota
)

type boilerStatusRegisterDefinition struct {
	field    boilerStatusField
	decoder  boilerStatusRegisterDecoder
	opcode   byte
	group    byte
	instance byte
	addr     uint16
}

// findBoilerAddress is retained for potential future use (e.g. broadcast sniffing).
// Currently unused — boiler data is read via B524 registers on the controller.
func boilerStatusRegisterDefinitionsForTier(tier boilerStatusTier) []boilerStatusRegisterDefinition {
	switch tier {
	case boilerStatusTierFast:
		return []boilerStatusRegisterDefinition{
			{
				field:    boilerStatusFieldFlowTemperature,
				decoder:  boilerStatusRegisterDecoderFloat32,
				opcode:   localRegulator.opcode,
				group:    localRegulator.group,
				instance: regulatorInstance,
				addr:     system_flow_temperature,
			},
		}
	case boilerStatusTierMedium, boilerStatusTierSlow:
		// Reduced profile: scaffolding only. Additional mappings are added once
		// authoritative B524 semantics are promoted for boiler_status.
		return nil
	default:
		return nil
	}
}

// refreshBoilerStatus reads boiler operational data via B524 registers on the controller.
// The BAI boiler does not respond to direct B504 reads from third-party sources — it only
// accepts requests from its paired controller. Instead, the controller (VRC/BASV2) mirrors
// boiler data in its own B524 register space, which we can read reliably.
func (p *vaillantSemanticPoller) refreshBoilerStatus(ctx context.Context) {
	p.refreshBoilerStatusTier(ctx, boilerStatusTierFast)
}

func (p *vaillantSemanticPoller) refreshBoilerStatusTier(ctx context.Context, tier boilerStatusTier) {
	if p == nil {
		return
	}

	p.mu.Lock()
	controller := p.controller
	boilerAddress := p.boilerAddress
	p.mu.Unlock()
	if controller == 0 && boilerAddress == 0 {
		if p.mergePublishedBoilerFlow() {
			p.publishBoilerStatus(semanticSnapshotSourceLive)
		}
		return
	}

	snapshot := &vaillantBoilerSnapshot{}
	updated := false
	if controller != 0 {
		updated = p.refreshBoilerStatusB524(ctx, tier, snapshot) || updated
	}
	if boilerAddress != 0 {
		updated = p.refreshBoilerStatusB509(ctx, boilerAddress, tier, snapshot) || updated
	}
	dhwUpdated := false
	if tier == boilerStatusTierFast {
		dhwUpdated = p.mergeBoilerDHWFieldsFromSnapshot(snapshot)
	}

	// Preserve last-known values on transient/partial failures.
	if !updated {
		if dhwUpdated {
			p.mu.Lock()
			p.boiler = mergeBoilerSnapshotNonDestructive(p.boiler, snapshot)
			p.mu.Unlock()
		}
		if p.mergePublishedBoilerFlow() {
			p.publishBoilerStatus(semanticSnapshotSourceLive)
		}
		return
	}

	p.mu.Lock()
	p.boiler = mergeBoilerSnapshotNonDestructive(p.boiler, snapshot)
	p.mu.Unlock()

	p.publishBoilerStatus(semanticSnapshotSourceLive)
}

func (p *vaillantSemanticPoller) refreshBoilerStatusB524(ctx context.Context, tier boilerStatusTier, snapshot *vaillantBoilerSnapshot) bool {
	if p == nil || snapshot == nil {
		return false
	}

	if tier == boilerStatusTierFast {
		return p.mergeBoilerMirrorFieldsFromSnapshots(snapshot)
	}

	updated := false
	registers := boilerStatusRegisterDefinitionsForTier(tier)
	for _, register := range registers {
		switch register.decoder {
		case boilerStatusRegisterDecoderFloat32:
			value, ok := p.readB524Float32LE(ctx, register.opcode, register.group, register.instance, register.addr)
			if !ok {
				continue
			}
			updated = true
			switch register.field {
			case boilerStatusFieldFlowTemperature:
				// Controller flow is not qualified boiler flow.
				_ = value
			}
		}
	}

	return updated
}

func (p *vaillantSemanticPoller) mergeBoilerMirrorFieldsFromSnapshots(snapshot *vaillantBoilerSnapshot) bool {
	if p == nil || snapshot == nil {
		return false
	}

	p.mu.Lock()
	controller := p.controller
	system := cloneSystemSnapshot(p.system)
	p.mu.Unlock()
	if controller == 0 {
		return false
	}

	now := p.now()
	maxAge := p.boilerMirrorMaxAge()
	// Fresh controller flow triggers publication without supplying boiler flow.
	updated := system != nil && system.Controller == controller && system.SystemFlowTemperature != nil && semanticSnapshotObservedWithin(system.SystemFlowTemperatureLiveAt, now, maxAge)
	// Circuit 0 is not a boiler source. Its pump status and circuit state stay
	// on the circuit surface; the boiler pump and heating-status diagnostic
	// remain absent here, so partial merges preserve independent boiler evidence.
	if updated {
		p.mergeBoilerDHWFieldsFromSnapshot(snapshot)
	}
	return updated
}

func (p *vaillantSemanticPoller) boilerMirrorMaxAge() time.Duration {
	interval := p.configInterval
	if interval <= 0 {
		interval = ebusgateway.DefaultConfig().SemanticConfigInterval
	}
	fastInterval := p.boilerFastInterval
	if fastInterval <= 0 {
		fastInterval = 30 * time.Second
	}
	return interval + fastInterval
}

func semanticSnapshotObservedWithin(observedAt, now time.Time, maxAge time.Duration) bool {
	if observedAt.IsZero() {
		return false
	}
	if maxAge <= 0 {
		return true
	}
	if now.Before(observedAt) {
		return true
	}
	return now.Sub(observedAt) <= maxAge
}

func (p *vaillantSemanticPoller) mergeBoilerDHWFieldsFromSnapshot(snapshot *vaillantBoilerSnapshot) bool {
	if p == nil || snapshot == nil {
		return false
	}

	p.mu.Lock()
	dhw := p.dhw
	if dhw == nil {
		p.mu.Unlock()
		return false
	}
	current := cloneFloat64Ptr(dhw.CurrentTempC)
	target := cloneFloat64Ptr(dhw.TargetTempC)
	operationMode := strings.TrimSpace(dhw.ConfigurationDHWOperationMode)
	p.mu.Unlock()

	updated := false
	if current != nil {
		snapshot.DhwTemperatureC = current
		updated = true
	}
	if target != nil {
		snapshot.DhwTargetTemperatureC = target
		updated = true
	}
	if operationMode != "" {
		if parsed, err := strconv.Atoi(operationMode); err == nil {
			snapshot.DhwOperatingMode = &parsed
			updated = true
		}
	}
	return updated
}

// refreshBoilerStatusB509 reads the B509 registers of one boiler status tier
// into snapshot and reports whether any read succeeded.
func (p *vaillantSemanticPoller) refreshBoilerStatusB509(ctx context.Context, boilerAddress byte, tier boilerStatusTier, snapshot *vaillantBoilerSnapshot) bool {
	if p == nil || snapshot == nil || boilerAddress == 0 {
		return false
	}

	switch tier {
	case boilerStatusTierFast:
		return p.refreshBoilerStatusB509Fast(ctx, boilerAddress, snapshot)
	case boilerStatusTierMedium:
		return p.refreshBoilerStatusB509Medium(ctx, boilerAddress, snapshot)
	case boilerStatusTierSlow:
		return p.refreshBoilerStatusB509Slow(ctx, boilerAddress, snapshot)
	}
	return false
}

// refreshBoilerStatusB509Fast reads the fast tier: flow evidence, pumps,
// flame, gas valve, fan, modulation, state and hot water demand.
func (p *vaillantSemanticPoller) refreshBoilerStatusB509Fast(ctx context.Context, boilerAddress byte, snapshot *vaillantBoilerSnapshot) bool {
	// Acquisition retains native evidence; only the hub result supplies flow.
	_, updated := p.readB509Value(ctx, boilerAddress, boiler_b509_flow_temperature)
	if value := p.readB509OnOff(ctx, boilerAddress, boiler_b509_central_heating_pump); value != nil {
		snapshot.CentralHeatingPumpActive = value
		updated = true
	}
	if value := p.readB509DATA2b(ctx, boilerAddress, boiler_b509_water_pressure); value != nil {
		snapshot.WaterPressureBar = value
		updated = true
	}
	if value := p.readB509OnOff(ctx, boilerAddress, boiler_b509_flame_active); value != nil {
		snapshot.FlameActive = value
		updated = true
	}
	if value := p.readB509OnOff(ctx, boilerAddress, boiler_b509_gas_valve_active); value != nil {
		snapshot.GasValveActive = value
		updated = true
	}
	if value := p.readB509UINInt(ctx, boilerAddress, boiler_b509_fan_speed_rpm); value != nil {
		snapshot.FanSpeedRpm = value
		updated = true
	}
	if value := p.readB509SINScaled(ctx, boilerAddress, boiler_b509_modulation_pct, 10); value != nil {
		snapshot.ModulationPct = value
		updated = true
	}
	if value := p.readB509UCHInt(ctx, boilerAddress, boiler_b509_state_number); value != nil {
		snapshot.StateNumber = value
		updated = true
	}
	if value := p.readB509UCHFloat(ctx, boilerAddress, boiler_b509_diverter_valve_position); value != nil {
		snapshot.DiverterValvePositionPct = value
		updated = true
	}
	if value := p.readB509OnOff(ctx, boilerAddress, boiler_b509_dhw_demand_active); value != nil {
		snapshot.DhwDemandActive = value
		updated = true
	}
	if value := p.readB509UIN100(ctx, boilerAddress, boiler_b509_dhw_water_flow_lpm); value != nil {
		snapshot.DhwWaterFlowLpm = value
		updated = true
	}
	return updated
}

// refreshBoilerStatusB509Medium reads the medium tier: configured maxima
// and partial loads, desired temperatures, auxiliary pumps and flows.
func (p *vaillantSemanticPoller) refreshBoilerStatusB509Medium(ctx context.Context, boilerAddress byte, snapshot *vaillantBoilerSnapshot) bool {
	updated := false
	if value := p.readB509BoilerConfigFloat(ctx, boilerAddress, "maximum_heating_flow_temperature_setpoint"); value != nil {
		snapshot.FlowsetHcMaxC = value
		updated = true
	}
	if value := p.readB509BoilerConfigFloat(ctx, boilerAddress, "maximum_dhw_flow_temperature_setpoint"); value != nil {
		snapshot.FlowsetHwcMaxC = value
		updated = true
	}
	if value := p.readB509BoilerConfigFloat(ctx, boilerAddress, "heating_partial_load_power"); value != nil {
		snapshot.PartloadHcKW = value
		updated = true
	}
	if value := p.readB509BoilerConfigFloat(ctx, boilerAddress, "dhw_partial_load_power"); value != nil {
		snapshot.PartloadHwcKW = value
		updated = true
	}
	if value := p.readB509Percent0(ctx, boilerAddress, boiler_b509_storage_load_pump_pct); value != nil {
		snapshot.StorageLoadPumpPct = value
		updated = true
	}
	if value := p.readB509DATA2c(ctx, boilerAddress, boiler_b509_flow_temp_desired_c); value != nil {
		snapshot.FlowTempDesiredC = value
		updated = true
	}
	if value := p.readB509DATA2c(ctx, boilerAddress, boiler_b509_dhw_temp_desired_c); value != nil {
		snapshot.DhwTempDesiredC = value
		updated = true
	}
	if value := p.readB509OnOff(ctx, boilerAddress, boiler_b509_circulation_pump_active); value != nil {
		snapshot.CirculationPumpActive = value
		updated = true
	}
	if value := p.readB509OnOff(ctx, boilerAddress, boiler_b509_external_pump_active); value != nil {
		snapshot.ExternalPumpActive = value
		updated = true
	}
	if value := p.readB509OnOff(ctx, boilerAddress, boiler_b509_heating_switch_active); value != nil {
		snapshot.HeatingSwitchActive = value
		updated = true
	}
	if value := p.readB509UINInt(ctx, boilerAddress, boiler_b509_target_fan_speed_rpm); value != nil {
		snapshot.TargetFanSpeedRpm = value
		updated = true
	}
	if value := p.readB509SINScaled(ctx, boilerAddress, boiler_b509_ionisation_voltage_ua, 10); value != nil {
		snapshot.IonisationVoltageUa = value
		updated = true
	}
	if value := p.readB509UIN100(ctx, boilerAddress, boiler_b509_primary_circuit_flow_lpm); value != nil {
		snapshot.PrimaryCircuitFlowLpm = value
		updated = true
	}
	return updated
}

// refreshBoilerStatusB509Slow reads the slow tier: operating counters and
// the installer and maintenance configuration.
func (p *vaillantSemanticPoller) refreshBoilerStatusB509Slow(ctx context.Context, boilerAddress byte, snapshot *vaillantBoilerSnapshot) bool {
	updated := false
	if value := p.readB509Hoursum2(ctx, boilerAddress, boiler_b509_central_heating_hours); value != nil {
		snapshot.CentralHeatingHours = value
		updated = true
	}
	if value := p.readB509Hoursum2(ctx, boilerAddress, boiler_b509_dhw_hours); value != nil {
		snapshot.DhwHours = value
		updated = true
	}
	if value := p.readB509UINInt(ctx, boilerAddress, boiler_b509_central_heating_starts); value != nil {
		snapshot.CentralHeatingStarts = value
		updated = true
	}
	if value := p.readB509UINInt(ctx, boilerAddress, boiler_b509_dhw_starts); value != nil {
		snapshot.DhwStarts = value
		updated = true
	}
	if value := p.readB509Hoursum2(ctx, boilerAddress, boiler_b509_pump_hours); value != nil {
		snapshot.PumpHours = value
		updated = true
	}
	if value := p.readB509Hoursum2(ctx, boilerAddress, boiler_b509_fan_hours); value != nil {
		snapshot.FanHours = value
		updated = true
	}
	if value := p.readB509UCHInt(ctx, boilerAddress, boiler_b509_deactivations_ifc); value != nil {
		snapshot.DeactivationsIFC = value
		updated = true
	}
	if value := p.readB509UCHInt(ctx, boilerAddress, boiler_b509_deactivations_limit); value != nil {
		snapshot.DeactivationsTemplimiter = value
		updated = true
	}
	// Installer/maintenance config.
	if value := p.readB509UCHInt(ctx, boilerAddress, boiler_b509_installer_menu_code); value != nil {
		snapshot.InstallerMenuCode = value
		updated = true
	}
	if value := p.readB509PhoneBCD(ctx, boilerAddress, boiler_b509_phone_number); value != nil {
		snapshot.PhoneNumber = value
		updated = true
	}
	if value := p.readB509Hoursum2Int(ctx, boilerAddress, boiler_b509_hours_till_service); value != nil {
		snapshot.HoursTillService = value
		updated = true
	}
	return updated
}

func mergeBoilerSnapshotNonDestructive(existing, incoming *vaillantBoilerSnapshot) *vaillantBoilerSnapshot {
	merged := cloneBoilerSnapshot(existing)
	if merged == nil {
		merged = &vaillantBoilerSnapshot{}
	}
	if incoming != nil {
		if incoming.FlowTemperatureC != nil {
			merged.FlowTemperatureC = cloneFloat64Ptr(incoming.FlowTemperatureC)
		}
		if incoming.CentralHeatingPumpActive != nil {
			merged.CentralHeatingPumpActive = cloneBoilerBoolPtr(incoming.CentralHeatingPumpActive)
		}
		if incoming.WaterPressureBar != nil {
			merged.WaterPressureBar = cloneFloat64Ptr(incoming.WaterPressureBar)
		}
		if incoming.ExternalPumpActive != nil {
			merged.ExternalPumpActive = cloneBoilerBoolPtr(incoming.ExternalPumpActive)
		}
		if incoming.CirculationPumpActive != nil {
			merged.CirculationPumpActive = cloneBoilerBoolPtr(incoming.CirculationPumpActive)
		}
		if incoming.GasValveActive != nil {
			merged.GasValveActive = cloneBoilerBoolPtr(incoming.GasValveActive)
		}
		if incoming.FlameActive != nil {
			merged.FlameActive = cloneBoilerBoolPtr(incoming.FlameActive)
		}
		if incoming.DiverterValvePositionPct != nil {
			merged.DiverterValvePositionPct = cloneFloat64Ptr(incoming.DiverterValvePositionPct)
		}
		if incoming.FanSpeedRpm != nil {
			merged.FanSpeedRpm = cloneBoilerIntPtr(incoming.FanSpeedRpm)
		}
		if incoming.TargetFanSpeedRpm != nil {
			merged.TargetFanSpeedRpm = cloneBoilerIntPtr(incoming.TargetFanSpeedRpm)
		}
		if incoming.IonisationVoltageUa != nil {
			merged.IonisationVoltageUa = cloneFloat64Ptr(incoming.IonisationVoltageUa)
		}
		if incoming.DhwWaterFlowLpm != nil {
			merged.DhwWaterFlowLpm = cloneFloat64Ptr(incoming.DhwWaterFlowLpm)
		}
		if incoming.DhwDemandActive != nil {
			merged.DhwDemandActive = cloneBoilerBoolPtr(incoming.DhwDemandActive)
		}
		if incoming.HeatingSwitchActive != nil {
			merged.HeatingSwitchActive = cloneBoilerBoolPtr(incoming.HeatingSwitchActive)
		}
		if incoming.StorageLoadPumpPct != nil {
			merged.StorageLoadPumpPct = cloneFloat64Ptr(incoming.StorageLoadPumpPct)
		}
		if incoming.ModulationPct != nil {
			merged.ModulationPct = cloneFloat64Ptr(incoming.ModulationPct)
		}
		if incoming.PrimaryCircuitFlowLpm != nil {
			merged.PrimaryCircuitFlowLpm = cloneFloat64Ptr(incoming.PrimaryCircuitFlowLpm)
		}
		if incoming.FlowTempDesiredC != nil {
			merged.FlowTempDesiredC = cloneFloat64Ptr(incoming.FlowTempDesiredC)
		}
		if incoming.DhwTempDesiredC != nil {
			merged.DhwTempDesiredC = cloneFloat64Ptr(incoming.DhwTempDesiredC)
		}
		if incoming.StateNumber != nil {
			merged.StateNumber = cloneBoilerIntPtr(incoming.StateNumber)
		}
		if incoming.DhwTemperatureC != nil {
			merged.DhwTemperatureC = cloneFloat64Ptr(incoming.DhwTemperatureC)
		}
		if incoming.DhwTargetTemperatureC != nil {
			merged.DhwTargetTemperatureC = cloneFloat64Ptr(incoming.DhwTargetTemperatureC)
		}
		if incoming.DhwOperatingMode != nil {
			merged.DhwOperatingMode = cloneBoilerIntPtr(incoming.DhwOperatingMode)
		}
		if incoming.FlowsetHcMaxC != nil {
			merged.FlowsetHcMaxC = cloneFloat64Ptr(incoming.FlowsetHcMaxC)
		}
		if incoming.FlowsetHwcMaxC != nil {
			merged.FlowsetHwcMaxC = cloneFloat64Ptr(incoming.FlowsetHwcMaxC)
		}
		if incoming.PartloadHcKW != nil {
			merged.PartloadHcKW = cloneFloat64Ptr(incoming.PartloadHcKW)
		}
		if incoming.PartloadHwcKW != nil {
			merged.PartloadHwcKW = cloneFloat64Ptr(incoming.PartloadHwcKW)
		}
		if incoming.HeatingStatusRaw != nil {
			merged.HeatingStatusRaw = cloneBoilerIntPtr(incoming.HeatingStatusRaw)
		}
		if incoming.DhwStatusRaw != nil {
			merged.DhwStatusRaw = cloneBoilerIntPtr(incoming.DhwStatusRaw)
		}
		if incoming.CentralHeatingHours != nil {
			merged.CentralHeatingHours = cloneFloat64Ptr(incoming.CentralHeatingHours)
		}
		if incoming.DhwHours != nil {
			merged.DhwHours = cloneFloat64Ptr(incoming.DhwHours)
		}
		if incoming.CentralHeatingStarts != nil {
			merged.CentralHeatingStarts = cloneBoilerIntPtr(incoming.CentralHeatingStarts)
		}
		if incoming.DhwStarts != nil {
			merged.DhwStarts = cloneBoilerIntPtr(incoming.DhwStarts)
		}
		if incoming.PumpHours != nil {
			merged.PumpHours = cloneFloat64Ptr(incoming.PumpHours)
		}
		if incoming.FanHours != nil {
			merged.FanHours = cloneFloat64Ptr(incoming.FanHours)
		}
		if incoming.DeactivationsIFC != nil {
			merged.DeactivationsIFC = cloneBoilerIntPtr(incoming.DeactivationsIFC)
		}
		if incoming.DeactivationsTemplimiter != nil {
			merged.DeactivationsTemplimiter = cloneBoilerIntPtr(incoming.DeactivationsTemplimiter)
		}
		if incoming.InstallerMenuCode != nil {
			merged.InstallerMenuCode = cloneBoilerIntPtr(incoming.InstallerMenuCode)
		}
		if incoming.PhoneNumber != nil {
			merged.PhoneNumber = cloneStringPtr(incoming.PhoneNumber)
		}
		if incoming.HoursTillService != nil {
			merged.HoursTillService = cloneBoilerIntPtr(incoming.HoursTillService)
		}
	}

	// Closed decision: do not map GG=0x02 RR=0x0008 as boiler return temperature.
	// Keep return temperature unset in B524-derived snapshots.
	merged.ReturnTemperatureC = nil
	return merged
}

func cloneBoilerSnapshot(snapshot *vaillantBoilerSnapshot) *vaillantBoilerSnapshot {
	if snapshot == nil {
		return nil
	}
	return &vaillantBoilerSnapshot{
		FlowTemperatureC:         cloneFloat64Ptr(snapshot.FlowTemperatureC),
		ReturnTemperatureC:       cloneFloat64Ptr(snapshot.ReturnTemperatureC),
		CentralHeatingPumpActive: cloneBoilerBoolPtr(snapshot.CentralHeatingPumpActive),
		WaterPressureBar:         cloneFloat64Ptr(snapshot.WaterPressureBar),
		ExternalPumpActive:       cloneBoilerBoolPtr(snapshot.ExternalPumpActive),
		CirculationPumpActive:    cloneBoilerBoolPtr(snapshot.CirculationPumpActive),
		GasValveActive:           cloneBoilerBoolPtr(snapshot.GasValveActive),
		FlameActive:              cloneBoilerBoolPtr(snapshot.FlameActive),
		DiverterValvePositionPct: cloneFloat64Ptr(snapshot.DiverterValvePositionPct),
		FanSpeedRpm:              cloneBoilerIntPtr(snapshot.FanSpeedRpm),
		TargetFanSpeedRpm:        cloneBoilerIntPtr(snapshot.TargetFanSpeedRpm),
		IonisationVoltageUa:      cloneFloat64Ptr(snapshot.IonisationVoltageUa),
		DhwWaterFlowLpm:          cloneFloat64Ptr(snapshot.DhwWaterFlowLpm),
		DhwDemandActive:          cloneBoilerBoolPtr(snapshot.DhwDemandActive),
		HeatingSwitchActive:      cloneBoilerBoolPtr(snapshot.HeatingSwitchActive),
		StorageLoadPumpPct:       cloneFloat64Ptr(snapshot.StorageLoadPumpPct),
		ModulationPct:            cloneFloat64Ptr(snapshot.ModulationPct),
		PrimaryCircuitFlowLpm:    cloneFloat64Ptr(snapshot.PrimaryCircuitFlowLpm),
		FlowTempDesiredC:         cloneFloat64Ptr(snapshot.FlowTempDesiredC),
		DhwTempDesiredC:          cloneFloat64Ptr(snapshot.DhwTempDesiredC),
		StateNumber:              cloneBoilerIntPtr(snapshot.StateNumber),
		DhwTemperatureC:          cloneFloat64Ptr(snapshot.DhwTemperatureC),
		DhwTargetTemperatureC:    cloneFloat64Ptr(snapshot.DhwTargetTemperatureC),
		DhwOperatingMode:         cloneBoilerIntPtr(snapshot.DhwOperatingMode),
		FlowsetHcMaxC:            cloneFloat64Ptr(snapshot.FlowsetHcMaxC),
		FlowsetHwcMaxC:           cloneFloat64Ptr(snapshot.FlowsetHwcMaxC),
		PartloadHcKW:             cloneFloat64Ptr(snapshot.PartloadHcKW),
		PartloadHwcKW:            cloneFloat64Ptr(snapshot.PartloadHwcKW),
		HeatingStatusRaw:         cloneBoilerIntPtr(snapshot.HeatingStatusRaw),
		DhwStatusRaw:             cloneBoilerIntPtr(snapshot.DhwStatusRaw),
		CentralHeatingHours:      cloneFloat64Ptr(snapshot.CentralHeatingHours),
		DhwHours:                 cloneFloat64Ptr(snapshot.DhwHours),
		CentralHeatingStarts:     cloneBoilerIntPtr(snapshot.CentralHeatingStarts),
		DhwStarts:                cloneBoilerIntPtr(snapshot.DhwStarts),
		PumpHours:                cloneFloat64Ptr(snapshot.PumpHours),
		FanHours:                 cloneFloat64Ptr(snapshot.FanHours),
		DeactivationsIFC:         cloneBoilerIntPtr(snapshot.DeactivationsIFC),
		DeactivationsTemplimiter: cloneBoilerIntPtr(snapshot.DeactivationsTemplimiter),
		InstallerMenuCode:        cloneBoilerIntPtr(snapshot.InstallerMenuCode),
		PhoneNumber:              cloneStringPtr(snapshot.PhoneNumber),
		HoursTillService:         cloneBoilerIntPtr(snapshot.HoursTillService),
	}
}

func cloneBoilerBoolPtr(value *bool) *bool {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func cloneBoilerIntPtr(value *int) *int {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func cloneBoilerModeIntPtr(mode *string) *int {
	if mode == nil {
		return nil
	}
	switch strings.ToUpper(strings.TrimSpace(*mode)) {
	case "OFF":
		value := 0
		return &value
	case "ON":
		value := 1
		return &value
	case "AUTO":
		value := 2
		return &value
	default:
		return nil
	}
}

func (p *vaillantSemanticPoller) findBoilerAddress() byte {
	if p == nil || p.reg == nil {
		return 0
	}

	// Phase C M-C6b: select the routing target byte for B5.09
	// boiler reads/writes. An aliased canonical pair (e.g. BAI
	// 0x03↔0x08) whose display is the initiator side (0x03) must
	// resolve to 0x08 here, otherwise refreshBoilerStatusB509
	// builds frames with Target: 0x03 and the read/write fails.
	// TargetAddressForRouting prefers AddressByRole(SlotRoleSlave)
	// and falls back to PrimaryDisplayAddress when no target-role
	// face exists.
	// P9.3 — race-free boiler-candidate enumeration via
	// IterateSnapshots + isBoilerDeviceSnapshotCandidate +
	// SnapshotTargetAddressForRouting.
	selected := byte(0)
	p.reg.IterateSnapshots(func(snap registry.DeviceEntrySnapshot) bool {
		if !isBoilerDeviceSnapshotCandidate(snap) {
			return true
		}
		addr := ebusgateway.SnapshotTargetAddressForRouting(snap)
		if addr == 0x08 {
			selected = addr
			return false
		}
		if selected == 0 || addr < selected {
			selected = addr
		}
		return true
	})
	return selected
}

// isBoilerDeviceSnapshotCandidate is the snapshot-based counterpart of
// isBoilerDeviceCandidate (P9.3). Reads the snapshot's DeviceID
// (race-free) and applies the same prefix-based BAI/BMU classification.
func isBoilerDeviceSnapshotCandidate(snap registry.DeviceEntrySnapshot) bool {
	normalizedID := normalizeDeviceID(snap.DeviceID)
	if strings.HasPrefix(normalizedID, "BAI") {
		return true
	}
	return strings.HasPrefix(normalizedID, "BMU")
}

// heatPumpDeviceIDFamilies are the identification prefixes that the public
// device-class table lists for heat-pump-class devices. A prefix is an
// identity hint only; it authorizes no register read or decoding.
var heatPumpDeviceIDFamilies = []string{"EHP", "HMU", "VWL", "VWS", "VWZ"}

// isHeatPumpDeviceSnapshotCandidate reports whether snap carries a
// heat-pump-class identity: a heat-pump device-ID family or a Vaillant serial
// whose part number the product catalog classifies with role "Heat Pump" in
// every row. A boiler-class identity is never heat-pump class, and an
// identity that is neither is unknown, not heat-pump class.
func isHeatPumpDeviceSnapshotCandidate(snap registry.DeviceEntrySnapshot, catalog productids.Catalog, catalogErr error) bool {
	if isBoilerDeviceSnapshotCandidate(snap) {
		return false
	}
	normalizedID := normalizeDeviceID(snap.DeviceID)
	for _, family := range heatPumpDeviceIDFamilies {
		if strings.HasPrefix(normalizedID, family) {
			return true
		}
	}
	if catalogErr != nil || !strings.EqualFold(snap.Manufacturer, "Vaillant") {
		return false
	}
	partNumber := strings.TrimSpace(extractPartNumberFromSerial(snap.SerialNumber))
	if partNumber == "" {
		return false
	}
	found := false
	for _, record := range catalog.All {
		if strings.TrimSpace(record.PartNumber) != partNumber {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(record.Role), "Heat Pump") {
			return false
		}
		found = true
	}
	return found
}

// findHeatPumpClassAddress returns the routing address of the lowest-addressed
// heat-pump-class identity in the registry, or 0 when there is none.
func (p *vaillantSemanticPoller) findHeatPumpClassAddress() byte {
	if p == nil || p.reg == nil {
		return 0
	}
	selected := byte(0)
	p.reg.IterateSnapshots(func(snap registry.DeviceEntrySnapshot) bool {
		if !isHeatPumpDeviceSnapshotCandidate(snap, p.catalog, p.catalogErr) {
			return true
		}
		if addr := ebusgateway.SnapshotTargetAddressForRouting(snap); addr != 0 && (selected == 0 || addr < selected) {
			selected = addr
		}
		return true
	})
	return selected
}

// refreshHeatSourcePathReason sets the startup reason
// no_supported_heat_source_path while the poller has no supported heat-source
// data path: no B524 root, no boiler-class heat source, and a heat-pump-class
// identity in the inventory. It clears the reason as soon as a root or a
// boiler-class heat source exists. A process that has discovered a root once,
// or an inventory the catalog classifies as holding a controller, keeps the
// reason clear: there the missing root is a failing read, not a bus without a
// supported path. It reads the registry only and sends nothing.
func (p *vaillantSemanticPoller) refreshHeatSourcePathReason() {
	if !p.publishing() {
		return
	}
	p.mu.Lock()
	controller, boilerAddress := p.controller, p.boilerAddress
	everDiscovered, regCap := p.controllerEverDiscovered, p.regulatorCapability
	p.mu.Unlock()
	heatPump := byte(0)
	if controller == 0 && boilerAddress == 0 && !everDiscovered && regCap != productids.ControllerPresent {
		heatPump = p.findHeatPumpClassAddress()
	}
	reason, changed := p.publication.setNoSupportedHeatSourcePath(heatPump != 0)
	if !changed {
		return
	}
	if heatPump != 0 {
		log.Printf("semantic_startup_reason reason=%s state=set heat_source=0x%02x", reason, heatPump)
		return
	}
	log.Printf("semantic_startup_reason reason=%s state=cleared controller=0x%02x boiler=0x%02x", reason, controller, boilerAddress)
}
