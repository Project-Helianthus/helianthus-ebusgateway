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
				OperatingMode:              zone.Config.OperatingMode,
				Preset:                     zone.Config.Preset,
				TargetTempC:                cloneFloatPtr(zone.Config.TargetTempC),
				AllowedModes:               append([]string(nil), zone.Config.AllowedModes...),
				CircuitType:                zone.Config.CircuitType,
				AssociatedCircuit:          cloneIntPtr(zone.Config.AssociatedCircuit),
				RoomTemperatureZoneMapping: cloneIntPtr(zone.Config.RoomTemperatureZoneMapping),
				QuickVeto:                  zone.Config.QuickVeto,
				QuickVetoSetpointC:         cloneFloatPtr(zone.Config.QuickVetoSetpointC),
				QuickVetoDurationH:         cloneFloatPtr(zone.Config.QuickVetoDurationH),
				QuickVetoExpiry:            zone.Config.QuickVetoExpiry,
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
			ManagingDevice: mcp.ManagingDevice{
				Role:     string(circuit.ManagingDevice.Role),
				DeviceID: cloneStringPtr(circuit.ManagingDevice.DeviceID),
				Address:  cloneIntPtr(circuit.ManagingDevice.Address),
			},
		}
	}
	return out
}

func (adapter mcpSemanticProviderAdapter) RadioDevices() []mcp.RadioDevice {
	if adapter.provider == nil {
		return nil
	}
	devices := adapter.provider.RadioDevices()
	if len(devices) == 0 {
		return nil
	}
	out := make([]mcp.RadioDevice, len(devices))
	for i, device := range devices {
		out[i] = mcp.RadioDevice{
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
		}
	}
	return out
}

func (adapter mcpSemanticProviderAdapter) FM5SemanticMode() mcp.Fm5SemanticMode {
	if adapter.provider == nil {
		return mcp.Fm5SemanticModeAbsent
	}
	mode := adapter.provider.FM5SemanticMode()
	switch mode {
	case graphql.Fm5SemanticModeInterpreted:
		return mcp.Fm5SemanticModeInterpreted
	case graphql.Fm5SemanticModeGPIOOnly:
		return mcp.Fm5SemanticModeGPIOOnly
	default:
		return mcp.Fm5SemanticModeAbsent
	}
}

func (adapter mcpSemanticProviderAdapter) Solar() *mcp.SolarStatus {
	if adapter.provider == nil {
		return nil
	}
	status := adapter.provider.Solar()
	if status == nil {
		return nil
	}
	return &mcp.SolarStatus{
		CollectorTemperatureC: cloneFloatPtr(status.CollectorTemperatureC),
		ReturnTemperatureC:    cloneFloatPtr(status.ReturnTemperatureC),
		PumpActive:            cloneBoolPtr(status.PumpActive),
		CurrentYield:          cloneFloatPtr(status.CurrentYield),
		PumpHours:             cloneFloatPtr(status.PumpHours),
		SolarEnabled:          cloneBoolPtr(status.SolarEnabled),
		FunctionMode:          cloneBoolPtr(status.FunctionMode),
	}
}

func (adapter mcpSemanticProviderAdapter) Cylinders() []mcp.CylinderStatus {
	if adapter.provider == nil {
		return nil
	}
	cylinders := adapter.provider.Cylinders()
	if len(cylinders) == 0 {
		return nil
	}
	out := make([]mcp.CylinderStatus, len(cylinders))
	for i, cylinder := range cylinders {
		out[i] = mcp.CylinderStatus{
			Index:             cylinder.Index,
			TemperatureC:      cloneFloatPtr(cylinder.TemperatureC),
			MaxSetpointC:      cloneFloatPtr(cylinder.MaxSetpointC),
			ChargeHysteresisC: cloneFloatPtr(cylinder.ChargeHysteresisC),
			ChargeOffsetC:     cloneFloatPtr(cylinder.ChargeOffsetC),
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
		Config: &mcp.BoilerConfig{
			DhwOperatingMode: cloneStringPtr(status.Config.DhwOperatingMode),
			FlowsetHcMaxC:    cloneFloatPtr(status.Config.FlowsetHcMaxC),
			FlowsetHwcMaxC:   cloneFloatPtr(status.Config.FlowsetHwcMaxC),
			PartloadHcKW:     cloneFloatPtr(status.Config.PartloadHcKW),
			PartloadHwcKW:    cloneFloatPtr(status.Config.PartloadHwcKW),
		},
		Diagnostics: &mcp.BoilerDiagnostics{
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
	if len(series.Monthly) > 0 {
		out.Monthly = make([]float64, len(series.Monthly))
		copy(out.Monthly, series.Monthly)
	}
	return out
}

func (adapter mcpSemanticProviderAdapter) Schedules() *mcp.ScheduleStatus {
	if adapter.provider == nil {
		return nil
	}
	status := adapter.provider.Schedules()
	if status == nil {
		return nil
	}
	out := &mcp.ScheduleStatus{}
	if len(status.Programs) > 0 {
		out.Programs = make([]mcp.ScheduleProgram, len(status.Programs))
		for i, prog := range status.Programs {
			mp := mcp.ScheduleProgram{
				Zone: prog.Zone,
				HC:   prog.HC,
			}
			if prog.Config != nil {
				mp.Config = &mcp.ScheduleConfig{
					MaxSlots:       prog.Config.MaxSlots,
					TimeResolution: prog.Config.TimeResolution,
					MinDuration:    prog.Config.MinDuration,
					HasTemperature: prog.Config.HasTemperature,
					TempSlots:      prog.Config.TempSlots,
					MinTempC:       cloneFloatPtr(prog.Config.MinTempC),
					MaxTempC:       cloneFloatPtr(prog.Config.MaxTempC),
				}
			}
			if len(prog.SlotsUsed) > 0 {
				mp.SlotsUsed = make([]int, len(prog.SlotsUsed))
				copy(mp.SlotsUsed, prog.SlotsUsed)
			}
			if len(prog.Days) > 0 {
				mp.Days = make([]mcp.ScheduleDayProgram, len(prog.Days))
				for j, day := range prog.Days {
					md := mcp.ScheduleDayProgram{Weekday: day.Weekday}
					if len(day.Slots) > 0 {
						md.Slots = make([]mcp.ScheduleTimerSlot, len(day.Slots))
						for k, slot := range day.Slots {
							md.Slots[k] = mcp.ScheduleTimerSlot{
								StartHour:      slot.StartHour,
								StartMinute:    slot.StartMinute,
								EndHour:        slot.EndHour,
								EndMinute:      slot.EndMinute,
								TemperatureC:   cloneFloatPtr(slot.TemperatureC),
								TemperatureRaw: cloneIntPtr(slot.TemperatureRaw),
							}
						}
					}
					mp.Days[j] = md
				}
			}
			out.Programs[i] = mp
		}
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
