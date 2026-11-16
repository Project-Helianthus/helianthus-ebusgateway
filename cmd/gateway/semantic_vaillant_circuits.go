package main

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
)

func (p *vaillantSemanticPoller) refreshCircuits(ctx context.Context) {
	if p == nil {
		return
	}

	now := p.now()
	p.mu.Lock()
	controller := p.controller
	instances, fullScan := p.circuitRefreshInstancesLocked(now)
	p.mu.Unlock()
	if controller == 0 {
		return
	}

	discovered, inactive, probeSuccess := p.probeCircuitInstances(ctx, controller, instances)
	if !probeSuccess {
		if !fullScan {
			p.mu.Lock()
			p.lastCircuitFullScanAt = time.Time{}
			p.lastCircuitFullScanComplete = false
			p.mu.Unlock()
		}
		return
	}

	updates := make(map[byte]*vaillantCircuitSnapshot, len(discovered))
	anyRead := false
	for instance, discoveredSnapshot := range discovered {
		snapshot := cloneCircuitSnapshot(discoveredSnapshot)
		if snapshot == nil {
			continue
		}
		anyRead = true
		p.readCircuitDetails(ctx, instance, snapshot, now)
		updates[instance] = snapshot
	}

	p.mu.Lock()
	if p.circuits == nil {
		p.circuits = make(map[byte]*vaillantCircuitSnapshot)
	}
	for instance := range inactive {
		delete(p.circuits, instance)
	}
	for instance, incoming := range updates {
		p.circuits[instance] = mergeCircuitSnapshotNonDestructive(p.circuits[instance], incoming)
	}
	if fullScan {
		p.lastCircuitFullScanAt = now
		p.lastCircuitFullScanComplete = len(discovered)+len(inactive) == len(instances)
	}
	p.mu.Unlock()

	source := semanticSnapshotSourceCache
	if anyRead || len(inactive) > 0 {
		source = semanticSnapshotSourceLive
	}
	p.publishCircuits(source)
}

// probeCircuitInstances reads the circuit type of each instance in order. It
// returns the active circuits, the instances that reported no circuit, and
// whether any type read succeeded.
func (p *vaillantSemanticPoller) probeCircuitInstances(ctx context.Context, controller byte, instances []byte) (map[byte]*vaillantCircuitSnapshot, map[byte]struct{}, bool) {
	discovered := make(map[byte]*vaillantCircuitSnapshot, 4)
	inactive := make(map[byte]struct{})
	probeSuccess := false

	for _, instance := range instances {
		circuitTypeRaw, ok := p.readB524Uint16(ctx, localCircuits.opcode, localCircuits.group, instance, circuit_type)
		if !ok || circuitTypeRaw == nil {
			continue
		}
		probeSuccess = true
		switch *circuitTypeRaw {
		case 0x0000, 0x00FF, 0xFFFF:
			if snapshot, known := p.readDhwPseudoCircuitEvidence(ctx, instance); snapshot != nil {
				snapshot.Controller = controller
				discovered[instance] = snapshot
				continue
			} else if !known && instance == dhwPseudoCircuitInstance {
				continue
			}
			inactive[instance] = struct{}{}
		default:
			discovered[instance] = &vaillantCircuitSnapshot{
				Instance:       instance,
				Active:         true,
				Controller:     controller,
				CircuitTypeRaw: cloneUint16Ptr(circuitTypeRaw),
			}
		}
	}
	return discovered, inactive, probeSuccess
}

// readCircuitDetails reads the detail registers of one active circuit in a
// fixed order. A failed read keeps the field the snapshot already holds.
func (p *vaillantSemanticPoller) readCircuitDetails(ctx context.Context, instance byte, snapshot *vaillantCircuitSnapshot, now time.Time) {
	if raw := p.readCircuitUint16(ctx, instance, circuit_cooling_enabled); raw != nil {
		snapshot.CoolingEnabledRaw = raw
	}
	if value := p.readCircuitFloat(ctx, instance, circuit_flow_setpoint); value != nil {
		snapshot.FlowSetpointC = value
	}
	if value := p.readCircuitFloat(ctx, instance, circuit_flow_temp); value != nil {
		snapshot.FlowTemperatureC = value
	}
	if value := p.readCircuitFloat(ctx, instance, circuit_heating_curve); value != nil {
		snapshot.HeatingCurve = value
	}
	if value := p.readCircuitFloat(ctx, instance, circuit_flow_temp_max); value != nil {
		snapshot.FlowTempMaxC = value
	}
	if value := p.readCircuitFloat(ctx, instance, circuit_flow_temp_min); value != nil {
		snapshot.FlowTempMinC = value
	}
	if value := p.readCircuitFloat(ctx, instance, circuit_summer_limit); value != nil {
		snapshot.SummerLimitC = value
	}
	if raw := p.readCircuitUint16(ctx, instance, circuit_room_temp_control); raw != nil {
		snapshot.RoomTempControlRaw = raw
	}
	if raw := p.readCircuitUint16(ctx, instance, circuit_circuit_state); raw != nil {
		snapshot.CircuitStateRaw = raw
		snapshot.CircuitStateLiveAt = now
	}
	if value := p.readB524U8(ctx, localCircuits.opcode, localCircuits.group, instance, circuit_special_function_mode); value != nil {
		snapshot.SpecialFunctionMode = cloneUint8Ptr(value)
	}
	if raw := p.readCircuitUint16(ctx, instance, circuit_pump_status); raw != nil {
		snapshot.PumpStatusRaw = raw
		snapshot.PumpStatusLiveAt = now
	}
	if value := p.readCircuitFloat(ctx, instance, circuit_mixer_position); value != nil {
		snapshot.MixerPositionPct = value
	}
	if value := p.readCircuitFloat(ctx, instance, circuit_humidity); value != nil {
		snapshot.HumidityPct = value
	}
	if value := p.readCircuitFloat(ctx, instance, circuit_dew_point); value != nil {
		snapshot.DewPointC = value
	}
	if value := p.readCircuitUint32(ctx, instance, circuit_pump_hours); value != nil {
		snapshot.PumpHoursRaw = value
	}
	if value := p.readCircuitUint32(ctx, instance, circuit_pump_starts); value != nil {
		snapshot.PumpStartsRaw = value
	}
}

// readCircuitFloat reads one float register of a circuit, or nil when the
// read fails.
func (p *vaillantSemanticPoller) readCircuitFloat(ctx context.Context, instance byte, addr uint16) *float64 {
	value, ok := p.readB524Float32LE(ctx, localCircuits.opcode, localCircuits.group, instance, addr)
	if !ok {
		return nil
	}
	return &value
}

// readCircuitUint16 reads one 16-bit register of a circuit as a detached
// value, or nil when the read fails or carries no value.
func (p *vaillantSemanticPoller) readCircuitUint16(ctx context.Context, instance byte, addr uint16) *uint16 {
	raw, ok := p.readB524Uint16(ctx, localCircuits.opcode, localCircuits.group, instance, addr)
	if !ok || raw == nil {
		return nil
	}
	return cloneUint16Ptr(raw)
}

// readCircuitUint32 reads one 32-bit counter register of a circuit, or nil
// when the read fails.
func (p *vaillantSemanticPoller) readCircuitUint32(ctx context.Context, instance byte, addr uint16) *uint32 {
	value, ok := p.readB524Uint32LE(ctx, localCircuits.opcode, localCircuits.group, instance, addr)
	if !ok {
		return nil
	}
	return &value
}

func (p *vaillantSemanticPoller) readDhwPseudoCircuitEvidence(ctx context.Context, instance byte) (*vaillantCircuitSnapshot, bool) {
	if p == nil || instance != dhwPseudoCircuitInstance {
		return nil, false
	}
	snapshot := newDhwPseudoCircuitSnapshot(instance)
	flow, flowKnown := p.readDhwPseudoCircuitTemperatureEvidence(ctx, instance, circuit_flow_temp)
	if isDhwPseudoCircuitTemperatureEvidence(flow) {
		snapshot.FlowTemperatureC = flow
	}
	if snapshot.FlowTemperatureC == nil {
		return nil, flowKnown
	}
	return snapshot, true
}

func (p *vaillantSemanticPoller) readDhwPseudoCircuitTemperatureEvidence(ctx context.Context, instance byte, addr uint16) (*float64, bool) {
	raw, ok := p.readB524Value(ctx, localCircuits.opcode, localCircuits.group, instance, addr)
	if !ok {
		return nil, false
	}
	return decodeB524Float32FromRaw(raw), true
}

func newDhwPseudoCircuitSnapshot(instance byte) *vaillantCircuitSnapshot {
	circuitType := dhwPseudoCircuitTypeRaw
	return &vaillantCircuitSnapshot{
		Instance:       instance,
		Active:         true,
		CircuitTypeRaw: &circuitType,
	}
}

func isDhwPseudoCircuitTemperatureEvidence(value *float64) bool {
	if value == nil {
		return false
	}
	return *value >= 5 && *value <= 95
}

func allCircuitRefreshInstances() []byte {
	instances := make([]byte, 0, 0x0B)
	for instance := byte(0x00); instance <= 0x0A; instance++ {
		instances = append(instances, instance)
	}
	return instances
}

func (p *vaillantSemanticPoller) circuitRefreshInstancesLocked(now time.Time) ([]byte, bool) {
	if p == nil {
		return nil, false
	}
	interval := p.circuitFullScanInterval
	if interval <= 0 {
		interval = semanticCircuitFullScanInterval
	}
	if len(p.circuits) == 0 || p.lastCircuitFullScanAt.IsZero() || now.Sub(p.lastCircuitFullScanAt) >= interval {
		return allCircuitRefreshInstances(), true
	}
	if !p.lastCircuitFullScanComplete && now.Sub(p.lastCircuitFullScanAt) >= semanticCircuitPartialScanInterval {
		return allCircuitRefreshInstances(), true
	}

	instances := make([]byte, 0, len(p.circuits))
	for instance, snapshot := range p.circuits {
		if snapshot == nil || !snapshot.Active {
			continue
		}
		instances = append(instances, instance)
	}
	if len(instances) == 0 {
		return allCircuitRefreshInstances(), true
	}
	slices.Sort(instances)
	return instances, false
}

func mergeCircuitSnapshotNonDestructive(existing, incoming *vaillantCircuitSnapshot) *vaillantCircuitSnapshot {
	merged := cloneCircuitSnapshot(existing)
	if merged == nil {
		merged = &vaillantCircuitSnapshot{}
	}
	if incoming == nil {
		return merged
	}

	merged.Instance = incoming.Instance
	merged.Active = merged.Active || incoming.Active
	controllerChanged := incoming.Controller != 0 && incoming.Controller != merged.Controller
	if controllerChanged && incoming.CircuitStateRaw == nil {
		merged.CircuitStateRaw = nil
		merged.CircuitStateLiveAt = time.Time{}
	}
	if controllerChanged && incoming.PumpStatusRaw == nil {
		merged.PumpStatusRaw = nil
		merged.PumpStatusLiveAt = time.Time{}
	}
	if incoming.Controller != 0 {
		merged.Controller = incoming.Controller
	}
	if incoming.CircuitTypeRaw != nil {
		merged.CircuitTypeRaw = cloneUint16Ptr(incoming.CircuitTypeRaw)
	}
	if incoming.CoolingEnabledRaw != nil {
		merged.CoolingEnabledRaw = cloneUint16Ptr(incoming.CoolingEnabledRaw)
	}
	if incoming.FlowSetpointC != nil {
		merged.FlowSetpointC = cloneFloat64Ptr(incoming.FlowSetpointC)
	}
	if incoming.FlowTemperatureC != nil {
		merged.FlowTemperatureC = cloneFloat64Ptr(incoming.FlowTemperatureC)
	}
	if incoming.HeatingCurve != nil {
		merged.HeatingCurve = cloneFloat64Ptr(incoming.HeatingCurve)
	}
	if incoming.FlowTempMaxC != nil {
		merged.FlowTempMaxC = cloneFloat64Ptr(incoming.FlowTempMaxC)
	}
	if incoming.FlowTempMinC != nil {
		merged.FlowTempMinC = cloneFloat64Ptr(incoming.FlowTempMinC)
	}
	if incoming.SummerLimitC != nil {
		merged.SummerLimitC = cloneFloat64Ptr(incoming.SummerLimitC)
	}
	if incoming.RoomTempControlRaw != nil {
		merged.RoomTempControlRaw = cloneUint16Ptr(incoming.RoomTempControlRaw)
	}
	if incoming.CircuitStateRaw != nil {
		merged.CircuitStateRaw = cloneUint16Ptr(incoming.CircuitStateRaw)
		merged.CircuitStateLiveAt = incoming.CircuitStateLiveAt
	}
	if incoming.SpecialFunctionMode != nil {
		merged.SpecialFunctionMode = cloneUint8Ptr(incoming.SpecialFunctionMode)
	}
	if incoming.PumpStatusRaw != nil {
		merged.PumpStatusRaw = cloneUint16Ptr(incoming.PumpStatusRaw)
		merged.PumpStatusLiveAt = incoming.PumpStatusLiveAt
	}
	if incoming.MixerPositionPct != nil {
		merged.MixerPositionPct = cloneFloat64Ptr(incoming.MixerPositionPct)
	}
	if incoming.HumidityPct != nil {
		merged.HumidityPct = cloneFloat64Ptr(incoming.HumidityPct)
	}
	if incoming.DewPointC != nil {
		merged.DewPointC = cloneFloat64Ptr(incoming.DewPointC)
	}
	if incoming.PumpHoursRaw != nil {
		merged.PumpHoursRaw = cloneUint32Ptr(incoming.PumpHoursRaw)
	}
	if incoming.PumpStartsRaw != nil {
		merged.PumpStartsRaw = cloneUint32Ptr(incoming.PumpStartsRaw)
	}
	return merged
}

// circuitsViewLocked builds the native view of the active circuits in
// instance order with the managing-device decision taken from the retained
// system snapshot and FM5 mode. The caller holds p.mu.
func (p *vaillantSemanticPoller) circuitsViewLocked() vaillantCircuitsView {
	instances := make([]byte, 0, len(p.circuits))
	for instance := range p.circuits {
		instances = append(instances, instance)
	}
	slices.Sort(instances)
	circuits := make([]*vaillantCircuitSnapshot, 0, len(instances))
	for _, instance := range instances {
		entry := p.circuits[instance]
		if entry == nil || !entry.Active {
			continue
		}
		circuits = append(circuits, cloneCircuitSnapshot(entry))
	}
	return vaillantCircuitsView{
		circuits:                circuits,
		managedByFunctionModule: circuitsManagedByFunctionModule(p.system, p.fm5Mode),
	}
}

func cloneCircuitSnapshot(snapshot *vaillantCircuitSnapshot) *vaillantCircuitSnapshot {
	if snapshot == nil {
		return nil
	}
	return &vaillantCircuitSnapshot{
		Instance:            snapshot.Instance,
		Active:              snapshot.Active,
		Controller:          snapshot.Controller,
		CircuitTypeRaw:      cloneUint16Ptr(snapshot.CircuitTypeRaw),
		CoolingEnabledRaw:   cloneUint16Ptr(snapshot.CoolingEnabledRaw),
		FlowSetpointC:       cloneFloat64Ptr(snapshot.FlowSetpointC),
		FlowTemperatureC:    cloneFloat64Ptr(snapshot.FlowTemperatureC),
		HeatingCurve:        cloneFloat64Ptr(snapshot.HeatingCurve),
		FlowTempMaxC:        cloneFloat64Ptr(snapshot.FlowTempMaxC),
		FlowTempMinC:        cloneFloat64Ptr(snapshot.FlowTempMinC),
		SummerLimitC:        cloneFloat64Ptr(snapshot.SummerLimitC),
		RoomTempControlRaw:  cloneUint16Ptr(snapshot.RoomTempControlRaw),
		CircuitStateRaw:     cloneUint16Ptr(snapshot.CircuitStateRaw),
		CircuitStateLiveAt:  snapshot.CircuitStateLiveAt,
		SpecialFunctionMode: cloneUint8Ptr(snapshot.SpecialFunctionMode),
		PumpStatusRaw:       cloneUint16Ptr(snapshot.PumpStatusRaw),
		PumpStatusLiveAt:    snapshot.PumpStatusLiveAt,
		// CalcFlowTempC stays unavailable, including snapshots retained before the RR0020 correction.
		MixerPositionPct: cloneFloat64Ptr(snapshot.MixerPositionPct),
		HumidityPct:      cloneFloat64Ptr(snapshot.HumidityPct),
		DewPointC:        cloneFloat64Ptr(snapshot.DewPointC),
		PumpHoursRaw:     cloneUint32Ptr(snapshot.PumpHoursRaw),
		PumpStartsRaw:    cloneUint32Ptr(snapshot.PumpStartsRaw),
	}
}

func circuitsEqual(a, b []graphql.CircuitStatus) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		left := a[i]
		right := b[i]
		if left.Index != right.Index || left.CircuitType != right.CircuitType || left.HasMixer != right.HasMixer {
			return false
		}
		if !boolPtrEquals(left.State.PumpActive, right.State.PumpActive) {
			return false
		}
		if !floatPtrEquals(left.State.MixerPositionPct, right.State.MixerPositionPct) {
			return false
		}
		if !floatPtrEquals(left.State.FlowTemperatureC, right.State.FlowTemperatureC) {
			return false
		}
		if !floatPtrEquals(left.State.FlowSetpointC, right.State.FlowSetpointC) {
			return false
		}
		if !floatPtrEquals(left.State.CalcFlowTempC, right.State.CalcFlowTempC) {
			return false
		}
		if left.State.CircuitState != right.State.CircuitState {
			return false
		}
		if !floatPtrEquals(left.State.Humidity, right.State.Humidity) {
			return false
		}
		if !floatPtrEquals(left.State.DewPoint, right.State.DewPoint) {
			return false
		}
		if !floatPtrEquals(left.State.PumpHours, right.State.PumpHours) {
			return false
		}
		if !intPtrEquals(left.State.PumpStarts, right.State.PumpStarts) {
			return false
		}
		if !floatPtrEquals(left.Config.HeatingCurve, right.Config.HeatingCurve) {
			return false
		}
		if !floatPtrEquals(left.Config.FlowTempMaxC, right.Config.FlowTempMaxC) {
			return false
		}
		if !floatPtrEquals(left.Config.FlowTempMinC, right.Config.FlowTempMinC) {
			return false
		}
		if !floatPtrEquals(left.Config.SummerLimitC, right.Config.SummerLimitC) {
			return false
		}
		if !intPtrEquals(left.Config.SpecialFunctionMode, right.Config.SpecialFunctionMode) {
			return false
		}
		if left.Config.RoomTempControl != right.Config.RoomTempControl {
			return false
		}
		if !boolPtrEquals(left.Config.CoolingEnabled, right.Config.CoolingEnabled) {
			return false
		}
		if left.ManagingDevice.Role != right.ManagingDevice.Role {
			return false
		}
		if !stringPtrEquals(left.ManagingDevice.DeviceID, right.ManagingDevice.DeviceID) {
			return false
		}
		if !intPtrEquals(left.ManagingDevice.Address, right.ManagingDevice.Address) {
			return false
		}
	}
	return true
}

func decodeHeatingCircuitTypeToken(raw *uint16) string {
	if raw == nil {
		return ""
	}
	switch *raw {
	case 1:
		return "heating"
	case 2:
		return "fixed_value"
	case 3:
		return "dhw"
	case 4:
		return "return_increase"
	default:
		return fmt.Sprintf("unknown_%d", *raw)
	}
}

func decodeRoomTempControlToken(raw *uint16) string {
	if raw == nil {
		return ""
	}
	switch *raw {
	case 0:
		return "off"
	case 1:
		return "modulating"
	case 2:
		return "thermostat"
	default:
		return fmt.Sprintf("unknown_%d", *raw)
	}
}

var circuitStateNames = map[uint16]string{
	0: CircuitStateStandby,
	1: CircuitStateHeating,
	2: CircuitStateCooling,
	3: CircuitStateDHW,
}

func decodeCircuitStateToken(raw *uint16) string {
	if raw == nil {
		return ""
	}
	if name, ok := circuitStateNames[*raw]; ok {
		return name
	}
	return fmt.Sprintf("unknown_%d", *raw)
}

const (
	circuitManagingDeviceVR71ID      = "VR_71"
	circuitManagingDeviceVR71Address = 0x26
)

func deriveCircuitManagingDevice(system *vaillantSystemSnapshot, fm5Mode graphql.Fm5SemanticMode) graphql.ManagingDevice {
	return circuitManagingDevice(circuitsManagedByFunctionModule(system, fm5Mode))
}

// circuitsManagedByFunctionModule reports whether the system scheme, the
// module configuration and the interpreted FM5 mode place the heating
// circuits under the function module.
func circuitsManagedByFunctionModule(system *vaillantSystemSnapshot, fm5Mode graphql.Fm5SemanticMode) bool {
	return system != nil &&
		system.SystemScheme != nil && *system.SystemScheme == 1 &&
		system.ModuleConfigurationVR71 != nil && *system.ModuleConfigurationVR71 == 2 &&
		fm5Mode == graphql.Fm5SemanticModeInterpreted
}

// circuitManagingDevice returns a detached managing device for one circuit.
func circuitManagingDevice(managedByFunctionModule bool) graphql.ManagingDevice {
	if managedByFunctionModule {
		deviceID := circuitManagingDeviceVR71ID
		address := circuitManagingDeviceVR71Address
		return graphql.ManagingDevice{
			Role:     graphql.ManagingDeviceRoleFunctionModule,
			DeviceID: &deviceID,
			Address:  &address,
		}
	}
	return graphql.ManagingDevice{Role: graphql.ManagingDeviceRoleUnknown}
}

func decodeValvePositionPct(raw *uint16) *float64 {
	if raw == nil {
		return nil
	}
	pct := float64(*raw) / 655.35
	return &pct
}

func decodeCircuitType(raw *uint16) string {
	if raw == nil {
		return ""
	}
	switch *raw {
	case 0:
		return "radiator"
	case 1:
		return "underfloor"
	case 2:
		return "mixed"
	default:
		return fmt.Sprintf("unknown_%d", *raw)
	}
}

func decodeAssociatedCircuit(raw *uint16) *int {
	if raw == nil {
		return nil
	}
	if *raw == 0xFF || *raw == 0xFFFF {
		return nil
	}
	v := int(*raw)
	return &v
}

func decodeRoomTemperatureZoneMapping(raw *uint16) *int {
	if raw == nil {
		return nil
	}
	if *raw == 0xFF || *raw == 0xFFFF {
		return nil
	}
	v := int(*raw)
	return &v
}
