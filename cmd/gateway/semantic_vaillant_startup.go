package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
)

func (p *vaillantSemanticPoller) boilerStatusTierSchedules() []boilerStatusTierSchedule {
	if p == nil {
		return nil
	}
	return []boilerStatusTierSchedule{
		{tier: boilerStatusTierFast, interval: p.boilerFastInterval, priority: semanticTaskPriorityHigh},
		{tier: boilerStatusTierMedium, interval: p.boilerMediumInterval, priority: semanticTaskPriorityMedium},
		{tier: boilerStatusTierSlow, interval: p.boilerSlowInterval, priority: semanticTaskPriorityLow},
	}
}

func (p *vaillantSemanticPoller) boilerStatusTierTask(tier boilerStatusTier) func(context.Context) {
	return func(ctx context.Context) {
		if tier == boilerStatusTierFast {
			p.refreshBoilerStatus(ctx)
			return
		}
		p.refreshBoilerStatusTier(ctx, tier)
	}
}

func boilerStatusTierTaskKey(tier boilerStatusTier) semanticTaskKey {
	switch tier {
	case boilerStatusTierFast:
		return semanticTaskRefreshBoilerFast
	case boilerStatusTierMedium:
		return semanticTaskRefreshBoilerMedium
	case boilerStatusTierSlow:
		return semanticTaskRefreshBoilerSlow
	default:
		return semanticTaskKey("")
	}
}

func (p *vaillantSemanticPoller) enqueueBoilerStatusPriming(ctx context.Context) {
	if p == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if p.tasks == nil {
		for _, schedule := range p.boilerStatusTierSchedules() {
			p.boilerStatusTierTask(schedule.tier)(ctx)
		}
		return
	}
	for _, schedule := range p.boilerStatusTierSchedules() {
		p.enqueueTask(boilerStatusTierTaskKey(schedule.tier), schedule.priority, p.boilerStatusTierTask(schedule.tier))
	}
}

func (p *vaillantSemanticPoller) enqueueControllerSemanticPriming(ctx context.Context) {
	if p == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if p.tasks == nil {
		p.refreshConfig(ctx)
		p.refreshCircuits(ctx)
		p.refreshSystem(ctx)
		p.refreshRadioDevices(ctx)
		p.refreshEnergy(ctx)
		return
	}
	p.enqueueTask(semanticTaskRefreshConfig, semanticTaskPriorityHigh, p.refreshConfig)
	p.enqueueTask(semanticTaskRefreshCircuits, semanticTaskPriorityMedium, p.refreshCircuits)
	p.enqueueTask(semanticTaskRefreshSystem, semanticTaskPriorityMedium, p.refreshSystem)
	p.enqueueTask(semanticTaskRefreshRadioDevices, semanticTaskPriorityMedium, p.refreshRadioDevices)
	p.enqueueTask(semanticTaskRefreshEnergy, semanticTaskPriorityMedium, p.refreshEnergy)
}

func (p *vaillantSemanticPoller) refreshStartupSemanticPlanes(ctx context.Context) {
	if p == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}

	p.mu.Lock()
	controller := p.controller
	p.mu.Unlock()
	if controller == 0 {
		p.refreshDiscovery(ctx)
		p.mu.Lock()
		controller = p.controller
		p.mu.Unlock()
	}
	if controller == 0 {
		return
	}
	if p.provider == nil {
		return
	}

	p.publishStartupSchedules()
	p.publishStartupPlaneDefaults()
	primingCtx := ctx
	cancel := func() {}
	if semanticStartupPrimingBudget > 0 {
		primingCtx, cancel = context.WithTimeout(ctx, semanticStartupPrimingBudget)
	}
	defer cancel()

	attempts := 0
	for {
		attempts++
		status := p.startupL1PrimingStatus()
		if !status.dhw {
			p.refreshDHWStartupUntilReady(primingCtx, 1)
			status = p.startupL1PrimingStatus()
		}
		if !status.zones {
			p.refreshZoneDiscovery(primingCtx, true)
			status = p.startupL1PrimingStatus()
		}
		if !status.dhw {
			p.refreshDHWStartupUntilReady(primingCtx, 1)
		}
		if !status.system {
			p.refreshSystemStartup(primingCtx)
		}
		if !status.boilerStatus {
			p.refreshBoilerStatusStartup(primingCtx)
		}
		status = p.startupL1PrimingStatus()
		if !status.circuits {
			p.refreshCircuitsStartup(primingCtx)
		}
		if !status.radioDevices {
			p.refreshRadioDevicesStartup(primingCtx)
		}
		status = p.startupL1PrimingStatus()
		if !status.fm5Satisfied && status.fm5Evidence && !status.fm5GateKnown {
			p.refreshSystemStartup(primingCtx)
			status = p.startupL1PrimingStatus()
		}
		if status.system && (!status.fm5Satisfied || !status.solar || !status.cylinders) {
			p.refreshFM5SemanticStartup(primingCtx)
		}
		status = p.startupL1PrimingStatus()
		if status.ready() {
			log.Printf("semantic_startup_l1_priming result=ready attempts=%d %s", attempts, status.String())
			return
		}
		if semanticStartupPrimingBudget <= 0 {
			log.Printf("semantic_startup_l1_priming result=incomplete attempts=%d %s", attempts, status.String())
			return
		}

		delay := semanticStartupPrimingRetryDelay
		if delay <= 0 {
			delay = 50 * time.Millisecond
		}
		timer := time.NewTimer(delay)
		select {
		case <-primingCtx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			log.Printf("semantic_startup_l1_priming result=deadline attempts=%d %s", attempts, status.String())
			return
		case <-timer.C:
		}
	}
}

func (p *vaillantSemanticPoller) publishStartupPlaneDefaults() {
	if p == nil || p.provider == nil {
		return
	}
	if p.provider.Circuits() == nil {
		p.provider.SetCircuits([]graphql.CircuitStatus{})
	}
	if p.provider.Solar() == nil {
		p.provider.SetSolar(&graphql.SolarStatus{})
	}
	if p.provider.Cylinders() == nil {
		p.provider.SetCylinders([]graphql.CylinderStatus{})
	}
}

func (p *vaillantSemanticPoller) refreshStartupCriticalSemanticPlanes(ctx context.Context) {
	if p == nil || p.provider == nil {
		return
	}

	p.publishStartupSchedules()
	status := p.startupL1PrimingStatus()
	if !status.dhw {
		p.refreshDHWStartupUntilReady(ctx, semanticStartupCriticalDHWAttempts)
	}
	status = p.startupL1PrimingStatus()
	if !status.system {
		p.refreshSystemStartup(ctx)
	}
	status = p.startupL1PrimingStatus()
	if !status.boilerStatus {
		p.refreshBoilerStatusStartup(ctx)
	}
}

func (p *vaillantSemanticPoller) refreshDHWStartupUntilReady(ctx context.Context, attempts int) bool {
	if p == nil {
		return false
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if attempts <= 0 {
		attempts = 1
	}
	for attempt := 0; attempt < attempts; attempt++ {
		if p.startupL1PrimingStatus().dhw {
			return true
		}
		p.refreshDHWStartup(ctx)
		if p.startupL1PrimingStatus().dhw {
			return true
		}
		if attempt == attempts-1 {
			break
		}
		delay := semanticStartupPrimingRetryDelay
		if delay <= 0 {
			delay = 50 * time.Millisecond
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return false
		case <-timer.C:
		}
	}
	return p.startupL1PrimingStatus().dhw
}

type startupL1PrimingStatus struct {
	zones        bool
	dhw          bool
	circuits     bool
	system       bool
	radioDevices bool
	fm5GateKnown bool
	fm5Evidence  bool
	fm5Required  bool
	fm5Satisfied bool
	solar        bool
	cylinders    bool
	boilerStatus bool
}

func (status startupL1PrimingStatus) ready() bool {
	return status.zones &&
		status.dhw &&
		status.circuits &&
		status.system &&
		status.radioDevices &&
		status.fm5Satisfied &&
		status.boilerStatus
}

func (status startupL1PrimingStatus) String() string {
	return fmt.Sprintf(
		"zones=%t dhw=%t circuits=%t system=%t radio_devices=%t fm5_gate_known=%t fm5_evidence=%t fm5_required=%t fm5_satisfied=%t solar=%t cylinders=%t boiler_status=%t",
		status.zones,
		status.dhw,
		status.circuits,
		status.system,
		status.radioDevices,
		status.fm5GateKnown,
		status.fm5Evidence,
		status.fm5Required,
		status.fm5Satisfied,
		status.solar,
		status.cylinders,
		status.boilerStatus,
	)
}

func (p *vaillantSemanticPoller) startupL1PrimingStatus() startupL1PrimingStatus {
	if p == nil || p.provider == nil {
		return startupL1PrimingStatus{}
	}
	p.mu.Lock()
	moduleConfig := (*uint16)(nil)
	if p.system != nil {
		moduleConfig = cloneUint16Ptr(p.system.ModuleConfigurationVR71)
	}
	fm5GateKnown := p.system != nil && p.system.ModuleConfigurationVR71 != nil
	radioSnapshots := make([]*vaillantRadioDeviceSnapshot, 0, len(p.radioDevices))
	for _, snapshot := range p.radioDevices {
		if snapshot != nil {
			radioSnapshots = append(radioSnapshots, cloneRadioSnapshot(snapshot))
		}
	}
	radioProbed := p.startupRadioDevicesProbed
	fm5RegistryEvidenceIgnored := p.fm5RegistryEvidenceIgnored
	p.mu.Unlock()
	liveFM5Evidence := hasFM5EvidenceFromRadioSnapshots(radioSnapshots)
	fm5Evidence := liveFM5Evidence || (!fm5RegistryEvidenceIgnored && p.hasFM5RegistryEvidence())
	fm5Required := fm5Evidence && moduleConfig != nil && *moduleConfig <= 2
	zonesReady := p.provider.Zones() != nil
	solarReady := p.provider.Solar() != nil
	cylinders := p.provider.Cylinders()
	cylindersPublished := cylinders != nil
	interpretedCylindersReady := len(cylinders) > 0
	fm5Mode := p.provider.FM5SemanticMode()
	fm5NonInterpretedPublished := fm5Evidence &&
		fm5Mode == graphql.Fm5SemanticModeGPIOOnly &&
		solarReady &&
		cylindersPublished
	fm5Satisfied := !fm5Evidence ||
		fm5NonInterpretedPublished ||
		(fm5GateKnown && !fm5Required) ||
		(fm5Required && solarReady && interpretedCylindersReady)
	return startupL1PrimingStatus{
		zones:        zonesReady,
		dhw:          p.provider.DHW() != nil,
		circuits:     p.provider.Circuits() != nil,
		system:       p.provider.System() != nil,
		radioDevices: radioProbed || len(p.provider.RadioDevices()) > 0,
		fm5GateKnown: fm5GateKnown,
		fm5Evidence:  fm5Evidence,
		fm5Required:  fm5Required,
		fm5Satisfied: fm5Satisfied,
		solar:        solarReady,
		cylinders:    cylindersPublished,
		boilerStatus: p.provider.BoilerStatus() != nil,
	}
}

func (p *vaillantSemanticPoller) startupProbeContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithTimeout(ctx, semanticStartupProbeTimeout)
}

func (p *vaillantSemanticPoller) readB524Startup(ctx context.Context, opcode, group, instance byte, addr uint16) ([]byte, bool) {
	probeCtx, cancel := p.startupProbeContext(ctx)
	defer cancel()
	return p.readB524ValueLive(probeCtx, opcode, group, instance, addr)
}

func (p *vaillantSemanticPoller) readB524Uint16Startup(ctx context.Context, opcode, group, instance byte, addr uint16) (*uint16, bool) {
	raw, ok := p.readB524Startup(ctx, opcode, group, instance, addr)
	if !ok {
		return nil, false
	}
	value, ok := decodeB524Uint16(raw)
	if !ok {
		return nil, false
	}
	return &value, true
}

func (p *vaillantSemanticPoller) publishStartupSchedules() {
	if p == nil || p.provider == nil || p.provider.Schedules() != nil {
		return
	}
	p.provider.SetSchedules(&graphql.ScheduleStatus{Programs: []graphql.ScheduleProgram{}})
}

func (p *vaillantSemanticPoller) refreshDHWStartup(ctx context.Context) {
	if p == nil {
		return
	}

	attempted := make(semanticFieldSet)
	status := &vaillantDhwSnapshot{}
	readAny := false
	if raw, ok := p.readB524Startup(ctx, localDHW.opcode, localDHW.group, dhwInstance, dhw_current_temp); ok {
		if value := decodeB524Float32FromRaw(raw); value != nil {
			status.CurrentTempC = value
			attempted[dhwFieldCurrentTempC] = struct{}{}
			readAny = true
		}
	}
	opModeRaw, opModeOK := p.readB524Uint16Startup(ctx, localDHW.opcode, localDHW.group, dhwInstance, dhw_operation_mode)
	if opModeOK {
		attempted[dhwFieldOperatingMode] = struct{}{}
		attempted[dhwFieldPreset] = struct{}{}
		attempted[dhwFieldDhwOperationModeRaw] = struct{}{}
		readAny = true
	}
	if opModeOK {
		status.OperatingMode, status.Preset = deriveDhwModeAndPreset(opModeRaw, nil)
		status.ConfigurationDHWOperationMode = formatUintToken(*opModeRaw)
	}

	p.mu.Lock()
	source := semanticSnapshotSourceCache
	if readAny {
		if p.dhw == nil {
			p.dhw = &vaillantDhwSnapshot{}
		}
		mergeDhwSnapshotFields(p.dhw, status, semanticSnapshotSourceLive, attempted)
		p.markDHWUpdatedNowLocked()
		source = semanticSnapshotSourceLive
	}
	hasSnapshot := p.dhw != nil
	p.mu.Unlock()
	if hasSnapshot {
		p.publishDHW(source)
	}
}

func (p *vaillantSemanticPoller) refreshCircuitsStartup(ctx context.Context) {
	if p == nil {
		return
	}

	p.mu.Lock()
	controller := p.controller
	p.mu.Unlock()
	instances := allCircuitRefreshInstances()
	updates := make(map[byte]*vaillantCircuitSnapshot)
	inactive := make(map[byte]struct{})
	for _, instance := range instances {
		circuitTypeRaw, ok := p.readB524Uint16Startup(ctx, localCircuits.opcode, localCircuits.group, instance, circuit_type)
		if !ok || circuitTypeRaw == nil {
			continue
		}
		switch *circuitTypeRaw {
		case 0x0000, 0x00FF, 0xFFFF:
			if snapshot, known := p.readDhwPseudoCircuitStartupEvidence(ctx, instance); snapshot != nil {
				snapshot.Controller = controller
				updates[instance] = snapshot
				continue
			} else if !known && instance == dhwPseudoCircuitInstance {
				continue
			}
			inactive[instance] = struct{}{}
			continue
		default:
			updates[instance] = &vaillantCircuitSnapshot{
				Instance:       instance,
				Active:         true,
				Controller:     controller,
				CircuitTypeRaw: cloneUint16Ptr(circuitTypeRaw),
			}
		}
	}
	if len(updates) == 0 && len(inactive) == 0 {
		p.mu.Lock()
		hasExisting := len(p.circuits) > 0
		p.mu.Unlock()
		if hasExisting {
			p.publishCircuits(semanticSnapshotSourceCache)
		}
		return
	}

	p.mu.Lock()
	if p.circuits == nil {
		p.circuits = make(map[byte]*vaillantCircuitSnapshot)
	}
	for instance := range inactive {
		delete(p.circuits, instance)
	}
	for instance, incoming := range updates {
		if incoming.CircuitStateRaw != nil {
			incoming.CircuitStateLiveAt = p.now()
		}
		if incoming.PumpStatusRaw != nil {
			incoming.PumpStatusLiveAt = p.now()
		}
		p.circuits[instance] = mergeCircuitSnapshotNonDestructive(p.circuits[instance], incoming)
	}
	p.lastCircuitFullScanAt = p.now()
	p.lastCircuitFullScanComplete = len(updates)+len(inactive) == len(instances)
	p.mu.Unlock()
	p.publishCircuits(semanticSnapshotSourceLive)
}

func (p *vaillantSemanticPoller) readDhwPseudoCircuitStartupEvidence(ctx context.Context, instance byte) (*vaillantCircuitSnapshot, bool) {
	if p == nil || instance != dhwPseudoCircuitInstance {
		return nil, false
	}
	snapshot := newDhwPseudoCircuitSnapshot(instance)
	flow, flowKnown := p.readDhwPseudoCircuitStartupTemperatureEvidence(ctx, instance, circuit_flow_temp)
	if isDhwPseudoCircuitTemperatureEvidence(flow) {
		snapshot.FlowTemperatureC = flow
	}
	calc, calcKnown := p.readDhwPseudoCircuitStartupTemperatureEvidence(ctx, instance, circuit_calc_flow_temp)
	if isDhwPseudoCircuitTemperatureEvidence(calc) {
		snapshot.CalcFlowTempC = calc
	}
	if snapshot.FlowTemperatureC == nil && snapshot.CalcFlowTempC == nil {
		return nil, flowKnown && calcKnown
	}
	return snapshot, true
}

func (p *vaillantSemanticPoller) readDhwPseudoCircuitStartupTemperatureEvidence(ctx context.Context, instance byte, addr uint16) (*float64, bool) {
	raw, ok := p.readB524Startup(ctx, localCircuits.opcode, localCircuits.group, instance, addr)
	if !ok {
		return nil, false
	}
	return decodeB524Float32FromRaw(raw), true
}

func (p *vaillantSemanticPoller) refreshSystemStartup(ctx context.Context) {
	if p == nil {
		return
	}

	p.mu.Lock()
	controller := p.controller
	p.mu.Unlock()
	snapshot := &vaillantSystemSnapshot{Controller: controller}
	readAny := false
	configurationAcquired := false
	if raw, ok := p.readB524Uint16Startup(ctx, localRegulator.opcode, localRegulator.group, regulatorInstance, system_scheme); ok && raw != nil {
		snapshot.SystemScheme = cloneUint16Ptr(raw)
		readAny = true
	}
	if raw, ok := p.readB524Uint16Startup(ctx, localRegulator.opcode, localRegulator.group, regulatorInstance, system_module_configuration_vr71); ok && raw != nil {
		snapshot.ModuleConfigurationVR71 = cloneUint16Ptr(raw)
		readAny = true
		configurationAcquired = true
	}
	if raw, ok := p.readB524Startup(ctx, localRegulator.opcode, localRegulator.group, regulatorInstance, system_water_pressure); ok {
		if value := decodeB524Float32FromRaw(raw); value != nil {
			snapshot.SystemWaterPressure = value
			readAny = true
		}
	}

	p.mu.Lock()
	source := semanticSnapshotSourceCache
	p.updateFM5ConfigurationAcquisitionLocked(configurationAcquired)
	if readAny {
		if snapshot.SystemFlowTemperature != nil {
			snapshot.SystemFlowTemperatureLiveAt = p.now()
		}
		p.updateSystemSnapshotLocked(mergeSystemSnapshotNonDestructive(p.system, snapshot))
		source = semanticSnapshotSourceLive
	}
	hasSnapshot := p.system != nil
	p.mu.Unlock()
	if hasSnapshot {
		p.publishSystem(source)
	}
}

func (p *vaillantSemanticPoller) refreshFM5ConfigurationForLateIdentity(ctx context.Context) {
	if p == nil {
		return
	}

	p.mu.Lock()
	controller := p.controller
	p.mu.Unlock()
	snapshot := &vaillantSystemSnapshot{Controller: controller}
	configurationAcquired := false
	if raw, ok := p.readB524Uint16Startup(ctx, localRegulator.opcode, localRegulator.group, regulatorInstance, system_module_configuration_vr71); ok && raw != nil {
		snapshot.ModuleConfigurationVR71 = cloneUint16Ptr(raw)
		configurationAcquired = true
	}

	p.mu.Lock()
	source := semanticSnapshotSourceCache
	p.updateFM5ConfigurationAcquisitionLocked(configurationAcquired)
	if configurationAcquired {
		p.updateSystemSnapshotLocked(mergeSystemSnapshotNonDestructive(p.system, snapshot))
		source = semanticSnapshotSourceLive
	}
	hasSnapshot := p.system != nil
	p.mu.Unlock()
	if hasSnapshot {
		p.publishSystem(source)
	}
}

func (p *vaillantSemanticPoller) refreshRadioDevicesStartup(ctx context.Context) {
	if p == nil {
		return
	}

	discovered := p.registryRadioDeviceSeeds()
	fullScanGroups := startupRadioFullScanGroups(discovered)
	fm5PositiveObserved := false
	verified := make(map[radioDeviceKey]bool)
	readAny := false
	probeSlot := func(grp b524GroupDef, instance byte) {
		connectedRaw, ok := p.readB524Startup(ctx, grp.opcode, grp.group, instance, device_slot_connected)
		if !ok || len(connectedRaw) == 0 {
			return
		}
		readAny = true
		connected := connectedRaw[0] == 1
		classAddress := p.readB524U8Startup(ctx, grp.opcode, grp.group, instance, device_slot_class_address)
		key := radioDeviceKey{Group: grp.group, Instance: instance}
		verified[key] = true
		include, slotMode := startupRadioDeviceInclude(grp.group, connected, classAddress)
		if !include {
			delete(discovered, key)
			return
		}
		discovered[key] = &vaillantRadioDeviceSnapshot{
			Group:              grp.group,
			Instance:           instance,
			SlotMode:           slotMode,
			DeviceConnected:    &connected,
			DeviceClassAddress: cloneUint8Ptr(classAddress),
			DeviceModel:        decodeRadioDeviceModel(classAddress),
		}
	}

	for _, grp := range remoteDeviceGroups {
		if grp.group == remoteFunctionalModules.group {
			continue
		}
		for instance := byte(0x00); instance <= semanticStartupSlotFastMaxInstance; instance++ {
			probeSlot(grp, instance)
		}
	}
	fm5LastInstance := semanticStartupSlotFastMaxInstance
	if fullScanGroups[remoteFunctionalModules.group] {
		fm5LastInstance = semanticStartupSlotFullMaxInstance
	}
	fm5Scan := p.scanFunctionalModuleIdentityBudgetAwareWith(ctx, 0x00, fm5LastInstance, p.readB524Startup)
	if fm5Scan.observedAny {
		readAny = true
	}
	for instance := byte(0x00); instance <= fm5LastInstance; instance++ {
		key := radioDeviceKey{Group: remoteFunctionalModules.group, Instance: instance}
		verified[key] = true
		if snapshot := fm5Scan.snapshots[instance]; snapshot != nil {
			discovered[key] = snapshot
		} else {
			delete(discovered, key)
		}
	}
	fm5PositiveObserved = fm5Scan.positiveSnapshot != nil
	fm5NamespaceComplete := fullScanGroups[remoteFunctionalModules.group] && fm5Scan.namespaceComplete
	if !fm5PositiveObserved {
		for _, grp := range remoteDeviceGroups {
			if grp.group == remoteFunctionalModules.group || !fullScanGroups[grp.group] {
				continue
			}
			for instance := semanticStartupSlotFastMaxInstance + 1; instance <= semanticStartupSlotFullMaxInstance; instance++ {
				probeSlot(grp, instance)
			}
		}
	}
	if readAny {
		for key, snapshot := range discovered {
			if snapshot != nil && snapshot.SlotMode == "registry" && !verified[key] {
				if fm5PositiveObserved && key.Group != remoteFunctionalModules.group {
					continue
				}
				delete(discovered, key)
			}
		}
	}

	liveFM5Evidence := hasFM5EvidenceFromRadioMap(discovered)
	p.mu.Lock()
	p.startupRadioDevicesProbed = readAny
	p.fm5IdentityScanComplete = fm5NamespaceComplete
	p.fm5IdentityIncoherent = !fm5NamespaceComplete && !liveFM5Evidence
	if fm5NamespaceComplete {
		p.fm5RegistryEvidenceIgnored = !liveFM5Evidence
	} else if liveFM5Evidence {
		p.fm5RegistryEvidenceIgnored = false
	}
	if len(discovered) == 0 {
		if readAny {
			p.radioDevices = make(map[radioDeviceKey]*vaillantRadioDeviceSnapshot)
			p.fm5EvidenceGeneration++
			if fm5NamespaceComplete {
				p.fm5IdentityObservedAt = p.now()
			}
		}
		p.mu.Unlock()
		if readAny {
			p.publishRadioDevices(semanticSnapshotSourceLive)
		}
		return
	}
	if p.radioDevices == nil || readAny {
		p.radioDevices = make(map[radioDeviceKey]*vaillantRadioDeviceSnapshot)
	}
	for key, snapshot := range discovered {
		p.radioDevices[key] = snapshot
	}
	if readAny {
		p.fm5EvidenceGeneration++
		if fm5NamespaceComplete || liveFM5Evidence {
			p.fm5IdentityObservedAt = p.now()
		}
	}
	p.mu.Unlock()
	source := semanticSnapshotSourceCache
	if readAny {
		source = semanticSnapshotSourceLive
	}
	p.publishRadioDevices(source)
}

func startupRadioFullScanGroups(discovered map[radioDeviceKey]*vaillantRadioDeviceSnapshot) map[byte]bool {
	seeded := make(map[byte]bool)
	highSeeded := make(map[byte]bool)
	for key := range discovered {
		seeded[key.Group] = true
		if key.Instance > semanticStartupSlotFastMaxInstance {
			highSeeded[key.Group] = true
		}
	}

	out := make(map[byte]bool)
	for _, grp := range remoteDeviceGroups {
		if !seeded[grp.group] || highSeeded[grp.group] {
			out[grp.group] = true
		}
	}
	return out
}

func startupRadioDeviceInclude(group byte, connected bool, classAddress *uint8) (bool, string) {
	switch group {
	case remoteRegulators.group, remoteThermostats.group:
		return connected, "startup"
	case remoteFunctionalModules.group:
		if connected {
			return true, "startup"
		}
		if classAddress != nil && *classAddress == circuitManagingDeviceVR71Address {
			return true, "inventory"
		}
		return false, "startup"
	default:
		return connected, "startup"
	}
}

func (p *vaillantSemanticPoller) registryRadioDeviceSeeds() map[radioDeviceKey]*vaillantRadioDeviceSnapshot {
	out := make(map[radioDeviceKey]*vaillantRadioDeviceSnapshot)
	if p == nil || p.reg == nil {
		return out
	}
	nextInstance := map[byte]byte{}
	p.reg.IterateSnapshots(func(snap registry.DeviceEntrySnapshot) bool {
		addr := ebusgateway.SnapshotTargetAddressForRouting(snap)
		deviceID := normalizeDeviceID(snap.DeviceID)
		var group byte
		switch {
		case strings.HasPrefix(deviceID, "BASV") || addr == 0x15:
			group = remoteRegulators.group
		case strings.HasPrefix(deviceID, "VR71") || strings.HasPrefix(deviceID, "FM5") || addr == circuitManagingDeviceVR71Address:
			group = remoteFunctionalModules.group
		case strings.HasPrefix(deviceID, "VR92"):
			group = remoteThermostats.group
		default:
			return true
		}
		instance := nextInstance[group]
		nextInstance[group] = instance + 1
		classAddress := uint8(addr)
		connected := true
		out[radioDeviceKey{Group: group, Instance: instance}] = &vaillantRadioDeviceSnapshot{
			Group:              group,
			Instance:           instance,
			SlotMode:           "registry",
			DeviceConnected:    &connected,
			DeviceClassAddress: &classAddress,
			DeviceModel:        decodeRadioDeviceModel(&classAddress),
		}
		return true
	})
	return out
}

func (p *vaillantSemanticPoller) readB524U8Startup(ctx context.Context, opcode, group, instance byte, addr uint16) *uint8 {
	raw, ok := p.readB524Startup(ctx, opcode, group, instance, addr)
	if !ok || len(raw) == 0 {
		return nil
	}
	value := raw[0]
	return &value
}

func (p *vaillantSemanticPoller) refreshFM5SemanticStartup(ctx context.Context) {
	if p == nil || p.provider == nil {
		return
	}

	evidence := p.captureFM5Evidence()
	moduleConfig := evidence.moduleConfig
	if evidence.configurationFailed {
		moduleConfig = nil
	}
	fm5GateSatisfied := moduleConfig != nil && *moduleConfig <= 2
	observedAt := p.now()
	evidenceStale := evidence.staleAt(observedAt, p.fm5EvidenceTTL)
	var incomingSolar *vaillantSolarSnapshot
	incomingCylinders := make(map[byte]*vaillantCylinderSnapshot)
	solarReadable := false
	cylindersReadable := false
	if evidence.controller != 0 && fm5GateSatisfied && !evidenceStale && !evidence.identityIncoherent {
		incomingSolar, solarReadable = p.readSolarSnapshotStartup(ctx)
		incomingCylinders, cylindersReadable = p.readCylinderSnapshotsStartup(ctx)
	}
	currentEvidence := p.captureFM5Evidence()
	incoherent := evidence.identityIncoherent || currentEvidence.identityIncoherent || !evidence.sameGeneration(currentEvidence)
	evidenceRevision := p.nextFM5EvidenceRevision(evidence.generation, currentEvidence.generation)
	negativeIdentityObserved := evidence.hasNegativeObservation() && currentEvidence.hasNegativeObservation()
	freshNegativeIdentityObserved := evidence.hasFreshNegativeObservation(observedAt, p.fm5EvidenceTTL) &&
		currentEvidence.hasFreshNegativeObservation(observedAt, p.fm5EvidenceTTL)
	var verdict graphql.Fm5Interpretation
	if freshNegativeIdentityObserved && !incoherent && evidence.controller != 0 && moduleConfig != nil {
		verdict = graphql.Fm5Interpretation{
			Mode:             graphql.Fm5SemanticModeAbsent,
			EvidenceRevision: evidenceRevision,
		}
	} else {
		verdict = deriveFM5Interpretation(
			evidence.controller != 0,
			moduleConfig,
			solarReadable,
			cylindersReadable,
			evidence.hasEvidence() || currentEvidence.hasEvidence() || negativeIdentityObserved,
			evidenceStale,
			incoherent,
			evidenceRevision,
		)
	}
	verdict = p.commitFM5Acquisition(currentEvidence, verdict, incomingSolar, incomingCylinders)
	for _, info := range fm5InventoryRegistryInfos(evidence.systemSnapshot, verdict.Mode) {
		p.reg.Register(preserveExistingRegistryMetadata(p.reg, info))
	}
	p.publishFM5Semantic(semanticSnapshotSourceLive)
}

func (p *vaillantSemanticPoller) readSolarSnapshotStartup(ctx context.Context) (*vaillantSolarSnapshot, bool) {
	incoming := &vaillantSolarSnapshot{}
	readAny := false
	if raw, ok := p.readB524Startup(ctx, localSolar.opcode, localSolar.group, solarInstance, solar_enabled); ok {
		incoming.SolarEnabled = decodeB524BoolFromRaw(raw)
		readAny = true
	}
	if raw, ok := p.readB524Startup(ctx, localSolar.opcode, localSolar.group, solarInstance, solar_collector_temp); ok {
		incoming.CollectorTemperatureC = decodeB524Float32FromRaw(raw)
		readAny = true
	}
	if raw, ok := p.readB524Startup(ctx, localSolar.opcode, localSolar.group, solarInstance, solar_pump_active); ok {
		incoming.PumpActive = decodeB524BoolFromRaw(raw)
		readAny = true
	}
	if !readAny {
		return nil, false
	}
	return incoming, true
}

func (p *vaillantSemanticPoller) readCylinderSnapshotsStartup(ctx context.Context) (map[byte]*vaillantCylinderSnapshot, bool) {
	out := make(map[byte]*vaillantCylinderSnapshot, 2)
	for instance := byte(0x00); instance <= 0x01; instance++ {
		raw, ok := p.readB524Startup(ctx, localCylinders.opcode, localCylinders.group, instance, cylinder_temperature)
		if !ok {
			continue
		}
		out[instance] = &vaillantCylinderSnapshot{
			Instance:     instance,
			TemperatureC: decodeB524Float32FromRaw(raw),
		}
	}
	if len(out) == 0 {
		return nil, false
	}
	return out, true
}

func (p *vaillantSemanticPoller) refreshBoilerStatusStartup(ctx context.Context) {
	if p == nil {
		return
	}

	p.mu.Lock()
	boilerAddress := p.boilerAddress
	p.mu.Unlock()
	if boilerAddress == 0 {
		boilerAddress = p.findBoilerAddress()
	}

	snapshot := &vaillantBoilerSnapshot{}
	updated := false
	if boilerAddress != 0 {
		probeCtx, cancel := p.startupProbeContext(ctx)
		if value := p.readB509DATA2c(probeCtx, boilerAddress, boiler_b509_flow_temperature); value != nil {
			snapshot.FlowTemperatureC = value
			updated = true
		}
		cancel()
	}
	if raw, ok := p.readB524Startup(ctx, localDHW.opcode, localDHW.group, dhwInstance, dhw_current_temp); ok {
		if value := decodeB524Float32FromRaw(raw); value != nil {
			snapshot.DhwTemperatureC = value
			updated = true
		}
	}

	p.mu.Lock()
	if boilerAddress != 0 {
		p.boilerAddress = boilerAddress
	}
	source := semanticSnapshotSourceCache
	if updated {
		p.boiler = mergeBoilerSnapshotNonDestructive(p.boiler, snapshot)
		source = semanticSnapshotSourceLive
	}
	hasSnapshot := p.boiler != nil
	p.mu.Unlock()
	if hasSnapshot {
		p.publishBoilerStatus(source)
	}
}
