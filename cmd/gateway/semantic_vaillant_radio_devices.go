package main

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
)

// deviceSlotKey identifies a single device slot by its OP=0x06 group+instance.
type deviceSlotKey struct {
	Group    byte
	Instance byte
}

// discoverDeviceSlots probes all OP=0x06 device slot groups to find which
// slots should be retained for steady-state detail refresh. Connected slots
// are retained. Disconnected functional module slots are retained only when
// identity registers prove inventory, preserving FM5 evidence without
// refreshing empty regulator/thermostat slots every config tick.
// This runs at startup and every deviceSlotRediscoveryTTL to avoid
// probing empty slots on every poll cycle (~30 timeouts eliminated).
func (p *vaillantSemanticPoller) discoverDeviceSlots(ctx context.Context) (map[deviceSlotKey]bool, bool) {
	active, observedAny, _, _ := p.discoverDeviceSlotsWithFM5Completeness(ctx)
	return active, observedAny
}

func (p *vaillantSemanticPoller) discoverDeviceSlotsWithFM5Completeness(ctx context.Context) (map[deviceSlotKey]bool, bool, bool, map[radioDeviceKey]*vaillantRadioDeviceSnapshot) {
	active := make(map[deviceSlotKey]bool)
	observedAny := false
	fm5NamespaceComplete := true
	fm5Snapshots := make(map[radioDeviceKey]*vaillantRadioDeviceSnapshot)
	for _, grp := range remoteDeviceGroups {
		for instance := byte(0x00); instance <= 0x0A; instance++ {
			if grp.group == remoteFunctionalModules.group {
				snapshot, observed, complete := p.readFunctionalModuleIdentitySnapshot(ctx, instance)
				if !observed {
					fm5NamespaceComplete = false
					continue
				}
				observedAny = true
				if !complete {
					fm5NamespaceComplete = false
				}
				if snapshot != nil {
					active[deviceSlotKey{Group: grp.group, Instance: instance}] = true
					fm5Snapshots[radioDeviceKey{Group: grp.group, Instance: instance}] = snapshot
				}
				continue
			}

			connectedRaw := p.readB524U8(ctx, grp.opcode, grp.group, instance, device_slot_connected)
			if connectedRaw == nil {
				continue
			}
			observedAny = true
			if *connectedRaw == 1 {
				active[deviceSlotKey{Group: grp.group, Instance: instance}] = true
			}
		}
	}
	return active, observedAny, fm5NamespaceComplete, fm5Snapshots
}

// radioSlotDiscovery is the outcome of the device slot discovery phase of
// one radio device refresh.
type radioSlotDiscovery struct {
	needed       bool
	observed     bool
	fm5Complete  bool
	activeSlots  map[deviceSlotKey]bool
	fm5Snapshots map[radioDeviceKey]*vaillantRadioDeviceSnapshot
}

// radioSlotDetailScan is the outcome of the detail read phase of one radio
// device refresh over the cached active slots.
type radioSlotDetailScan struct {
	discovered           map[radioDeviceKey]*vaillantRadioDeviceSnapshot
	readAny              bool
	fm5SlotReadAttempted bool
	fm5DetailIncomplete  bool
}

func (p *vaillantSemanticPoller) refreshRadioDevices(ctx context.Context) {
	if p == nil {
		return
	}

	p.mu.Lock()
	controller := p.controller
	needsDiscovery := !p.deviceSlotDiscoveryDone ||
		(p.deviceSlotRediscoveryTTL > 0 && p.now().Sub(p.deviceSlotDiscoveryAt) >= p.deviceSlotRediscoveryTTL)
	p.mu.Unlock()
	if controller == 0 {
		return
	}

	// Phase 1: Device slot discovery — full scan of all OP=0x06 groups.
	// Runs at startup and every deviceSlotRediscoveryTTL (30min default).
	discovery := p.discoverRadioSlots(ctx, needsDiscovery)

	p.mu.Lock()
	slots := p.deviceSlotCache
	p.mu.Unlock()

	if len(slots) == 0 {
		if discovery.observed {
			p.clearRadioDevicesWithoutSlots(discovery.fm5Complete)
			p.publishRadioDevices(semanticSnapshotSourceLive)
			p.refreshFM5Semantic(ctx)
		}
		return
	}

	// Phase 2: Read detail registers for cached active slots only.
	scan := radioSlotDetailScan{discovered: make(map[radioDeviceKey]*vaillantRadioDeviceSnapshot)}
	for key := range slots {
		if key.Group == remoteFunctionalModules.group {
			p.readFunctionalModuleSlotDetails(ctx, key, discovery, &scan)
			continue
		}
		p.readRadioSlotDetails(ctx, key, discovery, &scan)
	}

	completePhaseOneFM5Negative := discovery.needed && discovery.observed && discovery.fm5Complete && len(discovery.fm5Snapshots) == 0
	if !scan.readAny && len(discovery.fm5Snapshots) == 0 && !scan.fm5SlotReadAttempted && !completePhaseOneFM5Negative {
		return
	}

	for _, snapshot := range scan.discovered {
		info, ok := radioInventoryRegistryInfo(snapshot)
		if !ok {
			continue
		}
		p.reg.Register(preserveExistingRegistryMetadata(p.reg, info))
	}

	p.commitRadioDeviceScan(discovery, scan)
	p.publishRadioDevices(semanticSnapshotSourceLive)
	p.refreshFM5Semantic(ctx)
}

// discoverRadioSlots runs the device slot discovery phase when it is due and
// caches the active slots when any slot was observed.
func (p *vaillantSemanticPoller) discoverRadioSlots(ctx context.Context, needsDiscovery bool) radioSlotDiscovery {
	discovery := radioSlotDiscovery{
		needed:       needsDiscovery,
		activeSlots:  make(map[deviceSlotKey]bool),
		fm5Snapshots: make(map[radioDeviceKey]*vaillantRadioDeviceSnapshot),
	}
	if !needsDiscovery {
		return discovery
	}
	activeSlots, observedAny, complete, snapshots := p.discoverDeviceSlotsWithFM5Completeness(ctx)
	discovery.observed = observedAny
	discovery.fm5Complete = complete
	discovery.activeSlots = activeSlots
	discovery.fm5Snapshots = snapshots
	if observedAny {
		p.mu.Lock()
		p.deviceSlotCache = activeSlots
		p.deviceSlotDiscoveryDone = true
		p.deviceSlotDiscoveryAt = p.now()
		p.mu.Unlock()
	}
	return discovery
}

// clearRadioDevicesWithoutSlots drops the radio devices after an observed
// discovery found no active slot. Functional module devices are retained
// unless their namespace scan was complete.
func (p *vaillantSemanticPoller) clearRadioDevicesWithoutSlots(fm5ScanComplete bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	retained := make(map[radioDeviceKey]*vaillantRadioDeviceSnapshot)
	if !fm5ScanComplete {
		for key, snapshot := range p.radioDevices {
			if key.Group == remoteFunctionalModules.group {
				retained[key] = snapshot
			}
		}
	}
	p.radioDevices = retained
	p.fm5EvidenceGeneration++
	p.fm5IdentityScanComplete = fm5ScanComplete
	p.fm5IdentityIncoherent = !fm5ScanComplete
	if fm5ScanComplete {
		p.fm5RegistryEvidenceIgnored = true
		p.fm5IdentityObservedAt = p.now()
	}
}

// readFunctionalModuleSlotDetails reads the identity and, for a connected
// module, the detail registers of one functional module slot.
func (p *vaillantSemanticPoller) readFunctionalModuleSlotDetails(ctx context.Context, key deviceSlotKey, discovery radioSlotDiscovery, scan *radioSlotDetailScan) {
	group := key.Group
	instance := key.Instance
	scan.fm5SlotReadAttempted = true
	device, observed, complete := p.readFunctionalModuleIdentitySnapshot(ctx, instance)
	if !observed {
		scan.fm5DetailIncomplete = true
		return
	}
	scan.readAny = true
	radioKey := radioDeviceKey{Group: group, Instance: instance}
	if !complete {
		scan.fm5DetailIncomplete = true
		if phaseOne := discovery.fm5Snapshots[radioKey]; phaseOne != nil {
			scan.discovered[radioKey] = phaseOne
		} else if device != nil {
			scan.discovered[radioKey] = device
		}
		return
	}
	if phaseOne := discovery.fm5Snapshots[radioKey]; phaseOne != nil && device == nil {
		scan.fm5DetailIncomplete = true
		scan.discovered[radioKey] = phaseOne
		return
	}
	if device == nil {
		return
	}
	if device.DeviceConnected != nil && *device.DeviceConnected {
		p.readConnectedRadioSlotDetails(ctx, device, group, instance)
	}
	scan.discovered[radioKey] = device
}

// readRadioSlotDetails reads one regulator or thermostat slot. A slot whose
// connection read fails keeps its previous snapshot when discovery has just
// reported it active.
func (p *vaillantSemanticPoller) readRadioSlotDetails(ctx context.Context, key deviceSlotKey, discovery radioSlotDiscovery, scan *radioSlotDetailScan) {
	group := key.Group
	instance := key.Instance
	opcode := vaillantB524OpcodeRead
	connectedRaw := p.readB524U8(ctx, opcode, group, instance, device_slot_connected)
	if connectedRaw == nil {
		if discovery.activeSlots[key] {
			p.mu.Lock()
			previous := cloneRadioSnapshot(p.radioDevices[radioDeviceKey{Group: group, Instance: instance}])
			p.mu.Unlock()
			if previous != nil {
				scan.discovered[radioDeviceKey{Group: group, Instance: instance}] = previous
			}
		}
		return
	}
	scan.readAny = true

	connected := *connectedRaw == 1
	classAddress := p.readB524U8(ctx, opcode, group, instance, device_slot_class_address)
	firmware := p.readB524Firmware(ctx, opcode, group, instance, device_slot_firmware)
	hardware := p.readB524U16(ctx, opcode, group, instance, device_slot_hardware_identifier)

	slotMode := "active"
	include := false
	switch group {
	case remoteRegulators.group, remoteThermostats.group:
		include = connected
	default:
		include = connected
	}
	if !include {
		return
	}

	device := &vaillantRadioDeviceSnapshot{
		Group:              group,
		Instance:           instance,
		SlotMode:           slotMode,
		DeviceConnected:    cloneBoilerBoolPtr(&connected),
		DeviceClassAddress: cloneUint8Ptr(classAddress),
		DeviceModel:        decodeRadioDeviceModel(classAddress),
		FirmwareVersion:    cloneStringPtr(firmware),
		HardwareIdentifier: cloneUint16Ptr(hardware),
	}
	if connected {
		p.readConnectedRadioSlotDetails(ctx, device, group, instance)
	}
	scan.discovered[radioDeviceKey{Group: group, Instance: instance}] = device
}

// readConnectedRadioSlotDetails reads the pairing, reception, zone and room
// registers of a connected device slot in a fixed order.
func (p *vaillantSemanticPoller) readConnectedRadioSlotDetails(ctx context.Context, device *vaillantRadioDeviceSnapshot, group, instance byte) {
	opcode := vaillantB524OpcodeRead
	device.RemoteControlAddress = p.readB524U8(ctx, opcode, group, instance, device_slot_remote_control_address)
	device.DevicePaired = p.readB524Bool(ctx, opcode, group, instance, device_slot_paired)
	device.ReceptionStrength = p.readB524U8(ctx, opcode, group, instance, device_slot_reception_strength)
	device.ZoneAssignment = p.readB524U8(ctx, opcode, group, instance, device_slot_zone_assignment)
	device.RoomTemperatureC = p.readB524F32(ctx, opcode, group, instance, device_slot_room_temperature)
	device.RoomHumidityPct = p.readB524F32(ctx, opcode, group, instance, device_slot_room_humidity)
}

// commitRadioDeviceScan replaces the retained radio devices with the scan
// result and records the functional module identity evidence it carries.
func (p *vaillantSemanticPoller) commitRadioDeviceScan(discovery radioSlotDiscovery, scan radioSlotDetailScan) {
	discovered := scan.discovered
	needsDiscovery := discovery.needed
	fm5SlotReadAttempted := scan.fm5SlotReadAttempted
	currentFM5Evidence := hasFM5EvidenceFromRadioMap(discovered)
	authoritativeFM5Snapshot := needsDiscovery && discovery.observed && discovery.fm5Complete && !scan.fm5DetailIncomplete
	fm5IdentityIncoherent := scan.fm5DetailIncomplete ||
		(needsDiscovery && !authoritativeFM5Snapshot && !currentFM5Evidence) ||
		(!needsDiscovery && fm5SlotReadAttempted && !currentFM5Evidence)
	p.mu.Lock()
	defer p.mu.Unlock()
	if !authoritativeFM5Snapshot {
		for key, snapshot := range p.radioDevices {
			if key.Group == remoteFunctionalModules.group && discovered[key] == nil {
				discovered[key] = snapshot
			}
		}
	}
	p.radioDevices = discovered
	p.fm5EvidenceGeneration++
	if needsDiscovery || fm5SlotReadAttempted {
		p.fm5IdentityScanComplete = authoritativeFM5Snapshot
		p.fm5IdentityIncoherent = fm5IdentityIncoherent
		if authoritativeFM5Snapshot {
			p.fm5RegistryEvidenceIgnored = !currentFM5Evidence
			p.fm5IdentityObservedAt = p.now()
		} else if currentFM5Evidence {
			p.fm5RegistryEvidenceIgnored = false
			if !fm5IdentityIncoherent {
				p.fm5IdentityObservedAt = p.now()
			}
		}
	}
}

// radioDeviceViewsLocked returns detached copies of the retained radio
// devices ordered by group and instance. The caller holds p.mu.
func (p *vaillantSemanticPoller) radioDeviceViewsLocked() []*vaillantRadioDeviceSnapshot {
	keys := make([]radioDeviceKey, 0, len(p.radioDevices))
	for key := range p.radioDevices {
		keys = append(keys, key)
	}
	slices.SortFunc(keys, func(a, b radioDeviceKey) int {
		if a.Group != b.Group {
			if a.Group < b.Group {
				return -1
			}
			return 1
		}
		if a.Instance < b.Instance {
			return -1
		}
		if a.Instance > b.Instance {
			return 1
		}
		return 0
	})
	snapshots := make([]*vaillantRadioDeviceSnapshot, 0, len(keys))
	for _, key := range keys {
		snapshot := p.radioDevices[key]
		if snapshot == nil {
			continue
		}
		snapshots = append(snapshots, cloneRadioSnapshot(snapshot))
	}
	return snapshots
}

func cloneRadioSnapshot(snapshot *vaillantRadioDeviceSnapshot) *vaillantRadioDeviceSnapshot {
	if snapshot == nil {
		return nil
	}
	return &vaillantRadioDeviceSnapshot{
		Group:                snapshot.Group,
		Instance:             snapshot.Instance,
		SlotMode:             snapshot.SlotMode,
		DeviceConnected:      cloneBoilerBoolPtr(snapshot.DeviceConnected),
		DeviceClassAddress:   cloneUint8Ptr(snapshot.DeviceClassAddress),
		DeviceModel:          snapshot.DeviceModel,
		FirmwareVersion:      cloneStringPtr(snapshot.FirmwareVersion),
		HardwareIdentifier:   cloneUint16Ptr(snapshot.HardwareIdentifier),
		RemoteControlAddress: cloneUint8Ptr(snapshot.RemoteControlAddress),
		DevicePaired:         cloneBoilerBoolPtr(snapshot.DevicePaired),
		ReceptionStrength:    cloneUint8Ptr(snapshot.ReceptionStrength),
		ZoneAssignment:       cloneUint8Ptr(snapshot.ZoneAssignment),
		RoomTemperatureC:     cloneFloat64Ptr(snapshot.RoomTemperatureC),
		RoomHumidityPct:      cloneFloat64Ptr(snapshot.RoomHumidityPct),
	}
}

func radioInventoryRegistryInfo(snapshot *vaillantRadioDeviceSnapshot) (registry.DeviceInfo, bool) {
	if snapshot == nil || snapshot.DeviceClassAddress == nil {
		return registry.DeviceInfo{}, false
	}
	switch *snapshot.DeviceClassAddress {
	case circuitManagingDeviceVR71Address:
		return registry.DeviceInfo{
			Address:      circuitManagingDeviceVR71Address,
			Manufacturer: "Vaillant",
			DeviceID:     circuitManagingDeviceVR71ID,
		}, true
	default:
		return registry.DeviceInfo{}, false
	}
}

func preserveExistingRegistryMetadata(reg *registry.DeviceRegistry, info registry.DeviceInfo) registry.DeviceInfo {
	if reg == nil {
		return info
	}
	// P9.3 — race-free identity-field reads via IterateSnapshots. The
	// previous Iterate path read entry.Manufacturer / DeviceID /
	// SerialNumber / etc. lock-free outside the registry RLock; under
	// concurrent Register writes those string reads could tear.
	reg.IterateSnapshots(func(snap registry.DeviceEntrySnapshot) bool {
		if snap.PrimaryAddress != info.Address {
			return true
		}
		if info.Manufacturer == "" {
			info.Manufacturer = snap.Manufacturer
		}
		if info.DeviceID == "" {
			info.DeviceID = snap.DeviceID
		}
		if info.SerialNumber == "" {
			info.SerialNumber = snap.SerialNumber
		}
		if info.MacAddress == "" {
			info.MacAddress = snap.MacAddress
		}
		if info.SoftwareVersion == "" {
			info.SoftwareVersion = snap.SoftwareVersion
		}
		if info.HardwareVersion == "" {
			info.HardwareVersion = snap.HardwareVersion
		}
		return false
	})
	return info
}

func decodeRadioDeviceModel(classAddress *uint8) string {
	if classAddress == nil {
		return ""
	}
	switch *classAddress {
	case 0x15:
		return "VRC720"
	case 0x26:
		return "VR71/FM5"
	case 0x35:
		return "VR92"
	default:
		return fmt.Sprintf("Unknown (0x%02X)", *classAddress)
	}
}

func hasRemoteIdentityEvidence(classAddress *uint8, firmware *string, hardware *uint16) bool {
	if classAddress != nil && *classAddress == 0x26 {
		return true
	}
	if firmware != nil && strings.TrimSpace(*firmware) != "" {
		return true
	}
	if hardware != nil && *hardware != 0 && *hardware != 0xFFFF {
		return true
	}
	return false
}
