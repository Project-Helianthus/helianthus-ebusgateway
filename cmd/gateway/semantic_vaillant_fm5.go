package main

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
)

func (p *vaillantSemanticPoller) readFunctionalModuleIdentitySnapshot(ctx context.Context, instance byte) (*vaillantRadioDeviceSnapshot, bool, bool) {
	return p.readFunctionalModuleIdentitySnapshotWith(ctx, instance, p.readB524Value)
}

type functionalModuleIdentityReadFunc func(context.Context, byte, byte, byte, uint16) ([]byte, bool)

type functionalModuleIdentityProbe struct {
	instance     byte
	connectedRaw []byte
	connectedOK  bool
	classRaw     []byte
	classOK      bool
	completed    bool
}

type functionalModuleIdentityScanResult struct {
	snapshots         map[byte]*vaillantRadioDeviceSnapshot
	observedAny       bool
	namespaceComplete bool
	positiveInstance  byte
	positiveSnapshot  *vaillantRadioDeviceSnapshot
}

func (p *vaillantSemanticPoller) scanFunctionalModuleIdentityBudgetAwareWith(
	ctx context.Context,
	firstInstance byte,
	lastInstance byte,
	read functionalModuleIdentityReadFunc,
) functionalModuleIdentityScanResult {
	result := functionalModuleIdentityScanResult{
		snapshots:         make(map[byte]*vaillantRadioDeviceSnapshot),
		namespaceComplete: true,
	}
	if lastInstance < firstInstance {
		result.namespaceComplete = false
		return result
	}
	probes := make([]functionalModuleIdentityProbe, 0, int(lastInstance-firstInstance)+1)
	preflight := func(first byte, last byte) {
		for instance := first; instance <= last; instance++ {
			connectedRaw, connectedOK := read(ctx, vaillantB524OpcodeRead, remoteFunctionalModules.group, instance, device_slot_connected)
			classRaw, classOK := read(ctx, vaillantB524OpcodeRead, remoteFunctionalModules.group, instance, device_slot_class_address)
			if connectedOK && len(connectedRaw) > 0 {
				result.observedAny = true
			}
			probes = append(probes, functionalModuleIdentityProbe{
				instance:     instance,
				connectedRaw: connectedRaw,
				connectedOK:  connectedOK,
				classRaw:     classRaw,
				classOK:      classOK,
			})
		}
	}

	completeProbe := func(probe *functionalModuleIdentityProbe) functionalModuleIdentityTuple {
		firmwareRaw, firmwareOK := read(ctx, vaillantB524OpcodeRead, remoteFunctionalModules.group, probe.instance, device_slot_firmware)
		hardwareRaw, hardwareOK := read(ctx, vaillantB524OpcodeRead, remoteFunctionalModules.group, probe.instance, device_slot_hardware_identifier)
		probe.completed = true
		tuple := decodeFunctionalModuleIdentityTuple(
			probe.connectedRaw, probe.connectedOK,
			probe.classRaw, probe.classOK,
			firmwareRaw, firmwareOK,
			hardwareRaw, hardwareOK,
		)
		if !tuple.complete {
			result.namespaceComplete = false
		}
		if snapshot := functionalModuleIdentitySnapshot(probe.instance, tuple); snapshot != nil {
			result.snapshots[probe.instance] = snapshot
		}
		return tuple
	}

	completeCandidates := func(firstProbe int) bool {
		for i := firstProbe; i < len(probes); i++ {
			probe := &probes[i]
			if !probe.classOK || len(probe.classRaw) == 0 || probe.classRaw[0] != circuitManagingDeviceVR71Address {
				continue
			}
			tuple := completeProbe(probe)
			if completeVR71IdentityTuple(tuple) {
				result.namespaceComplete = false
				result.positiveInstance = probe.instance
				result.positiveSnapshot = result.snapshots[probe.instance]
				return true
			}
		}
		return false
	}

	fastLast := lastInstance
	if fastLast > semanticStartupSlotFastMaxInstance {
		fastLast = semanticStartupSlotFastMaxInstance
	}
	preflight(firstInstance, fastLast)
	if completeCandidates(0) {
		return result
	}
	if fastLast < lastInstance {
		firstTailProbe := len(probes)
		preflight(fastLast+1, lastInstance)
		if completeCandidates(firstTailProbe) {
			return result
		}
	}

	for i := range probes {
		if !probes[i].completed {
			completeProbe(&probes[i])
		}
	}
	return result
}

func (p *vaillantSemanticPoller) readFunctionalModuleIdentitySnapshotWith(
	ctx context.Context,
	instance byte,
	read functionalModuleIdentityReadFunc,
) (*vaillantRadioDeviceSnapshot, bool, bool) {
	connectedRaw, connectedOK := read(ctx, vaillantB524OpcodeRead, remoteFunctionalModules.group, instance, device_slot_connected)
	if !connectedOK || len(connectedRaw) == 0 {
		return nil, false, false
	}
	classRaw, classOK := read(ctx, vaillantB524OpcodeRead, remoteFunctionalModules.group, instance, device_slot_class_address)
	firmwareRaw, firmwareOK := read(ctx, vaillantB524OpcodeRead, remoteFunctionalModules.group, instance, device_slot_firmware)
	hardwareRaw, hardwareOK := read(ctx, vaillantB524OpcodeRead, remoteFunctionalModules.group, instance, device_slot_hardware_identifier)
	tuple := decodeFunctionalModuleIdentityTuple(
		connectedRaw, connectedOK,
		classRaw, classOK,
		firmwareRaw, firmwareOK,
		hardwareRaw, hardwareOK,
	)
	return functionalModuleIdentitySnapshot(instance, tuple), true, tuple.complete
}

func functionalModuleIdentitySnapshot(instance byte, tuple functionalModuleIdentityTuple) *vaillantRadioDeviceSnapshot {
	if tuple.classAddress != nil && *tuple.classAddress != circuitManagingDeviceVR71Address {
		return nil
	}
	if !tuple.connected && !hasRemoteIdentityEvidence(tuple.classAddress, tuple.firmware, tuple.hardware) {
		return nil
	}
	slotMode := "active"
	if !tuple.connected {
		slotMode = "inventory"
	}
	return &vaillantRadioDeviceSnapshot{
		Group:              remoteFunctionalModules.group,
		Instance:           instance,
		SlotMode:           slotMode,
		DeviceConnected:    cloneBoilerBoolPtr(&tuple.connected),
		DeviceClassAddress: cloneUint8Ptr(tuple.classAddress),
		DeviceModel:        decodeRadioDeviceModel(tuple.classAddress),
		FirmwareVersion:    cloneStringPtr(tuple.firmware),
		HardwareIdentifier: cloneUint16Ptr(tuple.hardware),
	}
}

func (p *vaillantSemanticPoller) refreshFM5AfterLateIdentity(ctx context.Context) {
	if p == nil {
		return
	}
	scan := p.scanFunctionalModuleIdentityBudgetAwareWith(ctx, 0x00, semanticStartupSlotFullMaxInstance, p.readB524Startup)
	if scan.positiveSnapshot == nil {
		return
	}
	instance := scan.positiveInstance
	snapshot := scan.positiveSnapshot

	deviceKey := deviceSlotKey{Group: remoteFunctionalModules.group, Instance: instance}
	radioKey := radioDeviceKey{Group: remoteFunctionalModules.group, Instance: instance}
	p.mu.Lock()
	if p.deviceSlotCache == nil {
		p.deviceSlotCache = make(map[deviceSlotKey]bool)
	}
	if p.radioDevices == nil {
		p.radioDevices = make(map[radioDeviceKey]*vaillantRadioDeviceSnapshot)
	}
	p.deviceSlotCache[deviceKey] = true
	p.radioDevices[radioKey] = snapshot
	p.fm5EvidenceGeneration++
	p.fm5IdentityScanComplete = false
	p.fm5IdentityIncoherent = false
	p.fm5RegistryEvidenceIgnored = false
	p.fm5IdentityObservedAt = p.now()
	p.mu.Unlock()

	p.publishRadioDevices(semanticSnapshotSourceLive)
	p.refreshFM5Semantic(ctx)
}

type functionalModuleIdentityTuple struct {
	connected    bool
	classAddress *uint8
	firmware     *string
	hardware     *uint16
	complete     bool
}

func completeVR71IdentityTuple(tuple functionalModuleIdentityTuple) bool {
	return tuple.complete && tuple.classAddress != nil && *tuple.classAddress == circuitManagingDeviceVR71Address
}

func decodeFunctionalModuleIdentityTuple(
	connectedRaw []byte,
	connectedOK bool,
	classRaw []byte,
	classOK bool,
	firmwareRaw []byte,
	firmwareOK bool,
	hardwareRaw []byte,
	hardwareOK bool,
) functionalModuleIdentityTuple {
	connected, connectedValid := decodeExactB524Bool(connectedRaw)
	tuple := functionalModuleIdentityTuple{
		connected: connected,
		complete: connectedOK && connectedValid &&
			classOK && len(classRaw) >= 1 &&
			firmwareOK && len(firmwareRaw) >= 3 &&
			hardwareOK && len(hardwareRaw) >= 2,
	}
	if classOK && len(classRaw) >= 1 && classRaw[0] != 0xFF {
		value := classRaw[0]
		tuple.classAddress = &value
	}
	if firmwareOK && len(firmwareRaw) >= 3 {
		tuple.firmware = decodeB524FirmwareVersion(firmwareRaw)
	}
	if hardwareOK && len(hardwareRaw) >= 2 {
		if value, ok := decodeB524Uint16(hardwareRaw); ok && value != 0xFFFF {
			tuple.hardware = &value
		}
	}
	if tuple.connected && !hasRemoteIdentityEvidence(tuple.classAddress, tuple.firmware, tuple.hardware) {
		tuple.complete = false
	}
	return tuple
}

func (p *vaillantSemanticPoller) refreshFM5Semantic(ctx context.Context) {
	if p == nil || p.liveProvider() == nil {
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
		incomingSolar, solarReadable = p.readSolarSnapshot(ctx)
		incomingCylinders, cylindersReadable = p.readCylinderSnapshots(ctx)
	}

	currentEvidence := p.captureFM5Evidence()
	incoherent := evidence.identityIncoherent || currentEvidence.identityIncoherent || !evidence.sameGeneration(currentEvidence)
	evidenceRevision := p.nextFM5EvidenceRevision(evidence.generation, currentEvidence.generation)
	negativeIdentityObserved := evidence.hasNegativeObservation() && currentEvidence.hasNegativeObservation()
	freshNegativeIdentityObserved := evidence.hasFreshNegativeObservation(observedAt, p.fm5EvidenceTTL) &&
		currentEvidence.hasFreshNegativeObservation(observedAt, p.fm5EvidenceTTL)
	var verdict vaillantFM5Interpretation
	if freshNegativeIdentityObserved && !incoherent && evidence.controller != 0 && moduleConfig != nil {
		verdict = vaillantFM5Interpretation{
			Mode:             vaillantFM5ModeAbsent,
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

// vaillantFM5Mode is the acquisition verdict on the function module: its
// solar and cylinder planes are interpreted, it is known only at GPIO level,
// or it is absent. The zero value means that no verdict was reached yet. The
// publication owner maps it to the published mode; the values are spelled
// like the published ones so that diagnostics read the same.
type vaillantFM5Mode string

const (
	vaillantFM5ModeInterpreted vaillantFM5Mode = "INTERPRETED"
	vaillantFM5ModeGPIOOnly    vaillantFM5Mode = "GPIO_ONLY"
	vaillantFM5ModeAbsent      vaillantFM5Mode = "ABSENT"
)

// vaillantFM5DegradedReason says why an acquisition did not reach, or did
// not keep, an interpreted verdict. Every reason except a configuration that
// is not interpretable is transient. The zero value means not degraded.
type vaillantFM5DegradedReason string

const (
	vaillantFM5DegradedControllerUnreachable         vaillantFM5DegradedReason = "CONTROLLER_UNREACHABLE"
	vaillantFM5DegradedConfigurationUnavailable      vaillantFM5DegradedReason = "CONFIGURATION_UNAVAILABLE"
	vaillantFM5DegradedConfigurationNotInterpretable vaillantFM5DegradedReason = "CONFIGURATION_NOT_INTERPRETABLE"
	vaillantFM5DegradedSolarAcquisitionFailed        vaillantFM5DegradedReason = "SOLAR_ACQUISITION_FAILED"
	vaillantFM5DegradedCylinderAcquisitionFailed     vaillantFM5DegradedReason = "CYLINDER_ACQUISITION_FAILED"
	vaillantFM5DegradedEvidenceStale                 vaillantFM5DegradedReason = "EVIDENCE_STALE"
	vaillantFM5DegradedIncoherentAcquisition         vaillantFM5DegradedReason = "INCOHERENT_ACQUISITION"
)

// vaillantFM5Interpretation is one acquisition verdict: the mode, the reason
// it is degraded, and the revision of the identity evidence it was derived
// from.
type vaillantFM5Interpretation struct {
	Mode             vaillantFM5Mode
	DegradedReason   vaillantFM5DegradedReason
	EvidenceRevision string
}

// valid reports whether the verdict is complete: it names its evidence
// revision, a retained interpreted or absent mode carries at most a transient
// reason, and a GPIO-only mode carries the not-interpretable configuration
// reason. The publication accepts exactly the valid verdicts.
func (verdict vaillantFM5Interpretation) valid() bool {
	if strings.TrimSpace(verdict.EvidenceRevision) == "" {
		return false
	}
	switch verdict.Mode {
	case vaillantFM5ModeInterpreted, vaillantFM5ModeAbsent:
		return verdict.DegradedReason == "" || isFM5TransientAcquisitionReason(verdict.DegradedReason)
	case vaillantFM5ModeGPIOOnly:
		return verdict.DegradedReason == vaillantFM5DegradedConfigurationNotInterpretable
	default:
		return false
	}
}

func deriveFM5SemanticMode(controllerReachable, fm5GateSatisfied, solarReadable, cylindersReadable, hasEvidence bool) vaillantFM5Mode {
	if controllerReachable && fm5GateSatisfied && solarReadable && cylindersReadable {
		return vaillantFM5ModeInterpreted
	}
	if hasEvidence {
		return vaillantFM5ModeGPIOOnly
	}
	return vaillantFM5ModeAbsent
}

type fm5EvidenceCapture struct {
	controller                  byte
	moduleConfig                *uint16
	systemSnapshot              *vaillantSystemSnapshot
	radioSnapshots              []*vaillantRadioDeviceSnapshot
	registryEvidence            string
	registryGeneration          uint64
	registryCoherent            bool
	registryEvidenceIgnored     bool
	identityAcquisitionComplete bool
	identityIncoherent          bool
	configurationFailed         bool
	identityObservedAt          time.Time
	generation                  uint64
}

func (p *vaillantSemanticPoller) captureFM5Evidence() fm5EvidenceCapture {
	if p == nil {
		return fm5EvidenceCapture{}
	}
	p.mu.Lock()
	capture := fm5EvidenceCapture{
		controller:                  p.controller,
		systemSnapshot:              cloneSystemSnapshot(p.system),
		registryEvidenceIgnored:     p.fm5RegistryEvidenceIgnored,
		identityAcquisitionComplete: p.fm5IdentityScanComplete,
		identityIncoherent:          p.fm5IdentityIncoherent,
		configurationFailed:         p.fm5ConfigurationFailed,
		identityObservedAt:          p.fm5IdentityObservedAt,
		generation:                  p.fm5EvidenceGeneration,
		radioSnapshots:              make([]*vaillantRadioDeviceSnapshot, 0, len(p.radioDevices)),
	}
	if p.system != nil {
		capture.moduleConfig = cloneUint16Ptr(p.system.ModuleConfigurationVR71)
	}
	for _, snapshot := range p.radioDevices {
		if snapshot != nil {
			capture.radioSnapshots = append(capture.radioSnapshots, cloneRadioSnapshot(snapshot))
		}
	}
	p.mu.Unlock()

	capture.registryEvidence, capture.registryGeneration, capture.registryCoherent = p.captureFM5RegistryEvidence()
	if capture.hasEvidence() && capture.identityObservedAt.IsZero() {
		observedAt := p.now()
		p.mu.Lock()
		if p.fm5IdentityObservedAt.IsZero() {
			p.fm5IdentityObservedAt = observedAt
		}
		capture.identityObservedAt = p.fm5IdentityObservedAt
		capture.generation = p.fm5EvidenceGeneration
		p.mu.Unlock()
	}
	return capture
}

func (capture fm5EvidenceCapture) hasEvidence() bool {
	if hasFM5EvidenceFromRadioSnapshots(capture.radioSnapshots) {
		return true
	}
	return !capture.registryEvidenceIgnored && capture.registryEvidence != ""
}

func (capture fm5EvidenceCapture) staleAt(now time.Time, ttl time.Duration) bool {
	if (!capture.hasEvidence() && !capture.identityAcquisitionComplete) || capture.identityObservedAt.IsZero() || ttl <= 0 {
		return false
	}
	return now.Sub(capture.identityObservedAt) > ttl
}

func (capture fm5EvidenceCapture) hasFreshNegativeObservation(now time.Time, ttl time.Duration) bool {
	return capture.hasNegativeObservation() && !capture.staleAt(now, ttl)
}

func (capture fm5EvidenceCapture) hasNegativeObservation() bool {
	return !capture.hasEvidence() && capture.identityAcquisitionComplete && !capture.identityObservedAt.IsZero()
}

func (capture fm5EvidenceCapture) sameGeneration(other fm5EvidenceCapture) bool {
	return capture.registryCoherent && other.registryCoherent &&
		capture.generation == other.generation &&
		capture.controller == other.controller &&
		uint16PointersEqual(capture.moduleConfig, other.moduleConfig) &&
		capture.registryEvidenceIgnored == other.registryEvidenceIgnored &&
		capture.identityAcquisitionComplete == other.identityAcquisitionComplete &&
		capture.identityIncoherent == other.identityIncoherent &&
		capture.configurationFailed == other.configurationFailed &&
		capture.identityObservedAt.Equal(other.identityObservedAt) &&
		capture.registryGeneration == other.registryGeneration &&
		capture.registryEvidence == other.registryEvidence
}

func (capture fm5EvidenceCapture) matchesLockedPoller(p *vaillantSemanticPoller) bool {
	if p == nil {
		return false
	}
	var moduleConfig *uint16
	if p.system != nil {
		moduleConfig = p.system.ModuleConfigurationVR71
	}
	return capture.generation == p.fm5EvidenceGeneration &&
		capture.controller == p.controller &&
		uint16PointersEqual(capture.moduleConfig, moduleConfig) &&
		capture.registryEvidenceIgnored == p.fm5RegistryEvidenceIgnored &&
		capture.identityAcquisitionComplete == p.fm5IdentityScanComplete &&
		capture.identityIncoherent == p.fm5IdentityIncoherent &&
		capture.configurationFailed == p.fm5ConfigurationFailed &&
		capture.identityObservedAt.Equal(p.fm5IdentityObservedAt)
}

func (p *vaillantSemanticPoller) commitFM5Acquisition(
	captured fm5EvidenceCapture,
	verdict vaillantFM5Interpretation,
	incomingSolar *vaillantSolarSnapshot,
	incomingCylinders map[byte]*vaillantCylinderSnapshot,
) vaillantFM5Interpretation {
	commit := func(registryGeneration uint64, registryCoherent bool) {
		p.mu.Lock()
		defer p.mu.Unlock()
		if !registryCoherent || !captured.registryCoherent ||
			captured.registryGeneration != registryGeneration ||
			!captured.matchesLockedPoller(p) {
			p.fm5EvidenceRevision++
			verdict = vaillantFM5Interpretation{
				Mode:             vaillantFM5ModeGPIOOnly,
				DegradedReason:   vaillantFM5DegradedIncoherentAcquisition,
				EvidenceRevision: formatFM5EvidenceRevision(captured.generation, p.fm5EvidenceGeneration, p.fm5EvidenceRevision),
			}
		}
		verdict = retainFM5StructuralMode(p.fm5Interpretation, verdict)
		if verdict.Mode != "" {
			p.fm5Mode = verdict.Mode
		}
		p.fm5Interpretation = verdict
		p.solar, p.solarCylinders = applyFM5Acquisition(
			p.solar, p.solarCylinders, incomingSolar, incomingCylinders, verdict,
		)
	}

	withGeneration := p.withFM5ObservationGeneration
	if withGeneration == nil && p.reg != nil {
		withGeneration = p.reg.WithObservationGeneration
	}
	if withGeneration == nil {
		commit(0, true)
		return verdict
	}
	committed := false
	if withGeneration(func(current uint64) {
		// Do not call any DeviceRegistry method from this callback. The
		// registry read lock is deliberately held across the generation
		// comparison and semantic commit.
		commit(current, true)
		committed = true
	}) && committed {
		return verdict
	}
	commit(captured.registryGeneration, false)
	return verdict
}

func (p *vaillantSemanticPoller) updateFM5ConfigurationAcquisitionLocked(acquired bool) {
	failed := !acquired
	if p.fm5ConfigurationFailed != failed {
		p.fm5ConfigurationFailed = failed
		p.fm5EvidenceGeneration++
	}
}

func uint16PointersEqual(left, right *uint16) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func deriveFM5Interpretation(
	controllerReachable bool,
	moduleConfig *uint16,
	solarReadable bool,
	cylindersReadable bool,
	hasEvidence bool,
	evidenceStale bool,
	incoherent bool,
	evidenceRevision string,
) vaillantFM5Interpretation {
	verdict := vaillantFM5Interpretation{EvidenceRevision: evidenceRevision}
	verdict.Mode = vaillantFM5ModeGPIOOnly
	switch {
	case incoherent:
		verdict.DegradedReason = vaillantFM5DegradedIncoherentAcquisition
	case !hasEvidence:
		return vaillantFM5Interpretation{}
	case !controllerReachable:
		verdict.DegradedReason = vaillantFM5DegradedControllerUnreachable
	case moduleConfig == nil:
		verdict.DegradedReason = vaillantFM5DegradedConfigurationUnavailable
	case evidenceStale:
		verdict.DegradedReason = vaillantFM5DegradedEvidenceStale
	case *moduleConfig > 2:
		verdict.DegradedReason = vaillantFM5DegradedConfigurationNotInterpretable
	case !solarReadable:
		verdict.DegradedReason = vaillantFM5DegradedSolarAcquisitionFailed
	case !cylindersReadable:
		verdict.DegradedReason = vaillantFM5DegradedCylinderAcquisitionFailed
	default:
		verdict.Mode = vaillantFM5ModeInterpreted
	}
	return verdict
}

func retainFM5StructuralMode(previous, candidate vaillantFM5Interpretation) vaillantFM5Interpretation {
	if !isFM5TransientAcquisitionReason(candidate.DegradedReason) {
		return candidate
	}
	if !previous.valid() {
		return vaillantFM5Interpretation{}
	}
	switch previous.Mode {
	case vaillantFM5ModeInterpreted, vaillantFM5ModeAbsent:
		candidate.Mode = previous.Mode
		return candidate
	case vaillantFM5ModeGPIOOnly:
		previous.EvidenceRevision = candidate.EvidenceRevision
		return previous
	default:
		return vaillantFM5Interpretation{}
	}
}

func isFM5TransientAcquisitionReason(reason vaillantFM5DegradedReason) bool {
	switch reason {
	case vaillantFM5DegradedControllerUnreachable,
		vaillantFM5DegradedConfigurationUnavailable,
		vaillantFM5DegradedSolarAcquisitionFailed,
		vaillantFM5DegradedCylinderAcquisitionFailed,
		vaillantFM5DegradedEvidenceStale,
		vaillantFM5DegradedIncoherentAcquisition:
		return true
	default:
		return false
	}
}

func applyFM5Acquisition(
	previousSolar *vaillantSolarSnapshot,
	previousCylinders map[byte]*vaillantCylinderSnapshot,
	incomingSolar *vaillantSolarSnapshot,
	incomingCylinders map[byte]*vaillantCylinderSnapshot,
	verdict vaillantFM5Interpretation,
) (*vaillantSolarSnapshot, map[byte]*vaillantCylinderSnapshot) {
	switch {
	case isFM5TransientAcquisitionReason(verdict.DegradedReason):
		return cloneSolarSnapshot(previousSolar), cloneCylinderSnapshotsMap(previousCylinders)
	case verdict.Mode == vaillantFM5ModeInterpreted:
		return mergeSolarSnapshotNonDestructive(previousSolar, incomingSolar),
			mergeCylinderSnapshotMapNonDestructive(previousCylinders, incomingCylinders)
	case isFM5StructuralWithdrawal(verdict):
		return nil, make(map[byte]*vaillantCylinderSnapshot)
	default:
		return cloneSolarSnapshot(previousSolar), cloneCylinderSnapshotsMap(previousCylinders)
	}
}

func isFM5StructuralWithdrawal(verdict vaillantFM5Interpretation) bool {
	return (verdict.Mode == vaillantFM5ModeAbsent && verdict.DegradedReason == "") ||
		(verdict.Mode == vaillantFM5ModeGPIOOnly &&
			verdict.DegradedReason == vaillantFM5DegradedConfigurationNotInterpretable)
}

func (p *vaillantSemanticPoller) nextFM5EvidenceRevision(startGeneration, endGeneration uint64) string {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.fm5EvidenceRevision++
	return formatFM5EvidenceRevision(startGeneration, endGeneration, p.fm5EvidenceRevision)
}

func formatFM5EvidenceRevision(startGeneration, endGeneration, acquisition uint64) string {
	if startGeneration != endGeneration {
		return fmt.Sprintf("fm5-g%d-%d-a%d", startGeneration, endGeneration, acquisition)
	}
	return fmt.Sprintf("fm5-g%d-a%d", endGeneration, acquisition)
}

func hasFM5EvidenceFromRadioSnapshots(snapshots []*vaillantRadioDeviceSnapshot) bool {
	for _, snapshot := range snapshots {
		if snapshot == nil {
			continue
		}
		if hasRemoteIdentityEvidence(snapshot.DeviceClassAddress, snapshot.FirmwareVersion, snapshot.HardwareIdentifier) {
			return true
		}
		if snapshot.DeviceClassAddress != nil && *snapshot.DeviceClassAddress == 0x26 {
			return true
		}
		if strings.EqualFold(strings.TrimSpace(snapshot.DeviceModel), "VR71/FM5") {
			return true
		}
	}
	return false
}

func hasFM5EvidenceFromRadioMap(snapshots map[radioDeviceKey]*vaillantRadioDeviceSnapshot) bool {
	for _, snapshot := range snapshots {
		if hasFM5EvidenceFromRadioSnapshots([]*vaillantRadioDeviceSnapshot{snapshot}) {
			return true
		}
	}
	return false
}

func (p *vaillantSemanticPoller) hasFM5RegistryEvidence() bool {
	return p.fm5RegistryEvidenceFingerprint() != ""
}

func (p *vaillantSemanticPoller) captureFM5RegistryEvidence() (string, uint64, bool) {
	if p == nil || p.reg == nil {
		return "", 0, true
	}
	for range 3 {
		var before uint64
		if !p.reg.WithObservationGeneration(func(current uint64) { before = current }) {
			return "", 0, false
		}
		fingerprint := p.fm5RegistryEvidenceFingerprint()
		var after uint64
		if !p.reg.WithObservationGeneration(func(current uint64) { after = current }) {
			return fingerprint, before, false
		}
		if before == after {
			return fingerprint, after, true
		}
	}
	return p.fm5RegistryEvidenceFingerprint(), 0, false
}

func (p *vaillantSemanticPoller) fm5RegistryEvidenceFingerprint() string {
	if p == nil || p.reg == nil {
		return ""
	}
	// P9.3 — race-free DeviceID read via snapshot.
	identities := make([]string, 0, 1)
	p.reg.IterateSnapshots(func(snap registry.DeviceEntrySnapshot) bool {
		deviceID := normalizeDeviceID(snap.DeviceID)
		if strings.HasPrefix(deviceID, "VR71") || strings.HasPrefix(deviceID, "FM5") {
			identities = append(identities, fmt.Sprintf("%02x:%s", snap.PrimaryAddress, deviceID))
		}
		return true
	})
	slices.Sort(identities)
	return strings.Join(identities, "|")
}

func fm5InventoryRegistryInfos(system *vaillantSystemSnapshot, fm5Mode vaillantFM5Mode) []registry.DeviceInfo {
	if system != nil &&
		system.SystemScheme != nil && *system.SystemScheme == 1 &&
		system.ModuleConfigurationVR71 != nil && *system.ModuleConfigurationVR71 == 2 &&
		fm5Mode == vaillantFM5ModeInterpreted {
		return []registry.DeviceInfo{{
			Address:      circuitManagingDeviceVR71Address,
			Manufacturer: "Vaillant",
			DeviceID:     circuitManagingDeviceVR71ID,
		}}
	}
	return nil
}
