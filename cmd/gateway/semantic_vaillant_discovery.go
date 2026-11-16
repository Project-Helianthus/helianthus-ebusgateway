package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"slices"
	"strings"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
	"github.com/Project-Helianthus/helianthus-ebusreg/vaillant/productids"
)

type regulatorAbsenceState string

const (
	// regulatorAbsenceUnknown is the state before the first regulator
	// recheck has evaluated the capability.
	regulatorAbsenceUnknown regulatorAbsenceState = "UNKNOWN"
	regulatorPresent        regulatorAbsenceState = "PRESENT"
	regulatorAbsenceGrace   regulatorAbsenceState = "ABSENCE_GRACE"
	regulatorAbsent         regulatorAbsenceState = "ABSENT"
)

type regulatorEnrichment struct {
	family   string
	deviceID string
}

type b524ProbeSpec struct {
	opcode   byte
	group    byte
	instance byte
	addr     uint16
}

// b524CapabilityProbes lists registers from different GG groups used for
// multi-register coherency verification during capability-first discovery.
var b524CapabilityProbes = []b524ProbeSpec{
	{opcode: localRegulator.opcode, group: localRegulator.group, instance: regulatorInstance, addr: 0x0001},
	{opcode: localDHW.opcode, group: localDHW.group, instance: dhwInstance, addr: dhw_operation_mode},
}

func (p *vaillantSemanticPoller) refreshRegulatorCapability(_ context.Context) {
	if p == nil || p.reg == nil {
		return
	}

	regCap := p.findRegulatorCapability()
	deviceCount := countRegistryDevices(p.reg)
	now := p.now()

	p.mu.Lock()
	prevCap := p.regulatorCapability
	prevAbsence := p.regAbsenceState
	prevDeviceCount := p.registryDeviceCount
	p.regulatorCapability = regCap
	p.registryDeviceCount = deviceCount

	// Absence grace FSM transitions.
	switch {
	case regCap == productids.ControllerPresent:
		if p.regAbsenceState != regulatorPresent {
			p.regAbsenceState = regulatorPresent
			p.regAbsenceSince = time.Time{}
		}
	case prevAbsence == regulatorPresent || prevAbsence == regulatorAbsenceUnknown:
		// Was present, or not yet evaluated, and is not present: enter the
		// grace window from this evaluation.
		p.regAbsenceState = regulatorAbsenceGrace
		p.regAbsenceSince = now
	case prevAbsence == regulatorAbsenceGrace:
		if !p.regAbsenceSince.IsZero() && now.Sub(p.regAbsenceSince) >= p.regulatorAbsenceGrace {
			p.regAbsenceState = regulatorAbsent
		}
	}
	newAbsence := p.regAbsenceState
	p.mu.Unlock()
	p.publishRegulatorCapability(regCap)
	semanticRegulatorState.Set(string(newAbsence))

	// Log capability changes.
	if regCap != prevCap {
		log.Printf("semantic_regulator_capability capability=%s", regCap.String())
	}

	// Log absence state transitions.
	if newAbsence != prevAbsence {
		semanticRegulatorTransitionsTotal.Add(fmt.Sprintf("%s->%s", prevAbsence, newAbsence), 1)
		log.Printf("semantic_regulator_transition from=%s to=%s capability=%s", prevAbsence, newAbsence, regCap.String())
		log.Printf("semantic_regulator_absence state=%s capability=%s", newAbsence, regCap.String())
	}

	// The reason depends on the regulator capability: re-evaluate it here so
	// an enrichment that classifies a registered identity as a controller
	// clears it without waiting for a discovery pass.
	p.refreshHeatSourcePathReason()

	// If device count changed, enqueue an immediate full discovery refresh.
	if deviceCount != prevDeviceCount && prevDeviceCount > 0 {
		log.Printf("semantic_regulator_recheck inventory_change prev=%d curr=%d", prevDeviceCount, deviceCount)
		p.enqueueTask(semanticTaskRefreshDiscovery, semanticTaskPriorityLow, p.refreshDiscovery)
	}
}

// fallbackDiscoveryAllowed reports whether the state and configuration tasks
// may run their fallback root discovery. On a bus where this process has never
// discovered a B524 root, once the regulator absence state is ABSENT, root
// discovery runs only from the discovery task at the configured discovery
// interval and from event-driven refreshes (inventory change, passive
// insertion and promotion). ABSENT alone does not prove that no controller
// exists (an unclassified or structurally registered controller also reaches
// it), so a process that has discovered a root once keeps the per-minute
// recovery, and a known controller without zones keeps the fallback. The
// fallback is suspended only while the periodic discovery task runs: the poll
// schedule starts that task's loop only for a positive discovery interval, so
// with the interval disabled the fallback stays the only periodic search.
func (p *vaillantSemanticPoller) fallbackDiscoveryAllowed(controller byte) bool {
	p.mu.Lock()
	periodicDiscovery := p.discoveryInterval > 0
	suspend := controller == 0 && p.regAbsenceState == regulatorAbsent && !p.controllerEverDiscovered && periodicDiscovery
	logSuspension := suspend && !p.discoveryFallbackSuspended
	p.discoveryFallbackSuspended = suspend
	interval := p.discoveryInterval
	p.mu.Unlock()
	if logSuspension {
		log.Printf("semantic_discovery_fallback_suspended regulator_state=%s discovery_interval=%s", regulatorAbsent, interval)
	}
	return !suspend
}

func (p *vaillantSemanticPoller) refreshDiscovery(ctx context.Context) {
	// Regulator capability is always recomputed, even when no B524 root is found.
	regCap := p.findRegulatorCapability()
	boilerAddress := p.findBoilerAddress()

	controller, discoveryErr := p.discoverB524Root(ctx)
	if discoveryErr != nil {
		log.Printf("semantic_b524_root: %v", discoveryErr)
		p.mu.Lock()
		prev := p.regulatorCapability
		prevController := p.controller
		prevBoilerAddress := p.boilerAddress
		p.controller = 0
		p.boilerAddress = boilerAddress
		p.regulatorCapability = regCap
		p.zones = make(map[byte]*vaillantZoneSnapshot)
		p.presence = make(map[byte]*zonePresenceRecord)
		p.circuits = make(map[byte]*vaillantCircuitSnapshot)
		p.fm5EvidenceGeneration++
		p.startupSemanticPrimed = false
		semanticZoneCount.Set(0)
		p.mu.Unlock()
		p.publishRegulatorCapability(regCap)
		p.refreshHeatSourcePathReason()
		if regCap != prev {
			log.Printf("semantic_regulator_capability capability=%s", regCap.String())
		}
		if prevController != 0 {
			log.Printf("semantic_controller_discovery address=0x00 source=no_coherent_responder")
		}
		if boilerAddress != prevBoilerAddress && boilerAddress != 0 && boilerAddress != 0x08 {
			log.Printf("semantic_boiler_discovery address=0x%02x source=nonstandard", boilerAddress)
		}
		if boilerAddress != prevBoilerAddress && boilerAddress != 0 {
			p.enqueueBoilerStatusPriming(ctx)
		}
		p.publishZones(semanticSnapshotSourceCache)
		p.publishDHW(semanticSnapshotSourceCache)
		p.publishCircuits(semanticSnapshotSourceCache)
		p.publishRadioDevices(semanticSnapshotSourceCache)
		p.refreshFM5Semantic(ctx)
		return
	}

	// When B524 root discovery converges via the structural-fallback
	// path, the controller address may not yet be in the registry —
	// the directed startup probe that would normally register it
	// either has not run for this address (e.g. the regulator missed
	// the startup window) or did not yield enrichment. Register a
	// minimal Vaillant entry so GraphQL devices and the router plane
	// reflect the regulator. Registry de-dupes on address, so a
	// no-op when the controller is already known.
	registered := p.registerStructuralControllerIfMissing(controller)

	if enrichment := p.enrichRegulatorIdentity(controller); enrichment != nil {
		log.Printf("semantic_controller_enrichment address=0x%02x family=%s device=%s",
			controller, enrichment.family, enrichment.deviceID)
	}

	// Recompute regulator capability after structural registration:
	// the registry just gained a new Vaillant entry, so the
	// capability lookup must run against the post-registration
	// inventory. Without this recompute, p.regulatorCapability
	// stays at the pre-registration value (typically ControllerNone
	// when the registry held only the boiler) until the next
	// regulatorRecheckInterval tick — leaving status surfaces
	// reporting "no regulator" while semantic discovery already
	// has one.
	if registered {
		regCap = p.findRegulatorCapability()
	}

	p.mu.Lock()
	prev := p.regulatorCapability
	prevController := p.controller
	prevBoilerAddress := p.boilerAddress
	p.controller = controller
	p.controllerEverDiscovered = true
	p.discoveryFallbackSuspended = false
	if controller != prevController {
		p.fm5EvidenceGeneration++
	}
	p.boilerAddress = boilerAddress
	p.regulatorCapability = regCap
	primeStartup := !p.startupSemanticPrimed
	if primeStartup {
		p.startupSemanticPrimed = true
	}
	p.mu.Unlock()
	p.publishRegulatorCapability(regCap)
	p.refreshHeatSourcePathReason()

	if primeStartup {
		p.refreshStartupCriticalSemanticPlanes(ctx)
	}
	p.refreshZoneDiscovery(ctx, primeStartup)
	if primeStartup {
		p.refreshStartupSemanticPlanes(ctx)
	}
	if regCap != prev {
		log.Printf("semantic_regulator_capability capability=%s", regCap.String())
	}
	if controller != prevController && controller != 0 {
		log.Printf("semantic_controller_discovery address=0x%02x", controller)
		p.enqueueControllerSemanticPriming(ctx)
	}
	if boilerAddress != prevBoilerAddress && boilerAddress != 0 && boilerAddress != 0x08 {
		log.Printf("semantic_boiler_discovery address=0x%02x source=nonstandard", boilerAddress)
	}
	if boilerAddress != prevBoilerAddress && boilerAddress != 0 {
		p.enqueueBoilerStatusPriming(ctx)
	}
}

func (p *vaillantSemanticPoller) tryRefreshFromEbusdGrab(ctx context.Context) bool {
	_, ok := p.tryRefreshFromEbusdGrabWithInstances(ctx)
	return ok
}

func (p *vaillantSemanticPoller) tryRefreshFromEbusdGrabWithInstances(ctx context.Context) (map[byte]bool, bool) {
	if p == nil {
		return nil, false
	}
	if p.transportConfig.Protocol != ebusgateway.TransportEbusdTCP {
		return nil, false
	}
	if p.refreshFromEbusdGrabFn != nil {
		return p.refreshFromEbusdGrabFn(ctx)
	}
	return p.refreshFromEbusdGrabWithInstances(ctx)
}

func normalizeDeviceID(value string) string {
	normalized := strings.ToUpper(strings.TrimSpace(value))
	normalized = strings.ReplaceAll(normalized, "_", "")
	normalized = strings.ReplaceAll(normalized, "-", "")
	normalized = strings.ReplaceAll(normalized, " ", "")
	return normalized
}

// findRegulatorCapability iterates all devices in the registry and determines
// the regulator capability strictly from the product_ids catalog classification.
// Returns ControllerPresent if any Vaillant device has role=="Regulator",
// ControllerNone if all Vaillant devices have known non-regulator roles,
// ControllerUnknown if any device has an unknown classification or no serial.
func (p *vaillantSemanticPoller) findRegulatorCapability() productids.ControllerCapability {
	if p == nil || p.reg == nil || p.catalogErr != nil {
		return productids.ControllerUnknown
	}

	// P9.3 — race-free Manufacturer/SerialNumber reads via snapshot.
	foundPresent := false
	hasUnknown := false
	hasAnyDevice := false
	p.reg.IterateSnapshots(func(snap registry.DeviceEntrySnapshot) bool {
		if !strings.EqualFold(snap.Manufacturer, "Vaillant") {
			return true
		}
		hasAnyDevice = true
		partNumber := extractPartNumberFromSerial(snap.SerialNumber)
		cap := p.catalog.ControllerCapability(partNumber)
		switch cap {
		case productids.ControllerPresent:
			foundPresent = true
			return false // stop iteration
		case productids.ControllerUnknown:
			hasUnknown = true
		}
		return true
	})

	if foundPresent {
		return productids.ControllerPresent
	}
	if !hasAnyDevice {
		return productids.ControllerUnknown
	}
	if hasUnknown {
		return productids.ControllerUnknown
	}
	return productids.ControllerNone
}

// extractPartNumberFromSerial extracts a 10-digit Vaillant part number from a
// device serial number string. The logic mirrors graphql.extractVaillantPartNumber.
func extractPartNumberFromSerial(serial string) string {
	serial = strings.TrimSpace(serial)
	if serial == "" {
		return ""
	}

	// Try dash-separated format: "YY-WW-CC-PPPPPPPPPP-SSSS-RRRRRR-XX"
	parts := strings.Split(serial, "-")
	if len(parts) >= 4 {
		partNumber := strings.TrimSpace(parts[3])
		if len(partNumber) == 10 && isAllDigits(partNumber) {
			return partNumber
		}
	}

	// Fallback: strip non-alphanumeric, take digits 6..16.
	compact := make([]byte, 0, len(serial))
	for i := 0; i < len(serial); i++ {
		ch := serial[i]
		if (ch >= '0' && ch <= '9') || (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') {
			compact = append(compact, ch)
		}
	}
	if len(compact) >= 16 {
		candidate := string(compact[6:16])
		if len(candidate) == 10 && isAllDigits(candidate) {
			return candidate
		}
	}

	return ""
}

// isAllDigits returns true if the string is non-empty and contains only ASCII digits.
func isAllDigits(value string) bool {
	if value == "" {
		return false
	}
	for i := 0; i < len(value); i++ {
		if value[i] < '0' || value[i] > '9' {
			return false
		}
	}
	return true
}

// b524DiscoveryOptions controls how discoverB524RootWithOptions builds its
// candidate set. The default zero value (augmentStructural=false) is the
// registry-health-check semantic — answers "does the existing registry
// contain a coherent B524 root?" without probing addresses outside the
// registry. Set augmentStructural=true for the production discovery
// path used by refreshDiscovery, which unconditionally probes the
// bounded Vaillant structural target set even when the registry has
// not yet learned about the regulator.
type b524DiscoveryOptions struct {
	augmentStructural bool
}

// discoverB524Root is the production discovery path. It augments the
// registry-derived candidate list with the bounded Vaillant structural
// target set {0x08, 0x15, 0x26}, probes D1-ordered (0x15 first), and
// stops at the first coherent candidate.
//
// Structural augmentation is unconditional and idempotent: the dedup
// map collapses overlap, and the structural set is bounded to three
// non-broadcast unicast targets. The alternative — gating on registry
// size — has a hostile-registry hole where any stray non-Vaillant
// entry (e.g. {0x08 boiler, 0xEC passive-only responder}) suppresses
// the fallback exactly when it is needed.
//
// Probing structural addresses that are not yet in the registry is
// safe: the unicast probe either elicits a coherent response (in
// which case refreshDiscovery registers the controller post-
// discovery via registerStructuralControllerIfMissing) or it does
// not. Source-address invariant is upheld — the probe uses the
// existing admitted source via probeFn.
func (p *vaillantSemanticPoller) discoverB524Root(ctx context.Context) (byte, error) {
	return p.discoverB524RootWithOptions(ctx, b524DiscoveryOptions{augmentStructural: true})
}

// discoverB524RootInRegistry is the registry-health-check path. It
// considers ONLY candidates already in the registry. Used by startup
// scan helpers to ask "is the existing registry coherent?", which is
// a distinct question from "where is the regulator?".
func (p *vaillantSemanticPoller) discoverB524RootInRegistry(ctx context.Context) (byte, error) {
	return p.discoverB524RootWithOptions(ctx, b524DiscoveryOptions{augmentStructural: false})
}

func (p *vaillantSemanticPoller) discoverB524RootWithOptions(ctx context.Context, opts b524DiscoveryOptions) (byte, error) {
	if p == nil || p.reg == nil {
		return 0, fmt.Errorf("gateway.discoverB524Root: nil poller or registry")
	}

	probeFn := p.b524ProbeFn
	if probeFn == nil {
		probeFn = p.probeB524Register
	}

	// Source-address invariant applies uniformly across both
	// candidate entry points (registry iteration AND structural
	// fallback). A configured source / companion target that happens
	// to be already present in the registry — e.g. an ebusd preload
	// imports 0x26 before semantic discovery runs, or a prior
	// session left the companion address registered — would otherwise
	// bypass the structural-augmentation guard via the dedup map and
	// be probed unicast. The shared filter ensures any candidate
	// matching the admitted source or companion is dropped regardless
	// of how it entered the candidate set.
	skipReservedSourceCompanion := func(addr byte) bool {
		reservedSource, reservedCompanion := p.sourceAndCompanion()
		if addr == reservedSource {
			return true
		}
		if reservedCompanion != 0 && addr == reservedCompanion {
			return true
		}
		return false
	}
	candidateSet := make(map[byte]struct{})
	var candidates []byte
	// P9.3 — race-free routing-address enumeration via
	// IterateSnapshots + SnapshotTargetAddressForRouting. The
	// previous Iterate path called entry.AddressByRole and
	// entry.PrimaryDisplayAddress lock-free.
	p.reg.IterateSnapshots(func(snap registry.DeviceEntrySnapshot) bool {
		// Phase C M-C6b: B524 candidates are routing targets, not
		// display addresses. An aliased regulator entry (e.g.
		// 0x10↔0x15 displayed as 0x10) must be probed at 0x15
		// where the responder lives — probing 0x10 misses the
		// coherent responder and reports no B524 root.
		addr := ebusgateway.SnapshotTargetAddressForRouting(snap)
		if skipReservedSourceCompanion(addr) {
			return true
		}
		if _, dup := candidateSet[addr]; !dup {
			candidateSet[addr] = struct{}{}
			candidates = append(candidates, addr)
		}
		return true
	})

	if opts.augmentStructural {
		for _, addr := range ebusgateway.VaillantStructuralStartupProbeTargets {
			if _, dup := candidateSet[addr]; dup {
				continue
			}
			if skipReservedSourceCompanion(addr) {
				continue
			}
			candidateSet[addr] = struct{}{}
			candidates = append(candidates, addr)
		}
	}

	if len(candidates) == 0 {
		return 0, fmt.Errorf("gateway.discoverB524Root: no devices in registry")
	}

	slices.Sort(candidates)

	if _, has0x15 := candidateSet[0x15]; has0x15 {
		ordered := make([]byte, 0, len(candidates))
		ordered = append(ordered, 0x15)
		for _, c := range candidates {
			if c != 0x15 {
				ordered = append(ordered, c)
			}
		}
		candidates = ordered
	}

	for _, addr := range candidates {
		coherent := true
		for _, probe := range b524CapabilityProbes {
			if !probeFn(ctx, addr, probe.opcode, probe.group, probe.instance, probe.addr) {
				coherent = false
				break
			}
		}
		if coherent {
			log.Printf("semantic_b524_root_discovery address=0x%02x probed=%d", addr, len(candidates))
			return addr, nil
		}
	}

	addrs := make([]string, len(candidates))
	for i, c := range candidates {
		addrs[i] = fmt.Sprintf("0x%02x", c)
	}
	return 0, fmt.Errorf("gateway.discoverB524Root: no coherent responder among [%s]", strings.Join(addrs, ", "))
}

// ConfirmB524Coherent runs the multi-register B524 capability probe
// against target and returns true if every probe responds coherently.
// Used by the runtime passive-promotion pipeline to validate that a
// candidate address discovered via passive evidence actually
// implements the Vaillant extended-register protocol before
// registering it. The probe sources from the poller's admitted
// source — source-address invariant is preserved.
func (p *vaillantSemanticPoller) ConfirmB524Coherent(ctx context.Context, target byte) bool {
	if p == nil {
		return false
	}
	probeFn := p.b524ProbeFn
	if probeFn == nil {
		probeFn = p.probeB524Register
	}
	for _, probe := range b524CapabilityProbes {
		if !probeFn(ctx, target, probe.opcode, probe.group, probe.instance, probe.addr) {
			return false
		}
	}
	return true
}

// EnqueueDiscoveryRefresh signals the semantic poller to re-run
// discovery on the next available task slot. Used by the passive-
// promotion pipeline after a late-arrival candidate is registered,
// so the regulator surface populates without a gateway restart.
func (p *vaillantSemanticPoller) EnqueueDiscoveryRefresh() {
	if p == nil {
		return
	}
	p.enqueueTask(semanticTaskRefreshDiscovery, semanticTaskPriorityHigh, p.refreshDiscovery)
}

// EnqueueAddressIdentityProbe schedules a per-address identity
// probe for the given address. Runs registry.Scan against just
// that address using the gateway's current scan source — on
// success, the registry's M6 identity-merge path collapses
// canonical pairs that share identity into a single DeviceEntry.
//
// Phase post-C P5 (live validation 2026-05-08): closes the gap
// where passive-observed addresses (e.g. NETX3 0xF1, 0xF6, BASV2
// 0x10) sit in the registry forever with empty manufacturer /
// deviceID / serialNumber because no path probes them for
// identity post-insertion.
//
// Bounded + idempotent: probes each address at most once per
// gateway lifetime via p.identityProbedAddresses (sync.Map).
// Probes that fail (timeout, NACK, etc.) are NOT retried — the
// next gateway restart re-attempts. This is intentional: NETX3's
// 0x04 broadcast face does not respond to active probes by spec,
// so retry would just spam the bus.
func (p *vaillantSemanticPoller) EnqueueAddressIdentityProbe(addr byte) {
	if p == nil || p.bus == nil || p.reg == nil {
		return
	}
	if addr == 0 || addr == 0xFE || addr == 0xAA {
		return
	}
	// P5 round-8 (Codex P2 follow-up 2026-05-08): skip the gateway's
	// own admitted source + companion. The existing
	// discoverB524RootInRegistry candidate path explicitly drops
	// these (skipReservedSourceCompanion); this probe path must
	// honor the same protection so we don't emit identity traffic
	// to our own admitted address (or its companion).
	probeSource, probeCompanion := p.sourceAndCompanion()
	if addr == probeSource {
		return
	}
	if probeCompanion != 0 && addr == probeCompanion {
		return
	}
	// P5 round-6 (Codex P2 follow-up 2026-05-08): resolve the input
	// address to its responder/target byte before probing. Active
	// identity reads (0x07/0x04 + B5.09 ScanID) must be addressed
	// to the responder face — sending them to an initiator/source
	// byte (e.g. BASV2 0x10, NETX3 0xF1) produces no response and
	// just times out. The existing startup scan path filters
	// initiator-capable addresses out of target lists for the same
	// reason.
	//
	// Resolution order:
	// 1. If addr is non-initiator-capable, it IS already a
	//    responder/target — probe directly.
	// 2. Otherwise, look up the entry containing addr and use
	//    TargetAddressForRouting (prefers SlotRoleSlave, falls
	//    back to PrimaryDisplayAddress when no target-role face
	//    exists).
	// 3. If neither yields a valid target, skip — initiator-only
	//    devices have no probable identity face.
	probeAddr := addr
	if protocol.IsInitiatorCapableAddress(addr) {
		// P9.3 — race-free identity-probe target resolution via
		// IterateSnapshots + SnapshotTargetAddressForRouting. The
		// semantic poller runs concurrently with passive inserter /
		// startup scan registry writes; the previous Iterate path
		// dereferenced live entry.Addresses() and TargetAddressForRouting.
		var resolvedTarget byte
		p.reg.IterateSnapshots(func(snap registry.DeviceEntrySnapshot) bool {
			if ebusgateway.SnapshotContainsAddress(snap, addr) {
				resolvedTarget = ebusgateway.SnapshotTargetAddressForRouting(snap)
				return false
			}
			return true
		})
		if resolvedTarget == 0 || protocol.IsInitiatorCapableAddress(resolvedTarget) {
			// No target-role face on the entry, OR the entry's
			// only addresses are initiator-capable — skip.
			log.Printf("semantic_address_identity_probe address=0x%02X status=skipped reason=no_responder_face", addr)
			return
		}
		probeAddr = resolvedTarget
	}
	if _, alreadyProbed := p.identityProbedAddresses.LoadOrStore(probeAddr, struct{}{}); alreadyProbed {
		return
	}
	scheduler := p.tasks
	probeFn := func(ctx context.Context) {
		probeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		scanSource, _ := p.sourceAndCompanion()
		_, err := registryScanDirectedFn(probeCtx, p.bus, p.reg, scanSource, []byte{probeAddr})
		if err != nil {
			log.Printf("semantic_address_identity_probe address=0x%02X status=fail err=%v", probeAddr, err)
			return
		}
		log.Printf("semantic_address_identity_probe address=0x%02X status=ok", probeAddr)
		if probeAddr == circuitManagingDeviceVR71Address {
			recoveryCtx, recoveryCancel := context.WithTimeout(ctx, semanticIdentityRecoveryTimeout)
			defer recoveryCancel()
			p.refreshFM5ConfigurationForLateIdentity(recoveryCtx)
			p.refreshFM5AfterLateIdentity(recoveryCtx)
		}
	}
	if scheduler == nil {
		// No scheduler wired (unit tests, etc.) — run inline.
		probeFn(context.Background())
		return
	}
	// Codex P2 follow-up on PR #583 (2026-05-08): submit BEFORE
	// committing the per-address probed marker. If the task queue
	// is overloaded and submit fails, roll back the sync.Map entry
	// so the address remains eligible for retry on the next passive
	// observation. Without rollback, an overloaded queue at startup
	// permanently blocks identity enrichment for the dropped
	// addresses until the next gateway restart.
	err := scheduler.submit(semanticTaskPriorityLow, func(taskCtx context.Context) {
		p.withPollLock(taskCtx, probeFn)
	})
	if err != nil {
		// Rollback so the (resolved) address is eligible for retry.
		p.identityProbedAddresses.Delete(probeAddr)
		if errors.Is(err, errSemanticTaskQueueOverloaded) {
			log.Printf("semantic_address_identity_probe address=0x%02X status=enqueue_dropped reason=queue_overloaded", probeAddr)
			return
		}
		log.Printf("semantic_address_identity_probe address=0x%02X status=enqueue_failed err=%v", probeAddr, err)
	}
}

// registerStructuralControllerIfMissing registers a minimal Vaillant
// device entry for a B524-coherent controller address that is not yet
// in the registry. This closes the gap where discoverB524Root converges
// via the structural-fallback path (probing 0x15 / 0x26 even when they
// are absent from the registry) — the address must surface in
// GraphQL devices and on the router plane after discovery.
//
// The minimal entry is safe: the address has just satisfied the B524
// multi-register coherency check, which is strong evidence of a
// Vaillant regulator. Registry de-dupes on address, so this is a
// no-op for already-known controllers. Manufacturer is the only
// identity field set; richer fields (DeviceID, SerialNumber,
// SoftwareVersion) are populated naturally by subsequent identity
// reads or by the discovery promotion pipeline once that lands.
// registerStructuralControllerIfMissing returns true iff the registry
// gained a new entry as a result of this call (false when the
// controller was already known, was zero, or the poller was nil).
// Callers use the return value to decide whether downstream caches
// (e.g. regulator capability) need recomputation.
func (p *vaillantSemanticPoller) registerStructuralControllerIfMissing(controller byte) bool {
	if p == nil || p.reg == nil || controller == 0 {
		return false
	}
	// Phase C M-C6b: containment must check the full address set,
	// not just PrimaryDisplayAddress. A controller (e.g. 0x15) that
	// is already an alias face on a registered entry (e.g. an
	// aliased 0x10↔0x15 regulator displayed as 0x10) must be
	// treated as already-known, otherwise the function violates
	// its idempotence contract and re-registers + re-refreshes
	// router planes on every discovery cycle.
	// P9.3 — race-free address-membership check via IterateSnapshots.
	// The semantic poller runs concurrently with passive inserter /
	// startup scan / identity probe goroutines that mutate the
	// registry; the previous Iterate path read entry.Addresses() on
	// a live *deviceEntry pointer, racing with concurrent Register /
	// AliasAddresses writes.
	already := false
	p.reg.IterateSnapshots(func(snap registry.DeviceEntrySnapshot) bool {
		if ebusgateway.SnapshotContainsAddress(snap, controller) {
			already = true
			return false
		}
		return true
	})
	if already {
		return false
	}
	p.reg.Register(registry.DeviceInfo{
		Address:      controller,
		Manufacturer: "Vaillant",
	})
	log.Printf("semantic_controller_registry_register address=0x%02x source=b524_structural_fallback", controller)
	if p.routerPlanesRefreshFn != nil {
		p.routerPlanesRefreshFn()
	}
	return true
}

// enrichRegulatorIdentity returns enrichment metadata for the B524 root device.
// Unknown identity is acceptable and does not block discovery.
func (p *vaillantSemanticPoller) enrichRegulatorIdentity(addr byte) *regulatorEnrichment {
	if p == nil || p.reg == nil {
		return nil
	}

	// Phase C M-C6b: discoverB524Root returns the routed controller
	// face (e.g. 0x15 for an aliased 0x10↔0x15 regulator). The
	// existing entry is displayed as 0x10, so matching only
	// PrimaryDisplayAddress would miss it and the enrichment
	// metadata/logging would be lost for the aliased path. Use the
	// full address-set membership check instead.
	//
	// P9.3 — race-free via IterateSnapshots; reads entry's DeviceID
	// from the value-typed snapshot (immune to concurrent Register
	// writes that would torn-read the deviceID string).
	var (
		matched  bool
		deviceID string
	)
	p.reg.IterateSnapshots(func(snap registry.DeviceEntrySnapshot) bool {
		if ebusgateway.SnapshotContainsAddress(snap, addr) {
			matched = true
			deviceID = snap.DeviceID
			return false
		}
		return true
	})
	if !matched {
		return nil
	}

	normalizedID := normalizeDeviceID(deviceID)
	families := []string{"BASV2", "BASS2", "CTLV2", "CTLS2", "E7C00"}
	var family string
	for _, f := range families {
		if strings.HasPrefix(normalizedID, f) {
			family = f
			break
		}
	}

	return &regulatorEnrichment{
		family:   family,
		deviceID: deviceID,
	}
}
