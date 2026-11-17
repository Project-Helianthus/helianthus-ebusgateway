package main

import (
	"sync"

	"github.com/Project-Helianthus/helianthus-ebusreg/vaillant/productids"
)

// vaillantSemanticPublisher is the publication owner as the acquisition
// owner reaches it. Every method speaks native values: views, detached
// snapshots, the controller capability, native schedule programs and the
// native adapter info go in; readiness of the published planes comes back as
// counts and flags. The acquisition owner never holds a published value, a
// provider or a hub, and the publication owner holds no bus access.
type vaillantSemanticPublisher interface {
	vaillantEnergyLedger
	vaillantAdapterInfoSink

	// live reports whether the owner holds a provider to publish to.
	live() bool
	// boilerPublishLock returns the lock that orders boiler-status
	// publication against the thermal withdrawal.
	boilerPublishLock() sync.Locker

	publishZoneViews(views []vaillantZoneView, source semanticSnapshotSource) bool
	publishDhwView(view vaillantDhwView, source semanticSnapshotSource) bool
	publishCircuitsView(view vaillantCircuitsView, source semanticSnapshotSource) bool
	publishBoilerStatusView(snapshot *vaillantBoilerSnapshot, source semanticSnapshotSource) bool
	withdrawBoilerFlowTemperature()
	publishRadioDeviceViews(views []*vaillantRadioDeviceSnapshot, source semanticSnapshotSource) bool
	publishSystemView(view vaillantSystemView, source semanticSnapshotSource) bool
	publishRegulatorCapability(capability productids.ControllerCapability)
	publishFM5View(view vaillantFM5View, source semanticSnapshotSource)
	fm5PublishedAsGPIOOnly() bool
	publishStartupPlaneDefaults()
	publishStartupSchedulePlaceholder()
	publishSchedulePrograms(programs []vaillantScheduleProgram)
	publishScheduleDay(zone int, hc string, weekday int, day vaillantScheduleDay, slotsUsed []int)
	// persistsCache reports whether the owner holds a cache persister.
	persistsCache() bool
	persistCache(overlays map[byte]vaillantZoneCacheOverlay)

	// setNoSupportedHeatSourcePath sets or clears the startup reason for a
	// bus without a supported heat source path and returns the reason and
	// whether the published state changed.
	setNoSupportedHeatSourcePath(active bool) (reason string, changed bool)
	// publishedPlanes reports which planes are published.
	publishedPlanes() vaillantPublishedPlanes
	// publishedZoneCount returns the number of published zones.
	publishedZoneCount() int
}

// vaillantPublishedPlanes is the native readiness of the published planes
// that startup priming waits for: whether each plane is published, and the
// number of published radio devices and cylinders.
type vaillantPublishedPlanes struct {
	zones            bool
	dhw              bool
	circuits         bool
	system           bool
	radioDeviceCount int
	solar            bool
	cylinders        bool
	cylinderCount    int
	boilerStatus     bool
}

// vaillantPublicationNoLock is the publish lock of a poller without a
// publication owner, such as the registry-only classifier built during the
// startup scan: it never publishes, so there is nothing to order.
type vaillantPublicationNoLock struct{}

func (vaillantPublicationNoLock) Lock()   {}
func (vaillantPublicationNoLock) Unlock() {}

// publishing reports whether the poller has a publication owner that holds a
// provider to publish to.
func (p *vaillantSemanticPoller) publishing() bool {
	return p != nil && p.publication != nil && p.publication.live()
}

// boilerPublishLock returns the owner's boiler publication lock, or a
// no-op lock when the poller has no publication owner.
func (p *vaillantSemanticPoller) boilerPublishLock() sync.Locker {
	if p == nil || p.publication == nil {
		return vaillantPublicationNoLock{}
	}
	return p.publication.boilerPublishLock()
}

// energyLedger returns the publication owner as the energy ledger of a
// register acquisition, or nil when the poller has no provider to hold the
// ledger.
func (p *vaillantSemanticPoller) energyLedger() vaillantEnergyLedger {
	if !p.publishing() {
		return nil
	}
	return p.publication
}

// publishZones hands the native zone views to the publication owner and
// persists the semantic cache after a publication.
func (p *vaillantSemanticPoller) publishZones(source semanticSnapshotSource) {
	if !p.publishing() {
		return
	}

	p.mu.Lock()
	views := p.zoneViewsLocked()
	p.mu.Unlock()

	if p.publication.publishZoneViews(views, source) {
		p.persistSemanticCache(source)
	}
}

// publishDHW applies stale expiry, hands the native hot water view to the
// publication owner and persists the semantic cache after a publication; an
// expiry persists the snapshot whatever its source.
func (p *vaillantSemanticPoller) publishDHW(source semanticSnapshotSource) {
	if !p.publishing() {
		return
	}

	p.mu.Lock()
	expired := p.expireDHWIfStaleLocked(source)
	view := p.dhwViewLocked(expired)
	p.mu.Unlock()

	if !p.publication.publishDhwView(view, source) {
		return
	}
	if view.snapshot == nil && view.expired {
		p.persistSemanticSnapshot()
		return
	}
	p.persistSemanticCache(source)
}

// publishCircuits hands the native circuits view to the publication owner
// and persists the semantic cache after a publication.
func (p *vaillantSemanticPoller) publishCircuits(source semanticSnapshotSource) {
	if !p.publishing() {
		return
	}

	p.mu.Lock()
	view := p.circuitsViewLocked()
	p.mu.Unlock()

	if p.publication.publishCircuitsView(view, source) {
		p.persistSemanticCache(source)
	}
}

// publishBoilerStatus merges the published thermal flow into the retained
// boiler snapshot, hands a detached copy to the publication owner and
// persists the semantic cache after a publication. It holds the owner's
// boiler publication lock across the merge, the copy and the publication.
func (p *vaillantSemanticPoller) publishBoilerStatus(source semanticSnapshotSource) {
	if !p.publishing() {
		return
	}

	boilerPublish := p.boilerPublishLock()
	boilerPublish.Lock()
	defer boilerPublish.Unlock()

	p.mergePublishedBoilerFlow()
	p.mu.Lock()
	snapshot := cloneBoilerSnapshot(p.boiler)
	p.mu.Unlock()

	if p.publication.publishBoilerStatusView(snapshot, source) {
		p.persistSemanticCache(source)
	}
}

// publishRadioDevices hands the native radio device views to the publication
// owner and persists the semantic cache after a publication.
func (p *vaillantSemanticPoller) publishRadioDevices(source semanticSnapshotSource) {
	if !p.publishing() {
		return
	}

	p.mu.Lock()
	views := p.radioDeviceViewsLocked()
	p.mu.Unlock()

	if p.publication.publishRadioDeviceViews(views, source) {
		p.persistSemanticCache(source)
	}
}

// publishSystem hands the native system view, with the hot water bivalence
// bound resolved for the discovered controller, to the publication owner;
// after a publication it persists the semantic cache and republishes the
// circuits.
func (p *vaillantSemanticPoller) publishSystem(source semanticSnapshotSource) {
	if !p.publishing() {
		return
	}

	p.mu.Lock()
	view := vaillantSystemView{snapshot: cloneSystemSnapshot(p.system)}
	p.mu.Unlock()
	if view.snapshot != nil {
		view.dhwBivalenceBound = p.controllerConfigBound()
	}

	if !p.publication.publishSystemView(view, source) {
		return
	}
	p.persistSemanticCache(source)
	p.publishCircuits(source)
}

// publishFM5Semantic hands the native FM5 view to the publication owner and
// then republishes the circuits, whose managing device follows the FM5 mode.
func (p *vaillantSemanticPoller) publishFM5Semantic(source semanticSnapshotSource) {
	if !p.publishing() {
		return
	}

	p.mu.Lock()
	view := vaillantFM5View{
		verdict:   p.fm5Interpretation,
		solar:     cloneSolarSnapshot(p.solar),
		cylinders: cloneCylinderSnapshotsMap(p.solarCylinders),
	}
	p.mu.Unlock()

	p.publication.publishFM5View(view, source)
	p.publishCircuits(source)
}

func (p *vaillantSemanticPoller) persistSemanticCache(source semanticSnapshotSource) {
	if p == nil || source != semanticSnapshotSourceLive {
		return
	}
	p.persistSemanticSnapshot()
}

// persistSemanticSnapshot hands the retained native zone values to the
// publication owner, which saves the published planes with them.
func (p *vaillantSemanticPoller) persistSemanticSnapshot() {
	if !p.publishing() || !p.publication.persistsCache() {
		return
	}
	p.mu.Lock()
	overlays := p.zoneCacheOverlaysLocked()
	p.mu.Unlock()
	p.publication.persistCache(overlays)
}

// zoneCacheOverlaysLocked returns a detached copy of the retained zone values
// that the persisted cache takes over the published zones, keyed by instance.
// The caller holds p.mu.
func (p *vaillantSemanticPoller) zoneCacheOverlaysLocked() map[byte]vaillantZoneCacheOverlay {
	overlays := make(map[byte]vaillantZoneCacheOverlay, len(p.zones))
	for instance, entry := range p.zones {
		if entry == nil {
			continue
		}
		overlays[instance] = vaillantZoneCacheOverlay{
			currentTempC:         cloneFloat64Ptr(entry.CurrentTempC),
			humidityPct:          cloneFloat64Ptr(entry.HumidityPct),
			roomMappingRaw:       cloneUint16Ptr(entry.ConfigurationRoomTemperatureZoneMappingRaw),
			associatedCircuitRaw: cloneUint16Ptr(entry.ConfigurationAssociatedCircuitRaw),
		}
	}
	return overlays
}
