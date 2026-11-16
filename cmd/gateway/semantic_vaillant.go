package main

import (
	"context"
	"expvar"
	"log"
	"sync"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
	"github.com/Project-Helianthus/helianthus-ebusreg/vaillant/productids"
)

const (
	semanticStartupProbeTimeout        = 900 * time.Millisecond
	semanticStartupSlotFastMaxInstance = byte(0x02)
	semanticStartupSlotFullMaxInstance = byte(0x0A)
	semanticStartupCriticalDHWAttempts = 3
	semanticIdentityRecoveryTimeout    = 35 * time.Second
	semanticCircuitFullScanInterval    = 30 * time.Minute
	semanticCircuitPartialScanInterval = 5 * time.Minute
)

var (
	semanticStartupPrimingBudget     = 35 * time.Second
	semanticStartupPrimingRetryDelay = 500 * time.Millisecond
)

var (
	semanticReadBreakerTransitionsTotal  = expvar.NewMap("semantic_read_breaker_transitions_total")
	semanticReadBreakerSuppressedTotal   = expvar.NewMap("semantic_read_breaker_suppressed_total")
	semanticZonePresenceTransitionsTotal = expvar.NewMap("semantic_zone_presence_transitions_total")
	semanticZoneCount                    = expvar.NewInt("semantic_zone_count")
	semanticDHWStaleExpiryTotal          = expvar.NewInt("semantic_dhw_stale_expiry_total")
	semanticDHWUpdatesTotal              = expvar.NewInt("semantic_dhw_updates_total")
	semanticRegulatorState               = expvar.NewString("semantic_regulator_state")
	semanticRegulatorTransitionsTotal    = expvar.NewMap("semantic_regulator_transitions_total")
	// semanticB524WidthMismatchTotal counts B524 replies whose value width
	// does not match the fixed-width reader, keyed by reader kind.
	semanticB524WidthMismatchTotal = expvar.NewMap("semantic_b524_width_mismatch_total")

	passiveShadowSubscriberPriority = ebusgateway.DedupSubscriberCritical
	passiveShadowSubscriberBuffer   = 256
	passiveShadowRetryDelay         = 100 * time.Millisecond
	defaultFM5EvidenceTTL           = 30 * time.Minute
)

func startVaillantSemanticPolling(ctx context.Context, cfg ebusgateway.Config, gateway *ebusgateway.Gateway, provider *graphql.LiveSemanticProvider, hub *graphql.BroadcastHub, startupBarrier <-chan struct{}) *vaillantSemanticPoller {
	if gateway == nil || gateway.Bus == nil || gateway.Registry == nil || provider == nil {
		return nil
	}

	cacheStore := newSemanticCacheStore(cfg.SemanticCachePath, log.Printf)
	cacheHydration, cacheLoaded := preloadSemanticCache(provider, cacheStore)
	poller := newVaillantSemanticPoller(cfg, gateway, provider, hub, cacheStore)
	poller.startupBarrier = startupBarrier
	if cacheLoaded {
		poller.hydrateFromCache(cacheHydration)
	}
	poller.Start(ctx)
	return poller
}

// sourceAndCompanion returns the initiator and companion currently selected
// for the poller under its state lock.
func (p *vaillantSemanticPoller) sourceAndCompanion() (byte, byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.source, p.companion
}

// setSourceAndCompanion rebinds the poller to the initiator and companion
// admitted by the current generation, so a successor that selects different
// addresses is not served with the first generation's cached ones.
func (p *vaillantSemanticPoller) setSourceAndCompanion(source, companion byte) {
	if p == nil {
		return
	}
	p.mu.Lock()
	p.source, p.companion = source, companion
	p.mu.Unlock()
}

type vaillantSemanticPoller struct {
	scheduler *ebusgateway.SemanticReadScheduler
	tasks     *semanticTaskScheduler

	reg    *registry.DeviceRegistry
	bus    *protocol.Bus
	shadow *ebusgateway.ShadowCache

	// publication owns the GraphQL provider, the broadcast hub, the cache
	// persister and the boiler publication lock; nil for a registry-only
	// poller, whose accessors then return nil collaborators.
	publication *vaillantSemanticPublication

	transportConfig ebusgateway.TransportConfig
	watchObserver   ebusgateway.WatchObserver
	watchEfficiency ebusgateway.WatchEfficiencyObserver

	source                   byte
	companion                byte
	requestTimeout           time.Duration
	discoveryInterval        time.Duration
	configInterval           time.Duration
	stateInterval            time.Duration
	energyInterval           time.Duration
	scheduleInterval         time.Duration
	boilerFastInterval       time.Duration
	boilerMediumInterval     time.Duration
	boilerSlowInterval       time.Duration
	zoneMissThreshold        int
	zoneHitThreshold         int
	dhwStaleTTL              time.Duration
	deviceSlotRediscoveryTTL time.Duration
	fm5EvidenceTTL           time.Duration
	circuitFullScanInterval  time.Duration

	pollMu sync.Mutex
	readMu sync.Mutex

	// identityProbedAddresses tracks per-address probe attempts for
	// EnqueueAddressIdentityProbe (P5). Each address is probed at
	// most once per gateway lifetime; failures are not retried.
	identityProbedAddresses sync.Map // map[byte]struct{}

	catalog    productids.Catalog
	catalogErr error

	mu                          sync.Mutex
	controller                  byte
	boilerAddress               byte
	boilerThermal               *boilerThermalPublisher
	regulatorCapability         productids.ControllerCapability
	regAbsenceState             regulatorAbsenceState
	discoveryFallbackSuspended  bool
	controllerEverDiscovered    bool
	regAbsenceSince             time.Time
	registryDeviceCount         int
	regulatorRecheckInterval    time.Duration
	regulatorAbsenceGrace       time.Duration
	zones                       map[byte]*vaillantZoneSnapshot
	presence                    map[byte]*zonePresenceRecord
	dhw                         *vaillantDhwSnapshot
	dhwLastUpdateAt             time.Time
	boiler                      *vaillantBoilerSnapshot
	system                      *vaillantSystemSnapshot
	circuits                    map[byte]*vaillantCircuitSnapshot
	lastCircuitFullScanAt       time.Time
	lastCircuitFullScanComplete bool
	radioDevices                map[radioDeviceKey]*vaillantRadioDeviceSnapshot
	deviceSlotCache             map[deviceSlotKey]bool // OP=0x06 slots retained for steady-state detail refresh
	deviceSlotDiscoveryDone     bool
	deviceSlotDiscoveryAt       time.Time
	startupSemanticPrimed       bool
	startupRadioDevicesProbed   bool
	fm5Mode                     vaillantFM5Mode
	fm5Interpretation           vaillantFM5Interpretation
	fm5EvidenceRevision         uint64
	fm5EvidenceGeneration       uint64
	fm5IdentityScanComplete     bool
	fm5IdentityIncoherent       bool
	fm5ConfigurationFailed      bool
	fm5RegistryEvidenceIgnored  bool
	fm5IdentityObservedAt       time.Time
	solar                       *vaillantSolarSnapshot
	solarCylinders              map[byte]*vaillantCylinderSnapshot

	adapterInfo    *vaillantAdapterInfoState
	startupBarrier <-chan struct{}

	refreshFromEbusdGrabFn       func(context.Context) (map[byte]bool, bool)
	b524ProbeFn                  func(ctx context.Context, target, opcode, group, instance byte, addr uint16) bool
	sendFrameFn                  func(ctx context.Context, frame protocol.Frame) (*protocol.Frame, error)
	routerPlanesRefreshFn        func()
	withFM5ObservationGeneration func(func(uint64)) bool
	nowFn                        func() time.Time
	// pauseFn replaces the waits between and after semantic read attempts;
	// nil waits on the wall clock. Offline scenarios keep pauses on a
	// simulated clock.
	pauseFn func(context.Context, time.Duration) bool

	// b524WidthMismatchLoggedAt holds, per logged selector, when the last
	// width mismatch line was written.
	b524WidthMismatchMu       sync.Mutex
	b524WidthMismatchLoggedAt map[b524WidthMismatchSelector]time.Time
}

func (p *vaillantSemanticPoller) Start(ctx context.Context) {
	p.pollSchedule().Start(ctx)
}

func (p *vaillantSemanticPoller) enqueueTask(key semanticTaskKey, priority semanticTaskPriority, fn func(context.Context)) {
	p.pollSchedule().enqueueTask(key, priority, fn)
}

func (p *vaillantSemanticPoller) withPollLock(ctx context.Context, fn func(context.Context)) {
	p.pollSchedule().withPollLock(ctx, fn)
}

// pause waits d, or until ctx ends, and reports whether the full pause
// elapsed.
func (p *vaillantSemanticPoller) pause(ctx context.Context, d time.Duration) bool {
	if p != nil && p.pauseFn != nil {
		return p.pauseFn(ctx, d)
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (p *vaillantSemanticPoller) now() time.Time {
	if p == nil || p.nowFn == nil {
		return time.Now()
	}
	return p.nowFn()
}
