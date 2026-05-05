package ebusgateway

import (
	"bytes"
	"context"
	"expvar"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"encoding/hex"

	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
)

const (
	observeFirstFramesHardCap      = 2048
	observeFirstErrorsHardCap      = 512
	observeFirstPeriodicitySamples = 3
	observeFirstBusyWindowHorizon  = time.Hour

	watchEfficiencySavedWindowSize = 32
	watchEfficiencySavedMinSamples = 5
	watchEfficiencySavedStaleTTL   = 15 * time.Minute

	specimenMaxPerFamily = 32
	specimenMaxFamilies  = 64
)

var (
	passiveWarmupStates       = []string{"unavailable", "warming_up", "available"}
	passiveUnavailableReasons = []string{
		"startup_timeout",
		"reconnect_timeout",
		"socket_loss",
		"flap_dampened",
		"unsupported_or_misconfigured",
		"capability_withdrawn",
	}
	passiveProbeOutcomes      = []string{"confirmed", "withdrawn", "timed_out"}
	passiveCompletionModes    = []string{"thresholds_met", "fallback_path", "warmup_disabled"}
	passiveWarmupBlockers     = []string{"connected_observation_window", "completed_transactions", "healthy_symbol_ingress", "post_reset_settling", "startup_outer_window"}
	busyWindows               = []string{"1m", "5m", "15m", "1h"}
	dedupDegradedReasons      = []string{"fingerprint_emission_failure", "observer_panic", "epoch_reset", "critical_overflow", "explicit_discontinuity", "dedup_output_overflow"}
	dedupPendingFlushReasons  = []string{"capacity", "grace_expiry", "epoch_reset", "critical_overflow"}
	passiveFanoutConsumers    = []string{"broadcast_listener", "dedup", "observability_store", "debug_summary"}
	reconstructorRecoveryKeys = []string{"unexpected_syn", "transport_reset", "decode_fault"}
)

type BusObservabilityStore struct {
	cfg Config

	now func() time.Time

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	reconstructor *PassiveTransactionReconstructor

	mu sync.RWMutex

	transportClass       string
	activeTimingQuality  string
	passiveTimingQuality string
	lastUpdatedAt        time.Time
	// Effective observe-first flags are config-derived and process-lifetime immutable.
	// This clock records when that normalized process configuration was captured.
	featureFlagsUpdatedAt time.Time

	frames     map[frameSeriesKey]uint64
	errors     map[errorSeriesKey]uint64
	frameBytes map[frameBytesSeriesKey]uint64

	recent      []BusMessageRecord
	recentStart int
	recentLen   int

	addressBuckets map[byte]string
	addressOrder   []byte

	busySegments []busySegment
	totalBusy    time.Duration

	periodicity               map[periodicityKey]*BusPeriodicityEntry
	periodicityOverflowTotal  uint64
	seriesBudgetOverflowTotal uint64
	watchEfficiency           watchEfficiencyRuntime

	passive passiveWarmupRuntime

	specimens map[string]*specimenFamilyBucket // keyed by family

	startupSurfaceProvider func() *BusObservabilityStartup

	energyFreshnessMetricsRefresher func(now time.Time, passiveState string)
	busAdmission                    *BusAdmission
	admissionStabilityWindow        *AdmissionStabilityWindow
}

// SetAdmissionStabilityWindow installs the AD08 / AD22 flap-mitigation
// window on the store. When set, RecordBusAdmissionTransition will only
// flip the envelope's bus_admission field after the new state has been
// stable for state_min_stability_s. Caller (cmd/gateway/main.go) constructs
// the window from cfg.StateMinStabilitySeconds and passes it once at
// startup. If unset, RecordBusAdmissionTransition writes through
// immediately (legacy behavior; tests may use this mode).
func (store *BusObservabilityStore) SetAdmissionStabilityWindow(window *AdmissionStabilityWindow) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.admissionStabilityWindow = window
}

// RecordBusAdmissionTransition is the production-side setter for the
// additive bus_admission field per AD08. The state argument MUST be one
// of {"pending", "active", "degraded"}; source/companionTarget are byte
// values from source-address selection or override; reason is non-empty only when
// state="degraded".
//
// When an AdmissionStabilityWindow is installed (production path), the
// state observation is gated through it; transient flaps within the
// window do NOT flip the envelope nor data_hash. When no window is
// installed, the transition is applied immediately (test path).
//
// Returns true if the envelope's bus_admission field actually changed
// as a result of this call (including stability-window-mediated flips).
func (store *BusObservabilityStore) RecordBusAdmissionTransition(state string, source, companionTarget byte, reason string) bool {
	store.mu.Lock()
	defer store.mu.Unlock()
	emittedState := state
	if store.admissionStabilityWindow != nil {
		var flipped bool
		emittedState, flipped = store.admissionStabilityWindow.Observe(state)
		if !flipped {
			return false
		}
	}
	store.busAdmission = &BusAdmission{
		State:           emittedState,
		Source:          source,
		CompanionTarget: companionTarget,
		Reason:          reason,
	}
	return true
}

type BusMessageRecord struct {
	Scope       string
	Family      string
	FrameType   string
	Outcome     string
	ObservedAt  time.Time
	Source      byte
	Target      byte
	RequestLen  int
	ResponseLen int
}

type BusPeriodicityEntry struct {
	SourceBucket   string
	TargetBucket   string
	Primary        byte
	Secondary      byte
	Family         string
	State          string
	LastSeen       time.Time
	SampleCount    int
	LastInterval   time.Duration
	MeanInterval   time.Duration
	MinInterval    time.Duration
	MaxInterval    time.Duration
	lastObservedAt time.Time
	totalInterval  time.Duration
}

type frameSeriesKey struct {
	Scope     string
	Source    string
	Target    string
	Family    string
	FrameType string
}

type errorSeriesKey struct {
	Scope string
	Class string
	Phase string
}

type frameBytesSeriesKey struct {
	Scope     string
	Family    string
	FrameType string
	Part      string
}

type periodicityKey struct {
	SourceBucket string
	TargetBucket string
	Primary      byte
	Secondary    byte
}

type busySegment struct {
	Start time.Time
	End   time.Time
}

type specimenEntry struct {
	Family       string
	Source       byte
	Target       byte
	FrameType    string
	RequestData  []byte
	ResponseData []byte
	RequestLen   int
	ResponseLen  int
	Outcome      string
	FirstSeenAt  time.Time
	LastSeenAt   time.Time
	Count        uint64
	dedupKey     string
}

type specimenFamilyBucket struct {
	entries [specimenMaxPerFamily]specimenEntry
	start   int
	length  int
}

type passiveWarmupRuntime struct {
	processStartedAt           time.Time
	startupWindowClosed        bool
	state                      string
	unavailableReason          string
	sessionStartedAt           time.Time
	sessionDeadline            time.Time
	settlingDeadline           time.Time
	connectedWindow            time.Duration
	requiredTransactions       int
	completedTransactions      int
	terminalEvents             int
	symbolBaseline             uint64
	fallbackHealthy            bool
	probeAttemptsTotal         uint64
	probeOutcomes              map[string]uint64
	transitions                map[string]uint64
	lastCompletionMode         string
	completedTransactionsTotal uint64
}

type watchEfficiencyBucketKey struct {
	Family           string
	FreshnessProfile string
}

type watchEfficiencyAmbiguousKey struct {
	Family string
	Reason string
}

type watchEfficiencyMissedKey struct {
	Bucket     watchEfficiencyBucketKey
	Limitation string
}

type watchEfficiencySavedWindow struct {
	samples      [watchEfficiencySavedWindowSize]time.Duration
	count        int
	next         int
	lastSampleAt time.Time
}

type watchEfficiencyBucketRuntime struct {
	passiveHits        uint64
	directApply        uint64
	activeReadsAvoided uint64
	saved              watchEfficiencySavedWindow
}

type watchEfficiencyRuntime struct {
	buckets                             map[watchEfficiencyBucketKey]*watchEfficiencyBucketRuntime
	ambiguous                           map[watchEfficiencyAmbiguousKey]uint64
	missed                              map[watchEfficiencyMissedKey]uint64
	directApplyCandidatesEvaluatedTotal uint64
}

type watchEfficiencySnapshot struct {
	buckets                             map[watchEfficiencyBucketKey]watchEfficiencyBucketRuntime
	ambiguous                           map[watchEfficiencyAmbiguousKey]uint64
	missed                              map[watchEfficiencyMissedKey]uint64
	directApplyCandidatesEvaluatedTotal uint64
}

func NewBusObservabilityStore(cfg Config) *BusObservabilityStore {
	cfg = applyDefaults(cfg)
	now := time.Now()
	store := &BusObservabilityStore{
		cfg:                   cfg,
		now:                   time.Now,
		transportClass:        string(canonicalTransportProtocol(cfg.TransportConfig.Protocol)),
		activeTimingQuality:   timingQualityForActive(cfg),
		passiveTimingQuality:  timingQualityForPassive(cfg),
		lastUpdatedAt:         now,
		featureFlagsUpdatedAt: now,
		frames:                make(map[frameSeriesKey]uint64),
		errors:                make(map[errorSeriesKey]uint64),
		frameBytes:            make(map[frameBytesSeriesKey]uint64),
		recent:                make([]BusMessageRecord, cfg.ObserveFirstRecentMessageCapacity),
		addressBuckets:        make(map[byte]string),
		specimens:             make(map[string]*specimenFamilyBucket),
		periodicity:           make(map[periodicityKey]*BusPeriodicityEntry),
		watchEfficiency: watchEfficiencyRuntime{
			buckets:   make(map[watchEfficiencyBucketKey]*watchEfficiencyBucketRuntime),
			ambiguous: make(map[watchEfficiencyAmbiguousKey]uint64),
			missed:    make(map[watchEfficiencyMissedKey]uint64),
		},
		passive: passiveWarmupRuntime{
			processStartedAt: now,
			state:            "unavailable",
			probeOutcomes:    make(map[string]uint64),
			transitions:      make(map[string]uint64),
		},
	}
	if reason := passiveTransportUnavailableReason(cfg); reason != "" {
		store.passive.unavailableReason = reason
	}
	return store
}

func (store *BusObservabilityStore) SetStartupSurfaceProvider(provider func() *BusObservabilityStartup) {
	if store == nil {
		return
	}
	store.mu.Lock()
	store.startupSurfaceProvider = provider
	store.mu.Unlock()
}

func (store *BusObservabilityStore) startupSurface() *BusObservabilityStartup {
	if store == nil {
		return nil
	}
	store.mu.RLock()
	provider := store.startupSurfaceProvider
	store.mu.RUnlock()
	if provider == nil {
		return nil
	}
	return cloneBusObservabilityStartup(provider())
}

func (store *BusObservabilityStore) OnBusEvent(event protocol.BusEvent) error {
	if store == nil {
		return nil
	}

	store.mu.Lock()
	defer store.mu.Unlock()

	mutated := false
	switch event.Kind {
	case protocol.BusEventAttemptComplete:
		store.recordActiveFrameLocked(event)
		mutated = true
	case protocol.BusEventTimeout, protocol.BusEventNACK, protocol.BusEventCRCMismatch, protocol.BusEventEchoMismatch:
		store.recordActiveErrorLocked(event)
		mutated = true
	case protocol.BusEventRetry:
		if event.Outcome == protocol.BusOutcomeCollision {
			store.incrementErrorLocked(errorSeriesKey{
				Scope: "active",
				Class: "collision",
				Phase: "request",
			})
			mutated = true
		}
	}
	if mutated {
		store.touchLocked(store.now())
	}
	return nil
}

func (store *BusObservabilityStore) AttachReconstructor(ctx context.Context, reconstructor *PassiveTransactionReconstructor) error {
	if store == nil || reconstructor == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}

	if store.cfg.LocalAddressSnapshotter != nil {
		reconstructor.SetLocalAddressSnapshotter(store.cfg.LocalAddressSnapshotter)
	}

	store.mu.Lock()
	if store.ctx != nil {
		store.mu.Unlock()
		return fmt.Errorf("observability store already attached")
	}
	store.ctx, store.cancel = context.WithCancel(ctx)
	store.reconstructor = reconstructor
	store.mu.Unlock()

	store.wg.Add(1)
	go store.runPassiveLoop()
	return nil
}

func (store *BusObservabilityStore) Close() error {
	if store == nil {
		return nil
	}
	store.mu.Lock()
	cancel := store.cancel
	store.cancel = nil
	store.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	store.wg.Wait()
	return nil
}

func (store *BusObservabilityStore) runPassiveLoop() {
	defer store.wg.Done()

	for {
		store.mu.RLock()
		ctx := store.ctx
		reconstructor := store.reconstructor
		store.mu.RUnlock()
		if ctx == nil || reconstructor == nil {
			return
		}
		if err := ctx.Err(); err != nil {
			return
		}

		subscription, err := reconstructor.Subscribe("observability-store", PassiveSubscriberCritical, 0)
		if err != nil {
			timer := time.NewTimer(100 * time.Millisecond)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
				continue
			}
		}

		snapshot := reconstructor.Snapshot()
		now := store.now()
		store.mu.Lock()
		store.refreshPassiveStateLocked(now, snapshot.TapStatus)
		store.bootstrapPassiveWarmupFromSnapshotLocked(now, snapshot)
		store.mu.Unlock()

		closedUnexpectedly := false
		for {
			select {
			case <-ctx.Done():
				subscription.Close()
				return
			case event, ok := <-subscription.Events():
				if !ok {
					closedUnexpectedly = ctx.Err() == nil
					goto nextSubscription
				}
				store.OnPassiveClassifiedEvent(event)
			}
		}

	nextSubscription:
		if closedUnexpectedly {
			store.mu.Lock()
			store.passiveNoteTimeoutLocked(store.now(), "socket_loss")
			store.mu.Unlock()
		}
	}
}

func (store *BusObservabilityStore) bootstrapPassiveWarmupFromSnapshotLocked(now time.Time, snapshot PassiveReconstructorSnapshot) {
	if store.passive.state == "warming_up" || store.passive.state == "available" {
		return
	}
	if !snapshot.TapStatus.Connected {
		return
	}

	observedAt := snapshot.TapStatus.LastConnectAt
	if observedAt.IsZero() {
		observedAt = now
	}
	store.passiveStartWarmupLocked(observedAt, false)
}

func (store *BusObservabilityStore) OnPassiveClassifiedEvent(event PassiveClassifiedEvent) {
	if store == nil {
		return
	}
	store.mu.Lock()
	defer store.mu.Unlock()

	store.refreshPassiveStateLocked(store.now(), store.reconstructorSnapshotLocked().TapStatus)

	switch event.Kind {
	case PassiveClassifiedEventBroadcastFrame, PassiveClassifiedEventMasterFrame, PassiveClassifiedEventTransaction:
		store.recordPassiveFrameLocked(event)
	case PassiveClassifiedEventAbandonedTransaction:
		store.recordPassiveAbandonedLocked(event)
	case PassiveClassifiedEventDiscontinuity:
		store.recordPassiveDiscontinuityLocked(event)
	}
	if !event.ObservedAt.IsZero() {
		store.touchLocked(event.ObservedAt)
	} else {
		store.touchLocked(store.now())
	}
}

func (store *BusObservabilityStore) ObserveWatchRead(event WatchEfficiencyReadEvent) {
	if store == nil {
		return
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	store.observeWatchReadLocked(event)
}

func (store *BusObservabilityStore) ObserveWatchDirectApply(event WatchEfficiencyDirectApplyEvent) {
	if store == nil {
		return
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	store.observeWatchDirectApplyLocked(event)
}

func (store *BusObservabilityStore) RecentMessages(limit int) []BusMessageRecord {
	if store == nil {
		return nil
	}
	store.mu.RLock()
	defer store.mu.RUnlock()
	return store.recentMessagesLocked(limit)
}

func (store *BusObservabilityStore) PeriodicitySnapshot() []BusPeriodicityEntry {
	if store == nil {
		return nil
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	store.evictStalePeriodicityLocked(store.now())
	return store.periodicitySnapshotLocked()
}

func (store *BusObservabilityStore) observeWatchReadLocked(event WatchEfficiencyReadEvent) {
	observedAt := event.ObservedAt
	if observedAt.IsZero() {
		observedAt = store.now()
	}

	bucket, reason, include := resolveWatchEfficiencyBucket(event.Key, event.Descriptor)
	if reason != "" {
		family := familyForAmbiguous(event.Key, event.Descriptor)
		store.watchEfficiency.ambiguous[watchEfficiencyAmbiguousKey{
			Family: family,
			Reason: reason,
		}]++
		return
	}
	if !include {
		return
	}

	series := store.ensureWatchEfficiencyBucketLocked(bucket)
	eligibleForMiss := watchEfficiencyEligibleForMiss(event.Descriptor, store.cfg.ObserveFirstFlags, event.MaxAge)
	if event.Stats.ActiveFetchAttempted && event.Stats.ActiveFetchSucceeded && eligibleForMiss {
		series.saved.add(event.Stats.ActiveFetchDuration, observedAt)
	}
	if event.Stats.ServedFromPassiveShadow {
		series.passiveHits++
		series.activeReadsAvoided++
		return
	}
	if !event.Stats.ActiveFetchAttempted {
		return
	}
	if !eligibleForMiss {
		return
	}
	limitation := watchEfficiencyTransportLimitation(store.cfg)
	if limitation == "" {
		return
	}
	store.watchEfficiency.missed[watchEfficiencyMissedKey{
		Bucket:     bucket,
		Limitation: limitation,
	}]++
}

func (store *BusObservabilityStore) observeWatchDirectApplyLocked(event WatchEfficiencyDirectApplyEvent) {
	candidateEvaluated := event.CandidateEvaluated
	accepted := event.Accepted
	if !candidateEvaluated && !accepted {
		// Preserve legacy semantics for call sites created before candidate accounting.
		candidateEvaluated = true
		accepted = true
	}
	if accepted {
		candidateEvaluated = true
	}
	if candidateEvaluated {
		store.watchEfficiency.directApplyCandidatesEvaluatedTotal++
	}

	bucket, reason, include := resolveWatchEfficiencyBucket(event.Key, event.Descriptor)
	if reason != "" {
		family := familyForAmbiguous(event.Key, event.Descriptor)
		store.watchEfficiency.ambiguous[watchEfficiencyAmbiguousKey{
			Family: family,
			Reason: reason,
		}]++
		return
	}
	if !include {
		return
	}
	if !accepted {
		return
	}
	series := store.ensureWatchEfficiencyBucketLocked(bucket)
	series.directApply++
	series.activeReadsAvoided++
}

func (store *BusObservabilityStore) ensureWatchEfficiencyBucketLocked(bucket watchEfficiencyBucketKey) *watchEfficiencyBucketRuntime {
	if store.watchEfficiency.buckets == nil {
		store.watchEfficiency.buckets = make(map[watchEfficiencyBucketKey]*watchEfficiencyBucketRuntime)
	}
	series := store.watchEfficiency.buckets[bucket]
	if series == nil {
		series = &watchEfficiencyBucketRuntime{}
		store.watchEfficiency.buckets[bucket] = series
	}
	return series
}

func (store *BusObservabilityStore) MetricsHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_, _ = w.Write([]byte(store.RenderPrometheus()))
	})
}

func (store *BusObservabilityStore) SetEnergyFreshnessMetricsRefresher(refresher func(now time.Time, passiveState string)) {
	if store == nil {
		return
	}
	store.mu.Lock()
	store.energyFreshnessMetricsRefresher = refresher
	store.mu.Unlock()
}

func (store *BusObservabilityStore) RenderPrometheus() string {
	if store == nil {
		return ""
	}

	store.mu.Lock()
	now := store.now()
	snapshot := store.reconstructorSnapshotLocked()
	store.refreshPassiveStateLocked(now, snapshot.TapStatus)
	store.evictStalePeriodicityLocked(now)

	frames := cloneFramesMap(store.frames)
	errors := cloneErrorsMap(store.errors)
	frameBytes := cloneFrameBytesMap(store.frameBytes)
	recentLen := store.recentLen
	totalBusy := store.totalBusy
	segments := append([]busySegment(nil), store.busySegments...)
	seriesOverflow := store.seriesBudgetOverflowTotal
	periodicityOverflow := store.periodicityOverflowTotal
	passive := store.passive
	passive.probeOutcomes = cloneStringUint64Map(passive.probeOutcomes)
	passive.transitions = cloneStringUint64Map(passive.transitions)
	watchEfficiency := store.watchEfficiencySnapshotLocked(now)
	activeTiming := store.activeTimingQuality
	passiveTiming := store.passiveTimingQuality
	transportClass := store.transportClass
	featureFlags := store.cfg.ObserveFirstFlags
	energyMetricsRefresher := store.energyFreshnessMetricsRefresher
	passiveState := passive.state
	store.mu.Unlock()

	if energyMetricsRefresher != nil {
		energyMetricsRefresher(now, passiveState)
	}

	var buffer bytes.Buffer
	writer := newPrometheusWriter(&buffer)

	writer.writeHelp("ebus_observability_transport_info", "Observe-first transport metadata.")
	writer.writeType("ebus_observability_transport_info", "gauge")
	writer.writeGaugeSample("ebus_observability_transport_info", 1, labelMap("scope", "active", "transport_class", transportClass, "timing_quality", activeTiming))
	writer.writeGaugeSample("ebus_observability_transport_info", 1, labelMap("scope", "passive", "transport_class", transportClass, "timing_quality", passiveTiming))

	writer.writeHelp("feature_flag_normalizations_total", "Observe-first feature-flag normalization events.")
	writer.writeType("feature_flag_normalizations_total", "counter")
	appliedNormalizations := make(map[ObserveFirstFeatureFlagNormalizationReason]struct{}, len(featureFlags.NormalizationReasons()))
	for _, reason := range featureFlags.NormalizationReasons() {
		appliedNormalizations[reason] = struct{}{}
	}
	for _, reason := range observeFirstFeatureFlagNormalizationReasons {
		value := 0.0
		if _, ok := appliedNormalizations[reason]; ok {
			value = 1
		}
		writer.writeCounterSample("feature_flag_normalizations_total", value, labelMap("reason", string(reason)))
	}

	writer.writeHelp("feature_flag_enabled", "Observe-first normalized feature-flag state.")
	writer.writeType("feature_flag_enabled", "gauge")
	for _, item := range []struct {
		name    string
		enabled bool
	}{
		{name: "observe_first_enabled", enabled: featureFlags.ObserveFirstEnabled()},
		{name: "passive_state_direct_apply", enabled: featureFlags.PassiveStateDirectApply()},
		{name: "passive_config_direct_apply", enabled: featureFlags.PassiveConfigDirectApply()},
	} {
		value := 0.0
		if item.enabled {
			value = 1
		}
		writer.writeGaugeSample("feature_flag_enabled", value, labelMap("flag", item.name))
	}

	writer.writeHelp("external_write_policy_state", "Observe-first normalized external write policy state.")
	writer.writeType("external_write_policy_state", "gauge")
	for _, policy := range observeFirstExternalWritePolicies {
		value := 0.0
		if featureFlags.ExternalWritePolicy() == policy {
			value = 1
		}
		writer.writeGaugeSample("external_write_policy_state", value, labelMap("policy", string(policy)))
	}

	writer.writeHelp("ebus_frames_observed_total", "Bounded observe-first frame counters.")
	writer.writeType("ebus_frames_observed_total", "counter")
	for _, item := range sortedFrameSeries(frames) {
		writer.writeCounterSample("ebus_frames_observed_total", float64(item.Value), labelMap(
			"scope", item.Key.Scope,
			"src", item.Key.Source,
			"dst", item.Key.Target,
			"family", item.Key.Family,
			"frame_type", item.Key.FrameType,
		))
	}

	writer.writeHelp("ebus_errors_total", "Bounded observe-first error counters.")
	writer.writeType("ebus_errors_total", "counter")
	for _, item := range sortedErrorSeries(errors) {
		writer.writeCounterSample("ebus_errors_total", float64(item.Value), labelMap(
			"scope", item.Key.Scope,
			"class", item.Key.Class,
			"phase", item.Key.Phase,
		))
	}

	writer.writeHelp("ebus_frame_bytes_total", "Aggregate retained frame byte counts.")
	writer.writeType("ebus_frame_bytes_total", "counter")
	for _, item := range sortedFrameBytesSeries(frameBytes) {
		writer.writeCounterSample("ebus_frame_bytes_total", float64(item.Value), labelMap(
			"scope", item.Key.Scope,
			"family", item.Key.Family,
			"frame_type", item.Key.FrameType,
			"part", item.Key.Part,
		))
	}

	writer.writeHelp("ebus_observability_recent_messages", "Current bounded recent-message occupancy.")
	writer.writeType("ebus_observability_recent_messages", "gauge")
	writer.writeGaugeSample("ebus_observability_recent_messages", float64(recentLen), nil)

	writer.writeHelp("ebus_observability_series_budget_overflow_total", "Observe-first series-budget overflow events.")
	writer.writeType("ebus_observability_series_budget_overflow_total", "counter")
	writer.writeCounterSample("ebus_observability_series_budget_overflow_total", float64(seriesOverflow), nil)

	if passive.state == "available" && passiveTiming != "unavailable" {
		writer.writeHelp("ebus_bus_busy_seconds_total", "Cumulative passive busy time.")
		writer.writeType("ebus_bus_busy_seconds_total", "counter")
		writer.writeCounterSample("ebus_bus_busy_seconds_total", totalBusy.Seconds(), nil)

		writer.writeHelp("ebus_bus_busy_ratio", "Recent passive busy ratio by window.")
		writer.writeType("ebus_bus_busy_ratio", "gauge")
		for _, window := range busyWindows {
			duration := parseBusyWindow(window)
			writer.writeGaugeSample("ebus_bus_busy_ratio", clippedRatio(windowBusyDuration(segments, now, duration), duration), labelMap("window", window))
		}
	}

	writer.writeHelp("ebus_passive_tap_reconnect_attempts_total", "Passive tap connect attempts.")
	writer.writeType("ebus_passive_tap_reconnect_attempts_total", "counter")
	writer.writeCounterSample("ebus_passive_tap_reconnect_attempts_total", float64(snapshot.TapStatus.ConnectAttemptCount), nil)

	writer.writeHelp("ebus_passive_tap_reconnect_successes_total", "Passive tap connect successes.")
	writer.writeType("ebus_passive_tap_reconnect_successes_total", "counter")
	writer.writeCounterSample("ebus_passive_tap_reconnect_successes_total", float64(snapshot.TapStatus.ConnectCount), nil)

	writer.writeHelp("ebus_passive_tap_reconnect_failures_total", "Passive tap connect failures.")
	writer.writeType("ebus_passive_tap_reconnect_failures_total", "counter")
	writer.writeCounterSample("ebus_passive_tap_reconnect_failures_total", float64(snapshot.TapStatus.ConnectFailureCount), nil)

	writer.writeHelp("ebus_passive_tap_connected", "Passive tap connected state.")
	writer.writeType("ebus_passive_tap_connected", "gauge")
	if snapshot.TapStatus.Connected {
		writer.writeGaugeSample("ebus_passive_tap_connected", 1, nil)
	} else {
		writer.writeGaugeSample("ebus_passive_tap_connected", 0, nil)
	}

	writer.writeHelp("ebus_passive_capability_probe_attempts_total", "Passive capability probe attempts.")
	writer.writeType("ebus_passive_capability_probe_attempts_total", "counter")
	writer.writeCounterSample("ebus_passive_capability_probe_attempts_total", float64(passive.probeAttemptsTotal), nil)

	writer.writeHelp("ebus_passive_capability_probe_outcomes_total", "Passive capability probe outcomes.")
	writer.writeType("ebus_passive_capability_probe_outcomes_total", "counter")
	for _, outcome := range passiveProbeOutcomes {
		writer.writeCounterSample("ebus_passive_capability_probe_outcomes_total", float64(passive.probeOutcomes[outcome]), labelMap("outcome", outcome))
	}

	writer.writeHelp("ebus_passive_warmup_state", "Global passive warmup state.")
	writer.writeType("ebus_passive_warmup_state", "gauge")
	for _, state := range passiveWarmupStates {
		value := 0.0
		if passive.state == state {
			value = 1
		}
		writer.writeGaugeSample("ebus_passive_warmup_state", value, labelMap("state", state))
	}

	writer.writeHelp("ebus_passive_capability_unavailable_reason", "Current passive unavailability reason.")
	writer.writeType("ebus_passive_capability_unavailable_reason", "gauge")
	for _, reason := range passiveUnavailableReasons {
		value := 0.0
		if passive.state == "unavailable" && passive.unavailableReason == reason {
			value = 1
		}
		writer.writeGaugeSample("ebus_passive_capability_unavailable_reason", value, labelMap("reason", reason))
	}

	writer.writeHelp("ebus_passive_warmup_elapsed_seconds", "Current passive warmup elapsed time.")
	writer.writeType("ebus_passive_warmup_elapsed_seconds", "gauge")
	writer.writeGaugeSample("ebus_passive_warmup_elapsed_seconds", passiveElapsedSeconds(passive, now), nil)

	writer.writeHelp("ebus_passive_warmup_completed_transactions", "Current passive warmup progress.")
	writer.writeType("ebus_passive_warmup_completed_transactions", "gauge")
	writer.writeGaugeSample("ebus_passive_warmup_completed_transactions", float64(passive.completedTransactions), nil)

	writer.writeHelp("ebus_passive_completed_transactions_total", "Cumulative completed passive transactions observed by the passive reconstructor.")
	writer.writeType("ebus_passive_completed_transactions_total", "counter")
	writer.writeCounterSample("ebus_passive_completed_transactions_total", float64(passive.completedTransactionsTotal), nil)

	writer.writeHelp("ebus_passive_warmup_required_transactions", "Current passive warmup threshold.")
	writer.writeType("ebus_passive_warmup_required_transactions", "gauge")
	writer.writeGaugeSample("ebus_passive_warmup_required_transactions", float64(passive.requiredTransactions), nil)

	writer.writeHelp("ebus_passive_warmup_completion_mode", "Passive warmup completion mode.")
	writer.writeType("ebus_passive_warmup_completion_mode", "gauge")
	for _, mode := range passiveCompletionModes {
		value := 0.0
		if passive.lastCompletionMode == mode {
			value = 1
		}
		writer.writeGaugeSample("ebus_passive_warmup_completion_mode", value, labelMap("mode", mode))
	}

	writer.writeHelp("ebus_passive_warmup_blocker_reason", "Dominant passive warmup blocker.")
	writer.writeType("ebus_passive_warmup_blocker_reason", "gauge")
	blocker := passiveBlockerReason(passive, now)
	for _, reason := range passiveWarmupBlockers {
		value := 0.0
		if blocker == reason {
			value = 1
		}
		writer.writeGaugeSample("ebus_passive_warmup_blocker_reason", value, labelMap("reason", reason))
	}

	writer.writeHelp("ebus_passive_capability_transitions_total", "Passive capability transitions.")
	writer.writeType("ebus_passive_capability_transitions_total", "counter")
	for _, item := range sortedTransitionMap(passive.transitions) {
		from, to, ok := strings.Cut(item.Key, "->")
		if !ok {
			continue
		}
		writer.writeCounterSample("ebus_passive_capability_transitions_total", float64(item.Value), labelMap("from", from, "to", to))
	}

	writer.writeHelp("ebus_periodicity_tuple_budget_overflow_total", "Periodicity tuple budget evictions.")
	writer.writeType("ebus_periodicity_tuple_budget_overflow_total", "counter")
	writer.writeCounterSample("ebus_periodicity_tuple_budget_overflow_total", float64(periodicityOverflow), nil)

	writer.writeHelp("ebus_dedup_degraded_state", "Current dedup degraded state.")
	writer.writeType("ebus_dedup_degraded_state", "gauge")
	writer.writeGaugeSample("ebus_dedup_degraded_state", readDedupDegradedState(), nil)

	writer.writeHelp("ebus_dedup_degraded_total", "Dedup degraded transitions by reason.")
	writer.writeType("ebus_dedup_degraded_total", "counter")
	for _, reason := range dedupDegradedReasons {
		writer.writeCounterSample("ebus_dedup_degraded_total", float64(readExpvarMapInt(observeFirstDedupDegradedTransitionsTotal, reason)), labelMap("reason", reason))
	}

	writer.writeHelp("ebus_dedup_epoch_resets_total", "Dedup epoch resets.")
	writer.writeType("ebus_dedup_epoch_resets_total", "counter")
	writer.writeCounterSample("ebus_dedup_epoch_resets_total", float64(readExpvarInt(observeFirstDedupEpochResetTotal)), nil)

	writer.writeHelp("ebus_dedup_pending_flush_total", "Dedup pending flushes by reason.")
	writer.writeType("ebus_dedup_pending_flush_total", "counter")
	for _, reason := range dedupPendingFlushReasons {
		writer.writeCounterSample("ebus_dedup_pending_flush_total", float64(readExpvarMapInt(observeFirstDedupPendingFlushTotal, reason)), labelMap("reason", reason))
	}

	writer.writeHelp("ebus_dedup_local_participant_inbound_total", "Dedup local-participant inbound traffic.")
	writer.writeType("ebus_dedup_local_participant_inbound_total", "counter")
	writer.writeCounterSample("ebus_dedup_local_participant_inbound_total", float64(readExpvarInt(observeFirstDedupLocalParticipantInboundTotal)), nil)

	writer.writeHelp("ebus_passive_fanout_overflow_total", "Passive classified fan-out overflow counts.")
	writer.writeType("ebus_passive_fanout_overflow_total", "counter")
	for _, consumer := range passiveFanoutConsumers {
		value := snapshot.FanoutOverflowTotal[consumer]
		writer.writeCounterSample("ebus_passive_fanout_overflow_total", float64(value), labelMap("consumer", consumer, "criticality", "critical"))
		writer.writeCounterSample("ebus_passive_fanout_overflow_total", 0, labelMap("consumer", consumer, "criticality", "noncritical"))
	}

	writer.writeHelp("ebus_passive_reconstructor_recoveries_total", "Passive reconstructor recovery counts.")
	writer.writeType("ebus_passive_reconstructor_recoveries_total", "counter")
	for _, reason := range reconstructorRecoveryKeys {
		writer.writeCounterSample("ebus_passive_reconstructor_recoveries_total", float64(snapshot.RecoveryTotal[reason]), labelMap("reason", reason))
	}

	writer.writeHelp("passive_hits_total", "Observe-first scheduler reads served from passive shadow.")
	writer.writeType("passive_hits_total", "counter")
	for _, item := range sortedWatchEfficiencyBuckets(watchEfficiency.buckets) {
		writer.writeCounterSample("passive_hits_total", float64(item.Value.passiveHits), labelMap(
			"family", item.Key.Family,
			"freshness_profile", item.Key.FreshnessProfile,
		))
	}

	writer.writeHelp("direct_apply_total", "Observe-first passive direct-apply writes accepted by shadow runtime descriptor bucket.")
	writer.writeType("direct_apply_total", "counter")
	for _, item := range sortedWatchEfficiencyBuckets(watchEfficiency.buckets) {
		writer.writeCounterSample("direct_apply_total", float64(item.Value.directApply), labelMap(
			"family", item.Key.Family,
			"freshness_profile", item.Key.FreshnessProfile,
		))
	}

	writer.writeHelp("ebus_passive_direct_apply_candidates_evaluated_total", "Observe-first passive direct-apply candidates evaluated, including rejected shadow writes.")
	writer.writeType("ebus_passive_direct_apply_candidates_evaluated_total", "counter")
	writer.writeCounterSample("ebus_passive_direct_apply_candidates_evaluated_total", float64(watchEfficiency.directApplyCandidatesEvaluatedTotal), nil)

	writer.writeHelp("active_reads_avoided_total", "Observe-first active reads avoided by passive shadow hits or direct-apply.")
	writer.writeType("active_reads_avoided_total", "counter")
	for _, item := range sortedWatchEfficiencyBuckets(watchEfficiency.buckets) {
		writer.writeCounterSample("active_reads_avoided_total", float64(item.Value.activeReadsAvoided), labelMap(
			"family", item.Key.Family,
			"freshness_profile", item.Key.FreshnessProfile,
		))
	}

	writer.writeHelp("active_read_saved_seconds", "Bucket-level estimate from recent active logical request-complete durations; not a per-key guarantee.")
	writer.writeType("active_read_saved_seconds", "gauge")
	for _, item := range sortedWatchEfficiencyBuckets(watchEfficiency.buckets) {
		estimate, ok := item.Value.saved.estimateSeconds(now)
		if !ok {
			continue
		}
		writer.writeGaugeSample("active_read_saved_seconds", estimate, labelMap(
			"family", item.Key.Family,
			"freshness_profile", item.Key.FreshnessProfile,
		))
	}

	writer.writeHelp("ambiguous_total", "Observe-first watch-efficiency events that could not be bucketed.")
	writer.writeType("ambiguous_total", "counter")
	for _, item := range sortedWatchEfficiencyAmbiguous(watchEfficiency.ambiguous) {
		writer.writeCounterSample("ambiguous_total", float64(item.Value), labelMap(
			"family", item.Key.Family,
			"reason", item.Key.Reason,
		))
	}

	writer.writeHelp("missed_due_to_transport_limitations_total", "Observe-first-eligible reads that fell back to active due to passive capability limitations.")
	writer.writeType("missed_due_to_transport_limitations_total", "counter")
	for _, item := range sortedWatchEfficiencyMissed(watchEfficiency.missed) {
		writer.writeCounterSample("missed_due_to_transport_limitations_total", float64(item.Value), labelMap(
			"family", item.Key.Bucket.Family,
			"freshness_profile", item.Key.Bucket.FreshnessProfile,
			"limitation", item.Key.Limitation,
		))
	}

	energyStates := []string{"never_seen", "fresh", "warming_up", "stale", "unavailable"}
	// Only emit energy broadcast metrics when at least one broadcast energy
	// value has ever been accepted by the merge store.  Buses without B516
	// energy response broadcasts produce 25 constant-zero series that clutter
	// dashboards.
	energyHasData := readExpvarNamedMapInt("semantic_energy_merges_total", "broadcast") > 0
	if energyHasData {
		writer.writeHelp("energy_broadcast_selectors", "Current energy selector freshness state counts (recomputed at scrape time).")
		writer.writeType("energy_broadcast_selectors", "gauge")
		for _, state := range energyStates {
			writer.writeGaugeSample("energy_broadcast_selectors", float64(readExpvarNamedMapInt("energy_broadcast_selectors", state)), labelMap("state", state))
		}

		writer.writeHelp("energy_broadcast_freshness_transitions_total", "Energy freshness state transitions by selector, including scrape-time recomputation transitions.")
		writer.writeType("energy_broadcast_freshness_transitions_total", "counter")
		for _, from := range energyStates {
			for _, to := range energyStates {
				key := from + "->" + to
				writer.writeCounterSample("energy_broadcast_freshness_transitions_total", float64(readExpvarNamedMapInt("energy_broadcast_freshness_transitions_total", key)), labelMap("from", from, "to", to))
			}
		}
	}

	return buffer.String()
}

func (store *BusObservabilityStore) recordActiveFrameLocked(event protocol.BusEvent) {
	if !event.HasRequest {
		return
	}
	local := store.localAddressSnapshotLocked()
	frameType := classifyActiveFrameType(event.Request, local)
	family := classifyFamily(event.Request)
	sourceBucket := store.normalizeAddressLocked(event.Request.Source)
	targetBucket := store.normalizeAddressLocked(event.Request.Target)

	store.incrementFrameLocked(frameSeriesKey{
		Scope:     "active",
		Source:    sourceBucket,
		Target:    targetBucket,
		Family:    family,
		FrameType: frameType,
	})
	store.frameBytes[frameBytesSeriesKey{Scope: "active", Family: family, FrameType: frameType, Part: "request"}] += float64Frame(frameWireLen(event.Request))
	if event.HasResponse {
		store.frameBytes[frameBytesSeriesKey{Scope: "active", Family: family, FrameType: frameType, Part: "response"}] += float64Frame(responseWireLen(event.Response))
	}
	store.pushRecentLocked(BusMessageRecord{
		Scope:       "active",
		Family:      family,
		FrameType:   frameType,
		Outcome:     "success",
		ObservedAt:  store.now(),
		Source:      event.Request.Source,
		Target:      event.Request.Target,
		RequestLen:  frameWireLen(event.Request),
		ResponseLen: responseLen(event),
	})
}

func (store *BusObservabilityStore) recordActiveErrorLocked(event protocol.BusEvent) {
	class, phase := classifyActiveError(event)
	if class == "" {
		return
	}
	store.incrementErrorLocked(errorSeriesKey{
		Scope: "active",
		Class: class,
		Phase: phase,
	})
	if !event.HasRequest {
		return
	}
	store.pushRecentLocked(BusMessageRecord{
		Scope:       "active",
		Family:      classifyFamily(event.Request),
		FrameType:   classifyActiveFrameType(event.Request, store.localAddressSnapshotLocked()),
		Outcome:     class,
		ObservedAt:  store.now(),
		Source:      event.Request.Source,
		Target:      event.Request.Target,
		RequestLen:  frameWireLen(event.Request),
		ResponseLen: responseLen(event),
	})
}

func (store *BusObservabilityStore) recordPassiveFrameLocked(event PassiveClassifiedEvent) {
	if !event.HasRequest {
		return
	}
	if event.Kind == PassiveClassifiedEventTransaction {
		store.passive.completedTransactionsTotal++
	}
	store.bootstrapPassiveWarmupFromTrafficLocked(event)
	now := event.ObservedAt
	local := store.localAddressSnapshotLocked()
	frameType := classifyPassiveFrameType(event, local)
	family := classifyFamily(event.Request)
	sourceBucket := store.normalizeAddressLocked(event.Request.Source)
	targetBucket := store.normalizeAddressLocked(event.Request.Target)

	store.incrementFrameLocked(frameSeriesKey{
		Scope:     "passive",
		Source:    sourceBucket,
		Target:    targetBucket,
		Family:    family,
		FrameType: frameType,
	})
	store.frameBytes[frameBytesSeriesKey{Scope: "passive", Family: family, FrameType: frameType, Part: "request"}] += float64Frame(frameWireLen(event.Request))
	if event.HasResponse {
		store.frameBytes[frameBytesSeriesKey{Scope: "passive", Family: family, FrameType: frameType, Part: "response"}] += float64Frame(responseWireLen(event.Response))
	}
	store.pushRecentLocked(BusMessageRecord{
		Scope:       "passive",
		Family:      family,
		FrameType:   frameType,
		Outcome:     "success",
		ObservedAt:  now,
		Source:      event.Request.Source,
		Target:      event.Request.Target,
		RequestLen:  frameWireLen(event.Request),
		ResponseLen: passiveResponseLen(event),
	})
	store.recordPassiveWarmupSuccessLocked(event)
	store.recordBusyLocked(event)
	store.recordPeriodicityLocked(event, family)
	if !isImplementedFamily(family) && event.HasRequest {
		store.pushSpecimenLocked(family, event, frameType, "success", event.ObservedAt)
	}
}

func (store *BusObservabilityStore) bootstrapPassiveWarmupFromTrafficLocked(event PassiveClassifiedEvent) {
	if store.passive.state != "unavailable" {
		return
	}
	switch store.passive.unavailableReason {
	case "capability_withdrawn", "unsupported_or_misconfigured":
		return
	}
	snapshot := store.reconstructorSnapshotLocked()
	if !snapshot.TapStatus.Connected {
		return
	}
	observedAt := event.ObservedAt
	if observedAt.IsZero() {
		observedAt = store.now()
	}
	store.passiveStartWarmupLocked(observedAt, false)
}

func isScanRelatedAbandon(reason PassiveAbandonReason) bool {
	switch reason {
	case PassiveAbandonReasonScanTimeout, PassiveAbandonReasonScanCollision, PassiveAbandonReasonArbitrationFragment:
		return true
	default:
		return false
	}
}

func (store *BusObservabilityStore) recordPassiveAbandonedLocked(event PassiveClassifiedEvent) {
	if event.HasRequest {
		family := classifyFamily(event.Request)
		local := store.localAddressSnapshotLocked()
		frameType := classifyPassiveFrameType(event, local)
		targetBucket := store.normalizeAddressLocked(event.Request.Target)
		if isScanRelatedAbandon(event.AbandonReason) {
			targetBucket = "scan"
		}
		store.incrementFrameLocked(frameSeriesKey{
			Scope:     "passive",
			Source:    store.normalizeAddressLocked(event.Request.Source),
			Target:    targetBucket,
			Family:    family,
			FrameType: frameType,
		})
		store.frameBytes[frameBytesSeriesKey{Scope: "passive", Family: family, FrameType: frameType, Part: "request"}] += float64Frame(frameWireLen(event.Request))
		if event.HasResponse {
			store.frameBytes[frameBytesSeriesKey{Scope: "passive", Family: family, FrameType: frameType, Part: "response"}] += float64Frame(responseWireLen(event.Response))
		}
		store.pushRecentLocked(BusMessageRecord{
			Scope:       "passive",
			Family:      family,
			FrameType:   frameType,
			Outcome:     string(event.AbandonReason),
			ObservedAt:  event.ObservedAt,
			Source:      event.Request.Source,
			Target:      event.Request.Target,
			RequestLen:  frameWireLen(event.Request),
			ResponseLen: passiveResponseLen(event),
		})
		if !isImplementedFamily(family) {
			store.pushSpecimenLocked(family, event, frameType, string(event.AbandonReason), event.ObservedAt)
		}
	}
	class, phase := classifyPassiveAbandon(event.AbandonReason)
	if class != "" {
		store.incrementErrorLocked(errorSeriesKey{
			Scope: "passive",
			Class: class,
			Phase: phase,
		})
	}
	store.passive.terminalEvents++
	if !event.ObservedAt.IsZero() {
		store.touchLocked(event.ObservedAt)
	} else {
		store.touchLocked(store.now())
	}
}

func (store *BusObservabilityStore) recordPassiveDiscontinuityLocked(event PassiveClassifiedEvent) {
	class := passiveDiscontinuityClass(event.DiscontinuityReason)
	if class != "" {
		store.incrementErrorLocked(errorSeriesKey{
			Scope: "passive",
			Class: class,
			Phase: "terminal",
		})
	}
	switch event.DiscontinuityReason {
	case PassiveDiscontinuityConnected:
		store.passiveStartWarmupLocked(event.ObservedAt, false)
	case PassiveDiscontinuityDisconnected:
		store.passiveNoteTimeoutLocked(event.ObservedAt, "socket_loss")
	case PassiveDiscontinuityTransportReset, PassiveDiscontinuityDecodeFault:
		store.passiveStartWarmupLocked(event.ObservedAt, true)
	case PassiveDiscontinuityCriticalSubscriberFault:
		if event.Subscriber != "" {
			store.passive.terminalEvents++
		}
	}
}

func (store *BusObservabilityStore) recordPassiveWarmupSuccessLocked(event PassiveClassifiedEvent) {
	if store.passive.state != "warming_up" {
		return
	}
	store.passive.terminalEvents++
	store.passive.completedTransactions++
	snapshot := store.reconstructorSnapshotLocked()
	if snapshot.TapStatus.ObservedSymbolCount-store.passive.symbolBaseline >= 100 && store.passive.terminalEvents > 0 {
		store.passive.fallbackHealthy = true
	}
	if event.Kind == PassiveClassifiedEventBroadcastFrame || event.Kind == PassiveClassifiedEventMasterFrame || event.Kind == PassiveClassifiedEventTransaction {
		store.passive.fallbackHealthy = true
	}
	store.promotePassiveIfReadyLocked(event.ObservedAt, snapshot.TapStatus)
}

func (store *BusObservabilityStore) recordBusyLocked(event PassiveClassifiedEvent) {
	if store.passiveTimingQuality == "unavailable" {
		return
	}
	if event.Timing.RequestStart.IsZero() || event.Timing.Terminal.IsZero() || !event.Timing.Terminal.After(event.Timing.RequestStart) {
		return
	}
	segment := busySegment{Start: event.Timing.RequestStart, End: event.Timing.Terminal}
	store.busySegments = append(store.busySegments, segment)
	store.totalBusy += segment.End.Sub(segment.Start)
	cutoff := store.now().Add(-observeFirstBusyWindowHorizon)
	filtered := store.busySegments[:0]
	for _, item := range store.busySegments {
		if item.End.After(cutoff) {
			filtered = append(filtered, item)
		}
	}
	store.busySegments = filtered
}

func (store *BusObservabilityStore) recordPeriodicityLocked(event PassiveClassifiedEvent, family string) {
	if store.passiveTimingQuality == "unavailable" || !event.HasRequest || event.Kind == PassiveClassifiedEventAbandonedTransaction {
		return
	}
	if event.ObservedAt.IsZero() {
		return
	}

	key := periodicityKey{
		SourceBucket: store.normalizeAddressLocked(event.Request.Source),
		TargetBucket: store.normalizeAddressLocked(event.Request.Target),
		Primary:      event.Request.Primary,
		Secondary:    event.Request.Secondary,
	}
	entry, ok := store.periodicity[key]
	if !ok {
		store.evictStalePeriodicityLocked(event.ObservedAt)
		if len(store.periodicity) >= store.cfg.ObserveFirstPeriodicityCapacity {
			if !store.evictLRUPeriodicityLocked() {
				store.periodicityOverflowTotal++
			}
		}
		entry = &BusPeriodicityEntry{
			SourceBucket: key.SourceBucket,
			TargetBucket: key.TargetBucket,
			Primary:      key.Primary,
			Secondary:    key.Secondary,
			Family:       family,
		}
		store.periodicity[key] = entry
	}

	if !entry.lastObservedAt.IsZero() && event.ObservedAt.After(entry.lastObservedAt) {
		interval := event.ObservedAt.Sub(entry.lastObservedAt)
		entry.SampleCount++
		entry.LastInterval = interval
		entry.totalInterval += interval
		if entry.MinInterval == 0 || interval < entry.MinInterval {
			entry.MinInterval = interval
		}
		if interval > entry.MaxInterval {
			entry.MaxInterval = interval
		}
		entry.MeanInterval = time.Duration(int64(entry.totalInterval) / int64(entry.SampleCount))
	}
	entry.Family = family
	entry.LastSeen = event.ObservedAt
	entry.lastObservedAt = event.ObservedAt
}

func (store *BusObservabilityStore) exportedPeriodicityEntryLocked(entry *BusPeriodicityEntry) BusPeriodicityEntry {
	item := *entry
	if item.SampleCount < observeFirstPeriodicitySamples || store.passive.state != "available" {
		item.State = "warming_up"
	} else {
		item.State = "available"
	}
	return item
}

func (store *BusObservabilityStore) incrementFrameLocked(key frameSeriesKey) {
	if _, ok := store.frames[key]; ok {
		store.frames[key]++
		return
	}
	totalSeries := len(store.frames) + len(store.errors) + len(store.frameBytes)
	if len(store.frames) >= observeFirstFramesHardCap || totalSeries >= store.cfg.ObserveFirstSeriesBudget {
		store.seriesBudgetOverflowTotal++
	}
	store.frames[key] = 1
}

func (store *BusObservabilityStore) incrementErrorLocked(key errorSeriesKey) {
	if _, ok := store.errors[key]; ok {
		store.errors[key]++
		return
	}
	totalSeries := len(store.frames) + len(store.errors) + len(store.frameBytes)
	if len(store.errors) >= observeFirstErrorsHardCap || totalSeries >= store.cfg.ObserveFirstSeriesBudget {
		store.seriesBudgetOverflowTotal++
	}
	store.errors[key]++
}

func (store *BusObservabilityStore) pushRecentLocked(record BusMessageRecord) {
	if len(store.recent) == 0 {
		return
	}
	if store.recentLen == len(store.recent) {
		store.recent[store.recentStart] = record
		store.recentStart = (store.recentStart + 1) % len(store.recent)
		return
	}
	idx := (store.recentStart + store.recentLen) % len(store.recent)
	store.recent[idx] = record
	store.recentLen++
}

func (store *BusObservabilityStore) normalizeAddressLocked(address byte) string {
	if address == protocol.AddressBroadcast {
		return "0xfe"
	}
	if value, ok := store.addressBuckets[address]; ok {
		return value
	}
	// No address cap — eBUS address space is protocol-bounded (max 25 initiator-capable,
	// max 256 total). Real buses have <10 participants. Every address gets its own bucket.
	value := fmt.Sprintf("0x%02x", address)
	store.addressBuckets[address] = value
	store.addressOrder = append(store.addressOrder, address)
	return value
}

func (store *BusObservabilityStore) localAddressSnapshotLocked() LocalAddressSnapshot {
	if store.cfg.LocalAddressSnapshotter == nil {
		return LocalAddressSnapshot{}
	}
	return store.cfg.LocalAddressSnapshotter.LocalAddressSnapshot()
}

func (store *BusObservabilityStore) evictStalePeriodicityLocked(now time.Time) {
	cutoff := now.Add(-store.cfg.ObserveFirstPeriodicityStaleTTL)
	mutated := false
	for key, entry := range store.periodicity {
		if entry.LastSeen.Before(cutoff) {
			delete(store.periodicity, key)
			mutated = true
		}
	}
	if mutated {
		store.touchLocked(now)
	}
}

func (store *BusObservabilityStore) evictLRUPeriodicityLocked() bool {
	var (
		oldestKey periodicityKey
		oldestAt  time.Time
		found     bool
	)
	for key, entry := range store.periodicity {
		if !found || entry.LastSeen.Before(oldestAt) {
			oldestKey = key
			oldestAt = entry.LastSeen
			found = true
		}
	}
	if !found {
		return false
	}
	delete(store.periodicity, oldestKey)
	store.periodicityOverflowTotal++
	return true
}

func (store *BusObservabilityStore) reconstructorSnapshotLocked() PassiveReconstructorSnapshot {
	if store.reconstructor == nil {
		return PassiveReconstructorSnapshot{}
	}
	return store.reconstructor.Snapshot()
}

func (store *BusObservabilityStore) refreshPassiveStateLocked(now time.Time, tapStatus PassiveTapStatus) {
	if store.passive.state == "" {
		store.passive.state = "unavailable"
		store.passive.processStartedAt = now
		store.touchLocked(now)
	}
	if !store.cfg.BroadcastListen {
		stateChanged := store.passive.state != "unavailable"
		store.setPassiveStateLocked(now, "unavailable")
		metadataChanged := false
		if store.passive.unavailableReason != "capability_withdrawn" {
			store.passive.unavailableReason = "capability_withdrawn"
			metadataChanged = true
		}
		if !store.passive.startupWindowClosed {
			store.passive.startupWindowClosed = true
			metadataChanged = true
		}
		if metadataChanged && !stateChanged {
			store.touchLocked(now)
		}
		return
	}
	if reason := passiveTransportUnavailableReason(store.cfg); reason != "" {
		stateChanged := store.passive.state != "unavailable"
		store.setPassiveStateLocked(now, "unavailable")
		metadataChanged := false
		if store.passive.unavailableReason != reason {
			store.passive.unavailableReason = reason
			metadataChanged = true
		}
		if !store.passive.startupWindowClosed {
			store.passive.startupWindowClosed = true
			metadataChanged = true
		}
		if metadataChanged && !stateChanged {
			store.touchLocked(now)
		}
		return
	}
	if !store.passive.startupWindowClosed && !now.Before(store.passive.processStartedAt.Add(store.cfg.ObserveFirstWarmupOuterWindow)) {
		if store.passive.state == "warming_up" && store.passive.fallbackHealthy {
			store.setPassiveStateLocked(now, "available")
			store.passive.lastCompletionMode = "fallback_path"
			store.passive.probeOutcomes["confirmed"]++
		} else if store.passive.state != "available" {
			store.setPassiveStateLocked(now, "unavailable")
			store.passive.unavailableReason = "startup_timeout"
			store.passive.probeOutcomes["timed_out"]++
			store.touchLocked(now)
		}
		store.passive.startupWindowClosed = true
	}
	if store.passive.state == "warming_up" && !store.passive.sessionDeadline.IsZero() && !now.Before(store.passive.sessionDeadline) {
		if store.passive.fallbackHealthy {
			store.setPassiveStateLocked(now, "available")
			store.passive.lastCompletionMode = "fallback_path"
			store.passive.probeOutcomes["confirmed"]++
		} else {
			store.setPassiveStateLocked(now, "unavailable")
			if store.passive.unavailableReason == "" {
				store.passive.unavailableReason = "reconnect_timeout"
			}
			store.passive.probeOutcomes["timed_out"]++
			store.touchLocked(now)
		}
	}
	if tapStatus.EndpointState == PassiveEndpointStateUnsupportedOrMisconfigured {
		stateChanged := store.passive.state != "unavailable"
		store.setPassiveStateLocked(now, "unavailable")
		if store.passive.unavailableReason != "unsupported_or_misconfigured" {
			store.passive.unavailableReason = "unsupported_or_misconfigured"
			if !stateChanged {
				store.touchLocked(now)
			}
		}
	}
	store.promotePassiveIfReadyLocked(now, tapStatus)
}

func (store *BusObservabilityStore) promotePassiveIfReadyLocked(now time.Time, tapStatus PassiveTapStatus) {
	if store.passive.state != "warming_up" {
		return
	}
	// requiredTransactions == 0 means warmup is disabled (adapter-direct
	// mode) — promote immediately without waiting for transactions.
	if store.passive.requiredTransactions == 0 {
		store.setPassiveStateLocked(now, "available")
		store.passive.lastCompletionMode = "warmup_disabled"
		store.passive.unavailableReason = ""
		store.passive.probeOutcomes["confirmed"]++
		store.passive.startupWindowClosed = true
		return
	}
	if store.passive.connectedWindow > 0 && now.Before(store.passive.sessionStartedAt.Add(store.passive.connectedWindow)) && store.passive.settlingDeadline.IsZero() {
		return
	}
	if !store.passive.settlingDeadline.IsZero() && now.Before(store.passive.settlingDeadline) {
		return
	}
	if store.passive.completedTransactions < store.passive.requiredTransactions {
		return
	}
	if tapStatus.ConnectCount == 0 && !store.passive.fallbackHealthy {
		return
	}
	store.setPassiveStateLocked(now, "available")
	store.passive.lastCompletionMode = "thresholds_met"
	store.passive.unavailableReason = ""
	store.passive.probeOutcomes["confirmed"]++
	store.passive.startupWindowClosed = true
}

func (store *BusObservabilityStore) passiveStartWarmupLocked(observedAt time.Time, postReset bool) {
	store.passive.probeAttemptsTotal++
	store.passive.sessionStartedAt = observedAt
	store.passive.completedTransactions = 0
	store.passive.terminalEvents = 0
	store.passive.fallbackHealthy = false
	store.passive.unavailableReason = ""
	if snapshot := store.reconstructorSnapshotLocked(); snapshot.TapStatus.ObservedSymbolCount > 0 {
		store.passive.symbolBaseline = snapshot.TapStatus.ObservedSymbolCount
	}
	if postReset {
		store.passive.connectedWindow = store.cfg.ObserveFirstWarmupPostResetWindow
		store.passive.requiredTransactions = store.cfg.ObserveFirstWarmupPostResetTransactions
		store.passive.settlingDeadline = observedAt.Add(store.cfg.ObserveFirstWarmupPostResetWindow)
	} else {
		store.passive.connectedWindow = store.cfg.ObserveFirstWarmupConnectedWindow
		store.passive.requiredTransactions = store.cfg.ObserveFirstWarmupCompletedTransactions
		store.passive.settlingDeadline = time.Time{}
	}
	if !store.passive.startupWindowClosed {
		store.passive.sessionDeadline = store.passive.processStartedAt.Add(store.cfg.ObserveFirstWarmupOuterWindow)
	} else {
		store.passive.sessionDeadline = observedAt.Add(store.cfg.ObserveFirstWarmupOuterWindow)
	}
	store.setPassiveStateLocked(observedAt, "warming_up")
	store.touchLocked(observedAt)
}

func (store *BusObservabilityStore) passiveNoteTimeoutLocked(at time.Time, reason string) {
	store.setPassiveStateLocked(at, "unavailable")
	store.passive.unavailableReason = reason
	store.passive.completedTransactions = 0
	store.passive.requiredTransactions = 0
	store.passive.settlingDeadline = time.Time{}
	store.touchLocked(at)
}

func (store *BusObservabilityStore) setPassiveStateLocked(at time.Time, next string) {
	if store.passive.state == next {
		return
	}
	key := store.passive.state + "->" + next
	store.passive.transitions[key]++
	store.passive.state = next
	if next != "warming_up" {
		store.passive.connectedWindow = 0
		store.passive.requiredTransactions = 0
		store.passive.completedTransactions = 0
		store.passive.terminalEvents = 0
		store.passive.settlingDeadline = time.Time{}
	}
	store.touchLocked(at)
}

func (store *BusObservabilityStore) touchLocked(at time.Time) {
	if store == nil || at.IsZero() {
		return
	}
	at = at.UTC()
	if store.lastUpdatedAt.IsZero() || at.After(store.lastUpdatedAt) {
		store.lastUpdatedAt = at
	}
}

func classifyFamily(frame protocol.Frame) string {
	switch {
	case frame.Primary == 0xB5 && frame.Secondary == 0x09:
		return "B509"
	case frame.Primary == 0xB5 && frame.Secondary == 0x16:
		return "B516"
	case frame.Primary == 0xB5 && frame.Secondary == 0x24:
		return "B524"
	case frame.Primary == 0xB5 && frame.Secondary == 0x55:
		return "B555"
	default:
		return fmt.Sprintf("%02X%02X", frame.Primary, frame.Secondary)
	}
}

func isImplementedFamily(family string) bool {
	switch family {
	case "B509", "B516", "B524", "B555":
		return true
	default:
		return false
	}
}

func specimenDedupKey(family string, source, target byte, frameType string, requestData []byte, outcome string, hasResponse bool, responseData []byte) string {
	key := family + fmt.Sprintf("/%02x/%02x/", source, target) + frameType + "/" + hex.EncodeToString(requestData) + "/" + outcome
	if hasResponse {
		key += "/" + hex.EncodeToString(responseData)
	}
	return key
}

func (store *BusObservabilityStore) pushSpecimenLocked(family string, event PassiveClassifiedEvent, frameType, outcome string, now time.Time) {
	bucket, exists := store.specimens[family]
	if !exists {
		if len(store.specimens) >= specimenMaxFamilies {
			return
		}
		bucket = &specimenFamilyBucket{}
		store.specimens[family] = bucket
	}

	key := specimenDedupKey(family, event.Request.Source, event.Request.Target, frameType, event.Request.Data, outcome, event.HasResponse, event.Response.Data)

	// Search for existing entry with same dedup key.
	for i := 0; i < bucket.length; i++ {
		idx := (bucket.start + i) % specimenMaxPerFamily
		if bucket.entries[idx].dedupKey == key {
			bucket.entries[idx].Count++
			bucket.entries[idx].LastSeenAt = now
			return
		}
	}

	// Build new entry.
	entry := specimenEntry{
		Family:      family,
		Source:      event.Request.Source,
		Target:      event.Request.Target,
		FrameType:   frameType,
		RequestData: append([]byte(nil), event.Request.Data...),
		RequestLen:  frameWireLen(event.Request),
		Outcome:     outcome,
		FirstSeenAt: now,
		LastSeenAt:  now,
		Count:       1,
		dedupKey:    key,
	}
	if event.HasResponse {
		entry.ResponseData = append([]byte(nil), event.Response.Data...)
		entry.ResponseLen = responseWireLen(event.Response)
	}

	// Ring buffer push.
	if bucket.length < specimenMaxPerFamily {
		idx := (bucket.start + bucket.length) % specimenMaxPerFamily
		bucket.entries[idx] = entry
		bucket.length++
	} else {
		bucket.entries[bucket.start] = entry
		bucket.start = (bucket.start + 1) % specimenMaxPerFamily
	}
}

func classifyActiveFrameType(frame protocol.Frame, local LocalAddressSnapshot) string {
	if frame.Type() == protocol.FrameTypeInitiatorInitiator && local.Known && frame.Target == local.Address {
		return "local_participant_inbound"
	}
	return classifyFrameType(frame.Type())
}

func classifyPassiveFrameType(event PassiveClassifiedEvent, local LocalAddressSnapshot) string {
	if event.Kind == PassiveClassifiedEventAbandonedTransaction {
		return "abandoned_partial"
	}
	if event.Request.Type() == protocol.FrameTypeInitiatorInitiator && local.Known && event.Request.Target == local.Address {
		return "local_participant_inbound"
	}
	return classifyFrameType(event.Request.Type())
}

func classifyFrameType(frameType protocol.FrameType) string {
	switch frameType {
	case protocol.FrameTypeBroadcast:
		return "broadcast"
	case protocol.FrameTypeInitiatorInitiator:
		return "initiator_initiator"
	case protocol.FrameTypeInitiatorTarget:
		return "initiator_target"
	default:
		return "abandoned_partial"
	}
}

func classifyActiveError(event protocol.BusEvent) (string, string) {
	switch event.Kind {
	case protocol.BusEventTimeout:
		return "timeout", "terminal"
	case protocol.BusEventNACK:
		return "nack", "ack"
	case protocol.BusEventCRCMismatch:
		return "crc_mismatch", "response"
	case protocol.BusEventEchoMismatch:
		return "echo_mismatch", "request"
	default:
		return "", ""
	}
}

func classifyPassiveAbandon(reason PassiveAbandonReason) (string, string) {
	switch reason {
	case PassiveAbandonReasonNACK:
		return "nack", "ack"
	case PassiveAbandonReasonCRCMismatch:
		return "crc_mismatch", "response"
	case PassiveAbandonReasonTransportReset:
		return "transport_reset", "terminal"
	case PassiveAbandonReasonDecodeFault:
		return "decode_fault", "request"
	case PassiveAbandonReasonNoResponse, PassiveAbandonReasonNoProgress, PassiveAbandonReasonDisconnected, PassiveAbandonReasonShutdown:
		return "timeout", "terminal"
	case PassiveAbandonReasonCorruptedRequest, PassiveAbandonReasonCorruptedTarget,
		PassiveAbandonReasonUnexpectedSYN, PassiveAbandonReasonUnexpectedSymbol,
		PassiveAbandonReasonScanTimeout, PassiveAbandonReasonScanCollision,
		PassiveAbandonReasonArbitrationFragment, PassiveAbandonReasonSelfEcho,
		PassiveAbandonReasonAmbiguousRetransmit:
		// Bus contention artifacts: CRC failures, arbitration noise, and
		// reconstructor desync are expected on a shared-bus passive tap.
		// The CRC check correctly identifies invalid frames — that is not
		// a software error, it is the bus operating as designed.
		return "", ""
	default:
		return "abandoned", "terminal"
	}
}

func passiveDiscontinuityClass(reason PassiveDiscontinuityReason) string {
	switch reason {
	case PassiveDiscontinuityTransportReset:
		return "transport_reset"
	case PassiveDiscontinuityDecodeFault:
		return "decode_fault"
	default:
		return ""
	}
}

func timingQualityForActive(cfg Config) string {
	if canonicalTransportProtocol(cfg.TransportConfig.Protocol) == TransportEbusdTCP {
		return "unavailable"
	}
	return "estimated"
}

func timingQualityForPassive(cfg Config) string {
	if !PassiveTransportSupported(cfg) || !cfg.BroadcastListen {
		return "unavailable"
	}
	return "estimated"
}

func frameWireLen(frame protocol.Frame) int {
	return 6 + len(frame.Data)
}

func responseWireLen(frame protocol.Frame) int {
	return 2 + len(frame.Data)
}

func responseLen(event protocol.BusEvent) int {
	if !event.HasResponse {
		return 0
	}
	return responseWireLen(event.Response)
}

func passiveResponseLen(event PassiveClassifiedEvent) int {
	if !event.HasResponse {
		return 0
	}
	return responseWireLen(event.Response)
}

func float64Frame(value int) uint64 {
	if value <= 0 {
		return 0
	}
	return uint64(value)
}

func passiveElapsedSeconds(state passiveWarmupRuntime, now time.Time) float64 {
	if state.sessionStartedAt.IsZero() || state.state != "warming_up" {
		return 0
	}
	return now.Sub(state.sessionStartedAt).Seconds()
}

func passiveBlockerReason(state passiveWarmupRuntime, now time.Time) string {
	if state.state != "warming_up" {
		return ""
	}
	if !state.settlingDeadline.IsZero() && now.Before(state.settlingDeadline) {
		return "post_reset_settling"
	}
	if state.completedTransactions < state.requiredTransactions {
		return "completed_transactions"
	}
	if !state.fallbackHealthy {
		return "healthy_symbol_ingress"
	}
	if state.connectedWindow > 0 && now.Before(state.sessionStartedAt.Add(state.connectedWindow)) {
		return "connected_observation_window"
	}
	return "startup_outer_window"
}

func windowBusyDuration(segments []busySegment, now time.Time, window time.Duration) time.Duration {
	if window <= 0 {
		return 0
	}
	cutoff := now.Add(-window)
	var total time.Duration
	for _, segment := range segments {
		start := segment.Start
		if start.Before(cutoff) {
			start = cutoff
		}
		end := segment.End
		if end.After(now) {
			end = now
		}
		if end.After(start) {
			total += end.Sub(start)
		}
	}
	return total
}

func parseBusyWindow(value string) time.Duration {
	switch value {
	case "1m":
		return time.Minute
	case "5m":
		return 5 * time.Minute
	case "15m":
		return 15 * time.Minute
	case "1h":
		return time.Hour
	default:
		return 0
	}
}

func clippedRatio(total time.Duration, window time.Duration) float64 {
	if window <= 0 {
		return 0
	}
	ratio := total.Seconds() / window.Seconds()
	if ratio < 0 {
		return 0
	}
	if ratio > 1 {
		return 1
	}
	return ratio
}

func (store *BusObservabilityStore) watchEfficiencySnapshotLocked(now time.Time) watchEfficiencySnapshot {
	snapshot := watchEfficiencySnapshot{
		buckets:                             make(map[watchEfficiencyBucketKey]watchEfficiencyBucketRuntime, len(store.watchEfficiency.buckets)),
		ambiguous:                           make(map[watchEfficiencyAmbiguousKey]uint64, len(store.watchEfficiency.ambiguous)),
		missed:                              make(map[watchEfficiencyMissedKey]uint64, len(store.watchEfficiency.missed)),
		directApplyCandidatesEvaluatedTotal: store.watchEfficiency.directApplyCandidatesEvaluatedTotal,
	}
	for key, series := range store.watchEfficiency.buckets {
		if series == nil {
			continue
		}
		item := *series
		if _, ok := item.saved.estimateSeconds(now); !ok {
			item.saved = watchEfficiencySavedWindow{}
		}
		snapshot.buckets[key] = item
	}
	for key, value := range store.watchEfficiency.ambiguous {
		snapshot.ambiguous[key] = value
	}
	for key, value := range store.watchEfficiency.missed {
		snapshot.missed[key] = value
	}
	return snapshot
}

func resolveWatchEfficiencyBucket(key WatchKey, descriptor WatchDescriptor) (watchEfficiencyBucketKey, string, bool) {
	if descriptor.Key == nil {
		return watchEfficiencyBucketKey{}, "missing_runtime_descriptor", false
	}
	family := string(descriptor.Family())
	if family == "" {
		return watchEfficiencyBucketKey{}, "unsupported_family", false
	}
	profile := string(descriptor.FreshnessProfile)
	if profile == "" {
		return watchEfficiencyBucketKey{}, "unsupported_freshness_profile", false
	}
	if descriptor.FreshnessProfile == WatchFreshnessProfileDebug {
		return watchEfficiencyBucketKey{}, "", false
	}
	if !watchEfficiencyProfileAllowed(descriptor.FreshnessProfile) {
		return watchEfficiencyBucketKey{}, "unsupported_freshness_profile", false
	}
	if !watchEfficiencyFamilyAllowed(descriptor.Family()) {
		return watchEfficiencyBucketKey{}, "unsupported_family", false
	}
	if descriptor.Family() == WatchFamilyB516 && descriptor.DirectApplyPolicy == WatchDirectApplyPolicyEnergyMergeOnly {
		return watchEfficiencyBucketKey{}, "", false
	}
	_ = key
	return watchEfficiencyBucketKey{
		Family:           family,
		FreshnessProfile: profile,
	}, "", true
}

func familyForAmbiguous(key WatchKey, descriptor WatchDescriptor) string {
	if descriptor.Key != nil {
		if family := string(descriptor.Family()); family != "" {
			return family
		}
	}
	if key != nil {
		if family := string(key.Family()); family != "" {
			return family
		}
	}
	return "unknown"
}

func watchEfficiencyFamilyAllowed(family WatchFamily) bool {
	switch family {
	case WatchFamilyB509, WatchFamilyB516, WatchFamilyB524, WatchFamilyB555:
		return true
	default:
		return false
	}
}

func watchEfficiencyProfileAllowed(profile WatchFreshnessProfile) bool {
	switch profile {
	case WatchFreshnessProfileStateFast, WatchFreshnessProfileStateSlow, WatchFreshnessProfileConfig, WatchFreshnessProfileDiscovery:
		return true
	default:
		return false
	}
}

func watchEfficiencyEligibleForMiss(descriptor WatchDescriptor, flags ObserveFirstFeatureFlags, maxAge time.Duration) bool {
	if maxAge <= 0 || !flags.ObserveFirstEnabled() {
		return false
	}
	switch descriptor.DirectApplyPolicy {
	case WatchDirectApplyPolicyStateDefault:
		return flags.PassiveStateDirectApply()
	case WatchDirectApplyPolicyConfigOptIn:
		return flags.PassiveConfigDirectApply()
	default:
		return false
	}
}

func watchEfficiencyTransportLimitation(cfg Config) string {
	if !PassiveTransportSupported(cfg) {
		return "transport_unavailable"
	}
	if !cfg.BroadcastListen {
		return "broadcast_unavailable"
	}
	return ""
}

func (window *watchEfficiencySavedWindow) add(sample time.Duration, observedAt time.Time) {
	if !window.lastSampleAt.IsZero() && observedAt.Sub(window.lastSampleAt) > watchEfficiencySavedStaleTTL {
		*window = watchEfficiencySavedWindow{}
	}
	if sample < 0 {
		sample = 0
	}
	window.samples[window.next] = sample
	if window.count < len(window.samples) {
		window.count++
	}
	window.next = (window.next + 1) % len(window.samples)
	window.lastSampleAt = observedAt
}

func (window watchEfficiencySavedWindow) estimateSeconds(now time.Time) (float64, bool) {
	if window.count < watchEfficiencySavedMinSamples {
		return 0, false
	}
	if window.lastSampleAt.IsZero() || now.Sub(window.lastSampleAt) > watchEfficiencySavedStaleTTL {
		return 0, false
	}
	var total time.Duration
	for index := 0; index < window.count; index++ {
		total += window.samples[index]
	}
	return total.Seconds() / float64(window.count), true
}

type frameSeriesItem struct {
	Key   frameSeriesKey
	Value uint64
}

type errorSeriesItem struct {
	Key   errorSeriesKey
	Value uint64
}

type frameBytesSeriesItem struct {
	Key   frameBytesSeriesKey
	Value uint64
}

type stringUint64Item struct {
	Key   string
	Value uint64
}

type watchEfficiencyBucketItem struct {
	Key   watchEfficiencyBucketKey
	Value watchEfficiencyBucketRuntime
}

type watchEfficiencyAmbiguousItem struct {
	Key   watchEfficiencyAmbiguousKey
	Value uint64
}

type watchEfficiencyMissedItem struct {
	Key   watchEfficiencyMissedKey
	Value uint64
}

func cloneFramesMap(input map[frameSeriesKey]uint64) map[frameSeriesKey]uint64 {
	output := make(map[frameSeriesKey]uint64, len(input))
	for key, value := range input {
		output[key] = value
	}
	return output
}

func cloneErrorsMap(input map[errorSeriesKey]uint64) map[errorSeriesKey]uint64 {
	output := make(map[errorSeriesKey]uint64, len(input))
	for key, value := range input {
		output[key] = value
	}
	return output
}

func cloneFrameBytesMap(input map[frameBytesSeriesKey]uint64) map[frameBytesSeriesKey]uint64 {
	output := make(map[frameBytesSeriesKey]uint64, len(input))
	for key, value := range input {
		output[key] = value
	}
	return output
}

func sortedFrameSeries(input map[frameSeriesKey]uint64) []frameSeriesItem {
	items := make([]frameSeriesItem, 0, len(input))
	for key, value := range input {
		items = append(items, frameSeriesItem{Key: key, Value: value})
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].Key.Scope != items[j].Key.Scope {
			return items[i].Key.Scope < items[j].Key.Scope
		}
		if items[i].Key.Source != items[j].Key.Source {
			return items[i].Key.Source < items[j].Key.Source
		}
		if items[i].Key.Target != items[j].Key.Target {
			return items[i].Key.Target < items[j].Key.Target
		}
		if items[i].Key.Family != items[j].Key.Family {
			return items[i].Key.Family < items[j].Key.Family
		}
		return items[i].Key.FrameType < items[j].Key.FrameType
	})
	return items
}

func sortedErrorSeries(input map[errorSeriesKey]uint64) []errorSeriesItem {
	items := make([]errorSeriesItem, 0, len(input))
	for key, value := range input {
		items = append(items, errorSeriesItem{Key: key, Value: value})
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].Key.Scope != items[j].Key.Scope {
			return items[i].Key.Scope < items[j].Key.Scope
		}
		if items[i].Key.Class != items[j].Key.Class {
			return items[i].Key.Class < items[j].Key.Class
		}
		return items[i].Key.Phase < items[j].Key.Phase
	})
	return items
}

func sortedFrameBytesSeries(input map[frameBytesSeriesKey]uint64) []frameBytesSeriesItem {
	items := make([]frameBytesSeriesItem, 0, len(input))
	for key, value := range input {
		items = append(items, frameBytesSeriesItem{Key: key, Value: value})
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].Key.Scope != items[j].Key.Scope {
			return items[i].Key.Scope < items[j].Key.Scope
		}
		if items[i].Key.Family != items[j].Key.Family {
			return items[i].Key.Family < items[j].Key.Family
		}
		if items[i].Key.FrameType != items[j].Key.FrameType {
			return items[i].Key.FrameType < items[j].Key.FrameType
		}
		return items[i].Key.Part < items[j].Key.Part
	})
	return items
}

func sortedTransitionMap(input map[string]uint64) []stringUint64Item {
	items := make([]stringUint64Item, 0, len(input))
	for key, value := range input {
		items = append(items, stringUint64Item{Key: key, Value: value})
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Key < items[j].Key })
	return items
}

func sortedWatchEfficiencyBuckets(input map[watchEfficiencyBucketKey]watchEfficiencyBucketRuntime) []watchEfficiencyBucketItem {
	items := make([]watchEfficiencyBucketItem, 0, len(input))
	for key, value := range input {
		items = append(items, watchEfficiencyBucketItem{Key: key, Value: value})
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].Key.Family != items[j].Key.Family {
			return items[i].Key.Family < items[j].Key.Family
		}
		return items[i].Key.FreshnessProfile < items[j].Key.FreshnessProfile
	})
	return items
}

func sortedWatchEfficiencyAmbiguous(input map[watchEfficiencyAmbiguousKey]uint64) []watchEfficiencyAmbiguousItem {
	items := make([]watchEfficiencyAmbiguousItem, 0, len(input))
	for key, value := range input {
		items = append(items, watchEfficiencyAmbiguousItem{Key: key, Value: value})
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].Key.Family != items[j].Key.Family {
			return items[i].Key.Family < items[j].Key.Family
		}
		return items[i].Key.Reason < items[j].Key.Reason
	})
	return items
}

func sortedWatchEfficiencyMissed(input map[watchEfficiencyMissedKey]uint64) []watchEfficiencyMissedItem {
	items := make([]watchEfficiencyMissedItem, 0, len(input))
	for key, value := range input {
		items = append(items, watchEfficiencyMissedItem{Key: key, Value: value})
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].Key.Bucket.Family != items[j].Key.Bucket.Family {
			return items[i].Key.Bucket.Family < items[j].Key.Bucket.Family
		}
		if items[i].Key.Bucket.FreshnessProfile != items[j].Key.Bucket.FreshnessProfile {
			return items[i].Key.Bucket.FreshnessProfile < items[j].Key.Bucket.FreshnessProfile
		}
		return items[i].Key.Limitation < items[j].Key.Limitation
	})
	return items
}

func cloneStringUint64Map(input map[string]uint64) map[string]uint64 {
	if len(input) == 0 {
		return nil
	}
	output := make(map[string]uint64, len(input))
	for key, value := range input {
		output[key] = value
	}
	return output
}

func readExpvarInt(value *expvar.Int) int64 {
	if value == nil {
		return 0
	}
	raw, err := strconv.ParseInt(value.String(), 10, 64)
	if err != nil {
		return 0
	}
	return raw
}

func readExpvarMapInt(value *expvar.Map, key string) int64 {
	if value == nil {
		return 0
	}
	var result int64
	value.Do(func(kv expvar.KeyValue) {
		if kv.Key != key {
			return
		}
		raw, err := strconv.ParseInt(kv.Value.String(), 10, 64)
		if err == nil {
			result = raw
		}
	})
	return result
}

func readExpvarNamedMapInt(name, key string) int64 {
	if strings.TrimSpace(name) == "" {
		return 0
	}
	value := expvar.Get(name)
	if value == nil {
		return 0
	}
	mapped, ok := value.(*expvar.Map)
	if !ok {
		return 0
	}
	return readExpvarMapInt(mapped, key)
}

func readDedupDegradedState() float64 {
	value := strings.Trim(observeFirstDedupState.String(), "\"")
	if value == "degraded" {
		return 1
	}
	return 0
}

type prometheusWriter struct {
	buffer *bytes.Buffer
	wrote  map[string]bool
}

func newPrometheusWriter(buffer *bytes.Buffer) *prometheusWriter {
	return &prometheusWriter{
		buffer: buffer,
		wrote:  make(map[string]bool),
	}
}

func (writer *prometheusWriter) writeHelp(name string, value string) {
	if writer.wrote[name+"#HELP"] {
		return
	}
	fmt.Fprintf(writer.buffer, "# HELP %s %s\n", name, value)
	writer.wrote[name+"#HELP"] = true
}

func (writer *prometheusWriter) writeType(name string, metricType string) {
	if writer.wrote[name+"#TYPE"] {
		return
	}
	fmt.Fprintf(writer.buffer, "# TYPE %s %s\n", name, metricType)
	writer.wrote[name+"#TYPE"] = true
}

func (writer *prometheusWriter) writeCounterSample(name string, value float64, labels map[string]string) {
	writer.writeSample(name, value, labels)
}

func (writer *prometheusWriter) writeGaugeSample(name string, value float64, labels map[string]string) {
	writer.writeSample(name, value, labels)
}

func (writer *prometheusWriter) writeSample(name string, value float64, labels map[string]string) {
	if len(labels) == 0 {
		fmt.Fprintf(writer.buffer, "%s %s\n", name, formatMetricValue(value))
		return
	}
	keys := make([]string, 0, len(labels))
	for key := range labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	writer.buffer.WriteString(name)
	writer.buffer.WriteByte('{')
	for index, key := range keys {
		if index > 0 {
			writer.buffer.WriteByte(',')
		}
		fmt.Fprintf(writer.buffer, `%s="%s"`, key, escapePrometheusLabel(labels[key]))
	}
	writer.buffer.WriteByte('}')
	writer.buffer.WriteByte(' ')
	writer.buffer.WriteString(formatMetricValue(value))
	writer.buffer.WriteByte('\n')
}

func labelMap(values ...string) map[string]string {
	if len(values) == 0 {
		return nil
	}
	labels := make(map[string]string, len(values)/2)
	for index := 0; index+1 < len(values); index += 2 {
		labels[values[index]] = values[index+1]
	}
	return labels
}

func escapePrometheusLabel(value string) string {
	replacer := strings.NewReplacer(`\`, `\\`, "\n", `\n`, `"`, `\"`)
	return replacer.Replace(value)
}

func formatMetricValue(value float64) string {
	return strconv.FormatFloat(value, 'f', -1, 64)
}
