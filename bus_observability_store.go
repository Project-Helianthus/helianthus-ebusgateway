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
	// echoMismatchSubclasses is a P10-added parallel counter
	// breaking down `class=echo_mismatch` events by subclass label.
	// Stored separately from `errors` so that the existing
	// `ebus_errors_total` time series shapes are unchanged
	// (preserves alert backward-compat). Surfaced as the new
	// `ebus_active_echo_mismatch_subclass_total` Prometheus metric.
	echoMismatchSubclasses map[echoMismatchSubclassKey]uint64

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

	// adaptermuxDiagProvider returns the latest adaptermux diagnostic
	// snapshot. Set via SetAdaptermuxDiagProvider; nil when no
	// adaptermux is wired (e.g. in tests). When non-nil, RenderPrometheus
	// emits the batch-21 forensic counters
	// (ebus_adaptermux_syn_seen_*) alongside the rest of the surface.
	adaptermuxDiagProvider func() AdaptermuxDiagSnapshot

	energyFreshnessMetricsRefresher func(now time.Time, passiveState string)
	busAdmission                    *BusAdmission
	admissionStabilityWindow        *AdmissionStabilityWindow

	// evidence buffers passive observations for the runtime discovery
	// promotion pipeline. Late-arriving devices (e.g. a regulator that
	// boots after the gateway) accumulate evidence as their request /
	// response traffic is observed; once an address crosses the
	// promotion threshold the discovery promoter probes it actively.
	//
	// The buffer is read by an external promoter goroutine via
	// EvidenceBuffer(); writes happen on the passive-event path under
	// the buffer's own mutex.
	evidence *EvidenceBuffer
	// evidenceSourceProvider returns the gateway's admitted source
	// address. The store filters self-source traffic out of evidence
	// recording so the gateway never promotes its own admitted source.
	// Nil-safe: when unset the store records all non-self-evident
	// traffic (which is the historical / test default).
	evidenceSourceProvider func() byte
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
		if !flipped && (emittedState == "" || emittedState != state) {
			return false
		}
	}
	store.busAdmission = &BusAdmission{
		State:           emittedState,
		Source:          source,
		CompanionTarget: companionTarget,
		Reason:          reason,
		SourceSelection: busAdmissionSourceSelectionFromTransition(emittedState, source, companionTarget, reason),
	}
	return true
}

func busAdmissionSourceSelectionFromTransition(state string, source, companionTarget uint8, reason string) *BusAdmissionSourceSelection {
	selection := &BusAdmissionSourceSelection{
		State:   state,
		Outcome: busAdmissionSourceSelectionOutcome(state, reason),
		Reason:  reason,
	}
	if source != 0 {
		if state == "degraded" {
			selection.FailedSource = uint8Ptr(source)
		} else {
			selection.SelectedSource = uint8Ptr(source)
		}
		if state == "active" {
			selection.LastSuccessfulSource = uint8Ptr(source)
		}
	}
	if companionTarget != 0 {
		selection.CompanionTarget = uint8Ptr(companionTarget)
	}
	if status := busAdmissionActiveProbeStatus(state, reason); status != "" {
		selection.ActiveProbe = &BusAdmissionActiveProbe{
			Target: selection.CompanionTarget,
			Status: status,
		}
	}
	return selection
}

func busAdmissionSourceSelectionOutcome(state, reason string) string {
	switch reason {
	case "active_probe_pending":
		return "not_started"
	case "active_probe_passed":
		return "active_probe_passed"
	case "source_selection_warmup_in_progress":
		return "not_started"
	}
	if state == "active" {
		return "active_probe_passed"
	}
	if state == "degraded" {
		return "operator_action_required"
	}
	return "not_started"
}

func busAdmissionActiveProbeStatus(state, reason string) string {
	switch reason {
	case "active_probe_pending", "active_probe_passed", "active_probe_failed":
		return reason
	}
	if state == "active" {
		return "active_probe_passed"
	}
	return ""
}

func uint8Ptr(value uint8) *uint8 {
	return &value
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

// echoMismatchSubclassKey is the storage key for the parallel
// echo_mismatch subclass counter (P10). Tracked SEPARATELY from
// errorSeriesKey to preserve backward-compat for existing alerts
// that filter on `ebus_errors_total{class="echo_mismatch"}` without
// grouping by subclass. Splitting the existing series via a new
// label would silently break total-count alerts because Prometheus
// `rate(...)>N` evaluates per-time-series, not aggregate (Codex P10
// review pass 1 MAJOR FINDING_1).
type echoMismatchSubclassKey struct {
	Subclass string
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
		cfg:                    cfg,
		now:                    time.Now,
		transportClass:         string(canonicalTransportProtocol(cfg.TransportConfig.Protocol)),
		activeTimingQuality:    timingQualityForActive(cfg),
		passiveTimingQuality:   timingQualityForPassive(cfg),
		lastUpdatedAt:          now,
		featureFlagsUpdatedAt:  now,
		frames:                 make(map[frameSeriesKey]uint64),
		errors:                 make(map[errorSeriesKey]uint64),
		frameBytes:             make(map[frameBytesSeriesKey]uint64),
		echoMismatchSubclasses: make(map[echoMismatchSubclassKey]uint64),
		recent:                 make([]BusMessageRecord, cfg.ObserveFirstRecentMessageCapacity),
		addressBuckets:         make(map[byte]string),
		specimens:              make(map[string]*specimenFamilyBucket),
		periodicity:            make(map[periodicityKey]*BusPeriodicityEntry),
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
	// Evidence buffer feeds the runtime passive-promotion pipeline.
	// 128 entries / Vaillant baseline seed are the long-standing
	// defaults validated by evidence_buffer_test.go. Construction
	// failure is logged but non-fatal — the store still operates as
	// a pure observability sink without the promotion pathway.
	if buf, err := NewEvidenceBuffer(128, VaillantBaselineTopologySeed); err == nil {
		store.evidence = buf
	}
	if reason := passiveTransportUnavailableReason(cfg); reason != "" {
		store.passive.unavailableReason = reason
	}
	return store
}

// EvidenceBuffer returns the runtime passive-evidence buffer used by the
// discovery promoter. Returns nil if the buffer was not constructed
// (e.g. invalid seed at startup); callers must nil-check.
func (store *BusObservabilityStore) EvidenceBuffer() *EvidenceBuffer {
	if store == nil {
		return nil
	}
	return store.evidence
}

// SetAdmittedSourceProvider installs the function the store uses to
// learn which address has been admitted as the gateway's source. This
// must be set after the source-address selection completes; the store
// uses it to filter self-source traffic out of evidence recording so
// the gateway never feeds its own probes back into the promotion
// pipeline. Nil-safe: when unset, all non-self-evident traffic flows
// to the buffer (legacy / test default).
func (store *BusObservabilityStore) SetAdmittedSourceProvider(provider func() byte) {
	if store == nil {
		return
	}
	store.mu.Lock()
	store.evidenceSourceProvider = provider
	store.mu.Unlock()
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
		store.recordEvidenceFromEventLocked(event)
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

// recordEvidenceFromEventLocked feeds passive-frame addresses into the
// runtime evidence buffer for the discovery-promotion pipeline.
//
// Evidence-strength classification:
//   - PassiveClassifiedEventTransaction whose response carries an
//     identity-bearing or B524 capability response → EvidenceStrong:
//     the address has demonstrably participated as a coherent Vaillant
//     responder, sufficient for single-observation promotion.
//   - PassiveClassifiedEventBroadcastFrame source (e.g. 0x07-class
//     sign-of-life) → EvidenceWeak: presence-only signal.
//   - PassiveClassifiedEventMasterFrame source / target,
//     PassiveClassifiedEventTransaction request source / target →
//     EvidenceWeak: traffic-only signal.
//
// Address filters applied:
//   - Skip the gateway's admitted source so the gateway never
//     feeds its own outbound traffic back into promotion.
//   - Skip broadcast (0xFE), SYN (0xAA), and pre-responder range
//     (<0x03) — these are protocol artifacts, not device addresses.
//   - 0xFF is the broadcast destination class (e.g. 0x07 0xFF
//     sign-of-life); record the SOURCE as presence evidence but
//     never the broadcast destination as a candidate.
//
// Caller holds store.mu.
func (store *BusObservabilityStore) recordEvidenceFromEventLocked(event PassiveClassifiedEvent) {
	if store == nil || store.evidence == nil {
		return
	}
	now := event.ObservedAt
	if now.IsZero() {
		now = store.now()
	}
	admittedSource := byte(0)
	if store.evidenceSourceProvider != nil {
		admittedSource = store.evidenceSourceProvider()
	}

	strongResponseEvidence := false
	if event.Kind == PassiveClassifiedEventTransaction && event.HasResponse {
		strongResponseEvidence = passiveResponseIsCoherentVaillantEvidence(event.Request, event.Response)
	}

	// Per-event dedupe: a single transaction has both
	// request.Target and response.Source pointing at the same
	// responder; recording both would double-count and let a single
	// (non-coherent) frame cross the weak-evidence promotion
	// threshold. Track the highest-strength record per address
	// within this event.
	perEvent := make(map[byte]EvidenceStrength, 4)
	upgradeCandidate := func(addr byte, strength EvidenceStrength) {
		if !isPassiveEvidenceCandidate(addr, admittedSource) {
			return
		}
		if existing, ok := perEvent[addr]; !ok || strength > existing {
			perEvent[addr] = strength
		}
	}
	candidateKind := make(map[byte]string, 4)
	upgradeKind := func(addr byte, kind string) {
		if !isPassiveEvidenceCandidate(addr, admittedSource) {
			return
		}
		if _, ok := candidateKind[addr]; !ok {
			candidateKind[addr] = kind
		}
	}

	switch event.Kind {
	case PassiveClassifiedEventBroadcastFrame:
		// 07 FF and other sign-of-life broadcasts are presence-only
		// evidence per operator requirement: "consumed passively as
		// presence evidence, not used as a discovery probe target."
		// PresenceOnly keeps the address fresh in the buffer (so LRU
		// eviction does not lose long-lived presence) but does NOT
		// contribute to the promotion threshold — broadcast traffic
		// alone never promotes.
		upgradeCandidate(event.Request.Source, EvidencePresenceOnly)
		upgradeKind(event.Request.Source, "broadcast_source")
	case PassiveClassifiedEventMasterFrame:
		upgradeCandidate(event.Request.Source, EvidenceWeak)
		upgradeKind(event.Request.Source, "master_source")
		upgradeCandidate(event.Request.Target, EvidenceWeak)
		upgradeKind(event.Request.Target, "master_target")
	case PassiveClassifiedEventTransaction:
		responderStrength := EvidenceWeak
		if strongResponseEvidence {
			responderStrength = EvidenceStrong
		}
		upgradeCandidate(event.Request.Source, EvidenceWeak)
		upgradeKind(event.Request.Source, "tx_request_source")
		upgradeCandidate(event.Request.Target, responderStrength)
		upgradeKind(event.Request.Target, "tx_responder")
		if event.HasResponse {
			// Identical to request.Target in normal transactions —
			// dedupe via perEvent map, but still record the address
			// when the reconstructor surfaces an aliased response
			// source distinct from request.Target.
			upgradeCandidate(event.Response.Source, responderStrength)
			upgradeKind(event.Response.Source, "tx_responder")
		}
	}

	for addr, strength := range perEvent {
		store.evidence.Record(EvidenceRecord{
			Address:  addr,
			Strength: strength,
			Observed: now,
			Kind:     candidateKind[addr],
		})
	}
}

// isPassiveEvidenceCandidate reports whether addr is a valid candidate
// for the runtime promotion pipeline. Filters out:
//
//   - the gateway's own admitted source (self-traffic),
//   - broadcast (0xFE), SYN (0xAA), the broadcast destination class
//     (0xFF),
//   - the initiator pre-arbitration range (<0x03),
//   - initiator-capable addresses per the eBUS address table
//     (e.g. 0x10, 0x31, 0x71, 0xF0). These are SOURCES; they do not
//     respond to active probes as targets. The same filter is
//     applied at the active-probe entry points
//     (sanitizeStartupProbeTargets / parseStartupProbeTargets), so
//     applying it here keeps the passive-evidence path consistent
//     with the rest of the gateway.
func isPassiveEvidenceCandidate(addr, admittedSource byte) bool {
	if addr == 0x00 || addr == 0xAA || addr == 0xFE || addr == 0xFF {
		return false
	}
	if addr < 0x03 {
		return false
	}
	if admittedSource != 0 && addr == admittedSource {
		return false
	}
	if protocol.IsInitiatorCapableAddress(addr) {
		return false
	}
	return true
}

// passiveResponseIsCoherentVaillantEvidence reports whether a request /
// response pair carries Vaillant-coherent evidence strong enough to
// promote on a single observation.
//
// Acceptance criteria — both must hold:
//
//   - request is a B524 extended-register read (Primary=0xB5 Secondary=0x24)
//     AND response.Data echoes the request's group and addr fields in a
//     valid B524 reply position (validated via IsB524ResponseCoherent).
//
// OR
//
//   - request is a B509 ScanID identity reply (Primary=0xB5 Secondary=0x09
//     with opcode 0x29 at request.Data[0]) and response.Data is at least
//     4 bytes (an identity-bearing reply payload).
//
// The shared B524 coherency helper (IsB524ResponseCoherent) makes the
// passive-evidence acceptance criterion identical to the active probe
// acceptance criterion in cmd/gateway/semantic_vaillant.go's
// isB524ProbeCoherent — both call sites use the same source of truth.
//
// This guards against stray fragments or spoofed frames promoting
// phantom addresses on a single observation: a passively observed
// frame must echo the requested register parameters in their
// canonical position before counting as strong evidence.
func passiveResponseIsCoherentVaillantEvidence(request, response protocol.Frame) bool {
	if request.Primary == 0xB5 && request.Secondary == 0x24 {
		group, addr, ok := b524RequestParameters(request.Data)
		if !ok {
			return false
		}
		return IsB524ResponseCoherent(response.Data, group, addr)
	}
	if request.Primary == 0xB5 && request.Secondary == 0x09 &&
		len(request.Data) > 0 && request.Data[0] == 0x29 &&
		len(response.Data) >= 4 {
		return true
	}
	return false
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

// AdaptermuxDiagSnapshot is the subset of adaptermux's
// activeTxnDiag snapshot exposed via the Prometheus surface for
// batch-21 forensic instrumentation. Field semantics match the
// originating counters in `internal/adaptermux/diag.go`. Plumbing
// through this small struct (rather than importing the full
// ActiveTxnSnapshot) keeps `bus_observability_store` decoupled from
// the adaptermux package.
type AdaptermuxDiagSnapshot struct {
	// SynSuppressedPreEcho mirrors activeTxnDiag.synSuppressedPreEcho
	// — total SYNs the existing P10.2 / pre-first-echo gate has
	// suppressed. Provided for context alongside the new batch-21
	// counters so an operator reading /metrics can compare the
	// suppressed population to the gap populations below.
	SynSuppressedPreEcho uint64
	// SynSeenDuringGrantWindow counts SYNs observed during gateway
	// ownership where gatewayTxnActive=false (Attack 1 — grant→first-
	// write window).
	SynSeenDuringGrantWindow uint64
	// SynSeenWhileInterWriteEmpty counts SYNs observed during a
	// gateway-owned active txn where the echo queue is empty AND at
	// least one byte has been delivered to active (Attack 3 — inter-
	// write queue-empty window).
	SynSeenWhileInterWriteEmpty uint64
	// SynSeenAfterTransportWindowExpired counts SYNs observed during
	// gateway ownership where the upstream ENH transport's
	// postGrantPreEcho window has closed via deadline-expiry in the
	// current transaction (Attack 2 — batch-22 round-3). Forensic
	// only; correlates with the residual pre_echo_syn rate
	// unexplained by Attack 1 + Attack 3 instrumentation.
	SynSeenAfterTransportWindowExpired uint64
}

// SetAdaptermuxDiagProvider registers a snapshot provider callback for
// batch-21 forensic counters. Wired in cmd/gateway/main.go after the
// adaptermux is constructed; the callback closes over the mux and is
// invoked on each /metrics scrape. nil-store and nil-provider are no-
// ops (defensive — keeps test setup that does not exercise adaptermux
// from blowing up).
func (store *BusObservabilityStore) SetAdaptermuxDiagProvider(provider func() AdaptermuxDiagSnapshot) {
	if store == nil {
		return
	}
	store.mu.Lock()
	store.adaptermuxDiagProvider = provider
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
	echoMismatchSubclasses := cloneEchoMismatchSubclassesMap(store.echoMismatchSubclasses)
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
	adaptermuxDiagProvider := store.adaptermuxDiagProvider
	store.mu.Unlock()

	// batch-21 diagnostic counters — snapshot OUTSIDE store.mu (the
	// provider holds adaptermux's stateMu internally; nesting locks
	// across packages would invite ABBA risk). Provider is nil when
	// no adaptermux is wired (tests, smoke setups); skip the section
	// in that case so the surface degrades cleanly.
	var adaptermuxDiag AdaptermuxDiagSnapshot
	haveAdaptermuxDiag := false
	if adaptermuxDiagProvider != nil {
		adaptermuxDiag = adaptermuxDiagProvider()
		haveAdaptermuxDiag = true
	}

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

	// P10 — parallel echo_mismatch subclass breakdown. Separate
	// metric (not a label dimension on ebus_errors_total) so that
	// existing alerts filtering on class=echo_mismatch keep working
	// without per-series fan-out.
	writer.writeHelp("ebus_active_echo_mismatch_subclass_total", "Active-scope echo_mismatch event count broken down by inferred subclass (byte-value-based + EchoWasEscaped discriminator for 0xAA; approximate for non-0xAA bytes): pre_echo_syn_raw (read 0xAA with WasEscaped=false — REAL wire SYN intrusion; mux SYN-suppression leak class) [batch-23 round-4 2026-05-17], pre_echo_syn_escaped_data (read 0xAA with WasEscaped=true — escape-decoded 0xAA payload from a third-party frame; wire-level contention class, Attack 10) [batch-23 round-4 2026-05-17], post_grant_collision_initiator (third-party initiator's SOF on the wire), post_grant_collision_target (third-party mid-frame target/broadcast byte), post_grant_ack (stale 0x00 from previous txn buffer), post_grant_nack (stale 0xFF), post_grant_reserved (mid-escape sequence), bit_flip (fallback; EMI/wire corruption). NOTE: the pre-batch-23 `pre_echo_syn` label is RETIRED — operators querying that legacy label after deploy see no data; sum `pre_echo_syn_raw` + `pre_echo_syn_escaped_data` to recover pre-batch-23 semantics.")
	writer.writeType("ebus_active_echo_mismatch_subclass_total", "counter")
	for _, item := range sortedEchoMismatchSubclassSeries(echoMismatchSubclasses) {
		writer.writeCounterSample("ebus_active_echo_mismatch_subclass_total", float64(item.Value), labelMap(
			"subclass", item.Key.Subclass,
		))
	}

	// batch-21 forensic counters (set via SetAdaptermuxDiagProvider).
	// Three SYN observation populations the operator measures vs the
	// `class=echo_mismatch,subclass=pre_echo_syn` rate to identify
	// which suppression-gap dominates. Forensic only — no behavior
	// change. See _work_adaptermux_audit/EBUSD-VERIFICATION-2026-05-15-batch21.md
	// for hypothesis A/B/C and the round-3 fix-prompt criteria.
	if haveAdaptermuxDiag {
		writer.writeHelp("ebus_adaptermux_syn_suppressed_pre_echo_total",
			"Total SYNs the adaptermux P10.2 + pre-first-echo gate has suppressed during gateway-owned active txns. Provided alongside the batch-21 syn_seen_* counters so the operator can compare suppressed vs leaked populations on a single scrape.")
		writer.writeType("ebus_adaptermux_syn_suppressed_pre_echo_total", "counter")
		writer.writeCounterSample("ebus_adaptermux_syn_suppressed_pre_echo_total",
			float64(adaptermuxDiag.SynSuppressedPreEcho), nil)

		writer.writeHelp("ebus_adaptermux_syn_seen_during_grant_window_total",
			"Diagnostic (batch-21): SYNs observed while gateway owned bus but gatewayTxnActive=false (the brief window between completeArbitrationGrant and the first activeTransport.Write recordSent). preEchoMidFrameSuppress requires gatewayTxnActive=true so any SYN here bypasses the gate. Forensic only — no behavior change. Operator measures rate vs ebus_active_echo_mismatch_subclass_total{subclass=\"pre_echo_syn_raw\"} to confirm Attack 1 (grant-lifecycle) dominance. NOTE (batch-23 round-5, 2026-05-17): the prior `pre_echo_syn` label was retired and split into `pre_echo_syn_raw` (real wire SYN intrusion — the class this counter correlates with) and `pre_echo_syn_escaped_data` (escape-decoded payload 0xAA from a third-party frame); Attack 1 is a raw-wire-SYN-leak hypothesis and does not implicate the escape-decoded subclass.")
		writer.writeType("ebus_adaptermux_syn_seen_during_grant_window_total", "counter")
		writer.writeCounterSample("ebus_adaptermux_syn_seen_during_grant_window_total",
			float64(adaptermuxDiag.SynSeenDuringGrantWindow), nil)

		writer.writeHelp("ebus_adaptermux_syn_seen_while_inter_write_empty_total",
			"Diagnostic (batch-21): SYNs observed during a gateway-owned active txn where the echo queue is empty AND at least one byte has been delivered to active. The peek-based P10.2 gate cannot suppress because there is no expected head to compare against. Forensic only — no behavior change. CAVEAT: this counter cannot be cleanly separated at SYN observation time from a legitimate end-of-transaction terminator SYN — both match the same state. Operator analysis: leak_rate ≈ counter − grantsTotal_in_window (each successful transaction has one legitimate terminator SYN; subtract that baseline to estimate the inter-write leak rate). Compare the residual to ebus_active_echo_mismatch_subclass_total{subclass=\"pre_echo_syn_raw\"} rate to confirm Attack 3 (queue-empty inter-write) dominance. NOTE (batch-23 round-5, 2026-05-17): the prior `pre_echo_syn` label was retired and split into `pre_echo_syn_raw` (real wire SYN intrusion — the class this counter correlates with) and `pre_echo_syn_escaped_data` (escape-decoded payload 0xAA from a third-party frame); Attack 3 is a raw-wire-SYN-leak hypothesis and does not implicate the escape-decoded subclass.")
		writer.writeType("ebus_adaptermux_syn_seen_while_inter_write_empty_total", "counter")
		writer.writeCounterSample("ebus_adaptermux_syn_seen_while_inter_write_empty_total",
			float64(adaptermuxDiag.SynSeenWhileInterWriteEmpty), nil)

		writer.writeHelp("ebus_adaptermux_syn_seen_after_transport_window_expired_total",
			"Diagnostic (batch-22 round-3): SYNs observed while gateway owned the bus AND the upstream ENH transport's postGrantPreEcho window had already closed via deadline-expiry in the current gateway transaction. The transport's 50ms window normally closes on the first non-SYN echo; closure-by-expiry indicates the gateway has not yet seen a real echo and the transport-layer SYN suppression is no longer active for this txn — any subsequent idle 0xAA on the wire reaches the gateway unsuppressed at the transport layer. Forensic only — no behavior change. Per Attack 2 (batch-22): events here are leak candidates beyond what Attack 1 + Attack 3 instrumentation covers (~54% pre_echo_syn residual unaccounted-for after round-2). Operator analysis: compare rate to ebus_active_echo_mismatch_subclass_total{subclass=\"pre_echo_syn_raw\"} to confirm Attack 2 dominance. NOTE (batch-23 round-5, 2026-05-17): the prior `pre_echo_syn` label was retired and split into `pre_echo_syn_raw` (real wire SYN intrusion — the class this counter correlates with) and `pre_echo_syn_escaped_data` (escape-decoded payload 0xAA from a third-party frame); Attack 2 is a raw-wire-SYN-leak hypothesis and does not implicate the escape-decoded subclass.")
		writer.writeType("ebus_adaptermux_syn_seen_after_transport_window_expired_total", "counter")
		writer.writeCounterSample("ebus_adaptermux_syn_seen_after_transport_window_expired_total",
			float64(adaptermuxDiag.SynSeenAfterTransportWindowExpired), nil)
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

	// A.9 — surface per-reason abandon counts so operators can compare
	// to Grafana ground-truth frame counts. Catches operator-confirmed
	// regressions like 0xF1→0x15 (B503) being abandoned with
	// unexpected_symbol or 0x03→0x04 (B511) being abandoned with
	// no_response despite valid frames on the wire.
	writer.writeHelp("ebus_passive_reconstructor_abandons_total", "Passive reconstructor abandon counts per reason.")
	writer.writeType("ebus_passive_reconstructor_abandons_total", "counter")
	abandonReasonsSorted := make([]string, 0, len(snapshot.AbandonsByReason))
	for reason := range snapshot.AbandonsByReason {
		abandonReasonsSorted = append(abandonReasonsSorted, reason)
	}
	sort.Strings(abandonReasonsSorted)
	for _, reason := range abandonReasonsSorted {
		writer.writeCounterSample("ebus_passive_reconstructor_abandons_total", float64(snapshot.AbandonsByReason[reason]), labelMap("reason", reason))
	}

	// P6 Layer 1 canary — bytes dropped because no inter-frame SYN
	// was observed. Spikes briefly at startup, then plateaus at
	// near-zero on a clean bus. Sustained non-zero rate after warmup
	// is operator signal for upstream investigation (overflow drop,
	// continuation-byte injection).
	writer.writeHelp("ebus_passive_reconstructor_prefix_resync_skipped_total", "Passive reconstructor bytes dropped because no SymbolSyn was observed since the previous frame boundary (P6 inter-frame SYN gate).")
	writer.writeType("ebus_passive_reconstructor_prefix_resync_skipped_total", "counter")
	writer.writeCounterSample("ebus_passive_reconstructor_prefix_resync_skipped_total", float64(snapshot.PrefixResyncSkippedTotal), nil)

	// P6 Layer 2 canary — direct measure of operator-confirmed Mode B
	// (SRC byte loss in the upstream ENH transport / proxy). Sustained
	// non-zero rate after deploy quantifies the cost of deferring the
	// P6.1 ENH-transport replay / P6.2 proxy fan-out follow-ups.
	writer.writeHelp("ebus_passive_reconstructor_invalid_src_class_skipped_total", "Passive reconstructor bytes rejected as non-initiator-class in source position (P6 SRC AddressClass validation; direct measure of upstream byte loss).")
	writer.writeType("ebus_passive_reconstructor_invalid_src_class_skipped_total", "counter")
	writer.writeCounterSample("ebus_passive_reconstructor_invalid_src_class_skipped_total", float64(snapshot.InvalidSrcClassSkippedTotal), nil)

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
	if event.Kind == protocol.BusEventEchoMismatch {
		// P10 — parallel echo_mismatch subclass counter for operator
		// observability. Stored in a separate map so the existing
		// `ebus_errors_total{class="echo_mismatch"}` time series is
		// not split (Codex P10 review pass 1 MAJOR FINDING_1).
		// batch-23 round-4 (2026-05-17): pass EchoWasEscaped so the
		// classifier can split `pre_echo_syn` into `pre_echo_syn_raw`
		// vs `pre_echo_syn_escaped_data` per the Attack 10 hypothesis.
		// Field is populated upstream by sendRawWithEcho's escape-
		// aware guards in protocol/bus.go ~1054.
		subclass := classifyEchoMismatchSubclass(event.Byte, event.EchoWasEscaped)
		store.echoMismatchSubclasses[echoMismatchSubclassKey{Subclass: subclass}]++
	}
	if !event.HasRequest {
		return
	}
	family := classifyFamily(event.Request)
	frameType := classifyActiveFrameType(event.Request, store.localAddressSnapshotLocked())
	// Active-path counting alignment with the passive path
	// (recordPassiveAbandonedLocked). The gateway began an active
	// transaction whose initial bytes hit the wire; that constitutes
	// an "observed attempt on the wire" exactly as a passive
	// abandoned transaction does. Count it under
	// `ebus_frames_observed_total{scope="active", …}` so the active
	// and passive scopes share semantics:
	//
	//   ebus_frames_observed_total{scope=X}     = attempts (success + failure)
	//   ebus_errors_total{scope=X, class=Y}     = subset of failures, by class
	//   active-error ratio                      = 1 - errors/observed
	//
	// Before this change, active frames_observed counted only
	// successes, so dashboards that read `rate(frames_observed{
	// scope="active"})` silently underreported attempts and offered
	// no comparable error-ratio query across scopes. Operator
	// confirmed Option A on the design discussion (release-note
	// flag: any consumer that read the active counter as "success
	// rate" must subtract `errors_total` to recover the old
	// meaning).
	store.incrementFrameLocked(frameSeriesKey{
		Scope:     "active",
		Source:    store.normalizeAddressLocked(event.Request.Source),
		Target:    store.normalizeAddressLocked(event.Request.Target),
		Family:    family,
		FrameType: frameType,
	})
	store.frameBytes[frameBytesSeriesKey{Scope: "active", Family: family, FrameType: frameType, Part: "request"}] += float64Frame(frameWireLen(event.Request))
	if event.HasResponse {
		// Partial-response failures (e.g. CRC mismatch on the
		// response) still carry response bytes that were on the
		// wire; book them so request+response bytes sum on
		// active-scope failure rows just as they do on the
		// passive abandoned-event row.
		store.frameBytes[frameBytesSeriesKey{Scope: "active", Family: family, FrameType: frameType, Part: "response"}] += float64Frame(responseWireLen(event.Response))
	}
	store.pushRecentLocked(BusMessageRecord{
		Scope:       "active",
		Family:      family,
		FrameType:   frameType,
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

// classifyEchoMismatchSubclass returns a sub-type label for an
// echo_mismatch event based on the byte that was read in place of
// the expected echo AND whether the byte was escape-decoded by the
// ENH transport. P10 (operator-directed) — the residual
// echo_mismatch counter (300+ events post-P7.1) hides several
// distinct phenomena that should be tracked separately for
// operator clarity.
//
// IMPORTANT — labels are byte-value-based with a single
// WasEscaped discriminator on the 0xAA case (batch-23 round-4 doc
// fix, 2026-05-17). For non-0xAA bytes the WasEscaped flag is
// ignored. The classifier still doesn't have access to active-path
// state (bytesDeliveredToActive, echoCursor position) so labels
// for non-0xAA bytes are best-effort directional signals rather
// than ground-truth root causes.
//
//   - "pre_echo_syn_raw" (batch-23 round-4, 2026-05-17) — read 0xAA
//     with WasEscaped=false. A REAL wire SYN arrived in echo
//     position. Most often a mux SYN-suppression leak (the gate at
//     internal/adaptermux/mux.go:1118-1133 was bypassed) OR an
//     idle SYN arriving in the brief window between the adapter
//     reporting STARTED and our first write byte reaching the
//     wire.
//
//   - "pre_echo_syn_escaped_data" (batch-23 round-4, 2026-05-17) —
//     read 0xAA with WasEscaped=true. The transport's escape
//     decoder unwrapped a wire `A9 01` pair from a third-party
//     frame's payload into a logical 0xAA. This is "Attack 10":
//     a foreign initiator was mid-frame on the wire and its
//     escaped payload byte 0xAA reached our echo-comparison
//     position. NOT a gateway-internal bug — wire-level
//     contention, same operational character as
//     post_grant_collision_*.
//
//   - "pre_echo_syn" (RETIRED, batch-23 round-4, 2026-05-17) —
//     pre-batch-23 this single label conflated both of the above.
//     Operators querying the old label see no data after deploy;
//     sum `pre_echo_syn_raw` + `pre_echo_syn_escaped_data` to
//     recover pre-batch-23 semantics.
//
//   - "post_grant_collision_initiator" — read an initiator-class
//     byte (initiator per AddressClassOf). The wire was carrying a
//     third-party initiator's source byte at the moment our TX
//     reached the wire. Real bus collision after the adapter's
//     STARTED but before the wire confirmed idle.
//
//   - "post_grant_collision_target" — read a target-class or
//     broadcast byte. The wire was carrying a third-party
//     transaction's target/broadcast position; we tried to TX
//     during another initiator's mid-frame write.
//
//   - "post_grant_ack" — read 0x00. Stale ACK byte from a previous
//     transaction's tail still in the adapter / TCP socket buffer
//     when we TX'd. Indicates adapter-buffering boundary races.
//     Live observation (P10 deploy 2026-05-10): 50% of residual
//     echo_mismatch events fall here.
//
//   - "post_grant_nack" — read 0xFF. Stale NACK byte (rare).
//
//   - "post_grant_reserved" — read 0xA9 (escape). The wire was
//     carrying an escape sequence header from another transaction.
//
//   - "bit_flip" — fallback. The byte doesn't match any known
//     pattern; likely EMI / wire corruption / single-bit flip.
//     Not fixable in software.
//
// Operator framing (2026-05-09): "After P7.1 passive errors dropped
// dramatically. Apart from CRC + collisions, we shouldn't see much
// else." The subclass labels surface which echo_mismatch events are
// real collisions (most of the 300) vs. wire-noise residuals.
func classifyEchoMismatchSubclass(readByte byte, echoWasEscaped bool) string {
	switch readByte {
	case protocol.SymbolSyn:
		// batch-23 round-4 (2026-05-17): split the previously single
		// `pre_echo_syn` label by EchoWasEscaped to distinguish a
		// real wire SYN intrusion (mux SYN-suppression leak class)
		// from an escape-decoded 0xAA data byte from a third-party
		// frame's payload (wire `A9 01` → logical 0xAA, WasEscaped=
		// true). Per Attack 10 hypothesis: the latter is responsible
		// for the ~19.6× pre_echo_syn:leak ratio observed in live HA
		// soak round-3 (cumulative=1216, Attack 1 counter=23820)
		// where none of the byte-value-only SYN counters tracked
		// 1:1 with the residual.
		if echoWasEscaped {
			return "pre_echo_syn_escaped_data"
		}
		return "pre_echo_syn_raw"
	case protocol.SymbolEscape:
		return "post_grant_reserved"
	case protocol.SymbolAck:
		return "post_grant_ack"
	case protocol.SymbolNack:
		return "post_grant_nack"
	}
	switch protocol.AddressClassOf(readByte) {
	case protocol.AddressClassMaster:
		return "post_grant_collision_initiator"
	case protocol.AddressClassSlave:
		return "post_grant_collision_target"
	case protocol.AddressClassBroadcast:
		// Broadcast is destined for all; reading one in echo
		// position means another initiator was emitting a broadcast
		// frame during our TX. Same class as the target case
		// (someone else was on the wire mid-frame).
		return "post_grant_collision_target"
	}
	return "bit_flip"
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
		PassiveAbandonReasonAmbiguousRetransmit,
		// F-19c (batch-16, Codex bot P2 round 2 on PR #629): the
		// new spec-bound reasons are STRUCTURAL reclassifications
		// of bus-noise abandons that were previously classified as
		// corrupted_request (or arbitration_fragment). They share
		// the same operational character — expected on a shared
		// bus passive tap, not a software error — so they must
		// fall into the same non-error bucket. Routing them
		// through the default ("abandoned", "terminal") branch
		// would increment passive error metrics on every
		// bad-LEN or buffer-overflow event and potentially fire
		// alerts in production windows where F-19c reclassifies
		// the same noise corruption_request used to silently absorb.
		PassiveAbandonReasonInvalidQQ, PassiveAbandonReasonInvalidZZ,
		PassiveAbandonReasonInvalidNNMaster, PassiveAbandonReasonInvalidNNSlave,
		PassiveAbandonReasonBufferOverflow:
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

// cloneEchoMismatchSubclassesMap returns a copy of the
// echoMismatchSubclasses map (P10 — parallel echo_mismatch
// subclass counter). Used by RenderPrometheus to take a stable
// snapshot under the store mutex.
func cloneEchoMismatchSubclassesMap(input map[echoMismatchSubclassKey]uint64) map[echoMismatchSubclassKey]uint64 {
	output := make(map[echoMismatchSubclassKey]uint64, len(input))
	for key, value := range input {
		output[key] = value
	}
	return output
}

// echoMismatchSubclassSeriesItem pairs a subclass key with its
// counter value for deterministic sorted iteration.
type echoMismatchSubclassSeriesItem struct {
	Key   echoMismatchSubclassKey
	Value uint64
}

// sortedEchoMismatchSubclassSeries returns the subclass series
// items sorted by Subclass label for deterministic Prometheus
// output (operator-friendly: alphabetic order is stable across
// scrapes).
func sortedEchoMismatchSubclassSeries(input map[echoMismatchSubclassKey]uint64) []echoMismatchSubclassSeriesItem {
	items := make([]echoMismatchSubclassSeriesItem, 0, len(input))
	for key, value := range input {
		items = append(items, echoMismatchSubclassSeriesItem{Key: key, Value: value})
	}
	sort.Slice(items, func(i, j int) bool {
		return items[i].Key.Subclass < items[j].Key.Subclass
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
