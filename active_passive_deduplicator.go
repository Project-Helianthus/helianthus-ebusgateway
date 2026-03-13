package ebusgateway

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"expvar"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	ebuserrors "github.com/Project-Helianthus/helianthus-ebusgo/errors"
	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
)

const (
	dedupAttemptBudgetUnit            = 250 * time.Millisecond
	dedupCollisionResyncBudgetUnit    = 125 * time.Millisecond
	dedupPublicationSlack             = 250 * time.Millisecond
	dedupPendingMatchSlack            = 250 * time.Millisecond
	defaultDedupPassiveDeliveryBudget = 500 * time.Millisecond
	defaultDedupPendingCapacity       = 256
	dedupRecoveryHealthyWindow        = 5 * time.Second
	dedupRecoveryMinimumEvents        = 10
	defaultDedupExpiryTick            = 100 * time.Millisecond

	defaultDedupCriticalSubscriberBuffer    = 128
	defaultDedupNonCriticalSubscriberBuffer = 32
)

var (
	observeFirstDedupState                        = expvar.NewString("observe_first_dedup_state")
	observeFirstDedupEpoch                        = expvar.NewInt("observe_first_dedup_epoch")
	observeFirstDedupEpochResetTotal              = expvar.NewInt("observe_first_dedup_epoch_reset_total")
	observeFirstDedupPendingFlushTotal            = expvar.NewMap("observe_first_dedup_pending_flush_total")
	observeFirstDedupAdjudicationsTotal           = expvar.NewMap("observe_first_dedup_adjudications_total")
	observeFirstDedupDegradedTransitionsTotal     = expvar.NewMap("observe_first_dedup_degraded_transitions_total")
	observeFirstDedupLocalParticipantInboundTotal = expvar.NewInt("observe_first_dedup_local_participant_inbound_total")
)

type DedupTransactionClass string

const (
	DedupTransactionClassMasterTarget       DedupTransactionClass = "master_target"
	DedupTransactionClassMasterMaster       DedupTransactionClass = "master_master"
	DedupTransactionClassBroadcast          DedupTransactionClass = "broadcast"
	DedupTransactionClassLocalParticipantIn DedupTransactionClass = "local_participant_inbound"
	DedupTransactionClassAbandonedPartial   DedupTransactionClass = "abandoned_partial"
)

type DedupOutcomeClass string

const (
	DedupOutcomeSuccess        DedupOutcomeClass = "success"
	DedupOutcomeNACK           DedupOutcomeClass = "nack"
	DedupOutcomeTimeout        DedupOutcomeClass = "timeout"
	DedupOutcomeCollision      DedupOutcomeClass = "collision"
	DedupOutcomeTransportReset DedupOutcomeClass = "transport_reset"
	DedupOutcomeDecodeReset    DedupOutcomeClass = "decode_reset"
	DedupOutcomeAbandoned      DedupOutcomeClass = "abandoned"
)

type DedupResponseClass string

const (
	DedupResponseValueBearing     DedupResponseClass = "value_bearing"
	DedupResponseACKOnly          DedupResponseClass = "ack_only"
	DedupResponseHeaderOnly       DedupResponseClass = "header_only"
	DedupResponseErrorOrAmbiguous DedupResponseClass = "error_or_ambiguous"
)

type DedupDisposition string

const (
	DedupDispositionUnmatchedThirdParty DedupDisposition = "unmatched_third_party"
	DedupDispositionMatchedActiveCopy   DedupDisposition = "matched_active_duplicate"
	DedupDispositionLocalParticipantIn  DedupDisposition = "local_participant_inbound"
	DedupDispositionObservabilityOnly   DedupDisposition = "observability_only"
	DedupDispositionDiscontinuity       DedupDisposition = "discontinuity"
)

type LocalAddressSnapshot struct {
	Address byte
	Known   bool
	Epoch   uint64
}

type LocalBusAddressSnapshotter interface {
	LocalAddressSnapshot() LocalAddressSnapshot
}

type ActiveTransactionFingerprint struct {
	Epoch            uint64
	TransactionClass DedupTransactionClass
	OutcomeClass     DedupOutcomeClass
	ResponseClass    DedupResponseClass
	FamilyPolicy     ObserveFirstFamilyPolicy
	Source           byte
	Target           byte
	RequestBytes     []byte
	ResponseBytes    []byte
	Hash             [32]byte
	ObservedAt       time.Time
}

type PassiveTransactionFingerprint struct {
	Epoch            uint64
	TransactionClass DedupTransactionClass
	OutcomeClass     DedupOutcomeClass
	ResponseClass    DedupResponseClass
	FamilyPolicy     ObserveFirstFamilyPolicy
	SharedWatchKey   WatchKey
	Source           byte
	Target           byte
	RequestBytes     []byte
	ResponseBytes    []byte
	Hash             [32]byte
	ObservedAt       time.Time
}

type AdjudicatedPassiveEvent struct {
	Event                   PassiveClassifiedEvent
	Fingerprint             PassiveTransactionFingerprint
	FamilyPolicy            ObserveFirstFamilyPolicy
	Disposition             DedupDisposition
	SuppressShadow          bool
	SuppressWatchEfficiency bool
	ThirdPartyEligible      bool
	ObservabilityOnly       bool
	LocalParticipantInbound bool
	MatchedActiveDuplicate  bool
	Epoch                   uint64
}

type DedupSubscriberPriority uint8

const (
	DedupSubscriberCritical DedupSubscriberPriority = iota + 1
	DedupSubscriberNonCritical
)

type AdjudicatedPassiveSubscription struct {
	id           uint64
	name         string
	priority     DedupSubscriberPriority
	ch           chan AdjudicatedPassiveEvent
	deduplicator *ActivePassiveDeduplicator
	closeOnce    sync.Once
}

func (subscription *AdjudicatedPassiveSubscription) Events() <-chan AdjudicatedPassiveEvent {
	if subscription == nil {
		return nil
	}
	return subscription.ch
}

func (subscription *AdjudicatedPassiveSubscription) Close() {
	if subscription == nil || subscription.deduplicator == nil {
		return
	}
	subscription.deduplicator.unsubscribe(subscription)
}

type dedupTimingBudgets struct {
	LogicalRequestEnvelope     time.Duration
	ActivePublishBudget        time.Duration
	PassiveDeliveryBudget      time.Duration
	PendingGraceTimeout        time.Duration
	ActiveFingerprintRetention time.Duration
	PendingCapacity            int
	RecoveryHysteresis         time.Duration
	RecoveryEventThreshold     int
}

type retainedActiveFingerprint struct {
	Fingerprint ActiveTransactionFingerprint
	ExpiresAt   time.Time
	Count       int
}

type pendingPassiveFingerprint struct {
	Event       PassiveClassifiedEvent
	Fingerprint PassiveTransactionFingerprint
	InsertedAt  time.Time
	ReleaseAt   time.Time
	Epoch       uint64
}

type recoveryState struct {
	StartedAt       time.Time
	PassiveEvidence int
}

type ActivePassiveDeduplicator struct {
	cfg     Config
	nowFunc func() time.Time
	budgets dedupTimingBudgets

	ctx           context.Context
	cancel        context.CancelFunc
	wg            sync.WaitGroup
	reconstructor *PassiveTransactionReconstructor

	mu           sync.Mutex
	currentEpoch uint64
	degraded     bool
	recovery     recoveryState
	localAddr    LocalAddressSnapshot
	active       map[[32]byte]retainedActiveFingerprint
	pending      []pendingPassiveFingerprint

	subscribersMu sync.Mutex
	subscribers   map[uint64]*AdjudicatedPassiveSubscription
	nextSubID     atomic.Uint64

	closeOnce sync.Once
}

func NewActivePassiveDeduplicator(cfg Config) (*ActivePassiveDeduplicator, error) {
	cfg = applyDefaults(cfg)
	requiredBudgets, err := deriveDedupTimingBudgets(cfg.BusConfig.RetryEnvelope())
	if err != nil {
		return nil, err
	}
	budgets := dedupTimingBudgets{
		LogicalRequestEnvelope:     requiredBudgets.LogicalRequestEnvelope,
		ActivePublishBudget:        cfg.PassiveDedupActivePublishBudget,
		PassiveDeliveryBudget:      cfg.PassiveDedupPassiveDeliveryBudget,
		PendingGraceTimeout:        cfg.PassiveDedupPendingGraceTimeout,
		ActiveFingerprintRetention: cfg.PassiveDedupActiveFingerprintRetention,
		PendingCapacity:            cfg.PassiveDedupPendingCapacity,
		RecoveryHysteresis:         cfg.PassiveDedupRecoveryHysteresis,
		RecoveryEventThreshold:     cfg.PassiveDedupRecoveryEventThreshold,
	}
	if budgets.ActivePublishBudget < requiredBudgets.ActivePublishBudget ||
		budgets.PendingGraceTimeout < requiredBudgets.PendingGraceTimeout ||
		budgets.ActiveFingerprintRetention < requiredBudgets.ActiveFingerprintRetention ||
		budgets.PendingCapacity < requiredBudgets.PendingCapacity ||
		budgets.PassiveDeliveryBudget < requiredBudgets.PassiveDeliveryBudget {
		return nil, fmt.Errorf("passive dedup budgets below validated minimums: %+v < %+v", budgets, requiredBudgets)
	}

	deduplicator := &ActivePassiveDeduplicator{
		cfg:          cfg,
		nowFunc:      time.Now,
		budgets:      budgets,
		currentEpoch: 1,
		degraded:     true,
		active:       make(map[[32]byte]retainedActiveFingerprint),
		pending:      make([]pendingPassiveFingerprint, 0, budgets.PendingCapacity),
		subscribers:  make(map[uint64]*AdjudicatedPassiveSubscription),
	}
	if cfg.LocalAddressSnapshotter != nil {
		deduplicator.localAddr = cfg.LocalAddressSnapshotter.LocalAddressSnapshot()
	}
	deduplicator.updateExpvarsLocked()
	return deduplicator, nil
}

func deriveDedupTimingBudgets(envelope protocol.BusRetryEnvelope) (dedupTimingBudgets, error) {
	logicalEnvelope := logicalRequestRetryEnvelope(envelope)
	if logicalEnvelope <= 0 {
		return dedupTimingBudgets{}, fmt.Errorf("dedup retry envelope invalid")
	}
	activePublish := logicalEnvelope + dedupPublicationSlack
	pendingGrace := maxDuration(activePublish, defaultDedupPassiveDeliveryBudget) + dedupPendingMatchSlack
	return dedupTimingBudgets{
		LogicalRequestEnvelope:     logicalEnvelope,
		ActivePublishBudget:        activePublish,
		PassiveDeliveryBudget:      defaultDedupPassiveDeliveryBudget,
		PendingGraceTimeout:        pendingGrace,
		ActiveFingerprintRetention: pendingGrace + defaultDedupPassiveDeliveryBudget,
		PendingCapacity:            defaultDedupPendingCapacity,
		RecoveryHysteresis:         dedupRecoveryHealthyWindow,
		RecoveryEventThreshold:     dedupRecoveryMinimumEvents,
	}, nil
}

func defaultPassiveDedupBudgets(envelope protocol.BusRetryEnvelope) dedupTimingBudgets {
	budgets, err := deriveDedupTimingBudgets(envelope)
	if err != nil {
		return dedupTimingBudgets{
			PendingCapacity:        defaultDedupPendingCapacity,
			RecoveryHysteresis:     dedupRecoveryHealthyWindow,
			RecoveryEventThreshold: dedupRecoveryMinimumEvents,
		}
	}
	return budgets
}

func logicalRequestRetryEnvelope(envelope protocol.BusRetryEnvelope) time.Duration {
	policy := envelope.InitiatorTarget
	if envelope.InitiatorInitiator.TimeoutRetries+envelope.InitiatorInitiator.NACKRetries >
		envelope.InitiatorTarget.TimeoutRetries+envelope.InitiatorTarget.NACKRetries {
		policy = envelope.InitiatorInitiator
	}

	timeoutRetries := maxInt(policy.TimeoutRetries, 0)
	nackRetries := maxInt(policy.NACKRetries, 0)
	collisionResync := maxInt(envelope.CollisionResyncSYNCount, 0)

	return time.Duration(1+timeoutRetries+nackRetries)*dedupAttemptBudgetUnit +
		time.Duration(collisionResync)*dedupCollisionResyncBudgetUnit
}

func (deduplicator *ActivePassiveDeduplicator) Budgets() dedupTimingBudgets {
	if deduplicator == nil {
		return dedupTimingBudgets{}
	}
	return deduplicator.budgets
}

func (deduplicator *ActivePassiveDeduplicator) LocalAddressSnapshot() LocalAddressSnapshot {
	if deduplicator == nil {
		return LocalAddressSnapshot{}
	}
	deduplicator.mu.Lock()
	defer deduplicator.mu.Unlock()
	return deduplicator.localAddr
}

func (deduplicator *ActivePassiveDeduplicator) Subscribe(name string, priority DedupSubscriberPriority, buffer int) (*AdjudicatedPassiveSubscription, error) {
	if deduplicator == nil {
		return nil, fmt.Errorf("deduplicator missing: %w", ebuserrors.ErrInvalidPayload)
	}
	if name == "" {
		return nil, fmt.Errorf("dedup subscriber missing name: %w", ebuserrors.ErrInvalidPayload)
	}
	if priority == 0 {
		priority = DedupSubscriberNonCritical
	}
	if buffer <= 0 {
		buffer = defaultDedupSubscriberBuffer(priority)
	}

	subscription := &AdjudicatedPassiveSubscription{
		id:           deduplicator.nextSubID.Add(1),
		name:         name,
		priority:     priority,
		ch:           make(chan AdjudicatedPassiveEvent, buffer),
		deduplicator: deduplicator,
	}

	deduplicator.subscribersMu.Lock()
	defer deduplicator.subscribersMu.Unlock()
	if deduplicator.subscribers == nil {
		return nil, fmt.Errorf("deduplicator closed: %w", ebuserrors.ErrTransportClosed)
	}
	deduplicator.subscribers[subscription.id] = subscription
	return subscription, nil
}

func defaultDedupSubscriberBuffer(priority DedupSubscriberPriority) int {
	if priority == DedupSubscriberCritical {
		return defaultDedupCriticalSubscriberBuffer
	}
	return defaultDedupNonCriticalSubscriberBuffer
}

func (deduplicator *ActivePassiveDeduplicator) AttachReconstructor(ctx context.Context, reconstructor *PassiveTransactionReconstructor) error {
	if deduplicator == nil {
		return fmt.Errorf("deduplicator missing: %w", ebuserrors.ErrInvalidPayload)
	}
	if reconstructor == nil {
		return fmt.Errorf("dedup reconstructor missing: %w", ebuserrors.ErrInvalidPayload)
	}
	if ctx == nil {
		ctx = context.Background()
	}

	deduplicator.mu.Lock()
	defer deduplicator.mu.Unlock()
	if deduplicator.ctx != nil {
		return fmt.Errorf("dedup reconstructor already attached: %w", ebuserrors.ErrInvalidPayload)
	}

	dedupCtx, cancel := context.WithCancel(ctx)
	deduplicator.ctx = dedupCtx
	deduplicator.cancel = cancel
	deduplicator.reconstructor = reconstructor

	deduplicator.wg.Add(2)
	go deduplicator.runPassiveLoop()
	go deduplicator.runExpiryLoop()
	return nil
}

func (deduplicator *ActivePassiveDeduplicator) Close() error {
	if deduplicator == nil {
		return nil
	}

	deduplicator.closeOnce.Do(func() {
		if deduplicator.cancel != nil {
			deduplicator.cancel()
		}
		deduplicator.wg.Wait()
		deduplicator.closeAllSubscribers()
	})
	return nil
}

func (deduplicator *ActivePassiveDeduplicator) runPassiveLoop() {
	defer deduplicator.wg.Done()

	for {
		ctx, reconstructor := deduplicator.snapshotRuntime()
		if ctx == nil || reconstructor == nil {
			return
		}
		if err := ctx.Err(); err != nil {
			return
		}

		subscription, err := reconstructor.Subscribe("active-passive-dedup", PassiveSubscriberCritical, 0)
		if err != nil {
			timer := time.NewTimer(defaultDedupExpiryTick)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
				continue
			}
		}

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
				deduplicator.OnPassiveClassifiedEvent(event)
			}
		}

	nextSubscription:
		if closedUnexpectedly {
			deduplicator.handleCriticalReset(time.Now(), PassiveDiscontinuityCriticalSubscriberFault, "passive_subscription_reset")
		}
	}
}

func (deduplicator *ActivePassiveDeduplicator) runExpiryLoop() {
	defer deduplicator.wg.Done()

	ticker := time.NewTicker(defaultDedupExpiryTick)
	defer ticker.Stop()

	for {
		ctx, _ := deduplicator.snapshotRuntime()
		if ctx == nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := deduplicator.now()
			outputs := deduplicator.releaseExpiredPending(now)
			deduplicator.publishAll(outputs)
		}
	}
}

func (deduplicator *ActivePassiveDeduplicator) snapshotRuntime() (context.Context, *PassiveTransactionReconstructor) {
	deduplicator.mu.Lock()
	defer deduplicator.mu.Unlock()
	return deduplicator.ctx, deduplicator.reconstructor
}

func (deduplicator *ActivePassiveDeduplicator) OnBusEvent(event protocol.BusEvent) error {
	if deduplicator == nil {
		return nil
	}

	now := deduplicator.now()
	outputs := deduplicator.handleBusEvent(event, now)
	deduplicator.publishAll(outputs)
	return nil
}

func (deduplicator *ActivePassiveDeduplicator) OnPassiveClassifiedEvent(event PassiveClassifiedEvent) {
	if deduplicator == nil {
		return
	}
	observedAt := event.ObservedAt
	if observedAt.IsZero() {
		observedAt = deduplicator.now()
		event.ObservedAt = observedAt
	}
	outputs := deduplicator.handlePassiveEvent(event, observedAt)
	deduplicator.publishAll(outputs)
}

func (deduplicator *ActivePassiveDeduplicator) handleBusEvent(event protocol.BusEvent, now time.Time) []AdjudicatedPassiveEvent {
	deduplicator.mu.Lock()
	defer deduplicator.mu.Unlock()

	outputs := deduplicator.releaseExpiredPendingLocked(now)
	deduplicator.pruneActiveLocked(now)

	switch event.Kind {
	case protocol.BusEventObserverFault:
		clear(deduplicator.active)
		deduplicator.enterDegradedLocked("observer_fault")
		return outputs
	case protocol.BusEventAttemptComplete:
	default:
		return outputs
	}

	if event.Outcome != protocol.BusOutcomeSuccess || !event.HasRequest {
		return outputs
	}

	if deduplicator.updateLocalAddressLocked(event.Request.Source, now) {
		outputs = append(outputs, deduplicator.makeDiscontinuityLocked(now, PassiveDiscontinuityTransportReset, "local_address_epoch")...)
	}

	fingerprint, ok := buildActiveFingerprint(
		deduplicator.currentEpoch,
		event,
		now,
		deduplicator.cfg.ObserveFirstFlags,
		deduplicator.cfg.WatchObserver,
	)
	if !ok {
		deduplicator.enterDegradedLocked("active_fingerprint_build")
		return outputs
	}

	if existing, ok := deduplicator.active[fingerprint.Hash]; ok {
		existing.Count++
		existing.Fingerprint = fingerprint
		existing.ExpiresAt = now.Add(deduplicator.budgets.ActiveFingerprintRetention)
		deduplicator.active[fingerprint.Hash] = existing
	} else {
		deduplicator.active[fingerprint.Hash] = retainedActiveFingerprint{
			Fingerprint: fingerprint,
			ExpiresAt:   now.Add(deduplicator.budgets.ActiveFingerprintRetention),
			Count:       1,
		}
	}

	outputs = append(outputs, deduplicator.matchPendingLocked(fingerprint, now)...)
	deduplicator.recordRecoveryEvidenceLocked(now)
	return outputs
}

func (deduplicator *ActivePassiveDeduplicator) handlePassiveEvent(event PassiveClassifiedEvent, now time.Time) []AdjudicatedPassiveEvent {
	deduplicator.mu.Lock()
	defer deduplicator.mu.Unlock()

	outputs := deduplicator.releaseExpiredPendingLocked(now)
	deduplicator.pruneActiveLocked(now)

	if event.Kind == PassiveClassifiedEventDiscontinuity {
		return append(outputs, deduplicator.makeDiscontinuityLocked(now, event.DiscontinuityReason, "passive_epoch_boundary")...)
	}

	if event.Kind == PassiveClassifiedEventBroadcastFrame {
		deduplicator.recordRecoveryEvidenceLocked(now)
		return outputs
	}

	if event.Kind != PassiveClassifiedEventTransaction &&
		event.Kind != PassiveClassifiedEventMasterFrame &&
		event.Kind != PassiveClassifiedEventAbandonedTransaction {
		return outputs
	}

	fingerprint, matchEligible, immediateDisposition := deduplicator.buildPassiveFingerprintLocked(event, now)
	if !matchEligible {
		outputs = append(outputs, deduplicator.publishImmediateLocked(event, fingerprint, immediateDisposition)...)
		return outputs
	}

	if deduplicator.consumeActiveLocked(fingerprint.Hash) {
		outputs = append(outputs, adjudicateMatchedActiveDuplicate(event, fingerprint, deduplicator.currentEpoch))
		incrementExpvarMap(observeFirstDedupAdjudicationsTotal, string(DedupDispositionMatchedActiveCopy))
		deduplicator.recordRecoveryEvidenceLocked(now)
		return outputs
	}

	for len(deduplicator.pending) >= deduplicator.budgets.PendingCapacity {
		index := deduplicator.oldestReleasablePendingLocked(now)
		if index < 0 {
			deduplicator.enterDegradedLocked("pending_capacity")
			outputs = append(outputs, adjudicateObservabilityOnly(event, fingerprint, deduplicator.currentEpoch))
			incrementExpvarMap(observeFirstDedupAdjudicationsTotal, string(DedupDispositionObservabilityOnly))
			return outputs
		}
		outputs = append(outputs, deduplicator.releasePendingIndexLocked(index, now, "grace_expiry")...)
	}

	deduplicator.pending = append(deduplicator.pending, pendingPassiveFingerprint{
		Event:       event,
		Fingerprint: fingerprint,
		InsertedAt:  now,
		ReleaseAt:   now.Add(deduplicator.budgets.PendingGraceTimeout),
		Epoch:       deduplicator.currentEpoch,
	})
	deduplicator.recordRecoveryEvidenceLocked(now)
	return outputs
}

func (deduplicator *ActivePassiveDeduplicator) buildPassiveFingerprintLocked(event PassiveClassifiedEvent, observedAt time.Time) (PassiveTransactionFingerprint, bool, DedupDisposition) {
	transactionClass := deduplicator.passiveTransactionClassLocked(event)
	outcome := passiveOutcomeClass(event)
	responseClass := observeFirstResponseClassForPassiveEvent(event, outcome)

	fingerprint := PassiveTransactionFingerprint{
		Epoch:            deduplicator.currentEpoch,
		TransactionClass: transactionClass,
		OutcomeClass:     outcome,
		ResponseClass:    responseClass,
		ObservedAt:       observedAt,
	}
	if event.HasRequest {
		fingerprint.Source = event.Request.Source
		fingerprint.Target = event.Request.Target
		fingerprint.RequestBytes = canonicalFrameBytes(event.Request)
		if key, ok := PassiveWatchKeyFromEvent(event); ok {
			fingerprint.SharedWatchKey = mustCloneWatchKey(key)
		}
		observation := observeFirstWatchObservationForKey(fingerprint.SharedWatchKey, deduplicator.cfg.WatchObserver)
		fingerprint.FamilyPolicy = observeFirstFamilyPolicy(
			ObserveFirstTrafficScopePassive,
			event.Request,
			responseClass,
			observation,
			deduplicator.cfg.ObserveFirstFlags,
		)
	}
	if event.HasResponse {
		fingerprint.ResponseBytes = canonicalFrameBytes(event.Response)
	}
	fingerprint.Hash = hashTransactionIdentity(
		transactionClass,
		fingerprint.Source,
		fingerprint.Target,
		responseClass,
		outcome,
		fingerprint.RequestBytes,
		fingerprint.ResponseBytes,
	)

	switch transactionClass {
	case DedupTransactionClassBroadcast:
		return fingerprint, false, DedupDispositionObservabilityOnly
	case DedupTransactionClassLocalParticipantIn:
		return fingerprint, false, DedupDispositionLocalParticipantIn
	case DedupTransactionClassMasterMaster:
		if !deduplicator.localAddr.Known {
			return fingerprint, false, DedupDispositionObservabilityOnly
		}
	}

	if outcome != DedupOutcomeSuccess {
		return fingerprint, false, DedupDispositionObservabilityOnly
	}
	return fingerprint, true, ""
}

func (deduplicator *ActivePassiveDeduplicator) passiveTransactionClassLocked(event PassiveClassifiedEvent) DedupTransactionClass {
	switch event.Kind {
	case PassiveClassifiedEventBroadcastFrame:
		return DedupTransactionClassBroadcast
	case PassiveClassifiedEventMasterFrame:
		if deduplicator.localAddr.Known && event.HasRequest && event.Request.Target == deduplicator.localAddr.Address {
			return DedupTransactionClassLocalParticipantIn
		}
		return DedupTransactionClassMasterMaster
	case PassiveClassifiedEventTransaction:
		return DedupTransactionClassMasterTarget
	case PassiveClassifiedEventAbandonedTransaction:
		if event.FrameType == protocol.FrameTypeInitiatorInitiator &&
			deduplicator.localAddr.Known &&
			event.HasRequest &&
			event.Request.Target == deduplicator.localAddr.Address {
			return DedupTransactionClassLocalParticipantIn
		}
		return DedupTransactionClassAbandonedPartial
	default:
		return DedupTransactionClassAbandonedPartial
	}
}

func passiveOutcomeClass(event PassiveClassifiedEvent) DedupOutcomeClass {
	switch event.Kind {
	case PassiveClassifiedEventTransaction, PassiveClassifiedEventMasterFrame, PassiveClassifiedEventBroadcastFrame:
		return DedupOutcomeSuccess
	case PassiveClassifiedEventAbandonedTransaction:
		switch event.AbandonReason {
		case PassiveAbandonReasonNACK:
			return DedupOutcomeNACK
		case PassiveAbandonReasonNoResponse, PassiveAbandonReasonNoProgress, PassiveAbandonReasonDisconnected, PassiveAbandonReasonShutdown:
			return DedupOutcomeTimeout
		case PassiveAbandonReasonTransportReset:
			return DedupOutcomeTransportReset
		case PassiveAbandonReasonDecodeFault:
			return DedupOutcomeDecodeReset
		case PassiveAbandonReasonAmbiguousRetransmit:
			return DedupOutcomeCollision
		default:
			return DedupOutcomeAbandoned
		}
	case PassiveClassifiedEventDiscontinuity:
		switch event.DiscontinuityReason {
		case PassiveDiscontinuityTransportReset, PassiveDiscontinuityDisconnected, PassiveDiscontinuityConnected, PassiveDiscontinuityShutdown:
			return DedupOutcomeTransportReset
		default:
			return DedupOutcomeDecodeReset
		}
	default:
		return DedupOutcomeAbandoned
	}
}

func passiveResponseClass(event PassiveClassifiedEvent, outcome DedupOutcomeClass) DedupResponseClass {
	if outcome != DedupOutcomeSuccess {
		return DedupResponseErrorOrAmbiguous
	}
	if !event.HasResponse {
		if event.FrameType == protocol.FrameTypeInitiatorInitiator {
			return DedupResponseACKOnly
		}
		return DedupResponseErrorOrAmbiguous
	}
	if len(event.Response.Data) == 0 {
		return DedupResponseHeaderOnly
	}
	return DedupResponseValueBearing
}

func buildActiveFingerprint(
	epoch uint64,
	event protocol.BusEvent,
	observedAt time.Time,
	flags ObserveFirstFeatureFlagView,
	observer WatchObserver,
) (ActiveTransactionFingerprint, bool) {
	if !event.HasRequest {
		return ActiveTransactionFingerprint{}, false
	}

	var transactionClass DedupTransactionClass
	switch event.FrameType {
	case protocol.FrameTypeBroadcast:
		transactionClass = DedupTransactionClassBroadcast
	case protocol.FrameTypeInitiatorInitiator:
		transactionClass = DedupTransactionClassMasterMaster
	case protocol.FrameTypeInitiatorTarget:
		transactionClass = DedupTransactionClassMasterTarget
	default:
		return ActiveTransactionFingerprint{}, false
	}

	responseClass := observeFirstResponseClassForActiveEvent(event)
	requestBytes := canonicalFrameBytes(event.Request)
	var responseBytes []byte
	if event.HasResponse {
		responseBytes = canonicalFrameBytes(event.Response)
	}

	fingerprint := ActiveTransactionFingerprint{
		Epoch:            epoch,
		TransactionClass: transactionClass,
		OutcomeClass:     DedupOutcomeSuccess,
		ResponseClass:    responseClass,
		FamilyPolicy: observeFirstFamilyPolicy(
			ObserveFirstTrafficScopeActive,
			event.Request,
			responseClass,
			observeFirstWatchObservationForFrame(event.Request, observer),
			flags,
		),
		Source:        event.Request.Source,
		Target:        event.Request.Target,
		RequestBytes:  requestBytes,
		ResponseBytes: responseBytes,
		ObservedAt:    observedAt,
	}
	fingerprint.Hash = hashTransactionIdentity(
		transactionClass,
		fingerprint.Source,
		fingerprint.Target,
		responseClass,
		DedupOutcomeSuccess,
		requestBytes,
		responseBytes,
	)
	return fingerprint, true
}

func activeResponseClass(event protocol.BusEvent) DedupResponseClass {
	if !event.HasResponse {
		if event.FrameType == protocol.FrameTypeInitiatorInitiator {
			return DedupResponseACKOnly
		}
		return DedupResponseErrorOrAmbiguous
	}
	if len(event.Response.Data) == 0 {
		return DedupResponseHeaderOnly
	}
	return DedupResponseValueBearing
}

func canonicalFrameBytes(frame protocol.Frame) []byte {
	raw := make([]byte, 0, 7+len(frame.Data))
	raw = append(raw, frame.Source, frame.Target, frame.Primary, frame.Secondary, byte(len(frame.Data)))
	raw = append(raw, frame.Data...)
	raw = append(raw, protocol.CRC(raw))
	return raw
}

func hashTransactionIdentity(class DedupTransactionClass, source, target byte, responseClass DedupResponseClass, outcome DedupOutcomeClass, requestBytes, responseBytes []byte) [32]byte {
	buf := make([]byte, 0, 4+len(requestBytes)+len(responseBytes)+16)
	buf = append(buf, byte(len(class)))
	buf = append(buf, []byte(class)...)
	buf = append(buf, source, target)
	buf = append(buf, byte(len(responseClass)))
	buf = append(buf, []byte(responseClass)...)
	buf = append(buf, byte(len(outcome)))
	buf = append(buf, []byte(outcome)...)
	buf = appendLengthPrefixed(buf, requestBytes)
	buf = appendLengthPrefixed(buf, responseBytes)
	return sha256.Sum256(buf)
}

func appendLengthPrefixed(buf, payload []byte) []byte {
	var length [2]byte
	binary.BigEndian.PutUint16(length[:], uint16(len(payload)))
	buf = append(buf, length[:]...)
	buf = append(buf, payload...)
	return buf
}

func (deduplicator *ActivePassiveDeduplicator) matchPendingLocked(active ActiveTransactionFingerprint, now time.Time) []AdjudicatedPassiveEvent {
	if len(deduplicator.pending) == 0 {
		return nil
	}

	outputs := make([]AdjudicatedPassiveEvent, 0, 1)
	next := deduplicator.pending[:0]
	matched := false
	for _, pending := range deduplicator.pending {
		if pending.Epoch != deduplicator.currentEpoch || pending.Fingerprint.Epoch != deduplicator.currentEpoch {
			next = append(next, pending)
			continue
		}
		if !matched && pending.Fingerprint.Hash == active.Hash {
			outputs = append(outputs, adjudicateMatchedActiveDuplicate(pending.Event, pending.Fingerprint, deduplicator.currentEpoch))
			incrementExpvarMap(observeFirstDedupAdjudicationsTotal, string(DedupDispositionMatchedActiveCopy))
			deduplicator.consumeActiveLocked(active.Hash)
			matched = true
			continue
		}
		next = append(next, pending)
	}
	deduplicator.pending = next
	deduplicator.recordRecoveryEvidenceLocked(now)
	return outputs
}

func (deduplicator *ActivePassiveDeduplicator) releaseExpiredPending(now time.Time) []AdjudicatedPassiveEvent {
	deduplicator.mu.Lock()
	defer deduplicator.mu.Unlock()
	return deduplicator.releaseExpiredPendingLocked(now)
}

func (deduplicator *ActivePassiveDeduplicator) releaseExpiredPendingLocked(now time.Time) []AdjudicatedPassiveEvent {
	if len(deduplicator.pending) == 0 {
		return nil
	}

	outputs := make([]AdjudicatedPassiveEvent, 0)
	next := deduplicator.pending[:0]
	for _, pending := range deduplicator.pending {
		if pending.ReleaseAt.After(now) {
			next = append(next, pending)
			continue
		}
		outputs = append(outputs, deduplicator.releasePendingAsOutputLocked(pending)...)
		incrementExpvarMap(observeFirstDedupPendingFlushTotal, "grace_expiry")
	}
	deduplicator.pending = next
	return outputs
}

func (deduplicator *ActivePassiveDeduplicator) oldestReleasablePendingLocked(now time.Time) int {
	index := -1
	var oldest time.Time
	for i, pending := range deduplicator.pending {
		if pending.ReleaseAt.After(now) {
			continue
		}
		if index < 0 || pending.ReleaseAt.Before(oldest) {
			index = i
			oldest = pending.ReleaseAt
		}
	}
	return index
}

func (deduplicator *ActivePassiveDeduplicator) releasePendingIndexLocked(index int, now time.Time, reason string) []AdjudicatedPassiveEvent {
	if index < 0 || index >= len(deduplicator.pending) {
		return nil
	}
	pending := deduplicator.pending[index]
	copy(deduplicator.pending[index:], deduplicator.pending[index+1:])
	deduplicator.pending = deduplicator.pending[:len(deduplicator.pending)-1]
	incrementExpvarMap(observeFirstDedupPendingFlushTotal, reason)
	return deduplicator.releasePendingAsOutputLocked(pending)
}

func (deduplicator *ActivePassiveDeduplicator) releasePendingAsOutputLocked(pending pendingPassiveFingerprint) []AdjudicatedPassiveEvent {
	if deduplicator.degraded {
		incrementExpvarMap(observeFirstDedupAdjudicationsTotal, string(DedupDispositionObservabilityOnly))
		return []AdjudicatedPassiveEvent{adjudicateObservabilityOnly(pending.Event, pending.Fingerprint, deduplicator.currentEpoch)}
	}
	if !dedupFamilyPolicyAllowsRuntimeThirdParty(pending.Fingerprint.FamilyPolicy) {
		incrementExpvarMap(observeFirstDedupAdjudicationsTotal, string(DedupDispositionObservabilityOnly))
		return []AdjudicatedPassiveEvent{adjudicateObservabilityOnly(pending.Event, pending.Fingerprint, deduplicator.currentEpoch)}
	}
	incrementExpvarMap(observeFirstDedupAdjudicationsTotal, string(DedupDispositionUnmatchedThirdParty))
	return []AdjudicatedPassiveEvent{adjudicateUnmatchedThirdParty(pending.Event, pending.Fingerprint, deduplicator.currentEpoch)}
}

func (deduplicator *ActivePassiveDeduplicator) publishImmediateLocked(event PassiveClassifiedEvent, fingerprint PassiveTransactionFingerprint, disposition DedupDisposition) []AdjudicatedPassiveEvent {
	switch disposition {
	case DedupDispositionLocalParticipantIn:
		observeFirstDedupLocalParticipantInboundTotal.Add(1)
		incrementExpvarMap(observeFirstDedupAdjudicationsTotal, string(DedupDispositionLocalParticipantIn))
		return []AdjudicatedPassiveEvent{adjudicateLocalParticipantInbound(event, fingerprint, deduplicator.currentEpoch)}
	case DedupDispositionUnmatchedThirdParty:
		if dedupFamilyPolicyAllowsRuntimeThirdParty(fingerprint.FamilyPolicy) {
			incrementExpvarMap(observeFirstDedupAdjudicationsTotal, string(DedupDispositionUnmatchedThirdParty))
			return []AdjudicatedPassiveEvent{adjudicateUnmatchedThirdParty(event, fingerprint, deduplicator.currentEpoch)}
		}
		incrementExpvarMap(observeFirstDedupAdjudicationsTotal, string(DedupDispositionObservabilityOnly))
		return []AdjudicatedPassiveEvent{adjudicateObservabilityOnly(event, fingerprint, deduplicator.currentEpoch)}
	default:
		incrementExpvarMap(observeFirstDedupAdjudicationsTotal, string(DedupDispositionObservabilityOnly))
		return []AdjudicatedPassiveEvent{adjudicateObservabilityOnly(event, fingerprint, deduplicator.currentEpoch)}
	}
}

func adjudicateMatchedActiveDuplicate(event PassiveClassifiedEvent, fingerprint PassiveTransactionFingerprint, epoch uint64) AdjudicatedPassiveEvent {
	return AdjudicatedPassiveEvent{
		Event:                   event,
		Fingerprint:             fingerprint,
		FamilyPolicy:            fingerprint.FamilyPolicy,
		Disposition:             DedupDispositionMatchedActiveCopy,
		SuppressShadow:          true,
		SuppressWatchEfficiency: true,
		MatchedActiveDuplicate:  true,
		Epoch:                   epoch,
	}
}

func adjudicateUnmatchedThirdParty(event PassiveClassifiedEvent, fingerprint PassiveTransactionFingerprint, epoch uint64) AdjudicatedPassiveEvent {
	return AdjudicatedPassiveEvent{
		Event:              event,
		Fingerprint:        fingerprint,
		FamilyPolicy:       fingerprint.FamilyPolicy,
		Disposition:        DedupDispositionUnmatchedThirdParty,
		ThirdPartyEligible: true,
		SuppressShadow:     dedupFamilyPolicySuppressesShadow(fingerprint.FamilyPolicy),
		Epoch:              epoch,
	}
}

func adjudicateObservabilityOnly(event PassiveClassifiedEvent, fingerprint PassiveTransactionFingerprint, epoch uint64) AdjudicatedPassiveEvent {
	return AdjudicatedPassiveEvent{
		Event:             event,
		Fingerprint:       fingerprint,
		FamilyPolicy:      fingerprint.FamilyPolicy,
		Disposition:       DedupDispositionObservabilityOnly,
		ObservabilityOnly: true,
		Epoch:             epoch,
	}
}

func adjudicateLocalParticipantInbound(event PassiveClassifiedEvent, fingerprint PassiveTransactionFingerprint, epoch uint64) AdjudicatedPassiveEvent {
	return AdjudicatedPassiveEvent{
		Event:                   event,
		Fingerprint:             fingerprint,
		FamilyPolicy:            fingerprint.FamilyPolicy,
		Disposition:             DedupDispositionLocalParticipantIn,
		ObservabilityOnly:       true,
		LocalParticipantInbound: true,
		SuppressWatchEfficiency: true,
		Epoch:                   epoch,
	}
}

func makeAdjudicatedDiscontinuity(event PassiveClassifiedEvent, epoch uint64) AdjudicatedPassiveEvent {
	return AdjudicatedPassiveEvent{
		Event:             event,
		Disposition:       DedupDispositionDiscontinuity,
		ObservabilityOnly: true,
		Epoch:             epoch,
	}
}

func observeFirstWatchObservationForFrame(frame protocol.Frame, observer WatchObserver) WatchObservation {
	key, ok := PassiveWatchKeyFromFrame(frame)
	if !ok {
		return WatchObservation{State: WatchObservationStateCatalogMiss}
	}
	return observeFirstWatchObservationForKey(key, observer)
}

func observeFirstWatchObservationForKey(key WatchKey, observer WatchObserver) WatchObservation {
	if key == nil {
		return WatchObservation{State: WatchObservationStateCatalogMiss}
	}
	if observer == nil {
		return observeFirstDefaultWatchObservationForKey(key)
	}
	return observer.Observe(key)
}

func dedupFamilyPolicyAllowsRuntimeThirdParty(policy ObserveFirstFamilyPolicy) bool {
	if policy.UsesRuntimeExternalWritePolicy {
		return policy.EffectiveExternalWritePolicy != ObserveFirstExternalWritePolicyRecordOnly
	}
	if policy.CorrelationPolicy == WatchCorrelationPolicyRecordInvalidate {
		return true
	}
	switch policy.DirectApplyPolicy {
	case ObserveFirstDirectApplyPolicyStateDefault, ObserveFirstDirectApplyPolicyConfigOptIn, ObserveFirstDirectApplyPolicyEnergyMergeOnly:
		return true
	default:
		return false
	}
}

func observeFirstDefaultWatchObservationForKey(key WatchKey) WatchObservation {
	if fallback, ok := observeFirstDefaultB524Observation(key); ok {
		return fallback
	}
	return WatchObservation{State: WatchObservationStateCatalogMiss}
}

func observeFirstDefaultB524Observation(key WatchKey) (WatchObservation, bool) {
	b524, ok := asB524WatchKey(key)
	if !ok {
		return WatchObservation{}, false
	}
	if b524.Opcode != 0x02 && b524.Opcode != 0x06 {
		return WatchObservation{}, false
	}
	canonicalKey := NewB524WatchKey(
		b524.Target,
		b524.Opcode,
		b524.Group,
		b524.Instance,
		b524.RegisterAddress,
	)
	return WatchObservation{
		State: WatchObservationStateActive,
		Descriptor: WatchDescriptor{
			Key:               canonicalKey,
			SemanticClass:     WatchSemanticClassState,
			CorrelationPolicy: WatchCorrelationPolicyRequestResponse,
			DirectApplyPolicy: WatchDirectApplyPolicyStateDefault,
		},
		HasDescriptor: true,
	}, true
}

func asB524WatchKey(key WatchKey) (B524WatchKey, bool) {
	switch typed := key.(type) {
	case B524WatchKey:
		return typed, true
	case *B524WatchKey:
		if typed == nil {
			return B524WatchKey{}, false
		}
		return *typed, true
	default:
		return B524WatchKey{}, false
	}
}

func dedupFamilyPolicySuppressesShadow(policy ObserveFirstFamilyPolicy) bool {
	if !policy.UsesRuntimeExternalWritePolicy {
		return false
	}
	switch policy.EffectiveExternalWritePolicy {
	case ObserveFirstExternalWritePolicyInvalidateOnly, ObserveFirstExternalWritePolicyRecordAndInvalidate:
		return true
	default:
		return false
	}
}

func (deduplicator *ActivePassiveDeduplicator) makeDiscontinuityLocked(observedAt time.Time, reason PassiveDiscontinuityReason, degradeReason string) []AdjudicatedPassiveEvent {
	pendingCount := len(deduplicator.pending)
	if pendingCount > 0 {
		for range deduplicator.pending {
			incrementExpvarMap(observeFirstDedupPendingFlushTotal, "epoch_reset")
		}
	}
	deduplicator.pending = deduplicator.pending[:0]
	clear(deduplicator.active)
	deduplicator.currentEpoch++
	observeFirstDedupEpoch.Set(int64(deduplicator.currentEpoch))
	observeFirstDedupEpochResetTotal.Add(1)
	deduplicator.enterDegradedLocked(degradeReason)
	deduplicator.recovery = recoveryState{}
	deduplicator.updateExpvarsLocked()

	event := PassiveClassifiedEvent{
		Kind:                PassiveClassifiedEventDiscontinuity,
		ObservedAt:          observedAt,
		DiscontinuityReason: reason,
		Err:                 context.Canceled,
	}
	incrementExpvarMap(observeFirstDedupAdjudicationsTotal, string(DedupDispositionDiscontinuity))
	return []AdjudicatedPassiveEvent{makeAdjudicatedDiscontinuity(event, deduplicator.currentEpoch)}
}

func (deduplicator *ActivePassiveDeduplicator) handleCriticalReset(observedAt time.Time, reason PassiveDiscontinuityReason, degradeReason string) {
	outputs := func() []AdjudicatedPassiveEvent {
		deduplicator.mu.Lock()
		defer deduplicator.mu.Unlock()
		return deduplicator.makeDiscontinuityLocked(observedAt, reason, degradeReason)
	}()
	deduplicator.publishAll(outputs)
}

func (deduplicator *ActivePassiveDeduplicator) updateLocalAddressLocked(address byte, observedAt time.Time) bool {
	if address == 0 {
		return false
	}
	if !deduplicator.localAddr.Known {
		deduplicator.localAddr.Known = true
		deduplicator.localAddr.Address = address
		deduplicator.localAddr.Epoch = deduplicator.currentEpoch
		_ = observedAt
		return false
	}
	if deduplicator.localAddr.Address == address {
		return false
	}

	deduplicator.localAddr.Address = address
	deduplicator.localAddr.Epoch++
	_ = observedAt
	return true
}

func (deduplicator *ActivePassiveDeduplicator) pruneActiveLocked(now time.Time) {
	for key, active := range deduplicator.active {
		if active.ExpiresAt.After(now) {
			continue
		}
		delete(deduplicator.active, key)
	}
}

func (deduplicator *ActivePassiveDeduplicator) consumeActiveLocked(hash [32]byte) bool {
	active, ok := deduplicator.active[hash]
	if !ok || active.Count <= 0 {
		return false
	}
	if active.Count == 1 {
		delete(deduplicator.active, hash)
		return true
	}
	active.Count--
	deduplicator.active[hash] = active
	return true
}

func (deduplicator *ActivePassiveDeduplicator) enterDegradedLocked(reason string) {
	if !deduplicator.degraded {
		incrementExpvarMap(observeFirstDedupDegradedTransitionsTotal, reason)
	}
	deduplicator.degraded = true
	deduplicator.recovery = recoveryState{}
	deduplicator.updateExpvarsLocked()
}

func (deduplicator *ActivePassiveDeduplicator) recordRecoveryEvidenceLocked(now time.Time) {
	if !deduplicator.degraded {
		return
	}
	if deduplicator.recovery.StartedAt.IsZero() {
		deduplicator.recovery.StartedAt = now
	}
	deduplicator.recovery.PassiveEvidence++
	if now.Sub(deduplicator.recovery.StartedAt) < deduplicator.budgets.RecoveryHysteresis {
		return
	}
	if deduplicator.recovery.PassiveEvidence < deduplicator.budgets.RecoveryEventThreshold {
		return
	}
	deduplicator.degraded = false
	deduplicator.updateExpvarsLocked()
}

func (deduplicator *ActivePassiveDeduplicator) updateExpvarsLocked() {
	if deduplicator.degraded {
		observeFirstDedupState.Set("degraded")
	} else {
		observeFirstDedupState.Set("healthy")
	}
	observeFirstDedupEpoch.Set(int64(deduplicator.currentEpoch))
}

func (deduplicator *ActivePassiveDeduplicator) publishAll(events []AdjudicatedPassiveEvent) {
	for _, event := range events {
		deduplicator.publish(event)
	}
}

func (deduplicator *ActivePassiveDeduplicator) publish(event AdjudicatedPassiveEvent) {
	deduplicator.subscribersMu.Lock()
	subscribers := make([]*AdjudicatedPassiveSubscription, 0, len(deduplicator.subscribers))
	for _, subscription := range deduplicator.subscribers {
		subscribers = append(subscribers, subscription)
	}
	deduplicator.subscribersMu.Unlock()

	var criticalOverflow bool
	for _, subscription := range subscribers {
		if trySendAdjudicatedEvent(subscription.ch, event) {
			continue
		}
		if subscription.priority != DedupSubscriberCritical {
			continue
		}
		criticalOverflow = true
		deduplicator.unsubscribeWithFault(subscription, event.Event.ObservedAt, event.Epoch)
	}
	if criticalOverflow {
		deduplicator.handleCriticalReset(event.Event.ObservedAt, PassiveDiscontinuityCriticalSubscriberFault, "dedup_output_overflow")
	}
}

func trySendAdjudicatedEvent(ch chan AdjudicatedPassiveEvent, event AdjudicatedPassiveEvent) (sent bool) {
	defer func() {
		if recover() != nil {
			sent = false
		}
	}()

	select {
	case ch <- event:
		return true
	default:
		return false
	}
}

func (deduplicator *ActivePassiveDeduplicator) unsubscribe(subscription *AdjudicatedPassiveSubscription) {
	deduplicator.unsubscribeCore(subscription, nil)
}

func (deduplicator *ActivePassiveDeduplicator) unsubscribeWithFault(subscription *AdjudicatedPassiveSubscription, observedAt time.Time, epoch uint64) {
	deduplicator.unsubscribeCore(subscription, &AdjudicatedPassiveEvent{
		Event: PassiveClassifiedEvent{
			Kind:                PassiveClassifiedEventDiscontinuity,
			ObservedAt:          observedAt,
			DiscontinuityReason: PassiveDiscontinuityCriticalSubscriberFault,
			Subscriber:          subscription.name,
		},
		Disposition:       DedupDispositionDiscontinuity,
		ObservabilityOnly: true,
		Epoch:             epoch,
	})
}

func (deduplicator *ActivePassiveDeduplicator) unsubscribeCore(subscription *AdjudicatedPassiveSubscription, fault *AdjudicatedPassiveEvent) {
	if deduplicator == nil || subscription == nil {
		return
	}

	subscription.closeOnce.Do(func() {
		deduplicator.subscribersMu.Lock()
		if deduplicator.subscribers != nil {
			delete(deduplicator.subscribers, subscription.id)
		}
		deduplicator.subscribersMu.Unlock()

		if fault != nil {
			drainAdjudicatedSubscription(subscription.ch)
			_ = trySendAdjudicatedEvent(subscription.ch, *fault)
		}
		close(subscription.ch)
	})
}

func (deduplicator *ActivePassiveDeduplicator) closeAllSubscribers() {
	deduplicator.subscribersMu.Lock()
	subscribers := make([]*AdjudicatedPassiveSubscription, 0, len(deduplicator.subscribers))
	for _, subscription := range deduplicator.subscribers {
		subscribers = append(subscribers, subscription)
	}
	deduplicator.subscribers = nil
	deduplicator.subscribersMu.Unlock()

	for _, subscription := range subscribers {
		deduplicator.unsubscribe(subscription)
	}
}

func drainAdjudicatedSubscription(ch chan AdjudicatedPassiveEvent) {
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}

func incrementExpvarMap(counter *expvar.Map, key string) {
	if counter == nil || key == "" {
		return
	}
	counter.Add(key, 1)
}

func (deduplicator *ActivePassiveDeduplicator) now() time.Time {
	if deduplicator == nil || deduplicator.nowFunc == nil {
		return time.Now()
	}
	return deduplicator.nowFunc()
}

func maxDuration(left, right time.Duration) time.Duration {
	if left >= right {
		return left
	}
	return right
}

func maxInt(left, right int) int {
	if left >= right {
		return left
	}
	return right
}

type chainedBusObserver []protocol.BusObserver

func (chain chainedBusObserver) OnBusEvent(event protocol.BusEvent) error {
	for _, observer := range chain {
		if observer == nil {
			continue
		}
		if err := observer.OnBusEvent(event); err != nil {
			return err
		}
	}
	return nil
}

func ChainBusObservers(observers ...protocol.BusObserver) protocol.BusObserver {
	filtered := make(chainedBusObserver, 0, len(observers))
	for _, observer := range observers {
		if observer == nil {
			continue
		}
		if chain, ok := observer.(chainedBusObserver); ok {
			filtered = append(filtered, chain...)
			continue
		}
		filtered = append(filtered, observer)
	}
	if len(filtered) == 0 {
		return nil
	}
	if len(filtered) == 1 {
		return filtered[0]
	}
	return filtered
}
