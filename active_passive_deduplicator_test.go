package ebusgateway

import (
	"expvar"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
)

func TestActivePassiveDeduplicator_DefaultBudgetsStayFrozenToM1Envelope(t *testing.T) {
	cfg := DefaultConfig()

	deduplicator, err := NewActivePassiveDeduplicator(cfg)
	if err != nil {
		t.Fatalf("NewActivePassiveDeduplicator error = %v", err)
	}

	budgets := deduplicator.Budgets()
	if budgets.LogicalRequestEnvelope != 1250*time.Millisecond {
		t.Fatalf("LogicalRequestEnvelope = %s; want 1250ms", budgets.LogicalRequestEnvelope)
	}
	if budgets.ActivePublishBudget != 1500*time.Millisecond {
		t.Fatalf("ActivePublishBudget = %s; want 1500ms", budgets.ActivePublishBudget)
	}
	if budgets.PendingGraceTimeout != 1750*time.Millisecond {
		t.Fatalf("PendingGraceTimeout = %s; want 1750ms", budgets.PendingGraceTimeout)
	}
	if budgets.ActiveFingerprintRetention != 2250*time.Millisecond {
		t.Fatalf("ActiveFingerprintRetention = %s; want 2250ms", budgets.ActiveFingerprintRetention)
	}
	if budgets.PendingCapacity != 256 {
		t.Fatalf("PendingCapacity = %d; want 256", budgets.PendingCapacity)
	}
}

func TestActivePassiveDeduplicator_MatchesPendingPassiveFirstArrival(t *testing.T) {
	deduplicator := newTestDeduplicator(t)
	subscription, err := deduplicator.Subscribe("test", DedupSubscriberCritical, 16)
	if err != nil {
		t.Fatalf("Subscribe error = %v", err)
	}
	defer subscription.Close()

	base := time.Unix(0, 0)
	deduplicator.nowFunc = func() time.Time { return base }

	request, response := directTransactionPair()
	setKnownLocalAddress(deduplicator, request.Source)
	deduplicator.OnPassiveClassifiedEvent(passiveTransactionEvent(base, request, response))

	select {
	case event := <-subscription.Events():
		t.Fatalf("unexpected early adjudicated event: %+v", event)
	default:
	}

	deduplicator.nowFunc = func() time.Time { return base.Add(500 * time.Millisecond) }
	if err := deduplicator.OnBusEvent(activeAttemptEvent(request, response)); err != nil {
		t.Fatalf("OnBusEvent error = %v", err)
	}

	event := requireAdjudicatedEvent(t, subscription, DedupDispositionMatchedActiveCopy)
	if !event.MatchedActiveDuplicate {
		t.Fatal("MatchedActiveDuplicate = false; want true")
	}
	if !event.SuppressShadow || !event.SuppressWatchEfficiency {
		t.Fatalf("suppression flags = shadow:%t watch:%t; want both true", event.SuppressShadow, event.SuppressWatchEfficiency)
	}
	if event.ThirdPartyEligible {
		t.Fatal("ThirdPartyEligible = true; want false for matched duplicate")
	}
}

func TestActivePassiveDeduplicator_MatchesDelayedPassiveWithinRetention(t *testing.T) {
	deduplicator := newTestDeduplicator(t)
	subscription, err := deduplicator.Subscribe("test", DedupSubscriberCritical, 16)
	if err != nil {
		t.Fatalf("Subscribe error = %v", err)
	}
	defer subscription.Close()

	base := time.Unix(0, 0)
	deduplicator.nowFunc = func() time.Time { return base }

	request, response := directTransactionPair()
	setKnownLocalAddress(deduplicator, request.Source)
	if err := deduplicator.OnBusEvent(activeAttemptEvent(request, response)); err != nil {
		t.Fatalf("OnBusEvent error = %v", err)
	}

	deduplicator.nowFunc = func() time.Time { return base.Add(2 * time.Second) }
	deduplicator.OnPassiveClassifiedEvent(passiveTransactionEvent(base.Add(2*time.Second), request, response))

	event := requireAdjudicatedEvent(t, subscription, DedupDispositionMatchedActiveCopy)
	if !event.MatchedActiveDuplicate {
		t.Fatal("MatchedActiveDuplicate = false; want true")
	}
	if got, want := event.Fingerprint.Source, request.Source; got != want {
		t.Fatalf("fingerprint source = 0x%02x; want 0x%02x", got, want)
	}
}

func TestActivePassiveDeduplicator_PassiveFingerprintCarriesSharedWatchKey(t *testing.T) {
	deduplicator := newTestDeduplicator(t)
	subscription, err := deduplicator.Subscribe("test", DedupSubscriberCritical, 16)
	if err != nil {
		t.Fatalf("Subscribe error = %v", err)
	}
	defer subscription.Close()

	forceHealthyDedup(deduplicator)

	base := time.Unix(0, 0)
	deduplicator.nowFunc = func() time.Time { return base }

	request := protocol.Frame{
		Source:    0x31,
		Target:    0x15,
		Primary:   0xB5,
		Secondary: 0x24,
		Data:      []byte{0x06, 0x00, 0x03, 0x01, 0x1C, 0x00},
	}
	response := protocol.Frame{
		Source:    request.Target,
		Target:    request.Source,
		Primary:   request.Primary,
		Secondary: request.Secondary,
		Data:      []byte{0x11, 0x22},
	}

	deduplicator.OnPassiveClassifiedEvent(passiveTransactionEvent(base, request, response))
	assertNoAdjudicatedEvent(t, subscription, 25*time.Millisecond)

	deduplicator.nowFunc = func() time.Time { return base.Add(deduplicator.budgets.PendingGraceTimeout + time.Millisecond) }
	deduplicator.publishAll(deduplicator.releaseExpiredPending(deduplicator.now()))

	event := requireAdjudicatedEvent(t, subscription, DedupDispositionUnmatchedThirdParty)
	if event.Fingerprint.SharedWatchKey == nil {
		t.Fatal("SharedWatchKey = nil; want parsed passive watch key")
	}
	want := NewB524WatchKey(0x15, 0x06, 0x03, 0x01, 0x001C).Canonical()
	if got := event.Fingerprint.SharedWatchKey.Canonical(); got != want {
		t.Fatalf("SharedWatchKey.Canonical() = %q; want %q", got, want)
	}
}

func TestActivePassiveDeduplicator_PassiveFingerprintCarriesSharedB509WatchKeyForPassiveOpcode(t *testing.T) {
	deduplicator := newTestDeduplicator(t)
	subscription, err := deduplicator.Subscribe("test", DedupSubscriberCritical, 16)
	if err != nil {
		t.Fatalf("Subscribe error = %v", err)
	}
	defer subscription.Close()

	forceHealthyDedup(deduplicator)

	base := time.Unix(0, 0)
	deduplicator.nowFunc = func() time.Time { return base }

	request := protocol.Frame{
		Source:    0x10,
		Target:    0x08,
		Primary:   0xB5,
		Secondary: 0x09,
		Data:      []byte{0x29, 0x02, 0x00},
	}
	response := protocol.Frame{
		Source:    request.Target,
		Target:    request.Source,
		Primary:   request.Primary,
		Secondary: request.Secondary,
		Data:      []byte{0x29, 0x02, 0x00, 0x01},
	}

	deduplicator.OnPassiveClassifiedEvent(passiveTransactionEvent(base, request, response))
	assertNoAdjudicatedEvent(t, subscription, 25*time.Millisecond)

	deduplicator.nowFunc = func() time.Time { return base.Add(deduplicator.budgets.PendingGraceTimeout + time.Millisecond) }
	deduplicator.publishAll(deduplicator.releaseExpiredPending(deduplicator.now()))

	event := requireAdjudicatedEvent(t, subscription, DedupDispositionUnmatchedThirdParty)
	if event.Fingerprint.SharedWatchKey == nil {
		t.Fatal("SharedWatchKey = nil; want parsed passive watch key")
	}
	want := NewB509WatchKey(0x08, 0x0200).Canonical()
	if got := event.Fingerprint.SharedWatchKey.Canonical(); got != want {
		t.Fatalf("SharedWatchKey.Canonical() = %q; want %q", got, want)
	}
}

func TestActivePassiveDeduplicator_ConsumesActiveFingerprintAfterSingleMatch(t *testing.T) {
	deduplicator := newTestDeduplicator(t)
	subscription, err := deduplicator.Subscribe("test", DedupSubscriberCritical, 16)
	if err != nil {
		t.Fatalf("Subscribe error = %v", err)
	}
	defer subscription.Close()

	forceHealthyDedup(deduplicator)

	base := time.Unix(0, 0)
	deduplicator.nowFunc = func() time.Time { return base }

	request, response := directTransactionPair()
	setKnownLocalAddress(deduplicator, request.Source)
	if err := deduplicator.OnBusEvent(activeAttemptEvent(request, response)); err != nil {
		t.Fatalf("OnBusEvent error = %v", err)
	}

	deduplicator.OnPassiveClassifiedEvent(passiveTransactionEvent(base.Add(10*time.Millisecond), request, response))
	requireAdjudicatedEvent(t, subscription, DedupDispositionMatchedActiveCopy)

	deduplicator.OnPassiveClassifiedEvent(passiveTransactionEvent(base.Add(20*time.Millisecond), request, response))
	assertNoAdjudicatedEvent(t, subscription, 25*time.Millisecond)

	deduplicator.nowFunc = func() time.Time { return base.Add(deduplicator.budgets.PendingGraceTimeout + time.Second) }
	deduplicator.publishAll(deduplicator.releaseExpiredPending(deduplicator.now()))

	event := requireAdjudicatedEvent(t, subscription, DedupDispositionUnmatchedThirdParty)
	if !event.ThirdPartyEligible {
		t.Fatal("ThirdPartyEligible = false; want true")
	}
}

func TestActivePassiveDeduplicator_OneActiveFingerprintMatchesOnlyOnePendingEntry(t *testing.T) {
	deduplicator := newTestDeduplicator(t)
	subscription, err := deduplicator.Subscribe("test", DedupSubscriberCritical, 16)
	if err != nil {
		t.Fatalf("Subscribe error = %v", err)
	}
	defer subscription.Close()

	forceHealthyDedup(deduplicator)

	base := time.Unix(0, 0)
	deduplicator.nowFunc = func() time.Time { return base }

	request, response := directTransactionPair()
	setKnownLocalAddress(deduplicator, request.Source)
	deduplicator.OnPassiveClassifiedEvent(passiveTransactionEvent(base, request, response))
	deduplicator.OnPassiveClassifiedEvent(passiveTransactionEvent(base.Add(10*time.Millisecond), request, response))
	assertNoAdjudicatedEvent(t, subscription, 25*time.Millisecond)

	deduplicator.nowFunc = func() time.Time { return base.Add(500 * time.Millisecond) }
	if err := deduplicator.OnBusEvent(activeAttemptEvent(request, response)); err != nil {
		t.Fatalf("OnBusEvent error = %v", err)
	}

	requireAdjudicatedEvent(t, subscription, DedupDispositionMatchedActiveCopy)
	assertNoAdjudicatedEvent(t, subscription, 25*time.Millisecond)

	deduplicator.nowFunc = func() time.Time { return base.Add(deduplicator.budgets.PendingGraceTimeout + time.Second) }
	deduplicator.publishAll(deduplicator.releaseExpiredPending(deduplicator.now()))

	event := requireAdjudicatedEvent(t, subscription, DedupDispositionUnmatchedThirdParty)
	if !event.ThirdPartyEligible {
		t.Fatal("ThirdPartyEligible = false; want true")
	}
}

func TestActivePassiveDeduplicator_ObserverFaultKeepsPendingReleaseObservabilityOnly(t *testing.T) {
	deduplicator := newTestDeduplicator(t)
	subscription, err := deduplicator.Subscribe("test", DedupSubscriberCritical, 16)
	if err != nil {
		t.Fatalf("Subscribe error = %v", err)
	}
	defer subscription.Close()

	forceHealthyDedup(deduplicator)

	base := time.Unix(0, 0)
	deduplicator.nowFunc = func() time.Time { return base }

	request, response := directTransactionPair()
	setKnownLocalAddress(deduplicator, request.Source)
	deduplicator.OnPassiveClassifiedEvent(passiveTransactionEvent(base, request, response))

	deduplicator.nowFunc = func() time.Time { return base.Add(100 * time.Millisecond) }
	if err := deduplicator.OnBusEvent(protocol.BusEvent{
		Kind:    protocol.BusEventObserverFault,
		Outcome: protocol.BusOutcomeObserverFault,
	}); err != nil {
		t.Fatalf("observer fault event error = %v", err)
	}

	deduplicator.nowFunc = func() time.Time { return base.Add(deduplicator.budgets.PendingGraceTimeout + time.Millisecond) }
	deduplicator.publishAll(deduplicator.releaseExpiredPending(deduplicator.now()))

	event := requireAdjudicatedEvent(t, subscription, DedupDispositionObservabilityOnly)
	if !event.ObservabilityOnly {
		t.Fatal("ObservabilityOnly = false; want true")
	}
	if event.ThirdPartyEligible {
		t.Fatal("ThirdPartyEligible = true; want false while degraded")
	}
}

func TestActivePassiveDeduplicator_LocalParticipantInboundUsesRuntimeAddress(t *testing.T) {
	deduplicator := newTestDeduplicator(t)
	subscription, err := deduplicator.Subscribe("test", DedupSubscriberCritical, 16)
	if err != nil {
		t.Fatalf("Subscribe error = %v", err)
	}
	defer subscription.Close()

	base := time.Unix(0, 0)
	deduplicator.nowFunc = func() time.Time { return base }

	request, response := directTransactionPair()
	if err := deduplicator.OnBusEvent(activeAttemptEvent(request, response)); err != nil {
		t.Fatalf("OnBusEvent error = %v", err)
	}

	masterFrame := protocol.Frame{
		Source:    0x10,
		Target:    request.Source,
		Primary:   0x01,
		Secondary: 0x02,
		Data:      []byte{0x03},
	}
	deduplicator.OnPassiveClassifiedEvent(PassiveClassifiedEvent{
		Kind:       PassiveClassifiedEventMasterFrame,
		FrameType:  protocol.FrameTypeInitiatorInitiator,
		Request:    masterFrame,
		HasRequest: true,
		ObservedAt: base.Add(100 * time.Millisecond),
	})

	event := requireAdjudicatedEvent(t, subscription, DedupDispositionLocalParticipantIn)
	if !event.LocalParticipantInbound {
		t.Fatal("LocalParticipantInbound = false; want true")
	}
	if !event.SuppressWatchEfficiency {
		t.Fatal("SuppressWatchEfficiency = false; want true")
	}
	if event.ThirdPartyEligible {
		t.Fatal("ThirdPartyEligible = true; want false")
	}
}

func TestActivePassiveDeduplicator_InitialLocalAddressLearnDoesNotResetEpoch(t *testing.T) {
	deduplicator := newTestDeduplicator(t)
	subscription, err := deduplicator.Subscribe("test", DedupSubscriberCritical, 16)
	if err != nil {
		t.Fatalf("Subscribe error = %v", err)
	}
	defer subscription.Close()

	forceHealthyDedup(deduplicator)

	base := time.Unix(0, 0)
	deduplicator.nowFunc = func() time.Time { return base }
	request, response := directTransactionPair()

	beforeEpoch := deduplicator.currentEpoch
	if err := deduplicator.OnBusEvent(activeAttemptEvent(request, response)); err != nil {
		t.Fatalf("OnBusEvent error = %v", err)
	}

	assertNoAdjudicatedEvent(t, subscription, 25*time.Millisecond)
	if deduplicator.currentEpoch != beforeEpoch {
		t.Fatalf("currentEpoch = %d; want %d", deduplicator.currentEpoch, beforeEpoch)
	}
	if !deduplicator.localAddr.Known || deduplicator.localAddr.Address != request.Source {
		t.Fatalf("localAddr = %+v; want known source 0x%02x", deduplicator.localAddr, request.Source)
	}
}

func TestActivePassiveDeduplicator_DegradedSkipsActiveFingerprintSuppression(t *testing.T) {
	deduplicator := newTestDeduplicator(t)
	subscription, err := deduplicator.Subscribe("test", DedupSubscriberCritical, 16)
	if err != nil {
		t.Fatalf("Subscribe error = %v", err)
	}
	defer subscription.Close()

	forceHealthyDedup(deduplicator)

	base := time.Unix(0, 0)
	deduplicator.nowFunc = func() time.Time { return base }
	request, response := directTransactionPair()
	setKnownLocalAddress(deduplicator, request.Source)

	if err := deduplicator.OnBusEvent(activeAttemptEvent(request, response)); err != nil {
		t.Fatalf("OnBusEvent error = %v", err)
	}
	assertNoAdjudicatedEvent(t, subscription, 25*time.Millisecond)

	deduplicator.nowFunc = func() time.Time { return base.Add(100 * time.Millisecond) }
	if err := deduplicator.OnBusEvent(protocol.BusEvent{
		Kind:    protocol.BusEventObserverFault,
		Outcome: protocol.BusOutcomeObserverFault,
	}); err != nil {
		t.Fatalf("observer fault event error = %v", err)
	}

	deduplicator.nowFunc = func() time.Time { return base.Add(200 * time.Millisecond) }
	deduplicator.OnPassiveClassifiedEvent(passiveTransactionEvent(base.Add(200*time.Millisecond), request, response))

	deduplicator.nowFunc = func() time.Time { return base.Add(deduplicator.budgets.PendingGraceTimeout + time.Second) }
	deduplicator.publishAll(deduplicator.releaseExpiredPending(deduplicator.now()))

	event := requireAdjudicatedEvent(t, subscription, DedupDispositionObservabilityOnly)
	if !event.ObservabilityOnly {
		t.Fatal("ObservabilityOnly = false; want true")
	}
	if event.MatchedActiveDuplicate {
		t.Fatal("MatchedActiveDuplicate = true; want false while degraded")
	}
	if len(deduplicator.active) != 0 {
		t.Fatalf("active fingerprints = %d; want 0 after observer fault", len(deduplicator.active))
	}
}

func TestActivePassiveDeduplicator_DiscontinuityResetsEpochAndFlushesPending(t *testing.T) {
	deduplicator := newTestDeduplicator(t)
	subscription, err := deduplicator.Subscribe("test", DedupSubscriberCritical, 16)
	if err != nil {
		t.Fatalf("Subscribe error = %v", err)
	}
	defer subscription.Close()

	forceHealthyDedup(deduplicator)
	base := time.Unix(0, 0)
	deduplicator.nowFunc = func() time.Time { return base }
	request, response := directTransactionPair()
	setKnownLocalAddress(deduplicator, request.Source)

	deduplicator.OnPassiveClassifiedEvent(passiveTransactionEvent(base, request, response))

	beforeEpochResets := expvarIntValue(observeFirstDedupEpochResetTotal)
	beforePendingEpochFlush := expvarMapInt64(observeFirstDedupPendingFlushTotal, "epoch_reset")

	deduplicator.OnPassiveClassifiedEvent(PassiveClassifiedEvent{
		Kind:                PassiveClassifiedEventDiscontinuity,
		ObservedAt:          base.Add(100 * time.Millisecond),
		DiscontinuityReason: PassiveDiscontinuityTransportReset,
	})

	discontinuity := requireAdjudicatedEvent(t, subscription, DedupDispositionDiscontinuity)
	if discontinuity.Event.DiscontinuityReason != PassiveDiscontinuityTransportReset {
		t.Fatalf("DiscontinuityReason = %q; want %q", discontinuity.Event.DiscontinuityReason, PassiveDiscontinuityTransportReset)
	}
	assertNoAdjudicatedEvent(t, subscription, 25*time.Millisecond)
	if got := expvarIntValue(observeFirstDedupEpochResetTotal); got != beforeEpochResets+1 {
		t.Fatalf("epoch reset total = %d; want %d", got, beforeEpochResets+1)
	}
	if got := expvarMapInt64(observeFirstDedupPendingFlushTotal, "epoch_reset"); got != beforePendingEpochFlush+1 {
		t.Fatalf("pending epoch_reset flush total = %d; want %d", got, beforePendingEpochFlush+1)
	}
	if len(deduplicator.pending) != 0 {
		t.Fatalf("pending entries = %d; want 0 after epoch reset", len(deduplicator.pending))
	}
}

func TestActivePassiveDeduplicator_CriticalSubscriberOverflowAdvancesEpoch(t *testing.T) {
	deduplicator := newTestDeduplicator(t)
	subscription, err := deduplicator.Subscribe("overflow", DedupSubscriberCritical, 1)
	if err != nil {
		t.Fatalf("Subscribe error = %v", err)
	}
	defer subscription.Close()

	forceHealthyDedup(deduplicator)
	request, _ := directTransactionPair()
	setKnownLocalAddress(deduplicator, request.Source)

	base := time.Unix(0, 0)
	masterFrame := PassiveClassifiedEvent{
		Kind:       PassiveClassifiedEventMasterFrame,
		FrameType:  protocol.FrameTypeInitiatorInitiator,
		Request:    protocol.Frame{Source: 0x10, Target: request.Source, Primary: 0x01, Secondary: 0x02, Data: []byte{0x03}},
		HasRequest: true,
		ObservedAt: base,
	}
	beforeEpochResets := expvarIntValue(observeFirstDedupEpochResetTotal)

	deduplicator.OnPassiveClassifiedEvent(masterFrame)
	deduplicator.OnPassiveClassifiedEvent(PassiveClassifiedEvent{
		Kind:       masterFrame.Kind,
		FrameType:  masterFrame.FrameType,
		Request:    masterFrame.Request,
		HasRequest: true,
		ObservedAt: base.Add(10 * time.Millisecond),
	})

	fault := requireAdjudicatedEvent(t, subscription, DedupDispositionDiscontinuity)
	if fault.Event.DiscontinuityReason != PassiveDiscontinuityCriticalSubscriberFault {
		t.Fatalf("DiscontinuityReason = %q; want %q", fault.Event.DiscontinuityReason, PassiveDiscontinuityCriticalSubscriberFault)
	}
	if got := expvarIntValue(observeFirstDedupEpochResetTotal); got != beforeEpochResets+1 {
		t.Fatalf("epoch reset total = %d; want %d", got, beforeEpochResets+1)
	}
}

func TestChainBusObserversCallsAllObservers(t *testing.T) {
	first := &countingObserver{}
	second := &countingObserver{}
	observer := ChainBusObservers(first, second)
	if observer == nil {
		t.Fatal("ChainBusObservers returned nil")
	}

	err := observer.OnBusEvent(protocol.BusEvent{Kind: protocol.BusEventAttemptComplete})
	if err != nil {
		t.Fatalf("OnBusEvent error = %v", err)
	}
	if first.calls != 1 || second.calls != 1 {
		t.Fatalf("observer calls = (%d, %d); want (1, 1)", first.calls, second.calls)
	}
}

func newTestDeduplicator(t *testing.T) *ActivePassiveDeduplicator {
	t.Helper()

	deduplicator, err := NewActivePassiveDeduplicator(DefaultConfig())
	if err != nil {
		t.Fatalf("NewActivePassiveDeduplicator error = %v", err)
	}
	return deduplicator
}

func forceHealthyDedup(deduplicator *ActivePassiveDeduplicator) {
	deduplicator.mu.Lock()
	defer deduplicator.mu.Unlock()
	deduplicator.degraded = false
	deduplicator.recovery = recoveryState{}
	deduplicator.updateExpvarsLocked()
}

func setKnownLocalAddress(deduplicator *ActivePassiveDeduplicator, address byte) {
	deduplicator.mu.Lock()
	defer deduplicator.mu.Unlock()
	deduplicator.localAddr = LocalAddressSnapshot{
		Address: address,
		Known:   true,
		Epoch:   deduplicator.currentEpoch,
	}
}

func directTransactionPair() (protocol.Frame, protocol.Frame) {
	request := protocol.Frame{
		Source:    0x31,
		Target:    0x08,
		Primary:   0xB5,
		Secondary: 0x09,
		Data:      []byte{0x01},
	}
	response := protocol.Frame{
		Source:    request.Target,
		Target:    request.Source,
		Primary:   request.Primary,
		Secondary: request.Secondary,
		Data:      []byte{0x11, 0x22},
	}
	return request, response
}

func passiveTransactionEvent(observedAt time.Time, request, response protocol.Frame) PassiveClassifiedEvent {
	return PassiveClassifiedEvent{
		Kind:        PassiveClassifiedEventTransaction,
		FrameType:   protocol.FrameTypeInitiatorTarget,
		Request:     request,
		Response:    response,
		HasRequest:  true,
		HasResponse: true,
		ObservedAt:  observedAt,
	}
}

func activeAttemptEvent(request, response protocol.Frame) protocol.BusEvent {
	return protocol.BusEvent{
		Kind:        protocol.BusEventAttemptComplete,
		FrameType:   request.Type(),
		Outcome:     protocol.BusOutcomeSuccess,
		Request:     request,
		Response:    response,
		HasRequest:  true,
		HasResponse: true,
	}
}

func requireAdjudicatedEvent(t *testing.T, subscription *AdjudicatedPassiveSubscription, disposition DedupDisposition) AdjudicatedPassiveEvent {
	t.Helper()

	timeout := time.NewTimer(time.Second)
	defer timeout.Stop()

	for {
		select {
		case event, ok := <-subscription.Events():
			if !ok {
				t.Fatal("subscription closed before adjudicated event arrived")
			}
			if event.Disposition == disposition {
				return event
			}
		case <-timeout.C:
			t.Fatalf("timeout waiting for adjudicated disposition %q", disposition)
		}
	}
}

func assertNoAdjudicatedEvent(t *testing.T, subscription *AdjudicatedPassiveSubscription, wait time.Duration) {
	t.Helper()

	timer := time.NewTimer(wait)
	defer timer.Stop()

	select {
	case event, ok := <-subscription.Events():
		if !ok {
			return
		}
		t.Fatalf("unexpected adjudicated event: %+v", event)
	case <-timer.C:
	}
}

func expvarIntValue(value *expvar.Int) int64 {
	if value == nil {
		return 0
	}
	return value.Value()
}

func expvarMapInt64(value *expvar.Map, key string) int64 {
	if value == nil {
		return 0
	}
	variable := value.Get(key)
	if variable == nil {
		return 0
	}
	counter, ok := variable.(*expvar.Int)
	if !ok {
		return 0
	}
	return counter.Value()
}

type countingObserver struct {
	calls int
}

func (observer *countingObserver) OnBusEvent(event protocol.BusEvent) error {
	observer.calls++
	return nil
}
