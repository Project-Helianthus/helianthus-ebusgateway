package main

import (
	"context"
	"slices"
	"testing"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
)

func TestHandleAdjudicatedPassiveEvent_UnmatchedValueBearingReadWritesShadow(t *testing.T) {
	t.Parallel()

	key := ebusgateway.NewB509WatchKey(0x08, 0x0200)
	poller := newVaillantSemanticPoller(
		observeFirstStateShadowRuntimeConfig(ebusgateway.ObserveFirstExternalWritePolicyRecordOnly),
		&ebusgateway.Gateway{},
		graphql.NewLiveSemanticProvider(),
		nil,
		nil,
	)

	observedAt := time.Now()
	event := ebusgateway.AdjudicatedPassiveEvent{
		Event: ebusgateway.PassiveClassifiedEvent{
			HasRequest:  true,
			HasResponse: true,
			Request: protocol.Frame{
				Source:    0x10,
				Target:    0x08,
				Primary:   vaillantB509Primary,
				Secondary: vaillantB509Secondary,
				Data:      buildB509ReadSelector(0x0200),
			},
			Response: protocol.Frame{
				Source:    0x08,
				Target:    0x10,
				Primary:   vaillantB509Primary,
				Secondary: vaillantB509Secondary,
				Data:      []byte{vaillantB509OpcodeRead, 0x02, 0x00, 0xAA, 0x55},
			},
		},
		Fingerprint: ebusgateway.PassiveTransactionFingerprint{
			SharedWatchKey: key,
			ObservedAt:     observedAt,
			ResponseClass:  ebusgateway.DedupResponseValueBearing,
			FamilyPolicy: ebusgateway.ObserveFirstFamilyPolicy{
				RequestIntent:     ebusgateway.ObserveFirstRequestIntentRead,
				ResponseClass:     ebusgateway.DedupResponseValueBearing,
				DirectApplyPolicy: ebusgateway.ObserveFirstDirectApplyPolicyStateDefault,
			},
		},
		Disposition: ebusgateway.DedupDispositionUnmatchedThirdParty,
	}

	poller.handleAdjudicatedPassiveEvent(event)

	lookup := poller.shadow.Lookup(key, 10*time.Second)
	if !lookup.Found {
		t.Fatal("shadow lookup found = false; want passive value-bearing read in shadow")
	}
	if !lookup.Eligible {
		t.Fatalf("shadow lookup eligible = false; want true for freshly written passive read (state=%s)", lookup.Entry.State)
	}
	if !slices.Equal(lookup.Entry.Value, []byte{0xAA, 0x55}) {
		t.Fatalf("shadow value = %x; want %x", lookup.Entry.Value, []byte{0xAA, 0x55})
	}
	if lookup.Entry.ObservedAt.IsZero() || !lookup.Entry.ObservedAt.Equal(observedAt) {
		t.Fatalf("shadow observed_at = %s; want %s", lookup.Entry.ObservedAt.UTC().Format(time.RFC3339), observedAt.UTC().Format(time.RFC3339))
	}
	if lookup.Entry.Pinned {
		t.Fatal("shadow entry pinned = true; want passive fallback bootstrap to remain unpinned")
	}
	if summary := poller.shadow.Summary(); summary.StaticPinnedFootprint != 0 {
		t.Fatalf("Summary().StaticPinnedFootprint = %d; want 0 for passive fallback bootstrap", summary.StaticPinnedFootprint)
	}
}

func TestHandleAdjudicatedPassiveEvent_UnmatchedValueBearingB524ReadWritesShadow(t *testing.T) {
	t.Parallel()

	key := ebusgateway.NewB524WatchKey(0x15, 0x02, 0x03, 0x01, 0x001C)
	poller := newVaillantSemanticPoller(
		observeFirstStateShadowRuntimeConfig(ebusgateway.ObserveFirstExternalWritePolicyRecordOnly),
		&ebusgateway.Gateway{},
		graphql.NewLiveSemanticProvider(),
		nil,
		nil,
	)

	observedAt := time.Now()
	event := ebusgateway.AdjudicatedPassiveEvent{
		Event: ebusgateway.PassiveClassifiedEvent{
			HasRequest:  true,
			HasResponse: true,
			Request: protocol.Frame{
				Source:    0x10,
				Target:    0x15,
				Primary:   vaillantExtRegisterPrimary,
				Secondary: vaillantExtRegisterSecondary,
				Data:      buildB524ReadSelector(0x02, 0x03, 0x01, 0x001C),
			},
			Response: protocol.Frame{
				Source:    0x15,
				Target:    0x10,
				Primary:   vaillantExtRegisterPrimary,
				Secondary: vaillantExtRegisterSecondary,
				Data:      []byte{0x02, 0x01, 0x03, 0x1C, 0x00, 0x42, 0x01},
			},
		},
		Fingerprint: ebusgateway.PassiveTransactionFingerprint{
			SharedWatchKey: key,
			ObservedAt:     observedAt,
			ResponseClass:  ebusgateway.DedupResponseValueBearing,
			FamilyPolicy: ebusgateway.ObserveFirstFamilyPolicy{
				RequestIntent:     ebusgateway.ObserveFirstRequestIntentRead,
				ResponseClass:     ebusgateway.DedupResponseValueBearing,
				DirectApplyPolicy: ebusgateway.ObserveFirstDirectApplyPolicyStateDefault,
			},
		},
		Disposition: ebusgateway.DedupDispositionUnmatchedThirdParty,
	}

	poller.handleAdjudicatedPassiveEvent(event)

	lookup := poller.shadow.Lookup(key, 10*time.Second)
	if !lookup.Found {
		t.Fatal("shadow lookup found = false; want passive B524 value-bearing read in shadow")
	}
	if !lookup.Eligible {
		t.Fatalf("shadow lookup eligible = false; want true for freshly written passive B524 read (state=%s)", lookup.Entry.State)
	}
	if !slices.Equal(lookup.Entry.Value, []byte{0x42, 0x01}) {
		t.Fatalf("shadow value = %x; want %x", lookup.Entry.Value, []byte{0x42, 0x01})
	}
}

func TestPassiveShadowLaneEnabled_EnergyMergeOnlyPolicyDisabled(t *testing.T) {
	t.Parallel()

	flags := ebusgateway.NormalizeObserveFirstFeatureFlags(
		true,
		true,
		true,
		ebusgateway.ObserveFirstExternalWritePolicyRecordOnly,
	)
	policy := ebusgateway.ObserveFirstFamilyPolicy{
		RequestIntent:     ebusgateway.ObserveFirstRequestIntentRead,
		DirectApplyPolicy: ebusgateway.ObserveFirstDirectApplyPolicyEnergyMergeOnly,
	}

	if passiveShadowLaneEnabled(flags, policy) {
		t.Fatal("passiveShadowLaneEnabled() = true; want false for energy_merge_only carve-out")
	}
}

func TestHandleAdjudicatedPassiveEvent_UnmatchedExternalWriteInvalidatesShadow(t *testing.T) {
	t.Parallel()

	key := ebusgateway.NewB509WatchKey(0x08, 0x0200)
	poller := newVaillantSemanticPoller(
		observeFirstStateShadowRuntimeConfig(ebusgateway.ObserveFirstExternalWritePolicyInvalidateOnly),
		&ebusgateway.Gateway{},
		graphql.NewLiveSemanticProvider(),
		nil,
		nil,
	)

	_ = poller.prepareSemanticReadWatch(key)
	seedAt := time.Unix(100, 0)
	seed := poller.shadow.Write(ebusgateway.ShadowWrite{
		Key:        key,
		Source:     ebusgateway.ShadowWriteSourcePassive,
		Confidence: ebusgateway.ShadowConfidenceHigh,
		Value:      []byte{0x11, 0x22},
		ObservedAt: seedAt,
	})
	if !seed.Accepted {
		t.Fatalf("seed shadow write rejected: %s", seed.Reason)
	}

	invalidatedAt := seedAt.Add(5 * time.Second)
	event := ebusgateway.AdjudicatedPassiveEvent{
		Event: ebusgateway.PassiveClassifiedEvent{
			HasRequest: true,
			Request: protocol.Frame{
				Source:    0x10,
				Target:    0x08,
				Primary:   vaillantB509Primary,
				Secondary: vaillantB509Secondary,
				Data:      buildB509WriteSelector(0x0200, []byte{0x33, 0x44}),
			},
		},
		Fingerprint: ebusgateway.PassiveTransactionFingerprint{
			SharedWatchKey: key,
			ObservedAt:     invalidatedAt,
			FamilyPolicy: ebusgateway.ObserveFirstFamilyPolicy{
				RequestIntent:                  ebusgateway.ObserveFirstRequestIntentWrite,
				UsesRuntimeExternalWritePolicy: true,
				EffectiveExternalWritePolicy:   ebusgateway.ObserveFirstExternalWritePolicyInvalidateOnly,
			},
		},
		Disposition: ebusgateway.DedupDispositionUnmatchedThirdParty,
	}

	poller.handleAdjudicatedPassiveEvent(event)

	lookup := poller.shadow.Lookup(key, 10*time.Second)
	if !lookup.Found {
		t.Fatal("shadow lookup found = false; want invalidated entry preserved")
	}
	if lookup.Eligible {
		t.Fatalf("shadow lookup eligible = true; want false after external-write invalidation (state=%s)", lookup.Entry.State)
	}
	if lookup.Entry.State != ebusgateway.ShadowEntryStateInvalidated && lookup.Entry.State != ebusgateway.ShadowEntryStateTombstone {
		t.Fatalf("shadow state = %s; want invalidated or tombstone", lookup.Entry.State)
	}
}

func TestHandleAdjudicatedPassiveEvent_IgnoresNonApplicableEvents(t *testing.T) {
	t.Parallel()

	baseEvent := func() ebusgateway.AdjudicatedPassiveEvent {
		return ebusgateway.AdjudicatedPassiveEvent{
			Event: ebusgateway.PassiveClassifiedEvent{
				HasRequest:  true,
				HasResponse: true,
				Request: protocol.Frame{
					Source:    0x10,
					Target:    0x08,
					Primary:   vaillantB509Primary,
					Secondary: vaillantB509Secondary,
					Data:      buildB509ReadSelector(0x0200),
				},
				Response: protocol.Frame{
					Source:    0x08,
					Target:    0x10,
					Primary:   vaillantB509Primary,
					Secondary: vaillantB509Secondary,
					Data:      []byte{vaillantB509OpcodeRead, 0x02, 0x00, 0xAA},
				},
			},
			Fingerprint: ebusgateway.PassiveTransactionFingerprint{
				SharedWatchKey: ebusgateway.NewB509WatchKey(0x08, 0x0200),
				ObservedAt:     time.Unix(300, 0),
				ResponseClass:  ebusgateway.DedupResponseValueBearing,
				FamilyPolicy: ebusgateway.ObserveFirstFamilyPolicy{
					RequestIntent:     ebusgateway.ObserveFirstRequestIntentRead,
					ResponseClass:     ebusgateway.DedupResponseValueBearing,
					DirectApplyPolicy: ebusgateway.ObserveFirstDirectApplyPolicyStateDefault,
				},
			},
			Disposition: ebusgateway.DedupDispositionUnmatchedThirdParty,
		}
	}

	tests := []struct {
		name   string
		mutate func(*ebusgateway.AdjudicatedPassiveEvent)
	}{
		{
			name: "matched active duplicate",
			mutate: func(event *ebusgateway.AdjudicatedPassiveEvent) {
				event.MatchedActiveDuplicate = true
			},
		},
		{
			name: "observability only",
			mutate: func(event *ebusgateway.AdjudicatedPassiveEvent) {
				event.ObservabilityOnly = true
			},
		},
		{
			name: "local participant inbound",
			mutate: func(event *ebusgateway.AdjudicatedPassiveEvent) {
				event.LocalParticipantInbound = true
			},
		},
		{
			name: "header only response class",
			mutate: func(event *ebusgateway.AdjudicatedPassiveEvent) {
				event.Fingerprint.ResponseClass = ebusgateway.DedupResponseHeaderOnly
				event.Fingerprint.FamilyPolicy.ResponseClass = ebusgateway.DedupResponseHeaderOnly
			},
		},
		{
			name: "ack only response class",
			mutate: func(event *ebusgateway.AdjudicatedPassiveEvent) {
				event.Fingerprint.ResponseClass = ebusgateway.DedupResponseACKOnly
				event.Fingerprint.FamilyPolicy.ResponseClass = ebusgateway.DedupResponseACKOnly
			},
		},
		{
			name: "no response bytes",
			mutate: func(event *ebusgateway.AdjudicatedPassiveEvent) {
				event.Event.HasResponse = false
			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			poller := newVaillantSemanticPoller(
				ebusgateway.Config{},
				&ebusgateway.Gateway{},
				graphql.NewLiveSemanticProvider(),
				nil,
				nil,
			)
			event := baseEvent()
			test.mutate(&event)

			poller.handleAdjudicatedPassiveEvent(event)

			key := ebusgateway.NewB509WatchKey(0x08, 0x0200)
			lookup := poller.shadow.Lookup(key, 10*time.Second)
			if lookup.Found {
				t.Fatalf("shadow lookup found = true; want ignored event to skip shadow mutation (state=%s value=%x)", lookup.Entry.State, lookup.Entry.Value)
			}
		})
	}
}

func TestHandleAdjudicatedPassiveEvent_ReadSkipsShadowWhenObserveFirstMasterDisabled(t *testing.T) {
	t.Parallel()

	key := ebusgateway.NewB509WatchKey(0x08, 0x0200)
	cfg := observeFirstStateShadowConfig(key)
	cfg.ObserveFirstFlags = ebusgateway.DefaultObserveFirstFeatureFlags()
	poller := newVaillantSemanticPoller(
		cfg,
		&ebusgateway.Gateway{},
		graphql.NewLiveSemanticProvider(),
		nil,
		nil,
	)

	event := ebusgateway.AdjudicatedPassiveEvent{
		Event: ebusgateway.PassiveClassifiedEvent{
			HasRequest:  true,
			HasResponse: true,
			Request: protocol.Frame{
				Source:    0x10,
				Target:    0x08,
				Primary:   vaillantB509Primary,
				Secondary: vaillantB509Secondary,
				Data:      buildB509ReadSelector(0x0200),
			},
			Response: protocol.Frame{
				Source:    0x08,
				Target:    0x10,
				Primary:   vaillantB509Primary,
				Secondary: vaillantB509Secondary,
				Data:      []byte{vaillantB509OpcodeRead, 0x02, 0x00, 0xAA},
			},
		},
		Fingerprint: ebusgateway.PassiveTransactionFingerprint{
			SharedWatchKey: key,
			ObservedAt:     time.Unix(310, 0),
			ResponseClass:  ebusgateway.DedupResponseValueBearing,
			FamilyPolicy: ebusgateway.ObserveFirstFamilyPolicy{
				RequestIntent:     ebusgateway.ObserveFirstRequestIntentRead,
				ResponseClass:     ebusgateway.DedupResponseValueBearing,
				DirectApplyPolicy: ebusgateway.ObserveFirstDirectApplyPolicyStateDefault,
			},
		},
		Disposition: ebusgateway.DedupDispositionUnmatchedThirdParty,
	}

	poller.handleAdjudicatedPassiveEvent(event)

	if lookup := poller.shadow.Lookup(key, 10*time.Second); lookup.Found {
		t.Fatalf("shadow lookup found = true; want observe-first global gate to block passive shadow write (state=%s value=%x)", lookup.Entry.State, lookup.Entry.Value)
	}
}

func TestHandleAdjudicatedPassiveEvent_ReadWithoutSharedWatchKeyDoesNotPolluteShadow(t *testing.T) {
	t.Parallel()

	key := ebusgateway.NewB509WatchKey(0x08, 0x0200)
	poller := newVaillantSemanticPoller(
		observeFirstStateShadowRuntimeConfig(ebusgateway.ObserveFirstExternalWritePolicyRecordOnly),
		&ebusgateway.Gateway{},
		graphql.NewLiveSemanticProvider(),
		nil,
		nil,
	)

	event := ebusgateway.AdjudicatedPassiveEvent{
		Event: ebusgateway.PassiveClassifiedEvent{
			HasRequest:  true,
			HasResponse: true,
			Request: protocol.Frame{
				Source:    0x10,
				Target:    0x08,
				Primary:   vaillantB509Primary,
				Secondary: vaillantB509Secondary,
				Data:      buildB509ReadSelector(0x0200),
			},
			Response: protocol.Frame{
				Source:    0x08,
				Target:    0x10,
				Primary:   vaillantB509Primary,
				Secondary: vaillantB509Secondary,
				Data:      []byte{vaillantB509OpcodeRead, 0x02, 0x00, 0xAB},
			},
		},
		Fingerprint: ebusgateway.PassiveTransactionFingerprint{
			ObservedAt:    time.Unix(320, 0),
			ResponseClass: ebusgateway.DedupResponseValueBearing,
			FamilyPolicy: ebusgateway.ObserveFirstFamilyPolicy{
				RequestIntent:     ebusgateway.ObserveFirstRequestIntentRead,
				ResponseClass:     ebusgateway.DedupResponseValueBearing,
				DirectApplyPolicy: ebusgateway.ObserveFirstDirectApplyPolicyStateDefault,
			},
		},
		Disposition: ebusgateway.DedupDispositionUnmatchedThirdParty,
	}

	poller.handleAdjudicatedPassiveEvent(event)

	if lookup := poller.shadow.Lookup(key, 10*time.Second); lookup.Found {
		t.Fatalf("shadow lookup found = true; want shared-watchkey-absent traffic to be ignored (state=%s value=%x)", lookup.Entry.State, lookup.Entry.Value)
	}
}

func TestHandleAdjudicatedPassiveEvent_ReadUnknownWatchKeyDoesNotPolluteShadow(t *testing.T) {
	t.Parallel()

	key := ebusgateway.NewB509WatchKey(0x08, 0x0200)
	cfg := observeFirstStateShadowRuntimeConfig(ebusgateway.ObserveFirstExternalWritePolicyRecordOnly)
	poller := newVaillantSemanticPoller(
		cfg,
		&ebusgateway.Gateway{},
		graphql.NewLiveSemanticProvider(),
		nil,
		nil,
	)

	event := ebusgateway.AdjudicatedPassiveEvent{
		Event: ebusgateway.PassiveClassifiedEvent{
			HasRequest:  true,
			HasResponse: true,
			Request: protocol.Frame{
				Source:    0x10,
				Target:    0x08,
				Primary:   vaillantB509Primary,
				Secondary: vaillantB509Secondary,
				Data:      buildB509ReadSelector(0x0200),
			},
			Response: protocol.Frame{
				Source:    0x08,
				Target:    0x10,
				Primary:   vaillantB509Primary,
				Secondary: vaillantB509Secondary,
				Data:      []byte{vaillantB509OpcodeRead, 0x02, 0x00, 0xAC},
			},
		},
		Fingerprint: ebusgateway.PassiveTransactionFingerprint{
			SharedWatchKey: unknownSemanticWatchKey{canonical: "unknown:08:0200"},
			ObservedAt:     time.Unix(330, 0),
			ResponseClass:  ebusgateway.DedupResponseValueBearing,
			FamilyPolicy: ebusgateway.ObserveFirstFamilyPolicy{
				RequestIntent:     ebusgateway.ObserveFirstRequestIntentRead,
				ResponseClass:     ebusgateway.DedupResponseValueBearing,
				DirectApplyPolicy: ebusgateway.ObserveFirstDirectApplyPolicyStateDefault,
			},
		},
		Disposition: ebusgateway.DedupDispositionUnmatchedThirdParty,
	}

	poller.handleAdjudicatedPassiveEvent(event)

	if lookup := poller.shadow.Lookup(key, 10*time.Second); lookup.Found {
		t.Fatalf("shadow lookup found = true; want unknown shared watch key to be ignored (state=%s value=%x)", lookup.Entry.State, lookup.Entry.Value)
	}
}

func TestAttachPassiveShadowProducer_DedupSuppressedWriteStillInvalidatesShadow(t *testing.T) {
	policies := []ebusgateway.ObserveFirstExternalWritePolicy{
		ebusgateway.ObserveFirstExternalWritePolicyInvalidateOnly,
		ebusgateway.ObserveFirstExternalWritePolicyRecordAndInvalidate,
	}

	for _, policy := range policies {
		policy := policy
		t.Run(string(policy), func(t *testing.T) {
			key := ebusgateway.NewB509WatchKey(0x08, 0x0200)
			cfg := observeFirstStateShadowRuntimeConfig(policy)
			cfg.PassiveDedupRecoveryHysteresis = time.Nanosecond
			cfg.PassiveDedupRecoveryEventThreshold = 2

			deduplicator, err := ebusgateway.NewActivePassiveDeduplicator(cfg)
			if err != nil {
				t.Fatalf("NewActivePassiveDeduplicator() error = %v", err)
			}
			t.Cleanup(func() {
				_ = deduplicator.Close()
			})

			poller := newVaillantSemanticPoller(
				cfg,
				&ebusgateway.Gateway{},
				graphql.NewLiveSemanticProvider(),
				nil,
				nil,
			)

			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			if err := poller.AttachPassiveShadowProducer(ctx, deduplicator); err != nil {
				t.Fatalf("AttachPassiveShadowProducer() error = %v", err)
			}

			witness, err := deduplicator.Subscribe("shadow-regression-witness", ebusgateway.DedupSubscriberCritical, 128)
			if err != nil {
				t.Fatalf("Subscribe(witness) error = %v", err)
			}
			t.Cleanup(witness.Close)

			_ = poller.prepareSemanticReadWatch(key)
			seedAt := time.Now()
			seed := poller.shadow.Write(ebusgateway.ShadowWrite{
				Key:        key,
				Source:     ebusgateway.ShadowWriteSourcePassive,
				Confidence: ebusgateway.ShadowConfidenceHigh,
				Value:      []byte{0x11, 0x22},
				ObservedAt: seedAt,
			})
			if !seed.Accepted {
				t.Fatalf("seed shadow write rejected: %s", seed.Reason)
			}

			base := seedAt.Add(10 * time.Millisecond)
			deduplicator.OnPassiveClassifiedEvent(passiveBroadcastClassifiedEvent(base))
			deduplicator.OnPassiveClassifiedEvent(passiveB509WriteClassifiedEvent(base.Add(2*time.Nanosecond), 0x10, 0x08, 0x0200, []byte{0x33, 0x44}))
			deduplicator.OnPassiveClassifiedEvent(passiveBroadcastClassifiedEvent(base.Add(deduplicator.Budgets().PendingGraceTimeout + 4*time.Nanosecond)))

			adjudicated := waitForAdjudicatedEvent(t, witness, 2*time.Second, func(event ebusgateway.AdjudicatedPassiveEvent) bool {
				return event.Disposition == ebusgateway.DedupDispositionUnmatchedThirdParty &&
					event.FamilyPolicy.RequestIntent == ebusgateway.ObserveFirstRequestIntentWrite
			})
			if !adjudicated.SuppressShadow {
				t.Fatalf("SuppressShadow = false; want true for policy %q", policy)
			}
			if adjudicated.Fingerprint.SharedWatchKey == nil {
				t.Fatal("SharedWatchKey = nil; want dedup-emitted shared key for passive write invalidation")
			}
			if got := adjudicated.Fingerprint.SharedWatchKey.Canonical(); got != key.Canonical() {
				t.Fatalf("SharedWatchKey.Canonical() = %q; want %q", got, key.Canonical())
			}

			lookup := waitForShadowIneligible(t, poller.shadow, key, 2*time.Second)
			if lookup.Entry.State != ebusgateway.ShadowEntryStateInvalidated && lookup.Entry.State != ebusgateway.ShadowEntryStateTombstone {
				t.Fatalf("shadow state = %s; want invalidated or tombstone", lookup.Entry.State)
			}
			if lookup.Entry.InvalidationReason != ebusgateway.ShadowInvalidationReasonExternalWrite {
				t.Fatalf("invalidation reason = %s; want %s", lookup.Entry.InvalidationReason, ebusgateway.ShadowInvalidationReasonExternalWrite)
			}
			if lookup.Entry.InvalidationSource != ebusgateway.ShadowInvalidationSourcePassive {
				t.Fatalf("invalidation source = %s; want %s", lookup.Entry.InvalidationSource, ebusgateway.ShadowInvalidationSourcePassive)
			}
		})
	}
}

func TestAttachPassiveShadowProducer_ResubscribesAfterCriticalOverflow(t *testing.T) {
	originalPriority := passiveShadowSubscriberPriority
	originalBuffer := passiveShadowSubscriberBuffer
	originalRetryDelay := passiveShadowRetryDelay
	passiveShadowSubscriberPriority = ebusgateway.DedupSubscriberCritical
	passiveShadowSubscriberBuffer = 1
	passiveShadowRetryDelay = time.Millisecond
	t.Cleanup(func() {
		passiveShadowSubscriberPriority = originalPriority
		passiveShadowSubscriberBuffer = originalBuffer
		passiveShadowRetryDelay = originalRetryDelay
	})

	key := ebusgateway.NewB509WatchKey(0x08, 0x0200)
	cfg := observeFirstStateShadowRuntimeConfig(ebusgateway.ObserveFirstExternalWritePolicyInvalidateOnly)
	cfg.PassiveDedupRecoveryHysteresis = time.Nanosecond
	cfg.PassiveDedupRecoveryEventThreshold = 2

	deduplicator, err := ebusgateway.NewActivePassiveDeduplicator(cfg)
	if err != nil {
		t.Fatalf("NewActivePassiveDeduplicator() error = %v", err)
	}
	t.Cleanup(func() {
		_ = deduplicator.Close()
	})

	poller := newVaillantSemanticPoller(
		cfg,
		&ebusgateway.Gateway{},
		graphql.NewLiveSemanticProvider(),
		nil,
		nil,
	)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := poller.AttachPassiveShadowProducer(ctx, deduplicator); err != nil {
		t.Fatalf("AttachPassiveShadowProducer() error = %v", err)
	}

	witness, err := deduplicator.Subscribe("shadow-overflow-witness", ebusgateway.DedupSubscriberCritical, 4096)
	if err != nil {
		t.Fatalf("Subscribe(witness) error = %v", err)
	}
	t.Cleanup(witness.Close)

	_ = poller.prepareSemanticReadWatch(key)
	seedAt := time.Now()
	seed := poller.shadow.Write(ebusgateway.ShadowWrite{
		Key:        key,
		Source:     ebusgateway.ShadowWriteSourcePassive,
		Confidence: ebusgateway.ShadowConfidenceHigh,
		Value:      []byte{0x01},
		ObservedAt: seedAt,
	})
	if !seed.Accepted {
		t.Fatalf("seed shadow write rejected: %s", seed.Reason)
	}

	base := seedAt.Add(10 * time.Millisecond)
	deduplicator.OnPassiveClassifiedEvent(passiveBroadcastClassifiedEvent(base))
	for index := 0; index < 256; index++ {
		deduplicator.OnPassiveClassifiedEvent(passiveB509WriteClassifiedEvent(base.Add(2*time.Nanosecond), 0x10, 0x08, 0x0200, []byte{byte(index)}))
	}
	deduplicator.OnPassiveClassifiedEvent(passiveBroadcastClassifiedEvent(base.Add(deduplicator.Budgets().PendingGraceTimeout + 4*time.Nanosecond)))

	_ = waitForAdjudicatedEvent(t, witness, 2*time.Second, func(event ebusgateway.AdjudicatedPassiveEvent) bool {
		return event.Disposition == ebusgateway.DedupDispositionDiscontinuity &&
			event.Event.DiscontinuityReason == ebusgateway.PassiveDiscontinuityCriticalSubscriberFault
	})
	time.Sleep(50 * time.Millisecond)

	reseedAt := base.Add(time.Second)
	startGeneration := poller.shadow.SnapshotEligibility(key).Generation
	reseed := poller.shadow.Write(ebusgateway.ShadowWrite{
		Key:             key,
		Source:          ebusgateway.ShadowWriteSourceActiveConfirmed,
		Confidence:      ebusgateway.ShadowConfidenceHigh,
		Value:           []byte{0x7A},
		ObservedAt:      reseedAt,
		StartGeneration: startGeneration,
	})
	if !reseed.Accepted {
		t.Fatalf("reseed shadow write rejected: %s", reseed.Reason)
	}
	if lookup := poller.shadow.Lookup(key, 10*time.Second); !lookup.Found || lookup.Entry.State != ebusgateway.ShadowEntryStatePresent {
		t.Fatalf("shadow reseed lookup = %+v; want present seeded entry before resubscribe verification", lookup)
	}

	wave := reseedAt.Add(10 * time.Millisecond)
	deduplicator.OnPassiveClassifiedEvent(passiveBroadcastClassifiedEvent(wave))
	deduplicator.OnPassiveClassifiedEvent(passiveB509WriteClassifiedEvent(wave.Add(2*time.Nanosecond), 0x11, 0x08, 0x0200, []byte{0xAA}))
	deduplicator.OnPassiveClassifiedEvent(passiveBroadcastClassifiedEvent(wave.Add(deduplicator.Budgets().PendingGraceTimeout + 4*time.Nanosecond)))

	lookup := waitForShadowIneligible(t, poller.shadow, key, 2*time.Second)
	if lookup.Entry.InvalidationSource != ebusgateway.ShadowInvalidationSourcePassive {
		t.Fatalf("invalidation source = %s; want %s", lookup.Entry.InvalidationSource, ebusgateway.ShadowInvalidationSourcePassive)
	}
}
