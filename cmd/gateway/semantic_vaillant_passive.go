package main

import (
	"context"
	"log"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
)

func (p *vaillantSemanticPoller) AttachPassiveShadowProducer(ctx context.Context, deduplicator *ebusgateway.ActivePassiveDeduplicator) error {
	if p == nil || deduplicator == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}

	subscription, err := deduplicator.Subscribe(
		"semantic-vaillant-shadow",
		passiveShadowSubscriberPriority,
		passiveShadowSubscriberBuffer,
	)
	if err != nil {
		return err
	}

	go func(subscription *ebusgateway.AdjudicatedPassiveSubscription) {
		for {
			closedUnexpectedly := false
		consume:
			for {
				select {
				case <-ctx.Done():
					subscription.Close()
					return
				case event, ok := <-subscription.Events():
					if !ok {
						closedUnexpectedly = ctx.Err() == nil
						break consume
					}
					p.handleAdjudicatedPassiveEvent(event)
				}
			}
			subscription.Close()
			if !closedUnexpectedly {
				return
			}
			for {
				if !waitForPassiveShadowRetry(ctx) {
					return
				}
				next, err := deduplicator.Subscribe(
					"semantic-vaillant-shadow",
					passiveShadowSubscriberPriority,
					passiveShadowSubscriberBuffer,
				)
				if err != nil {
					continue
				}
				subscription = next
				break
			}
		}
	}(subscription)

	return nil
}

func waitForPassiveShadowRetry(ctx context.Context) bool {
	delay := passiveShadowRetryDelay
	if delay <= 0 {
		delay = 100 * time.Millisecond
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (p *vaillantSemanticPoller) handleAdjudicatedPassiveEvent(event ebusgateway.AdjudicatedPassiveEvent) {
	if p == nil || p.shadow == nil {
		return
	}
	if event.Disposition != ebusgateway.DedupDispositionUnmatchedThirdParty ||
		event.ObservabilityOnly ||
		event.LocalParticipantInbound ||
		event.MatchedActiveDuplicate {
		return
	}

	familyPolicy := event.Fingerprint.FamilyPolicy
	if !passiveShadowLaneEnabled(p.shadow.FeatureFlags(), familyPolicy) {
		return
	}

	key, ok := clonePassiveAdjudicatedWatchKey(event)
	if !ok || key == nil {
		return
	}
	runtime := p.bootstrapPassiveSharedWatchKey(key)

	switch familyPolicy.RequestIntent {
	case ebusgateway.ObserveFirstRequestIntentWrite:
		if !shouldInvalidatePassiveExternalWrite(familyPolicy) {
			return
		}
		invalidatedAt := event.Fingerprint.ObservedAt
		if invalidatedAt.IsZero() {
			invalidatedAt = p.now()
		}
		p.shadow.Invalidate(ebusgateway.ShadowInvalidation{
			Key:           key,
			Reason:        ebusgateway.ShadowInvalidationReasonExternalWrite,
			Source:        ebusgateway.ShadowInvalidationSourcePassive,
			InvalidatedAt: invalidatedAt,
		})
		return
	case ebusgateway.ObserveFirstRequestIntentRead:
	default:
		return
	}
	if event.SuppressShadow {
		return
	}

	if event.Fingerprint.ResponseClass != ebusgateway.DedupResponseValueBearing {
		return
	}
	value, ok := parsePassiveShadowPayload(event, key)
	if !ok || len(value) == 0 {
		return
	}

	observedAt := event.Fingerprint.ObservedAt
	if observedAt.IsZero() {
		observedAt = p.now()
	}
	result := p.shadow.Write(ebusgateway.ShadowWrite{
		Key:        key,
		Source:     ebusgateway.ShadowWriteSourcePassive,
		Confidence: ebusgateway.ShadowConfidenceHigh,
		Value:      value,
		ObservedAt: observedAt,
	})
	p.emitWatchDirectApplyEfficiency(runtime, observedAt, true, result.Accepted)
	if !result.Accepted {
		log.Printf("semantic_passive_shadow_write_rejected key=%q reason=%s", key.Canonical(), result.Reason)
		return
	}
}

func passiveShadowLaneEnabled(flags ebusgateway.ObserveFirstFeatureFlags, policy ebusgateway.ObserveFirstFamilyPolicy) bool {
	if !flags.ObserveFirstEnabled() {
		return false
	}

	switch policy.RequestIntent {
	case ebusgateway.ObserveFirstRequestIntentRead:
		switch policy.DirectApplyPolicy {
		case ebusgateway.ObserveFirstDirectApplyPolicyStateDefault:
			return flags.PassiveStateDirectApply()
		case ebusgateway.ObserveFirstDirectApplyPolicyConfigOptIn:
			return flags.PassiveConfigDirectApply()
		default:
			return false
		}
	case ebusgateway.ObserveFirstRequestIntentWrite:
		if !policy.UsesRuntimeExternalWritePolicy {
			return false
		}
		return policy.EffectiveExternalWritePolicy != ebusgateway.ObserveFirstExternalWritePolicyRecordOnly
	default:
		return false
	}
}

func shouldInvalidatePassiveExternalWrite(policy ebusgateway.ObserveFirstFamilyPolicy) bool {
	if !policy.UsesRuntimeExternalWritePolicy {
		return false
	}
	switch policy.EffectiveExternalWritePolicy {
	case ebusgateway.ObserveFirstExternalWritePolicyInvalidateOnly,
		ebusgateway.ObserveFirstExternalWritePolicyRecordAndInvalidate:
		return true
	default:
		return false
	}
}

func clonePassiveAdjudicatedWatchKey(event ebusgateway.AdjudicatedPassiveEvent) (ebusgateway.WatchKey, bool) {
	return cloneSemanticWatchKey(event.Fingerprint.SharedWatchKey)
}

func cloneSemanticWatchKey(key ebusgateway.WatchKey) (ebusgateway.WatchKey, bool) {
	switch typed := key.(type) {
	case ebusgateway.B509WatchKey:
		cloned := ebusgateway.NewB509WatchKey(typed.Target, typed.RegisterAddress)
		return cloned, true
	case *ebusgateway.B509WatchKey:
		if typed == nil {
			return nil, false
		}
		cloned := ebusgateway.NewB509WatchKey(typed.Target, typed.RegisterAddress)
		return cloned, true
	case ebusgateway.B524WatchKey:
		cloned := ebusgateway.NewB524WatchKey(typed.Target, typed.Opcode, typed.Group, typed.Instance, typed.RegisterAddress)
		return cloned, true
	case *ebusgateway.B524WatchKey:
		if typed == nil {
			return nil, false
		}
		cloned := ebusgateway.NewB524WatchKey(typed.Target, typed.Opcode, typed.Group, typed.Instance, typed.RegisterAddress)
		return cloned, true
	default:
		return nil, false
	}
}

func parsePassiveShadowPayload(event ebusgateway.AdjudicatedPassiveEvent, key ebusgateway.WatchKey) ([]byte, bool) {
	if !event.Event.HasResponse {
		return nil, false
	}
	switch typed := key.(type) {
	case ebusgateway.B509WatchKey:
		return parseB509ReadPayload(event.Event.Response.Data, typed.RegisterAddress)
	case *ebusgateway.B509WatchKey:
		if typed == nil {
			return nil, false
		}
		return parseB509ReadPayload(event.Event.Response.Data, typed.RegisterAddress)
	case ebusgateway.B524WatchKey:
		return parseB524ReadPayload(event.Event.Response.Data, typed.Opcode, typed.Group, typed.Instance, typed.RegisterAddress)
	case *ebusgateway.B524WatchKey:
		if typed == nil {
			return nil, false
		}
		return parseB524ReadPayload(event.Event.Response.Data, typed.Opcode, typed.Group, typed.Instance, typed.RegisterAddress)
	default:
		return nil, false
	}
}
