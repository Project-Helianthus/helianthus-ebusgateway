package ebusgateway

import "github.com/Project-Helianthus/helianthus-ebusgo/protocol"

type ObserveFirstFamily string

const (
	ObserveFirstFamilyOther ObserveFirstFamily = "other"
	ObserveFirstFamilyB509  ObserveFirstFamily = "B509"
	ObserveFirstFamilyB516  ObserveFirstFamily = "B516"
	ObserveFirstFamilyB524  ObserveFirstFamily = "B524"
	ObserveFirstFamilyB555  ObserveFirstFamily = "B555"
)

type ObserveFirstTrafficScope string

const (
	ObserveFirstTrafficScopeActive  ObserveFirstTrafficScope = "active"
	ObserveFirstTrafficScopePassive ObserveFirstTrafficScope = "passive"
)

type ObserveFirstRequestIntent string

const (
	ObserveFirstRequestIntentUnknown   ObserveFirstRequestIntent = "unknown"
	ObserveFirstRequestIntentRead      ObserveFirstRequestIntent = "read"
	ObserveFirstRequestIntentWrite     ObserveFirstRequestIntent = "write"
	ObserveFirstRequestIntentBroadcast ObserveFirstRequestIntent = "broadcast"
)

type ObserveFirstDirectApplyPolicy string

const (
	ObserveFirstDirectApplyPolicyNever           ObserveFirstDirectApplyPolicy = "never"
	ObserveFirstDirectApplyPolicyStateDefault    ObserveFirstDirectApplyPolicy = "state_default"
	ObserveFirstDirectApplyPolicyConfigOptIn     ObserveFirstDirectApplyPolicy = "config_opt_in"
	ObserveFirstDirectApplyPolicyEnergyMergeOnly ObserveFirstDirectApplyPolicy = "energy_merge_only"
)

type ObserveFirstFamilyPolicy struct {
	Family                         ObserveFirstFamily
	RequestIntent                  ObserveFirstRequestIntent
	ResponseClass                  DedupResponseClass
	CorrelationPolicy              WatchCorrelationPolicy
	DirectApplyPolicy              ObserveFirstDirectApplyPolicy
	UsesRuntimeExternalWritePolicy bool
	EffectiveExternalWritePolicy   ObserveFirstExternalWritePolicy
}

func observeFirstResponseClassForPassiveEvent(event PassiveClassifiedEvent, outcome DedupOutcomeClass) DedupResponseClass {
	return observeFirstResponseClass(
		event.Request,
		event.FrameType,
		outcome,
		event.Response,
		event.HasResponse,
	)
}

func observeFirstResponseClassForActiveEvent(event protocol.BusEvent) DedupResponseClass {
	return observeFirstResponseClass(
		event.Request,
		event.FrameType,
		DedupOutcomeSuccess,
		event.Response,
		event.HasResponse,
	)
}

func observeFirstResponseClass(request protocol.Frame, frameType protocol.FrameType, outcome DedupOutcomeClass, response protocol.Frame, hasResponse bool) DedupResponseClass {
	if outcome != DedupOutcomeSuccess {
		return DedupResponseErrorOrAmbiguous
	}
	if !hasResponse {
		if frameType == protocol.FrameTypeInitiatorInitiator {
			return DedupResponseACKOnly
		}
		return DedupResponseErrorOrAmbiguous
	}

	family := observeFirstFamilyFromFrame(request)
	intent := observeFirstRequestIntentFromFrame(request)

	switch family {
	case ObserveFirstFamilyB509:
		return observeFirstB509ResponseClass(request, intent, response.Data)
	case ObserveFirstFamilyB524:
		return observeFirstB524ResponseClass(request, intent, response.Data)
	default:
		return observeFirstGenericResponseClass(response.Data)
	}
}

func observeFirstFamilyPolicy(scope ObserveFirstTrafficScope, request protocol.Frame, responseClass DedupResponseClass, observation WatchObservation, flags ObserveFirstFeatureFlagView) ObserveFirstFamilyPolicy {
	family := observeFirstFamilyFromFrame(request)
	intent := observeFirstRequestIntentFromFrame(request)

	policy := ObserveFirstFamilyPolicy{
		Family:            family,
		RequestIntent:     intent,
		ResponseClass:     responseClass,
		CorrelationPolicy: WatchCorrelationPolicyRecordOnly,
		DirectApplyPolicy: ObserveFirstDirectApplyPolicyNever,
	}

	if scope == ObserveFirstTrafficScopePassive && intent == ObserveFirstRequestIntentWrite {
		policy.UsesRuntimeExternalWritePolicy = true
		policy.EffectiveExternalWritePolicy = NormalizeObserveFirstFeatureFlagsFromView(flags).ExternalWritePolicy()
	}

	switch family {
	case ObserveFirstFamilyB516:
		policy.CorrelationPolicy = WatchCorrelationPolicyBroadcastSelector
		if responseClass == DedupResponseValueBearing {
			policy.DirectApplyPolicy = ObserveFirstDirectApplyPolicyEnergyMergeOnly
		}
	case ObserveFirstFamilyB524:
		policy.CorrelationPolicy = WatchCorrelationPolicyRequestResponse
		if observeFirstB524CorrelatedStateRead(request, observation) && responseClass == DedupResponseValueBearing {
			policy.DirectApplyPolicy = ObserveFirstDirectApplyPolicyStateDefault
		}
	case ObserveFirstFamilyB509:
		policy.CorrelationPolicy = WatchCorrelationPolicyRequestResponse
		if intent == ObserveFirstRequestIntentRead && responseClass == DedupResponseValueBearing {
			policy.DirectApplyPolicy = ObserveFirstDirectApplyPolicyStateDefault
		}
	case ObserveFirstFamilyB555:
		policy.CorrelationPolicy = WatchCorrelationPolicyRecordInvalidate
	default:
		if scope == ObserveFirstTrafficScopePassive && intent == ObserveFirstRequestIntentBroadcast {
			policy.CorrelationPolicy = WatchCorrelationPolicyBroadcastSelector
		}
	}

	return policy
}

func observeFirstFamilyFromFrame(frame protocol.Frame) ObserveFirstFamily {
	if frame.Primary != 0xB5 {
		return ObserveFirstFamilyOther
	}
	switch frame.Secondary {
	case 0x09:
		return ObserveFirstFamilyB509
	case 0x16:
		return ObserveFirstFamilyB516
	case 0x24:
		return ObserveFirstFamilyB524
	case 0x55:
		return ObserveFirstFamilyB555
	default:
		return ObserveFirstFamilyOther
	}
}

func observeFirstRequestIntentFromFrame(frame protocol.Frame) ObserveFirstRequestIntent {
	switch observeFirstFamilyFromFrame(frame) {
	case ObserveFirstFamilyB509:
		if len(frame.Data) == 0 {
			return ObserveFirstRequestIntentUnknown
		}
		switch frame.Data[0] {
		case 0x0D, 0x29:
			return ObserveFirstRequestIntentRead
		case 0x0E:
			return ObserveFirstRequestIntentWrite
		default:
			return ObserveFirstRequestIntentUnknown
		}
	case ObserveFirstFamilyB516:
		return ObserveFirstRequestIntentBroadcast
	case ObserveFirstFamilyB524:
		if len(frame.Data) == 0 {
			return ObserveFirstRequestIntentUnknown
		}
		switch frame.Data[0] {
		case 0x02, 0x06:
			if len(frame.Data) >= 2 && frame.Data[1] == 0x00 {
				return ObserveFirstRequestIntentRead
			}
			if len(frame.Data) >= 2 {
				return ObserveFirstRequestIntentWrite
			}
			return ObserveFirstRequestIntentUnknown
		case 0x03:
			return ObserveFirstRequestIntentRead
		default:
			return ObserveFirstRequestIntentUnknown
		}
	case ObserveFirstFamilyB555:
		if len(frame.Data) == 0 {
			return ObserveFirstRequestIntentUnknown
		}
		switch frame.Data[0] {
		case 0xA3, 0xA4, 0xA5:
			return ObserveFirstRequestIntentRead
		case 0xA6:
			return ObserveFirstRequestIntentWrite
		default:
			return ObserveFirstRequestIntentUnknown
		}
	default:
		return ObserveFirstRequestIntentUnknown
	}
}

func observeFirstGenericResponseClass(payload []byte) DedupResponseClass {
	if len(payload) == 0 {
		return DedupResponseHeaderOnly
	}
	return DedupResponseValueBearing
}

func observeFirstB509ResponseClass(request protocol.Frame, intent ObserveFirstRequestIntent, payload []byte) DedupResponseClass {
	if len(payload) == 0 {
		return DedupResponseHeaderOnly
	}
	if intent == ObserveFirstRequestIntentWrite {
		return DedupResponseHeaderOnly
	}
	if intent != ObserveFirstRequestIntentRead {
		return observeFirstGenericResponseClass(payload)
	}

	addr, ok := observeFirstExpectedB509Address(request)
	if !ok {
		return observeFirstGenericResponseClass(payload)
	}

	addrHi := byte(addr >> 8)
	addrLo := byte(addr)

	if len(payload) >= 3 && (payload[0] == 0x0D || payload[0] == 0x29) && payload[1] == addrHi && payload[2] == addrLo {
		if len(payload) == 3 {
			return DedupResponseHeaderOnly
		}
		return DedupResponseValueBearing
	}
	if len(payload) >= 2 && payload[0] == addrHi && payload[1] == addrLo {
		if len(payload) == 2 {
			return DedupResponseHeaderOnly
		}
		return DedupResponseValueBearing
	}
	return observeFirstGenericResponseClass(payload)
}

func observeFirstB524ResponseClass(request protocol.Frame, intent ObserveFirstRequestIntent, payload []byte) DedupResponseClass {
	if len(payload) == 0 {
		return DedupResponseHeaderOnly
	}
	if intent != ObserveFirstRequestIntentRead {
		if intent == ObserveFirstRequestIntentWrite {
			return DedupResponseHeaderOnly
		}
		return observeFirstGenericResponseClass(payload)
	}
	if observeFirstIsB524TimerRead(request) {
		return observeFirstGenericResponseClass(payload)
	}

	opcode, group, instance, addr, ok := observeFirstExpectedB524ReadSelector(request)
	if !ok {
		return observeFirstGenericResponseClass(payload)
	}
	if len(payload) == 1 && payload[0] == 0x00 {
		return DedupResponseErrorOrAmbiguous
	}
	if len(payload) < 4 {
		return DedupResponseErrorOrAmbiguous
	}
	if len(payload) >= 5 {
		replyInstance := payload[1]
		replyGroup := payload[2]
		replyAddr := uint16(payload[3]) | uint16(payload[4])<<8
		if replyGroup == group && replyAddr == addr {
			if !observeFirstMatchesB524ReplyInstance(replyInstance, instance) {
				return DedupResponseErrorOrAmbiguous
			}
			if len(payload) == 5 {
				return DedupResponseHeaderOnly
			}
			return DedupResponseValueBearing
		}
	}

	replyGroup := payload[1]
	replyAddr := uint16(payload[2]) | uint16(payload[3])<<8
	if replyGroup != group || replyAddr != addr {
		_ = opcode
		return DedupResponseErrorOrAmbiguous
	}
	if len(payload) == 4 {
		return DedupResponseHeaderOnly
	}
	return DedupResponseValueBearing
}

func observeFirstExpectedB509Address(frame protocol.Frame) (uint16, bool) {
	if len(frame.Data) < 3 {
		return 0, false
	}
	switch frame.Data[0] {
	case 0x0D, 0x29, 0x0E:
		return uint16(frame.Data[1])<<8 | uint16(frame.Data[2]), true
	default:
		return 0, false
	}
}

func observeFirstExpectedB524ReadSelector(frame protocol.Frame) (opcode, group, instance byte, addr uint16, ok bool) {
	if len(frame.Data) < 6 {
		return 0, 0, 0, 0, false
	}
	if frame.Data[0] != 0x02 && frame.Data[0] != 0x06 {
		return 0, 0, 0, 0, false
	}
	if frame.Data[1] != 0x00 {
		return 0, 0, 0, 0, false
	}
	return frame.Data[0], frame.Data[2], frame.Data[3], uint16(frame.Data[4]) | uint16(frame.Data[5])<<8, true
}

func observeFirstB524CorrelatedStateRead(frame protocol.Frame, observation WatchObservation) bool {
	_, _, _, _, ok := observeFirstExpectedB524ReadSelector(frame)
	if !ok {
		return false
	}
	if !observation.HasDescriptor || observation.State != WatchObservationStateActive {
		return false
	}
	descriptor := observation.Descriptor
	if descriptor.SemanticClass != WatchSemanticClassState {
		return false
	}
	if descriptor.CorrelationPolicy != WatchCorrelationPolicyRequestResponse {
		return false
	}
	return descriptor.DirectApplyPolicy == WatchDirectApplyPolicyStateDefault
}

func observeFirstIsB524TimerRead(frame protocol.Frame) bool {
	return len(frame.Data) > 0 && frame.Data[0] == 0x03
}

func observeFirstMatchesB524ReplyInstance(replyInstance, requestedInstance byte) bool {
	if replyInstance == requestedInstance {
		return true
	}
	if requestedInstance < 0xFF && replyInstance == requestedInstance+1 {
		return true
	}
	return false
}
