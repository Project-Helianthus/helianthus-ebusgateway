package ebusgateway

import (
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
)

func testActiveStateObservation(key WatchKey) WatchObservation {
	return WatchObservation{
		State: WatchObservationStateActive,
		Descriptor: WatchDescriptor{
			Key:               key,
			SemanticClass:     WatchSemanticClassState,
			CorrelationPolicy: WatchCorrelationPolicyRequestResponse,
			DirectApplyPolicy: WatchDirectApplyPolicyStateDefault,
		},
		HasDescriptor: true,
	}
}

func TestObserveFirstResponseClass_B509ReadMapsHeaderOnlyAndValueBearing(t *testing.T) {
	request := protocol.Frame{
		Source:    0x31,
		Target:    0x08,
		Primary:   0xB5,
		Secondary: 0x09,
		Data:      []byte{0x29, 0x02, 0x00},
	}

	headerOnly := observeFirstResponseClass(
		request,
		protocol.FrameTypeInitiatorTarget,
		DedupOutcomeSuccess,
		protocol.Frame{Data: []byte{0x29, 0x02, 0x00}},
		true,
	)
	if headerOnly != DedupResponseHeaderOnly {
		t.Fatalf("header-only response class = %q; want %q", headerOnly, DedupResponseHeaderOnly)
	}

	valueBearing := observeFirstResponseClass(
		request,
		protocol.FrameTypeInitiatorTarget,
		DedupOutcomeSuccess,
		protocol.Frame{Data: []byte{0x29, 0x02, 0x00, 0x7F}},
		true,
	)
	if valueBearing != DedupResponseValueBearing {
		t.Fatalf("value-bearing response class = %q; want %q", valueBearing, DedupResponseValueBearing)
	}

	singleByteValue := observeFirstResponseClass(
		request,
		protocol.FrameTypeInitiatorTarget,
		DedupOutcomeSuccess,
		protocol.Frame{Data: []byte{0x7F}},
		true,
	)
	if singleByteValue != DedupResponseValueBearing {
		t.Fatalf("single-byte response class = %q; want %q", singleByteValue, DedupResponseValueBearing)
	}

	emptyPayload := observeFirstResponseClass(
		request,
		protocol.FrameTypeInitiatorTarget,
		DedupOutcomeSuccess,
		protocol.Frame{Data: nil},
		true,
	)
	if emptyPayload != DedupResponseHeaderOnly {
		t.Fatalf("empty response class = %q; want %q", emptyPayload, DedupResponseHeaderOnly)
	}
}

func TestObserveFirstResponseClass_B524ReadMapsHeaderValueAndAmbiguous(t *testing.T) {
	request := protocol.Frame{
		Source:    0x10,
		Target:    0x15,
		Primary:   0xB5,
		Secondary: 0x24,
		Data:      []byte{0x06, 0x00, 0x03, 0x01, 0x1C, 0x00},
	}

	headerOnly := observeFirstResponseClass(
		request,
		protocol.FrameTypeInitiatorTarget,
		DedupOutcomeSuccess,
		protocol.Frame{Data: []byte{0x42, 0x01, 0x03, 0x1C, 0x00}},
		true,
	)
	if headerOnly != DedupResponseHeaderOnly {
		t.Fatalf("header-only response class = %q; want %q", headerOnly, DedupResponseHeaderOnly)
	}

	valueBearing := observeFirstResponseClass(
		request,
		protocol.FrameTypeInitiatorTarget,
		DedupOutcomeSuccess,
		protocol.Frame{Data: []byte{0x42, 0x01, 0x03, 0x1C, 0x00, 0x55}},
		true,
	)
	if valueBearing != DedupResponseValueBearing {
		t.Fatalf("value-bearing response class = %q; want %q", valueBearing, DedupResponseValueBearing)
	}

	ambiguous := observeFirstResponseClass(
		request,
		protocol.FrameTypeInitiatorTarget,
		DedupOutcomeSuccess,
		protocol.Frame{Data: []byte{0x42, 0x01, 0x04, 0x1C, 0x00, 0x55}},
		true,
	)
	if ambiguous != DedupResponseErrorOrAmbiguous {
		t.Fatalf("ambiguous response class = %q; want %q", ambiguous, DedupResponseErrorOrAmbiguous)
	}
}

func TestObserveFirstFamilyPolicy_B516UsesEnergyMergeOnly(t *testing.T) {
	request := protocol.Frame{
		Source:    0x15,
		Target:    0xFE,
		Primary:   0xB5,
		Secondary: 0x16,
		Data:      []byte{0x01, 0x02},
	}

	policy := observeFirstFamilyPolicy(
		ObserveFirstTrafficScopePassive,
		request,
		DedupResponseValueBearing,
		WatchObservation{State: WatchObservationStateCatalogMiss},
		DefaultObserveFirstFeatureFlags(),
	)
	if policy.Family != ObserveFirstFamilyB516 {
		t.Fatalf("Family = %q; want %q", policy.Family, ObserveFirstFamilyB516)
	}
	if policy.CorrelationPolicy != WatchCorrelationPolicyBroadcastSelector {
		t.Fatalf("CorrelationPolicy = %q; want %q", policy.CorrelationPolicy, WatchCorrelationPolicyBroadcastSelector)
	}
	if policy.DirectApplyPolicy != ObserveFirstDirectApplyPolicyEnergyMergeOnly {
		t.Fatalf("DirectApplyPolicy = %q; want %q", policy.DirectApplyPolicy, ObserveFirstDirectApplyPolicyEnergyMergeOnly)
	}
	if policy.UsesRuntimeExternalWritePolicy {
		t.Fatal("UsesRuntimeExternalWritePolicy = true; want false")
	}
}

func TestObserveFirstFamilyPolicy_B524ReadAndObservedWriteStaySeparated(t *testing.T) {
	readRequest := protocol.Frame{
		Source:    0x31,
		Target:    0x15,
		Primary:   0xB5,
		Secondary: 0x24,
		Data:      []byte{0x06, 0x00, 0x03, 0x01, 0x1C, 0x00},
	}
	readPolicy := observeFirstFamilyPolicy(
		ObserveFirstTrafficScopePassive,
		readRequest,
		DedupResponseValueBearing,
		testActiveStateObservation(NewB524WatchKey(0x15, 0x06, 0x03, 0x01, 0x001C)),
		DefaultObserveFirstFeatureFlags(),
	)
	if readPolicy.DirectApplyPolicy != ObserveFirstDirectApplyPolicyStateDefault {
		t.Fatalf("B524 read DirectApplyPolicy = %q; want %q", readPolicy.DirectApplyPolicy, ObserveFirstDirectApplyPolicyStateDefault)
	}
	if readPolicy.UsesRuntimeExternalWritePolicy {
		t.Fatal("B524 read UsesRuntimeExternalWritePolicy = true; want false")
	}

	writeRequest := protocol.Frame{
		Source:    0x10,
		Target:    0x15,
		Primary:   0xB5,
		Secondary: 0x24,
		Data:      []byte{0x06, 0x01, 0x03, 0x01, 0x1C, 0x00},
	}
	for _, policyValue := range []ObserveFirstExternalWritePolicy{
		ObserveFirstExternalWritePolicyInvalidateOnly,
		ObserveFirstExternalWritePolicyRecordOnly,
		ObserveFirstExternalWritePolicyRecordAndInvalidate,
	} {
		flags := NormalizeObserveFirstFeatureFlags(true, true, false, policyValue)
		writePolicy := observeFirstFamilyPolicy(
			ObserveFirstTrafficScopePassive,
			writeRequest,
			DedupResponseHeaderOnly,
			testActiveStateObservation(NewB524WatchKey(0x15, 0x06, 0x03, 0x01, 0x001C)),
			flags,
		)
		if writePolicy.DirectApplyPolicy != ObserveFirstDirectApplyPolicyNever {
			t.Fatalf("B524 write DirectApplyPolicy = %q; want %q for %q", writePolicy.DirectApplyPolicy, ObserveFirstDirectApplyPolicyNever, policyValue)
		}
		if !writePolicy.UsesRuntimeExternalWritePolicy {
			t.Fatalf("B524 write UsesRuntimeExternalWritePolicy = false; want true for %q", policyValue)
		}
		if writePolicy.EffectiveExternalWritePolicy != policyValue {
			t.Fatalf("B524 write EffectiveExternalWritePolicy = %q; want %q", writePolicy.EffectiveExternalWritePolicy, policyValue)
		}
	}
}

func TestObserveFirstFamilyPolicy_B509DirectApplyRequiresPayloadBearingRead(t *testing.T) {
	readRequest := protocol.Frame{
		Source:    0x31,
		Target:    0x08,
		Primary:   0xB5,
		Secondary: 0x09,
		Data:      []byte{0x29, 0x02, 0x00},
	}

	headerOnly := observeFirstFamilyPolicy(
		ObserveFirstTrafficScopePassive,
		readRequest,
		DedupResponseHeaderOnly,
		testActiveStateObservation(NewB509WatchKey(0x08, 0x0200)),
		DefaultObserveFirstFeatureFlags(),
	)
	if headerOnly.DirectApplyPolicy != ObserveFirstDirectApplyPolicyNever {
		t.Fatalf("B509 header-only DirectApplyPolicy = %q; want %q", headerOnly.DirectApplyPolicy, ObserveFirstDirectApplyPolicyNever)
	}

	valueBearing := observeFirstFamilyPolicy(
		ObserveFirstTrafficScopePassive,
		readRequest,
		DedupResponseValueBearing,
		testActiveStateObservation(NewB509WatchKey(0x08, 0x0200)),
		DefaultObserveFirstFeatureFlags(),
	)
	if valueBearing.DirectApplyPolicy != ObserveFirstDirectApplyPolicyStateDefault {
		t.Fatalf("B509 value-bearing DirectApplyPolicy = %q; want %q", valueBearing.DirectApplyPolicy, ObserveFirstDirectApplyPolicyStateDefault)
	}

	writeRequest := protocol.Frame{
		Source:    0x31,
		Target:    0x08,
		Primary:   0xB5,
		Secondary: 0x09,
		Data:      []byte{0x0E, 0x02, 0x00, 0x55},
	}
	writePolicy := observeFirstFamilyPolicy(
		ObserveFirstTrafficScopePassive,
		writeRequest,
		DedupResponseHeaderOnly,
		testActiveStateObservation(NewB509WatchKey(0x08, 0x0200)),
		NormalizeObserveFirstFeatureFlags(true, true, false, ObserveFirstExternalWritePolicyRecordAndInvalidate),
	)
	if writePolicy.DirectApplyPolicy != ObserveFirstDirectApplyPolicyNever {
		t.Fatalf("B509 write DirectApplyPolicy = %q; want %q", writePolicy.DirectApplyPolicy, ObserveFirstDirectApplyPolicyNever)
	}
	if !writePolicy.UsesRuntimeExternalWritePolicy {
		t.Fatal("B509 write UsesRuntimeExternalWritePolicy = false; want true")
	}
}

func TestObserveFirstFamilyPolicy_B555UsesConservativeRecordInvalidate(t *testing.T) {
	request := protocol.Frame{
		Source:    0x10,
		Target:    0x15,
		Primary:   0xB5,
		Secondary: 0x55,
		Data:      []byte{0xA3, 0x01},
	}

	policy := observeFirstFamilyPolicy(
		ObserveFirstTrafficScopePassive,
		request,
		DedupResponseHeaderOnly,
		WatchObservation{State: WatchObservationStateCatalogMiss},
		DefaultObserveFirstFeatureFlags(),
	)
	if policy.Family != ObserveFirstFamilyB555 {
		t.Fatalf("Family = %q; want %q", policy.Family, ObserveFirstFamilyB555)
	}
	if policy.CorrelationPolicy != WatchCorrelationPolicyRecordInvalidate {
		t.Fatalf("CorrelationPolicy = %q; want %q", policy.CorrelationPolicy, WatchCorrelationPolicyRecordInvalidate)
	}
	if policy.DirectApplyPolicy != ObserveFirstDirectApplyPolicyNever {
		t.Fatalf("DirectApplyPolicy = %q; want %q", policy.DirectApplyPolicy, ObserveFirstDirectApplyPolicyNever)
	}
}

func TestObserveFirstFamilyPolicy_B524StateDefaultRequiresActiveDescriptorBackedStateRead(t *testing.T) {
	readRequest := protocol.Frame{
		Source:    0x31,
		Target:    0x15,
		Primary:   0xB5,
		Secondary: 0x24,
		Data:      []byte{0x06, 0x00, 0x03, 0x01, 0x1C, 0x00},
	}
	baseFlags := DefaultObserveFirstFeatureFlags()
	cases := []struct {
		name        string
		request     protocol.Frame
		observation WatchObservation
	}{
		{
			name:        "catalog miss",
			request:     readRequest,
			observation: WatchObservation{State: WatchObservationStateCatalogMiss},
		},
		{
			name:    "inactive descriptor",
			request: readRequest,
			observation: WatchObservation{
				State: WatchObservationStateInactive,
				Descriptor: WatchDescriptor{
					Key:               NewB524WatchKey(0x15, 0x06, 0x03, 0x01, 0x001C),
					SemanticClass:     WatchSemanticClassState,
					CorrelationPolicy: WatchCorrelationPolicyRequestResponse,
					DirectApplyPolicy: WatchDirectApplyPolicyStateDefault,
				},
				HasDescriptor: true,
			},
		},
		{
			name:    "config descriptor",
			request: readRequest,
			observation: WatchObservation{
				State: WatchObservationStateActive,
				Descriptor: WatchDescriptor{
					Key:               NewB524WatchKey(0x15, 0x06, 0x03, 0x01, 0x001C),
					SemanticClass:     WatchSemanticClassConfig,
					CorrelationPolicy: WatchCorrelationPolicyRequestResponse,
					DirectApplyPolicy: WatchDirectApplyPolicyStateDefault,
				},
				HasDescriptor: true,
			},
		},
		{
			name:    "timer read",
			request: protocol.Frame{Source: 0x31, Target: 0x15, Primary: 0xB5, Secondary: 0x24, Data: []byte{0x03, 0x01, 0x00}},
			observation: WatchObservation{
				State: WatchObservationStateActive,
				Descriptor: WatchDescriptor{
					Key:               NewB524WatchKey(0x15, 0x03, 0x03, 0x01, 0x0000),
					SemanticClass:     WatchSemanticClassState,
					CorrelationPolicy: WatchCorrelationPolicyRequestResponse,
					DirectApplyPolicy: WatchDirectApplyPolicyStateDefault,
				},
				HasDescriptor: true,
			},
		},
		{
			name:    "write",
			request: protocol.Frame{Source: 0x10, Target: 0x15, Primary: 0xB5, Secondary: 0x24, Data: []byte{0x06, 0x01, 0x03, 0x01, 0x1C, 0x00}},
			observation: WatchObservation{
				State: WatchObservationStateActive,
				Descriptor: WatchDescriptor{
					Key:               NewB524WatchKey(0x15, 0x06, 0x03, 0x01, 0x001C),
					SemanticClass:     WatchSemanticClassState,
					CorrelationPolicy: WatchCorrelationPolicyRequestResponse,
					DirectApplyPolicy: WatchDirectApplyPolicyStateDefault,
				},
				HasDescriptor: true,
			},
		},
	}
	for _, testCase := range cases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			policy := observeFirstFamilyPolicy(
				ObserveFirstTrafficScopePassive,
				testCase.request,
				DedupResponseValueBearing,
				testCase.observation,
				baseFlags,
			)
			if policy.DirectApplyPolicy != ObserveFirstDirectApplyPolicyNever {
				t.Fatalf("DirectApplyPolicy = %q; want %q", policy.DirectApplyPolicy, ObserveFirstDirectApplyPolicyNever)
			}
		})
	}
}
