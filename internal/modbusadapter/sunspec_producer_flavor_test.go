package modbusadapter

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	sunspectest "github.com/Project-Helianthus/helianthus-ebusgateway/internal/modbusadapter/testfixture"
	modbusreg "github.com/Project-Helianthus/helianthus-modbusreg"
)

// froniusFlavorEvaluationIDs is the registry's flavor evaluation order.
var froniusFlavorEvaluationIDs = []string{
	modbusreg.SunSpecFroniusObservedFlavorID,
	modbusreg.SunSpecFroniusObservedFlavorV11ID,
	modbusreg.SunSpecFroniusObservedFlavorV12ID,
}

func froniusFlavorEvaluations(reasons ...modbusreg.SunSpecFroniusFlavorReason) []SunSpecFlavorEvaluation {
	evaluations := make([]SunSpecFlavorEvaluation, len(reasons))
	for index, reason := range reasons {
		evaluations[index] = SunSpecFlavorEvaluation{FlavorID: froniusFlavorEvaluationIDs[index], Reason: reason}
	}
	return evaluations
}

func startFroniusSunSpecProducer(t *testing.T, words []uint16) (*Adapter, *SunSpecProducer) {
	t.Helper()
	listener, _ := serveSunSpecChain(t, words)
	adapter, err := Start(context.Background(), integrationConfig(t, "tcp://"+listener.Addr().String()), realDialer, realFactory)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = adapter.Close() })
	producer, err := NewSunSpecProducer(adapter, SunSpecProducerConfig{
		UnitID: 1, AuthorizationScope: "smoke:fronius-readonly", ReadTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("NewSunSpecProducer: %v", err)
	}
	return adapter, producer
}

func TestSunSpecProducerQualifiesFroniusCurrentFirmwareTupleThroughRegistry(t *testing.T) {
	adapter, producer := startFroniusSunSpecProducer(t,
		sunspectest.ObservedFroniusFloatControlsWordsForFirmware(sunspectest.FroniusFirmwareCurrent))

	result, err := producer.Qualify(context.Background(), SunSpecPollIdentity{PollGeneration: 61, DeadlineIdentity: 161})
	if err != nil {
		t.Fatalf("Qualify(current firmware tuple): %v", err)
	}
	if result.Outcome != SunSpecQualificationGO || result.ObservationCount != 1 || result.SampleID == "" ||
		result.CapabilityID != modbusreg.SunSpecThreePhaseMonitoringCapabilityID ||
		result.CapabilityReason != modbusreg.SunSpecCapabilityReasonAdmitted ||
		result.FlavorID != modbusreg.SunSpecFroniusObservedFlavorV12ID ||
		result.FlavorReason != modbusreg.SunSpecFroniusFlavorReasonMatched {
		t.Fatalf("qualification result = %#v; want one GO observation on flavor 1.2.0", result)
	}
	if want := froniusFlavorEvaluations(
		modbusreg.SunSpecFroniusFlavorReasonFirmwareMismatch,
		modbusreg.SunSpecFroniusFlavorReasonFirmwareMismatch,
		modbusreg.SunSpecFroniusFlavorReasonMatched,
	); !reflect.DeepEqual(result.FlavorEvaluations, want) {
		t.Fatalf("flavor evaluations = %#v; want %#v", result.FlavorEvaluations, want)
	}
	if got := sunSpecWireKeys(result.Chain.Occurrences()); !reflect.DeepEqual(got, []modbusreg.SunSpecWireKey{
		{ModelID: 1, ModelLength: 65}, {ModelID: 113, ModelLength: 60},
		{ModelID: 120, ModelLength: 26}, {ModelID: 121, ModelLength: 30},
		{ModelID: 122, ModelLength: 44}, {ModelID: 123, ModelLength: 24},
		{ModelID: 160, ModelLength: 88}, {ModelID: 124, ModelLength: 24},
	}) {
		t.Fatalf("published SunSpec chain = %v; want the walked 1.2.0 chain", got)
	}

	// A terminal GO is publishable only with its registry-owned capture
	// retained and serializable under the new flavor.
	retained, encoded, ok := adapter.SunSpecQualificationObservation(result.CapabilityID, result.SampleID)
	if !ok || retained.SampleID() != result.SampleID || retained.Flavor().FlavorID() != modbusreg.SunSpecFroniusObservedFlavorV12ID {
		t.Fatalf("retained observation ok=%v flavor=%q; want sample %q on flavor 1.2.0", ok, retained.Flavor().FlavorID(), result.SampleID)
	}
	if !strings.Contains(string(encoded), `"flavor_id":"`+modbusreg.SunSpecFroniusObservedFlavorV12ID+`"`) {
		t.Fatal("retained serialized evidence does not carry flavor 1.2.0")
	}
}

func TestSunSpecProducerReportsRegistryFlavorEvaluationsInRegistryOrder(t *testing.T) {
	const (
		firmwareMismatch = modbusreg.SunSpecFroniusFlavorReasonFirmwareMismatch
		chainMismatch    = modbusreg.SunSpecFroniusFlavorReasonChainMismatch
		matched          = modbusreg.SunSpecFroniusFlavorReasonMatched
	)
	for _, test := range []struct {
		name            string
		words           []uint16
		wantOutcome     SunSpecQualificationOutcome
		wantFlavorID    string
		wantFlavor      modbusreg.SunSpecFroniusFlavorReason
		wantEvaluations []SunSpecFlavorEvaluation
	}{
		{
			name:         "GO flavor 1.0.0",
			words:        sunspectest.ObservedFroniusFloatWords(),
			wantOutcome:  SunSpecQualificationGO,
			wantFlavorID: modbusreg.SunSpecFroniusObservedFlavorID, wantFlavor: matched,
			wantEvaluations: froniusFlavorEvaluations(matched, chainMismatch, firmwareMismatch),
		},
		{
			name:         "GO flavor 1.1.0",
			words:        sunspectest.ObservedFroniusFloatControlsWords(),
			wantOutcome:  SunSpecQualificationGO,
			wantFlavorID: modbusreg.SunSpecFroniusObservedFlavorV11ID, wantFlavor: matched,
			wantEvaluations: froniusFlavorEvaluations(chainMismatch, matched, firmwareMismatch),
		},
		{
			name:            "NO_GO at a later patch firmware",
			words:           sunspectest.ObservedFroniusFloatControlsWordsForFirmware("1.42.10-1"),
			wantOutcome:     SunSpecQualificationNoGo,
			wantFlavor:      firmwareMismatch,
			wantEvaluations: froniusFlavorEvaluations(firmwareMismatch, firmwareMismatch, firmwareMismatch),
		},
		{
			name:            "NO_GO at a prefix of the current firmware",
			words:           sunspectest.ObservedFroniusFloatControlsWordsForFirmware("1.42.9"),
			wantOutcome:     SunSpecQualificationNoGo,
			wantFlavor:      firmwareMismatch,
			wantEvaluations: froniusFlavorEvaluations(firmwareMismatch, firmwareMismatch, firmwareMismatch),
		},
		{
			name:            "NO_GO at a different revision of the current firmware",
			words:           sunspectest.ObservedFroniusFloatControlsWordsForFirmware("1.42.9-2"),
			wantOutcome:     SunSpecQualificationNoGo,
			wantFlavor:      firmwareMismatch,
			wantEvaluations: froniusFlavorEvaluations(firmwareMismatch, firmwareMismatch, firmwareMismatch),
		},
		{
			name:            "NO_GO at the current firmware without the controls model",
			words:           sunspectest.ObservedFroniusFloatWordsForFirmware(sunspectest.FroniusFirmwareCurrent),
			wantOutcome:     SunSpecQualificationNoGo,
			wantFlavor:      "",
			wantEvaluations: froniusFlavorEvaluations(firmwareMismatch, firmwareMismatch, chainMismatch),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, producer := startFroniusSunSpecProducer(t, test.words)
			result, err := producer.Qualify(context.Background(), SunSpecPollIdentity{PollGeneration: 62, DeadlineIdentity: 162})
			if err != nil {
				t.Fatalf("Qualify: %v", err)
			}
			if result.Outcome != test.wantOutcome ||
				result.CapabilityReason != modbusreg.SunSpecCapabilityReasonAdmitted ||
				result.FlavorID != test.wantFlavorID || result.FlavorReason != test.wantFlavor ||
				!reflect.DeepEqual(result.FlavorEvaluations, test.wantEvaluations) {
				t.Fatalf("result = %#v; want outcome=%s flavor=%q reason=%q evaluations=%#v",
					result, test.wantOutcome, test.wantFlavorID, test.wantFlavor, test.wantEvaluations)
			}
			if test.wantOutcome == SunSpecQualificationNoGo {
				if result.ObservationCount != 0 || result.SampleID != "" {
					t.Fatalf("NO_GO result = %#v; want no observation", result)
				}
				assertNoSunSpecQualificationChain(t, result)
			}
		})
	}
}

func TestSunSpecProducerLeavesFlavorEvaluationsEmptyBeforeCapabilityAdmission(t *testing.T) {
	_, producer := startFroniusSunSpecProducer(t, sunSpecWords(1, 65, 101, 50, 0xffff, 0))
	result, err := producer.Qualify(context.Background(), SunSpecPollIdentity{PollGeneration: 63, DeadlineIdentity: 163})
	if err != nil {
		t.Fatalf("Qualify: %v", err)
	}
	if result.Outcome == SunSpecQualificationGO || result.CapabilityReason == modbusreg.SunSpecCapabilityReasonAdmitted ||
		len(result.FlavorEvaluations) != 0 || result.FlavorID != "" {
		t.Fatalf("result = %#v; want a refused capability without flavor evaluations", result)
	}
}
