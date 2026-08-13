package main

import (
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
)

func TestDeriveFM5Interpretation_ClosedReasonsAndPrecedence(t *testing.T) {
	configInterpretable := uint16(2)
	configUnsupported := uint16(3)
	tests := []struct {
		name                string
		controllerReachable bool
		moduleConfig        *uint16
		solarReadable       bool
		cylindersReadable   bool
		hasEvidence         bool
		wantMode            graphql.Fm5SemanticMode
		wantReason          graphql.Fm5SemanticDegradedReason
	}{
		{"absent without evidence", false, nil, false, false, false, graphql.Fm5SemanticModeAbsent, ""},
		{"controller unavailable", false, nil, false, false, true, graphql.Fm5SemanticModeGPIOOnly, graphql.Fm5SemanticDegradedReasonControllerUnreachable},
		{"configuration unavailable", true, nil, false, false, true, graphql.Fm5SemanticModeGPIOOnly, graphql.Fm5SemanticDegradedReasonConfigurationUnavailable},
		{"configuration deliberately unsupported", true, &configUnsupported, false, false, true, graphql.Fm5SemanticModeGPIOOnly, graphql.Fm5SemanticDegradedReasonConfigurationNotInterpretable},
		{"solar read failed before cylinder", true, &configInterpretable, false, false, true, graphql.Fm5SemanticModeGPIOOnly, graphql.Fm5SemanticDegradedReasonSolarAcquisitionFailed},
		{"cylinder read failed", true, &configInterpretable, true, false, true, graphql.Fm5SemanticModeGPIOOnly, graphql.Fm5SemanticDegradedReasonCylinderAcquisitionFailed},
		{"coherent interpretation", true, &configInterpretable, true, true, true, graphql.Fm5SemanticModeInterpreted, ""},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := deriveFM5Interpretation(
				test.controllerReachable,
				test.moduleConfig,
				test.solarReadable,
				test.cylindersReadable,
				test.hasEvidence,
				"acq-42",
			)
			if got.Mode != test.wantMode || got.DegradedReason != test.wantReason {
				t.Fatalf("deriveFM5Interpretation() = %#v; want mode=%s reason=%s", got, test.wantMode, test.wantReason)
			}
			if got.EvidenceRevision != "acq-42" {
				t.Fatalf("evidence revision = %q; want acq-42", got.EvidenceRevision)
			}
		})
	}
}

func TestApplyFM5Acquisition_RetainsTransientAndWithdrawsStructural(t *testing.T) {
	collector := 61.5
	temperature := 47.25
	previousSolar := &vaillantSolarSnapshot{CollectorTemperatureC: &collector}
	previousCylinders := map[byte]*vaillantCylinderSnapshot{
		0: {Instance: 0, TemperatureC: &temperature},
	}

	retainedSolar, retainedCylinders := applyFM5Acquisition(
		previousSolar,
		previousCylinders,
		nil,
		nil,
		graphql.Fm5Interpretation{
			Mode:             graphql.Fm5SemanticModeGPIOOnly,
			DegradedReason:   graphql.Fm5SemanticDegradedReasonSolarAcquisitionFailed,
			EvidenceRevision: "acq-43",
		},
	)
	if retainedSolar == nil || retainedSolar.CollectorTemperatureC == nil || *retainedSolar.CollectorTemperatureC != collector {
		t.Fatalf("transient solar retention = %#v; want prior coherent snapshot", retainedSolar)
	}
	if retainedCylinders[0] == nil || retainedCylinders[0].TemperatureC == nil || *retainedCylinders[0].TemperatureC != temperature {
		t.Fatalf("transient cylinder retention = %#v; want prior coherent instance", retainedCylinders)
	}

	withdrawnSolar, withdrawnCylinders := applyFM5Acquisition(
		previousSolar,
		previousCylinders,
		nil,
		nil,
		graphql.Fm5Interpretation{
			Mode:             graphql.Fm5SemanticModeGPIOOnly,
			DegradedReason:   graphql.Fm5SemanticDegradedReasonConfigurationNotInterpretable,
			EvidenceRevision: "acq-44",
		},
	)
	if withdrawnSolar != nil || len(withdrawnCylinders) != 0 {
		t.Fatalf("structural withdrawal = solar %#v cylinders %#v; want empty", withdrawnSolar, withdrawnCylinders)
	}
}

func TestNewGatewayBuildInfo_RejectsMissingReleaseAuthority(t *testing.T) {
	if _, err := newGatewayBuildInfo("", "build-1"); err == nil {
		t.Fatal("empty release version accepted")
	}
	info, err := newGatewayBuildInfo("0.6.42", "build-1")
	if err != nil {
		t.Fatalf("newGatewayBuildInfo: %v", err)
	}
	if info.ReleaseVersion != "0.6.42" || info.BuildID != "build-1" {
		t.Fatalf("build info = %#v; want one injected version/build identity", info)
	}
	dev, err := newGatewayBuildInfo("dev", "unknown")
	if err != nil || dev.ReleaseVersion != "dev" {
		t.Fatalf("development build info = %#v, %v", dev, err)
	}
}

type legacyOnlySemanticProvider struct {
	graphql.SemanticProvider
}

func TestPortalFM5Interpretation_LegacyGPIOOnlyIsExplained(t *testing.T) {
	provider := graphql.NewLiveSemanticProvider()
	provider.SetFM5SemanticMode(graphql.Fm5SemanticModeGPIOOnly)
	legacy := legacyOnlySemanticProvider{SemanticProvider: provider}

	verdict := portalFM5Interpretation(legacy)
	if err := verdict.Validate(); err != nil {
		t.Fatalf("Portal fallback verdict invalid: %v", err)
	}
	if verdict.DegradedReason != graphql.Fm5SemanticDegradedReasonIncoherentAcquisition {
		t.Fatalf("Portal fallback reason = %q; want %q", verdict.DegradedReason, graphql.Fm5SemanticDegradedReasonIncoherentAcquisition)
	}
}
