package main

import (
	"context"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
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
		evidenceStale       bool
		incoherent          bool
		wantMode            graphql.Fm5SemanticMode
		wantReason          graphql.Fm5SemanticDegradedReason
	}{
		{"absent without evidence", false, nil, false, false, false, false, false, graphql.Fm5SemanticModeAbsent, ""},
		{"controller unavailable", false, nil, false, false, true, false, false, graphql.Fm5SemanticModeGPIOOnly, graphql.Fm5SemanticDegradedReasonControllerUnreachable},
		{"configuration unavailable", true, nil, false, false, true, false, false, graphql.Fm5SemanticModeGPIOOnly, graphql.Fm5SemanticDegradedReasonConfigurationUnavailable},
		{"configuration deliberately unsupported", true, &configUnsupported, false, false, true, false, false, graphql.Fm5SemanticModeGPIOOnly, graphql.Fm5SemanticDegradedReasonConfigurationNotInterpretable},
		{"stale identity before family reads", true, &configInterpretable, false, false, true, true, false, graphql.Fm5SemanticModeGPIOOnly, graphql.Fm5SemanticDegradedReasonEvidenceStale},
		{"solar read failed before cylinder", true, &configInterpretable, false, false, true, false, false, graphql.Fm5SemanticModeGPIOOnly, graphql.Fm5SemanticDegradedReasonSolarAcquisitionFailed},
		{"cylinder read failed", true, &configInterpretable, true, false, true, false, false, graphql.Fm5SemanticModeGPIOOnly, graphql.Fm5SemanticDegradedReasonCylinderAcquisitionFailed},
		{"generation changed during acquisition", true, &configInterpretable, true, true, true, false, true, graphql.Fm5SemanticModeGPIOOnly, graphql.Fm5SemanticDegradedReasonIncoherentAcquisition},
		{"coherent interpretation", true, &configInterpretable, true, true, true, false, false, graphql.Fm5SemanticModeInterpreted, ""},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := deriveFM5Interpretation(
				test.controllerReachable,
				test.moduleConfig,
				test.solarReadable,
				test.cylindersReadable,
				test.hasEvidence,
				test.evidenceStale,
				test.incoherent,
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

func TestFM5EvidenceCapture_DetectsStaleAndGenerationChange(t *testing.T) {
	now := time.Date(2026, 8, 14, 1, 0, 0, 0, time.UTC)
	classAddress := uint8(circuitManagingDeviceVR71Address)
	config := uint16(2)
	poller := &vaillantSemanticPoller{
		controller:            0x15,
		system:                &vaillantSystemSnapshot{ModuleConfigurationVR71: &config},
		radioDevices:          map[radioDeviceKey]*vaillantRadioDeviceSnapshot{{Group: remoteFunctionalModules.group}: {DeviceClassAddress: &classAddress}},
		fm5EvidenceTTL:        5 * time.Minute,
		fm5IdentityObservedAt: now.Add(-6 * time.Minute),
		fm5EvidenceGeneration: 7,
		nowFn:                 func() time.Time { return now },
	}

	before := poller.captureFM5Evidence()
	if !before.staleAt(now, poller.fm5EvidenceTTL) {
		t.Fatal("retained FM5 identity outside TTL was not stale")
	}
	poller.mu.Lock()
	poller.fm5EvidenceGeneration++
	if before.matchesLockedPoller(poller) {
		poller.mu.Unlock()
		t.Fatal("final locked FM5 generation revalidation accepted a changed generation")
	}
	poller.mu.Unlock()
	after := poller.captureFM5Evidence()
	if before.sameGeneration(after) {
		t.Fatal("FM5 evidence generation change was not detected")
	}
}

func TestRefreshFM5Semantic_StaleEvidenceSkipsReadsAndRetainsCoherentFamily(t *testing.T) {
	now := time.Date(2026, 8, 14, 1, 0, 0, 0, time.UTC)
	classAddress := uint8(circuitManagingDeviceVR71Address)
	config := uint16(2)
	collector := 61.5
	temperature := 47.25
	provider := graphql.NewLiveSemanticProvider()
	poller := &vaillantSemanticPoller{
		reg:                   registry.NewDeviceRegistry(nil),
		provider:              provider,
		controller:            0x15,
		system:                &vaillantSystemSnapshot{Controller: 0x15, ModuleConfigurationVR71: &config},
		radioDevices:          map[radioDeviceKey]*vaillantRadioDeviceSnapshot{{Group: remoteFunctionalModules.group}: {DeviceClassAddress: &classAddress}},
		solar:                 &vaillantSolarSnapshot{CollectorTemperatureC: &collector},
		solarCylinders:        map[byte]*vaillantCylinderSnapshot{0: {Instance: 0, TemperatureC: &temperature}},
		fm5EvidenceTTL:        5 * time.Minute,
		fm5IdentityObservedAt: now.Add(-6 * time.Minute),
		fm5EvidenceGeneration: 7,
		nowFn:                 func() time.Time { return now },
	}

	poller.refreshFM5Semantic(context.Background())
	verdict := provider.FM5Interpretation()
	if verdict.Mode != graphql.Fm5SemanticModeGPIOOnly || verdict.DegradedReason != graphql.Fm5SemanticDegradedReasonEvidenceStale {
		t.Fatalf("stale refresh verdict = %#v; want GPIO_ONLY/EVIDENCE_STALE", verdict)
	}
	if solar := provider.Solar(); solar == nil || solar.CollectorTemperatureC == nil || *solar.CollectorTemperatureC != collector {
		t.Fatalf("stale refresh solar = %#v; want retained coherent snapshot", solar)
	}
	if cylinders := provider.Cylinders(); len(cylinders) != 1 || cylinders[0].TemperatureC == nil || *cylinders[0].TemperatureC != temperature {
		t.Fatalf("stale refresh cylinders = %#v; want retained coherent instance", cylinders)
	}
}

func TestCommitFM5Acquisition_RegistryMutationAfterPostReadCaptureIsIncoherent(t *testing.T) {
	classAddress := uint8(circuitManagingDeviceVR71Address)
	config := uint16(2)
	reg := registry.NewDeviceRegistry(nil)
	poller := &vaillantSemanticPoller{
		reg:            reg,
		controller:     0x15,
		system:         &vaillantSystemSnapshot{Controller: 0x15, ModuleConfigurationVR71: &config},
		radioDevices:   map[radioDeviceKey]*vaillantRadioDeviceSnapshot{{Group: remoteFunctionalModules.group}: {DeviceClassAddress: &classAddress}},
		solarCylinders: make(map[byte]*vaillantCylinderSnapshot),
		nowFn:          time.Now,
	}
	captured := poller.captureFM5Evidence()
	reg.Register(registry.DeviceInfo{
		Address:      circuitManagingDeviceVR71Address,
		Manufacturer: "Vaillant",
		DeviceID:     circuitManagingDeviceVR71ID,
	})

	verdict := poller.commitFM5Acquisition(captured, graphql.Fm5Interpretation{
		Mode:             graphql.Fm5SemanticModeInterpreted,
		EvidenceRevision: "fm5-g0-a1",
	}, &vaillantSolarSnapshot{}, map[byte]*vaillantCylinderSnapshot{})
	if verdict.Mode != graphql.Fm5SemanticModeGPIOOnly || verdict.DegradedReason != graphql.Fm5SemanticDegradedReasonIncoherentAcquisition {
		t.Fatalf("registry interleaving verdict = %#v; want GPIO_ONLY/INCOHERENT_ACQUISITION", verdict)
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
