package main

import (
	"context"
	"errors"
	"testing"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
)

func TestDeriveFM5Interpretation_CoherentClassificationsAndUnavailableBootstrap(t *testing.T) {
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
		{"incomplete bootstrap", false, nil, false, false, false, false, false, "", ""},
		{"no completed negative identity acquisition", true, &configInterpretable, false, false, false, false, false, "", ""},
		{"configuration deliberately unsupported", true, &configUnsupported, false, false, true, false, false, graphql.Fm5SemanticModeGPIOOnly, graphql.Fm5SemanticDegradedReasonConfigurationNotInterpretable},
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
			if test.wantMode == "" && got.EvidenceRevision != "" {
				t.Fatalf("unavailable bootstrap evidence revision = %q; want empty", got.EvidenceRevision)
			}
			if test.wantMode != "" && got.EvidenceRevision != "acq-42" {
				t.Fatalf("evidence revision = %q; want acq-42", got.EvidenceRevision)
			}
		})
	}
}

func TestDeriveFM5Interpretation_TransientReasonPrecedence(t *testing.T) {
	configInterpretable := uint16(2)
	tests := []struct {
		name                string
		controllerReachable bool
		moduleConfig        *uint16
		solarReadable       bool
		cylindersReadable   bool
		evidenceStale       bool
		incoherent          bool
		wantReason          graphql.Fm5SemanticDegradedReason
	}{
		{"controller unavailable", false, nil, false, false, false, false, graphql.Fm5SemanticDegradedReasonControllerUnreachable},
		{"configuration unavailable", true, nil, false, false, false, false, graphql.Fm5SemanticDegradedReasonConfigurationUnavailable},
		{"stale identity before family reads", true, &configInterpretable, false, false, true, false, graphql.Fm5SemanticDegradedReasonEvidenceStale},
		{"solar read failed before cylinder", true, &configInterpretable, false, false, false, false, graphql.Fm5SemanticDegradedReasonSolarAcquisitionFailed},
		{"cylinder read failed", true, &configInterpretable, true, false, false, false, graphql.Fm5SemanticDegradedReasonCylinderAcquisitionFailed},
		{"generation changed during acquisition", true, &configInterpretable, true, true, false, true, graphql.Fm5SemanticDegradedReasonIncoherentAcquisition},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := deriveFM5Interpretation(
				test.controllerReachable,
				test.moduleConfig,
				test.solarReadable,
				test.cylindersReadable,
				true,
				test.evidenceStale,
				test.incoherent,
				"acq-42",
			)
			if got.DegradedReason != test.wantReason {
				t.Fatalf("deriveFM5Interpretation() = %#v; want reason=%s", got, test.wantReason)
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
			Mode:             graphql.Fm5SemanticModeInterpreted,
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

func TestCommitFM5Acquisition_RetainsCoherentModeAcrossTransientFailures(t *testing.T) {
	config := uint16(2)
	tests := []struct {
		coherentMode graphql.Fm5SemanticMode
		reason       graphql.Fm5SemanticDegradedReason
	}{
		{graphql.Fm5SemanticModeInterpreted, graphql.Fm5SemanticDegradedReasonControllerUnreachable},
		{graphql.Fm5SemanticModeInterpreted, graphql.Fm5SemanticDegradedReasonConfigurationUnavailable},
		{graphql.Fm5SemanticModeInterpreted, graphql.Fm5SemanticDegradedReasonSolarAcquisitionFailed},
		{graphql.Fm5SemanticModeInterpreted, graphql.Fm5SemanticDegradedReasonCylinderAcquisitionFailed},
		{graphql.Fm5SemanticModeInterpreted, graphql.Fm5SemanticDegradedReasonEvidenceStale},
		{graphql.Fm5SemanticModeInterpreted, graphql.Fm5SemanticDegradedReasonIncoherentAcquisition},
		{graphql.Fm5SemanticModeAbsent, graphql.Fm5SemanticDegradedReasonControllerUnreachable},
	}

	for _, test := range tests {
		t.Run(string(test.coherentMode)+"/"+string(test.reason), func(t *testing.T) {
			collector := 61.5
			temperature := 47.25
			poller := &vaillantSemanticPoller{
				controller:            0x15,
				system:                &vaillantSystemSnapshot{Controller: 0x15, ModuleConfigurationVR71: &config},
				fm5Mode:               test.coherentMode,
				fm5Interpretation:     graphql.Fm5Interpretation{Mode: test.coherentMode, EvidenceRevision: "fm5-g7-a41"},
				fm5EvidenceGeneration: 7,
				solarCylinders:        make(map[byte]*vaillantCylinderSnapshot),
			}
			if test.coherentMode == graphql.Fm5SemanticModeInterpreted {
				poller.solar = &vaillantSolarSnapshot{CollectorTemperatureC: &collector}
				poller.solarCylinders[0] = &vaillantCylinderSnapshot{Instance: 0, TemperatureC: &temperature}
			}
			captured := fm5EvidenceCapture{
				controller:       0x15,
				moduleConfig:     &config,
				generation:       7,
				registryCoherent: true,
			}

			got := poller.commitFM5Acquisition(captured, graphql.Fm5Interpretation{
				Mode:             graphql.Fm5SemanticModeGPIOOnly,
				DegradedReason:   test.reason,
				EvidenceRevision: "fm5-g7-a42",
			}, nil, nil)

			if got.Mode != test.coherentMode || got.DegradedReason != test.reason {
				t.Fatalf("transient commit verdict = %#v; want retained %s/%s", got, test.coherentMode, test.reason)
			}
			if got.EvidenceRevision != "fm5-g7-a42" || got.EvidenceRevision == "fm5-g7-a41" {
				t.Fatalf("transient commit revision = %q; want advanced fm5-g7-a42", got.EvidenceRevision)
			}
			if poller.fm5Mode != test.coherentMode {
				t.Fatalf("legacy FM5 scalar = %s; want retained %s", poller.fm5Mode, test.coherentMode)
			}
			if test.coherentMode == graphql.Fm5SemanticModeInterpreted {
				if poller.solar == nil || poller.solar.CollectorTemperatureC == nil || *poller.solar.CollectorTemperatureC != collector {
					t.Fatalf("retained solar = %#v; want prior coherent value", poller.solar)
				}
				if poller.solarCylinders[0] == nil || poller.solarCylinders[0].TemperatureC == nil || *poller.solarCylinders[0].TemperatureC != temperature {
					t.Fatalf("retained cylinders = %#v; want prior coherent value", poller.solarCylinders)
				}
			}
		})
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
		fm5Mode:               graphql.Fm5SemanticModeInterpreted,
		fm5Interpretation: graphql.Fm5Interpretation{
			Mode:             graphql.Fm5SemanticModeInterpreted,
			EvidenceRevision: "fm5-g7-a41",
		},
		fm5EvidenceRevision: 41,
		nowFn:               func() time.Time { return now },
	}

	poller.refreshFM5Semantic(context.Background())
	verdict := provider.FM5Interpretation()
	if verdict.Mode != graphql.Fm5SemanticModeInterpreted || verdict.DegradedReason != graphql.Fm5SemanticDegradedReasonEvidenceStale {
		t.Fatalf("stale refresh verdict = %#v; want INTERPRETED/EVIDENCE_STALE", verdict)
	}
	if verdict.EvidenceRevision == "fm5-g7-a41" {
		t.Fatalf("stale refresh evidence revision = %q; want advanced revision", verdict.EvidenceRevision)
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
		reg:          reg,
		controller:   0x15,
		system:       &vaillantSystemSnapshot{Controller: 0x15, ModuleConfigurationVR71: &config},
		radioDevices: map[radioDeviceKey]*vaillantRadioDeviceSnapshot{{Group: remoteFunctionalModules.group}: {DeviceClassAddress: &classAddress}},
		fm5Mode:      graphql.Fm5SemanticModeInterpreted,
		fm5Interpretation: graphql.Fm5Interpretation{
			Mode:             graphql.Fm5SemanticModeInterpreted,
			EvidenceRevision: "fm5-g0-a0",
		},
		solarCylinders: make(map[byte]*vaillantCylinderSnapshot),
		nowFn:          time.Now,
	}
	captured := poller.captureFM5Evidence()
	if !captured.registryCoherent {
		t.Fatal("initial registry capture is incoherent")
	}
	reg.Register(registry.DeviceInfo{
		Address:      circuitManagingDeviceVR71Address,
		Manufacturer: "Vaillant",
		DeviceID:     circuitManagingDeviceVR71ID,
	})

	verdict := poller.commitFM5Acquisition(captured, graphql.Fm5Interpretation{
		Mode:             graphql.Fm5SemanticModeInterpreted,
		EvidenceRevision: "fm5-g0-a1",
	}, &vaillantSolarSnapshot{}, map[byte]*vaillantCylinderSnapshot{})
	if verdict.Mode != graphql.Fm5SemanticModeInterpreted || verdict.DegradedReason != graphql.Fm5SemanticDegradedReasonIncoherentAcquisition {
		t.Fatalf("registry interleaving verdict = %#v; want INTERPRETED/INCOHERENT_ACQUISITION", verdict)
	}
	if verdict.EvidenceRevision == "fm5-g0-a0" {
		t.Fatalf("registry interleaving revision = %q; want advanced revision", verdict.EvidenceRevision)
	}
}

func TestRefreshDiscovery_TransientRootFailureRetainsFM5ClassificationAndValues(t *testing.T) {
	config := uint16(2)
	classAddress := uint8(circuitManagingDeviceVR71Address)
	collector := 61.5
	temperature := 47.25
	provider := graphql.NewLiveSemanticProvider()
	previous := graphql.Fm5Interpretation{
		Mode:             graphql.Fm5SemanticModeInterpreted,
		EvidenceRevision: "fm5-g7-a41",
	}
	provider.SetFM5Interpretation(previous)
	provider.SetSolar(&graphql.SolarStatus{CollectorTemperatureC: &collector})
	provider.SetCylinders([]graphql.CylinderStatus{{Index: 0, TemperatureC: &temperature}})

	reg := registry.NewDeviceRegistry(nil)
	reg.Register(registry.DeviceInfo{Address: 0x15, Manufacturer: "Vaillant", DeviceID: "BASV2"})
	poller := &vaillantSemanticPoller{
		reg:                   reg,
		provider:              provider,
		controller:            0x15,
		system:                &vaillantSystemSnapshot{Controller: 0x15, ModuleConfigurationVR71: &config},
		radioDevices:          map[radioDeviceKey]*vaillantRadioDeviceSnapshot{{Group: remoteFunctionalModules.group}: {DeviceClassAddress: &classAddress}},
		zones:                 make(map[byte]*vaillantZoneSnapshot),
		presence:              make(map[byte]*zonePresenceRecord),
		circuits:              make(map[byte]*vaillantCircuitSnapshot),
		fm5Mode:               graphql.Fm5SemanticModeInterpreted,
		fm5Interpretation:     previous,
		fm5EvidenceRevision:   41,
		fm5EvidenceGeneration: 7,
		fm5IdentityObservedAt: time.Now(),
		solar:                 &vaillantSolarSnapshot{CollectorTemperatureC: &collector},
		solarCylinders:        map[byte]*vaillantCylinderSnapshot{0: {Instance: 0, TemperatureC: &temperature}},
		b524ProbeFn:           mockB524Probe(map[byte]bool{}, nil),
		nowFn:                 time.Now,
	}

	poller.refreshDiscovery(context.Background())

	verdict := provider.FM5Interpretation()
	if verdict.Mode != graphql.Fm5SemanticModeInterpreted || verdict.DegradedReason != graphql.Fm5SemanticDegradedReasonControllerUnreachable {
		t.Errorf("root-discovery failure verdict = %#v; want retained INTERPRETED/CONTROLLER_UNREACHABLE", verdict)
	}
	if verdict.EvidenceRevision == "" || verdict.EvidenceRevision == previous.EvidenceRevision {
		t.Errorf("root-discovery failure revision = %q; want advancement from %q", verdict.EvidenceRevision, previous.EvidenceRevision)
	}
	if got := provider.FM5SemanticMode(); got != graphql.Fm5SemanticModeInterpreted {
		t.Errorf("root-discovery failure legacy scalar = %s; want retained INTERPRETED", got)
	}
	if solar := provider.Solar(); solar == nil || solar.CollectorTemperatureC == nil || *solar.CollectorTemperatureC != collector {
		t.Errorf("root-discovery failure solar = %#v; want retained collector temperature %.2f", solar, collector)
	}
	if cylinders := provider.Cylinders(); len(cylinders) != 1 || cylinders[0].TemperatureC == nil || *cylinders[0].TemperatureC != temperature {
		t.Errorf("root-discovery failure cylinders = %#v; want retained temperature %.2f", cylinders, temperature)
	}
}

func TestRefreshFM5Semantic_StaleIdentityOutranksUnsupportedConfiguration(t *testing.T) {
	now := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)
	configUnsupported := uint16(3)
	classAddress := uint8(circuitManagingDeviceVR71Address)
	newPoller := func(provider *graphql.LiveSemanticProvider) *vaillantSemanticPoller {
		return &vaillantSemanticPoller{
			reg:                   registry.NewDeviceRegistry(nil),
			provider:              provider,
			controller:            0x15,
			system:                &vaillantSystemSnapshot{Controller: 0x15, ModuleConfigurationVR71: &configUnsupported},
			radioDevices:          map[radioDeviceKey]*vaillantRadioDeviceSnapshot{{Group: remoteFunctionalModules.group}: {DeviceClassAddress: &classAddress}},
			solarCylinders:        make(map[byte]*vaillantCylinderSnapshot),
			fm5EvidenceTTL:        5 * time.Minute,
			fm5IdentityObservedAt: now.Add(-6 * time.Minute),
			fm5EvidenceGeneration: 7,
			fm5EvidenceRevision:   41,
			nowFn:                 func() time.Time { return now },
		}
	}

	t.Run("bootstrap remains unavailable", func(t *testing.T) {
		provider := graphql.NewLiveSemanticProvider()
		poller := newPoller(provider)
		poller.refreshFM5Semantic(context.Background())
		if got := provider.FM5Interpretation(); got.Mode != "" || got.DegradedReason != "" || got.EvidenceRevision != "" {
			t.Fatalf("stale unsupported bootstrap verdict = %#v; want unavailable zero tuple", got)
		}
	})

	t.Run("coherent family retains prior classification and values", func(t *testing.T) {
		collector := 61.5
		temperature := 47.25
		provider := graphql.NewLiveSemanticProvider()
		poller := newPoller(provider)
		poller.fm5Mode = graphql.Fm5SemanticModeInterpreted
		poller.fm5Interpretation = graphql.Fm5Interpretation{
			Mode:             graphql.Fm5SemanticModeInterpreted,
			EvidenceRevision: "fm5-g7-a41",
		}
		poller.solar = &vaillantSolarSnapshot{CollectorTemperatureC: &collector}
		poller.solarCylinders[0] = &vaillantCylinderSnapshot{Instance: 0, TemperatureC: &temperature}

		poller.refreshFM5Semantic(context.Background())
		got := provider.FM5Interpretation()
		if got.Mode != graphql.Fm5SemanticModeInterpreted || got.DegradedReason != graphql.Fm5SemanticDegradedReasonEvidenceStale {
			t.Errorf("stale unsupported retained verdict = %#v; want INTERPRETED/EVIDENCE_STALE", got)
		}
		if got.EvidenceRevision == "" || got.EvidenceRevision == "fm5-g7-a41" {
			t.Errorf("stale unsupported revision = %q; want advancement", got.EvidenceRevision)
		}
		if solar := provider.Solar(); solar == nil || solar.CollectorTemperatureC == nil || *solar.CollectorTemperatureC != collector {
			t.Errorf("stale unsupported solar = %#v; want retained coherent value", solar)
		}
		if cylinders := provider.Cylinders(); len(cylinders) != 1 || cylinders[0].TemperatureC == nil || *cylinders[0].TemperatureC != temperature {
			t.Errorf("stale unsupported cylinders = %#v; want retained coherent value", cylinders)
		}
	})
}

func TestRefreshFM5Semantic_AbsenceRequiresFreshCompletedNegativeIdentityAcquisition(t *testing.T) {
	now := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)
	config := uint16(2)
	newPoller := func(provider *graphql.LiveSemanticProvider) *vaillantSemanticPoller {
		return &vaillantSemanticPoller{
			reg:            registry.NewDeviceRegistry(nil),
			provider:       provider,
			controller:     0x15,
			system:         &vaillantSystemSnapshot{Controller: 0x15, ModuleConfigurationVR71: &config},
			radioDevices:   make(map[radioDeviceKey]*vaillantRadioDeviceSnapshot),
			solarCylinders: make(map[byte]*vaillantCylinderSnapshot),
			fm5EvidenceTTL: 5 * time.Minute,
			nowFn:          func() time.Time { return now },
		}
	}

	provider := graphql.NewLiveSemanticProvider()
	poller := newPoller(provider)
	poller.refreshFM5Semantic(context.Background())
	if got := provider.FM5Interpretation(); got.Mode != "" || got.DegradedReason != "" || got.EvidenceRevision != "" {
		t.Errorf("unobserved startup verdict = %#v; want unavailable zero tuple", got)
	}

	provider = graphql.NewLiveSemanticProvider()
	poller = newPoller(provider)
	poller.startupRadioDevicesProbed = true
	poller.fm5IdentityScanComplete = true
	poller.fm5IdentityObservedAt = now
	poller.refreshFM5Semantic(context.Background())
	if got := provider.FM5Interpretation(); got.Mode != graphql.Fm5SemanticModeAbsent || got.DegradedReason != "" || got.EvidenceRevision == "" {
		t.Errorf("fresh completed negative verdict = %#v; want healthy ABSENT with revision", got)
	}
}

func TestFM5Acquisition_EmptyEvidenceGenerationInterleavingRetainsPriorClassification(t *testing.T) {
	configUnsupported := uint16(3)
	collector := 61.5
	temperature := 47.25
	reg := registry.NewDeviceRegistry(nil)
	poller := &vaillantSemanticPoller{
		reg:          reg,
		controller:   0x15,
		system:       &vaillantSystemSnapshot{Controller: 0x15, ModuleConfigurationVR71: &configUnsupported},
		radioDevices: make(map[radioDeviceKey]*vaillantRadioDeviceSnapshot),
		fm5Mode:      graphql.Fm5SemanticModeInterpreted,
		fm5Interpretation: graphql.Fm5Interpretation{
			Mode:             graphql.Fm5SemanticModeInterpreted,
			EvidenceRevision: "fm5-g7-a41",
		},
		fm5EvidenceGeneration: 7,
		fm5EvidenceRevision:   41,
		solar:                 &vaillantSolarSnapshot{CollectorTemperatureC: &collector},
		solarCylinders:        map[byte]*vaillantCylinderSnapshot{0: {Instance: 0, TemperatureC: &temperature}},
		nowFn:                 time.Now,
	}

	before := poller.captureFM5Evidence()
	reg.Register(registry.DeviceInfo{Address: 0x08, Manufacturer: "Vaillant", DeviceID: "BAI00"})
	after := poller.captureFM5Evidence()
	if before.hasEvidence() || after.hasEvidence() {
		t.Fatal("non-FM5 registry mutation unexpectedly fabricated FM5 identity evidence")
	}
	if before.sameGeneration(after) {
		t.Fatal("registry observation-generation interleaving was not detected")
	}

	verdict := deriveFM5Interpretation(
		before.controller != 0,
		before.moduleConfig,
		false,
		false,
		false,
		false,
		true,
		poller.nextFM5EvidenceRevision(before.generation, after.generation),
	)
	verdict = poller.commitFM5Acquisition(after, verdict, nil, nil)

	if verdict.Mode != graphql.Fm5SemanticModeInterpreted || verdict.DegradedReason != graphql.Fm5SemanticDegradedReasonIncoherentAcquisition {
		t.Errorf("empty-evidence interleaving verdict = %#v; want retained INTERPRETED/INCOHERENT_ACQUISITION", verdict)
	}
	if verdict.EvidenceRevision == "" || verdict.EvidenceRevision == "fm5-g7-a41" {
		t.Errorf("empty-evidence interleaving revision = %q; want advancement", verdict.EvidenceRevision)
	}
	if poller.fm5Mode != graphql.Fm5SemanticModeInterpreted {
		t.Errorf("empty-evidence interleaving legacy scalar = %s; want retained INTERPRETED", poller.fm5Mode)
	}
	if poller.solar == nil || poller.solar.CollectorTemperatureC == nil || *poller.solar.CollectorTemperatureC != collector {
		t.Errorf("empty-evidence interleaving solar = %#v; want retained coherent value", poller.solar)
	}
	if poller.solarCylinders[0] == nil || poller.solarCylinders[0].TemperatureC == nil || *poller.solarCylinders[0].TemperatureC != temperature {
		t.Errorf("empty-evidence interleaving cylinders = %#v; want retained coherent value", poller.solarCylinders)
	}
}

func TestFM5IdentityClassification_RequiresCompleteFunctionalModuleNamespaceScan(t *testing.T) {
	now := time.Date(2026, 8, 15, 15, 0, 0, 0, time.UTC)
	config := uint16(2)
	newPoller := func(provider *graphql.LiveSemanticProvider) *vaillantSemanticPoller {
		return &vaillantSemanticPoller{
			scheduler:               ebusgateway.NewSemanticReadScheduler(),
			reg:                     registry.NewDeviceRegistry(nil),
			provider:                provider,
			source:                  0x7F,
			controller:              0x15,
			requestTimeout:          50 * time.Millisecond,
			system:                  &vaillantSystemSnapshot{Controller: 0x15, ModuleConfigurationVR71: &config},
			radioDevices:            make(map[radioDeviceKey]*vaillantRadioDeviceSnapshot),
			solarCylinders:          make(map[byte]*vaillantCylinderSnapshot),
			fm5EvidenceTTL:          5 * time.Minute,
			deviceSlotCache:         make(map[deviceSlotKey]bool),
			deviceSlotDiscoveryDone: false,
			nowFn:                   func() time.Time { return now },
		}
	}
	assertUnavailable := func(t *testing.T, got graphql.Fm5Interpretation) {
		t.Helper()
		if got.Mode != "" || got.DegradedReason != "" || got.EvidenceRevision != "" {
			t.Fatalf("partial FM5 namespace verdict = %#v; want unavailable zero tuple", got)
		}
	}
	partialResponder := func(cancel context.CancelFunc) func(context.Context, protocol.Frame) (*protocol.Frame, error) {
		return func(_ context.Context, frame protocol.Frame) (*protocol.Frame, error) {
			if len(frame.Data) != 6 {
				return nil, errors.New("invalid B524 selector")
			}
			if frame.Data[2] == remoteFunctionalModules.group {
				cancel()
				return nil, errors.New("functional-module namespace timeout")
			}
			return testB524ResponseForSelectorPayload(frame.Data, 0x00, 0x00, 0x00, 0x00), nil
		}
	}

	t.Run("startup non-FM5 success does not classify absence", func(t *testing.T) {
		provider := graphql.NewLiveSemanticProvider()
		poller := newPoller(provider)
		ctx, cancel := context.WithCancel(context.Background())
		poller.sendFrameFn = partialResponder(cancel)
		poller.refreshRadioDevicesStartup(ctx)
		poller.refreshFM5Semantic(context.Background())
		assertUnavailable(t, provider.FM5Interpretation())
	})

	t.Run("steady non-FM5 success does not classify absence", func(t *testing.T) {
		provider := graphql.NewLiveSemanticProvider()
		poller := newPoller(provider)
		ctx, cancel := context.WithCancel(context.Background())
		poller.sendFrameFn = partialResponder(cancel)
		poller.refreshRadioDevices(ctx)
		assertUnavailable(t, provider.FM5Interpretation())
	})

	t.Run("complete negative functional-module scan permits absence", func(t *testing.T) {
		provider := graphql.NewLiveSemanticProvider()
		poller := newPoller(provider)
		poller.sendFrameFn = func(_ context.Context, frame protocol.Frame) (*protocol.Frame, error) {
			if len(frame.Data) != 6 {
				return nil, errors.New("invalid B524 selector")
			}
			group := frame.Data[2]
			addr := uint16(frame.Data[4]) | uint16(frame.Data[5])<<8
			if group == remoteFunctionalModules.group {
				switch addr {
				case device_slot_connected:
					return testB524ResponseForSelectorPayload(frame.Data, 0x00), nil
				case device_slot_class_address:
					return testB524ResponseForSelectorPayload(frame.Data, 0xFF), nil
				case device_slot_firmware:
					return testB524ResponseForSelectorPayload(frame.Data, 0xFF, 0xFF, 0xFF), nil
				case device_slot_hardware_identifier:
					return testB524ResponseForSelectorPayload(frame.Data, 0xFF, 0xFF), nil
				}
			}
			return testB524ResponseForSelectorPayload(frame.Data, 0x00, 0x00, 0x00, 0x00), nil
		}
		poller.refreshRadioDevicesStartup(context.Background())
		poller.refreshFM5Semantic(context.Background())
		got := provider.FM5Interpretation()
		if got.Mode != graphql.Fm5SemanticModeAbsent || got.DegradedReason != "" || got.EvidenceRevision == "" {
			t.Fatalf("complete negative FM5 namespace verdict = %#v; want healthy ABSENT with revision", got)
		}
	})
}

func TestFM5StartupIdentityClassification_UsesCompleteFunctionalModuleIdentityTuple(t *testing.T) {
	now := time.Date(2026, 8, 15, 16, 0, 0, 0, time.UTC)
	config := uint16(2)
	tests := []struct {
		name             string
		connectedPayload []byte
		firmwarePayload  []byte
		hardwarePayload  []byte
		firmwareTimeout  bool
		wantMode         graphql.Fm5SemanticMode
	}{
		{
			name:            "firmware-only identity",
			firmwarePayload: []byte{0x00, 0x00, 0x00},
			hardwarePayload: []byte{0xFF, 0xFF},
			wantMode:        graphql.Fm5SemanticModeInterpreted,
		},
		{
			name:            "hardware-only identity",
			firmwarePayload: []byte{0xFF, 0xFF, 0xFF},
			hardwarePayload: []byte{0x34, 0x12},
			wantMode:        graphql.Fm5SemanticModeInterpreted,
		},
		{
			name:            "missing firmware field",
			firmwarePayload: []byte{0xFF, 0xFF, 0xFF},
			hardwarePayload: []byte{0xFF, 0xFF},
			firmwareTimeout: true,
		},
		{
			name:             "invalid connected sentinel",
			connectedPayload: []byte{0xFF},
			firmwarePayload:  []byte{0xFF, 0xFF, 0xFF},
			hardwarePayload:  []byte{0xFF, 0xFF},
		},
		{
			name:            "short hardware field",
			firmwarePayload: []byte{0xFF, 0xFF, 0xFF},
			hardwarePayload: []byte{0xFF},
		},
		{
			name:             "connected slot without identity",
			connectedPayload: []byte{0x01},
			firmwarePayload:  []byte{0xFF, 0xFF, 0xFF},
			hardwarePayload:  []byte{0xFF, 0xFF},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			provider := graphql.NewLiveSemanticProvider()
			poller := &vaillantSemanticPoller{
				scheduler:       ebusgateway.NewSemanticReadScheduler(),
				reg:             registry.NewDeviceRegistry(nil),
				provider:        provider,
				source:          0x7F,
				controller:      0x15,
				requestTimeout:  50 * time.Millisecond,
				system:          &vaillantSystemSnapshot{Controller: 0x15, ModuleConfigurationVR71: &config},
				radioDevices:    make(map[radioDeviceKey]*vaillantRadioDeviceSnapshot),
				solarCylinders:  make(map[byte]*vaillantCylinderSnapshot),
				fm5EvidenceTTL:  5 * time.Minute,
				deviceSlotCache: make(map[deviceSlotKey]bool),
				nowFn:           func() time.Time { return now },
			}
			poller.sendFrameFn = func(_ context.Context, frame protocol.Frame) (*protocol.Frame, error) {
				if len(frame.Data) != 6 {
					return nil, errors.New("invalid B524 selector")
				}
				group := frame.Data[2]
				instance := frame.Data[3]
				addr := uint16(frame.Data[4]) | uint16(frame.Data[5])<<8
				if group == remoteFunctionalModules.group {
					switch addr {
					case device_slot_connected:
						payload := []byte{0x00}
						if instance == 0x04 && test.connectedPayload != nil {
							payload = test.connectedPayload
						}
						return testB524ResponseForSelectorPayload(frame.Data, payload...), nil
					case device_slot_class_address:
						return testB524ResponseForSelectorPayload(frame.Data, 0xFF), nil
					case device_slot_firmware:
						if instance == 0x04 && test.firmwareTimeout {
							return nil, errors.New("firmware timeout")
						}
						payload := []byte{0xFF, 0xFF, 0xFF}
						if instance == 0x04 {
							payload = test.firmwarePayload
						}
						return testB524ResponseForSelectorPayload(frame.Data, payload...), nil
					case device_slot_hardware_identifier:
						payload := []byte{0xFF, 0xFF}
						if instance == 0x04 {
							payload = test.hardwarePayload
						}
						return testB524ResponseForSelectorPayload(frame.Data, payload...), nil
					}
				}
				return testB524ResponseForSelectorPayload(frame.Data, 0x00, 0x00, 0x00, 0x00), nil
			}

			poller.refreshRadioDevicesStartup(context.Background())
			poller.refreshFM5Semantic(context.Background())
			got := provider.FM5Interpretation()
			if test.wantMode == "" {
				if got.Mode != "" || got.DegradedReason != "" || got.EvidenceRevision != "" {
					t.Fatalf("incomplete startup identity tuple verdict = %#v; want unavailable zero tuple", got)
				}
				return
			}
			if got.Mode != test.wantMode || got.DegradedReason != "" || got.EvidenceRevision == "" {
				t.Fatalf("startup identity tuple verdict = %#v; want healthy %s with revision", got, test.wantMode)
			}
		})
	}
}

func TestRefreshRadioDevices_IncompleteCurrentFM5IdentityTupleRetainsPriorClassification(t *testing.T) {
	now := time.Date(2026, 8, 15, 17, 0, 0, 0, time.UTC)
	config := uint16(2)
	collector := 61.5
	temperature := 47.25
	previous := graphql.Fm5Interpretation{
		Mode:             graphql.Fm5SemanticModeInterpreted,
		EvidenceRevision: "fm5-g7-a41",
	}
	zeroFreshnessObserver := staticSemanticReadWatchObserver{
		observation: ebusgateway.WatchObservation{
			State:         ebusgateway.WatchObservationStateActive,
			HasDescriptor: true,
			Descriptor: ebusgateway.WatchDescriptor{
				SemanticClass:     ebusgateway.WatchSemanticClassDebug,
				FreshnessProfile:  ebusgateway.WatchFreshnessProfileDebug,
				DecoderID:         "test.fm5.current.identity",
				CorrelationPolicy: ebusgateway.WatchCorrelationPolicyRequestResponse,
				DirectApplyPolicy: ebusgateway.WatchDirectApplyPolicyNever,
			},
			Sources: []ebusgateway.WatchActivationSource{ebusgateway.WatchActivationSourcePoller},
		},
	}

	newPoller := func(provider *graphql.LiveSemanticProvider) *vaillantSemanticPoller {
		return &vaillantSemanticPoller{
			scheduler:               ebusgateway.NewSemanticReadScheduler(),
			watchObserver:           zeroFreshnessObserver,
			reg:                     registry.NewDeviceRegistry(nil),
			provider:                provider,
			source:                  0x7F,
			controller:              0x15,
			requestTimeout:          50 * time.Millisecond,
			system:                  &vaillantSystemSnapshot{Controller: 0x15, ModuleConfigurationVR71: &config},
			deviceSlotCache:         make(map[deviceSlotKey]bool),
			fm5Mode:                 graphql.Fm5SemanticModeInterpreted,
			fm5Interpretation:       previous,
			fm5EvidenceRevision:     41,
			fm5EvidenceGeneration:   7,
			fm5IdentityScanComplete: true,
			fm5IdentityObservedAt:   now,
			fm5EvidenceTTL:          5 * time.Minute,
			solar:                   &vaillantSolarSnapshot{CollectorTemperatureC: &collector},
			solarCylinders:          map[byte]*vaillantCylinderSnapshot{0: {Instance: 0, TemperatureC: &temperature}},
			nowFn:                   func() time.Time { return now },
		}
	}
	newProvider := func() *graphql.LiveSemanticProvider {
		provider := graphql.NewLiveSemanticProvider()
		provider.SetFM5Interpretation(previous)
		provider.SetSolar(&graphql.SolarStatus{CollectorTemperatureC: &collector})
		provider.SetCylinders([]graphql.CylinderStatus{{Index: 0, TemperatureC: &temperature}})
		return provider
	}
	assertRetainedMode := func(t *testing.T, provider *graphql.LiveSemanticProvider, wantMode graphql.Fm5SemanticMode) {
		t.Helper()
		got := provider.FM5Interpretation()
		if got.Mode != wantMode || got.DegradedReason != graphql.Fm5SemanticDegradedReasonIncoherentAcquisition {
			t.Errorf("incomplete current identity verdict = %#v; want retained %s/INCOHERENT_ACQUISITION", got, wantMode)
		}
		if got.EvidenceRevision == "" || got.EvidenceRevision == previous.EvidenceRevision {
			t.Errorf("incomplete current identity revision = %q; want advancement from %q", got.EvidenceRevision, previous.EvidenceRevision)
		}
		if gotMode := provider.FM5SemanticMode(); gotMode != wantMode {
			t.Errorf("incomplete current identity legacy scalar = %s; want retained %s", gotMode, wantMode)
		}
		if solar := provider.Solar(); solar == nil || solar.CollectorTemperatureC == nil || *solar.CollectorTemperatureC != collector {
			t.Errorf("incomplete current identity solar = %#v; want retained collector temperature", solar)
		}
		if cylinders := provider.Cylinders(); len(cylinders) != 1 || cylinders[0].TemperatureC == nil || *cylinders[0].TemperatureC != temperature {
			t.Errorf("incomplete current identity cylinders = %#v; want retained temperature", cylinders)
		}
	}
	assertRetained := func(t *testing.T, provider *graphql.LiveSemanticProvider) {
		t.Helper()
		assertRetainedMode(t, provider, graphql.Fm5SemanticModeInterpreted)
	}

	t.Run("same-call firmware identity then detail timeout", func(t *testing.T) {
		provider := newProvider()
		poller := newPoller(provider)
		firmwareReads := 0
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		poller.sendFrameFn = func(_ context.Context, frame protocol.Frame) (*protocol.Frame, error) {
			if len(frame.Data) != 6 {
				return nil, errors.New("invalid B524 selector")
			}
			group := frame.Data[2]
			instance := frame.Data[3]
			addr := uint16(frame.Data[4]) | uint16(frame.Data[5])<<8
			if group == remoteFunctionalModules.group {
				switch addr {
				case device_slot_connected:
					return testB524ResponseForSelectorPayload(frame.Data, 0x00), nil
				case device_slot_class_address:
					return testB524ResponseForSelectorPayload(frame.Data, 0xFF), nil
				case device_slot_firmware:
					if instance == 0x04 {
						firmwareReads++
						if firmwareReads > 1 {
							cancel()
							return nil, errors.New("current firmware detail timeout")
						}
						return testB524ResponseForSelectorPayload(frame.Data, 0x00, 0x00, 0x00), nil
					}
					return testB524ResponseForSelectorPayload(frame.Data, 0xFF, 0xFF, 0xFF), nil
				case device_slot_hardware_identifier:
					return testB524ResponseForSelectorPayload(frame.Data, 0xFF, 0xFF), nil
				}
			}
			return testB524ResponseForSelectorPayload(frame.Data, 0x00, 0x00, 0x00, 0x00), nil
		}

		poller.refreshRadioDevices(ctx)
		if firmwareReads < 2 {
			t.Fatalf("firmware reads = %d; want discovery identity followed by current detail read", firmwareReads)
		}
		assertRetained(t, provider)
	})

	t.Run("full scan invalid connected sentinel", func(t *testing.T) {
		provider := newProvider()
		poller := newPoller(provider)
		poller.sendFrameFn = func(_ context.Context, frame protocol.Frame) (*protocol.Frame, error) {
			if len(frame.Data) != 6 {
				return nil, errors.New("invalid B524 selector")
			}
			group := frame.Data[2]
			instance := frame.Data[3]
			addr := uint16(frame.Data[4]) | uint16(frame.Data[5])<<8
			if group == remoteFunctionalModules.group {
				switch addr {
				case device_slot_connected:
					value := byte(0x00)
					if instance == 0x04 {
						value = 0xFF
					}
					return testB524ResponseForSelectorPayload(frame.Data, value), nil
				case device_slot_class_address:
					return testB524ResponseForSelectorPayload(frame.Data, 0xFF), nil
				case device_slot_firmware:
					return testB524ResponseForSelectorPayload(frame.Data, 0xFF, 0xFF, 0xFF), nil
				case device_slot_hardware_identifier:
					return testB524ResponseForSelectorPayload(frame.Data, 0xFF, 0xFF), nil
				}
			}
			return testB524ResponseForSelectorPayload(frame.Data, 0x00, 0x00, 0x00, 0x00), nil
		}

		poller.refreshRadioDevices(context.Background())
		assertRetained(t, provider)
	})

	t.Run("full scan connected slot without identity", func(t *testing.T) {
		for _, wantMode := range []graphql.Fm5SemanticMode{
			graphql.Fm5SemanticModeInterpreted,
			graphql.Fm5SemanticModeAbsent,
		} {
			t.Run(string(wantMode), func(t *testing.T) {
				provider := newProvider()
				poller := newPoller(provider)
				prior := previous
				prior.Mode = wantMode
				provider.SetFM5Interpretation(prior)
				poller.fm5Mode = wantMode
				poller.fm5Interpretation = prior
				poller.sendFrameFn = func(_ context.Context, frame protocol.Frame) (*protocol.Frame, error) {
					if len(frame.Data) != 6 {
						return nil, errors.New("invalid B524 selector")
					}
					group := frame.Data[2]
					instance := frame.Data[3]
					addr := uint16(frame.Data[4]) | uint16(frame.Data[5])<<8
					if group == remoteFunctionalModules.group {
						switch addr {
						case device_slot_connected:
							value := byte(0x00)
							if instance == 0x04 {
								value = 0x01
							}
							return testB524ResponseForSelectorPayload(frame.Data, value), nil
						case device_slot_class_address:
							return testB524ResponseForSelectorPayload(frame.Data, 0xFF), nil
						case device_slot_firmware:
							return testB524ResponseForSelectorPayload(frame.Data, 0xFF, 0xFF, 0xFF), nil
						case device_slot_hardware_identifier:
							return testB524ResponseForSelectorPayload(frame.Data, 0xFF, 0xFF), nil
						}
					}
					return testB524ResponseForSelectorPayload(frame.Data, 0x00, 0x00, 0x00, 0x00), nil
				}

				poller.refreshRadioDevices(context.Background())
				assertRetainedMode(t, provider, wantMode)
			})
		}
	})

	t.Run("cached hardware identity then later detail timeout", func(t *testing.T) {
		provider := newProvider()
		poller := newPoller(provider)
		hardware := uint16(0x1234)
		poller.radioDevices = map[radioDeviceKey]*vaillantRadioDeviceSnapshot{
			{Group: remoteFunctionalModules.group, Instance: 0x04}: {
				Group:              remoteFunctionalModules.group,
				Instance:           0x04,
				SlotMode:           "inventory",
				HardwareIdentifier: &hardware,
			},
		}
		poller.deviceSlotCache[deviceSlotKey{Group: remoteFunctionalModules.group, Instance: 0x04}] = true
		poller.deviceSlotDiscoveryDone = true
		poller.deviceSlotDiscoveryAt = now
		poller.deviceSlotRediscoveryTTL = time.Hour
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		poller.sendFrameFn = func(_ context.Context, frame protocol.Frame) (*protocol.Frame, error) {
			if len(frame.Data) != 6 {
				return nil, errors.New("invalid B524 selector")
			}
			group := frame.Data[2]
			addr := uint16(frame.Data[4]) | uint16(frame.Data[5])<<8
			if group == remoteFunctionalModules.group {
				switch addr {
				case device_slot_connected:
					return testB524ResponseForSelectorPayload(frame.Data, 0x00), nil
				case device_slot_class_address:
					return testB524ResponseForSelectorPayload(frame.Data, 0xFF), nil
				case device_slot_firmware:
					return testB524ResponseForSelectorPayload(frame.Data, 0xFF, 0xFF, 0xFF), nil
				case device_slot_hardware_identifier:
					cancel()
					return nil, errors.New("current hardware detail timeout")
				}
			}
			return testB524ResponseForSelectorPayload(frame.Data, 0x00, 0x00, 0x00, 0x00), nil
		}

		poller.refreshRadioDevices(ctx)
		assertRetained(t, provider)
	})

	t.Run("cached identity then first connected read timeout", func(t *testing.T) {
		provider := newProvider()
		poller := newPoller(provider)
		hardware := uint16(0x1234)
		poller.radioDevices = map[radioDeviceKey]*vaillantRadioDeviceSnapshot{
			{Group: remoteFunctionalModules.group, Instance: 0x04}: {
				Group:              remoteFunctionalModules.group,
				Instance:           0x04,
				SlotMode:           "inventory",
				HardwareIdentifier: &hardware,
			},
		}
		poller.deviceSlotCache[deviceSlotKey{Group: remoteFunctionalModules.group, Instance: 0x04}] = true
		poller.deviceSlotDiscoveryDone = true
		poller.deviceSlotDiscoveryAt = now
		poller.deviceSlotRediscoveryTTL = time.Hour
		connectedReads := 0
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		poller.sendFrameFn = func(_ context.Context, frame protocol.Frame) (*protocol.Frame, error) {
			if len(frame.Data) != 6 {
				return nil, errors.New("invalid B524 selector")
			}
			if frame.Data[2] == remoteFunctionalModules.group {
				addr := uint16(frame.Data[4]) | uint16(frame.Data[5])<<8
				if addr == device_slot_connected {
					connectedReads++
					cancel()
					return nil, errors.New("current connected timeout")
				}
			}
			return testB524ResponseForSelectorPayload(frame.Data, 0x00, 0x00, 0x00, 0x00), nil
		}

		poller.refreshRadioDevices(ctx)
		if connectedReads == 0 {
			t.Fatal("current device_connected read was not attempted")
		}
		assertRetained(t, provider)
	})

	t.Run("complete FM negative survives non-FM detail timeout", func(t *testing.T) {
		provider := newProvider()
		poller := newPoller(provider)
		connected := true
		classAddress := uint8(0x15)
		roomTemperature := 21.375
		roomHumidity := 48.0
		roomMapping := uint16(1)
		poller.radioDevices = map[radioDeviceKey]*vaillantRadioDeviceSnapshot{
			{Group: remoteRegulators.group, Instance: 0x01}: {
				Group:              remoteRegulators.group,
				Instance:           0x01,
				SlotMode:           "active",
				DeviceConnected:    &connected,
				DeviceClassAddress: &classAddress,
				DeviceModel:        "retained regulator",
				RoomTemperatureC:   &roomTemperature,
				RoomHumidityPct:    &roomHumidity,
			},
		}
		poller.zones = map[byte]*vaillantZoneSnapshot{
			0x00: {
				Instance: 0x00,
				Present:  true,
				Name:     "Living",
				ConfigurationRoomTemperatureZoneMappingRaw: &roomMapping,
			},
		}
		poller.publishRadioDevices(semanticSnapshotSourceLive)
		poller.publishZones(semanticSnapshotSourceLive)
		retainedRadioDevices := provider.RadioDevices()
		if len(retainedRadioDevices) != 1 {
			t.Fatalf("retained radio fixture = %#v; want one regulator", retainedRadioDevices)
		}
		retainedZones := provider.Zones()
		if len(retainedZones) != 1 || retainedZones[0].State.CurrentTempC == nil || *retainedZones[0].State.CurrentTempC != roomTemperature || retainedZones[0].State.CurrentHumidityPct == nil || *retainedZones[0].State.CurrentHumidityPct != roomHumidity {
			t.Fatalf("retained zone fixture = %#v; want regulator temperature/humidity fallback", retainedZones)
		}
		regulatorConnectedReads := 0
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		poller.sendFrameFn = func(_ context.Context, frame protocol.Frame) (*protocol.Frame, error) {
			if len(frame.Data) != 6 {
				return nil, errors.New("invalid B524 selector")
			}
			group := frame.Data[2]
			instance := frame.Data[3]
			addr := uint16(frame.Data[4]) | uint16(frame.Data[5])<<8
			if group == remoteFunctionalModules.group {
				switch addr {
				case device_slot_connected:
					return testB524ResponseForSelectorPayload(frame.Data, 0x00), nil
				case device_slot_class_address:
					return testB524ResponseForSelectorPayload(frame.Data, 0xFF), nil
				case device_slot_firmware:
					return testB524ResponseForSelectorPayload(frame.Data, 0xFF, 0xFF, 0xFF), nil
				case device_slot_hardware_identifier:
					return testB524ResponseForSelectorPayload(frame.Data, 0xFF, 0xFF), nil
				}
			}
			if group == remoteRegulators.group && instance == 0x01 && addr == device_slot_connected {
				regulatorConnectedReads++
				if regulatorConnectedReads > 1 {
					cancel()
					return nil, errors.New("current regulator detail timeout")
				}
				return testB524ResponseForSelectorPayload(frame.Data, 0x01), nil
			}
			return testB524ResponseForSelectorPayload(frame.Data, 0x00, 0x00, 0x00, 0x00), nil
		}

		poller.refreshRadioDevices(ctx)
		if regulatorConnectedReads < 2 {
			t.Fatalf("regulator connected reads = %d; want discovery success then detail timeout", regulatorConnectedReads)
		}
		got := provider.FM5Interpretation()
		if got.Mode != graphql.Fm5SemanticModeAbsent || got.DegradedReason != "" {
			t.Errorf("complete negative with non-FM detail timeout verdict = %#v; want healthy ABSENT", got)
		}
		if got.EvidenceRevision == "" || got.EvidenceRevision == previous.EvidenceRevision {
			t.Errorf("complete negative with non-FM detail timeout revision = %q; want advancement", got.EvidenceRevision)
		}
		if solar := provider.Solar(); solar == nil || solar.CollectorTemperatureC != nil {
			t.Errorf("complete negative with non-FM detail timeout solar = %#v; want cleared structural plane", solar)
		}
		if cylinders := provider.Cylinders(); len(cylinders) != 0 {
			t.Errorf("complete negative with non-FM detail timeout cylinders = %#v; want cleared structural plane", cylinders)
		}

		poller.publishZones(semanticSnapshotSourceLive)
		if radioDevices := provider.RadioDevices(); !radioDevicesEqual(radioDevices, retainedRadioDevices) {
			t.Errorf("complete negative with non-FM detail timeout radio devices = %#v; want retained %#v", radioDevices, retainedRadioDevices)
		}
		zones := provider.Zones()
		if len(zones) != 1 || zones[0].State.CurrentTempC == nil || *zones[0].State.CurrentTempC != roomTemperature || zones[0].State.CurrentHumidityPct == nil || *zones[0].State.CurrentHumidityPct != roomHumidity {
			t.Errorf("complete negative with non-FM detail timeout zone fallback = %#v; want retained regulator temperature/humidity", zones)
		}
	})
}

func TestRefreshSystem_FM5ConfigurationRequiresCurrentAcquisition(t *testing.T) {
	now := time.Date(2026, 8, 15, 18, 0, 0, 0, time.UTC)
	configInterpretable := uint16(2)
	configUnsupported := uint16(3)
	classAddress := uint8(circuitManagingDeviceVR71Address)
	collector := 61.5
	temperature := 47.25
	previous := graphql.Fm5Interpretation{
		Mode:             graphql.Fm5SemanticModeInterpreted,
		EvidenceRevision: "fm5-g7-a41",
	}
	newPoller := func(provider *graphql.LiveSemanticProvider, config *uint16) *vaillantSemanticPoller {
		return &vaillantSemanticPoller{
			scheduler:             ebusgateway.NewSemanticReadScheduler(),
			reg:                   registry.NewDeviceRegistry(nil),
			provider:              provider,
			source:                0x7F,
			controller:            0x15,
			requestTimeout:        50 * time.Millisecond,
			system:                &vaillantSystemSnapshot{Controller: 0x15, ModuleConfigurationVR71: config},
			radioDevices:          map[radioDeviceKey]*vaillantRadioDeviceSnapshot{{Group: remoteFunctionalModules.group}: {DeviceClassAddress: &classAddress}},
			fm5Mode:               previous.Mode,
			fm5Interpretation:     previous,
			fm5EvidenceRevision:   41,
			fm5EvidenceGeneration: 7,
			fm5IdentityObservedAt: now,
			fm5EvidenceTTL:        5 * time.Minute,
			solar:                 &vaillantSolarSnapshot{CollectorTemperatureC: &collector},
			solarCylinders:        map[byte]*vaillantCylinderSnapshot{0: {Instance: 0, TemperatureC: &temperature}},
			nowFn:                 func() time.Time { return now },
		}
	}
	newProvider := func() *graphql.LiveSemanticProvider {
		provider := graphql.NewLiveSemanticProvider()
		provider.SetFM5Interpretation(previous)
		provider.SetSolar(&graphql.SolarStatus{CollectorTemperatureC: &collector})
		provider.SetCylinders([]graphql.CylinderStatus{{Index: 0, TemperatureC: &temperature}})
		return provider
	}
	assertRetained := func(t *testing.T, provider *graphql.LiveSemanticProvider, familyReads int) {
		t.Helper()
		got := provider.FM5Interpretation()
		if got.Mode != graphql.Fm5SemanticModeInterpreted || got.DegradedReason != graphql.Fm5SemanticDegradedReasonConfigurationUnavailable {
			t.Errorf("configuration timeout verdict = %#v; want retained INTERPRETED/CONFIGURATION_UNAVAILABLE", got)
		}
		if got.EvidenceRevision == "" || got.EvidenceRevision == previous.EvidenceRevision {
			t.Errorf("configuration timeout revision = %q; want advancement from %q", got.EvidenceRevision, previous.EvidenceRevision)
		}
		if familyReads != 0 {
			t.Errorf("configuration timeout family reads = %d; want 0", familyReads)
		}
		if solar := provider.Solar(); solar == nil || solar.CollectorTemperatureC == nil || *solar.CollectorTemperatureC != collector {
			t.Errorf("configuration timeout solar = %#v; want retained collector temperature", solar)
		}
		if cylinders := provider.Cylinders(); len(cylinders) != 1 || cylinders[0].TemperatureC == nil || *cylinders[0].TemperatureC != temperature {
			t.Errorf("configuration timeout cylinders = %#v; want retained temperature", cylinders)
		}
	}

	t.Run("all system fields fail", func(t *testing.T) {
		provider := newProvider()
		poller := newPoller(provider, &configInterpretable)
		familyReads := 0
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		poller.sendFrameFn = func(_ context.Context, frame protocol.Frame) (*protocol.Frame, error) {
			if len(frame.Data) == 6 && (frame.Data[2] == localSolar.group || frame.Data[2] == localCylinders.group) {
				familyReads++
			}
			cancel()
			return nil, errors.New("system acquisition timeout")
		}

		poller.refreshSystem(ctx)
		assertRetained(t, provider, familyReads)
	})

	t.Run("another system field succeeds", func(t *testing.T) {
		provider := newProvider()
		poller := newPoller(provider, &configInterpretable)
		familyReads := 0
		configReads := 0
		poller.sendFrameFn = func(_ context.Context, frame protocol.Frame) (*protocol.Frame, error) {
			if len(frame.Data) != 6 {
				return nil, errors.New("invalid B524 selector")
			}
			group := frame.Data[2]
			addr := uint16(frame.Data[4]) | uint16(frame.Data[5])<<8
			if group == localRegulator.group && addr == system_module_configuration_vr71 {
				configReads++
				return nil, errors.New("module configuration timeout")
			}
			if group == localSolar.group || group == localCylinders.group {
				familyReads++
			}
			return testB524ResponseForSelectorPayload(frame.Data, 0x00, 0x00, 0x00, 0x00), nil
		}

		poller.refreshSystem(context.Background())
		if configReads == 0 {
			t.Fatal("module_configuration_vr71 read was not attempted")
		}
		assertRetained(t, provider, familyReads)
	})

	t.Run("stale unsupported config cannot classify bootstrap", func(t *testing.T) {
		provider := graphql.NewLiveSemanticProvider()
		poller := newPoller(provider, &configUnsupported)
		poller.fm5Mode = ""
		poller.fm5Interpretation = graphql.Fm5Interpretation{}
		poller.solar = nil
		poller.solarCylinders = make(map[byte]*vaillantCylinderSnapshot)
		poller.sendFrameFn = func(_ context.Context, frame protocol.Frame) (*protocol.Frame, error) {
			if len(frame.Data) != 6 {
				return nil, errors.New("invalid B524 selector")
			}
			group := frame.Data[2]
			addr := uint16(frame.Data[4]) | uint16(frame.Data[5])<<8
			if group == localRegulator.group && addr == system_module_configuration_vr71 {
				return nil, errors.New("module configuration timeout")
			}
			return testB524ResponseForSelectorPayload(frame.Data, 0x00, 0x00, 0x00, 0x00), nil
		}

		poller.refreshSystem(context.Background())
		if got := provider.FM5Interpretation(); got != (graphql.Fm5Interpretation{}) {
			t.Errorf("stale unsupported config bootstrap verdict = %#v; want unavailable zero tuple", got)
		}
	})
}

func TestFM5PositiveIdentityDoesNotRequireNegativeNamespaceCompleteness(t *testing.T) {
	now := time.Date(2026, 8, 15, 19, 0, 0, 0, time.UTC)
	config := uint16(2)
	newPoller := func(provider *graphql.LiveSemanticProvider, reg *registry.DeviceRegistry) *vaillantSemanticPoller {
		return &vaillantSemanticPoller{
			scheduler:       ebusgateway.NewSemanticReadScheduler(),
			reg:             reg,
			provider:        provider,
			source:          0x7F,
			controller:      0x15,
			requestTimeout:  50 * time.Millisecond,
			system:          &vaillantSystemSnapshot{Controller: 0x15, ModuleConfigurationVR71: &config},
			radioDevices:    make(map[radioDeviceKey]*vaillantRadioDeviceSnapshot),
			solarCylinders:  make(map[byte]*vaillantCylinderSnapshot),
			fm5EvidenceTTL:  5 * time.Minute,
			deviceSlotCache: make(map[deviceSlotKey]bool),
			nowFn:           func() time.Time { return now },
		}
	}
	assertInterpreted := func(t *testing.T, provider *graphql.LiveSemanticProvider) {
		t.Helper()
		got := provider.FM5Interpretation()
		if got.Mode != graphql.Fm5SemanticModeInterpreted || got.DegradedReason != "" || got.EvidenceRevision == "" {
			t.Fatalf("positive partial-namespace verdict = %#v; want healthy INTERPRETED with revision", got)
		}
	}
	responder := func(positiveInstance byte, timeoutInstance *byte) func(context.Context, protocol.Frame) (*protocol.Frame, error) {
		return func(_ context.Context, frame protocol.Frame) (*protocol.Frame, error) {
			if len(frame.Data) != 6 {
				return nil, errors.New("invalid B524 selector")
			}
			group := frame.Data[2]
			instance := frame.Data[3]
			addr := uint16(frame.Data[4]) | uint16(frame.Data[5])<<8
			if group == remoteFunctionalModules.group {
				if timeoutInstance != nil && instance == *timeoutInstance && addr == device_slot_connected {
					return nil, errors.New("unrelated functional-module slot timeout")
				}
				switch addr {
				case device_slot_connected:
					return testB524ResponseForSelectorPayload(frame.Data, 0x00), nil
				case device_slot_class_address:
					value := byte(0xFF)
					if instance == positiveInstance {
						value = circuitManagingDeviceVR71Address
					}
					return testB524ResponseForSelectorPayload(frame.Data, value), nil
				case device_slot_firmware:
					return testB524ResponseForSelectorPayload(frame.Data, 0xFF, 0xFF, 0xFF), nil
				case device_slot_hardware_identifier:
					return testB524ResponseForSelectorPayload(frame.Data, 0xFF, 0xFF), nil
				}
			}
			return testB524ResponseForSelectorPayload(frame.Data, 0x00, 0x00, 0x00, 0x00), nil
		}
	}

	t.Run("startup registry-seeded low-slot positive", func(t *testing.T) {
		provider := graphql.NewLiveSemanticProvider()
		reg := registry.NewDeviceRegistry(nil)
		reg.Register(registry.DeviceInfo{Address: circuitManagingDeviceVR71Address, Manufacturer: "Vaillant", DeviceID: circuitManagingDeviceVR71ID})
		poller := newPoller(provider, reg)
		poller.sendFrameFn = responder(0x00, nil)

		poller.refreshRadioDevicesStartup(context.Background())
		poller.refreshFM5SemanticStartup(context.Background())
		assertInterpreted(t, provider)
	})

	t.Run("startup positive with unrelated slot timeout", func(t *testing.T) {
		provider := graphql.NewLiveSemanticProvider()
		poller := newPoller(provider, registry.NewDeviceRegistry(nil))
		timeoutInstance := byte(0x07)
		poller.sendFrameFn = responder(0x04, &timeoutInstance)

		poller.refreshRadioDevicesStartup(context.Background())
		poller.refreshFM5SemanticStartup(context.Background())
		assertInterpreted(t, provider)
	})

	t.Run("steady positive with unrelated slot timeout", func(t *testing.T) {
		provider := graphql.NewLiveSemanticProvider()
		poller := newPoller(provider, registry.NewDeviceRegistry(nil))
		timeoutInstance := byte(0x07)
		poller.sendFrameFn = responder(0x04, &timeoutInstance)

		poller.refreshRadioDevices(context.Background())
		assertInterpreted(t, provider)
	})
}

func TestFM5Startup_UnseededLowSlotPositiveDoesNotLosePrimingBudget(t *testing.T) {
	now := time.Date(2026, 8, 15, 20, 0, 0, 0, time.UTC)
	config := uint16(2)
	const (
		collectorTemperature = 61.5
		cylinderTemperature  = 47.25
	)
	provider := graphql.NewLiveSemanticProvider()
	poller := &vaillantSemanticPoller{
		scheduler:       ebusgateway.NewSemanticReadScheduler(),
		reg:             registry.NewDeviceRegistry(nil),
		provider:        provider,
		source:          0x7F,
		controller:      0x15,
		requestTimeout:  50 * time.Millisecond,
		system:          &vaillantSystemSnapshot{Controller: 0x15, ModuleConfigurationVR71: &config},
		radioDevices:    make(map[radioDeviceKey]*vaillantRadioDeviceSnapshot),
		solarCylinders:  make(map[byte]*vaillantCylinderSnapshot),
		fm5EvidenceTTL:  5 * time.Minute,
		deviceSlotCache: make(map[deviceSlotKey]bool),
		nowFn:           func() time.Time { return now },
	}

	startupCtx, cancelStartup := context.WithCancel(context.Background())
	defer cancelStartup()
	tailEntered := false
	blockedFamilyReads := 0
	poller.sendFrameFn = func(ctx context.Context, frame protocol.Frame) (*protocol.Frame, error) {
		if len(frame.Data) != 6 {
			return nil, errors.New("invalid B524 selector")
		}
		group := frame.Data[2]
		instance := frame.Data[3]
		addr := uint16(frame.Data[4]) | uint16(frame.Data[5])<<8

		if group == remoteFunctionalModules.group && instance >= semanticStartupSlotFastMaxInstance+1 {
			tailEntered = true
			cancelStartup()
		}
		if (group == localSolar.group || group == localCylinders.group) && ctx.Err() != nil {
			blockedFamilyReads++
			return nil, ctx.Err()
		}

		if group == remoteFunctionalModules.group {
			switch addr {
			case device_slot_connected:
				connected := byte(0x00)
				if instance == 0x01 {
					connected = 0x01
				}
				return testB524ResponseForSelectorPayload(frame.Data, connected), nil
			case device_slot_class_address:
				classAddress := byte(0xFF)
				if instance == 0x01 {
					classAddress = circuitManagingDeviceVR71Address
				}
				return testB524ResponseForSelectorPayload(frame.Data, classAddress), nil
			case device_slot_firmware:
				firmware := []byte{0xFF, 0xFF, 0xFF}
				if instance == 0x01 {
					firmware = []byte{0x01, 0x00, 0x00}
				}
				return testB524ResponseForSelectorPayload(frame.Data, firmware...), nil
			case device_slot_hardware_identifier:
				hardware := []byte{0xFF, 0xFF}
				if instance == 0x01 {
					hardware = []byte{0x71, 0x00}
				}
				return testB524ResponseForSelectorPayload(frame.Data, hardware...), nil
			}
		}
		if group == localSolar.group {
			switch addr {
			case solar_collector_temp:
				return testB524ResponseForSelectorPayload(frame.Data, testB524Float32Payload(collectorTemperature)...), nil
			case solar_enabled, solar_pump_active:
				return testB524ResponseForSelectorPayload(frame.Data, 0x01), nil
			}
		}
		if group == localCylinders.group && addr == cylinder_temperature {
			return testB524ResponseForSelectorPayload(frame.Data, testB524Float32Payload(cylinderTemperature)...), nil
		}
		return testB524ResponseForSelectorPayload(frame.Data, 0x00, 0x00, 0x00, 0x00), nil
	}

	poller.refreshRadioDevicesStartup(startupCtx)
	poller.refreshFM5SemanticStartup(startupCtx)

	got := provider.FM5Interpretation()
	if got.Mode != graphql.Fm5SemanticModeInterpreted || got.DegradedReason != "" || got.EvidenceRevision == "" {
		t.Fatalf("unseeded low-slot startup verdict = %#v; want healthy INTERPRETED with revision (tail_entered=%t blocked_family_reads=%d)", got, tailEntered, blockedFamilyReads)
	}
	if solar := provider.Solar(); solar == nil || solar.CollectorTemperatureC == nil || *solar.CollectorTemperatureC != collectorTemperature {
		t.Fatalf("unseeded low-slot startup solar = %#v; want collector temperature %v", solar, collectorTemperature)
	}
	if cylinders := provider.Cylinders(); len(cylinders) == 0 || cylinders[0].TemperatureC == nil || *cylinders[0].TemperatureC != cylinderTemperature {
		t.Fatalf("unseeded low-slot startup cylinders = %#v; want temperature %v", cylinders, cylinderTemperature)
	}
}

func TestEnqueueAddressIdentityProbe_LateFM5SuccessTriggersBoundedSemanticReacquisition(t *testing.T) {
	originalScanDirected := registryScanDirectedFn
	t.Cleanup(func() { registryScanDirectedFn = originalScanDirected })

	now := time.Date(2026, 8, 15, 20, 30, 0, 0, time.UTC)
	provider := graphql.NewLiveSemanticProvider()
	taskScheduler := newSemanticTaskScheduler()
	poller := &vaillantSemanticPoller{
		scheduler:                ebusgateway.NewSemanticReadScheduler(),
		tasks:                    taskScheduler,
		reg:                      registry.NewDeviceRegistry(nil),
		bus:                      &protocol.Bus{},
		provider:                 provider,
		source:                   0x7F,
		controller:               0x15,
		requestTimeout:           50 * time.Millisecond,
		system:                   &vaillantSystemSnapshot{Controller: 0x15},
		radioDevices:             make(map[radioDeviceKey]*vaillantRadioDeviceSnapshot),
		solarCylinders:           make(map[byte]*vaillantCylinderSnapshot),
		fm5EvidenceTTL:           5 * time.Minute,
		deviceSlotRediscoveryTTL: 30 * time.Minute,
		deviceSlotCache:          make(map[deviceSlotKey]bool),
		nowFn:                    func() time.Time { return now },
	}

	scanSeamCalled := false
	registryScanDirectedFn = func(_ context.Context, _ registry.ScanBus, reg *registry.DeviceRegistry, source byte, targets []byte) ([]registry.DeviceEntry, error) {
		scanSeamCalled = true
		if source != poller.source || len(targets) != 1 || targets[0] != circuitManagingDeviceVR71Address {
			return nil, errors.New("unexpected directed identity target")
		}
		entry := reg.Register(registry.DeviceInfo{
			Address:      circuitManagingDeviceVR71Address,
			Manufacturer: "Vaillant",
			DeviceID:     circuitManagingDeviceVR71ID,
		})
		return []registry.DeviceEntry{entry}, nil
	}

	poller.sendFrameFn = func(_ context.Context, frame protocol.Frame) (*protocol.Frame, error) {
		if len(frame.Data) != 6 {
			return nil, errors.New("invalid B524 selector")
		}
		group := frame.Data[2]
		instance := frame.Data[3]
		addr := uint16(frame.Data[4]) | uint16(frame.Data[5])<<8
		if group == localRegulator.group && addr == system_module_configuration_vr71 {
			return testB524ResponseForSelectorPayload(frame.Data, 0x02, 0x00), nil
		}
		if group == remoteFunctionalModules.group {
			switch addr {
			case device_slot_connected:
				connected := byte(0x00)
				if instance == 0x01 {
					connected = 0x01
				}
				return testB524ResponseForSelectorPayload(frame.Data, connected), nil
			case device_slot_class_address:
				classAddress := byte(0xFF)
				if instance == 0x01 {
					classAddress = circuitManagingDeviceVR71Address
				}
				return testB524ResponseForSelectorPayload(frame.Data, classAddress), nil
			case device_slot_firmware:
				firmware := []byte{0xFF, 0xFF, 0xFF}
				if instance == 0x01 {
					firmware = []byte{0x01, 0x00, 0x00}
				}
				return testB524ResponseForSelectorPayload(frame.Data, firmware...), nil
			case device_slot_hardware_identifier:
				hardware := []byte{0xFF, 0xFF}
				if instance == 0x01 {
					hardware = []byte{0x71, 0x00}
				}
				return testB524ResponseForSelectorPayload(frame.Data, hardware...), nil
			}
		}
		if group == localSolar.group {
			switch addr {
			case solar_collector_temp:
				return testB524ResponseForSelectorPayload(frame.Data, testB524Float32Payload(61.5)...), nil
			case solar_enabled, solar_pump_active:
				return testB524ResponseForSelectorPayload(frame.Data, 0x01), nil
			}
		}
		if group == localCylinders.group && addr == cylinder_temperature {
			return testB524ResponseForSelectorPayload(frame.Data, testB524Float32Payload(47.25)...), nil
		}
		return testB524ResponseForSelectorPayload(frame.Data, 0x00, 0x00, 0x00, 0x00), nil
	}

	poller.EnqueueAddressIdentityProbe(circuitManagingDeviceVR71Address)
	probeTask := taskScheduler.nextTask(context.Background())
	if probeTask == nil {
		t.Fatal("late identity probe was not scheduled")
	}
	canceledProbeCtx, cancelProbe := context.WithCancel(context.Background())
	cancelProbe()
	probeTask.run(canceledProbeCtx)
	taskScheduler.taskDone(probeTask)

	scheduled := make([]semanticTaskKey, 0, 2)
	for len(scheduled) < 2 {
		taskScheduler.mu.Lock()
		pending := len(taskScheduler.queue)
		taskScheduler.mu.Unlock()
		if pending == 0 {
			break
		}
		task := taskScheduler.nextTask(context.Background())
		scheduled = append(scheduled, task.key)
		task.run(context.Background())
		taskScheduler.taskDone(task)
	}
	verdict := provider.FM5Interpretation()
	if !scanSeamCalled || len(scheduled) != 0 ||
		verdict.Mode != graphql.Fm5SemanticModeInterpreted || verdict.DegradedReason != "" || verdict.EvidenceRevision == "" {
		t.Fatalf("late FM5 recovery seam_called=%t scheduled=%v verdict=%#v; want directed success with inline bounded recovery, no extra queued tasks, and healthy INTERPRETED without ticker", scanSeamCalled, scheduled, verdict)
	}
}

func newFM5DeadlineRecoveryTestPoller(tasks *semanticTaskScheduler) (*vaillantSemanticPoller, *graphql.LiveSemanticProvider) {
	now := time.Date(2026, 8, 15, 21, 0, 0, 0, time.UTC)
	provider := graphql.NewLiveSemanticProvider()
	return &vaillantSemanticPoller{
		scheduler:                ebusgateway.NewSemanticReadScheduler(),
		tasks:                    tasks,
		reg:                      registry.NewDeviceRegistry(nil),
		bus:                      &protocol.Bus{},
		provider:                 provider,
		source:                   0x7F,
		controller:               0x15,
		requestTimeout:           50 * time.Millisecond,
		system:                   &vaillantSystemSnapshot{Controller: 0x15},
		radioDevices:             make(map[radioDeviceKey]*vaillantRadioDeviceSnapshot),
		solarCylinders:           make(map[byte]*vaillantCylinderSnapshot),
		fm5EvidenceTTL:           5 * time.Minute,
		deviceSlotRediscoveryTTL: 30 * time.Minute,
		deviceSlotCache:          make(map[deviceSlotKey]bool),
		nowFn:                    func() time.Time { return now },
	}, provider
}

func fm5InstanceOneSemanticTestResponse(frame protocol.Frame) (*protocol.Frame, error) {
	if len(frame.Data) != 6 {
		return nil, errors.New("invalid B524 selector")
	}
	group := frame.Data[2]
	instance := frame.Data[3]
	addr := uint16(frame.Data[4]) | uint16(frame.Data[5])<<8
	if group == localRegulator.group && addr == system_module_configuration_vr71 {
		return testB524ResponseForSelectorPayload(frame.Data, 0x02, 0x00), nil
	}
	if group == remoteFunctionalModules.group {
		switch addr {
		case device_slot_connected:
			if instance == 0x01 {
				return testB524ResponseForSelectorPayload(frame.Data, 0x01), nil
			}
			return testB524ResponseForSelectorPayload(frame.Data, 0x00), nil
		case device_slot_class_address:
			if instance == 0x01 {
				return testB524ResponseForSelectorPayload(frame.Data, circuitManagingDeviceVR71Address), nil
			}
			return testB524ResponseForSelectorPayload(frame.Data, 0xFF), nil
		case device_slot_firmware:
			if instance == 0x01 {
				return testB524ResponseForSelectorPayload(frame.Data, 0x01, 0x00, 0x00), nil
			}
			return testB524ResponseForSelectorPayload(frame.Data, 0xFF, 0xFF, 0xFF), nil
		case device_slot_hardware_identifier:
			if instance == 0x01 {
				return testB524ResponseForSelectorPayload(frame.Data, 0x71, 0x00), nil
			}
			return testB524ResponseForSelectorPayload(frame.Data, 0xFF, 0xFF), nil
		}
	}
	if group == localSolar.group && (addr == solar_enabled || addr == solar_pump_active) {
		return testB524ResponseForSelectorPayload(frame.Data, 0x01), nil
	}
	if group == localSolar.group || group == localCylinders.group {
		return testB524ResponseForSelectorPayload(frame.Data, testB524Float32Payload(47.25)...), nil
	}
	return testB524ResponseForSelectorPayload(frame.Data, 0x00, 0x00, 0x00, 0x00), nil
}

func TestFM5Startup_PositiveFastSlotClassifiesBeforeUnrelatedRadioTailDeadline(t *testing.T) {
	poller, provider := newFM5DeadlineRecoveryTestPoller(nil)
	config := uint16(2)
	poller.system.ModuleConfigurationVR71 = &config
	startupCtx, cancelStartup := context.WithCancel(context.Background())
	defer cancelStartup()
	tailEntered := false
	poller.sendFrameFn = func(ctx context.Context, frame protocol.Frame) (*protocol.Frame, error) {
		if len(frame.Data) == 6 &&
			(frame.Data[2] == remoteRegulators.group || frame.Data[2] == remoteThermostats.group) &&
			frame.Data[3] > semanticStartupSlotFastMaxInstance {
			tailEntered = true
			cancelStartup()
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return fm5InstanceOneSemanticTestResponse(frame)
	}

	poller.refreshRadioDevicesStartup(startupCtx)
	poller.refreshFM5SemanticStartup(startupCtx)

	verdict := provider.FM5Interpretation()
	if verdict.Mode != graphql.Fm5SemanticModeInterpreted || verdict.DegradedReason != "" || verdict.EvidenceRevision == "" {
		t.Fatalf("deadline-aware startup verdict=%#v tail_entered=%t; want healthy INTERPRETED before unrelated regulator/thermostat tail", verdict, tailEntered)
	}
}

func TestEnqueueAddressIdentityProbe_SaturatedQueueCoalescesGuaranteedOrderedFM5Recovery(t *testing.T) {
	originalScanDirected := registryScanDirectedFn
	t.Cleanup(func() { registryScanDirectedFn = originalScanDirected })

	taskScheduler := newSemanticTaskScheduler()
	taskScheduler.maxDepth = 4
	poller, provider := newFM5DeadlineRecoveryTestPoller(taskScheduler)
	scanSeamCalled := false
	registryScanDirectedFn = func(_ context.Context, _ registry.ScanBus, reg *registry.DeviceRegistry, _ byte, targets []byte) ([]registry.DeviceEntry, error) {
		scanSeamCalled = true
		if len(targets) != 1 || targets[0] != circuitManagingDeviceVR71Address {
			return nil, errors.New("unexpected directed identity target")
		}
		entry := reg.Register(registry.DeviceInfo{
			Address:      circuitManagingDeviceVR71Address,
			Manufacturer: "Vaillant",
			DeviceID:     circuitManagingDeviceVR71ID,
		})
		return []registry.DeviceEntry{entry}, nil
	}

	readOrder := make([]string, 0, 2)
	systemRecorded := false
	radioRecorded := false
	poller.sendFrameFn = func(_ context.Context, frame protocol.Frame) (*protocol.Frame, error) {
		if len(frame.Data) == 6 {
			group := frame.Data[2]
			addr := uint16(frame.Data[4]) | uint16(frame.Data[5])<<8
			if group == localRegulator.group && addr == system_module_configuration_vr71 && !systemRecorded {
				readOrder = append(readOrder, "system")
				systemRecorded = true
			} else if (group == remoteRegulators.group || group == remoteThermostats.group || group == remoteFunctionalModules.group) && !radioRecorded {
				readOrder = append(readOrder, "radio")
				radioRecorded = true
			}
		}
		return fm5InstanceOneSemanticTestResponse(frame)
	}

	if err := taskScheduler.submitCoalesced(semanticTaskRefreshRadioDevices, semanticTaskPriorityLow, func(ctx context.Context) {
		poller.refreshRadioDevices(ctx)
	}); err != nil {
		t.Fatalf("queue older radio task: %v", err)
	}
	poller.EnqueueAddressIdentityProbe(circuitManagingDeviceVR71Address)
	for _, key := range []semanticTaskKey{"fm5-p2-fill-1", "fm5-p2-fill-2"} {
		if err := taskScheduler.submitCoalesced(key, semanticTaskPriorityLow, func(context.Context) {}); err != nil {
			t.Fatalf("fill queue before probe: %v", err)
		}
	}

	var probeTask *semanticTask
	taskScheduler.mu.Lock()
	for i, task := range taskScheduler.queue {
		if task.key == "" {
			probeTask = task
			taskScheduler.queue = append(taskScheduler.queue[:i], taskScheduler.queue[i+1:]...)
			break
		}
	}
	taskScheduler.mu.Unlock()
	if probeTask == nil {
		t.Fatal("late identity probe task missing from saturated queue fixture")
	}
	if err := taskScheduler.submitCoalesced("fm5-p2-fill-3", semanticTaskPriorityLow, func(context.Context) {}); err != nil {
		t.Fatalf("refill queue while identity probe runs: %v", err)
	}
	probeTask.run(context.Background())
	taskScheduler.taskDone(probeTask)

	recoveryTasks := 0
	taskScheduler.mu.Lock()
	for _, task := range taskScheduler.queue {
		if task.key == semanticTaskRefreshSystem || task.key == semanticTaskRefreshRadioDevices {
			recoveryTasks++
		}
	}
	taskScheduler.mu.Unlock()
	for {
		taskScheduler.mu.Lock()
		pending := len(taskScheduler.queue)
		taskScheduler.mu.Unlock()
		if pending == 0 {
			break
		}
		task := taskScheduler.nextTask(context.Background())
		task.run(context.Background())
		taskScheduler.taskDone(task)
	}

	verdict := provider.FM5Interpretation()
	ordered := len(readOrder) >= 2 && readOrder[0] == "system" && readOrder[1] == "radio"
	if !scanSeamCalled || recoveryTasks != 1 || !ordered ||
		verdict.Mode != graphql.Fm5SemanticModeInterpreted || verdict.DegradedReason != "" || verdict.EvidenceRevision == "" {
		t.Fatalf("saturated late recovery seam_called=%t recovery_tasks=%d read_order=%v verdict=%#v; want one guaranteed coalesced system-then-radio recovery and healthy INTERPRETED", scanSeamCalled, recoveryTasks, readOrder, verdict)
	}
}

func TestEnqueueAddressIdentityProbe_LateFM5RecoveryReadsTargetedGateBeforeNoncriticalSystem(t *testing.T) {
	originalScanDirected := registryScanDirectedFn
	t.Cleanup(func() { registryScanDirectedFn = originalScanDirected })

	taskScheduler := newSemanticTaskScheduler()
	poller, provider := newFM5DeadlineRecoveryTestPoller(taskScheduler)
	scanSeamCalled := false
	registryScanDirectedFn = func(_ context.Context, _ registry.ScanBus, reg *registry.DeviceRegistry, _ byte, targets []byte) ([]registry.DeviceEntry, error) {
		scanSeamCalled = true
		if len(targets) != 1 || targets[0] != circuitManagingDeviceVR71Address {
			return nil, errors.New("unexpected directed identity target")
		}
		entry := reg.Register(registry.DeviceInfo{
			Address:      circuitManagingDeviceVR71Address,
			Manufacturer: "Vaillant",
			DeviceID:     circuitManagingDeviceVR71ID,
		})
		return []registry.DeviceEntry{entry}, nil
	}

	taskCtx, cancelTask := context.WithCancel(context.Background())
	defer cancelTask()
	readOrder := make([]string, 0, 2)
	blockedNoncriticalSystem := false
	gateRecorded := false
	radioRecorded := false
	poller.sendFrameFn = func(ctx context.Context, frame protocol.Frame) (*protocol.Frame, error) {
		if len(frame.Data) != 6 {
			return nil, errors.New("invalid B524 selector")
		}
		group := frame.Data[2]
		addr := uint16(frame.Data[4]) | uint16(frame.Data[5])<<8
		if group == localRegulator.group && addr == system_module_configuration_vr71 {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if !gateRecorded {
				readOrder = append(readOrder, "gate")
				gateRecorded = true
			}
			return fm5InstanceOneSemanticTestResponse(frame)
		}
		targetedSystemRead := addr == system_scheme || addr == system_water_pressure
		if group == localRegulator.group && !targetedSystemRead && !radioRecorded {
			blockedNoncriticalSystem = true
			cancelTask()
			return nil, context.DeadlineExceeded
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if (group == remoteRegulators.group || group == remoteThermostats.group || group == remoteFunctionalModules.group) && !radioRecorded {
			readOrder = append(readOrder, "radio")
			radioRecorded = true
		}
		return fm5InstanceOneSemanticTestResponse(frame)
	}

	poller.EnqueueAddressIdentityProbe(circuitManagingDeviceVR71Address)
	probeTask := taskScheduler.nextTask(context.Background())
	if probeTask == nil {
		t.Fatal("late identity probe task was not scheduled")
	}
	probeTask.run(taskCtx)
	taskScheduler.taskDone(probeTask)

	verdict := provider.FM5Interpretation()
	ordered := len(readOrder) == 2 && readOrder[0] == "gate" && readOrder[1] == "radio"
	if !scanSeamCalled || blockedNoncriticalSystem || !ordered ||
		verdict.Mode != graphql.Fm5SemanticModeInterpreted || verdict.DegradedReason != "" || verdict.EvidenceRevision == "" {
		t.Fatalf("deadline-aware late recovery seam_called=%t blocked_noncritical=%t read_order=%v verdict=%#v; want targeted gate then radio and healthy INTERPRETED without ordinary ticker", scanSeamCalled, blockedNoncriticalSystem, readOrder, verdict)
	}
}

func TestRefreshRadioDevices_CompleteNegativeFM5ScanSupersedesRetainedRegistryIdentity(t *testing.T) {
	now := time.Date(2026, 8, 15, 15, 0, 0, 0, time.UTC)
	config := uint16(2)
	classAddress := uint8(circuitManagingDeviceVR71Address)
	collector := 61.5
	temperature := 47.25
	previous := graphql.Fm5Interpretation{
		Mode:             graphql.Fm5SemanticModeInterpreted,
		EvidenceRevision: "fm5-g7-a41",
	}
	provider := graphql.NewLiveSemanticProvider()
	provider.SetFM5Interpretation(previous)
	provider.SetSolar(&graphql.SolarStatus{CollectorTemperatureC: &collector})
	provider.SetCylinders([]graphql.CylinderStatus{{Index: 0, TemperatureC: &temperature}})
	reg := registry.NewDeviceRegistry(nil)
	reg.Register(registry.DeviceInfo{
		Address:      circuitManagingDeviceVR71Address,
		Manufacturer: "Vaillant",
		DeviceID:     circuitManagingDeviceVR71ID,
	})
	poller := &vaillantSemanticPoller{
		scheduler:             ebusgateway.NewSemanticReadScheduler(),
		reg:                   reg,
		provider:              provider,
		source:                0x7F,
		controller:            0x15,
		requestTimeout:        50 * time.Millisecond,
		system:                &vaillantSystemSnapshot{Controller: 0x15, ModuleConfigurationVR71: &config},
		radioDevices:          map[radioDeviceKey]*vaillantRadioDeviceSnapshot{{Group: remoteFunctionalModules.group}: {DeviceClassAddress: &classAddress}},
		deviceSlotCache:       make(map[deviceSlotKey]bool),
		fm5Mode:               graphql.Fm5SemanticModeInterpreted,
		fm5Interpretation:     previous,
		fm5EvidenceRevision:   41,
		fm5EvidenceGeneration: 7,
		fm5IdentityObservedAt: now,
		fm5EvidenceTTL:        5 * time.Minute,
		solar:                 &vaillantSolarSnapshot{CollectorTemperatureC: &collector},
		solarCylinders:        map[byte]*vaillantCylinderSnapshot{0: {Instance: 0, TemperatureC: &temperature}},
		nowFn:                 func() time.Time { return now },
	}
	poller.sendFrameFn = func(_ context.Context, frame protocol.Frame) (*protocol.Frame, error) {
		if len(frame.Data) != 6 {
			return nil, errors.New("invalid B524 selector")
		}
		group := frame.Data[2]
		addr := uint16(frame.Data[4]) | uint16(frame.Data[5])<<8
		if group == remoteFunctionalModules.group {
			switch addr {
			case device_slot_connected:
				return testB524ResponseForSelectorPayload(frame.Data, 0x00), nil
			case device_slot_class_address:
				return testB524ResponseForSelectorPayload(frame.Data, 0xFF), nil
			case device_slot_firmware:
				return testB524ResponseForSelectorPayload(frame.Data, 0xFF, 0xFF, 0xFF), nil
			case device_slot_hardware_identifier:
				return testB524ResponseForSelectorPayload(frame.Data, 0xFF, 0xFF), nil
			}
		}
		return testB524ResponseForSelectorPayload(frame.Data, 0x00, 0x00, 0x00, 0x00), nil
	}

	poller.refreshRadioDevices(context.Background())

	got := provider.FM5Interpretation()
	if got.Mode != graphql.Fm5SemanticModeAbsent || got.DegradedReason != "" {
		t.Errorf("complete negative scan verdict = %#v; want healthy ABSENT despite retained registry entry", got)
	}
	if got.EvidenceRevision == "" || got.EvidenceRevision == previous.EvidenceRevision {
		t.Errorf("complete negative scan revision = %q; want advancement from %q", got.EvidenceRevision, previous.EvidenceRevision)
	}
	if gotMode := provider.FM5SemanticMode(); gotMode != graphql.Fm5SemanticModeAbsent {
		t.Errorf("complete negative scan legacy scalar = %s; want ABSENT", gotMode)
	}
	if solar := provider.Solar(); solar == nil || solar.CollectorTemperatureC != nil {
		t.Errorf("complete negative scan solar = %#v; want cleared structural plane", solar)
	}
	if cylinders := provider.Cylinders(); len(cylinders) != 0 {
		t.Errorf("complete negative scan cylinders = %#v; want cleared structural plane", cylinders)
	}
}

func TestCommitFM5Acquisition_SerializesWriterAfterCommitWithoutDeadlock(t *testing.T) {
	classAddress := uint8(circuitManagingDeviceVR71Address)
	config := uint16(2)
	reg := registry.NewDeviceRegistry(nil)
	reg.Register(registry.DeviceInfo{
		Address:      circuitManagingDeviceVR71Address,
		Manufacturer: "Vaillant",
		DeviceID:     circuitManagingDeviceVR71ID,
	})
	poller := &vaillantSemanticPoller{
		reg:            reg,
		controller:     0x15,
		system:         &vaillantSystemSnapshot{Controller: 0x15, ModuleConfigurationVR71: &config},
		radioDevices:   map[radioDeviceKey]*vaillantRadioDeviceSnapshot{{Group: remoteFunctionalModules.group}: {DeviceClassAddress: &classAddress}},
		solarCylinders: make(map[byte]*vaillantCylinderSnapshot),
		nowFn:          time.Now,
	}
	captured := poller.captureFM5Evidence()
	if !captured.registryCoherent {
		t.Fatal("initial registry capture is incoherent")
	}

	readLocked := make(chan struct{})
	poller.withFM5ObservationGeneration = func(fn func(uint64)) bool {
		return reg.WithObservationGeneration(func(current uint64) {
			close(readLocked)
			fn(current)
		})
	}
	poller.mu.Lock()
	commitDone := make(chan graphql.Fm5Interpretation, 1)
	go func() {
		commitDone <- poller.commitFM5Acquisition(captured, graphql.Fm5Interpretation{
			Mode:             graphql.Fm5SemanticModeInterpreted,
			EvidenceRevision: "fm5-g0-a1",
		}, &vaillantSolarSnapshot{}, map[byte]*vaillantCylinderSnapshot{})
	}()
	<-readLocked

	writerDone := make(chan struct{})
	go func() {
		reg.Register(registry.DeviceInfo{
			Address:      circuitManagingDeviceVR71Address + 1,
			Manufacturer: "Vaillant",
			DeviceID:     "VR71-WRITER-AFTER",
		})
		close(writerDone)
	}()
	poller.mu.Unlock()

	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	var verdict graphql.Fm5Interpretation
	select {
	case verdict = <-commitDone:
	case <-deadline.C:
		t.Fatal("FM5 commit deadlocked with queued registry writer")
	}
	select {
	case <-writerDone:
	case <-deadline.C:
		t.Fatal("registry writer did not complete after FM5 read critical section")
	}
	if verdict.Mode != graphql.Fm5SemanticModeInterpreted || verdict.DegradedReason != "" {
		t.Fatalf("serialized commit verdict = %#v; want INTERPRETED", verdict)
	}
	_, afterGeneration, coherent := poller.captureFM5RegistryEvidence()
	if !coherent || afterGeneration != captured.registryGeneration+1 {
		t.Fatalf("writer-after generation = %d coherent=%t; want %d true", afterGeneration, coherent, captured.registryGeneration+1)
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

func TestResolveGatewayBuildInfo_UsesOneResolvedIdentityAcrossEvidenceSurfaces(t *testing.T) {
	originalResolver := gatewayBuildRevisionResolver
	t.Cleanup(func() { gatewayBuildRevisionResolver = originalResolver })
	calls := 0
	gatewayBuildRevisionResolver = func() (string, error) {
		calls++
		return "0123456789abcdef0123456789abcdef01234567", nil
	}

	info, err := resolveGatewayBuildInfo("0.6.42", "unknown")
	if err != nil {
		t.Fatalf("resolveGatewayBuildInfo() error = %v", err)
	}
	if calls != 1 {
		t.Fatalf("revision resolver calls = %d; want 1", calls)
	}
	if info.BuildID != "0123456789abcdef0123456789abcdef01234567" {
		t.Fatalf("BuildID = %q; want resolved full revision", info.BuildID)
	}
	if got, want := info.EvidenceVersion(), "0.6.42+git.0123456789abcdef0123456789abcdef01234567"; got != want {
		t.Fatalf("EvidenceVersion() = %q; want %q", got, want)
	}
	oneShot := info.OneShotEvidenceIdentity()
	if oneShot.RecorderVersion != info.EvidenceVersion() || oneShot.ReplayVersion != info.EvidenceVersion() || oneShot.OperationVersion != "git:"+info.BuildID {
		t.Fatalf("one-shot identity = %#v; want the same resolved build identity", oneShot)
	}
	if got, want := gatewayBuildString(info), "0.6.42+0123456789abcdef0123456789abcdef01234567"; got != want {
		t.Fatalf("gateway build = %q; want %q", got, want)
	}
}

func TestResolveGatewayBuildInfo_UnavailableRevisionFallsBackToSingleUnknownIdentity(t *testing.T) {
	originalResolver := gatewayBuildRevisionResolver
	t.Cleanup(func() { gatewayBuildRevisionResolver = originalResolver })
	calls := 0
	gatewayBuildRevisionResolver = func() (string, error) {
		calls++
		return "", errors.New("vcs metadata omitted")
	}

	info, err := resolveGatewayBuildInfo("dev", "unknown")
	if err != nil {
		t.Fatalf("resolveGatewayBuildInfo() error = %v", err)
	}
	if calls != 1 {
		t.Fatalf("revision resolver calls = %d; want 1", calls)
	}
	if info.BuildID != "unknown" {
		t.Fatalf("BuildID = %q; want unknown fallback", info.BuildID)
	}
	if got, want := info.EvidenceVersion(), "dev+build.unknown"; got != want {
		t.Fatalf("EvidenceVersion() = %q; want %q", got, want)
	}
	oneShot := info.OneShotEvidenceIdentity()
	if oneShot.RecorderVersion != "dev+build.unknown" || oneShot.ReplayVersion != "dev+build.unknown" || oneShot.OperationVersion != "build:unknown" {
		t.Fatalf("one-shot identity = %#v; want same unknown fallback", oneShot)
	}
	if got, want := gatewayBuildString(info), "dev+unknown"; got != want {
		t.Fatalf("gateway build = %q; want %q", got, want)
	}
}

func TestGatewayBuildInfo_ExplicitNonRevisionBuildIDHasClosedEvidenceForm(t *testing.T) {
	info, err := resolveGatewayBuildInfo("0.6.42", "ci-build-42")
	if err != nil {
		t.Fatalf("resolveGatewayBuildInfo() error = %v", err)
	}
	if got, want := info.EvidenceVersion(), "0.6.42+build.ci-build-42"; got != want {
		t.Fatalf("EvidenceVersion() = %q; want %q", got, want)
	}
	if got, want := info.OneShotEvidenceIdentity().OperationVersion, "build:ci-build-42"; got != want {
		t.Fatalf("OperationVersion = %q; want %q", got, want)
	}
}

type legacyOnlySemanticProvider struct {
	graphql.SemanticProvider
}

func TestPortalFM5Interpretation_UnavailableBeforeFirstCoherentClassification(t *testing.T) {
	provider := graphql.NewLiveSemanticProvider()

	if got := portalFM5Interpretation(provider); got != (graphql.Fm5Interpretation{}) {
		t.Fatalf("Portal FM5 interpretation = %#v; want unavailable zero tuple", got)
	}
	if got := provider.FM5SemanticMode(); got != graphql.Fm5SemanticModeAbsent {
		t.Fatalf("legacy FM5 scalar = %s; want stable ABSENT", got)
	}
}

func TestPortalFM5Interpretation_LegacyOnlyProviderIsUnavailable(t *testing.T) {
	provider := graphql.NewLiveSemanticProvider()
	provider.SetFM5SemanticMode(graphql.Fm5SemanticModeGPIOOnly)
	legacy := legacyOnlySemanticProvider{SemanticProvider: provider}

	if got := legacy.FM5SemanticMode(); got != graphql.Fm5SemanticModeGPIOOnly {
		t.Fatalf("legacy FM5 scalar = %s; want stable GPIO_ONLY", got)
	}
	if got := portalFM5Interpretation(legacy); got != (graphql.Fm5Interpretation{}) {
		t.Fatalf("Portal legacy-only interpretation = %#v; want unavailable zero tuple", got)
	}
}

func TestMCPSemanticProviderAdapter_LegacyOnlyInterpretationIsUnavailable(t *testing.T) {
	provider := graphql.NewLiveSemanticProvider()
	provider.SetFM5SemanticMode(graphql.Fm5SemanticModeGPIOOnly)
	legacy := legacyOnlySemanticProvider{SemanticProvider: provider}

	if got := legacy.FM5SemanticMode(); got != graphql.Fm5SemanticModeGPIOOnly {
		t.Fatalf("legacy FM5 scalar = %s; want stable GPIO_ONLY", got)
	}
	mcpVerdict := (mcpSemanticProviderAdapter{provider: legacy}).FM5Interpretation()
	if mcpVerdict.Mode != "" || mcpVerdict.DegradedReason != nil || mcpVerdict.EvidenceRevision != "" {
		t.Fatalf("MCP adapter legacy-only interpretation = %#v; want unavailable zero tuple", mcpVerdict)
	}
}
