package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
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
		{"fresh coherent absence", true, &configInterpretable, false, false, false, false, false, graphql.Fm5SemanticModeAbsent, ""},
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

func TestPortalFM5Interpretation_LegacyGPIOOnlyIsStructurallyExplained(t *testing.T) {
	provider := graphql.NewLiveSemanticProvider()
	provider.SetFM5SemanticMode(graphql.Fm5SemanticModeGPIOOnly)
	legacy := legacyOnlySemanticProvider{SemanticProvider: provider}

	verdict := portalFM5Interpretation(legacy)
	if err := verdict.Validate(); err != nil {
		t.Fatalf("Portal fallback verdict invalid: %v", err)
	}
	if verdict.DegradedReason != graphql.Fm5SemanticDegradedReasonConfigurationNotInterpretable {
		t.Fatalf("Portal fallback reason = %q; want %q", verdict.DegradedReason, graphql.Fm5SemanticDegradedReasonConfigurationNotInterpretable)
	}
}
