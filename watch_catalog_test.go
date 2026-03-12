package ebusgateway

import (
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
)

func TestWatchDescriptorEffectiveFreshnessTTLUsesProfileDefault(t *testing.T) {
	t.Parallel()

	descriptor := WatchDescriptor{
		Key:               NewB524WatchKey(0x15, 0x06, 0x01, 0x00, 0x0005),
		SemanticClass:     WatchSemanticClassState,
		FreshnessProfile:  WatchFreshnessProfileStateFast,
		DecoderID:         "vaillant.f32le",
		CorrelationPolicy: WatchCorrelationPolicyRequestResponse,
		DirectApplyPolicy: WatchDirectApplyPolicyStateDefault,
	}

	ttl, err := descriptor.EffectiveFreshnessTTL()
	if err != nil {
		t.Fatalf("EffectiveFreshnessTTL() error = %v", err)
	}
	if ttl != 10*time.Second {
		t.Fatalf("EffectiveFreshnessTTL() = %s; want 10s", ttl)
	}
}

func TestWatchDescriptorRejectsInvalidSemanticPairing(t *testing.T) {
	t.Parallel()

	descriptor := WatchDescriptor{
		Key:               NewB524WatchKey(0x15, 0x06, 0x01, 0x00, 0x0005),
		SemanticClass:     WatchSemanticClassState,
		FreshnessProfile:  WatchFreshnessProfileConfig,
		DecoderID:         "vaillant.f32le",
		CorrelationPolicy: WatchCorrelationPolicyRequestResponse,
		DirectApplyPolicy: WatchDirectApplyPolicyStateDefault,
	}

	if _, err := descriptor.EffectiveFreshnessTTL(); err == nil {
		t.Fatal("EffectiveFreshnessTTL() succeeded for invalid semantic class / freshness profile pairing")
	}
}

func TestWatchDescriptorRejectsWidenedTTL(t *testing.T) {
	t.Parallel()

	ttl := 31 * time.Second
	descriptor := WatchDescriptor{
		Key:               NewB524WatchKey(0x15, 0x06, 0x01, 0x00, 0x0005),
		SemanticClass:     WatchSemanticClassState,
		FreshnessProfile:  WatchFreshnessProfileStateSlow,
		DecoderID:         "vaillant.f32le",
		CorrelationPolicy: WatchCorrelationPolicyRequestResponse,
		DirectApplyPolicy: WatchDirectApplyPolicyStateDefault,
		FreshnessTTL:      &ttl,
	}

	if _, err := descriptor.EffectiveFreshnessTTL(); err == nil {
		t.Fatal("EffectiveFreshnessTTL() succeeded for widened ttl")
	}
}

func TestNewWatchCatalogRejectsDuplicateKeys(t *testing.T) {
	t.Parallel()

	descriptor := WatchDescriptor{
		Key:               NewB509WatchKey(0x08, 0x0200),
		SemanticClass:     WatchSemanticClassState,
		FreshnessProfile:  WatchFreshnessProfileStateSlow,
		DecoderID:         "vaillant.data2b",
		CorrelationPolicy: WatchCorrelationPolicyRequestResponse,
		DirectApplyPolicy: WatchDirectApplyPolicyStateDefault,
	}

	if _, err := NewWatchCatalog([]WatchDescriptor{descriptor, descriptor}); err == nil {
		t.Fatal("NewWatchCatalog() succeeded for duplicate descriptor key")
	}
}

func TestWatchActivationSetUnionSemanticsAndObservationSummary(t *testing.T) {
	t.Parallel()

	activeKey := NewB524WatchKey(0x15, 0x06, 0x03, 0x00, 0x000F)
	inactiveKey := NewB509WatchKey(0x08, 0x0200)

	catalog, err := NewWatchCatalog([]WatchDescriptor{
		{
			Key:               activeKey,
			SemanticClass:     WatchSemanticClassState,
			FreshnessProfile:  WatchFreshnessProfileStateFast,
			DecoderID:         "vaillant.f32le",
			CorrelationPolicy: WatchCorrelationPolicyRequestResponse,
			DirectApplyPolicy: WatchDirectApplyPolicyStateDefault,
		},
		{
			Key:               inactiveKey,
			SemanticClass:     WatchSemanticClassState,
			FreshnessProfile:  WatchFreshnessProfileStateSlow,
			DecoderID:         "vaillant.data2b",
			CorrelationPolicy: WatchCorrelationPolicyRequestResponse,
			DirectApplyPolicy: WatchDirectApplyPolicyStateDefault,
		},
	})
	if err != nil {
		t.Fatalf("NewWatchCatalog() error = %v", err)
	}

	activations := NewWatchActivationSet(catalog)
	if err := activations.Activate(WatchActivationSourcePoller, activeKey); err != nil {
		t.Fatalf("Activate(poller) error = %v", err)
	}
	if err := activations.Activate(WatchActivationSourceTooling, activeKey); err != nil {
		t.Fatalf("Activate(tooling) error = %v", err)
	}

	active := activations.Observe(activeKey)
	if active.State != WatchObservationStateActive {
		t.Fatalf("Observe(activeKey).State = %s; want %s", active.State, WatchObservationStateActive)
	}
	if got, want := active.Sources, []WatchActivationSource{WatchActivationSourcePoller, WatchActivationSourceTooling}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("Observe(activeKey).Sources = %v; want %v", got, want)
	}

	inactive := activations.Observe(inactiveKey)
	if inactive.State != WatchObservationStateInactive {
		t.Fatalf("Observe(inactiveKey).State = %s; want %s", inactive.State, WatchObservationStateInactive)
	}

	miss := activations.Observe(NewB524WatchKey(0x15, 0x06, 0x03, 0x00, 0x0022))
	if miss.State != WatchObservationStateCatalogMiss {
		t.Fatalf("Observe(miss).State = %s; want %s", miss.State, WatchObservationStateCatalogMiss)
	}

	summary := activations.Summary()
	if summary.ActiveTotal != 1 || summary.InactiveTotal != 1 || summary.CatalogMissTotal != 1 {
		t.Fatalf("Summary() = %+v; want active=1 inactive=1 catalog_miss=1", summary)
	}

	activations.Deactivate(WatchActivationSourcePoller, activeKey)
	remaining := activations.ActiveSources(activeKey)
	if len(remaining) != 1 || remaining[0] != WatchActivationSourceTooling {
		t.Fatalf("ActiveSources() after partial deactivate = %v; want [tooling]", remaining)
	}
}

func TestWatchActivationSetRejectsCatalogMissActivation(t *testing.T) {
	t.Parallel()

	catalog, err := NewWatchCatalog([]WatchDescriptor{
		{
			Key:               NewB509WatchKey(0x08, 0x0200),
			SemanticClass:     WatchSemanticClassState,
			FreshnessProfile:  WatchFreshnessProfileStateSlow,
			DecoderID:         "vaillant.data2b",
			CorrelationPolicy: WatchCorrelationPolicyRequestResponse,
			DirectApplyPolicy: WatchDirectApplyPolicyStateDefault,
		},
	})
	if err != nil {
		t.Fatalf("NewWatchCatalog() error = %v", err)
	}

	activations := NewWatchActivationSet(catalog)
	if err := activations.Activate(WatchActivationSourcePoller, NewB524WatchKey(0x15, 0x06, 0x01, 0x00, 0x0005)); err == nil {
		t.Fatal("Activate() succeeded for key outside immutable catalog")
	}
}

func TestPassiveWatchKeyFromEventMatchesB524SchedulerKey(t *testing.T) {
	t.Parallel()

	expected := NewB524WatchKey(0x15, 0x06, 0x03, 0x01, 0x001C)
	event := PassiveClassifiedEvent{
		HasRequest: true,
		Request: protocol.Frame{
			Source:    0x10,
			Target:    0x15,
			Primary:   0xB5,
			Secondary: 0x24,
			Data:      []byte{0x06, 0x00, 0x03, 0x01, 0x1C, 0x00},
		},
	}

	key, ok := PassiveWatchKeyFromEvent(event)
	if !ok {
		t.Fatal("PassiveWatchKeyFromEvent() = !ok; want ok")
	}
	if got := key.Canonical(); got != expected.Canonical() {
		t.Fatalf("PassiveWatchKeyFromEvent().Canonical() = %q; want %q", got, expected.Canonical())
	}
}

func TestPassiveWatchKeyFromEventMatchesB509SchedulerKey(t *testing.T) {
	t.Parallel()

	expected := NewB509WatchKey(0x08, 0x0200)
	event := PassiveClassifiedEvent{
		HasRequest: true,
		Request: protocol.Frame{
			Source:    0x10,
			Target:    0x08,
			Primary:   0xB5,
			Secondary: 0x09,
			Data:      []byte{0x0D, 0x02, 0x00},
		},
	}

	key, ok := PassiveWatchKeyFromEvent(event)
	if !ok {
		t.Fatal("PassiveWatchKeyFromEvent() = !ok; want ok")
	}
	if got := key.Canonical(); got != expected.Canonical() {
		t.Fatalf("PassiveWatchKeyFromEvent().Canonical() = %q; want %q", got, expected.Canonical())
	}
}

func TestWatchKeyBuildersRemainStable(t *testing.T) {
	t.Parallel()

	if got := NewB516WatchKey(0x15, 0x01, 0x02, 0x03).Canonical(); got != "b516:15:01:02:03" {
		t.Fatalf("NewB516WatchKey().Canonical() = %q; want %q", got, "b516:15:01:02:03")
	}
	if got := NewB555WatchKey(0x15, 0xA5, 0x02, 0x06, 0x03).Canonical(); got != "b555:15:a5:02:06:03" {
		t.Fatalf("NewB555WatchKey().Canonical() = %q; want %q", got, "b555:15:a5:02:06:03")
	}
}
