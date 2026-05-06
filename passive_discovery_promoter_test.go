package ebusgateway

import (
	"context"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
)

func newTestPromoter(t *testing.T, reg *registry.DeviceRegistry, evidence *EvidenceBuffer, confirmFn func(byte) bool) *PassiveDiscoveryPromoter {
	t.Helper()
	promoter, err := NewPassiveDiscoveryPromoter(PassiveDiscoveryPromoterOptions{
		Registry:       reg,
		EvidenceBuffer: evidence,
		ConfirmFn: func(_ context.Context, target byte) bool {
			return confirmFn(target)
		},
		AdmittedSourceFn: func() byte { return 0x71 },
		TickInterval:     1 * time.Millisecond,
		Now:              time.Now,
	})
	if err != nil {
		t.Fatalf("NewPassiveDiscoveryPromoter: %v", err)
	}
	return promoter
}

// TestPassiveDiscoveryPromoter_RegistersConfirmedAddress pins the
// happy path: an address promoted by passive evidence + coherent
// active confirmation lands in the registry as a Vaillant device.
func TestPassiveDiscoveryPromoter_RegistersConfirmedAddress(t *testing.T) {
	t.Parallel()

	reg := registry.NewDeviceRegistry(nil)
	reg.Register(registry.DeviceInfo{Address: 0x08, Manufacturer: "Vaillant", DeviceID: "BAI00"})

	buf, err := NewEvidenceBuffer(128, VaillantBaselineTopologySeed)
	if err != nil {
		t.Fatalf("NewEvidenceBuffer: %v", err)
	}
	now := time.Now()
	buf.Record(EvidenceRecord{Address: 0x15, Strength: EvidenceStrong, Observed: now, Kind: "test"})

	promoter := newTestPromoter(t, reg, buf, func(target byte) bool {
		return target == 0x15
	})
	promoter.processOnce(context.Background())

	if !registryContains(reg, 0x15) {
		t.Fatal("promoter did not register confirmed 0x15 in registry")
	}
	snap := promoter.Snapshot()
	if snap.ConfirmedTotal != 1 {
		t.Fatalf("ConfirmedTotal = %d; want 1", snap.ConfirmedTotal)
	}
}

// TestPassiveDiscoveryPromoter_DoesNotRegisterOnConfirmationFailure pins
// the negative case: an address promoted by evidence but NOT coherent
// under active confirmation must not be registered. Failed attempts
// schedule per-address backoff for retry.
func TestPassiveDiscoveryPromoter_DoesNotRegisterOnConfirmationFailure(t *testing.T) {
	t.Parallel()

	reg := registry.NewDeviceRegistry(nil)
	buf, _ := NewEvidenceBuffer(128, VaillantBaselineTopologySeed)
	buf.Record(EvidenceRecord{Address: 0x15, Strength: EvidenceStrong, Observed: time.Now(), Kind: "test"})

	promoter := newTestPromoter(t, reg, buf, func(byte) bool { return false })
	promoter.processOnce(context.Background())

	if registryContains(reg, 0x15) {
		t.Fatal("promoter registered 0x15 despite confirmation failure")
	}
	snap := promoter.Snapshot()
	if snap.RejectedTotal != 1 {
		t.Fatalf("RejectedTotal = %d; want 1", snap.RejectedTotal)
	}
	if state, ok := snap.PendingByAddr[0x15]; !ok || state != 1 {
		t.Fatalf("PendingByAddr[0x15] = %d (ok=%v); want 1", state, ok)
	}
}

// TestPassiveDiscoveryPromoter_SkipsAlreadyRegisteredAddress prevents
// duplicate registration: an address already in the registry must
// not generate another confirmation probe even if it appears in
// PromotedAddresses.
func TestPassiveDiscoveryPromoter_SkipsAlreadyRegisteredAddress(t *testing.T) {
	t.Parallel()

	reg := registry.NewDeviceRegistry(nil)
	reg.Register(registry.DeviceInfo{Address: 0x15, Manufacturer: "Vaillant", DeviceID: "BASV2"})

	buf, _ := NewEvidenceBuffer(128, VaillantBaselineTopologySeed)
	buf.Record(EvidenceRecord{Address: 0x15, Strength: EvidenceStrong, Observed: time.Now(), Kind: "test"})

	probed := false
	promoter := newTestPromoter(t, reg, buf, func(byte) bool {
		probed = true
		return true
	})
	promoter.processOnce(context.Background())

	if probed {
		t.Fatal("promoter probed an already-registered address; should be filtered before active confirmation")
	}
	snap := promoter.Snapshot()
	if snap.SkippedTotal == 0 {
		t.Fatal("SkippedTotal = 0; want >= 1 for filtered address")
	}
}

// TestPassiveDiscoveryPromoter_SkipsAdmittedSource pins source-address
// invariant on the active-confirmation side: the admitted source must
// never be probed as a target even if it shows up in PromotedAddresses
// (e.g. via passive self-traffic loop).
func TestPassiveDiscoveryPromoter_SkipsAdmittedSource(t *testing.T) {
	t.Parallel()

	reg := registry.NewDeviceRegistry(nil)
	buf, _ := NewEvidenceBuffer(128, VaillantBaselineTopologySeed)
	// Force-promote 0x71 (the admitted source in newTestPromoter).
	buf.Record(EvidenceRecord{Address: 0x71, Strength: EvidenceStrong, Observed: time.Now(), Kind: "test"})

	probed := false
	promoter := newTestPromoter(t, reg, buf, func(byte) bool {
		probed = true
		return true
	})
	promoter.processOnce(context.Background())

	if probed {
		t.Fatal("promoter probed admitted source 0x71 as target — source-address invariant violated")
	}
	if registryContains(reg, 0x71) {
		t.Fatal("promoter registered admitted source 0x71 — source-address invariant violated")
	}
}

// TestPassiveDiscoveryPromoter_LateArrivalRegulator_RegistersWithoutRestart
// reproduces the late-startup regulator scenario: gateway started with
// an empty registry; over time the regulator boots and broadcasts
// presence + B524 traffic; the bus_observability store records evidence;
// the promoter confirms and registers without a gateway restart.
func TestPassiveDiscoveryPromoter_LateArrivalRegulator_RegistersWithoutRestart(t *testing.T) {
	t.Parallel()

	store := specimenPassiveStore()
	reg := registry.NewDeviceRegistry(nil)

	base := time.Now().UTC()
	store.now = func() time.Time { return base.Add(60 * time.Second) }

	// Gateway started with no regulator on the bus. Time passes; the
	// regulator boots and emits a sign-of-life broadcast (presence-
	// only — does not promote on its own).
	store.OnPassiveClassifiedEvent(observabilityPassiveBroadcastEvent(base.Add(60*time.Second), 0x15, 0x07, 0xFF))
	// Then it polls the boiler with two coherent B524 reads. The
	// regulator address 0x15 appears as request source (weak evidence,
	// recorded twice → crosses the weak threshold).
	store.OnPassiveClassifiedEvent(observabilityCoherentB524TransactionEvent(base.Add(61*time.Second), 0x15, 0x08))
	store.OnPassiveClassifiedEvent(observabilityCoherentB524TransactionEvent(base.Add(62*time.Second), 0x15, 0x08))

	semanticRefreshed := false
	routerRefreshed := false

	promoter, err := NewPassiveDiscoveryPromoter(PassiveDiscoveryPromoterOptions{
		Registry:       reg,
		EvidenceBuffer: store.EvidenceBuffer(),
		ConfirmFn: func(_ context.Context, target byte) bool {
			return target == 0x15
		},
		SemanticRefreshFn: func() { semanticRefreshed = true },
		RouterRefreshFn:   func() { routerRefreshed = true },
		AdmittedSourceFn:  func() byte { return 0x7F },
		TickInterval:      1 * time.Millisecond,
		Now:               time.Now,
	})
	if err != nil {
		t.Fatalf("NewPassiveDiscoveryPromoter: %v", err)
	}

	promoter.processOnce(context.Background())

	if !registryContains(reg, 0x15) {
		t.Fatal("late-arrival regulator 0x15 not registered after passive evidence + active confirmation")
	}
	if !semanticRefreshed {
		t.Fatal("semantic refresh not signaled after late-arrival registration; regulator surface won't populate")
	}
	if !routerRefreshed {
		t.Fatal("router planes not refreshed after late-arrival registration; live event routing won't include 0x15")
	}
}

// TestPassiveDiscoveryPromoter_LateArrivalVR71_RegistersWithoutRestart
// covers the user-stated parallel scenario for 0x26 (VR_71 / FM5
// primary controller) booting after the gateway. Same shape as the
// 0x15 regulator test, using 0x26 as the late arrival.
func TestPassiveDiscoveryPromoter_LateArrivalVR71_RegistersWithoutRestart(t *testing.T) {
	t.Parallel()

	store := specimenPassiveStore()
	reg := registry.NewDeviceRegistry(nil)

	base := time.Now().UTC()
	store.now = func() time.Time { return base.Add(60 * time.Second) }

	store.OnPassiveClassifiedEvent(observabilityCoherentB524TransactionEvent(base.Add(61*time.Second), 0x26, 0x08))
	store.OnPassiveClassifiedEvent(observabilityCoherentB524TransactionEvent(base.Add(62*time.Second), 0x26, 0x08))

	promoter, _ := NewPassiveDiscoveryPromoter(PassiveDiscoveryPromoterOptions{
		Registry:       reg,
		EvidenceBuffer: store.EvidenceBuffer(),
		ConfirmFn: func(_ context.Context, target byte) bool {
			return target == 0x26
		},
		AdmittedSourceFn: func() byte { return 0x7F },
		TickInterval:     1 * time.Millisecond,
		Now:              time.Now,
	})
	promoter.processOnce(context.Background())

	if !registryContains(reg, 0x26) {
		t.Fatal("late-arrival VR_71 / FM5 0x26 not registered after passive evidence + active confirmation")
	}
}

// TestPassiveDiscoveryPromoter_DemotesAfterMaxFailedConfirmations pins
// the B5 contract from adversarial review: a candidate that fails
// confirmation maxConfirmationAttempts times is demoted from the
// EvidenceBuffer's promoted set, so the promoter does not retry it
// every backoff cycle for the gateway lifetime.
func TestPassiveDiscoveryPromoter_DemotesAfterMaxFailedConfirmations(t *testing.T) {
	t.Parallel()

	reg := registry.NewDeviceRegistry(nil)
	buf, _ := NewEvidenceBuffer(128, VaillantBaselineTopologySeed)
	buf.Record(EvidenceRecord{Address: 0x42, Strength: EvidenceStrong, Observed: time.Now(), Kind: "test"})

	if got := buf.PromotedAddresses(); len(got) == 0 {
		t.Fatal("test setup invalid: 0x42 not promoted before processOnce")
	}

	calls := 0
	now := time.Now()
	currentNow := now
	promoter, _ := NewPassiveDiscoveryPromoter(PassiveDiscoveryPromoterOptions{
		Registry:       reg,
		EvidenceBuffer: buf,
		ConfirmFn: func(_ context.Context, _ byte) bool {
			calls++
			return false
		},
		AdmittedSourceFn: func() byte { return 0x71 },
		TickInterval:     1 * time.Millisecond,
		Now:              func() time.Time { return currentNow },
	})

	// Drive maxConfirmationAttempts ticks; advance currentNow past
	// each attempt's backoff so subsequent ticks are not gated by
	// the per-address backoff.
	for i := 0; i < 10; i++ {
		promoter.processOnce(context.Background())
		currentNow = currentNow.Add(2 * time.Hour)
	}

	if calls != maxConfirmationAttempts {
		t.Fatalf("confirm calls = %d; want %d (caps at maxConfirmationAttempts)", calls, maxConfirmationAttempts)
	}
	for _, addr := range buf.PromotedAddresses() {
		if addr == 0x42 {
			t.Fatalf("address 0x42 still promoted after %d failed attempts; demotion contract violated", maxConfirmationAttempts)
		}
	}
}

// TestPassiveDiscoveryPromoter_CoalesceSemanticRefreshPerTick pins the
// refresh-coalescing contract from adversarial review N7: even when
// multiple candidates confirm in the same tick, the promoter emits at
// most ONE semantic refresh signal (the queued task will see all newly-
// registered devices on the next discovery scan).
func TestPassiveDiscoveryPromoter_CoalesceSemanticRefreshPerTick(t *testing.T) {
	t.Parallel()

	reg := registry.NewDeviceRegistry(nil)
	buf, _ := NewEvidenceBuffer(128, VaillantBaselineTopologySeed)
	buf.Record(EvidenceRecord{Address: 0x15, Strength: EvidenceStrong, Observed: time.Now(), Kind: "test"})
	buf.Record(EvidenceRecord{Address: 0x26, Strength: EvidenceStrong, Observed: time.Now(), Kind: "test"})

	semanticCalls := 0
	routerCalls := 0
	promoter, _ := NewPassiveDiscoveryPromoter(PassiveDiscoveryPromoterOptions{
		Registry:          reg,
		EvidenceBuffer:    buf,
		ConfirmFn:         func(_ context.Context, _ byte) bool { return true },
		SemanticRefreshFn: func() { semanticCalls++ },
		RouterRefreshFn:   func() { routerCalls++ },
		AdmittedSourceFn:  func() byte { return 0x71 },
		TickInterval:      1 * time.Millisecond,
		Now:               time.Now,
	})

	promoter.processOnce(context.Background())

	if semanticCalls != 1 {
		t.Fatalf("semantic refresh fired %d times in single tick with 2 confirmations; want 1 (coalesced)", semanticCalls)
	}
	// Router refresh runs once per registration (registry-walk needed
	// to reflect each new entry on the plane).
	if routerCalls != 2 {
		t.Fatalf("router refresh fired %d times for 2 confirmations; want 2 (per-registration)", routerCalls)
	}
}

// TestPassiveDiscoveryPromoter_SemanticRefreshFiresAfterAllRegistrations
// closes Codex PR #561 P2 finding: when multiple candidates confirm in
// the same tick, the single coalesced semantic refresh must fire AFTER
// all registrations have committed. Otherwise the semantic discovery
// task could start and snapshot the registry before later candidates
// in the same tick are added — those devices would miss the immediate
// refresh and wait for the periodic discovery interval.
//
// The test pins the ordering by capturing the registry size at the
// moment the semantic refresh fires.
func TestPassiveDiscoveryPromoter_SemanticRefreshFiresAfterAllRegistrations(t *testing.T) {
	t.Parallel()

	reg := registry.NewDeviceRegistry(nil)
	buf, _ := NewEvidenceBuffer(128, VaillantBaselineTopologySeed)
	buf.Record(EvidenceRecord{Address: 0x15, Strength: EvidenceStrong, Observed: time.Now(), Kind: "test"})
	buf.Record(EvidenceRecord{Address: 0x26, Strength: EvidenceStrong, Observed: time.Now(), Kind: "test"})

	registrySizeAtRefresh := -1
	promoter, _ := NewPassiveDiscoveryPromoter(PassiveDiscoveryPromoterOptions{
		Registry:       reg,
		EvidenceBuffer: buf,
		ConfirmFn:      func(_ context.Context, _ byte) bool { return true },
		SemanticRefreshFn: func() {
			count := 0
			reg.Iterate(func(_ registry.DeviceEntry) bool { count++; return true })
			registrySizeAtRefresh = count
		},
		AdmittedSourceFn: func() byte { return 0x71 },
		TickInterval:     1 * time.Millisecond,
		Now:              time.Now,
	})

	promoter.processOnce(context.Background())

	if registrySizeAtRefresh != 2 {
		t.Fatalf("registry size at semantic refresh = %d; want 2 (refresh must fire AFTER all registrations commit)", registrySizeAtRefresh)
	}
}

// TestPassiveDiscoveryPromoter_NoSemanticRefreshWhenNoConfirmations pins
// the negative case: a tick with no successful confirmations must NOT
// emit a semantic refresh signal — there's no inventory change for the
// poller to discover.
func TestPassiveDiscoveryPromoter_NoSemanticRefreshWhenNoConfirmations(t *testing.T) {
	t.Parallel()

	reg := registry.NewDeviceRegistry(nil)
	buf, _ := NewEvidenceBuffer(128, VaillantBaselineTopologySeed)
	buf.Record(EvidenceRecord{Address: 0x42, Strength: EvidenceStrong, Observed: time.Now(), Kind: "test"})

	semanticCalls := 0
	promoter, _ := NewPassiveDiscoveryPromoter(PassiveDiscoveryPromoterOptions{
		Registry:          reg,
		EvidenceBuffer:    buf,
		ConfirmFn:         func(_ context.Context, _ byte) bool { return false }, // never coherent
		SemanticRefreshFn: func() { semanticCalls++ },
		AdmittedSourceFn:  func() byte { return 0x71 },
		TickInterval:      1 * time.Millisecond,
		Now:               time.Now,
	})

	promoter.processOnce(context.Background())

	if semanticCalls != 0 {
		t.Fatalf("semantic refresh fired %d times with zero confirmations; want 0 (no inventory change)", semanticCalls)
	}
}

// TestPassiveDiscoveryPromoter_GatesProbesOnAdmittedSource pins the
// source-admission invariant from Codex PR #561 review: while admission
// is pending (admittedSource == 0) the promoter MUST NOT issue active
// confirmation probes. The confirmFn sources from the semantic
// poller's configured source, which is set even before admission
// completes; running confirmations during the pending window would
// emit gateway-originated traffic that the admission FSM has not yet
// approved.
func TestPassiveDiscoveryPromoter_GatesProbesOnAdmittedSource(t *testing.T) {
	t.Parallel()

	reg := registry.NewDeviceRegistry(nil)
	buf, _ := NewEvidenceBuffer(128, VaillantBaselineTopologySeed)
	buf.Record(EvidenceRecord{Address: 0x15, Strength: EvidenceStrong, Observed: time.Now(), Kind: "test"})

	confirmCalls := 0
	admittedSource := byte(0) // admission pending

	promoter, _ := NewPassiveDiscoveryPromoter(PassiveDiscoveryPromoterOptions{
		Registry:       reg,
		EvidenceBuffer: buf,
		ConfirmFn: func(_ context.Context, _ byte) bool {
			confirmCalls++
			return true
		},
		AdmittedSourceFn: func() byte { return admittedSource },
		TickInterval:     1 * time.Millisecond,
		Now:              time.Now,
	})

	// Tick 1: admission still pending. No probes should fire.
	promoter.processOnce(context.Background())
	if confirmCalls != 0 {
		t.Fatalf("confirm called %d times while admission pending; admission gate violated", confirmCalls)
	}
	if registryContains(reg, 0x15) {
		t.Fatal("registry mutated while admission pending; promotion side effects must defer until admission resolves")
	}

	// Tick 2: admission resolves. Probe + register should fire.
	admittedSource = 0x71
	promoter.processOnce(context.Background())
	if confirmCalls != 1 {
		t.Fatalf("confirm called %d times after admission active; want 1", confirmCalls)
	}
	if !registryContains(reg, 0x15) {
		t.Fatal("post-admission registration did not occur")
	}
}

// TestPassiveDiscoveryPromoter_BackoffOnRepeatedFailure pins the
// rate-limiting contract: a per-address attempt that fails extends
// the next-attempt deadline; back-to-back ticks must not retry until
// the backoff elapses.
func TestPassiveDiscoveryPromoter_BackoffOnRepeatedFailure(t *testing.T) {
	t.Parallel()

	reg := registry.NewDeviceRegistry(nil)
	buf, _ := NewEvidenceBuffer(128, VaillantBaselineTopologySeed)
	buf.Record(EvidenceRecord{Address: 0x15, Strength: EvidenceStrong, Observed: time.Now(), Kind: "test"})

	confirmCalls := 0
	promoter, _ := NewPassiveDiscoveryPromoter(PassiveDiscoveryPromoterOptions{
		Registry:       reg,
		EvidenceBuffer: buf,
		ConfirmFn: func(_ context.Context, _ byte) bool {
			confirmCalls++
			return false
		},
		AdmittedSourceFn: func() byte { return 0x71 },
		TickInterval:     1 * time.Millisecond,
		Now:              time.Now,
	})

	promoter.processOnce(context.Background())
	promoter.processOnce(context.Background())

	if confirmCalls != 1 {
		t.Fatalf("confirm calls = %d; want 1 (second tick must be backoff-suppressed)", confirmCalls)
	}
}

func registryContains(reg *registry.DeviceRegistry, addr byte) bool {
	found := false
	reg.Iterate(func(e registry.DeviceEntry) bool {
		if e != nil && e.Address() == addr {
			found = true
			return false
		}
		return true
	})
	return found
}
