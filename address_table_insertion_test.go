// RED tests for cruise plan address-table-registry-w19-26 M0B/gateway — references M3/M4 + M2A + M1 + M2 symbols that intentionally don't exist yet.
package ebusgateway

import (
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
)

func TestFirstObservation_PositiveACKOnly(t *testing.T) {
	table, inserter := newATRInsertionHarness(t, DefaultConfig())

	event := atrPassiveTransactionEvent(time.Now().UTC(), 0x10, 0x99, protocol.SymbolAck)
	inserter.OnPassiveClassifiedEvent(event)

	slot := requireATRSlot(t, table, 0x99)
	if slot.VerificationState != "corroborated" {
		t.Fatalf("slot[0x99].VerificationState = %q; want corroborated", slot.VerificationState)
	}
}

func TestFirstObservation_NoNACKInsertion(t *testing.T) {
	table, inserter := newATRInsertionHarness(t, DefaultConfig())

	event := atrPassiveTransactionEvent(time.Now().UTC(), 0x10, 0x99, protocol.SymbolNack)
	inserter.OnPassiveClassifiedEvent(event)

	assertATRSlotAbsent(t, table, 0x99)
}

func TestFirstObservation_SelfSourceExcluded(t *testing.T) {
	cfg := DefaultConfig()
	cfg.AdmittedSource = func() byte { return 0x7F }
	table, inserter := newATRInsertionHarness(t, cfg)

	admittedSource := cfg.AdmittedSource()
	event := atrPassiveTransactionEvent(time.Now().UTC(), admittedSource, 0x88, protocol.SymbolAck)
	inserter.OnPassiveClassifiedEvent(event)

	assertATRSlotAbsent(t, table, admittedSource)
}

// TestCompanionInsert_RequiresCorroboration asserts that a second positive
// acknowledgement from the same source inside the corroboration interval
// does not admit the canonical companion: two seconds apart, and one
// nanosecond short of the default five seconds, both leave 0xF6 absent and
// keep the first qualifying observation as the anchor.
func TestCompanionInsert_RequiresCorroboration(t *testing.T) {
	table, inserter := newATRInsertionHarness(t, DefaultConfig())
	base := atrBaseTime()

	inserter.OnPassiveClassifiedEvent(atrPassiveTransactionEvent(base, 0xF1, 0x99, protocol.SymbolAck))
	requireATRObservation(t, inserter, 0xF1, 1, base)
	assertATRSlotAbsent(t, table, 0xF6)

	inserter.OnPassiveClassifiedEvent(atrPassiveTransactionEvent(base.Add(2*time.Second), 0xF1, 0x99, protocol.SymbolAck))
	requireATRObservation(t, inserter, 0xF1, 1, base)
	assertATRSlotAbsent(t, table, 0xF6)

	inserter.OnPassiveClassifiedEvent(atrPassiveTransactionEvent(base.Add(5*time.Second-time.Nanosecond), 0xF1, 0x99, protocol.SymbolAck))
	requireATRObservation(t, inserter, 0xF1, 1, base)
	assertATRSlotAbsent(t, table, 0xF6)
}

// TestCompanionInsert_AdmitsAtOrAfterInterval asserts that a second positive
// acknowledgement at, or after, the corroboration interval inserts the
// companion exactly once and aliases the canonical pair.
func TestCompanionInsert_AdmitsAtOrAfterInterval(t *testing.T) {
	for _, tc := range []struct {
		name   string
		offset time.Duration
	}{
		{name: "exactly_at_interval", offset: 5 * time.Second},
		{name: "after_interval", offset: 9 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			table, inserter := newATRInsertionHarness(t, DefaultConfig())
			var newInsertions int
			inserter.SetEnrichmentRefreshFn(func() { newInsertions++ })
			base := atrBaseTime()

			inserter.OnPassiveClassifiedEvent(atrPassiveTransactionEvent(base, 0xF1, 0x99, protocol.SymbolAck))
			if newInsertions != 2 {
				t.Fatalf("new insertions after first observation = %d; want 2 (source and target)", newInsertions)
			}
			assertATRSlotAbsent(t, table, 0xF6)

			inserter.OnPassiveClassifiedEvent(atrPassiveTransactionEvent(base.Add(tc.offset), 0xF1, 0x99, protocol.SymbolAck))
			requireATRObservation(t, inserter, 0xF1, 2, base)
			companion := requireATRSlot(t, table, 0xF6)
			if companion.Role != "target" || companion.DiscoverySource != "passive_observed" || companion.VerificationState != "corroborated" {
				t.Fatalf("slot[0xF6] = %+v; want passive_observed corroborated target", companion)
			}
			if newInsertions != 3 {
				t.Fatalf("new insertions after corroboration = %d; want 3 (companion inserted once)", newInsertions)
			}
			requireATRAliased(t, table, 0xF1, 0xF6)

			inserter.OnPassiveClassifiedEvent(atrPassiveTransactionEvent(base.Add(tc.offset+time.Second), 0xF1, 0x99, protocol.SymbolAck))
			if newInsertions != 3 {
				t.Fatalf("new insertions after a later observation = %d; want 3 (no second companion insertion)", newInsertions)
			}
			requireATRObservation(t, inserter, 0xF1, 2, base)
			requireATRAliased(t, table, 0xF1, 0xF6)
		})
	}
}

// TestCompanionInsert_UsesConfiguredInterval asserts that the inserter reads
// PassiveCompanionCorroborationInterval and falls back to five seconds when
// the configured value is zero or negative.
func TestCompanionInsert_UsesConfiguredInterval(t *testing.T) {
	for _, tc := range []struct {
		name      string
		interval  time.Duration
		tooEarly  time.Duration
		qualifies time.Duration
	}{
		{name: "explicit_thirty_seconds", interval: 30 * time.Second, tooEarly: 29 * time.Second, qualifies: 30 * time.Second},
		{name: "zero_falls_back_to_default", interval: 0, tooEarly: 5*time.Second - time.Millisecond, qualifies: 5 * time.Second},
		{name: "negative_falls_back_to_default", interval: -time.Minute, tooEarly: 5*time.Second - time.Millisecond, qualifies: 5 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.PassiveCompanionCorroborationInterval = tc.interval
			table, inserter := newATRInsertionHarness(t, cfg)
			base := atrBaseTime()

			inserter.OnPassiveClassifiedEvent(atrPassiveTransactionEvent(base, 0xF1, 0x99, protocol.SymbolAck))
			inserter.OnPassiveClassifiedEvent(atrPassiveTransactionEvent(base.Add(tc.tooEarly), 0xF1, 0x99, protocol.SymbolAck))
			assertATRSlotAbsent(t, table, 0xF6)

			inserter.OnPassiveClassifiedEvent(atrPassiveTransactionEvent(base.Add(tc.qualifies), 0xF1, 0x99, protocol.SymbolAck))
			requireATRSlot(t, table, 0xF6)
		})
	}

	t.Run("empty_config_uses_default", func(t *testing.T) {
		table, inserter := newATRInsertionHarness(t, Config{})
		base := atrBaseTime()
		inserter.OnPassiveClassifiedEvent(atrPassiveTransactionEvent(base, 0xF1, 0x99, protocol.SymbolAck))
		inserter.OnPassiveClassifiedEvent(atrPassiveTransactionEvent(base.Add(4*time.Second), 0xF1, 0x99, protocol.SymbolAck))
		assertATRSlotAbsent(t, table, 0xF6)
		inserter.OnPassiveClassifiedEvent(atrPassiveTransactionEvent(base.Add(5*time.Second), 0xF1, 0x99, protocol.SymbolAck))
		requireATRSlot(t, table, 0xF6)
	})
}

// TestCompanionInsert_FailsClosed asserts that inputs which are not two
// independently timed positive acknowledgements from a canonical source
// never produce a companion.
func TestCompanionInsert_FailsClosed(t *testing.T) {
	base := atrBaseTime()
	ack := func(at time.Time, source byte) PassiveClassifiedEvent {
		return atrPassiveTransactionEvent(at, source, 0x99, protocol.SymbolAck)
	}
	burst := make([]PassiveClassifiedEvent, 0, 1000)
	for k := 0; k < 1000; k++ {
		burst = append(burst, ack(base.Add(time.Duration(k)*4*time.Millisecond), 0xF1))
	}

	for _, tc := range []struct {
		name        string
		admitted    byte
		events      []PassiveClassifiedEvent
		absent      []byte
		wantNoState bool
	}{
		{
			name:   "repeated_timestamp",
			events: []PassiveClassifiedEvent{ack(base, 0xF1), ack(base, 0xF1), ack(base, 0xF1)},
			absent: []byte{0xF6},
		},
		{
			name:   "reordered_timestamps",
			events: []PassiveClassifiedEvent{ack(base.Add(10*time.Second), 0xF1), ack(base, 0xF1), ack(base.Add(12*time.Second), 0xF1)},
			absent: []byte{0xF6},
		},
		{
			name:        "zero_timestamps",
			events:      []PassiveClassifiedEvent{ack(time.Time{}, 0xF1), ack(time.Time{}, 0xF1)},
			absent:      []byte{0xF6},
			wantNoState: true,
		},
		{
			name:   "zero_then_late_timestamp",
			events: []PassiveClassifiedEvent{ack(time.Time{}, 0xF1), ack(base.Add(time.Hour), 0xF1)},
			absent: []byte{0xF6},
		},
		{
			name:   "burst_inside_interval",
			events: burst,
			absent: []byte{0xF6},
		},
		{
			name: "negative_acknowledgements",
			events: []PassiveClassifiedEvent{
				atrPassiveTransactionEvent(base, 0xF1, 0x99, protocol.SymbolNack),
				atrPassiveTransactionEvent(base.Add(10*time.Second), 0xF1, 0x99, protocol.SymbolNack),
			},
			absent:      []byte{0xF1, 0xF6},
			wantNoState: true,
		},
		{
			name: "positive_then_negative_acknowledgement",
			events: []PassiveClassifiedEvent{
				ack(base, 0xF1),
				atrPassiveTransactionEvent(base.Add(10*time.Second), 0xF1, 0x99, protocol.SymbolNack),
			},
			absent: []byte{0xF6},
		},
		{
			name: "acknowledgement_position_ff",
			events: []PassiveClassifiedEvent{
				atrPassiveTransactionEvent(base, 0xF1, 0x99, 0xFF),
				atrPassiveTransactionEvent(base.Add(10*time.Second), 0xF1, 0x99, 0xFF),
			},
			absent:      []byte{0xF1, 0xF6},
			wantNoState: true,
		},
		{
			name:        "gateway_own_source",
			admitted:    0x7F,
			events:      []PassiveClassifiedEvent{ack(base, 0x7F), ack(base.Add(10*time.Second), 0x7F)},
			absent:      []byte{0x7F, 0x84},
			wantNoState: true,
		},
		{
			name:        "source_ff_on_acknowledged_path",
			events:      []PassiveClassifiedEvent{ack(base, 0xFF), ack(base.Add(10*time.Second), 0xFF)},
			absent:      []byte{0x04},
			wantNoState: true,
		},
		{
			name:   "broadcast_inside_interval",
			events: []PassiveClassifiedEvent{atrPassiveBroadcastEvent(base, 0xF1), atrPassiveBroadcastEvent(base.Add(2*time.Second), 0xF1)},
			absent: []byte{0xF1, 0xF6},
		},
		{
			name:   "broadcast_zero_then_late_timestamp",
			events: []PassiveClassifiedEvent{atrPassiveBroadcastEvent(time.Time{}, 0xF1), atrPassiveBroadcastEvent(base.Add(time.Hour), 0xF1)},
			absent: []byte{0xF1, 0xF6},
		},
		{
			name:        "companion_address_as_source",
			events:      []PassiveClassifiedEvent{ack(base, 0x15), ack(base.Add(10*time.Second), 0x15)},
			absent:      []byte{0x10},
			wantNoState: true,
		},
		{
			name:        "source_without_companion",
			events:      []PassiveClassifiedEvent{ack(base, 0x26), ack(base.Add(10*time.Second), 0x26)},
			absent:      []byte{0x21, 0x2B},
			wantNoState: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := DefaultConfig()
			if tc.admitted != 0 {
				admitted := tc.admitted
				cfg.AdmittedSource = func() byte { return admitted }
			}
			table, inserter := newATRInsertionHarness(t, cfg)
			for _, event := range tc.events {
				inserter.OnPassiveClassifiedEvent(event)
			}
			for _, address := range tc.absent {
				assertATRSlotAbsent(t, table, address)
			}
			for source, observation := range inserter.observationsByAddr {
				if observation.PositiveACKCount >= 2 {
					t.Fatalf("observationsByAddr[0x%02X] = %+v; want fewer than two qualifying observations", source, observation)
				}
			}
			if tc.wantNoState && len(inserter.observationsByAddr) != 0 {
				t.Fatalf("observationsByAddr = %+v; want no corroboration state", inserter.observationsByAddr)
			}
		})
	}
}

// TestCompanionInsert_ObservationStateIsPerSourceAndBounded asserts that
// corroboration is keyed by the exact source address (an observation of one
// source never corroborates another) and that the state holds at most one
// entry per canonical source, however much traffic arrives.
func TestCompanionInsert_ObservationStateIsPerSourceAndBounded(t *testing.T) {
	t.Run("per_source", func(t *testing.T) {
		table, inserter := newATRInsertionHarness(t, DefaultConfig())
		base := atrBaseTime()
		inserter.OnPassiveClassifiedEvent(atrPassiveTransactionEvent(base, 0xF1, 0x99, protocol.SymbolAck))
		inserter.OnPassiveClassifiedEvent(atrPassiveTransactionEvent(base.Add(10*time.Second), 0x10, 0x99, protocol.SymbolAck))
		assertATRSlotAbsent(t, table, 0xF6)
		assertATRSlotAbsent(t, table, 0x15)
		requireATRObservation(t, inserter, 0xF1, 1, base)
		requireATRObservation(t, inserter, 0x10, 1, base.Add(10*time.Second))
	})

	t.Run("bounded", func(t *testing.T) {
		_, inserter := newATRInsertionHarness(t, DefaultConfig())
		admitted := inserter.admittedSource()
		base := atrBaseTime()
		for round := 0; round < 3; round++ {
			for source := 0; source < 256; source++ {
				at := base.Add(time.Duration(round) * time.Millisecond)
				inserter.OnPassiveClassifiedEvent(atrPassiveTransactionEvent(at, byte(source), 0x99, protocol.SymbolAck))
				inserter.OnPassiveClassifiedEvent(atrPassiveBroadcastEvent(at, byte(source)))
			}
		}
		canonicalSources := 0
		for source := 0; source < 256; source++ {
			if protocol.IsCanonicalSource(byte(source)) {
				canonicalSources++
			}
		}
		if len(inserter.observationsByAddr) > canonicalSources {
			t.Fatalf("len(observationsByAddr) = %d; want at most %d (one per canonical source)", len(inserter.observationsByAddr), canonicalSources)
		}
		for source := range inserter.observationsByAddr {
			if _, ok := protocol.CompanionOfSource(source); !ok || source == admitted {
				t.Fatalf("observationsByAddr holds state for 0x%02X; want only canonical sources other than the gateway's own", source)
			}
		}
	})
}

// TestCompanionInsert_BroadcastPathUsesSameInterval asserts that the
// broadcast path applies the same time-qualified corroboration to the
// broadcaster and its canonical companion.
func TestCompanionInsert_BroadcastPathUsesSameInterval(t *testing.T) {
	table, inserter := newATRInsertionHarness(t, DefaultConfig())
	base := atrBaseTime()

	inserter.OnPassiveClassifiedEvent(atrPassiveBroadcastEvent(base, 0xFF))
	inserter.OnPassiveClassifiedEvent(atrPassiveBroadcastEvent(base.Add(2*time.Second), 0xFF))
	assertATRSlotAbsent(t, table, 0xFF)
	assertATRSlotAbsent(t, table, 0x04)

	inserter.OnPassiveClassifiedEvent(atrPassiveBroadcastEvent(base.Add(5*time.Second), 0xFF))
	requireATRSlot(t, table, 0xFF)
	requireATRSlot(t, table, 0x04)
	requireATRAliased(t, table, 0xFF, 0x04)
}

// TestCompanionInsert_CurrentIdentityWitnessAdmitsOnFirstACK asserts that
// one positive acknowledgement plus a current qualified identity witness for
// the exact source admits the companion through the registry, with the
// alias, runtime-state and identity-probe bookkeeping, while the
// two-acknowledgement state still counts a single observation.
func TestCompanionInsert_CurrentIdentityWitnessAdmitsOnFirstACK(t *testing.T) {
	table, inserter := newATRInsertionHarness(t, DefaultConfig())
	var observed, probed []byte
	refreshes := 0
	inserter.SetRuntimeStateObserver(func(addr byte, _ time.Time, _ string) { observed = append(observed, addr) })
	inserter.SetEnrichmentIdentityProbeFn(func(addr byte) { probed = append(probed, addr) })
	inserter.SetEnrichmentRefreshFn(func() { refreshes++ })
	table.reg.Register(atrQualifiedIdentity(0x10, "SN-SYNTH-WITNESS"))
	base := atrBaseTime()

	inserter.OnPassiveClassifiedEvent(atrPassiveTransactionEvent(base, 0x10, 0x99, protocol.SymbolAck))

	// Source 0x10 and target 0x99 are inserted by the first-observation path
	// (or already present); the witness-admitted companion must add one more
	// discovery refresh on top of those.
	if refreshes < 2 {
		t.Fatalf("discovery refresh count = %d; want a refresh for the witness-admitted companion as well", refreshes)
	}
	companion := requireATRSlot(t, table, 0x15)
	if companion.Role != "target" || companion.DiscoverySource != "passive_observed" || companion.VerificationState != "corroborated" {
		t.Fatalf("slot[0x15] = %+v; want passive_observed corroborated target", companion)
	}
	snapshot, ok := table.reg.LookupSlotSnapshot(0x15)
	if !ok || !snapshot.LastObservedAt.Equal(base) || snapshot.Role != registry.SlotRoleSlave {
		t.Fatalf("registry slot[0x15] = %+v, present=%v; want target observed at %v", snapshot, ok, base)
	}
	requireATRAliased(t, table, 0x10, 0x15)
	requireATRObservation(t, inserter, 0x10, 1, base)
	if !containsATRAddress(observed, 0x15) {
		t.Fatalf("runtime-state observer addresses = % X; want 0x15", observed)
	}
	if !containsATRAddress(probed, 0x15) {
		t.Fatalf("identity probe addresses = % X; want 0x15", probed)
	}
}

// TestCompanionInsert_IdentityWitnessBecomesCurrentBeforeCorroboration
// asserts that a witness recorded after the first acknowledgement is honoured
// on a later positive acknowledgement inside the interval.
func TestCompanionInsert_IdentityWitnessBecomesCurrentBeforeCorroboration(t *testing.T) {
	table, inserter := newATRInsertionHarness(t, DefaultConfig())
	base := atrBaseTime()

	inserter.OnPassiveClassifiedEvent(atrPassiveTransactionEvent(base, 0x10, 0x99, protocol.SymbolAck))
	assertATRSlotAbsent(t, table, 0x15)

	table.reg.Register(atrQualifiedIdentity(0x10, "SN-SYNTH-LATE"))
	inserter.OnPassiveClassifiedEvent(atrPassiveTransactionEvent(base.Add(time.Second), 0x10, 0x99, protocol.SymbolAck))
	requireATRSlot(t, table, 0x15)
	requireATRObservation(t, inserter, 0x10, 1, base)
}

// TestCompanionInsert_IdentityWitnessRejected asserts that witnesses which
// are missing, partial, retired, replaced, asserted by passive or static
// evidence, alias-only or held for another address do not admit the
// companion on one acknowledgement, and leave the two-acknowledgement path
// intact.
func TestCompanionInsert_IdentityWitnessRejected(t *testing.T) {
	base := atrBaseTime()
	for _, tc := range []struct {
		name  string
		setup func(t *testing.T, reg *registry.DeviceRegistry)
	}{
		{name: "missing", setup: func(*testing.T, *registry.DeviceRegistry) {}},
		{name: "partial_identity", setup: func(_ *testing.T, reg *registry.DeviceRegistry) {
			reg.Register(registry.DeviceInfo{Address: 0x10, Manufacturer: "ACME", DeviceID: "REGULATOR"})
		}},
		{name: "retired_after_identity_change", setup: func(_ *testing.T, reg *registry.DeviceRegistry) {
			reg.Register(atrQualifiedIdentity(0x10, "SN-SYNTH-RETIRED"))
			reg.Register(registry.DeviceInfo{Address: 0x10, Manufacturer: "OTHER"})
		}},
		{name: "stale_after_replacement", setup: func(_ *testing.T, reg *registry.DeviceRegistry) {
			reg.Register(atrQualifiedIdentity(0x10, "SN-SYNTH-OLD"))
			reg.RegisterStaticSeed(atrQualifiedIdentity(0x10, "SN-SYNTH-NEW"), registry.SlotRoleMaster, base.Add(-time.Minute))
		}},
		{name: "passive_asserted_identity", setup: func(_ *testing.T, reg *registry.DeviceRegistry) {
			reg.RegisterPassiveObserved(atrQualifiedIdentity(0x10, "SN-SYNTH-PASSIVE"), registry.SlotRoleMaster, base.Add(-time.Minute))
		}},
		{name: "static_identity", setup: func(_ *testing.T, reg *registry.DeviceRegistry) {
			reg.RegisterStaticSeed(atrQualifiedIdentity(0x10, "SN-SYNTH-STATIC"), registry.SlotRoleMaster, base.Add(-time.Minute))
		}},
		{name: "alias_only", setup: func(t *testing.T, reg *registry.DeviceRegistry) {
			reg.Register(atrQualifiedIdentity(0x00, "SN-SYNTH-ALIAS"))
			if err := reg.AliasAddresses(0x00, 0x10); err != nil {
				t.Fatalf("AliasAddresses: %v", err)
			}
		}},
		{name: "wrong_address", setup: func(_ *testing.T, reg *registry.DeviceRegistry) {
			reg.Register(atrQualifiedIdentity(0x30, "SN-SYNTH-OTHER"))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			table, inserter := newATRInsertionHarness(t, DefaultConfig())
			tc.setup(t, table.reg)

			inserter.OnPassiveClassifiedEvent(atrPassiveTransactionEvent(base, 0x10, 0x99, protocol.SymbolAck))
			assertATRSlotAbsent(t, table, 0x15)
			assertATRSlotAbsent(t, table, 0x35)

			inserter.OnPassiveClassifiedEvent(atrPassiveTransactionEvent(base.Add(2*time.Second), 0x10, 0x99, protocol.SymbolAck))
			assertATRSlotAbsent(t, table, 0x15)

			inserter.OnPassiveClassifiedEvent(atrPassiveTransactionEvent(base.Add(5*time.Second), 0x10, 0x99, protocol.SymbolAck))
			requireATRSlot(t, table, 0x15)
			requireATRAliased(t, table, 0x10, 0x15)
			assertATRSlotAbsent(t, table, 0x35)
		})
	}
}

// TestCompanionInsert_IdentityWitnessHonoursFailClosedInputs asserts that a
// current witness does not admit the companion from a negative
// acknowledgement, a zero timestamp, the gateway's own source or a broadcast.
func TestCompanionInsert_IdentityWitnessHonoursFailClosedInputs(t *testing.T) {
	base := atrBaseTime()
	for _, tc := range []struct {
		name     string
		admitted byte
		event    PassiveClassifiedEvent
	}{
		{name: "negative_acknowledgement", event: atrPassiveTransactionEvent(base, 0x10, 0x99, protocol.SymbolNack)},
		{name: "zero_timestamp", event: atrPassiveTransactionEvent(time.Time{}, 0x10, 0x99, protocol.SymbolAck)},
		{name: "gateway_own_source", admitted: 0x10, event: atrPassiveTransactionEvent(base, 0x10, 0x99, protocol.SymbolAck)},
		{name: "broadcast", event: atrPassiveBroadcastEvent(base, 0x10)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := DefaultConfig()
			if tc.admitted != 0 {
				admitted := tc.admitted
				cfg.AdmittedSource = func() byte { return admitted }
			}
			table, inserter := newATRInsertionHarness(t, cfg)
			table.reg.Register(atrQualifiedIdentity(0x10, "SN-SYNTH-GUARD"))

			inserter.OnPassiveClassifiedEvent(tc.event)
			assertATRSlotAbsent(t, table, 0x15)
		})
	}
}

// TestFirstObservation_SourceInserted asserts that on a positive ACK the
// initiator address (request src) is inserted as an "initiator" slot — not
// just the target. Per architecture/atr/03-ack-nack-insertion-rules.md
// "Address Eligibility":
//
//	"request `src` MAY create a slot if it is not the gateway's own admitted source"
//
// PR #564 implemented dst insertion + companion-after-corroboration but
// missed src insertion, so initiator addresses like NETX3 0xF1 never
// landed in the registry passively even though their traffic was observed.
func TestFirstObservation_SourceInserted(t *testing.T) {
	table, inserter := newATRInsertionHarness(t, DefaultConfig())

	event := atrPassiveTransactionEvent(time.Now().UTC(), 0xF1, 0x99, protocol.SymbolAck)
	inserter.OnPassiveClassifiedEvent(event)

	srcSlot := requireATRSlot(t, table, 0xF1)
	if srcSlot.Role != "initiator" {
		t.Fatalf("slot[0xF1].Role = %q; want initiator", srcSlot.Role)
	}
	if srcSlot.DiscoverySource != "passive_observed" {
		t.Fatalf("slot[0xF1].DiscoverySource = %q; want passive_observed", srcSlot.DiscoverySource)
	}
	if srcSlot.VerificationState != "corroborated" {
		t.Fatalf("slot[0xF1].VerificationState = %q; want corroborated", srcSlot.VerificationState)
	}

	dstSlot := requireATRSlot(t, table, 0x99)
	if dstSlot.Role != "target" {
		t.Fatalf("slot[0x99].Role = %q; want target", dstSlot.Role)
	}
}

// TestCanonicalAliasing_SourceCompanionPair_AfterSecondCorroboration asserts
// that when both a canonical source AND its canonical companion land in the
// registry, the inserter aliases them as a single device — so MCP/GraphQL
// queries return one device with two addresses, not two unrelated entries.
//
// Per architecture/ebus_standard/12-source-address-table.md, 0x10 ↔ 0x15 is
// the canonical Heating regulator pair. After the inserter sees:
//  1. positive ACK from 0x10 → some-target  (inserts 0x10 as initiator)
//  2. positive ACK from some-source → 0x15  (inserts 0x15 as target — but
//     the companion-pair gate requires 2 observations of source for
//     companion(source) insertion; here 0x15 is inserted as direct dst)
//
// At the moment both 0x10 and 0x15 are present in the table, the inserter
// MUST call AliasAddresses(0x10, 0x15) so registry.Lookup(0x10) and
// registry.Lookup(0x15) return the same DeviceEntry.
func TestCanonicalAliasing_SourceCompanionPair_AfterBothObserved(t *testing.T) {
	table, inserter := newATRInsertionHarness(t, DefaultConfig())
	base := time.Now().UTC()

	// Step 1: observe 0x10 → 0x33-style-target (positive ACK) → inserts
	// 0x10 as initiator + the target as target.
	first := atrPassiveTransactionEvent(base, 0x10, 0x99, protocol.SymbolAck)
	inserter.OnPassiveClassifiedEvent(first)
	requireATRSlot(t, table, 0x10)

	// Step 2: observe a different source emitting to 0x15 (canonical
	// companion of 0x10) with positive ACK → inserts 0x15 as target.
	// Now both 0x10 and 0x15 are present — the inserter MUST alias them.
	second := atrPassiveTransactionEvent(base.Add(time.Second), 0x71, 0x15, protocol.SymbolAck)
	inserter.OnPassiveClassifiedEvent(second)
	requireATRSlot(t, table, 0x15)

	// Step 3: assert canonical aliasing — both addresses resolve to the
	// same DeviceEntry in the wrapped registry, and the entry exposes
	// both addresses via Addresses().
	entryA, okA := table.reg.Lookup(0x10)
	entryB, okB := table.reg.Lookup(0x15)
	if !okA || !okB {
		t.Fatalf("registry.Lookup ok values: 0x10=%v 0x15=%v; want both true", okA, okB)
	}
	if entryA.PrimaryDisplayAddress() != entryB.PrimaryDisplayAddress() {
		t.Fatalf("canonical pair 0x10 ↔ 0x15 not aliased: 0x10 entry primary=0x%02X, 0x15 entry primary=0x%02X; want same primary", entryA.PrimaryDisplayAddress(), entryB.PrimaryDisplayAddress())
	}
	addrs := entryA.Addresses()
	hasMaster, hasCompanion := false, false
	for _, a := range addrs {
		if a == 0x10 {
			hasMaster = true
		}
		if a == 0x15 {
			hasCompanion = true
		}
	}
	if !hasMaster || !hasCompanion {
		t.Fatalf("DeviceEntry.Addresses() = %v; want both 0x10 and 0x15", addrs)
	}
}

// TestCanonicalAliasing_FFAndZeroFour_BothDirections asserts the wrap-pair
// aliasing for 0xFF ↔ 0x04. NETX3 publishes a target at 0x04 whose canonical
// source is 0xFF (per architecture/ebus_standard/12-source-address-table.md
// row 25, with byte-modulo arithmetic 0xFF + 0x05 = 0x04). Tests both
// orderings: 0xFF first then 0x04 (forward) and 0x04 first then 0xFF
// (reverse).
func TestCanonicalAliasing_FFAndZeroFour_BothDirections(t *testing.T) {
	t.Parallel()

	t.Run("FF_then_04", func(t *testing.T) {
		table, inserter := newATRInsertionHarness(t, DefaultConfig())
		base := time.Now().UTC()

		// 0xFF as src (frame-start, not NACK byte) → inserts 0xFF.
		ff := atrPassiveTransactionEvent(base, 0xFF, 0x99, protocol.SymbolAck)
		inserter.OnPassiveClassifiedEvent(ff)
		requireATRSlot(t, table, 0xFF)

		// Some-source → 0x04 with positive ACK → inserts 0x04.
		zero4 := atrPassiveTransactionEvent(base.Add(time.Second), 0x10, 0x04, protocol.SymbolAck)
		inserter.OnPassiveClassifiedEvent(zero4)
		requireATRSlot(t, table, 0x04)

		entryFF, okFF := table.reg.Lookup(0xFF)
		entry04, ok04 := table.reg.Lookup(0x04)
		if !okFF || !ok04 {
			t.Fatalf("registry.Lookup ok: 0xFF=%v 0x04=%v", okFF, ok04)
		}
		if entryFF.PrimaryDisplayAddress() != entry04.PrimaryDisplayAddress() {
			t.Fatalf("0xFF ↔ 0x04 wrap-pair not aliased: 0xFF primary=0x%02X, 0x04 primary=0x%02X", entryFF.PrimaryDisplayAddress(), entry04.PrimaryDisplayAddress())
		}
	})

	t.Run("04_then_FF", func(t *testing.T) {
		table, inserter := newATRInsertionHarness(t, DefaultConfig())
		base := time.Now().UTC()

		// Reverse ordering: 0x04 inserted first via dst observation.
		zero4 := atrPassiveTransactionEvent(base, 0x10, 0x04, protocol.SymbolAck)
		inserter.OnPassiveClassifiedEvent(zero4)
		requireATRSlot(t, table, 0x04)

		ff := atrPassiveTransactionEvent(base.Add(time.Second), 0xFF, 0x99, protocol.SymbolAck)
		inserter.OnPassiveClassifiedEvent(ff)
		requireATRSlot(t, table, 0xFF)

		entryFF, _ := table.reg.Lookup(0xFF)
		entry04, _ := table.reg.Lookup(0x04)
		if entryFF.PrimaryDisplayAddress() != entry04.PrimaryDisplayAddress() {
			t.Fatalf("0xFF ↔ 0x04 wrap-pair (reverse order) not aliased: 0xFF primary=0x%02X, 0x04 primary=0x%02X", entryFF.PrimaryDisplayAddress(), entry04.PrimaryDisplayAddress())
		}
	})
}

// TestCanonicalAliasing_NonCanonicalAddresses_NotAliased asserts that
// non-canonical companions (e.g. real but non-table addresses 0x26 / 0xEC
// for VR_71 / SOL00) are NOT aliased even if they appear in the registry
// alongside other addresses. Aliasing only fires for the 25 canonical
// pairs from sourceAddressTableV1.
func TestCanonicalAliasing_NonCanonicalAddresses_NotAliased(t *testing.T) {
	table, inserter := newATRInsertionHarness(t, DefaultConfig())
	base := time.Now().UTC()

	// 0x26 (VR_71 target-only address) is observed as target. Its hypothetical
	// "companion" via math 0x21 is not in the canonical table, and the
	// canonical-table-backed lookup correctly rejects it.
	first := atrPassiveTransactionEvent(base, 0x10, 0x26, protocol.SymbolAck)
	inserter.OnPassiveClassifiedEvent(first)
	requireATRSlot(t, table, 0x26)

	// 0xEC (SOL00 target-only address) is observed as target. Same situation.
	second := atrPassiveTransactionEvent(base.Add(time.Second), 0x10, 0xEC, protocol.SymbolAck)
	inserter.OnPassiveClassifiedEvent(second)
	requireATRSlot(t, table, 0xEC)

	// 0x26 and 0xEC must remain separate device entries.
	entryA, okA := table.reg.Lookup(0x26)
	entryB, okB := table.reg.Lookup(0xEC)
	if !okA || !okB {
		t.Fatalf("registry.Lookup ok: 0x26=%v 0xEC=%v; want both true", okA, okB)
	}
	if entryA.PrimaryDisplayAddress() == entryB.PrimaryDisplayAddress() {
		t.Fatalf("non-canonical addresses 0x26 and 0xEC unexpectedly aliased to primary 0x%02X", entryA.PrimaryDisplayAddress())
	}
}

// TestAddressSlot_TierAndFreeUse_PopulatedFromCanonicalTable asserts that
// when a canonical source/companion address lands in the AddressTable, its
// PriorityTier and FreeUse fields are populated from the docs-owned eBUS
// standard table (sourceAddressTableV1) — operators can then see at a
// glance whether an observed address is preallocated (e.g. 0xF1 p1
// "Heating regulator") or free-use (e.g. 0x7F p4 "free-use Burner controller 8
// recommendation").
func TestAddressSlot_TierAndFreeUse_PopulatedFromCanonicalTable(t *testing.T) {
	table, inserter := newATRInsertionHarness(t, DefaultConfig())
	base := time.Now().UTC()

	// 0xF1 (Heating regulator p1, NOT free-use)
	inserter.OnPassiveClassifiedEvent(atrPassiveTransactionEvent(base, 0xF1, 0x99, protocol.SymbolAck))
	slot, _ := table.Lookup(0xF1)
	if slot == nil {
		t.Fatalf("Lookup(0xF1) = nil after insertion")
	}
	if string(slot.PriorityTier) != "p1" {
		t.Fatalf("slot[0xF1].PriorityTier = %q; want p1", slot.PriorityTier)
	}
	if slot.FreeUse {
		t.Fatalf("slot[0xF1].FreeUse = true; want false (Heating regulator preallocated)")
	}

	// 0x7F (free-use Burner controller 8 recommendation, p4)
	inserter.OnPassiveClassifiedEvent(atrPassiveTransactionEvent(base.Add(time.Second), 0x7F, 0x88, protocol.SymbolAck))
	slot, _ = table.Lookup(0x7F)
	if slot == nil {
		t.Fatalf("Lookup(0x7F) = nil after insertion")
	}
	if string(slot.PriorityTier) != "p4" {
		t.Fatalf("slot[0x7F].PriorityTier = %q; want p4", slot.PriorityTier)
	}
	if !slot.FreeUse {
		t.Fatalf("slot[0x7F].FreeUse = false; want true (free-use Burner controller 8 recommendation)")
	}
}

// TestAddressSlot_CompanionInheritsTierFromSourceRow asserts that when a
// canonical companion (e.g. 0x15, 0xF6, 0x04, 0x84) is inserted as target
// — without its source ever being observed — the slot's PriorityTier and
// FreeUse fields are populated from the row's source side. Both halves of
// a canonical pair share the same tier/free-use because the eBUS standard
// table row defines a single class for the pair.
func TestAddressSlot_CompanionInheritsTierFromSourceRow(t *testing.T) {
	cases := []struct {
		addr     byte
		wantTier string
		wantFree bool
	}{
		{0x15, "p0", false}, // companion of 0x10 (Heating regulator p0)
		{0xF6, "p1", false}, // companion of 0xF1 (Heating regulator p1)
		{0x04, "p4", false}, // companion of 0xFF (PC p4) — wrap pair
		{0x84, "p4", true},  // companion of 0x7F (free-use Burner controller 8 p4)
		{0x38, "p2", false}, // companion of 0x33 (Burner controller 3 p2)
	}
	for _, tc := range cases {
		tc := tc
		t.Run("companion_0x"+itoaHex(tc.addr), func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.AdmittedSource = func() byte { return 0x71 }
			table, inserter := newATRInsertionHarness(t, cfg)
			// Insert via 0x10 → companion as target with positive ACK.
			inserter.OnPassiveClassifiedEvent(atrPassiveTransactionEvent(time.Now().UTC(), 0x10, tc.addr, protocol.SymbolAck))
			slot, _ := table.Lookup(tc.addr)
			if slot == nil {
				t.Fatalf("Lookup(0x%02X) = nil", tc.addr)
			}
			if string(slot.PriorityTier) != tc.wantTier {
				t.Fatalf("slot[0x%02X].PriorityTier = %q; want %q", tc.addr, slot.PriorityTier, tc.wantTier)
			}
			if slot.FreeUse != tc.wantFree {
				t.Fatalf("slot[0x%02X].FreeUse = %v; want %v", tc.addr, slot.FreeUse, tc.wantFree)
			}
		})
	}
}

func itoaHex(b byte) string {
	const hex = "0123456789ABCDEF"
	return string([]byte{hex[b>>4], hex[b&0x0F]})
}

// TestAddressSlot_NonCanonical_TierEmpty asserts that addresses NOT in the
// canonical table (e.g. 0x26 VR_71 target-only) get empty PriorityTier and
// FreeUse=false, since they have no canonical row.
func TestAddressSlot_NonCanonical_TierEmpty(t *testing.T) {
	cfg := DefaultConfig()
	cfg.AdmittedSource = func() byte { return 0x7F }
	table, inserter := newATRInsertionHarness(t, cfg)

	inserter.OnPassiveClassifiedEvent(atrPassiveTransactionEvent(time.Now().UTC(), 0x10, 0x26, protocol.SymbolAck))
	slot, _ := table.Lookup(0x26)
	if slot == nil {
		t.Fatalf("Lookup(0x26) = nil after insertion")
	}
	if string(slot.PriorityTier) != "" {
		t.Fatalf("slot[0x26].PriorityTier = %q; want empty (non-canonical)", slot.PriorityTier)
	}
	if slot.FreeUse {
		t.Fatalf("slot[0x26].FreeUse = true; want false (non-canonical)")
	}
}

// TestCanonicalAliasing_BothPreRegistered_AliasOnObservation asserts that
// when both halves of a canonical pair were registered BEFORE the inserter
// runs (e.g. via startup active scan or static seed), a subsequent passive
// observation of either half still triggers the alias merge. Without this,
// devices that the active scan picks up as separate entries (the common
// case for 0x10/0x15 BASV2 controller pair) would never become aliased.
func TestCanonicalAliasing_BothPreRegistered_AliasOnObservation(t *testing.T) {
	table, inserter := newATRInsertionHarness(t, DefaultConfig())

	// Pre-register both 0x10 and 0x15 as if startup active scan had
	// discovered them as separate entries. Force them apart by giving
	// each a distinct identity so registry.Register doesn't auto-merge.
	table.reg.Register(registry.DeviceInfo{Address: 0x10, Manufacturer: "Vaillant", DeviceID: "VRC700", SerialNumber: "SN-VRC"})
	table.reg.Register(registry.DeviceInfo{Address: 0x15, Manufacturer: "Vaillant", DeviceID: "BASV2", SerialNumber: "SN-BASV2"})

	entry10Pre, _ := table.reg.Lookup(0x10)
	entry15Pre, _ := table.reg.Lookup(0x15)
	if entry10Pre.PrimaryDisplayAddress() == entry15Pre.PrimaryDisplayAddress() {
		t.Fatalf("test setup: 0x10 and 0x15 unexpectedly share primary 0x%02X (Register auto-merged)", entry10Pre.PrimaryDisplayAddress())
	}

	// Observe passive traffic with 0x10 as src. Inserter sees 0x10 in
	// table.Lookup → existing → MUST still attempt alias of 0x10's
	// canonical companion 0x15.
	event := atrPassiveTransactionEvent(time.Now().UTC(), 0x10, 0x99, protocol.SymbolAck)
	inserter.OnPassiveClassifiedEvent(event)

	entry10, _ := table.reg.Lookup(0x10)
	entry15, _ := table.reg.Lookup(0x15)
	if entry10.PrimaryDisplayAddress() != entry15.PrimaryDisplayAddress() {
		t.Fatalf("pre-registered canonical pair 0x10 ↔ 0x15 not aliased after passive observation: 0x10 primary=0x%02X, 0x15 primary=0x%02X", entry10.PrimaryDisplayAddress(), entry15.PrimaryDisplayAddress())
	}
}

func TestFFDisambiguation_FrameStartVsACKPosition(t *testing.T) {
	table, inserter := newATRInsertionHarness(t, DefaultConfig())
	base := time.Now().UTC()

	frameStartFF := atrPassiveTransactionEvent(base, 0xFF, 0x04, protocol.SymbolAck)
	inserter.OnPassiveClassifiedEvent(frameStartFF)
	before := requireATRSlot(t, table, 0xFF)

	nackPositionFF := atrPassiveTransactionEvent(base.Add(time.Second), 0x10, 0x99, protocol.SymbolNack)
	inserter.OnPassiveClassifiedEvent(nackPositionFF)
	after := requireATRSlot(t, table, 0xFF)

	if after.VerificationState != before.VerificationState {
		t.Fatalf("slot[0xFF].VerificationState changed after NACK-position 0xFF: got %q; want %q", after.VerificationState, before.VerificationState)
	}
}

func newATRInsertionHarness(t *testing.T, cfg Config) (*AddressTable, *AddressTableInserter) {
	t.Helper()

	table := NewAddressTable()
	inserter := NewAddressTableInserter(table, cfg)
	return table, inserter
}

func atrPassiveTransactionEvent(observedAt time.Time, source, target, ack byte) PassiveClassifiedEvent {
	event := observabilityPassiveTransactionEvent(observedAt, source, target, 0xB5, 0x24)
	event.ACKCorrelation = PassiveACKCorrelation{
		Byte:            ack,
		Position:        PassiveACKPositionRequestACK,
		CompleteRequest: true,
		Correlator:      PassiveACKCorrelatorM2A,
	}
	return event
}

func atrBaseTime() time.Time {
	return time.Date(2026, time.January, 5, 12, 0, 0, 0, time.UTC)
}

func atrPassiveBroadcastEvent(observedAt time.Time, source byte) PassiveClassifiedEvent {
	return PassiveClassifiedEvent{
		Kind:      PassiveClassifiedEventBroadcastFrame,
		FrameType: protocol.FrameTypeBroadcast,
		Request: protocol.Frame{
			Source:    source,
			Target:    protocol.AddressBroadcast,
			Primary:   0xB5,
			Secondary: 0x16,
			Data:      []byte{0x01},
		},
		HasRequest: true,
		ObservedAt: observedAt,
	}
}

func atrQualifiedIdentity(address byte, serial string) registry.DeviceInfo {
	return registry.DeviceInfo{
		Address:      address,
		Manufacturer: "ACME",
		DeviceID:     "REGULATOR",
		SerialNumber: serial,
	}
}

func requireATRObservation(t *testing.T, inserter *AddressTableInserter, source byte, wantCount int, wantFirst time.Time) {
	t.Helper()

	observation, ok := inserter.observationsByAddr[source]
	if !ok {
		t.Fatalf("observationsByAddr[0x%02X] missing; want count=%d first=%v", source, wantCount, wantFirst)
	}
	if observation.PositiveACKCount != wantCount || !observation.FirstPositiveAt.Equal(wantFirst) {
		t.Fatalf("observationsByAddr[0x%02X] = %+v; want count=%d first=%v", source, observation, wantCount, wantFirst)
	}
}

func requireATRAliased(t *testing.T, table *AddressTable, a, b byte) {
	t.Helper()

	entryA, okA := table.reg.Lookup(a)
	entryB, okB := table.reg.Lookup(b)
	if !okA || !okB {
		t.Fatalf("registry.Lookup ok: 0x%02X=%v 0x%02X=%v; want both true", a, okA, b, okB)
	}
	if entryA.PrimaryDisplayAddress() != entryB.PrimaryDisplayAddress() {
		t.Fatalf("canonical pair 0x%02X / 0x%02X not aliased: primaries 0x%02X and 0x%02X", a, b, entryA.PrimaryDisplayAddress(), entryB.PrimaryDisplayAddress())
	}
}

func containsATRAddress(addresses []byte, want byte) bool {
	for _, address := range addresses {
		if address == want {
			return true
		}
	}
	return false
}

func requireATRSlot(t *testing.T, table *AddressTable, address byte) *AddressSlot {
	t.Helper()

	slot, ok := table.Lookup(address)
	if !ok {
		t.Fatalf("AddressTable.Lookup(0x%02X) ok = false; want true", address)
	}
	if slot == nil {
		t.Fatalf("AddressTable.Lookup(0x%02X) slot = nil; want AddressSlot", address)
	}
	return slot
}

func assertATRSlotAbsent(t *testing.T, table *AddressTable, address byte) {
	t.Helper()

	slot, ok := table.Lookup(address)
	if ok || slot != nil {
		t.Fatalf("AddressTable.Lookup(0x%02X) = (%+v, %v); want (nil, false)", address, slot, ok)
	}
}
