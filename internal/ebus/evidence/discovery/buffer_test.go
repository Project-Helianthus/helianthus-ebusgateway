package discovery

import (
	"fmt"
	"reflect"
	"testing"
	"time"
)

var testBaselineSeed = []byte{0x08, 0x15, 0x26, 0x04, 0xF6, 0xEC}

func TestDiscoveryEvidence_PromotionOnTwoObservations(t *testing.T) {
	b, err := NewEvidenceBuffer(128, testBaselineSeed)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	promoted1 := b.Record(EvidenceRecord{Address: 0x20, Strength: EvidenceWeak, Observed: now})
	if promoted1 {
		t.Errorf("promotion on first observation — expected none")
	}
	promoted2 := b.Record(EvidenceRecord{Address: 0x20, Strength: EvidenceWeak, Observed: now.Add(time.Second)})
	if !promoted2 {
		t.Errorf("expected promotion on second observation")
	}
}

func TestDiscoveryEvidence_PromotionOnSingleStrongEvidence(t *testing.T) {
	b, _ := NewEvidenceBuffer(128, testBaselineSeed)
	promoted := b.Record(EvidenceRecord{Address: 0x20, Strength: EvidenceStrong, Observed: time.Now()})
	if !promoted {
		t.Errorf("expected immediate promotion on single strong evidence")
	}
}

func TestDiscoveryEvidence_FloodWithDefaultSeed_BaselineSurvives(t *testing.T) {
	b, _ := NewEvidenceBuffer(128, testBaselineSeed)
	now := time.Now()
	for _, addr := range testBaselineSeed {
		b.Record(EvidenceRecord{Address: addr, Strength: EvidenceWeak, Observed: now})
	}
	for i := 0; i < 1000; i++ {
		addr := byte(0x20 + (i % 0xD0))
		b.Record(EvidenceRecord{Address: addr, Strength: EvidenceWeak, Observed: now.Add(time.Duration(i) * time.Millisecond)})
	}
	if b.Len() > 128 {
		t.Errorf("buffer overflow: len=%d > 128", b.Len())
	}
	for _, baseAddr := range testBaselineSeed {
		promoted := b.PromotedAddresses()
		found := false
		for _, p := range promoted {
			if p == baseAddr {
				found = true
				break
			}
		}
		if !found {
			b.mu.Lock()
			_, ok := b.entries[baseAddr]
			b.mu.Unlock()
			if !ok {
				t.Errorf("baseline address 0x%02X was evicted — AD05 violation", baseAddr)
			}
		}
	}
}

func TestDiscoveryEvidence_FloodWithNonDefaultSeed_BaselineSurvives(t *testing.T) {
	customSeed := []byte{0x03, 0x10, 0x50}
	b, err := NewEvidenceBuffer(128, customSeed)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	for _, addr := range customSeed {
		b.Record(EvidenceRecord{Address: addr, Strength: EvidenceWeak, Observed: now})
	}
	for i := 0; i < 1000; i++ {
		addr := byte(0x60 + (i % 0x90))
		b.Record(EvidenceRecord{Address: addr, Strength: EvidenceWeak, Observed: now.Add(time.Duration(i) * time.Millisecond)})
	}
	if b.Len() > 128 {
		t.Errorf("buffer overflow: len=%d > 128", b.Len())
	}
	for _, baseAddr := range customSeed {
		b.mu.Lock()
		_, ok := b.entries[baseAddr]
		b.mu.Unlock()
		if !ok {
			t.Errorf("custom-seed baseline 0x%02X was evicted — AD05 violation", baseAddr)
		}
	}
}

func TestDiscoveryEvidence_ValidateBaselineTopologySeed_RejectsOutOfRange(t *testing.T) {
	cases := [][]byte{
		{0x00, 0x08},
		{0x08, 0xFE},
		{0xAA, 0x08},
		{0x08, 0xFF},
	}
	for i, seed := range cases {
		if err := ValidateBaselineTopologySeed(seed); err == nil {
			t.Errorf("case %d: expected error for seed %v", i, seed)
		}
	}
}

func TestDiscoveryEvidence_NewRejectsOutOfRangeMaxEntries(t *testing.T) {
	if _, err := NewEvidenceBuffer(31, testBaselineSeed); err == nil {
		t.Error("expected error for max_entries=31 (below min 32)")
	}
	if _, err := NewEvidenceBuffer(1025, testBaselineSeed); err == nil {
		t.Error("expected error for max_entries=1025 (above max 1024)")
	}
	if _, err := NewEvidenceBuffer(128, testBaselineSeed); err != nil {
		t.Errorf("expected no error for max_entries=128: %v", err)
	}
}

func newDiscoveryTestBuffer(t *testing.T) *EvidenceBuffer {
	t.Helper()
	b, err := NewEvidenceBuffer(32, nil)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestDiscoveryEvidence_PresenceOnlyDoesNotCount(t *testing.T) {
	b := newDiscoveryTestBuffer(t)
	for i := 0; i < 4; i++ {
		if b.Record(EvidenceRecord{Address: 0x20, Strength: EvidencePresenceOnly}) {
			t.Fatal("presence-only evidence promoted")
		}
	}
	if b.Len() != 1 || !Contains(b, 0x20) || len(b.PromotedAddresses()) != 0 {
		t.Fatal("presence must be retained without promotion")
	}
	if b.Record(EvidenceRecord{Address: 0x20, Strength: EvidenceWeak}) {
		t.Fatal("presence contributed toward weak promotion")
	}
	if !b.Record(EvidenceRecord{Address: 0x20, Strength: EvidenceWeak}) {
		t.Fatal("two fresh weak observations must promote")
	}
}

func TestDiscoveryEvidence_DemotionResetsCounters(t *testing.T) {
	for _, strength := range []EvidenceStrength{EvidenceWeak, EvidenceStrong} {
		t.Run(fmt.Sprint(strength), func(t *testing.T) {
			b := newDiscoveryTestBuffer(t)
			b.Record(EvidenceRecord{Address: 0x20, Strength: strength})
			if strength == EvidenceWeak {
				b.Record(EvidenceRecord{Address: 0x20, Strength: strength})
			}
			if !b.Demote(0x20) || b.Demote(0x20) || b.Demote(0x21) {
				t.Fatal("demotion must report only a promoted-to-unpromoted transition")
			}
			if b.Len() != 1 || !Contains(b, 0x20) || len(b.PromotedAddresses()) != 0 {
				t.Fatal("demotion must retain the address without promotion")
			}
			if b.Record(EvidenceRecord{Address: 0x20, Strength: EvidenceWeak}) {
				t.Fatal("old counters survived demotion")
			}
			if !b.Record(EvidenceRecord{Address: 0x20, Strength: EvidenceWeak}) {
				t.Fatal("two new weak observations must re-promote")
			}
		})
	}
}

func TestDiscoveryEvidence_SortedDetachedReads(t *testing.T) {
	b := newDiscoveryTestBuffer(t)
	for _, addr := range []byte{0x50, 0x10, 0x30} {
		b.Record(EvidenceRecord{Address: addr, Strength: EvidenceStrong})
	}
	want := []byte{0x10, 0x30, 0x50}
	for i := 0; i < 64; i++ {
		got := b.PromotedAddresses()
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("sorted addresses = %v, want %v", got, want)
		}
		got[0] = 0xEE
	}
	retained := b.PromotedAddresses()
	b.Demote(0x30)
	b.Record(EvidenceRecord{Address: 0x40, Strength: EvidenceStrong})
	if !reflect.DeepEqual(retained, want) {
		t.Fatalf("later storage mutation changed previous read: %v", retained)
	}
	if got := b.PromotedAddresses(); !reflect.DeepEqual(got, []byte{0x10, 0x40, 0x50}) {
		t.Fatalf("fresh read = %v", got)
	}
}

func TestDiscoveryEvidence_LRUPresenceRefresh(t *testing.T) {
	b := newDiscoveryTestBuffer(t)
	for addr := byte(0); addr < 32; addr++ {
		b.Record(EvidenceRecord{Address: addr, Strength: EvidenceWeak})
	}
	b.Record(EvidenceRecord{Address: 0, Strength: EvidencePresenceOnly})
	b.Record(EvidenceRecord{Address: 32, Strength: EvidenceWeak})
	if b.Len() != 32 || !Contains(b, 0) || Contains(b, 1) || !Contains(b, 32) {
		t.Fatal("must evict oldest untouched address at capacity")
	}
	if len(b.PromotedAddresses()) != 0 {
		t.Fatal("presence refresh must not promote a weak record")
	}
}

func TestDiscoveryEvidence_AddressKeyIsolation(t *testing.T) {
	// Address is the sole storage key; vary it alone across the byte space.
	for other := 0; other < 256; other++ {
		if other == 0x20 {
			continue
		}
		b := newDiscoveryTestBuffer(t)
		b.Record(EvidenceRecord{Address: 0x20, Strength: EvidenceWeak})
		if b.Record(EvidenceRecord{Address: byte(other), Strength: EvidenceWeak}) {
			t.Fatalf("distinct address 0x%02X shared a promotion counter", other)
		}
		if b.Len() != 2 || !Contains(b, 0x20) || !Contains(b, byte(other)) {
			t.Fatalf("distinct address 0x%02X did not get its own entry", other)
		}
		if !b.Record(EvidenceRecord{Address: 0x20, Strength: EvidenceWeak}) {
			t.Fatal("original address lost its counter")
		}
		if got := b.PromotedAddresses(); !reflect.DeepEqual(got, []byte{0x20}) {
			t.Fatalf("promotion leaked to another address: %v", got)
		}
	}
}

func TestDiscoveryEvidence_ProtectedCapacityAndDetachedSeed(t *testing.T) {
	seed := make([]byte, 33)
	for i := range seed {
		seed[i] = byte(i + 3)
	}
	b, err := NewEvidenceBuffer(32, seed)
	if err != nil {
		t.Fatal(err)
	}
	seed[0] = 0x60
	for addr := byte(3); addr < 35; addr++ {
		b.Record(EvidenceRecord{Address: addr, Strength: EvidenceWeak})
	}
	if b.Record(EvidenceRecord{Address: 0x60, Strength: EvidenceStrong}) || b.Len() != 32 || Contains(b, 0x60) {
		t.Fatal("full protected buffer must reject non-baseline evidence")
	}
	// Preserve existing behavior: a missing protected address can exceed capacity
	// when every retained entry is protected, rather than evicting a baseline.
	b.Record(EvidenceRecord{Address: 35, Strength: EvidenceWeak})
	if b.Len() != 33 {
		t.Fatalf("protected insertion length = %d, want 33", b.Len())
	}
	for addr := byte(3); addr <= 35; addr++ {
		if !Contains(b, addr) {
			t.Fatalf("baseline 0x%02X was lost", addr)
		}
	}
}
