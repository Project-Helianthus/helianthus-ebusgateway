package ebusgateway

import (
	"testing"
	"time"
)

func TestEvidenceBuffer_PromotionOnTwoObservations(t *testing.T) {
	b, err := NewEvidenceBuffer(128, VaillantBaselineTopologySeed)
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

func TestEvidenceBuffer_PromotionOnSingleStrongEvidence(t *testing.T) {
	b, _ := NewEvidenceBuffer(128, VaillantBaselineTopologySeed)
	promoted := b.Record(EvidenceRecord{Address: 0x20, Strength: EvidenceStrong, Observed: time.Now()})
	if !promoted {
		t.Errorf("expected immediate promotion on single strong evidence")
	}
}

func TestEvidenceBuffer_FloodWithDefaultSeed_BaselineSurvives(t *testing.T) {
	b, _ := NewEvidenceBuffer(128, VaillantBaselineTopologySeed)
	now := time.Now()
	for _, addr := range VaillantBaselineTopologySeed {
		b.Record(EvidenceRecord{Address: addr, Strength: EvidenceWeak, Observed: now})
	}
	for i := 0; i < 1000; i++ {
		addr := byte(0x20 + (i % 0xD0))
		b.Record(EvidenceRecord{Address: addr, Strength: EvidenceWeak, Observed: now.Add(time.Duration(i) * time.Millisecond)})
	}
	if b.Len() > 128 {
		t.Errorf("buffer overflow: len=%d > 128", b.Len())
	}
	for _, baseAddr := range VaillantBaselineTopologySeed {
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

func TestEvidenceBuffer_FloodWithNonDefaultSeed_BaselineSurvives(t *testing.T) {
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

func TestEvidenceBuffer_ValidateBaselineTopologySeed_RejectsOutOfRange(t *testing.T) {
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

func TestEvidenceBuffer_NewRejectsOutOfRangeMaxEntries(t *testing.T) {
	if _, err := NewEvidenceBuffer(31, VaillantBaselineTopologySeed); err == nil {
		t.Error("expected error for max_entries=31 (below min 32)")
	}
	if _, err := NewEvidenceBuffer(1025, VaillantBaselineTopologySeed); err == nil {
		t.Error("expected error for max_entries=1025 (above max 1024)")
	}
	if _, err := NewEvidenceBuffer(128, VaillantBaselineTopologySeed); err != nil {
		t.Errorf("expected no error for max_entries=128: %v", err)
	}
}
