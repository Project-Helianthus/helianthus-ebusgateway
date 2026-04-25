package ebusgateway

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

// EvidenceStrength classifies an observation's contribution weight toward
// promoting a suspect to a confirmed-identity candidate.
type EvidenceStrength uint8

const (
	EvidenceWeak EvidenceStrength = iota + 1
	EvidenceStrong
)

// EvidenceRecord is a single observation for a bus address.
type EvidenceRecord struct {
	Address  byte
	Strength EvidenceStrength
	Observed time.Time
	Kind     string
}

// BaselineTopologySeed is the set of addresses protected from LRU eviction.
// Vaillant default: {0x08 BAI00, 0x15 BASV2, 0x26 VR_71, 0x04 NETX3-A,
// 0xF6 NETX3-B, 0xEC SOL00}. Operators MAY override via config; validation
// enforces slave-range [0x03, 0xFE] excluding 0xAA (SYN) and 0xFE (broadcast).
var VaillantBaselineTopologySeed = []byte{0x08, 0x15, 0x26, 0x04, 0xF6, 0xEC}

// ValidateBaselineTopologySeed checks the config-provided seed against the
// slave-address range.
func ValidateBaselineTopologySeed(seed []byte) error {
	for _, addr := range seed {
		if addr < 0x03 || addr >= 0xFE {
			return fmt.Errorf("evidence: baseline seed 0x%02X out of slave range [0x03, 0xFE]", addr)
		}
		if addr == 0xAA {
			return fmt.Errorf("evidence: baseline seed must not include SYN (0xAA)")
		}
	}
	return nil
}

// EvidenceBuffer is a bounded, LRU-with-baseline-protection buffer of
// per-address observations. Its max capacity is max_entries=128 (per AD05).
// When the buffer is full and a new non-baseline address is observed, the
// oldest non-baseline entry is evicted. Baseline addresses are NEVER
// evicted — they may update in place but never leave the buffer.
type EvidenceBuffer struct {
	mu         sync.Mutex
	maxEntries int
	baseline   map[byte]struct{}
	entries    map[byte]*addressState
	tick       uint64
}

type addressState struct {
	address      byte
	observations int
	strongObs    int
	lastObserved time.Time
	touchedAt    uint64
	promoted     bool
}

func NewEvidenceBuffer(maxEntries int, baseline []byte) (*EvidenceBuffer, error) {
	if err := ValidateBaselineTopologySeed(baseline); err != nil {
		return nil, err
	}
	if maxEntries < 32 || maxEntries > 1024 {
		return nil, fmt.Errorf("evidence: max_entries=%d out of range [32, 1024]", maxEntries)
	}
	buf := &EvidenceBuffer{
		maxEntries: maxEntries,
		baseline:   make(map[byte]struct{}, len(baseline)),
		entries:    make(map[byte]*addressState, maxEntries),
	}
	for _, addr := range baseline {
		buf.baseline[addr] = struct{}{}
	}
	return buf, nil
}

// Record adds or updates evidence for an address. Returns whether the address
// crossed the promotion threshold as a result (≥2 observations OR any strong).
func (b *EvidenceBuffer) Record(record EvidenceRecord) (promoted bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.tick++
	state, ok := b.entries[record.Address]
	if !ok {
		if len(b.entries) >= b.maxEntries {
			b.evictOldestNonBaselineLocked()
		}
		if len(b.entries) >= b.maxEntries {
			if _, isBaseline := b.baseline[record.Address]; !isBaseline {
				return false
			}
		}
		state = &addressState{address: record.Address}
		b.entries[record.Address] = state
	}

	state.observations++
	if record.Strength == EvidenceStrong {
		state.strongObs++
	}
	state.lastObserved = record.Observed
	state.touchedAt = b.tick

	if !state.promoted && (state.observations >= 2 || state.strongObs >= 1) {
		state.promoted = true
		return true
	}
	return false
}

func (b *EvidenceBuffer) evictOldestNonBaselineLocked() {
	var (
		victimAddr byte
		victimTick = ^uint64(0)
		found      bool
	)
	for addr, st := range b.entries {
		if _, base := b.baseline[addr]; base {
			continue
		}
		if st.touchedAt < victimTick {
			victimTick = st.touchedAt
			victimAddr = addr
			found = true
		}
	}
	if found {
		delete(b.entries, victimAddr)
	}
}

// PromotedAddresses returns the sorted set of addresses that have crossed
// the promotion threshold. Caller uses this to build the directed probe
// target list for the ebusreg ScanDirected call.
func (b *EvidenceBuffer) PromotedAddresses() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()

	out := make([]byte, 0)
	for addr, st := range b.entries {
		if st.promoted {
			out = append(out, addr)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// Len returns the current count of stored entries.
func (b *EvidenceBuffer) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.entries)
}
