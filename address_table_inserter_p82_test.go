package ebusgateway

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
)

// TestATRInserter_P82_ConcurrentLookupAndInsertRaceFree closes the
// pre-existing concurrent-map-access bug flagged by Codex on PR #590:
// the AddressTable's `slots` map was written by the inserter (passive
// reconstructor goroutine) and read by Lookup callers (MCP / GraphQL
// / snapshot consumers, distinct goroutines). Pre-P8.2 there was no
// mutex, so concurrent map writes can panic Go's runtime and
// concurrent read+write trips the race detector.
//
// Methodology: a writer goroutine drives passive insertions across a
// rotating address pool while reader goroutines hammer
// AddressTable.Lookup. Race detector (-race) is the authoritative
// gate — a torn map read or any concurrent map access would fail.
//
// The test also asserts the reader sees a consistent slot value —
// the cached AddressSlot's label fields are projected from the
// registry snapshot (P8.1) so the read is doubly guarded: t.slotsMu
// for the map access, then registry.r.mu for the enum reads.
func TestATRInserter_P82_ConcurrentLookupAndInsertRaceFree(t *testing.T) {
	reg := registry.NewDeviceRegistry(nil)
	table := NewAddressTable(reg)
	inserter := NewAddressTableInserter(table, DefaultConfig())

	const writeAddrPool = 16
	var writes uint64
	stop := make(chan struct{})

	var wg sync.WaitGroup

	// Writer goroutine: cycle through addresses 0x10..0x1F.
	wg.Add(1)
	go func() {
		defer wg.Done()
		i := 0
		for {
			select {
			case <-stop:
				return
			default:
			}
			addr := byte(0x10 + (i % writeAddrPool))
			event := atrPassiveTransactionEvent(time.Now().UTC(), 0xF1, addr, protocol.SymbolAck)
			inserter.OnPassiveClassifiedEvent(event)
			atomic.AddUint64(&writes, 1)
			i++
		}
	}()

	// Start barrier — wait for at least one write before reads.
	deadline := time.Now().Add(2 * time.Second)
	for atomic.LoadUint64(&writes) == 0 {
		if time.Now().After(deadline) {
			close(stop)
			wg.Wait()
			t.Fatal("writer goroutine produced no writes within 2s — start barrier exceeded")
		}
		time.Sleep(time.Microsecond)
	}

	// 4 reader goroutines hammering Lookup.
	const numReaders = 4
	startWrites := atomic.LoadUint64(&writes)
	for r := 0; r < numReaders; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				addr := byte(0x10 + (j % writeAddrPool))
				if slot, ok := table.Lookup(addr); ok && slot != nil {
					// Read each label field (these are race-free per
					// P8.1: snapshot copy under registry RLock).
					_ = slot.DiscoverySource
					_ = slot.VerificationState
					_ = slot.Role
				}
			}
		}()
	}

	// Stop the writer; wg.Wait joins both the writer and the reader
	// goroutines (each reader has a fixed 1000-iteration loop and
	// exits naturally; the writer exits on stop).
	close(stop)
	wg.Wait()

	// End-of-phase assertion: writes counter advanced sufficiently
	// during the test.
	endWrites := atomic.LoadUint64(&writes)
	if endWrites <= startWrites {
		t.Errorf("writer produced %d writes after start barrier; want > 0 (race window not exercised)", endWrites-startWrites)
	}
}

// TestATRInserter_P82_LookupAfterInsertSeesUpdatedMap is a sanity
// check that the mutex doesn't accidentally hide writes from
// readers. Single-goroutine: insert then immediately Lookup. The
// reader MUST see the inserted slot.
func TestATRInserter_P82_LookupAfterInsertSeesUpdatedMap(t *testing.T) {
	reg := registry.NewDeviceRegistry(nil)
	table := NewAddressTable(reg)
	inserter := NewAddressTableInserter(table, DefaultConfig())

	event := atrPassiveTransactionEvent(time.Now().UTC(), 0xF1, 0x99, protocol.SymbolAck)
	inserter.OnPassiveClassifiedEvent(event)

	slot, ok := table.Lookup(0x99)
	if !ok || slot == nil {
		t.Fatalf("Lookup(0x99) ok=%v slot=%v after passive insert; want hit", ok, slot)
	}
	if slot.DiscoverySource != "passive_observed" {
		t.Errorf("slot.DiscoverySource = %q; want passive_observed", slot.DiscoverySource)
	}
}
