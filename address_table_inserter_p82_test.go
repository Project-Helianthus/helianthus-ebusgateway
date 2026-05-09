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
// Methodology: a writer goroutine directly mutates t.slots (under
// the new slotsMu Lock) and reader goroutines hammer
// AddressTable.Lookup (which takes RLock). Race detector (-race) is
// the authoritative gate — any unsynced map access would fail.
//
// Why bypass the inserter (Codex P8.2 review MINOR FINDING_1): the
// inserter's maybeInsert short-circuits via `if _, exists :=
// table.Lookup(addr); exists { return }` once an address has been
// registered, so a writer cycling a small address pool only mutates
// the map on the FIRST pass. End-of-phase counters tracking
// OnPassiveClassifiedEvent calls thus over-count actual writes, and
// the test could appear to pass while only exercising a tiny race
// window. Driving the map mutations directly here ensures every
// iteration of the writer goroutine produces a real
// `t.slots[addr] = ...` assignment that overlaps with the reader
// goroutines' map reads.
//
// We still go through the slotsMu lock so the test exercises the
// same locking path the inserter uses in production.
func TestATRInserter_P82_ConcurrentLookupAndInsertRaceFree(t *testing.T) {
	reg := registry.NewDeviceRegistry(nil)
	table := NewAddressTable(reg)

	const writeAddrPool = 16
	var slotWrites uint64
	stop := make(chan struct{})

	var writerWg sync.WaitGroup
	var readerWg sync.WaitGroup

	// Writer goroutine: directly hammer t.slots under slotsMu Lock,
	// using a rotating pool of addresses. Each iteration is a real
	// map mutation (not gated by the inserter's existence-check
	// short-circuit), so the slotWrites counter accurately reflects
	// the number of map assignments.
	writerWg.Add(1)
	go func() {
		defer writerWg.Done()
		i := 0
		for {
			select {
			case <-stop:
				return
			default:
			}
			addr := byte(0x10 + (i % writeAddrPool))
			table.slotsMu.Lock()
			table.slots[addr] = &AddressSlot{
				Addr:              addr,
				Role:              "target",
				DiscoverySource:   "passive_observed",
				VerificationState: "corroborated_pending",
			}
			table.slotsMu.Unlock()
			atomic.AddUint64(&slotWrites, 1)
			i++
		}
	}()

	// Start barrier — wait for at least one slot write before
	// kicking the readers (proves the writer is producing real map
	// mutations before reads begin).
	deadline := time.Now().Add(2 * time.Second)
	for atomic.LoadUint64(&slotWrites) == 0 {
		if time.Now().After(deadline) {
			close(stop)
			writerWg.Wait()
			t.Fatal("writer goroutine produced no slot writes within 2s — start barrier exceeded")
		}
		time.Sleep(time.Microsecond)
	}

	startWrites := atomic.LoadUint64(&slotWrites)

	// 4 reader goroutines hammering Lookup. Tracked in a separate
	// WaitGroup so we can join them BEFORE stopping the writer —
	// Codex P8.2 pass 2 MINOR: closing stop immediately after
	// spawning readers would let the writer exit before the readers
	// actually execute map reads, defeating the overlap test.
	const numReaders = 4
	for r := 0; r < numReaders; r++ {
		readerWg.Add(1)
		go func() {
			defer readerWg.Done()
			for j := 0; j < 1000; j++ {
				addr := byte(0x10 + (j % writeAddrPool))
				if slot, ok := table.Lookup(addr); ok && slot != nil {
					_ = slot.DiscoverySource
					_ = slot.VerificationState
					_ = slot.Role
				}
			}
		}()
	}

	// Wait for readers FIRST — writer keeps producing map writes
	// throughout the reader phase, ensuring real overlap.
	readerWg.Wait()
	// End-of-phase write counter sample: must have advanced during
	// the reader phase (proves the writer kept producing map
	// mutations while readers ran, not just before they spawned).
	endWrites := atomic.LoadUint64(&slotWrites)

	// Now stop the writer.
	close(stop)
	writerWg.Wait()

	// The threshold is intentionally low — goroutine scheduling can
	// starve the writer when readers dominate the runqueue, but the
	// reader-then-writer join order ensures the writer was running
	// for the full reader phase. A non-zero advance proves overlap;
	// the race detector is the authoritative correctness gate.
	if endWrites-startWrites < 1 {
		t.Errorf("writer produced 0 slot writes during reader phase; want >= 1 (race window not exercised)")
	}
}

// TestATRInserter_P82_ConcurrentLookupAndInserterEndToEnd is the
// end-to-end variant: the writer goes through the actual inserter,
// which exercises the inserter's call site of slotsMu.Lock. Limited
// race-window exposure (the inserter short-circuits after first-pass
// admission), but proves the lock IS taken on the production path.
func TestATRInserter_P82_ConcurrentLookupAndInserterEndToEnd(t *testing.T) {
	reg := registry.NewDeviceRegistry(nil)
	table := NewAddressTable(reg)
	inserter := NewAddressTableInserter(table, DefaultConfig())

	stop := make(chan struct{})
	var inserterCalls uint64

	var writerWg sync.WaitGroup
	var readerWg sync.WaitGroup

	writerWg.Add(1)
	go func() {
		defer writerWg.Done()
		i := 0
		for {
			select {
			case <-stop:
				return
			default:
			}
			// Cycle distinct addresses so each call MAY produce a
			// real map write the first time it's seen. After all
			// 240 candidate addresses have been admitted the
			// inserter starts short-circuiting — that's fine; this
			// test focuses on proving the mutex is taken, not on
			// long-running overlap.
			addr := byte(0x10 + byte(i%240))
			event := atrPassiveTransactionEvent(time.Now().UTC(), 0xF1, addr, protocol.SymbolAck)
			inserter.OnPassiveClassifiedEvent(event)
			atomic.AddUint64(&inserterCalls, 1)
			i++
		}
	}()

	// Wait for at least one inserter call.
	deadline := time.Now().Add(2 * time.Second)
	for atomic.LoadUint64(&inserterCalls) == 0 {
		if time.Now().After(deadline) {
			close(stop)
			writerWg.Wait()
			t.Fatal("inserter goroutine made no calls within 2s")
		}
		time.Sleep(time.Microsecond)
	}

	const numReaders = 4
	for r := 0; r < numReaders; r++ {
		readerWg.Add(1)
		go func() {
			defer readerWg.Done()
			for j := 0; j < 500; j++ {
				addr := byte(0x10 + byte(j%240))
				_, _ = table.Lookup(addr)
			}
		}()
	}

	// Codex P8.2 pass 2 MINOR — wait for readers to complete BEFORE
	// stopping the writer. Closing stop too early would let the
	// writer exit before the reader phase actually runs.
	readerWg.Wait()
	close(stop)
	writerWg.Wait()
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
