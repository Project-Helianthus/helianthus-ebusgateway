package main

import (
	"testing"
	"time"
)

// TestEvictionBlocklist_MarkClearLifecycle pins the AD23 anti-resurrection
// contract: M5 eviction marks an addr; subsequent passive observation
// (via the AddressTableInserter observer hook) clears it; the registry
// reconciler skips marked addrs. Codex P2 follow-up on PR #615.
func TestEvictionBlocklist_MarkClearLifecycle(t *testing.T) {
	b := newEvictionBlocklist()

	if b.isEvicted(0x08) {
		t.Errorf("fresh blocklist: 0x08 must not be evicted")
	}

	b.markEvicted(0x08)
	if !b.isEvicted(0x08) {
		t.Errorf("after markEvicted(0x08): isEvicted must return true")
	}
	if b.isEvicted(0x15) {
		t.Errorf("other addrs must not be affected by markEvicted(0x08)")
	}

	// Fresh passive observation clears the eviction.
	b.clear(0x08)
	if b.isEvicted(0x08) {
		t.Errorf("after clear(0x08): isEvicted must return false")
	}

	// Re-eviction (later cycle re-discovers the address as no_reply)
	// is idempotent.
	b.markEvicted(0x08)
	b.markEvicted(0x08)
	if !b.isEvicted(0x08) {
		t.Errorf("re-marking after clear must work")
	}
}

// TestEvictionBlocklist_EvictionTimeMonotonic verifies that evictionTime
// returns a non-zero timestamp after markEvicted, and that the timestamp
// advances when an addr is re-evicted after a clear (the reconciler uses
// this timestamp to gate against registry LastObservedAt for active-
// rediscovery resurrection — Codex P2 follow-up on PR #615).
func TestEvictionBlocklist_EvictionTimeMonotonic(t *testing.T) {
	b := newEvictionBlocklist()

	if _, ok := b.evictionTime(0x08); ok {
		t.Errorf("fresh blocklist: evictionTime(0x08) must return ok=false")
	}

	b.markEvicted(0x08)
	t1, ok := b.evictionTime(0x08)
	if !ok {
		t.Fatalf("after markEvicted: evictionTime(0x08) must return ok=true")
	}
	if t1.IsZero() {
		t.Errorf("evictionTime must be a real timestamp, not zero")
	}

	time.Sleep(2 * time.Millisecond)
	b.clear(0x08)
	b.markEvicted(0x08)
	t2, _ := b.evictionTime(0x08)
	if !t2.After(t1) {
		t.Errorf("re-eviction timestamp t2=%v must be after t1=%v", t2, t1)
	}
}

// TestRevalidationRotation_ShouldSkipBeforeAndAfterMark verifies the basic
// "probed-once then skip" contract.
func TestRevalidationRotation_ShouldSkipBeforeAndAfterMark(t *testing.T) {
	r := newRevalidationRotation()

	if r.shouldSkip(0x08) {
		t.Errorf("addr 0x08 must not be skipped before any markProbed")
	}

	r.markProbed(0x08)
	if !r.shouldSkip(0x08) {
		t.Errorf("addr 0x08 must be skipped after markProbed")
	}
	if r.shouldSkip(0x15) {
		t.Errorf("addr 0x15 must not be skipped (only 0x08 was marked)")
	}
}

// TestRevalidationRotation_ResetWhenRoundComplete verifies the round-complete
// detection: every cached addr probed → reset and start fresh.
func TestRevalidationRotation_ResetWhenRoundComplete(t *testing.T) {
	r := newRevalidationRotation()
	addrs := []byte{0x08, 0x15, 0xF6}

	// Empty round → not complete (no addrs supplied).
	if r.resetIfRoundComplete(nil) {
		t.Errorf("empty addrs must not trigger reset")
	}

	// Partial probing → not complete.
	r.markProbed(0x08)
	r.markProbed(0x15)
	if r.resetIfRoundComplete(addrs) {
		t.Errorf("partial round (2/3 probed) must not trigger reset")
	}
	if r.shouldSkip(0xF6) {
		t.Errorf("addr 0xF6 must still be eligible (not yet probed)")
	}

	// All probed → reset.
	r.markProbed(0xF6)
	if !r.resetIfRoundComplete(addrs) {
		t.Errorf("complete round (3/3 probed) must trigger reset")
	}

	// After reset, every addr is eligible again.
	for _, a := range addrs {
		if r.shouldSkip(a) {
			t.Errorf("addr 0x%02X must NOT be skipped after round reset", a)
		}
	}
}

// TestRevalidationRotation_64MemberRoundRobinAcceptance simulates the M5
// 64-member fixture across multiple cycles and verifies that ALL members
// are probed within two cap=32 cycles + reset on cycle 3 (Codex P2 follow-up
// on PR #615 — without rotation, the same 32 newest members would be
// probed every cycle indefinitely).
func TestRevalidationRotation_64MemberRoundRobinAcceptance(t *testing.T) {
	r := newRevalidationRotation()
	addrs := make([]byte, 64)
	for i := range addrs {
		addrs[i] = byte(0x01 + i)
	}

	// Simulate cycle 1: 32 newest get marked probed.
	for i := 0; i < 32; i++ {
		r.markProbed(addrs[i])
	}
	if r.resetIfRoundComplete(addrs) {
		t.Errorf("after cycle 1 (32/64), round must NOT be complete")
	}
	for i := 0; i < 32; i++ {
		if !r.shouldSkip(addrs[i]) {
			t.Errorf("cycle 1: addr 0x%02X must be skipped (already probed)", addrs[i])
		}
	}
	for i := 32; i < 64; i++ {
		if r.shouldSkip(addrs[i]) {
			t.Errorf("cycle 1: addr 0x%02X must be eligible (not yet probed)", addrs[i])
		}
	}

	// Simulate cycle 2: remaining 32 get probed.
	for i := 32; i < 64; i++ {
		r.markProbed(addrs[i])
	}
	// Now resetIfRoundComplete should clear and return true.
	if !r.resetIfRoundComplete(addrs) {
		t.Errorf("after cycle 2 (64/64), round MUST be complete and reset")
	}

	// Cycle 3 starts fresh — all 64 eligible again.
	for _, a := range addrs {
		if r.shouldSkip(a) {
			t.Errorf("cycle 3 (post-reset): addr 0x%02X must be eligible", a)
		}
	}
}
