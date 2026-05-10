package main

import (
	"testing"
)

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
