package v8classifier

import (
	"sync"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/protocol/telegram_fsm"
	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
)

// Phase 3 Step B3.6a: tests for the outbound admin-event surface
// (per-classifier ring buffer + auto-emission of FSM
// DecisionProtocolFault events). Per v8 invariant I1: events are
// out-of-band only — these tests confirm the ring buffer captures
// them, the Drain pattern is multi-consumer safe, and overflow
// drops the OLDEST entry rather than blocking the producer.

func TestAdminEventKind_String(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		k    AdminEventKind
		want string
	}{
		{AdminEventKindNone, "none"},
		{AdminEventKindProtocolFault, "protocol_fault"},
		{AdminEventKindEchoSoftTimeout, "echo_soft_timeout"},
		{AdminEventKindEchoHardTimeout, "echo_hard_timeout"},
		{AdminEventKindQueueOverflow, "queue_overflow"},
		{AdminEventKind(99), "unknown"},
	} {
		if got := tc.k.String(); got != tc.want {
			t.Errorf("AdminEventKind(%d).String() = %q; want %q", tc.k, got, tc.want)
		}
	}
}

func TestAdminEventBuffer_EmitAndDrain(t *testing.T) {
	t.Parallel()
	var b adminEventBuffer

	// Drain on an empty buffer.
	events, dropped := b.drain()
	if len(events) != 0 {
		t.Errorf("drain empty: got %d events; want 0", len(events))
	}
	if dropped != 0 {
		t.Errorf("drain empty: dropped=%d; want 0", dropped)
	}

	// Emit 3 events.
	now := time.Unix(0, 0)
	for i := 0; i < 3; i++ {
		b.emit(ClassifierAdminEvent{
			At:   now.Add(time.Duration(i) * time.Millisecond),
			Kind: AdminEventKindProtocolFault,
			Byte: byte(i),
		})
	}
	if got := b.pending(); got != 3 {
		t.Errorf("pending after 3 emits = %d; want 3", got)
	}

	events, dropped = b.drain()
	if len(events) != 3 {
		t.Fatalf("drain: got %d events; want 3", len(events))
	}
	if dropped != 0 {
		t.Errorf("drain: dropped=%d; want 0 (no overflow)", dropped)
	}
	for i, ev := range events {
		if ev.Kind != AdminEventKindProtocolFault {
			t.Errorf("event %d kind=%v; want ProtocolFault", i, ev.Kind)
		}
		if ev.Byte != byte(i) {
			t.Errorf("event %d byte=0x%02X; want 0x%02X", i, ev.Byte, byte(i))
		}
	}

	// Buffer drained — pending=0.
	if got := b.pending(); got != 0 {
		t.Errorf("pending after drain = %d; want 0", got)
	}
}

func TestAdminEventBuffer_Overflow_DropsOldest(t *testing.T) {
	t.Parallel()
	var b adminEventBuffer
	now := time.Unix(0, 0)

	// Emit cap+5 events. The first 5 must be dropped FIFO.
	const overage = 5
	total := adminEventBufferCap + overage
	for i := 0; i < total; i++ {
		b.emit(ClassifierAdminEvent{
			At:   now.Add(time.Duration(i) * time.Millisecond),
			Kind: AdminEventKindProtocolFault,
			Byte: byte(i),
		})
	}

	events, dropped := b.drain()
	if len(events) != adminEventBufferCap {
		t.Errorf("after overflow: got %d events; want %d (cap)", len(events), adminEventBufferCap)
	}
	if dropped != overage {
		t.Errorf("dropped=%d; want %d", dropped, overage)
	}

	// Verify the OLDEST `overage` events were dropped — the
	// surviving Byte values should be in [overage, total).
	for i, ev := range events {
		want := byte(i + overage)
		if ev.Byte != want {
			t.Errorf("event %d after overflow: Byte=0x%02X; want 0x%02X (oldest dropped FIFO)",
				i, ev.Byte, want)
		}
	}
}

// TestAdminEventBuffer_RejectsNoneKind pins the Codex round-1
// LOW fix: emit(Kind=None) is a no-op so the sentinel never
// pollutes the recorded buffer. None is the zero value and has
// no production caller; the guard enforces the documented
// invariant.
func TestAdminEventBuffer_RejectsNoneKind(t *testing.T) {
	t.Parallel()
	var b adminEventBuffer
	for i := 0; i < 10; i++ {
		b.emit(ClassifierAdminEvent{
			Kind: AdminEventKindNone,
			Byte: byte(i),
		})
	}
	if got := b.pending(); got != 0 {
		t.Errorf("pending after emit(Kind=None)×10 = %d; want 0 (rejected)", got)
	}
	events, dropped := b.drain()
	if len(events) != 0 {
		t.Errorf("drain after Kind=None emits: %d events; want 0", len(events))
	}
	if dropped != 0 {
		t.Errorf("dropped=%d; want 0 (Kind=None rejection is NOT a drop — it's a refusal to enqueue)", dropped)
	}
}

func TestAdminEventBuffer_DroppedCounter_ResetOnDrain(t *testing.T) {
	t.Parallel()
	var b adminEventBuffer

	// Cause a drop.
	for i := 0; i < adminEventBufferCap+1; i++ {
		b.emit(ClassifierAdminEvent{Kind: AdminEventKindProtocolFault})
	}
	_, dropped := b.drain()
	if dropped != 1 {
		t.Errorf("first drain after one overflow: dropped=%d; want 1", dropped)
	}

	// Drain again immediately — counter should be reset.
	_, dropped = b.drain()
	if dropped != 0 {
		t.Errorf("second drain: dropped=%d; want 0 (counter reset on first drain)", dropped)
	}
}

func TestClassifier_ProtocolFault_EmitsAdminEvent(t *testing.T) {
	t.Parallel()
	c := New(ModeShadow)
	now := time.Unix(0, 0)

	// Enter passive tracking + drive the FSM to a point where
	// a fault fires (NN > 16 in MASTER_HEADER byte 4 per v8).
	c.Observe(transport.StreamEvent{
		Kind: transport.StreamEventStarted, Data: 0x71,
	}, now)
	// PB, SB, ZZ — three plain bytes after the StreamEventStarted's
	// consumed QQ. Indices 1-3 in MASTER_HEADER.
	for _, b := range []byte{0x10, 0xB5, 0x16} {
		c.Observe(transport.StreamEvent{
			Kind: transport.StreamEventByte, Byte: b,
		}, now)
	}
	// MASTER_HEADER byte 4 (NN slot) = 0xFF — exceeds 16 → fault.
	c.Observe(transport.StreamEvent{
		Kind: transport.StreamEventByte, Byte: 0xFF,
	}, now)

	if got := c.FsmProtocolFaultTotal(); got != 1 {
		t.Fatalf("FsmProtocolFaultTotal()=%d; want 1 (precondition for admin emit)", got)
	}

	// Drain admin events; the protocol fault should appear.
	events, dropped := c.DrainAdminEvents()
	if dropped != 0 {
		t.Errorf("DrainAdminEvents: dropped=%d; want 0", dropped)
	}
	if len(events) != 1 {
		t.Fatalf("DrainAdminEvents: got %d events; want 1", len(events))
	}
	ev := events[0]
	if ev.Kind != AdminEventKindProtocolFault {
		t.Errorf("event kind=%v; want AdminEventKindProtocolFault", ev.Kind)
	}
	if ev.Byte != 0xFF {
		t.Errorf("event byte=0x%02X; want 0xFF (the offending NN)", ev.Byte)
	}
	if ev.FSMState != telegram_fsm.StateAborted {
		t.Errorf("event FSMState=%v; want StateAborted (FSM aborted on fault)", ev.FSMState)
	}
	if ev.At.IsZero() {
		t.Error("event At=zero; want monotonic-clock timestamp")
	}
}

func TestClassifier_DrainAdminEvents_NilSafe(t *testing.T) {
	t.Parallel()
	var c *Classifier
	events, dropped := c.DrainAdminEvents()
	if events != nil || dropped != 0 {
		t.Errorf("nil.DrainAdminEvents = (%v, %d); want (nil, 0)", events, dropped)
	}
	if got := c.PendingAdminEvents(); got != 0 {
		t.Errorf("nil.PendingAdminEvents = %d; want 0", got)
	}
}

func TestClassifier_DrainAdminEvents_OffMode_NoEvents(t *testing.T) {
	t.Parallel()
	c := New(ModeOff)
	// ModeOff: Observe is a no-op, no FSM, no admin events.
	now := time.Unix(0, 0)
	for i := 0; i < 10; i++ {
		c.Observe(transport.StreamEvent{
			Kind: transport.StreamEventByte, Byte: 0xFF,
		}, now)
	}
	events, dropped := c.DrainAdminEvents()
	if len(events) != 0 || dropped != 0 {
		t.Errorf("ModeOff DrainAdminEvents = (%d events, dropped=%d); want (0, 0)",
			len(events), dropped)
	}
}

func TestClassifier_PendingAdminEvents_ReflectsBuffer(t *testing.T) {
	t.Parallel()
	c := New(ModeShadow)
	now := time.Unix(0, 0)

	if got := c.PendingAdminEvents(); got != 0 {
		t.Errorf("PendingAdminEvents() at construction = %d; want 0", got)
	}

	// Drive two faults (each fault aborts; then we need a Reset
	// before driving the next).
	for i := 0; i < 2; i++ {
		c.Observe(transport.StreamEvent{
			Kind: transport.StreamEventStarted, Data: 0x71,
		}, now)
		for _, b := range []byte{0x10, 0xB5, 0x16} {
			c.Observe(transport.StreamEvent{
				Kind: transport.StreamEventByte, Byte: b,
			}, now)
		}
		c.Observe(transport.StreamEvent{
			Kind: transport.StreamEventByte, Byte: 0xFF,
		}, now)
		// Reset to ready for next iteration.
		c.Observe(transport.StreamEvent{
			Kind: transport.StreamEventReset,
		}, now)
	}

	if got := c.PendingAdminEvents(); got < 2 {
		t.Errorf("PendingAdminEvents()=%d; want >= 2 (two faults emitted)", got)
	}
	events, _ := c.DrainAdminEvents()
	if len(events) < 2 {
		t.Errorf("Drain returned %d events; want >= 2", len(events))
	}
	if got := c.PendingAdminEvents(); got != 0 {
		t.Errorf("after Drain: PendingAdminEvents()=%d; want 0", got)
	}
}

// TestAdminEventBuffer_ConcurrentDrainerVsProducer pins both
// the multi-consumer-safe contract AND a basic accounting
// invariant under concurrent producer/drainer activity
// (Codex round-1 LOW on PR #643 — the test previously only
// proved -race wouldn't fire).
//
// Invariant: total_drained + final_drain + dropped == total_emitted.
// Every emitted event either:
//   (a) gets drained mid-run, OR
//   (b) gets drained on the final flush, OR
//   (c) gets dropped (counted in `dropped`).
// No event silently disappears.
//
// The mutex inside adminEventBuffer guarantees no data race;
// the test fires under -race to verify.
func TestAdminEventBuffer_ConcurrentDrainerVsProducer(t *testing.T) {
	t.Parallel()
	var b adminEventBuffer
	stop := make(chan struct{})

	const totalEmitted = 1000

	// Drainer goroutine accumulates seen counts + dropped counts.
	var seen uint64
	var seenDropped uint64
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				events, dropped := b.drain()
				seen += uint64(len(events))
				seenDropped += dropped
			}
		}
	}()

	// Producer: totalEmitted emits.
	for i := 0; i < totalEmitted; i++ {
		b.emit(ClassifierAdminEvent{
			Kind: AdminEventKindProtocolFault,
			Byte: byte(i),
		})
	}
	close(stop)
	wg.Wait()

	// Final drain to flush any remainder.
	finalEvents, finalDropped := b.drain()
	seen += uint64(len(finalEvents))
	seenDropped += finalDropped

	// Accounting invariant.
	if total := seen + seenDropped; total != uint64(totalEmitted) {
		t.Errorf("accounting: seen=%d + dropped=%d = %d; want %d (no events silently lost)",
			seen, seenDropped, total, totalEmitted)
	}
}
