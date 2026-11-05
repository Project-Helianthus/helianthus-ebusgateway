package teslaobserve

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"
)

type hookHarness struct {
	clock    *syntheticClock
	source   *scriptedSource
	endpoint *Endpoint
}

func newHookHarness(t *testing.T, script []scriptedByte) *hookHarness {
	t.Helper()
	clock := &syntheticClock{base: time.Unix(0, 0)}
	source := &scriptedSource{clock: clock, bytes: script}
	endpoint, err := NewEndpoint(syntheticLabel, 7, source, clock.now)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = endpoint.Close() })
	return &hookHarness{clock: clock, source: source, endpoint: endpoint}
}

func (harness *hookHarness) request() Request {
	return Request{
		ExpectedGeneration: harness.endpoint.Generation(), Deadline: harness.clock.base.Add(ms(syntheticDeadline)),
		ClockEpoch: syntheticClockEpoch, RequestedBytes: syntheticRequested,
		SegmentCap: syntheticSegmentCap, SegmentGap: 500 * time.Microsecond,
	}
}

func (harness *hookHarness) observe(t *testing.T) Observation {
	t.Helper()
	observation, err := harness.endpoint.Observe(context.Background(), harness.request())
	if err != nil {
		t.Fatal(err)
	}
	return observation
}

func TestTeslaObserveHookFragmentedBaselineRetainsSegmentsAndOffsets(t *testing.T) {
	harness := newHookHarness(t, segmentsAt(syntheticFragments, syntheticOffsets, 5))
	observation := harness.observe(t)
	if observation.Termination != TerminationCaptured || string(observation.Window()) != "TESLA\x00PASS\x00" {
		t.Fatalf("termination %s window %q", observation.Termination, observation.Window())
	}
	if len(observation.Segments) != 4 || observation.Coverage.SegmentCount != 4 || observation.Coverage.CapturedBytes != 11 {
		t.Fatalf("segments %d coverage %+v", len(observation.Segments), observation.Coverage)
	}
	for index, fragment := range syntheticFragments {
		segment := observation.Segments[index]
		if string(segment.Bytes) != fragment || segment.Direction != DirectionReceive || len(segment.Offsets) != len(fragment) {
			t.Fatalf("segment %d = %+v", index, segment)
		}
		for _, offset := range segment.Offsets {
			if offset != ms(syntheticOffsets[index]) {
				t.Fatalf("segment %d offset %s", index, offset)
			}
		}
	}
	if observation.Return != ms(5) || observation.Deadline != ms(syntheticDeadline) || observation.Generation != 1 ||
		observation.Instance != 7 || observation.Endpoint != syntheticLabel || observation.ClockEpoch != syntheticClockEpoch {
		t.Fatalf("identity/time = %+v", observation)
	}
	if observation.Coverage.Framing != FramingUnknown || observation.Coverage.TimingResolution != TimingResolutionHostRead ||
		len(observation.Coverage.Limitations) == 0 {
		t.Fatalf("coverage diagnostics = %+v", observation.Coverage)
	}
}

func TestTeslaObserveHookSyntheticTerminations(t *testing.T) {
	oneByte := make([]string, 9)
	offsets := make([]int, 9)
	for index := range oneByte {
		oneByte[index], offsets[index] = string("TESLA\x00PAS"[index]), index+1
	}
	cases := []struct {
		name     string
		script   []scriptedByte
		want     Termination
		captured int
		overflow int
	}{
		{"late return at deadline keeps bytes", segmentsAt(syntheticFragments, syntheticOffsets, syntheticDeadline), TerminationDeadline, 11, 0},
		{"partial keeps six bytes", segmentsAt([]string{"TESLA\x00"}, []int{1}, 0), TerminationDeadline, 6, 0},
		{"missing keeps nothing", nil, TerminationDeadline, 0, 0},
		{"leading unrelated byte is not trimmed", segmentsAt([]string{"X", "TE", "SLA\x00", "PA", "SS"}, []int{1, 2, 3, 4, 5}, 6), TerminationCaptured, 11, 0},
		{"segment exhaustion keeps eight bytes and the consumed ninth", segmentsAt(oneByte, offsets, 0), TerminationSegmentBound, 8, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			observation := newHookHarness(t, tc.script).observe(t)
			if observation.Termination != tc.want || observation.Coverage.CapturedBytes != tc.captured ||
				len(observation.Coverage.Overflow) != tc.overflow {
				t.Fatalf("termination %s captured %d overflow %d", observation.Termination, observation.Coverage.CapturedBytes, len(observation.Coverage.Overflow))
			}
			if tc.name == "leading unrelated byte is not trimmed" && string(observation.Window()) != "XTESLA\x00PASS" {
				t.Fatalf("window %q", observation.Window())
			}
		})
	}
}

// Each field of the observation binding is varied alone; the production
// check that each mutation defeats is named beside it.
func TestTeslaObserveWithCurrentRejectsEachSingleFieldChange(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*Observation)
	}{
		{"endpoint label (label equality check)", func(o *Observation) { o.Endpoint = "synthetic-f" }},
		{"instance (instance equality check)", func(o *Observation) { o.Instance++ }},
		{"generation (generation equality check)", func(o *Observation) { o.Generation++ }},
		{"observation identity (issued identity check)", func(o *Observation) { o.ID++ }},
		{"clock epoch (clock domain check)", func(o *Observation) { o.ClockEpoch = "synthetic-d" }},
		{"deadline offset (issued deadline check)", func(o *Observation) { o.Deadline++ }},
		{"return offset (issued return check)", func(o *Observation) { o.Return = ms(syntheticDeadline) }},
		{"termination (issued termination check)", func(o *Observation) { o.Termination = TerminationDeadline }},
		{"one native byte (exact byte check)", func(o *Observation) { o.Segments[2].Bytes[0] ^= 0x01 }},
		{"segment order (exact order check)", func(o *Observation) { o.Segments[0], o.Segments[1] = o.Segments[1], o.Segments[0] }},
		{"segment direction (direction check)", func(o *Observation) { o.Segments[3].Direction = "transmit" }},
		{"one receipt offset (exact offset check)", func(o *Observation) { o.Segments[1].Offsets[0]++ }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			harness := newHookHarness(t, segmentsAt(syntheticFragments, syntheticOffsets, 5))
			observation := harness.observe(t)
			changed := observation.clone()
			tc.mutate(&changed)
			committed := false
			if err := harness.endpoint.WithCurrent(context.Background(), changed, func() error { committed = true; return nil }); !errors.Is(err, ErrNotCurrent) || committed {
				t.Fatalf("changed observation err=%v committed=%t", err, committed)
			}
			// The unchanged observation still binds: the rejection came from
			// the varied field alone.
			if err := harness.endpoint.WithCurrent(context.Background(), observation, func() error { committed = true; return nil }); err != nil || !committed {
				t.Fatalf("unchanged observation err=%v committed=%t", err, committed)
			}
		})
	}
}

func TestTeslaObserveWithCurrentFailsAfterDeadlineCancelFenceCloseAndReuse(t *testing.T) {
	cases := []struct {
		name   string
		before func(*hookHarness, context.CancelFunc)
		want   error
	}{
		{"deadline reached before commit", func(h *hookHarness, _ context.CancelFunc) { h.clock.push(ms(syntheticDeadline)) }, ErrNotCurrent},
		{"context canceled before commit", func(_ *hookHarness, cancel context.CancelFunc) { cancel() }, ErrNotCurrent},
		{"generation fenced before commit", func(h *hookHarness, _ context.CancelFunc) { h.endpoint.Fence() }, ErrNotCurrent},
		{"endpoint closed before commit", func(h *hookHarness, _ context.CancelFunc) { _ = h.endpoint.Close() }, ErrClosed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			harness := newHookHarness(t, segmentsAt(syntheticFragments, syntheticOffsets, 5))
			observation := harness.observe(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			tc.before(harness, cancel)
			committed := false
			err := harness.endpoint.WithCurrent(ctx, observation, func() error { committed = true; return nil })
			if !errors.Is(err, tc.want) || committed {
				t.Fatalf("err=%v committed=%t", err, committed)
			}
		})
	}
}

func TestTeslaObserveObservationIsUsableOnce(t *testing.T) {
	harness := newHookHarness(t, segmentsAt(syntheticFragments, syntheticOffsets, 5))
	observation := harness.observe(t)
	if err := harness.endpoint.WithCurrent(context.Background(), observation, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := harness.endpoint.WithCurrent(context.Background(), observation, func() error { return nil }); !errors.Is(err, ErrNotCurrent) {
		t.Fatalf("second use err=%v", err)
	}
}

func TestTeslaObserveReturnedObservationIsDetached(t *testing.T) {
	harness := newHookHarness(t, segmentsAt(syntheticFragments, syntheticOffsets, 5))
	observation := harness.observe(t)
	pristine := observation.clone()
	observation.Segments[0].Bytes[0] = 'X'
	observation.Segments[0].Offsets[0] = 0
	if err := harness.endpoint.WithCurrent(context.Background(), pristine, func() error { return nil }); err != nil {
		t.Fatalf("retained evidence changed through a returned slice: %v", err)
	}
}

func TestTeslaObserveRefusalsConsumeNoBytes(t *testing.T) {
	harness := newHookHarness(t, segmentsAt(syntheticFragments, syntheticOffsets, 5))
	base := harness.request()
	invalid := map[string]func(*Request){
		"zero generation":     func(r *Request) { r.ExpectedGeneration = 0 },
		"empty clock epoch":   func(r *Request) { r.ClockEpoch = "" },
		"zero deadline":       func(r *Request) { r.Deadline = time.Time{} },
		"zero requested":      func(r *Request) { r.RequestedBytes = 0 },
		"requested over cap":  func(r *Request) { r.RequestedBytes = MaxRequestedBytes + 1 },
		"zero segment cap":    func(r *Request) { r.SegmentCap = 0 },
		"segment cap over":    func(r *Request) { r.SegmentCap = MaxSegmentCap + 1 },
		"zero segment gap":    func(r *Request) { r.SegmentGap = 0 },
		"deadline in past":    func(r *Request) { r.Deadline = harness.clock.base },
		"deadline too far":    func(r *Request) { r.Deadline = harness.clock.base.Add(MaxObservationWindow + time.Millisecond) },
		"wrong generation":    func(r *Request) { r.ExpectedGeneration = 2 },
		"nil context request": nil,
	}
	for name, mutate := range invalid {
		request := base
		ctx := context.Background()
		if mutate == nil {
			ctx = nil
		} else {
			mutate(&request)
		}
		//nolint:staticcheck // a nil context is the refusal under test
		if _, err := harness.endpoint.Observe(ctx, request); err == nil {
			t.Fatalf("%s: accepted", name)
		}
	}
	if len(harness.source.bytes) != 11 {
		t.Fatalf("refusals consumed %d bytes", 11-len(harness.source.bytes))
	}
	if _, err := harness.endpoint.Observe(context.Background(), base); err != nil {
		t.Fatal(err)
	}
	if _, err := harness.endpoint.Observe(context.Background(), base); !errors.Is(err, ErrSpent) {
		t.Fatalf("second observation on one instance: %v", err)
	}
}

type blockingSource struct {
	started chan struct{}
	closed  atomic.Bool
	done    chan struct{}
}

func newBlockingSource() *blockingSource {
	return &blockingSource{started: make(chan struct{}, 1), done: make(chan struct{})}
}

func (source *blockingSource) ReceiveByte(ctx context.Context) (byte, error) {
	select {
	case source.started <- struct{}{}:
	default:
	}
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-source.done:
		return 0, io.ErrClosedPipe
	}
}

func (source *blockingSource) Close() error {
	if source.closed.CompareAndSwap(false, true) {
		close(source.done)
	}
	return nil
}

func TestTeslaObserveBusyCloseFenceAndCancelInterruptTheReader(t *testing.T) {
	cases := []struct {
		name      string
		interrupt func(*Endpoint, context.CancelFunc)
		want      Termination
	}{
		{"close", func(e *Endpoint, _ context.CancelFunc) { _ = e.Close() }, TerminationClosed},
		{"fence", func(e *Endpoint, _ context.CancelFunc) { e.Fence() }, TerminationGenerationLost},
		{"cancel", func(_ *Endpoint, cancel context.CancelFunc) { cancel() }, TerminationCanceled},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			source := newBlockingSource()
			endpoint, err := NewEndpoint("synthetic-e", 1, source, time.Now)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = endpoint.Close() }()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			request := Request{ExpectedGeneration: 1, Deadline: time.Now().Add(10 * time.Second), ClockEpoch: "c",
				RequestedBytes: 11, SegmentCap: 8, SegmentGap: time.Millisecond}
			result := make(chan Observation, 1)
			go func() {
				observation, _ := endpoint.Observe(ctx, request)
				result <- observation
			}()
			<-source.started
			if _, err := endpoint.Observe(context.Background(), request); !errors.Is(err, ErrBusy) {
				t.Fatalf("concurrent observation: %v", err)
			}
			tc.interrupt(endpoint, cancel)
			select {
			case observation := <-result:
				if observation.Termination != tc.want {
					t.Fatalf("termination %s, want %s", observation.Termination, tc.want)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("reader was not interrupted")
			}
		})
	}
}

type failingSource struct{}

func (failingSource) ReceiveByte(context.Context) (byte, error) {
	return 0, errors.New("synthetic framing error")
}
func (failingSource) Close() error { return nil }

func TestTeslaObserveReadErrorAndLabelRules(t *testing.T) {
	endpoint, err := NewEndpoint("synthetic-e", 1, failingSource{}, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	observation, err := endpoint.Observe(context.Background(), Request{ExpectedGeneration: 1, Deadline: time.Now().Add(time.Second),
		ClockEpoch: "c", RequestedBytes: 1, SegmentCap: 1, SegmentGap: time.Millisecond})
	if err != nil || observation.Termination != TerminationReadError || observation.Cause == "" {
		t.Fatalf("err=%v observation=%+v", err, observation)
	}
	for _, label := range []string{"", "/dev/ttyS0", "Tesla", "a b", string(make([]byte, 65))} {
		if ValidLabel(label) {
			t.Fatalf("label %q accepted", label)
		}
	}
	if _, err := NewEndpoint("synthetic-e", 0, failingSource{}, time.Now); err == nil {
		t.Fatal("zero instance accepted")
	}
}
