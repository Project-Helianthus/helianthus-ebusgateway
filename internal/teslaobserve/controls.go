package teslaobserve

import (
	"context"
	"sync"
	"time"
)

// Control rows of the worksheet that an offline control supports.
const (
	ControlRowPositive = "positive_control"
	ControlRowToken    = "offline_token_cases"
	ControlRowWitness  = "offline_witness_cases"
)

// ControlResult is one synthetic offline control. It tests fail-closed
// policy only and establishes nothing about firmware behavior.
type ControlResult struct {
	Name                string      `json:"name"`
	Row                 string      `json:"row"`
	ExpectedTermination Termination `json:"expected_termination"`
	Termination         Termination `json:"termination"`
	CapturedBytes       int         `json:"captured_bytes"`
	ExpectedReady       bool        `json:"expected_offline_ready"`
	Ready               bool        `json:"offline_ready"`
	AcquireRefused      bool        `json:"acquire_refused"`
	LifecycleState      string      `json:"lifecycle_state"`
	FramesSent          int64       `json:"frames_sent"`
	Pass                bool        `json:"pass"`
}

// syntheticClock is a monotonic test clock in synthetic milliseconds. Each
// call consumes the next queued reading, then holds the last one.
type syntheticClock struct {
	mu      sync.Mutex
	base    time.Time
	current time.Duration
	queue   []time.Duration
}

func (clock *syntheticClock) now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	if len(clock.queue) > 0 {
		clock.current, clock.queue = clock.queue[0], clock.queue[1:]
	}
	return clock.base.Add(clock.current)
}

func (clock *syntheticClock) push(readings ...time.Duration) {
	clock.mu.Lock()
	clock.queue = append(clock.queue, readings...)
	clock.mu.Unlock()
}

// scriptedByte is delivered at its synthetic offset; then queues further
// clock readings, such as the return offset after the last byte.
type scriptedByte struct {
	value byte
	at    time.Duration
	then  []time.Duration
}

// scriptedSource replays constructed bytes and then blocks until ctx ends.
type scriptedSource struct {
	clock *syntheticClock
	mu    sync.Mutex
	bytes []scriptedByte
}

func (source *scriptedSource) ReceiveByte(ctx context.Context) (byte, error) {
	source.mu.Lock()
	if len(source.bytes) == 0 {
		source.mu.Unlock()
		<-ctx.Done()
		return 0, ctx.Err()
	}
	next := source.bytes[0]
	source.bytes = source.bytes[1:]
	source.mu.Unlock()
	source.clock.push(append([]time.Duration{next.at}, next.then...)...)
	return next.value, nil
}

func (*scriptedSource) Close() error { return nil }

func ms(value int) time.Duration { return time.Duration(value) * time.Millisecond }

// segmentsAt places each text fragment at its synthetic millisecond offset.
func segmentsAt(fragments []string, offsets []int, returnAt int) []scriptedByte {
	var script []scriptedByte
	for index, fragment := range fragments {
		for position := 0; position < len(fragment); position++ {
			script = append(script, scriptedByte{value: fragment[position], at: ms(offsets[index])})
		}
	}
	if returnAt > 0 && len(script) > 0 {
		script[len(script)-1].then = []time.Duration{ms(returnAt)}
	}
	return script
}

// The synthetic baseline of the public hook contract: label E, generation 1,
// deadline 50, requested count 11, segment cap 8, with the existing
// lifecycle comparator split into four fragments. Constructed offline; not a
// capture.
var (
	syntheticFragments = []string{"TE", "SLA\x00", "PA", "SS\x00"}
	syntheticOffsets   = []int{1, 2, 3, 4}
)

const (
	syntheticLabel      = "synthetic-e"
	syntheticClockEpoch = "synthetic-c"
	syntheticDeadline   = 50
	syntheticRequested  = 11
	syntheticSegmentCap = 8
)

type controlCase struct {
	name        string
	row         string
	script      []scriptedByte
	termination Termination
	ready       bool
	// mutate changes the detached observation before it is reported.
	mutate func(*Observation)
	// before runs ahead of the currentness check inside the report.
	before func(endpoint *Endpoint, cancel context.CancelFunc)
	// lifecycleNow overrides the lifecycle clock.
	lifecycleNow func() func() time.Time
	// reuse reports the same observation twice and records the second pass.
	reuse bool
}

func controlCases() []controlCase {
	baseline := func() []scriptedByte { return segmentsAt(syntheticFragments, syntheticOffsets, 5) }
	oneByteSegments := make([]string, 9)
	offsets := make([]int, 9)
	for index := range oneByteSegments {
		oneByteSegments[index] = string("TESLA\x00PAS"[index])
		offsets[index] = index + 1
	}
	return []controlCase{
		{name: "synthetic_fragmented_baseline", row: ControlRowPositive, script: baseline(), termination: TerminationCaptured, ready: true},
		{name: "missing_sequence", row: ControlRowToken, termination: TerminationDeadline},
		{name: "partial_sequence", row: ControlRowToken, script: segmentsAt([]string{"TESLA\x00"}, []int{1}, 0), termination: TerminationDeadline},
		{name: "reordered_segments", row: ControlRowToken, script: segmentsAt([]string{"PA", "SS\x00", "TE", "SLA\x00"}, syntheticOffsets, 5), termination: TerminationCaptured},
		{name: "wrong_direction", row: ControlRowToken, script: baseline(), termination: TerminationCaptured,
			mutate: func(observation *Observation) { observation.Segments[1].Direction = "transmit" }},
		{name: "leading_unrelated_byte", row: ControlRowToken, script: segmentsAt([]string{"X", "TE", "SLA\x00", "PA", "SS"}, []int{1, 2, 3, 4, 5}, 6), termination: TerminationCaptured},
		{name: "segment_exhaustion", row: ControlRowToken, script: segmentsAt(oneByteSegments, offsets, 0), termination: TerminationSegmentBound},
		{name: "late_return_at_deadline", row: ControlRowWitness, script: segmentsAt(syntheticFragments, syntheticOffsets, syntheticDeadline), termination: TerminationDeadline},
		{name: "canceled_before_commit", row: ControlRowWitness, script: baseline(), termination: TerminationCaptured,
			before: func(_ *Endpoint, cancel context.CancelFunc) { cancel() }},
		{name: "prior_generation", row: ControlRowWitness, script: baseline(), termination: TerminationCaptured,
			before: func(endpoint *Endpoint, _ context.CancelFunc) { endpoint.Fence() }},
		{name: "close_before_commit", row: ControlRowWitness, script: baseline(), termination: TerminationCaptured,
			before: func(endpoint *Endpoint, _ context.CancelFunc) { _ = endpoint.Close() }},
		{name: "expired_activation_window", row: ControlRowWitness, script: baseline(), termination: TerminationCaptured,
			lifecycleNow: expiringLifecycleClock},
		{name: "reused_observation", row: ControlRowWitness, script: baseline(), termination: TerminationCaptured, reuse: true},
	}
}

// expiringLifecycleClock returns the activation start on its first reading
// and a time past the activation window afterwards.
func expiringLifecycleClock() func() time.Time {
	base := time.Unix(0, 0)
	var mu sync.Mutex
	calls := 0
	return func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		calls++
		if calls == 1 {
			return base
		}
		return base.Add(2 * MaxObservationWindow)
	}
}

// RunControls executes every synthetic offline control. No control opens a
// device or reaches a transport.
func RunControls(ctx context.Context) []ControlResult {
	cases := controlCases()
	results := make([]ControlResult, 0, len(cases))
	for index, control := range cases {
		results = append(results, runControl(ctx, uint64(index+1), control))
	}
	return results
}

func runControl(parent context.Context, instance uint64, control controlCase) ControlResult {
	result := ControlResult{
		Name: control.name, Row: control.row,
		ExpectedTermination: control.termination, ExpectedReady: control.ready,
	}
	clock := &syntheticClock{base: time.Unix(0, 0)}
	endpoint, err := NewEndpoint(syntheticLabel, instance, &scriptedSource{clock: clock, bytes: control.script}, clock.now)
	if err != nil {
		return result
	}
	defer func() { _ = endpoint.Close() }()
	observation, err := endpoint.Observe(parent, Request{
		ExpectedGeneration: endpoint.Generation(), Deadline: clock.base.Add(ms(syntheticDeadline)),
		ClockEpoch: syntheticClockEpoch, RequestedBytes: syntheticRequested,
		SegmentCap: syntheticSegmentCap, SegmentGap: 500 * time.Microsecond,
	})
	if err != nil {
		return result
	}
	result.Termination, result.CapturedBytes = observation.Termination, observation.Coverage.CapturedBytes
	if control.mutate != nil {
		control.mutate(&observation)
	}
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	report := observationReport{endpoint: endpoint, observation: observation}
	if control.before != nil {
		report.before = func() { control.before(endpoint, cancel) }
	}
	var lifecycleNow func() time.Time
	if control.lifecycleNow != nil {
		lifecycleNow = control.lifecycleNow()
	}
	replay := replayLifecycle(ctx, report, observation.Endpoint, lifecycleNow)
	if control.reuse {
		second := replayLifecycle(ctx, report, observation.Endpoint, nil)
		second.FramesSent += replay.FramesSent
		replay = second
	}
	result.Ready, result.AcquireRefused = replay.Ready, replay.AcquireRefused
	result.LifecycleState, result.FramesSent = replay.State, replay.FramesSent
	result.Pass = result.Termination == control.termination && result.Ready == control.ready &&
		result.FramesSent == 0 && (result.Ready || result.AcquireRefused)
	return result
}
