// Package teslaobserve implements the passive activation observation run for
// the Tesla Wall Connector Gen3 HSC family. It follows the public
// helianthus-docs-modbus contracts protocols/tesla/activation-observation-hook-v1.md
// (bounded, receive-only, pre-exchange byte observation) and
// protocols/tesla/activation-evidence-worksheet.md (the rows a run fills).
//
// The run is NO_SEND by construction: the byte source has no transmit, flush,
// reset or discovery method, and the read acquisition is exercised only
// against a control endpoint that has no transport. Nothing here is a
// production activation witness, a qualification decision or proof of
// activation.
package teslaobserve

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// Termination is the bounded reason an observation ended.
type Termination string

const (
	TerminationCaptured       Termination = "captured"
	TerminationDeadline       Termination = "deadline"
	TerminationCanceled       Termination = "canceled"
	TerminationClosed         Termination = "closed"
	TerminationGenerationLost Termination = "generation_lost"
	TerminationReadError      Termination = "read_error"
	TerminationSegmentBound   Termination = "segment_bound"
)

// DirectionReceive is the only direction this observer can see: bytes
// received at the observing port. The sender stays Unknown.
const DirectionReceive = "receive"

const (
	// MaxRequestedBytes bounds the allocation of one observation window.
	MaxRequestedBytes = 4096
	// MaxSegmentCap bounds the segment metadata of one observation.
	MaxSegmentCap = 1024
	// MaxObservationWindow matches the read lifecycle's activation window bound.
	MaxObservationWindow = 60 * time.Second
	// TimingResolutionHostRead names the only timing the observer has: the
	// host monotonic clock sampled when each byte read returns.
	TimingResolutionHostRead = "host_read_return"
	// FramingUnknown is the framing/integrity result: no diagnostic is applied
	// and activation bytes are not assumed to be RTU frames.
	FramingUnknown = "unknown"
)

var (
	ErrInvalidRequest = errors.New("tesla activation observation request is invalid")
	ErrBusy           = errors.New("tesla activation observation endpoint is busy")
	ErrClosed         = errors.New("tesla activation observation endpoint is closed")
	ErrGeneration     = errors.New("tesla activation observation generation is not current")
	// ErrSpent reports a second observation on one endpoint instance. Bytes
	// queued after the first window have Unknown provenance, so a fresh
	// window needs a fresh instance.
	ErrSpent      = errors.New("tesla activation observation endpoint instance is spent")
	ErrNotCurrent = errors.New("tesla activation observation is not current")
)

// ByteSource is a receive-only byte stream owned by one endpoint instance.
// ReceiveByte must return when ctx ends or the source is closed.
type ByteSource interface {
	ReceiveByte(ctx context.Context) (byte, error)
	Close() error
}

// Request carries the explicit bounds of one observation. Deadline is an
// absolute time on the endpoint clock and never slides when bytes arrive.
type Request struct {
	ExpectedGeneration uint64
	Deadline           time.Time
	ClockEpoch         string
	RequestedBytes     int
	SegmentCap         int
	// SegmentGap splits segments on host receipt gaps. A segment boundary is
	// not a wire-frame boundary.
	SegmentGap time.Duration
}

// Segment is one ordered run of received bytes with per-byte receipt
// offsets relative to the observation start.
type Segment struct {
	Direction string          `json:"direction"`
	Bytes     []byte          `json:"bytes"`
	Offsets   []time.Duration `json:"offsets_ns"`
}

// Coverage reports what the window covered. A byte consumed after the
// segment cap is kept in Overflow rather than discarded.
type Coverage struct {
	RequestedBytes   int             `json:"requested_bytes"`
	CapturedBytes    int             `json:"captured_bytes"`
	SegmentCount     int             `json:"segment_count"`
	SegmentCap       int             `json:"segment_cap"`
	SegmentGap       time.Duration   `json:"segment_gap_ns"`
	TimingResolution string          `json:"timing_resolution"`
	Framing          string          `json:"framing_integrity"`
	Overflow         []byte          `json:"overflow_bytes,omitempty"`
	OverflowOffsets  []time.Duration `json:"overflow_offsets_ns,omitempty"`
	Limitations      []string        `json:"limitations"`
}

// Observation is a detached result. It carries no stream, buffer alias or
// capability; later changes to it cannot change the endpoint's retained copy.
type Observation struct {
	Endpoint    string        `json:"endpoint"`
	Instance    uint64        `json:"instance"`
	Generation  uint64        `json:"generation"`
	ID          uint64        `json:"observation_id"`
	ClockEpoch  string        `json:"clock_epoch"`
	Deadline    time.Duration `json:"deadline_offset_ns"`
	Return      time.Duration `json:"return_offset_ns"`
	Segments    []Segment     `json:"segments"`
	Termination Termination   `json:"termination"`
	Cause       string        `json:"cause,omitempty"`
	Coverage    Coverage      `json:"coverage"`
	deadlineAt  time.Time
}

// Window returns the captured bytes in receipt order.
func (observation Observation) Window() []byte {
	var window []byte
	for _, segment := range observation.Segments {
		window = append(window, segment.Bytes...)
	}
	return window
}

func (observation Observation) clone() Observation {
	out := observation
	out.Segments = make([]Segment, len(observation.Segments))
	for index, segment := range observation.Segments {
		out.Segments[index] = Segment{
			Direction: segment.Direction,
			Bytes:     append([]byte(nil), segment.Bytes...),
			Offsets:   append([]time.Duration(nil), segment.Offsets...),
		}
	}
	out.Coverage.Overflow = append([]byte(nil), observation.Coverage.Overflow...)
	out.Coverage.OverflowOffsets = append([]time.Duration(nil), observation.Coverage.OverflowOffsets...)
	out.Coverage.Limitations = append([]string(nil), observation.Coverage.Limitations...)
	return out
}

func sameObservation(left, right Observation) bool {
	if left.Endpoint != right.Endpoint || left.Instance != right.Instance || left.Generation != right.Generation ||
		left.ID != right.ID || left.ClockEpoch != right.ClockEpoch || left.Deadline != right.Deadline ||
		left.Return != right.Return || left.Termination != right.Termination || len(left.Segments) != len(right.Segments) {
		return false
	}
	for index := range left.Segments {
		a, b := left.Segments[index], right.Segments[index]
		if a.Direction != b.Direction || string(a.Bytes) != string(b.Bytes) || len(a.Offsets) != len(b.Offsets) {
			return false
		}
		for offset := range a.Offsets {
			if a.Offsets[offset] != b.Offsets[offset] {
				return false
			}
		}
	}
	return true
}

var observationLimitations = []string{
	"sender attribution is Unknown: only the receive side of the observing port is visible",
	"bytes buffered by the host before the port was opened have Unknown provenance",
	"receipt offsets are host read-return times; on-wire timing is Unknown",
	"segment boundaries are host receipt gaps, not wire frames",
	"no framing or integrity diagnostic is applied",
}

// Endpoint owns one receive-only source instance and its single-flight
// observation slot. Close, Fence and WithCurrent share one mutex, so a
// readiness commit made inside WithCurrent cannot be overtaken by either.
type Endpoint struct {
	label    string
	instance uint64
	now      func() time.Time

	mu         sync.Mutex
	source     ByteSource
	generation uint64
	closed     bool
	busy       bool
	spent      bool
	cancel     context.CancelFunc
	issued     *Observation
	usable     bool
	closeErr   error
}

// NewEndpoint takes ownership of source. label is a public endpoint label and
// must never be a local path; instance is an opaque local identity.
func NewEndpoint(label string, instance uint64, source ByteSource, now func() time.Time) (*Endpoint, error) {
	if !ValidLabel(label) || instance == 0 || source == nil || now == nil {
		return nil, ErrInvalidRequest
	}
	return &Endpoint{label: label, instance: instance, now: now, source: source, generation: 1}, nil
}

// ValidLabel accepts 1..64 characters of [a-z0-9._-]; a path separator can
// never appear.
func ValidLabel(label string) bool {
	if label == "" || len(label) > 64 {
		return false
	}
	for _, r := range label {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '.', r == '_', r == '-':
		default:
			return false
		}
	}
	return true
}

// Generation returns the current unfenced generation.
func (endpoint *Endpoint) Generation() uint64 {
	endpoint.mu.Lock()
	defer endpoint.mu.Unlock()
	return endpoint.generation
}

func validRequest(request Request) bool {
	return request.ExpectedGeneration != 0 && request.ClockEpoch != "" && !request.Deadline.IsZero() &&
		request.RequestedBytes > 0 && request.RequestedBytes <= MaxRequestedBytes &&
		request.SegmentCap > 0 && request.SegmentCap <= MaxSegmentCap &&
		request.SegmentGap > 0 && request.SegmentGap <= MaxObservationWindow
}

// Observe captures one bounded window. Invalid input, a busy or spent
// endpoint and a stale generation fail before any byte is consumed.
func (endpoint *Endpoint) Observe(ctx context.Context, request Request) (Observation, error) {
	if ctx == nil || !validRequest(request) {
		return Observation{}, ErrInvalidRequest
	}
	endpoint.mu.Lock()
	switch {
	case endpoint.closed:
		endpoint.mu.Unlock()
		return Observation{}, ErrClosed
	case endpoint.busy:
		endpoint.mu.Unlock()
		return Observation{}, ErrBusy
	case endpoint.spent:
		endpoint.mu.Unlock()
		return Observation{}, ErrSpent
	case request.ExpectedGeneration != endpoint.generation:
		endpoint.mu.Unlock()
		return Observation{}, ErrGeneration
	}
	start := endpoint.now()
	budget := request.Deadline.Sub(start)
	if budget <= 0 || budget > MaxObservationWindow {
		endpoint.mu.Unlock()
		return Observation{}, ErrInvalidRequest
	}
	if err := ctx.Err(); err != nil {
		endpoint.mu.Unlock()
		return Observation{}, err
	}
	readCtx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	endpoint.busy, endpoint.spent, endpoint.cancel = true, true, cancel
	generation, source := endpoint.generation, endpoint.source
	endpoint.mu.Unlock()

	var (
		segments []Segment
		captured int
		last     time.Duration
		overflow []byte
		overAt   []time.Duration
		readErr  error
		bounded  bool
	)
	for captured < request.RequestedBytes {
		value, err := source.ReceiveByte(readCtx)
		at := endpoint.offset(start)
		if err != nil {
			readErr = err
			break
		}
		if len(segments) == 0 || at-last > request.SegmentGap {
			if len(segments) == request.SegmentCap {
				overflow, overAt, bounded = []byte{value}, []time.Duration{at}, true
				break
			}
			segments = append(segments, Segment{Direction: DirectionReceive})
		}
		current := &segments[len(segments)-1]
		current.Bytes = append(current.Bytes, value)
		current.Offsets = append(current.Offsets, at)
		captured++
		last = at
	}
	returned := endpoint.now()

	endpoint.mu.Lock()
	defer endpoint.mu.Unlock()
	endpoint.busy, endpoint.cancel = false, nil
	observation := Observation{
		Endpoint: endpoint.label, Instance: endpoint.instance, Generation: generation,
		ID: 1, ClockEpoch: request.ClockEpoch, Deadline: budget,
		Return: nonNegative(returned.Sub(start)), Segments: segments, deadlineAt: request.Deadline,
		Coverage: Coverage{
			RequestedBytes: request.RequestedBytes, CapturedBytes: captured, SegmentCount: len(segments),
			SegmentCap: request.SegmentCap, SegmentGap: request.SegmentGap,
			TimingResolution: TimingResolutionHostRead, Framing: FramingUnknown,
			Overflow: overflow, OverflowOffsets: overAt,
			Limitations: append([]string(nil), observationLimitations...),
		},
	}
	switch {
	case endpoint.closed:
		observation.Termination = TerminationClosed
	case endpoint.generation != generation:
		observation.Termination = TerminationGenerationLost
	case ctx.Err() != nil:
		observation.Termination = TerminationCanceled
	case !returned.Before(request.Deadline) || errors.Is(readErr, context.DeadlineExceeded):
		observation.Termination = TerminationDeadline
	case bounded:
		observation.Termination = TerminationSegmentBound
	case readErr != nil:
		observation.Termination = TerminationReadError
	default:
		observation.Termination = TerminationCaptured
	}
	if readErr != nil && observation.Termination == TerminationReadError {
		observation.Cause = boundedCause(readErr)
	}
	retained := observation.clone()
	retained.deadlineAt = request.Deadline
	endpoint.issued = &retained
	endpoint.usable = observation.Termination == TerminationCaptured
	return observation.clone(), nil
}

func (endpoint *Endpoint) offset(start time.Time) time.Duration {
	return nonNegative(endpoint.now().Sub(start))
}

func nonNegative(value time.Duration) time.Duration {
	if value < 0 {
		return 0
	}
	return value
}

// maxCauseBytes bounds every recorded cause. A persisted cause is checked
// against it in runes, which never exceeds the byte count.
const maxCauseBytes = 160

// boundedCause is never empty, so a recorded failure always carries a cause.
func boundedCause(err error) string {
	text := err.Error()
	if text == "" {
		return "error without a message"
	}
	if len(text) > maxCauseBytes {
		text = text[:maxCauseBytes]
	}
	return text
}

// Fence advances the generation, invalidates every issued observation and
// interrupts an active reader before a successor can consume bytes.
func (endpoint *Endpoint) Fence() uint64 {
	endpoint.mu.Lock()
	defer endpoint.mu.Unlock()
	endpoint.generation++
	endpoint.usable = false
	if endpoint.cancel != nil {
		endpoint.cancel()
	}
	return endpoint.generation
}

// Close is terminal. It invalidates issued observations and closes the
// source, which interrupts a blocked read.
func (endpoint *Endpoint) Close() error {
	endpoint.mu.Lock()
	defer endpoint.mu.Unlock()
	if endpoint.closed {
		return endpoint.closeErr
	}
	endpoint.closed, endpoint.usable = true, false
	if endpoint.cancel != nil {
		endpoint.cancel()
	}
	endpoint.closeErr = endpoint.source.Close()
	return endpoint.closeErr
}

// WithCurrent rechecks observation against the owner and runs commit while
// holding the endpoint mutex. Endpoint label, instance, generation,
// observation identity, clock epoch, the exact detached bytes and offsets,
// the deadline and ctx must all still hold. An observation is usable once.
func (endpoint *Endpoint) WithCurrent(ctx context.Context, observation Observation, commit func() error) error {
	if ctx == nil || commit == nil {
		return ErrInvalidRequest
	}
	endpoint.mu.Lock()
	defer endpoint.mu.Unlock()
	issued := endpoint.issued
	switch {
	case endpoint.closed:
		return ErrClosed
	case issued == nil || !endpoint.usable:
		return ErrNotCurrent
	case observation.Endpoint != endpoint.label || observation.Instance != endpoint.instance ||
		observation.Generation != endpoint.generation:
		return fmt.Errorf("%w: identity", ErrNotCurrent)
	case !sameObservation(observation, *issued):
		return fmt.Errorf("%w: evidence differs from the issued observation", ErrNotCurrent)
	case ctx.Err() != nil:
		endpoint.usable = false
		return fmt.Errorf("%w: %w", ErrNotCurrent, ctx.Err())
	case !endpoint.now().Before(issued.deadlineAt):
		endpoint.usable = false
		return fmt.Errorf("%w: deadline", ErrNotCurrent)
	}
	endpoint.usable = false
	return commit()
}
