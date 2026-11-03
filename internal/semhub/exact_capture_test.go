package semhub

import (
	"context"
	"errors"
	"maps"
	"reflect"
	"strconv"
	"testing"
	"time"

	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
)

const exactOtherEpoch semreg.ClockEpochID = "clock:exact-other"

var exactEpoch = testMono(0).ClockEpochID

const exactSecond = int64(1_000_000_000)

func exactPoint(epoch semreg.ClockEpochID, ns int64) semreg.MonotonicPoint {
	return semreg.MonotonicPoint{ClockEpochID: epoch, Nanoseconds: semreg.Uint64(strconv.FormatInt(ns, 10))}
}

func exactSample(wall semreg.TimePoint, points ...semreg.MonotonicPoint) CaptureClockSample {
	sample := CaptureClockSample{Wall: wall, Monotonic: map[semreg.ClockEpochID]semreg.MonotonicPoint{}}
	for _, point := range points {
		sample.Monotonic[point.ClockEpochID] = point
	}
	return sample
}

// exactSampler counts calls and proves each call runs under the hub read lock.
type exactSampler struct {
	t      *testing.T
	h      *Hub
	calls  int
	sample CaptureClockSample
	err    error
}

func (s *exactSampler) fn() func() (CaptureClockSample, error) {
	return func() (CaptureClockSample, error) {
		s.calls++
		if s.h != nil && s.h.mu.TryLock() {
			s.h.mu.Unlock()
			s.t.Error("clock sample taken outside the hub read lock")
		}
		return s.sample, s.err
	}
}

func exactCapture(t *testing.T, h *Hub, sample CaptureClockSample, ids ...semreg.AssetID) (CoherentCapture, int) {
	t.Helper()
	sampler := &exactSampler{t: t, h: h, sample: sample}
	capture, err := h.CaptureExact(context.Background(), ids, sampler.fn())
	if err != nil {
		t.Fatal(err)
	}
	return capture, sampler.calls
}

// exactOracle evaluates the ungated audit snapshot directly with SemReg.
func exactOracle(t *testing.T, h *Hub, id semreg.AssetID, wall semreg.TimePoint, point semreg.MonotonicPoint) semreg.EvaluationView {
	t.Helper()
	audit, err := h.Inspect(id)
	if err != nil || !audit.Present {
		t.Fatalf("audit %s: %v", id, err)
	}
	view, err := semreg.EvaluateSnapshot(audit.Snapshot, semreg.EvaluationContext{EvaluatedAt: wall, EvaluateMonotonic: point})
	if err != nil {
		t.Fatal(err)
	}
	return view
}

func assertExactOracle(t *testing.T, got CapturedAsset, want semreg.EvaluationView) {
	t.Helper()
	if !got.PublicAdmitted || got.Snapshot == nil || got.Evaluation == nil {
		t.Fatalf("not admitted: %+v", got)
	}
	if !reflect.DeepEqual(*got.Evaluation, want) {
		t.Fatalf("evaluation differs from direct oracle:\n got=%+v\nwant=%+v", *got.Evaluation, want)
	}
	digest, err := got.Evaluation.EvaluationDigestValue()
	if err != nil || digest != got.Evaluation.EvaluationDigest {
		t.Fatalf("evaluation digest %v/%v", digest, err)
	}
}

// publishOnClock publishes the synthetic family with receipt and evaluation
// coordinates in an arbitrary clock domain.
func publishOnClock(t *testing.T, h *Hub, handle *Handle, id semreg.AssetID, family string, value, wall int64, point semreg.MonotonicPoint) {
	t.Helper()
	tx, err := h.Begin(handle, id)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Abort()
	batch := testBatch(t, tx, family, value, wall)
	batch.FactUpserts[0].Times.ReceiptMonotonic, batch.FactUpserts[0].Times.EvaluateMonotonic = point, point
	if batch.BatchDigest, err = batch.ComputedDigest(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := tx.Apply(batch, point); err != nil {
		t.Fatal(err)
	}
	if outcome, err := tx.Commit(nil); err != nil || outcome != Accepted {
		t.Fatalf("publish %s=%v/%v", id, outcome, err)
	}
}

func inspectAll(t *testing.T, h *Hub, ids ...semreg.AssetID) []InspectView {
	t.Helper()
	views := make([]InspectView, 0, len(ids))
	for _, id := range ids {
		view, err := h.Inspect(id)
		if err != nil {
			t.Fatal(err)
		}
		views = append(views, view)
	}
	return views
}

// assetState reads the stored publication directly; synthetic control gates
// lack the operation that Inspect reports.
type assetState struct {
	Snapshot  semreg.Snapshot
	Canonical []byte
	Present   bool
	HubSeq    uint64
	Gate      *controlAsset
	Source    []any
}

func readAssetState(h *Hub, id semreg.AssetID) assetState {
	h.mu.RLock()
	defer h.mu.RUnlock()
	target := h.assets[id]
	snapshot, canonical, present := target.kernel.Current()
	src := h.sources[target.source]
	return assetState{Snapshot: snapshot, Canonical: canonical, Present: present, HubSeq: target.hubSeq, Gate: target.gate, Source: []any{src.id, src.admitted, src.epoch, src.generation, src.live, src.fenced, maps.Clone(src.retired)}}
}

func exactSingleAsset(t *testing.T, at int64) (*Hub, semreg.AssetID) {
	t.Helper()
	h := newTestHub(t)
	id := semreg.AssetID("asset:exact")
	mustDeclare(t, h, id, "source:exact", "exact", nil)
	handle := admitLegacy(t, h, "source:exact", "exact", "epoch:exact", 1)
	mustPublish(t, h, handle, id, "exact", 1, at)
	return h, id
}

// Wall moves forward and backward, including before the snapshot's wall point,
// while the same-epoch tick stays fixed. A wall-derived tick (the legacy route)
// would fail the backward cases and age the forward ones.
func TestExactCaptureWallMovesWithFixedTick(t *testing.T) {
	h, id := exactSingleAsset(t, 7)
	before := inspectAll(t, h, id)
	tick := testMono(37)
	utc := testWall(10_000)
	utc.ClockID = "clock.utc"
	for _, wall := range []semreg.TimePoint{testWall(-1_000), testWall(6), testWall(7), testWall(37), testWall(10_000), utc} {
		capture, calls := exactCapture(t, h, exactSample(wall, tick), id)
		got := capture.Assets[0]
		assertExactOracle(t, got, exactOracle(t, h, id, wall, tick))
		if calls != 1 || capture.EvaluatedAt != wall || got.Evaluation.Context != (semreg.EvaluationContext{EvaluatedAt: wall, EvaluateMonotonic: tick}) {
			t.Fatalf("wall %s: calls=%d context=%+v", wall.UnixNanoseconds, calls, got.Evaluation.Context)
		}
		if fact := got.Evaluation.Facts[0]; fact.Freshness != semreg.FreshnessFresh || fact.EffectiveAvailability != semreg.AvailabilityAvailable {
			t.Fatalf("wall %s aged same-epoch data: %+v", wall.UnixNanoseconds, fact)
		}
	}
	// The legacy route keeps its wall-derived aging: 400 wall seconds expire
	// the fact that the exact route still evaluates as fresh at tick 37.
	legacy, err := h.Capture(context.Background(), []semreg.AssetID{id}, func() time.Time { return time.Unix(1800000407, 0) })
	if err != nil || legacy.Assets[0].Evaluation.Facts[0].Freshness != semreg.FreshnessExpired {
		t.Fatalf("legacy route changed: %+v/%v", legacy.Assets[0].Evaluation, err)
	}
	if after := inspectAll(t, h, id); !reflect.DeepEqual(before, after) {
		t.Fatal("exact capture mutated hub")
	}
}

// The fixture policy is fresh below 60 s and retained below 300 s of
// same-epoch elapsed ticks. The wall point stays fixed and earlier than every
// tick so no wall elapsed time can explain the result.
func TestExactCaptureMonotonicThresholds(t *testing.T) {
	h, id := exactSingleAsset(t, 7)
	wall := testWall(7)
	base := 7 * exactSecond
	for _, tc := range []struct {
		elapsed      int64
		freshness    semreg.Freshness
		availability semreg.Availability
	}{
		{0, semreg.FreshnessFresh, semreg.AvailabilityAvailable},
		{59 * exactSecond, semreg.FreshnessFresh, semreg.AvailabilityAvailable},
		{60*exactSecond - 1, semreg.FreshnessFresh, semreg.AvailabilityAvailable},
		{60 * exactSecond, semreg.FreshnessStale, semreg.AvailabilityDegraded},
		{299 * exactSecond, semreg.FreshnessStale, semreg.AvailabilityDegraded},
		{300*exactSecond - 1, semreg.FreshnessStale, semreg.AvailabilityDegraded},
		{300 * exactSecond, semreg.FreshnessExpired, semreg.AvailabilityUnavailable},
	} {
		tick := exactPoint(exactEpoch, base+tc.elapsed)
		capture, _ := exactCapture(t, h, exactSample(wall, tick), id)
		got := capture.Assets[0]
		assertExactOracle(t, got, exactOracle(t, h, id, wall, tick))
		if fact := got.Evaluation.Facts[0]; fact.Freshness != tc.freshness || fact.EffectiveAvailability != tc.availability || got.Evaluation.Context.EvaluateMonotonic != tick {
			t.Fatalf("elapsed %dns: %+v context=%+v", tc.elapsed, fact, got.Evaluation.Context)
		}
	}
}

// A retained observation leaves the evaluation exactly at its expiry tick and
// stays in the stored snapshot.
func TestExactCaptureRetainedExpiryEdge(t *testing.T) {
	h := newTestHub(t)
	id := semreg.AssetID("asset:retained")
	mustDeclare(t, h, id, "source:retained", "retained", nil)
	handle := admitLegacy(t, h, "source:retained", "retained", "epoch:retained", 1)
	mustPublish(t, h, handle, id, "retained", 1, 1)
	if _, err := h.Fence(testFence("source:retained", "epoch:retained", 1, 2)); err != nil {
		t.Fatal(err)
	}
	next, err := h.Activate(handle, 2)
	if err != nil {
		t.Fatal(err)
	}
	mustPublish(t, h, next, id, "retained", 2, 3)
	before := inspectAll(t, h, id)
	if len(before[0].Snapshot.Retained) != 1 {
		t.Fatal("fixture lacks a retained observation")
	}
	wall := testWall(3)
	for _, tc := range []struct {
		tick     int64
		retained int
	}{{301*exactSecond - 1, 1}, {301 * exactSecond, 0}} {
		point := exactPoint(exactEpoch, tc.tick)
		capture, _ := exactCapture(t, h, exactSample(wall, point), id)
		assertExactOracle(t, capture.Assets[0], exactOracle(t, h, id, wall, point))
		if got := capture.Assets[0].Evaluation; len(got.Retained) != tc.retained || got.Facts[0].Freshness != semreg.FreshnessStale {
			t.Fatalf("tick %d: retained=%d facts=%+v", tc.tick, len(got.Retained), got.Facts)
		}
	}
	if after := inspectAll(t, h, id); !reflect.DeepEqual(before, after) || len(after[0].Snapshot.Retained) != 1 {
		t.Fatal("evaluation expiry changed stored retention")
	}
}

// Two assets in one epoch with different recorded ticks share the single
// sampled coordinate; per-asset aging from wall time would give each its own.
func TestExactCaptureSharedEpochUsesOneCoordinate(t *testing.T) {
	h := newTestHub(t)
	first, second := semreg.AssetID("asset:first"), semreg.AssetID("asset:second")
	for _, id := range []semreg.AssetID{first, second} {
		mustDeclare(t, h, id, "source:shared", "shared", nil)
	}
	handle := admitLegacy(t, h, "source:shared", "shared", "epoch:shared", 1)
	mustPublish(t, h, handle, first, "shared", 1, 7)
	mustPublish(t, h, handle, second, "shared", 2, 20)
	wall, tick := testWall(20), testMono(70)
	capture, calls := exactCapture(t, h, exactSample(wall, tick), first, second)
	if calls != 1 {
		t.Fatalf("sampled %d times", calls)
	}
	for index, id := range []semreg.AssetID{first, second} {
		assertExactOracle(t, capture.Assets[index], exactOracle(t, h, id, wall, tick))
		if capture.Assets[index].Evaluation.Context.EvaluateMonotonic != tick {
			t.Fatalf("%s used another coordinate", id)
		}
	}
	if capture.Assets[0].Evaluation.Facts[0].Freshness != semreg.FreshnessStale || capture.Assets[1].Evaluation.Facts[0].Freshness != semreg.FreshnessFresh {
		t.Fatal("shared coordinate did not age each receipt independently")
	}
}

// Distinct epochs have unrelated tick origins; each admitted snapshot needs the
// coordinate of its own epoch, in either selection order.
func TestExactCaptureDistinctEpochs(t *testing.T) {
	h := newTestHub(t)
	a, b := semreg.AssetID("asset:epoch-a"), semreg.AssetID("asset:epoch-b")
	mustDeclare(t, h, a, "source:epoch-a", "epocha", nil)
	mustDeclare(t, h, b, "source:epoch-b", "epochb", nil)
	mustPublish(t, h, admitLegacy(t, h, "source:epoch-a", "epocha", "epoch:a", 1), a, "epocha", 1, 7)
	publishOnClock(t, h, admitLegacy(t, h, "source:epoch-b", "epochb", "epoch:b", 1), b, "epochb", 2, 7, exactPoint(exactOtherEpoch, 1_000_000*exactSecond))
	before := inspectAll(t, h, a, b)
	wall := testWall(7)
	pointA, pointB := testMono(30), exactPoint(exactOtherEpoch, 1_000_120*exactSecond)
	for _, ids := range [][]semreg.AssetID{{a, b}, {b, a}} {
		capture, calls := exactCapture(t, h, exactSample(wall, pointA, pointB), ids...)
		if calls != 1 {
			t.Fatalf("sampled %d times", calls)
		}
		for index, id := range ids {
			point := map[semreg.AssetID]semreg.MonotonicPoint{a: pointA, b: pointB}[id]
			assertExactOracle(t, capture.Assets[index], exactOracle(t, h, id, wall, point))
		}
	}
	got, _ := exactCapture(t, h, exactSample(wall, pointA, pointB), a, b)
	if got.Assets[0].Evaluation.Facts[0].Freshness != semreg.FreshnessFresh || got.Assets[1].Evaluation.Facts[0].Freshness != semreg.FreshnessStale {
		t.Fatalf("epoch coordinates crossed: %+v / %+v", got.Assets[0].Evaluation.Facts, got.Assets[1].Evaluation.Facts)
	}
	for name, sample := range map[string]CaptureClockSample{
		"missing a":      exactSample(wall, pointB),
		"missing b":      exactSample(wall, pointA),
		"b under a key":  {Wall: wall, Monotonic: map[semreg.ClockEpochID]semreg.MonotonicPoint{exactEpoch: pointB, exactOtherEpoch: pointB}},
		"b regresses":    exactSample(wall, pointA, exactPoint(exactOtherEpoch, 1_000_000*exactSecond-1)),
		"a origin for b": exactSample(wall, pointA, exactPoint(exactOtherEpoch, 30*exactSecond)),
	} {
		sampler := &exactSampler{t: t, h: h, sample: sample}
		if _, err := h.CaptureExact(context.Background(), []semreg.AssetID{a, b}, sampler.fn()); errorID(err) != semreg.InvalidTime || sampler.calls != 1 {
			t.Fatalf("%s: err=%v calls=%d", name, err, sampler.calls)
		}
	}
	if after := inspectAll(t, h, a, b); !reflect.DeepEqual(before, after) {
		t.Fatal("refused capture mutated hub")
	}
}

func TestExactCaptureRefusesInvalidSamples(t *testing.T) {
	h, id := exactSingleAsset(t, 7)
	before := inspectAll(t, h, id)
	wall, tick := testWall(7), testMono(30)
	with := func(extra map[semreg.ClockEpochID]semreg.MonotonicPoint) CaptureClockSample {
		sample := exactSample(wall, tick)
		for key, point := range extra {
			sample.Monotonic[key] = point
		}
		return sample
	}
	badWall := func(mutate func(*semreg.TimePoint)) CaptureClockSample {
		sample := exactSample(wall, tick)
		mutate(&sample.Wall)
		return sample
	}
	oversized := exactSample(wall, tick)
	for index := 0; len(oversized.Monotonic) <= maxCaptureClockCoordinates; index++ {
		epoch := semreg.ClockEpochID("clock:unused-" + strconv.Itoa(index))
		oversized.Monotonic[epoch] = exactPoint(epoch, 1)
	}
	cases := map[string]CaptureClockSample{
		"nil map":               {Wall: wall},
		"empty map":             {Wall: wall, Monotonic: map[semreg.ClockEpochID]semreg.MonotonicPoint{}},
		"other epoch only":      exactSample(wall, exactPoint(exactOtherEpoch, 30*exactSecond)),
		"key mismatch":          {Wall: wall, Monotonic: map[semreg.ClockEpochID]semreg.MonotonicPoint{exactEpoch: exactPoint(exactOtherEpoch, 30*exactSecond)}},
		"zero point":            {Wall: wall, Monotonic: map[semreg.ClockEpochID]semreg.MonotonicPoint{exactEpoch: {}}},
		"malformed tick":        {Wall: wall, Monotonic: map[semreg.ClockEpochID]semreg.MonotonicPoint{exactEpoch: {ClockEpochID: exactEpoch, Nanoseconds: "abc"}}},
		"negative tick":         {Wall: wall, Monotonic: map[semreg.ClockEpochID]semreg.MonotonicPoint{exactEpoch: {ClockEpochID: exactEpoch, Nanoseconds: "-1"}}},
		"empty tick":            {Wall: wall, Monotonic: map[semreg.ClockEpochID]semreg.MonotonicPoint{exactEpoch: {ClockEpochID: exactEpoch}}},
		"regressing tick":       exactSample(wall, exactPoint(exactEpoch, 7*exactSecond-1)),
		"unused key mismatch":   with(map[semreg.ClockEpochID]semreg.MonotonicPoint{"clock:unused": exactPoint("clock:other-unused", 1)}),
		"unused malformed":      with(map[semreg.ClockEpochID]semreg.MonotonicPoint{"clock:unused": {ClockEpochID: "clock:unused", Nanoseconds: "x"}}),
		"invalid key":           with(map[semreg.ClockEpochID]semreg.MonotonicPoint{"": {Nanoseconds: "1"}}),
		"oversized":             oversized,
		"wall nanoseconds":      badWall(func(p *semreg.TimePoint) { p.UnixNanoseconds = "x" }),
		"wall clock":            badWall(func(p *semreg.TimePoint) { p.ClockID = "" }),
		"wall uncertainty":      badWall(func(p *semreg.TimePoint) { p.UncertaintyNS = "-1" }),
		"zero wall":             {Monotonic: exactSample(wall, tick).Monotonic},
		"unknown-only bad wall": badWall(func(p *semreg.TimePoint) { p.ClockID = "" }),
	}
	for name, sample := range cases {
		ids := []semreg.AssetID{id}
		if name == "unknown-only bad wall" {
			ids = []semreg.AssetID{"asset:unknown"}
		}
		sampler := &exactSampler{t: t, h: h, sample: sample}
		if capture, err := h.CaptureExact(context.Background(), ids, sampler.fn()); errorID(err) != semreg.InvalidTime || sampler.calls != 1 || capture.Assets != nil {
			t.Fatalf("%s: err=%v calls=%d capture=%+v", name, err, sampler.calls, capture)
		}
	}
	// Exactly the bound, every entry well formed, is a valid control.
	bounded := exactSample(wall, tick)
	for index := 0; len(bounded.Monotonic) < maxCaptureClockCoordinates; index++ {
		epoch := semreg.ClockEpochID("clock:unused-" + strconv.Itoa(index))
		bounded.Monotonic[epoch] = exactPoint(epoch, 1)
	}
	capture, _ := exactCapture(t, h, bounded, id)
	assertExactOracle(t, capture.Assets[0], exactOracle(t, h, id, wall, tick))
	if after := inspectAll(t, h, id); !reflect.DeepEqual(before, after) {
		t.Fatal("refused capture mutated hub")
	}
}

func TestExactCaptureSamplerAndSelection(t *testing.T) {
	h, id := exactSingleAsset(t, 7)
	before := inspectAll(t, h, id)
	valid := exactSample(testWall(7), testMono(30))
	var absent *Hub
	sampler := &exactSampler{t: t, sample: valid}
	if _, err := absent.CaptureExact(context.Background(), []semreg.AssetID{id}, sampler.fn()); !errors.Is(err, ErrCaptureUnavailable) || sampler.calls != 0 {
		t.Fatalf("nil hub: %v calls=%d", err, sampler.calls)
	}
	if _, err := h.CaptureExact(context.Background(), []semreg.AssetID{id}, nil); errorID(err) != semreg.InvalidTime {
		t.Fatalf("nil sampler: %v", err)
	}
	failure := errors.New("sampler failed")
	sampler = &exactSampler{t: t, h: h, sample: valid, err: failure}
	if _, err := h.CaptureExact(context.Background(), []semreg.AssetID{id}, sampler.fn()); !errors.Is(err, failure) || sampler.calls != 1 {
		t.Fatalf("sampler error: %v calls=%d", err, sampler.calls)
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	sampler = &exactSampler{t: t, h: h, sample: valid}
	if _, err := h.CaptureExact(cancelled, []semreg.AssetID{id}, sampler.fn()); !errors.Is(err, context.Canceled) || sampler.calls != 0 {
		t.Fatalf("cancelled: %v calls=%d", err, sampler.calls)
	}
	tooMany := make([]semreg.AssetID, 33)
	for index := range tooMany {
		tooMany[index] = semreg.AssetID("asset:many-" + strconv.Itoa(index))
	}
	for _, ids := range [][]semreg.AssetID{nil, {}, tooMany} {
		sampler = &exactSampler{t: t, h: h, sample: valid}
		if _, err := h.CaptureExact(context.Background(), ids, sampler.fn()); errorID(err) != semreg.BoundsExceeded || sampler.calls != 0 {
			t.Fatalf("bounds %d: %v calls=%d", len(ids), err, sampler.calls)
		}
	}
	for _, ids := range [][]semreg.AssetID{{id, id}, {""}} {
		sampler = &exactSampler{t: t, h: h, sample: valid}
		if _, err := h.CaptureExact(context.Background(), ids, sampler.fn()); errorID(err) != semreg.InvalidValue || sampler.calls != 1 {
			t.Fatalf("selection %v: %v calls=%d", ids, err, sampler.calls)
		}
	}
	h.mu.Lock()
	h.restoring = true
	h.mu.Unlock()
	sampler = &exactSampler{t: t, h: h, sample: valid}
	if _, err := h.CaptureExact(context.Background(), []semreg.AssetID{id}, sampler.fn()); !errors.Is(err, ErrCaptureUnavailable) || sampler.calls != 0 {
		t.Fatalf("restoring: %v calls=%d", err, sampler.calls)
	}
	h.mu.Lock()
	h.restoring = false
	h.mu.Unlock()
	// Cancellation between selected assets returns no partial capture.
	ctx, cancelRead := context.WithCancel(context.Background())
	h.faults = func(stage string, asset semreg.AssetID) {
		if stage == "capture-read" && asset == id {
			cancelRead()
		}
	}
	sampler = &exactSampler{t: t, h: h, sample: valid}
	capture, err := h.CaptureExact(ctx, []semreg.AssetID{id, "asset:unknown"}, sampler.fn())
	h.faults = nil
	if !errors.Is(err, context.Canceled) || capture.Assets != nil || sampler.calls != 1 {
		t.Fatalf("mid-capture cancel: %v %+v calls=%d", err, capture, sampler.calls)
	}
	if after := inspectAll(t, h, id); !reflect.DeepEqual(before, after) {
		t.Fatal("refused capture mutated hub")
	}
}

// Each admission dimension is varied alone after a valid control. A withheld
// snapshot is never evaluated, so it needs no coordinate either.
func TestExactCaptureAdmissionGates(t *testing.T) {
	type gateCase struct {
		reason string
		mutate func(*testing.T, *Hub, semreg.AssetID, *Handle)
	}
	direct := func(change func(*Hub, semreg.AssetID)) func(*testing.T, *Hub, semreg.AssetID, *Handle) {
		return func(_ *testing.T, h *Hub, id semreg.AssetID, _ *Handle) {
			h.mu.Lock()
			change(h, id)
			h.mu.Unlock()
		}
	}
	privateRetained := func(t *testing.T, h *Hub, id semreg.AssetID, handle *Handle) {
		if _, err := h.Fence(testFence("source:gate", "epoch:gate", 1, 2)); err != nil {
			t.Fatal(err)
		}
		next, err := h.Activate(handle, 2)
		if err != nil {
			t.Fatal(err)
		}
		mustPublish(t, h, next, id, "gate", 2, 3)
	}
	cases := map[string]gateCase{
		"source fenced": {"semantic.read.reason.source_unavailable", func(t *testing.T, h *Hub, _ semreg.AssetID, _ *Handle) {
			if _, err := h.Fence(testFence("source:gate", "epoch:gate", 1, 2)); err != nil {
				t.Fatal(err)
			}
		}},
		"source not admitted": {"semantic.read.reason.source_unavailable", direct(func(h *Hub, _ semreg.AssetID) { h.sources["source:gate"].admitted = false })},
		"source not live":     {"semantic.read.reason.source_unavailable", direct(func(h *Hub, _ semreg.AssetID) { h.sources["source:gate"].live = false })},
		"source epoch retired": {"semantic.read.reason.source_unavailable", direct(func(h *Hub, _ semreg.AssetID) {
			h.sources["source:gate"].retired = map[semreg.SourceEpochID]bool{"epoch:gate": true}
		})},
		"restore failed": {"semantic.read.reason.publication_withheld", direct(func(h *Hub, id semreg.AssetID) { h.assets[id].restoreFailed = true })},
		"pending semantic": {"semantic.read.reason.publication_withheld", direct(func(h *Hub, id semreg.AssetID) {
			h.assets[id].gate = &controlAsset{semantic: SemanticUnbuilt, reprojection: ReprojectionSettled}
		})},
		"pending reprojection": {"semantic.read.reason.publication_withheld", direct(func(h *Hub, id semreg.AssetID) {
			h.assets[id].gate = &controlAsset{semantic: SemanticAccepted, reprojection: ReprojectionUnsettled}
		})},
		"current source":     {"semantic.read.reason.publication_withheld", direct(func(h *Hub, _ semreg.AssetID) { h.sources["source:gate"].id = "source:other" })},
		"current epoch":      {"semantic.read.reason.publication_withheld", direct(func(h *Hub, _ semreg.AssetID) { h.sources["source:gate"].epoch = "epoch:other" })},
		"current generation": {"semantic.read.reason.publication_withheld", direct(func(h *Hub, _ semreg.AssetID) { h.sources["source:gate"].generation++ })},
		"private retained":   {"semantic.read.reason.publication_withheld", privateRetained},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			h := newTestHub(t)
			id := semreg.AssetID("asset:gate")
			mustDeclare(t, h, id, "source:gate", "gate", nil)
			handle := admitLegacy(t, h, "source:gate", "gate", "epoch:gate", 1)
			tx, err := h.Begin(handle, id)
			if err != nil {
				t.Fatal(err)
			}
			batch := testBatch(t, tx, "gate", 1, 1)
			if name == "private retained" {
				batch.FactUpserts[0].Evidence[0].Access, batch.FactUpserts[0].Evidence[0].Redaction = semreg.EvidenceAccessAuthorized, semreg.RedactionNone
				if batch.BatchDigest, err = batch.ComputedDigest(); err != nil {
					t.Fatal(err)
				}
			}
			if _, _, err := tx.Apply(batch, testMono(1)); err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Commit(nil); err != nil {
				t.Fatal(err)
			}
			wall, tick := testWall(3), testMono(30)
			if name != "private retained" {
				control, _ := exactCapture(t, h, exactSample(wall, tick), id)
				assertExactOracle(t, control.Assets[0], exactOracle(t, h, id, wall, tick))
			}
			tc.mutate(t, h, id, handle)
			before := readAssetState(h, id)
			for _, sample := range []CaptureClockSample{exactSample(wall, tick), {Wall: wall}} {
				capture, calls := exactCapture(t, h, sample, id)
				got := capture.Assets[0]
				if calls != 1 || got.PublicAdmitted || got.Snapshot != nil || got.Evaluation != nil || got.UnavailableReason != tc.reason {
					t.Fatalf("gate bypassed: %+v", got)
				}
			}
			if after := readAssetState(h, id); !reflect.DeepEqual(before, after) {
				t.Fatal("withheld capture mutated hub")
			}
		})
	}
	t.Run("private current", func(t *testing.T) {
		h := newTestHub(t)
		id := semreg.AssetID("asset:gate")
		mustDeclare(t, h, id, "source:gate", "gate", nil)
		handle := admitLegacy(t, h, "source:gate", "gate", "epoch:gate", 1)
		mustPublish(t, h, handle, id, "gate", 1, 1)
		control, _ := exactCapture(t, h, exactSample(testWall(3), testMono(30)), id)
		assertExactOracle(t, control.Assets[0], exactOracle(t, h, id, testWall(3), testMono(30)))
		tx, err := h.Begin(handle, id)
		if err != nil {
			t.Fatal(err)
		}
		batch := testBatch(t, tx, "gate", 2, 2)
		batch.FactUpserts[0].Evidence[0].Access, batch.FactUpserts[0].Evidence[0].Redaction = semreg.EvidenceAccessAuthorized, semreg.RedactionNone
		if batch.BatchDigest, err = batch.ComputedDigest(); err != nil {
			t.Fatal(err)
		}
		if _, _, err := tx.Apply(batch, testMono(2)); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Commit(nil); err != nil {
			t.Fatal(err)
		}
		capture, _ := exactCapture(t, h, exactSample(testWall(3), testMono(30)), id)
		if got := capture.Assets[0]; got.PublicAdmitted || got.Snapshot != nil || got.UnavailableReason != "semantic.read.reason.publication_withheld" {
			t.Fatalf("private evidence exposed: %+v", got)
		}
	})
}

func TestExactCaptureUnavailableSiblings(t *testing.T) {
	h := newTestHub(t)
	readable, unadmitted, unpublished := semreg.AssetID("asset:readable"), semreg.AssetID("asset:unadmitted"), semreg.AssetID("asset:unpublished")
	mustDeclare(t, h, readable, "source:readable", "readable", nil)
	mustDeclare(t, h, unpublished, "source:readable", "readable", nil)
	mustDeclare(t, h, unadmitted, "source:unadmitted", "unadmitted", nil)
	mustPublish(t, h, admitLegacy(t, h, "source:readable", "readable", "epoch:readable", 1), readable, "readable", 1, 7)
	wall, tick := testWall(7), testMono(30)
	ids := []semreg.AssetID{"asset:unknown", unadmitted, readable, unpublished}
	capture, calls := exactCapture(t, h, exactSample(wall, tick), ids...)
	if calls != 1 || len(capture.Assets) != len(ids) {
		t.Fatalf("calls=%d assets=%d", calls, len(capture.Assets))
	}
	for index, reason := range []string{"semantic.read.reason.asset_unknown", "semantic.read.reason.source_unavailable", "", "semantic.read.reason.snapshot_unavailable"} {
		got := capture.Assets[index]
		if got.AssetID != ids[index] || got.UnavailableReason != reason || (reason != "") == (got.Snapshot != nil) {
			t.Fatalf("asset %d: %+v", index, got)
		}
	}
	assertExactOracle(t, capture.Assets[2], exactOracle(t, h, readable, wall, tick))
}

func TestExactCaptureDetachedResults(t *testing.T) {
	h, id := exactSingleAsset(t, 7)
	before := inspectAll(t, h, id)
	wall, tick := testWall(7), testMono(30)
	sample := exactSample(wall, tick)
	capture, _ := exactCapture(t, h, sample, id)
	want := exactOracle(t, h, id, wall, tick)
	sample.Monotonic[exactEpoch] = testMono(400)
	delete(sample.Monotonic, exactEpoch)
	if !reflect.DeepEqual(*capture.Assets[0].Evaluation, want) {
		t.Fatal("result aliases the caller's sample map")
	}
	got := capture.Assets[0]
	got.Snapshot.Facts[0].Candidates[0].Evidence[0].Owner = "mutated.owner"
	got.Snapshot.Revisions.Semantic = "999"
	got.Evaluation.Facts[0].Freshness = semreg.FreshnessExpired
	if after := inspectAll(t, h, id); !reflect.DeepEqual(before, after) {
		t.Fatal("result aliases hub state")
	}
	later := testMono(100)
	next, _ := exactCapture(t, h, exactSample(wall, later), id)
	assertExactOracle(t, next.Assets[0], exactOracle(t, h, id, wall, later))
	if next.Assets[0].Snapshot == got.Snapshot || next.Assets[0].Evaluation == got.Evaluation || next.Metadata == capture.Metadata {
		t.Fatal("capture aliases another read")
	}
}

func TestExactCaptureSerializesPublishAcrossSelection(t *testing.T) {
	h := newTestHub(t)
	first, second := semreg.AssetID("asset:first"), semreg.AssetID("asset:second")
	for _, id := range []semreg.AssetID{first, second} {
		mustDeclare(t, h, id, "source:pair", "pair", nil)
	}
	handle := admitLegacy(t, h, "source:pair", "pair", "epoch:pair", 1)
	for _, id := range []semreg.AssetID{first, second} {
		mustPublish(t, h, handle, id, "pair", 1, 1)
	}
	before, _ := h.Inspect(second)
	tx, err := h.Begin(handle, second)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := tx.Apply(testBatch(t, tx, "pair", 2, 2), testMono(2)); err != nil {
		t.Fatal(err)
	}
	entered, release, committing := make(chan struct{}), make(chan struct{}), make(chan struct{})
	h.faults = func(stage string, id semreg.AssetID) {
		if stage == "capture-read" && id == first {
			close(entered)
			<-release
		}
		if stage == "commit" {
			close(committing)
		}
	}
	wall, tick := testWall(2), testMono(4)
	sampler := &exactSampler{t: t, h: h, sample: exactSample(wall, tick)}
	type result struct {
		capture CoherentCapture
		err     error
	}
	read := make(chan result, 1)
	go func() {
		c, e := h.CaptureExact(context.Background(), []semreg.AssetID{first, second}, sampler.fn())
		read <- result{c, e}
	}()
	<-entered
	written := make(chan error, 1)
	go func() { _, e := tx.Commit(nil); written <- e }()
	<-committing
	close(release)
	r := <-read
	if err := <-written; err != nil {
		t.Fatal(err)
	}
	h.faults = nil
	if r.err != nil || sampler.calls != 1 {
		t.Fatalf("err=%v calls=%d", r.err, sampler.calls)
	}
	oracle, err := semreg.EvaluateSnapshot(before.Snapshot, semreg.EvaluationContext{EvaluatedAt: wall, EvaluateMonotonic: tick})
	if err != nil {
		t.Fatal(err)
	}
	if r.capture.Assets[1].Snapshot.SnapshotID != before.Snapshot.SnapshotID || !reflect.DeepEqual(*r.capture.Assets[1].Evaluation, oracle) {
		t.Fatal("publication entered a partial capture")
	}
	if next, _ := exactCapture(t, h, exactSample(wall, tick), second); next.Assets[0].Snapshot.SnapshotID == before.Snapshot.SnapshotID {
		t.Fatal("next request missed publication")
	}
}
