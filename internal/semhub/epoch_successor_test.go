package semhub

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
)

// The hub-level A2 vectors (LIFE-05, LIFE-25, LIFE-31, LIFE-32) and the
// negative cases of the hub-native successor admission. Native close proof,
// the durable intent/outcome protocol and native reconnect are service and
// manager obligations outside this seam; the vectors model them only by
// ordering hub calls.

var successorAssets = []semreg.AssetID{"asset:succ-a", "asset:succ-b"}

const successorSource semreg.SourceID = "source:succ"

type successorFixture struct {
	hub     *Hub
	journal *MemoryJournal
	mu      sync.Mutex
	fail    map[semreg.AssetID]error
}

func (f *successorFixture) onControl(event ControlEvent) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.fail[event.Asset]
}

func (f *successorFixture) setFail(id semreg.AssetID, err error) {
	f.mu.Lock()
	f.fail[id] = err
	f.mu.Unlock()
}

// newSuccessorFixture declares assets for one hub-native source; it admits
// nothing, so each vector starts from process start.
func newSuccessorFixture(t *testing.T, assets ...semreg.AssetID) *successorFixture {
	t.Helper()
	f := &successorFixture{journal: NewMemoryJournal(), fail: map[semreg.AssetID]error{}}
	hub, err := New(Options{Journal: f.journal})
	if err != nil {
		t.Fatal(err)
	}
	f.hub = hub
	for _, id := range assets {
		mustDeclare(t, hub, id, successorSource, "native-family", f.onControl)
	}
	return f
}

func admitNative(t *testing.T, hub *Hub, src semreg.SourceID) *Handle {
	t.Helper()
	return admitNativeAs(t, hub, src, "native-family")
}

func admitNativeAs(t *testing.T, hub *Hub, src semreg.SourceID, driver string) *Handle {
	t.Helper()
	handle, err := hub.AdmitSource(Admission{Source: src, Driver: driver})
	if err != nil {
		t.Fatal(err)
	}
	return handle
}

func retireRequest(handle *Handle, at int64) RetireRequest {
	return RetireRequest{Source: handle.Source(), Epoch: handle.Epoch(), Reason: "lifecycle.source_stopped",
		Evidence: []semreg.EvidenceRef{testEvidence("semhubtest.retire")}, ObservedAt: testWall(at), Monotonic: testMono(at)}
}

func mustRetire(t *testing.T, hub *Hub, handle *Handle, at int64) ControlResult {
	t.Helper()
	result, err := hub.RetireEpoch(retireRequest(handle, at))
	if err != nil {
		t.Fatalf("retire %s: %v", handle.Epoch(), err)
	}
	return result
}

func mustSucceed(t *testing.T, hub *Hub, previous *Handle, operation string) *Handle {
	t.Helper()
	successor, err := hub.AdmitSuccessor(Succession{Previous: previous, Operation: operation})
	if err != nil {
		t.Fatalf("succession %s: %v", operation, err)
	}
	return successor
}

func nextSequence(t *testing.T, hub *Hub, id semreg.AssetID, handle *Handle) semreg.Uint64 {
	t.Helper()
	next, err := hub.NextSequence(id, handle.Source(), handle.Epoch(), handle.Generation())
	if err != nil {
		t.Fatal(err)
	}
	return next
}

// unchanged captures everything a refused succession must leave untouched.
type unchangedState struct {
	journal int
	cursor  Cursor
	info    SourceInfo
	ordinal uint64
	count   int
}

func captureState(t *testing.T, f *successorFixture, src semreg.SourceID) unchangedState {
	t.Helper()
	info, _ := f.hub.Source(src)
	f.hub.mu.RLock()
	record := f.hub.sources[src]
	state := unchangedState{journal: len(f.journal.Records()), cursor: f.hub.Cursor(), info: info, ordinal: record.ordinal, count: len(record.successions)}
	f.hub.mu.RUnlock()
	return state
}

func assertUnchanged(t *testing.T, f *successorFixture, src semreg.SourceID, before unchangedState, label string) {
	t.Helper()
	if after := captureState(t, f, src); after != before {
		t.Fatalf("%s mutated the hub: before=%+v after=%+v", label, before, after)
	}
}

func assertStaleEpoch(t *testing.T, hub *Hub, handle *Handle, id semreg.AssetID) {
	t.Helper()
	if _, err := publish(t, hub, handle, id, "succ", 99, 50); errorID(err) != semreg.StaleSourceEpoch {
		t.Fatalf("retired predecessor published: %v", err)
	}
	if _, err := hub.Activate(handle, handle.Generation()+1); errorID(err) != semreg.StaleSourceEpoch {
		t.Fatalf("retired predecessor activated: %v", err)
	}
}

// successionRecords returns the journal records of successions, in order.
func successionRecords(records []EpochRecord) []EpochRecord {
	out := []EpochRecord{}
	for _, record := range records {
		if record.Ordinal != 0 {
			out = append(out, record)
		}
	}
	return out
}

func TestSuccessorLIFE05CloseBeforePrepareRestart(t *testing.T) {
	f := newSuccessorFixture(t, successorAssets...)
	predecessor := admitNative(t, f.hub, successorSource)
	for _, id := range successorAssets {
		mustPublish(t, f.hub, predecessor, id, "succ", 10, 1)
		mustPublish(t, f.hub, predecessor, id, "succ", 20, 2)
	}
	// No overlapping owner: a live predecessor has no successor.
	before := captureState(t, f, successorSource)
	if _, err := f.hub.AdmitSuccessor(Succession{Previous: predecessor, Operation: "restart-1"}); !errors.Is(err, ErrAdmission) {
		t.Fatalf("succession of a live predecessor=%v", err)
	}
	assertUnchanged(t, f, successorSource, before, "live-predecessor refusal")
	mustRetire(t, f.hub, predecessor, 4)
	oldCursors := map[semreg.AssetID]semreg.Uint64{}
	for _, id := range successorAssets {
		oldCursors[id] = nextSequence(t, f.hub, id, predecessor)
	}
	journalBefore := len(f.journal.Records())
	successor := mustSucceed(t, f.hub, predecessor, "restart-1")
	if successor.Generation() != 1 || successor.Epoch() == predecessor.Epoch() || successor.Epoch() != successorEpoch(f.hub.Incarnation(), successorSource, 1) {
		t.Fatalf("successor epoch=%q generation=%d", successor.Epoch(), successor.Generation())
	}
	if successor.Incarnation(successorAssets[0]).First {
		t.Fatal("successor reported as the first admission")
	}
	records := f.journal.Records()[journalBefore:]
	want := []EpochRecord{
		{Incarnation: f.hub.Incarnation(), Source: successorSource, Epoch: predecessor.Epoch(), Generation: 1, Kind: EventSourceRetired, Ordinal: 1},
		{Incarnation: f.hub.Incarnation(), Source: successorSource, Epoch: successor.Epoch(), Generation: 1, Kind: EventEpochAdmitted, Ordinal: 1},
	}
	if !reflect.DeepEqual(records, want) {
		t.Fatalf("succession journal=%+v", records)
	}
	assertStaleEpoch(t, f.hub, predecessor, successorAssets[0])
	for _, id := range successorAssets {
		if next := nextSequence(t, f.hub, id, successor); next != "1" {
			t.Fatalf("%s successor cursor=%s", id, next)
		}
		if next := nextSequence(t, f.hub, id, predecessor); next != oldCursors[id] {
			t.Fatalf("%s predecessor cursor moved: %s -> %s", id, oldCursors[id], next)
		}
		mustPublish(t, f.hub, successor, id, "succ", 30, 5)
		view, _ := f.hub.Inspect(id)
		states := map[semreg.SourceEpochID]semreg.SourceState{}
		for _, descriptor := range view.Snapshot.Sources {
			states[descriptor.SourceEpochID] = descriptor.State
		}
		if states[predecessor.Epoch()] != semreg.SourceRetired || states[successor.Epoch()] != semreg.SourceCurrent {
			t.Fatalf("%s sources=%+v", id, view.Snapshot.Sources)
		}
	}
	// A same-epoch manager retry keeps the successor epoch and advances the
	// driver generation through Fence and Activate.
	if _, err := f.hub.Fence(testFence(successorSource, successor.Epoch(), 1, 6)); err != nil {
		t.Fatal(err)
	}
	retried, err := f.hub.Activate(successor, 2)
	if err != nil {
		t.Fatal(err)
	}
	if retried.Epoch() != successor.Epoch() || retried.Generation() != 2 {
		t.Fatalf("manager retry epoch=%q generation=%d", retried.Epoch(), retried.Generation())
	}
	// A wake of the restart while the successor's own control work is
	// unfinished still resolves to the recorded successor, never a new one.
	f.hub.faults = func(stage string, id semreg.AssetID) {
		if stage == "before-swap" && id == successorAssets[1] {
			f.hub.faults = nil
			panic("injected interruption")
		}
	}
	for _, id := range successorAssets {
		mustPublish(t, f.hub, retried, id, "succ", 35, 7)
	}
	if _, err := f.hub.Fence(testFence(successorSource, retried.Epoch(), 2, 8)); err == nil {
		t.Fatal("interrupted fence reported success")
	}
	// A late replay of the restart resolves to the recorded generation-1
	// successor, which the activation already made obsolete.
	late, err := f.hub.AdmitSuccessor(Succession{Previous: predecessor, Operation: "restart-1"})
	if err != nil || *late != *successor {
		t.Fatalf("late replay=%v/%v", late, err)
	}
	if err := f.hub.checkHandle(late); errorID(err) != semreg.StaleDriverGeneration {
		t.Fatalf("obsolete successor handle published: %v", err)
	}
}

func TestSuccessorLIFE25InitiallyStoppedThenRestart(t *testing.T) {
	f := newSuccessorFixture(t, successorAssets...)
	// Process start with desired STOPPED: the inert generation-1 epoch is
	// admitted before any provider work.
	initial := admitNative(t, f.hub, successorSource)
	// First START claims that epoch; it never allocates a second one.
	before := captureState(t, f, successorSource)
	if _, err := f.hub.AdmitSuccessor(Succession{Previous: initial, Operation: "start-1"}); !errors.Is(err, ErrAdmission) {
		t.Fatalf("first START allocated a successor: %v", err)
	}
	assertUnchanged(t, f, successorSource, before, "first START")
	// Per-asset barrier cursors advance independently.
	mustPublish(t, f.hub, initial, successorAssets[0], "succ", 1, 1)
	mustPublish(t, f.hub, initial, successorAssets[0], "succ", 2, 2)
	mustPublish(t, f.hub, initial, successorAssets[1], "succ", 3, 2)
	if a, b := nextSequence(t, f.hub, successorAssets[0], initial), nextSequence(t, f.hub, successorAssets[1], initial); a != "3" || b != "2" {
		t.Fatalf("initial cursors=%s/%s", a, b)
	}
	// RESTART: retire (its accepted control batch advances each asset's own
	// cursor once), then exactly one new epoch at generation 1.
	mustRetire(t, f.hub, initial, 3)
	if a, b := nextSequence(t, f.hub, successorAssets[0], initial), nextSequence(t, f.hub, successorAssets[1], initial); a != "4" || b != "3" {
		t.Fatalf("retired cursors=%s/%s", a, b)
	}
	first := mustSucceed(t, f.hub, initial, "restart-1")
	for _, id := range successorAssets {
		if next := nextSequence(t, f.hub, id, first); next != "1" {
			t.Fatalf("%s carried an old cursor into the new epoch: %s", id, next)
		}
	}
	if a, b := nextSequence(t, f.hub, successorAssets[0], initial), nextSequence(t, f.hub, successorAssets[1], initial); a != "4" || b != "3" {
		t.Fatalf("retired cursors changed: %s/%s", a, b)
	}
	mustPublish(t, f.hub, first, successorAssets[0], "succ", 4, 4)
	// A second RESTART advances the ordinal and derives a distinct epoch.
	mustRetire(t, f.hub, first, 5)
	second := mustSucceed(t, f.hub, first, "restart-2")
	if second.Epoch() != successorEpoch(f.hub.Incarnation(), successorSource, 2) || second.Epoch() == first.Epoch() || second.Epoch() == initial.Epoch() || second.Generation() != 1 {
		t.Fatalf("second successor epoch=%q generation=%d", second.Epoch(), second.Generation())
	}
	admitted := 0
	for _, record := range f.journal.Records() {
		if record.Kind == EventEpochAdmitted {
			admitted++
		}
	}
	ordinals := []uint64{}
	for _, record := range successionRecords(f.journal.Records()) {
		ordinals = append(ordinals, record.Ordinal)
	}
	if admitted != 3 || !reflect.DeepEqual(ordinals, []uint64{1, 1, 2, 2}) {
		t.Fatalf("admissions=%d ordinals=%v", admitted, ordinals)
	}
	for _, id := range successorAssets {
		if next := nextSequence(t, f.hub, id, second); next != "1" {
			t.Fatalf("%s second successor cursor=%s", id, next)
		}
	}
	assertStaleEpoch(t, f.hub, first, successorAssets[0])
}

func TestSuccessorLIFE31SecondStartAfterStopReusesOneSuccessor(t *testing.T) {
	f := newSuccessorFixture(t, successorAssets...)
	started := admitNative(t, f.hub, successorSource)
	for _, id := range successorAssets {
		mustPublish(t, f.hub, started, id, "succ", 1, 1)
	}
	// Completed STOP retires the first epoch permanently.
	mustRetire(t, f.hub, started, 2)
	journalBefore := len(f.journal.Records())
	// The second START is woken concurrently and repeatedly.
	const wakes = 8
	handles := make([]*Handle, wakes)
	errs := make([]error, wakes)
	var wg sync.WaitGroup
	for i := 0; i < wakes; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			handles[i], errs[i] = f.hub.AdmitSuccessor(Succession{Previous: started, Operation: "start-2"})
		}(i)
	}
	wg.Wait()
	for i := range handles {
		if errs[i] != nil || *handles[i] != *handles[0] {
			t.Fatalf("wake %d=%v/%v", i, handles[i], errs[i])
		}
	}
	// Component recovery before Prepare resumes the same successor.
	recovered := mustSucceed(t, f.hub, started, "start-2")
	if *recovered != *handles[0] || recovered.Generation() != 1 {
		t.Fatalf("recovery resolved another successor: %+v", recovered)
	}
	if records := successionRecords(f.journal.Records()[journalBefore:]); len(records) != 2 || records[1].Ordinal != 1 || records[1].Epoch != recovered.Epoch() {
		t.Fatalf("succession journal=%+v", records)
	}
	// Another operation cannot claim a second successor of the predecessor.
	before := captureState(t, f, successorSource)
	if _, err := f.hub.AdmitSuccessor(Succession{Previous: started, Operation: "start-3"}); !errors.Is(err, ErrAdmission) {
		t.Fatalf("second operation=%v", err)
	}
	assertUnchanged(t, f, successorSource, before, "second-operation refusal")
	assertStaleEpoch(t, f.hub, started, successorAssets[1])
	for _, id := range successorAssets {
		if next := nextSequence(t, f.hub, id, recovered); next != "1" {
			t.Fatalf("%s successor cursor=%s", id, next)
		}
		mustPublish(t, f.hub, recovered, id, "succ", 2, 3)
	}
}

func TestSuccessorLIFE32UncertainRetirementBlocksSuccession(t *testing.T) {
	f := newSuccessorFixture(t, successorAssets...)
	predecessor := admitNative(t, f.hub, successorSource)
	for _, id := range successorAssets {
		mustPublish(t, f.hub, predecessor, id, "succ", 1, 1)
	}
	cursors := map[semreg.AssetID]semreg.Uint64{}
	for _, id := range successorAssets {
		cursors[id] = nextSequence(t, f.hub, id, predecessor)
	}
	f.hub.faults = func(stage string, id semreg.AssetID) {
		if stage == "before-swap" && id == successorAssets[1] {
			f.hub.faults = nil
			panic("injected withdrawal loss")
		}
	}
	result, err := f.hub.RetireEpoch(retireRequest(predecessor, 2))
	if err == nil || result.Assets[1].Outcome != SemanticUncertain {
		t.Fatalf("retire=%+v err=%v", result.Assets, err)
	}
	pending := f.hub.attempts(t, result.ID, successorAssets[1])
	before := captureState(t, f, successorSource)
	var unfinished *ControlPendingError
	if _, err := f.hub.AdmitSuccessor(Succession{Previous: predecessor, Operation: "restart-1"}); !errors.As(err, &unfinished) || unfinished.ID != result.ID {
		t.Fatalf("succession over uncertain withdrawal=%v", err)
	}
	assertUnchanged(t, f, successorSource, before, "uncertain-withdrawal refusal")
	if !reflect.DeepEqual(f.hub.attempts(t, result.ID, successorAssets[1]), pending) {
		t.Fatal("refused succession changed the pending exact bytes")
	}
	// Resolution resubmits the exact retained bytes; only then can the
	// successor be admitted.
	if _, err := f.hub.RetryControl(result.ID); err != nil {
		t.Fatal(err)
	}
	if attempts := f.hub.attempts(t, result.ID, successorAssets[1]); len(attempts) != 1 || !reflect.DeepEqual(attempts, pending) {
		t.Fatalf("uncertain withdrawal was rebuilt: %d attempts", len(attempts))
	}
	successor := mustSucceed(t, f.hub, predecessor, "restart-1")
	for _, id := range successorAssets {
		if next := nextSequence(t, f.hub, id, predecessor); compareDecimal(next, cursors[id]) < 0 {
			t.Fatalf("%s accepted cursor reset: %s -> %s", id, cursors[id], next)
		}
		if next := nextSequence(t, f.hub, id, successor); next != "1" {
			t.Fatalf("%s successor cursor=%s", id, next)
		}
	}
	if records := successionRecords(f.journal.Records()); len(records) != 2 || records[1].Epoch != successor.Epoch() {
		t.Fatalf("succession journal=%+v", records)
	}
}

func TestSuccessorRefusesUnsettledReprojection(t *testing.T) {
	f := newSuccessorFixture(t, successorAssets...)
	predecessor := admitNative(t, f.hub, successorSource)
	for _, id := range successorAssets {
		mustPublish(t, f.hub, predecessor, id, "succ", 1, 1)
	}
	f.setFail(successorAssets[0], errors.New("cache rebuild failed"))
	result := mustRetire(t, f.hub, predecessor, 2)
	before := captureState(t, f, successorSource)
	var settling *SettlePendingError
	if _, err := f.hub.AdmitSuccessor(Succession{Previous: predecessor, Operation: "restart-1"}); !errors.As(err, &settling) || settling.ID != result.ID {
		t.Fatalf("succession over unsettled reprojection=%v", err)
	}
	assertUnchanged(t, f, successorSource, before, "unsettled-reprojection refusal")
	f.setFail(successorAssets[0], nil)
	if err := f.hub.Resettle(successorAssets[0]); err != nil {
		t.Fatal(err)
	}
	mustSucceed(t, f.hub, predecessor, "restart-1")
}

func TestSuccessorRefusesAbandonedRetirement(t *testing.T) {
	f := newSuccessorFixture(t, successorAssets...)
	predecessor := admitNative(t, f.hub, successorSource)
	for _, id := range successorAssets {
		mustPublish(t, f.hub, predecessor, id, "succ", 1, 1)
	}
	f.hub.faults = func(stage string, id semreg.AssetID) {
		if stage == "before-submit" && id == successorAssets[1] {
			f.hub.faults = nil
			intrude(t, f.hub, id)
		}
	}
	result, _ := f.hub.RetireEpoch(retireRequest(predecessor, 2))
	if err := f.hub.AbandonControl(result.ID); err != nil {
		t.Fatal(err)
	}
	before := captureState(t, f, successorSource)
	if _, err := f.hub.AdmitSuccessor(Succession{Previous: predecessor, Operation: "restart-1"}); !errors.Is(err, ErrControlAbandoned) {
		t.Fatalf("succession over an abandoned retirement=%v", err)
	}
	assertUnchanged(t, f, successorSource, before, "abandoned-retirement refusal")
}

func TestSuccessorJournalRejectionExposesNothing(t *testing.T) {
	f := newSuccessorFixture(t, successorAssets...)
	predecessor := admitNative(t, f.hub, successorSource)
	mustPublish(t, f.hub, predecessor, successorAssets[0], "succ", 1, 1)
	mustRetire(t, f.hub, predecessor, 2)
	f.journal.mu.Lock()
	f.journal.failure = errors.New("journal store unavailable")
	f.journal.mu.Unlock()
	before := captureState(t, f, successorSource)
	if handle, err := f.hub.AdmitSuccessor(Succession{Previous: predecessor, Operation: "restart-1"}); err == nil || handle != nil {
		t.Fatalf("succession with a failing journal=%v/%v", handle, err)
	}
	assertUnchanged(t, f, successorSource, before, "journal rejection")
	f.journal.mu.Lock()
	f.journal.failure = nil
	f.journal.mu.Unlock()
	// The rejected attempt consumed no ordinal and recorded no claim.
	successor := mustSucceed(t, f.hub, predecessor, "restart-1")
	if successor.Epoch() != successorEpoch(f.hub.Incarnation(), successorSource, 1) {
		t.Fatalf("journal rejection consumed an ordinal: %q", successor.Epoch())
	}
}

func TestSuccessorCounterExhaustionDoesNotWrap(t *testing.T) {
	for _, field := range []string{"ordinal", "admission", "token"} {
		t.Run(field, func(t *testing.T) {
			f := newSuccessorFixture(t, successorAssets...)
			predecessor := admitNative(t, f.hub, successorSource)
			mustRetire(t, f.hub, predecessor, 1)
			f.hub.mu.Lock()
			record := f.hub.sources[successorSource]
			counter := map[string]*uint64{"ordinal": &record.ordinal, "admission": &record.admission, "token": &record.token}[field]
			saved := *counter
			*counter = ^uint64(0)
			f.hub.mu.Unlock()
			if field == "admission" {
				// The predecessor handle must still name the current admission.
				predecessor.admission = ^uint64(0)
			}
			before := captureState(t, f, successorSource)
			if _, err := f.hub.AdmitSuccessor(Succession{Previous: predecessor, Operation: "restart-1"}); !errors.Is(err, ErrCounterExhausted) || !errors.Is(err, ErrAdmission) {
				t.Fatalf("exhausted %s=%v", field, err)
			}
			assertUnchanged(t, f, successorSource, before, "exhaustion")
			f.hub.mu.Lock()
			if *counter != ^uint64(0) {
				t.Fatalf("%s wrapped to %d", field, *counter)
			}
			*counter = saved
			f.hub.mu.Unlock()
			if field == "admission" {
				predecessor.admission = saved
			}
			successor := mustSucceed(t, f.hub, predecessor, "restart-1")
			if successor.Epoch() != successorEpoch(f.hub.Incarnation(), successorSource, 1) {
				t.Fatalf("exhaustion left a partial mutation: %q", successor.Epoch())
			}
		})
	}
}

func TestSuccessorKeepsTheIdentityRegime(t *testing.T) {
	f := newSuccessorFixture(t)
	mustDeclare(t, f.hub, "asset:succ-legacy", "source:succ-legacy", "growatt_storage", nil)
	legacy, err := f.hub.AdmitSource(Admission{Source: "source:succ-legacy", Driver: "growatt_storage", Legacy: LegacyGrowattStorage, Epoch: "epoch:legacy-1", Generation: 1})
	if err != nil {
		t.Fatal(err)
	}
	mustPublish(t, f.hub, legacy, "asset:succ-legacy", "succ", 1, 1)
	mustRetire(t, f.hub, legacy, 2)
	before := captureState(t, f, "source:succ-legacy")
	if _, err := f.hub.AdmitSuccessor(Succession{Previous: legacy, Operation: "restart-1"}); !errors.Is(err, ErrAdmission) {
		t.Fatalf("legacy source used the hub-native succession: %v", err)
	}
	assertUnchanged(t, f, "source:succ-legacy", before, "legacy refusal")
	// The legacy successor path is unchanged: generation current+1, new epoch.
	next, err := f.hub.AdmitSource(Admission{Source: "source:succ-legacy", Driver: "growatt_storage", Legacy: LegacyGrowattStorage, Epoch: "epoch:legacy-2", Generation: 2})
	if err != nil {
		t.Fatal(err)
	}
	mustPublish(t, f.hub, next, "asset:succ-legacy", "succ", 2, 3)
	// A hub-native successor stays hub-native.
	native := admitNative(t, f.hub, "source:succ-native")
	mustRetire(t, f.hub, native, 4)
	successor := mustSucceed(t, f.hub, native, "restart-1")
	if info, _ := f.hub.Source("source:succ-native"); info.Legacy != "" || info.Epoch != successor.Epoch() || info.Generation != 1 || !info.Live {
		t.Fatalf("successor view=%+v", info)
	}
	if _, err := f.hub.AdmitSource(Admission{Source: "source:succ-native", Driver: "native-family", Legacy: LegacyGrowattStorage, Epoch: "epoch:switch", Generation: 2}); !errors.Is(err, ErrAdmission) {
		t.Fatalf("hub-native source switched regime: %v", err)
	}
}

func TestSuccessorRequestValidationAndWithdrawal(t *testing.T) {
	f := newSuccessorFixture(t)
	other := newTestHub(t)
	foreign := admitNative(t, other, successorSource)
	initial := admitNative(t, f.hub, successorSource)
	// A fence is no retirement: the same epoch could still be reactivated.
	if _, err := f.hub.Fence(testFence(successorSource, initial.Epoch(), 1, 1)); err != nil {
		t.Fatal(err)
	}
	before := captureState(t, f, successorSource)
	if _, err := f.hub.AdmitSuccessor(Succession{Previous: initial, Operation: "restart-1"}); !errors.Is(err, ErrAdmission) {
		t.Fatalf("succession of a fenced, unretired predecessor=%v", err)
	}
	assertUnchanged(t, f, successorSource, before, "fenced-predecessor refusal")
	mustRetire(t, f.hub, initial, 1)
	before = captureState(t, f, successorSource)
	for name, request := range map[string]Succession{
		"nil predecessor":     {Operation: "restart-1"},
		"foreign predecessor": {Previous: foreign, Operation: "restart-1"},
		"empty operation":     {Previous: initial},
		"invalid operation":   {Previous: initial, Operation: " restart"},
	} {
		if handle, err := f.hub.AdmitSuccessor(request); err == nil || handle != nil {
			t.Fatalf("%s accepted: %v", name, handle)
		}
	}
	assertUnchanged(t, f, successorSource, before, "invalid requests")
	successor := mustSucceed(t, f.hub, initial, "restart-1")
	// A journalled succession is not withdrawable.
	before = captureState(t, f, successorSource)
	if err := f.hub.Withdraw(successor); !errors.Is(err, ErrAdmission) {
		t.Fatalf("successor withdrawn: %v", err)
	}
	assertUnchanged(t, f, successorSource, before, "successor withdrawal")
	// A failed successor start is retired and succeeded again instead.
	mustRetire(t, f.hub, successor, 2)
	again := mustSucceed(t, f.hub, successor, "restart-2")
	if again.Epoch() != successorEpoch(f.hub.Incarnation(), successorSource, 2) {
		t.Fatalf("re-succession epoch=%q", again.Epoch())
	}
	// The obsolete initial admission cannot claim another successor.
	if _, err := f.hub.AdmitSuccessor(Succession{Previous: initial, Operation: "restart-3"}); !errors.Is(err, ErrAdmission) {
		t.Fatalf("obsolete admission claimed a successor: %v", err)
	}
	// The unchanged hub-native initial withdrawal still readmits its epoch.
	fresh := newTestHub(t)
	first := admitNative(t, fresh, "source:withdraw")
	if err := fresh.Withdraw(first); err != nil {
		t.Fatal(err)
	}
	readmitted := admitNative(t, fresh, "source:withdraw")
	if readmitted.Epoch() != first.Epoch() || readmitted.Generation() != 1 {
		t.Fatalf("withdrawn hub-native admission readmitted as %q/%d", readmitted.Epoch(), readmitted.Generation())
	}
	// The withdrawn admission's handle names the same epoch but an obsolete
	// admission identity: it cannot claim the readmission's successor.
	mustRetire(t, fresh, readmitted, 3)
	if _, err := fresh.AdmitSuccessor(Succession{Previous: first, Operation: "restart-1"}); errorID(err) != semreg.StaleSourceEpoch {
		t.Fatalf("withdrawn admission claimed a successor: %v", err)
	}
	mustSucceed(t, fresh, readmitted, "restart-1")
}

func TestSuccessorAssetlessRetirementIsTruthful(t *testing.T) {
	f := newSuccessorFixture(t)
	initial := admitNative(t, f.hub, successorSource)
	sub := f.hub.Subscribe()
	defer sub.Close()
	result := mustRetire(t, f.hub, initial, 1)
	if len(result.Assets) != 0 {
		t.Fatalf("assetless retirement covered assets: %+v", result.Assets)
	}
	successor := mustSucceed(t, f.hub, initial, "start-2")
	want := []EpochRecord{
		{Incarnation: f.hub.Incarnation(), Source: successorSource, Epoch: initial.Epoch(), Generation: 1, Kind: EventEpochAdmitted},
		{Incarnation: f.hub.Incarnation(), Source: successorSource, Epoch: initial.Epoch(), Generation: 1, Kind: EventSourceRetired},
		{Incarnation: f.hub.Incarnation(), Source: successorSource, Epoch: initial.Epoch(), Generation: 1, Kind: EventSourceRetired, Ordinal: 1},
		{Incarnation: f.hub.Incarnation(), Source: successorSource, Epoch: successor.Epoch(), Generation: 1, Kind: EventEpochAdmitted, Ordinal: 1},
	}
	for i := range want {
		if got := f.journal.Records(); i >= len(got) || len(got[i].Assets) != 0 {
			t.Fatalf("assetless journal claims assets: %+v", got)
		}
	}
	got := f.journal.Records()
	for i := range got {
		got[i].Assets = nil
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("assetless journal=%+v", got)
	}
	events := []Event{}
	for len(events) < 2 {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		event, err := sub.Next(ctx)
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		events = append(events, event)
	}
	if events[0].Kind != EventSourceRetired || !events[0].NoPublication || events[0].Asset != "" || events[1].Kind != EventEpochAdmitted || events[1].Epoch != successor.Epoch() || events[1].Generation != 1 {
		t.Fatalf("assetless events=%+v", events)
	}
}

func TestSuccessorExportRestoreRoundTrip(t *testing.T) {
	origin := recoveryHub(t, nil, func(ControlEvent) error { return nil })
	predecessor := admitNativeAs(t, origin, "source:restore", "synthetic-family")
	for _, id := range recoveryAssets {
		mustPublish(t, origin, predecessor, id, "synthetic", 10, 1)
	}
	mustRetire(t, origin, predecessor, 2)
	successor := mustSucceed(t, origin, predecessor, "restart-1")
	mustPublish(t, origin, successor, recoveryAssets[0], "synthetic", 20, 3)
	record := origin.ExportRecovery()
	if family, ok := record.Regimes["source:restore"]; !ok || family != "" {
		t.Fatalf("hub-native regime not exported: %v", record.Regimes)
	}
	exported := successionRecords(record.Epochs)
	if len(exported) != 2 || exported[0].Epoch != predecessor.Epoch() || exported[1].Epoch != successor.Epoch() || exported[1].Ordinal != 1 || exported[1].Generation != 1 {
		t.Fatalf("exported succession=%+v", exported)
	}
	fresh := recoveryHub(t, nil, func(ControlEvent) error { return nil })
	if err := fresh.Restore(record); err != nil {
		t.Fatal(err)
	}
	again := fresh.ExportRecovery()
	if !reflect.DeepEqual(again.Epochs, record.Epochs) || !reflect.DeepEqual(again.Regimes, record.Regimes) {
		t.Fatalf("succession history did not round-trip: %+v", successionRecords(again.Epochs))
	}
	// Prior-incarnation authority is history only.
	for _, old := range []*Handle{predecessor, successor} {
		if _, err := fresh.AdmitSuccessor(Succession{Previous: old, Operation: "restart-1"}); errorID(err) != semreg.StaleSourceEpoch {
			t.Fatalf("prior-incarnation handle claimed a successor: %v", err)
		}
	}
	if _, err := fresh.AdmitSource(Admission{Source: "source:restore", Driver: "synthetic-family", Legacy: LegacyGrowattStorage, Epoch: "epoch:switch", Generation: 1}); !errors.Is(err, ErrAdmission) {
		t.Fatalf("restored hub-native source switched regime: %v", err)
	}
	current := admitNativeAs(t, fresh, "source:restore", "synthetic-family")
	if current.Incarnation(recoveryAssets[0]).First || current.Epoch() == predecessor.Epoch() || current.Epoch() == successor.Epoch() {
		t.Fatalf("new incarnation admission=%q first=%t", current.Epoch(), current.Incarnation(recoveryAssets[0]).First)
	}
	// A restored retirement does not retire the new incarnation's epoch.
	if _, err := fresh.AdmitSuccessor(Succession{Previous: current, Operation: "restart-2"}); !errors.Is(err, ErrAdmission) {
		t.Fatalf("restored retirement admitted a successor of a live epoch: %v", err)
	}
	mustRetire(t, fresh, current, 4)
	next := mustSucceed(t, fresh, current, "restart-2")
	if next.Epoch() != successorEpoch(fresh.Incarnation(), "source:restore", 1) || next.Epoch() == successor.Epoch() {
		t.Fatalf("new incarnation successor epoch=%q", next.Epoch())
	}
	final := successionRecords(fresh.ExportRecovery().Epochs)
	if len(final) != 4 || !reflect.DeepEqual(final[:2], exported) || final[3].Incarnation != fresh.Incarnation() || final[3].Ordinal != 1 {
		t.Fatalf("restored and new successions=%+v", final)
	}
	second := recoveryHub(t, nil, func(ControlEvent) error { return nil })
	if err := second.Restore(fresh.ExportRecovery()); err != nil {
		t.Fatalf("checkpoint with two incarnations of successions refused: %v", err)
	}
	if got := fmt.Sprint(successionRecords(second.ExportRecovery().Epochs)); got != fmt.Sprint(final) {
		t.Fatalf("second round trip=%s", got)
	}
}

func TestRestoreRejectsMalformedSuccessionHistory(t *testing.T) {
	mutations := map[string]func(*RecoveryRecord){
		"unrelated-activation":    func(r *RecoveryRecord) { r.Epochs[0].Kind = EventGenerationActivated; r.Epochs[0].Ordinal = 1 },
		"unrelated-fence":         func(r *RecoveryRecord) { r.Epochs[0].Kind = EventGenerationFenced; r.Epochs[0].Ordinal = 1 },
		"unrelated-revocation":    func(r *RecoveryRecord) { r.Epochs[0].Kind = EventGenerationRevoked; r.Epochs[0].Ordinal = 1 },
		"initial-admission":       func(r *RecoveryRecord) { r.Epochs[0].Ordinal = 1 },
		"missing-admission":       func(r *RecoveryRecord) { r.Epochs = r.Epochs[:3] },
		"missing-retirement":      func(r *RecoveryRecord) { r.Epochs = append(r.Epochs[:2], r.Epochs[3:]...) },
		"zero-admission-ordinal":  func(r *RecoveryRecord) { r.Epochs[3].Ordinal = 0 },
		"zero-retirement-ordinal": func(r *RecoveryRecord) { r.Epochs[2].Ordinal = 0 },
		"mismatched-ordinal":      func(r *RecoveryRecord) { r.Epochs[3].Ordinal = 2 },
		"mismatched-incarnation":  func(r *RecoveryRecord) { r.Epochs[3].Incarnation = "incarnation:other" },
		"mismatched-source": func(r *RecoveryRecord) {
			r.Regimes["source:other"] = ""
			r.Drivers["source:other"] = "synthetic-family"
			r.Epochs[3].Source = "source:other"
		},
		"wrong-admission-kind":         func(r *RecoveryRecord) { r.Epochs[3].Kind = EventGenerationActivated },
		"wrong-generation":             func(r *RecoveryRecord) { r.Epochs[3].Generation = 2 },
		"wrong-predecessor-generation": func(r *RecoveryRecord) { r.Epochs[2].Generation = 2 },
		"unknown-predecessor":          func(r *RecoveryRecord) { r.Epochs[2].Epoch = "epoch:unknown" },
		"unretired-predecessor":        func(r *RecoveryRecord) { r.Epochs[1].Kind = EventGenerationFenced },
		"wrong-successor-epoch":        func(r *RecoveryRecord) { r.Epochs[3].Epoch = "epoch:other" },
		"same-epoch":                   func(r *RecoveryRecord) { r.Epochs[3].Epoch = r.Epochs[2].Epoch },
		"retirement-assets":            func(r *RecoveryRecord) { r.Epochs[2].Assets = []semreg.AssetID{recoveryAssets[0]} },
		"admission-assets":             func(r *RecoveryRecord) { r.Epochs[3].Assets = []semreg.AssetID{recoveryAssets[0]} },
		"legacy-regime":                func(r *RecoveryRecord) { r.Regimes["source:restore"] = LegacyGrowattStorage },
		"nonadjacent-pair": func(r *RecoveryRecord) {
			r.Epochs = append(r.Epochs[:3], append([]EpochRecord{r.Epochs[1]}, r.Epochs[3:]...)...)
		},
		"skipped-first-ordinal": func(r *RecoveryRecord) {
			r.Epochs[2].Ordinal, r.Epochs[3].Ordinal = 2, 2
			r.Epochs[3].Epoch = successorEpoch(r.Epochs[3].Incarnation, r.Epochs[3].Source, 2)
		},
		"duplicate-pair": func(r *RecoveryRecord) { r.Epochs = append(r.Epochs, r.Epochs[2:4]...) },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			origin := recoveryHub(t, nil, func(ControlEvent) error { return nil })
			initial := admitNativeAs(t, origin, "source:restore", "synthetic-family")
			mustRetire(t, origin, initial, 1)
			mustSucceed(t, origin, initial, "restart-1")
			record := origin.ExportRecovery()
			if len(record.Epochs) != 4 {
				t.Fatalf("unexpected fixture history: %+v", record.Epochs)
			}
			mutate(&record)
			fresh := recoveryHub(t, nil, nil)
			if err := fresh.Restore(record); !errors.Is(err, ErrRestore) {
				t.Fatalf("malformed succession accepted: %v", err)
			}
			if len(fresh.ExportRecovery().Epochs) != 0 || len(fresh.restoredEpochs) != 0 {
				t.Fatal("invalid succession retained or reserved")
			}
			for _, id := range recoveryAssets {
				if fresh.Readable(id) {
					t.Fatalf("invalid checkpoint left %s readable", id)
				}
			}
		})
	}
}

func TestRestoreSuccessionOrdinalsArePerSource(t *testing.T) {
	origin := recoveryHub(t, nil, func(ControlEvent) error { return nil })
	for _, id := range []semreg.SourceID{"source:restore", "source:other"} {
		initial := admitNativeAs(t, origin, id, "synthetic-family")
		mustRetire(t, origin, initial, 1)
		next := mustSucceed(t, origin, initial, "restart-1")
		mustRetire(t, origin, next, 2)
		mustSucceed(t, origin, next, "restart-2")
	}
	record := origin.ExportRecovery()
	fresh := recoveryHub(t, nil, func(ControlEvent) error { return nil })
	if err := fresh.Restore(record); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(record.Epochs, fresh.ExportRecovery().Epochs) {
		t.Fatal("valid succession history changed on restore")
	}
}
