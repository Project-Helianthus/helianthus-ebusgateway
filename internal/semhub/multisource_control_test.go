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

// The two-contributor fixture: owner A and declared contributor B publish on
// one shared asset. Each declared reprojection callback (A's as the owner's,
// B's as the contributor's) records every control event it receives and can
// be told to block or fail on the events of one controlling source.
type multiSourceFixture struct {
	hub  *Hub
	a, b *Handle

	mu       sync.Mutex
	seen     map[string][]ControlEvent
	fail     map[semreg.SourceID]error
	entered  map[semreg.SourceID]chan struct{}
	released map[semreg.SourceID]chan struct{}
}

const (
	ownerRecipient = "owner"
	peerRecipient  = "peer"
)

func newMultiSourceFixture(t *testing.T) *multiSourceFixture {
	t.Helper()
	f := &multiSourceFixture{hub: newTestHub(t), seen: map[string][]ControlEvent{}, fail: map[semreg.SourceID]error{}, entered: map[semreg.SourceID]chan struct{}{}, released: map[semreg.SourceID]chan struct{}{}}
	peer := peerSpec()
	peer.OnControl = f.callback(peerRecipient)
	spec := sharedSpec(peer)
	spec.OnControl = f.callback(ownerRecipient)
	if _, err := f.hub.Declare(spec); err != nil {
		t.Fatal(err)
	}
	f.a = admitLegacy(t, f.hub, sharedOwner, sharedDriver, "epoch:a", 1)
	f.b = admitLegacy(t, f.hub, sharedPeer, peerDriver, "epoch:b", 1)
	return f
}

// callback returns the reprojection callback of one recipient. Only the
// owner's callback blocks, so one control event blocks exactly once.
func (f *multiSourceFixture) callback(recipient string) func(ControlEvent) error {
	return func(event ControlEvent) error {
		f.mu.Lock()
		f.seen[recipient] = append(f.seen[recipient], event)
		err := f.fail[event.Control.Source]
		entered, released := f.entered[event.Control.Source], f.released[event.Control.Source]
		if recipient == ownerRecipient && entered != nil {
			delete(f.entered, event.Control.Source)
		} else {
			entered, released = nil, nil
		}
		f.mu.Unlock()
		if entered != nil {
			close(entered)
			<-released
		}
		return err
	}
}

// hold makes the owner's callback block on the next control event of src. It
// returns a channel closed when the callback is entered and the release.
func (f *multiSourceFixture) hold(t *testing.T, src semreg.SourceID) (<-chan struct{}, func()) {
	t.Helper()
	entered, released := make(chan struct{}), make(chan struct{})
	f.mu.Lock()
	f.entered[src], f.released[src] = entered, released
	f.mu.Unlock()
	var once sync.Once
	release := func() { once.Do(func() { close(released) }) }
	t.Cleanup(release)
	return entered, release
}

func (f *multiSourceFixture) setFail(src semreg.SourceID, err error) {
	f.mu.Lock()
	f.fail[src] = err
	f.mu.Unlock()
}

func (f *multiSourceFixture) events(recipient string) []ControlEvent {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]ControlEvent(nil), f.seen[recipient]...)
}

func (f *multiSourceFixture) publishBoth(t *testing.T, at int64) {
	t.Helper()
	mustPublish(t, f.hub, f.a, sharedAsset, "fam_a", 1, at)
	mustPublish(t, f.hub, f.b, sharedAsset, "fam_b", 2, at)
}

// interruptNextSubmit makes the next control submission on the shared asset
// stop before its kernel swap, leaving that control's outcome uncertain.
func interruptNextSubmit(hub *Hub) {
	hub.faults = func(stage string, id semreg.AssetID) {
		if stage == "before-swap" && id == sharedAsset {
			hub.faults = nil
			panic("injected interruption")
		}
	}
}

func cursorState(t *testing.T, hub *Hub, src semreg.SourceID, epoch semreg.SourceEpochID, generation semreg.Uint64) (found, fenced bool) {
	t.Helper()
	view, err := hub.Inspect(sharedAsset)
	if err != nil {
		t.Fatal(err)
	}
	cursor, ok := cursorFor(view.Snapshot, src, epoch, generation)
	return ok, cursor.Fenced
}

// captureDecision is the admission decision of one captured asset.
type captureDecision struct {
	Source, Read, Public bool
	Reason               string
	Snapshot             bool
}

func decisionOf(item CapturedAsset) captureDecision {
	return captureDecision{Source: item.SourceAdmitted, Read: item.ReadAdmitted, Public: item.PublicAdmitted, Reason: item.UnavailableReason, Snapshot: item.Snapshot != nil}
}

// bothCaptures captures id through Capture and CaptureExact and requires the
// same decision from both.
func bothCaptures(t *testing.T, hub *Hub, id semreg.AssetID) (CapturedAsset, captureDecision) {
	t.Helper()
	coherent, err := hub.Capture(context.Background(), []semreg.AssetID{id}, func() time.Time { return time.Unix(1800000100, 0) })
	if err != nil {
		t.Fatal(err)
	}
	exact, err := hub.CaptureExact(context.Background(), []semreg.AssetID{id}, func() (CaptureClockSample, error) {
		return CaptureClockSample{Wall: testWall(100), Monotonic: map[semreg.ClockEpochID]semreg.MonotonicPoint{testMono(0).ClockEpochID: testMono(100)}}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	got, again := decisionOf(coherent.Assets[0]), decisionOf(exact.Assets[0])
	if got != again {
		t.Fatalf("Capture=%+v CaptureExact=%+v", got, again)
	}
	return coherent.Assets[0], got
}

// (a) A fence of A that has not reached a semantic outcome blocks A's own
// publications only: B publishes on the shared asset meanwhile.
func TestMultiSourceControlPeerPublishesWhileFencePending(t *testing.T) {
	f := newMultiSourceFixture(t)
	f.publishBoth(t, 1)
	interruptNextSubmit(f.hub)
	result, err := f.hub.Fence(testFence(sharedOwner, "epoch:a", 1, 2))
	if err == nil || len(result.Assets) != 1 || result.Assets[0].Outcome != SemanticUncertain {
		t.Fatalf("interrupted fence=%+v err=%v", result.Assets, err)
	}
	if _, err := f.hub.Begin(f.a, sharedAsset); !errors.Is(err, ErrControlPending) {
		t.Fatalf("owner publication behind its own pending control=%v", err)
	}
	if outcome, err := publish(t, f.hub, f.b, sharedAsset, "fam_b", 3, 3); err != nil || outcome != Accepted {
		t.Fatalf("peer publication while the owner's fence is pending=%v/%v", outcome, err)
	}
	if f.hub.Admitted(sharedAsset) || f.hub.ReadAdmitted(sharedAsset) {
		t.Fatal("reads admitted while the owner's control is pending")
	}
	// The retained attempt predates B's publication: it resolves through a
	// proven revision conflict and a rebuilt attempt.
	var retried ControlResult
	for attempt := 0; attempt < 3; attempt++ {
		if retried, err = f.hub.RetryControl(result.ID); err == nil {
			break
		}
	}
	if err != nil || !semanticSuccess(retried.Assets[0].Outcome) {
		t.Fatalf("retry=%+v err=%v", retried.Assets, err)
	}
	waitFor(t, func() bool { return f.hub.Admitted(sharedAsset) })
	if found, fenced := cursorState(t, f.hub, sharedOwner, "epoch:a", "1"); !found || !fenced {
		t.Fatal("the owner's generation is not fenced")
	}
	if found, fenced := cursorState(t, f.hub, sharedPeer, "epoch:b", "1"); !found || fenced {
		t.Fatal("the peer's generation was fenced by the owner's control")
	}
	mustPublish(t, f.hub, f.b, sharedAsset, "fam_b", 4, 4)
}

// (b) An abandoned control of A keeps A's gate open but does not starve B,
// and B's own open gate never blocks A's next activation.
func TestMultiSourceControlAbandonedControlDoesNotStarvePeer(t *testing.T) {
	f := newMultiSourceFixture(t)
	f.publishBoth(t, 1)
	interruptNextSubmit(f.hub)
	result, _ := f.hub.Fence(testFence(sharedOwner, "epoch:a", 1, 2))
	if err := f.hub.AbandonControl(result.ID); err != nil {
		t.Fatal(err)
	}
	if outcome, err := publish(t, f.hub, f.b, sharedAsset, "fam_b", 3, 3); err != nil || outcome != Accepted {
		t.Fatalf("peer publication behind the owner's abandoned control=%v/%v", outcome, err)
	}
	if f.hub.Admitted(sharedAsset) {
		t.Fatal("reads admitted behind an abandoned control")
	}
	if _, err := f.hub.Activate(f.a, 2); !errors.Is(err, ErrControlAbandoned) {
		t.Fatalf("owner activation behind its own abandoned control=%v", err)
	}

	// B fences its own generation and its reprojection keeps failing: B's
	// gate stays open.
	f.setFail(sharedPeer, errors.New("peer reprojection refused"))
	if _, err := f.hub.Fence(testFence(sharedPeer, "epoch:b", 1, 4)); err != nil {
		t.Fatalf("peer fence=%v", err)
	}
	if _, err := f.hub.Begin(f.b, sharedAsset); !errors.Is(err, ErrReprojectionPending) {
		t.Fatalf("peer publication behind its own unsettled control=%v", err)
	}

	// A's later successful control closes A's abandoned operation; A then
	// activates and publishes while B's gate is still open.
	if fenced, err := f.hub.Fence(testFence(sharedOwner, "epoch:a", 1, 5)); err != nil || !semanticSuccess(fenced.Assets[0].Outcome) {
		t.Fatalf("owner fence after the abandonment=%+v err=%v", fenced.Assets, err)
	}
	next, err := f.hub.Activate(f.a, 2)
	if err != nil {
		t.Fatalf("owner activation while the peer's gate is open=%v", err)
	}
	mustPublish(t, f.hub, next, sharedAsset, "fam_a", 6, 6)
	if f.hub.Admitted(sharedAsset) || f.hub.Settled(sharedAsset) {
		t.Fatal("reads admitted while the peer's gate is open")
	}
	f.setFail(sharedPeer, nil)
	if err := f.hub.Resettle(sharedAsset); err != nil {
		t.Fatalf("resettle=%v", err)
	}
	if !f.hub.Admitted(sharedAsset) {
		t.Fatal("reads stayed withheld after every gate settled")
	}
	if _, err := f.hub.Activate(f.b, 2); err != nil {
		t.Fatalf("peer activation after settlement=%v", err)
	}
}

// (b) The control of B never takes over the settlement of A's control: A's
// reprojection settles A's own gate and A activates without waiting for B.
func TestMultiSourceControlPeerControlKeepsSettlementOwnership(t *testing.T) {
	f := newMultiSourceFixture(t)
	f.publishBoth(t, 1)
	entered, release := f.hold(t, sharedOwner)
	done := make(chan error, 1)
	go func() {
		_, err := f.hub.Fence(testFence(sharedOwner, "epoch:a", 1, 2))
		done <- err
	}()
	<-entered
	if result, err := f.hub.Fence(testFence(sharedPeer, "epoch:b", 1, 3)); err != nil || !semanticSuccess(result.Assets[0].Outcome) {
		t.Fatalf("peer fence while the owner's reprojection runs=%+v err=%v", result.Assets, err)
	}
	release()
	if err := <-done; err != nil {
		t.Fatalf("owner fence=%v", err)
	}
	start := time.Now()
	nextA, err := f.hub.Activate(f.a, 2)
	if err != nil {
		t.Fatalf("owner activation=%v", err)
	}
	if elapsed := time.Since(start); elapsed >= ControlSettleTimeout {
		t.Fatalf("owner activation waited %v", elapsed)
	}
	nextB, err := f.hub.Activate(f.b, 2)
	if err != nil {
		t.Fatalf("peer activation=%v", err)
	}
	waitFor(t, func() bool { return f.hub.Admitted(sharedAsset) })
	mustPublish(t, f.hub, nextA, sharedAsset, "fam_a", 4, 4)
	mustPublish(t, f.hub, nextB, sharedAsset, "fam_b", 5, 5)
}

// (c) A generation transition of A on another asset covers the shared asset
// with a fence of A's replaced generation only; B keeps publishing.
func TestMultiSourceControlTransitionCoverFencesOnlyItsSource(t *testing.T) {
	f := newMultiSourceFixture(t)
	mustDeclare(t, f.hub, "asset:a-only", sharedOwner, sharedDriver, nil)
	f.publishBoth(t, 1)
	mustPublish(t, f.hub, f.a, "asset:a-only", "fam_a", 1, 1)
	entered, release := f.hold(t, sharedOwner)
	next := transitionGeneration(t, f.hub, f.a, "asset:a-only", 2)
	<-entered
	if outcome, err := publish(t, f.hub, f.b, sharedAsset, "fam_b", 3, 3); err != nil || outcome != Accepted {
		t.Fatalf("peer publication under the owner's transition cover=%v/%v", outcome, err)
	}
	// Both sources are admitted with a current cursor; only A's open gate
	// withholds the asset.
	if _, got := bothCaptures(t, f.hub, sharedAsset); f.hub.Admitted(sharedAsset) || got != (captureDecision{Source: true, Reason: "semantic.read.reason.publication_withheld"}) {
		t.Fatalf("reads under the cover's reprojection=%+v", got)
	}
	// The gate is A's on the shared asset only: A's other asset stays readable
	// and A's new generation publishes there meanwhile.
	if !f.hub.Admitted("asset:a-only") {
		t.Fatal("the cover closed the transitioning asset")
	}
	mustPublish(t, f.hub, next, "asset:a-only", "fam_a", 3, 3)
	if found, fenced := cursorState(t, f.hub, sharedOwner, "epoch:a", "1"); !found || !fenced {
		t.Fatal("the cover did not fence the owner's replaced generation")
	}
	if found, fenced := cursorState(t, f.hub, sharedPeer, "epoch:b", "1"); !found || fenced {
		t.Fatal("the cover fenced the peer's generation")
	}
	view, err := f.hub.Inspect(sharedAsset)
	if err != nil {
		t.Fatal(err)
	}
	for _, fence := range view.Snapshot.Fences {
		if fence.SourceID != sharedOwner || fence.SourceEpochID != "epoch:a" || fence.DriverGeneration != "1" {
			t.Fatalf("the cover fenced another key: %+v", fence)
		}
	}
	if len(view.Snapshot.Fences) != 1 {
		t.Fatalf("fences=%+v", view.Snapshot.Fences)
	}
	release()
	waitFor(t, func() bool { return f.hub.Admitted(sharedAsset) })
	mustPublish(t, f.hub, next, sharedAsset, "fam_a", 4, 4)
	mustPublish(t, f.hub, f.b, sharedAsset, "fam_b", 5, 5)
}

// (d) Reads of the shared asset are withheld while any contributor's gate is
// open, and admitted only once every gate has settled.
func TestMultiSourceControlReadsWithheldWhileAnyGateOpen(t *testing.T) {
	f := newMultiSourceFixture(t)
	f.publishBoth(t, 1)
	if !f.hub.Admitted(sharedAsset) {
		t.Fatal("published shared asset not admitted")
	}
	enteredA, releaseA := f.hold(t, sharedOwner)
	enteredB, releaseB := f.hold(t, sharedPeer)
	done := make(chan error, 1)
	go func() {
		_, err := f.hub.Fence(testFence(sharedOwner, "epoch:a", 1, 2))
		done <- err
	}()
	<-enteredA
	if _, err := f.hub.Fence(testFence(sharedPeer, "epoch:b", 1, 3)); err != nil {
		t.Fatalf("peer fence=%v", err)
	}
	withheld := func(stage string) {
		t.Helper()
		// Both sources are fenced here, so the reason is the source's; the
		// read gate itself stays closed.
		item, decision := bothCaptures(t, f.hub, sharedAsset)
		if f.hub.Admitted(sharedAsset) || f.hub.ReadAdmitted(sharedAsset) || f.hub.Readable(sharedAsset) || decision.Read || decision.Public || decision.Reason == "" || item.Snapshot != nil {
			t.Fatalf("%s: reads admitted with an open gate: %+v", stage, decision)
		}
	}
	withheld("both gates open")
	releaseA()
	<-enteredB
	// A's gate has settled; B's reprojection is still running.
	withheld("peer gate open")
	if f.hub.Settled(sharedAsset) {
		t.Fatal("settled while the peer's reprojection runs")
	}
	releaseB()
	if err := <-done; err != nil {
		t.Fatalf("owner fence=%v", err)
	}
	waitFor(t, func() bool { return f.hub.Admitted(sharedAsset) })
	if !f.hub.Settled(sharedAsset) || !f.hub.ReadAdmitted(sharedAsset) {
		t.Fatal("not settled after every gate settled")
	}
}

// (e) Every control event of the shared asset reaches the owner's and the
// contributor's reprojection callbacks, in HubSeq order.
func TestMultiSourceControlEventReachesEveryContributorInHubSeqOrder(t *testing.T) {
	f := newMultiSourceFixture(t)
	f.publishBoth(t, 1)
	fenceA, err := f.hub.Fence(testFence(sharedOwner, "epoch:a", 1, 2))
	if err != nil {
		t.Fatal(err)
	}
	fenceB, err := f.hub.Fence(testFence(sharedPeer, "epoch:b", 1, 3))
	if err != nil {
		t.Fatal(err)
	}
	nextA, err := f.hub.Activate(f.a, 2)
	if err != nil {
		t.Fatal(err)
	}
	mustPublish(t, f.hub, nextA, sharedAsset, "fam_a", 3, 4)
	if _, err := f.hub.RetireEpoch(RetireRequest{Source: sharedOwner, Epoch: "epoch:a", Reason: "lifecycle.retired", Evidence: []semreg.EvidenceRef{testEvidence("semhubtest.retire")}, ObservedAt: testWall(5), Monotonic: testMono(5)}); err != nil {
		t.Fatal(err)
	}
	want := []string{
		fmt.Sprintf("%s/%d/%d", sharedOwner, fenceA.ID.N, fenceA.Assets[0].HubSeq),
		fmt.Sprintf("%s/%d/%d", sharedPeer, fenceB.ID.N, fenceB.Assets[0].HubSeq),
	}
	for _, recipient := range []string{ownerRecipient, peerRecipient} {
		events := f.events(recipient)
		if len(events) != 3 {
			t.Fatalf("%s received %d control events", recipient, len(events))
		}
		for index, event := range events {
			if event.Asset != sharedAsset || index > 0 && event.HubSeq <= events[index-1].HubSeq {
				t.Fatalf("%s events out of HubSeq order: %+v", recipient, events)
			}
			if index < len(want) && fmt.Sprintf("%s/%d/%d", event.Control.Source, event.Control.N, event.HubSeq) != want[index] {
				t.Fatalf("%s event %d=%s/%d/%d want %s", recipient, index, event.Control.Source, event.Control.N, event.HubSeq, want[index])
			}
		}
		if events[0].Kind != EventGenerationFenced || events[2].Kind != EventSourceRetired || events[2].Control.Source != sharedOwner {
			t.Fatalf("%s event kinds: %+v", recipient, events)
		}
	}
	// Both recipients received the same events.
	if owner, peer := f.events(ownerRecipient), f.events(peerRecipient); !reflect.DeepEqual(owner, peer) {
		t.Fatal("the owner and the contributor received different control events")
	}
}

// (f) A shared asset is admitted while at least one contributor is admitted
// with a published current cursor; every contributor's state stays visible.
func TestMultiSourceControlCaptureAdmitsThroughAnyContributor(t *testing.T) {
	f := newMultiSourceFixture(t)
	unavailable := captureDecision{Source: true, Read: true, Reason: "semantic.read.reason.snapshot_unavailable"}
	if _, got := bothCaptures(t, f.hub, sharedAsset); got != unavailable {
		t.Fatalf("unpublished=%+v", got)
	}
	admitted := captureDecision{Source: true, Read: true, Public: true, Snapshot: true}
	// Only the contributor has published: admitted through B.
	mustPublish(t, f.hub, f.b, sharedAsset, "fam_b", 2, 1)
	if _, got := bothCaptures(t, f.hub, sharedAsset); got != admitted {
		t.Fatalf("contributor only=%+v", got)
	}
	mustPublish(t, f.hub, f.a, sharedAsset, "fam_a", 1, 2)
	if _, err := f.hub.Fence(testFence(sharedOwner, "epoch:a", 1, 3)); err != nil {
		t.Fatal(err)
	}
	item, got := bothCaptures(t, f.hub, sharedAsset)
	if got != admitted {
		t.Fatalf("owner fenced=%+v", got)
	}
	audit, err := f.hub.Inspect(sharedAsset)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(*item.Snapshot, audit.Snapshot) {
		t.Fatal("the captured snapshot differs from the stored one")
	}
	descriptors := map[semreg.SourceID]semreg.SourceState{}
	for _, descriptor := range item.Snapshot.Sources {
		descriptors[descriptor.SourceID] = descriptor.State
	}
	if len(descriptors) != 2 || descriptors[sharedOwner] != semreg.SourceCurrent || descriptors[sharedPeer] != semreg.SourceCurrent {
		t.Fatalf("source descriptors=%v", descriptors)
	}
	owner, _ := cursorFor(*item.Snapshot, sharedOwner, "epoch:a", "1")
	peer, _ := cursorFor(*item.Snapshot, sharedPeer, "epoch:b", "1")
	if !owner.Fenced || peer.Fenced || peer.LastSequence != "1" {
		t.Fatalf("cursors owner=%+v peer=%+v", owner, peer)
	}
	// Both contributors fenced: no contributor is admitted.
	if _, err := f.hub.Fence(testFence(sharedPeer, "epoch:b", 1, 4)); err != nil {
		t.Fatal(err)
	}
	if _, got := bothCaptures(t, f.hub, sharedAsset); got != (captureDecision{Read: true, Reason: "semantic.read.reason.source_unavailable"}) {
		t.Fatalf("both fenced=%+v", got)
	}
	// A successor generation without a publication never exposes the
	// predecessor's snapshot.
	nextA, err := f.hub.Activate(f.a, 2)
	if err != nil {
		t.Fatal(err)
	}
	if _, got := bothCaptures(t, f.hub, sharedAsset); got != (captureDecision{Source: true, Read: true, Reason: "semantic.read.reason.publication_withheld"}) {
		t.Fatalf("owner successor unpublished=%+v", got)
	}
	mustPublish(t, f.hub, nextA, sharedAsset, "fam_a", 3, 5)
	if _, got := bothCaptures(t, f.hub, sharedAsset); got != admitted {
		t.Fatalf("owner successor published=%+v", got)
	}
	// The contributor's fenced generation stays visible beside the owner's
	// current one.
	item, _ = bothCaptures(t, f.hub, sharedAsset)
	if peer, ok := cursorFor(*item.Snapshot, sharedPeer, "epoch:b", "1"); !ok || !peer.Fenced {
		t.Fatalf("the contributor's fenced cursor is not visible: %+v", peer)
	}
}

// (g) For single-source assets the capture decision and reason are the ones
// the hub returned before contributors had their own gates. Each row was
// recorded on the code before this change; the evaluation is checked against
// the stored snapshot.
func TestMultiSourceControlSingleSourceCaptureUnchanged(t *testing.T) {
	const id, src = semreg.AssetID("asset:single"), semreg.SourceID("source:single")
	type step func(t *testing.T, h *Hub, handle **Handle)
	fence := func(generation uint64) step {
		return func(t *testing.T, h *Hub, handle **Handle) {
			if _, err := h.Fence(testFence(src, "epoch:single", generation, 10)); err != nil {
				t.Fatal(err)
			}
		}
	}
	activate := func(t *testing.T, h *Hub, handle **Handle) {
		next, err := h.Activate(*handle, (*handle).Generation()+1)
		if err != nil {
			t.Fatal(err)
		}
		*handle = next
	}
	publishStep := func(t *testing.T, h *Hub, handle **Handle) { mustPublish(t, h, *handle, id, "single", 5, 11) }
	retire := func(t *testing.T, h *Hub, handle **Handle) {
		if _, err := h.RetireEpoch(RetireRequest{Source: src, Epoch: "epoch:single", Reason: "lifecycle.retired", Evidence: []semreg.EvidenceRef{testEvidence("semhubtest.retire")}, ObservedAt: testWall(12), Monotonic: testMono(12)}); err != nil {
			t.Fatal(err)
		}
	}
	interrupted := func(t *testing.T, h *Hub, handle **Handle) {
		h.faults = func(stage string, at semreg.AssetID) {
			if stage == "before-swap" && at == id {
				h.faults = nil
				panic("injected interruption")
			}
		}
		if _, err := h.Fence(testFence(src, "epoch:single", (*handle).Generation(), 10)); err == nil {
			t.Fatal("interrupted fence reported success")
		}
	}
	restoreFailed := func(t *testing.T, h *Hub, handle **Handle) {
		h.mu.Lock()
		h.assets[id].restoreFailed = true
		h.mu.Unlock()
	}
	withheld := captureDecision{Source: true, Read: true, Reason: "semantic.read.reason.publication_withheld"}
	cases := []struct {
		name      string
		admit     bool
		published bool
		steps     []step
		want      captureDecision
	}{
		{"not admitted", false, false, nil, captureDecision{Read: true, Reason: "semantic.read.reason.source_unavailable"}},
		{"admitted unpublished", true, false, nil, captureDecision{Source: true, Read: true, Reason: "semantic.read.reason.snapshot_unavailable"}},
		{"published", true, true, nil, captureDecision{Source: true, Read: true, Public: true, Snapshot: true}},
		{"fenced", true, true, []step{fence(1)}, captureDecision{Read: true, Reason: "semantic.read.reason.source_unavailable"}},
		{"fenced before publication", true, false, []step{fence(1)}, captureDecision{Read: true, Reason: "semantic.read.reason.source_unavailable"}},
		{"successor unpublished", true, true, []step{fence(1), activate}, withheld},
		{"successor published", true, true, []step{fence(1), activate, publishStep}, captureDecision{Source: true, Read: true, Public: true, Snapshot: true}},
		{"control pending", true, true, []step{interrupted}, captureDecision{Reason: "semantic.read.reason.source_unavailable"}},
		{"retired", true, true, []step{retire}, captureDecision{Read: true, Reason: "semantic.read.reason.source_unavailable"}},
		{"restore failed", true, true, []step{restoreFailed}, captureDecision{Source: true, Reason: "semantic.read.reason.publication_withheld"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newTestHub(t)
			mustDeclare(t, h, id, src, "single", nil)
			var handle *Handle
			if tc.admit {
				handle = admitLegacy(t, h, src, "single", "epoch:single", 1)
			}
			if tc.published {
				mustPublish(t, h, handle, id, "single", 1, 1)
			}
			for _, apply := range tc.steps {
				apply(t, h, &handle)
			}
			item, got := bothCaptures(t, h, id)
			if got != tc.want {
				t.Fatalf("decision=%+v want %+v", got, tc.want)
			}
			if item.Snapshot != nil {
				audit, err := h.Inspect(id)
				if err != nil || !reflect.DeepEqual(*item.Snapshot, audit.Snapshot) {
					t.Fatalf("captured snapshot differs from the stored one: %v", err)
				}
			}
		})
	}

	// A transition cover on the source's other asset withholds that asset
	// while its reprojection runs, exactly as before.
	h := newTestHub(t)
	entered, released := make(chan struct{}), make(chan struct{})
	var once sync.Once
	mustDeclare(t, h, id, src, "single", nil)
	mustDeclare(t, h, "asset:single-other", src, "single", func(ControlEvent) error {
		once.Do(func() { close(entered); <-released })
		return nil
	})
	defer func() {
		select {
		case <-released:
		default:
			close(released)
		}
	}()
	handle := admitLegacy(t, h, src, "single", "epoch:single", 1)
	mustPublish(t, h, handle, id, "single", 1, 1)
	mustPublish(t, h, handle, "asset:single-other", "single", 1, 1)
	transitionGeneration(t, h, handle, id, 2)
	<-entered
	if _, got := bothCaptures(t, h, "asset:single-other"); got != (captureDecision{Source: true, Reason: "semantic.read.reason.publication_withheld"}) {
		t.Fatalf("cover pending=%+v", got)
	}
	close(released)
	waitFor(t, func() bool { return h.Admitted("asset:single-other") })
	if _, got := bothCaptures(t, h, "asset:single-other"); got != withheld {
		t.Fatalf("cover settled, successor unpublished=%+v", got)
	}
	coherent, err := h.Capture(context.Background(), []semreg.AssetID{"asset:unknown"}, func() time.Time { return time.Unix(1800000100, 0) })
	if err != nil || decisionOf(coherent.Assets[0]) != (captureDecision{Reason: "semantic.read.reason.asset_unknown"}) {
		t.Fatalf("unknown asset=%+v err=%v", coherent.Assets, err)
	}
}

// Two contributors run their own publication, fence and activation cycles on
// one shared asset concurrently with readers, under the race detector.
func TestMultiSourceControlConcurrentContributorCycles(t *testing.T) {
	f := newMultiSourceFixture(t)
	f.publishBoth(t, 1)
	cycles := 3
	if testing.Short() {
		cycles = 2
	}
	stop := make(chan struct{})
	var readers sync.WaitGroup
	readers.Add(1)
	go func() {
		defer readers.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			f.hub.Admitted(sharedAsset)
			f.hub.Settled(sharedAsset)
			if _, err := f.hub.Capture(context.Background(), []semreg.AssetID{sharedAsset}, func() time.Time { return time.Unix(1800000100, 0) }); err != nil {
				t.Error(err)
				return
			}
			if _, err := f.hub.Inspect(sharedAsset); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	run := func(handle *Handle, src semreg.SourceID, epoch semreg.SourceEpochID, family string, errs chan<- error) {
		at := int64(10)
		for cycle := 0; cycle < cycles; cycle++ {
			for n := 0; n < 3; n++ {
				at++
				outcome, err := publish(t, f.hub, handle, sharedAsset, family, at, at)
				if err != nil || outcome != Accepted {
					errs <- fmt.Errorf("%s publish: %v/%v", src, outcome, err)
					return
				}
			}
			at++
			if _, err := f.hub.Fence(testFence(src, epoch, handle.Generation(), at)); err != nil {
				errs <- fmt.Errorf("%s fence: %w", src, err)
				return
			}
			next, err := f.hub.Activate(handle, handle.Generation()+1)
			if err != nil {
				errs <- fmt.Errorf("%s activate: %w", src, err)
				return
			}
			handle = next
		}
		errs <- nil
	}
	errs := make(chan error, 2)
	go run(f.a, sharedOwner, "epoch:a", "fam_a", errs)
	go run(f.b, sharedPeer, "epoch:b", "fam_b", errs)
	for range 2 {
		if err := <-errs; err != nil {
			t.Error(err)
		}
	}
	close(stop)
	readers.Wait()
	waitFor(t, func() bool { return f.hub.Admitted(sharedAsset) })
	for _, recipient := range []string{ownerRecipient, peerRecipient} {
		events := f.events(recipient)
		if len(events) != 2*cycles {
			t.Fatalf("%s received %d control events, want %d", recipient, len(events), 2*cycles)
		}
		for index := 1; index < len(events); index++ {
			if events[index].HubSeq <= events[index-1].HubSeq {
				t.Fatalf("%s events out of HubSeq order", recipient)
			}
		}
	}
}

// A retry of one contributor's unsettled reprojection never delivers its older
// snapshot after a newer control event of the asset has been delivered.
func TestMultiSourceControlRetryDoesNotReplayOlderSnapshot(t *testing.T) {
	f := newMultiSourceFixture(t)
	f.publishBoth(t, 1)
	f.setFail(sharedOwner, errors.New("reprojection refused"))
	_, _ = f.hub.Fence(testFence(sharedOwner, "epoch:a", 1, 2))
	f.setFail(sharedOwner, nil)
	if result, err := f.hub.Fence(testFence(sharedPeer, "epoch:b", 1, 3)); err != nil || !semanticSuccess(result.Assets[0].Outcome) {
		t.Fatalf("peer fence=%+v err=%v", result.Assets, err)
	}
	if err := f.hub.Resettle(sharedAsset); err != nil {
		t.Fatalf("resettle=%v", err)
	}
	for _, recipient := range []string{ownerRecipient, peerRecipient} {
		events := f.events(recipient)
		for index := 1; index < len(events); index++ {
			if events[index].HubSeq < events[index-1].HubSeq {
				t.Fatalf("%s received an older snapshot after a newer one: %+v", recipient, events)
			}
		}
	}
	waitFor(t, func() bool { return f.hub.Settled(sharedAsset) })
}

// A retry of one contributor's unsettled reprojection after a peer
// publication committed on the asset delivers the asset's current snapshot,
// which already holds the control's effect, so the retried callbacks never
// replace the peer's newer view with the older control snapshot.
func TestMultiSourceControlRetryCoalescesOntoPeerPublication(t *testing.T) {
	f := newMultiSourceFixture(t)
	f.publishBoth(t, 1)
	f.setFail(sharedOwner, errors.New("reprojection refused"))
	fenced, _ := f.hub.Fence(testFence(sharedOwner, "epoch:a", 1, 2))
	if len(fenced.Assets) != 1 || !semanticSuccess(fenced.Assets[0].Outcome) {
		t.Fatalf("owner fence=%+v", fenced.Assets)
	}
	waitFor(t, func() bool { return len(f.events(ownerRecipient)) > 0 })
	if f.hub.Settled(sharedAsset) {
		t.Fatal("the failed reprojection settled")
	}
	f.setFail(sharedOwner, nil)
	tx, err := f.hub.Begin(f.b, sharedAsset)
	if err != nil {
		t.Fatalf("peer publication behind the owner's unsettled gate=%v", err)
	}
	defer tx.Abort()
	if _, _, err := tx.Apply(testBatch(t, tx, "fam_b", 5, 3), testMono(3)); err != nil {
		t.Fatal(err)
	}
	var committed Committed
	if outcome, err := tx.Commit(func(c Committed) { committed = c }); err != nil || outcome != Accepted {
		t.Fatalf("peer publication=%v/%v", outcome, err)
	}
	if committed.HubSeq <= fenced.Assets[0].HubSeq {
		t.Fatalf("publication HubSeq %d is not after the control HubSeq %d", committed.HubSeq, fenced.Assets[0].HubSeq)
	}
	before := map[string]int{ownerRecipient: len(f.events(ownerRecipient)), peerRecipient: len(f.events(peerRecipient))}
	if err := f.hub.Resettle(sharedAsset); err != nil {
		t.Fatalf("resettle=%v", err)
	}
	for _, recipient := range []string{ownerRecipient, peerRecipient} {
		retried := f.events(recipient)[before[recipient]:]
		if len(retried) == 0 {
			t.Fatalf("%s received no retried control event", recipient)
		}
		for _, event := range retried {
			if event.Control != fenced.ID {
				t.Fatalf("%s retried event names control %+v, want %+v", recipient, event.Control, fenced.ID)
			}
			if event.HubSeq < committed.HubSeq || event.View.HubSeq < committed.HubSeq || event.SnapshotID != committed.Snapshot.SnapshotID || event.View.Snapshot.SnapshotID != committed.Snapshot.SnapshotID {
				t.Fatalf("%s retried event replays an older view: event=%d/%s view=%d/%s, peer publication=%d/%s", recipient, event.HubSeq, event.SnapshotID, event.View.HubSeq, event.View.Snapshot.SnapshotID, committed.HubSeq, committed.Snapshot.SnapshotID)
			}
			if cursor, ok := cursorFor(event.View.Snapshot, sharedOwner, "epoch:a", "1"); !ok || !cursor.Fenced {
				t.Fatalf("%s retried view lacks the owner's fence", recipient)
			}
		}
	}
	waitFor(t, func() bool { return f.hub.Settled(sharedAsset) && f.hub.Admitted(sharedAsset) })
}
