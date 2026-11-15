package semhub

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
)

// The peer fixture: owner A and declared contributors B and C publish on two
// shared assets. Every reprojection callback records the events it receives,
// keyed by asset and receiving source, and appends one entry to a global log
// so the delivery order across callbacks is observable.
const sharedSecond semreg.AssetID = "asset:shared-second"

type peerFixture struct {
	hub     *Hub
	a, b, c *Handle

	mu      sync.Mutex
	seen    map[string][]ControlEvent
	log     []string
	fail    map[semreg.SourceID]error
	entered map[semreg.SourceID]chan struct{}
	release map[semreg.SourceID]chan struct{}
}

func peerKey(id semreg.AssetID, recipient semreg.SourceID) string {
	return string(id) + "|" + string(recipient)
}

func newPeerFixture(t *testing.T) *peerFixture {
	t.Helper()
	f := &peerFixture{hub: newTestHub(t), seen: map[string][]ControlEvent{}, fail: map[semreg.SourceID]error{}, entered: map[semreg.SourceID]chan struct{}{}, release: map[semreg.SourceID]chan struct{}{}}
	for _, id := range []semreg.AssetID{sharedAsset, sharedSecond} {
		spec := AssetSpec{AssetID: id, SourceID: sharedOwner, Driver: sharedDriver, Validators: []semreg.PackValidator{newTestValidator()}, OnControl: f.callback(sharedOwner),
			Contributors: []ContributorSpec{
				{SourceID: sharedPeer, Driver: peerDriver, Validators: []semreg.PackValidator{newTestValidator()}, OnControl: f.callback(sharedPeer)},
				{SourceID: sharedOther, Driver: otherDriver, Validators: []semreg.PackValidator{newTestValidator()}, OnControl: f.callback(sharedOther)},
			}}
		if _, err := f.hub.Declare(spec); err != nil {
			t.Fatal(err)
		}
	}
	f.a = admitLegacy(t, f.hub, sharedOwner, sharedDriver, "epoch:a", 1)
	f.b = admitLegacy(t, f.hub, sharedPeer, peerDriver, "epoch:b", 1)
	f.c = admitLegacy(t, f.hub, sharedOther, otherDriver, "epoch:c", 1)
	return f
}

// callback returns the reprojection callback of one receiving source. Only
// peer reprojection events (no control identifier) block or fail.
func (f *peerFixture) callback(recipient semreg.SourceID) func(ControlEvent) error {
	return func(event ControlEvent) error {
		f.mu.Lock()
		key := peerKey(event.Asset, recipient)
		f.seen[key] = append(f.seen[key], event)
		f.log = append(f.log, fmt.Sprintf("%s/%s/%d", event.Asset, recipient, event.HubSeq))
		var err error
		var entered, release chan struct{}
		if event.Control == (ControlID{}) {
			err = f.fail[recipient]
			entered, release = f.entered[recipient], f.release[recipient]
			delete(f.entered, recipient)
		}
		f.mu.Unlock()
		if entered != nil {
			close(entered)
			<-release
		}
		return err
	}
}

// hold makes the next peer reprojection of recipient block until released.
func (f *peerFixture) hold(t *testing.T, recipient semreg.SourceID) (<-chan struct{}, func()) {
	t.Helper()
	entered, release := make(chan struct{}), make(chan struct{})
	f.mu.Lock()
	f.entered[recipient], f.release[recipient] = entered, release
	f.mu.Unlock()
	var once sync.Once
	done := func() { once.Do(func() { close(release) }) }
	t.Cleanup(done)
	return entered, done
}

func (f *peerFixture) setFail(recipient semreg.SourceID, err error) {
	f.mu.Lock()
	f.fail[recipient] = err
	f.mu.Unlock()
}

func (f *peerFixture) events(id semreg.AssetID, recipient semreg.SourceID) []ControlEvent {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]ControlEvent(nil), f.seen[peerKey(id, recipient)]...)
}

func (f *peerFixture) takeLog() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := f.log
	f.log = nil
	return out
}

// commit publishes one batch of handle on id and returns what onCommit saw.
func commitPeer(t *testing.T, hub *Hub, handle *Handle, id semreg.AssetID, family string, value, at int64) Committed {
	t.Helper()
	tx, err := hub.Begin(handle, id)
	if err != nil {
		t.Fatalf("begin %s on %s: %v", handle.Source(), id, err)
	}
	defer tx.Abort()
	if _, _, err := tx.Apply(testBatch(t, tx, family, value, at), testMono(at)); err != nil {
		t.Fatal(err)
	}
	var committed Committed
	if outcome, err := tx.Commit(func(c Committed) { committed = c }); err != nil || outcome != Accepted {
		t.Fatalf("commit %s on %s=%v/%v", handle.Source(), id, outcome, err)
	}
	return committed
}

// One commit of a contributor reaches the reprojection callback of every
// other publishing source of that asset exactly once, with the committed
// snapshot and HubSeq, in the order of the asset's publishing sources and
// never the committing source's own callback. Each field of the delivery key
// is varied: the asset (two shared assets), the committing source (each of
// the three) and the HubSeq (successive commits).
func TestPeerReprojectionReachesEveryOtherContributorOnceInHubSeqOrder(t *testing.T) {
	f := newPeerFixture(t)
	type step struct {
		handle *Handle
		asset  semreg.AssetID
		family string
	}
	steps := []step{
		{f.a, sharedAsset, "fam_a"}, {f.b, sharedAsset, "fam_b"}, {f.c, sharedAsset, "fam_c"},
		{f.b, sharedSecond, "fam_b"}, {f.a, sharedSecond, "fam_a"}, {f.a, sharedAsset, "fam_a"},
	}
	sources := []semreg.SourceID{sharedOwner, sharedPeer, sharedOther}
	for index, s := range steps {
		before := map[string]int{}
		for _, id := range []semreg.AssetID{sharedAsset, sharedSecond} {
			for _, recipient := range sources {
				before[peerKey(id, recipient)] = len(f.events(id, recipient))
			}
		}
		at := int64(index + 1)
		committed := commitPeer(t, f.hub, s.handle, s.asset, s.family, at, at)
		var want []string
		for _, id := range []semreg.AssetID{sharedAsset, sharedSecond} {
			for _, recipient := range sources {
				got := f.events(id, recipient)[before[peerKey(id, recipient)]:]
				if id != s.asset || recipient == s.handle.Source() {
					if len(got) != 0 {
						t.Fatalf("step %d: %s on %s received %d events for a commit of %s on %s", index, recipient, id, len(got), s.handle.Source(), s.asset)
					}
					continue
				}
				want = append(want, fmt.Sprintf("%s/%s/%d", id, recipient, committed.HubSeq))
				if len(got) != 1 {
					t.Fatalf("step %d: %s on %s received %d events, want 1", index, recipient, id, len(got))
				}
				event := got[0]
				if event.Asset != s.asset || event.Kind != EventPublished || event.Control != (ControlID{}) || event.HubSeq != committed.HubSeq || event.View.HubSeq != committed.HubSeq || event.SnapshotID != committed.Snapshot.SnapshotID || event.View.Snapshot.SnapshotID != committed.Snapshot.SnapshotID || !event.View.Present {
					t.Fatalf("step %d: %s received %+v, want the committed snapshot %s at HubSeq %d", index, recipient, event, committed.Snapshot.SnapshotID, committed.HubSeq)
				}
			}
		}
		if got := f.takeLog(); fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("step %d: delivery order %v, want %v", index, got, want)
		}
		if !f.hub.Admitted(s.asset) || !f.hub.Settled(s.asset) {
			t.Fatalf("step %d: %s not admitted after its peer reprojection settled", index, s.asset)
		}
	}
	// A control event of one source interleaves with the peer reprojections in
	// HubSeq order for every receiver.
	if _, err := f.hub.Fence(testFence(sharedPeer, "epoch:b", 1, 10)); err != nil {
		t.Fatal(err)
	}
	commitPeer(t, f.hub, f.c, sharedAsset, "fam_c", 11, 11)
	for _, id := range []semreg.AssetID{sharedAsset, sharedSecond} {
		for _, recipient := range sources {
			events := f.events(id, recipient)
			for index := 1; index < len(events); index++ {
				if events[index].HubSeq <= events[index-1].HubSeq {
					t.Fatalf("%s on %s received events out of HubSeq order: %d after %d", recipient, id, events[index].HubSeq, events[index-1].HubSeq)
				}
			}
		}
	}
}

// Reads of the asset stay withheld while a peer reprojection is running or
// has failed, without refusing any source's publication, and are admitted
// once every receiver acknowledged the asset's latest snapshot.
func TestPeerReprojectionGatesReadsUntilSettled(t *testing.T) {
	f := newPeerFixture(t)
	commitPeer(t, f.hub, f.a, sharedAsset, "fam_a", 1, 1)
	commitPeer(t, f.hub, f.b, sharedAsset, "fam_b", 2, 2)
	if !f.hub.Admitted(sharedAsset) {
		t.Fatal("published shared asset not admitted")
	}
	entered, release := f.hold(t, sharedPeer)
	done := make(chan Committed, 1)
	go func() { done <- commitPeer(t, f.hub, f.a, sharedAsset, "fam_a", 3, 3) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the contributor's peer reprojection callback was not entered")
	}
	withheld := func(stage string) {
		t.Helper()
		item, decision := bothCaptures(t, f.hub, sharedAsset)
		if f.hub.Admitted(sharedAsset) || f.hub.ReadAdmitted(sharedAsset) || f.hub.Readable(sharedAsset) || f.hub.Settled(sharedAsset) || decision.Read || decision.Public || decision.Reason != "semantic.read.reason.publication_withheld" || item.Snapshot != nil {
			t.Fatalf("%s: reads admitted before the peer reprojection settled: %+v", stage, decision)
		}
	}
	withheld("receiver running")
	// A publication on the other shared asset is accepted and returns while
	// the scheduler is busy; its peer reprojection runs after the held one,
	// in HubSeq order.
	second := commitPeer(t, f.hub, f.a, sharedSecond, "fam_a", 3, 3)
	// No publication is refused while a peer reprojection runs, the
	// receiver's own included.
	latest := commitPeer(t, f.hub, f.b, sharedAsset, "fam_b", 4, 4)
	withheld("receiver running after its own commit")
	release()
	first := <-done
	waitFor(t, func() bool {
		return f.hub.Admitted(sharedAsset) && f.hub.Settled(sharedAsset) && f.hub.Admitted(sharedSecond) && f.hub.Settled(sharedSecond)
	})
	for _, recipient := range []semreg.SourceID{sharedPeer, sharedOther} {
		if events := f.events(sharedSecond, recipient); len(events) != 1 || events[0].HubSeq != second.HubSeq {
			t.Fatalf("%s on %s received %+v, want one event at HubSeq %d", recipient, sharedSecond, events, second.HubSeq)
		}
	}
	for _, recipient := range []semreg.SourceID{sharedOwner, sharedPeer, sharedOther} {
		events := f.events(sharedAsset, recipient)
		if last := events[len(events)-1]; last.HubSeq != latest.HubSeq || last.SnapshotID != latest.Snapshot.SnapshotID {
			t.Fatalf("%s left on HubSeq %d, want the latest commit %d", recipient, last.HubSeq, latest.HubSeq)
		}
	}
	if got := f.events(sharedAsset, sharedPeer); got[len(got)-2].HubSeq != first.HubSeq {
		t.Fatalf("the held receiver did not first receive the commit it was owed: %d, want %d", got[len(got)-2].HubSeq, first.HubSeq)
	}

	// A failing receiver keeps reads withheld until Resettle delivers again.
	refused := errors.New("receiver refused")
	f.setFail(sharedOther, refused)
	failed := commitPeer(t, f.hub, f.a, sharedAsset, "fam_a", 5, 5)
	withheld("receiver failed")
	if err := f.hub.Resettle(sharedAsset); !errors.Is(err, refused) {
		t.Fatalf("resettle while the receiver fails=%v", err)
	}
	withheld("receiver still failing")
	commitPeer(t, f.hub, f.b, sharedAsset, "fam_b", 6, 6)
	f.setFail(sharedOther, nil)
	before := len(f.events(sharedAsset, sharedOther))
	if err := f.hub.Resettle(sharedAsset); err != nil {
		t.Fatalf("resettle=%v", err)
	}
	if !f.hub.Admitted(sharedAsset) || !f.hub.Settled(sharedAsset) {
		t.Fatal("reads withheld after the failed receiver settled")
	}
	retried := f.events(sharedAsset, sharedOther)[before:]
	if len(retried) != 1 || retried[0].HubSeq <= failed.HubSeq {
		t.Fatalf("resettle delivered %+v, want one event newer than %d", retried, failed.HubSeq)
	}
	if err := f.hub.Resettle(sharedAsset); err != nil || len(f.events(sharedAsset, sharedOther)) != before+1 {
		t.Fatalf("a settled asset redelivered: err=%v", err)
	}
}

// A single-source asset never queues a peer reprojection: its callback is
// not called on publication, the scheduler queue stays empty at the commit's
// onCommit, and the stream carries only the publication event. The same
// commit shape on a shared asset of the same hub queues one entry and runs
// the contributor's callback once, never the committing owner's.
func TestPeerReprojectionNeverQueuedForASingleSourceAsset(t *testing.T) {
	hub := newTestHub(t)
	var mu sync.Mutex
	calls := map[string]int{}
	count := func(label string) func(ControlEvent) error {
		return func(ControlEvent) error {
			mu.Lock()
			calls[label]++
			mu.Unlock()
			return nil
		}
	}
	mustDeclare(t, hub, "asset:single", sharedOwner, sharedDriver, count("single/owner"))
	peer := peerSpec()
	peer.OnControl = count("shared/peer")
	spec := sharedSpec(peer)
	spec.OnControl = count("shared/owner")
	if _, err := hub.Declare(spec); err != nil {
		t.Fatal(err)
	}
	a := admitLegacy(t, hub, sharedOwner, sharedDriver, "epoch:a", 1)
	hub.mu.RLock()
	singlePeers := hub.assets["asset:single"].peers
	hub.mu.RUnlock()
	if singlePeers != nil {
		t.Fatal("a single-source asset carries peer reprojection state")
	}
	for index, c := range []struct {
		asset  semreg.AssetID
		queued int
		calls  map[string]int
	}{
		{"asset:single", 0, map[string]int{}},
		{"asset:single", 0, map[string]int{}},
		{sharedAsset, 1, map[string]int{"shared/peer": 1}},
	} {
		at := int64(index + 1)
		cursor := hub.Cursor()
		tx, err := hub.Begin(a, c.asset)
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := tx.Apply(testBatch(t, tx, "fam_a", at, at), testMono(at)); err != nil {
			t.Fatal(err)
		}
		queued := -1
		if outcome, err := tx.Commit(func(Committed) {
			hub.mu.RLock()
			queued = len(hub.queue)
			hub.mu.RUnlock()
		}); err != nil || outcome != Accepted {
			t.Fatalf("commit on %s=%v/%v", c.asset, outcome, err)
		}
		if queued != c.queued {
			t.Fatalf("%s: %d entries queued at onCommit, want %d", c.asset, queued, c.queued)
		}
		events, err := hub.Since(cursor)
		if err != nil {
			t.Fatal(err)
		}
		if len(events) != 1 || events[0].Kind != EventPublished || events[0].Asset != c.asset {
			t.Fatalf("%s: commit emitted %+v, want one publication event", c.asset, events)
		}
		mu.Lock()
		got := fmt.Sprint(calls)
		mu.Unlock()
		if got != fmt.Sprint(c.calls) {
			t.Fatalf("%s: reprojection calls %s, want %s", c.asset, got, fmt.Sprint(c.calls))
		}
		hub.mu.RLock()
		left := len(hub.queue)
		hub.mu.RUnlock()
		if left != 0 || !hub.Admitted(c.asset) {
			t.Fatalf("%s: %d entries left after Commit returned, admitted=%v", c.asset, left, hub.Admitted(c.asset))
		}
	}
}

// The pinned single-publisher event stream is byte-identical.
func TestPeerReprojectionKeepsTheScriptedEventStreamGolden(t *testing.T) {
	TestScriptedEventStreamGolden(t)
}
