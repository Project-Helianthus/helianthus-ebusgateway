package semhub

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
)

func m11Capture(t *testing.T, h *Hub, ids ...semreg.AssetID) CoherentCapture {
	t.Helper()
	capture, err := h.Capture(context.Background(), ids, func() time.Time { return time.Unix(1800000004, 0) })
	if err != nil {
		t.Fatal(err)
	}
	return capture
}

func TestM11CaptureDetachedAndUnavailable(t *testing.T) {
	h := newTestHub(t)
	id := semreg.AssetID("asset:capture")
	mustDeclare(t, h, id, "source:capture", "capture", nil)
	if got := m11Capture(t, h, id).Assets[0]; got.UnavailableReason != "semantic.read.reason.source_unavailable" || got.Snapshot != nil {
		t.Fatalf("unadmitted=%+v", got)
	}
	handle := admitLegacy(t, h, "source:capture", "capture", "epoch:capture", 1)
	if got := m11Capture(t, h, id).Assets[0]; got.UnavailableReason != "semantic.read.reason.snapshot_unavailable" {
		t.Fatalf("unpublished=%+v", got)
	}
	mustPublish(t, h, handle, id, "capture", 11, 1)
	before, _ := h.Inspect(id)
	capture := m11Capture(t, h, id, "asset:unknown")
	got := capture.Assets[0]
	if !got.SourceAdmitted || !got.ReadAdmitted || !got.PublicAdmitted || got.UnavailableReason != "" || !reflect.DeepEqual(*got.Snapshot, before.Snapshot) {
		t.Fatalf("capture=%+v", got)
	}
	if got.Evaluation.SnapshotID != got.Snapshot.SnapshotID || got.Evaluation.Revisions != got.Snapshot.Revisions || got.Evaluation.Context != capture.Context {
		t.Fatal("mixed evaluation")
	}
	if capture.Assets[1].UnavailableReason != "semantic.read.reason.asset_unknown" {
		t.Fatal("unknown asset disappeared")
	}
	got.Snapshot.Facts[0].Candidates[0].Evidence[0].Owner = "mutated.owner"
	got.Evaluation.Facts[0].Freshness = semreg.FreshnessExpired
	after, _ := h.Inspect(id)
	if !reflect.DeepEqual(before, after) {
		t.Fatal("capture mutated hub")
	}
	if next := m11Capture(t, h, id); reflect.DeepEqual(next.Assets[0].Snapshot, got.Snapshot) || next.Metadata == capture.Metadata {
		t.Fatal("capture aliases another read")
	}
	if _, err := h.Fence(testFence("source:capture", "epoch:capture", 1, 3)); err != nil {
		t.Fatal(err)
	}
	if got := m11Capture(t, h, id).Assets[0]; got.UnavailableReason != "semantic.read.reason.source_unavailable" || got.Snapshot != nil {
		t.Fatalf("fenced=%+v", got)
	}
	next, err := h.Activate(handle, 2)
	if err != nil {
		t.Fatal(err)
	}
	if got := m11Capture(t, h, id).Assets[0]; got.UnavailableReason != "semantic.read.reason.publication_withheld" || got.Snapshot != nil {
		t.Fatalf("predecessor exposed=%+v", got)
	}
	mustPublish(t, h, next, id, "capture", 22, 4)
	if got := m11Capture(t, h, id).Assets[0]; !got.PublicAdmitted {
		t.Fatalf("successor withheld=%+v", got)
	}
}

func TestM11CaptureAdmissionAndExposure(t *testing.T) {
	for _, name := range []string{"restore failed", "pending semantic", "pending reprojection", "private current", "private retained"} {
		t.Run(name, func(t *testing.T) {
			h := newTestHub(t)
			id := semreg.AssetID("asset:safety")
			mustDeclare(t, h, id, "source:safety", "safety", nil)
			handle := admitLegacy(t, h, "source:safety", "safety", "epoch:safety", 1)
			tx, err := h.Begin(handle, id)
			if err != nil {
				t.Fatal(err)
			}
			batch := testBatch(t, tx, "safety", 1, 1)
			if name == "private current" || name == "private retained" {
				batch.FactUpserts[0].Evidence[0].Access = semreg.EvidenceAccessAuthorized
				batch.FactUpserts[0].Evidence[0].Redaction = semreg.RedactionNone
				batch.BatchDigest, err = batch.ComputedDigest()
				if err != nil {
					t.Fatal(err)
				}
			}
			if _, _, err := tx.Apply(batch, testMono(1)); err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Commit(nil); err != nil {
				t.Fatal(err)
			}
			if name == "private retained" {
				if _, err := h.Fence(testFence("source:safety", "epoch:safety", 1, 2)); err != nil {
					t.Fatal(err)
				}
				next, err := h.Activate(handle, 2)
				if err != nil {
					t.Fatal(err)
				}
				mustPublish(t, h, next, id, "safety", 2, 3)
				audit, err := h.Inspect(id)
				if err != nil {
					t.Fatal(err)
				}
				if len(audit.Snapshot.Retained) != 1 || audit.Snapshot.Retained[0].Candidate.Evidence[0].Redaction != semreg.RedactionNone {
					t.Fatal("fixture lacks private retained evidence")
				}
			}
			h.mu.Lock()
			switch name {
			case "restore failed":
				h.assets[id].restoreFailed = true
			case "pending semantic":
				h.assets[id].gate = &controlAsset{semantic: SemanticUnbuilt, reprojection: ReprojectionSettled}
			case "pending reprojection":
				h.assets[id].gate = &controlAsset{semantic: SemanticAccepted, reprojection: ReprojectionUnsettled}
			}
			h.mu.Unlock()
			if got := m11Capture(t, h, id).Assets[0]; got.UnavailableReason != "semantic.read.reason.publication_withheld" || got.Snapshot != nil || got.Evaluation != nil {
				t.Fatalf("unsafe=%+v", got)
			}
		})
	}
}

func TestM11CaptureSerializesPublishAcrossSelection(t *testing.T) {
	h := newTestHub(t)
	for _, id := range []semreg.AssetID{"asset:first", "asset:second"} {
		mustDeclare(t, h, id, "source:pair", "pair", nil)
	}
	handle := admitLegacy(t, h, "source:pair", "pair", "epoch:pair", 1)
	for _, id := range []semreg.AssetID{"asset:first", "asset:second"} {
		mustPublish(t, h, handle, id, "pair", 1, 1)
	}
	before, _ := h.Inspect("asset:second")
	tx, err := h.Begin(handle, "asset:second")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := tx.Apply(testBatch(t, tx, "pair", 2, 2), testMono(2)); err != nil {
		t.Fatal(err)
	}
	entered, release, committing := make(chan struct{}), make(chan struct{}), make(chan struct{})
	h.faults = func(stage string, id semreg.AssetID) {
		if stage == "capture-read" && id == "asset:first" {
			close(entered)
			<-release
		}
		if stage == "commit" {
			close(committing)
		}
	}
	type result struct {
		capture CoherentCapture
		err     error
	}
	read := make(chan result, 1)
	go func() {
		c, e := h.Capture(context.Background(), []semreg.AssetID{"asset:first", "asset:second"}, func() time.Time { return time.Unix(1800000004, 0) })
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
	if r.err != nil {
		t.Fatal(r.err)
	}
	if r.capture.Assets[1].Snapshot.SnapshotID != before.Snapshot.SnapshotID {
		t.Fatal("publication entered a partial capture")
	}
	if next := m11Capture(t, h, "asset:second"); next.Assets[0].Snapshot.SnapshotID == before.Snapshot.SnapshotID {
		t.Fatal("next request missed publication")
	}
}

func TestM11CaptureErrors(t *testing.T) {
	var absent *Hub
	if _, err := absent.Capture(context.Background(), []semreg.AssetID{"asset:a"}, func() time.Time { return time.Unix(0, 1) }); !errors.Is(err, ErrCaptureUnavailable) {
		t.Fatal(err)
	}
	h := newTestHub(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := h.Capture(ctx, []semreg.AssetID{"asset:a"}, func() time.Time { return time.Unix(0, 1) }); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	for _, ids := range [][]semreg.AssetID{nil, {"asset:a", "asset:a"}, {""}} {
		if _, err := h.Capture(context.Background(), ids, func() time.Time { return time.Unix(0, 1) }); err == nil {
			t.Fatalf("accepted %v", ids)
		}
	}
}

func TestM11CaptureCurrentAdmissionTuple(t *testing.T) {
	for name, mutate := range map[string]func(*source){
		"source":     func(s *source) { s.id = "source:other" },
		"epoch":      func(s *source) { s.epoch = "epoch:other" },
		"generation": func(s *source) { s.generation++ },
	} {
		t.Run(name, func(t *testing.T) {
			h := newTestHub(t)
			mustDeclare(t, h, "asset:tuple", "source:tuple", "tuple", nil)
			handle := admitLegacy(t, h, "source:tuple", "tuple", "epoch:tuple", 1)
			mustPublish(t, h, handle, "asset:tuple", "tuple", 1, 1)
			h.mu.Lock()
			mutate(h.sources["source:tuple"])
			h.mu.Unlock()
			got := m11Capture(t, h, "asset:tuple").Assets[0]
			if got.UnavailableReason != "semantic.read.reason.publication_withheld" || got.Snapshot != nil {
				t.Fatalf("mismatched admission exposed: %+v", got)
			}
		})
	}
}

func TestM11AssetIDsDetachedOrderedSelection(t *testing.T) {
	var absent *Hub
	if got := absent.AssetIDs(); got != nil {
		t.Fatalf("nil hub selection=%v", got)
	}
	h := newTestHub(t)
	if got := h.AssetIDs(); len(got) != 0 {
		t.Fatalf("empty hub selection=%v", got)
	}
	mustDeclare(t, h, "asset:z", "source:selection", "selection", nil)
	mustDeclare(t, h, "asset:a", "source:selection", "selection", nil)
	want := []semreg.AssetID{"asset:a", "asset:z"}
	ids := h.AssetIDs()
	if !reflect.DeepEqual(ids, want) {
		t.Fatalf("selection=%v", ids)
	}
	ids[0] = "asset:mutated"
	if got := h.AssetIDs(); !reflect.DeepEqual(got, want) {
		t.Fatalf("selection aliases hub: %v", got)
	}
	capture := m11Capture(t, h, h.AssetIDs()...)
	for _, asset := range capture.Assets {
		if asset.SourceAdmitted || asset.Snapshot != nil || asset.UnavailableReason != "semantic.read.reason.source_unavailable" {
			t.Fatalf("selection bypassed admission: %+v", asset)
		}
	}
}

func TestM11CaptureSamplesClockUnderReadLock(t *testing.T) {
	h := newTestHub(t)
	mustDeclare(t, h, "asset:clock", "source:clock", "clock", nil)
	handle := admitLegacy(t, h, "source:clock", "clock", "epoch:clock", 1)
	mustPublish(t, h, handle, "asset:clock", "clock", 1, 2)
	calls := 0
	at := time.Unix(1800000004, 0)
	capture, err := h.Capture(context.Background(), []semreg.AssetID{"asset:clock"}, func() time.Time {
		calls++
		if h.mu.TryLock() {
			h.mu.Unlock()
			t.Fatal("clock sampled outside hub read lock")
		}
		return at
	})
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 || capture.Assets[0].Snapshot == nil || capture.Assets[0].Evaluation.Context.EvaluatedAt != capture.Context.EvaluatedAt || capture.Context.EvaluatedAt.UnixNanoseconds != "1800000004000000000" {
		t.Fatalf("incoherent clock capture: %+v, calls=%d", capture, calls)
	}
}
