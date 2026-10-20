package runtimecontrol

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/drivermanager"
)

var errP10CommitFailure = errors.New("synthetic pre-persistence failure")

type p10Store struct {
	Store
	mu             sync.Mutex
	record         ControlRecord
	revision       CatalogRevision
	fail           bool
	adopted        bool
	afterPlan      func()
	driverBarrier  func()
	catalogBarrier func()
}

func (p *p10Store) AdmissionOpen() bool { return true }
func (p *p10Store) Driver(id DriverID) (ControlRecord, error) {
	p.mu.Lock()
	barrier := p.driverBarrier
	p.driverBarrier = nil
	p.mu.Unlock()
	if barrier != nil {
		barrier()
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if id != p.record.DriverID {
		return ControlRecord{}, errors.New("unknown synthetic family")
	}
	r := p.record
	r.Current, r.Reserved = cloneTransition(r.Current), cloneTransition(r.Reserved)
	return r, nil
}
func (p *p10Store) Catalog() (ControlCatalog, error) {
	p.mu.Lock()
	barrier := p.catalogBarrier
	p.catalogBarrier = nil
	p.mu.Unlock()
	if barrier != nil {
		barrier()
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	r := p.record
	r.Current, r.Reserved = cloneTransition(r.Current), cloneTransition(r.Reserved)
	return ControlCatalog{CatalogRevision: p.revision, Drivers: []ControlRecord{r}}, nil
}
func (p *p10Store) AdvanceCatalog(context.Context) (CatalogRevision, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.revision++
	return p.revision, nil
}
func (p *p10Store) CommitWork(_ context.Context, id DriverID, current, reserved *Transition) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if id != p.record.DriverID {
		return errors.New("wrong family")
	}
	p.record.Current, p.record.Reserved = cloneTransition(current), cloneTransition(reserved)
	return nil
}
func (p *p10Store) Decide(_ context.Context, method Method, req Request, planner Planner) (DesiredView, error) {
	p.mu.Lock()
	if req.ExpectedRevision != p.record.DesiredRevision {
		p.mu.Unlock()
		return DesiredView{}, &Refusal{Code: RefusalRevisionConflict}
	}
	plan, refusal := planner(PlanInput{Method: method, DriverID: req.DriverID, HasRecord: true, Current: cloneTransition(p.record.Current), Reserved: cloneTransition(p.record.Reserved)})
	if refusal != nil {
		p.mu.Unlock()
		return DesiredView{}, refusal
	}
	fail, adopted, hook := p.fail, p.adopted, p.afterPlan
	p.afterPlan = nil
	if !fail || adopted {
		p.record.DesiredRevision++
		p.record.DesiredState = DesiredRunning
		if method == MethodStop {
			p.record.DesiredState = DesiredStopped
		}
		p.record.Current, p.record.Reserved = plan.Current, plan.Reserved
		next := p.record.Current
		if p.record.Reserved != nil {
			next = p.record.Reserved
		}
		next.DesiredRevision, next.Sequence = p.record.DesiredRevision, 1
		if p.record.Reserved != nil {
			next.Sequence = p.record.Current.Sequence + 1
		}
		p.revision++
	}
	view := DesiredView{DesiredState: p.record.DesiredState, DesiredRevision: p.record.DesiredRevision}
	p.mu.Unlock()
	// The store's serialization point has ended; the caller has not yet
	// processed the commit result. No store mutex is held at this barrier.
	if hook != nil {
		hook()
	}
	if fail {
		return DesiredView{}, errP10CommitFailure
	}
	return view, nil
}

func p10Service(t *testing.T, id DriverID, revision DesiredRevision) (*Service, *p10Store, *fakeExec) {
	t.Helper()
	store := &p10Store{record: ControlRecord{DriverID: id, Enabled: true, HasRecord: true, DesiredState: DesiredStopped, DesiredRevision: revision}, revision: 1}
	exec := &fakeExec{}
	svc, err := New(Options{Store: store, Recovery: &fakeRecovery{}, Incarnation: "synthetic", Drivers: []DriverConfig{{DriverID: id, Executor: exec}}})
	if err != nil {
		t.Fatal(err)
	}
	svc.ctx, svc.cancel = context.WithCancel(context.Background())
	// Drive reconciliation explicitly so publication barriers cannot depend on
	// scheduler timing or an unrelated worker's catalog reads.
	svc.started, svc.admitting = true, true
	t.Cleanup(svc.Close)
	if !svc.commitView(svc.ctx, nil, false) {
		t.Fatal("initial publication failed")
	}
	return svc, store, exec
}

func p10Wait(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("barrier watchdog expired")
	}
}

func p10Busy(t *testing.T, err error) {
	t.Helper()
	var refusal *Refusal
	if !errors.As(err, &refusal) || refusal.Code != RefusalTransitionBusy || refusal.Row != 8 {
		t.Fatalf("want row-8 TRANSITION_BUSY, got %v", err)
	}
}

func TestP10FailedCommitsBalanceHolds(t *testing.T) {
	for _, tc := range []struct {
		name     string
		id       DriverID
		revision DesiredRevision
	}{
		{"base", "family.alpha", 11},
		{"driver", "family.beta", 11},
		{"revision", "family.alpha", 17},
	} {
		t.Run(tc.name, func(t *testing.T) { p10FailedCommitsBalanceHolds(t, tc.id, tc.revision) })
	}
}

func p10FailedCommitsBalanceHolds(t *testing.T, id DriverID, revision DesiredRevision) {
	t.Helper()
	svc, store, exec := p10Service(t, id, revision)
	d := svc.drivers[id]
	// Two failed commits, each with a concurrent changing contender in the
	// post-planner/pre-result window. A contender must not acquire a second hold.
	for round := 0; round < 2; round++ {
		entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
		var once sync.Once
		unblock := func() { once.Do(func() { close(release) }) }
		t.Cleanup(func() { unblock(); p10Wait(t, done) })
		store.mu.Lock()
		store.fail = true
		store.afterPlan = func() { close(entered); <-release }
		store.mu.Unlock()
		var failed error
		go func() {
			defer close(done)
			_, failed = svc.Decide(context.Background(), MethodStart, Request{DriverID: d.id, ExpectedRevision: revision})
		}()
		p10Wait(t, entered)
		_, err := svc.Decide(context.Background(), MethodRestart, Request{DriverID: d.id, ExpectedRevision: revision})
		p10Busy(t, err)
		d.mu.Lock()
		holds := d.deciding
		d.mu.Unlock()
		if holds != 1 {
			t.Fatalf("overlapping owner: deciding=%d", holds)
		}
		unblock()
		p10Wait(t, done)
		if !errors.Is(failed, errP10CommitFailure) {
			t.Fatalf("failure = %v", failed)
		}
		d.mu.Lock()
		holds, ambiguous := d.deciding, d.ambiguous
		d.mu.Unlock()
		if holds != 0 || ambiguous != nil {
			t.Fatalf("leaked decision after failed commit: %d %+v", holds, ambiguous)
		}
	}
	if len(svc.EventsSince(0).Events) != 0 {
		t.Fatal("failed commits emitted acceptance")
	}
	store.mu.Lock()
	store.fail = false
	store.mu.Unlock()
	if _, err := svc.Decide(context.Background(), MethodStart, Request{DriverID: d.id, ExpectedRevision: revision}); err != nil {
		t.Fatal(err)
	}
	if progressed, _ := svc.reconcileStep(d); !progressed {
		t.Fatal("reconciliation did not progress")
	}
	if exec.count("start") != 1 {
		t.Fatalf("subsequent starts = %d", exec.count("start"))
	}
}

func TestP10AdoptedPublicationRetainsOwner(t *testing.T) {
	for _, tc := range []struct {
		name     string
		id       DriverID
		revision DesiredRevision
	}{
		{"base", "family.alpha", 11},
		{"driver", "family.beta", 11},
		{"revision", "family.alpha", 17},
	} {
		for _, blockRead := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/block-read-%t", tc.name, blockRead), func(t *testing.T) {
				p10AdoptedPublicationRetainsOwner(t, tc.id, tc.revision, blockRead)
			})
		}
	}
}

func p10AdoptedPublicationRetainsOwner(t *testing.T, id DriverID, revision DesiredRevision, blockRead bool) {
	t.Helper()
	svc, store, _ := p10Service(t, id, revision)
	d := svc.drivers[id]
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(func() { unblock(); p10Wait(t, done) })
	store.mu.Lock()
	store.fail, store.adopted = true, true
	barrier := func() { close(entered); <-release }
	if blockRead {
		store.driverBarrier = barrier
	} else {
		store.catalogBarrier = barrier
	}
	store.mu.Unlock()
	var failed error
	go func() {
		defer close(done)
		_, failed = svc.Decide(context.Background(), MethodStart, Request{DriverID: d.id, ExpectedRevision: revision})
	}()
	p10Wait(t, entered)
	// Direct marker oracle: deciding is a redundant admission guard, so busy
	// alone cannot detect premature removal of ambiguous ownership.
	d.mu.Lock()
	retained := d.ambiguous != nil && d.ambiguous.resolving && d.deciding == 1
	d.mu.Unlock()
	if !retained {
		t.Error("ambiguous owner cleared before acceptance publication")
	}
	if svc.List().Drivers[0].DesiredRevision != revision {
		t.Error("view published before barrier")
	}
	_, err := svc.Decide(context.Background(), MethodStop, Request{DriverID: d.id, ExpectedRevision: revision + 1})
	p10Busy(t, err)
	// A concurrent readback must return without publishing or waiting for the
	// resolver that owns the acceptance, whether it is reading or publishing.
	readbackDone := make(chan struct{})
	t.Cleanup(func() { unblock(); p10Wait(t, readbackDone) })
	go func() {
		defer close(readbackDone)
		svc.readbackResolved()
	}()
	p10Wait(t, readbackDone)
	if svc.List().Drivers[0].DesiredRevision != revision {
		t.Error("concurrent readback published adopted view before acceptance")
	}
	unblock()
	p10Wait(t, done)
	if !errors.Is(failed, errP10CommitFailure) {
		t.Fatalf("commit result = %v", failed)
	}
	page := svc.EventsSince(0)
	if page.Resync || len(page.Events) != 1 {
		t.Fatalf("want one retained acceptance: %+v", page)
	}
	event := page.Events[0]
	if event.Kind != EventDesiredAccepted || event.DriverID != id || event.DesiredRevision != revision+1 || event.DesiredState != DesiredRunning {
		t.Fatalf("acceptance = %+v", event)
	}
	if svc.List().Drivers[0].DesiredRevision != revision+1 {
		t.Fatal("adopted view not published")
	}
	d.mu.Lock()
	holds, ambiguous := d.deciding, d.ambiguous
	d.mu.Unlock()
	if holds != 0 || ambiguous != nil {
		t.Fatalf("owner not released: %d %+v", holds, ambiguous)
	}
	store.mu.Lock()
	store.fail, store.adopted = false, false
	store.mu.Unlock()
	if _, err := svc.Decide(context.Background(), MethodStop, Request{DriverID: d.id, ExpectedRevision: revision + 1}); err != nil {
		t.Fatalf("post-publication admission: %v", err)
	}
}

func TestP10RecoveredCleanupCorrelation(t *testing.T) {
	type identity struct {
		id         DriverID
		epoch      SourceEpoch
		generation DriverGeneration
		revision   DesiredRevision
		sequence   LifecycleSequence
	}
	base := identity{"family.alpha", "historical.alpha", 3, 11, 7}
	variants := []struct {
		name     string
		identity identity
	}{{"base", base}}
	for _, field := range []string{"driver", "epoch", "generation", "revision", "sequence"} {
		v := base
		switch field {
		case "driver":
			v.id = "family.beta"
		case "epoch":
			v.epoch = "historical.beta"
		case "generation":
			v.generation = 5
		case "revision":
			v.revision = 17
		case "sequence":
			v.sequence = 13
		}
		variants = append(variants, struct {
			name     string
			identity identity
		}{field, v})
	}
	for _, kind := range []TransitionKind{KindStart, KindRestart} {
		for _, semantic := range []bool{false, true} {
			for _, variant := range variants {
				t.Run(fmt.Sprintf("%s/semantic-%t/%s", kind, semantic, variant.name), func(t *testing.T) {
					v := variant.identity
					svc, store, exec := p10Service(t, v.id, v.revision)
					d := svc.drivers[v.id]
					transition := Transition{Kind: kind, Phase: PhaseActive, Epoch: v.epoch, Generation: v.generation, DesiredRevision: v.revision, Sequence: v.sequence, NativeClosePending: true, SemanticCleanupPending: true, AttemptsUsed: svc.cleanupLimit() - 1, DeadlineUTC: time.Now().Add(svc.budget)}
					store.mu.Lock()
					store.record.DesiredState = DesiredRunning
					store.record.DesiredRevision = v.revision
					store.record.Current = &transition
					store.revision++
					store.mu.Unlock()
					exec.setStop(func(_ context.Context, c LifecycleCorrelation) StopOutcome {
						if c.DriverID != v.id || c.Epoch != v.epoch || c.Generation != v.generation || c.Sequence != v.sequence {
							t.Errorf("cleanup correlation = %+v, want %+v", c, v)
						}
						r, err := store.Driver(v.id)
						if err != nil || r.Current.AttemptsUsed != svc.cleanupLimit() {
							t.Error("cleanup effect preceded durable charge")
						}
						return StopOutcome{NativeClosed: semantic, SemanticCleaned: false}
					})
					svc.reconcileStep(d)
					if exec.count("stop") != 1 || exec.count("start") != 0 {
						t.Fatalf("unexpected executor calls: %+v", exec.snapshot())
					}
					events := svc.EventsSince(0).Events
					if len(events) != 1 {
						t.Fatalf("failure event count: %+v", events)
					}
					e := events[0]
					reason := drivermanager.ReasonCloseUnconfirmed
					outcome := "CLOSE_UNCONFIRMED"
					if semantic {
						reason = drivermanager.ReasonRetryExhausted
						outcome = "CLEANUP_STALLED"
					}
					if e.Kind != EventObservedChanged || e.ObservedState != drivermanager.ObservedFailed || e.Reason != reason || e.DriverID != v.id || e.Epoch != v.epoch || e.Generation != v.generation || e.DesiredRevision != v.revision {
						t.Fatalf("failure event = %+v, want %+v reason %s", e, v, reason)
					}
					// Events omit sequence; check the installed correlation directly as an
					// independent oracle rather than claiming event coverage of that field.
					d.mu.Lock()
					corr := d.corr
					d.mu.Unlock()
					if corr.DriverID != v.id || corr.Epoch != v.epoch || corr.Generation != v.generation || corr.Sequence != v.sequence {
						t.Fatalf("installed correlation = %+v, want %+v", corr, v)
					}
					diags := svc.Diagnostics(v.id)
					if len(diags) != 1 || diags[0].Outcome != outcome || diags[0].DesiredRevision != v.revision {
						t.Fatalf("cleanup diagnostic = %+v", diags)
					}
					r, _ := store.Driver(v.id)
					if r.Current.Epoch != v.epoch || r.Current.Generation != v.generation || r.Current.Sequence != v.sequence || r.Current.DesiredRevision != v.revision || !r.Current.SemanticCleanupPending || r.Current.NativeClosePending == semantic {
						t.Fatalf("lost native evidence: %+v", r.Current)
					}
				})
			}
		}
	}
}
