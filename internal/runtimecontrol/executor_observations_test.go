package runtimecontrol

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/drivermanager"
)

// This is a regression oracle for the existing publication seam, not an executor
// observation API. The first publisher is paused before its catalog advance;
// replacement observations publish before that old publisher resumes.
type p14PublicationStore struct {
	Store
	mu           sync.Mutex
	catalog      ControlCatalog
	calls        atomic.Uint32
	failFirstTwo bool
	entered      chan struct{}
	release      chan struct{}
}

func (s *p14PublicationStore) AdvanceCatalog(ctx context.Context) (CatalogRevision, error) {
	call := s.calls.Add(1)
	if call == 1 {
		close(s.entered)
		select {
		case <-s.release:
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	if s.failFirstTwo && call <= 2 {
		return 0, errors.New("synthetic catalog advance failure")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.catalog.CatalogRevision++
	return s.catalog.CatalogRevision, nil
}

func (s *p14PublicationStore) Catalog() (ControlCatalog, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c := s.catalog
	c.Drivers = append([]ControlRecord(nil), c.Drivers...)
	return c, nil
}

func p14Wait(t *testing.T, ch <-chan struct{}, label string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("watchdog: %s did not finish", label)
	}
}

func TestP14ObservationPublicationRejectsSupersededCorrelation(t *testing.T) {
	for _, tc := range []struct {
		name       string
		epoch      SourceEpoch
		generation DriverGeneration
	}{
		{name: "epoch", epoch: "synthetic.successor", generation: 3},
		{name: "generation", epoch: "synthetic.predecessor", generation: 4},
		{name: "epoch_and_generation", epoch: "synthetic.successor", generation: 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const id DriverID = "fam.executor"
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			store := &p14PublicationStore{
				catalog: ControlCatalog{CatalogRevision: 1, Drivers: []ControlRecord{{
					DriverID: id, Enabled: true, DesiredState: DesiredRunning, DesiredRevision: 1,
				}}},
				entered: make(chan struct{}), release: make(chan struct{}),
			}
			oldCorr := LifecycleCorrelation{DriverID: id, Epoch: "synthetic.predecessor", Generation: 3, Sequence: 7}
			oldAttempt := &attempt{corr: oldCorr, admitted: true}
			d := &driverState{id: id, corr: oldCorr, live: oldAttempt}
			s := &Service{store: store, ctx: ctx, drivers: map[DriverID]*driverState{id: d},
				order: []DriverID{id}, events: newEventLog(16), observed: map[DriverID]observation{
					id: {state: drivermanager.ObservedRunning, reason: drivermanager.ReasonNone,
						epoch: "synthetic.predecessor", generation: 3},
				}}
			s.view.Store(&Catalog{})
			if !s.publish(nil) {
				t.Fatal("positive control: initial RUNNING view did not publish")
			}
			old := observation{state: drivermanager.ObservedFailed, reason: drivermanager.ReasonInternalError,
				epoch: "synthetic.predecessor", generation: 3}
			d.mu.Lock()
			changed := s.stageObservation(id, old)
			d.mu.Unlock()
			if !changed {
				t.Fatal("positive control: predecessor observation was not staged")
			}
			done := make(chan struct{})
			var release sync.Once
			unblock := func() { release.Do(func() { close(store.release) }) }
			t.Cleanup(func() {
				unblock()
				p14Wait(t, done, "joined predecessor publisher")
			})
			go func() {
				defer close(done)
				s.commitObserved(d, old)
			}()
			p14Wait(t, store.entered, "predecessor catalog entry")
			if got, err := s.Get(id); err != nil || got.ObservedState != drivermanager.ObservedRunning {
				t.Fatalf("positive control: unpersisted observation visible: %+v, %v", got, err)
			}

			// Drive the existing staging/publication seams in STOP/replacement
			// order. No lifecycle executor or device callback is installed.
			d.mu.Lock()
			oldAttempt.fenced = true
			d.live = nil
			d.mu.Unlock()
			stopping := observation{state: drivermanager.ObservedStopping, reason: drivermanager.ReasonStopRequested,
				epoch: "synthetic.predecessor", generation: 3}
			s.stageObservation(id, stopping)
			s.commitObserved(d, stopping)
			if got, err := s.Get(id); err != nil || got.ObservedState != drivermanager.ObservedStopping {
				t.Fatalf("positive control: STOP view missing: %+v, %v", got, err)
			}
			nextCorr := LifecycleCorrelation{DriverID: id, Epoch: tc.epoch, Generation: tc.generation, Sequence: 7}
			d.mu.Lock()
			d.live = &attempt{corr: nextCorr, admitted: true}
			d.corr = nextCorr
			next := observation{state: drivermanager.ObservedFailed, reason: drivermanager.ReasonInternalError,
				epoch: tc.epoch, generation: tc.generation}
			s.stageObservation(id, next)
			d.mu.Unlock()
			s.commitObserved(d, next)
			view := s.List()
			if len(view.Drivers) != 1 || view.Drivers[0].ObservedState != drivermanager.ObservedFailed ||
				view.Drivers[0].Reason != drivermanager.ReasonInternalError {
				t.Fatalf("positive control: replacement view missing: %+v", view)
			}
			page := s.EventsSince(0)
			if len(page.Events) != 2 || page.Resync {
				t.Fatalf("positive control: expected STOP and replacement events: %+v", page)
			}
			last := page.Events[1]
			if last.Epoch != tc.epoch || last.Generation != tc.generation || last.CatalogRevision != view.Revision {
				t.Fatalf("positive control: replacement correlation missing: %+v", last)
			}
			unblock()
			p14Wait(t, done, "predecessor publication return")
			late := s.EventsSince(view.EventCursor)
			if late.Resync || len(late.Events) != 0 {
				t.Fatalf("fenced predecessor published after replacement: %+v", late)
			}
		})
	}
}

// The replacement defers first, then the blocked predecessor defers and replaces
// it. Retrying must expose the replacement view with a resync notification.
func TestP14DeferredObservationPublicationSignalsSupersededGap(t *testing.T) {
	for _, tc := range []struct {
		name       string
		epoch      SourceEpoch
		generation DriverGeneration
	}{
		{name: "epoch", epoch: "synthetic.successor", generation: 3},
		{name: "generation", epoch: "synthetic.predecessor", generation: 4},
		{name: "epoch_and_generation", epoch: "synthetic.successor", generation: 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const id DriverID = "fam.executor"
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			store := &p14PublicationStore{
				catalog: ControlCatalog{CatalogRevision: 1, Drivers: []ControlRecord{{
					DriverID: id, Enabled: true, DesiredState: DesiredRunning, DesiredRevision: 1,
				}}},
				entered: make(chan struct{}), release: make(chan struct{}), failFirstTwo: true,
			}
			d := &driverState{id: id}
			old := observation{state: drivermanager.ObservedFailed, reason: drivermanager.ReasonInternalError,
				epoch: "synthetic.predecessor", generation: 3}
			s := &Service{store: store, ctx: ctx, order: []DriverID{id}, events: newEventLog(16),
				observed: map[DriverID]observation{id: {state: drivermanager.ObservedRunning,
					reason: drivermanager.ReasonNone, epoch: old.epoch, generation: old.generation}},
				// Keep retries manual so no timer can disturb the forced deferral order.
				retryArmed: true,
			}
			s.view.Store(&Catalog{})
			if !s.publish(nil) {
				t.Fatal("positive control: initial view did not publish")
			}
			initial := s.List()
			notify := s.EventNotify()
			if !s.stageObservation(id, old) {
				t.Fatal("positive control: predecessor observation was not staged")
			}
			done := make(chan struct{})
			var release sync.Once
			unblock := func() { release.Do(func() { close(store.release) }) }
			t.Cleanup(func() {
				unblock()
				p14Wait(t, done, "joined deferred predecessor publisher")
			})
			go func() {
				defer close(done)
				s.commitObserved(d, old)
			}()
			p14Wait(t, store.entered, "predecessor catalog entry")
			next := old
			next.epoch, next.generation = tc.epoch, tc.generation
			if !s.stageObservation(id, next) {
				t.Fatal("positive control: replacement observation was not staged")
			}
			s.commitObserved(d, next)
			if len(s.pendingEv) != 1 || s.pendingEv[0].Epoch != next.epoch || s.pendingEv[0].Generation != next.generation {
				t.Fatalf("replacement did not defer first: %+v", s.pendingEv)
			}
			unblock()
			p14Wait(t, done, "predecessor deferral")
			if len(s.pendingEv) != 1 || s.pendingEv[0].Epoch != old.epoch || s.pendingEv[0].Generation != old.generation {
				t.Fatalf("predecessor did not defer last: %+v", s.pendingEv)
			}
			if got := s.List(); got.Revision != initial.Revision || got.Drivers[0].ObservedState != drivermanager.ObservedRunning {
				t.Fatalf("failed advances exposed staged observation: %+v", got)
			}
			select {
			case <-notify:
				t.Fatal("failed advances notified event readers")
			default:
			}
			if !s.commitView(ctx, nil, true) {
				t.Fatal("retry did not publish")
			}
			view := s.List()
			if view.Revision <= initial.Revision || view.Drivers[0].ObservedState != next.state || view.Drivers[0].Reason != next.reason {
				t.Fatalf("retry did not expose replacement view: %+v", view)
			}
			select {
			case <-notify:
			default:
				t.Fatal("replacement transition did not notify event readers")
			}
			page := s.EventsSince(initial.EventCursor)
			if !page.Resync || len(page.Events) != 0 || page.Next != view.EventCursor || view.EventCursor <= initial.EventCursor {
				t.Fatalf("discarded deferred observation did not produce a coherent resync gap: %+v, view %+v", page, view)
			}
		})
	}
}

func (s *p14PublicationStore) AdmissionOpen() bool { return true }
