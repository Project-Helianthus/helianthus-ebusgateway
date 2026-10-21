package main

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	eebusruntime "github.com/Project-Helianthus/helianthus-eebusreg"
	"github.com/Project-Helianthus/helianthus-eebusreg/eebusraw"
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/projection"
)

type m18Runtime struct {
	msp06GatewayRuntime
	snapshot func() (eebusruntime.SnapshotV1, error)
}

func (r *m18Runtime) Snapshot() (eebusruntime.SnapshotV1, error) { return r.snapshot() }

func (r *m18Runtime) FeaturesDataGet(context.Context, eebusraw.ReadAuthorizationV1, eebusraw.FeatureDataGetRequestV1) (eebusraw.FeatureDataGetDataV1, *eebusraw.ErrorV1) {
	r.featuresCalls++
	return eebusraw.FeatureDataGetDataV1{}, nil
}

func m18Snapshot(t *testing.T) eebusruntime.SnapshotV1 {
	t.Helper()
	identity, err := eebusraw.RedactID(eebusraw.IDKindPeer, "synthetic-runtime")
	if err != nil {
		t.Fatal(err)
	}
	at := time.Unix(1, 0).UTC()
	snapshot, err := eebusruntime.NewSnapshotV1(eebusruntime.SnapshotV1{
		Meta: eebusruntime.SnapshotMetaV1{Contract: eebusruntime.SnapshotContractV1,
			Runtime: identity, LocalSKI: strings.Repeat("a", 40), MaskTier: eebusraw.MaskTierRaw,
			CapturedAt: at, DataTimestamp: at},
		Status: eebusruntime.RuntimeObservationV1{State: eebusruntime.ObservedRuntimeStateV1Ready},
	})
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func TestM18RefreshPublishesOnceWithoutDirectPromotion(t *testing.T) {
	native := m18Snapshot(t)
	reads, calls := 0, 0
	runtime := &m18Runtime{snapshot: func() (eebusruntime.SnapshotV1, error) { reads++; return native, nil }}
	adapter := &eebusRuntimeAdapter{runtime: runtime}
	if err := adapter.setSemregPublicationHook(func(got eebusSemregObservation) error {
		calls++
		if !reflect.DeepEqual(got.Native, native) || got.Evidence.Digest != semreg.Digest(native.Meta.DataHash) {
			t.Fatal("native snapshot or computed provenance digest changed")
		}
		if len(got.Items) != 18 {
			t.Fatalf("items = %d; want all 18 documented catalog references", len(got.Items))
		}
		for _, item := range got.Items {
			if item.CatalogRef == "" || item.Quality.Qualification != semreg.QualificationUnknown ||
				item.Quality.Promotion != semreg.PromotionUnpromoted || item.Disposition != projection.ProjectionUnknown ||
				!reflect.DeepEqual(item.Quality.Reasons, []semreg.DefinitionID{eebusPendingNormativeRevision}) || len(item.Loss) != 2 {
				t.Fatalf("candidate is not honestly unknown: %#v", item)
			}
		}
		got.Native.Meta.LocalSKI = "changed"
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	provider := newEEBusPromotedSemanticProvider(adapter)
	provider.Refresh(context.Background())
	if reads != 1 || calls != 1 || runtime.featuresCalls != 0 {
		t.Fatalf("snapshot reads/hooks/feature reads = %d/%d/%d", reads, calls, runtime.featuresCalls)
	}
	if !reflect.DeepEqual(provider.Overlay(), (&eebusPromotedSemanticProvider{}).Overlay()) {
		t.Fatal("unknown observation became a direct semantic overlay")
	}
	if native.Validate() != nil {
		t.Fatal("hook mutated native snapshot")
	}
}

func TestM18InvalidReadAndHookFailurePreserveAcceptedEvidence(t *testing.T) {
	native := m18Snapshot(t)
	readErr := error(nil)
	adapter := &eebusRuntimeAdapter{runtime: &m18Runtime{snapshot: func() (eebusruntime.SnapshotV1, error) { return native, readErr }}}
	accepted, calls := "", 0
	failure := errors.New("synthetic publication failure")
	fail := false
	if err := adapter.setSemregPublicationHook(func(observation eebusSemregObservation) error {
		calls++
		if fail {
			return failure
		}
		accepted = string(observation.Evidence.Digest)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := adapter.refreshSemregSnapshot(context.Background()); err != nil {
		t.Fatal(err)
	}
	want := accepted
	readErr = errors.New("synthetic native read failure")
	if _, err := adapter.refreshSemregSnapshot(context.Background()); !errors.Is(err, readErr) {
		t.Fatal("read error lost")
	}
	readErr = nil
	native.Meta.DataHash = "invalid"
	if _, err := adapter.refreshSemregSnapshot(context.Background()); err == nil {
		t.Fatal("invalid native evidence published")
	}
	native = m18Snapshot(t)
	fail = true
	if _, err := adapter.refreshSemregSnapshot(context.Background()); !errors.Is(err, failure) {
		t.Fatal("hook error lost")
	}
	if calls != 2 || accepted != want {
		t.Fatal("failed refresh retried or replaced accepted evidence")
	}
	if _, err := adapter.Snapshot(); err != nil {
		t.Fatal("publication failure changed native compatibility read")
	}
	if calls != 2 {
		t.Fatal("compatibility read unexpectedly published twice")
	}
}

func TestM18RetirementAndReplacementFenceDelayedRefresh(t *testing.T) {
	for _, action := range []string{"disconnect", "replacement", "terminal", "shutdown", "hook replacement"} {
		t.Run(action, func(t *testing.T) {
			native := m18Snapshot(t)
			entered, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
			old := &m18Runtime{snapshot: func() (eebusruntime.SnapshotV1, error) { close(entered); <-release; return native, nil }}
			slot := newEEBusRuntimeSlot(old)
			adapter := &eebusRuntimeAdapter{runtime: slot}
			calls := 0
			hook := func(eebusSemregObservation) error { calls++; return nil }
			if err := adapter.setSemregPublicationHook(hook); err != nil {
				t.Fatal(err)
			}
			go func() { _, err := adapter.refreshSemregSnapshot(context.Background()); done <- err }()
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("read did not start")
			}
			shutdownDone := make(chan error, 1)
			switch action {
			case "disconnect", "replacement":
				if !slot.BeginRetire() {
					t.Fatal("retire rejected")
				}
			case "terminal":
				slot.FenceTerminal()
			case "shutdown":
				go func() { shutdownDone <- adapter.Shutdown() }()
				deadline := time.After(time.Second)
				for {
					adapter.publicationMu.Lock()
					closed := adapter.publicationClosed
					adapter.publicationMu.Unlock()
					if closed {
						break
					}
					select {
					case <-deadline:
						t.Fatal("shutdown did not fence")
					default:
						time.Sleep(time.Millisecond)
					}
				}
			case "hook replacement":
				if err := adapter.setSemregPublicationHook(hook); err != nil {
					t.Fatal(err)
				}
			}
			close(release)
			select {
			case err := <-done:
				if err == nil {
					t.Fatal("stale refresh accepted")
				}
			case <-time.After(time.Second):
				t.Fatal("refresh hung")
			}
			if calls != 0 {
				t.Fatal("stale hook ran")
			}
			if action == "shutdown" {
				select {
				case err := <-shutdownDone:
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(time.Second):
					t.Fatal("shutdown hung")
				}
			}
			if action == "replacement" {
				next := &m18Runtime{snapshot: func() (eebusruntime.SnapshotV1, error) { return native, nil }}
				if !slot.Replace(next) {
					t.Fatal("replacement rejected")
				}
				if _, err := adapter.refreshSemregSnapshot(context.Background()); err != nil {
					t.Fatal(err)
				}
				if calls != 1 {
					t.Fatal("replacement did not publish exactly once")
				}
			}
		})
	}
}

func TestM18ReportedUseCaseRevisionCannotQualifyMapping(t *testing.T) {
	for _, revision := range []string{"", "synthetic-reported-revision"} {
		for _, state := range []eebusruntime.ObservedRuntimeStateV1{eebusruntime.ObservedRuntimeStateV1Ready, eebusruntime.ObservedRuntimeStateV1Stopped} {
			t.Run(string(state)+"/"+revision, func(t *testing.T) {
				native := m18Snapshot(t)
				native.Meta.DataHash = ""
				native.Status.State = state
				usecase := eebusruntime.UseCaseV1{ContextAddress: "synthetic-context", Name: "synthetic-use-case", Actor: "synthetic-actor"}
				if revision != "" {
					usecase.Version, usecase.DocumentSubrevision = &revision, &revision
				}
				native.UseCases = []eebusruntime.UseCaseV1{usecase}
				var err error
				native, err = eebusruntime.NewSnapshotV1(native)
				if err != nil {
					t.Fatal(err)
				}
				observation, err := eebusSemregObservationFrom(native)
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(observation.Native.UseCases, native.UseCases) {
					t.Fatal("native revision evidence lost")
				}
				for _, item := range observation.Items {
					if item.Quality.Qualification != semreg.QualificationUnknown || item.Quality.Promotion != semreg.PromotionUnpromoted || item.Quality.Validity != semreg.ValidityUnknown || item.Quality.Freshness != semreg.FreshnessUnknown || item.Disposition != projection.ProjectionUnknown {
						t.Fatal("reported revision or connection state qualified a mapping")
					}
					if !reflect.DeepEqual(item.Evidence, []semreg.EvidenceRef{observation.Evidence}) || item.Loss[0].Description != string(eebusPendingNormativeRevision) {
						t.Fatal("per-item provenance or loss missing")
					}
					if state != eebusruntime.ObservedRuntimeStateV1Ready && item.Quality.Availability != semreg.AvailabilityUnavailable {
						t.Fatal("disconnected item remained available")
					}
				}
				if revision != "" {
					*observation.Native.UseCases[0].Version = "mutated"
					if *native.UseCases[0].Version != revision {
						t.Fatal("nested native evidence aliases hook payload")
					}
				}
			})
		}
	}
}

func TestM18SlotGenerationRejectsReadAfterReplacementClearsRetirement(t *testing.T) {
	native := m18Snapshot(t)
	entered, release := make(chan struct{}), make(chan struct{})
	old := &m18Runtime{snapshot: func() (eebusruntime.SnapshotV1, error) {
		close(entered)
		<-release
		return native, nil
	}}
	slot := newEEBusRuntimeSlot(old)
	adapter := &eebusRuntimeAdapter{runtime: slot}
	calls := 0
	if err := adapter.setSemregPublicationHook(func(eebusSemregObservation) error { calls++; return nil }); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, err := adapter.refreshSemregSnapshot(context.Background()); done <- err }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("native read did not start")
	}
	// Hold the final publication boundary while the old native reader drains
	// and replacement installs a ready successor. Neither closed nor retiring
	// can detect this stale result; the slot generation must reject it.
	adapter.publicationMu.Lock()
	close(release)
	replaced := make(chan bool, 1)
	go func() {
		replaced <- slot.Replace(&m18Runtime{snapshot: func() (eebusruntime.SnapshotV1, error) { return native, nil }})
	}()
	select {
	case ok := <-replaced:
		if !ok {
			adapter.publicationMu.Unlock()
			t.Fatal("replacement rejected")
		}
	case <-time.After(time.Second):
		adapter.publicationMu.Unlock()
		t.Fatal("replacement failed to drain the native reader")
	}
	adapter.publicationMu.Unlock()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("predecessor result crossed successor generation")
		}
	case <-time.After(time.Second):
		t.Fatal("refresh hung")
	}
	if calls != 0 {
		t.Fatal("predecessor invoked publication hook")
	}
	if _, err := adapter.refreshSemregSnapshot(context.Background()); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatal("successor did not publish")
	}
}

type m18AfterContextCheck struct {
	context.Context
	afterCheck func()
}

func (ctx *m18AfterContextCheck) Err() error {
	err := ctx.Context.Err()
	if ctx.afterCheck != nil {
		afterCheck := ctx.afterCheck
		ctx.afterCheck = nil
		afterCheck()
	}
	return err
}

func TestM18CancellationAfterReadBeforePublication(t *testing.T) {
	native := m18Snapshot(t)
	reads, calls := 0, 0
	slot := newEEBusRuntimeSlot(&m18Runtime{snapshot: func() (eebusruntime.SnapshotV1, error) {
		reads++
		return native, nil
	}})
	adapter := &eebusRuntimeAdapter{runtime: slot}
	if err := adapter.setSemregPublicationHook(func(eebusSemregObservation) error { calls++; return nil }); err != nil {
		t.Fatal(err)
	}
	base, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx := &m18AfterContextCheck{Context: base, afterCheck: func() {
		if reads != 1 {
			t.Fatalf("snapshot reads = %d; want 1 before cancellation", reads)
		}
		// Cancel after the first context check sampled nil, while the slot
		// lock is held, before refresh can enter its publication boundary.
		slot.operationMu.Lock()
		cancel()
		slot.operationMu.Unlock()
	}}
	if _, err := adapter.refreshSemregSnapshot(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("refresh error = %v; want context.Canceled", err)
	}
	if calls != 0 {
		t.Fatalf("publication calls = %d; want 0", calls)
	}
}

func TestM18CanceledRefreshAndAbsentHookCannotClaimPublication(t *testing.T) {
	native := m18Snapshot(t)
	ctx, cancel := context.WithCancel(context.Background())
	adapter := &eebusRuntimeAdapter{runtime: &m18Runtime{snapshot: func() (eebusruntime.SnapshotV1, error) { cancel(); return native, nil }}}
	calls := 0
	if err := adapter.setSemregPublicationHook(func(eebusSemregObservation) error { calls++; return nil }); err != nil {
		t.Fatal(err)
	}
	if _, err := adapter.refreshSemregSnapshot(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled refresh accepted")
	}
	if calls != 0 {
		t.Fatal("canceled refresh invoked hook")
	}
	if err := adapter.setSemregPublicationHook(nil); err != nil {
		t.Fatal(err)
	}
	if _, err := adapter.refreshSemregSnapshot(context.Background()); err == nil {
		t.Fatal("absent hook claimed publication")
	}
	if _, err := adapter.Snapshot(); err != nil {
		t.Fatal("absent hook changed native compatibility read")
	}
}
