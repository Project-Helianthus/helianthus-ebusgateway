package runtimecontrol

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/drivermanager"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimestate"
)

func p90AssertRefusals(t *testing.T, svc *Service, id DriverID, code RefusalCode, retryable bool, reason string) {
	t.Helper()
	for _, method := range []Method{MethodStart, MethodStop, MethodRestart} {
		t.Run(string(method), func(t *testing.T) {
			_, err := svc.Control(deadlineCtx(t), method, body(id, 1))
			var refusal *Refusal
			if !errors.As(err, &refusal) {
				t.Fatalf("refusal = %v", err)
			}
			if refusal.Code != code || refusal.Retryable() != retryable || refusal.Reason != reason || refusal.Row != 3 {
				t.Fatalf("refusal = %+v, retryable=%v; want %s, retryable=%v, reason=%q, row=3", refusal, refusal.Retryable(), code, retryable, reason)
			}
		})
	}
}

func TestP90DurableWithholdingPrecondition(t *testing.T) {
	for _, cause := range []string{"corrupt", "missing-guid"} {
		for _, configured := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/enabled=%v", cause, configured), func(t *testing.T) {
				path := filepath.Join(t.TempDir(), "state.json")
				wantDisposition := runtimestate.ControlLoadMissing
				wantReason := "runtime_control is not loaded: MISSING"
				if cause == "corrupt" {
					if err := os.WriteFile(path, []byte("{synthetic-corruption"), 0o600); err != nil {
						t.Fatal(err)
					}
					wantDisposition = runtimestate.ControlLoadCorrupt
					wantReason = "runtime_control is not loaded: CORRUPT"
				}
				manager := runtimestate.New(runtimestate.Options{Path: path, Logger: quietLogger()})
				t.Cleanup(func() {
					if err := manager.Stop(context.Background()); err != nil {
						t.Error(err)
					}
				})
				if _, err := manager.Load(context.Background()); err != nil {
					t.Fatal(err)
				}
				if got := manager.ControlLoad().Disposition; got != wantDisposition {
					t.Fatalf("disposition = %s, want %s", got, wantDisposition)
				}
				id := DriverID("fam.alpha")
				_, bootstrapErr := manager.BootstrapControl(context.Background(), []runtimestate.ControlDriverConfig{{DriverID: string(id), Enabled: configured}})
				if cause == "missing-guid" && !errors.Is(bootstrapErr, runtimestate.ErrControlBootstrapGUID) {
					t.Fatalf("bootstrap = %v", bootstrapErr)
				}
				exec := &fakeExec{}
				svc, err := New(Options{Store: &managerStore{m: manager}, Recovery: &fakeRecovery{}, Incarnation: "p90", Drivers: []DriverConfig{{DriverID: id, Executor: exec}}})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(svc.Close)
				if err := svc.Start(context.Background()); err == nil {
					t.Fatal("durable withholding started service")
				}
				p90AssertRefusals(t, svc, id, RefusalFailedPrecondition, false, wantReason)
				view, err := svc.Get(id)
				if err != nil || view.Enabled != configured || view.DesiredState != DesiredStopped || view.ObservedState != drivermanager.ObservedFailed {
					t.Fatalf("withheld view = %+v, %v; want enabled=%v, STOPPED, FAILED", view, err, configured)
				}
				if calls := exec.snapshot(); len(calls) != 0 {
					t.Fatalf("withheld executor calls = %+v", calls)
				}
			})
		}
	}
}

type p90CatalogErrorStore struct {
	Store
	err error
}

func (s p90CatalogErrorStore) Catalog() (ControlCatalog, error) { return ControlCatalog{}, s.err }

func TestP90TransientStorePreconditionClassification(t *testing.T) {
	transient := errors.New("synthetic transient store failure")
	f := newFixture(t, svcOpts{drivers: enabled("fam.alpha"), noStart: true, wrap: func(s Store) Store { return p90CatalogErrorStore{Store: s, err: transient} }})
	if err := f.svc.Start(context.Background()); !errors.Is(err, transient) {
		t.Fatalf("Start = %v", err)
	}
	p90AssertRefusals(t, f.svc, "fam.alpha", RefusalUnavailable, true, "control service not running")
	if calls := f.execs["fam.alpha"].snapshot(); len(calls) != 0 {
		t.Fatalf("executor calls = %+v", calls)
	}
}

func TestP90WithheldViewPreconditionPreservesIntent(t *testing.T) {
	for _, readable := range []bool{false, true} {
		for _, configured := range []bool{false, true} {
			for _, desired := range []DesiredState{DesiredRunning, DesiredStopped} {
				t.Run(fmt.Sprintf("readable=%v/enabled=%v/desired=%s", readable, configured, desired), func(t *testing.T) {
					f := newFixture(t, svcOpts{
						drivers: []runtimestate.ControlDriverConfig{{DriverID: "fam.alpha", Enabled: configured}},
						state:   controlState(7, 11, map[string]string{"fam.alpha": recordJSON(desired, 5, 0, "")}, ""),
						noStart: true,
						wrap: func(s Store) Store {
							if readable {
								return roStore{Store: s}
							}
							return p90CatalogErrorStore{Store: s, err: &Refusal{Code: RefusalFailedPrecondition, Row: 3, Reason: "synthetic durable withholding"}}
						},
					})
					if err := f.svc.Start(context.Background()); err == nil {
						t.Fatal("withheld service started")
					}
					reason := "synthetic durable withholding"
					if readable {
						reason = "control state is read-only"
					}
					p90AssertRefusals(t, f.svc, "fam.alpha", RefusalFailedPrecondition, false, reason)
					view, err := f.svc.Get("fam.alpha")
					if err != nil || view.Enabled != configured || view.DesiredState != desired || view.DesiredRevision != 5 || view.ObservedState != drivermanager.ObservedFailed {
						t.Fatalf("view = %+v, %v; want enabled=%v, desired=%s, revision=5, FAILED", view, err, configured, desired)
					}
					if calls := f.execs["fam.alpha"].snapshot(); len(calls) != 0 {
						t.Fatalf("executor calls = %+v", calls)
					}
				})
			}
		}
	}
}
