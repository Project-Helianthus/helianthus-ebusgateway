package main

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimestate"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semhub"
	"github.com/Project-Helianthus/helianthus-ebusgateway/portal"
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
)

func TestM11RuntimeControlHandsProcessHubToSharedReads(t *testing.T) {
	hub, id := m11SidecarPublication(t)
	control := startGatewayRuntimeControl(context.Background(), gatewayRuntimeControlOptions{
		Hub: hub, Store: runtimestate.New(runtimestate.Options{Path: filepath.Join(t.TempDir(), "runtime_state.json")}),
		OnError: func(error) {},
	})
	t.Cleanup(func() {
		if err := control.CloseAdmission(context.Background()); err != nil {
			t.Error(err)
		}
	})
	if control.bridge != nil || control.withheld == nil {
		t.Fatal("fixture did not withhold ebus.primary")
	}
	provider := newGatewaySemanticSnapshotProvider(control)
	provider.now = func() time.Time { return time.Unix(0, 1800000000000000020) }
	_, inputs, err := provider.Capture(context.Background(), []semreg.AssetID{id})
	if err != nil {
		t.Fatal(err)
	}
	if len(inputs) != 1 || inputs[0].Snapshot == nil {
		t.Fatal("sidecar publication unavailable")
	}
	opts := portal.Options{}
	configureGatewayPortalSemanticRead(&opts, provider)
	view, err := opts.SharedSemanticView(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(view.Assets) != 1 || view.Assets[0].Snapshot == nil {
		t.Fatal("Portal lost sidecar publication")
	}
}

func TestM11RuntimeControlWithoutProcessHubUnavailable(t *testing.T) {
	control := startGatewayRuntimeControl(context.Background(), gatewayRuntimeControlOptions{
		Store: runtimestate.New(runtimestate.Options{Path: filepath.Join(t.TempDir(), "runtime_state.json")}), OnError: func(error) {},
	})
	t.Cleanup(func() {
		if err := control.CloseAdmission(context.Background()); err != nil {
			t.Error(err)
		}
	})
	provider := newGatewaySemanticSnapshotProvider(control)
	if _, _, err := provider.Capture(context.Background(), []semreg.AssetID{"asset:absent"}); !errors.Is(err, semhub.ErrCaptureUnavailable) {
		t.Fatalf("missing hub outcome=%v", err)
	}
	opts := portal.Options{}
	configureGatewayPortalSemanticRead(&opts, provider)
	if view, err := opts.SharedSemanticView(context.Background()); !errors.Is(err, semhub.ErrCaptureUnavailable) || len(view.Assets) != 0 {
		t.Fatalf("missing hub Portal outcome=%+v, %v", view, err)
	}
}
