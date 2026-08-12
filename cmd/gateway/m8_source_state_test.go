package main

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
)

func TestIssue788M8SourceStateReadsLiveOwnersDeterministically(t *testing.T) {
	deviceRegistry := registry.NewDeviceRegistry(nil)
	deviceRegistry.Register(registry.DeviceInfo{Address: 0x08, Manufacturer: "Vaillant", DeviceID: "BAI00"})
	deviceRegistry.Register(registry.DeviceInfo{Address: 0x26, Manufacturer: "Vaillant", DeviceID: "VR_71"})
	semantic := graphql.NewLiveSemanticProvider()
	semantic.SetZones([]graphql.Zone{{ID: "zone-b"}, {ID: "zone-a"}})
	semantic.SetDHW(&graphql.DhwStatus{})
	provider := newM8SourceStateProvider("ens", deviceRegistry, semantic)

	debug := issue788DecodeSourceState(t, provider, "ebus.debug")
	if debug["transport"] != "ens" || debug["runtime_state"] != "running" || debug["registry_device_count"] != float64(2) {
		t.Fatalf("debug = %#v", debug)
	}
	routing := issue788DecodeSourceState(t, provider, "command.routing")
	wantRoutes := []any{
		map[string]any{"semantic_path": "/dhw/operating_mode", "source": "ebus"},
		map[string]any{"semantic_path": "/zones/1/target_temperature", "source": "ebus"},
		map[string]any{"semantic_path": "/zones/2/target_temperature", "source": "ebus"},
	}
	if !reflect.DeepEqual(routing["routes"], wantRoutes) {
		t.Fatalf("routes = %#v", routing["routes"])
	}
	semanticState := issue788DecodeSourceState(t, provider, "semantic.registry")
	if semanticState["authority"] != "ebus.promoted" {
		t.Fatalf("semantic registry = %#v", semanticState)
	}
}

func issue788DecodeSourceState(t *testing.T, provider *m8SourceStateProvider, inputID string) map[string]any {
	t.Helper()
	raw, err := provider.M8SourceState(context.Background(), inputID)
	if err != nil {
		t.Fatal(err)
	}
	var value map[string]any
	if err := json.Unmarshal(raw, &value); err != nil {
		t.Fatal(err)
	}
	return value
}
