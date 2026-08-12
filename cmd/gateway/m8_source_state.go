package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
)

type m8SourceStateProvider struct {
	transport string
	registry  *registry.DeviceRegistry
	semantic  graphql.SemanticProvider
}

type m8SourceRoute struct {
	SemanticPath string `json:"semantic_path"`
	Source       string `json:"source"`
}

func newM8SourceStateProvider(
	transport string,
	deviceRegistry *registry.DeviceRegistry,
	semantic graphql.SemanticProvider,
) *m8SourceStateProvider {
	if deviceRegistry == nil || semantic == nil {
		return nil
	}
	return &m8SourceStateProvider{transport: transport, registry: deviceRegistry, semantic: semantic}
}

func (provider *m8SourceStateProvider) M8SourceState(_ context.Context, inputID string) (json.RawMessage, error) {
	if provider == nil || provider.registry == nil || provider.semantic == nil {
		return nil, errors.New("M8 source-state provider unavailable")
	}
	switch inputID {
	case "ebus.debug":
		count := 0
		provider.registry.IterateSnapshots(func(registry.DeviceEntrySnapshot) bool {
			count++
			return true
		})
		return json.Marshal(struct {
			Transport           string `json:"transport"`
			RuntimeState        string `json:"runtime_state"`
			RegistryDeviceCount int    `json:"registry_device_count"`
		}{Transport: provider.transport, RuntimeState: "running", RegistryDeviceCount: count})
	case "command.routing":
		routes := provider.routes()
		return json.Marshal(struct {
			Fallback any             `json:"fallback"`
			Routes   []m8SourceRoute `json:"routes"`
		}{Fallback: nil, Routes: routes})
	case "semantic.registry":
		routes := provider.routes()
		type leaf struct {
			Path           string `json:"path"`
			PromotionState string `json:"promotion_state"`
			Source         string `json:"source"`
		}
		leaves := make([]leaf, len(routes))
		for index, route := range routes {
			leaves[index] = leaf{Path: route.SemanticPath, PromotionState: "PROMOTED", Source: route.Source}
		}
		return json.Marshal(struct {
			Authority string `json:"authority"`
			Leaves    []leaf `json:"leaves"`
		}{Authority: "ebus.promoted", Leaves: leaves})
	default:
		return nil, fmt.Errorf("unsupported M8 source input %q", inputID)
	}
}

func (provider *m8SourceStateProvider) routes() []m8SourceRoute {
	zones := provider.semantic.Zones()
	sort.Slice(zones, func(left, right int) bool { return zones[left].ID < zones[right].ID })
	routes := make([]m8SourceRoute, 0, len(zones)+1)
	if provider.semantic.DHW() != nil {
		routes = append(routes, m8SourceRoute{SemanticPath: "/dhw/operating_mode", Source: "ebus"})
	}
	for index := range zones {
		routes = append(routes, m8SourceRoute{
			SemanticPath: fmt.Sprintf("/zones/%d/target_temperature", index+1), Source: "ebus",
		})
	}
	return routes
}
