package main

import (
	"context"
	"fmt"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
	"github.com/Project-Helianthus/helianthus-ebusreg/vaillant/productids"
	graphqlgo "github.com/graphql-go/graphql"
)

func TestIssue946CatalogAggregateProjectsRegulatorCapability(t *testing.T) {
	catalog, err := productids.LoadCatalog()
	if err != nil {
		t.Fatalf("LoadCatalog() error: %v", err)
	}

	tests := []struct {
		name       string
		devices    []registry.DeviceInfo
		catalogErr error
		want       graphql.RegulatorCapability
	}{
		{
			name: "present wins over unknown",
			devices: []registry.DeviceInfo{
				{Address: 0x15, Manufacturer: "Vaillant", SerialNumber: "21-22-09-0020028521-0082-005409-N4"},
				{Address: 0x60, Manufacturer: "Vaillant"},
			},
			want: graphql.RegulatorCapabilityPresent,
		},
		{
			name: "none requires catalog-known non-regulators",
			devices: []registry.DeviceInfo{
				{Address: 0x15, Manufacturer: "Vaillant", SerialNumber: "21-22-09-0010002315-0082-005409-N4"},
			},
			want: graphql.RegulatorCapabilityNone,
		},
		{
			name: "mixed known and unknown is unknown",
			devices: []registry.DeviceInfo{
				{Address: 0x15, Manufacturer: "Vaillant", SerialNumber: "21-22-09-0010002315-0082-005409-N4"},
				{Address: 0x60, Manufacturer: "Vaillant"},
			},
			want: graphql.RegulatorCapabilityUnknown,
		},
		{
			name:       "catalog failure is unknown",
			devices:    []registry.DeviceInfo{{Address: 0x15, Manufacturer: "Vaillant", SerialNumber: "21-22-09-0020028521-0082-005409-N4"}},
			catalogErr: fmt.Errorf("catalog unavailable"),
			want:       graphql.RegulatorCapabilityUnknown,
		},
		{
			name: "no vaillant identity is unknown",
			devices: []registry.DeviceInfo{
				{Address: 0x15, Manufacturer: "other", DeviceID: "BASV"},
			},
			want: graphql.RegulatorCapabilityUnknown,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			provider := graphql.NewLiveSemanticProvider()
			poller := &vaillantSemanticPoller{
				reg:        newTestRegistry(tc.devices...),
				catalog:    catalog,
				catalogErr: tc.catalogErr,
				provider:   provider,
			}
			poller.refreshRegulatorCapability(context.Background())
			if got := provider.RegulatorCapability(); got != tc.want {
				t.Fatalf("published regulator capability = %q; want %q", got, tc.want)
			}
		})
	}
}

func TestIssue946RegulatorCapabilityMCPGraphQLParityAndSnapshot(t *testing.T) {
	provider := graphql.NewLiveSemanticProvider()
	provider.SetRegulatorCapability(graphql.RegulatorCapabilityPresent)

	builder := graphql.NewBuilder(nil, nil)
	builder.SetSemanticProvider(provider)
	schema, err := graphql.NewQuerySchema(builder)
	if err != nil {
		t.Fatalf("NewQuerySchema() error: %v", err)
	}
	result := graphqlgo.Do(graphqlgo.Params{Schema: schema, RequestString: `{ regulator_capability }`})
	if len(result.Errors) != 0 {
		t.Fatalf("GraphQL errors = %v", result.Errors)
	}
	graphQLCapability := result.Data.(map[string]any)["regulator_capability"]

	server, err := mcp.NewServer(emptyMCPRegistry{}, nil)
	if err != nil {
		t.Fatalf("mcp.NewServer() error: %v", err)
	}
	server.SetStatusProvider(newMCPRuntimeStatusProvider(provider, nil))
	live := mcpCallToolEnvelope(t, server.Handler(), "ebus.v1.runtime.status.get", `{}`)
	liveData := live["data"].(map[string]any)
	if got := liveData["regulator_capability"]; got != graphQLCapability {
		t.Fatalf("MCP capability = %#v; GraphQL = %#v", got, graphQLCapability)
	}

	captured := mcpCallToolEnvelope(t, server.Handler(), "ebus.v1.snapshot.capture", `{}`)
	snapshotID := captured["data"].(map[string]any)["snapshot_id"].(string)
	provider.SetRegulatorCapability(graphql.RegulatorCapabilityNone)
	snapshot := mcpCallToolEnvelope(t, server.Handler(), "ebus.v1.runtime.status.get", `{"consistency":{"mode":"SNAPSHOT","snapshot_id":"`+snapshotID+`"}}`)
	if got := snapshot["data"].(map[string]any)["regulator_capability"]; got != "PRESENT" {
		t.Fatalf("snapshot regulator capability = %#v; want PRESENT", got)
	}
	semanticSnapshot := mcpCallToolEnvelope(t, server.Handler(), "ebus.v1.semantic.snapshot.get", `{"planes":["runtime_status"],"consistency":{"mode":"SNAPSHOT","snapshot_id":"`+snapshotID+`"}}`)
	semanticRuntime := semanticSnapshot["data"].(map[string]any)["planes"].(map[string]any)["runtime_status"].(map[string]any)
	if got := semanticRuntime["regulator_capability"]; got != "PRESENT" {
		t.Fatalf("semantic snapshot regulator capability = %#v; want PRESENT", got)
	}
	if got := mcpCallToolEnvelope(t, server.Handler(), "ebus.v1.runtime.status.get", `{}`)["data"].(map[string]any)["regulator_capability"]; got != "NONE" {
		t.Fatalf("live regulator capability after update = %#v; want NONE", got)
	}
}

func TestIssue946RegulatorCapabilityInvalidProviderValueFailsClosed(t *testing.T) {
	provider := graphql.NewLiveSemanticProvider()
	provider.SetRegulatorCapability(graphql.RegulatorCapability("invalid"))
	if got := provider.RegulatorCapability(); got != graphql.RegulatorCapabilityUnknown {
		t.Fatalf("invalid provider capability = %q; want UNKNOWN", got)
	}
}
