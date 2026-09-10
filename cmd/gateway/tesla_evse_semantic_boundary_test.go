package main

import (
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/modbusadapter"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
)

// The only EVSE source accepted by issue #961 is an in-process injected
// record bundle.  Production gateway construction must not turn an MCP/GraphQL
// semantic read surface into a serial/TCP acquisition or control route.
func TestGatewayProductionCompositionDoesNotProvideTeslaEVSESemanticRecords(t *testing.T) {
	provider := newGatewayModbusMCPProvider(&modbusadapter.Adapter{})
	if _, ok := provider.(mcp.TeslaGen3EVSESemanticProvider); ok {
		t.Fatal("production gateway unexpectedly composes Tesla EVSE semantic records")
	}
}
