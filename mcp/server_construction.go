package mcp

import (
	"fmt"

	ebuserrors "github.com/Project-Helianthus/helianthus-ebusgo/errors"
	ebusstdcat "github.com/Project-Helianthus/helianthus-ebusreg/catalog/ebus_standard"
)

func NewServer(reg Registry, invoker Invoker) (*Server, error) {
	if reg == nil {
		return nil, fmt.Errorf("mcp server missing registry: %w", ebuserrors.ErrInvalidPayload)
	}

	server := &Server{
		registry:       reg,
		invoker:        invoker,
		statusProvider: staticStatusProvider{},
		watch:          staticWatchSummaryProvider{},
		semantic:       staticSemanticProvider{},
		idempotency:    make(map[string]idempotencyEntry),
		snapshots:      make(map[string]snapshotState),
		serverVersion:  "0.0.0",
	}
	server.tools = defaultServerTools()

	// Wire the ebus_standard L7 MCP surfaces (M4_GATEWAY_MCP). The
	// embedded catalog is SHA256-pinned and consumed read-only; see
	// mcp/ebus_standard_wiring.go. Without this call the four
	// ebus.v1.ebus_standard.* surfaces are unreachable at runtime because
	// handleToolsCall rejects unknown tool names before dispatch.
	RegisterEbusStandardTools(server, ebusstdcat.MustEmbeddedCatalog())

	return server, nil
}
