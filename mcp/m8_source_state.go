package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
)

const m8SourceStateToolName = "helianthus.experimental.m8_source_state.get"

var m8SourceStateInputIDs = []string{
	"ebus.debug",
	"command.routing",
	"semantic.registry",
}

func m8SourceStateTool() Tool {
	return Tool{
		Name:        m8SourceStateToolName,
		Description: "Capture one direct owner-local M8 source-state observation.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"input_id": map[string]any{"type": "string", "enum": append([]string(nil), m8SourceStateInputIDs...)},
			},
			"required":             []string{"input_id"},
			"additionalProperties": false,
		},
	}
}

func (server *Server) handleM8SourceState(ctx context.Context, raw json.RawMessage) (any, *rpcError) {
	if eebusV1BoundaryFromContext(ctx) != eebusV1OperatorBoundary || m8SourceScopeFromContext(ctx) {
		return nil, rpcErrorInvalidParams(fmt.Sprintf("unknown tool %q", m8SourceStateToolName))
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var request struct {
		InputID string `json:"input_id"`
	}
	if err := decoder.Decode(&request); err != nil || decoder.Decode(&struct{}{}) != io.EOF || !m8SourceStateInputAllowed(request.InputID) {
		return callToolResultText(mustJSON(map[string]any{"category": "INVALID_REQUEST"}), true), nil
	}
	if !server.m8SourceStateAvailable() {
		return nil, rpcErrorInvalidParams(fmt.Sprintf("unknown tool %q", m8SourceStateToolName))
	}
	value, err := server.m8DirectSourceState(request.InputID)
	if err != nil {
		return callToolResultText(mustJSON(map[string]any{"category": "ACQUISITION_FAILED"}), true), nil
	}
	return callToolResultText(mustJSON(map[string]any{"input_id": request.InputID, "data": value}), false), nil
}

func m8SourceStateInputAllowed(inputID string) bool {
	for _, allowed := range m8SourceStateInputIDs {
		if inputID == allowed {
			return true
		}
	}
	return false
}

func (server *Server) m8SourceStateAvailable() bool {
	if server == nil {
		return false
	}
	server.eebusV1Mu.RLock()
	defer server.eebusV1Mu.RUnlock()
	return server.eebusV1 != nil
}

type m8DirectRoute struct {
	SemanticPath string `json:"semantic_path"`
	Source       string `json:"source"`
}

var m8SemanticReadTools = []string{
	toolSemanticZonesGetName,
	toolSemanticCircuitsGetName,
	toolSemanticRadioGetName,
	toolSemanticFM5ModeGetName,
	toolSemanticSolarGetName,
	toolSemanticCylindersGetName,
	toolSemanticDHWGetName,
	toolSemanticEnergyGetName,
	toolSemanticBoilerGetName,
	toolSemanticSystemGetName,
	toolSemanticAdapterInfoGetName,
	toolSemanticSchedulesGetName,
	toolSemanticSnapshotName,
}

func (server *Server) m8DirectSourceState(inputID string) (any, error) {
	switch inputID {
	case "ebus.debug":
		deviceCount := 0
		server.registry.IterateSnapshots(func(registry.DeviceEntrySnapshot) bool {
			deviceCount++
			return true
		})
		runtimeState := "unavailable"
		if server.statusProvider != nil {
			runtimeState = server.statusProvider.DaemonStatus().Status
		}
		return map[string]any{
			"bus_observability_registered": server.bus != nil,
			"registry_device_count":        deviceCount,
			"runtime_state":                runtimeState,
			"semantic_provider_registered": server.semantic != nil,
		}, nil
	case "command.routing":
		return map[string]any{"fallback": nil, "routes": server.m8DirectCommandRoutes()}, nil
	case "semantic.registry":
		leaves := make([]map[string]any, 0, len(m8SemanticReadTools))
		if server.semantic != nil {
			for _, name := range m8SemanticReadTools {
				if server.hasToolNamed(name) {
					leaves = append(leaves, map[string]any{
						"path": "/mcp/" + name, "promotion_state": "PROMOTED", "source": "ebus",
					})
				}
			}
		}
		return map[string]any{"authority": "ebus.promoted", "leaves": leaves}, nil
	default:
		return nil, fmt.Errorf("unsupported M8 source input %q", inputID)
	}
}

func (server *Server) m8DirectCommandRoutes() []m8DirectRoute {
	routes := make([]m8DirectRoute, 0, 4)
	if server.configWriter != nil {
		for _, name := range []string{toolSemanticSystemSetConfigName, toolSemanticBoilerSetConfigName} {
			routes = append(routes, m8DirectRoute{SemanticPath: "/mcp/" + name, Source: "ebus"})
		}
	}
	if server.scheduleWriter != nil {
		for _, name := range []string{toolSemanticSchedulesSetZoneName, toolSemanticSchedulesSetDhwName} {
			routes = append(routes, m8DirectRoute{SemanticPath: "/mcp/" + name, Source: "ebus"})
		}
	}
	return routes
}
