package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sort"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/m8sourcestate"
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

type m8DebugSourceOwner interface {
	M8DebugSourceState() m8sourcestate.DebugState
}

type m8CommandRoutingOwner interface {
	M8CommandRoutingState() m8sourcestate.CommandRoutingFragment
}

type m8SemanticRegistryOwner interface {
	M8SemanticRegistryState() (m8sourcestate.SemanticRegistry, error)
}

func (server *Server) m8DirectSourceState(inputID string) (any, error) {
	switch inputID {
	case "ebus.debug":
		owner, ok := server.bus.(m8DebugSourceOwner)
		if !ok {
			return nil, fmt.Errorf("eBUS debug owner does not expose M8 source state")
		}
		return owner.M8DebugSourceState(), nil
	case "command.routing":
		return server.m8DirectCommandRoutingState()
	case "semantic.registry":
		owner, ok := server.semantic.(m8SemanticRegistryOwner)
		if !ok {
			return nil, fmt.Errorf("semantic registry owner does not expose M8 source state")
		}
		state, err := owner.M8SemanticRegistryState()
		if err != nil {
			return nil, err
		}
		state.Leaves = append([]m8sourcestate.SemanticLeaf(nil), state.Leaves...)
		sort.Slice(state.Leaves, func(i, j int) bool { return state.Leaves[i].Path < state.Leaves[j].Path })
		for index := 1; index < len(state.Leaves); index++ {
			if state.Leaves[index-1].Path == state.Leaves[index].Path {
				return nil, fmt.Errorf("duplicate semantic leaf %q", state.Leaves[index].Path)
			}
		}
		return state, nil
	default:
		return nil, fmt.Errorf("unsupported M8 source input %q", inputID)
	}
}

func (server *Server) m8DirectCommandRoutingState() (m8sourcestate.CommandRouting, error) {
	state := m8sourcestate.CommandRouting{Routes: []m8sourcestate.CommandRoute{}}
	for _, candidate := range []any{server.configWriter, server.scheduleWriter} {
		if candidate == nil {
			continue
		}
		owner, ok := candidate.(m8CommandRoutingOwner)
		if !ok {
			return m8sourcestate.CommandRouting{}, fmt.Errorf("command routing owner does not expose M8 source state")
		}
		fragment := owner.M8CommandRoutingState()
		state.Routes = append(state.Routes, fragment.Routes...)
		if fragment.Fallback != nil {
			if state.Fallback != nil {
				return m8sourcestate.CommandRouting{}, fmt.Errorf("multiple command routing fallbacks")
			}
			fallback := *fragment.Fallback
			state.Fallback = &fallback
		}
	}
	sort.Slice(state.Routes, func(i, j int) bool { return state.Routes[i].SemanticPath < state.Routes[j].SemanticPath })
	for index := 1; index < len(state.Routes); index++ {
		if state.Routes[index-1].SemanticPath == state.Routes[index].SemanticPath {
			return m8sourcestate.CommandRouting{}, fmt.Errorf("duplicate command route %q", state.Routes[index].SemanticPath)
		}
	}
	return state, nil
}
