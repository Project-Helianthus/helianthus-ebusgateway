package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
)

const m8SourceStateToolName = "helianthus.experimental.m8_source_state.get"

var m8SourceStateInputIDs = []string{
	"ebus.debug",
	"command.routing",
	"semantic.registry",
}

type M8SourceStateProvider interface {
	M8SourceState(context.Context, string) (json.RawMessage, error)
}

func (server *Server) RegisterM8SourceStateProvider(provider M8SourceStateProvider) error {
	if server == nil {
		return errors.New("MCP server is nil")
	}
	if nilM8SourceStateProvider(provider) {
		return errors.New("M8 source-state provider is nil")
	}
	server.eebusV1Mu.Lock()
	defer server.eebusV1Mu.Unlock()
	if server.m8SourceState != nil {
		return errors.New("M8 source-state provider is already registered")
	}
	server.m8SourceState = provider
	return nil
}

func nilM8SourceStateProvider(provider M8SourceStateProvider) bool {
	if provider == nil {
		return true
	}
	value := reflect.ValueOf(provider)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
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
	server.eebusV1Mu.RLock()
	provider := server.m8SourceState
	server.eebusV1Mu.RUnlock()
	if nilM8SourceStateProvider(provider) {
		return nil, rpcErrorInvalidParams(fmt.Sprintf("unknown tool %q", m8SourceStateToolName))
	}
	value, err := provider.M8SourceState(ctx, request.InputID)
	if err != nil || len(value) == 0 || !json.Valid(value) {
		return callToolResultText(mustJSON(map[string]any{"category": "ACQUISITION_FAILED"}), true), nil
	}
	var decoded any
	if err := json.Unmarshal(value, &decoded); err != nil {
		return callToolResultText(mustJSON(map[string]any{"category": "ACQUISITION_FAILED"}), true), nil
	}
	return callToolResultText(mustJSON(map[string]any{"input_id": request.InputID, "data": decoded}), false), nil
}

func m8SourceStateInputAllowed(inputID string) bool {
	for _, allowed := range m8SourceStateInputIDs {
		if inputID == allowed {
			return true
		}
	}
	return false
}
