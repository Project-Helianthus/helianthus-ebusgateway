package mcp

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	ebuserrors "github.com/d3vi1/helianthus-ebusgo/errors"
	"github.com/d3vi1/helianthus-ebusreg/registry"
	"github.com/d3vi1/helianthus-ebusreg/router"
)

type Registry interface {
	Iterate(func(registry.DeviceEntry) bool)
	Lookup(address byte) (registry.DeviceEntry, bool)
}

type Invoker interface {
	Invoke(ctx context.Context, plane router.Plane, methodName string, params map[string]any) (any, error)
}

type Server struct {
	registry Registry
	invoker  Invoker

	tools []Tool
}

func NewServer(reg Registry, invoker Invoker) (*Server, error) {
	if reg == nil {
		return nil, fmt.Errorf("mcp server missing registry: %w", ebuserrors.ErrInvalidPayload)
	}

	server := &Server{
		registry: reg,
		invoker:  invoker,
	}
	server.tools = []Tool{
		{
			Name:        "ebus.devices",
			Description: "List devices discovered on the eBUS, including planes and methods.",
			InputSchema: map[string]any{"type": "object", "additionalProperties": false},
		},
		{
			Name:        "ebus.invoke",
			Description: "Invoke a plane method on a device.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"address": map[string]any{"type": "integer", "minimum": 0, "maximum": 255},
					"plane":   map[string]any{"type": "string"},
					"method":  map[string]any{"type": "string"},
					"params":  map[string]any{"type": "object"},
				},
				"required":             []string{"address", "plane", "method"},
				"additionalProperties": false,
			},
		},
	}

	return server, nil
}

func (s *Server) Handler() http.Handler {
	return http.HandlerFunc(s.ServeHTTP)
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r == nil {
		http.Error(w, "request missing", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodPost:
		s.handlePost(w, r)
	default:
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handlePost(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		http.Error(w, "read failed", http.StatusBadRequest)
		return
	}
	defer func() { _ = r.Body.Close() }()

	var req rpcRequest
	if err := json.Unmarshal(body, &req); err != nil {
		s.writeRPCError(w, req.ID, rpcErrorInvalidRequest("invalid json"))
		return
	}
	if req.JSONRPC != "2.0" || req.Method == "" {
		s.writeRPCError(w, req.ID, rpcErrorInvalidRequest("invalid jsonrpc request"))
		return
	}

	result, rpcErr := s.dispatch(r.Context(), req.Method, req.Params)
	if req.ID == nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if rpcErr != nil {
		s.writeRPCError(w, req.ID, rpcErr)
		return
	}
	s.writeRPCResult(w, req.ID, result)
}

func (s *Server) dispatch(ctx context.Context, method string, params json.RawMessage) (any, *rpcError) {
	switch method {
	case "initialize":
		return s.handleInitialize(params)
	case "tools/list":
		return s.handleToolsList()
	case "tools/call":
		return s.handleToolsCall(ctx, params)
	case "ping":
		return map[string]any{}, nil
	default:
		return nil, rpcErrorMethodNotFound(fmt.Sprintf("method %q not found", method))
	}
}

func (s *Server) handleInitialize(params json.RawMessage) (any, *rpcError) {
	var initParams map[string]any
	if len(params) > 0 {
		if err := json.Unmarshal(params, &initParams); err != nil {
			return nil, rpcErrorInvalidParams("initialize params invalid")
		}
	}

	sessionID, err := newSessionID()
	if err != nil {
		return nil, rpcErrorInternal("failed to generate session id")
	}

	return map[string]any{
		"protocolVersion": "2025-03-26",
		"capabilities": map[string]any{
			"tools": map[string]any{},
		},
		"serverInfo": map[string]any{
			"name":    "helianthus-ebusgateway",
			"version": "0.0.0",
		},
		"sessionId": sessionID,
	}, nil
}

func (s *Server) handleToolsList() (any, *rpcError) {
	return map[string]any{
		"tools": s.tools,
	}, nil
}

type callToolParams struct {
	Name      string         `json:"name"`
	Arguments map[string]any `json:"arguments"`
}

func (s *Server) handleToolsCall(ctx context.Context, params json.RawMessage) (any, *rpcError) {
	var call callToolParams
	if err := json.Unmarshal(params, &call); err != nil {
		return nil, rpcErrorInvalidParams("tools/call params invalid")
	}
	if call.Name == "" {
		return nil, rpcErrorInvalidParams("tools/call missing name")
	}

	switch call.Name {
	case "ebus.devices":
		devices := s.listDevices()
		text := mustJSON(devices)
		return callToolResultText(text, false), nil
	case "ebus.invoke":
		if s.invoker == nil {
			return nil, rpcErrorInternal("server missing invoker")
		}
		out, err := s.invoke(ctx, call.Arguments)
		if err != nil {
			return callToolResultText(err.Error(), true), nil
		}
		return callToolResultText(mustJSON(out), false), nil
	default:
		return nil, rpcErrorInvalidParams(fmt.Sprintf("unknown tool %q", call.Name))
	}
}

type deviceInfo struct {
	Address         int         `json:"address"`
	Manufacturer    string      `json:"manufacturer"`
	DeviceID        string      `json:"device_id"`
	SoftwareVersion string      `json:"software_version"`
	HardwareVersion string      `json:"hardware_version"`
	Planes          []planeInfo `json:"planes"`
}

type planeInfo struct {
	Name    string       `json:"name"`
	Methods []methodInfo `json:"methods"`
}

type methodInfo struct {
	Name      string `json:"name"`
	ReadOnly  bool   `json:"read_only"`
	Primary   int    `json:"primary"`
	Secondary int    `json:"secondary"`
}

func (s *Server) listDevices() []deviceInfo {
	out := make([]deviceInfo, 0)
	s.registry.Iterate(func(entry registry.DeviceEntry) bool {
		device := deviceInfo{
			Address:         int(entry.Address()),
			Manufacturer:    entry.Manufacturer(),
			DeviceID:        entry.DeviceID(),
			SoftwareVersion: entry.SoftwareVersion(),
			HardwareVersion: entry.HardwareVersion(),
			Planes:          make([]planeInfo, 0),
		}

		for _, plane := range entry.Planes() {
			pi := planeInfo{
				Name:    plane.Name(),
				Methods: make([]methodInfo, 0, len(plane.Methods())),
			}
			for _, method := range plane.Methods() {
				template := method.Template()
				primary := 0
				secondary := 0
				if template != nil {
					primary = int(template.Primary())
					secondary = int(template.Secondary())
				}
				pi.Methods = append(pi.Methods, methodInfo{
					Name:      method.Name(),
					ReadOnly:  method.ReadOnly(),
					Primary:   primary,
					Secondary: secondary,
				})
			}
			device.Planes = append(device.Planes, pi)
		}

		out = append(out, device)
		return true
	})
	return out
}

func (s *Server) invoke(ctx context.Context, args map[string]any) (any, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if len(args) == 0 {
		return nil, fmt.Errorf("missing arguments: %w", ebuserrors.ErrInvalidPayload)
	}

	address, err := parseAddress(args["address"])
	if err != nil {
		return nil, err
	}
	planeName, _ := args["plane"].(string)
	if planeName == "" {
		return nil, fmt.Errorf("missing plane: %w", ebuserrors.ErrInvalidPayload)
	}
	methodName, _ := args["method"].(string)
	if methodName == "" {
		return nil, fmt.Errorf("missing method: %w", ebuserrors.ErrInvalidPayload)
	}
	params, _ := args["params"].(map[string]any)

	entry, ok := s.registry.Lookup(address)
	if !ok {
		return nil, fmt.Errorf("unknown device 0x%02x: %w", address, ebuserrors.ErrNoSuchDevice)
	}

	var plane registry.Plane
	for _, candidate := range entry.Planes() {
		if candidate != nil && candidate.Name() == planeName {
			plane = candidate
			break
		}
	}
	if plane == nil {
		return nil, fmt.Errorf("unknown plane %q: %w", planeName, ebuserrors.ErrInvalidPayload)
	}

	routerPlane, ok := plane.(router.Plane)
	if !ok {
		return nil, fmt.Errorf("plane %q not invokable: %w", planeName, ebuserrors.ErrInvalidPayload)
	}

	return s.invoker.Invoke(ctx, routerPlane, methodName, params)
}

func parseAddress(raw any) (byte, error) {
	switch value := raw.(type) {
	case int:
		return toAddress(value)
	case int64:
		return toAddress(int(value))
	case float64:
		return toAddress(int(value))
	default:
		return 0, fmt.Errorf("invalid address: %w", ebuserrors.ErrInvalidPayload)
	}
}

func toAddress(value int) (byte, error) {
	if value < 0 || value > 0xFF {
		return 0, fmt.Errorf("invalid address: %w", ebuserrors.ErrInvalidPayload)
	}
	return byte(value), nil
}

type Tool struct {
	Name        string         `json:"name"`
	Description string         `json:"description,omitempty"`
	InputSchema map[string]any `json:"inputSchema"`
}

func callToolResultText(text string, isError bool) map[string]any {
	return map[string]any{
		"content": []map[string]any{
			{"type": "text", "text": text},
		},
		"isError": isError,
	}
}

func mustJSON(value any) string {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Sprintf("%v", value)
	}
	return string(data)
}

func newSessionID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

type rpcRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      any             `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type rpcResponse struct {
	JSONRPC string    `json:"jsonrpc"`
	ID      any       `json:"id,omitempty"`
	Result  any       `json:"result,omitempty"`
	Error   *rpcError `json:"error,omitempty"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
}

func (s *Server) writeRPCResult(w http.ResponseWriter, id any, result any) {
	s.writeRPC(w, rpcResponse{JSONRPC: "2.0", ID: id, Result: result})
}

func (s *Server) writeRPCError(w http.ResponseWriter, id any, err *rpcError) {
	s.writeRPC(w, rpcResponse{JSONRPC: "2.0", ID: id, Error: err})
}

func (s *Server) writeRPC(w http.ResponseWriter, response rpcResponse) {
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(response)
}

func rpcErrorInvalidRequest(message string) *rpcError {
	return &rpcError{Code: -32600, Message: message}
}

func rpcErrorMethodNotFound(message string) *rpcError {
	return &rpcError{Code: -32601, Message: message}
}

func rpcErrorInvalidParams(message string) *rpcError {
	return &rpcError{Code: -32602, Message: message}
}

func rpcErrorInternal(message string) *rpcError {
	return &rpcError{Code: -32603, Message: message}
}
