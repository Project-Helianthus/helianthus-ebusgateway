package mcp

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"sort"
	"strings"
	"time"

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

type ServiceStatus struct {
	Status           string `json:"status"`
	FirmwareVersion  string `json:"firmware_version"`
	UpdatesAvailable bool   `json:"updates_available"`
	InitiatorAddress string `json:"initiator_address,omitempty"`
}

type StatusProvider interface {
	DaemonStatus() ServiceStatus
	AdapterStatus() ServiceStatus
}

type Zone struct {
	ID            string   `json:"id"`
	Name          string   `json:"name"`
	OperatingMode string   `json:"operating_mode"`
	Preset        string   `json:"preset"`
	CurrentTempC  *float64 `json:"current_temp_c,omitempty"`
	TargetTempC   *float64 `json:"target_temp_c,omitempty"`
	HeatingDemand *float64 `json:"heating_demand,omitempty"`
}

type DhwStatus struct {
	OperatingMode string   `json:"operating_mode"`
	Preset        string   `json:"preset"`
	CurrentTempC  *float64 `json:"current_temp_c,omitempty"`
	TargetTempC   *float64 `json:"target_temp_c,omitempty"`
	HeatingDemand *float64 `json:"heating_demand,omitempty"`
}

type EnergySeries struct {
	Today  float64   `json:"today"`
	Yearly []float64 `json:"yearly"`
}

type EnergyChannel struct {
	DHW     EnergySeries `json:"dhw"`
	Climate EnergySeries `json:"climate"`
}

type EnergyTotals struct {
	Gas      EnergyChannel `json:"gas"`
	Electric EnergyChannel `json:"electric"`
	Solar    EnergyChannel `json:"solar"`
}

type SemanticProvider interface {
	Zones() []Zone
	DHW() *DhwStatus
	EnergyTotals() *EnergyTotals
}

type Server struct {
	registry       Registry
	invoker        Invoker
	statusProvider StatusProvider
	semantic       SemanticProvider

	tools []Tool
}

const (
	toolRuntimeStatusGetName  = "ebus.v1.runtime.status.get"
	toolSemanticZonesGetName  = "ebus.v1.semantic.zones.get"
	toolSemanticDHWGetName    = "ebus.v1.semantic.dhw.get"
	toolSemanticEnergyGetName = "ebus.v1.semantic.energy_totals.get"
	toolDevicesV1Name         = "ebus.v1.registry.devices.list"
	toolDeviceGetV1Name       = "ebus.v1.registry.devices.get"
	toolPlanesListV1Name      = "ebus.v1.registry.planes.list"
	toolMethodsListV1Name     = "ebus.v1.registry.methods.list"
	toolInvokeV1Name          = "ebus.v1.rpc.invoke"
	toolDevicesLegacyName     = "ebus.devices"
	toolInvokeLegacyName      = "ebus.invoke"
	methodMutabilityUnknown   = "unknown"
	methodMutabilityReadOnly  = "read_only"
	methodMutabilityMutating  = "mutating"
	methodDangerUnknown       = "unknown"
	methodDangerSafe          = "safe"
	methodDangerDangerous     = "dangerous"
)

var errInvokePermissionDenied = errors.New("invoke permission denied")

type staticStatusProvider struct{}
type staticSemanticProvider struct{}

func (staticStatusProvider) DaemonStatus() ServiceStatus {
	return ServiceStatus{
		Status:           "running",
		FirmwareVersion:  "",
		UpdatesAvailable: false,
		InitiatorAddress: "",
	}
}

func (staticStatusProvider) AdapterStatus() ServiceStatus {
	return ServiceStatus{
		Status:           "unknown",
		FirmwareVersion:  "",
		UpdatesAvailable: false,
	}
}

func (staticSemanticProvider) Zones() []Zone {
	return nil
}

func (staticSemanticProvider) DHW() *DhwStatus {
	return nil
}

func (staticSemanticProvider) EnergyTotals() *EnergyTotals {
	return nil
}

func NewServer(reg Registry, invoker Invoker) (*Server, error) {
	if reg == nil {
		return nil, fmt.Errorf("mcp server missing registry: %w", ebuserrors.ErrInvalidPayload)
	}

	server := &Server{
		registry:       reg,
		invoker:        invoker,
		statusProvider: staticStatusProvider{},
		semantic:       staticSemanticProvider{},
	}
	server.tools = []Tool{
		{
			Name:        toolRuntimeStatusGetName,
			Description: "Get runtime daemon and adapter status.",
			InputSchema: map[string]any{"type": "object", "additionalProperties": false},
		},
		{
			Name:        toolSemanticZonesGetName,
			Description: "Get semantic zones snapshot.",
			InputSchema: map[string]any{"type": "object", "additionalProperties": false},
		},
		{
			Name:        toolSemanticDHWGetName,
			Description: "Get semantic domestic hot water snapshot.",
			InputSchema: map[string]any{"type": "object", "additionalProperties": false},
		},
		{
			Name:        toolSemanticEnergyGetName,
			Description: "Get semantic energy totals snapshot.",
			InputSchema: map[string]any{"type": "object", "additionalProperties": false},
		},
		{
			Name:        toolDevicesV1Name,
			Description: "List devices discovered on the eBUS, including planes and methods.",
			InputSchema: map[string]any{"type": "object", "additionalProperties": false},
		},
		{
			Name:        toolDeviceGetV1Name,
			Description: "Get one device by address.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"address": map[string]any{"type": "integer", "minimum": 0, "maximum": 255},
				},
				"required":             []string{"address"},
				"additionalProperties": false,
			},
		},
		{
			Name:        toolPlanesListV1Name,
			Description: "List registry planes for one device address.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"address": map[string]any{"type": "integer", "minimum": 0, "maximum": 255},
				},
				"required":             []string{"address"},
				"additionalProperties": false,
			},
		},
		{
			Name:        toolMethodsListV1Name,
			Description: "List registry methods for a device address and plane.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"address": map[string]any{"type": "integer", "minimum": 0, "maximum": 255},
					"plane":   map[string]any{"type": "string"},
				},
				"required":             []string{"address", "plane"},
				"additionalProperties": false,
			},
		},
		{
			Name:        toolInvokeV1Name,
			Description: "Invoke a plane method on a device.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"address":         map[string]any{"type": "integer", "minimum": 0, "maximum": 255},
					"plane":           map[string]any{"type": "string"},
					"method":          map[string]any{"type": "string"},
					"params":          map[string]any{"type": "object"},
					"intent":          map[string]any{"type": "string", "enum": []string{"READ_ONLY", "MUTATE"}},
					"allow_dangerous": map[string]any{"type": "boolean"},
					"idempotency_key": map[string]any{"type": "string"},
				},
				"required":             []string{"address", "plane", "method", "intent", "allow_dangerous"},
				"additionalProperties": false,
			},
		},
		{
			Name:        toolDevicesLegacyName,
			Description: "Compatibility alias for ebus.v1.registry.devices.list.",
			InputSchema: map[string]any{"type": "object", "additionalProperties": false},
		},
		{
			Name:        toolInvokeLegacyName,
			Description: "Compatibility alias for ebus.v1.rpc.invoke.",
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

func (s *Server) SetStatusProvider(provider StatusProvider) {
	if s == nil || provider == nil {
		return
	}
	s.statusProvider = provider
}

func (s *Server) SetSemanticProvider(provider SemanticProvider) {
	if s == nil || provider == nil {
		return
	}
	s.semantic = provider
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
	case toolRuntimeStatusGetName:
		status := map[string]any{
			"daemon_status":  s.statusProvider.DaemonStatus(),
			"adapter_status": s.statusProvider.AdapterStatus(),
		}
		return callToolResultText(mustJSON(newToolEnvelope(status, nil)), false), nil
	case toolSemanticZonesGetName:
		zones := s.snapshotZones()
		return callToolResultText(mustJSON(newToolEnvelope(zones, nil)), false), nil
	case toolSemanticDHWGetName:
		return callToolResultText(mustJSON(newToolEnvelope(s.snapshotDHW(), nil)), false), nil
	case toolSemanticEnergyGetName:
		return callToolResultText(mustJSON(newToolEnvelope(s.snapshotEnergyTotals(), nil)), false), nil
	case toolDevicesV1Name, toolDevicesLegacyName:
		devices := s.listDevices()
		text := mustJSON(newToolEnvelope(devices, nil))
		return callToolResultText(text, false), nil
	case toolDeviceGetV1Name:
		device, err := s.getDevice(call.Arguments)
		if err != nil {
			return callToolResultText(mustJSON(newToolEnvelope(nil, err)), true), nil
		}
		return callToolResultText(mustJSON(newToolEnvelope(device, nil)), false), nil
	case toolPlanesListV1Name:
		planes, err := s.listPlanes(call.Arguments)
		if err != nil {
			return callToolResultText(mustJSON(newToolEnvelope(nil, err)), true), nil
		}
		return callToolResultText(mustJSON(newToolEnvelope(planes, nil)), false), nil
	case toolMethodsListV1Name:
		methods, err := s.listMethods(call.Arguments)
		if err != nil {
			return callToolResultText(mustJSON(newToolEnvelope(nil, err)), true), nil
		}
		return callToolResultText(mustJSON(newToolEnvelope(methods, nil)), false), nil
	case toolInvokeV1Name:
		if s.invoker == nil {
			return nil, rpcErrorInternal("server missing invoker")
		}
		if err := s.enforceInvokeV1Safety(call.Arguments); err != nil {
			return callToolResultText(mustJSON(newToolEnvelope(nil, err)), true), nil
		}
		out, err := s.invoke(ctx, call.Arguments)
		if err != nil {
			return callToolResultText(mustJSON(newToolEnvelope(nil, err)), true), nil
		}
		return callToolResultText(mustJSON(newToolEnvelope(out, nil)), false), nil
	case toolInvokeLegacyName:
		if s.invoker == nil {
			return nil, rpcErrorInternal("server missing invoker")
		}
		out, err := s.invoke(ctx, call.Arguments)
		if err != nil {
			return callToolResultText(mustJSON(newToolEnvelope(nil, err)), true), nil
		}
		return callToolResultText(mustJSON(newToolEnvelope(out, nil)), false), nil
	default:
		return nil, rpcErrorInvalidParams(fmt.Sprintf("unknown tool %q", call.Name))
	}
}

func newToolEnvelope(data any, err error) map[string]any {
	meta := map[string]any{
		"contract": map[string]any{
			"name":  "helianthus-ebus-mcp",
			"major": 1,
			"minor": 0,
		},
		"consistency": map[string]any{
			"mode": "LIVE",
		},
		"data_timestamp": time.Now().UTC().Format(time.RFC3339Nano),
		"data_hash":      hashData(data),
	}

	var envelopeError any
	if err != nil {
		code, retriable, sourceLayer := classifyToolError(err)
		envelopeError = map[string]any{
			"code":         code,
			"message":      err.Error(),
			"retriable":    retriable,
			"source_layer": sourceLayer,
		}
	}

	return map[string]any{
		"meta":  meta,
		"data":  data,
		"error": envelopeError,
	}
}

func hashData(data any) string {
	raw, err := json.Marshal(data)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func classifyToolError(err error) (code string, retriable bool, sourceLayer string) {
	switch {
	case errors.Is(err, errInvokePermissionDenied):
		return "PERMISSION_DENIED", false, "gateway"
	case errors.Is(err, ebuserrors.ErrInvalidPayload):
		return "INVALID_ARGUMENT", false, "ebusreg"
	case errors.Is(err, ebuserrors.ErrNoSuchDevice):
		return "NOT_FOUND", false, "ebusreg"
	case errors.Is(err, ebuserrors.ErrNACK):
		return "PROTOCOL_ERROR", false, "ebusgo"
	case errors.Is(err, ebuserrors.ErrTimeout):
		return "TIMEOUT", true, "ebusgo"
	case errors.Is(err, ebuserrors.ErrCRCMismatch):
		return "PROTOCOL_ERROR", true, "ebusgo"
	case errors.Is(err, ebuserrors.ErrTransportClosed):
		return "BUS_UNAVAILABLE", false, "ebusgo"
	case errors.Is(err, ebuserrors.ErrBusCollision):
		return "BUS_UNAVAILABLE", true, "ebusgo"
	case errors.Is(err, ebuserrors.ErrRetryExhausted):
		return "BUS_UNAVAILABLE", true, "ebusgo"
	default:
		return "INTERNAL", false, "gateway"
	}
}

func (s *Server) enforceInvokeV1Safety(args map[string]any) error {
	address, err := parseAddress(args["address"])
	if err != nil {
		return err
	}
	planeName, _ := args["plane"].(string)
	if planeName == "" {
		return fmt.Errorf("missing plane: %w", ebuserrors.ErrInvalidPayload)
	}
	methodName, _ := args["method"].(string)
	if methodName == "" {
		return fmt.Errorf("missing method: %w", ebuserrors.ErrInvalidPayload)
	}
	intent, _ := args["intent"].(string)
	intent = strings.TrimSpace(intent)
	if intent == "" {
		return fmt.Errorf("missing intent: %w", ebuserrors.ErrInvalidPayload)
	}
	allowDangerous, ok := args["allow_dangerous"].(bool)
	if !ok {
		return fmt.Errorf("missing allow_dangerous: %w", ebuserrors.ErrInvalidPayload)
	}

	methodKnown, methodReadOnly, err := s.lookupMethodMutability(address, planeName, methodName)
	if err != nil {
		return err
	}

	switch intent {
	case "READ_ONLY":
		if !methodKnown || !methodReadOnly {
			return fmt.Errorf("READ_ONLY intent denied for mutating/unknown method: %w", errInvokePermissionDenied)
		}
	case "MUTATE":
		if !allowDangerous {
			return fmt.Errorf("MUTATE intent requires allow_dangerous=true: %w", errInvokePermissionDenied)
		}
		idempotencyKey, _ := args["idempotency_key"].(string)
		if strings.TrimSpace(idempotencyKey) == "" {
			return fmt.Errorf("MUTATE intent requires idempotency_key: %w", errInvokePermissionDenied)
		}
	default:
		return fmt.Errorf("invalid intent %q: %w", intent, ebuserrors.ErrInvalidPayload)
	}

	return nil
}

func (s *Server) lookupMethodMutability(address byte, planeName string, methodName string) (methodKnown bool, methodReadOnly bool, err error) {
	entry, ok := s.registry.Lookup(address)
	if !ok {
		return false, false, fmt.Errorf("unknown device 0x%02x: %w", address, ebuserrors.ErrNoSuchDevice)
	}

	var plane registry.Plane
	for _, candidate := range entry.Planes() {
		if candidate != nil && candidate.Name() == planeName {
			plane = candidate
			break
		}
	}
	if plane == nil {
		return false, false, fmt.Errorf("unknown plane %q: %w", planeName, ebuserrors.ErrInvalidPayload)
	}

	for _, method := range plane.Methods() {
		if method != nil && method.Name() == methodName {
			return true, method.ReadOnly(), nil
		}
	}
	return false, false, nil
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
	Name     string       `json:"name"`
	Routable bool         `json:"routable"`
	Methods  []methodInfo `json:"methods"`
}

type methodInfo struct {
	Name       string `json:"name"`
	ReadOnly   bool   `json:"read_only"`
	Primary    int    `json:"primary"`
	Secondary  int    `json:"secondary"`
	Mutability string `json:"mutability"`
	Danger     string `json:"danger_level"`
	Routable   bool   `json:"routable"`
}

func (s *Server) listDevices() []deviceInfo {
	out := make([]deviceInfo, 0)
	s.registry.Iterate(func(entry registry.DeviceEntry) bool {
		out = append(out, buildDeviceInfo(entry))
		return true
	})
	sort.Slice(out, func(i, j int) bool {
		if out[i].Address != out[j].Address {
			return out[i].Address < out[j].Address
		}
		if out[i].Manufacturer != out[j].Manufacturer {
			return out[i].Manufacturer < out[j].Manufacturer
		}
		return out[i].DeviceID < out[j].DeviceID
	})
	return out
}

func (s *Server) getDevice(args map[string]any) (deviceInfo, error) {
	address, err := parseAddress(args["address"])
	if err != nil {
		return deviceInfo{}, err
	}
	entry, ok := s.registry.Lookup(address)
	if !ok {
		return deviceInfo{}, fmt.Errorf("unknown device 0x%02x: %w", address, ebuserrors.ErrNoSuchDevice)
	}
	return buildDeviceInfo(entry), nil
}

func (s *Server) listPlanes(args map[string]any) ([]planeInfo, error) {
	address, err := parseAddress(args["address"])
	if err != nil {
		return nil, err
	}
	entry, ok := s.registry.Lookup(address)
	if !ok {
		return nil, fmt.Errorf("unknown device 0x%02x: %w", address, ebuserrors.ErrNoSuchDevice)
	}
	return buildPlaneInfoList(entry.Planes()), nil
}

func (s *Server) listMethods(args map[string]any) ([]methodInfo, error) {
	address, err := parseAddress(args["address"])
	if err != nil {
		return nil, err
	}
	planeName, _ := args["plane"].(string)
	planeName = strings.TrimSpace(planeName)
	if planeName == "" {
		return nil, fmt.Errorf("missing plane: %w", ebuserrors.ErrInvalidPayload)
	}

	entry, ok := s.registry.Lookup(address)
	if !ok {
		return nil, fmt.Errorf("unknown device 0x%02x: %w", address, ebuserrors.ErrNoSuchDevice)
	}
	plane, found := findPlane(entry.Planes(), planeName)
	if !found {
		return nil, fmt.Errorf("unknown plane %q: %w", planeName, ebuserrors.ErrInvalidPayload)
	}

	return buildMethodInfoList(plane), nil
}

func (s *Server) snapshotZones() []Zone {
	if s == nil || s.semantic == nil {
		return nil
	}
	zones := s.semantic.Zones()
	if len(zones) == 0 {
		return nil
	}
	out := make([]Zone, len(zones))
	copy(out, zones)
	sort.Slice(out, func(i, j int) bool {
		if out[i].ID != out[j].ID {
			return out[i].ID < out[j].ID
		}
		return out[i].Name < out[j].Name
	})
	return out
}

func (s *Server) snapshotDHW() *DhwStatus {
	if s == nil || s.semantic == nil {
		return nil
	}
	source := s.semantic.DHW()
	if source == nil {
		return nil
	}
	copy := *source
	return &copy
}

func (s *Server) snapshotEnergyTotals() *EnergyTotals {
	if s == nil || s.semantic == nil {
		return nil
	}
	source := s.semantic.EnergyTotals()
	if source == nil {
		return nil
	}
	copy := *source
	copy.Gas = cloneEnergyChannel(copy.Gas)
	copy.Electric = cloneEnergyChannel(copy.Electric)
	copy.Solar = cloneEnergyChannel(copy.Solar)
	return &copy
}

func cloneEnergyChannel(channel EnergyChannel) EnergyChannel {
	channel.DHW = cloneEnergySeries(channel.DHW)
	channel.Climate = cloneEnergySeries(channel.Climate)
	return channel
}

func cloneEnergySeries(series EnergySeries) EnergySeries {
	if len(series.Yearly) == 0 {
		return EnergySeries{Today: series.Today}
	}
	values := make([]float64, len(series.Yearly))
	copy(values, series.Yearly)
	return EnergySeries{
		Today:  series.Today,
		Yearly: values,
	}
}

func buildDeviceInfo(entry registry.DeviceEntry) deviceInfo {
	return deviceInfo{
		Address:         int(entry.Address()),
		Manufacturer:    entry.Manufacturer(),
		DeviceID:        entry.DeviceID(),
		SoftwareVersion: entry.SoftwareVersion(),
		HardwareVersion: entry.HardwareVersion(),
		Planes:          buildPlaneInfoList(entry.Planes()),
	}
}

func buildPlaneInfoList(planes []registry.Plane) []planeInfo {
	out := make([]planeInfo, 0, len(planes))
	for _, plane := range planes {
		if plane == nil {
			continue
		}
		_, routable := plane.(router.Plane)
		out = append(out, planeInfo{
			Name:     plane.Name(),
			Routable: routable,
			Methods:  buildMethodInfoList(plane),
		})
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Name < out[j].Name
	})
	return out
}

func buildMethodInfoList(plane registry.Plane) []methodInfo {
	if plane == nil {
		return nil
	}
	methods := plane.Methods()
	out := make([]methodInfo, 0, len(methods))
	for _, method := range methods {
		if method == nil {
			continue
		}
		template := method.Template()
		primary := 0
		secondary := 0
		if template != nil {
			primary = int(template.Primary())
			secondary = int(template.Secondary())
		}
		mutability, danger, routable := extractMethodMetadata(method, plane)
		out = append(out, methodInfo{
			Name:       method.Name(),
			ReadOnly:   method.ReadOnly(),
			Primary:    primary,
			Secondary:  secondary,
			Mutability: mutability,
			Danger:     danger,
			Routable:   routable,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Name != out[j].Name {
			return out[i].Name < out[j].Name
		}
		if out[i].Primary != out[j].Primary {
			return out[i].Primary < out[j].Primary
		}
		return out[i].Secondary < out[j].Secondary
	})
	return out
}

func extractMethodMetadata(method registry.Method, plane registry.Plane) (mutability string, danger string, routable bool) {
	if method == nil {
		return methodMutabilityUnknown, methodDangerUnknown, false
	}

	if method.ReadOnly() {
		mutability = methodMutabilityReadOnly
	} else {
		mutability = methodMutabilityMutating
	}
	if value, ok := invokeStringNoArg(method, "Mutability"); ok {
		if normalized, valid := normalizeMethodMutability(value); valid {
			mutability = normalized
		}
	}

	if mutability == methodMutabilityReadOnly {
		danger = methodDangerSafe
	} else {
		danger = methodDangerDangerous
	}
	if value, ok := invokeStringNoArg(method, "Danger"); ok {
		if normalized, valid := normalizeMethodDanger(value); valid {
			danger = normalized
		}
	}

	_, routable = plane.(router.Plane)
	if value, ok := invokeBoolNoArg(method, "Routable"); ok {
		routable = value
	}

	return mutability, danger, routable
}

func normalizeMethodMutability(raw string) (string, bool) {
	value := strings.ToLower(strings.TrimSpace(raw))
	value = strings.ReplaceAll(value, "-", "_")
	switch value {
	case methodMutabilityUnknown, methodMutabilityReadOnly, methodMutabilityMutating:
		return value, true
	default:
		return "", false
	}
}

func normalizeMethodDanger(raw string) (string, bool) {
	value := strings.ToLower(strings.TrimSpace(raw))
	switch value {
	case methodDangerUnknown, methodDangerSafe, methodDangerDangerous:
		return value, true
	default:
		return "", false
	}
}

func invokeStringNoArg(target any, methodName string) (string, bool) {
	if target == nil {
		return "", false
	}
	value := reflect.ValueOf(target)
	method := value.MethodByName(methodName)
	if !method.IsValid() || method.Type().NumIn() != 0 || method.Type().NumOut() != 1 {
		return "", false
	}
	out := method.Call(nil)
	if len(out) != 1 {
		return "", false
	}
	return fmt.Sprint(out[0].Interface()), true
}

func invokeBoolNoArg(target any, methodName string) (bool, bool) {
	if target == nil {
		return false, false
	}
	value := reflect.ValueOf(target)
	method := value.MethodByName(methodName)
	if !method.IsValid() || method.Type().NumIn() != 0 || method.Type().NumOut() != 1 || method.Type().Out(0).Kind() != reflect.Bool {
		return false, false
	}
	out := method.Call(nil)
	if len(out) != 1 {
		return false, false
	}
	return out[0].Bool(), true
}

func findPlane(planes []registry.Plane, planeName string) (registry.Plane, bool) {
	for _, plane := range planes {
		if plane != nil && plane.Name() == planeName {
			return plane, true
		}
	}
	return nil, false
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
