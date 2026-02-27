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
	"sync"
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

type BoilerState struct {
	FlowTemperatureC         *float64 `json:"flow_temperature_c,omitempty"`
	ReturnTemperatureC       *float64 `json:"return_temperature_c,omitempty"`
	CentralHeatingPumpActive *bool    `json:"central_heating_pump_active,omitempty"`
	DhwTemperatureC          *float64 `json:"dhw_temperature_c,omitempty"`
	DhwTargetTemperatureC    *float64 `json:"dhw_target_temperature_c,omitempty"`
}

type BoilerConfig struct {
	DhwOperatingMode *string `json:"dhw_operating_mode,omitempty"`
}

type BoilerDiagnostics struct {
	HeatingStatusRaw *int `json:"heating_status_raw,omitempty"`
	DhwStatusRaw     *int `json:"dhw_status_raw,omitempty"`
}

type BoilerStatus struct {
	State       *BoilerState       `json:"state,omitempty"`
	Config      *BoilerConfig      `json:"config,omitempty"`
	Diagnostics *BoilerDiagnostics `json:"diagnostics,omitempty"`
}

type SemanticProvider interface {
	Zones() []Zone
	DHW() *DhwStatus
	EnergyTotals() *EnergyTotals
	BoilerStatus() *BoilerStatus
}

type Server struct {
	registry       Registry
	invoker        Invoker
	statusProvider StatusProvider
	semantic       SemanticProvider
	idempotencyMu  sync.Mutex
	idempotency    map[string]idempotencyEntry
	snapshotMu     sync.RWMutex
	snapshots      map[string]snapshotState

	tools []Tool
}

const (
	toolRuntimeStatusGetName  = "ebus.v1.runtime.status.get"
	toolSemanticZonesGetName  = "ebus.v1.semantic.zones.get"
	toolSemanticDHWGetName    = "ebus.v1.semantic.dhw.get"
	toolSemanticEnergyGetName  = "ebus.v1.semantic.energy_totals.get"
	toolSemanticBoilerGetName  = "ebus.v1.semantic.boiler_status.get"
	toolSemanticSnapshotName   = "ebus.v1.semantic.snapshot.get"
	toolSnapshotCaptureName   = "ebus.v1.snapshot.capture"
	toolSnapshotDropName      = "ebus.v1.snapshot.drop"
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
	defaultInvokeTimeout      = 3 * time.Second
	defaultIdempotencyTTL     = 30 * time.Second
	defaultSnapshotTTL        = 5 * time.Minute
	defaultSnapshotReadTTL    = 10 * time.Second
)

var errInvokePermissionDenied = errors.New("invoke permission denied")
var errInvokeIdempotencyConflict = errors.New("invoke idempotency conflict")
var errSnapshotNotFound = errors.New("snapshot not found")

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

func (staticSemanticProvider) BoilerStatus() *BoilerStatus {
	return nil
}

type idempotencyEntry struct {
	signature string
	result    any
	expiresAt time.Time
}

type invokeV1Policy struct {
	intent         string
	idempotencyKey string
	timeout        time.Duration
}

type envelopeConsistency struct {
	mode          string
	snapshotID    string
	dataTimestamp time.Time
}

type snapshotState struct {
	id        string
	createdAt time.Time
	expiresAt time.Time

	runtime map[string]any
	zones   []Zone
	dhw     *DhwStatus
	energy  *EnergyTotals
	boiler  *BoilerStatus
	devices []deviceInfo
}

func consistencyInputProperty() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"mode":        map[string]any{"type": "string", "enum": []string{"LIVE", "SNAPSHOT"}},
			"snapshot_id": map[string]any{"type": "string"},
		},
		"additionalProperties": false,
	}
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
		idempotency:    make(map[string]idempotencyEntry),
		snapshots:      make(map[string]snapshotState),
	}
	server.tools = []Tool{
		{
			Name:        toolRuntimeStatusGetName,
			Description: "Get runtime daemon and adapter status.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"consistency": consistencyInputProperty(),
				},
				"additionalProperties": false,
			},
		},
		{
			Name:        toolSemanticZonesGetName,
			Description: "Get semantic zones snapshot.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"consistency": consistencyInputProperty(),
				},
				"additionalProperties": false,
			},
		},
		{
			Name:        toolSemanticDHWGetName,
			Description: "Get semantic domestic hot water snapshot.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"consistency": consistencyInputProperty(),
				},
				"additionalProperties": false,
			},
		},
		{
			Name:        toolSemanticEnergyGetName,
			Description: "Get semantic energy totals snapshot.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"consistency": consistencyInputProperty(),
				},
				"additionalProperties": false,
			},
		},
		{
			Name:        toolSemanticBoilerGetName,
			Description: "Get semantic boiler status snapshot (flow/return temps, pump, DHW temps, diagnostics).",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"consistency": consistencyInputProperty(),
				},
				"additionalProperties": false,
			},
		},
		{
			Name:        toolSemanticSnapshotName,
			Description: "Get a consistent semantic snapshot across selected semantic planes.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"planes": map[string]any{
						"type": "array",
						"items": map[string]any{
							"type": "string",
							"enum": []string{"runtime_status", "zones", "dhw", "energy_totals", "boiler_status"},
						},
					},
					"timeout_ms": map[string]any{"type": "integer", "minimum": 1},
					"allow_partial": map[string]any{
						"type": "boolean",
					},
					"consistency": consistencyInputProperty(),
				},
				"additionalProperties": false,
			},
		},
		{
			Name:        toolSnapshotCaptureName,
			Description: "Capture a read snapshot for deterministic MCP reads.",
			InputSchema: map[string]any{"type": "object", "additionalProperties": false},
		},
		{
			Name:        toolSnapshotDropName,
			Description: "Drop a previously captured snapshot.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"snapshot_id": map[string]any{"type": "string"},
				},
				"required":             []string{"snapshot_id"},
				"additionalProperties": false,
			},
		},
		{
			Name:        toolDevicesV1Name,
			Description: "List devices discovered on the eBUS, including planes and methods.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"consistency": consistencyInputProperty(),
				},
				"additionalProperties": false,
			},
		},
		{
			Name:        toolDeviceGetV1Name,
			Description: "Get one device by address.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"address":     map[string]any{"type": "integer", "minimum": 0, "maximum": 255},
					"consistency": consistencyInputProperty(),
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
					"address":     map[string]any{"type": "integer", "minimum": 0, "maximum": 255},
					"consistency": consistencyInputProperty(),
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
					"address":     map[string]any{"type": "integer", "minimum": 0, "maximum": 255},
					"plane":       map[string]any{"type": "string"},
					"consistency": consistencyInputProperty(),
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
					"timeout_ms":      map[string]any{"type": "integer", "minimum": 1},
				},
				"required":             []string{"address", "plane", "method", "intent", "allow_dangerous"},
				"additionalProperties": false,
			},
		},
		{
			Name:        toolDevicesLegacyName,
			Description: "Compatibility alias for ebus.v1.registry.devices.list.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"consistency": consistencyInputProperty(),
				},
				"additionalProperties": false,
			},
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
		consistency, snapshot, err := s.resolveConsistency(call.Arguments)
		if err != nil {
			return callToolResultText(mustJSON(newToolEnvelope(nil, err)), true), nil
		}
		status := s.runtimeStatus(snapshot)
		return callToolResultText(mustJSON(newToolEnvelopeWithConsistency(status, nil, consistency)), false), nil
	case toolSemanticZonesGetName:
		consistency, snapshot, err := s.resolveConsistency(call.Arguments)
		if err != nil {
			return callToolResultText(mustJSON(newToolEnvelope(nil, err)), true), nil
		}
		zones := s.snapshotZones(snapshot)
		return callToolResultText(mustJSON(newToolEnvelopeWithConsistency(zones, nil, consistency)), false), nil
	case toolSemanticDHWGetName:
		consistency, snapshot, err := s.resolveConsistency(call.Arguments)
		if err != nil {
			return callToolResultText(mustJSON(newToolEnvelope(nil, err)), true), nil
		}
		return callToolResultText(mustJSON(newToolEnvelopeWithConsistency(s.snapshotDHW(snapshot), nil, consistency)), false), nil
	case toolSemanticEnergyGetName:
		consistency, snapshot, err := s.resolveConsistency(call.Arguments)
		if err != nil {
			return callToolResultText(mustJSON(newToolEnvelope(nil, err)), true), nil
		}
		return callToolResultText(mustJSON(newToolEnvelopeWithConsistency(s.snapshotEnergyTotals(snapshot), nil, consistency)), false), nil
	case toolSemanticBoilerGetName:
		consistency, snapshot, err := s.resolveConsistency(call.Arguments)
		if err != nil {
			return callToolResultText(mustJSON(newToolEnvelope(nil, err)), true), nil
		}
		return callToolResultText(mustJSON(newToolEnvelopeWithConsistency(s.snapshotBoilerStatus(snapshot), nil, consistency)), false), nil
	case toolSemanticSnapshotName:
		consistency, snapshot, err := s.resolveConsistency(call.Arguments)
		if err != nil {
			return callToolResultText(mustJSON(newToolEnvelope(nil, err)), true), nil
		}
		data, err := s.readSemanticSnapshot(ctx, call.Arguments, snapshot)
		if err != nil {
			return callToolResultText(mustJSON(newToolEnvelopeWithConsistency(nil, err, consistency)), true), nil
		}
		return callToolResultText(mustJSON(newToolEnvelopeWithConsistency(data, nil, consistency)), false), nil
	case toolSnapshotCaptureName:
		snapshotID, createdAt, err := s.captureSnapshot()
		if err != nil {
			return callToolResultText(mustJSON(newToolEnvelope(nil, err)), true), nil
		}
		data := map[string]any{
			"snapshot_id": snapshotID,
			"created_at":  createdAt.UTC().Format(time.RFC3339Nano),
		}
		return callToolResultText(mustJSON(newToolEnvelope(data, nil)), false), nil
	case toolSnapshotDropName:
		if err := s.dropSnapshot(call.Arguments); err != nil {
			return callToolResultText(mustJSON(newToolEnvelope(nil, err)), true), nil
		}
		return callToolResultText(mustJSON(newToolEnvelope(map[string]any{"dropped": true}, nil)), false), nil
	case toolDevicesV1Name, toolDevicesLegacyName:
		consistency, snapshot, err := s.resolveConsistency(call.Arguments)
		if err != nil {
			return callToolResultText(mustJSON(newToolEnvelope(nil, err)), true), nil
		}
		devices := s.listDevices(snapshot)
		text := mustJSON(newToolEnvelopeWithConsistency(devices, nil, consistency))
		return callToolResultText(text, false), nil
	case toolDeviceGetV1Name:
		consistency, snapshot, err := s.resolveConsistency(call.Arguments)
		if err != nil {
			return callToolResultText(mustJSON(newToolEnvelope(nil, err)), true), nil
		}
		device, err := s.getDevice(call.Arguments, snapshot)
		if err != nil {
			return callToolResultText(mustJSON(newToolEnvelopeWithConsistency(nil, err, consistency)), true), nil
		}
		return callToolResultText(mustJSON(newToolEnvelopeWithConsistency(device, nil, consistency)), false), nil
	case toolPlanesListV1Name:
		consistency, snapshot, err := s.resolveConsistency(call.Arguments)
		if err != nil {
			return callToolResultText(mustJSON(newToolEnvelope(nil, err)), true), nil
		}
		planes, err := s.listPlanes(call.Arguments, snapshot)
		if err != nil {
			return callToolResultText(mustJSON(newToolEnvelopeWithConsistency(nil, err, consistency)), true), nil
		}
		return callToolResultText(mustJSON(newToolEnvelopeWithConsistency(planes, nil, consistency)), false), nil
	case toolMethodsListV1Name:
		consistency, snapshot, err := s.resolveConsistency(call.Arguments)
		if err != nil {
			return callToolResultText(mustJSON(newToolEnvelope(nil, err)), true), nil
		}
		methods, err := s.listMethods(call.Arguments, snapshot)
		if err != nil {
			return callToolResultText(mustJSON(newToolEnvelopeWithConsistency(nil, err, consistency)), true), nil
		}
		return callToolResultText(mustJSON(newToolEnvelopeWithConsistency(methods, nil, consistency)), false), nil
	case toolInvokeV1Name:
		if s.invoker == nil {
			return nil, rpcErrorInternal("server missing invoker")
		}
		policy, err := s.enforceInvokeV1Safety(call.Arguments)
		if err != nil {
			return callToolResultText(mustJSON(newToolEnvelope(nil, err)), true), nil
		}
		signature := ""
		if policy.intent == "MUTATE" {
			signature, err = buildInvokeIdempotencySignature(call.Arguments)
			if err != nil {
				return callToolResultText(mustJSON(newToolEnvelope(nil, err)), true), nil
			}
			if cached, ok, err := s.lookupIdempotency(policy.idempotencyKey, signature); err != nil {
				return callToolResultText(mustJSON(newToolEnvelope(nil, err)), true), nil
			} else if ok {
				return callToolResultText(mustJSON(newToolEnvelope(cached, nil)), false), nil
			}
		}

		invokeCtx := ctx
		cancel := func() {}
		if policy.timeout > 0 {
			invokeCtx, cancel = context.WithTimeout(ctx, policy.timeout)
		}
		defer cancel()

		out, err := s.invoke(invokeCtx, call.Arguments)
		if err != nil {
			return callToolResultText(mustJSON(newToolEnvelope(nil, err)), true), nil
		}
		if policy.intent == "MUTATE" {
			s.storeIdempotency(policy.idempotencyKey, signature, out)
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

func (s *Server) resolveConsistency(args map[string]any) (envelopeConsistency, *snapshotState, error) {
	consistency := envelopeConsistency{
		mode:          "LIVE",
		dataTimestamp: time.Now().UTC(),
	}
	if args == nil {
		return consistency, nil, nil
	}
	raw, ok := args["consistency"]
	if !ok || raw == nil {
		return consistency, nil, nil
	}
	value, ok := raw.(map[string]any)
	if !ok {
		return envelopeConsistency{}, nil, fmt.Errorf("invalid consistency payload: %w", ebuserrors.ErrInvalidPayload)
	}
	mode, _ := value["mode"].(string)
	mode = strings.ToUpper(strings.TrimSpace(mode))
	if mode == "" || mode == "LIVE" {
		return consistency, nil, nil
	}
	if mode != "SNAPSHOT" {
		return envelopeConsistency{}, nil, fmt.Errorf("invalid consistency mode %q: %w", mode, ebuserrors.ErrInvalidPayload)
	}
	snapshotID, _ := value["snapshot_id"].(string)
	snapshotID = strings.TrimSpace(snapshotID)
	if snapshotID == "" {
		return envelopeConsistency{}, nil, fmt.Errorf("missing consistency.snapshot_id: %w", ebuserrors.ErrInvalidPayload)
	}
	snapshot, ok := s.getSnapshot(snapshotID)
	if !ok {
		return envelopeConsistency{}, nil, fmt.Errorf("unknown snapshot %q: %w", snapshotID, errSnapshotNotFound)
	}
	consistency.mode = "SNAPSHOT"
	consistency.snapshotID = snapshotID
	consistency.dataTimestamp = snapshot.createdAt
	return consistency, &snapshot, nil
}

func (s *Server) captureSnapshot() (snapshotID string, createdAt time.Time, err error) {
	id, err := newSessionID()
	if err != nil {
		return "", time.Time{}, fmt.Errorf("snapshot id generation failed: %w", err)
	}
	now := time.Now().UTC()
	snapshot := snapshotState{
		id:        id,
		createdAt: now,
		expiresAt: now.Add(defaultSnapshotTTL),
		runtime:   s.runtimeStatus(nil),
		zones:     s.snapshotZones(nil),
		dhw:       s.snapshotDHW(nil),
		energy:    s.snapshotEnergyTotals(nil),
		boiler:    s.snapshotBoilerStatus(nil),
		devices:   s.listDevices(nil),
	}

	s.snapshotMu.Lock()
	s.cleanupExpiredSnapshotsLocked(now)
	s.snapshots[id] = cloneSnapshotState(snapshot)
	s.snapshotMu.Unlock()

	return id, now, nil
}

func (s *Server) dropSnapshot(args map[string]any) error {
	snapshotID, err := parseSnapshotID(args)
	if err != nil {
		return err
	}
	s.snapshotMu.Lock()
	defer s.snapshotMu.Unlock()
	if _, ok := s.snapshots[snapshotID]; !ok {
		return fmt.Errorf("unknown snapshot %q: %w", snapshotID, errSnapshotNotFound)
	}
	delete(s.snapshots, snapshotID)
	return nil
}

func (s *Server) getSnapshot(snapshotID string) (snapshotState, bool) {
	now := time.Now().UTC()
	s.snapshotMu.Lock()
	defer s.snapshotMu.Unlock()
	s.cleanupExpiredSnapshotsLocked(now)
	snapshot, ok := s.snapshots[snapshotID]
	if !ok {
		return snapshotState{}, false
	}
	return cloneSnapshotState(snapshot), true
}

func (s *Server) cleanupExpiredSnapshotsLocked(now time.Time) {
	for id, snapshot := range s.snapshots {
		if now.After(snapshot.expiresAt) {
			delete(s.snapshots, id)
		}
	}
}

func parseSnapshotID(args map[string]any) (string, error) {
	if args == nil {
		return "", fmt.Errorf("missing snapshot_id: %w", ebuserrors.ErrInvalidPayload)
	}
	snapshotID, _ := args["snapshot_id"].(string)
	snapshotID = strings.TrimSpace(snapshotID)
	if snapshotID == "" {
		return "", fmt.Errorf("missing snapshot_id: %w", ebuserrors.ErrInvalidPayload)
	}
	return snapshotID, nil
}

func cloneSnapshotState(snapshot snapshotState) snapshotState {
	var dhwCopy *DhwStatus
	if snapshot.dhw != nil {
		value := *snapshot.dhw
		dhwCopy = &value
	}
	var energyCopy *EnergyTotals
	if snapshot.energy != nil {
		value := *snapshot.energy
		value.Gas = cloneEnergyChannel(value.Gas)
		value.Electric = cloneEnergyChannel(value.Electric)
		value.Solar = cloneEnergyChannel(value.Solar)
		energyCopy = &value
	}
	var boilerCopy *BoilerStatus
	if snapshot.boiler != nil {
		boilerCopy = cloneMCPBoilerStatus(snapshot.boiler)
	}
	return snapshotState{
		id:        snapshot.id,
		createdAt: snapshot.createdAt,
		expiresAt: snapshot.expiresAt,
		runtime:   cloneMap(snapshot.runtime),
		zones:     cloneZones(snapshot.zones),
		dhw:       dhwCopy,
		energy:    energyCopy,
		boiler:    boilerCopy,
		devices:   cloneDeviceInfoList(snapshot.devices),
	}
}

func newToolEnvelope(data any, err error) map[string]any {
	return newToolEnvelopeWithConsistency(data, err, envelopeConsistency{
		mode:          "LIVE",
		dataTimestamp: time.Now().UTC(),
	})
}

func newToolEnvelopeWithConsistency(data any, err error, consistency envelopeConsistency) map[string]any {
	mode := strings.TrimSpace(consistency.mode)
	if mode == "" {
		mode = "LIVE"
	}
	timestamp := consistency.dataTimestamp
	if timestamp.IsZero() {
		timestamp = time.Now().UTC()
	}
	consistencyMeta := map[string]any{
		"mode": mode,
	}
	if strings.TrimSpace(consistency.snapshotID) != "" {
		consistencyMeta["snapshot_id"] = consistency.snapshotID
	}

	meta := map[string]any{
		"contract": map[string]any{
			"name":  "helianthus-ebus-mcp",
			"major": 1,
			"minor": 0,
		},
		"consistency":    consistencyMeta,
		"data_timestamp": timestamp.UTC().Format(time.RFC3339Nano),
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
	case errors.Is(err, context.DeadlineExceeded):
		return "TIMEOUT", true, "gateway"
	case errors.Is(err, errInvokePermissionDenied):
		return "PERMISSION_DENIED", false, "gateway"
	case errors.Is(err, errInvokeIdempotencyConflict):
		return "CONFLICT", false, "gateway"
	case errors.Is(err, errSnapshotNotFound):
		return "NOT_FOUND", false, "gateway"
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

func (s *Server) enforceInvokeV1Safety(args map[string]any) (invokeV1Policy, error) {
	policy := invokeV1Policy{
		timeout: defaultInvokeTimeout,
	}

	address, err := parseAddress(args["address"])
	if err != nil {
		return invokeV1Policy{}, err
	}
	planeName, _ := args["plane"].(string)
	if planeName == "" {
		return invokeV1Policy{}, fmt.Errorf("missing plane: %w", ebuserrors.ErrInvalidPayload)
	}
	methodName, _ := args["method"].(string)
	if methodName == "" {
		return invokeV1Policy{}, fmt.Errorf("missing method: %w", ebuserrors.ErrInvalidPayload)
	}
	intent, _ := args["intent"].(string)
	intent = strings.TrimSpace(intent)
	if intent == "" {
		return invokeV1Policy{}, fmt.Errorf("missing intent: %w", ebuserrors.ErrInvalidPayload)
	}
	policy.intent = intent
	allowDangerous, ok := args["allow_dangerous"].(bool)
	if !ok {
		return invokeV1Policy{}, fmt.Errorf("missing allow_dangerous: %w", ebuserrors.ErrInvalidPayload)
	}
	if timeout, err := parseInvokeTimeout(args["timeout_ms"]); err != nil {
		return invokeV1Policy{}, err
	} else if timeout > 0 {
		policy.timeout = timeout
	}

	methodKnown, methodReadOnly, err := s.lookupMethodMutability(address, planeName, methodName)
	if err != nil {
		return invokeV1Policy{}, err
	}

	switch intent {
	case "READ_ONLY":
		if !methodKnown || !methodReadOnly {
			return invokeV1Policy{}, fmt.Errorf("READ_ONLY intent denied for mutating/unknown method: %w", errInvokePermissionDenied)
		}
	case "MUTATE":
		if !allowDangerous {
			return invokeV1Policy{}, fmt.Errorf("MUTATE intent requires allow_dangerous=true: %w", errInvokePermissionDenied)
		}
		idempotencyKey, _ := args["idempotency_key"].(string)
		if strings.TrimSpace(idempotencyKey) == "" {
			return invokeV1Policy{}, fmt.Errorf("MUTATE intent requires idempotency_key: %w", errInvokePermissionDenied)
		}
		policy.idempotencyKey = strings.TrimSpace(idempotencyKey)
	default:
		return invokeV1Policy{}, fmt.Errorf("invalid intent %q: %w", intent, ebuserrors.ErrInvalidPayload)
	}

	return policy, nil
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

func parseInvokeTimeout(raw any) (time.Duration, error) {
	if raw == nil {
		return defaultInvokeTimeout, nil
	}

	switch value := raw.(type) {
	case int:
		if value <= 0 {
			return 0, fmt.Errorf("invalid timeout_ms: %w", ebuserrors.ErrInvalidPayload)
		}
		return time.Duration(value) * time.Millisecond, nil
	case int64:
		if value <= 0 {
			return 0, fmt.Errorf("invalid timeout_ms: %w", ebuserrors.ErrInvalidPayload)
		}
		return time.Duration(value) * time.Millisecond, nil
	case float64:
		if value <= 0 || value != float64(int(value)) {
			return 0, fmt.Errorf("invalid timeout_ms: %w", ebuserrors.ErrInvalidPayload)
		}
		return time.Duration(int(value)) * time.Millisecond, nil
	default:
		return 0, fmt.Errorf("invalid timeout_ms: %w", ebuserrors.ErrInvalidPayload)
	}
}

func buildInvokeIdempotencySignature(args map[string]any) (string, error) {
	payload := map[string]any{
		"address": args["address"],
		"plane":   args["plane"],
		"method":  args["method"],
		"params":  args["params"],
		"intent":  args["intent"],
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("failed to canonicalize idempotency payload: %w", ebuserrors.ErrInvalidPayload)
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

func (s *Server) lookupIdempotency(key string, signature string) (any, bool, error) {
	now := time.Now()
	s.idempotencyMu.Lock()
	defer s.idempotencyMu.Unlock()

	for candidate, entry := range s.idempotency {
		if now.After(entry.expiresAt) {
			delete(s.idempotency, candidate)
		}
	}

	entry, ok := s.idempotency[key]
	if !ok {
		return nil, false, nil
	}
	if entry.signature != signature {
		return nil, false, fmt.Errorf("idempotency key reused with different payload: %w", errInvokeIdempotencyConflict)
	}
	return cloneJSONValue(entry.result), true, nil
}

func (s *Server) storeIdempotency(key string, signature string, result any) {
	if strings.TrimSpace(key) == "" {
		return
	}
	s.idempotencyMu.Lock()
	s.idempotency[key] = idempotencyEntry{
		signature: signature,
		result:    cloneJSONValue(result),
		expiresAt: time.Now().Add(defaultIdempotencyTTL),
	}
	s.idempotencyMu.Unlock()
}

func cloneJSONValue(value any) any {
	raw, err := json.Marshal(value)
	if err != nil {
		return value
	}
	var out any
	if err := json.Unmarshal(raw, &out); err != nil {
		return value
	}
	return out
}

func cloneMap(source map[string]any) map[string]any {
	if source == nil {
		return nil
	}
	out := make(map[string]any, len(source))
	for key, value := range source {
		out[key] = cloneJSONValue(value)
	}
	return out
}

func cloneZones(source []Zone) []Zone {
	if len(source) == 0 {
		return nil
	}
	out := make([]Zone, len(source))
	copy(out, source)
	return out
}

func cloneMethodInfoList(source []methodInfo) []methodInfo {
	if len(source) == 0 {
		return nil
	}
	out := make([]methodInfo, len(source))
	copy(out, source)
	return out
}

func clonePlaneInfoList(source []planeInfo) []planeInfo {
	if len(source) == 0 {
		return nil
	}
	out := make([]planeInfo, len(source))
	for i, plane := range source {
		out[i] = planeInfo{
			Name:     plane.Name,
			Routable: plane.Routable,
			Methods:  cloneMethodInfoList(plane.Methods),
		}
	}
	return out
}

func cloneDeviceInfoList(source []deviceInfo) []deviceInfo {
	if len(source) == 0 {
		return nil
	}
	out := make([]deviceInfo, len(source))
	for i, device := range source {
		out[i] = deviceInfo{
			Address:         device.Address,
			Manufacturer:    device.Manufacturer,
			DeviceID:        device.DeviceID,
			SoftwareVersion: device.SoftwareVersion,
			HardwareVersion: device.HardwareVersion,
			Planes:          clonePlaneInfoList(device.Planes),
		}
	}
	return out
}

func findDeviceInfoByAddress(devices []deviceInfo, address byte) (deviceInfo, bool) {
	for _, device := range devices {
		if device.Address == int(address) {
			return device, true
		}
	}
	return deviceInfo{}, false
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

func (s *Server) listDevices(snapshot *snapshotState) []deviceInfo {
	if snapshot != nil {
		return cloneDeviceInfoList(snapshot.devices)
	}
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

func (s *Server) getDevice(args map[string]any, snapshot *snapshotState) (deviceInfo, error) {
	address, err := parseAddress(args["address"])
	if err != nil {
		return deviceInfo{}, err
	}
	if snapshot != nil {
		device, ok := findDeviceInfoByAddress(snapshot.devices, address)
		if !ok {
			return deviceInfo{}, fmt.Errorf("unknown device 0x%02x: %w", address, ebuserrors.ErrNoSuchDevice)
		}
		return device, nil
	}
	entry, ok := s.registry.Lookup(address)
	if !ok {
		return deviceInfo{}, fmt.Errorf("unknown device 0x%02x: %w", address, ebuserrors.ErrNoSuchDevice)
	}
	return buildDeviceInfo(entry), nil
}

func (s *Server) listPlanes(args map[string]any, snapshot *snapshotState) ([]planeInfo, error) {
	address, err := parseAddress(args["address"])
	if err != nil {
		return nil, err
	}
	if snapshot != nil {
		device, ok := findDeviceInfoByAddress(snapshot.devices, address)
		if !ok {
			return nil, fmt.Errorf("unknown device 0x%02x: %w", address, ebuserrors.ErrNoSuchDevice)
		}
		return clonePlaneInfoList(device.Planes), nil
	}
	entry, ok := s.registry.Lookup(address)
	if !ok {
		return nil, fmt.Errorf("unknown device 0x%02x: %w", address, ebuserrors.ErrNoSuchDevice)
	}
	return buildPlaneInfoList(entry.Planes()), nil
}

func (s *Server) listMethods(args map[string]any, snapshot *snapshotState) ([]methodInfo, error) {
	address, err := parseAddress(args["address"])
	if err != nil {
		return nil, err
	}
	planeName, _ := args["plane"].(string)
	planeName = strings.TrimSpace(planeName)
	if planeName == "" {
		return nil, fmt.Errorf("missing plane: %w", ebuserrors.ErrInvalidPayload)
	}

	if snapshot != nil {
		device, ok := findDeviceInfoByAddress(snapshot.devices, address)
		if !ok {
			return nil, fmt.Errorf("unknown device 0x%02x: %w", address, ebuserrors.ErrNoSuchDevice)
		}
		for _, plane := range device.Planes {
			if plane.Name == planeName {
				return cloneMethodInfoList(plane.Methods), nil
			}
		}
		return nil, fmt.Errorf("unknown plane %q: %w", planeName, ebuserrors.ErrInvalidPayload)
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

func (s *Server) runtimeStatus(snapshot *snapshotState) map[string]any {
	if snapshot != nil {
		return cloneMap(snapshot.runtime)
	}
	return map[string]any{
		"daemon_status":  s.statusProvider.DaemonStatus(),
		"adapter_status": s.statusProvider.AdapterStatus(),
	}
}

func (s *Server) snapshotZones(snapshot *snapshotState) []Zone {
	if snapshot != nil {
		return cloneZones(snapshot.zones)
	}
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

func (s *Server) snapshotDHW(snapshot *snapshotState) *DhwStatus {
	if snapshot != nil {
		if snapshot.dhw == nil {
			return nil
		}
		copy := *snapshot.dhw
		return &copy
	}
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

func (s *Server) snapshotEnergyTotals(snapshot *snapshotState) *EnergyTotals {
	if snapshot != nil {
		if snapshot.energy == nil {
			return nil
		}
		copy := *snapshot.energy
		copy.Gas = cloneEnergyChannel(copy.Gas)
		copy.Electric = cloneEnergyChannel(copy.Electric)
		copy.Solar = cloneEnergyChannel(copy.Solar)
		return &copy
	}
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

func (s *Server) snapshotBoilerStatus(snapshot *snapshotState) *BoilerStatus {
	if snapshot != nil {
		if snapshot.boiler == nil {
			return nil
		}
		return cloneMCPBoilerStatus(snapshot.boiler)
	}
	if s == nil || s.semantic == nil {
		return nil
	}
	source := s.semantic.BoilerStatus()
	if source == nil {
		return nil
	}
	return cloneMCPBoilerStatus(source)
}

func cloneMCPBoilerStatus(status *BoilerStatus) *BoilerStatus {
	if status == nil {
		return nil
	}
	cp := *status
	if cp.State != nil {
		s := *cp.State
		cp.State = &s
	}
	if cp.Config != nil {
		c := *cp.Config
		cp.Config = &c
	}
	if cp.Diagnostics != nil {
		d := *cp.Diagnostics
		cp.Diagnostics = &d
	}
	return &cp
}

type semanticSnapshotOptions struct {
	planes       []string
	timeout      time.Duration
	allowPartial bool
}

func (s *Server) readSemanticSnapshot(ctx context.Context, args map[string]any, snapshot *snapshotState) (map[string]any, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	options, err := parseSemanticSnapshotOptions(args)
	if err != nil {
		return nil, err
	}

	deadlineCtx, cancel := context.WithTimeout(ctx, options.timeout)
	defer cancel()

	data := make(map[string]any, len(options.planes))
	completed := make([]string, 0, len(options.planes))
	errorPlanes := make([]map[string]any, 0)

	perPlane := options.timeout / time.Duration(len(options.planes))
	if perPlane <= 0 {
		perPlane = options.timeout
	}

	for _, plane := range options.planes {
		select {
		case <-deadlineCtx.Done():
			err := deadlineCtx.Err()
			if options.allowPartial {
				errorPlanes = append(errorPlanes, newSnapshotPlaneError(plane, err))
				continue
			}
			return nil, err
		default:
		}

		planeCtx, planeCancel := context.WithTimeout(deadlineCtx, perPlane)
		value, planeErr := s.readSemanticPlane(planeCtx, plane, snapshot)
		planeCancel()
		if planeErr != nil {
			if options.allowPartial {
				errorPlanes = append(errorPlanes, newSnapshotPlaneError(plane, planeErr))
				continue
			}
			return nil, planeErr
		}
		data[plane] = value
		completed = append(completed, plane)
	}

	result := map[string]any{
		"planes":           data,
		"completed_planes": completed,
	}
	if options.allowPartial && len(errorPlanes) > 0 {
		result["error_planes"] = errorPlanes
	}
	return result, nil
}

func parseSemanticSnapshotOptions(args map[string]any) (semanticSnapshotOptions, error) {
	options := semanticSnapshotOptions{
		planes:       []string{"runtime_status", "zones", "dhw", "energy_totals", "boiler_status"},
		timeout:      defaultSnapshotReadTTL,
		allowPartial: false,
	}
	if args == nil {
		return options, nil
	}

	parsedPlanes, err := parseSemanticSnapshotPlanes(args["planes"])
	if err != nil {
		return semanticSnapshotOptions{}, err
	}
	if len(parsedPlanes) > 0 {
		options.planes = parsedPlanes
	}

	if rawTimeout, ok := args["timeout_ms"]; ok {
		timeout, err := parseInvokeTimeout(rawTimeout)
		if err != nil {
			return semanticSnapshotOptions{}, err
		}
		if timeout > 0 {
			options.timeout = timeout
		}
	}

	if rawPartial, ok := args["allow_partial"]; ok {
		value, ok := rawPartial.(bool)
		if !ok {
			return semanticSnapshotOptions{}, fmt.Errorf("invalid allow_partial: %w", ebuserrors.ErrInvalidPayload)
		}
		options.allowPartial = value
	}

	return options, nil
}

func parseSemanticSnapshotPlanes(raw any) ([]string, error) {
	if raw == nil {
		return nil, nil
	}

	items, ok := raw.([]any)
	if !ok {
		if typed, ok := raw.([]string); ok {
			if len(typed) == 0 {
				return nil, nil
			}
			items = make([]any, 0, len(typed))
			for _, value := range typed {
				items = append(items, value)
			}
		} else {
			return nil, fmt.Errorf("invalid planes: %w", ebuserrors.ErrInvalidPayload)
		}
	}

	parsed := make([]string, 0, len(items))
	seen := make(map[string]struct{}, len(items))
	for _, item := range items {
		value, ok := item.(string)
		if !ok {
			return nil, fmt.Errorf("invalid planes item: %w", ebuserrors.ErrInvalidPayload)
		}
		normalized := strings.ToLower(strings.TrimSpace(value))
		switch normalized {
		case "runtime_status", "zones", "dhw", "energy_totals", "boiler_status":
		default:
			return nil, fmt.Errorf("unsupported plane %q: %w", value, ebuserrors.ErrInvalidPayload)
		}
		if _, exists := seen[normalized]; exists {
			continue
		}
		seen[normalized] = struct{}{}
		parsed = append(parsed, normalized)
	}
	return parsed, nil
}

func (s *Server) readSemanticPlane(ctx context.Context, plane string, snapshot *snapshotState) (any, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	var value any
	switch plane {
	case "runtime_status":
		value = s.runtimeStatus(snapshot)
	case "zones":
		value = s.snapshotZones(snapshot)
	case "dhw":
		value = s.snapshotDHW(snapshot)
	case "energy_totals":
		value = s.snapshotEnergyTotals(snapshot)
	case "boiler_status":
		value = s.snapshotBoilerStatus(snapshot)
	default:
		return nil, fmt.Errorf("unsupported plane %q: %w", plane, ebuserrors.ErrInvalidPayload)
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	return value, nil
}

func newSnapshotPlaneError(plane string, err error) map[string]any {
	code, retriable, sourceLayer := classifyToolError(err)
	return map[string]any{
		"plane":        plane,
		"code":         code,
		"message":      err.Error(),
		"retriable":    retriable,
		"source_layer": sourceLayer,
	}
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
