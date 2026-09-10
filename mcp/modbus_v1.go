package mcp

import (
	"context"
	"errors"
	"math"
	"strings"
	"sync"
	"time"
)

const (
	ModbusV1RawReadTool               = "modbus.v1.raw.read"
	ModbusV1ProfileObservationGetTool = "modbus.v1.profile.observation.get"
	SemanticV1PVCurrentGetTool        = "semantic.v1.pv.current.get"
	modbusV1MaxReadWords              = 125
	modbusV1MaxIdentityBytes          = 128
	ModbusV1MaxRawReadsPerWindow      = 4
	ModbusV1RawReadWindow             = time.Second
)

var ErrModbusV1ResourceExhausted = errors.New("modbus V1 raw read quota exhausted")

// ModbusRawReadRequest is the closed read-only phase-one operation surface.
type ModbusRawReadRequest struct {
	UnitID   byte   `json:"unit_id"`
	Function byte   `json:"function"`
	Offset   uint16 `json:"offset"`
	Quantity uint16 `json:"quantity"`
}

// ModbusRawReadResult retains sanitized wire and logical-view provenance.
type ModbusRawReadResult struct {
	EndpointRef         string   `json:"endpoint_ref"`
	UnitID              byte     `json:"unit_id"`
	Function            byte     `json:"function"`
	Offset              uint16   `json:"offset"`
	Quantity            uint16   `json:"quantity"`
	Words               []uint16 `json:"words"`
	WireBytesHex        string   `json:"wire_bytes_hex,omitempty"`
	WireResponseID      uint64   `json:"wire_response_id"`
	LogicalViewID       uint64   `json:"logical_view_id"`
	PhysicalRequestID   uint64   `json:"physical_request_id"`
	ConnectionID        uint64   `json:"connection_id"`
	TransportGeneration uint64   `json:"transport_generation"`
	PollGenerationID    uint64   `json:"poll_generation_id"`
	DeadlineIdentity    uint64   `json:"deadline_identity"`
}

type ModbusReplayView struct {
	LogicalViewID  uint64   `json:"logical_view_id"`
	WireResponseID uint64   `json:"wire_response_id"`
	Offset         uint16   `json:"offset"`
	Words          []uint16 `json:"words"`
}

type ModbusProfileObservationResult struct {
	ProfileID          string             `json:"profile_id"`
	ProfileVersion     string             `json:"profile_version,omitempty"`
	CodecVersion       string             `json:"codec_version,omitempty"`
	SampleID           string             `json:"sample_id"`
	PollGenerationID   uint64             `json:"poll_generation_id,omitempty"`
	SourceValidity     string             `json:"source_validity"`
	SourceTime         string             `json:"source_time,omitempty"`
	LocalReceiptTime   string             `json:"local_receipt_time,omitempty"`
	DetectionEvidence  []string           `json:"detection_evidence"`
	ActivationEvidence []string           `json:"activation_evidence"`
	ObservationJSONB64 string             `json:"observation_json_base64"`
	Replay             []ModbusReplayView `json:"replay"`
}

type ModbusV1Provider interface {
	RawRead(context.Context, ModbusRawReadRequest) (ModbusRawReadResult, error)
	ProfileObservation(context.Context, string, string) (ModbusProfileObservationResult, error)
	SemanticPVCurrent(context.Context, string, string) (SemanticPVCurrentResult, error)
}

// modbusV1CoreAvailability permits a production composition to add a native
// optional provider without silently advertising the unrelated TCP core.
type modbusV1CoreAvailability interface {
	ModbusV1CoreAvailable() bool
}

type SemanticPVCurrentResult struct {
	Data      any
	Evaluated string
}

var modbusV1Providers = struct {
	sync.RWMutex
	byServer map[*Server]ModbusV1Provider
}{byServer: make(map[*Server]ModbusV1Provider)}

func RegisterModbusV1Tools(server *Server, provider ModbusV1Provider) {
	if server == nil || provider == nil {
		return
	}
	coreAvailable := true
	if availability, ok := provider.(modbusV1CoreAvailability); ok {
		coreAvailable = availability.ModbusV1CoreAvailable()
	}
	if coreAvailable {
		modbusV1Providers.Lock()
		modbusV1Providers.byServer[server] = provider
		modbusV1Providers.Unlock()
		server.tools = append(server.tools,
			Tool{
				Name:        ModbusV1RawReadTool,
				Description: "Read one bounded FC03 or FC04 Modbus range with exact sanitized provenance.",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"unit_id":  map[string]any{"type": "integer", "minimum": 1, "maximum": 247},
						"function": map[string]any{"type": "integer", "enum": []int{3, 4}},
						"offset":   map[string]any{"type": "integer", "minimum": 0, "maximum": 65535},
						"quantity": map[string]any{"type": "integer", "minimum": 1, "maximum": modbusV1MaxReadWords},
					},
					"required":             []string{"unit_id", "function", "offset", "quantity"},
					"additionalProperties": false,
				},
			},
			Tool{
				Name:        ModbusV1ProfileObservationGetTool,
				Description: "Get one retained profile observation with detector, activation, and exact replay evidence.",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"profile_id": map[string]any{"type": "string", "minLength": 1, "maxLength": modbusV1MaxIdentityBytes},
						"sample_id":  map[string]any{"type": "string", "minLength": 1, "maxLength": modbusV1MaxIdentityBytes},
					},
					"required":             []string{"profile_id", "sample_id"},
					"additionalProperties": false,
				},
			},
			Tool{
				Name:        SemanticV1PVCurrentGetTool,
				Description: "Get one immutable evaluated SemReg PV projection from an admitted SunSpec observation.",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"profile_id": map[string]any{"type": "string", "minLength": 1, "maxLength": modbusV1MaxIdentityBytes},
						"sample_id":  map[string]any{"type": "string", "minLength": 1, "maxLength": modbusV1MaxIdentityBytes},
					},
					"required":             []string{"profile_id", "sample_id"},
					"additionalProperties": false,
				},
			},
		)
	}
	if coreAvailable {
		registerTeslaFC100SummaryV1Tool(server, provider)
		registerTeslaHSCV1Tool(server, provider)
		registerTeslaWCVitalsV1Tool(server, provider)
		registerTeslaGen3EVSECurrentLimitV1Tool(server, provider)
		registerGrowattProtocolIIV1Tool(server, provider)
		registerOutBackAXSV1Tool(server, provider)
		registerHuaweiEMMAV1Tool(server, provider)
		registerHuaweiSmartLoggerV1Tool(server, provider)
		registerHuaweiSDongleV1Tool(server, provider)
		registerFroniusSunSpecV1Tool(server, provider)
	}
	registerGrowattBMSRS485V202Tool(server, provider)
	registerGrowattStorageSemanticTool(server, provider)
	registerTeslaGen3EVSESemanticTool(server, provider)
}

func (server *Server) handleModbusV1Call(ctx context.Context, name string, args map[string]any) (map[string]any, bool) {
	if result, handled := server.handleTeslaFC100SummaryV1Call(ctx, name, args); handled {
		return result, true
	}
	if result, handled := server.handleTeslaHSCV1Call(ctx, name, args); handled {
		return result, true
	}
	if result, handled := server.handleTeslaWCVitalsV1Call(ctx, name, args); handled {
		return result, true
	}
	if result, handled := server.handleTeslaGen3EVSECurrentLimitV1Call(ctx, name, args); handled {
		return result, true
	}
	if result, handled := server.handleGrowattProtocolIIV1Call(ctx, name, args); handled {
		return result, true
	}
	if result, handled := server.handleOutBackAXSV1Call(ctx, name, args); handled {
		return result, true
	}
	if result, handled := server.handleHuaweiEMMAV1Call(ctx, name, args); handled {
		return result, true
	}
	if result, handled := server.handleHuaweiSmartLoggerV1Call(ctx, name, args); handled {
		return result, true
	}
	if result, handled := server.handleHuaweiSDongleV1Call(ctx, name, args); handled {
		return result, true
	}
	if result, handled := server.handleFroniusSunSpecV1Call(ctx, name, args); handled {
		return result, true
	}
	if result, handled := server.handleGrowattBMSRS485V202Call(ctx, name, args); handled {
		return result, true
	}
	if result, handled := server.handleGrowattStorageSemanticCall(ctx, name, args); handled {
		return result, true
	}
	if result, handled := server.handleTeslaGen3EVSESemanticCall(ctx, name, args); handled {
		return result, true
	}
	modbusV1Providers.RLock()
	provider := modbusV1Providers.byServer[server]
	modbusV1Providers.RUnlock()
	if provider == nil {
		return nil, false
	}
	var data any
	var err error
	providerCalled := false
	var consistencyMode string
	dataTimestamp := time.Now().UTC().Format(time.RFC3339Nano)
	switch name {
	case ModbusV1RawReadTool:
		envelope := modbusV1RawReadCall(ctx, provider, args)
		_, failed := envelope["error"].(map[string]any)
		return callToolResultText(mustJSON(envelope), failed), true
	case ModbusV1ProfileObservationGetTool:
		consistencyMode = "RETAINED_SOURCE_OBSERVATION"
		var profileID, sampleID string
		profileID, sampleID, err = parseModbusProfileRequest(args)
		if err == nil {
			providerCalled = true
			data, err = provider.ProfileObservation(ctx, profileID, sampleID)
			if result, ok := data.(ModbusProfileObservationResult); ok && result.LocalReceiptTime != "" {
				dataTimestamp = result.LocalReceiptTime
			}
		}
	case SemanticV1PVCurrentGetTool:
		consistencyMode = "EVALUATED_SEMREG_PUBLICATION"
		var profileID, sampleID string
		profileID, sampleID, err = parseModbusProfileRequest(args)
		if err == nil {
			providerCalled = true
			result, callErr := provider.SemanticPVCurrent(ctx, profileID, sampleID)
			err = callErr
			if err == nil {
				dataTimestamp = result.Evaluated
				data = result.Data
			}
		}
	default:
		return nil, false
	}
	if err != nil {
		data = nil
	}
	return callToolResultText(mustJSON(newModbusV1Envelope(
		data, err, providerCalled, consistencyMode, dataTimestamp,
	)), err != nil), true
}

// InvokeModbusV1RawRead is the single closed raw-read invocation core used by
// both the MCP tool and the Portal BFF. It owns parsing, provider invocation,
// quota propagation, and the stable MCP envelope.
func InvokeModbusV1RawRead(ctx context.Context, provider ModbusV1Provider, args map[string]any) map[string]any {
	return modbusV1RawReadCall(ctx, provider, args)
}

func modbusV1RawReadCall(ctx context.Context, provider ModbusV1Provider, args map[string]any) map[string]any {
	dataTimestamp := time.Now().UTC().Format(time.RFC3339Nano)
	request, err := parseModbusRawReadRequest(args)
	providerCalled := false
	var data any
	if err == nil && provider != nil {
		providerCalled = true
		data, err = provider.RawRead(ctx, request)
	}
	if provider == nil {
		err = errors.New("modbus provider unavailable")
	}
	if err != nil {
		data = nil
	}
	return newModbusV1Envelope(data, err, providerCalled, "LIVE", dataTimestamp)
}

func parseModbusRawReadRequest(args map[string]any) (ModbusRawReadRequest, error) {
	if !closedArguments(args, "unit_id", "function", "offset", "quantity") {
		return ModbusRawReadRequest{}, errors.New("invalid Modbus raw read arguments")
	}
	unitID, ok := boundedInteger(args["unit_id"], 1, 247)
	if !ok {
		return ModbusRawReadRequest{}, errors.New("unit_id must be an integer from 1 through 247")
	}
	function, ok := boundedInteger(args["function"], 3, 4)
	if !ok || (function != 3 && function != 4) {
		return ModbusRawReadRequest{}, errors.New("function must be read-only FC03 or FC04")
	}
	offset, ok := boundedInteger(args["offset"], 0, math.MaxUint16)
	if !ok {
		return ModbusRawReadRequest{}, errors.New("offset must be an unsigned 16-bit integer")
	}
	quantity, ok := boundedInteger(args["quantity"], 1, modbusV1MaxReadWords)
	if !ok || offset+quantity > math.MaxUint16+1 {
		return ModbusRawReadRequest{}, errors.New("quantity must be 1 through 125 and remain inside the address space")
	}
	return ModbusRawReadRequest{UnitID: byte(unitID), Function: byte(function), Offset: uint16(offset), Quantity: uint16(quantity)}, nil
}

func parseModbusProfileRequest(args map[string]any) (string, string, error) {
	if !closedArguments(args, "profile_id", "sample_id") {
		return "", "", errors.New("invalid profile observation arguments")
	}
	profileID, profileOK := boundedIdentity(args["profile_id"])
	sampleID, sampleOK := boundedIdentity(args["sample_id"])
	if !profileOK || !sampleOK {
		return "", "", errors.New("profile_id and sample_id must be non-empty bounded strings")
	}
	return profileID, sampleID, nil
}

func closedArguments(args map[string]any, names ...string) bool {
	if len(args) != len(names) {
		return false
	}
	allowed := make(map[string]struct{}, len(names))
	for _, name := range names {
		allowed[name] = struct{}{}
	}
	for name := range args {
		if _, ok := allowed[name]; !ok {
			return false
		}
	}
	return true
}

func boundedInteger(value any, minValue, maxValue int) (int, bool) {
	number, ok := value.(float64)
	if !ok || math.Trunc(number) != number || number < float64(minValue) || number > float64(maxValue) {
		return 0, false
	}
	return int(number), true
}

func boundedIdentity(value any) (string, bool) {
	text, ok := value.(string)
	text = strings.TrimSpace(text)
	return text, ok && text != "" && len(text) <= modbusV1MaxIdentityBytes
}

func newModbusV1Envelope(
	data any,
	err error,
	providerCalled bool,
	consistencyMode string,
	dataTimestamp string,
) map[string]any {
	var envelopeError any
	if err != nil {
		code := "INVALID_ARGUMENT"
		message := err.Error()
		retriable := false
		if providerCalled {
			code = "UNAVAILABLE"
			message = "modbus provider unavailable"
			retriable = true
		}
		if errors.Is(err, ErrModbusV1ResourceExhausted) {
			code = "RESOURCE_EXHAUSTED"
			message = "modbus raw read quota exhausted"
		}
		envelopeError = map[string]any{
			"code":         code,
			"message":      message,
			"retriable":    retriable,
			"source_layer": "modbus",
		}
	}
	return map[string]any{
		"meta": map[string]any{
			"contract":       map[string]any{"name": "helianthus-modbus-mcp", "major": 1, "minor": 0},
			"consistency":    map[string]any{"mode": consistencyMode},
			"data_timestamp": dataTimestamp,
			"data_hash":      hashData(data),
			"limits": map[string]any{
				"raw_read_max_words":           modbusV1MaxReadWords,
				"raw_reads_per_window":         ModbusV1MaxRawReadsPerWindow,
				"raw_read_window_milliseconds": ModbusV1RawReadWindow.Milliseconds(),
				"identity_max_bytes":           modbusV1MaxIdentityBytes,
			},
		},
		"data":  data,
		"error": envelopeError,
	}
}
