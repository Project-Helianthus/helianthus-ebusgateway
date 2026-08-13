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
}

var modbusV1Providers = struct {
	sync.RWMutex
	byServer map[*Server]ModbusV1Provider
}{byServer: make(map[*Server]ModbusV1Provider)}

func RegisterModbusV1Tools(server *Server, provider ModbusV1Provider) {
	if server == nil || provider == nil {
		return
	}
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
	)
}

func (server *Server) handleModbusV1Call(ctx context.Context, name string, args map[string]any) (map[string]any, bool) {
	modbusV1Providers.RLock()
	provider := modbusV1Providers.byServer[server]
	modbusV1Providers.RUnlock()
	if provider == nil {
		return nil, false
	}
	var data any
	var err error
	providerCalled := false
	consistencyMode := "LIVE"
	dataTimestamp := time.Now().UTC().Format(time.RFC3339Nano)
	switch name {
	case ModbusV1RawReadTool:
		var request ModbusRawReadRequest
		request, err = parseModbusRawReadRequest(args)
		if err == nil {
			providerCalled = true
			data, err = provider.RawRead(ctx, request)
		}
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
		retriable := false
		if providerCalled {
			code = "UNAVAILABLE"
			retriable = true
		}
		if errors.Is(err, ErrModbusV1ResourceExhausted) {
			code = "RESOURCE_EXHAUSTED"
		}
		envelopeError = map[string]any{
			"code":         code,
			"message":      err.Error(),
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
