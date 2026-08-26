package mcp

import (
	"context"
	"errors"
	"sync"
)

const TeslaHSCV1StatusGetTool = "modbus.v1.tesla.hsc.status.get"

const maxTeslaHSCV1NativeRecords = 8

// TeslaHSCV1NativeRecord retains one native Tesla HSC message with the
// firmware-scoped names and provenance supplied by the selected codec.
type TeslaHSCV1NativeRecord struct {
	Function      byte     `json:"function"`
	Payload       []byte   `json:"payload"`
	Compatibility string   `json:"compatibility"`
	Provenance    string   `json:"provenance"`
	Family        uint64   `json:"family,omitempty"`
	RequestTag    uint64   `json:"request_tag,omitempty"`
	ResponseTag   uint64   `json:"response_tag,omitempty"`
	RequestName   string   `json:"request_name,omitempty"`
	ResponseName  string   `json:"response_name,omitempty"`
	FieldNames    []string `json:"field_names,omitempty"`
}

// TeslaHSCV1Result is the native Tesla HSC profile snapshot. It carries the
// complete records emitted by an already-correlated injected provider.
type TeslaHSCV1Result struct {
	Disposition     string                   `json:"disposition"`
	Compatibility   string                   `json:"compatibility"`
	OutboundAllowed bool                     `json:"outbound_allowed"`
	NativeRecords   []TeslaHSCV1NativeRecord `json:"native_records,omitempty"`
}

// TeslaHSCV1Provider supplies a native profile snapshot without transport I/O.
type TeslaHSCV1Provider interface {
	TeslaHSCV1(context.Context) (TeslaHSCV1Result, error)
}

var teslaHSCV1Providers = struct {
	sync.RWMutex
	byServer map[*Server]TeslaHSCV1Provider
}{byServer: make(map[*Server]TeslaHSCV1Provider)}

func registerTeslaHSCV1Tool(server *Server, provider ModbusV1Provider) {
	tesla, ok := provider.(TeslaHSCV1Provider)
	if !ok || tesla == nil {
		return
	}
	teslaHSCV1Providers.Lock()
	teslaHSCV1Providers.byServer[server] = tesla
	teslaHSCV1Providers.Unlock()
	server.tools = append(server.tools, Tool{Name: TeslaHSCV1StatusGetTool, Description: "Get native Tesla HSC records and firmware-scoped provenance from an injected correlated provider.", InputSchema: map[string]any{"type": "object", "properties": map[string]any{}, "additionalProperties": false}})
}

func (server *Server) handleTeslaHSCV1Call(ctx context.Context, name string, args map[string]any) (map[string]any, bool) {
	if name != TeslaHSCV1StatusGetTool {
		return nil, false
	}
	if len(args) != 0 {
		return callToolResultText(mustJSON(newModbusV1Envelope(nil, errors.New("invalid Tesla HSC status arguments"), false, "RETAINED_PROFILE", "")), true), true
	}
	teslaHSCV1Providers.RLock()
	provider := teslaHSCV1Providers.byServer[server]
	teslaHSCV1Providers.RUnlock()
	if provider == nil {
		return callToolResultText(mustJSON(newModbusV1Envelope(nil, errors.New("tesla HSC provider unavailable"), false, "RETAINED_PROFILE", "")), true), true
	}
	result, err := provider.TeslaHSCV1(ctx)
	if err == nil && !validTeslaHSCV1Result(result) {
		result = TeslaHSCV1Result{}
		err = errors.New("tesla HSC native provider result is invalid")
	}
	return callToolResultText(mustJSON(newModbusV1Envelope(result, err, true, "RETAINED_PROFILE", "")), err != nil), true
}

func validTeslaHSCV1Result(result TeslaHSCV1Result) bool {
	if len(result.NativeRecords) > maxTeslaHSCV1NativeRecords {
		return false
	}
	for _, record := range result.NativeRecords {
		if !validTeslaHSCV1NativeRecord(record) {
			return false
		}
	}
	return true
}

func validTeslaHSCV1NativeRecord(record TeslaHSCV1NativeRecord) bool {
	if len(record.Payload) > 252 || record.Compatibility == "" || record.Provenance == "" {
		return false
	}
	switch record.Function {
	case 100, 101, 102:
		return true
	default:
		return false
	}
}
