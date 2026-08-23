package mcp

import (
	"context"
	"errors"
	"sync"
)

const TeslaHSCV1StatusGetTool = "modbus.v1.tesla.hsc.status.get"

// TeslaHSCV1Result is the redacted, read-only Tesla profile snapshot.
type TeslaHSCV1Result struct {
	Disposition     string `json:"disposition"`
	Compatibility   string `json:"compatibility"`
	OutboundAllowed bool   `json:"outbound_allowed"`
	RetainedLength  int    `json:"retained_length"`
	RetainedDigest  string `json:"retained_digest"`
}

// TeslaHSCV1Provider supplies a redacted profile snapshot without transport I/O.
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
	server.tools = append(server.tools, Tool{Name: TeslaHSCV1StatusGetTool, Description: "Get the read-only Tesla HSC profile disposition and redacted opaque retention metadata.", InputSchema: map[string]any{"type": "object", "properties": map[string]any{}, "additionalProperties": false}})
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
		return callToolResultText(mustJSON(newModbusV1Envelope(nil, errors.New("Tesla HSC provider unavailable"), false, "RETAINED_PROFILE", "")), true), true
	}
	result, err := provider.TeslaHSCV1(ctx)
	return callToolResultText(mustJSON(newModbusV1Envelope(result, err, true, "RETAINED_PROFILE", "")), err != nil), true
}
