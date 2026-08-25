package mcp

import (
	"context"
	"errors"
	"sync"
)

const (
	GrowattProtocolIIV1IdentityGetTool = "modbus.v1.growatt.protocol_ii.identity.get"
	GrowattProtocolIIV1Profile         = "growatt.protocol_ii.tl3_x.identity.readonly.v1"
)

// GrowattProtocolIIV1Result is the only identity status projected to MCP.
// Raw identity words and individual tuple values remain provider-private.
type GrowattProtocolIIV1Result struct {
	Profile           string `json:"profile"`
	Disposition       string `json:"disposition"`
	Family            string `json:"family"`
	IdentityQualified bool   `json:"identity_qualified"`
	IdentityRedacted  bool   `json:"identity_redacted"`
	OutboundAllowed   bool   `json:"outbound_allowed"`
}

type GrowattProtocolIIV1Provider interface {
	GrowattProtocolIIV1(context.Context) (GrowattProtocolIIV1Result, error)
}

var growattProtocolIIV1Providers = struct {
	sync.RWMutex
	byServer map[*Server]GrowattProtocolIIV1Provider
}{byServer: make(map[*Server]GrowattProtocolIIV1Provider)}

func registerGrowattProtocolIIV1Tool(server *Server, provider ModbusV1Provider) {
	growatt, ok := provider.(GrowattProtocolIIV1Provider)
	if !ok || growatt == nil {
		return
	}
	growattProtocolIIV1Providers.Lock()
	growattProtocolIIV1Providers.byServer[server] = growatt
	growattProtocolIIV1Providers.Unlock()
	server.tools = append(server.tools, Tool{Name: GrowattProtocolIIV1IdentityGetTool, Description: "Get the redacted read-only Growatt Protocol II identity qualification.", InputSchema: map[string]any{"type": "object", "properties": map[string]any{}, "additionalProperties": false}})
}

func (server *Server) handleGrowattProtocolIIV1Call(ctx context.Context, name string, args map[string]any) (map[string]any, bool) {
	if name != GrowattProtocolIIV1IdentityGetTool {
		return nil, false
	}
	if len(args) != 0 {
		return callToolResultText(mustJSON(newModbusV1Envelope(nil, errors.New("invalid Growatt Protocol II identity arguments"), false, "RETAINED_PROFILE", "")), true), true
	}
	growattProtocolIIV1Providers.RLock()
	provider := growattProtocolIIV1Providers.byServer[server]
	growattProtocolIIV1Providers.RUnlock()
	if provider == nil {
		return callToolResultText(mustJSON(newModbusV1Envelope(nil, errors.New("Growatt Protocol II provider unavailable"), false, "RETAINED_PROFILE", "")), true), true
	}
	result, err := provider.GrowattProtocolIIV1(ctx)
	result = failClosedGrowattProtocolIIV1Result(result)
	return callToolResultText(mustJSON(newModbusV1Envelope(result, err, true, "RETAINED_PROFILE", "")), err != nil), true
}

func failClosedGrowattProtocolIIV1Result(result GrowattProtocolIIV1Result) GrowattProtocolIIV1Result {
	result.IdentityRedacted = true
	result.OutboundAllowed = false
	return result
}
