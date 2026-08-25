package mcp

import (
	"context"
	"errors"
	"sync"
)

const (
	HuaweiSmartLoggerV1StatusGetTool = "modbus.v1.huawei.smartlogger.status.get"
	HuaweiSmartLoggerV1Profile       = "huawei.smartlogger.readonly.v1"
)

// HuaweiSmartLoggerV1Result is intentionally limited to redacted, read-only
// qualification status. Inventory and transport evidence remain private.
type HuaweiSmartLoggerV1Result struct {
	Profile         string `json:"profile"`
	Family          string `json:"family"`
	Qualified       bool   `json:"qualified"`
	RawRedacted     bool   `json:"raw_redacted"`
	OutboundAllowed bool   `json:"outbound_allowed"`
}

type HuaweiSmartLoggerV1Provider interface {
	HuaweiSmartLoggerV1(context.Context) (HuaweiSmartLoggerV1Result, error)
}

var huaweiSmartLoggerV1Providers = struct {
	sync.RWMutex
	byServer map[*Server]HuaweiSmartLoggerV1Provider
}{byServer: make(map[*Server]HuaweiSmartLoggerV1Provider)}

func registerHuaweiSmartLoggerV1Tool(server *Server, provider ModbusV1Provider) {
	value, ok := provider.(HuaweiSmartLoggerV1Provider)
	if !ok || value == nil {
		return
	}
	huaweiSmartLoggerV1Providers.Lock()
	huaweiSmartLoggerV1Providers.byServer[server] = value
	huaweiSmartLoggerV1Providers.Unlock()
	server.tools = append(server.tools, Tool{Name: HuaweiSmartLoggerV1StatusGetTool, Description: "Get the redacted read-only Huawei SmartLogger status.", InputSchema: map[string]any{"type": "object", "properties": map[string]any{}, "additionalProperties": false}})
}

func (server *Server) handleHuaweiSmartLoggerV1Call(ctx context.Context, name string, args map[string]any) (map[string]any, bool) {
	if name != HuaweiSmartLoggerV1StatusGetTool {
		return nil, false
	}
	if len(args) != 0 {
		return callToolResultText(mustJSON(newModbusV1Envelope(nil, errors.New("invalid Huawei SmartLogger status arguments"), false, "RETAINED_PROFILE", "")), true), true
	}
	huaweiSmartLoggerV1Providers.RLock()
	provider := huaweiSmartLoggerV1Providers.byServer[server]
	huaweiSmartLoggerV1Providers.RUnlock()
	if provider == nil {
		return callToolResultText(mustJSON(newModbusV1Envelope(nil, errors.New("huawei SmartLogger provider unavailable"), false, "RETAINED_PROFILE", "")), true), true
	}
	result, err := provider.HuaweiSmartLoggerV1(ctx)
	result.RawRedacted = true
	result.OutboundAllowed = false
	return callToolResultText(mustJSON(newModbusV1Envelope(result, err, true, "RETAINED_PROFILE", "")), err != nil), true
}
