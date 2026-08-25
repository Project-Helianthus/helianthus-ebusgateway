package mcp

import (
	"context"
	"errors"
	"sync"
)

const (
	HuaweiSDongleV1StatusGetTool = "modbus.v1.huawei.sdongle.status.get"
	HuaweiSDongleV1Profile       = "huawei.sdongle.readonly.v1"
)

type HuaweiSDongleV1Result struct {
	Profile         string `json:"profile"`
	Family          string `json:"family"`
	Disposition     string `json:"disposition"`
	Qualified       bool   `json:"qualified"`
	RawRedacted     bool   `json:"raw_redacted"`
	OutboundAllowed bool   `json:"outbound_allowed"`
}
type HuaweiSDongleV1Provider interface {
	HuaweiSDongleV1(context.Context) (HuaweiSDongleV1Result, error)
}

var huaweiSDongleV1Providers = struct {
	sync.RWMutex
	byServer map[*Server]HuaweiSDongleV1Provider
}{byServer: map[*Server]HuaweiSDongleV1Provider{}}

func registerHuaweiSDongleV1Tool(s *Server, p ModbusV1Provider) {
	v, ok := p.(HuaweiSDongleV1Provider)
	if !ok || v == nil {
		return
	}
	huaweiSDongleV1Providers.Lock()
	huaweiSDongleV1Providers.byServer[s] = v
	huaweiSDongleV1Providers.Unlock()
	s.tools = append(s.tools, Tool{Name: HuaweiSDongleV1StatusGetTool, Description: "Get the redacted pre-live Huawei S-Dongle status.", InputSchema: map[string]any{"type": "object", "properties": map[string]any{}, "additionalProperties": false}})
}
func (s *Server) handleHuaweiSDongleV1Call(ctx context.Context, n string, a map[string]any) (map[string]any, bool) {
	if n != HuaweiSDongleV1StatusGetTool {
		return nil, false
	}
	if len(a) != 0 {
		return callToolResultText(mustJSON(newModbusV1Envelope(nil, errors.New("invalid Huawei S-Dongle status arguments"), false, "RETAINED_PROFILE", "")), true), true
	}
	huaweiSDongleV1Providers.RLock()
	p := huaweiSDongleV1Providers.byServer[s]
	huaweiSDongleV1Providers.RUnlock()
	if p == nil {
		return callToolResultText(mustJSON(newModbusV1Envelope(nil, errors.New("huawei S-Dongle provider unavailable"), false, "RETAINED_PROFILE", "")), true), true
	}
	r, e := p.HuaweiSDongleV1(ctx)
	r.Profile = HuaweiSDongleV1Profile
	r.Family = "S-Dongle"
	r.Disposition = "PRE_LIVE_INSUFFICIENT_EVIDENCE"
	r.Qualified = false
	r.RawRedacted = true
	r.OutboundAllowed = false
	return callToolResultText(mustJSON(newModbusV1Envelope(r, e, true, "RETAINED_PROFILE", "")), e != nil), true
}
