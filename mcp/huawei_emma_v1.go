package mcp

import (
	"context"
	"errors"
	"sync"
)

const (
	HuaweiEMMAV1IdentityGetTool = "modbus.v1.huawei.emma.identity.get"
	HuaweiEMMAV1Profile         = "huawei.emma.readonly.v1"
)

type HuaweiEMMAV1Result struct {
	Profile         string `json:"profile"`
	CanonicalClass  string `json:"canonical_class"`
	ModelVariant    string `json:"model_variant"`
	Qualified       bool   `json:"qualified"`
	RawRedacted     bool   `json:"raw_redacted"`
	OutboundAllowed bool   `json:"outbound_allowed"`
}
type HuaweiEMMAV1Provider interface {
	HuaweiEMMAV1(context.Context) (HuaweiEMMAV1Result, error)
}

var huaweiEMMAV1Providers = struct {
	sync.RWMutex
	byServer map[*Server]HuaweiEMMAV1Provider
}{byServer: map[*Server]HuaweiEMMAV1Provider{}}

func registerHuaweiEMMAV1Tool(s *Server, p ModbusV1Provider) {
	v, ok := p.(HuaweiEMMAV1Provider)
	if !ok || v == nil {
		return
	}
	huaweiEMMAV1Providers.Lock()
	huaweiEMMAV1Providers.byServer[s] = v
	huaweiEMMAV1Providers.Unlock()
	s.tools = append(s.tools, Tool{Name: HuaweiEMMAV1IdentityGetTool, Description: "Get the redacted read-only Huawei EMMA identity qualification.", InputSchema: map[string]any{"type": "object", "properties": map[string]any{}, "additionalProperties": false}})
}
func (s *Server) handleHuaweiEMMAV1Call(ctx context.Context, n string, a map[string]any) (map[string]any, bool) {
	if n != HuaweiEMMAV1IdentityGetTool {
		return nil, false
	}
	if len(a) != 0 {
		return callToolResultText(mustJSON(newModbusV1Envelope(nil, errors.New("invalid Huawei EMMA identity arguments"), false, "RETAINED_PROFILE", "")), true), true
	}
	huaweiEMMAV1Providers.RLock()
	p := huaweiEMMAV1Providers.byServer[s]
	huaweiEMMAV1Providers.RUnlock()
	if p == nil {
		return callToolResultText(mustJSON(newModbusV1Envelope(nil, errors.New("huawei EMMA provider unavailable"), false, "RETAINED_PROFILE", "")), true), true
	}
	r, e := p.HuaweiEMMAV1(ctx)
	r.RawRedacted = true
	r.OutboundAllowed = false
	return callToolResultText(mustJSON(newModbusV1Envelope(r, e, true, "RETAINED_PROFILE", "")), e != nil), true
}
