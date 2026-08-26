package mcp

import (
	"context"
	"errors"
	"sync"
)

const (
	OutBackAXSV1StatusGetTool = "modbus.v1.outback.axs.status.get"
	OutBackAXSV1Profile       = "outback.axs.readonly.v1"
)

type OutBackAXSV1Result struct {
	Profile            string   `json:"profile"`
	Qualified          bool     `json:"qualified"`
	FirmwareMajor      uint16   `json:"firmware_major"`
	FirmwareMid        uint16   `json:"firmware_mid"`
	FirmwareMinor      uint16   `json:"firmware_minor"`
	BatteryTemperature int16    `json:"battery_temperature"`
	AmbientTemperature int16    `json:"ambient_temperature"`
	TemperatureScale   int16    `json:"temperature_scale"`
	Error              uint16   `json:"error"`
	Status             uint16   `json:"status"`
	RawWords           []uint16 `json:"raw_words"`
	OutboundAllowed    bool     `json:"outbound_allowed"`
}
type OutBackAXSV1Provider interface {
	OutBackAXSV1(context.Context) (OutBackAXSV1Result, error)
}

var outBackAXSV1Providers = struct {
	sync.RWMutex
	byServer map[*Server]OutBackAXSV1Provider
}{byServer: make(map[*Server]OutBackAXSV1Provider)}

func registerOutBackAXSV1Tool(server *Server, provider ModbusV1Provider) {
	p, ok := provider.(OutBackAXSV1Provider)
	if !ok || p == nil {
		return
	}
	outBackAXSV1Providers.Lock()
	outBackAXSV1Providers.byServer[server] = p
	outBackAXSV1Providers.Unlock()
	server.tools = append(server.tools, Tool{Name: OutBackAXSV1StatusGetTool, Description: "Get the native OutBack AXS observation status.", InputSchema: map[string]any{"type": "object", "properties": map[string]any{}, "additionalProperties": false}})
}
func (server *Server) handleOutBackAXSV1Call(ctx context.Context, name string, args map[string]any) (map[string]any, bool) {
	if name != OutBackAXSV1StatusGetTool {
		return nil, false
	}
	if len(args) != 0 {
		return callToolResultText(mustJSON(newModbusV1Envelope(nil, errors.New("invalid OutBack AXS status arguments"), false, "RETAINED_PROFILE", "")), true), true
	}
	outBackAXSV1Providers.RLock()
	p := outBackAXSV1Providers.byServer[server]
	outBackAXSV1Providers.RUnlock()
	if p == nil {
		return callToolResultText(mustJSON(newModbusV1Envelope(nil, errors.New("outback AXS provider unavailable"), false, "RETAINED_PROFILE", "")), true), true
	}
	r, err := p.OutBackAXSV1(ctx)
	r.RawWords = append([]uint16(nil), r.RawWords...)
	return callToolResultText(mustJSON(newModbusV1Envelope(r, err, true, "RETAINED_PROFILE", "")), err != nil), true
}
