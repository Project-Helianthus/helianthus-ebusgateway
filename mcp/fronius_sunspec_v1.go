package mcp

import (
	"context"
	"errors"
	"math"
	"sync"

	modbusreg "github.com/Project-Helianthus/helianthus-modbusreg"
)

const (
	FroniusSunSpecV1StatusGetTool = "modbus.v1.fronius.sunspec.status.get"
	FroniusSunSpecV1Profile       = "sunspec.fronius.readonly.v1"
)

// FroniusSunSpecV1Telemetry is the bounded standard Model 113 projection
// exposed by the read-only Fronius status tool.
type FroniusSunSpecV1Telemetry struct {
	ACActivePowerWatts      float64 `json:"ac_active_power_watts"`
	ACFrequencyHertz        float64 `json:"ac_frequency_hertz"`
	LifetimeEnergyWattHours float64 `json:"lifetime_energy_watt_hours"`
	OperatingState          string  `json:"operating_state"`
}

type FroniusSunSpecV1Result struct {
	Profile         string                     `json:"profile"`
	Capability      string                     `json:"capability"`
	Qualification   string                     `json:"qualification"`
	Qualified       bool                       `json:"qualified"`
	Telemetry       *FroniusSunSpecV1Telemetry `json:"telemetry,omitempty"`
	RawRedacted     bool                       `json:"raw_redacted"`
	OutboundAllowed bool                       `json:"outbound_allowed"`
}

type FroniusSunSpecV1Provider interface {
	FroniusSunSpecV1(context.Context) (FroniusSunSpecV1Result, error)
}

var froniusSunSpecV1Providers = struct {
	sync.RWMutex
	byServer map[*Server]FroniusSunSpecV1Provider
}{byServer: make(map[*Server]FroniusSunSpecV1Provider)}

func registerFroniusSunSpecV1Tool(server *Server, provider ModbusV1Provider) {
	p, ok := provider.(FroniusSunSpecV1Provider)
	if !ok || p == nil {
		return
	}
	froniusSunSpecV1Providers.Lock()
	froniusSunSpecV1Providers.byServer[server] = p
	froniusSunSpecV1Providers.Unlock()
	server.tools = append(server.tools, Tool{Name: FroniusSunSpecV1StatusGetTool, Description: "Get the redacted, read-only Fronius SunSpec qualification and standard telemetry.", InputSchema: map[string]any{"type": "object", "properties": map[string]any{}, "additionalProperties": false}})
}

func (server *Server) handleFroniusSunSpecV1Call(ctx context.Context, name string, args map[string]any) (map[string]any, bool) {
	if name != FroniusSunSpecV1StatusGetTool {
		return nil, false
	}
	if len(args) != 0 {
		return callToolResultText(mustJSON(newModbusV1Envelope(nil, errors.New("invalid Fronius SunSpec status arguments"), false, "RETAINED_PROFILE", "")), true), true
	}
	froniusSunSpecV1Providers.RLock()
	p := froniusSunSpecV1Providers.byServer[server]
	froniusSunSpecV1Providers.RUnlock()
	if p == nil {
		return callToolResultText(mustJSON(newModbusV1Envelope(nil, errors.New("fronius SunSpec provider unavailable"), false, "RETAINED_PROFILE", "")), true), true
	}
	result, err := p.FroniusSunSpecV1(ctx)
	return callToolResultText(mustJSON(newModbusV1Envelope(normalizeFroniusSunSpecV1Result(result, err), err, true, "RETAINED_PROFILE", "")), err != nil), true
}

func normalizeFroniusSunSpecV1Result(result FroniusSunSpecV1Result, err error) FroniusSunSpecV1Result {
	result.Profile = FroniusSunSpecV1Profile
	result.Capability = modbusreg.SunSpecThreePhaseMonitoringCapabilityID
	result.RawRedacted = true
	result.OutboundAllowed = false
	if err != nil || !result.Qualified || !validFroniusSunSpecV1Telemetry(result.Telemetry) {
		result.Qualified = false
		result.Qualification = "NOT_QUALIFIED"
		result.Telemetry = nil
		return result
	}
	result.Qualification = "QUALIFIED"
	telemetry := *result.Telemetry
	result.Telemetry = &telemetry
	return result
}

func validFroniusSunSpecV1Telemetry(telemetry *FroniusSunSpecV1Telemetry) bool {
	if telemetry == nil || math.IsNaN(telemetry.ACActivePowerWatts) || math.IsInf(telemetry.ACActivePowerWatts, 0) || math.IsNaN(telemetry.ACFrequencyHertz) || math.IsInf(telemetry.ACFrequencyHertz, 0) || math.IsNaN(telemetry.LifetimeEnergyWattHours) || math.IsInf(telemetry.LifetimeEnergyWattHours, 0) {
		return false
	}
	switch telemetry.OperatingState {
	case "OFF", "SLEEPING", "STARTING", "MPPT", "THROTTLED", "SHUTTING_DOWN", "FAULT", "STANDBY":
		return true
	default:
		return false
	}
}
