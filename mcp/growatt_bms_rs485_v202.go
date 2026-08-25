package mcp

import (
	"context"
	"errors"
	"math"
	"regexp"
	"sync"

	modbusreg "github.com/Project-Helianthus/helianthus-modbusreg"
)

const (
	GrowattBMSRS485V202StatusGetTool = "modbus.v1.growatt.bms.rs485.status.get"
	GrowattBMSRS485V202Profile       = "growatt.bms.rs485.1xsxxp.v2_02.readonly.v1"
)

var (
	ErrGrowattBMSRS485V202ProviderUnavailable = errors.New("growatt BMS RS485 provider unavailable")
	ErrGrowattBMSRS485V202NotQualified        = errors.New("growatt BMS RS485 typed status is not qualified")
	growattBMSRS485V202Version                = regexp.MustCompile(`^[0-9]{1,3}\.[0-9]{1,3}$`)
)

type GrowattBMSRS485V202Identity struct {
	MCUSoftwareVersion string `json:"mcu_software_version"`
	GaugeVersion       string `json:"gauge_version"`
	BMSCompany         uint8  `json:"bms_company"`
	BMSGeneration      uint8  `json:"bms_generation"`
	PackCompany        uint8  `json:"pack_company"`
	PackGeneration     uint8  `json:"pack_generation"`
}

type GrowattBMSRS485V202Status struct {
	OperatingState string `json:"operating_state"`
	SOCPercent     uint8  `json:"soc_percent"`
	CycleCount     uint16 `json:"cycle_count"`
}

type GrowattBMSRS485V202Telemetry struct {
	PackVoltageVolts            float64 `json:"pack_voltage_volts"`
	PackCurrentAmps             float64 `json:"pack_current_amps"`
	TemperatureCelsius          int16   `json:"temperature_celsius"`
	RemainingCapacityAmpHours   float64 `json:"remaining_capacity_amp_hours"`
	FullChargeCapacityAmpHours  float64 `json:"full_charge_capacity_amp_hours"`
	ContinuousChargeSeconds     uint16  `json:"continuous_charge_seconds"`
	CurrentCycleChargeAmpHours  float64 `json:"current_cycle_charge_amp_hours"`
	AverageCellVoltageVolts     float64 `json:"average_cell_voltage_volts"`
	FloatingPackVoltageVolts    float64 `json:"floating_pack_voltage_volts"`
	CumulativeChargeAmpHours    float64 `json:"cumulative_charge_amp_hours"`
	CumulativeDischargeAmpHours float64 `json:"cumulative_discharge_amp_hours"`
}

type GrowattBMSRS485V202Revision struct {
	Family             string `json:"family"`
	FileRevision       string `json:"file_revision"`
	HeaderVersion      string `json:"header_version"`
	CumulativeRevision string `json:"cumulative_revision"`
}

type GrowattBMSRS485V202Result struct {
	Profile         string                       `json:"profile"`
	Revision        GrowattBMSRS485V202Revision  `json:"revision"`
	Qualified       bool                         `json:"qualified"`
	RawRedacted     bool                         `json:"raw_redacted"`
	OutboundAllowed bool                         `json:"outbound_allowed"`
	Identity        GrowattBMSRS485V202Identity  `json:"identity"`
	Status          GrowattBMSRS485V202Status    `json:"status"`
	Telemetry       GrowattBMSRS485V202Telemetry `json:"telemetry"`
}

// GrowattBMSRS485V202Provider supplies only a previously decoded typed status.
// It has no route for raw words, a serial, an endpoint, or a transport handle.
type GrowattBMSRS485V202Provider interface {
	GrowattBMSRS485V202(context.Context) (modbusreg.GrowattBMSTypedReadOnlyStatus, error)
}

var growattBMSRS485V202Providers = struct {
	sync.RWMutex
	byServer map[*Server]GrowattBMSRS485V202Provider
}{byServer: make(map[*Server]GrowattBMSRS485V202Provider)}

func registerGrowattBMSRS485V202Tool(server *Server, provider ModbusV1Provider) {
	p, ok := provider.(GrowattBMSRS485V202Provider)
	if !ok || p == nil {
		return
	}
	growattBMSRS485V202Providers.Lock()
	growattBMSRS485V202Providers.byServer[server] = p
	growattBMSRS485V202Providers.Unlock()
	server.tools = append(server.tools, Tool{Name: GrowattBMSRS485V202StatusGetTool, Description: "Get the redacted, read-only Growatt BMS RS485 typed status.", InputSchema: map[string]any{"type": "object", "properties": map[string]any{}, "additionalProperties": false}})
}

func (server *Server) handleGrowattBMSRS485V202Call(ctx context.Context, name string, args map[string]any) (map[string]any, bool) {
	if name != GrowattBMSRS485V202StatusGetTool {
		return nil, false
	}
	if len(args) != 0 {
		return callToolResultText(mustJSON(newModbusV1Envelope(nil, errors.New("invalid Growatt BMS RS485 status arguments"), false, "RETAINED_PROFILE", "")), true), true
	}
	growattBMSRS485V202Providers.RLock()
	p := growattBMSRS485V202Providers.byServer[server]
	growattBMSRS485V202Providers.RUnlock()
	if p == nil {
		return callToolResultText(mustJSON(newModbusV1Envelope(nil, ErrGrowattBMSRS485V202ProviderUnavailable, false, "RETAINED_PROFILE", "")), true), true
	}
	status, err := p.GrowattBMSRS485V202(ctx)
	if err != nil {
		return callToolResultText(mustJSON(newModbusV1Envelope(nil, err, true, "RETAINED_PROFILE", "")), true), true
	}
	result, err := growattBMSRS485V202Result(status)
	return callToolResultText(mustJSON(newModbusV1Envelope(result, err, true, "RETAINED_PROFILE", "")), err != nil), true
}

func growattBMSRS485V202Result(status modbusreg.GrowattBMSTypedReadOnlyStatus) (GrowattBMSRS485V202Result, error) {
	if status.OutboundAllowed() ||
		status.Revision != (modbusreg.GrowattBMSRevisionTuple{Family: "1xSxxP ESS", FileRevision: "Rev2.01", HeaderVersion: "V2.0", CumulativeRevision: "2.02"}) ||
		!growattBMSRS485V202Version.MatchString(status.MCUSoftwareVersion) || !growattBMSRS485V202Version.MatchString(status.GaugeVersion) ||
		status.SOCPercent > 100 || status.TemperatureCelsius < -127 || status.TemperatureCelsius > 127 ||
		!validGrowattBMSRS485V202State(status.OperatingState) || !validGrowattBMSRS485V202Telemetry(status) {
		return GrowattBMSRS485V202Result{}, ErrGrowattBMSRS485V202NotQualified
	}
	return GrowattBMSRS485V202Result{
		Profile: GrowattBMSRS485V202Profile,
		Revision: GrowattBMSRS485V202Revision{
			Family:             status.Revision.Family,
			FileRevision:       status.Revision.FileRevision,
			HeaderVersion:      status.Revision.HeaderVersion,
			CumulativeRevision: status.Revision.CumulativeRevision,
		},
		Qualified:       true,
		RawRedacted:     true,
		OutboundAllowed: false,
		Identity: GrowattBMSRS485V202Identity{
			MCUSoftwareVersion: status.MCUSoftwareVersion, GaugeVersion: status.GaugeVersion,
			BMSCompany: status.BMSCompany, BMSGeneration: status.BMSGeneration,
			PackCompany: status.PackCompany, PackGeneration: status.PackGeneration,
		},
		Status: GrowattBMSRS485V202Status{OperatingState: string(status.OperatingState), SOCPercent: status.SOCPercent, CycleCount: status.CycleCount},
		Telemetry: GrowattBMSRS485V202Telemetry{
			PackVoltageVolts: status.PackVoltageVolts, PackCurrentAmps: status.PackCurrentAmps, TemperatureCelsius: status.TemperatureCelsius,
			RemainingCapacityAmpHours: status.RemainingCapacityAmpHours, FullChargeCapacityAmpHours: status.FullChargeCapacityAmpHours,
			ContinuousChargeSeconds: status.ContinuousChargeSeconds, CurrentCycleChargeAmpHours: status.CurrentCycleChargeAmpHours,
			AverageCellVoltageVolts: status.AverageCellVoltageVolts, FloatingPackVoltageVolts: status.FloatingPackVoltageVolts,
			CumulativeChargeAmpHours: status.CumulativeChargeAmpHours, CumulativeDischargeAmpHours: status.CumulativeDischargeAmpHours,
		},
	}, nil
}

func validGrowattBMSRS485V202State(state modbusreg.GrowattBMSOperatingState) bool {
	switch state {
	case modbusreg.GrowattBMSStateSoftStarting, modbusreg.GrowattBMSStateStandby, modbusreg.GrowattBMSStateCharging, modbusreg.GrowattBMSStateDischarging:
		return true
	default:
		return false
	}
}

func validGrowattBMSRS485V202Telemetry(status modbusreg.GrowattBMSTypedReadOnlyStatus) bool {
	for _, value := range []float64{
		status.PackVoltageVolts, status.PackCurrentAmps, status.RemainingCapacityAmpHours, status.FullChargeCapacityAmpHours,
		status.CurrentCycleChargeAmpHours, status.AverageCellVoltageVolts, status.FloatingPackVoltageVolts,
		status.CumulativeChargeAmpHours, status.CumulativeDischargeAmpHours,
	} {
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return false
		}
	}
	return status.PackVoltageVolts >= 0 && status.RemainingCapacityAmpHours >= 0 && status.FullChargeCapacityAmpHours >= 0 &&
		status.CurrentCycleChargeAmpHours >= 0 && status.AverageCellVoltageVolts >= 0 && status.FloatingPackVoltageVolts >= 0 &&
		status.CumulativeChargeAmpHours >= 0 && status.CumulativeDischargeAmpHours >= 0
}
