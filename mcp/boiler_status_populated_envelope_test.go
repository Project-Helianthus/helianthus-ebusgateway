package mcp

import (
	"encoding/json"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
)

// TestSemanticBoilerStatusPopulatedEnvelopePinsRegisterNames pins the exact
// ebus_v1_semantic_boiler_status_get output (keys and data_hash) for a fully
// populated state, config and diagnostics payload, so the published register
// names are reviewed byte for byte.
func TestSemanticBoilerStatusPopulatedEnvelopePinsRegisterNames(t *testing.T) {
	f, i, b := 1.5, 2, true
	s := "text"
	server, err := NewServer(registry.NewDeviceRegistry(nil), &testInvoker{})
	if err != nil {
		t.Fatalf("NewServer error = %v", err)
	}
	server.SetSemanticProvider(testSemanticProvider{
		boiler: &BoilerStatus{
			State: &BoilerState{
				FlowTemperatureC: &f, ReturnTemperatureC: &f, CentralHeatingPumpActive: &b,
				WaterPressureBar: &f, ExternalPumpActive: &b, CirculationPumpActive: &b,
				GasValveActive: &b, FlameActive: &b, DiverterValvePositionPct: &f,
				FanSpeedRpm: &i, TargetFanSpeedRpm: &i, IonisationVoltageUa: &f,
				DhwWaterFlowLpm: &f, DhwDemandActive: &b, HeatingSwitchActive: &b,
				StorageLoadPumpPct: &f, ModulationPct: &f, PrimaryCircuitFlowLpm: &f,
				FlowTempDesiredC: &f, DhwTempDesiredC: &f, StateNumber: &i,
				DhwTemperatureC: &f, DhwTargetTemperatureC: &f,
			},
			Config: &BoilerConfig{
				DhwOperatingMode: &s, FlowsetHcMaxC: &f, FlowsetHwcMaxC: &f,
				PartloadHcKW: &f, PartloadHwcKW: &f, InstallerMenuCode: &i,
				PhoneNumber: &s, HoursTillService: &i,
			},
			Diagnostics: &BoilerDiagnostics{
				HeatingStatusRaw: &i, DhwStatusRaw: &i, CentralHeatingHours: &f,
				DhwHours: &f, CentralHeatingStarts: &i, DhwStarts: &i,
				PumpHours: &f, FanHours: &f, DeactivationsIFC: &i, DeactivationsTemplimiter: &i,
			},
		},
	})
	env := envelopeFromResult(t, doRPC(t, server.Handler(), rpcRequest{
		JSONRPC: "2.0",
		ID:      1,
		Method:  "tools/call",
		Params:  json.RawMessage(`{"name":"ebus_v1_semantic_boiler_status_get","arguments":{}}`),
	}))
	data, err := json.Marshal(env["data"])
	if err != nil {
		t.Fatalf("marshal data: %v", err)
	}
	const wantData = `{"config":{"central_heating_pump_mode":2,"dhw_operating_mode":"text","dhw_partial_load_power":1.5,"heating_partial_load_power":1.5,"installer_phone_number":"text","maximum_dhw_flow_temperature_setpoint":1.5,"maximum_heating_flow_temperature_setpoint":1.5,"service_time_remaining":2},"diagnostics":{"central_heating_operating_time":1.5,"central_heating_start_count":2,"dhw_operating_time":1.5,"dhw_start_count":2,"dhw_status_raw":2,"fan_operating_time":1.5,"fire_control_deactivation_count":2,"heating_status_raw":2,"pump_operating_time":1.5,"temperature_limiter_deactivation_count":2},"state":{"boiler_state_code":2,"central_heating_pump_active":true,"circulation_pump_active":true,"dhw_demand_active":true,"dhw_target_temperature_c":1.5,"dhw_temperature_c":1.5,"dhw_temperature_setpoint":1.5,"dhw_water_flow_rate":1.5,"diverter_valve_position":1.5,"external_pump_active":true,"fan_speed":2,"fan_speed_setpoint":2,"flame_active":true,"flame_ionization_current":1.5,"flow_temperature":1.5,"flow_temperature_setpoint":1.5,"gas_valve_active":true,"heating_switch_active":true,"modulation_setpoint":1.5,"primary_circuit_flow_rate":1.5,"return_temperature_c":1.5,"storage_loading_pump_level":1.5,"water_pressure":1.5}}`
	if string(data) != wantData {
		t.Fatalf("populated boiler data mismatch\nwant: %s\ngot:  %s", wantData, data)
	}
	hash, _ := env["meta"].(map[string]any)["data_hash"].(string)
	const wantHash = "3f184f5355ce91377b805812bb3f975a2da765586b20f37769cc8fc07bf35010"
	if hash != wantHash {
		t.Fatalf("populated boiler data_hash = %q; want %q", hash, wantHash)
	}
}
