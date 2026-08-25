package mcp

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
	modbusreg "github.com/Project-Helianthus/helianthus-modbusreg"
)

type growattBMSRS485V202Fixture struct{ *modbusV1FixtureProvider }

func (*growattBMSRS485V202Fixture) GrowattBMSRS485V202(context.Context) (modbusreg.GrowattBMSTypedReadOnlyStatus, error) {
	return growattBMSRS485V202FixtureStatus(), nil
}

func TestGrowattBMSRS485V202ToolProjectsOnlyQualifiedTypedStatus(t *testing.T) {
	s, err := NewServer(&testRegistry{entries: map[byte]registry.DeviceEntry{}}, &testInvoker{})
	if err != nil {
		t.Fatal(err)
	}
	RegisterModbusV1Tools(s, &growattBMSRS485V202Fixture{&modbusV1FixtureProvider{}})
	r := msp06Call(t, s.Handler(), GrowattBMSRS485V202StatusGetTool, map[string]any{})
	if r.isError {
		t.Fatal(r)
	}
	d := msp06Map(t, r.envelope["data"], "data")
	if d["profile"] != GrowattBMSRS485V202Profile || d["qualified"] != true || d["raw_redacted"] != true || d["outbound_allowed"] != false {
		t.Fatalf("%#v", d)
	}
	identity := msp06Map(t, d["identity"], "identity")
	status := msp06Map(t, d["status"], "status")
	telemetry := msp06Map(t, d["telemetry"], "telemetry")
	revision := msp06Map(t, d["revision"], "revision")
	if identity["mcu_software_version"] != "1.2" || identity["bms_company"] != json.Number("4") || status["operating_state"] != "charging" || status["soc_percent"] != json.Number("75") || telemetry["pack_voltage_volts"] != json.Number("52") || telemetry["pack_current_amps"] != json.Number("-1") || telemetry["cumulative_discharge_amp_hours"] != json.Number("0.6") {
		t.Fatalf("data=%#v", d)
	}
	if revision["family"] != "1xSxxP ESS" || revision["file_revision"] != "Rev2.01" || revision["header_version"] != "V2.0" || revision["cumulative_revision"] != "2.02" {
		t.Fatalf("revision=%#v", revision)
	}
	for _, key := range []string{"raw", "serial", "endpoint", "slice", "transport", "request", "control"} {
		if _, ok := d[key]; ok {
			t.Fatal(key)
		}
	}
}

func TestGrowattBMSRS485V202ToolRejectsNonExactRevision(t *testing.T) {
	s, err := NewServer(&testRegistry{entries: map[byte]registry.DeviceEntry{}}, &testInvoker{})
	if err != nil {
		t.Fatal(err)
	}
	status := growattBMSRS485V202FixtureStatus()
	status.Revision.HeaderVersion = "V2.1"
	RegisterModbusV1Tools(s, growattBMSRS485V202ResultFixture{modbusV1FixtureProvider: &modbusV1FixtureProvider{}, status: status})
	r := msp06Call(t, s.Handler(), GrowattBMSRS485V202StatusGetTool, map[string]any{})
	if !r.isError {
		t.Fatal(r)
	}
	if r.envelope["data"] != nil {
		t.Fatalf("data=%#v", r.envelope["data"])
	}
}

type growattBMSRS485V202ResultFixture struct {
	*modbusV1FixtureProvider
	status modbusreg.GrowattBMSTypedReadOnlyStatus
}

func (f growattBMSRS485V202ResultFixture) GrowattBMSRS485V202(context.Context) (modbusreg.GrowattBMSTypedReadOnlyStatus, error) {
	return f.status, nil
}

func growattBMSRS485V202FixtureStatus() modbusreg.GrowattBMSTypedReadOnlyStatus {
	return modbusreg.GrowattBMSTypedReadOnlyStatus{
		Revision:                    modbusreg.GrowattBMSRevisionTuple{Family: "1xSxxP ESS", FileRevision: "Rev2.01", HeaderVersion: "V2.0", CumulativeRevision: "2.02"},
		MCUSoftwareVersion:          "1.2",
		GaugeVersion:                "3.4",
		BMSCompany:                  4,
		BMSGeneration:               2,
		PackCompany:                 1,
		PackGeneration:              3,
		OperatingState:              modbusreg.GrowattBMSStateCharging,
		SOCPercent:                  75,
		PackVoltageVolts:            52,
		PackCurrentAmps:             -1,
		TemperatureCelsius:          25,
		RemainingCapacityAmpHours:   32,
		FullChargeCapacityAmpHours:  50,
		CycleCount:                  110,
		ContinuousChargeSeconds:     100,
		CurrentCycleChargeAmpHours:  12.3,
		AverageCellVoltageVolts:     3.3,
		FloatingPackVoltageVolts:    51.2,
		CumulativeChargeAmpHours:    0.5,
		CumulativeDischargeAmpHours: 0.6,
	}
}
