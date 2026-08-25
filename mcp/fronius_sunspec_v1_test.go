package mcp

import (
	"context"
	"math"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
	modbusreg "github.com/Project-Helianthus/helianthus-modbusreg"
)

type froniusSunSpecFixture struct{ *modbusV1FixtureProvider }

func (*froniusSunSpecFixture) FroniusSunSpecV1(context.Context) (FroniusSunSpecV1Result, error) {
	return FroniusSunSpecV1Result{
		Qualified: true,
		Telemetry: &FroniusSunSpecV1Telemetry{
			ACActivePowerWatts:       8421.5,
			ACFrequencyHertz:         50,
			LifetimeEnergyWattHours:  123456.75,
			OperatingState:           "MPPT",
		},
	}, nil
}

func TestFroniusSunSpecV1ToolProjectsOnlyQualifiedStandardTelemetry(t *testing.T) {
	s, err := NewServer(&testRegistry{entries: map[byte]registry.DeviceEntry{}}, &testInvoker{})
	if err != nil {
		t.Fatal(err)
	}
	RegisterModbusV1Tools(s, &froniusSunSpecFixture{&modbusV1FixtureProvider{}})
	r := msp06Call(t, s.Handler(), FroniusSunSpecV1StatusGetTool, map[string]any{})
	if r.isError {
		t.Fatal(r)
	}
	d := msp06Map(t, r.envelope["data"], "data")
	if d["profile"] != FroniusSunSpecV1Profile || d["capability"] != modbusreg.SunSpecThreePhaseMonitoringCapabilityID || d["qualification"] != "QUALIFIED" || d["qualified"] != true || d["raw_redacted"] != true || d["outbound_allowed"] != false {
		t.Fatalf("%#v", d)
	}
	telemetry := msp06Map(t, d["telemetry"], "telemetry")
	if telemetry["ac_active_power_watts"] != 8421.5 || telemetry["ac_frequency_hertz"] != 50.0 || telemetry["lifetime_energy_watt_hours"] != 123456.75 || telemetry["operating_state"] != "MPPT" {
		t.Fatalf("%#v", telemetry)
	}
	for _, key := range []string{"endpoint", "raw", "replay", "firmware", "configuration", "control"} {
		if _, ok := d[key]; ok {
			t.Fatal(key)
		}
	}
}

func TestFroniusSunSpecV1ToolFailsClosedForUnqualifiedOrInvalidTelemetry(t *testing.T) {
	for _, result := range []FroniusSunSpecV1Result{
		{Qualified: false, Telemetry: &FroniusSunSpecV1Telemetry{ACActivePowerWatts: 1}},
		{Qualified: true, Telemetry: &FroniusSunSpecV1Telemetry{ACActivePowerWatts: math.NaN()}},
	} {
		s, err := NewServer(&testRegistry{entries: map[byte]registry.DeviceEntry{}}, &testInvoker{})
		if err != nil {
			t.Fatal(err)
		}
		RegisterModbusV1Tools(s, froniusSunSpecResultFixture{modbusV1FixtureProvider: &modbusV1FixtureProvider{}, result: result})
		r := msp06Call(t, s.Handler(), FroniusSunSpecV1StatusGetTool, map[string]any{})
		if r.isError {
			t.Fatal(r)
		}
		d := msp06Map(t, r.envelope["data"], "data")
		if d["qualification"] != "NOT_QUALIFIED" || d["qualified"] != false {
			t.Fatalf("%#v", d)
		}
		if _, ok := d["telemetry"]; ok {
			t.Fatalf("telemetry must be absent: %#v", d)
		}
	}
}

type froniusSunSpecResultFixture struct {
	*modbusV1FixtureProvider
	result FroniusSunSpecV1Result
}

func (f froniusSunSpecResultFixture) FroniusSunSpecV1(context.Context) (FroniusSunSpecV1Result, error) {
	return f.result, nil
}
