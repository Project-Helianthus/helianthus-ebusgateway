package mcp

import (
	"context"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
)

type smartLoggerFixture struct{ *modbusV1FixtureProvider }

func (*smartLoggerFixture) HuaweiSmartLoggerV1(context.Context) (HuaweiSmartLoggerV1Result, error) {
	return HuaweiSmartLoggerV1Result{Profile: HuaweiSmartLoggerV1Profile, Family: "SmartLogger", Qualified: false, RawRedacted: false, OutboundAllowed: true}, nil
}

func TestHuaweiSmartLoggerV1ToolPreservesNativeState(t *testing.T) {
	s, err := NewServer(&testRegistry{entries: map[byte]registry.DeviceEntry{}}, &testInvoker{})
	if err != nil {
		t.Fatal(err)
	}
	RegisterModbusV1Tools(s, &smartLoggerFixture{&modbusV1FixtureProvider{}})
	r := msp06Call(t, s.Handler(), HuaweiSmartLoggerV1StatusGetTool, map[string]any{})
	if r.isError {
		t.Fatal(r)
	}
	d := msp06Map(t, r.envelope["data"], "data")
	if d["profile"] != HuaweiSmartLoggerV1Profile || d["family"] != "SmartLogger" || d["raw_redacted"] != false || d["outbound_allowed"] != true {
		t.Fatalf("%#v", d)
	}
	for _, key := range []string{"mei", "inventory", "endpoint", "firmware", "child"} {
		if _, ok := d[key]; ok {
			t.Fatal(key)
		}
	}
}
