package mcp

import (
	"context"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
)

type sdongleFixture struct{ *modbusV1FixtureProvider }

func (*sdongleFixture) HuaweiSDongleV1(context.Context) (HuaweiSDongleV1Result, error) {
	return HuaweiSDongleV1Result{Profile: HuaweiSDongleV1Profile, Family: "S-DongleA-05", Disposition: "OBSERVED_UNQUALIFIED", Qualified: false, RawRedacted: false, OutboundAllowed: true}, nil
}

func TestHuaweiSDongleV1ToolPreservesNativeDisposition(t *testing.T) {
	s, err := NewServer(&testRegistry{entries: map[byte]registry.DeviceEntry{}}, &testInvoker{})
	if err != nil {
		t.Fatal(err)
	}
	RegisterModbusV1Tools(s, &sdongleFixture{&modbusV1FixtureProvider{}})
	r := msp06Call(t, s.Handler(), HuaweiSDongleV1StatusGetTool, map[string]any{})
	if r.isError {
		t.Fatal(r)
	}
	d := msp06Map(t, r.envelope["data"], "data")
	if d["profile"] != HuaweiSDongleV1Profile || d["family"] != "S-DongleA-05" || d["disposition"] != "OBSERVED_UNQUALIFIED" || d["qualified"] != false || d["raw_redacted"] != false || d["outbound_allowed"] != true {
		t.Fatalf("%#v", d)
	}
	for _, key := range []string{"timeout", "endpoint", "mei", "firmware", "child"} {
		if _, ok := d[key]; ok {
			t.Fatal(key)
		}
	}
}
