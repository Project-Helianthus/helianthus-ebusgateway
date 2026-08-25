package mcp

import (
	"context"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
)

type sdongleFixture struct{ *modbusV1FixtureProvider }

func (*sdongleFixture) HuaweiSDongleV1(context.Context) (HuaweiSDongleV1Result, error) {
	return HuaweiSDongleV1Result{Profile: HuaweiSDongleV1Profile, Family: "S-Dongle", Disposition: "PRE_LIVE_INSUFFICIENT_EVIDENCE"}, nil
}

func TestHuaweiSDongleV1ToolStaysPreLiveAndRedacted(t *testing.T) {
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
	if d["profile"] != HuaweiSDongleV1Profile || d["disposition"] != "PRE_LIVE_INSUFFICIENT_EVIDENCE" || d["qualified"] != false || d["raw_redacted"] != true || d["outbound_allowed"] != false {
		t.Fatalf("%#v", d)
	}
	for _, key := range []string{"timeout", "endpoint", "mei", "firmware", "child"} {
		if _, ok := d[key]; ok {
			t.Fatal(key)
		}
	}
}
