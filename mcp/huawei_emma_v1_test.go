package mcp

import (
	"context"
	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
	"testing"
)

type emmaFixture struct{ *modbusV1FixtureProvider }

func (*emmaFixture) HuaweiEMMAV1(context.Context) (HuaweiEMMAV1Result, error) {
	return HuaweiEMMAV1Result{Profile: HuaweiEMMAV1Profile, CanonicalClass: "EMMA", ModelVariant: "EMMA-A02", Qualified: true}, nil
}
func TestHuaweiEMMAV1ToolRedactsOfflineIdentity(t *testing.T) {
	s, e := NewServer(&testRegistry{entries: map[byte]registry.DeviceEntry{}}, &testInvoker{})
	if e != nil {
		t.Fatal(e)
	}
	RegisterModbusV1Tools(s, &emmaFixture{&modbusV1FixtureProvider{}})
	r := msp06Call(t, s.Handler(), HuaweiEMMAV1IdentityGetTool, map[string]any{})
	if r.isError {
		t.Fatal(r)
	}
	d := msp06Map(t, r.envelope["data"], "data")
	if d["qualified"] != true || d["raw_redacted"] != true || d["outbound_allowed"] != false {
		t.Fatalf("%#v", d)
	}
	for _, k := range []string{"mei", "inventory", "endpoint", "offering", "firmware"} {
		if _, ok := d[k]; ok {
			t.Fatal(k)
		}
	}
}
