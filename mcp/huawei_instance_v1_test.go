package mcp

import (
	"context"
	"encoding/json"
	"os"
	"reflect"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
)

type h10InstanceFixture struct {
	*modbusV1FixtureProvider
	calls         int
	family, label string
}

func (p *h10InstanceFixture) HuaweiInstanceStatus(_ context.Context, family, label string) (HuaweiInstanceStatus, error) {
	p.calls++
	p.family, p.label = family, label
	if family == "huawei.emma" {
		return HuaweiInstanceStatus{Instance: label, Family: family, Disposition: "NO_ADMITTED_CHILD_PROFILE", Children: []HuaweiInstanceChildStatus{}, PublishedAssetIDs: []string{}, Loss: []string{}}, nil
	}
	childFamily, profile := "SUN2000 M3 series", "huawei.sun2000.m3.monitoring-readonly@1.0.0"
	if family == "huawei.sdongle" {
		childFamily, profile = "SUN2000 M1 series", "huawei.sun2000.m1.monitoring-readonly@1.0.0"
	}
	asset := "pv-asset-demo-" + label
	return HuaweiInstanceStatus{Instance: label, Family: family, Disposition: "COMPLETE", Qualified: true, Children: []HuaweiInstanceChildStatus{{Unit: 1, Family: childFamily, Profile: profile, Evidence: "CURRENT", Reason: "read", AssetID: asset}}, PublishedAssetIDs: []string{asset}, Loss: []string{"BATTERY_NOT_ADMITTED", "METER_NOT_ADMITTED"}}, nil
}
func TestH10NativeInstanceSchemaAndEnvelope(t *testing.T) {
	s, err := NewServer(&testRegistry{entries: map[byte]registry.DeviceEntry{}}, &testInvoker{})
	if err != nil {
		t.Fatal(err)
	}
	provider := &h10InstanceFixture{modbusV1FixtureProvider: &modbusV1FixtureProvider{}}
	RegisterModbusV1Tools(s, provider)
	for _, test := range []struct{ name, family string }{{HuaweiSmartLoggerV1StatusGetTool, "huawei.smartlogger"}, {HuaweiSDongleV1StatusGetTool, "huawei.sdongle"}, {HuaweiEMMAV1StatusGetTool, "huawei.emma"}} {
		var schema map[string]any
		for _, tool := range s.tools {
			if tool.Name == test.name {
				schema = tool.InputSchema
			}
		}
		h10Golden(t, "huawei_instance_schema.golden.json", schema)
		for _, label := range []string{"alpha", "beta"} {
			result := msp06Call(t, s.Handler(), test.name, map[string]any{"instance": label})
			if result.isError || provider.family != test.family || provider.label != label {
				t.Fatal("instance routing mismatch")
			}
			data := msp06Map(t, result.envelope["data"], "data")
			if result.envelope["meta"] == nil || result.envelope["error"] != nil {
				t.Fatal("envelope lost")
			}
			if label == "alpha" {
				name := map[string]string{"huawei.emma": "emma", "huawei.smartlogger": "smartlogger", "huawei.sdongle": "sdongle"}[test.family]
				h10Golden(t, "huawei_"+name+"_instance_status.golden.json", data)
			}
			replay := msp06Call(t, s.Handler(), test.name, map[string]any{"instance": label})
			if !reflect.DeepEqual(result.envelope["meta"], replay.envelope["meta"]) {
				t.Fatal("identical status changed snapshot hash or metadata")
			}
		}
		for _, args := range []map[string]any{{}, {"instance": ""}, {"instance": 1}, {"instance": "alpha", "extra": true}} {
			calls := provider.calls
			if result := msp06Call(t, s.Handler(), test.name, args); !result.isError || provider.calls != calls {
				t.Fatal("invalid arguments reached provider")
			}
		}
	}
}
func h10Golden(t *testing.T, name string, value any) {
	t.Helper()
	data, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatal(err)
	}
	var want, got any
	if err := json.Unmarshal(data, &want); err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(encoded, &got); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("golden %s differs: %s", name, encoded)
	}
}

type h10EMMAIdentityFixture struct{ *h10InstanceFixture }

func (p *h10EMMAIdentityFixture) HuaweiEMMAV1(context.Context) (HuaweiEMMAV1Result, error) {
	return HuaweiEMMAV1Result{Profile: HuaweiEMMAV1Profile}, nil
}

func TestH10EMMAIdentityToolSurvivesInstanceStatus(t *testing.T) {
	s, err := NewServer(&testRegistry{entries: map[byte]registry.DeviceEntry{}}, &testInvoker{})
	if err != nil {
		t.Fatal(err)
	}
	RegisterModbusV1Tools(s, &h10EMMAIdentityFixture{&h10InstanceFixture{modbusV1FixtureProvider: &modbusV1FixtureProvider{}}})
	found := map[string]bool{}
	for _, tool := range s.tools {
		found[tool.Name] = true
	}
	if !found[HuaweiEMMAV1StatusGetTool] || !found[HuaweiEMMAV1IdentityGetTool] {
		t.Fatalf("status and identity tools must both register: %v", found)
	}
}

type h10CorelessInstanceFixture struct{ *h10InstanceFixture }

func (h10CorelessInstanceFixture) ModbusV1CoreAvailable() bool { return false }

func TestH10HuaweiInstanceToolsRegisterWithoutSunSpecCore(t *testing.T) {
	s, err := NewServer(&testRegistry{entries: map[byte]registry.DeviceEntry{}}, &testInvoker{})
	if err != nil {
		t.Fatal(err)
	}
	RegisterModbusV1Tools(s, h10CorelessInstanceFixture{&h10InstanceFixture{modbusV1FixtureProvider: &modbusV1FixtureProvider{}}})
	have := map[string]bool{}
	for _, tool := range s.tools {
		have[tool.Name] = true
	}
	for _, name := range []string{HuaweiSmartLoggerV1StatusGetTool, HuaweiSDongleV1StatusGetTool, HuaweiEMMAV1StatusGetTool} {
		if !have[name] {
			t.Fatalf("%s not registered without SunSpec core", name)
		}
	}
	if have[ModbusV1RawReadTool] {
		t.Fatal("SunSpec tool registered without core")
	}
}
