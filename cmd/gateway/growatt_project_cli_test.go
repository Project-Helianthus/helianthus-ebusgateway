package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"

	canbus "github.com/Project-Helianthus/helianthus-canbus"
	canbusreg "github.com/Project-Helianthus/helianthus-canbusreg"
	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
)

func cx21Context(source uint8) growattProjectContext {
	c := growattProjectContext{Source: source, InterfaceIndex: 7, Destination: 244, Endpoint: "synthetic", Group: "group", Slot: "slot", Epoch: 1, SerialSlot: 1, SerialSlotValid: true, NativeSlot: 1, NativeGroup: 1, SlotValid: true, GroupValid: true, PublisherResolved: true, MeasurementValid: true, TemperatureValid: true, TopologyAssociated: true, FieldAge: "0s"}
	c.Cells.Count, c.Cells.CountValid = 8, true
	c.Sensors.Count, c.Sensors.CountValid = 4, true
	for i := range c.Cells.Gates {
		c.Cells.Gates[i] = growattProjectGate{Configured: true, Valid: true, Age: "0s"}
		c.Sensors.Gates[i] = c.Cells.Gates[i]
	}
	return c
}
func cx21JSON(t *testing.T, v any) string {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}
func cx21Config(t *testing.T, link bool) ebusgateway.GrowattProjectCANConfig {
	t.Helper()
	c := ebusgateway.GrowattProjectCANConfig{Enabled: true, ListenOnlyConfirmed: true, Interface: "synthetic0", StaleAfter: time.Second, ProjectCode: "QAAA", Release: "0011", NativeProfile: "standard", Scope: "local", Bank: "ordinary", Context: cx21JSON(t, []growattProjectContext{cx21Context(1)})}
	if link {
		c.SourceSelectors = `[{"source":1,"use_default":true,"contract":"release11"}]`
	}
	return c
}
func cx21Admission(t *testing.T, c ebusgateway.GrowattProjectCANConfig, link bool) growattProjectAdmission {
	t.Helper()
	a, err := admitGrowattProject(c, link)
	if err != nil {
		t.Fatal(err)
	}
	return a
}
func cx21Flags(t *testing.T, c ebusgateway.GrowattProjectCANConfig, link bool) []string {
	t.Helper()
	prefix := "-growatt-bms-pcs-can-"
	if link {
		prefix = "-growatt-bms-link-can-"
	}
	result := []string{prefix + "enabled", prefix + "interface=" + c.Interface, prefix + "listen-only-confirmed", prefix + "stale-after=" + c.StaleAfter.String(), prefix + "project-code=" + c.ProjectCode, prefix + "release=" + c.Release, prefix + "native-profile=" + c.NativeProfile, prefix + "scope=" + c.Scope, prefix + "bank=" + c.Bank, prefix + "context=" + c.Context}
	if link {
		result = append(result, prefix+"source-selectors="+c.SourceSelectors)
	} else if c.NativeContract != "" {
		result = append(result, prefix+"native-contract="+c.NativeContract)
	}
	return result
}
func TestCX21FlagsDefaultsAndIndependentInstances(t *testing.T) {
	cfg := can03Parse(t)
	if cfg.GrowattPCSCANConfig != (ebusgateway.GrowattProjectCANConfig{}) || cfg.GrowattLINKCANConfig != (ebusgateway.GrowattProjectCANConfig{}) {
		t.Fatal("active disabled defaults")
	}
	pcs, link := cx21Config(t, false), cx21Config(t, true)
	link.Interface = "synthetic1"
	got := can03Parse(t, append(cx21Flags(t, pcs, false), cx21Flags(t, link, true)...)...)
	if got.GrowattPCSCANConfig != pcs || got.GrowattLINKCANConfig != link {
		t.Fatal("independent parse changed values")
	}
	other := can03Parse(t, cx21Flags(t, pcs, false)...)
	if other.GrowattLINKCANConfig != (ebusgateway.GrowattProjectCANConfig{}) || got.GrowattLINKCANConfig != link {
		t.Fatal("cross-instance contamination")
	}
	for _, c := range []struct {
		config ebusgateway.GrowattProjectCANConfig
		link   bool
	}{{pcs, false}, {link, true}} {
		cx21Admission(t, c.config, c.link)
	}
	var previous string
	for i := 0; i < 2; i++ {
		fs := flag.NewFlagSet("synthetic", flag.ContinueOnError)
		cfg := ebusgateway.DefaultConfig()
		bindFlags(fs, &cfg)
		var help bytes.Buffer
		fs.SetOutput(&help)
		fs.PrintDefaults()
		for _, family := range []string{"pcs", "link"} {
			for _, field := range []string{"enabled", "interface", "listen-only-confirmed", "stale-after", "project-code", "release", "native-profile", "scope", "bank", "context"} {
				if !strings.Contains(help.String(), "-growatt-bms-"+family+"-can-"+field) {
					t.Fatal(field)
				}
			}
		}
		if !strings.Contains(help.String(), `[{"source":1,"use_default":true,"contract":"release11"}]`) {
			t.Fatal("selector escaping")
		}
		if i > 0 && help.String() != previous {
			t.Fatal("nondeterministic help")
		}
		previous = help.String()
	}
}
func TestCX21RefusesEveryMissingFlag(t *testing.T) {
	for _, link := range []bool{false, true} {
		c := cx21Config(t, link)
		args := cx21Flags(t, c, link)
		for i, arg := range args {
			if strings.Contains(arg, "enabled") {
				continue
			}
			t.Run(fmt.Sprintf("link=%v/%s", link, strings.SplitN(arg, "=", 2)[0]), func(t *testing.T) {
				// LINK family defaults are required only for explicit opt-in entries.
				cfg := can03Parse(t, append(append([]string{}, args[:i]...), args[i+1:]...)...)
				candidate := cfg.GrowattPCSCANConfig
				if link {
					candidate = cfg.GrowattLINKCANConfig
				}
				if _, err := admitGrowattProject(candidate, link); err == nil {
					t.Fatal("missing input admitted")
				}
			})
		}
	}
}
func TestCX21MissingContextAssertions(t *testing.T) {
	for _, link := range []bool{false, true} {
		c := cx21Config(t, link)
		var contexts []map[string]any
		if err := json.Unmarshal([]byte(c.Context), &contexts); err != nil {
			t.Fatal(err)
		}
		for key := range contexts[0] {
			t.Run(fmt.Sprintf("link=%v/%s", link, key), func(t *testing.T) {
				var values []map[string]any
				_ = json.Unmarshal([]byte(c.Context), &values)
				delete(values[0], key)
				candidate := c
				candidate.Context = cx21JSON(t, values)
				_, err := admitGrowattProject(candidate, link)
				if err == nil || !strings.Contains(err.Error(), key) {
					t.Fatalf("missing %s: %v", key, err)
				}
			})
		}
	}
	for _, key := range []string{"count", "count_valid", "gates"} {
		t.Run("nested/"+key, func(t *testing.T) {
			c := cx21Config(t, true)
			var values []map[string]any
			_ = json.Unmarshal([]byte(c.Context), &values)
			delete(values[0]["cells"].(map[string]any), key)
			c.Context = cx21JSON(t, values)
			if _, err := admitGrowattProject(c, true); err == nil {
				t.Fatal("missing nested assertion")
			}
		})
	}
	for _, key := range []string{"configured", "valid", "status", "age"} {
		t.Run("gate/"+key, func(t *testing.T) {
			c := cx21Config(t, true)
			var values []map[string]any
			_ = json.Unmarshal([]byte(c.Context), &values)
			delete(values[0]["cells"].(map[string]any)["gates"].([]any)[0].(map[string]any), key)
			c.Context = cx21JSON(t, values)
			if _, err := admitGrowattProject(c, true); err == nil {
				t.Fatal("missing gate assertion")
			}
		})
	}
}
func TestCX21InvalidAndDisabledConfigs(t *testing.T) {
	mutations := map[string]func(*ebusgateway.GrowattProjectCANConfig){
		"confirmation": func(c *ebusgateway.GrowattProjectCANConfig) { c.ListenOnlyConfirmed = false },
		"zero stale":   func(c *ebusgateway.GrowattProjectCANConfig) { c.StaleAfter = 0 }, "negative stale": func(c *ebusgateway.GrowattProjectCANConfig) { c.StaleAfter = -1 },
		"project case": func(c *ebusgateway.GrowattProjectCANConfig) { c.ProjectCode = "qaaa" }, "release": func(c *ebusgateway.GrowattProjectCANConfig) { c.Release = "9999" },
		"profile case": func(c *ebusgateway.GrowattProjectCANConfig) { c.NativeProfile = "Standard" }, "scope case": func(c *ebusgateway.GrowattProjectCANConfig) { c.Scope = "Local" },
		"bank case": func(c *ebusgateway.GrowattProjectCANConfig) { c.Bank = "Ordinary" }, "bank mismatch": func(c *ebusgateway.GrowattProjectCANConfig) { c.Bank = "parallel" },
		"context trailing": func(c *ebusgateway.GrowattProjectCANConfig) { c.Context += " []" }, "context unknown": func(c *ebusgateway.GrowattProjectCANConfig) {
			c.Context = strings.Replace(c.Context, `"source":1`, `"source":1,"unknown":true`, 1)
		},
		"context duplicate": func(c *ebusgateway.GrowattProjectCANConfig) {
			c.Context = strings.Replace(c.Context, `"source":1`, `"source":1,"source":1`, 1)
		},
		"context null": func(c *ebusgateway.GrowattProjectCANConfig) {
			c.Context = strings.Replace(c.Context, `"epoch":1`, `"epoch":null`, 1)
		},
	}
	for _, link := range []bool{false, true} {
		for name, mutate := range mutations {
			t.Run(fmt.Sprintf("link=%v/%s", link, name), func(t *testing.T) {
				c := cx21Config(t, link)
				mutate(&c)
				if _, err := admitGrowattProject(c, link); err == nil {
					t.Fatal("invalid config admitted")
				}
			})
		}
		c := cx21Config(t, link)
		c.StaleAfter = time.Nanosecond
		cx21Admission(t, c, link)
		valid := reflect.ValueOf(cx21Config(t, link))
		typ := valid.Type()
		for i := 0; i < valid.NumField(); i++ {
			if typ.Field(i).Name == "Enabled" || valid.Field(i).IsZero() {
				continue
			}
			t.Run(fmt.Sprintf("disabled=%v/%s", link, typ.Field(i).Name), func(t *testing.T) {
				var c ebusgateway.GrowattProjectCANConfig
				reflect.ValueOf(&c).Elem().Field(i).Set(valid.Field(i))
				if _, err := admitGrowattProject(c, link); err == nil {
					t.Fatal("disabled active field admitted")
				}
			})
		}
	}
	for _, raw := range []string{"invalid", "1"} {
		cfg := ebusgateway.DefaultConfig()
		fs := flag.NewFlagSet("synthetic", flag.ContinueOnError)
		fs.SetOutput(io.Discard)
		bindFlags(fs, &cfg)
		if fs.Parse([]string{"-growatt-bms-pcs-can-stale-after=" + raw}) == nil {
			t.Fatal("invalid duration parse")
		}
	}
}
func TestCX21ProfilesRoutesAndAmbiguity(t *testing.T) {
	for _, profile := range []string{"standard", "extended19", "parallel"} {
		c := cx21Config(t, false)
		c.NativeProfile = profile
		if profile == "extended19" {
			c.Release = "0010"
		}
		if profile == "parallel" {
			c.Bank = "parallel"
		}
		cx21Admission(t, c, false)
	}
	for _, destination := range []int{243, 244, 245, 246} {
		t.Run(fmt.Sprint(destination), func(t *testing.T) {
			c := cx21Config(t, true)
			context := cx21Context(1)
			context.Destination = uint8(destination)
			c.Context = cx21JSON(t, []growattProjectContext{context})
			_, err := admitGrowattProject(c, true)
			if (err == nil) != (destination == 244 || destination == 245) {
				t.Fatal(err)
			}
		})
	}
	c := cx21Config(t, true)
	c.Scope = "aggregate"
	if _, err := admitGrowattProject(c, true); err == nil {
		t.Fatal("LINK aggregate")
	}
	for _, contract := range []string{"", "release11", "legacy19", "Release11"} {
		c := cx21Config(t, false)
		c.ProjectCode = "QAAa"
		c.Release = "9011"
		c.NativeContract = contract
		_, err := admitGrowattProject(c, false)
		if (err == nil) != (contract == "release11" || contract == "legacy19") {
			t.Fatalf("contract %q: %v", contract, err)
		}
	}
}
func TestCX21LINKSelectorsStrictAndHeterogeneous(t *testing.T) {
	c := cx21Config(t, true)
	c.ProjectCode = ""
	c.Release = ""
	c.SourceSelectors = `[{"source":1,"project_code":"QAAA","release":"0005","contract":"legacy19"},{"source":2,"project_code":"QAAa","release":"9011","contract":"release11"}]`
	c.Context = cx21JSON(t, []growattProjectContext{cx21Context(1), cx21Context(2)})
	a := cx21Admission(t, c, true)
	if len(a.selections) != 2 || a.selections[0].CX01Spec().Release != "0005" || a.selections[1].CX01Spec().ProjectCode != "QAAa" {
		t.Fatal("heterogeneous map lost")
	}
	selected, err := a.link.Selections()
	if err != nil || !reflect.DeepEqual(selected, a.selections) {
		t.Fatal("factory differs from preflight", err)
	}
	bad := []string{`[]`, `null`, `[`, c.SourceSelectors + ` []`, `[{"source":1,"source":2,"use_default":true,"contract":"release11"}]`, `[{"source":1.0,"use_default":true,"contract":"release11"}]`, `[{"source":1,"use_default":false,"contract":"release11"}]`, `[{"source":1,"use_default":true,"project_code":"QAAA","contract":"release11"}]`, `[{"source":1,"use_default":true,"contract":"Release11"}]`, `[{"source":1,"use_default":true,"contract":"release11","extra":0}]`, `[{"source":1,"use_default":true,"contract":"release11"},{"source":1,"use_default":true,"contract":"release11"}]`}
	for i, value := range bad {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			c := cx21Config(t, true)
			c.SourceSelectors = value
			if _, err := admitGrowattProject(c, true); err == nil {
				t.Fatal("invalid selector admitted")
			}
		})
	}
	for _, source := range []int{0, 1, 243, 244, 245, 246, 254, 255, 256} {
		t.Run("source/"+fmt.Sprint(source), func(t *testing.T) {
			value := fmt.Sprintf(`[{"source":%d,"use_default":true,"contract":"release11"}]`, source)
			_, err := ebusgateway.ParseGrowattLINKSourceSelectors(value, "QAAA", "0011")
			want := source >= 1 && source <= 254 && source != 244 && source != 245
			if (err == nil) != want {
				t.Fatal(err)
			}
		})
	}
	for _, count := range []int{32, 33} {
		entries := []map[string]any{}
		for i := 1; i <= count; i++ {
			entries = append(entries, map[string]any{"source": i, "use_default": true, "contract": "release11"})
		}
		_, err := ebusgateway.ParseGrowattLINKSourceSelectors(cx21JSON(t, entries), "QAAA", "0011")
		if (err == nil) != (count == 32) {
			t.Fatal("selector bound", err)
		}
	}
	c = cx21Config(t, true)
	c.SourceSelectors = `[{"source":1,"use_default":true,"contract":"release11"}]`
	c.ProjectCode = ""
	if _, err := admitGrowattProject(c, true); err == nil {
		t.Fatal("missing opt-in default")
	}
}
func TestCX21AssociationKeyEveryField(t *testing.T) {
	// Each mutation must withhold association; the complete native selection,
	// including interface name/index and route, is the map key.
	mutations := map[string]func(*canbusreg.CX01SelectionSpec){
		"interface name": func(n *canbusreg.CX01SelectionSpec) {
			n.Interface, _ = canbus.NewInterfaceIdentity("synthetic2", n.Interface.Index())
		}, "interface index": func(n *canbusreg.CX01SelectionSpec) {
			n.Interface, _ = canbus.NewInterfaceIdentity(n.Interface.Name(), 8)
		},
		"flavor": func(n *canbusreg.CX01SelectionSpec) { n.Flavor = "pcs-parallel" }, "PCSFlavor": func(n *canbusreg.CX01SelectionSpec) { n.PCSFlavor = "pcs-parallel" }, "project": func(n *canbusreg.CX01SelectionSpec) { n.ProjectCode = "QAAa" }, "release": func(n *canbusreg.CX01SelectionSpec) { n.Release = "0005" }, "contract": func(n *canbusreg.CX01SelectionSpec) { n.Choice = "legacy19" }, "direction": func(n *canbusreg.CX01SelectionSpec) { n.Direction = "pcs-to-bms" }, "scope": func(n *canbusreg.CX01SelectionSpec) { n.Scope = "aggregate" }, "bank": func(n *canbusreg.CX01SelectionSpec) { n.Bank = "parallel" }, "endpoint": func(n *canbusreg.CX01SelectionSpec) { n.Endpoint = "other" }, "group": func(n *canbusreg.CX01SelectionSpec) { n.Group = "other" }, "slot": func(n *canbusreg.CX01SelectionSpec) { n.Slot = "other" }, "route": func(n *canbusreg.CX01SelectionSpec) { n.RouteSet = false }, "destination": func(n *canbusreg.CX01SelectionSpec) { n.Destination = 245 }, "source": func(n *canbusreg.CX01SelectionSpec) { n.Source = 2 }, "epoch": func(n *canbusreg.CX01SelectionSpec) { n.Epoch = 2 },
	}
	for _, link := range []bool{false, true} {
		a := cx21Admission(t, cx21Config(t, link), link)
		for name, mutate := range mutations {
			t.Run(fmt.Sprintf("link=%v/%s", link, name), func(t *testing.T) {
				n := a.selections[0].CX01Spec()
				mutate(&n)
				s, _ := canbusreg.NewCXSelection(n)
				if got := a.associate(s, canbusreg.Evidence{}); got.Selection != (canbusreg.CXSelection{}) {
					t.Fatal("foreign key acquired association")
				}
			})
		}
	}
}
