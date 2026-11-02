package graphql

import (
	"context"
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/vaillant/systemconfig"
	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
	graphqlgo "github.com/graphql-go/graphql"
)

func TestM13InjectedSystemConfigBound(t *testing.T) {
	provider := NewLiveSemanticProvider()
	builder := NewBuilder(registry.NewDeviceRegistry(nil), nil)
	builder.SetSemanticProvider(provider)
	schema, err := NewSchema(builder, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	query := `{ system { config { dhwBivalencePointBound { minC maxC source reason profile narrowedOnly } dhw_bivalence_point_bound { min_c max_c source reason profile narrowed_only } } } }`
	for _, bound := range []*ConfigBound{
		nil,
		{MinC: -7, MaxC: 13, Source: "synthetic", Reason: "fixture", Profile: "family-a", NarrowedOnly: true},
		{MinC: -3, MaxC: 9, Source: "synthetic-other", Reason: "other", Profile: "family-b", NarrowedOnly: false},
	} {
		provider.SetSystem(&SystemStatus{Config: SystemConfig{DhwBivalenceBound: bound}})
		result := graphqlgo.Do(graphqlgo.Params{Schema: schema, RequestString: query})
		if len(result.Errors) != 0 {
			t.Fatal(result.Errors)
		}
		config := result.Data.(map[string]interface{})["system"].(map[string]interface{})["config"].(map[string]interface{})
		for _, alias := range []string{"dhwBivalencePointBound", "dhw_bivalence_point_bound"} {
			if bound == nil {
				if config[alias] != nil {
					t.Fatalf("absent bound: %v", config)
				}
				continue
			}
			min, max, narrowed := "minC", "maxC", "narrowedOnly"
			if alias == "dhw_bivalence_point_bound" {
				min, max, narrowed = "min_c", "max_c", "narrowed_only"
			}
			want := map[string]interface{}{min: bound.MinC, max: bound.MaxC, narrowed: bound.NarrowedOnly, "source": bound.Source, "reason": bound.Reason, "profile": bound.Profile}
			if !reflect.DeepEqual(config[alias], want) {
				t.Fatalf("%s = %#v; want %#v", alias, config[alias], want)
			}
		}
		supplied := builder.systemConfigBound()
		if !reflect.DeepEqual(supplied, bound) {
			t.Fatalf("mutation bound = %v; want %v", supplied, bound)
		}
		if supplied != nil {
			supplied.MaxC++
			if builder.systemConfigBound().MaxC != bound.MaxC {
				t.Fatal("provider bound was aliased")
			}
		}
	}
}

func TestM13SystemConfigBoundMutationAdmission(t *testing.T) {
	reg := mutationTestRegistry{entries: map[byte]registry.DeviceEntry{0x15: testControllerEntryWithMethods(mutationSetExtRegisterMethod, mutationGetExtRegisterMethod)}, order: []byte{0x15}}
	for _, bound := range []*ConfigBound{nil, {MinC: -2, MaxC: 4, Source: ConfigBoundSourceControllerProfile, Profile: "family", NarrowedOnly: true, ControllerAddress: 0x15, ControllerRegistryIdentity: systemconfig.CaptureBoundRegistryIdentity(reg, 0x15)}, {MinC: 5, MaxC: -5}} {
		invoker := &mutationTestInvoker{}
		result := ApplyConfigMutation(context.Background(), reg, invoker, 0x7F, true, SystemConfigFieldDHWBivalencePoint, "5", systemConfigFieldSpecs, bound)
		if result.Success || len(invoker.calls) != 0 {
			t.Fatalf("bound %v: result=%+v calls=%d", bound, result, len(invoker.calls))
		}
	}
	provider := NewLiveSemanticProvider()
	provider.SetSystem(&SystemStatus{Config: SystemConfig{DhwBivalenceBound: &ConfigBound{MinC: -2, MaxC: 4, Source: ConfigBoundSourceControllerProfile, Profile: "family", NarrowedOnly: true, ControllerAddress: 0x15, ControllerRegistryIdentity: systemconfig.CaptureBoundRegistryIdentity(reg, 0x15)}}})
	builder := NewBuilder(nil, nil)
	builder.SetSemanticProvider(provider)
	invoker := &mutationTestInvoker{}
	builder.SetAdmittedMutationSource(0x7F)
	schema, err := NewSchema(builder, reg, invoker, nil)
	if err != nil {
		t.Fatal(err)
	}
	data := executeMutation(t, schema, `mutation { setSystemConfig(field: "dhwBivalencePointC", value: "5") { success error } }`)
	result := data["setSystemConfig"].(map[string]interface{})
	if result["success"] != false || !strings.Contains(result["error"].(string), "[-2, 4]") || len(invoker.calls) != 0 {
		t.Fatalf("result=%v calls=%d", data, len(invoker.calls))
	}
	provider.SetSystem(&SystemStatus{})
	missing := executeMutation(t, schema, `mutation { setSystemConfig(field: "dhwBivalencePointC", value: "0") { success error } }`)["setSystemConfig"].(map[string]interface{})
	if missing["success"] != false || !strings.Contains(missing["error"].(string), "missing resolved driver bound") || len(invoker.calls) != 0 {
		t.Fatalf("missing bound: %v calls=%d", missing, len(invoker.calls))
	}
	provider.SetSystem(&SystemStatus{Config: SystemConfig{DhwBivalenceBound: &ConfigBound{MinC: -2, MaxC: 4, Source: ConfigBoundSourceControllerProfile, Profile: "family", NarrowedOnly: true, ControllerAddress: 0x15, ControllerRegistryIdentity: systemconfig.CaptureBoundRegistryIdentity(reg, 0x15)}}})
	invoker = &mutationTestInvoker{}
	_ = ApplyConfigMutation(context.Background(), reg, invoker, 0x7F, true, SystemConfigFieldDHWBivalencePoint, "4", systemConfigFieldSpecs, builder.systemConfigBound())
	if len(invoker.calls) == 0 {
		t.Fatal("supplied in-bound value did not reach invoker")
	}
}

func buildMutationSchemaForM13(t *testing.T, reg InvokeRegistry, invoker Invoker, bound func() *ConfigBound) graphqlgo.Schema {
	t.Helper()
	schema, err := graphqlgo.NewSchema(graphqlgo.SchemaConfig{
		Query:    graphqlgo.NewObject(graphqlgo.ObjectConfig{Name: "Query", Fields: graphqlgo.Fields{"ok": &graphqlgo.Field{Type: graphqlgo.Boolean}}}),
		Mutation: buildMutationType(reg, invoker, nil, nil, nil, func() (byte, bool) { return 0x7F, true }, bound),
	})
	if err != nil {
		t.Fatal(err)
	}
	return schema
}

func TestM13ValueTypeMapping(t *testing.T) {
	for _, pair := range []struct {
		native      systemconfig.ValueType
		application configValueType
	}{
		{systemconfig.ValueFloat32, configValueFloat32}, {systemconfig.ValueUint16, configValueUint16},
		{systemconfig.ValueBoolU8, configValueBoolU8}, {systemconfig.ValueEnumU16, configValueEnumU16},
		{systemconfig.ValueCString, configValueCString}, {systemconfig.ValueDateHDA3, configValueDateHDA3},
	} {
		native := systemconfig.ConfigFieldSpec{RegisterGroup: 2, RegisterAddr: 17, ValueType: pair.native, Min: -3, Max: 8, MaxLen: 12, Enum: map[string]uint16{"fixture": 7}}
		app := adaptSystemConfigSpec(native)
		if app.conversionErr != nil || app.valueType != pair.application {
			t.Fatalf("mapping %v: %+v", pair.native, app)
		}
		got, err := nativeConfigSpec(app)
		if err != nil || !reflect.DeepEqual(got, native) {
			t.Fatalf("round trip %v: %+v, %v", pair.native, got, err)
		}
	}
	bad := adaptSystemConfigSpec(systemconfig.ConfigFieldSpec{ValueType: systemconfig.ValueType(-1)})
	specs := map[string]configFieldSpec{"bad": bad, "good": adaptSystemConfigSpec(systemconfig.ConfigFieldSpec{ValueType: systemconfig.ValueBoolU8})}
	if _, err := resolveConfigFieldSpec("system", "bad", specs); err == nil {
		t.Fatal("unknown native type acquired a spec")
	}
	if _, err := resolveConfigFieldSpec("system", "good", specs); err != nil {
		t.Fatal(err)
	}
	for _, spec := range []configFieldSpec{bad, {valueType: configValueType(-1)}} {
		if _, err := nativeConfigSpec(spec); err == nil {
			t.Fatal("unknown type converted")
		}
		if payload, err := encodeConfigValue(spec, "0"); err == nil || payload != nil {
			t.Fatal("unknown type encoded")
		}
		if err := confirmDecodableReadback(spec, []byte{0}); err == nil {
			t.Fatal("unknown type decoded")
		}
		if configReadbackMatchesWrite(spec, []byte{0}, []byte{0}) {
			t.Fatal("unknown type compared")
		}
		invoker := &mutationTestInvoker{}
		_ = ApplyConfigMutation(context.Background(), mutationTestRegistry{}, invoker, 0x7F, true, "bad", "0", map[string]configFieldSpec{"bad": spec})
		if len(invoker.calls) != 0 {
			t.Fatal("unknown type invoked")
		}
	}
}

func TestInjectedBoundRefusesUnqualifiedOrWidenedAuthority(t *testing.T) {
	reg := mutationTestRegistry{entries: map[byte]registry.DeviceEntry{0x15: testControllerEntryWithMethods(mutationSetExtRegisterMethod, mutationGetExtRegisterMethod)}}
	valid := ConfigBound{ControllerAddress: 0x15, ControllerRegistryIdentity: systemconfig.CaptureBoundRegistryIdentity(reg, 0x15), MinC: -2, MaxC: 4, Source: ConfigBoundSourceControllerProfile, Profile: "family", NarrowedOnly: true}
	for _, tc := range []struct {
		name   string
		change func(*ConfigBound)
		value  string
	}{
		{"non-finite minimum", func(b *ConfigBound) { b.MinC = math.NaN() }, "0"},
		{"non-finite maximum", func(b *ConfigBound) { b.MaxC = math.Inf(1) }, "0"},
		{"upper endpoint", func(b *ConfigBound) { b.MaxC = 100 }, "75"},
		{"lower endpoint", func(b *ConfigBound) { b.MinC = -30 }, "-25"},
		{"wide bound with in-policy value", func(b *ConfigBound) { b.MaxC = 100 }, "0"},
		{"unknown source", func(b *ConfigBound) { b.Source = "synthetic" }, "0"},
		{"unqualified profile", func(b *ConfigBound) { b.NarrowedOnly = false }, "0"},
		{"missing profile", func(b *ConfigBound) { b.Profile = "" }, "0"},
		{"missing registry identity", func(b *ConfigBound) { b.ControllerRegistryIdentity = systemconfig.BoundRegistryIdentity{} }, "0"},
		{"missing target", func(b *ConfigBound) { b.ControllerAddress = 0 }, "0"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bound := valid
			tc.change(&bound)
			invoker := &mutationTestInvoker{}
			result := ApplyConfigMutation(context.Background(), reg, invoker, 0x7F, true, SystemConfigFieldDHWBivalencePoint, tc.value, systemConfigFieldSpecs, &bound)
			if result.Success || len(invoker.calls) != 0 {
				t.Fatalf("result=%+v calls=%d", result, len(invoker.calls))
			}
		})
	}
}

func TestInjectedBoundRefusesDifferentRoutedController(t *testing.T) {
	reg := mutationTestRegistry{entries: map[byte]registry.DeviceEntry{
		0x15: testControllerEntryWithMethods(mutationSetExtRegisterMethod, mutationGetExtRegisterMethod),
		0x35: testControllerEntryWithMethods(mutationSetExtRegisterMethod, mutationGetExtRegisterMethod),
	}}
	provider := NewLiveSemanticProvider()
	provider.SetSystem(&SystemStatus{Config: SystemConfig{DhwBivalenceBound: &ConfigBound{
		ControllerAddress: 0x35, MinC: -20, MaxC: 50, Source: ConfigBoundSourceControllerProfile, Profile: "other-family", NarrowedOnly: true,
	}}})
	builder := NewBuilder(nil, nil)
	builder.SetSemanticProvider(provider)
	builder.SetAdmittedMutationSource(0x7F)
	invoker := &mutationTestInvoker{}
	schema, err := NewSchema(builder, reg, invoker, nil)
	if err != nil {
		t.Fatal(err)
	}
	result := executeMutation(t, schema, `mutation { setSystemConfig(field: "dhwBivalencePointC", value: "40") { success error } }`)["setSystemConfig"].(map[string]interface{})
	if result["success"] != false || !strings.Contains(result["error"].(string), "does not match routed controller") || len(invoker.calls) != 0 {
		t.Fatalf("result=%v calls=%d", result, len(invoker.calls))
	}
}

// These fixtures expose the same snapshot/generation contract as the registry.
func (reg mutationTestRegistry) LookupEntrySnapshot(address byte) (registry.DeviceEntrySnapshot, bool) {
	entry, ok := reg.Lookup(address)
	if !ok || entry == nil {
		return registry.DeviceEntrySnapshot{}, false
	}
	return registry.DeviceEntrySnapshot{Manufacturer: entry.Manufacturer(), DeviceID: entry.DeviceID(),
		SoftwareVersion: entry.SoftwareVersion(), HardwareVersion: entry.HardwareVersion(),
		SerialNumber: entry.SerialNumber(), MacAddress: entry.MacAddress()}, true
}

func (reg mutationTestRegistry) WithObservationGeneration(fn func(uint64)) bool { fn(0); return true }

func TestInjectedBoundRefusesSameAddressIdentityReplacement(t *testing.T) {
	reg := mutationTestRegistry{entries: map[byte]registry.DeviceEntry{0x15: testControllerEntryWithMethods(mutationSetExtRegisterMethod, mutationGetExtRegisterMethod)}}
	bound := ConfigBound{ControllerAddress: 0x15, ControllerRegistryIdentity: systemconfig.CaptureBoundRegistryIdentity(reg, 0x15), MinC: -20, MaxC: 50, Source: ConfigBoundSourceUnmatchedPolicy}
	provider := NewLiveSemanticProvider()
	provider.SetSystem(&SystemStatus{Config: SystemConfig{DhwBivalenceBound: &bound}})
	replacement := reg.entries[0x15].(mockEntry)
	replacement.info.DeviceID = "BASV0"
	replacement.info.SoftwareVersion = "0217"
	replacement.info.HardwareVersion = "8803"
	reg.entries[0x15] = replacement
	invoker := &mutationTestInvoker{}
	schema := buildMutationSchemaForM13(t, reg, invoker, func() *ConfigBound { return provider.System().Config.DhwBivalenceBound })
	result := executeMutation(t, schema, `mutation { setSystemConfig(field: "dhwBivalencePointC", value: "40") { success error } }`)["setSystemConfig"].(map[string]interface{})
	if result["success"] != false || !strings.Contains(result["error"].(string), "identity or qualification changed") || len(invoker.calls) != 0 {
		t.Fatalf("result=%v calls=%d", result, len(invoker.calls))
	}
}

type boundGenerationTestRegistry struct {
	mutationTestRegistry
	generation uint64
}

func (reg *boundGenerationTestRegistry) WithObservationGeneration(fn func(uint64)) bool {
	fn(reg.generation)
	return true
}

func TestInjectedBoundRefusesRegistryQualificationChange(t *testing.T) {
	reg := &boundGenerationTestRegistry{mutationTestRegistry: mutationTestRegistry{entries: map[byte]registry.DeviceEntry{0x15: testControllerEntryWithMethods(mutationSetExtRegisterMethod, mutationGetExtRegisterMethod)}}}
	bound := ConfigBound{ControllerAddress: 0x15, MinC: -20, MaxC: 50, Source: ConfigBoundSourceUnmatchedPolicy,
		ControllerRegistryIdentity: systemconfig.CaptureBoundRegistryIdentity(reg, 0x15)}
	invoker := &mutationTestInvoker{}
	_ = ApplyConfigMutation(context.Background(), reg, invoker, 0x7F, true, SystemConfigFieldDHWBivalencePoint, "40", systemConfigFieldSpecs, &bound)
	if len(invoker.calls) == 0 {
		t.Fatal("current provenance did not reach invoker")
	}
	reg.generation++
	invoker = &mutationTestInvoker{}
	result := ApplyConfigMutation(context.Background(), reg, invoker, 0x7F, true, SystemConfigFieldDHWBivalencePoint, "40", systemConfigFieldSpecs, &bound)
	if result.Success || !strings.Contains(result.Error, "identity or qualification changed") || len(invoker.calls) != 0 {
		t.Fatalf("result=%+v calls=%d", result, len(invoker.calls))
	}
}
