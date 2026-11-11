package graphql

import (
	"context"
	"encoding/json"
	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
	graphqlgo "github.com/graphql-go/graphql"
	"os"
	"regexp"
	"testing"
)

func TestNamingContractSchema(t *testing.T) {
	schema, err := NewSchema(NewBuilder(nil, nil), mutationTestRegistry{}, &mutationTestInvoker{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	valid := regexp.MustCompile(`^[a-z][a-z0-9_]*$`)
	for name, typ := range schema.TypeMap() {
		if len(name) > 1 && name[:2] == "__" {
			continue
		}
		switch name {
		case "String", "Int", "Float", "Boolean", "ID":
			continue
		}
		if !valid.MatchString(name) {
			t.Errorf("type %s", name)
		}
		if obj, ok := typ.(*graphqlgo.Object); ok {
			for field, def := range obj.Fields() {
				if !valid.MatchString(field) {
					t.Errorf("field %s.%s", name, field)
				}
				for _, arg := range def.Args {
					if !valid.MatchString(arg.Name()) {
						t.Errorf("arg %s", arg.Name())
					}
				}
			}
		}
	}

	for _, old := range []string{"currentTempC", "current_temp_c", "currentHumidityPct", "current_humidity_pct"} {
		r := graphqlgo.Do(graphqlgo.Params{Schema: schema, RequestString: "{ zones { state { " + old + " } } }"})
		if len(r.Errors) == 0 {
			t.Errorf("old zone field accepted: %s", old)
		}
	}
	result := graphqlgo.Do(graphqlgo.Params{Schema: schema, RequestString: `{ __typename }`, Context: context.Background()})
	if len(result.Errors) > 0 {
		t.Fatal(result.Errors)
	}
	for _, old := range []string{"semanticSnapshot", "busSummary"} {
		r := graphqlgo.Do(graphqlgo.Params{Schema: schema, RequestString: "{ " + old + " { __typename } }"})
		if len(r.Errors) == 0 {
			t.Errorf("accepted old root %s", old)
		}
	}
}

func TestNamingContractCircuitIdentity(t *testing.T) {
	expected := map[string]uint16{"circuit_heating_curve_factor": 0x000F, "heating_circuit_flow_temperature_setpoint_maximum": 0x0010, "circuit_flow_temperature_setpoint_minimum": 0x0012, "heating_circuit_outdoor_temperature_limit_maximum": 0x0014, "circuit_room_temperature_influence_mode": 0x0015, "circuit_cooling_enabled": 0x0006}
	for name, addr := range expected {
		s, ok := circuitConfigFieldSpecs[name]
		if !ok || s.group != 0x02 || s.addr != addr {
			t.Errorf("incorrect binding %s", name)
		}
	}
	for _, old := range []string{"heatingCurve", "heating_curve", "flowTempMaxC", "flowTempMinC", "summerLimitC", "roomTempControl", "coolingEnabled", "frostProtC", "frost_prot_c"} {
		if _, err := resolveConfigFieldSpec("circuit", old, circuitConfigFieldSpecs); err == nil {
			t.Errorf("accepted %s", old)
		}
	}
}

type namingProofProvider struct{ staticSemanticProvider }

func (namingProofProvider) Zones() []Zone {
	temp, humidity, target := 0.0, 42.5, 19.5
	return []Zone{{ID: "zone:synthetic", State: ZoneState{CurrentTempC: &temp, CurrentHumidityPct: &humidity}, Config: ZoneConfig{TargetTempC: &target}}}
}
func TestNamingContractDifferential(t *testing.T) {
	b := NewBuilder(nil, nil)
	b.SetSemanticProvider(namingProofProvider{})
	s, e := NewSchema(b, nil, nil, nil)
	if e != nil {
		t.Fatal(e)
	}
	r := graphqlgo.Do(graphqlgo.Params{Schema: s, RequestString: `{ zones { id state { zone_room_temperature zone_room_humidity } config { target_temp_c } } }`})
	if len(r.Errors) > 0 {
		t.Fatal(r.Errors)
	}
	raw, e := json.Marshal(r.Data)
	if e != nil {
		t.Fatal(e)
	}
	if path := os.Getenv("NAMING_PROOF_OUTPUT"); path != "" {
		if e := os.WriteFile(path, raw, 0600); e != nil {
			t.Fatal(e)
		}
	}
}

func TestNamingContractOldSetterNoDispatch(t *testing.T) {
	invoker := &mutationTestInvoker{}
	reg := mutationTestRegistry{entries: map[byte]registry.DeviceEntry{0x15: testControllerEntryWithMethods(mutationSetExtRegisterMethod, mutationGetExtRegisterMethod)}, order: []byte{0x15}}
	schema := buildMutationSchema(t, reg, invoker, nil, nil)
	for _, field := range []string{"heatingCurve", "flowTempMaxC", "flowTempMinC", "summerLimitC", "roomTempControl", "coolingEnabled", "frostProtC", "frost_prot_c"} {
		result := graphqlgo.Do(graphqlgo.Params{Schema: schema, RequestString: `mutation { set_circuit_config(index: 1, field: "` + field + `", value: "1") { success error } }`})
		if len(result.Errors) != 0 {
			t.Fatal(result.Errors)
		}
		payload := result.Data.(map[string]any)["set_circuit_config"].(map[string]any)
		if payload["success"] == true {
			t.Errorf("old setter accepted: %s", field)
		}
	}
	if len(invoker.calls) != 0 {
		t.Fatal("rejected spelling dispatched native call")
	}
}
