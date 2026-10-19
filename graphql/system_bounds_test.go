package graphql

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"errors"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/vaillant/systemconfig"
	ebuserrors "github.com/Project-Helianthus/helianthus-ebusgo/errors"
	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
	vaillantsystem "github.com/Project-Helianthus/helianthus-ebusreg/vaillant/system"
)

var (
	boundTestBASV0 = vaillantsystem.B524ControllerIdentity{EID: "BASV0", SoftwareRaw: "0217", HardwareRaw: "8803", APIVersion: "1", Revision: "1"}
	boundTestBASV2 = vaillantsystem.B524ControllerIdentity{EID: "BASV2", SoftwareRaw: "0507", HardwareRaw: "1704", APIVersion: "1", Revision: "1"}
)

func TestEncodeSystemConfigValueForControllerDHWBivalence(t *testing.T) {
	cases := []struct {
		name       string
		id         vaillantsystem.B524ControllerIdentity
		value      string
		wantErr    bool
		wantSubstr string
	}{
		{"BASV0 accepts 20", boundTestBASV0, "20", false, ""},
		{"BASV0 accepts -20", boundTestBASV0, "-20", false, ""},
		{"BASV0 rejects 21", boundTestBASV0, "21", true, "controller profile BASV0/SW0217/HW8803/API1.1 allows [-20, 20]"},
		{"BASV0 rejects 50", boundTestBASV0, "50", true, "allows [-20, 20]"},
		{"BASV0 rejects -21", boundTestBASV0, "-21", true, "allows [-20, 20]"},
		{"BASV2 falls to unmatched policy and accepts 20", boundTestBASV2, "20", false, ""},
		{"BASV2 accepts 21 under the unmatched policy", boundTestBASV2, "21", false, ""},
		{"BASV2 accepts 50 under the unmatched policy", boundTestBASV2, "50", false, ""},
		{"BASV2 rejects 51 under the unmatched policy", boundTestBASV2, "51", true, "conservative policy bound [-20, 50]"},
		{"unknown identity accepts 50", vaillantsystem.B524ControllerIdentity{}, "50", false, ""},
		{"unknown identity rejects 51", vaillantsystem.B524ControllerIdentity{}, "51", true, "conservative policy bound [-20, 50]"},
		{"unknown identity accepts 21", vaillantsystem.B524ControllerIdentity{}, "21", false, ""},
		{"unknown identity accepts 0", vaillantsystem.B524ControllerIdentity{}, "0", false, ""},
	}
	for _, tc := range cases {
		payload, spec, bound, err := systemconfig.EncodeSystemConfigValueForController(SystemConfigFieldDHWBivalencePoint, tc.value, tc.id)
		if bound == nil {
			t.Fatalf("%s: bound = nil; want the active bound for dhwBivalencePointC", tc.name)
		}
		if tc.wantErr {
			if err == nil {
				t.Errorf("%s: error = nil; want rejection (payload %x)", tc.name, payload)
				continue
			}
			if !errors.Is(err, ebuserrors.ErrInvalidPayload) {
				t.Errorf("%s: error %v does not wrap ErrInvalidPayload", tc.name, err)
			}
			if !strings.Contains(err.Error(), tc.wantSubstr) {
				t.Errorf("%s: error = %q; want substring %q", tc.name, err.Error(), tc.wantSubstr)
			}
			if payload != nil {
				t.Errorf("%s: rejected value still produced payload %x", tc.name, payload)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s: error = %v; want accepted", tc.name, err)
			continue
		}
		if len(payload) != 4 || spec.Group() != 0x00 || spec.Addr() != 0x0001 {
			t.Errorf("%s: payload=%x group=%#x addr=%#x; want 4-byte payload for GG00 RR0001", tc.name, payload, spec.Group(), spec.Addr())
		}
	}
}

func TestEncodeSystemConfigValueForControllerKeepsOtherSystemNumbers(t *testing.T) {
	// The other system numbers keep their fixed ranges for every identity and
	// carry no bound.
	identities := []vaillantsystem.B524ControllerIdentity{boundTestBASV0, boundTestBASV2, {}}
	cases := []struct {
		field string
		ok    []string
		bad   []string
	}{
		{"maxRoomHumidityPct", []string{"30", "80"}, []string{"29", "81"}},
		{"hcBivalencePointC", []string{"-20", "30"}, []string{"-21", "31"}},
		{"hcEmergencyTempC", []string{"20", "80"}, []string{"19", "81"}},
		{"hwcMaxFlowTempC", []string{"15", "80"}, []string{"14", "81"}},
		{"installerMenuCode", []string{"0", "999"}, []string{"1000"}},
	}
	for _, id := range identities {
		for _, tc := range cases {
			for _, value := range tc.ok {
				if _, _, bound, err := systemconfig.EncodeSystemConfigValueForController(tc.field, value, id); err != nil || bound != nil {
					t.Errorf("%s=%s id=%s: err=%v bound=%v; want accepted without a bound", tc.field, value, id.EID, err, bound)
				}
			}
			for _, value := range tc.bad {
				if _, _, bound, err := systemconfig.EncodeSystemConfigValueForController(tc.field, value, id); err == nil || bound != nil {
					t.Errorf("%s=%s id=%s: err=%v bound=%v; want rejected without a bound", tc.field, value, id.EID, err, bound)
				}
			}
		}
	}
	if got := systemConfigFieldSpecs["hcBivalencePointC"]; got.min != -20 || got.max != 30 {
		t.Fatalf("hcBivalencePointC spec changed: %+v", got)
	}
}

func TestSystemConfigFieldSpecDefaultIsConservative(t *testing.T) {
	// EncodeSystemConfigValue has no identity; it must never be wider than the
	// unmatched policy.
	if _, _, err := EncodeSystemConfigValue(SystemConfigFieldDHWBivalencePoint, "51"); err == nil {
		t.Fatal("identity-free encode accepted 51; want the released -20..50 default")
	}
	if _, _, err := EncodeSystemConfigValue(SystemConfigFieldDHWBivalencePoint, "50"); err != nil {
		t.Fatalf("identity-free encode rejected 50: %v", err)
	}
}

func TestApplyConfigMutationDHWBivalenceRejectsOutOfBoundBeforeInvoke(t *testing.T) {
	reg := mutationTestRegistry{
		entries: map[byte]registry.DeviceEntry{
			0x15: testControllerEntryWithMethods(mutationSetExtRegisterMethod, mutationGetExtRegisterMethod),
		},
		order: []byte{0x15},
	}
	for _, value := range []string{"51", "50.5", "-21"} {
		invoker := &mutationTestInvoker{}
		result := ApplyConfigMutation(context.Background(), reg, invoker, 0x7F, true, SystemConfigFieldDHWBivalencePoint, value, systemConfigFieldSpecs, &ConfigBound{MinC: -20, MaxC: 50, Source: ConfigBoundSourceUnmatchedPolicy, Reason: ConfigBoundReasonUnknownProfile})
		if result.Success {
			t.Errorf("value %s: success = true; want rejection", value)
		}
		if !strings.Contains(result.Error, "unmatched") || !strings.Contains(result.Error, "[-20, 50]") {
			t.Errorf("value %s: error = %q; want the unmatched policy bound named", value, result.Error)
		}
		if len(invoker.calls) != 0 {
			t.Errorf("value %s: invoker calls = %d; want 0 before any bus write", value, len(invoker.calls))
		}
	}

	invoker := &mutationTestInvoker{}
	_ = ApplyConfigMutation(context.Background(), reg, invoker, 0x7F, true, SystemConfigFieldDHWBivalencePoint, "50", systemConfigFieldSpecs, &ConfigBound{MinC: -20, MaxC: 50, Source: ConfigBoundSourceUnmatchedPolicy})
	if len(invoker.calls) == 0 {
		t.Fatal("in-bound value 50 never reached the invoker")
	}
}

func TestSystemQueryExposesDHWBivalenceBound(t *testing.T) {
	for _, active := range []ConfigBound{
		{MinC: -20, MaxC: 50, Source: ConfigBoundSourceUnmatchedPolicy, Reason: ConfigBoundReasonUnknownProfile},
		systemconfig.ResolveDHWBivalenceBound(boundTestBASV0),
	} {
		t.Run(active.Source, func(t *testing.T) {
			provider := NewLiveSemanticProvider()
			value := 5.0
			provider.SetSystem(&SystemStatus{Config: SystemConfig{
				DhwBivalencePoint: &value,
				DhwBivalenceBound: &active,
			}})
			builder := NewBuilder(registry.NewDeviceRegistry(nil), nil)
			builder.SetSemanticProvider(provider)
			handler, err := NewHandler(builder)
			if err != nil {
				t.Fatalf("NewHandler error = %v", err)
			}
			query := `{ system { config { dhwBivalencePoint dhwBivalencePointBound { minC maxC source reason profile narrowedOnly } dhw_bivalence_point_bound { min_c max_c source narrowed_only } } } }`
			body, _ := json.Marshal(map[string]string{"query": query})
			req := httptest.NewRequest(http.MethodPost, "/graphql", strings.NewReader(string(body)))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
			}
			var response struct {
				Data struct {
					System struct {
						Config struct {
							DhwBivalencePoint      float64 `json:"dhwBivalencePoint"`
							DhwBivalencePointBound struct {
								NarrowedOnly *bool   `json:"narrowedOnly"`
								MinC         float64 `json:"minC"`
								MaxC         float64 `json:"maxC"`
								Source       string  `json:"source"`
								Reason       *string `json:"reason"`
								Profile      *string `json:"profile"`
							} `json:"dhwBivalencePointBound"`
							Snake struct {
								NarrowedOnly *bool   `json:"narrowed_only"`
								MinC         float64 `json:"min_c"`
								MaxC         float64 `json:"max_c"`
								Source       string  `json:"source"`
							} `json:"dhw_bivalence_point_bound"`
						} `json:"config"`
					} `json:"system"`
				} `json:"data"`
				Errors []any `json:"errors"`
			}
			if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil || len(response.Errors) != 0 {
				t.Fatalf("response err=%v errors=%v body=%s", err, response.Errors, rec.Body.String())
			}
			bound := response.Data.System.Config.DhwBivalencePointBound
			if bound.MinC != active.MinC || bound.MaxC != active.MaxC || bound.Source != active.Source || bound.NarrowedOnly == nil || *bound.NarrowedOnly != active.NarrowedOnly {
				t.Fatalf("dhwBivalencePointBound = %+v; want %+v", bound, active)
			}
			if (active.Reason == "" && bound.Reason != nil) || (active.Reason != "" && (bound.Reason == nil || *bound.Reason != active.Reason)) ||
				(active.Profile == "" && bound.Profile != nil) || (active.Profile != "" && (bound.Profile == nil || *bound.Profile != active.Profile)) {
				t.Fatalf("reason/profile = %v/%v; want %+v", bound.Reason, bound.Profile, active)
			}
			snake := response.Data.System.Config.Snake
			if response.Data.System.Config.DhwBivalencePoint != 5 || snake.MinC != active.MinC || snake.MaxC != active.MaxC || snake.Source != active.Source || snake.NarrowedOnly == nil || *snake.NarrowedOnly != active.NarrowedOnly {
				t.Fatalf("config = %+v; want value 5 and the snake_case alias to match", response.Data.System.Config)
			}
		})
	}
}

func TestNumericConfigNonFiniteRejectedBeforeInvoke(t *testing.T) {
	reg := mutationTestRegistry{
		entries: map[byte]registry.DeviceEntry{
			0x15: testControllerEntryWithMethods(mutationSetExtRegisterMethod, mutationGetExtRegisterMethod),
		},
		order: []byte{0x15},
	}
	for _, scope := range []struct {
		name, mutation string
		specs          map[string]configFieldSpec
	}{
		{"system", "setSystemConfig", systemConfigFieldSpecs},
		{"circuit", "setCircuitConfig", circuitConfigFieldSpecs},
		{"zone", "setZoneConfig", zoneConfigFieldSpecs},
	} {
		for field, spec := range scope.specs {
			if spec.valueType != configValueFloat32 && spec.valueType != configValueUint16 {
				continue
			}
			for _, value := range []string{"NaN", "Inf", "-Inf"} {
				t.Run(scope.name+"/"+field+"/"+value, func(t *testing.T) {
					// The registry/invoker system adapter used outside GraphQL.
					invoker := &mutationTestInvoker{}
					var result ConfigMutationResult
					if scope.name == "system" {
						result = ApplyConfigMutation(context.Background(), reg, invoker, 0x7F, true, field, value, scope.specs, &ConfigBound{MinC: -20, MaxC: 50})
					} else {
						result = applyConfigMutation(context.Background(), reg, invoker, 0x7F, true, spec, 0x00, value)
					}
					if result.Success || !strings.Contains(result.Error, "must be finite") || len(invoker.calls) != 0 {
						t.Fatalf("registry result=%+v invokes=%d; want non-finite rejection and zero invokes/writes", result, len(invoker.calls))
					}

					index := ""
					if scope.name != "system" {
						index = "index: 0, "
					}
					query := fmt.Sprintf(`mutation { %s(%sfield: %q, value: %q) { success error } }`, scope.mutation, index, field, value)
					schema := buildMutationSchema(t, reg, invoker, nil, nil)
					if scope.name == "system" {
						schema = buildMutationSchemaForM13(t, reg, invoker, func() *ConfigBound { return &ConfigBound{MinC: -20, MaxC: 50} })
					}
					data := executeMutation(t, schema, query)
					payload, ok := data[scope.mutation].(map[string]any)
					if !ok || payload["success"] != false || !strings.Contains(fmt.Sprint(payload["error"]), "must be finite") || len(invoker.calls) != 0 {
						t.Fatalf("GraphQL result=%v invokes=%d; want non-finite rejection and zero invokes/writes", data, len(invoker.calls))
					}
				})
			}
		}
	}
}

func TestDHWBivalenceBoundRegistrySnapshotConcurrentRegister(t *testing.T) {
	reg := registry.NewDeviceRegistry(nil)
	infos := []registry.DeviceInfo{
		{Address: 0x15, Manufacturer: "Vaillant", DeviceID: "BASV0", SoftwareVersion: "0217", HardwareVersion: "8803"},
		{Address: 0x15, Manufacturer: "Vaillant", DeviceID: "BASV2", SoftwareVersion: "0507", HardwareVersion: "1704"},
	}
	entry := reg.Register(infos[0])
	start, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		<-start
		for i := 0; i < 3000; i++ {
			if got := reg.Register(infos[i%len(infos)]); got != entry {
				t.Error("Register replaced the entry; test must exercise live entry updates")
				return
			}
		}
	}()
	defer func() { <-done }()
	close(start)
	for i := 0; i < 3000; i++ {
		identity := systemconfig.ControllerIdentityFromRegistry(reg)
		a, b := boundTestBASV0, boundTestBASV2
		a.APIVersion, a.Revision, b.APIVersion, b.Revision = "", "", "", ""
		if identity != a && identity != b {
			t.Fatalf("incoherent controller snapshot: %+v", identity)
		}
		bound := systemconfig.ResolveDHWBivalenceBound(identity)
		if bound != (ConfigBound{MinC: -20, MaxC: 50, Source: ConfigBoundSourceUnmatchedPolicy, Reason: ConfigBoundReasonUnknownProfile}) {
			t.Fatalf("bound = %+v; want conservative unmatched policy", bound)
		}
		invoker := &mutationTestInvoker{}
		result := ApplyConfigMutation(context.Background(), reg, invoker, 0x7F, true, SystemConfigFieldDHWBivalencePoint, "51", systemConfigFieldSpecs, &bound)
		if result.Success || !strings.Contains(result.Error, "conservative policy bound [-20, 50]") || len(invoker.calls) != 0 {
			t.Fatalf("fallback bound validation: result=%+v calls=%d", result, len(invoker.calls))
		}
	}
}

func TestDHWBivalenceBoundRegistrySnapshotCanonicalPriority(t *testing.T) {
	reg := registry.NewDeviceRegistry(nil)
	for _, info := range []registry.DeviceInfo{
		{Address: 0x42, DeviceID: "off-canonical"},
		{Address: 0x35, DeviceID: "alternate"},
		{Address: 0x15, DeviceID: " basv0 ", SoftwareVersion: " 0217 ", HardwareVersion: " 8803 "},
	} {
		reg.Register(info)
	}
	want := vaillantsystem.B524ControllerIdentity{EID: "BASV0", SoftwareRaw: "0217", HardwareRaw: "8803"}
	if got := systemconfig.ControllerIdentityFromRegistry(reg); got != want {
		t.Fatalf("controller identity = %+v; want %+v", got, want)
	}
}
