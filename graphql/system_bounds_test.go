package graphql

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"errors"
	ebuserrors "github.com/Project-Helianthus/helianthus-ebusgo/errors"
	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
	vaillantsystem "github.com/Project-Helianthus/helianthus-ebusreg/vaillant/system"
)

var (
	boundTestBASV0 = vaillantsystem.B524ControllerIdentity{EID: "BASV0", SoftwareRaw: "0217", HardwareRaw: "8803", APIVersion: "1", Revision: "1"}
	boundTestBASV2 = vaillantsystem.B524ControllerIdentity{EID: "BASV2", SoftwareRaw: "0507", HardwareRaw: "1704", APIVersion: "1", Revision: "1"}
)

func TestResolveDHWBivalenceBoundBASV0UsesProfile(t *testing.T) {
	got := ResolveDHWBivalenceBound(boundTestBASV0)
	want := ConfigBound{MinC: -20, MaxC: 20, Source: ConfigBoundSourceControllerProfile, NarrowedOnly: true, Profile: "BASV0/SW0217/HW8803/API1.1"}
	if got != want {
		t.Fatalf("BASV0 bound = %+v; want %+v", got, want)
	}
}

func TestResolveDHWBivalenceBoundUnmatchedIdentitiesUseConservativePolicy(t *testing.T) {
	cases := []struct {
		name string
		id   vaillantsystem.B524ControllerIdentity
	}{
		{"BASV2 is not seeded", boundTestBASV2},
		{"BASV2 without API qualifier", vaillantsystem.B524ControllerIdentity{EID: "BASV2", SoftwareRaw: "0507", HardwareRaw: "1704"}},
		{"BASV0 without API qualifier", vaillantsystem.B524ControllerIdentity{EID: "BASV0", SoftwareRaw: "0217", HardwareRaw: "8803"}},
		{"BASV0 other software", vaillantsystem.B524ControllerIdentity{EID: "BASV0", SoftwareRaw: "0218", HardwareRaw: "8803", APIVersion: "1", Revision: "1"}},
		{"BASV0 other API revision", vaillantsystem.B524ControllerIdentity{EID: "BASV0", SoftwareRaw: "0217", HardwareRaw: "8803", APIVersion: "1", Revision: "2"}},
		{"zero identity", vaillantsystem.B524ControllerIdentity{}},
	}
	for _, tc := range cases {
		got := ResolveDHWBivalenceBound(tc.id)
		if got.MinC != -20 || got.MaxC != 20 || got.Source != ConfigBoundSourceUnmatchedPolicy || got.Reason != ConfigBoundReasonUnknownProfile || got.Profile != "" || got.NarrowedOnly {
			t.Errorf("%s: bound = %+v; want unmatched policy -20..20 (unknown_profile)", tc.name, got)
		}
	}
}

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
		{"BASV2 rejects 21 under the unmatched policy", boundTestBASV2, "21", true, "controller identity unmatched (unknown_profile): conservative policy bound [-20, 20] applies"},
		{"BASV2 rejects 50 under the unmatched policy", boundTestBASV2, "50", true, "unmatched"},
		{"unknown identity rejects 21", vaillantsystem.B524ControllerIdentity{}, "21", true, "unmatched"},
		{"unknown identity accepts 0", vaillantsystem.B524ControllerIdentity{}, "0", false, ""},
	}
	for _, tc := range cases {
		payload, spec, bound, err := EncodeSystemConfigValueForController(SystemConfigFieldDHWBivalencePoint, tc.value, tc.id)
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
				if _, _, bound, err := EncodeSystemConfigValueForController(tc.field, value, id); err != nil || bound != nil {
					t.Errorf("%s=%s id=%s: err=%v bound=%v; want accepted without a bound", tc.field, value, id.EID, err, bound)
				}
			}
			for _, value := range tc.bad {
				if _, _, bound, err := EncodeSystemConfigValueForController(tc.field, value, id); err == nil || bound != nil {
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
	if _, _, err := EncodeSystemConfigValue(SystemConfigFieldDHWBivalencePoint, "21"); err == nil {
		t.Fatal("identity-free encode accepted 21; want the conservative -20..20 default")
	}
	if _, _, err := EncodeSystemConfigValue(SystemConfigFieldDHWBivalencePoint, "20"); err != nil {
		t.Fatalf("identity-free encode rejected 20: %v", err)
	}
}

func TestApplyConfigMutationDHWBivalenceRejectsOutOfBoundBeforeInvoke(t *testing.T) {
	reg := mutationTestRegistry{
		entries: map[byte]registry.DeviceEntry{
			0x15: testControllerEntryWithMethods(mutationSetExtRegisterMethod, mutationGetExtRegisterMethod),
		},
		order: []byte{0x15},
	}
	for _, value := range []string{"21", "50", "-21"} {
		invoker := &mutationTestInvoker{}
		result := ApplyConfigMutation(context.Background(), reg, invoker, 0x7F, true, SystemConfigFieldDHWBivalencePoint, value, systemConfigFieldSpecs)
		if result.Success {
			t.Errorf("value %s: success = true; want rejection", value)
		}
		if !strings.Contains(result.Error, "unmatched") || !strings.Contains(result.Error, "[-20, 20]") {
			t.Errorf("value %s: error = %q; want the unmatched policy bound named", value, result.Error)
		}
		if len(invoker.calls) != 0 {
			t.Errorf("value %s: invoker calls = %d; want 0 before any bus write", value, len(invoker.calls))
		}
	}

	invoker := &mutationTestInvoker{}
	_ = ApplyConfigMutation(context.Background(), reg, invoker, 0x7F, true, SystemConfigFieldDHWBivalencePoint, "20", systemConfigFieldSpecs)
	if len(invoker.calls) == 0 {
		t.Fatal("in-bound value 20 never reached the invoker")
	}
}

func TestSystemQueryExposesDHWBivalenceBound(t *testing.T) {
	for _, active := range []ConfigBound{
		{MinC: -20, MaxC: 20, Source: ConfigBoundSourceUnmatchedPolicy, Reason: ConfigBoundReasonUnknownProfile},
		ResolveDHWBivalenceBound(boundTestBASV0),
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
			query := `{ system { config { dhwBivalencePoint dhwBivalencePointBound { minC maxC source reason profile narrowedOnly } dhw_bivalence_point_bound { minC maxC source narrowedOnly } } } }`
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
								NarrowedOnly *bool   `json:"narrowedOnly"`
								MaxC         float64 `json:"maxC"`
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
			if response.Data.System.Config.DhwBivalencePoint != 5 || snake.MaxC != active.MaxC || snake.Source != active.Source || snake.NarrowedOnly == nil || *snake.NarrowedOnly != active.NarrowedOnly {
				t.Fatalf("config = %+v; want value 5 and the snake_case alias to match", response.Data.System.Config)
			}
		})
	}
}

func TestDHWBivalenceBoundLookupFallbacks(t *testing.T) {
	type fallbackCase struct {
		name   string
		result vaillantsystem.B524BoundResult
		reason string
	}
	cases := []fallbackCase{
		{"unknown profile", vaillantsystem.B524BoundResult{Status: vaillantsystem.B524BoundUnknownProfile}, ConfigBoundReasonUnknownProfile},
		{"unknown register", vaillantsystem.B524BoundResult{Status: vaillantsystem.B524BoundUnknownRegister}, ConfigBoundReasonUnknownRegister},
		{"unexpected status", vaillantsystem.B524BoundResult{Status: vaillantsystem.B524BoundStatus(99)}, ConfigBoundReasonUnknownProfile},
		{"unqualified bound", vaillantsystem.B524BoundResult{Status: vaillantsystem.B524BoundFound, Bound: vaillantsystem.B524RegisterBound{Min: -20, Max: 50}}, ConfigBoundReasonUnqualifiedBound},
	}
	for _, invalid := range []struct {
		name     string
		min, max float64
	}{
		{"NaN min", math.NaN(), 50},
		{"NaN max", -20, math.NaN()},
		{"negative infinity min", math.Inf(-1), 50},
		{"positive infinity min", math.Inf(1), 50},
		{"negative infinity max", -20, math.Inf(-1)},
		{"positive infinity max", -20, math.Inf(1)},
		{"inverted range", 50, -20},
		{"empty intersection above policy", 25, 30},
		{"empty intersection below policy", -30, -25},
	} {
		cases = append(cases, fallbackCase{
			name: invalid.name,
			result: vaillantsystem.B524BoundResult{
				Status: vaillantsystem.B524BoundFound,
				Bound:  vaillantsystem.B524RegisterBound{Qualified: true, Min: invalid.min, Max: invalid.max},
			},
			reason: ConfigBoundReasonInvalidBound,
		})
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := dhwBivalenceBoundFromResult(boundTestBASV0, tc.result)
			want := ConfigBound{MinC: -20, MaxC: 20, Source: ConfigBoundSourceUnmatchedPolicy, Reason: tc.reason}
			if got != want {
				t.Fatalf("bound = %+v; want %+v with no substituted profile or catalog bound", got, want)
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
						result = ApplyConfigMutation(context.Background(), reg, invoker, 0x7F, true, field, value, scope.specs)
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
					data := executeMutation(t, buildMutationSchema(t, reg, invoker, nil, nil), query)
					payload, ok := data[scope.mutation].(map[string]any)
					if !ok || payload["success"] != false || !strings.Contains(fmt.Sprint(payload["error"]), "must be finite") || len(invoker.calls) != 0 {
						t.Fatalf("GraphQL result=%v invokes=%d; want non-finite rejection and zero invokes/writes", data, len(invoker.calls))
					}
				})
			}
		}
	}
}

func TestDHWBivalenceProfileBoundOnlyNarrowsPolicy(t *testing.T) {
	for _, tc := range []struct {
		name                       string
		min, max, wantMin, wantMax float64
	}{
		{"cannot widen maximum", -20, 30, -20, 20},
		{"narrows maximum", -20, 15, -20, 15},
		{"cannot widen minimum", -30, 15, -20, 15},
		{"narrows minimum", -15, 20, -15, 20},
		{"single point intersection", 20, 30, 20, 20},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := dhwBivalenceBoundFromResult(boundTestBASV0, vaillantsystem.B524BoundResult{
				Status: vaillantsystem.B524BoundFound,
				Bound:  vaillantsystem.B524RegisterBound{Qualified: true, Min: tc.min, Max: tc.max},
			})
			want := ConfigBound{MinC: tc.wantMin, MaxC: tc.wantMax, Source: ConfigBoundSourceControllerProfile, Profile: "BASV0/SW0217/HW8803/API1.1", NarrowedOnly: true}
			if got != want {
				t.Fatalf("bound = %+v; want %+v", got, want)
			}
		})
	}
}

// Register updates the same live entry while both identity selection and the
// fallback mutation's bound validation run. Every observed tuple must be one
// complete registration, even when all three identity fields change together.
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
		identity := controllerIdentityFromRegistry(reg)
		a, b := boundTestBASV0, boundTestBASV2
		a.APIVersion, a.Revision, b.APIVersion, b.Revision = "", "", "", ""
		if identity != a && identity != b {
			t.Fatalf("incoherent controller snapshot: %+v", identity)
		}
		bound := ResolveDHWBivalenceBound(identity)
		if bound != unmatchedDHWBivalenceBound(ConfigBoundReasonUnknownProfile) {
			t.Fatalf("bound = %+v; want conservative unmatched policy", bound)
		}
		invoker := &mutationTestInvoker{}
		result := ApplyConfigMutation(context.Background(), reg, invoker, 0x7F, true, SystemConfigFieldDHWBivalencePoint, "21", systemConfigFieldSpecs)
		if result.Success || !strings.Contains(result.Error, "conservative policy bound [-20, 20]") || len(invoker.calls) != 0 {
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
	if got := controllerIdentityFromRegistry(reg); got != want {
		t.Fatalf("controller identity = %+v; want %+v", got, want)
	}
}
