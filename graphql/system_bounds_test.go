package graphql

import (
	"context"
	"encoding/json"
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
	want := ConfigBound{MinC: -20, MaxC: 20, Source: ConfigBoundSourceControllerProfile, Profile: "BASV0/SW0217/HW8803/API1.1"}
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
		if got.MinC != -20 || got.MaxC != 20 || got.Source != ConfigBoundSourceUnmatchedPolicy || got.Reason != ConfigBoundReasonUnknownProfile || got.Profile != "" {
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
	provider := NewLiveSemanticProvider()
	value := 5.0
	provider.SetSystem(&SystemStatus{Config: SystemConfig{
		DhwBivalencePoint: &value,
		DhwBivalenceBound: &ConfigBound{MinC: -20, MaxC: 20, Source: ConfigBoundSourceUnmatchedPolicy, Reason: ConfigBoundReasonUnknownProfile},
	}})
	builder := NewBuilder(registry.NewDeviceRegistry(nil), nil)
	builder.SetSemanticProvider(provider)
	handler, err := NewHandler(builder)
	if err != nil {
		t.Fatalf("NewHandler error = %v", err)
	}
	query := `{ system { config { dhwBivalencePoint dhwBivalencePointBound { minC maxC source reason profile } dhw_bivalence_point_bound { minC maxC source } } } }`
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
						MinC    float64 `json:"minC"`
						MaxC    float64 `json:"maxC"`
						Source  string  `json:"source"`
						Reason  *string `json:"reason"`
						Profile *string `json:"profile"`
					} `json:"dhwBivalencePointBound"`
					Snake struct {
						MaxC   float64 `json:"maxC"`
						Source string  `json:"source"`
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
	if bound.MinC != -20 || bound.MaxC != 20 || bound.Source != "unmatched_policy" || bound.Reason == nil || *bound.Reason != "unknown_profile" || bound.Profile != nil {
		t.Fatalf("dhwBivalencePointBound = %+v; want -20..20 unmatched_policy/unknown_profile without profile", bound)
	}
	if response.Data.System.Config.DhwBivalencePoint != 5 || response.Data.System.Config.Snake.MaxC != 20 || response.Data.System.Config.Snake.Source != "unmatched_policy" {
		t.Fatalf("config = %+v; want value 5 and the snake_case alias to match", response.Data.System.Config)
	}
}
