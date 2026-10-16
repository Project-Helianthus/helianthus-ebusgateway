package systemconfig

import (
	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
	vaillantsystem "github.com/Project-Helianthus/helianthus-ebusreg/vaillant/system"
	"math"
	"strings"
	"testing"
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
		if got.MinC != -20 || got.MaxC != 50 || got.Source != ConfigBoundSourceUnmatchedPolicy || got.Reason != ConfigBoundReasonUnknownProfile || got.Profile != "" || got.NarrowedOnly {
			t.Errorf("%s: bound = %+v; want unmatched policy -20..50 (unknown_profile)", tc.name, got)
		}
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
		{"empty intersection above policy", 51, 60},
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
			want := ConfigBound{MinC: -20, MaxC: 50, Source: ConfigBoundSourceUnmatchedPolicy, Reason: tc.reason}
			if got != want {
				t.Fatalf("bound = %+v; want %+v with no substituted profile or catalog bound", got, want)
			}
		})
	}
}

func TestDHWBivalenceProfileBoundOnlyNarrowsPolicy(t *testing.T) {
	for _, tc := range []struct {
		name                       string
		min, max, wantMin, wantMax float64
	}{
		{"cannot widen maximum", -20, 60, -20, 50},
		{"released maximum profile", -20, 50, -20, 50},
		{"BASV0 profile narrows maximum", -20, 20, -20, 20},
		{"narrows maximum", -20, 15, -20, 15},
		{"cannot widen minimum", -30, 15, -20, 15},
		{"narrows minimum", -15, 20, -15, 20},
		{"single point intersection", 50, 60, 50, 50},
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

func TestSystemConfigBoundSnapshotConcurrentRefresh(t *testing.T) {
	reg := registry.NewDeviceRegistry(nil)
	infos := []registry.DeviceInfo{
		{Address: 0x15, DeviceID: "BASV0", SoftwareVersion: "0217", HardwareVersion: "8803"},
		{Address: 0x15, DeviceID: "synthetic-family", SoftwareVersion: "0000", HardwareVersion: "0001"},
	}
	reg.Register(infos[0])
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 3000; i++ {
			reg.Register(infos[i%2])
		}
	}()
	defer func() { <-done }()
	for i := 0; i < 3000; i++ {
		id := ControllerIdentityFromRegistry(reg)
		if id != ControllerIdentityFromSnapshot(registry.DeviceEntrySnapshot{DeviceID: infos[0].DeviceID, SoftwareVersion: infos[0].SoftwareVersion, HardwareVersion: infos[0].HardwareVersion}) && id != ControllerIdentityFromSnapshot(registry.DeviceEntrySnapshot{DeviceID: infos[1].DeviceID, SoftwareVersion: infos[1].SoftwareVersion, HardwareVersion: infos[1].HardwareVersion}) {
			t.Fatalf("torn identity: %+v", id)
		}
		if got := ResolveDHWBivalenceBound(id); got != unmatchedDHWBivalenceBound(ConfigBoundReasonUnknownProfile) {
			t.Fatalf("unexpected bound: %+v", got)
		}
	}
	if id := ControllerIdentityFromRegistry(struct{}{}); id != (ControllerIdentity{}) {
		t.Fatalf("non-snapshot registry supplied identity: %+v", id)
	}
}

func TestSystemConfigFiniteInputAndSpecIsolation(t *testing.T) {
	for name, spec := range SystemConfigFieldSpecs() {
		if spec.ValueType != ValueFloat32 && spec.ValueType != ValueUint16 {
			continue
		}
		for _, value := range []string{"NaN", "Inf", "-Inf"} {
			payload, _, _, err := EncodeSystemConfigValueForController(name, value, ControllerIdentity{})
			if err == nil || !strings.Contains(err.Error(), "must be finite") || payload != nil {
				t.Fatalf("%s=%s: payload=%x err=%v", name, value, payload, err)
			}
		}
	}
	specs := SystemConfigFieldSpecs()
	delete(specs, SystemConfigFieldDHWBivalencePoint)
	if _, _, err := EncodeSystemConfigValue(SystemConfigFieldDHWBivalencePoint, "50"); err != nil {
		t.Fatal(err)
	}
}
