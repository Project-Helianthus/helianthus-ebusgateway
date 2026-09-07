package mcp

import (
	"context"
	"errors"
	"testing"
)

type growattProtocolIIV1RuntimeFixture struct {
	snapshot GrowattProtocolIIV1ProviderSnapshot
	err      error
}

func (fixture growattProtocolIIV1RuntimeFixture) GrowattProtocolIIV1Snapshot(context.Context) (GrowattProtocolIIV1ProviderSnapshot, error) {
	return fixture.snapshot, fixture.err
}

func TestGrowattProtocolIIV1RuntimeProjectsValidatedNativeIdentity(t *testing.T) {
	input := tinyGrowattIdentityInput()
	input.Profile.Family = "MAC"
	runtime, err := NewGrowattProtocolIIV1Runtime(growattProtocolIIV1RuntimeFixture{snapshot: GrowattProtocolIIV1ProviderSnapshot{
		Profile: GrowattProtocolIIV1Profile, OfflineIdentityAdmitted: true, Identity: input,
	}})
	if err != nil {
		t.Fatal(err)
	}
	result, err := runtime.GrowattProtocolIIV1(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	native := result.NativeIdentity
	if result.Profile != GrowattProtocolIIV1Profile || result.Family != "MAC" || !result.IdentityQualified ||
		native.Family != "MAC" || native.UnitID != 1 || native.Firmware != "FW-1" || native.Serial != "SN-0001" ||
		native.DeviceType != 0x1234 || native.ModelBuild != [2]uint16{0x4d41, 0x5831} || native.ProtocolVersion != 0x0124 || len(native.Slices) != 5 {
		t.Fatalf("result = %#v", result)
	}
	input.Slices[0].Words[0] = 0
	if native.Slices[0].Words[0] != 0x4657 {
		t.Fatalf("runtime retained fixture aliases: %#v", native.Slices)
	}
}

func TestGrowattProtocolIIV1RuntimeRejectsUnqualifiedOrInvalidSnapshots(t *testing.T) {
	for name, mutate := range map[string]func(*GrowattProtocolIIV1ProviderSnapshot){
		"wrong profile": func(snapshot *GrowattProtocolIIV1ProviderSnapshot) { snapshot.Profile = "other" },
		"not admitted":  func(snapshot *GrowattProtocolIIV1ProviderSnapshot) { snapshot.OfflineIdentityAdmitted = false },
		"invalid identity": func(snapshot *GrowattProtocolIIV1ProviderSnapshot) {
			snapshot.Identity.Profile.Family = "MIX"
		},
	} {
		t.Run(name, func(t *testing.T) {
			snapshot := GrowattProtocolIIV1ProviderSnapshot{
				Profile: GrowattProtocolIIV1Profile, OfflineIdentityAdmitted: true, Identity: fixtureInput(tinyGrowattIdentityInput()),
			}
			mutate(&snapshot)
			runtime, err := NewGrowattProtocolIIV1Runtime(growattProtocolIIV1RuntimeFixture{snapshot: snapshot})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := runtime.GrowattProtocolIIV1(context.Background()); !errors.Is(err, ErrGrowattProtocolIIV1NotAdmitted) {
				t.Fatalf("error = %v", err)
			}
		})
	}
}
