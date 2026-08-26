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

func TestGrowattProtocolIIV1RuntimeProjectsOnlySanitizedIdentityStatus(t *testing.T) {
	runtime, err := NewGrowattProtocolIIV1Runtime(growattProtocolIIV1RuntimeFixture{snapshot: GrowattProtocolIIV1ProviderSnapshot{
		Profile:                 GrowattProtocolIIV1Profile,
		Family:                  "MAC",
		OfflineIdentityAdmitted: true,
		OutboundAllowed:         true,
		UnitID:                  1,
		IdentitySlices:          growattProtocolIITestSlices(),
	}})
	if err != nil {
		t.Fatal(err)
	}
	result, err := runtime.GrowattProtocolIIV1(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.Profile != GrowattProtocolIIV1Profile || result.Family != "MAC" || !result.IdentityQualified ||
		result.NativeIdentity.Family != "MAC" || result.NativeIdentity.UnitID != 1 || len(result.NativeIdentity.Slices) != 5 {
		t.Fatalf("result = %#v", result)
	}
}

func TestGrowattProtocolIIV1RuntimeRejectsUnqualifiedOrUnexpectedSnapshots(t *testing.T) {
	for name, snapshot := range map[string]GrowattProtocolIIV1ProviderSnapshot{
		"wrong profile":  {Profile: "other", Family: "MAX", OfflineIdentityAdmitted: true},
		"unknown family": {Profile: GrowattProtocolIIV1Profile, Family: "MIX", OfflineIdentityAdmitted: true},
		"not admitted":   {Profile: GrowattProtocolIIV1Profile, Family: "MAX", OfflineIdentityAdmitted: false},
	} {
		t.Run(name, func(t *testing.T) {
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
