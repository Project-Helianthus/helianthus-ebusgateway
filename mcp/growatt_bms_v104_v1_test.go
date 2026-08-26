package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

type growattBMSV104FixtureProvider struct {
	snapshot GrowattBMSV104ProviderSnapshot
	err      error
}

func (provider growattBMSV104FixtureProvider) GrowattBMSV104Snapshot(context.Context) (GrowattBMSV104ProviderSnapshot, error) {
	return provider.snapshot, provider.err
}

func TestGrowattBMSV104SnapshotGetProjectsAdmittedNativeData(t *testing.T) {
	runtime, err := NewGrowattBMSV104Runtime(growattBMSV104FixtureProvider{snapshot: growattBMSV104FixtureSnapshot()})
	if err != nil {
		t.Fatal(err)
	}

	result, err := runtime.SnapshotGet(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.Profile != GrowattBMSV104Profile || !result.Admitted || result.OutboundAllowed || len(result.RawEvidence) != 1 {
		t.Fatalf("result = %#v", result)
	}
	if result.RawEvidence[0].Identifier != 0x311 || result.RawEvidence[0].DLC != 8 || result.RawEvidence[0].RawDLC != 8 || result.RawEvidence[0].Data[0] != 0x02 {
		t.Fatalf("raw evidence = %#v", result.RawEvidence)
	}
	if result.Limits.ChargeVoltageDecivolts != 568 || result.Measurements.SOCPercent != 80 || result.Status.PackCount != 2 {
		t.Fatalf("projection = %#v", result)
	}
}

func TestGrowattBMSV104SnapshotGetPreservesNativeEvidenceAndCapability(t *testing.T) {
	snapshot := growattBMSV104FixtureSnapshot()
	snapshot.OutboundAllowed = true
	snapshot.RawEvidence = []GrowattBMSV104RawEvidence{{Interface: "can0", Sequence: 7, MonotonicNanos: 99, Identifier: 0x311, DLC: 8, RawDLC: 8, Data: [8]byte{0xde, 0xad, 0xbe, 0xef}}}
	runtime, err := NewGrowattBMSV104Runtime(growattBMSV104FixtureProvider{snapshot: snapshot})
	if err != nil {
		t.Fatal(err)
	}

	result, err := runtime.SnapshotGet(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	if !result.OutboundAllowed || !strings.Contains(string(encoded), "\"raw_evidence\":") || !strings.Contains(string(encoded), "\"interface\":\"can0\"") || !strings.Contains(string(encoded), "\"data\":[222,173,190,239,0,0,0,0]") {
		t.Fatalf("result = %s", encoded)
	}
	if len(result.RawEvidence) != 1 || result.RawEvidence[0].Sequence != 7 || result.RawEvidence[0].MonotonicNanos != 99 || result.RawEvidence[0].Identifier != 0x311 || result.RawEvidence[0].Data[3] != 0xef {
		t.Fatalf("raw evidence = %#v", result.RawEvidence)
	}
}

func TestGrowattBMSV104SnapshotGetDropsMalformedRawEvidenceWithoutSuppressingProjection(t *testing.T) {
	snapshot := growattBMSV104FixtureSnapshot()
	snapshot.RawEvidence = []GrowattBMSV104RawEvidence{{Identifier: 0x311, Extended: true, DLC: 8, RawDLC: 8, Data: [8]byte{0xde, 0xad, 0xbe, 0xef}}}
	runtime, err := NewGrowattBMSV104Runtime(growattBMSV104FixtureProvider{snapshot: snapshot})
	if err != nil {
		t.Fatal(err)
	}

	result, err := runtime.SnapshotGet(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(result.RawEvidence) != 0 || result.Measurements.SOCPercent != 80 || result.Status.PackCount != 2 {
		t.Fatalf("result = %#v", result)
	}
}

func TestGrowattBMSV104SnapshotGetFailsClosed(t *testing.T) {
	providerFailure := errors.New("provider failure")
	for _, testCase := range []struct {
		name     string
		provider GrowattBMSV104SnapshotProvider
		want     error
	}{
		{name: "nil provider", want: ErrGrowattBMSV104ProviderUnavailable},
		{name: "provider failure", provider: growattBMSV104FixtureProvider{err: providerFailure}, want: providerFailure},
		{name: "wrong profile", provider: growattBMSV104FixtureProvider{snapshot: GrowattBMSV104ProviderSnapshot{Admitted: true, Profile: "other"}}, want: ErrGrowattBMSV104NotAdmitted},
		{name: "not admitted", provider: growattBMSV104FixtureProvider{snapshot: GrowattBMSV104ProviderSnapshot{Profile: GrowattBMSV104Profile}}, want: ErrGrowattBMSV104NotAdmitted},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			runtime, err := NewGrowattBMSV104Runtime(testCase.provider)
			if testCase.provider == nil {
				if !errors.Is(err, testCase.want) {
					t.Fatalf("NewGrowattBMSV104Runtime() error = %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err := runtime.SnapshotGet(context.Background()); !errors.Is(err, testCase.want) {
				t.Fatalf("SnapshotGet() error = %v, want %v", err, testCase.want)
			}
		})
	}
}

func growattBMSV104FixtureSnapshot() GrowattBMSV104ProviderSnapshot {
	return GrowattBMSV104ProviderSnapshot{
		Profile:         GrowattBMSV104Profile,
		Admitted:        true,
		OutboundAllowed: false,
		Limits: GrowattBMSV104Limits{
			ChargeVoltageDecivolts: 568,
		},
		Status: GrowattBMSV104Status{PackCount: 2, TotalCellCount: 16},
		Measurements: GrowattBMSV104Measurements{
			SOCPercent: 80,
		},
		RawEvidence: []GrowattBMSV104RawEvidence{{Identifier: 0x311, DLC: 8, RawDLC: 8, Data: [8]byte{0x02, 0x38}}},
	}
}
