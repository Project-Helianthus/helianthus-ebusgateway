package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

type greeVRFCANCandidateV1FixtureProvider struct {
	snapshot GreeVRFCANCandidateV1ProviderSnapshot
	err      error
}

func (provider greeVRFCANCandidateV1FixtureProvider) GreeVRFCANCandidateV1Snapshot(context.Context) (GreeVRFCANCandidateV1ProviderSnapshot, error) {
	return provider.snapshot, provider.err
}

func TestGreeVRFCANCandidateV1SnapshotGetProjectsAdmittedOpaqueCellValues(t *testing.T) {
	runtime, err := NewGreeVRFCANCandidateV1Runtime(greeVRFCANCandidateV1FixtureProvider{snapshot: greeVRFCANCandidateV1FixtureSnapshot()})
	if err != nil {
		t.Fatal(err)
	}

	result, err := runtime.SnapshotGet(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.Profile != GreeVRFCANCandidateV1Profile || !result.Admitted || result.OutboundAllowed {
		t.Fatalf("result = %#v", result)
	}
	if result.Class8 != 0xf7 || result.Opaque7 != 1 || result.Unit7 != 8 || result.Opcode7 != 0x58 || len(result.OpaqueCells) != 2 || result.OpaqueCells[0].Cell != 0x13 || result.OpaqueCells[0].Value != 0xa4 || result.OpaqueCells[1].Cell != 0x14 || result.OpaqueCells[1].Value != 0xa5 {
		t.Fatalf("metadata = %#v", result)
	}
}

func TestGreeVRFCANCandidateV1SnapshotGetPreservesNativeEvidenceAndCapability(t *testing.T) {
	snapshot := greeVRFCANCandidateV1FixtureSnapshot()
	snapshot.OutboundAllowed = true
	snapshot.RawEvidence = []GreeVRFCANCandidateV1RawEvidence{{Interface: "can0", Sequence: 7, MonotonicNanos: 99, Identifier: 0x1ee04458, Extended: true, DLC: 3, RawDLC: 3, Data: [8]byte{0xde, 0xad, 0xbe, 0xef}}}
	snapshot.OpaqueCells = []GreeVRFCANCandidateV1OpaqueCell{{Cell: 0x13, Value: 0xde}}
	runtime, err := NewGreeVRFCANCandidateV1Runtime(greeVRFCANCandidateV1FixtureProvider{snapshot: snapshot})
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
	if !result.OutboundAllowed || !strings.Contains(string(encoded), "\"raw_evidence\":") || !strings.Contains(string(encoded), "\"dlc\":3") || !strings.Contains(string(encoded), "\"value\":222") {
		t.Fatalf("unsafe result = %s", encoded)
	}
	if len(result.RawEvidence) != 1 || result.RawEvidence[0].Identifier != 0x1ee04458 || result.RawEvidence[0].Data[0] != 0xde || result.RawEvidence[0].Data[3] != 0 || result.RawEvidence[0].Interface != "can0" || result.RawEvidence[0].DLC != 3 || result.RawEvidence[0].RawDLC != 3 {
		t.Fatalf("raw evidence = %#v", result.RawEvidence)
	}
}

func TestGreeVRFCANCandidateV1SnapshotGetOrdersOpaqueCellsDeterministically(t *testing.T) {
	snapshot := greeVRFCANCandidateV1FixtureSnapshot()
	snapshot.OpaqueCells = []GreeVRFCANCandidateV1OpaqueCell{{Cell: 0x14, Value: 0xa5}, {Cell: 0x13, Value: 0xa4}}
	runtime, err := NewGreeVRFCANCandidateV1Runtime(greeVRFCANCandidateV1FixtureProvider{snapshot: snapshot})
	if err != nil {
		t.Fatal(err)
	}

	result, err := runtime.SnapshotGet(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.OpaqueCells[0].Cell != 0x13 || result.OpaqueCells[0].Value != 0xa4 || result.OpaqueCells[1].Cell != 0x14 || result.OpaqueCells[1].Value != 0xa5 {
		t.Fatalf("opaque cells = %#v", result.OpaqueCells)
	}
}

func TestGreeVRFCANCandidateV1SnapshotGetFailsClosed(t *testing.T) {
	providerFailure := errors.New("provider failure")
	for _, testCase := range []struct {
		name     string
		provider GreeVRFCANCandidateV1SnapshotProvider
		want     error
	}{
		{name: "nil provider", want: ErrGreeVRFCANCandidateV1ProviderUnavailable},
		{name: "provider failure", provider: greeVRFCANCandidateV1FixtureProvider{err: providerFailure}, want: providerFailure},
		{name: "wrong profile", provider: greeVRFCANCandidateV1FixtureProvider{snapshot: GreeVRFCANCandidateV1ProviderSnapshot{Admitted: true, Profile: "other", OpaqueCells: []GreeVRFCANCandidateV1OpaqueCell{{Cell: 0x13}}}}, want: ErrGreeVRFCANCandidateV1NotAdmitted},
		{name: "not admitted", provider: greeVRFCANCandidateV1FixtureProvider{snapshot: GreeVRFCANCandidateV1ProviderSnapshot{Profile: GreeVRFCANCandidateV1Profile, OpaqueCells: []GreeVRFCANCandidateV1OpaqueCell{{Cell: 0x13}}}}, want: ErrGreeVRFCANCandidateV1NotAdmitted},
		{name: "no cells", provider: greeVRFCANCandidateV1FixtureProvider{snapshot: GreeVRFCANCandidateV1ProviderSnapshot{Profile: GreeVRFCANCandidateV1Profile, Admitted: true}}, want: ErrGreeVRFCANCandidateV1NotAdmitted},
		{name: "unsupported opcode", provider: greeVRFCANCandidateV1FixtureProvider{snapshot: GreeVRFCANCandidateV1ProviderSnapshot{Profile: GreeVRFCANCandidateV1Profile, Admitted: true, Class8: 0xf7, Unit7: 8, Opcode7: 0x12, OpaqueCells: []GreeVRFCANCandidateV1OpaqueCell{{Cell: 0x13}}}}, want: ErrGreeVRFCANCandidateV1NotAdmitted},
		{name: "opaque field exceeds seven bits", provider: greeVRFCANCandidateV1FixtureProvider{snapshot: GreeVRFCANCandidateV1ProviderSnapshot{Profile: GreeVRFCANCandidateV1Profile, Admitted: true, Class8: 0xf7, Opaque7: 0xff, Unit7: 8, Opcode7: 0x58, OpaqueCells: []GreeVRFCANCandidateV1OpaqueCell{{Cell: 0x13}}}}, want: ErrGreeVRFCANCandidateV1NotAdmitted},
		{name: "unknown cell", provider: greeVRFCANCandidateV1FixtureProvider{snapshot: GreeVRFCANCandidateV1ProviderSnapshot{Profile: GreeVRFCANCandidateV1Profile, Admitted: true, Class8: 0xf7, Unit7: 8, Opcode7: 0x58, OpaqueCells: []GreeVRFCANCandidateV1OpaqueCell{{Cell: 0x1c}}}}, want: ErrGreeVRFCANCandidateV1NotAdmitted},
		{name: "duplicate cell", provider: greeVRFCANCandidateV1FixtureProvider{snapshot: GreeVRFCANCandidateV1ProviderSnapshot{Profile: GreeVRFCANCandidateV1Profile, Admitted: true, Class8: 0xf7, Unit7: 8, Opcode7: 0x58, OpaqueCells: []GreeVRFCANCandidateV1OpaqueCell{{Cell: 0x13}, {Cell: 0x13}}}}, want: ErrGreeVRFCANCandidateV1NotAdmitted},
		{name: "invalid raw DLC", provider: greeVRFCANCandidateV1FixtureProvider{snapshot: GreeVRFCANCandidateV1ProviderSnapshot{Profile: GreeVRFCANCandidateV1Profile, Admitted: true, Class8: 0xf7, Unit7: 8, Opcode7: 0x58, OpaqueCells: []GreeVRFCANCandidateV1OpaqueCell{{Cell: 0x13}}, RawEvidence: []GreeVRFCANCandidateV1RawEvidence{{DLC: 3, RawDLC: 4}}}}, want: ErrGreeVRFCANCandidateV1NotAdmitted},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			runtime, err := NewGreeVRFCANCandidateV1Runtime(testCase.provider)
			if testCase.provider == nil {
				if !errors.Is(err, testCase.want) {
					t.Fatalf("NewGreeVRFCANCandidateV1Runtime() error = %v", err)
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

func greeVRFCANCandidateV1FixtureSnapshot() GreeVRFCANCandidateV1ProviderSnapshot {
	return GreeVRFCANCandidateV1ProviderSnapshot{
		Profile:  GreeVRFCANCandidateV1Profile,
		Admitted: true,
		Class8:   0xf7,
		Opaque7:  1,
		Unit7:    8,
		Opcode7:  0x58,
		OpaqueCells: []GreeVRFCANCandidateV1OpaqueCell{
			{Cell: 0x13, Value: 0xa4},
			{Cell: 0x14, Value: 0xa5},
		},
		RawEvidence: []GreeVRFCANCandidateV1RawEvidence{{Identifier: 0x1ee04458, DLC: 3, RawDLC: 3, Data: [8]byte{0x5d, 0xa4, 0xa5}}},
	}
}
