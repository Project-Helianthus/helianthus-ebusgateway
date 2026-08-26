package mcp

import (
	"context"
	"errors"
)

var (
	ErrGrowattProtocolIIV1ProviderUnavailable = errors.New("growatt Protocol II provider is unavailable")
	ErrGrowattProtocolIIV1NotAdmitted         = errors.New("growatt Protocol II identity is not admitted")
)

// GrowattProtocolIIV1ProviderSnapshot carries the caller-selected native
// identity words validated by this runtime.
type GrowattProtocolIIV1ProviderSnapshot struct {
	Profile                 string
	Family                  string
	OfflineIdentityAdmitted bool
	OutboundAllowed         bool
	UnitID                  byte
	IdentitySlices          []GrowattProtocolIIV1NativeIdentitySlice
}

type GrowattProtocolIIV1SnapshotProvider interface {
	GrowattProtocolIIV1Snapshot(context.Context) (GrowattProtocolIIV1ProviderSnapshot, error)
}

type GrowattProtocolIIV1Runtime struct {
	provider GrowattProtocolIIV1SnapshotProvider
}

func NewGrowattProtocolIIV1Runtime(provider GrowattProtocolIIV1SnapshotProvider) (*GrowattProtocolIIV1Runtime, error) {
	if provider == nil {
		return nil, ErrGrowattProtocolIIV1ProviderUnavailable
	}
	return &GrowattProtocolIIV1Runtime{provider: provider}, nil
}

func (runtime *GrowattProtocolIIV1Runtime) GrowattProtocolIIV1(ctx context.Context) (GrowattProtocolIIV1Result, error) {
	if runtime == nil || runtime.provider == nil {
		return GrowattProtocolIIV1Result{}, ErrGrowattProtocolIIV1ProviderUnavailable
	}
	snapshot, err := runtime.provider.GrowattProtocolIIV1Snapshot(ctx)
	if err != nil {
		return GrowattProtocolIIV1Result{}, err
	}
	if snapshot.Profile != GrowattProtocolIIV1Profile || !validGrowattProtocolIIV1Family(snapshot.Family) || !snapshot.OfflineIdentityAdmitted {
		return GrowattProtocolIIV1Result{}, ErrGrowattProtocolIIV1NotAdmitted
	}
	if snapshot.UnitID == 0 || snapshot.UnitID > 247 || !validGrowattProtocolIISlices(snapshot.IdentitySlices) {
		return GrowattProtocolIIV1Result{}, ErrGrowattProtocolIIV1NotAdmitted
	}
	return GrowattProtocolIIV1Result{Profile: GrowattProtocolIIV1Profile, Disposition: "OFFLINE_IDENTITY_ADMITTED", Family: snapshot.Family, IdentityQualified: true, NativeIdentity: GrowattProtocolIIV1NativeIdentity{Family: snapshot.Family, UnitID: snapshot.UnitID, Slices: cloneGrowattProtocolIISlices(snapshot.IdentitySlices)}}, nil
}

func validGrowattProtocolIISlices(slices []GrowattProtocolIIV1NativeIdentitySlice) bool {
	want := [...]struct {
		offset uint16
		words  int
	}{{9, 6}, {23, 5}, {43, 1}, {82, 2}, {88, 1}}
	if len(slices) != len(want) {
		return false
	}
	for i, expected := range want {
		if slices[i].Offset != expected.offset || len(slices[i].Words) != expected.words {
			return false
		}
	}
	return true
}

func cloneGrowattProtocolIISlices(slices []GrowattProtocolIIV1NativeIdentitySlice) []GrowattProtocolIIV1NativeIdentitySlice {
	cloned := make([]GrowattProtocolIIV1NativeIdentitySlice, len(slices))
	for i, slice := range slices {
		cloned[i] = GrowattProtocolIIV1NativeIdentitySlice{Offset: slice.Offset, Words: append([]uint16(nil), slice.Words...)}
	}
	return cloned
}

func validGrowattProtocolIIV1Family(family string) bool {
	switch family {
	case "MAX", "MID", "MAC":
		return true
	default:
		return false
	}
}
