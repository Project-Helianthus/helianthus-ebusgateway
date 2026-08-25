package mcp

import (
	"context"
	"errors"
)

var (
	ErrGrowattProtocolIIV1ProviderUnavailable = errors.New("growatt Protocol II provider is unavailable")
	ErrGrowattProtocolIIV1NotAdmitted         = errors.New("growatt Protocol II identity is not admitted")
)

// GrowattProtocolIIV1ProviderSnapshot is provider-private. RawIdentity is
// deliberately excluded from the MCP result.
type GrowattProtocolIIV1ProviderSnapshot struct {
	Profile                 string
	Family                  string
	OfflineIdentityAdmitted bool
	OutboundAllowed         bool
	RawIdentity             []uint16
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
	return GrowattProtocolIIV1Result{Profile: GrowattProtocolIIV1Profile, Disposition: "OFFLINE_IDENTITY_ADMITTED", Family: snapshot.Family, IdentityQualified: true, IdentityRedacted: true, OutboundAllowed: false}, nil
}

func validGrowattProtocolIIV1Family(family string) bool {
	switch family {
	case "MAX", "MID", "MAC":
		return true
	default:
		return false
	}
}
