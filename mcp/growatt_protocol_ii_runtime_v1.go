package mcp

import (
	"context"
	"errors"

	modbusreg "github.com/Project-Helianthus/helianthus-modbusreg"
)

var (
	ErrGrowattProtocolIIV1ProviderUnavailable = errors.New("growatt Protocol II provider is unavailable")
	ErrGrowattProtocolIIV1NotAdmitted         = errors.New("growatt Protocol II identity is not admitted")
)

// GrowattProtocolIIV1ProviderSnapshot carries one caller-selected offline
// identity tuple. It neither discovers a profile nor permits another request.
type GrowattProtocolIIV1ProviderSnapshot struct {
	Profile                 string
	OfflineIdentityAdmitted bool
	Identity                modbusreg.GrowattProtocolIIIdentityInput
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
	if snapshot.Profile != GrowattProtocolIIV1Profile || !snapshot.OfflineIdentityAdmitted {
		return GrowattProtocolIIV1Result{}, ErrGrowattProtocolIIV1NotAdmitted
	}
	observation, err := modbusreg.DecodeGrowattProtocolIIIdentity(snapshot.Identity)
	if err != nil {
		return GrowattProtocolIIV1Result{}, ErrGrowattProtocolIIV1NotAdmitted
	}
	return growattProtocolIIV1ResultFromObservation(observation), nil
}
