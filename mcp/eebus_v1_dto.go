package mcp

import (
	"errors"
	"fmt"
	"strings"
	"time"

	eebusruntime "github.com/Project-Helianthus/helianthus-eebusreg"
	"github.com/Project-Helianthus/helianthus-eebusreg/eebusraw"
)

const (
	eebusV1ContractName      = "helianthus-eebus-mcp"
	eebusV1MaxCollectionSize = 16384
)

type eebusV1ContractV1 struct {
	Name  string `json:"name"`
	Major int    `json:"major"`
	Minor int    `json:"minor"`
}

var eebusV1Contract = eebusV1ContractV1{Name: eebusV1ContractName, Major: 1, Minor: 0}

type eebusV1IdentityDigestV1 = eebusraw.RedactedID

type eebusV1DegradationDataV1 struct {
	Reason string `json:"reason"`
	Since  string `json:"since"`
}

type eebusV1RuntimeStatusDataV1 struct {
	State       string                    `json:"state"`
	Degradation *eebusV1DegradationDataV1 `json:"degradation,omitempty"`
}

type eebusV1ServicesListDataV1 struct {
	Services any `json:"services"`
}

type eebusV1SessionsListDataV1 struct {
	Sessions any `json:"sessions"`
}

type eebusV1PairingStatusDataV1 struct {
	Pairing any `json:"pairing"`
}

type eebusV1TopologyDataV1 struct {
	Devices []eebusruntime.RedactedDeviceV1 `json:"devices"`
}

type eebusV1RawTopologyDataV1 struct {
	Devices  []eebusruntime.DeviceV1            `json:"devices"`
	Entities []eebusruntime.EntityV1            `json:"entities"`
	Features []eebusruntime.FeatureV1           `json:"features"`
	UseCases []eebusruntime.UseCaseV1           `json:"usecases"`
	Opaque   []eebusruntime.OpaqueObservationV1 `json:"opaque"`
}

type eebusV1SnapshotMetaDataV1 struct {
	Contract      string                  `json:"contract"`
	Runtime       eebusV1IdentityDigestV1 `json:"runtime"`
	MaskTier      string                  `json:"mask_tier"`
	CapturedAt    string                  `json:"captured_at"`
	DataTimestamp string                  `json:"data_timestamp"`
	DataHash      string                  `json:"data_hash"`
}

type eebusV1SnapshotDataV1 struct {
	Meta     eebusV1SnapshotMetaDataV1        `json:"meta"`
	Status   eebusV1RuntimeStatusDataV1       `json:"status"`
	Pairing  []eebusraw.PairingState          `json:"pairing"`
	Services []eebusruntime.RedactedServiceV1 `json:"services"`
	Sessions []eebusruntime.RedactedSessionV1 `json:"sessions"`
	Topology eebusV1TopologyDataV1            `json:"topology"`
}

type eebusV1Projection struct {
	Snapshot      eebusV1SnapshotDataV1
	Source        *eebusruntime.SnapshotV1
	Runtime       eebusV1RuntimeStatusDataV1
	DataTimestamp string
	RuntimeKey    string
	ContentHash   string
	Boundary      eebusV1Boundary
}

func eebusV1ProjectSnapshot(value any, _ []byte) (eebusV1Projection, error) {
	snapshot, ok := value.(eebusruntime.SnapshotV1)
	if !ok {
		return eebusV1Projection{}, errors.New("provider returned an unsupported snapshot type")
	}
	return eebusV1ProjectSnapshotForBoundary(snapshot, eebusV1PublicBoundary)
}

func eebusV1ProjectSnapshotForBoundary(source eebusruntime.SnapshotV1, boundary eebusV1Boundary) (eebusV1Projection, error) {
	if err := source.Validate(); err != nil {
		return eebusV1Projection{}, fmt.Errorf("validate provider snapshot: %w", err)
	}
	source = source.Clone()
	runtime := eebusV1ProjectRuntime(source.Status)
	projection := eebusV1Projection{
		Runtime:       runtime,
		DataTimestamp: eebusV1Timestamp(source.Meta.DataTimestamp),
		RuntimeKey:    string(source.Meta.Runtime.Kind) + "\x00" + source.Meta.Runtime.Digest,
		ContentHash:   source.Meta.DataHash,
		Boundary:      boundary,
	}
	if boundary == eebusV1OperatorBoundary {
		projection.Source = &source
		return projection, nil
	}
	if boundary != eebusV1PublicBoundary {
		return eebusV1Projection{}, errors.New("unknown eeBUS authorization boundary")
	}
	redacted, err := eebusruntime.BuildRedactedSnapshotV1(source)
	if err != nil {
		return eebusV1Projection{}, fmt.Errorf("build redacted snapshot: %w", err)
	}
	projection.ContentHash = redacted.Meta.DataHash
	projection.Snapshot = eebusV1SnapshotDataV1{
		Meta: eebusV1SnapshotMetaDataV1{
			Contract:      redacted.Meta.Contract,
			Runtime:       redacted.Meta.Runtime,
			MaskTier:      string(redacted.Meta.MaskTier),
			CapturedAt:    eebusV1Timestamp(redacted.Meta.CapturedAt),
			DataTimestamp: eebusV1Timestamp(redacted.Meta.DataTimestamp),
			DataHash:      redacted.Meta.DataHash,
		},
		Status:   runtime,
		Pairing:  append([]eebusraw.PairingState(nil), redacted.Pairing...),
		Services: append([]eebusruntime.RedactedServiceV1(nil), redacted.Services...),
		Sessions: append([]eebusruntime.RedactedSessionV1(nil), redacted.Sessions...),
		Topology: eebusV1TopologyDataV1{
			Devices: append([]eebusruntime.RedactedDeviceV1(nil), redacted.Devices...),
		},
	}
	return projection, nil
}

func (projection eebusV1Projection) capturedSnapshot() any {
	if projection.Boundary == eebusV1OperatorBoundary && projection.Source != nil {
		return projection.Source.Clone()
	}
	return projection.Snapshot
}

func eebusV1ProjectRuntime(source eebusruntime.RuntimeObservationV1) eebusV1RuntimeStatusDataV1 {
	result := eebusV1RuntimeStatusDataV1{State: string(source.State)}
	if source.Degradation != nil {
		result.Degradation = &eebusV1DegradationDataV1{
			Reason: string(source.Degradation.Reason),
			Since:  eebusV1Timestamp(source.Degradation.Since),
		}
	}
	return result
}

func eebusV1RawServiceDigest(service eebusruntime.ServiceV1) string {
	identity := strings.Join([]string{
		service.SKI,
		eebusV1OptionalString(service.SHIPID),
		eebusV1OptionalString(service.Identifier),
		string(service.Kind),
	}, "\x00")
	id, err := eebusraw.RedactID(eebusraw.IDKindPeer, identity)
	if err != nil {
		return ""
	}
	return id.Digest
}

func eebusV1RawSessionDigest(session eebusruntime.SessionV1) string {
	id, err := eebusraw.RedactID(eebusraw.IDKindSession, session.ID)
	if err != nil {
		return ""
	}
	return id.Digest
}

func eebusV1OptionalString(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func eebusV1ValidateCollectionSizes(lengths ...int) error {
	for _, length := range lengths {
		if length > eebusV1MaxCollectionSize {
			return errors.New("provider collection exceeds maximum size")
		}
	}
	return nil
}

func eebusV1Timestamp(value time.Time) string {
	return value.UTC().Format(time.RFC3339Nano)
}
