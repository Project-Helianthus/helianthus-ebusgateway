package mcp

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	eebusruntime "github.com/Project-Helianthus/helianthus-eebusreg"
	"github.com/Project-Helianthus/helianthus-eebusreg/eebusraw"
)

const (
	eebusV1ContractName           = "helianthus-eebus-mcp"
	eebusV1MaxCollectionSize      = 16384
	eebusV1PublicPseudonymContext = "helianthus-eebus-mcp/public-pseudonym/v1"
	eebusV1PseudonymDomainRuntime = "runtime"
	eebusV1PseudonymDomainService = "service"
	eebusV1PseudonymDomainSession = "session"
	eebusV1PseudonymDomainRemote  = "session-remote"
	eebusV1PseudonymDomainDevice  = "device"
	eebusV1PseudonymDomainEntity  = "entity"
	eebusV1PseudonymDomainFeature = "feature"
	eebusV1PseudonymDomainUseCase = "usecase"
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

type eebusV1RawServiceDataV1 struct {
	IDDigest        string                              `json:"id_digest"`
	SKI             string                              `json:"ski"`
	SHIPID          *string                             `json:"ship_id,omitempty"`
	Kind            eebusruntime.ServiceKindV1          `json:"kind"`
	Visible         bool                                `json:"visible"`
	Paired          bool                                `json:"paired"`
	Name            *string                             `json:"name,omitempty"`
	Identifier      *string                             `json:"identifier,omitempty"`
	Brand           *string                             `json:"brand,omitempty"`
	Type            *string                             `json:"type,omitempty"`
	Model           *string                             `json:"model,omitempty"`
	SecondaryDigest *string                             `json:"secondary_digest,omitempty"`
	Opaque          *[]eebusruntime.OpaqueObservationV1 `json:"opaque,omitempty"`
}

type eebusV1RawSessionDataV1 struct {
	IDDigest  string                              `json:"id_digest"`
	ID        string                              `json:"id"`
	RemoteSKI string                              `json:"remote_ski"`
	State     eebusruntime.ObservedSessionStateV1 `json:"state"`
	Since     time.Time                           `json:"since"`
	Opaque    *[]eebusruntime.OpaqueObservationV1 `json:"opaque,omitempty"`
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

func eebusV1ProjectSnapshot(value any, pseudonymKey []byte) (eebusV1Projection, error) {
	snapshot, ok := value.(eebusruntime.SnapshotV1)
	if !ok {
		return eebusV1Projection{}, errors.New("provider returned an unsupported snapshot type")
	}
	return eebusV1ProjectSnapshotForBoundary(snapshot, eebusV1PublicBoundary, pseudonymKey)
}

func eebusV1ProjectSnapshotForBoundary(
	source eebusruntime.SnapshotV1,
	boundary eebusV1Boundary,
	pseudonymKey []byte,
) (eebusV1Projection, error) {
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
	keyed, err := eebusV1KeyPublicSnapshot(redacted, pseudonymKey, runtime)
	if err != nil {
		return eebusV1Projection{}, err
	}
	projection.ContentHash = keyed.Meta.DataHash
	projection.Snapshot = keyed
	return projection, nil
}

func eebusV1KeyPublicSnapshot(
	source eebusruntime.RedactedSnapshotV1,
	pseudonymKey []byte,
	runtime eebusV1RuntimeStatusDataV1,
) (eebusV1SnapshotDataV1, error) {
	runtimeID, err := eebusV1KeyRedactedID(pseudonymKey, eebusV1PseudonymDomainRuntime, source.Meta.Runtime)
	if err != nil {
		return eebusV1SnapshotDataV1{}, errors.New("pseudonymize public runtime identity")
	}
	services := make([]eebusruntime.RedactedServiceV1, len(source.Services))
	for index, service := range source.Services {
		service.ID, err = eebusV1KeyRedactedID(pseudonymKey, eebusV1PseudonymDomainService, service.ID)
		if err != nil {
			return eebusV1SnapshotDataV1{}, errors.New("pseudonymize public service identity")
		}
		services[index] = service
	}
	sort.Slice(services, func(left, right int) bool {
		return services[left].ID.Digest < services[right].ID.Digest
	})

	sessions := make([]eebusruntime.RedactedSessionV1, len(source.Sessions))
	for index, session := range source.Sessions {
		session.ID, err = eebusV1KeyRedactedID(pseudonymKey, eebusV1PseudonymDomainSession, session.ID)
		if err != nil {
			return eebusV1SnapshotDataV1{}, errors.New("pseudonymize public session identity")
		}
		session.Remote, err = eebusV1KeyRedactedID(pseudonymKey, eebusV1PseudonymDomainRemote, session.Remote)
		if err != nil {
			return eebusV1SnapshotDataV1{}, errors.New("pseudonymize public session remote identity")
		}
		sessions[index] = session
	}
	sort.Slice(sessions, func(left, right int) bool {
		return sessions[left].ID.Digest < sessions[right].ID.Digest
	})

	devices := make([]eebusruntime.RedactedDeviceV1, len(source.Devices))
	for index, device := range source.Devices {
		device.ID, err = eebusV1KeyRedactedID(pseudonymKey, eebusV1PseudonymDomainDevice, device.ID)
		if err != nil {
			return eebusV1SnapshotDataV1{}, errors.New("pseudonymize public device identity")
		}
		device.Entities = append([]eebusruntime.RedactedEntityV1(nil), device.Entities...)
		for entityIndex, entity := range device.Entities {
			entity.ID, err = eebusV1KeyRedactedID(pseudonymKey, eebusV1PseudonymDomainEntity, entity.ID)
			if err != nil {
				return eebusV1SnapshotDataV1{}, errors.New("pseudonymize public entity identity")
			}
			entity.Features = append([]eebusruntime.RedactedFeatureV1(nil), entity.Features...)
			for featureIndex, feature := range entity.Features {
				feature.ID, err = eebusV1KeyRedactedID(pseudonymKey, eebusV1PseudonymDomainFeature, feature.ID)
				if err != nil {
					return eebusV1SnapshotDataV1{}, errors.New("pseudonymize public feature identity")
				}
				entity.Features[featureIndex] = feature
			}
			sort.Slice(entity.Features, func(left, right int) bool {
				return entity.Features[left].ID.Digest < entity.Features[right].ID.Digest
			})
			device.Entities[entityIndex] = entity
		}
		sort.Slice(device.Entities, func(left, right int) bool {
			return device.Entities[left].ID.Digest < device.Entities[right].ID.Digest
		})
		device.UseCaseClaims = append([]eebusruntime.RedactedUseCaseV1(nil), device.UseCaseClaims...)
		for useCaseIndex, useCase := range device.UseCaseClaims {
			useCase.ID, err = eebusV1KeyRedactedID(pseudonymKey, eebusV1PseudonymDomainUseCase, useCase.ID)
			if err != nil {
				return eebusV1SnapshotDataV1{}, errors.New("pseudonymize public usecase identity")
			}
			device.UseCaseClaims[useCaseIndex] = useCase
		}
		sort.Slice(device.UseCaseClaims, func(left, right int) bool {
			return device.UseCaseClaims[left].ID.Digest < device.UseCaseClaims[right].ID.Digest
		})
		devices[index] = device
	}
	sort.Slice(devices, func(left, right int) bool {
		return devices[left].ID.Digest < devices[right].ID.Digest
	})

	result := eebusV1SnapshotDataV1{
		Meta: eebusV1SnapshotMetaDataV1{
			Contract: source.Meta.Contract, Runtime: runtimeID, MaskTier: string(source.Meta.MaskTier),
			CapturedAt:    eebusV1Timestamp(source.Meta.CapturedAt),
			DataTimestamp: eebusV1Timestamp(source.Meta.DataTimestamp),
		},
		Status: runtime, Pairing: append([]eebusraw.PairingState(nil), source.Pairing...),
		Services: services, Sessions: sessions, Topology: eebusV1TopologyDataV1{Devices: devices},
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		return eebusV1SnapshotDataV1{}, errors.New("marshal keyed public snapshot")
	}
	_, result.Meta.DataHash, err = eebusV1CanonicalHashJSON(encoded, "/meta/captured_at", "/meta/data_hash")
	if err != nil {
		return eebusV1SnapshotDataV1{}, errors.New("hash keyed public snapshot")
	}
	return result, nil
}

func eebusV1KeyRedactedID(key []byte, domain string, source eebusraw.RedactedID) (eebusraw.RedactedID, error) {
	if len(key) != sha256.Size || domain == "" || source.Digest == "" {
		return eebusraw.RedactedID{}, errors.New("invalid public pseudonym input")
	}
	if err := source.Validate(); err != nil {
		return eebusraw.RedactedID{}, errors.New("invalid redacted source identity")
	}
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte(eebusV1PublicPseudonymContext))
	_, _ = mac.Write([]byte{0})
	_, _ = mac.Write([]byte(domain))
	_, _ = mac.Write([]byte{0})
	_, _ = mac.Write([]byte(source.Kind))
	_, _ = mac.Write([]byte{0})
	_, _ = mac.Write([]byte(source.Digest))
	return eebusraw.RedactedID{
		Kind: source.Kind, Masked: source.Masked, Digest: "sha256:" + hex.EncodeToString(mac.Sum(nil)),
	}, nil
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
	sum := sha256.Sum256([]byte(identity))
	return base64.RawURLEncoding.EncodeToString(sum[:])
}

func eebusV1RawSessionDigest(session eebusruntime.SessionV1) string {
	sum := sha256.Sum256([]byte(session.ID))
	return base64.RawURLEncoding.EncodeToString(sum[:])
}

func eebusV1ProjectRawServices(services []eebusruntime.ServiceV1) []eebusV1RawServiceDataV1 {
	result := make([]eebusV1RawServiceDataV1, len(services))
	for index, service := range services {
		result[index] = eebusV1RawServiceDataV1{
			IDDigest: eebusV1RawServiceDigest(service),
			SKI:      service.SKI, SHIPID: service.SHIPID, Kind: service.Kind,
			Visible: service.Visible, Paired: service.Paired, Name: service.Name,
			Identifier: service.Identifier, Brand: service.Brand, Type: service.Type,
			Model: service.Model, SecondaryDigest: service.SecondaryDigest, Opaque: service.Opaque,
		}
	}
	return result
}

func eebusV1ProjectRawSessions(sessions []eebusruntime.SessionV1) []eebusV1RawSessionDataV1 {
	result := make([]eebusV1RawSessionDataV1, len(sessions))
	for index, session := range sessions {
		result[index] = eebusV1RawSessionDataV1{
			IDDigest: eebusV1RawSessionDigest(session),
			ID:       session.ID, RemoteSKI: session.RemoteSKI, State: session.State,
			Since: session.Since, Opaque: session.Opaque,
		}
	}
	return result
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
