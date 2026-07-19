package mcp

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"
)

const (
	eebusV1ContractName = "helianthus-eebus-mcp"
	eebusV1MaskTier     = "redacted"
	eebusV1AuthScope    = "eebus.raw.read"

	eebusV1SourceSnapshotContract = "helianthus.eebus.runtime.raw-snapshot.v1"
	eebusV1SourceRedactedValue    = "[redacted]"
)

type eebusV1ContractV1 struct {
	Name  string `json:"name"`
	Major int    `json:"major"`
	Minor int    `json:"minor"`
}

var eebusV1Contract = eebusV1ContractV1{Name: eebusV1ContractName, Major: 1, Minor: 0}

type eebusV1IdentityDigestV1 struct {
	Kind   string `json:"kind"`
	Digest string `json:"digest"`
}

type eebusV1EvidenceDescriptorV1 struct {
	Kind          string `json:"kind"`
	Digest        string `json:"digest"`
	Size          int64  `json:"size"`
	DataTimestamp string `json:"data_timestamp"`
}

type eebusV1DegradationDataV1 struct {
	Reason string `json:"reason"`
	Since  string `json:"since"`
}

type eebusV1RuntimeStatusDataV1 struct {
	State       string                    `json:"state"`
	Degradation *eebusV1DegradationDataV1 `json:"degradation,omitempty"`
}

type eebusV1ServiceDataV1 struct {
	ID       eebusV1IdentityDigestV1       `json:"id"`
	Kind     string                        `json:"kind"`
	Visible  bool                          `json:"visible"`
	Paired   bool                          `json:"paired"`
	Evidence []eebusV1EvidenceDescriptorV1 `json:"evidence,omitempty"`
}

type eebusV1ServicesListDataV1 struct {
	Services []eebusV1ServiceDataV1 `json:"services"`
}

type eebusV1SessionDataV1 struct {
	ID       eebusV1IdentityDigestV1       `json:"id"`
	Remote   eebusV1IdentityDigestV1       `json:"remote"`
	State    string                        `json:"state"`
	Since    string                        `json:"since,omitempty"`
	Evidence []eebusV1EvidenceDescriptorV1 `json:"evidence,omitempty"`
}

type eebusV1SessionsListDataV1 struct {
	Sessions []eebusV1SessionDataV1 `json:"sessions"`
}

type eebusV1PairingDataV1 struct {
	Remote   eebusV1IdentityDigestV1       `json:"remote"`
	State    string                        `json:"state"`
	Since    string                        `json:"since,omitempty"`
	Evidence []eebusV1EvidenceDescriptorV1 `json:"evidence,omitempty"`
}

type eebusV1PairingStatusDataV1 struct {
	Pairing []eebusV1PairingDataV1 `json:"pairing"`
}

type eebusV1FeatureDataV1 struct {
	ID       eebusV1IdentityDigestV1       `json:"id"`
	Role     string                        `json:"role"`
	Evidence []eebusV1EvidenceDescriptorV1 `json:"evidence,omitempty"`
}

type eebusV1EntityDataV1 struct {
	ID       eebusV1IdentityDigestV1       `json:"id"`
	Features []eebusV1FeatureDataV1        `json:"features"`
	Evidence []eebusV1EvidenceDescriptorV1 `json:"evidence,omitempty"`
}

type eebusV1UseCaseClaimDataV1 struct {
	ID       eebusV1IdentityDigestV1       `json:"id"`
	Evidence []eebusV1EvidenceDescriptorV1 `json:"evidence,omitempty"`
}

type eebusV1DeviceDataV1 struct {
	ID            eebusV1IdentityDigestV1       `json:"id"`
	Entities      []eebusV1EntityDataV1         `json:"entities"`
	UseCaseClaims []eebusV1UseCaseClaimDataV1   `json:"usecase_claims"`
	Evidence      []eebusV1EvidenceDescriptorV1 `json:"evidence,omitempty"`
}

type eebusV1TopologyDataV1 struct {
	Devices []eebusV1DeviceDataV1 `json:"devices"`
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
	Meta     eebusV1SnapshotMetaDataV1     `json:"meta"`
	Status   eebusV1RuntimeStatusDataV1    `json:"status"`
	Pairing  []eebusV1PairingDataV1        `json:"pairing"`
	Services []eebusV1ServiceDataV1        `json:"services"`
	Sessions []eebusV1SessionDataV1        `json:"sessions"`
	Topology eebusV1TopologyDataV1         `json:"topology"`
	Evidence []eebusV1EvidenceDescriptorV1 `json:"evidence,omitempty"`
}

type eebusV1Projection struct {
	Snapshot      eebusV1SnapshotDataV1
	Runtime       eebusV1RuntimeStatusDataV1
	DataTimestamp string
	RuntimeKey    string
}

type eebusV1SourceIdentity struct {
	Kind   string `json:"kind"`
	Masked string `json:"masked"`
	Digest string `json:"digest,omitempty"`
}

type eebusV1SourceEvidence struct {
	Kind          string            `json:"kind"`
	Digest        string            `json:"digest"`
	Size          int64             `json:"size"`
	DataTimestamp time.Time         `json:"data_timestamp"`
	Unknown       []json.RawMessage `json:"unknown,omitempty"`
}

type eebusV1SourceSnapshotMeta struct {
	Contract      string                `json:"contract"`
	Runtime       eebusV1SourceIdentity `json:"runtime"`
	LocalSKI      eebusV1SourceIdentity `json:"local_ski"`
	MaskTier      string                `json:"mask_tier"`
	CapturedAt    time.Time             `json:"captured_at"`
	DataTimestamp time.Time             `json:"data_timestamp"`
	DataHash      string                `json:"data_hash,omitempty"`
}

type eebusV1SourceDegradation struct {
	Reason string    `json:"reason"`
	Since  time.Time `json:"since"`
}

type eebusV1SourceRuntimeObservation struct {
	State       string                    `json:"state"`
	Degradation *eebusV1SourceDegradation `json:"degradation,omitempty"`
}

type eebusV1SourcePairing struct {
	Remote  eebusV1SourceIdentity   `json:"remote"`
	State   string                  `json:"state"`
	Since   time.Time               `json:"since,omitempty"`
	Raw     []eebusV1SourceEvidence `json:"raw,omitempty"`
	Unknown []json.RawMessage       `json:"unknown,omitempty"`
}

type eebusV1SourceService struct {
	ID      eebusV1SourceIdentity   `json:"id"`
	Kind    string                  `json:"kind"`
	Visible bool                    `json:"visible"`
	Paired  bool                    `json:"paired"`
	Raw     []eebusV1SourceEvidence `json:"raw,omitempty"`
	Unknown []json.RawMessage       `json:"unknown,omitempty"`
}

type eebusV1SourceSession struct {
	ID      eebusV1SourceIdentity   `json:"id"`
	Remote  eebusV1SourceIdentity   `json:"remote"`
	State   string                  `json:"state"`
	Since   time.Time               `json:"since,omitempty"`
	Raw     []eebusV1SourceEvidence `json:"raw,omitempty"`
	Unknown []json.RawMessage       `json:"unknown,omitempty"`
}

type eebusV1SourceFeature struct {
	ID      eebusV1SourceIdentity   `json:"id"`
	Role    string                  `json:"role"`
	Raw     []eebusV1SourceEvidence `json:"raw,omitempty"`
	Unknown []json.RawMessage       `json:"unknown,omitempty"`
}

type eebusV1SourceEntity struct {
	ID       eebusV1SourceIdentity   `json:"id"`
	Features []eebusV1SourceFeature  `json:"features,omitempty"`
	Raw      []eebusV1SourceEvidence `json:"raw,omitempty"`
	Unknown  []json.RawMessage       `json:"unknown,omitempty"`
}

type eebusV1SourceUseCaseClaim struct {
	ID      eebusV1SourceIdentity   `json:"id"`
	Raw     []eebusV1SourceEvidence `json:"raw,omitempty"`
	Unknown []json.RawMessage       `json:"unknown,omitempty"`
}

type eebusV1SourceDevice struct {
	ID            eebusV1SourceIdentity       `json:"id"`
	Entities      []eebusV1SourceEntity       `json:"entities,omitempty"`
	UseCaseClaims []eebusV1SourceUseCaseClaim `json:"usecase_claims,omitempty"`
	Raw           []eebusV1SourceEvidence     `json:"raw,omitempty"`
	Unknown       []json.RawMessage           `json:"unknown,omitempty"`
}

type eebusV1SourceTopology struct {
	Devices []eebusV1SourceDevice `json:"devices,omitempty"`
}

type eebusV1SourceSnapshot struct {
	Meta     eebusV1SourceSnapshotMeta       `json:"meta"`
	Status   eebusV1SourceRuntimeObservation `json:"status"`
	Pairing  []eebusV1SourcePairing          `json:"pairing,omitempty"`
	Services []eebusV1SourceService          `json:"services,omitempty"`
	Sessions []eebusV1SourceSession          `json:"sessions,omitempty"`
	Topology eebusV1SourceTopology           `json:"topology"`
	Raw      []eebusV1SourceEvidence         `json:"raw,omitempty"`
}

type eebusV1Pseudonymizer struct {
	key        []byte
	runtimeKey string
}

func newEEBusV1Pseudonymizer(key []byte, runtime eebusV1SourceIdentity) (*eebusV1Pseudonymizer, error) {
	if len(key) != sha256.Size {
		return nil, errors.New("pseudonym key must contain 32 bytes")
	}
	if err := eebusV1ValidateSourceIdentity(runtime); err != nil {
		return nil, fmt.Errorf("runtime identity: %w", err)
	}
	return &eebusV1Pseudonymizer{
		key:        append([]byte(nil), key...),
		runtimeKey: runtime.Kind + "\x00" + runtime.Digest,
	}, nil
}

func (p *eebusV1Pseudonymizer) identity(kind string, source eebusV1SourceIdentity) (eebusV1IdentityDigestV1, error) {
	if p == nil || len(p.key) != sha256.Size {
		return eebusV1IdentityDigestV1{}, errors.New("pseudonymizer is unavailable")
	}
	if err := eebusV1ValidateSourceIdentity(source); err != nil {
		return eebusV1IdentityDigestV1{}, err
	}
	mac := hmac.New(sha256.New, p.key)
	for _, component := range []string{
		eebusV1ContractName,
		"identity-v1",
		kind,
		p.runtimeKey,
		source.Kind,
		source.Digest,
	} {
		_, _ = mac.Write([]byte(component))
		_, _ = mac.Write([]byte{0})
	}
	return eebusV1IdentityDigestV1{
		Kind:   kind,
		Digest: base64.RawURLEncoding.EncodeToString(mac.Sum(nil)),
	}, nil
}

func eebusV1ProjectSnapshot(value any, pseudonymKey []byte) (eebusV1Projection, error) {
	source, err := eebusV1DecodeSourceSnapshot(value)
	if err != nil {
		return eebusV1Projection{}, err
	}
	if err := eebusV1ValidateSourceSnapshot(source); err != nil {
		return eebusV1Projection{}, err
	}
	pseudonyms, err := newEEBusV1Pseudonymizer(pseudonymKey, source.Meta.Runtime)
	if err != nil {
		return eebusV1Projection{}, err
	}

	runtime, err := eebusV1ProjectRuntime(source.Status)
	if err != nil {
		return eebusV1Projection{}, err
	}
	runtimeID, err := pseudonyms.identity("runtime", source.Meta.Runtime)
	if err != nil {
		return eebusV1Projection{}, err
	}
	services, err := eebusV1ProjectServices(source.Services, pseudonyms)
	if err != nil {
		return eebusV1Projection{}, err
	}
	sessions, err := eebusV1ProjectSessions(source.Sessions, pseudonyms)
	if err != nil {
		return eebusV1Projection{}, err
	}
	pairing, err := eebusV1ProjectPairing(source.Pairing, pseudonyms)
	if err != nil {
		return eebusV1Projection{}, err
	}
	topology, err := eebusV1ProjectTopology(source.Topology, pseudonyms)
	if err != nil {
		return eebusV1Projection{}, err
	}
	evidence, err := eebusV1ProjectEvidence(source.Raw)
	if err != nil {
		return eebusV1Projection{}, err
	}

	snapshot := eebusV1SnapshotDataV1{
		Meta: eebusV1SnapshotMetaDataV1{
			Contract:      eebusV1SourceSnapshotContract,
			Runtime:       runtimeID,
			MaskTier:      eebusV1MaskTier,
			CapturedAt:    eebusV1Timestamp(source.Meta.CapturedAt),
			DataTimestamp: eebusV1Timestamp(source.Meta.DataTimestamp),
		},
		Status:   runtime,
		Pairing:  pairing,
		Services: services,
		Sessions: sessions,
		Topology: topology,
		Evidence: evidence,
	}
	encoded, err := json.Marshal(snapshot)
	if err != nil {
		return eebusV1Projection{}, errors.New("marshal projected snapshot")
	}
	_, contentHash, err := eebusV1CanonicalHashJSON(encoded, "/meta/data_hash", "/meta/captured_at")
	if err != nil {
		return eebusV1Projection{}, fmt.Errorf("hash projected snapshot: %w", err)
	}
	snapshot.Meta.DataHash = contentHash
	return eebusV1Projection{
		Snapshot:      snapshot,
		Runtime:       runtime,
		DataTimestamp: snapshot.Meta.DataTimestamp,
		RuntimeKey:    pseudonyms.runtimeKey,
	}, nil
}

func eebusV1DecodeSourceSnapshot(value any) (eebusV1SourceSnapshot, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return eebusV1SourceSnapshot{}, errors.New("marshal provider snapshot")
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	var snapshot eebusV1SourceSnapshot
	if err := decoder.Decode(&snapshot); err != nil {
		return eebusV1SourceSnapshot{}, errors.New("decode provider snapshot")
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return eebusV1SourceSnapshot{}, errors.New("provider snapshot contains trailing data")
	}
	return snapshot, nil
}

func eebusV1ValidateSourceSnapshot(snapshot eebusV1SourceSnapshot) error {
	if snapshot.Meta.Contract != eebusV1SourceSnapshotContract || snapshot.Meta.MaskTier != eebusV1MaskTier {
		return errors.New("invalid source snapshot metadata")
	}
	if err := eebusV1ValidateSourceIdentityKind(snapshot.Meta.Runtime, "peer", "local-ski"); err != nil {
		return err
	}
	if err := eebusV1ValidateSourceIdentityKind(snapshot.Meta.LocalSKI, "local-ski"); err != nil {
		return err
	}
	if err := eebusV1ValidateSourceTimestamp(snapshot.Meta.CapturedAt, true); err != nil {
		return err
	}
	if err := eebusV1ValidateSourceTimestamp(snapshot.Meta.DataTimestamp, true); err != nil {
		return err
	}
	if snapshot.Meta.DataHash != "" && !eebusV1ValidSourceDigest(snapshot.Meta.DataHash) {
		return errors.New("invalid source snapshot hash")
	}
	if err := eebusV1ValidateSourceRuntime(snapshot.Status); err != nil {
		return err
	}
	visibleService := false
	for _, service := range snapshot.Services {
		visibleService = visibleService || service.Visible
		if err := eebusV1ValidateSourceIdentityKind(service.ID, "peer"); err != nil {
			return err
		}
		if service.Kind != "local" && service.Kind != "remote" {
			return errors.New("service contains an unsupported kind")
		}
		if len(service.Unknown) != 0 {
			return errors.New("service contains unknown source fields")
		}
		if err := eebusV1ValidateSourceEvidence(service.Raw); err != nil {
			return err
		}
	}
	if !visibleService && snapshot.Status.State == "ready" {
		return errors.New("ready runtime has no visible services")
	}
	for _, session := range snapshot.Sessions {
		if err := eebusV1ValidateSourceIdentityKind(session.ID, "session"); err != nil {
			return err
		}
		if err := eebusV1ValidateSourceIdentityKind(session.Remote, "remote-ski", "peer"); err != nil {
			return err
		}
		if session.State != "connecting" && session.State != "connected" && session.State != "disconnected" && session.State != "degraded" {
			return errors.New("session contains an unsupported state")
		}
		if err := eebusV1ValidateSourceTimestamp(session.Since, false); err != nil || len(session.Unknown) != 0 {
			return errors.New("session contains an unsupported value")
		}
		if err := eebusV1ValidateSourceEvidence(session.Raw); err != nil {
			return err
		}
	}
	for _, item := range snapshot.Pairing {
		if err := eebusV1ValidateSourceIdentityKind(item.Remote, "remote-ski", "peer"); err != nil {
			return err
		}
		if item.State != "unpaired" && item.State != "paired" && item.State != "denied" {
			return errors.New("pairing contains an unsupported state")
		}
		if err := eebusV1ValidateSourceTimestamp(item.Since, false); err != nil || len(item.Unknown) != 0 {
			return errors.New("pairing contains an unsupported value")
		}
		if err := eebusV1ValidateSourceEvidence(item.Raw); err != nil {
			return err
		}
	}
	for _, device := range snapshot.Topology.Devices {
		if err := eebusV1ValidateSourceIdentityKind(device.ID, "peer"); err != nil {
			return err
		}
		if len(device.Unknown) != 0 {
			return errors.New("device contains unknown source fields")
		}
		if err := eebusV1ValidateSourceEvidence(device.Raw); err != nil {
			return err
		}
		for _, entity := range device.Entities {
			if err := eebusV1ValidateSourceIdentityKind(entity.ID, "peer"); err != nil {
				return err
			}
			if len(entity.Unknown) != 0 {
				return errors.New("entity contains unknown source fields")
			}
			if err := eebusV1ValidateSourceEvidence(entity.Raw); err != nil {
				return err
			}
			for _, feature := range entity.Features {
				if err := eebusV1ValidateSourceIdentityKind(feature.ID, "peer"); err != nil {
					return err
				}
				if (feature.Role != "client" && feature.Role != "server") || len(feature.Unknown) != 0 {
					return errors.New("feature contains an unsupported value")
				}
				if err := eebusV1ValidateSourceEvidence(feature.Raw); err != nil {
					return err
				}
			}
		}
		for _, claim := range device.UseCaseClaims {
			if err := eebusV1ValidateSourceIdentityKind(claim.ID, "peer"); err != nil {
				return err
			}
			if len(claim.Unknown) != 0 {
				return errors.New("use-case claim contains unknown source fields")
			}
			if err := eebusV1ValidateSourceEvidence(claim.Raw); err != nil {
				return err
			}
		}
	}
	return eebusV1ValidateSourceEvidence(snapshot.Raw)
}

func eebusV1ValidateSourceRuntime(source eebusV1SourceRuntimeObservation) error {
	switch source.State {
	case "stopped", "starting", "ready", "degraded", "shutdown":
	default:
		return errors.New("unsupported runtime state")
	}
	if source.State == "degraded" && source.Degradation == nil {
		return errors.New("degraded runtime lacks details")
	}
	if source.State != "degraded" && source.Degradation != nil {
		return errors.New("runtime degradation is inconsistent")
	}
	if source.Degradation == nil {
		return nil
	}
	switch source.Degradation.Reason {
	case "missing-discovery", "denied-trust", "remote-disconnect", "certificate-unavailable", "no-visible-services", "no-data":
	default:
		return errors.New("unsupported degradation reason")
	}
	return eebusV1ValidateSourceTimestamp(source.Degradation.Since, true)
}

func eebusV1ValidateSourceIdentity(source eebusV1SourceIdentity) error {
	if source.Masked != eebusV1SourceRedactedValue || !eebusV1ValidSourceDigest(source.Digest) {
		return errors.New("invalid redacted source identity")
	}
	switch source.Kind {
	case "local-ski", "remote-ski", "certificate-fingerprint", "peer", "session":
		return nil
	default:
		return errors.New("unsupported source identity kind")
	}
}

func eebusV1ValidateSourceIdentityKind(source eebusV1SourceIdentity, allowed ...string) error {
	if err := eebusV1ValidateSourceIdentity(source); err != nil {
		return err
	}
	for _, kind := range allowed {
		if source.Kind == kind {
			return nil
		}
	}
	return errors.New("source identity has an invalid kind")
}

func eebusV1ValidateSourceEvidence(source []eebusV1SourceEvidence) error {
	for _, object := range source {
		if err := eebusV1ValidateSourceEvidenceObject(object); err != nil {
			return err
		}
	}
	return nil
}

func eebusV1ValidateSourceEvidenceObject(object eebusV1SourceEvidence) error {
	switch object.Kind {
	case "identity", "topology", "service", "session", "unknown":
	default:
		return errors.New("unsupported evidence kind")
	}
	if !eebusV1ValidSourceDigest(object.Digest) || object.Size < 0 || object.Size > eebusV1MaxSafeInteger {
		return errors.New("invalid evidence descriptor")
	}
	if err := eebusV1ValidateSourceTimestamp(object.DataTimestamp, true); err != nil {
		return err
	}
	if len(object.Unknown) != 0 {
		return errors.New("evidence contains unknown source fields")
	}
	return nil
}

func eebusV1ValidateSourceTimestamp(value time.Time, required bool) error {
	if value.IsZero() {
		if required {
			return errors.New("source timestamp is required")
		}
		return nil
	}
	if _, err := value.UTC().MarshalJSON(); err != nil {
		return errors.New("invalid source timestamp")
	}
	return nil
}

func eebusV1ValidSourceDigest(value string) bool {
	if len(value) != len("sha256:")+64 || !strings.HasPrefix(value, "sha256:") || value != strings.ToLower(value) {
		return false
	}
	_, err := hex.DecodeString(strings.TrimPrefix(value, "sha256:"))
	return err == nil
}

func eebusV1ProjectRuntime(source eebusV1SourceRuntimeObservation) (eebusV1RuntimeStatusDataV1, error) {
	switch source.State {
	case "stopped", "starting", "ready", "degraded", "shutdown":
	default:
		return eebusV1RuntimeStatusDataV1{}, errors.New("unsupported runtime state")
	}
	result := eebusV1RuntimeStatusDataV1{State: source.State}
	if source.Degradation != nil {
		result.Degradation = &eebusV1DegradationDataV1{
			Reason: source.Degradation.Reason,
			Since:  eebusV1Timestamp(source.Degradation.Since),
		}
	}
	return result, nil
}

func eebusV1ProjectServices(source []eebusV1SourceService, pseudonyms *eebusV1Pseudonymizer) ([]eebusV1ServiceDataV1, error) {
	result := make([]eebusV1ServiceDataV1, 0, len(source))
	for _, item := range source {
		id, err := pseudonyms.identity("service", item.ID)
		if err != nil {
			return nil, err
		}
		evidence, err := eebusV1ProjectEvidence(item.Raw)
		if err != nil {
			return nil, err
		}
		result = append(result, eebusV1ServiceDataV1{
			ID: id, Kind: item.Kind, Visible: item.Visible, Paired: item.Paired, Evidence: evidence,
		})
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].ID.Digest != result[j].ID.Digest {
			return result[i].ID.Digest < result[j].ID.Digest
		}
		return result[i].Kind < result[j].Kind
	})
	if err := eebusV1RejectDuplicateKeys(len(result), func(index int) string {
		return result[index].ID.Digest + "\x00" + result[index].Kind
	}); err != nil {
		return nil, err
	}
	return result, nil
}

func eebusV1ProjectSessions(source []eebusV1SourceSession, pseudonyms *eebusV1Pseudonymizer) ([]eebusV1SessionDataV1, error) {
	result := make([]eebusV1SessionDataV1, 0, len(source))
	for _, item := range source {
		id, err := pseudonyms.identity("session", item.ID)
		if err != nil {
			return nil, err
		}
		remote, err := pseudonyms.identity("remote", item.Remote)
		if err != nil {
			return nil, err
		}
		evidence, err := eebusV1ProjectEvidence(item.Raw)
		if err != nil {
			return nil, err
		}
		result = append(result, eebusV1SessionDataV1{
			ID: id, Remote: remote, State: item.State, Since: eebusV1OptionalTimestamp(item.Since), Evidence: evidence,
		})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID.Digest < result[j].ID.Digest })
	if err := eebusV1RejectDuplicateKeys(len(result), func(index int) string { return result[index].ID.Digest }); err != nil {
		return nil, err
	}
	return result, nil
}

func eebusV1ProjectPairing(source []eebusV1SourcePairing, pseudonyms *eebusV1Pseudonymizer) ([]eebusV1PairingDataV1, error) {
	result := make([]eebusV1PairingDataV1, 0, len(source))
	for _, item := range source {
		remote, err := pseudonyms.identity("remote", item.Remote)
		if err != nil {
			return nil, err
		}
		evidence, err := eebusV1ProjectEvidence(item.Raw)
		if err != nil {
			return nil, err
		}
		result = append(result, eebusV1PairingDataV1{
			Remote: remote, State: item.State, Since: eebusV1OptionalTimestamp(item.Since), Evidence: evidence,
		})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Remote.Digest < result[j].Remote.Digest })
	if err := eebusV1RejectDuplicateKeys(len(result), func(index int) string { return result[index].Remote.Digest }); err != nil {
		return nil, err
	}
	return result, nil
}

func eebusV1ProjectTopology(source eebusV1SourceTopology, pseudonyms *eebusV1Pseudonymizer) (eebusV1TopologyDataV1, error) {
	result := eebusV1TopologyDataV1{Devices: make([]eebusV1DeviceDataV1, 0, len(source.Devices))}
	for _, sourceDevice := range source.Devices {
		deviceID, err := pseudonyms.identity("device", sourceDevice.ID)
		if err != nil {
			return eebusV1TopologyDataV1{}, err
		}
		deviceEvidence, err := eebusV1ProjectEvidence(sourceDevice.Raw)
		if err != nil {
			return eebusV1TopologyDataV1{}, err
		}
		device := eebusV1DeviceDataV1{
			ID:            deviceID,
			Entities:      make([]eebusV1EntityDataV1, 0, len(sourceDevice.Entities)),
			UseCaseClaims: make([]eebusV1UseCaseClaimDataV1, 0, len(sourceDevice.UseCaseClaims)),
			Evidence:      deviceEvidence,
		}
		for _, sourceEntity := range sourceDevice.Entities {
			entityID, err := pseudonyms.identity("entity", sourceEntity.ID)
			if err != nil {
				return eebusV1TopologyDataV1{}, err
			}
			entityEvidence, err := eebusV1ProjectEvidence(sourceEntity.Raw)
			if err != nil {
				return eebusV1TopologyDataV1{}, err
			}
			entity := eebusV1EntityDataV1{
				ID: entityID, Features: make([]eebusV1FeatureDataV1, 0, len(sourceEntity.Features)), Evidence: entityEvidence,
			}
			for _, sourceFeature := range sourceEntity.Features {
				featureID, err := pseudonyms.identity("feature", sourceFeature.ID)
				if err != nil {
					return eebusV1TopologyDataV1{}, err
				}
				featureEvidence, err := eebusV1ProjectEvidence(sourceFeature.Raw)
				if err != nil {
					return eebusV1TopologyDataV1{}, err
				}
				entity.Features = append(entity.Features, eebusV1FeatureDataV1{
					ID: featureID, Role: sourceFeature.Role, Evidence: featureEvidence,
				})
			}
			sort.Slice(entity.Features, func(i, j int) bool { return entity.Features[i].ID.Digest < entity.Features[j].ID.Digest })
			if err := eebusV1RejectDuplicateKeys(len(entity.Features), func(index int) string { return entity.Features[index].ID.Digest }); err != nil {
				return eebusV1TopologyDataV1{}, err
			}
			device.Entities = append(device.Entities, entity)
		}
		sort.Slice(device.Entities, func(i, j int) bool { return device.Entities[i].ID.Digest < device.Entities[j].ID.Digest })
		if err := eebusV1RejectDuplicateKeys(len(device.Entities), func(index int) string { return device.Entities[index].ID.Digest }); err != nil {
			return eebusV1TopologyDataV1{}, err
		}
		for _, sourceClaim := range sourceDevice.UseCaseClaims {
			claimID, err := pseudonyms.identity("usecase-claim", sourceClaim.ID)
			if err != nil {
				return eebusV1TopologyDataV1{}, err
			}
			claimEvidence, err := eebusV1ProjectEvidence(sourceClaim.Raw)
			if err != nil {
				return eebusV1TopologyDataV1{}, err
			}
			device.UseCaseClaims = append(device.UseCaseClaims, eebusV1UseCaseClaimDataV1{ID: claimID, Evidence: claimEvidence})
		}
		sort.Slice(device.UseCaseClaims, func(i, j int) bool {
			return device.UseCaseClaims[i].ID.Digest < device.UseCaseClaims[j].ID.Digest
		})
		if err := eebusV1RejectDuplicateKeys(len(device.UseCaseClaims), func(index int) string { return device.UseCaseClaims[index].ID.Digest }); err != nil {
			return eebusV1TopologyDataV1{}, err
		}
		result.Devices = append(result.Devices, device)
	}
	sort.Slice(result.Devices, func(i, j int) bool { return result.Devices[i].ID.Digest < result.Devices[j].ID.Digest })
	if err := eebusV1RejectDuplicateKeys(len(result.Devices), func(index int) string { return result.Devices[index].ID.Digest }); err != nil {
		return eebusV1TopologyDataV1{}, err
	}
	return result, nil
}

func eebusV1ProjectEvidence(source []eebusV1SourceEvidence) ([]eebusV1EvidenceDescriptorV1, error) {
	if len(source) == 0 {
		return nil, nil
	}
	result := make([]eebusV1EvidenceDescriptorV1, 0, len(source))
	for _, object := range source {
		if err := eebusV1ValidateSourceEvidenceObject(object); err != nil {
			return nil, errors.New("invalid evidence descriptor")
		}
		result = append(result, eebusV1EvidenceDescriptorV1{
			Kind: object.Kind, Digest: object.Digest, Size: object.Size, DataTimestamp: eebusV1Timestamp(object.DataTimestamp),
		})
	}
	sort.Slice(result, func(i, j int) bool {
		left, right := result[i], result[j]
		if left.Kind != right.Kind {
			return left.Kind < right.Kind
		}
		if left.Digest != right.Digest {
			return left.Digest < right.Digest
		}
		if left.Size != right.Size {
			return left.Size < right.Size
		}
		return left.DataTimestamp < right.DataTimestamp
	})
	if err := eebusV1RejectDuplicateKeys(len(result), func(index int) string {
		item := result[index]
		return fmt.Sprintf("%s\x00%s\x00%020d\x00%s", item.Kind, item.Digest, item.Size, item.DataTimestamp)
	}); err != nil {
		return nil, err
	}
	return result, nil
}

func eebusV1RejectDuplicateKeys(length int, key func(int) string) error {
	for index := 1; index < length; index++ {
		if key(index-1) == key(index) {
			return errors.New("duplicate projected identity")
		}
	}
	return nil
}

func eebusV1Timestamp(value time.Time) string {
	return value.UTC().Format(time.RFC3339Nano)
}

func eebusV1OptionalTimestamp(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return eebusV1Timestamp(value)
}
