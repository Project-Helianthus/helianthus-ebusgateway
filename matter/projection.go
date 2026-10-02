// Package matter implements the pure, read-only Matter target projection.
package matter

import (
	"bytes"
	"encoding/json"
	"errors"
	"math/big"
	"sort"

	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/packs/evse"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/packs/infrastructure"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/packs/pv"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/packs/storage"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/packs/thermal"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/projection"
)

var (
	matterCurrentMinimum = big.NewInt(-4611686018427387904)
	matterCurrentMaximum = big.NewInt(4611686018427387904)
)

const (
	Contract                                       = "helianthus.gateway.matter-projection/v1"
	TargetID                   semreg.TargetID     = "matter.data-model"
	TargetVersion              semreg.VersionLabel = "1.7-draft-ballot-0.9+29b4768a513cf566011ab8cd60df1bc495204953"
	MappingRevision            semreg.Uint64       = "1"
	MatterModelCommit                              = "29b4768a513cf566011ab8cd60df1bc495204953"
	MatterSpecCommit                               = "214e40c9d51cfe89050eae68ca5b76238fcfa332"
	DocsSemanticCommit                             = "30f5e5c79ac6da3a7c7c10c990599906d1dfd0cb"
	DocsSemanticContractPath                       = "api/v1/targets/matter-1.7-ballot-0.9-v1.json"
	DocsSemanticContractSHA256                     = "5ae81d5e0971d25ead08f982fdf31caf47ada4838d0ee6f2b3e719d11a6df39c"
)

// ElementKind identifies a Matter data-model element. This projection contains
// attributes only; it never creates commands, endpoints, fabric state, or I/O.
type ElementKind string

const ElementAttribute ElementKind = "attribute"

// TargetElement is a typed, numeric Matter identity. Numeric values are pinned
// to connectedhomeip data_model/1.7 at MatterModelCommit.
type TargetElement struct {
	DeviceType uint32      `json:"device_type"`
	Cluster    uint32      `json:"cluster"`
	Kind       ElementKind `json:"kind"`
	ElementID  uint32      `json:"element_id"`
}

// AttributeValue is the deliberately small target value surface for v1.
type AttributeValue struct {
	Milliampere string `json:"milliamperes"`
}

// TargetAttribute is one detached projected Matter attribute.
type TargetAttribute struct {
	Element TargetElement  `json:"element"`
	Value   AttributeValue `json:"value"`
}

// Document is the deterministic, non-runtime Matter target payload.
type Document struct {
	Contract        string              `json:"contract"`
	TargetID        semreg.TargetID     `json:"target_id"`
	TargetVersion   semreg.VersionLabel `json:"target_version"`
	MappingRevision semreg.Uint64       `json:"mapping_revision"`
	Attributes      []TargetAttribute   `json:"attributes"`
}

// Result contains detached target data and the validated SemReg accounting report.
type Result struct {
	Document Document                    `json:"document"`
	Report   projection.ProjectionReport `json:"report"`
}

var (
	// Electrical Sensor device type, Electrical Power Measurement cluster, and
	// ActiveCurrent attribute. Their decimal forms make accidental string IDs
	// impossible at call sites: 0x0510, 0x0090, and 0x0005 respectively.
	evseActiveCurrent = TargetElement{DeviceType: 0x0510, Cluster: 0x0090, Kind: ElementAttribute, ElementID: 0x0005}
	errInput          = errors.New("matter projection input is not eligible")
)

// Manifest derives the complete ledger from the five pinned pack catalogs.
// It never retains metadata owned by SemReg.
func Manifest() projection.ProjectionManifest {
	metadata := []semreg.PackMetadata{evse.Metadata(), infrastructure.Metadata(), pv.Metadata(), storage.Metadata(), thermal.Metadata()}
	packs := make([]semreg.PackRef, 0, len(metadata))
	for _, item := range metadata {
		packs = append(packs, item.Pack)
	}
	sort.Slice(packs, func(i, j int) bool { return string(packs[i].ID) < string(packs[j].ID) })
	return projection.ProjectionManifest{TargetID: TargetID, TargetVersion: TargetVersion, KernelVersion: semreg.ContractKernelV1, PackVersions: packs, MappingRevision: MappingRevision}
}

// Ledger accounts for every metadata field, capability, and operation once.
// SemReg services deliberately have no projection item kind and are excluded.
func Ledger() ([]projection.RequestedItem, []projection.ProjectionDisposition) {
	metadata := []semreg.PackMetadata{evse.Metadata(), infrastructure.Metadata(), pv.Metadata(), storage.Metadata(), thermal.Metadata()}
	requested := make([]projection.RequestedItem, 0)
	for _, pack := range metadata {
		for _, field := range pack.Fields {
			requested = append(requested, projection.RequestedItem{Kind: projection.ItemFact, ItemID: field.Ref.ID})
		}
		for _, capability := range pack.Capabilities {
			requested = append(requested, projection.RequestedItem{Kind: projection.ItemCapability, ItemID: capability.Ref.ID})
		}
		for _, operation := range pack.Operations {
			requested = append(requested, projection.RequestedItem{Kind: projection.ItemOperation, ItemID: operation.Ref.ID})
		}
	}
	sort.Slice(requested, func(i, j int) bool {
		if requested[i].Kind != requested[j].Kind {
			return requested[i].Kind < requested[j].Kind
		}
		return requested[i].ItemID < requested[j].ItemID
	})
	dispositions := make([]projection.ProjectionDisposition, len(requested))
	for i, item := range requested {
		reason := semreg.DefinitionID("matter.unmapped.v1")
		dispositions[i] = projection.ProjectionDisposition{
			Kind:       item.Kind,
			ItemID:     item.ItemID,
			Outcome:    projection.ProjectionUnknown,
			Reason:     &reason,
			SourceKeys: []semreg.FactKey{},
			Loss: []projection.LossDetail{{
				Kind:        projection.LossPolicy,
				SourceItems: []semreg.DefinitionID{item.ItemID},
				Description: "no target lookup, inferred fallback, authority, intent, route, or operation is created",
				Reversible:  false,
			}},
		}
	}
	return requested, dispositions
}

// Project converts one already-evaluated immutable SemReg snapshot. It has no
// clock, retained state, endpoint allocator, route, authority, intent, command,
// dispatch, or native transport dependency.
func Project(snapshot semreg.Snapshot, evaluation semreg.EvaluationView) (Result, error) {
	if err := snapshot.Validate(); err != nil {
		return Result{}, err
	}
	if err := evaluation.Validate(); err != nil {
		return Result{}, err
	}
	if evaluation.SnapshotID != snapshot.SnapshotID || evaluation.Revisions != snapshot.Revisions {
		return Result{}, &semreg.Error{ID: semreg.RevisionConflict, Detail: "matter projection evaluation"}
	}
	if err := validateEvaluationCorrespondence(snapshot, evaluation); err != nil {
		return Result{}, err
	}
	if hasConflict(snapshot) {
		return Result{}, &semreg.Error{ID: semreg.PreconditionFailed, Detail: "matter projection conflict"}
	}
	if err := eligibleEvaluation(evaluation); err != nil {
		return Result{}, err
	}

	requested, dispositions := Ledger()
	attributes, replacement, err := mapEVSEActiveCurrent(snapshot, evaluation)
	if err != nil {
		return Result{}, err
	}
	if replacement != nil {
		for i := range dispositions {
			if dispositions[i].Kind == projection.ItemFact && dispositions[i].ItemID == "evse.ac.current" {
				dispositions[i] = *replacement
				break
			}
		}
	}
	report, err := projection.Project(snapshot, Manifest(), requested, dispositions, nil)
	if err != nil {
		return Result{}, err
	}
	document := Document{Contract: Contract, TargetID: TargetID, TargetVersion: TargetVersion, MappingRevision: MappingRevision, Attributes: attributes}
	return detach(Result{Document: document, Report: report})
}

func validateEvaluationCorrespondence(snapshot semreg.Snapshot, evaluation semreg.EvaluationView) error {
	candidates := make(map[semreg.CandidateID]semreg.Uint64)
	for _, envelope := range snapshot.Facts {
		for _, candidate := range envelope.Candidates {
			candidates[candidate.CandidateID] = candidate.Revision
		}
	}
	if len(candidates) != len(evaluation.Facts) {
		return &semreg.Error{ID: semreg.PreconditionFailed, Detail: "matter projection evaluation completeness"}
	}
	for _, evaluated := range evaluation.Facts {
		revision, ok := candidates[evaluated.CandidateID]
		if !ok {
			return &semreg.Error{ID: semreg.DanglingReference, Detail: "matter projection evaluated candidate"}
		}
		if revision != evaluated.CandidateRevision {
			return &semreg.Error{ID: semreg.RevisionConflict, Detail: "matter projection candidate revision"}
		}
	}
	return nil
}

func hasConflict(snapshot semreg.Snapshot) bool {
	for _, fact := range snapshot.Facts {
		if len(fact.Conflicts) != 0 {
			return true
		}
	}
	return false
}

func eligibleEvaluation(evaluation semreg.EvaluationView) error {
	for _, item := range evaluation.Facts {
		if item.Freshness != semreg.FreshnessFresh || item.EffectiveAvailability != semreg.AvailabilityAvailable {
			return &semreg.Error{ID: semreg.PreconditionFailed, Detail: "matter projection freshness or availability"}
		}
	}
	return nil
}

func mapEVSEActiveCurrent(snapshot semreg.Snapshot, evaluation semreg.EvaluationView) ([]TargetAttribute, *projection.ProjectionDisposition, error) {
	eligible := make(map[semreg.CandidateID]struct{}, len(evaluation.Facts))
	for _, item := range evaluation.Facts {
		eligible[item.CandidateID] = struct{}{}
	}
	var found *semreg.FactEnvelope
	var candidate *semreg.FactCandidate
	for i := range snapshot.Facts {
		fact := &snapshot.Facts[i]
		if fact.Key.PackID != "helianthus.pack.evse" || fact.Key.PackVersion != "1.0.0" || fact.Key.FactID != "evse.ac.current" {
			continue
		}
		for j := range fact.Candidates {
			current := &fact.Candidates[j]
			if _, ok := eligible[current.CandidateID]; !ok {
				continue
			}
			if current.Quality.Assertion != semreg.AssertionObserved || current.Quality.Qualification != semreg.QualificationQualified || current.Quality.Promotion != semreg.PromotionPromoted || current.Quality.Validity != semreg.ValidityGood || current.Quality.Availability != semreg.AvailabilityAvailable || current.Quality.Freshness != semreg.FreshnessFresh {
				return nil, nil, &semreg.Error{ID: semreg.PreconditionFailed, Detail: "matter projection current qualification"}
			}
			if err := evse.New().ValidateFact(fact.Key, current.Value); err != nil {
				return nil, nil, err
			}
			if found != nil {
				return nil, nil, &semreg.Error{ID: semreg.PreconditionFailed, Detail: "matter projection ambiguous current"}
			}
			found, candidate = fact, current
		}
	}
	if found == nil {
		return []TargetAttribute{}, nil, nil
	}
	ma, err := amperesToMilliampere(*candidate.Value)
	if err != nil {
		return nil, nil, err
	}
	reason := semreg.DefinitionID("matter.phase_endpoint_identity_loss.v1")
	disposition := projection.ProjectionDisposition{
		Kind:       projection.ItemFact,
		ItemID:     "evse.ac.current",
		Outcome:    projection.ProjectionTransformed,
		SourceKeys: []semreg.FactKey{found.Key},
		Reason:     &reason,
		Loss: []projection.LossDetail{{
			Kind:        projection.LossUnit,
			SourceItems: []semreg.DefinitionID{"evse.ac.current"},
			Description: "amperes are represented as integral milliamperes only when exact and representable",
			Reversible:  false,
		}},
	}
	return []TargetAttribute{{Element: evseActiveCurrent, Value: AttributeValue{Milliampere: ma}}}, &disposition, nil
}

func amperesToMilliampere(value semreg.Value) (string, error) {
	if value.Kind != semreg.ValueQuantity || value.Quantity == nil || value.Quantity.Unit != "unit.ampere" {
		return "", errInput
	}
	coefficient, ok := new(big.Int).SetString(value.Quantity.Number.Coefficient, 10)
	if !ok {
		return "", errInput
	}
	exponent := int(value.Quantity.Number.Exponent10) + 3
	if exponent < 0 {
		divisor := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(-exponent)), nil)
		quotient, remainder := new(big.Int), new(big.Int)
		quotient.QuoRem(coefficient, divisor, remainder)
		if remainder.Sign() != 0 {
			return "", &semreg.Error{ID: semreg.PreconditionFailed, Detail: "matter projection current precision"}
		}
		if quotient.Cmp(matterCurrentMinimum) < 0 || quotient.Cmp(matterCurrentMaximum) > 0 {
			return "", &semreg.Error{ID: semreg.PreconditionFailed, Detail: "matter projection current range"}
		}
		return quotient.String(), nil
	}
	result := new(big.Int).Mul(coefficient, new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(exponent)), nil))
	if result.Cmp(matterCurrentMinimum) < 0 || result.Cmp(matterCurrentMaximum) > 0 {
		return "", &semreg.Error{ID: semreg.PreconditionFailed, Detail: "matter projection current range"}
	}
	return result.String(), nil
}

func detach(result Result) (Result, error) {
	raw, err := json.Marshal(result)
	if err != nil {
		return Result{}, err
	}
	var out Result
	if err := json.Unmarshal(raw, &out); err != nil {
		return Result{}, err
	}
	return out, nil
}

// CanonicalJSON returns byte-stable target/report output for a successful projection.
func CanonicalJSON(result Result) ([]byte, error) {
	if result.Document.Contract != Contract || result.Document.TargetID != TargetID || result.Document.TargetVersion != TargetVersion || result.Document.MappingRevision != MappingRevision {
		return nil, errInput
	}
	if err := result.Report.Validate(); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(result)
	if err != nil {
		return nil, err
	}
	var compact bytes.Buffer
	if err := json.Compact(&compact, raw); err != nil {
		return nil, err
	}
	return compact.Bytes(), nil
}
