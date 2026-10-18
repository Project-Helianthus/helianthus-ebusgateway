// Package semanticview builds detached, transport-neutral SemReg read values.
package semanticview

import (
	"encoding/json"
	"reflect"
	"sort"

	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/packs/evse"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/packs/infrastructure"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/packs/pv"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/packs/storage"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/packs/thermal"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/projection"
)

const reasonPrefix = "semantic.read.reason."

// Input is captured coherently by the caller, including any publication safety
// withholding. Build performs no acquisition or publication and reads no clock.
type Input struct {
	AssetID           semreg.AssetID
	Snapshot          *semreg.Snapshot
	Evaluation        *semreg.EvaluationView
	Selections        []semreg.Selection
	UnavailableReason string
}

type View struct {
	Assets []Asset `json:"assets"`
}
type Asset struct {
	AssetID           semreg.AssetID                   `json:"asset_id"`
	Snapshot          *semreg.Snapshot                 `json:"snapshot,omitempty"`
	Evaluation        *semreg.EvaluationView           `json:"evaluation,omitempty"`
	Projection        *projection.ProjectionReport     `json:"projection,omitempty"`
	Selections        []semreg.Selection               `json:"selections"`
	UnavailableReason string                           `json:"unavailable_reason,omitempty"`
	Availability      *projection.PackAvailabilityView `json:"-"`
}

func failure(id semreg.ErrorID) error { return &semreg.Error{ID: id, Detail: "semantic snapshot read"} }

// Build consumes complete public-safe captures. Native safety and lifecycle
// gates must be applied before capture; unsafe assets use publication_withheld.
// A nil provider is an error, never an empty successful publication.
func Build(metadata *semreg.PackMetadataRegistry, inputs []Input) (View, error) {
	if metadata == nil {
		return View{}, failure(semreg.DefinitionOwnerMissing)
	}
	if len(inputs) < 1 || len(inputs) > 32 {
		return View{}, failure(semreg.BoundsExceeded)
	}
	result := View{Assets: make([]Asset, 0, len(inputs))}
	seen := map[semreg.AssetID]bool{}
	var context *semreg.EvaluationContext
	for _, input := range inputs {
		if input.AssetID.Validate() != nil || len(input.AssetID) > 256 || seen[input.AssetID] {
			return View{}, failure(semreg.InvalidValue)
		}
		seen[input.AssetID] = true
		asset := Asset{AssetID: input.AssetID, Selections: []semreg.Selection{}}
		if input.UnavailableReason != "" {
			switch input.UnavailableReason {
			case reasonPrefix + "asset_unknown", reasonPrefix + "snapshot_unavailable", reasonPrefix + "source_unavailable", reasonPrefix + "publication_withheld":
			default:
				return View{}, failure(semreg.InvalidValue)
			}
			if input.Snapshot != nil || input.Evaluation != nil || len(input.Selections) != 0 {
				return View{}, failure(semreg.InvalidValue)
			}
			asset.UnavailableReason = input.UnavailableReason
			result.Assets = append(result.Assets, asset)
			continue
		}
		if input.Snapshot == nil || input.Evaluation == nil {
			return View{}, failure(semreg.MissingMember)
		}
		snapshot, err := clone(*input.Snapshot)
		if err != nil {
			return View{}, failure(semreg.InvalidValue)
		}
		evaluation, err := clone(*input.Evaluation)
		if err != nil {
			return View{}, failure(semreg.InvalidValue)
		}
		if snapshot.AssetID != input.AssetID {
			return View{}, failure(semreg.RevisionConflict)
		}
		if context != nil && !reflect.DeepEqual(*context, evaluation.Context) {
			return View{}, failure(semreg.RevisionConflict)
		}
		context = &evaluation.Context
		availability, err := projection.DerivePackAvailability(metadata, snapshot, evaluation)
		if err != nil {
			return View{}, failure(semreg.ErrorIdentifier(err))
		}
		report, err := project(metadata, snapshot, evaluation, availability)
		if err != nil {
			return View{}, failure(semreg.ErrorIdentifier(err))
		}
		selections := make([]semreg.Selection, 0, len(input.Selections))
		for _, selection := range input.Selections {
			if err := semreg.ValidateSelection(snapshot, evaluation, selection); err != nil {
				return View{}, failure(semreg.ErrorIdentifier(err))
			}
			detached, err := clone(selection)
			if err != nil {
				return View{}, failure(semreg.InvalidValue)
			}
			selections = append(selections, detached)
		}
		sort.Slice(selections, func(i, j int) bool { return selectionKey(selections[i]) < selectionKey(selections[j]) })
		for i := 1; i < len(selections); i++ {
			if selectionKey(selections[i-1]) == selectionKey(selections[i]) {
				return View{}, failure(semreg.DuplicateKey)
			}
		}
		asset.Snapshot = &snapshot
		asset.Evaluation = &evaluation
		asset.Projection = &report
		asset.Availability = &availability
		asset.Selections = selections
		result.Assets = append(result.Assets, asset)
	}
	sort.Slice(result.Assets, func(i, j int) bool { return result.Assets[i].AssetID < result.Assets[j].AssetID })
	raw, err := json.Marshal(result)
	if err != nil {
		return View{}, failure(semreg.InvalidValue)
	}
	if len(raw) > 4*1024*1024 {
		return View{}, failure(semreg.BoundsExceeded)
	}
	return result, nil
}

func clone[T semreg.Record](value T) (T, error) {
	var out T
	raw, err := semreg.CanonicalJSON(value)
	if err != nil {
		return out, err
	}
	return semreg.Decode[T](raw)
}
func selectionKey(s semreg.Selection) string {
	key, _ := semreg.CanonicalJSON(s.Key)
	return string(key) + "\x00" + string(s.PolicyID) + "\x00" + string(s.PolicyVersion)
}

func project(metadata *semreg.PackMetadataRegistry, s semreg.Snapshot, e semreg.EvaluationView, a projection.PackAvailabilityView) (projection.ProjectionReport, error) {
	applicable := map[semreg.PackRef]bool{}
	items := map[projection.RequestedItem]bool{}
	for _, f := range s.Facts {
		applicable[semreg.PackRef{ID: f.Key.PackID, Version: f.Key.PackVersion}] = true
		items[projection.RequestedItem{Kind: projection.ItemFact, ItemID: f.Key.FactID}] = true
	}
	for _, v := range s.Services {
		applicable[v.Definition.Pack] = true
	}
	for _, v := range s.Capabilities {
		applicable[v.Definition.Pack] = true
		items[projection.RequestedItem{Kind: projection.ItemCapability, ItemID: v.Definition.ID}] = true
	}
	manifest := projection.ProjectionManifest{TargetID: "target:semantic-snapshot-read", TargetVersion: "1", KernelVersion: semreg.ContractKernelV1, MappingRevision: "1", PackVersions: []semreg.PackRef{}}
	for _, provider := range []semreg.PackMetadata{evse.Metadata(), infrastructure.Metadata(), pv.Metadata(), storage.Metadata(), thermal.Metadata()} {
		m := provider
		if !applicable[m.Pack] {
			continue
		}
		for _, f := range m.Fields {
			items[projection.RequestedItem{Kind: projection.ItemFact, ItemID: f.Ref.ID}] = true
		}
		for _, c := range m.Capabilities {
			items[projection.RequestedItem{Kind: projection.ItemCapability, ItemID: c.Ref.ID}] = true
		}
	}
	// Preserve numeric version ordering for exact pack references.
	for pack := range applicable {
		manifest.PackVersions = append(manifest.PackVersions, pack)
	}
	sort.Slice(manifest.PackVersions, func(i, j int) bool {
		x, y := manifest.PackVersions[i], manifest.PackVersions[j]
		if x.ID != y.ID {
			return x.ID < y.ID
		}
		return versionLess(x.Version, y.Version)
	})
	requested := make([]projection.RequestedItem, 0, len(items))
	for item := range items {
		requested = append(requested, item)
	}
	sort.Slice(requested, func(i, j int) bool {
		if requested[i].Kind != requested[j].Kind {
			return requested[i].Kind < requested[j].Kind
		}
		return requested[i].ItemID < requested[j].ItemID
	})
	type fieldKey struct {
		pack semreg.PackRef
		id   semreg.DefinitionID
	}
	fields := map[fieldKey]semreg.DefinitionRef{}
	for _, provider := range []semreg.PackMetadata{evse.Metadata(), infrastructure.Metadata(), pv.Metadata(), storage.Metadata(), thermal.Metadata()} {
		for _, field := range provider.Fields {
			fields[fieldKey{field.Ref.Pack, field.Ref.ID}] = field.Ref
		}
	}
	services := map[semreg.ServiceInstanceID]semreg.DefinitionRef{}
	for _, service := range s.Services {
		services[service.InstanceID] = service.Definition
	}
	dispositions := make([]projection.ProjectionDisposition, 0, len(requested))
	for _, item := range requested {
		d := projection.ProjectionDisposition{Kind: item.Kind, ItemID: item.ItemID, SourceKeys: []semreg.FactKey{}, Loss: []projection.LossDetail{}, Outcome: projection.ProjectionUnknown}
		reason := semreg.DefinitionID(reasonPrefix + "value_unknown")
		reasons := []semreg.DefinitionID{}
		if item.Kind == projection.ItemFact {
			// Availability and exactness are tracked per complete source key: one
			// available sibling must not hide an unavailable or withdrawn one.
			hasValue, unsupported, found := false, true, false
			allKeysAvailable, allKeysExact, anyUnknown := true, true, false
			for _, f := range s.Facts {
				if f.Key.FactID != item.ItemID {
					continue
				}
				d.SourceKeys = append(d.SourceKeys, f.Key)
				keyAvailable, keyExact := false, false
				for _, c := range f.Candidates {
					found = true
					hasValue = hasValue || c.Value != nil
					unsupported = unsupported && c.Quality.Qualification == semreg.QualificationUnsupported
					anyUnknown = anyUnknown || (c.Value != nil && c.Quality.Qualification == semreg.QualificationUnknown)
					reasons = append(reasons, c.Quality.Reasons...)
					for _, v := range e.Facts {
						if v.CandidateID == c.CandidateID && v.CandidateRevision == c.Revision {
							ok := v.EffectiveAvailability != semreg.AvailabilityUnavailable && v.EffectiveAvailability != semreg.AvailabilityWithdrawn
							keyAvailable = keyAvailable || ok
							// Exact needs a value that is qualified, promoted, valid (good, or
							// suspect as the kernel allows for promoted state) and available.
							keyExact = keyExact || (ok && c.Value != nil && c.Quality.Qualification == semreg.QualificationQualified &&
								c.Quality.Promotion == semreg.PromotionPromoted && (c.Quality.Validity == semreg.ValidityGood || c.Quality.Validity == semreg.ValiditySuspect))
						}
					}
				}
				allKeysAvailable = allKeysAvailable && keyAvailable
				allKeysExact = allKeysExact && keyExact
			}
			switch {
			case found && !allKeysAvailable:
				// Any unavailable key withholds the whole disposition (SourceKeys names
				// every key); the withheld key's value is never reported as exact.
				d.Outcome = projection.ProjectionWithheld
				reason = reasonPrefix + "fact_unavailable"
			case found && unsupported:
				d.Outcome = projection.ProjectionUnsupported
			case hasValue && allKeysExact:
				d.Outcome = projection.ProjectionExact
			case hasValue && anyUnknown:
				d.Outcome = projection.ProjectionUnknown
			case hasValue:
				// A valued candidate that is not qualified, promoted and valid stays
				// audit evidence only.
				d.Outcome = projection.ProjectionWithheld
				reason = reasonPrefix + "capability_unqualified"
			}
		} else {
			reason = reasonPrefix + "capability_unknown"
			// Multiple native instances remain intact; a definition is admitted only
			// when every instance is qualified and available for this read.
			found, qualified, unsupported, unknown, available := false, true, true, false, true
			for _, p := range a.Packs {
				for _, c := range p.Capabilities {
					if c.Definition.ID != item.ItemID {
						continue
					}
					found = true
					qualified = qualified && c.Qualification == semreg.QualificationQualified
					unsupported = unsupported && c.Qualification == semreg.QualificationUnsupported
					unknown = unknown || c.Qualification == semreg.QualificationUnknown
					available = available && c.Availability != semreg.AvailabilityUnavailable && c.Availability != semreg.AvailabilityWithdrawn
				}
			}
			// Supporting keys are joined through the pack's declared service and
			// capability ownership, including the complete dimensioned FactKey.
			for _, f := range s.Facts {
				matched := false
				field, known := fields[fieldKey{semreg.PackRef{ID: f.Key.PackID, Version: f.Key.PackVersion}, f.Key.FactID}]
				for _, c := range s.Capabilities {
					if known && c.Definition.ID == item.ItemID && metadata.FieldMatches(field, services[c.ServiceInstance], c.Definition) {
						matched = true
					}
				}
				if matched {
					d.SourceKeys = append(d.SourceKeys, f.Key)
					for _, candidate := range f.Candidates {
						reasons = append(reasons, candidate.Quality.Reasons...)
					}
				}
			}
			if found {
				switch {
				case !available:
					d.Outcome = projection.ProjectionWithheld
					reason = reasonPrefix + "capability_unavailable"
				case unsupported:
					d.Outcome = projection.ProjectionUnsupported
				case unknown:
				case !qualified:
					d.Outcome = projection.ProjectionWithheld
					reason = reasonPrefix + "capability_unqualified"
				default:
					d.Outcome = projection.ProjectionExact
				}
			}
		}
		for _, r := range reasons {
			if r == "unknown_pending_std_01" {
				d.Outcome = projection.ProjectionUnknown
				reason = r
				break
			}
		}
		if d.Outcome != projection.ProjectionExact {
			if len(reasons) > 0 && reason != "unknown_pending_std_01" {
				sort.Slice(reasons, func(i, j int) bool { return reasons[i] < reasons[j] })
				reason = reasons[0]
			}
			d.Reason = &reason
		}
		dispositions = append(dispositions, d)
	}
	return projection.Project(s, manifest, requested, dispositions, nil)
}

func versionLess(a, b semreg.SemanticVersion) bool {
	// Semantic versions have validated integer components; compare numerically
	// without narrowing arbitrary-sized components to machine integers.
	var x, y [3]string
	copy(x[:], splitVersion(string(a)))
	copy(y[:], splitVersion(string(b)))
	for i := range x {
		if len(x[i]) != len(y[i]) {
			return len(x[i]) < len(y[i])
		}
		if x[i] != y[i] {
			return x[i] < y[i]
		}
	}
	return false
}
func splitVersion(s string) []string {
	var parts []string
	start := 0
	for i := range s {
		if s[i] == '.' {
			parts = append(parts, s[start:i])
			start = i + 1
		}
	}
	return append(parts, s[start:])
}
