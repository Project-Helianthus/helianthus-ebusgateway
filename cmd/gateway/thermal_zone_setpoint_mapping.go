package main

import (
	"encoding/json"
	"math/big"
	"sort"
	"strconv"
	"strings"

	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/packs/thermal"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/projection"
)

// These are adapter-local native roles, not new semantic definitions or role tokens.
type thermalZoneSetpointRole string

const (
	thermalZoneSetpointHeatingManual    thermalZoneSetpointRole = "heating_manual"
	thermalZoneSetpointHeatingEffective thermalZoneSetpointRole = "heating_effective"
	thermalZoneSetpointCoolingManual    thermalZoneSetpointRole = "cooling_manual"
	thermalZoneSetpointCoolingEffective thermalZoneSetpointRole = "cooling_effective"
	thermalZoneSetpointVeto             thermalZoneSetpointRole = "veto"
	thermalZoneSetpointHoliday          thermalZoneSetpointRole = "holiday"
)

type thermalZoneSetpointSelector struct {
	PB, SB, Opcode, OperationType, Group, Instance byte
	Register                                       uint16
	Direction                                      string
}

// Association and service identity are independent of display labels and source paths.
type thermalZoneSetpointIdentity struct {
	Asset               semreg.AssetID
	Service             semreg.ServiceInstanceID
	Source              semreg.SourceID
	Epoch               semreg.SourceEpochID
	Generation          semreg.Uint64
	Binding             semreg.NativeBindingID
	BindingRevision     semreg.Uint64
	AssociationRevision semreg.Uint64
	Selector            thermalZoneSetpointSelector
}

type thermalZoneSetpointObservation struct {
	Identity      thermalZoneSetpointIdentity
	Role          thermalZoneSetpointRole
	Present       bool
	Value         semreg.Quantity
	Codec         string
	Validity      semreg.Validity
	Qualification semreg.Qualification
	Freshness     semreg.Freshness
	ReceivedAt    semreg.TimePoint
	Receipt       semreg.MonotonicPoint
	FreshForNS    semreg.Uint64
	NativePayload []byte
	Evidence      []semreg.EvidenceRef
	// The caller supplies independently qualified interval semantics. Dates alone
	// never establish activity; an unknown or elapsed interval is withheld.
	WindowKnown bool
	Active      bool
	Until       semreg.MonotonicPoint
}

// No mappings are supplied by default. Approved means the native owner has
// independently established this exact role/key, topology, unit, codec and bounds.
// Pack validation alone is not evidence of representability or qualification.
type thermalZoneSetpointMapping struct {
	Identity         thermalZoneSetpointIdentity
	Role             thermalZoneSetpointRole
	Approved         bool
	Qualification    semreg.Qualification
	Field            semreg.DefinitionRef
	Key              semreg.FactKey
	SemanticRevision semreg.Uint64
	Unit             semreg.DefinitionID
	Codec            string
	BoundsKnown      bool
	Minimum, Maximum semreg.Decimal
	Evidence         []semreg.EvidenceRef
}

type thermalZoneSetpointHistory struct {
	Observation thermalZoneSetpointObservation
	Mapping     *thermalZoneSetpointMapping
	Current     bool
	Reason      string
}

// History is a diagnostic window, not an append-only event log. Keep at most
// this many prior records, in addition to every record evaluated on this call.
const thermalZoneSetpointRetainedHistoryLimit = 64

type thermalZoneSetpointSelection struct {
	Observation      thermalZoneSetpointObservation
	Key              semreg.FactKey
	Field            semreg.DefinitionRef
	SemanticRevision semreg.Uint64
	MappingEvidence  []semreg.EvidenceRef
}

type thermalZoneSetpointInput struct {
	// Mappings describe only the caller's current bindings. Observations are a
	// partial update: omitted roles may retain a still-fresh prior selection;
	// Present=false explicitly records a missing read without refreshing it.
	Mappings         []thermalZoneSetpointMapping
	Observations     []thermalZoneSetpointObservation
	Previous         []thermalZoneSetpointHistory
	Evaluation       semreg.EvaluationContext
	SemanticRevision semreg.Uint64
	Requested        []projection.RequestedItem
	// Already projected items from other sources must survive this source's loss.
	OtherDispositions []projection.ProjectionDisposition
}

type thermalZoneSetpointResult struct {
	Selected     []thermalZoneSetpointSelection
	History      []thermalZoneSetpointHistory
	Dispositions []projection.ProjectionDisposition
	Evaluation   semreg.EvaluationContext
}

// mapThermalZoneSetpoints is a detached read-only mapping, not publication or
// completed cutover. Native losses live in History, outside requested accounting.
func mapThermalZoneSetpoints(input thermalZoneSetpointInput) (thermalZoneSetpointResult, error) {
	in, err := thermalZoneSetpointCopy(input)
	if err != nil {
		return thermalZoneSetpointResult{}, err
	}
	out := thermalZoneSetpointResult{Evaluation: in.Evaluation}
	// Prefer the immediately preceding selections over older diagnostic records.
	sort.SliceStable(in.Previous, func(i, j int) bool {
		return in.Previous[i].Current && !in.Previous[j].Current
	})
	retained := map[string]bool{}
	for _, previous := range in.Previous {
		if previous.Current {
			replaced := false
			for _, observation := range in.Observations {
				if observation.Identity == previous.Observation.Identity && observation.Role == previous.Observation.Role {
					replaced = true
				}
			}
			if !replaced {
				in.Observations = append(in.Observations, previous.Observation)
			}
		}
		previous.Current = false
		if previous.Reason == "" {
			previous.Reason = "retained_non_current"
		}
		// Current records are re-evaluated above even when the diagnostic window
		// is full; truncation must never discard a still-fresh selection.
		record, err := json.Marshal(previous)
		if err != nil {
			return thermalZoneSetpointResult{}, err
		}
		if !retained[string(record)] && len(out.History) < thermalZoneSetpointRetainedHistoryLimit {
			out.History = append(out.History, previous)
			retained[string(record)] = true
		}
	}
	matched := make([]bool, len(in.Mappings))
	for _, observation := range in.Observations {
		reason := "unresolved_role"
		var context *thermalZoneSetpointMapping
		mappingIndex, count := -1, 0
		for i, mapping := range in.Mappings {
			if mapping.Identity == observation.Identity && mapping.Role == observation.Role {
				matched[i] = true
				mappingIndex, count = i, count+1
			}
		}
		if count == 1 {
			mapping := in.Mappings[mappingIndex]
			context = &mapping
			reason = thermalZoneSetpointAdmission(in, mapping, observation)
			if reason == "" {
				out.Selected = append(out.Selected, thermalZoneSetpointSelection{
					Observation: observation, Key: mapping.Key, Field: mapping.Field,
					SemanticRevision: mapping.SemanticRevision, MappingEvidence: mapping.Evidence,
				})
			}
		} else if count > 1 {
			reason = "ambiguous_mapping"
		}
		out.History = append(out.History, thermalZoneSetpointHistory{Observation: observation, Mapping: context, Current: reason == "", Reason: reason})
	}
	for i, mapping := range in.Mappings {
		if !matched[i] {
			out.History = append(out.History, thermalZoneSetpointHistory{
				Observation: thermalZoneSetpointObservation{Identity: mapping.Identity, Role: mapping.Role}, Mapping: &mapping, Reason: "missing",
			})
		}
	}
	// Equal numbers do not resolve competing candidates or role/key collisions.
	counts := map[string]int{}
	for _, selection := range out.Selected {
		counts[thermalZoneSetpointKey(selection.Observation.Identity.Asset, selection.Key)]++
	}
	selected := out.Selected[:0]
	for _, selection := range out.Selected {
		if counts[thermalZoneSetpointKey(selection.Observation.Identity.Asset, selection.Key)] != 1 {
			for i := range out.History {
				if out.History[i].Current && out.History[i].Observation.Identity == selection.Observation.Identity && out.History[i].Observation.Role == selection.Observation.Role {
					out.History[i].Current, out.History[i].Reason = false, "conflicting_meaning"
				}
			}
			continue
		}
		selected = append(selected, selection)
	}
	out.Selected = selected
	// Only caller-requested IDs are accounted for. Never manufacture a definition
	// for a native role or emit per-source losses as extra shared dispositions.
	seen := map[projection.RequestedItem]bool{}
	for _, requested := range in.Requested {
		if seen[requested] {
			continue
		}
		seen[requested] = true
		reason := semreg.DefinitionID("reason.ebus_zone_setpoint_not_selected")
		d := projection.ProjectionDisposition{
			Kind: requested.Kind, ItemID: requested.ItemID, Outcome: projection.ProjectionWithheld,
			SourceKeys: []semreg.FactKey{}, Loss: []projection.LossDetail{}, Reason: &reason,
		}
		if requested.Kind == projection.ItemFact && requested.ItemID == "thermal.setpoint.temperature" {
			d.Loss = []projection.LossDetail{{Kind: projection.LossPolicy, SourceItems: []semreg.DefinitionID{}, Description: "No current independently qualified role-preserving zone setpoint is selected; native role losses remain separate.", Reversible: true}}
		}
		for _, other := range in.OtherDispositions {
			if other.Kind == requested.Kind && other.ItemID == requested.ItemID && (other.Outcome == projection.ProjectionExact || other.Outcome == projection.ProjectionTransformed) {
				d = other
				break
			}
		}
		if requested.Kind == projection.ItemFact && requested.ItemID == "thermal.setpoint.temperature" {
			for _, selection := range out.Selected {
				if d.Outcome == projection.ProjectionWithheld {
					d.Loss = []projection.LossDetail{}
					d.Reason = nil
					d.Outcome = projection.ProjectionExact
				}
				d.SourceKeys = append(d.SourceKeys, selection.Key)
			}
		}
		keys := map[string]semreg.FactKey{}
		for _, key := range d.SourceKeys {
			canonical, err := semreg.CanonicalJSON(key)
			if err != nil {
				return thermalZoneSetpointResult{}, err
			}
			keys[string(canonical)] = key
		}
		ordered := make([]string, 0, len(keys))
		for key := range keys {
			ordered = append(ordered, key)
		}
		sort.Strings(ordered)
		d.SourceKeys = make([]semreg.FactKey, 0, len(keys))
		for _, key := range ordered {
			d.SourceKeys = append(d.SourceKeys, keys[key])
		}
		if err := d.Validate(); err != nil {
			return thermalZoneSetpointResult{}, err
		}
		out.Dispositions = append(out.Dispositions, d)
	}
	thermalZoneSetpointSort(out.Selected)
	thermalZoneSetpointSort(out.History)
	thermalZoneSetpointSort(out.Dispositions)
	// Also detach the separate output collections from one another.
	return thermalZoneSetpointCopy(out)
}

func thermalZoneSetpointAdmission(in thermalZoneSetpointInput, m thermalZoneSetpointMapping, o thermalZoneSetpointObservation) string {
	if !thermalZoneSetpointIdentityValid(m.Identity) || !thermalZoneSetpointSelectorValid(o.Role, o.Identity.Selector) {
		return "native_identity"
	}
	if !o.Present {
		return "missing"
	}
	if !m.Approved || m.Qualification != semreg.QualificationQualified || o.Qualification != semreg.QualificationQualified || len(m.Evidence) == 0 || len(o.Evidence) == 0 {
		return "unqualified_role"
	}
	for _, evidence := range append(append([]semreg.EvidenceRef{}, m.Evidence...), o.Evidence...) {
		if evidence.Validate() != nil {
			return "invalid_evidence"
		}
	}
	if in.SemanticRevision == "" || m.SemanticRevision != in.SemanticRevision || m.SemanticRevision.Validate() != nil {
		return "semantic_revision"
	}
	if o.Validity != semreg.ValidityGood {
		return "invalid"
	}
	if o.Freshness != semreg.FreshnessFresh || !thermalZoneSetpointFresh(o, in.Evaluation) {
		return "not_fresh"
	}
	if o.Role == thermalZoneSetpointVeto || o.Role == thermalZoneSetpointHoliday {
		until, err := strconv.ParseUint(string(o.Until.Nanoseconds), 10, 64)
		now, nowErr := strconv.ParseUint(string(in.Evaluation.EvaluateMonotonic.Nanoseconds), 10, 64)
		if !o.WindowKnown || !o.Active || o.Until.ClockEpochID != in.Evaluation.EvaluateMonotonic.ClockEpochID || err != nil || nowErr != nil || now >= until {
			return "inactive_or_unknown_interval"
		}
	}
	if m.Unit != "unit.celsius" || o.Value.Unit != m.Unit || m.Codec != "f32_le" || o.Codec != m.Codec {
		return "unit_or_codec"
	}
	if !m.BoundsKnown || m.Minimum.Validate() != nil || m.Maximum.Validate() != nil || o.Value.Number.Validate() != nil {
		return "invalid_native_bounds_or_value"
	}
	minimum, maximum, number := thermalZoneSetpointNumber(m.Minimum), thermalZoneSetpointNumber(m.Maximum), thermalZoneSetpointNumber(o.Value.Number)
	if minimum.Cmp(maximum) > 0 || number.Cmp(minimum) < 0 || number.Cmp(maximum) > 0 {
		return "native_range"
	}
	pack := thermal.New()
	ref := semreg.DefinitionRef{Pack: pack.Pack(), ID: "thermal.setpoint.temperature", Version: pack.Pack().Version}
	value := semreg.Value{Kind: semreg.ValueQuantity, Quantity: &o.Value}
	if m.Field != ref || m.Key.FactID != ref.ID || pack.ValidateFact(m.Key, &value) != nil {
		return "unrepresentable_key_or_pack_range"
	}
	if strings.TrimSpace(*m.Key.Dimensions[0].Value.Text) == "" {
		return "unrepresentable_key_or_pack_range"
	}
	for _, other := range in.Mappings {
		if other.Approved && other.Qualification == semreg.QualificationQualified && other.Identity.Asset == m.Identity.Asset &&
			(other.Role != m.Role || other.Identity.Service != m.Identity.Service) && thermalZoneSetpointKey(other.Identity.Asset, other.Key) == thermalZoneSetpointKey(m.Identity.Asset, m.Key) {
			return "role_key_collision"
		}
	}
	// Both declared native endpoints must also lie inside the pack range. A
	// partially overlapping range is not widened or silently intersected.
	for _, bound := range []semreg.Decimal{m.Minimum, m.Maximum} {
		v := semreg.Value{Kind: semreg.ValueQuantity, Quantity: &semreg.Quantity{Number: bound, Unit: m.Unit}}
		if pack.ValidateFact(m.Key, &v) != nil {
			return "incoherent_pack_bounds"
		}
	}
	return ""
}

func thermalZoneSetpointIdentityValid(id thermalZoneSetpointIdentity) bool {
	return id.Asset.Validate() == nil && id.Service.Validate() == nil && id.Source.Validate() == nil && id.Epoch.Validate() == nil &&
		id.Binding.Validate() == nil && id.Generation.Validate() == nil && id.BindingRevision.Validate() == nil && id.AssociationRevision.Validate() == nil
}

func thermalZoneSetpointSelectorValid(role thermalZoneSetpointRole, s thermalZoneSetpointSelector) bool {
	registers := map[thermalZoneSetpointRole]uint16{
		thermalZoneSetpointHeatingManual: 0x0022, thermalZoneSetpointHeatingEffective: 0x0014,
		thermalZoneSetpointCoolingManual: 0x0023, thermalZoneSetpointCoolingEffective: 0x0015,
		thermalZoneSetpointVeto: 0x0008, thermalZoneSetpointHoliday: 0x0005,
	}
	r, ok := registers[role]
	return ok && s.PB == 0xb5 && s.SB == 0x24 && s.Opcode == 0x02 && s.OperationType == 0x00 && s.Group == 0x03 && s.Register == r && s.Direction == "target_to_initiator"
}

func thermalZoneSetpointFresh(o thermalZoneSetpointObservation, e semreg.EvaluationContext) bool {
	if o.ReceivedAt.Validate() != nil || o.Receipt.Validate() != nil || e.EvaluatedAt.Validate() != nil || e.EvaluateMonotonic.Validate() != nil || o.Receipt.ClockEpochID != e.EvaluateMonotonic.ClockEpochID {
		return false
	}
	received, a := strconv.ParseUint(string(o.Receipt.Nanoseconds), 10, 64)
	now, b := strconv.ParseUint(string(e.EvaluateMonotonic.Nanoseconds), 10, 64)
	maxAge, c := strconv.ParseUint(string(o.FreshForNS), 10, 64)
	return a == nil && b == nil && c == nil && maxAge > 0 && now >= received && now-received < maxAge
}

func thermalZoneSetpointNumber(d semreg.Decimal) *big.Rat {
	n, _ := new(big.Rat).SetString(d.Coefficient)
	power := int64(d.Exponent10)
	if power < 0 {
		power = -power
	}
	scale := new(big.Rat).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(power), nil))
	if d.Exponent10 < 0 {
		return n.Quo(n, scale)
	}
	return n.Mul(n, scale)
}

func thermalZoneSetpointKey(asset semreg.AssetID, key semreg.FactKey) string {
	b, _ := json.Marshal(struct {
		Asset semreg.AssetID
		Key   semreg.FactKey
	}{asset, key})
	return string(b)
}

func thermalZoneSetpointCopy[T any](v T) (T, error) {
	var out T
	b, err := json.Marshal(v)
	if err == nil {
		err = json.Unmarshal(b, &out)
	}
	return out, err
}

func thermalZoneSetpointSort[T any](values []T) {
	sort.Slice(values, func(i, j int) bool {
		a, _ := json.Marshal(values[i])
		b, _ := json.Marshal(values[j])
		return string(a) < string(b)
	})
}
