package main

import (
	"strconv"

	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/packs/thermal"
)

// These adapter records are native evidence, not new Thermal definitions.
// Selectors follow the public dhw-mode-semreg-cutover contract. No enum label
// here asserts family support, active loading, membership, or system state.
type thermalDhwModeSelector struct {
	PB, SB, Opcode, OperationType, Group, Instance byte
	Register                                       uint16
	RequestDirection, ValueDirection               string
}

type thermalDhwModeIdentity struct {
	Asset               semreg.AssetID
	Service             semreg.ServiceInstanceID
	Source              semreg.SourceID
	SourceEpoch         semreg.SourceEpochID
	DriverGeneration    semreg.Uint64
	Binding             semreg.NativeBindingID
	BindingRevision     semreg.Uint64
	AssociationRevision semreg.Uint64
	SemanticRevision    semreg.Uint64
	NativeIdentity      string
	Initiator, Target   string
	Family, Decoder     string
	Selector            thermalDhwModeSelector
}

type thermalDhwModeObservation struct {
	Identity         thermalDhwModeIdentity
	Raw              *uint16
	Reply            []byte
	Validity         semreg.Validity
	EvidenceStatus   string // Proven, Hypothesis, Unknown; independent of qualification.
	Evidence         semreg.EvidenceRef
	ReceivedAt       semreg.TimePoint
	ReceiptMonotonic semreg.MonotonicPoint
	FreshnessPolicy  semreg.FreshnessPolicy
}

// Qualification is supplied by the native owner and bound to this exact key.
// Supplying bytes, metadata inventory, or a successful read cannot create it.
type thermalDhwModeQualification struct {
	Identity      thermalDhwModeIdentity
	Qualification semreg.Qualification
	Admitted      bool
	Evidence      semreg.EvidenceRef
}

type thermalDhwModeInput struct {
	Current, LastGood *thermalDhwModeObservation
	Qualification     thermalDhwModeQualification
}

type thermalDhwModeMappingInput struct {
	// Identity identifies the independently supplied presence/read-service selector.
	Identity                 thermalDhwModeIdentity
	Mode, SpecialMode, State thermalDhwModeInput
	ReadService              thermalDhwModeInput
	ReadCapability           semreg.CapabilityInstanceID
	Evaluation               semreg.EvaluationContext
}

type thermalDhwModeComponent struct {
	Expected                   thermalDhwModeIdentity
	Qualification              thermalDhwModeQualification
	Attempt, History, Selected *thermalDhwModeObservation
	Freshness                  semreg.Freshness
	AttemptFreshness           semreg.Freshness
	HistoryFreshness           semreg.Freshness
	NativeEligible             bool
	AttemptLoss                string
	ConsumerLoss               string
}

type thermalDhwModeMappingResult struct {
	Native       []thermalDhwModeComponent
	ReadService  thermalDhwModeComponent
	Services     []semreg.ServiceInstance
	Capabilities []semreg.CapabilityInstance
	Evaluation   semreg.EvaluationContext
}

// mapThermalDhwMode is a pure, per-binding contribution. It emits no facts or
// requested-item dispositions: those require aggregation with other sources.
// Native losses therefore cannot overwrite another source's system projection.
// Registration, persistence, arbitration and acquisition remain caller-owned.
func mapThermalDhwMode(in thermalDhwModeMappingInput) thermalDhwModeMappingResult {
	out := thermalDhwModeMappingResult{
		Native: make([]thermalDhwModeComponent, 0, 3), Evaluation: in.Evaluation,
		Services: []semreg.ServiceInstance{}, Capabilities: []semreg.CapabilityInstance{},
	}
	for _, slot := range []struct {
		register uint16
		input    thermalDhwModeInput
	}{{0x0003, in.Mode}, {0x000D, in.SpecialMode}, {0x000F, in.State}} {
		key := in.Identity
		key.Selector.Register = slot.register
		component := thermalDhwModeMapComponent(key, slot.input, in.Evaluation)
		component.ConsumerLoss = "no_dedicated_thermal_definition"
		out.Native = append(out.Native, component)
	}
	out.ReadService = thermalDhwModeMapComponent(in.Identity, in.ReadService, in.Evaluation)
	// A retained presence read alone cannot readmit a failed current service.
	if !out.ReadService.NativeEligible || out.ReadService.AttemptLoss != "" ||
		in.Identity.Selector.Register != 0x0001 || *out.ReadService.Selected.Raw == 0 {
		return out
	}
	metadata := thermal.Metadata()
	var serviceRef, capabilityRef semreg.DefinitionRef
	for _, service := range metadata.Services {
		if service.Ref.ID == "thermal.service.dhw" && service.FactKeyDimension.ID == "thermal.dimension.dhw" {
			serviceRef = service.Ref
		}
	}
	for _, capability := range metadata.Capabilities {
		if capability.Ref.ID == "thermal.capability.read.dhw" && capability.Service == serviceRef {
			capabilityRef = capability.Ref
		}
	}
	k := in.Identity
	service := semreg.ServiceInstance{
		InstanceID: k.Service, AssetID: k.Asset, Definition: serviceRef, BindingID: k.Binding,
		SourceEpochID: k.SourceEpoch, DriverGeneration: k.DriverGeneration,
		Qualification: semreg.QualificationQualified, Availability: semreg.AvailabilityAvailable,
		Revision: k.AssociationRevision,
	}
	capability := semreg.CapabilityInstance{
		InstanceID: in.ReadCapability, AssetID: k.Asset, ServiceInstance: k.Service,
		Definition: capabilityRef, BindingID: k.Binding, SourceEpochID: k.SourceEpoch,
		DriverGeneration: k.DriverGeneration, Qualification: semreg.QualificationQualified,
		Availability: semreg.AvailabilityAvailable, Constraints: []semreg.TypedField{},
		ActivationEvidence: []semreg.EvidenceRef{in.ReadService.Qualification.Evidence},
		Revision:           k.AssociationRevision,
	}
	validator := thermal.New()
	if validator.ValidateService(service) == nil && validator.ValidateCapability(capability) == nil {
		out.Services = append(out.Services, service)
		out.Capabilities = append(out.Capabilities, capability)
	}
	return out
}

func thermalDhwModeMapComponent(key thermalDhwModeIdentity, in thermalDhwModeInput, evaluation semreg.EvaluationContext) thermalDhwModeComponent {
	out := thermalDhwModeComponent{
		Expected: key, Qualification: in.Qualification,
		Attempt: thermalDhwModeClone(in.Current), History: thermalDhwModeClone(in.LastGood),
		Freshness: semreg.FreshnessUnknown, AttemptFreshness: semreg.FreshnessUnknown, HistoryFreshness: semreg.FreshnessUnknown,
	}
	// Age is evaluated independently even for malformed or unqualified attempts.
	if in.Current != nil {
		out.AttemptFreshness = thermalDhwModeFreshness(*in.Current, evaluation)
	}
	if in.LastGood != nil {
		out.HistoryFreshness = thermalDhwModeFreshness(*in.LastGood, evaluation)
	}
	out.AttemptLoss = thermalDhwModeObservationLoss(key, in.Current)
	if out.AttemptLoss == "" {
		out.Selected = thermalDhwModeClone(in.Current)
	} else if thermalDhwModeObservationLoss(key, in.LastGood) == "" {
		out.Selected = thermalDhwModeClone(in.LastGood)
	}
	if out.Selected != nil {
		out.Freshness = thermalDhwModeFreshness(*out.Selected, evaluation)
	}
	q := in.Qualification
	out.NativeEligible = out.Selected != nil && out.Freshness == semreg.FreshnessFresh &&
		q.Identity == key && q.Qualification == semreg.QualificationQualified && q.Admitted && q.Evidence.Validate() == nil
	return out
}

func thermalDhwModeObservationLoss(key thermalDhwModeIdentity, observation *thermalDhwModeObservation) string {
	if observation == nil {
		return "missing"
	}
	if observation.Identity != key || !thermalDhwModeIdentityValid(key) {
		return "identity_mismatch_or_unknown"
	}
	if observation.Raw == nil || observation.Validity != semreg.ValidityGood || observation.Evidence.Validate() != nil {
		return "invalid_or_unknown"
	}
	width := 2
	if key.Selector.Register == 0x000D {
		width = 1
	}
	if len(observation.Reply) != width {
		return "invalid_width"
	}
	// Check consistency with caller-supplied decoding; do not acquire or label it.
	if uint16(observation.Reply[0]) != *observation.Raw&0xff ||
		(width == 1 && *observation.Raw > 255) ||
		(width == 2 && uint16(observation.Reply[1]) != *observation.Raw>>8) {
		return "decode_mismatch"
	}
	return ""
}

func thermalDhwModeIdentityValid(k thermalDhwModeIdentity) bool {
	s := k.Selector
	if s.PB != 0xB5 || s.SB != 0x24 || s.Opcode != 0x02 || s.OperationType != 0 ||
		s.Group != 0x01 || s.Instance != 0 || s.RequestDirection != "initiator_to_target" ||
		s.ValueDirection != "target_to_initiator" {
		return false
	}
	if s.Register != 0x0001 && s.Register != 0x0003 && s.Register != 0x000D && s.Register != 0x000F {
		return false
	}
	for _, revision := range []semreg.Uint64{k.DriverGeneration, k.BindingRevision, k.AssociationRevision, k.SemanticRevision} {
		if revision.Validate() != nil || revision == "0" {
			return false
		}
	}
	return k.Asset.Validate() == nil && k.Service.Validate() == nil && k.Source.Validate() == nil &&
		k.SourceEpoch.Validate() == nil && k.Binding.Validate() == nil && k.NativeIdentity != "" &&
		k.Initiator != "" && k.Target != "" && k.Family != "" && k.Decoder != ""
}

func thermalDhwModeFreshness(o thermalDhwModeObservation, evaluation semreg.EvaluationContext) semreg.Freshness {
	if o.FreshnessPolicy.Validate() != nil {
		return semreg.FreshnessUnknown
	}
	times := semreg.Times{ReceivedAt: o.ReceivedAt, ReceiptMonotonic: o.ReceiptMonotonic,
		EvaluatedAt: evaluation.EvaluatedAt, EvaluateMonotonic: evaluation.EvaluateMonotonic}
	elapsed, err := times.ElapsedMonotonicNS()
	if err != nil {
		return semreg.FreshnessUnknown
	}
	age, ageErr := strconv.ParseUint(string(elapsed), 10, 64)
	fresh, freshErr := strconv.ParseUint(string(o.FreshnessPolicy.FreshForNS), 10, 64)
	retain, retainErr := strconv.ParseUint(string(o.FreshnessPolicy.RetainForNS), 10, 64)
	if ageErr != nil || freshErr != nil || retainErr != nil {
		return semreg.FreshnessUnknown
	}
	if age >= retain {
		return semreg.FreshnessExpired
	}
	if age >= fresh {
		return semreg.FreshnessStale
	}
	return semreg.FreshnessFresh
}

func thermalDhwModeClone(in *thermalDhwModeObservation) *thermalDhwModeObservation {
	if in == nil {
		return nil
	}
	out := *in
	out.Reply = append([]byte(nil), in.Reply...)
	if in.Raw != nil {
		raw := *in.Raw
		out.Raw = &raw
	}
	return &out
}
