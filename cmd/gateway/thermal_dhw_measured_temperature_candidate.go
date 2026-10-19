package main

import (
	"encoding/binary"
	"math"
	"strconv"

	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
)

// This seam consumes already correlated native observations. It neither reads
// a bus nor normalizes reply instances. All losses below are local text.
type dhwMeasuredSelector struct {
	PB, SB, Opcode, Operation, Group, Instance byte
	Register                                   uint16
	Direction                                  string
}

type dhwMeasuredIdentity struct {
	Asset                                                              semreg.AssetID
	Service                                                            semreg.ServiceInstanceID
	Source                                                             semreg.SourceID
	Epoch                                                              semreg.SourceEpochID
	Generation, BindingRevision, AssociationRevision, SemanticRevision semreg.Uint64
	Binding                                                            semreg.NativeBindingID
	Native, Family, Profile, ProfileRevision, DecoderRevision          string
	Selector                                                           dhwMeasuredSelector
}

// Qualification and validity bounds are supplied by the native owner, never
// inferred from a plausible value or from the Thermal pack's interval.
type dhwMeasuredCodec struct {
	Identity         dhwMeasuredIdentity
	Qualified        bool
	Codec, Unit      string
	BoundsKnown      bool
	Minimum, Maximum float64
	Evidence         semreg.EvidenceRef
}

// Value extraction and Correlated are trusted native-owner assertions; this
// seam requires raw evidence but does not verify framing or byte consistency.
type dhwMeasuredObservation struct {
	Identity                    dhwMeasuredIdentity
	RawRequest, RawReply, Value []byte
	Correlated                  bool
	Outcome                     string
	EvidenceStatus              string // Proven/Hypothesis/Unknown is independent of qualification.
	Evidence                    semreg.EvidenceRef
	ReceivedAt                  semreg.TimePoint
	Receipt                     semreg.MonotonicPoint
	FreshForNS                  semreg.Uint64
}

type dhwMeasuredInput struct {
	Codec             dhwMeasuredCodec
	Attempt, Previous *dhwMeasuredObservation
}

type dhwMeasuredRequest struct {
	Identity              dhwMeasuredIdentity // RR0005; presence must match except for RR0001.
	Measurement, Presence dhwMeasuredInput
	Evaluation            semreg.EvaluationContext
}

type dhwMeasuredComponent struct {
	Attempt, History, Selected *dhwMeasuredObservation
	LastGoodValue              *float64 // Native Celsius or presence UIN, never a SemReg decimal.
	Freshness                  semreg.Freshness
	AttemptLoss                string
}

type dhwMeasuredCandidate struct {
	Identity              dhwMeasuredIdentity
	Measurement, Presence dhwMeasuredComponent
	Celsius               *float64
	Withheld, Loss        string
	Facts                 []semreg.FactCandidate
}

// Each request remains a distinct controller source. A boiler mirror must use
// this same candidate; this function performs no asset/definition arbitration.
func mapDHWMeasuredTemperature(in dhwMeasuredRequest) dhwMeasuredCandidate {
	out := dhwMeasuredCandidate{Identity: in.Identity, Withheld: "final fact key and decimal policy unresolved", Facts: []semreg.FactCandidate{}}
	out.Measurement = dhwMeasuredMapComponent(in.Identity, in.Measurement, in.Evaluation)
	presenceKey := in.Identity
	presenceKey.Selector.Register = 1
	out.Presence = dhwMeasuredMapComponent(presenceKey, in.Presence, in.Evaluation)
	switch {
	case !dhwMeasuredKeyValid(in.Identity) || in.Identity.Selector.Register != 5:
		out.Loss = "unsupported measurement identity"
	case out.Measurement.AttemptLoss != "":
		out.Loss = "measurement: " + out.Measurement.AttemptLoss
	case out.Presence.AttemptLoss != "":
		out.Loss = "presence: " + out.Presence.AttemptLoss
	case out.Presence.LastGoodValue == nil || *out.Presence.LastGoodValue == 0:
		out.Loss = "qualified presence is absent"
	default:
		v := *out.Measurement.LastGoodValue
		out.Celsius = &v
	}
	return out
}

func dhwMeasuredKeyValid(k dhwMeasuredIdentity) bool {
	s := k.Selector
	if s.PB != 0xb5 || s.SB != 0x24 || s.Opcode != 2 || s.Operation != 0 || s.Group != 1 || s.Instance != 0 || (s.Register != 1 && s.Register != 5) || s.Direction != "target_to_initiator" {
		return false
	}
	for _, r := range []semreg.Uint64{k.Generation, k.BindingRevision, k.AssociationRevision, k.SemanticRevision} {
		if r.Validate() != nil || r == "0" {
			return false
		}
	}
	return k.Asset.Validate() == nil && k.Service.Validate() == nil && k.Source.Validate() == nil && k.Epoch.Validate() == nil && k.Binding.Validate() == nil && k.Native != "" && k.Family != "" && k.Profile != "" && k.ProfileRevision != "" && k.DecoderRevision != ""
}

func dhwMeasuredValue(key dhwMeasuredIdentity, codec dhwMeasuredCodec, o *dhwMeasuredObservation) (float64, string) {
	if o == nil {
		return 0, "missing"
	}
	if !dhwMeasuredKeyValid(key) || o.Identity != key || codec.Identity != key {
		return 0, "identity mismatch"
	}
	if !codec.Qualified || codec.Evidence.Validate() != nil || o.Evidence != codec.Evidence || o.Evidence.Validate() != nil {
		return 0, "qualification or evidence mismatch"
	}
	if !o.Correlated || o.Outcome != "ok" {
		return 0, "uncorrelated or unsuccessful reply"
	}
	if len(o.RawRequest) == 0 || len(o.RawReply) == 0 {
		return 0, "missing raw request or reply"
	}
	switch o.EvidenceStatus {
	case "Proven", "Hypothesis", "Unknown":
	default:
		return 0, "unknown evidence status"
	}
	if o.ReceivedAt.Validate() != nil || o.Receipt.Validate() != nil || o.FreshForNS.Validate() != nil || o.FreshForNS == "0" {
		return 0, "invalid receipt or freshness policy"
	}
	if key.Selector.Register == 1 {
		if codec.Codec != "uin16-le" || codec.Unit != "native.uin" || len(o.Value) != 2 {
			return 0, "presence codec or width unknown"
		}
		return float64(binary.LittleEndian.Uint16(o.Value)), ""
	}
	if codec.Codec != "f32-le" || codec.Unit != "unit.celsius" || !codec.BoundsKnown || math.IsNaN(codec.Minimum) || math.IsNaN(codec.Maximum) || math.IsInf(codec.Minimum, 0) || math.IsInf(codec.Maximum, 0) || codec.Minimum > codec.Maximum {
		return 0, "temperature codec or bounds unknown"
	}
	if len(o.Value) != 4 {
		return 0, "temperature width is not four"
	}
	v := float64(math.Float32frombits(binary.LittleEndian.Uint32(o.Value)))
	if math.IsNaN(v) || math.IsInf(v, 0) || v < -100 || v > 200 || v < codec.Minimum || v > codec.Maximum {
		return 0, "nonfinite or outside supplied bounds"
	}
	return v, ""
}

func dhwMeasuredFreshness(o *dhwMeasuredObservation, e semreg.EvaluationContext) semreg.Freshness {
	if o == nil || o.ReceivedAt.Validate() != nil || o.Receipt.Validate() != nil || e.EvaluatedAt.Validate() != nil || e.EvaluateMonotonic.Validate() != nil || o.FreshForNS.Validate() != nil || o.FreshForNS == "0" || o.Receipt.ClockEpochID != e.EvaluateMonotonic.ClockEpochID {
		return semreg.FreshnessUnknown
	}
	r, _ := strconv.ParseUint(string(o.Receipt.Nanoseconds), 10, 64)
	n, _ := strconv.ParseUint(string(e.EvaluateMonotonic.Nanoseconds), 10, 64)
	f, _ := strconv.ParseUint(string(o.FreshForNS), 10, 64)
	if n < r {
		return semreg.FreshnessUnknown
	}
	if n-r >= f {
		return semreg.FreshnessStale
	}
	return semreg.FreshnessFresh
}

func dhwMeasuredMapComponent(key dhwMeasuredIdentity, in dhwMeasuredInput, e semreg.EvaluationContext) dhwMeasuredComponent {
	out := dhwMeasuredComponent{Attempt: dhwMeasuredClone(in.Attempt), History: dhwMeasuredClone(in.Previous), Freshness: semreg.FreshnessUnknown}
	if v, loss := dhwMeasuredValue(key, in.Codec, in.Previous); loss == "" {
		out.Selected, out.LastGoodValue = dhwMeasuredClone(in.Previous), &v
	}
	v, loss := dhwMeasuredValue(key, in.Codec, in.Attempt)
	if loss == "" && dhwMeasuredFreshness(in.Attempt, e) != semreg.FreshnessFresh {
		loss = "receipt stale or freshness unknown"
	}
	if loss == "" && out.Selected != nil {
		a, _ := strconv.ParseUint(string(in.Attempt.Receipt.Nanoseconds), 10, 64)
		p, _ := strconv.ParseUint(string(out.Selected.Receipt.Nanoseconds), 10, 64)
		if in.Attempt.Receipt.ClockEpochID != out.Selected.Receipt.ClockEpochID || a <= p {
			loss = "duplicate, older or incomparable receipt"
		}
	}
	if loss == "" {
		out.Selected, out.LastGoodValue = dhwMeasuredClone(in.Attempt), &v
	}
	out.AttemptLoss = loss
	out.Freshness = dhwMeasuredFreshness(out.Selected, e)
	return out
}

func dhwMeasuredClone(o *dhwMeasuredObservation) *dhwMeasuredObservation {
	if o == nil {
		return nil
	}
	c := *o
	c.RawRequest = append([]byte(nil), o.RawRequest...)
	c.RawReply = append([]byte(nil), o.RawReply...)
	c.Value = append([]byte(nil), o.Value...)
	return &c
}
