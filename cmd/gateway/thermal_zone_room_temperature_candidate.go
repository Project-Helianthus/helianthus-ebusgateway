package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"

	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
)

type zoneRoomSelector struct {
	PB, SB, Opcode, Operation, Group, Instance byte
	Register                                   uint16
	Direction, Initiator, Target, ReplySource  string
}

// Identity is native lineage, never a display ID or a proposed FactKey.
type zoneRoomIdentity struct {
	Asset                                                              semreg.AssetID
	Service                                                            semreg.ServiceInstanceID
	Source                                                             semreg.SourceID
	Epoch                                                              semreg.SourceEpochID
	Generation, BindingRevision, AssociationRevision                   semreg.Uint64
	Binding                                                            semreg.NativeBindingID
	Family, Profile, ProfileRevision, DecoderRevision, MappingRevision string
	Selector                                                           zoneRoomSelector
}

type zoneRoomCodec struct {
	Qualified        bool
	Encoding, Unit   string
	Width            int
	BoundsKnown      bool
	Minimum, Maximum float64
}

type zoneRoomObservation struct {
	Identity                    zoneRoomIdentity
	Present, Correlated         bool
	RawRequest, RawReply, Value []byte
	Codec                       zoneRoomCodec
	Evidence                    []semreg.EvidenceRef
	EvidenceStatus              string // Proven/Hypothesis/Unknown does not qualify a source.
	ReceivedAt                  semreg.TimePoint
	Receipt                     semreg.MonotonicPoint
	FreshForNS                  semreg.Uint64 // Explicit caller policy; zero/unknown never means fresh.
}

type zoneRoomRecord struct {
	Observation   zoneRoomObservation
	NativeCelsius *float64
	Loss          string // Local diagnostic text, never a reason DefinitionID.
}

type zoneRoomCandidateInput struct {
	Identity         zoneRoomIdentity // RR000F identity of this one native slot.
	Codec            zoneRoomCodec    // Independently qualified exact profile metadata.
	BindingQualified bool
	BindingWidth     int    // Supplied independently; no default native width.
	BindingValue     []byte // Independently correlated RR0013 bytes, no enum interpretation.
	Observations     []zoneRoomObservation
	Previous         []zoneRoomRecord
	Evaluation       semreg.EvaluationContext
}

type zoneRoomCandidateResult struct {
	History                                 []zoneRoomRecord
	Attempts                                []zoneRoomRecord
	Temperature, SensorBinding, Association *zoneRoomRecord
	LatestLoss, Freshness, Withheld         string
	// There is deliberately no SemReg Fact, FactKey, quantity or admission field.
}

func zoneRoomClone(o zoneRoomObservation) zoneRoomObservation {
	o.RawRequest = append([]byte(nil), o.RawRequest...)
	o.RawReply = append([]byte(nil), o.RawReply...)
	o.Value = append([]byte(nil), o.Value...)
	o.Evidence = append([]semreg.EvidenceRef(nil), o.Evidence...)
	return o
}

func zoneRoomIdentityKnown(i zoneRoomIdentity) bool {
	return i.Asset.Validate() == nil && i.Service.Validate() == nil && i.Source.Validate() == nil && i.Epoch.Validate() == nil &&
		i.Generation.Validate() == nil && i.Binding.Validate() == nil && i.BindingRevision.Validate() == nil && i.AssociationRevision.Validate() == nil &&
		i.Family != "" && i.Profile != "" && i.ProfileRevision != "" && i.DecoderRevision != "" && i.MappingRevision != "" &&
		i.Selector.Initiator != "" && i.Selector.Target != "" && i.Selector.ReplySource != ""
}

func zoneRoomSelectorSupported(s zoneRoomSelector) bool {
	return s.PB == 0xb5 && s.SB == 0x24 && s.Opcode == 2 && s.Operation == 0 && s.Group == 3 &&
		s.Register == 0x0f && s.Direction == "target_to_initiator"
}

func zoneRoomDecode(o zoneRoomObservation, codec zoneRoomCodec) (*float64, string) {
	if !o.Present {
		return nil, "missing value"
	}
	if !o.Correlated {
		return nil, "uncorrelated reply"
	}
	if !codec.Qualified || o.Codec != codec || codec.Width != 4 || codec.Unit != "unit.celsius" || (codec.Encoding != "f32_le" && codec.Encoding != "f32_be") {
		return nil, "unknown or mismatched qualified codec"
	}
	if len(o.Value) != codec.Width {
		return nil, "exact width mismatch"
	}
	var bits uint32
	if codec.Encoding == "f32_le" {
		bits = binary.LittleEndian.Uint32(o.Value)
	} else {
		bits = binary.BigEndian.Uint32(o.Value)
	}
	v := float64(math.Float32frombits(bits))
	if math.IsNaN(v) || math.IsInf(v, 0) || v < -100 || v > 200 {
		return nil, "nonfinite or outside pack bounds"
	}
	if codec.BoundsKnown && (math.IsNaN(codec.Minimum) || math.IsNaN(codec.Maximum) || math.IsInf(codec.Minimum, 0) || math.IsInf(codec.Maximum, 0) || codec.Minimum > codec.Maximum || v < codec.Minimum || v > codec.Maximum) {
		return nil, "outside supplied profile bounds or malformed bounds"
	}
	if o.ReceivedAt.Validate() != nil || o.Receipt.Validate() != nil {
		return nil, "malformed original receipt"
	}
	return &v, ""
}

func zoneRoomFresh(o zoneRoomObservation, e semreg.EvaluationContext) string {
	if o.ReceivedAt.Validate() != nil || o.Receipt.Validate() != nil || o.FreshForNS.Validate() != nil || e.EvaluatedAt.Validate() != nil || e.EvaluateMonotonic.Validate() != nil || o.Receipt.ClockEpochID != e.EvaluateMonotonic.ClockEpochID {
		return "unknown"
	}
	r, a := strconv.ParseUint(string(o.Receipt.Nanoseconds), 10, 64)
	n, b := strconv.ParseUint(string(e.EvaluateMonotonic.Nanoseconds), 10, 64)
	f, c := strconv.ParseUint(string(o.FreshForNS), 10, 64)
	if a != nil || b != nil || c != nil || f == 0 || n < r {
		return "unknown"
	}
	if n-r >= f {
		return "stale"
	}
	return "fresh"
}

// mapZoneRoomTemperatureCandidate never acquires, publishes, rounds or guesses a
// binding. Every prior record is revalidated; caller flags cannot admit history.
func mapZoneRoomTemperatureCandidate(in zoneRoomCandidateInput) zoneRoomCandidateResult {
	out := zoneRoomCandidateResult{Withheld: "final FactKey binding and decimal loss policy unresolved", Freshness: "unknown", LatestLoss: "missing observation"}
	observations := make([]zoneRoomObservation, 0, len(in.Previous)+len(in.Observations))
	for _, p := range in.Previous {
		observations = append(observations, zoneRoomClone(p.Observation))
	}
	for _, o := range in.Observations {
		observations = append(observations, zoneRoomClone(o))
	}
	// Sorting the complete native record preserves evidence distinctions and makes
	// permutations deterministic. Equal receipt conflicts never choose a winner.
	sort.Slice(observations, func(i, j int) bool {
		return zoneRoomOrder(observations[i]) < zoneRoomOrder(observations[j])
	})
	evaluationValid := in.Evaluation.EvaluatedAt.Validate() == nil && in.Evaluation.EvaluateMonotonic.Validate() == nil
	var temperature, binding, association []int
	malformedBinding := false
	malformedAttempt := false
	for _, o := range observations {
		r := zoneRoomRecord{Observation: o}
		expected := in.Identity
		expected.Selector.Register = o.Identity.Selector.Register
		switch {
		case !zoneRoomIdentityKnown(in.Identity) || !zoneRoomSelectorSupported(in.Identity.Selector):
			r.Loss = "unknown or unsupported slot"
		case o.Identity != expected:
			r.Loss = "foreign native identity"
		case o.Identity.Selector.Register != 0x0f && o.Identity.Selector.Register != 0x13 && o.Identity.Selector.Register != 0x1c:
			r.Loss = "unsupported register"
		case !o.Present || !o.Correlated || len(o.Value) == 0:
			r.Loss = "missing or uncorrelated evidence"
		case o.ReceivedAt.Validate() != nil || o.Receipt.Validate() != nil:
			r.Loss = "malformed original receipt"
		case o.Identity.Selector.Register == 0x0f:
			r.NativeCelsius, r.Loss = zoneRoomDecode(o, in.Codec)
		default:
			r.Loss = "association bytes only; interpretation withheld"
		}
		out.History = append(out.History, r)
		idx := len(out.History) - 1
		if o.Identity == expected && zoneRoomIdentityKnown(in.Identity) && zoneRoomSelectorSupported(in.Identity.Selector) && o.ReceivedAt.Validate() == nil && o.Receipt.Validate() == nil {
			switch o.Identity.Selector.Register {
			case 0x0f:
				temperature = append(temperature, idx)
			case 0x13:
				if in.BindingQualified && in.BindingWidth > 0 && len(o.Value) == in.BindingWidth && o.Present && o.Correlated {
					binding = append(binding, idx)
				}
			case 0x1c:
				if o.Present && o.Correlated && len(o.Value) > 0 {
					association = append(association, idx)
				}
			}
		}
	}
	// Select by typed monotonic receipt only, never wall time or input ordering.
	newest := func(indices []int, goodOnly bool) *zoneRoomRecord {
		if !evaluationValid {
			return nil
		}
		var best *zoneRoomRecord
		var point uint64
		conflict := false
		for _, idx := range indices {
			r := &out.History[idx]
			if goodOnly && r.NativeCelsius == nil {
				continue
			}
			if r.Observation.Receipt.ClockEpochID != in.Evaluation.EvaluateMonotonic.ClockEpochID {
				continue
			}
			n, err := strconv.ParseUint(string(r.Observation.Receipt.Nanoseconds), 10, 64)
			if err != nil {
				continue
			}
			if best == nil || n > point {
				best, point, conflict = r, n, false
			} else if n == point && !zoneRoomSameObservation(r.Observation, best.Observation) {
				conflict = true
			}
		}
		// Rejected temperature attempts still conflict with a decodable record at
		// the same receipt; decoding cannot resolve ambiguous native evidence.
		if best != nil && goodOnly {
			for _, idx := range indices {
				o := out.History[idx].Observation
				if o.Receipt == best.Observation.Receipt && !zoneRoomSameObservation(o, best.Observation) {
					conflict = true
				}
			}
		}
		if conflict {
			return nil
		}
		if best == nil {
			return nil
		}
		// Selected records also own their bytes independently of the diagnostic list.
		copy := *best
		copy.Observation = zoneRoomClone(best.Observation)
		if best.NativeCelsius != nil {
			v := *best.NativeCelsius
			copy.NativeCelsius = &v
		}
		return &copy
	}
	out.Temperature = newest(temperature, true)
	out.SensorBinding, out.Association = newest(binding, false), newest(association, false)
	// A changed binding may add raw temperature evidence, but cannot replace the
	// old sensor's retained value. Keep only temperatures preceding that change.
	if out.SensorBinding != nil && in.BindingQualified && !bytes.Equal(out.SensorBinding.Observation.Value, in.BindingValue) {
		var beforeChange []int
		for _, idx := range temperature {
			o := out.History[idx].Observation
			if o.Receipt.ClockEpochID == out.SensorBinding.Observation.Receipt.ClockEpochID && zoneRoomAfter(out.SensorBinding.Observation.Receipt, o.Receipt) {
				beforeChange = append(beforeChange, idx)
			}
		}
		out.Temperature = newest(beforeChange, true)
	}

	if latest := newest(temperature, false); latest != nil {
		out.LatestLoss = latest.Loss
	} else if len(temperature) > 0 {
		out.LatestLoss = "ambiguous or incomparable receipts"
	}
	if out.Temperature != nil {
		out.Freshness = zoneRoomFresh(out.Temperature.Observation, in.Evaluation)
	}
	if !in.BindingQualified || in.BindingWidth <= 0 || len(in.BindingValue) != in.BindingWidth || out.SensorBinding == nil || !bytes.Equal(out.SensorBinding.Observation.Value, in.BindingValue) {
		out.LatestLoss = "sensor binding unknown or changed; requalification and new correlated temperature required"
	} else if out.Temperature != nil && (out.Temperature.Observation.Receipt.ClockEpochID != out.SensorBinding.Observation.Receipt.ClockEpochID || zoneRoomAfter(out.SensorBinding.Observation.Receipt, out.Temperature.Observation.Receipt)) {
		out.LatestLoss = "temperature predates binding evidence"
	}

	for _, attempt := range in.Observations {
		for _, record := range out.History {
			if zoneRoomSameObservation(attempt, record.Observation) {
				copy := record
				copy.Observation = zoneRoomClone(record.Observation)
				if record.NativeCelsius != nil {
					v := *record.NativeCelsius
					copy.NativeCelsius = &v
				}
				out.Attempts = append(out.Attempts, copy)
				break
			}
		}
		expected := in.Identity
		expected.Selector.Register = attempt.Identity.Selector.Register
		if attempt.Identity != expected {
			continue
		}
		if attempt.Identity.Selector.Register == 0x13 && (!attempt.Present || !attempt.Correlated || !in.BindingQualified || in.BindingWidth <= 0 || len(attempt.Value) != in.BindingWidth) {
			malformedBinding = true
		}
		if (attempt.Identity.Selector.Register == 0x0f || attempt.Identity.Selector.Register == 0x13 || attempt.Identity.Selector.Register == 0x1c) && (attempt.ReceivedAt.Validate() != nil || attempt.Receipt.Validate() != nil) {
			malformedAttempt = true
		}
	}
	sort.Slice(out.Attempts, func(i, j int) bool {
		return zoneRoomOrder(out.Attempts[i].Observation) < zoneRoomOrder(out.Attempts[j].Observation)
	})
	if malformedBinding {
		out.LatestLoss = "sensor binding malformed or unqualified; retained association is history"
	}
	if malformedAttempt {
		out.LatestLoss = "malformed attempt receipt; ordering unknown"
	}
	if !evaluationValid {
		out.LatestLoss = "malformed evaluation point; selection withheld"
	}
	return out
}

func zoneRoomAfter(a, b semreg.MonotonicPoint) bool {
	av, _ := strconv.ParseUint(string(a.Nanoseconds), 10, 64)
	bv, _ := strconv.ParseUint(string(b.Nanoseconds), 10, 64)
	return av > bv
}

func zoneRoomSameObservation(a, b zoneRoomObservation) bool {
	return zoneRoomOrder(a) == zoneRoomOrder(b)
}

func zoneRoomOrder(o zoneRoomObservation) string {
	b, err := json.Marshal(o)
	if err != nil {
		// Malformed codec bounds can contain nonfinite floats. Preserve every
		// identity and evidence field even when JSON cannot encode those bounds.
		return fmt.Sprintf("%#v", o)
	}
	return string(b)
}
