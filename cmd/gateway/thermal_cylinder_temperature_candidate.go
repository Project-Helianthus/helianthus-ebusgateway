package main

import (
	"encoding/binary"
	"encoding/json"
	"math"
	"sort"
	"strconv"

	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
)

// Adapter-local records deliberately have no SemReg fact, key or decimal value.
// The public cylinder-temperature-cutover contract leaves that binding unresolved.
type cylinderTemperatureSelector struct {
	PB, SB                     byte
	Opcode                     uint16
	Operation, Group, Instance byte
	Register                   uint16
	Direction                  string
}

type cylinderTemperatureIdentity struct {
	Selector                                                          cylinderTemperatureSelector
	Asset, Service, Source, Epoch, Generation, Binding                string
	BindingRevision, AssociationRevision, MappingRevision             string
	Family, Profile, ProfileRevision, RegistryRevision, CodecRevision string
	RequestSource, RequestDestination                                 string
	Evidence                                                          semreg.EvidenceRef
}

type cylinderTemperatureCodec struct {
	Identity               cylinderTemperatureIdentity
	Qualified, BoundsKnown bool
	Encoding, Unit         string
	Minimum, Maximum       float64
}

type cylinderTemperatureObservation struct {
	Identity                                       cylinderTemperatureIdentity
	RawRequest, RawReply, ValueBytes               []byte
	Correlated, Successful, PresenceKnown, Present bool
	EvidenceStatus                                 string // Proven/Hypothesis/Unknown is independent of qualification.
	ReceivedAt                                     semreg.TimePoint
	Receipt                                        semreg.MonotonicPoint
	FreshForNS                                     semreg.Uint64
	FM5Revision                                    string
}

type cylinderTemperatureRecord struct {
	Observation cylinderTemperatureObservation
	Codec       cylinderTemperatureCodec
}

type cylinderTemperatureFM5 struct {
	Interpretation graphql.Fm5Interpretation
	Coherent       bool
	Receipt        semreg.MonotonicPoint
	FreshForNS     semreg.Uint64
}

type cylinderTemperatureRequest struct {
	Identity cylinderTemperatureIdentity
	Codec    cylinderTemperatureCodec
	FM5      cylinderTemperatureFM5
	Attempt  *cylinderTemperatureObservation
	Previous *cylinderTemperatureRecord
}

type cylinderTemperatureCandidate struct {
	Identity                       cylinderTemperatureIdentity
	FM5                            cylinderTemperatureFM5
	Attempt                        *cylinderTemperatureObservation
	History                        *cylinderTemperatureRecord
	LastGood                       *cylinderTemperatureRecord
	NativeValue                    *float64
	Current, Structural, Withdrawn bool
	Loss, Withheld                 string // Local diagnostic text, never reason DefinitionIDs.
}

// mapCylinderTemperatureCandidates never acquires, qualifies, publishes or repairs
// one source from another. A failed latest attempt and original last good receipt
// remain separate. Duplicate requests are ambiguous rather than last-writer wins.
func mapCylinderTemperatureCandidates(requests []cylinderTemperatureRequest, now semreg.MonotonicPoint) []cylinderTemperatureCandidate {
	counts := make(map[cylinderTemperatureIdentity]int)
	for _, q := range requests {
		counts[q.Identity]++
	}
	out := make([]cylinderTemperatureCandidate, 0, len(requests))
	for _, q := range requests {
		r := cylinderTemperatureCandidate{Identity: q.Identity, FM5: q.FM5, Attempt: cloneCylinderObservation(q.Attempt), History: cloneCylinderRecord(q.Previous), Withheld: "complete Thermal binding and decimal policy unresolved"}
		if p := q.Previous; p != nil && sameCylinderTemperatureIdentity(p.Observation.Identity, q.Identity) && sameCylinderTemperatureCodec(p.Codec, q.Codec) {
			if _, loss := cylinderTemperatureNative(p.Observation, p.Codec); loss == "" {
				r.LastGood = cloneCylinderRecord(p)
				r.Structural = true
			}
		}
		fm5Valid := q.FM5.Interpretation.Validate() == nil && q.FM5.Coherent && cylinderTemperatureFresh(q.FM5.Receipt, now, q.FM5.FreshForNS)
		switch {
		case counts[q.Identity] != 1:
			r.Loss = "ambiguous duplicate instance binding"
		case !fm5Valid:
			r.Loss = "FM5 coherence or freshness unknown"
		case q.FM5.Interpretation.DegradedReason != "" && q.FM5.Interpretation.DegradedReason != graphql.Fm5SemanticDegradedReasonConfigurationNotInterpretable:
			r.Loss = "transient FM5 degradation"
		case q.FM5.Interpretation.Mode == graphql.Fm5SemanticModeAbsent || q.FM5.Interpretation.Mode == graphql.Fm5SemanticModeGPIOOnly:
			r.Loss, r.Structural, r.Withdrawn = "coherent structural withdrawal", false, true
			r.LastGood = nil // History remains inspectable, never a structural ghost.
		case q.Attempt == nil:
			r.Loss = "missing measurement"
		case !sameCylinderTemperatureIdentity(q.Attempt.Identity, q.Identity):
			r.Loss = "foreign observation identity"
		case q.Attempt.FM5Revision != q.FM5.Interpretation.EvidenceRevision:
			r.Loss = "FM5 revision mismatch"
		default:
			value, loss := cylinderTemperatureNative(*q.Attempt, q.Codec)
			r.Loss = loss
			if loss == "" {
				switch {
				case !cylinderTemperatureFresh(q.Attempt.Receipt, now, q.Attempt.FreshForNS):
					r.Loss = "measurement age unknown or stale"
				case r.LastGood != nil && !cylinderTemperatureNewer(q.Attempt.Receipt, r.LastGood.Observation.Receipt):
					r.Loss = "receipt replay or clock change"
				default:
					r.NativeValue, r.Current, r.Structural = &value, true, true
					r.LastGood = &cylinderTemperatureRecord{Observation: *cloneCylinderObservation(q.Attempt), Codec: q.Codec}
				}
			}
		}
		out = append(out, r)
	}
	// Complete detached records, including loss/provenance, determine ordering.
	sort.Slice(out, func(i, j int) bool {
		return cylinderTemperatureOrder(out[i]) < cylinderTemperatureOrder(out[j])
	})
	return out
}

func cylinderTemperatureNative(o cylinderTemperatureObservation, c cylinderTemperatureCodec) (float64, string) {
	s := o.Identity.Selector
	if s.PB != 0xb5 || s.SB != 0x24 || s.Opcode != 0xb524 || s.Operation != 2 || s.Group != 5 || s.Register != 4 || s.Direction != "target_to_initiator" {
		return 0, "unsupported native selector"
	}
	id := o.Identity
	for _, v := range []string{id.Asset, id.Service, id.Source, id.Epoch, id.Generation, id.Binding, id.BindingRevision, id.AssociationRevision, id.MappingRevision, id.Family, id.Profile, id.ProfileRevision, id.RegistryRevision, id.CodecRevision, id.RequestSource, id.RequestDestination} {
		if v == "" {
			return 0, "unknown native binding"
		}
	}
	if id.Evidence.Validate() != nil {
		return 0, "invalid native evidence"
	}
	if !sameCylinderTemperatureIdentity(c.Identity, id) || !c.Qualified {
		return 0, "unqualified codec identity"
	}
	if !o.Correlated || !o.Successful {
		return 0, "uncorrelated or failed read"
	}
	if !o.PresenceKnown || !o.Present {
		return 0, "native existence unknown or absent"
	}
	if o.ReceivedAt.Validate() != nil || o.Receipt.Validate() != nil {
		return 0, "invalid original receipt"
	}
	if c.Unit != "unit.celsius" {
		return 0, "native unit unknown"
	}
	if !c.BoundsKnown || math.IsNaN(c.Minimum) || math.IsNaN(c.Maximum) || math.IsInf(c.Minimum, 0) || math.IsInf(c.Maximum, 0) || c.Minimum > c.Maximum {
		return 0, "native bounds unknown"
	}
	if len(o.ValueBytes) != 4 {
		return 0, "invalid value width"
	}
	var bits uint32
	switch c.Encoding {
	case "f32-le":
		bits = binary.LittleEndian.Uint32(o.ValueBytes)
	case "f32-be":
		bits = binary.BigEndian.Uint32(o.ValueBytes)
	default:
		return 0, "native encoding unknown"
	}
	value := float64(math.Float32frombits(bits))
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return 0, "non-finite native value"
	}
	if value < c.Minimum || value > c.Maximum {
		return 0, "outside qualified native bounds"
	}
	return value, ""
}

func cylinderTemperatureFresh(receipt, now semreg.MonotonicPoint, window semreg.Uint64) bool {
	if receipt.Validate() != nil || now.Validate() != nil || window.Validate() != nil || receipt.ClockEpochID != now.ClockEpochID {
		return false
	}
	r, _ := strconv.ParseUint(string(receipt.Nanoseconds), 10, 64)
	n, _ := strconv.ParseUint(string(now.Nanoseconds), 10, 64)
	w, _ := strconv.ParseUint(string(window), 10, 64)
	return w > 0 && n >= r && n-r < w
}

func cylinderTemperatureNewer(a, b semreg.MonotonicPoint) bool {
	if a.Validate() != nil || b.Validate() != nil || a.ClockEpochID != b.ClockEpochID {
		return false
	}
	x, _ := strconv.ParseUint(string(a.Nanoseconds), 10, 64)
	y, _ := strconv.ParseUint(string(b.Nanoseconds), 10, 64)
	return x > y
}

func cloneCylinderObservation(o *cylinderTemperatureObservation) *cylinderTemperatureObservation {
	if o == nil {
		return nil
	}
	c := *o
	c.RawRequest = append([]byte(nil), o.RawRequest...)
	c.RawReply = append([]byte(nil), o.RawReply...)
	c.ValueBytes = append([]byte(nil), o.ValueBytes...)
	return &c
}
func cloneCylinderRecord(p *cylinderTemperatureRecord) *cylinderTemperatureRecord {
	if p == nil {
		return nil
	}
	c := *p
	c.Observation = *cloneCylinderObservation(&p.Observation)
	return &c
}

func sameCylinderTemperatureIdentity(a, b cylinderTemperatureIdentity) bool {
	return a == b
}
func sameCylinderTemperatureCodec(a, b cylinderTemperatureCodec) bool {
	if !sameCylinderTemperatureIdentity(a.Identity, b.Identity) {
		return false
	}
	a.Identity, b.Identity = cylinderTemperatureIdentity{}, cylinderTemperatureIdentity{}
	return a == b
}

// Non-finite rejected metadata must still have deterministic diagnostic ordering.
func cylinderTemperatureOrder(r cylinderTemperatureCandidate) string {
	var bounds []uint64
	for _, p := range []**cylinderTemperatureRecord{&r.History, &r.LastGood} {
		if *p != nil {
			c := **p
			bounds = append(bounds, math.Float64bits(c.Codec.Minimum), math.Float64bits(c.Codec.Maximum))
			c.Codec.Minimum, c.Codec.Maximum = 0, 0
			*p = &c
		}
	}
	key, _ := json.Marshal(struct {
		Record cylinderTemperatureCandidate
		Bounds []uint64
	}{r, bounds})
	return string(key)
}
