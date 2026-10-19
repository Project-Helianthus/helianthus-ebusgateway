package main

import (
	"encoding/binary"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strconv"

	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
)

// Native-only seam for the public circuit-temperature-cutover contract. No
// acquisition, FactKey, decimal conversion, capability or publication occurs.
type circuitTemperatureSelector struct {
	PB, SB, Opcode, Operation, Group, Instance byte
	Register                                   uint16
	Initiator, Responder                       byte
	Direction, Association                     string
}

type circuitTemperatureIdentity struct {
	Asset, Service, Source, Epoch, Generation, Binding                                       string
	BindingRevision, AssociationRevision, ProfileRevision, DecoderRevision, SemanticRevision string
	Family, Profile, Firmware                                                                string
	Selector                                                                                 circuitTemperatureSelector
}

// The caller qualifies this exact codec/binding; the mapper never guesses it.
// Bounds are optional native profile bounds, separate from semantic bounds.
type circuitTemperatureCodec struct {
	Qualified, ActiveCircuit, BoundsKnown bool
	ByteOrder                             string
	Minimum, Maximum                      float64
}

type circuitTemperatureObservation struct {
	Identity                         circuitTemperatureIdentity
	Codec                            circuitTemperatureCodec
	RawRequest, RawReply, ValueBytes []byte
	Evidence                         []semreg.EvidenceRef
	EvidenceStatus                   string // Proven/Hypothesis/Unknown; independent of qualification.
	Correlated                       bool
	Failure                          string
	ReceivedAt                       semreg.TimePoint
	Receipt                          semreg.MonotonicPoint
	FreshForNS                       semreg.Uint64
}

type circuitTemperatureInput struct {
	Identity           circuitTemperatureIdentity
	Codec              circuitTemperatureCodec
	Previous, Attempts []circuitTemperatureObservation
}

type circuitTemperatureCandidate struct {
	Identity             circuitTemperatureIdentity
	Role, Withheld, Loss string // Local diagnostics, never reason DefinitionIDs.
	Attempts, History    []circuitTemperatureObservation
	LastGood             *circuitTemperatureObservation
	NativeValue          *float64 // Exact float32 widened to float64; no decimal rounding.
	Freshness            semreg.Freshness
}

type circuitTemperatureResult struct {
	Candidates []circuitTemperatureCandidate
	Facts      []semreg.FactCandidate // Always empty while the final binding is unresolved.
}

func mapCircuitTemperature(inputs []circuitTemperatureInput, evaluation semreg.EvaluationContext) circuitTemperatureResult {
	out := circuitTemperatureResult{Facts: []semreg.FactCandidate{}}
	for _, in := range inputs {
		c := circuitTemperatureCandidate{Identity: in.Identity, Freshness: semreg.FreshnessUnknown, Loss: "missing", Withheld: "circuit binding and decimal policy unresolved"}
		switch in.Identity.Selector.Register {
		case 8:
			c.Role = "measured_flow"
		case 7:
			c.Role, c.Withheld = "requested_flow", "requested flow is not measured flow or a write capability"
		case 0x20:
			c.Role, c.Withheld = "unknown_raw", "RR0020 is not a temperature; status meaning Unknown"
		default:
			c.Role, c.Withheld = "unsupported", "unsupported native selector"
		}
		c.Attempts = circuitTemperatureCloneAll(in.Attempts)
		c.History = circuitTemperatureCloneAll(in.Previous)
		// Canonical ordering preserves every diagnostic record, including provenance
		// differences. Arrival order never breaks a tie or chooses an instance.
		sort.Slice(c.Attempts, func(i, j int) bool {
			return circuitTemperatureKey(c.Attempts[i]) < circuitTemperatureKey(c.Attempts[j])
		})
		sort.Slice(c.History, func(i, j int) bool { return circuitTemperatureKey(c.History[i]) < circuitTemperatureKey(c.History[j]) })
		all := append(circuitTemperatureCloneAll(c.History), c.Attempts...)
		best, value, conflict, loss := circuitTemperatureSelect(all, in)
		c.Loss = loss
		if conflict {
			c.Loss = "conflicting receipt or clock continuity"
			// A competing attempt cannot erase independently valid prior state.
			var priorConflict bool
			best, value, priorConflict, _ = circuitTemperatureSelect(c.History, in)
			if priorConflict {
				best, value = nil, nil
			}
		}
		if best != nil {
			c.LastGood, c.NativeValue = best, value
			c.Freshness = circuitTemperatureFreshness(*best, evaluation)
			if c.Loss == "missing" {
				c.Loss = "native candidate only"
			}
		}
		out.Candidates = append(out.Candidates, c)
	}
	return out
}

func circuitTemperatureSelect(records []circuitTemperatureObservation, in circuitTemperatureInput) (*circuitTemperatureObservation, *float64, bool, string) {
	var best *circuitTemperatureObservation
	var value *float64
	conflict, clockConflict := false, false
	var clock semreg.ClockEpochID
	loss := "missing"
	for _, o := range records {
		v, why := circuitTemperatureDecode(o, in)
		if why != "" {
			if loss == "missing" {
				loss = why
			}
			continue
		}
		if clock != "" && clock != o.Receipt.ClockEpochID {
			clockConflict = true
			continue
		}
		clock = o.Receipt.ClockEpochID
		if best == nil || circuitTemperatureNS(o.Receipt.Nanoseconds) > circuitTemperatureNS(best.Receipt.Nanoseconds) {
			copy := circuitTemperatureClone(o)
			best, value, conflict = &copy, v, false
		} else if o.Receipt.Nanoseconds == best.Receipt.Nanoseconds && !reflect.DeepEqual(o, *best) {
			conflict = true
		}
	}
	return best, value, conflict || clockConflict, loss
}

func circuitTemperatureDecode(o circuitTemperatureObservation, in circuitTemperatureInput) (*float64, string) {
	if o.Identity != in.Identity {
		return nil, "identity mismatch"
	}
	s := in.Identity.Selector
	if s.PB != 0xb5 || s.SB != 0x24 || s.Opcode != 2 || s.Operation != 0 || s.Group != 2 || s.Instance > 8 || s.Direction != "target_to_initiator" || s.Association == "" {
		return nil, "unsupported native selector"
	}
	id := in.Identity
	for _, field := range []string{id.Asset, id.Service, id.Source, id.Epoch, id.Generation, id.Binding, id.BindingRevision, id.AssociationRevision, id.ProfileRevision, id.DecoderRevision, id.SemanticRevision, id.Family, id.Profile} {
		if field == "" {
			return nil, "incomplete identity"
		}
	}
	if !o.Correlated || o.Failure != "" {
		return nil, "failed or unassociated observation"
	}
	if o.Codec != in.Codec || !o.Codec.Qualified || !o.Codec.ActiveCircuit {
		return nil, "codec or circuit qualification unknown"
	}
	if o.ReceivedAt.Validate() != nil || o.Receipt.Validate() != nil {
		return nil, "malformed receipt"
	}
	if len(o.Evidence) == 0 {
		return nil, "missing evidence"
	}
	for _, e := range o.Evidence {
		if e.Validate() != nil {
			return nil, "malformed evidence"
		}
	}
	if s.Register == 0x20 {
		if len(o.ValueBytes) != 1 {
			return nil, "RR0020 layout conflicts with native contract"
		}
		return nil, "RR0020 raw evidence only"
	}
	if s.Register != 7 && s.Register != 8 {
		return nil, "unsupported native register"
	}
	if len(o.ValueBytes) != 4 {
		return nil, "float requires exactly four value bytes"
	}
	var bits uint32
	switch o.Codec.ByteOrder {
	case "little":
		bits = binary.LittleEndian.Uint32(o.ValueBytes)
	case "big":
		bits = binary.BigEndian.Uint32(o.ValueBytes)
	default:
		return nil, "byte order unknown"
	}
	v := float64(math.Float32frombits(bits))
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return nil, "non-finite native value"
	}
	if o.Codec.BoundsKnown {
		lo, hi := o.Codec.Minimum, o.Codec.Maximum
		if math.IsNaN(lo) || math.IsNaN(hi) || math.IsInf(lo, 0) || math.IsInf(hi, 0) || lo > hi || v < lo || v > hi {
			return nil, "outside or malformed supplied profile bounds"
		}
	}
	if s.Register == 8 && (v < -100 || v > 200) {
		return nil, "outside water-temperature semantic bounds"
	}
	return &v, ""
}

func circuitTemperatureFreshness(o circuitTemperatureObservation, e semreg.EvaluationContext) semreg.Freshness {
	if o.ReceivedAt.Validate() != nil || o.Receipt.Validate() != nil || e.EvaluatedAt.Validate() != nil || e.EvaluateMonotonic.Validate() != nil || o.FreshForNS.Validate() != nil || o.FreshForNS == "0" || o.Receipt.ClockEpochID != e.EvaluateMonotonic.ClockEpochID {
		return semreg.FreshnessUnknown
	}
	receipt, now := circuitTemperatureNS(o.Receipt.Nanoseconds), circuitTemperatureNS(e.EvaluateMonotonic.Nanoseconds)
	if now < receipt {
		return semreg.FreshnessUnknown
	}
	if now-receipt > circuitTemperatureNS(o.FreshForNS) {
		return semreg.FreshnessStale
	}
	return semreg.FreshnessFresh
}

func circuitTemperatureNS(v semreg.Uint64) uint64 {
	n, _ := strconv.ParseUint(string(v), 10, 64)
	return n
}
func circuitTemperatureKey(o circuitTemperatureObservation) string {
	return fmt.Sprintf("%#v", o)
}
func circuitTemperatureClone(o circuitTemperatureObservation) circuitTemperatureObservation {
	o.RawRequest = append([]byte(nil), o.RawRequest...)
	o.RawReply = append([]byte(nil), o.RawReply...)
	o.ValueBytes = append([]byte(nil), o.ValueBytes...)
	o.Evidence = append([]semreg.EvidenceRef(nil), o.Evidence...)
	return o
}
func circuitTemperatureCloneAll(in []circuitTemperatureObservation) []circuitTemperatureObservation {
	out := make([]circuitTemperatureObservation, len(in))
	for i, o := range in {
		out[i] = circuitTemperatureClone(o)
	}
	return out
}
