package main

import (
	"math"
	"strconv"
	"strings"
	"time"

	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/packs/thermal"
)

// These adapter types describe caller-decoded evidence, not acquisition or an
// accepted sensor binding. See the public humidity-semreg-cutover contract.
type thermalHumiditySelector struct {
	PB, SB, Opcode, OperationType, Group, Instance byte
	Register                                       uint16
	Direction                                      string
}

// All fields participate in the retention fence. A slot alone is not identity.
type thermalHumidityIdentity struct {
	Asset                                                  semreg.AssetID
	ServiceInstance                                        semreg.ServiceInstanceID
	Source                                                 semreg.SourceID
	SourceEpoch                                            semreg.SourceEpochID
	DriverGeneration, BindingRevision, AssociationRevision string
	Sensor, Profile, Role                                  string
	Selector                                               thermalHumiditySelector
}

type thermalHumidityObservation struct {
	Identity                                                            thermalHumidityIdentity
	Percent                                                             *float64
	RawRequest, RawReply                                                []byte
	Binding, Assignment                                                 *uint16
	Connected                                                           *bool
	Codec, SourceUnit, ResponseStatus, ResponseShape, EvidenceReference string
	Correlated                                                          bool
	ReceivedAt                                                          time.Time
	ReceiptMonotonic                                                    time.Duration
	ClockEpoch                                                          semreg.ClockEpochID
	PhenomenonAt                                                        *time.Time
	Sequence                                                            string
	EvidenceStatus                                                      string // Proven/Hypothesis/Unknown, independent of qualification.
	Qualification, Freshness                                            string // Caller decisions; this mapper invents no TTL.
}

type thermalHumidityRequest struct {
	Identity              thermalHumidityIdentity
	Observation, Previous *thermalHumidityObservation
}

type thermalHumidityResult struct {
	Identity                   thermalHumidityIdentity
	Native, RetainedNative     *thermalHumidityObservation
	Validity                   string
	SourceLoss                 string
	Field, Service, Capability *semreg.DefinitionRef
	Candidate                  *semreg.Value
	// No complete FactKey or admission is established by the supplied contract.
	Withheld string
}

// mapThermalHumidity maps each source independently. Results are source loss
// records, never requested-item projection dispositions: an absent remote cannot
// overwrite a projected circuit or zone. It does not select a legacy fallback.
func mapThermalHumidity(requests []thermalHumidityRequest) []thermalHumidityResult {
	metadata := thermal.Metadata()
	ref := func(id semreg.DefinitionID) *semreg.DefinitionRef {
		for _, f := range metadata.Fields {
			if f.Ref.ID == id {
				r := f.Ref
				return &r
			}
		}
		for _, s := range metadata.Services {
			if s.Ref.ID == id {
				r := s.Ref
				return &r
			}
		}
		for _, c := range metadata.Capabilities {
			if c.Ref.ID == id {
				r := c.Ref
				return &r
			}
		}
		return nil
	}
	clone := func(in *thermalHumidityObservation) *thermalHumidityObservation {
		if in == nil {
			return nil
		}
		out := *in
		out.RawRequest = append([]byte(nil), in.RawRequest...)
		out.RawReply = append([]byte(nil), in.RawReply...)
		if in.Percent != nil {
			v := *in.Percent
			out.Percent = &v
		}
		if in.Binding != nil {
			v := *in.Binding
			out.Binding = &v
		}
		if in.Assignment != nil {
			v := *in.Assignment
			out.Assignment = &v
		}
		if in.Connected != nil {
			v := *in.Connected
			out.Connected = &v
		}
		if in.PhenomenonAt != nil {
			v := *in.PhenomenonAt
			out.PhenomenonAt = &v
		}
		return &out
	}
	valid := func(o *thermalHumidityObservation) bool {
		return o != nil && o.Percent != nil && !math.IsNaN(*o.Percent) && !math.IsInf(*o.Percent, 0) && *o.Percent >= 0 && *o.Percent <= 100
	}
	results := make([]thermalHumidityResult, 0, len(requests))
	for _, request := range requests {
		r := thermalHumidityResult{Identity: request.Identity, Native: clone(request.Observation), Validity: "missing", Withheld: "humidity_binding_unresolved"}
		s := request.Identity.Selector
		service := ""
		if s.PB == 0xb5 && s.SB == 0x24 && s.OperationType == 0 && s.Direction == "target_to_initiator" {
			switch {
			case s.Opcode == 2 && s.Group == 3 && s.Register == 0x28:
				service = "zone"
			case s.Opcode == 2 && s.Group == 2 && s.Register == 0x22:
				service = "circuit"
			case s.Opcode == 6 && (s.Group == 9 || s.Group == 10) && s.Register == 7:
				if request.Identity.ServiceInstance != "" {
					service = "zone"
				}
			}
		}
		measurement := service != "" || (s.PB == 0xb5 && s.SB == 0x24 && s.OperationType == 0 && s.Direction == "target_to_initiator" && s.Opcode == 6 && (s.Group == 9 || s.Group == 10) && s.Register == 7)
		if !measurement {
			r.Withheld = "unsupported_native_selector"
		} else {
			r.Field = ref("thermal.measurement.humidity_relative")
			if service != "" {
				r.Service = ref(semreg.DefinitionID("thermal.service." + service))
				r.Capability = ref(semreg.DefinitionID("thermal.capability.read." + service))
			}
		}
		if o := request.Observation; o != nil {
			switch {
			case o.Identity != request.Identity:
				r.Validity, r.SourceLoss = "identity_mismatch", "identity_changed"
			case o.Percent == nil:
				r.SourceLoss = "missing"
			case !valid(o):
				r.Validity, r.SourceLoss = "invalid", "invalid_percent"
			default:
				r.Validity = "valid"
				if measurement {
					// Fixed decimal notation preserves percent magnitude, including fractions.
					text := strconv.FormatFloat(*o.Percent, 'f', -1, 64)
					exponent := int32(0)
					if dot := strings.IndexByte(text, '.'); dot >= 0 {
						exponent = -int32(len(text) - dot - 1)
						text = text[:dot] + text[dot+1:]
					}
					if *o.Percent == 0 {
						text, exponent = "0", 0
					} else {
						text = strings.TrimLeft(text, "0")
						for strings.HasSuffix(text, "0") {
							text = strings.TrimSuffix(text, "0")
							exponent++
						}
					}
					number := semreg.Decimal{Coefficient: text, Exponent10: exponent}
					if number.Validate() == nil {
						r.Candidate = &semreg.Value{Kind: semreg.ValueQuantity, Quantity: &semreg.Quantity{Number: number, Unit: "unit.percent"}}
					} else {
						// Native validity does not imply exact pack representability.
						r.Withheld = "percent_unrepresentable"
					}
				}
			}
		} else {
			r.SourceLoss = "missing"
		}
		// Native retention is validity-only, not qualification or current admission.
		// Never refresh its receipt, freshness, or qualification on a failed read.
		if r.Validity == "valid" {
			r.RetainedNative = clone(request.Observation)
		} else if valid(request.Previous) && request.Previous.Identity == request.Identity {
			r.RetainedNative = clone(request.Previous)
		}
		results = append(results, r)
	}
	return results
}
