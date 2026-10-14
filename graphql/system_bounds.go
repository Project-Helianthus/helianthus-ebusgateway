package graphql

import (
	"fmt"
	"math"
	"strings"

	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
	vaillantsystem "github.com/Project-Helianthus/helianthus-ebusreg/vaillant/system"
)

// SystemConfigFieldDHWBivalencePoint is the system-config field whose bound is
// resolved per controller profile instead of being a fixed range.
const SystemConfigFieldDHWBivalencePoint = "dhwBivalencePointC"

// Sources of an active config bound.
const (
	// ConfigBoundSourceControllerProfile: the bound comes from a controller
	// profile whose identity matched exactly (ebusreg LookupB524Bound found).
	ConfigBoundSourceControllerProfile = "controller_profile"
	// ConfigBoundSourceUnmatchedPolicy: no profile bound applies; the
	// conservative gateway policy bound is active.
	ConfigBoundSourceUnmatchedPolicy = "unmatched_policy"
)

// Reasons the unmatched policy is active. They mirror the ebusreg lookup
// statuses; "unqualified_bound" and "invalid_bound" cover a found bound that
// must not be trusted for writes.
const (
	ConfigBoundReasonUnknownProfile   = "unknown_profile"
	ConfigBoundReasonUnknownRegister  = "unknown_register"
	ConfigBoundReasonUnqualifiedBound = "unqualified_bound"
	ConfigBoundReasonInvalidBound     = "invalid_bound"
)

// Unmatched-identity policy for dhwBivalencePointC: the range every retained
// controller profile agrees on (the smallest published maximum). The operator
// may change this policy to reject the write instead.
// dhwBivalenceInstance is the II byte every system-config write and read of
// dhwBivalencePointC sends; the profile bound is selector-scoped to it.
const dhwBivalenceInstance = byte(0x00)

const (
	unmatchedDHWBivalenceMinC = -20.0
	unmatchedDHWBivalenceMaxC = 20.0
)

// ConfigBound is the active write bound of one config field.
type ConfigBound struct {
	MinC   float64
	MaxC   float64
	Source string
	// Reason is empty for a controller-profile bound; for the unmatched policy
	// it names why no profile bound applies.
	Reason string
	// Profile names the matched controller identity (EID/software/hardware/API
	// version.revision); empty for the unmatched policy.
	Profile string
}

// Describe renders the bound for mutation errors.
func (b ConfigBound) Describe() string {
	if b.Source == ConfigBoundSourceControllerProfile {
		return fmt.Sprintf("controller profile %s allows [%.4g, %.4g]", b.Profile, b.MinC, b.MaxC)
	}
	return fmt.Sprintf("controller identity unmatched (%s): conservative policy bound [%.4g, %.4g] applies", b.Reason, b.MinC, b.MaxC)
}

// ControllerIdentityFromEntry builds the part of the B524 controller identity
// that the device registry knows (EID, raw software and hardware). The API
// version and revision are not registry data; callers that know them (read from
// B524 OP00 identifiers 0006h/0007h) set them afterwards.
//
// The identity is matched exactly by ebusreg: a missing field only matches a
// profile that did not record it, so an identity without the API qualifier
// never matches a profile that recorded one.
func ControllerIdentityFromEntry(entry registry.DeviceEntry) vaillantsystem.B524ControllerIdentity {
	if entry == nil {
		return vaillantsystem.B524ControllerIdentity{}
	}
	return vaillantsystem.B524ControllerIdentity{
		EID:         strings.ToUpper(strings.TrimSpace(entry.DeviceID())),
		SoftwareRaw: strings.TrimSpace(entry.SoftwareVersion()),
		HardwareRaw: strings.TrimSpace(entry.HardwareVersion()),
	}
}

func unmatchedDHWBivalenceBound(reason string) ConfigBound {
	return ConfigBound{
		MinC:   unmatchedDHWBivalenceMinC,
		MaxC:   unmatchedDHWBivalenceMaxC,
		Source: ConfigBoundSourceUnmatchedPolicy,
		Reason: reason,
	}
}

func controllerIdentityLabel(id vaillantsystem.B524ControllerIdentity) string {
	label := fmt.Sprintf("%s/SW%s/HW%s", id.EID, id.SoftwareRaw, id.HardwareRaw)
	if id.APIVersion != "" || id.Revision != "" {
		label += fmt.Sprintf("/API%s.%s", id.APIVersion, id.Revision)
	}
	return label
}

// ResolveDHWBivalenceBound resolves the dhwBivalencePointC bound from the exact
// controller identity through the ebusreg controller-profile lookup (OP02,
// GG=0x00, RR=0x0001). Only a found, retained-record-qualified, sane bound is
// used; every other outcome yields the conservative unmatched policy bound.
// No profile value is ever substituted for a different identity.
func ResolveDHWBivalenceBound(identity vaillantsystem.B524ControllerIdentity) ConfigBound {
	result := vaillantsystem.LookupB524Bound(identity, mutationB524OpcodeLocal, 0x00, dhwBivalenceInstance, 0x0001)
	switch result.Status {
	case vaillantsystem.B524BoundFound:
		bound := result.Bound
		if !bound.Qualified {
			return unmatchedDHWBivalenceBound(ConfigBoundReasonUnqualifiedBound)
		}
		if math.IsNaN(bound.Min) || math.IsNaN(bound.Max) || math.IsInf(bound.Min, 0) || math.IsInf(bound.Max, 0) || bound.Min > bound.Max {
			return unmatchedDHWBivalenceBound(ConfigBoundReasonInvalidBound)
		}
		return ConfigBound{
			MinC:    bound.Min,
			MaxC:    bound.Max,
			Source:  ConfigBoundSourceControllerProfile,
			Profile: controllerIdentityLabel(identity),
		}
	case vaillantsystem.B524BoundUnknownRegister:
		return unmatchedDHWBivalenceBound(ConfigBoundReasonUnknownRegister)
	default:
		return unmatchedDHWBivalenceBound(ConfigBoundReasonUnknownProfile)
	}
}

// EncodeSystemConfigValueForController validates and encodes a system config
// value. For dhwBivalencePointC the range is the bound resolved from identity;
// every other field keeps its fixed spec range. The returned spec carries the
// effective range, so the read-back checks are unchanged. A value outside the
// bound fails here, before any bus write, and the error names the active bound.
func EncodeSystemConfigValueForController(fieldName, rawValue string, identity vaillantsystem.B524ControllerIdentity) ([]byte, configFieldSpec, *ConfigBound, error) {
	spec, err := resolveConfigFieldSpec("system", fieldName, systemConfigFieldSpecs)
	if err != nil {
		return nil, configFieldSpec{}, nil, err
	}
	var bound *ConfigBound
	if fieldName == SystemConfigFieldDHWBivalencePoint {
		resolved := ResolveDHWBivalenceBound(identity)
		bound = &resolved
		spec.min = resolved.MinC
		spec.max = resolved.MaxC
	}
	payload, err := encodeConfigValue(spec, rawValue)
	if err != nil {
		if bound != nil {
			return nil, configFieldSpec{}, bound, fmt.Errorf("%s: %s: %w", fieldName, bound.Describe(), err)
		}
		return nil, configFieldSpec{}, nil, err
	}
	return payload, spec, bound, nil
}
