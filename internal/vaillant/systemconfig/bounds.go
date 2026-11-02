package systemconfig

import (
	"fmt"
	"math"
	"strings"

	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
	vaillantsystem "github.com/Project-Helianthus/helianthus-ebusreg/vaillant/system"
)

// SystemConfigFieldDHWBivalencePoint is the system-config field whose bound is
// conservatively narrowed by controller-profile evidence.
const SystemConfigFieldDHWBivalencePoint = "dhwBivalencePointC"

type ControllerIdentity = vaillantsystem.B524ControllerIdentity

var canonicalRegulatorTargetAddresses = [...]byte{0x15, 0x35, 0x75, 0xF5, 0x76, 0xF6}

func CanonicalRegulatorTargetAddresses() [6]byte {
	return canonicalRegulatorTargetAddresses
}

// Sources of an active config bound.
const (
	// ConfigBoundSourceControllerProfile: the bound comes from a controller
	// profile whose identity matched exactly (ebusreg LookupB524Bound found),
	// intersected with the conservative policy; it does not qualify an installation.
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

// Released compatibility policy window for dhwBivalencePointC. A controller-profile
// match identifies an observation context, not the target installation; profile
// evidence may narrow this window but must never widen it.

const (
	unmatchedDHWBivalenceMinC = -20.0
	unmatchedDHWBivalenceMaxC = 50.0
)

// ConfigBound is the active write bound of one config field.
type ConfigBound struct {
	// ControllerAddress binds injected write validation to its native target.
	// Zero means no target was supplied; it cannot authorize compatibility writes.
	ControllerAddress          byte                  `json:"-"`
	ControllerRegistryIdentity BoundRegistryIdentity `json:"-"`
	MinC                       float64
	MaxC                       float64
	Source                     string
	// Reason is empty for a controller-profile bound; for the unmatched policy
	// it names why no profile bound applies.
	Reason string
	// Profile names the matched controller identity (EID/software/hardware/API
	// version.revision); empty for the unmatched policy.
	Profile string
	// NarrowedOnly means profile evidence can only narrow the conservative
	// policy window, even when the effective endpoints are unchanged.
	NarrowedOnly bool
}

// Describe renders the bound for mutation errors.
func (b ConfigBound) Describe() string {
	if b.Source == ConfigBoundSourceControllerProfile {
		return fmt.Sprintf("controller profile %s allows [%.4g, %.4g]", b.Profile, b.MinC, b.MaxC)
	}
	return fmt.Sprintf("controller identity unmatched (%s): conservative policy bound [%.4g, %.4g] applies", b.Reason, b.MinC, b.MaxC)
}

// ControllerIdentityFromSnapshot builds the part of the B524 controller identity
// that the device registry knows (EID, raw software and hardware). The API
// version and revision are not registry data and remain absent here. Callers
// may supply them only from qualified identity data already held by the gateway;
// validation must never acquire them on demand.
//
// The identity is matched exactly by ebusreg: a missing field only matches a
// profile that did not record it, so an identity without the API qualifier
// never matches a profile that recorded one.
func ControllerIdentityFromSnapshot(entry registry.DeviceEntrySnapshot) vaillantsystem.B524ControllerIdentity {
	return vaillantsystem.B524ControllerIdentity{
		EID:         strings.ToUpper(strings.TrimSpace(entry.DeviceID)),
		SoftwareRaw: strings.TrimSpace(entry.SoftwareVersion),
		HardwareRaw: strings.TrimSpace(entry.HardwareVersion),
	}
}

// controllerIdentityFromRegistry uses the same canonical target priority as
// findControllerEntry, but never reads identity accessors on a live entry.
// Registries without atomic snapshots supply no identity, so the conservative
// unmatched policy applies.
func ControllerIdentityFromRegistry(reg any) vaillantsystem.B524ControllerIdentity {
	snapshots, ok := reg.(interface {
		LookupEntrySnapshot(byte) (registry.DeviceEntrySnapshot, bool)
	})
	if !ok {
		return vaillantsystem.B524ControllerIdentity{}
	}
	for _, addr := range canonicalRegulatorTargetAddresses {
		if snap, found := snapshots.LookupEntrySnapshot(addr); found {
			return ControllerIdentityFromSnapshot(snap)
		}
	}
	return vaillantsystem.B524ControllerIdentity{}
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
// intersected with the conservative policy window; an empty intersection or
// any other outcome yields the conservative unmatched policy bound.
// No profile value is ever substituted for a different identity.
func ResolveDHWBivalenceBound(identity vaillantsystem.B524ControllerIdentity) ConfigBound {
	spec := systemConfigFieldSpecs[SystemConfigFieldDHWBivalencePoint]
	result := vaillantsystem.LookupB524Bound(identity, OpcodeLocal, spec.Group(), Instance, spec.Addr())
	return dhwBivalenceBoundFromResult(identity, result)
}

// dhwBivalenceBoundFromResult keeps the conservative policy total even for an
// unknown lookup status, unqualified evidence, or malformed numeric bounds.
func dhwBivalenceBoundFromResult(identity vaillantsystem.B524ControllerIdentity, result vaillantsystem.B524BoundResult) ConfigBound {
	switch result.Status {
	case vaillantsystem.B524BoundFound:
		bound := result.Bound
		if !bound.Qualified {
			return unmatchedDHWBivalenceBound(ConfigBoundReasonUnqualifiedBound)
		}
		if math.IsNaN(bound.Min) || math.IsNaN(bound.Max) || math.IsInf(bound.Min, 0) || math.IsInf(bound.Max, 0) || bound.Min > bound.Max {
			return unmatchedDHWBivalenceBound(ConfigBoundReasonInvalidBound)
		}
		minC := math.Max(bound.Min, unmatchedDHWBivalenceMinC)
		maxC := math.Min(bound.Max, unmatchedDHWBivalenceMaxC)
		if minC > maxC {
			return unmatchedDHWBivalenceBound(ConfigBoundReasonInvalidBound)
		}
		return ConfigBound{
			NarrowedOnly: true,
			MinC:         minC,
			MaxC:         maxC,
			Source:       ConfigBoundSourceControllerProfile,
			Profile:      controllerIdentityLabel(identity),
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
func EncodeSystemConfigValueForController(fieldName, rawValue string, identity vaillantsystem.B524ControllerIdentity) ([]byte, ConfigFieldSpec, *ConfigBound, error) {
	spec, err := resolveConfigFieldSpec("system", fieldName, systemConfigFieldSpecs)
	if err != nil {
		return nil, ConfigFieldSpec{}, nil, err
	}
	var bound *ConfigBound
	if fieldName == SystemConfigFieldDHWBivalencePoint {
		resolved := ResolveDHWBivalenceBound(identity)
		bound = &resolved
		spec.Min = resolved.MinC
		spec.Max = resolved.MaxC
	}
	payload, err := EncodeValue(spec, rawValue)
	if err != nil {
		if bound != nil {
			return nil, ConfigFieldSpec{}, bound, fmt.Errorf("%s: %s: %w", fieldName, bound.Describe(), err)
		}
		return nil, ConfigFieldSpec{}, nil, err
	}
	return payload, spec, bound, nil
}

// BoundRegistryIdentity retains internal registry provenance, including the
// observation generation that fences qualification changes between publications.
// It is never projected onto public surfaces.
type BoundRegistryIdentity struct {
	Valid                                                    bool
	Address                                                  byte
	Generation                                               uint64
	Manufacturer, DeviceID, SoftwareVersion, HardwareVersion string
	SerialNumber, MacAddress                                 string
}

// CaptureBoundRegistryIdentity requires a coherent atomic registry snapshot.
func CaptureBoundRegistryIdentity(reg any, address byte) BoundRegistryIdentity {
	snapshots, ok := reg.(interface {
		LookupEntrySnapshot(byte) (registry.DeviceEntrySnapshot, bool)
		WithObservationGeneration(func(uint64)) bool
	})
	if !ok {
		return BoundRegistryIdentity{}
	}
	var before, after uint64
	if !snapshots.WithObservationGeneration(func(g uint64) { before = g }) {
		return BoundRegistryIdentity{}
	}
	snap, found := snapshots.LookupEntrySnapshot(address)
	if !snapshots.WithObservationGeneration(func(g uint64) { after = g }) || !found || before != after {
		return BoundRegistryIdentity{}
	}
	return BoundRegistryIdentity{Valid: true, Address: address, Generation: after,
		Manufacturer: snap.Manufacturer, DeviceID: snap.DeviceID,
		SoftwareVersion: snap.SoftwareVersion, HardwareVersion: snap.HardwareVersion,
		SerialNumber: snap.SerialNumber, MacAddress: snap.MacAddress}
}
