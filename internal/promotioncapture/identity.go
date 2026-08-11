package promotioncapture

import (
	"encoding/json"
	"fmt"
	"strings"
)

const (
	SourceProfileDomain = "HELIANTHUS:EEBUS:SOURCE-PROFILE:V1\x00"
	EEBusIdentityDomain = "HELIANTHUS:EEBUS:CAPTURED-IDENTITY:V1\x00"
	EBusSelectorDomain  = "HELIANTHUS:EBUS:B524-SELECTOR:V1\x00"
	CheckpointDomain    = "HELIANTHUS:LEAF-PROMOTION:WINDOW-CHECKPOINT:V1\x00"
)

func NewB524Identity(selector EBusSelector, sourceAddress int) (B524Identity, error) {
	if selector.Family != "B524" || selector.TargetAddress < 0 || selector.TargetAddress > 255 ||
		sourceAddress < 1 || sourceAddress > 255 {
		return B524Identity{}, fmt.Errorf("%w: invalid B524 identity", ErrInvalidEvidence)
	}
	targetDigest, err := CanonicalDigest(EBusSelectorDomain, map[string]any{
		"family": selector.Family, "target_address": selector.TargetAddress,
	})
	if err != nil {
		return B524Identity{}, err
	}
	identity := B524Identity{
		Family: selector.Family, TargetPseudonym: "target-" + targetDigest[7:39],
		TargetAddress: selector.TargetAddress, SourceAddress: sourceAddress,
		Opcode: selector.Opcode, GG: selector.GG, II: selector.II, RR: selector.RR,
		GroupMeaning: selector.GroupMeaning, InstanceGate: selector.InstanceGate,
		RegisterCategory: selector.RegisterCategory, UnitScaleSource: selector.UnitScaleSource,
	}
	digest, err := digestWithoutField(EBusSelectorDomain, identity, "selector_hash")
	if err != nil {
		return B524Identity{}, err
	}
	identity.SelectorHash = digest
	return identity, nil
}

func NewEEBusIdentity(source EEBusSource, serviceID, deviceAddress string, entityAddress []uint64, featureAddress uint64) (EEBusIdentity, error) {
	if strings.TrimSpace(serviceID) == "" || strings.TrimSpace(deviceAddress) == "" || len(entityAddress) == 0 {
		return EEBusIdentity{}, fmt.Errorf("%w: incomplete eeBUS identity", ErrInvalidEvidence)
	}
	sourceHash, err := CanonicalDigest(SourceProfileDomain, source)
	if err != nil {
		return EEBusIdentity{}, err
	}
	identity := EEBusIdentity{
		ServiceID: serviceID, DeviceAddress: deviceAddress,
		EntityAddress: append([]uint64(nil), entityAddress...), FeatureAddress: featureAddress,
		EntitySlot: source.EntitySlot, EntityType: source.EntityType,
		FeatureType: source.FeatureType, FeatureRole: source.FeatureRole,
		DescriptionFunctions: append([]string(nil), source.DescriptionFunctions...),
		ConstraintsFunction:  cloneStringPointer(source.ConstraintsFunction),
		ValueFunctions:       append([]string(nil), source.ValueFunctions...), FieldPath: source.FieldPath,
		Descriptor: append(json.RawMessage(nil), source.Descriptor...), Unit: cloneStringPointer(source.Unit),
		DeclaredConstraints: cloneJSONPointer(source.DeclaredConstraints),
		Conversion:          cloneJSONPointer(source.Conversion), ExactMapping: cloneJSONPointer(source.ExactMapping),
		MappingProfile: cloneJSONPointer(source.MappingProfile), SourceProfileHash: sourceHash,
	}
	digest, err := digestWithoutField(EEBusIdentityDomain, identity, "identity_hash")
	if err != nil {
		return EEBusIdentity{}, err
	}
	identity.IdentityHash = digest
	return identity, nil
}

func BindCheckpointHash(checkpoint *WindowCheckpoint) error {
	if checkpoint == nil {
		return fmt.Errorf("%w: nil checkpoint", ErrInvalidEvidence)
	}
	digest, err := digestWithoutField(CheckpointDomain, *checkpoint, "checkpoint_hash")
	if err != nil {
		return err
	}
	checkpoint.CheckpointHash = digest
	return nil
}

func digestWithoutField(domain string, value any, field string) (string, error) {
	object, err := objectWithoutField(value, field)
	if err != nil {
		return "", err
	}
	return CanonicalDigest(domain, object)
}

func objectWithoutField(value any, field string) (map[string]any, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.UseNumber()
	var object map[string]any
	if err := decoder.Decode(&object); err != nil {
		return nil, err
	}
	delete(object, field)
	return object, nil
}

func cloneJSONPointer[T any](value *T) *T {
	if value == nil {
		return nil
	}
	raw, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	var result T
	if err := json.Unmarshal(raw, &result); err != nil {
		panic(err)
	}
	return &result
}
