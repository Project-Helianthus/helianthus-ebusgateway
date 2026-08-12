package promotioncapture

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

const (
	SourceProfileDomain = "HELIANTHUS:EEBUS:SOURCE-PROFILE:V1\x00"
	EEBusIdentityDomain = "HELIANTHUS:EEBUS:CAPTURED-IDENTITY:V1\x00"
	EBusSelectorDomain  = "HELIANTHUS:EBUS:B524-SELECTOR:V1\x00"
	EBusB555Domain      = "HELIANTHUS:EBUS:B555-TIMER-SELECTOR:V1\x00"
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

func NewB555Identity(selector B555Selector, targetAddress, sourceAddress int) (EBusIdentity, error) {
	if selector != (B555Selector{
		Family: "B555", Operation: "TIMER_READ", TargetPseudonymRule: "active_controller_target_hash",
		DeviceFamily: "BASV2", ScheduleProgram: "DHW", SlotIndex: 0, DayOfWeek: "MONDAY",
		TimeIdentity: "00:00:00", OperationModeContext: "temp_slots_1_shared_setpoint",
		UnitScaleSource: "B555_DHW_TEMPERATURE_RAW_DIV10_C", FieldPath: "timerSlot.temperature",
		Unit: "degC", CouplingRule: "dhw_temp_slots_1_mirrors_b524_setpoint",
	}) || targetAddress < 0 || targetAddress > 255 || sourceAddress < 1 || sourceAddress > 255 {
		return EBusIdentity{}, fmt.Errorf("%w: invalid B555 identity", ErrInvalidEvidence)
	}
	targetDigest, err := CanonicalDigest(EBusSelectorDomain, map[string]any{
		"family": "B524", "target_address": targetAddress,
	})
	if err != nil {
		return EBusIdentity{}, err
	}
	identity := EBusIdentity{
		Family: selector.Family, Operation: selector.Operation,
		TargetPseudonymRule: selector.TargetPseudonymRule, TargetPseudonym: "target-" + targetDigest[7:39],
		TargetAddress: targetAddress, SourceAddress: sourceAddress, DeviceFamily: selector.DeviceFamily,
		ScheduleProgram: selector.ScheduleProgram, SlotIndex: selector.SlotIndex, DayOfWeek: selector.DayOfWeek,
		TimeIdentity: selector.TimeIdentity, OperationModeContext: selector.OperationModeContext,
		UnitScaleSource: selector.UnitScaleSource, FieldPath: selector.FieldPath, Unit: selector.Unit,
		CouplingRule: selector.CouplingRule,
	}
	digest, err := digestWithoutField(EBusB555Domain, identity, "selector_hash")
	if err != nil {
		return EBusIdentity{}, err
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
		DescriptionFunctions: append([]string{}, source.DescriptionFunctions...),
		ConstraintsFunction:  cloneStringPointer(source.ConstraintsFunction),
		ValueFunctions:       append([]string(nil), source.ValueFunctions...), FieldPath: source.FieldPath,
		Descriptor: append(json.RawMessage(nil), source.Descriptor...), Unit: cloneStringPointer(source.Unit),
		DeclaredConstraints: cloneJSONPointer(source.DeclaredConstraints),
		ExactMapping:        cloneJSONPointer(source.ExactMapping), SourceProfileHash: sourceHash,
	}
	digest, err := digestWithoutField(EEBusIdentityDomain, identity, "identity_hash")
	if err != nil {
		return EEBusIdentity{}, err
	}
	identity.IdentityHash = digest
	return identity, nil
}

func (identity EBusIdentity) MarshalJSON() ([]byte, error) {
	switch identity.Family {
	case "B524":
		return json.Marshal(struct {
			Family           string `json:"family"`
			TargetPseudonym  string `json:"target_pseudonym"`
			TargetAddress    int    `json:"target_address"`
			SourceAddress    int    `json:"source_address"`
			Opcode           int    `json:"opcode"`
			GG               int    `json:"GG"`
			II               int    `json:"II"`
			RR               int    `json:"RR"`
			GroupMeaning     string `json:"group_meaning"`
			InstanceGate     string `json:"instance_gate"`
			RegisterCategory string `json:"register_category"`
			UnitScaleSource  string `json:"unit_scale_source"`
			SelectorHash     string `json:"selector_hash"`
		}{
			identity.Family, identity.TargetPseudonym, identity.TargetAddress, identity.SourceAddress,
			identity.Opcode, identity.GG, identity.II, identity.RR, identity.GroupMeaning,
			identity.InstanceGate, identity.RegisterCategory, identity.UnitScaleSource, identity.SelectorHash,
		})
	case "B555":
		return json.Marshal(struct {
			Family               string `json:"family"`
			Operation            string `json:"operation"`
			TargetPseudonymRule  string `json:"target_pseudonym_rule"`
			TargetPseudonym      string `json:"target_pseudonym"`
			TargetAddress        int    `json:"target_address"`
			SourceAddress        int    `json:"source_address"`
			DeviceFamily         string `json:"device_family"`
			ScheduleProgram      string `json:"schedule_program"`
			SlotIndex            int    `json:"slot_index"`
			DayOfWeek            string `json:"day_of_week"`
			TimeIdentity         string `json:"time_identity"`
			OperationModeContext string `json:"operation_mode_context"`
			UnitScaleSource      string `json:"unit_scale_source"`
			FieldPath            string `json:"field_path"`
			Unit                 string `json:"unit"`
			CouplingRule         string `json:"coupling_rule"`
			SelectorHash         string `json:"selector_hash"`
		}{
			identity.Family, identity.Operation, identity.TargetPseudonymRule, identity.TargetPseudonym,
			identity.TargetAddress, identity.SourceAddress, identity.DeviceFamily, identity.ScheduleProgram,
			identity.SlotIndex, identity.DayOfWeek, identity.TimeIdentity, identity.OperationModeContext,
			identity.UnitScaleSource, identity.FieldPath, identity.Unit, identity.CouplingRule, identity.SelectorHash,
		})
	default:
		return nil, fmt.Errorf("%w: unknown eBUS identity family %q", ErrInvalidEvidence, identity.Family)
	}
}

func (identity *EBusIdentity) UnmarshalJSON(raw []byte) error {
	if identity == nil {
		return fmt.Errorf("%w: nil eBUS identity", ErrInvalidEvidence)
	}
	var envelope struct {
		Family string `json:"family"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return err
	}
	var decoded EBusIdentity
	switch envelope.Family {
	case "B524":
		var value struct {
			Family           string `json:"family"`
			TargetPseudonym  string `json:"target_pseudonym"`
			TargetAddress    int    `json:"target_address"`
			SourceAddress    int    `json:"source_address"`
			Opcode           int    `json:"opcode"`
			GG               int    `json:"GG"`
			II               int    `json:"II"`
			RR               int    `json:"RR"`
			GroupMeaning     string `json:"group_meaning"`
			InstanceGate     string `json:"instance_gate"`
			RegisterCategory string `json:"register_category"`
			UnitScaleSource  string `json:"unit_scale_source"`
			SelectorHash     string `json:"selector_hash"`
		}
		if err := decodeStrictJSON(raw, &value); err != nil {
			return err
		}
		decoded = EBusIdentity{
			Family: value.Family, TargetPseudonym: value.TargetPseudonym, TargetAddress: value.TargetAddress,
			SourceAddress: value.SourceAddress, Opcode: value.Opcode, GG: value.GG, II: value.II, RR: value.RR,
			GroupMeaning: value.GroupMeaning, InstanceGate: value.InstanceGate,
			RegisterCategory: value.RegisterCategory, UnitScaleSource: value.UnitScaleSource, SelectorHash: value.SelectorHash,
		}
	case "B555":
		var value struct {
			Family               string `json:"family"`
			Operation            string `json:"operation"`
			TargetPseudonymRule  string `json:"target_pseudonym_rule"`
			TargetPseudonym      string `json:"target_pseudonym"`
			TargetAddress        int    `json:"target_address"`
			SourceAddress        int    `json:"source_address"`
			DeviceFamily         string `json:"device_family"`
			ScheduleProgram      string `json:"schedule_program"`
			SlotIndex            int    `json:"slot_index"`
			DayOfWeek            string `json:"day_of_week"`
			TimeIdentity         string `json:"time_identity"`
			OperationModeContext string `json:"operation_mode_context"`
			UnitScaleSource      string `json:"unit_scale_source"`
			FieldPath            string `json:"field_path"`
			Unit                 string `json:"unit"`
			CouplingRule         string `json:"coupling_rule"`
			SelectorHash         string `json:"selector_hash"`
		}
		if err := decodeStrictJSON(raw, &value); err != nil {
			return err
		}
		decoded = EBusIdentity{
			Family: value.Family, Operation: value.Operation, TargetPseudonymRule: value.TargetPseudonymRule,
			TargetPseudonym: value.TargetPseudonym, TargetAddress: value.TargetAddress, SourceAddress: value.SourceAddress,
			DeviceFamily: value.DeviceFamily, ScheduleProgram: value.ScheduleProgram, SlotIndex: value.SlotIndex,
			DayOfWeek: value.DayOfWeek, TimeIdentity: value.TimeIdentity, OperationModeContext: value.OperationModeContext,
			UnitScaleSource: value.UnitScaleSource, FieldPath: value.FieldPath, Unit: value.Unit,
			CouplingRule: value.CouplingRule, SelectorHash: value.SelectorHash,
		}
	default:
		return fmt.Errorf("%w: unknown eBUS identity family %q", ErrInvalidEvidence, envelope.Family)
	}
	*identity = decoded
	return nil
}

func decodeStrictJSON(raw []byte, destination any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return fmt.Errorf("%w: trailing identity JSON", ErrInvalidEvidence)
	}
	return nil
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
