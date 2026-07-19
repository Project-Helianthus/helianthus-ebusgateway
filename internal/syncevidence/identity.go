package syncevidence

import (
	"bytes"
	"encoding/json"
)

func (identity EBusSourceIdentityV1) MarshalJSON() ([]byte, error) {
	switch identity.Family {
	case EBusFamilyB509:
		return json.Marshal(struct {
			Family          EBusFamily `json:"family"`
			TargetPseudonym string     `json:"target_pseudonym"`
			TargetAddress   uint8      `json:"target_address"`
			TargetProduct   string     `json:"target_product"`
			RegisterFamily  string     `json:"register_family"`
			RegisterID      uint16     `json:"register_id"`
			UnitScaleSource string     `json:"unit_scale_source"`
			EvidenceRole    string     `json:"evidence_role"`
		}{identity.Family, identity.TargetPseudonym, identity.TargetAddress, identity.TargetProduct, identity.RegisterFamily, identity.RegisterID, identity.UnitScaleSource, identity.EvidenceRole})
	case EBusFamilyB524:
		return json.Marshal(struct {
			Family           EBusFamily `json:"family"`
			TargetPseudonym  string     `json:"target_pseudonym"`
			Opcode           uint8      `json:"opcode"`
			GG               uint8      `json:"GG"`
			II               uint8      `json:"II"`
			RR               uint16     `json:"RR"`
			TargetAddress    uint8      `json:"target_address"`
			SourceAddress    uint8      `json:"source_address"`
			GroupMeaning     string     `json:"group_meaning"`
			InstanceGate     string     `json:"instance_gate"`
			RegisterCategory string     `json:"register_category"`
			UnitScaleSource  string     `json:"unit_scale_source"`
		}{identity.Family, identity.TargetPseudonym, identity.Opcode, identity.GG, identity.II, identity.RR, identity.TargetAddress, identity.SourceAddress, identity.GroupMeaning, identity.InstanceGate, identity.RegisterCategory, identity.UnitScaleSource})
	case EBusFamilyB555:
		return json.Marshal(struct {
			Family               EBusFamily `json:"family"`
			TargetPseudonym      string     `json:"target_pseudonym"`
			DeviceFamily         string     `json:"device_family"`
			ScheduleProgram      string     `json:"schedule_program"`
			SlotIndex            uint8      `json:"slot_index"`
			DayOfWeek            string     `json:"day_of_week"`
			TimeIdentity         string     `json:"time_identity"`
			OperationModeContext string     `json:"operation_mode_context"`
			UnitScaleSource      string     `json:"unit_scale_source"`
		}{identity.Family, identity.TargetPseudonym, identity.DeviceFamily, identity.ScheduleProgram, identity.SlotIndex, identity.DayOfWeek, identity.TimeIdentity, identity.OperationModeContext, identity.UnitScaleSource})
	default:
		return nil, ErrContractViolation
	}
}

func (identity *EBusSourceIdentityV1) UnmarshalJSON(data []byte) error {
	if identity == nil {
		return ErrContractViolation
	}
	var discriminator struct {
		Family EBusFamily `json:"family"`
	}
	if err := json.Unmarshal(data, &discriminator); err != nil {
		return ErrContractViolation
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	switch discriminator.Family {
	case EBusFamilyB509:
		var value struct {
			Family          EBusFamily `json:"family"`
			TargetPseudonym string     `json:"target_pseudonym"`
			TargetAddress   uint8      `json:"target_address"`
			TargetProduct   string     `json:"target_product"`
			RegisterFamily  string     `json:"register_family"`
			RegisterID      uint16     `json:"register_id"`
			UnitScaleSource string     `json:"unit_scale_source"`
			EvidenceRole    string     `json:"evidence_role"`
		}
		if err := decoder.Decode(&value); err != nil {
			return ErrContractViolation
		}
		*identity = EBusSourceIdentityV1{Family: value.Family, TargetPseudonym: value.TargetPseudonym, TargetAddress: value.TargetAddress, TargetProduct: value.TargetProduct, RegisterFamily: value.RegisterFamily, RegisterID: value.RegisterID, UnitScaleSource: value.UnitScaleSource, EvidenceRole: value.EvidenceRole}
	case EBusFamilyB524:
		var value struct {
			Family           EBusFamily `json:"family"`
			TargetPseudonym  string     `json:"target_pseudonym"`
			Opcode           uint8      `json:"opcode"`
			GG               uint8      `json:"GG"`
			II               uint8      `json:"II"`
			RR               uint16     `json:"RR"`
			TargetAddress    uint8      `json:"target_address"`
			SourceAddress    uint8      `json:"source_address"`
			GroupMeaning     string     `json:"group_meaning"`
			InstanceGate     string     `json:"instance_gate"`
			RegisterCategory string     `json:"register_category"`
			UnitScaleSource  string     `json:"unit_scale_source"`
		}
		if err := decoder.Decode(&value); err != nil {
			return ErrContractViolation
		}
		*identity = EBusSourceIdentityV1{Family: value.Family, TargetPseudonym: value.TargetPseudonym, Opcode: value.Opcode, GG: value.GG, II: value.II, RR: value.RR, TargetAddress: value.TargetAddress, SourceAddress: value.SourceAddress, GroupMeaning: value.GroupMeaning, InstanceGate: value.InstanceGate, RegisterCategory: value.RegisterCategory, UnitScaleSource: value.UnitScaleSource}
	case EBusFamilyB555:
		var value struct {
			Family               EBusFamily `json:"family"`
			TargetPseudonym      string     `json:"target_pseudonym"`
			DeviceFamily         string     `json:"device_family"`
			ScheduleProgram      string     `json:"schedule_program"`
			SlotIndex            uint8      `json:"slot_index"`
			DayOfWeek            string     `json:"day_of_week"`
			TimeIdentity         string     `json:"time_identity"`
			OperationModeContext string     `json:"operation_mode_context"`
			UnitScaleSource      string     `json:"unit_scale_source"`
		}
		if err := decoder.Decode(&value); err != nil {
			return ErrContractViolation
		}
		*identity = EBusSourceIdentityV1{Family: value.Family, TargetPseudonym: value.TargetPseudonym, DeviceFamily: value.DeviceFamily, ScheduleProgram: value.ScheduleProgram, SlotIndex: value.SlotIndex, DayOfWeek: value.DayOfWeek, TimeIdentity: value.TimeIdentity, OperationModeContext: value.OperationModeContext, UnitScaleSource: value.UnitScaleSource}
	default:
		return ErrContractViolation
	}
	return nil
}
