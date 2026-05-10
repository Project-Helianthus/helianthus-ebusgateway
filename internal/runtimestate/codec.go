// Package runtimestate — JSON codec with deterministic key order for AD13.
// Plan: runtime-state-w19-26.
//
// We marshal via fixed-shape DTO structs (not map[string]interface{}) so the
// order of keys in the output is determined by struct field declaration
// order — making byte-for-byte comparison stable across writes.

package runtimestate

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// fileDTO is the on-disk JSON shape (v1). DTOs are kept separate from the
// public types so we can evolve the wire format without churning callers.
type fileDTO struct {
	SchemaVersion int      `json:"schema_version"`
	Meta          metaDTO  `json:"meta"`
	EBus          *ebusDTO `json:"ebus,omitempty"`
}

type metaDTO struct {
	InstanceGUID string `json:"instance_guid"`
	WrittenAt    string `json:"written_at"`
	GatewayBuild string `json:"gateway_build,omitempty"`
	AddonVersion string `json:"addon_version,omitempty"`
}

type ebusDTO struct {
	SchemaVersion   int               `json:"schema_version"`
	Self            *selfDTO          `json:"self,omitempty"`
	KnownBusMembers []memberDTO       `json:"known_bus_members,omitempty"`
	Extra           map[string]string `json:"-"` // unknown future fields are ignored on load
}

type selfDTO struct {
	LastAdmittedSource int    `json:"last_admitted_source"`
	LastAdmittedAt     string `json:"last_admitted_at"`
	SelectionMethod    string `json:"selection_method"`
	CompanionTarget    *int   `json:"companion_target"`
}

type memberDTO struct {
	Addr          int          `json:"addr"`
	CompanionAddr *int         `json:"companion_addr"`
	Identity      *identityDTO `json:"identity"`
	LastSeenAt    string       `json:"last_seen_at"`
	LastSource    string       `json:"last_source"`
	Confidence    string       `json:"confidence"`
}

type identityDTO struct {
	Manufacturer string `json:"manufacturer,omitempty"`
	DeviceID     string `json:"device_id,omitempty"`
	SN           string `json:"sn,omitempty"`
}

// stateToDTO converts an in-memory State to its DTO for marshalling.
func stateToDTO(s *State) fileDTO {
	dto := fileDTO{
		SchemaVersion: s.SchemaVersion,
		Meta: metaDTO{
			InstanceGUID: s.Meta.InstanceGUID,
			WrittenAt:    formatTime(s.Meta.WrittenAt),
			GatewayBuild: s.Meta.GatewayBuild,
			AddonVersion: s.Meta.AddonVersion,
		},
	}
	if s.EBus != nil {
		ebus := &ebusDTO{SchemaVersion: s.EBus.SchemaVersion}
		if s.EBus.Self != nil {
			ebus.Self = &selfDTO{
				LastAdmittedSource: int(s.EBus.Self.LastAdmittedSource),
				LastAdmittedAt:     formatTime(s.EBus.Self.LastAdmittedAt),
				SelectionMethod:    string(s.EBus.Self.SelectionMethod),
			}
			if s.EBus.Self.CompanionTarget != nil {
				v := int(*s.EBus.Self.CompanionTarget)
				ebus.Self.CompanionTarget = &v
			}
		}
		for _, m := range s.EBus.KnownBusMembers {
			md := memberDTO{
				Addr:       int(m.Addr),
				LastSeenAt: formatTime(m.LastSeenAt),
				LastSource: string(m.LastSource),
				Confidence: string(m.Confidence),
			}
			if m.CompanionAddr != nil {
				v := int(*m.CompanionAddr)
				md.CompanionAddr = &v
			}
			if m.Identity != nil {
				md.Identity = &identityDTO{
					Manufacturer: m.Identity.Manufacturer,
					DeviceID:     m.Identity.DeviceID,
					SN:           m.Identity.SN,
				}
			}
			ebus.KnownBusMembers = append(ebus.KnownBusMembers, md)
		}
		dto.EBus = ebus
	}
	return dto
}

// dtoToState converts a parsed DTO back into the in-memory State.
func dtoToState(dto fileDTO) (*State, error) {
	s := &State{
		SchemaVersion: dto.SchemaVersion,
		Meta: Meta{
			InstanceGUID: dto.Meta.InstanceGUID,
			GatewayBuild: dto.Meta.GatewayBuild,
			AddonVersion: dto.Meta.AddonVersion,
		},
	}
	if t, err := parseTime(dto.Meta.WrittenAt); err == nil {
		s.Meta.WrittenAt = t
	}
	if dto.EBus != nil {
		ebus := &EBusNamespace{SchemaVersion: dto.EBus.SchemaVersion}
		if dto.EBus.Self != nil {
			selfT, _ := parseTime(dto.EBus.Self.LastAdmittedAt)
			ebus.Self = &Self{
				LastAdmittedSource: byte(dto.EBus.Self.LastAdmittedSource),
				LastAdmittedAt:     selfT,
				SelectionMethod:    SelectionMethod(dto.EBus.Self.SelectionMethod),
			}
			if dto.EBus.Self.CompanionTarget != nil {
				v := byte(*dto.EBus.Self.CompanionTarget)
				ebus.Self.CompanionTarget = &v
			}
		}
		seen := map[byte]int{}
		for _, m := range dto.EBus.KnownBusMembers {
			if _, dup := seen[byte(m.Addr)]; dup {
				return nil, fmt.Errorf("AD18: duplicate addr 0x%02X in known_bus_members", byte(m.Addr))
			}
			seen[byte(m.Addr)] = len(ebus.KnownBusMembers)
			lt, _ := parseTime(m.LastSeenAt)
			km := KnownBusMember{
				Addr:       byte(m.Addr),
				LastSeenAt: lt,
				LastSource: LastSource(m.LastSource),
				Confidence: Confidence(m.Confidence),
			}
			if m.CompanionAddr != nil {
				v := byte(*m.CompanionAddr)
				km.CompanionAddr = &v
			}
			if m.Identity != nil {
				km.Identity = &Identity{
					Manufacturer: m.Identity.Manufacturer,
					DeviceID:     m.Identity.DeviceID,
					SN:           m.Identity.SN,
				}
			}
			ebus.KnownBusMembers = append(ebus.KnownBusMembers, km)
		}
		s.EBus = ebus
	}
	return s, nil
}

// marshalState produces the JSON bytes for a State with deterministic key
// order (struct-driven). Adds a trailing newline so editors don't fight us.
func marshalState(s *State) ([]byte, error) {
	dto := stateToDTO(s)
	b, err := json.MarshalIndent(dto, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(b, '\n'), nil
}

// unmarshalState parses bytes into a State + reports schema-validation
// errors. Returns errSchemaTopLevel for top-level schema_version mismatch
// (treat as corrupt). For ebus.schema_version mismatch returns nil + an
// EBus-stripped state per AD12.
func unmarshalState(data []byte) (*State, error) {
	var dto fileDTO
	if err := json.Unmarshal(data, &dto); err != nil {
		return nil, err
	}
	if dto.SchemaVersion != SchemaVersion {
		return nil, errSchemaTopLevel
	}
	if dto.EBus != nil && dto.EBus.SchemaVersion != EBusSchemaVersion {
		// AD12: ignore the namespace, keep the rest.
		dto.EBus = nil
	}
	return dtoToState(dto)
}

var errSchemaTopLevel = errors.New("runtimestate: top-level schema_version mismatch")

func formatTime(t time.Time) string {
	if t.IsZero() {
		return "1970-01-01T00:00:00Z"
	}
	return t.UTC().Format(time.RFC3339Nano)
}

func parseTime(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, nil
	}
	return time.Parse(time.RFC3339Nano, s)
}
