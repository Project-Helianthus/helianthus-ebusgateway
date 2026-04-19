// Package ebus_standard implements the gateway MCP surfaces for the
// ebus_standard L7 namespace (M4_GATEWAY_MCP).
//
// Surfaces (canonical plan §4):
//
//   - ebus.v1.ebus_standard.services.list
//   - ebus.v1.ebus_standard.commands.list
//   - ebus.v1.ebus_standard.command.get
//   - ebus.v1.ebus_standard.decode
//
// The server consumes the shared provider from helianthus-ebusreg and the
// shared execution_policy module from internal/execution_policy. It does
// NOT duplicate catalog walking, safety logic, or identity construction —
// all of that lives in ebusreg.
package ebus_standard

import (
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"

	ebusstd "github.com/Project-Helianthus/helianthus-ebusreg/catalog/ebus_standard"
)

// Tool names exposed over MCP.
const (
	ToolServicesList = "ebus.v1.ebus_standard.services.list"
	ToolCommandsList = "ebus.v1.ebus_standard.commands.list"
	ToolCommandGet   = "ebus.v1.ebus_standard.command.get"
	ToolDecode       = "ebus.v1.ebus_standard.decode"
)

// ErrUnknownCommand is returned by command.get and decode when the catalog
// does not contain the requested command.
var ErrUnknownCommand = errors.New("ebus_standard: unknown command")

// ErrInvalidPayload is returned on malformed MCP arguments.
var ErrInvalidPayload = errors.New("ebus_standard: invalid payload")

// Server is the handler for the four ebus_standard MCP surfaces.
type Server struct {
	catalog ebusstd.Catalog
}

// NewServer returns a Server bound to the supplied catalog.
func NewServer(cat ebusstd.Catalog) *Server {
	return &Server{catalog: cat}
}

// ServicesList returns a stable, sorted list of services from the catalog.
// Fields are sorted deterministically so the data_hash is stable across
// runs and platforms.
func (s *Server) ServicesList() map[string]any {
	services := make([]map[string]any, 0, len(s.catalog.Services))
	for _, svc := range s.catalog.Services {
		services = append(services, map[string]any{
			"pb":            int(svc.PBValue()),
			"name":          svc.Name,
			"description":   svc.Description,
			"command_count": len(svc.Commands),
		})
	}
	sort.SliceStable(services, func(i, j int) bool {
		if services[i]["pb"].(int) != services[j]["pb"].(int) {
			return services[i]["pb"].(int) < services[j]["pb"].(int)
		}
		return services[i]["name"].(string) < services[j]["name"].(string)
	})
	return map[string]any{
		"namespace":       s.catalog.Namespace,
		"catalog_version": s.catalog.Version,
		"plan_sha256":     s.catalog.PlanSHA256,
		"services":        services,
	}
}

// CommandsList returns the commands of a service. The service is selected
// by pb; if pb is absent, all commands are returned sorted by pb,sb.
func (s *Server) CommandsList(pb *uint8) (map[string]any, error) {
	commands := make([]map[string]any, 0, 32)
	for _, svc := range s.catalog.Services {
		if pb != nil && svc.PBValue() != *pb {
			continue
		}
		for _, cmd := range svc.Commands {
			commands = append(commands, commandSummary(cmd))
		}
	}
	sort.SliceStable(commands, func(i, j int) bool {
		ai := commands[i]
		aj := commands[j]
		if ai["pb"].(int) != aj["pb"].(int) {
			return ai["pb"].(int) < aj["pb"].(int)
		}
		if ai["sb"].(int) != aj["sb"].(int) {
			return ai["sb"].(int) < aj["sb"].(int)
		}
		return ai["id"].(string) < aj["id"].(string)
	})
	return map[string]any{
		"namespace":       s.catalog.Namespace,
		"catalog_version": s.catalog.Version,
		"commands":        commands,
	}, nil
}

// CommandGet returns a single command with its full 14-tuple identity.
func (s *Server) CommandGet(id string) (map[string]any, error) {
	cmd, ok := s.findCommand(id)
	if !ok {
		return nil, fmt.Errorf("id=%q: %w", id, ErrUnknownCommand)
	}
	return map[string]any{
		"namespace":       s.catalog.Namespace,
		"catalog_version": s.catalog.Version,
		"command":         commandFull(cmd),
	}, nil
}

// DecodeInput is the input struct for the decode surface.
type DecodeInput struct {
	PB         uint8
	SB         uint8
	Direction  string
	FrameType  string
	PayloadHex string
}

// Decode decodes an observed payload against the catalog command that
// matches (PB, SB, Direction, FrameType). Unknown combinations return
// ErrUnknownCommand.
//
// Direction and FrameType are REQUIRED selectors: a decode request that
// supplies only (PB, SB) is ambiguous whenever the catalog contains
// multiple commands on the same PB/SB (e.g. request vs response), so
// missing selectors are rejected as INVALID_PAYLOAD rather than silently
// matching the first row. Regression for PR #505 r3106756020.
func (s *Server) Decode(in DecodeInput) (map[string]any, error) {
	if in.Direction == "" {
		return nil, fmt.Errorf("direction: %w: required (empty is not a wildcard)", ErrInvalidPayload)
	}
	if in.FrameType == "" {
		return nil, fmt.Errorf("frame_type: %w: required (empty is not a wildcard)", ErrInvalidPayload)
	}
	payload, err := hex.DecodeString(strings.TrimPrefix(strings.ToLower(in.PayloadHex), "0x"))
	if err != nil {
		return nil, fmt.Errorf("payload_hex: %w: %s", ErrInvalidPayload, err)
	}
	cmd, ok := s.findIdentity(in.PB, in.SB, in.Direction, in.FrameType)
	if !ok {
		return nil, fmt.Errorf("pb=0x%02X sb=0x%02X direction=%q frame_type=%q: %w",
			in.PB, in.SB, in.Direction, in.FrameType, ErrUnknownCommand)
	}

	// Field decode uses the catalog Request/Response parameter list
	// plus L7 types from helianthus-ebusgo when available. In M4 scope
	// the catalog Parameters are placeholders (M3 note) — report the
	// catalog identity plus raw payload as decoded evidence.
	fields := make([]map[string]any, 0, len(cmd.Response)+len(cmd.Request))
	for _, p := range cmd.Request {
		fields = append(fields, map[string]any{
			"name": p.Name, "type": p.Type, "role": "request",
		})
	}
	for _, p := range cmd.Response {
		fields = append(fields, map[string]any{
			"name": p.Name, "type": p.Type, "role": "response",
		})
	}
	return map[string]any{
		"namespace":         s.catalog.Namespace,
		"catalog_version":   s.catalog.Version,
		"command_id":        cmd.ID,
		"raw_bytes":         toIntSlice(payload),
		"fields":            fields,
		"replacement_value": false,
		"validity":          "catalog_identified",
	}, nil
}

func (s *Server) findCommand(id string) (ebusstd.Command, bool) {
	for _, svc := range s.catalog.Services {
		for _, cmd := range svc.Commands {
			if cmd.ID == id {
				return cmd, true
			}
		}
	}
	return ebusstd.Command{}, false
}

// findIdentity locates a catalog command by exact (pb, sb, direction,
// frameType) match. direction and frameType are REQUIRED — the caller
// (Decode) rejects empty selectors with ErrInvalidPayload before we get
// here. We still guard here so any future caller that forgets the
// validation gets a miss rather than a silent first-row match.
func (s *Server) findIdentity(pb, sb uint8, direction, frameType string) (ebusstd.Command, bool) {
	if direction == "" || frameType == "" {
		return ebusstd.Command{}, false
	}
	for _, svc := range s.catalog.Services {
		if svc.PBValue() != pb {
			continue
		}
		for _, cmd := range svc.Commands {
			if cmd.Identity.SBValue() != sb {
				continue
			}
			if string(cmd.Identity.Direction) != direction {
				continue
			}
			if string(cmd.Identity.TelegramClass) != frameType {
				continue
			}
			return cmd, true
		}
	}
	return ebusstd.Command{}, false
}

func commandSummary(cmd ebusstd.Command) map[string]any {
	return map[string]any{
		"id":           cmd.ID,
		"name":         cmd.Name,
		"pb":           int(cmd.Identity.PBValue()),
		"sb":           int(cmd.Identity.SBValue()),
		"safety_class": string(cmd.SafetyClass),
	}
}

func commandFull(cmd ebusstd.Command) map[string]any {
	req := make([]map[string]any, 0, len(cmd.Request))
	for _, p := range cmd.Request {
		req = append(req, map[string]any{"name": p.Name, "type": p.Type, "description": p.Description})
	}
	resp := make([]map[string]any, 0, len(cmd.Response))
	for _, p := range cmd.Response {
		resp = append(resp, map[string]any{"name": p.Name, "type": p.Type, "description": p.Description})
	}
	identity := map[string]any{
		"namespace":                         cmd.Identity.Namespace,
		"pb":                                int(cmd.Identity.PBValue()),
		"sb":                                int(cmd.Identity.SBValue()),
		"selector_path":                     cmd.Identity.SelectorPath,
		"telegram_class":                    string(cmd.Identity.TelegramClass),
		"direction":                         string(cmd.Identity.Direction),
		"request_or_response_role":          string(cmd.Identity.RequestOrResponseRole),
		"broadcast_or_addressed":            string(cmd.Identity.BroadcastOrAddressed),
		"answer_policy":                     string(cmd.Identity.AnswerPolicy),
		"length_prefix_mode":                string(cmd.Identity.LengthPrefixMode),
		"selector_decoder":                  cmd.Identity.SelectorDecoder,
		"service_variant":                   cmd.Identity.ServiceVariant,
		"transport_capability_requirements": append([]string{}, cmd.Identity.TransportCapabilityRequirements...),
		"version":                           cmd.Identity.Version,
	}
	return map[string]any{
		"id":           cmd.ID,
		"name":         cmd.Name,
		"description":  cmd.Description,
		"safety_class": string(cmd.SafetyClass),
		"identity":     identity,
		"request":      req,
		"response":     resp,
	}
}

func toIntSlice(b []byte) []int {
	out := make([]int, len(b))
	for i, v := range b {
		out[i] = int(v)
	}
	return out
}
