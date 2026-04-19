package mcp

import (
	"fmt"
	"math"
	"time"

	estd "github.com/Project-Helianthus/helianthus-ebusgateway/mcp/ebus_standard"
	ebusstd "github.com/Project-Helianthus/helianthus-ebusreg/catalog/ebus_standard"
)

// ebusStandardSubServer is the in-package interface backed by
// mcp/ebus_standard.Server. Typed as an interface so the main Server
// struct can hold a nil-equatable value when the sub-server is not
// installed, while tests can substitute a fake.
type ebusStandardSubServer interface {
	ServicesList() map[string]any
	CommandsList(pb *uint8) (map[string]any, error)
	CommandGet(id string) (map[string]any, error)
	Decode(in estd.DecodeInput) (map[string]any, error)
}

// RegisterEbusStandardTools installs the four ebus_standard L7 MCP
// surfaces into s. Safe to call once during gateway bootstrap.
//
// Surfaces added:
//   - ebus.v1.ebus_standard.services.list
//   - ebus.v1.ebus_standard.commands.list
//   - ebus.v1.ebus_standard.command.get
//   - ebus.v1.ebus_standard.decode
//
// The catalog is consumed read-only. All safety-class gating for catalog
// invocations is performed by internal/execution_policy at the provider
// boundary (helianthus-ebusreg). These MCP surfaces are themselves
// read_only_safe — they describe the catalog and decode observed bytes.
func RegisterEbusStandardTools(s *Server, cat ebusstd.Catalog) {
	if s == nil {
		return
	}
	sub := estd.NewServer(cat)

	s.tools = append(s.tools,
		Tool{
			Name:        estd.ToolServicesList,
			Description: "List ebus_standard L7 services from the catalog.",
			InputSchema: map[string]any{
				"type":                 "object",
				"properties":           map[string]any{},
				"additionalProperties": false,
			},
		},
		Tool{
			Name:        estd.ToolCommandsList,
			Description: "List ebus_standard commands, optionally filtered by PB.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"pb": map[string]any{"type": "integer", "minimum": 0, "maximum": 255},
				},
				"additionalProperties": false,
			},
		},
		Tool{
			Name:        estd.ToolCommandGet,
			Description: "Get an ebus_standard command by id with full 14-tuple identity.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"id": map[string]any{"type": "string"},
				},
				"required":             []string{"id"},
				"additionalProperties": false,
			},
		},
		Tool{
			Name:        estd.ToolDecode,
			Description: "Decode an observed eBUS payload against catalog identity (pb, sb, direction, frame_type, payload_hex).",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"pb":          map[string]any{"type": "integer", "minimum": 0, "maximum": 255},
					"sb":          map[string]any{"type": "integer", "minimum": 0, "maximum": 255},
					"direction":   map[string]any{"type": "string"},
					"frame_type":  map[string]any{"type": "string"},
					"payload_hex": map[string]any{"type": "string"},
				},
				"required":             []string{"pb", "sb", "direction", "frame_type", "payload_hex"},
				"additionalProperties": false,
			},
		},
	)

	s.ebusStandardServer = sub
}

// handleEbusStandardCall is invoked from handleToolsCall before the main
// switch. Returns (result, true, nil) when it handled the call.
func (s *Server) handleEbusStandardCall(name string, args map[string]any) (map[string]any, bool) {
	if s.ebusStandardServer == nil {
		return nil, false
	}
	ts := time.Now().UTC()
	switch name {
	case estd.ToolServicesList:
		data := s.ebusStandardServer.ServicesList()
		return callToolResultText(mustJSON(estd.NewEnvelope(data, nil, ts)), false), true
	case estd.ToolCommandsList:
		var pbp *uint8
		if raw, ok := args["pb"]; ok && raw != nil {
			v, ok := toUint8(raw)
			if !ok {
				// Malformed pb is an invalid payload, not an implicit
				// "unfiltered" request — otherwise clients that send
				// garbage get the full catalog back as a success, which
				// silently hides the contract violation.
				err := fmt.Errorf("pb: %w: expected integer in [0,255], got %T=%v",
					estd.ErrInvalidPayload, raw, raw)
				return callToolResultText(mustJSON(estd.NewEnvelope(nil, err, ts)), true), true
			}
			pbp = &v
		}
		data, err := s.ebusStandardServer.CommandsList(pbp)
		return callToolResultText(mustJSON(estd.NewEnvelope(data, err, ts)), err != nil), true
	case estd.ToolCommandGet:
		id, _ := args["id"].(string)
		data, err := s.ebusStandardServer.CommandGet(id)
		return callToolResultText(mustJSON(estd.NewEnvelope(data, err, ts)), err != nil), true
	case estd.ToolDecode:
		in := estd.DecodeInput{}
		rawPB, pbPresent := args["pb"]
		if !pbPresent || rawPB == nil {
			err := fmt.Errorf("pb: %w: required", estd.ErrInvalidPayload)
			return callToolResultText(mustJSON(estd.NewEnvelope(nil, err, ts)), true), true
		}
		v, ok := toUint8(rawPB)
		if !ok {
			err := fmt.Errorf("pb: %w: expected integer in [0,255], got %T=%v",
				estd.ErrInvalidPayload, rawPB, rawPB)
			return callToolResultText(mustJSON(estd.NewEnvelope(nil, err, ts)), true), true
		}
		in.PB = v
		rawSB, sbPresent := args["sb"]
		if !sbPresent || rawSB == nil {
			err := fmt.Errorf("sb: %w: required", estd.ErrInvalidPayload)
			return callToolResultText(mustJSON(estd.NewEnvelope(nil, err, ts)), true), true
		}
		v, ok = toUint8(rawSB)
		if !ok {
			err := fmt.Errorf("sb: %w: expected integer in [0,255], got %T=%v",
				estd.ErrInvalidPayload, rawSB, rawSB)
			return callToolResultText(mustJSON(estd.NewEnvelope(nil, err, ts)), true), true
		}
		in.SB = v
		if v, ok := args["direction"].(string); ok {
			in.Direction = v
		}
		if v, ok := args["frame_type"].(string); ok {
			in.FrameType = v
		}
		if v, ok := args["payload_hex"].(string); ok {
			in.PayloadHex = v
		}
		data, err := s.ebusStandardServer.Decode(in)
		return callToolResultText(mustJSON(estd.NewEnvelope(data, err, ts)), err != nil), true
	}
	return nil, false
}

func toUint8(v any) (uint8, bool) {
	switch x := v.(type) {
	case int:
		if x < 0 || x > 255 {
			return 0, false
		}
		return uint8(x), true
	case int64:
		if x < 0 || x > 255 {
			return 0, false
		}
		return uint8(x), true
	case float64:
		// Reject NaN, Inf, fractional values, and out-of-range. JSON
		// numbers decode to float64 by default, so a caller that sends
		// `3.9` must NOT be silently truncated to 3 — that masks a
		// contract violation. Parallel to toByteSource.float64.
		// Regression for PR #505 r3106756021.
		if math.IsNaN(x) || math.IsInf(x, 0) {
			return 0, false
		}
		if x < 0 || x > 255 {
			return 0, false
		}
		if math.Trunc(x) != x {
			return 0, false
		}
		return uint8(x), true
	}
	return 0, false
}
