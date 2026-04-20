package portal

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	estd "github.com/Project-Helianthus/helianthus-ebusgateway/mcp/ebus_standard"
)

// parsePBSBByte parses a PB/SB selector as a 1-byte value, using smart
// detection between decimal (default) and hex (explicit "0x"/"0X" prefix).
//
// Rationale: the MCP tool schema (mcp/ebus_standard_wiring.go) documents
// pb/sb as `{"type": "integer", "minimum": 0, "maximum": 255}` — a decimal
// integer contract. The portal UI placeholder historically advertised
// "0..255". A prior revision (round 8, hex-only parsing via explicit
// base=16) diverged from both surfaces: `pb=10` silently meant 0x10=16
// instead of decimal 10, quietly targeting the wrong catalog identity.
//
// This parser restores MCP-schema alignment while preserving the
// operator-friendly hex escape hatch:
//   - "0x" or "0X" prefix → strip prefix, parse as base=16 bitSize=8
//   - otherwise           → parse as explicit base=10 bitSize=8
//
// Explicit base=10 (not base=0) avoids Go's octal auto-detection: `010`
// parses as decimal 10, not octal 8. Bare hex like "ff" is rejected as
// non-decimal — operators must type "0xff" for hex, matching the
// placeholder text "0..255 (decimal) or 0xNN (hex)".
//
// Behavior:
//   - "10"    → 10         (decimal)
//   - "010"   → 10         (decimal; explicit base=10, no octal)
//   - "08"    → 8          (decimal)
//   - "0x10"  → 16         (hex after prefix strip)
//   - "0Xff"  → 255        (hex, case-insensitive prefix)
//   - "ff"    → error      (bare hex: not decimal, no 0x prefix)
//   - "256"   → error      (overflows bitSize=8)
//   - "banana"→ error      (invalid decimal)
func parsePBSBByte(raw string) (uint8, error) {
	if strings.HasPrefix(raw, "0x") || strings.HasPrefix(raw, "0X") {
		v, err := strconv.ParseUint(raw[2:], 16, 8)
		if err != nil {
			return 0, err
		}
		return uint8(v), nil
	}
	v, err := strconv.ParseUint(raw, 10, 8)
	if err != nil {
		return 0, err
	}
	return uint8(v), nil
}

// PortalEbusStandardServer is the sibling interface consumed by portal.Options
// for the ebus_standard L7 sub-server. It mirrors the unexported
// ebusStandardSubServer interface in mcp/ebus_standard_wiring.go so the portal
// package can accept an injected Server (or a test fake) without importing
// unexported names cross-package.
//
// The M5_PORTAL consumer UI is strictly read-only: these four methods expose
// the L7 catalog and decode observed frames. No invocation is performed here;
// the producer-side execution_policy still gates all writes at the mcp layer.
type PortalEbusStandardServer interface {
	ServicesList() map[string]any
	CommandsList(pb *uint8) (map[string]any, error)
	CommandGet(id string) (map[string]any, error)
	Decode(in estd.DecodeInput) (map[string]any, error)
}

// routeEbusStandard dispatches /api/v1/ebus-standard/* GET requests.
// Returns true if the path was handled (even with an error status).
//
// Mirrors the ExplorerBus nil-guard pattern: if no sub-server is injected
// via Options.EbusStandardServer, the route returns 404 NotFound so the
// capability simply appears absent to clients (matches how the explorer
// capability is hidden when ExplorerBus is nil).
func (h *handler) routeEbusStandard(w http.ResponseWriter, r *http.Request, path string) bool {
	const prefix = "ebus-standard/"
	if len(path) < len(prefix) || path[:len(prefix)] != prefix {
		return false
	}
	sub := path[len(prefix):]
	if h.opts.EbusStandardServer == nil {
		http.NotFound(w, r)
		return true
	}
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return true
	}
	ts := time.Now().UTC()
	switch sub {
	case "services":
		h.writeEbusStandardEnvelope(w, h.opts.EbusStandardServer.ServicesList(), nil, ts)
	case "commands":
		pbp, perr := parseOptionalPBParam(r.URL.Query())
		if perr != nil {
			h.writeEbusStandardEnvelope(w, nil, perr, ts)
			return true
		}
		data, err := h.opts.EbusStandardServer.CommandsList(pbp)
		h.writeEbusStandardEnvelope(w, data, err, ts)
	case "command":
		id := r.URL.Query().Get("id")
		if id == "" {
			h.writeEbusStandardEnvelope(w, nil,
				fmt.Errorf("id: %w: required", estd.ErrInvalidPayload), ts)
			return true
		}
		data, err := h.opts.EbusStandardServer.CommandGet(id)
		h.writeEbusStandardEnvelope(w, data, err, ts)
	case "decode":
		in, perr := parseDecodeQuery(r)
		if perr != nil {
			h.writeEbusStandardEnvelope(w, nil, perr, ts)
			return true
		}
		data, err := h.opts.EbusStandardServer.Decode(in)
		h.writeEbusStandardEnvelope(w, data, err, ts)
	default:
		http.NotFound(w, r)
	}
	return true
}

// writeEbusStandardEnvelope wraps data + err in the M4B envelope (same
// shape as the MCP surfaces emit) and writes it as JSON. HTTP status is
// always 200 OK — errors surface via envelope.error.code per the
// MCP-first invariant.
func (h *handler) writeEbusStandardEnvelope(w http.ResponseWriter, data any, err error, ts time.Time) {
	writeJSON(w, http.StatusOK, estd.NewEnvelope(data, err, ts))
}

// parseOptionalPBParam parses the optional "pb" query parameter. Mirrors
// the MCP surface behavior: absent = nil (unfiltered), malformed = error
// (INVALID_PAYLOAD) to avoid silently hiding contract violations.
//
// Takes url.Values (not a raw string) so we can distinguish "pb key missing"
// from "pb key present but empty" — same pattern as the payload_hex fix
// (e363e1b7). A bare q.Get("pb") would yield "" in both cases, silently
// mapping malformed ?pb= to the unfiltered path.
func parseOptionalPBParam(q url.Values) (*uint8, error) {
	if !q.Has("pb") {
		return nil, nil
	}
	raw := q.Get("pb")
	if raw == "" {
		return nil, fmt.Errorf("pb: %w: filter must be a 1-byte value (decimal 0..255 or 0xNN) or omitted entirely",
			estd.ErrInvalidPayload)
	}
	pb, err := parsePBSBByte(raw)
	if err != nil {
		return nil, fmt.Errorf("pb: %w: expected decimal 0..255 or 0xNN hex, got %q: %v",
			estd.ErrInvalidPayload, raw, err)
	}
	return &pb, nil
}

// parseDecodeQuery extracts the DecodeInput from query parameters. Unlike
// the MCP surface (which receives typed JSON), HTTP query strings are all
// textual — we parse pb/sb via parsePBSBByte (decimal default, 0x prefix
// for hex) matching the MCP tool schema's integer [0,255] contract.
// direction and frame_type are passed through verbatim to the sub-server,
// which enforces
// "non-empty" itself (ErrInvalidPayload) so we get consistent error wording
// for missing selectors.
func parseDecodeQuery(r *http.Request) (estd.DecodeInput, error) {
	q := r.URL.Query()
	var in estd.DecodeInput
	if pbStr := q.Get("pb"); pbStr == "" {
		return in, fmt.Errorf("pb: %w: required", estd.ErrInvalidPayload)
	} else if v, err := parsePBSBByte(pbStr); err != nil {
		return in, fmt.Errorf("pb: %w: expected decimal 0..255 or 0xNN hex, got %q",
			estd.ErrInvalidPayload, pbStr)
	} else {
		in.PB = v
	}
	if sbStr := q.Get("sb"); sbStr == "" {
		return in, fmt.Errorf("sb: %w: required", estd.ErrInvalidPayload)
	} else if v, err := parsePBSBByte(sbStr); err != nil {
		return in, fmt.Errorf("sb: %w: expected decimal 0..255 or 0xNN hex, got %q",
			estd.ErrInvalidPayload, sbStr)
	} else {
		in.SB = v
	}
	in.Direction = q.Get("direction")
	in.FrameType = q.Get("frame_type")
	// payload_hex: distinguish missing from explicitly-empty. The sub-server
	// Decode accepts empty payloads (hex.DecodeString("") -> nil), so a bare
	// q.Get would silently map "omitted" to "", hiding client request bugs
	// (caller gets success / UNKNOWN_COMMAND instead of INVALID_PAYLOAD).
	// Explicit empty (payload_hex=) is passed through and defers to backend
	// Decode semantics; missing key surfaces INVALID_PAYLOAD here.
	if !q.Has("payload_hex") {
		return in, fmt.Errorf("payload_hex: %w: required (explicit empty is allowed, but the key must be present)",
			estd.ErrInvalidPayload)
	}
	in.PayloadHex = q.Get("payload_hex")
	return in, nil
}

// assertEbusStandardErrUsed keeps the errors import live even if callers
// narrow usage later. The ErrInvalidPayload / ErrUnknownCommand errors
// from the sub-server are surfaced via classifyErr inside envelope.go.
var _ = errors.Is
