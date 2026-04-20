package portal

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	estd "github.com/Project-Helianthus/helianthus-ebusgateway/mcp/ebus_standard"
)

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
		pbp, perr := parseOptionalPBParam(r.URL.Query().Get("pb"))
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
func parseOptionalPBParam(raw string) (*uint8, error) {
	if raw == "" {
		return nil, nil
	}
	v, err := strconv.ParseUint(raw, 0, 16)
	if err != nil {
		return nil, fmt.Errorf("pb: %w: expected integer in [0,255], got %q: %v",
			estd.ErrInvalidPayload, raw, err)
	}
	if v > 255 {
		return nil, fmt.Errorf("pb: %w: out of range [0,255]: %d",
			estd.ErrInvalidPayload, v)
	}
	pb := uint8(v)
	return &pb, nil
}

// parseDecodeQuery extracts the DecodeInput from query parameters. Unlike
// the MCP surface (which receives typed JSON), HTTP query strings are all
// textual — we parse pb/sb as uint16 with [0,255] validation. direction and
// frame_type are passed through verbatim to the sub-server, which enforces
// "non-empty" itself (ErrInvalidPayload) so we get consistent error wording
// for missing selectors.
func parseDecodeQuery(r *http.Request) (estd.DecodeInput, error) {
	q := r.URL.Query()
	var in estd.DecodeInput
	if pbStr := q.Get("pb"); pbStr == "" {
		return in, fmt.Errorf("pb: %w: required", estd.ErrInvalidPayload)
	} else if v, err := strconv.ParseUint(pbStr, 0, 16); err != nil || v > 255 {
		return in, fmt.Errorf("pb: %w: expected [0,255], got %q",
			estd.ErrInvalidPayload, pbStr)
	} else {
		in.PB = uint8(v)
	}
	if sbStr := q.Get("sb"); sbStr == "" {
		return in, fmt.Errorf("sb: %w: required", estd.ErrInvalidPayload)
	} else if v, err := strconv.ParseUint(sbStr, 0, 16); err != nil || v > 255 {
		return in, fmt.Errorf("sb: %w: expected [0,255], got %q",
			estd.ErrInvalidPayload, sbStr)
	} else {
		in.SB = uint8(v)
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
