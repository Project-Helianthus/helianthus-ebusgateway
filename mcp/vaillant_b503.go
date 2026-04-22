package mcp

import (
	"context"
	"errors"
	"fmt"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/vaillant/b503session"
	"github.com/Project-Helianthus/helianthus-ebusgo/protocol/vaillant/b503"
)

// --- public API contracts -------------------------------------------------

// RPCDispatcher is the minimal substrate the B503 tools need to reach the
// wire. Production wires this to the gateway's raw RPC substrate; tests
// supply an in-memory stub. The target byte is the primary address
// of the Vaillant device; payload is the 2-byte (family, selector) request
// built by package b503.
type RPCDispatcher interface {
	Invoke(ctx context.Context, target byte, payload []byte) ([]byte, error)
}

// B503Availability is the public capability-signal enum surfaced via
// VaillantB503Availability(). Spec §11.
type B503Availability string

const (
	AvailabilityAvailable     B503Availability = "AVAILABLE"
	AvailabilityNotSupported  B503Availability = "NOT_SUPPORTED"
	AvailabilityTransportDown B503Availability = "TRANSPORT_DOWN"
	AvailabilitySessionBusy   B503Availability = "SESSION_BUSY"
	AvailabilityUnknown       B503Availability = "UNKNOWN"
)

// VaillantB503Options is the bootstrap bundle for the B503 tool surface.
// Dispatcher + SessionManager MUST be non-nil; DefaultTarget is the primary
// address used when a caller omits `target_address`.
type VaillantB503Options struct {
	Dispatcher     RPCDispatcher
	SessionManager *b503session.Manager
	DefaultTarget  byte
}

// Public tool names (spec §3 / plan AD02).
const (
	toolVaillantB503ErrorsGetName         = "ebus.v1.vaillant.errors.get"
	toolVaillantB503ErrorsHistoryGetName  = "ebus.v1.vaillant.errors.history.get"
	toolVaillantB503ServiceCurrentGetName = "ebus.v1.vaillant.service.current.get"
	toolVaillantB503ServiceHistoryGetName = "ebus.v1.vaillant.service.history.get"
	toolVaillantB503LiveMonitorName       = "ebus.v1.vaillant.live_monitor.get"
)

// --- server-side state ----------------------------------------------------

// b503State is attached to the Server via a package-private map keyed by
// *Server pointer identity. We avoid growing the Server struct to keep the
// diff minimal and the review surface tight.
type b503State struct {
	opts VaillantB503Options
}

var b503States = struct {
	byServer map[*Server]*b503State
}{byServer: make(map[*Server]*b503State)}

// RegisterVaillantB503Tools installs the 5 Vaillant B503 tools on s.
func RegisterVaillantB503Tools(s *Server, opts VaillantB503Options) {
	if s == nil {
		return
	}
	st := &b503State{opts: opts}
	b503States.byServer[s] = st

	targetProp := map[string]any{
		"target_address": map[string]any{
			"type":    "integer",
			"minimum": 0,
			"maximum": 255,
		},
	}

	s.tools = append(s.tools,
		Tool{
			Name:        toolVaillantB503ErrorsGetName,
			Description: "Get current Vaillant error slots via B503 family=0x00 selector=0x01 (READ).",
			InputSchema: map[string]any{
				"type":                 "object",
				"properties":           targetProp,
				"additionalProperties": false,
			},
		},
		Tool{
			Name:        toolVaillantB503ErrorsHistoryGetName,
			Description: "Get a Vaillant error-history record via B503 family=0x01 selector=0x01 (READ).",
			InputSchema: map[string]any{
				"type": "object",
				"properties": mergeProps(targetProp, map[string]any{
					"index": map[string]any{"type": "integer", "minimum": 0, "maximum": 255},
				}),
				"additionalProperties": false,
			},
		},
		Tool{
			Name:        toolVaillantB503ServiceCurrentGetName,
			Description: "Get current Vaillant service-message slots via B503 family=0x00 selector=0x02 (READ).",
			InputSchema: map[string]any{
				"type":                 "object",
				"properties":           targetProp,
				"additionalProperties": false,
			},
		},
		Tool{
			Name:        toolVaillantB503ServiceHistoryGetName,
			Description: "Get a Vaillant service-history record via B503 family=0x01 selector=0x02 (READ).",
			InputSchema: map[string]any{
				"type": "object",
				"properties": mergeProps(targetProp, map[string]any{
					"index": map[string]any{"type": "integer", "minimum": 0, "maximum": 255},
				}),
				"additionalProperties": false,
			},
		},
		Tool{
			Name:        toolVaillantB503LiveMonitorName,
			Description: "Vaillant HMU live-monitor session (SERVICE_WRITE). action ∈ {enable,read,disable}.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": mergeProps(targetProp, map[string]any{
					"action":       map[string]any{"type": "string", "enum": []string{"enable", "read", "disable"}},
					"issuer_token": map[string]any{"type": "string"},
				}),
				"required":             []string{"action"},
				"additionalProperties": false,
			},
		},
	)
}

func mergeProps(a, b map[string]any) map[string]any {
	out := make(map[string]any, len(a)+len(b))
	for k, v := range a {
		out[k] = v
	}
	for k, v := range b {
		out[k] = v
	}
	return out
}

// --- dispatch -------------------------------------------------------------

// handleVaillantB503Call is invoked from handleToolsCall before the main
// switch. Returns (result, true) when handled.
func (s *Server) handleVaillantB503Call(ctx context.Context, name string, args map[string]any) (map[string]any, bool) {
	st, ok := b503States.byServer[s]
	if !ok {
		return nil, false
	}
	switch name {
	case toolVaillantB503ErrorsGetName:
		return st.handleErrorsGet(ctx, args), true
	case toolVaillantB503ErrorsHistoryGetName:
		return st.handleErrorsHistoryGet(ctx, args), true
	case toolVaillantB503ServiceCurrentGetName:
		return st.handleServiceCurrentGet(ctx, args), true
	case toolVaillantB503ServiceHistoryGetName:
		return st.handleServiceHistoryGet(ctx, args), true
	case toolVaillantB503LiveMonitorName:
		return st.handleLiveMonitor(ctx, args), true
	}
	return nil, false
}

// target resolves the target device address from args. Returns
// (address, nil) when target_address is absent or a valid uint8; returns
// (0, errInvalidArgument-wrapped) when target_address is present but
// malformed. Silent fallback to DefaultTarget on malformed input would
// misroute reads/writes to a different device than the caller named.
func (st *b503State) target(args map[string]any) (byte, error) {
	raw, ok := args["target_address"]
	if !ok || raw == nil {
		return st.opts.DefaultTarget, nil
	}
	v, ok := toUint8(raw)
	if !ok {
		return 0, fmt.Errorf("%w: target_address must be an unsigned 8-bit integer (0-255)", errInvalidArgument)
	}
	return v, nil
}

// historyIndex extracts an optional `index` argument for history reads.
// Returns (idx, true, nil) when present and valid; (0, false, nil) when
// absent; (0, false, errInvalidArgument-wrapped) when present but
// malformed. Silently dropping a malformed index changes semantics from
// "requested record" to "whatever the decoder/device defaults to" and
// makes malformed input look successful while returning the wrong
// history entry.
func historyIndex(args map[string]any) (byte, bool, error) {
	raw, ok := args["index"]
	if !ok || raw == nil {
		return 0, false, nil
	}
	v, ok := toUint8(raw)
	if !ok {
		return 0, false, fmt.Errorf("%w: index must be an unsigned 8-bit integer (0-255)", errInvalidArgument)
	}
	return v, true, nil
}

func (st *b503State) handleErrorsGet(ctx context.Context, args map[string]any) map[string]any {
	target, err := st.target(args)
	if err != nil {
		return errorEnvelope(err)
	}
	resp, err := st.opts.Dispatcher.Invoke(ctx, target, b503.EncodeCurrentError())
	if err != nil {
		return errorEnvelope(fmt.Errorf("%w: %v", errUpstreamRPCFailed, err))
	}
	slots, err := b503.DecodeCurrentError(resp)
	if err != nil {
		return errorEnvelope(fmt.Errorf("%w: %v", errDecodeFailed, err))
	}
	return callToolResultText(mustJSON(newToolEnvelope(slotsToMap(slots), nil)), false)
}

func (st *b503State) handleServiceCurrentGet(ctx context.Context, args map[string]any) map[string]any {
	target, err := st.target(args)
	if err != nil {
		return errorEnvelope(err)
	}
	resp, err := st.opts.Dispatcher.Invoke(ctx, target, b503.EncodeCurrentService())
	if err != nil {
		return errorEnvelope(fmt.Errorf("%w: %v", errUpstreamRPCFailed, err))
	}
	slots, err := b503.DecodeCurrentService(resp)
	if err != nil {
		return errorEnvelope(fmt.Errorf("%w: %v", errDecodeFailed, err))
	}
	return callToolResultText(mustJSON(newToolEnvelope(slotsToMap(slots), nil)), false)
}

func (st *b503State) handleErrorsHistoryGet(ctx context.Context, args map[string]any) map[string]any {
	target, err := st.target(args)
	if err != nil {
		return errorEnvelope(err)
	}
	payload := b503.EncodeErrorHistory()
	if idx, present, err := historyIndex(args); err != nil {
		return errorEnvelope(err)
	} else if present {
		payload = append(payload, idx)
	}
	resp, err := st.opts.Dispatcher.Invoke(ctx, target, payload)
	if err != nil {
		return errorEnvelope(fmt.Errorf("%w: %v", errUpstreamRPCFailed, err))
	}
	rec, err := b503.DecodeErrorHistory(resp)
	if err != nil {
		return errorEnvelope(fmt.Errorf("%w: %v", errDecodeFailed, err))
	}
	return callToolResultText(mustJSON(newToolEnvelope(historyToMap(rec), nil)), false)
}

func (st *b503State) handleServiceHistoryGet(ctx context.Context, args map[string]any) map[string]any {
	target, err := st.target(args)
	if err != nil {
		return errorEnvelope(err)
	}
	payload := b503.EncodeServiceHistory()
	if idx, present, err := historyIndex(args); err != nil {
		return errorEnvelope(err)
	} else if present {
		payload = append(payload, idx)
	}
	resp, err := st.opts.Dispatcher.Invoke(ctx, target, payload)
	if err != nil {
		return errorEnvelope(fmt.Errorf("%w: %v", errUpstreamRPCFailed, err))
	}
	rec, err := b503.DecodeServiceHistory(resp)
	if err != nil {
		return errorEnvelope(fmt.Errorf("%w: %v", errDecodeFailed, err))
	}
	return callToolResultText(mustJSON(newToolEnvelope(historyToMap(rec), nil)), false)
}

func (st *b503State) handleLiveMonitor(ctx context.Context, args map[string]any) map[string]any {
	// action is required and MUST be a string. Silently defaulting to
	// "read" on absent or malformed input would turn malformed payloads
	// (e.g. `{}` or `{"action": 1}`) into real read operations that
	// refresh the idle timer and prolong SESSION_BUSY for other clients.
	raw, present := args["action"]
	if !present {
		return errorEnvelope(fmt.Errorf("%w: action is required (enable|read|disable)", errInvalidArgument))
	}
	action, ok := raw.(string)
	if !ok {
		return errorEnvelope(fmt.Errorf("%w: action must be a string (enable|read|disable)", errInvalidArgument))
	}
	switch action {
	case "enable", "read", "disable":
		// valid
	default:
		return errorEnvelope(fmt.Errorf("%w: action must be one of enable|read|disable, got %q", errInvalidArgument, action))
	}
	mgr := st.opts.SessionManager
	if mgr == nil {
		return errorEnvelope(errNotSupported)
	}

	switch action {
	case "enable":
		target, err := st.target(args)
		if err != nil {
			return errorEnvelope(err)
		}
		key, err := mgr.Enable(ctx)
		if err != nil {
			return errorEnvelope(normalizeSessionErr(err))
		}
		// Emit the request so the device acknowledges live-monitor
		// activation. Bound by SERVICE_WRITE safety class.
		if _, err := st.opts.Dispatcher.Invoke(ctx, target, b503.EncodeLiveMonitorMain()); err != nil {
			// Dispatcher failure → release session and surface upstream
			// error. Rebuild the SessionKey from the Manager's current
			// TransportKey in case OnEpochAdvance re-homed the session
			// between Enable and the failed Invoke. If even the rebuilt
			// key no longer matches (session already disabled by refresh
			// policy surfacing TRANSPORT_DOWN / SESSION_BUSY), Disable is
			// a no-op — the session is already released.
			rebuilt := b503session.SessionKey{
				Transport:   mgr.TransportKey(),
				IssuerToken: key.IssuerToken,
			}
			_ = mgr.Disable(rebuilt)
			return errorEnvelope(fmt.Errorf("%w: %v", errUpstreamRPCFailed, err))
		}
		data := map[string]any{"issuer_token": key.IssuerToken}
		return callToolResultText(mustJSON(newToolEnvelope(data, nil)), false)

	case "read":
		// Resolver per spec §8 / plan AD14: surface the current FSM outcome
		// literally and never leak EXPIRED. If OnEpochAdvance recently
		// resolved with ErrTransportDown, publicize that literally rather
		// than collapsing it to SESSION_BUSY.
		// Validate arguments BEFORE touching session state. A malformed
		// target_address must not refresh the idle timer (which would keep
		// the session lock alive and prolong SESSION_BUSY for other
		// clients) only to then fail with INVALID_ARGUMENT.
		target, err := st.target(args)
		if err != nil {
			return errorEnvelope(err)
		}
		if mgr.LastRefreshTransportDown() {
			return errorEnvelope(errTransportDown)
		}
		transport := mgr.TransportKey()
		if err := mgr.Read(transport); err != nil {
			return errorEnvelope(normalizeSessionErr(err))
		}
		resp, err := st.opts.Dispatcher.Invoke(ctx, target, b503.EncodeLiveMonitorMain())
		if err != nil {
			return errorEnvelope(fmt.Errorf("%w: %v", errUpstreamRPCFailed, err))
		}
		data := map[string]any{"raw_hex": hexString(resp)}
		return callToolResultText(mustJSON(newToolEnvelope(data, nil)), false)

	case "disable":
		tok, _ := args["issuer_token"].(string)
		transport := mgr.TransportKey()
		err := mgr.Disable(b503session.SessionKey{Transport: transport, IssuerToken: tok})
		if err != nil {
			return errorEnvelope(normalizeSessionErr(err))
		}
		return callToolResultText(mustJSON(newToolEnvelope(map[string]any{"disabled": true}, nil)), false)
	}
	return errorEnvelope(fmt.Errorf("%w: unknown action %q", errInvalidToken, action))
}

func slotsToMap(s b503.ErrorSlots) map[string]any {
	slots := make([]any, 5)
	for i, v := range s.Slots {
		if v == b503.EmptySlot {
			slots[i] = nil
		} else {
			slots[i] = int(v)
		}
	}
	out := map[string]any{
		"slots": slots,
	}
	if v, ok := s.FirstActive(); ok {
		out["first_active_error"] = int(v)
	} else {
		out["first_active_error"] = nil
	}
	return out
}

func historyToMap(r b503.ErrorHistoryRecord) map[string]any {
	slots := make([]any, 5)
	for i, v := range r.Slots {
		if v == b503.EmptySlot {
			slots[i] = nil
		} else {
			slots[i] = int(v)
		}
	}
	out := map[string]any{
		"index": int(r.Index),
		"slots": slots,
	}
	if v, ok := r.FirstActive(); ok {
		out["first_active_error"] = int(v)
	} else {
		out["first_active_error"] = nil
	}
	return out
}

func hexString(b []byte) string {
	const hexd = "0123456789abcdef"
	out := make([]byte, len(b)*2)
	for i, v := range b {
		out[2*i] = hexd[v>>4]
		out[2*i+1] = hexd[v&0x0f]
	}
	return string(out)
}

// --- error model ----------------------------------------------------------

var (
	errSessionBusy       = errors.New("b503mcp: SESSION_BUSY")
	errTransportDown     = errors.New("b503mcp: TRANSPORT_DOWN")
	errNotSupported      = errors.New("b503mcp: NOT_SUPPORTED")
	errInvalidToken      = errors.New("b503mcp: INVALID_TOKEN")
	errInvalidArgument   = errors.New("b503mcp: INVALID_ARGUMENT")
	errDecodeFailed      = errors.New("b503mcp: DECODE_FAILED")
	errUpstreamRPCFailed = errors.New("b503mcp: UPSTREAM_RPC_FAILED")
)

// normalizeSessionErr maps internal FSM errors to the public code set.
// EXPIRED is an internal-only state and is NEVER surfaced (spec §7.1.1).
func normalizeSessionErr(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, b503session.ErrTransportDown):
		return errTransportDown
	case errors.Is(err, b503session.ErrSessionBusy):
		return errSessionBusy
	case errors.Is(err, b503session.ErrWrongToken):
		// Spec §7.1.1: issuer_token mismatch surfaces as SESSION_BUSY on
		// the public wire (disable-with-wrong-token is indistinguishable
		// from second-claimant to the caller).
		return errSessionBusy
	case errors.Is(err, b503session.ErrNotActive):
		return errSessionBusy
	default:
		return errSessionBusy
	}
}

// classifyB503Error maps b503mcp sentinels to public envelope codes.
func classifyB503Error(err error) (string, bool) {
	switch {
	case errors.Is(err, errSessionBusy):
		return "SESSION_BUSY", true
	case errors.Is(err, errTransportDown):
		return "TRANSPORT_DOWN", true
	case errors.Is(err, errNotSupported):
		return "NOT_SUPPORTED", true
	case errors.Is(err, errInvalidToken):
		return "INVALID_TOKEN", true
	case errors.Is(err, errInvalidArgument):
		return "INVALID_ARGUMENT", true
	case errors.Is(err, errDecodeFailed):
		return "DECODE_FAILED", true
	case errors.Is(err, errUpstreamRPCFailed):
		return "UPSTREAM_RPC_FAILED", true
	}
	return "", false
}

func errorEnvelope(err error) map[string]any {
	return callToolResultText(mustJSON(newToolEnvelope(nil, err)), true)
}

// --- capability -----------------------------------------------------------

// VaillantB503Availability returns the current capability reason for the
// B503 surface on this server. Production wiring should evaluate this
// against a live probe; the conservative default is UNKNOWN when no session
// and dispatcher have been installed.
func (s *Server) VaillantB503Availability() B503Availability {
	st, ok := b503States.byServer[s]
	if !ok || st == nil {
		return AvailabilityUnknown
	}
	if st.opts.Dispatcher == nil || st.opts.SessionManager == nil {
		return AvailabilityUnknown
	}
	// If the session manager recently observed transport-down via
	// OnEpochAdvance, surface that literally.
	if st.opts.SessionManager.LastRefreshTransportDown() {
		return AvailabilityTransportDown
	}
	// Probe: attempt a lightweight CurrentError read. If the dispatcher
	// returns ErrTransportDown explicitly we surface TRANSPORT_DOWN; any
	// other probe error is conservatively treated as AVAILABLE because the
	// resolver can only definitively classify non-availability when the
	// transport layer reports down. A "no canned response" or generic
	// upstream error does NOT mean the surface is unavailable.
	_, err := st.opts.Dispatcher.Invoke(context.Background(), st.opts.DefaultTarget, b503.EncodeCurrentError())
	if err != nil && errors.Is(err, b503session.ErrTransportDown) {
		return AvailabilityTransportDown
	}
	return AvailabilityAvailable
}
