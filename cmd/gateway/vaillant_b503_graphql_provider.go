package main

// M2b_GATEWAY_GRAPHQL (execution-plans#19) — production wiring of the
// graphql.VaillantB503Provider. Bridges the GraphQL resolver layer into
// the same b503session.Manager + RPCDispatcher the MCP surface uses (see
// vaillant_b503_wiring.go). A single Manager across MCP + GraphQL is
// mandatory: GraphQL Enable/Read/Disable operating on a separate Manager
// would break the single-owner session invariant.
//
// Until the production raw-frame dispatcher lands (M2b/M3 follow-up),
// all wire-bound read paths surface UPSTREAM_RPC_FAILED / UNKNOWN via
// the stub dispatcher. This matches the M5 BENCH-REPLACE gate: GraphQL
// implementation is live, but schema-stable publication remains MUST-
// gated on operator-attested live-bus captures per
// matrix/M6a-vaillant-b503.md §8.

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/vaillant/b503session"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
	"github.com/Project-Helianthus/helianthus-ebusgo/protocol/vaillant/b503"
)

type b503GraphQLProvider struct {
	mgr        *b503session.Manager
	dispatcher mcp.RPCDispatcher
	mcpServer  *mcp.Server
	defTarget  byte
}

func newB503GraphQLProvider(rt *b503Runtime) *b503GraphQLProvider {
	if rt == nil {
		return nil
	}
	return &b503GraphQLProvider{
		mgr:        rt.manager,
		dispatcher: rt.dispatcher,
		mcpServer:  rt.mcpServer,
		defTarget:  defaultVaillantTarget,
	}
}

func (p *b503GraphQLProvider) targetOr(target *byte) byte {
	if target != nil {
		return *target
	}
	return p.defTarget
}

// publicB503GraphQLError preserves the public operation code at the GraphQL
// boundary. Stable B503 MCP reads and lifecycle operations normalize dispatcher
// timeouts with other non-transport upstream failures, so GraphQL must preserve
// that same public classification rather than expose a surface-specific code.
func publicB503GraphQLError(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, b503session.ErrCleanupPending):
		return fmt.Errorf("UNKNOWN: %w", err)
	case errors.Is(err, b503session.ErrTransportDown):
		return fmt.Errorf("TRANSPORT_DOWN: %w", err)
	case errors.Is(err, b503session.ErrSessionBusy),
		errors.Is(err, b503session.ErrWrongToken),
		errors.Is(err, b503session.ErrTargetMismatch),
		errors.Is(err, b503session.ErrNotActive):
		return fmt.Errorf("SESSION_BUSY: %w", err)
	case errors.Is(err, errRawFrameUpstreamTimeout):
		return fmt.Errorf("UPSTREAM_RPC_FAILED: %w", err)
	case errors.Is(err, errRawFrameUpstreamRPCFailed):
		return fmt.Errorf("UPSTREAM_RPC_FAILED: %w", err)
	case errors.Is(err, errRawFrameStaleEpoch):
		return fmt.Errorf("UPSTREAM_RPC_FAILED: %w", err)
	default:
		return err
	}
}

func (p *b503GraphQLProvider) Errors(ctx context.Context, target *byte) (graphql.VaillantB503Errors, error) {
	resp, err := p.dispatcher.Invoke(ctx, p.targetOr(target), b503.EncodeCurrentError())
	if err != nil {
		return graphql.VaillantB503Errors{}, publicB503GraphQLError(err)
	}
	slots, err := b503.DecodeCurrentError(resp)
	if err != nil {
		return graphql.VaillantB503Errors{}, err
	}
	return slotsToGraphQL(slots), nil
}

func (p *b503GraphQLProvider) ServiceCurrent(ctx context.Context, target *byte) (graphql.VaillantB503Errors, error) {
	resp, err := p.dispatcher.Invoke(ctx, p.targetOr(target), b503.EncodeCurrentService())
	if err != nil {
		return graphql.VaillantB503Errors{}, publicB503GraphQLError(err)
	}
	slots, err := b503.DecodeCurrentService(resp)
	if err != nil {
		return graphql.VaillantB503Errors{}, err
	}
	return slotsToGraphQL(slots), nil
}

func (p *b503GraphQLProvider) ErrorHistory(ctx context.Context, target *byte, index *byte) (graphql.VaillantB503HistoryRecord, error) {
	payload := b503.EncodeErrorHistory()
	if index != nil {
		payload = append(payload, *index)
	}
	resp, err := p.dispatcher.Invoke(ctx, p.targetOr(target), payload)
	if err != nil {
		return graphql.VaillantB503HistoryRecord{}, publicB503GraphQLError(err)
	}
	rec, err := b503.DecodeErrorHistory(resp)
	if err != nil {
		return graphql.VaillantB503HistoryRecord{}, err
	}
	return historyToGraphQL(rec), nil
}

func (p *b503GraphQLProvider) ErrorsHistory(ctx context.Context, target *byte, limit int) (graphql.VaillantB503HistoryList, error) {
	if p == nil || p.mcpServer == nil {
		return graphql.VaillantB503HistoryList{}, errors.New("vaillant B503 MCP provider unavailable")
	}
	result, err := p.mcpServer.VaillantB503ErrorsHistoryList(ctx, target, limit)
	if err != nil {
		return graphql.VaillantB503HistoryList{}, err
	}
	out := make([]graphql.VaillantB503HistoryRecord, len(result.Records))
	for index, record := range result.Records {
		out[index] = graphql.VaillantB503HistoryRecord{
			Index:            record.Index,
			FirstActiveError: cloneInt(record.FirstActiveError),
			Slots:            cloneIntPointers(record.Slots),
		}
	}
	var failure *graphql.VaillantB503HistoryFailure
	if result.Failure != nil {
		failure = &graphql.VaillantB503HistoryFailure{
			Index: result.Failure.Index, Code: result.Failure.Code, Message: result.Failure.Message,
		}
	}
	return graphql.VaillantB503HistoryList{Records: out, Failure: failure}, nil
}

func (p *b503GraphQLProvider) ServiceHistory(ctx context.Context, target *byte, index *byte) (graphql.VaillantB503HistoryRecord, error) {
	payload := b503.EncodeServiceHistory()
	if index != nil {
		payload = append(payload, *index)
	}
	resp, err := p.dispatcher.Invoke(ctx, p.targetOr(target), payload)
	if err != nil {
		return graphql.VaillantB503HistoryRecord{}, publicB503GraphQLError(err)
	}
	rec, err := b503.DecodeServiceHistory(resp)
	if err != nil {
		return graphql.VaillantB503HistoryRecord{}, err
	}
	return historyToGraphQL(rec), nil
}

func (p *b503GraphQLProvider) LiveMonitor(ctx context.Context, action string, issuerToken *string, target *byte) (graphql.VaillantB503LiveMonitor, error) {
	t := p.targetOr(target)
	switch action {
	case "enable":
		dispatch := p.liveMonitorDispatch()
		key, err := p.mgr.EnableOperation(ctx, t, dispatch)
		if err != nil {
			return graphql.VaillantB503LiveMonitor{}, publicB503GraphQLError(err)
		}
		// The GraphQL type has a dedicated issuerToken field (see
		// graphql/vaillant_b503.go buildVaillantB503Types). Use it so
		// callers receive the token verbatim for subsequent disable
		// calls — do NOT re-hex-encode (the token is already a hex
		// string from newIssuerToken()).
		return graphql.VaillantB503LiveMonitor{IssuerToken: key.IssuerToken}, nil
	case "read":
		resp, err := p.mgr.ReadOperation(ctx, t, p.liveMonitorDispatch())
		if err != nil {
			return graphql.VaillantB503LiveMonitor{}, publicB503GraphQLError(err)
		}
		return graphql.VaillantB503LiveMonitor{RawHex: hex.EncodeToString(resp)}, nil
	case "disable":
		if issuerToken == nil || *issuerToken == "" {
			return graphql.VaillantB503LiveMonitor{}, errors.New("issuer_token required for disable")
		}
		// issuer_token is passed through verbatim — it's generated by
		// b503session.newIssuerToken() as a hex-encoded opaque string
		// and stored on Manager.activeToken as that same hex string.
		// Hex-decoding here would mismatch the stored value and trap
		// clients with SESSION_BUSY until idle timeout.
		key := b503session.SessionKey{Transport: p.mgr.TransportKey(), IssuerToken: *issuerToken}
		if err := p.mgr.DisableOperation(ctx, key, t, p.liveMonitorDispatch()); err != nil {
			return graphql.VaillantB503LiveMonitor{}, publicB503GraphQLError(err)
		}
		return graphql.VaillantB503LiveMonitor{Disabled: true}, nil
	}
	return graphql.VaillantB503LiveMonitor{}, errors.New("invalid action (must be enable|read|disable)")
}

func (p *b503GraphQLProvider) liveMonitorDispatch() b503session.DispatchFunc {
	return func(ctx context.Context, target byte) b503session.DispatchOutcome {
		return mcp.InvokeB503Operation(ctx, p.dispatcher, target, b503.EncodeLiveMonitorMain())
	}
}

// Availability mirrors the MCP-layer VaillantB503AvailabilityCtx with the
// spec §11 enum. Sanitization defense-in-depth is implemented in the
// GraphQL layer (sanitizeAvailability) so EXPIRED can never leak even if
// a future MCP-side change returned it.
func (p *b503GraphQLProvider) Availability(ctx context.Context, target *byte) string {
	if target == nil {
		return string(p.mcpServer.VaillantB503AvailabilityCtx(ctx))
	}
	return string(p.mcpServer.VaillantB503AvailabilityAtCtx(ctx, *target))
}

func (p *b503GraphQLProvider) LiveMonitorSession(ctx context.Context, target *byte) (graphql.VaillantB503Session, error) {
	if p == nil || p.mcpServer == nil {
		return graphql.VaillantB503Session{}, errors.New("vaillant B503 MCP provider unavailable")
	}
	session, err := p.mcpServer.VaillantB503LiveMonitorSession(ctx, target)
	if err != nil {
		return graphql.VaillantB503Session{}, err
	}
	return graphql.VaillantB503Session{State: session.State, Owned: session.Owned}, nil
}

func cloneInt(value *int) *int {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func cloneIntPointers(values []*int) []*int {
	out := make([]*int, len(values))
	for index, value := range values {
		out[index] = cloneInt(value)
	}
	return out
}

func slotsToGraphQL(s b503.ErrorSlots) graphql.VaillantB503Errors {
	out := graphql.VaillantB503Errors{Slots: make([]*int, 5)}
	for i, v := range s.Slots {
		if v != b503.EmptySlot {
			n := int(v)
			out.Slots[i] = &n
		}
	}
	if v, ok := s.FirstActive(); ok {
		n := int(v)
		out.FirstActiveError = &n
	}
	return out
}

func historyToGraphQL(r b503.ErrorHistoryRecord) graphql.VaillantB503HistoryRecord {
	out := graphql.VaillantB503HistoryRecord{
		Index: int(r.Index),
		Slots: make([]*int, 5),
	}
	for i, v := range r.Slots {
		if v != b503.EmptySlot {
			n := int(v)
			out.Slots[i] = &n
		}
	}
	if v, ok := r.FirstActive(); ok {
		n := int(v)
		out.FirstActiveError = &n
	}
	return out
}
