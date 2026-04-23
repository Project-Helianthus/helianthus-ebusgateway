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

func (p *b503GraphQLProvider) Errors(ctx context.Context, target *byte) (graphql.VaillantB503Errors, error) {
	resp, err := p.dispatcher.Invoke(ctx, p.targetOr(target), b503.EncodeCurrentError())
	if err != nil {
		return graphql.VaillantB503Errors{}, err
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
		return graphql.VaillantB503Errors{}, err
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
		return graphql.VaillantB503HistoryRecord{}, err
	}
	rec, err := b503.DecodeErrorHistory(resp)
	if err != nil {
		return graphql.VaillantB503HistoryRecord{}, err
	}
	return historyToGraphQL(rec), nil
}

func (p *b503GraphQLProvider) ServiceHistory(ctx context.Context, target *byte, index *byte) (graphql.VaillantB503HistoryRecord, error) {
	payload := b503.EncodeServiceHistory()
	if index != nil {
		payload = append(payload, *index)
	}
	resp, err := p.dispatcher.Invoke(ctx, p.targetOr(target), payload)
	if err != nil {
		return graphql.VaillantB503HistoryRecord{}, err
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
		key, err := p.mgr.Enable(ctx)
		if err != nil {
			return graphql.VaillantB503LiveMonitor{}, err
		}
		if _, err := p.dispatcher.Invoke(ctx, t, b503.EncodeLiveMonitorMain()); err != nil {
			rebuilt := b503session.SessionKey{Transport: p.mgr.TransportKey(), IssuerToken: key.IssuerToken}
			_ = p.mgr.Disable(rebuilt)
			return graphql.VaillantB503LiveMonitor{}, err
		}
		// Enable returns the issuer_token via rawHex encoding — callers
		// parse it from the response. Alternative: extend the GraphQL
		// type to carry an explicit issuer_token field; keeping rawHex
		// here for minimal M2b surface (matches MCP contract).
		return graphql.VaillantB503LiveMonitor{RawHex: hex.EncodeToString([]byte(key.IssuerToken))}, nil
	case "read":
		if p.mgr.LastRefreshTransportDown() {
			return graphql.VaillantB503LiveMonitor{}, b503session.ErrTransportDown
		}
		if err := p.mgr.Read(p.mgr.TransportKey()); err != nil {
			return graphql.VaillantB503LiveMonitor{}, err
		}
		resp, err := p.dispatcher.Invoke(ctx, t, b503.EncodeLiveMonitorMain())
		if err != nil {
			return graphql.VaillantB503LiveMonitor{}, err
		}
		return graphql.VaillantB503LiveMonitor{RawHex: hex.EncodeToString(resp)}, nil
	case "disable":
		if issuerToken == nil || *issuerToken == "" {
			return graphql.VaillantB503LiveMonitor{}, errors.New("issuer_token required for disable")
		}
		tokenBytes, err := hex.DecodeString(*issuerToken)
		if err != nil {
			// token is opaque; treat any non-hex as raw string for compatibility
			tokenBytes = []byte(*issuerToken)
		}
		key := b503session.SessionKey{Transport: p.mgr.TransportKey(), IssuerToken: string(tokenBytes)}
		if err := p.mgr.Disable(key); err != nil {
			return graphql.VaillantB503LiveMonitor{}, err
		}
		return graphql.VaillantB503LiveMonitor{RawHex: ""}, nil
	}
	return graphql.VaillantB503LiveMonitor{}, errors.New("invalid action (must be enable|read|disable)")
}

// Availability mirrors the MCP-layer VaillantB503AvailabilityCtx with the
// spec §11 enum. Sanitization defense-in-depth is implemented in the
// GraphQL layer (sanitizeAvailability) so EXPIRED can never leak even if
// a future MCP-side change returned it.
func (p *b503GraphQLProvider) Availability(ctx context.Context) string {
	return string(p.mcpServer.VaillantB503AvailabilityCtx(ctx))
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
