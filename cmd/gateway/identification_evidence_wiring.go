package main

import (
	"sync/atomic"

	"github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
)

// identificationEvidenceStore holds the raw 07 04 evidence store chained into
// the bus observer by wireObserveFirstObservers. It is read-only to the MCP
// and GraphQL surfaces.
var identificationEvidenceStore atomic.Pointer[ebusgateway.IdentificationEvidenceStore]

func publishIdentificationEvidenceStore(store *ebusgateway.IdentificationEvidenceStore) {
	identificationEvidenceStore.Store(store)
}

func currentIdentificationEvidenceStore() *ebusgateway.IdentificationEvidenceStore {
	return identificationEvidenceStore.Load()
}

// The bus-observability provider adapters also satisfy the optional
// identification-evidence provider interfaces of the MCP server and GraphQL
// builder, which discover them when the bus-observability provider is set.
// Both read the store published by wireObserveFirstObservers and return an
// empty snapshot until it exists.

func (adapter mcpBusObservabilityProviderAdapter) IdentificationEvidence() mcp.IdentificationEvidenceSnapshot {
	snapshot := currentIdentificationEvidenceStore().Snapshot()
	out := mcp.IdentificationEvidenceSnapshot{SessionID: snapshot.SessionID}
	for _, ex := range snapshot.Exchanges {
		out.Exchanges = append(out.Exchanges, mcp.IdentificationEvidenceExchange{
			Address: ex.Address, Sequence: ex.Sequence, SessionID: ex.SessionID, CapturedAt: ex.CapturedAt,
			Request: ex.Request, Response: ex.Response,
		})
	}
	return out
}

func (adapter graphqlBusObservabilityProviderAdapter) IdentificationEvidence() graphql.IdentificationEvidenceSnapshot {
	snapshot := currentIdentificationEvidenceStore().Snapshot()
	out := graphql.IdentificationEvidenceSnapshot{SessionID: snapshot.SessionID}
	for _, ex := range snapshot.Exchanges {
		out.Exchanges = append(out.Exchanges, graphql.IdentificationEvidenceExchange{
			Address: ex.Address, Sequence: ex.Sequence, SessionID: ex.SessionID, CapturedAt: ex.CapturedAt,
			Request: ex.Request, Response: ex.Response,
		})
	}
	return out
}
