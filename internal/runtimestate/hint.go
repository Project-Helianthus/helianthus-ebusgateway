package runtimestate

// HintFromState extracts the cached source-address hint for the
// SourceAddressSelector from a loaded runtime_state State.
//
// Returns (last_admitted_source, true) when state.EBus.Self is populated
// (i.e. a prior admission cycle wrote it back via Manager.UpdateSelf);
// returns (0, false) for nil state, missing EBus namespace, or missing Self.
//
// AD24 contract: the returned hint is a HISTORICAL signal, never the
// current admitted source. Callers MUST pass it via
// SourceAddressSelectionConfig.HintCandidate so the selector validates it
// against the live bus before any surface (GraphQL gatewayIdentity,
// `helianthus_admitted_source` metric, MCP runtime status) reports a
// source as admitted.
func HintFromState(state *State) (byte, bool) {
	if state == nil || state.EBus == nil || state.EBus.Self == nil {
		return 0, false
	}
	return state.EBus.Self.LastAdmittedSource, true
}
