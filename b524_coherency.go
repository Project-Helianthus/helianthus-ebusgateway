package ebusgateway

// IsB524ResponseCoherent reports whether a B524 (Vaillant extended-
// register access) response payload is structurally coherent with the
// original request: the response echoes the request's group and
// register address in valid positions.
//
// Two valid echo positions are accepted because B524 replies use two
// frame layouts depending on whether the opcode is preceded by a
// status / length prefix:
//
//	layout-a  resp[1]=group, resp[2..3]=addr (LE)
//	layout-b  resp[2]=group, resp[3..4]=addr (LE)
//
// A response that fails both checks is not a coherent reply — it may
// be a NACK, a fragment, or a passively-misclassified frame.
//
// Used by:
//   - cmd/gateway/semantic_vaillant.go isB524ProbeCoherent (the
//     active-probe acceptance check during discoverB524Root)
//   - bus_observability_store.go passiveResponseIsCoherentVaillantEvidence
//     (the passive strong-evidence promotion gate)
//
// Both call sites share a single source of truth for "what counts as
// a coherent B524 response" so that passive-evidence promotion uses
// the same acceptance criterion as active discovery.
func IsB524ResponseCoherent(responseData []byte, group byte, addr uint16) bool {
	if len(responseData) < 4 {
		return false
	}
	if len(responseData) >= 5 {
		if responseData[2] == group && (uint16(responseData[3])|uint16(responseData[4])<<8) == addr {
			return true
		}
	}
	return responseData[1] == group && (uint16(responseData[2])|uint16(responseData[3])<<8) == addr
}

// b524RequestParameters extracts the (group, addr) tuple from a B524
// request's Data field. Returns false when the request is too short
// or the layout is unrecognized.
//
// B524 read request Data layout (canonical):
//
//	data[0] = opcode  (e.g. 0x06 = remote read, 0x02 = local)
//	data[1] = group   (semantic group identifier)
//	data[2] = instance
//	data[3] = addr_lo
//	data[4] = addr_hi
func b524RequestParameters(requestData []byte) (group byte, addr uint16, ok bool) {
	if len(requestData) < 5 {
		return 0, 0, false
	}
	return requestData[1], uint16(requestData[3]) | uint16(requestData[4])<<8, true
}
