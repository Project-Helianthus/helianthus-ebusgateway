package ebusgateway

// F-19c (batch-16) spec-bound-check helpers for the passive
// transaction reconstructor. Operate on POST-unescape logical bytes.
//
// Spec references:
//   - OSI-7 Application Layer Spec V1.6.1 §2.3 (NN cap on data length).
//   - john30/ebusd `symbol.h:39-66` (escape rule: QQ/ZZ are NEVER
//     escape-encoded — escape applies from PB onward).
//   - john30/ebusd `symbol.cpp:209-229` (initiator-address nibble rule:
//     both nibbles ∈ {0x0, 0x1, 0x3, 0x7, 0xF}; gives exactly 25
//     initiator addresses).
//   - john30/ebusd `symbol.cpp:268` (target-address rule: must NOT be
//     SYN/ESC/broadcast/initiator).

// isInitiatorAddr returns true iff both nibbles of b are in the eBUS
// initiator-address set {0x0, 0x1, 0x3, 0x7, 0xF}. This yields
// exactly 25 valid initiator addresses. Reference: john30/ebusd
// `symbol.cpp:209-229`.
func isInitiatorAddr(b byte) bool {
	return isInitiatorNibble(b&0x0F) && isInitiatorNibble((b>>4)&0x0F)
}

func isInitiatorNibble(n byte) bool {
	switch n {
	case 0x0, 0x1, 0x3, 0x7, 0xF:
		return true
	}
	return false
}

// isValidTargetAddr returns true iff b is a valid target destination
// address: not SYN (0xAA), not ESC (0xA9), not the broadcast address
// (0xFE), and not an initiator. Reference: john30/ebusd
// `symbol.cpp:268`.
//
// Note: 0xAA/0xA9 cannot legitimately appear at the ZZ position
// because QQ/ZZ are never escape-encoded (`symbol.h:41`); rejecting
// them here is part of F-19c's at-observation defense.
func isValidTargetAddr(b byte) bool {
	return b != 0xAA && b != 0xA9 && b != 0xFE && !isInitiatorAddr(b)
}
