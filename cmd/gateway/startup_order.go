package main

import "github.com/Project-Helianthus/helianthus-ebusgateway"

func shouldCloseSemanticBarrier(admissionPath ebusgateway.TransportAdmissionPath, overrideSet bool, joinResultNotNil bool) bool {
	if admissionPath != ebusgateway.TransportAdmissionJoinCapable {
		return true
	}
	return overrideSet || joinResultNotNil
}
