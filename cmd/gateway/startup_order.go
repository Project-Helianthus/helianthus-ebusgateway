package main

import "github.com/Project-Helianthus/helianthus-ebusgateway"

func shouldCloseSemanticBarrier(admissionPath ebusgateway.TransportAdmissionPath, overrideSet bool, sourceSelectionReady bool) bool {
	if admissionPath != ebusgateway.TransportAdmissionSourceSelectionCapable {
		return true
	}
	return overrideSet || sourceSelectionReady
}
