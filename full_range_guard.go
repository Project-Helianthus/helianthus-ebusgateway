package ebusgateway

import (
	"context"
	"fmt"

	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
)

var (
	scanWithFullRangeGuardScanFn         = registry.Scan
	scanWithFullRangeGuardScanDirectedFn = registry.ScanDirected
)

// ScanWithFullRangeGuard applies the AD05 full-range retry guard before
// dispatching to the ebusreg scan implementation.
func ScanWithFullRangeGuard(ctx context.Context, bus registry.ScanBus, reg *registry.DeviceRegistry, source byte, targets []byte, admissionPath TransportAdmissionPath, diagnosticFlag bool, evidenceHasVaillantRoot bool) ([]registry.DeviceEntry, error) {
	return scanWithFullRangeGuard(ctx, bus, reg, source, targets, admissionPath, diagnosticFlag, evidenceHasVaillantRoot)
}

func scanWithFullRangeGuard(ctx context.Context, bus registry.ScanBus, reg *registry.DeviceRegistry, source byte, targets []byte, admissionPath TransportAdmissionPath, diagnosticFlag bool, evidenceHasVaillantRoot bool) ([]registry.DeviceEntry, error) {
	if admissionPath == TransportAdmissionSourceSelectionCapable {
		if len(targets) == 0 {
			_ = diagnosticFlag
			_ = evidenceHasVaillantRoot
			return nil, fmt.Errorf("source selection: active probe requires explicit bounded targets")
		} else {
			return scanWithFullRangeGuardScanDirectedFn(ctx, bus, reg, source, targets)
		}
	}
	return scanWithFullRangeGuardScanFn(ctx, bus, reg, source, targets)
}
