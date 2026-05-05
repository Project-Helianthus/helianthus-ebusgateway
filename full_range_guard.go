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
			if !diagnosticFlag {
				return nil, fmt.Errorf("full-range retry: disabled by default on non-ebusd-tcp; set --diagnostic-full-range-retry to enable after at least one Vaillant root candidate is observed (AD05)")
			}
			if !evidenceHasVaillantRoot {
				return nil, fmt.Errorf("full-range retry: diagnostic flag set but evidence buffer has no Vaillant root candidate yet (AD05)")
			}
		} else {
			return scanWithFullRangeGuardScanDirectedFn(ctx, bus, reg, source, targets)
		}
	}
	return scanWithFullRangeGuardScanFn(ctx, bus, reg, source, targets)
}
