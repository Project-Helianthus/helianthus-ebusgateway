package main

import (
	"context"
	"errors"
	"fmt"
	"sync"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/eebuscommand"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
	eebusruntime "github.com/Project-Helianthus/helianthus-eebusreg"
	"github.com/Project-Helianthus/helianthus-eebusreg/eebusraw"
)

type eebusRuntimeFactory func(eebusruntime.Config) (eebusruntime.Runtime, error)
type eebusOperatorRuntimeFactory func(eebusruntime.Config) (eebusruntime.Runtime, eebusruntime.AdminV1, error)

var (
	resolveEEBusInterfaceAddressesFn eebusInterfaceAddressResolver = resolveEEBusInterfaceAddresses
	newEEBusRuntimeFn                eebusRuntimeFactory           = eebusruntime.New
	newEEBusOperatorRuntimeFn        eebusOperatorRuntimeFactory   = eebusruntime.NewOperatorRuntimeV1
)

type eebusRuntimeAdapter struct {
	runtime   eebusruntime.Runtime
	promotion mcp.LeafPromotionCapture

	shutdownOnce sync.Once
	shutdownErr  error
}

// eebusRuntimeSlot is the gateway-owned indirection used by the startup
// lifecycle. Readers retain the read lock through the delegated call, so a
// replacement cannot retire the old runtime while a request is still using
// it. The seam is intentionally eeBUS-local; a later DriverManager can own the
// same start/replace/stop shape without changing the runtime package.
type eebusRuntimeSlot struct {
	mu      sync.RWMutex
	runtime eebusruntime.Runtime
	closed  bool
	cancel  context.CancelFunc

	operationMu  sync.Mutex
	retiredErr   error
	shutdownOnce sync.Once
	shutdownErr  error
}

func newEEBusRuntimeSlot(runtime eebusruntime.Runtime) *eebusRuntimeSlot {
	return &eebusRuntimeSlot{runtime: runtime}
}

func (slot *eebusRuntimeSlot) setCancel(cancel context.CancelFunc) {
	if slot == nil {
		return
	}
	slot.operationMu.Lock()
	slot.cancel = cancel
	slot.operationMu.Unlock()
}

// Replace publishes exactly one new runtime and retires the old runtime only
// after every in-flight reader has released it. A replacement arriving after
// shutdown is rejected and shut down immediately.
func (slot *eebusRuntimeSlot) Replace(runtime eebusruntime.Runtime) bool {
	if slot == nil || runtime == nil {
		return false
	}
	slot.operationMu.Lock()
	defer slot.operationMu.Unlock()

	slot.mu.Lock()
	if slot.closed {
		slot.mu.Unlock()
		if err := runtime.Shutdown(); err != nil {
			slot.retiredErr = errors.Join(slot.retiredErr, err)
		}
		return false
	}
	previous := slot.runtime
	slot.runtime = runtime
	slot.mu.Unlock()

	if previous != nil {
		if err := previous.Shutdown(); err != nil {
			slot.retiredErr = errors.Join(slot.retiredErr, err)
		}
	}
	return true
}

// Retire makes the slot unavailable before a reconstruction attempt. It is
// separate from Shutdown so a successful attempt may publish a replacement.
func (slot *eebusRuntimeSlot) Retire() {
	if slot == nil {
		return
	}
	slot.operationMu.Lock()
	defer slot.operationMu.Unlock()

	slot.mu.Lock()
	if slot.closed {
		slot.mu.Unlock()
		return
	}
	previous := slot.runtime
	slot.runtime = nil
	slot.mu.Unlock()
	if previous != nil {
		if err := previous.Shutdown(); err != nil {
			slot.retiredErr = errors.Join(slot.retiredErr, err)
		}
	}
}

func (slot *eebusRuntimeSlot) Start(ctx context.Context) error {
	if slot == nil {
		return errors.New("eeBUS runtime unavailable")
	}
	slot.mu.RLock()
	defer slot.mu.RUnlock()
	if slot.runtime == nil {
		return errors.New("eeBUS runtime unavailable")
	}
	return slot.runtime.Start(ctx)
}

func (slot *eebusRuntimeSlot) Shutdown() error {
	if slot == nil {
		return nil
	}
	slot.shutdownOnce.Do(func() {
		slot.operationMu.Lock()
		defer slot.operationMu.Unlock()
		if slot.cancel != nil {
			slot.cancel()
		}
		slot.mu.Lock()
		slot.closed = true
		previous := slot.runtime
		slot.runtime = nil
		slot.mu.Unlock()
		if previous != nil {
			slot.shutdownErr = previous.Shutdown()
		}
		slot.shutdownErr = errors.Join(slot.retiredErr, slot.shutdownErr)
	})
	return slot.shutdownErr
}

func (slot *eebusRuntimeSlot) Snapshot() (eebusruntime.SnapshotV1, error) {
	if slot == nil {
		return eebusruntime.SnapshotV1{}, errors.New("eeBUS runtime unavailable")
	}
	slot.mu.RLock()
	defer slot.mu.RUnlock()
	if slot.runtime == nil {
		return eebusruntime.SnapshotV1{}, errors.New("eeBUS runtime unavailable")
	}
	return slot.runtime.Snapshot()
}

func (slot *eebusRuntimeSlot) PairingState() ([]eebusruntime.PairingObservationV1, error) {
	if slot == nil {
		return nil, errors.New("eeBUS runtime unavailable")
	}
	slot.mu.RLock()
	defer slot.mu.RUnlock()
	if slot.runtime == nil {
		return nil, errors.New("eeBUS runtime unavailable")
	}
	return slot.runtime.PairingState()
}

func (slot *eebusRuntimeSlot) FeaturesGet(
	ctx context.Context,
	authorization eebusraw.ReadAuthorizationV1,
	request eebusraw.FeaturesGetRequestV1,
) (eebusraw.FeaturesGetDataV1, *eebusraw.ErrorV1) {
	if slot == nil {
		return eebusraw.FeaturesGetDataV1{}, eebusRuntimeUnavailableError()
	}
	slot.mu.RLock()
	defer slot.mu.RUnlock()
	if slot.runtime == nil {
		return eebusraw.FeaturesGetDataV1{}, eebusRuntimeUnavailableError()
	}
	return slot.runtime.FeaturesGet(ctx, authorization, request)
}

func (slot *eebusRuntimeSlot) FeaturesDataGet(
	ctx context.Context,
	authorization eebusraw.ReadAuthorizationV1,
	request eebusraw.FeatureDataGetRequestV1,
) (eebusraw.FeatureDataGetDataV1, *eebusraw.ErrorV1) {
	if slot == nil {
		return eebusraw.FeatureDataGetDataV1{}, eebusRuntimeUnavailableError()
	}
	slot.mu.RLock()
	defer slot.mu.RUnlock()
	if slot.runtime == nil {
		return eebusraw.FeatureDataGetDataV1{}, eebusRuntimeUnavailableError()
	}
	return slot.runtime.FeaturesDataGet(ctx, authorization, request)
}

func (slot *eebusRuntimeSlot) FeaturesDataSet(
	ctx context.Context,
	authorization eebusraw.WriteAuthorizationV1,
	request eebusraw.FeatureDataSetRequestV1,
) (eebusruntime.RawMutationOutcomeV1, *eebusraw.ErrorV1) {
	if slot == nil {
		return eebusruntime.RawMutationOutcomeV1{}, eebusRuntimeUnavailableError()
	}
	slot.mu.RLock()
	defer slot.mu.RUnlock()
	if slot.runtime == nil {
		return eebusruntime.RawMutationOutcomeV1{}, eebusRuntimeUnavailableError()
	}
	runtime, ok := slot.runtime.(eebusruntime.RawMutationRuntimeV1)
	if !ok || runtime == nil {
		return eebusruntime.RawMutationOutcomeV1{}, eebusMutationUnavailableError()
	}
	return runtime.FeaturesDataSet(ctx, authorization, request)
}

func (slot *eebusRuntimeSlot) MutationsGet(
	ctx context.Context,
	authorization eebusraw.ReadAuthorizationV1,
	request eebusraw.MutationGetRequestV1,
) (eebusruntime.RawMutationOutcomeV1, *eebusraw.ErrorV1) {
	if slot == nil {
		return eebusruntime.RawMutationOutcomeV1{}, eebusRuntimeUnavailableError()
	}
	slot.mu.RLock()
	defer slot.mu.RUnlock()
	if slot.runtime == nil {
		return eebusruntime.RawMutationOutcomeV1{}, eebusRuntimeUnavailableError()
	}
	runtime, ok := slot.runtime.(eebusruntime.RawMutationRuntimeV1)
	if !ok || runtime == nil {
		return eebusruntime.RawMutationOutcomeV1{}, eebusMutationUnavailableError()
	}
	return runtime.MutationsGet(ctx, authorization, request)
}

func (slot *eebusRuntimeSlot) MutationsRollback(
	ctx context.Context,
	authorization eebusraw.WriteAuthorizationV1,
	request eebusraw.MutationRollbackRequestV1,
) (eebusruntime.RawMutationOutcomeV1, *eebusraw.ErrorV1) {
	if slot == nil {
		return eebusruntime.RawMutationOutcomeV1{}, eebusRuntimeUnavailableError()
	}
	slot.mu.RLock()
	defer slot.mu.RUnlock()
	if slot.runtime == nil {
		return eebusruntime.RawMutationOutcomeV1{}, eebusRuntimeUnavailableError()
	}
	runtime, ok := slot.runtime.(eebusruntime.RawMutationRuntimeV1)
	if !ok || runtime == nil {
		return eebusruntime.RawMutationOutcomeV1{}, eebusMutationUnavailableError()
	}
	return runtime.MutationsRollback(ctx, authorization, request)
}

func eebusRuntimeUnavailableError() *eebusraw.ErrorV1 {
	return eebusraw.NewErrorV1(
		eebusraw.ErrorCodeV1Disconnected,
		"raw eeBUS runtime is unavailable",
		true,
		eebusraw.SourceLayerV1Runtime,
	)
}

func eebusMutationUnavailableError() *eebusraw.ErrorV1 {
	return eebusraw.NewErrorV1(
		eebusraw.ErrorCodeV1UnsupportedOperation,
		"raw eeBUS mutation capability is unavailable",
		false,
		eebusraw.SourceLayerV1Runtime,
	)
}

func startEEBusOperatorRuntime(
	ctx context.Context,
	config ebusgateway.EEBusConfig,
	resolve eebusInterfaceAddressResolver,
	factory eebusOperatorRuntimeFactory,
) (*eebusRuntimeAdapter, eebusruntime.AdminV1, error) {
	runtimeConfig, err := mapEEBusRuntimeConfig(config, resolve)
	if err != nil {
		return nil, nil, fmt.Errorf("map eeBUS runtime configuration: %w", err)
	}
	if !config.Enabled {
		return nil, nil, nil
	}
	if factory == nil {
		return nil, nil, errors.New("enabled eeBUS admin configuration requires an operator runtime factory")
	}
	profile, err := loadEEBusMutationLabProfile(runtimeConfig.StateRoot)
	if err != nil {
		return nil, nil, fmt.Errorf("load eeBUS mutation lab profile: %w", err)
	}
	if profile != nil {
		runtimeConfig.MutationLabProfiles = []eebusraw.MutationLabProfileV1{profile.Clone()}
	}
	runtime, admin, factoryErr := factory(runtimeConfig)
	if runtime == nil || admin == nil {
		if runtime != nil {
			factoryErr = errors.Join(factoryErr, runtime.Shutdown())
		}
		if factoryErr != nil {
			return nil, nil, fmt.Errorf("construct eeBUS operator runtime: %w", factoryErr)
		}
		return nil, nil, errors.New("construct eeBUS operator runtime: factory returned incomplete capability pair")
	}
	adapter := &eebusRuntimeAdapter{runtime: runtime}
	if factoryErr != nil {
		return nil, nil, fmt.Errorf("construct eeBUS operator runtime: %w", errors.Join(factoryErr, adapter.Shutdown()))
	}
	if err := runtime.Start(ctx); err != nil {
		return nil, nil, fmt.Errorf("start eeBUS operator runtime: %w", errors.Join(err, adapter.Shutdown()))
	}
	return adapter, admin, nil
}

func (adapter *eebusRuntimeAdapter) SetLeafPromotionCapture(capture mcp.LeafPromotionCapture) {
	if adapter != nil {
		adapter.promotion = capture
	}
}

func (adapter *eebusRuntimeAdapter) LeafPromotionCapture() mcp.LeafPromotionCapture {
	if adapter == nil {
		return nil
	}
	return adapter.promotion
}

func eebusMCPProvider(adapter *eebusRuntimeAdapter) mcp.EEBusV1Provider {
	if adapter == nil {
		return nil
	}
	return adapter
}

func eebusMCPCommandRouter(adapter *eebusRuntimeAdapter) mcp.EEBusV1CommandRouter {
	if adapter == nil || adapter.runtime == nil {
		return nil
	}
	return eebuscommand.New(adapter.runtime)
}

func startEEBusRuntime(
	ctx context.Context,
	config ebusgateway.EEBusConfig,
	resolve eebusInterfaceAddressResolver,
	factory eebusRuntimeFactory,
) (*eebusRuntimeAdapter, error) {
	runtimeConfig, err := mapEEBusRuntimeConfig(config, resolve)
	if err != nil {
		return nil, fmt.Errorf("map eeBUS runtime configuration: %w", err)
	}
	if !config.Enabled {
		return nil, nil
	}
	if factory == nil {
		return nil, errors.New("enabled eeBUS configuration requires a runtime factory")
	}
	profile, err := loadEEBusMutationLabProfile(runtimeConfig.StateRoot)
	if err != nil {
		return nil, fmt.Errorf("load eeBUS mutation lab profile: %w", err)
	}
	if profile != nil {
		runtimeConfig.MutationLabProfiles = []eebusraw.MutationLabProfileV1{
			profile.Clone(),
		}
	}

	runtime, factoryErr := factory(runtimeConfig)
	if runtime == nil {
		if factoryErr != nil {
			return nil, fmt.Errorf("construct eeBUS runtime: %w", factoryErr)
		}
		return nil, errors.New("construct eeBUS runtime: factory returned nil")
	}
	adapter := &eebusRuntimeAdapter{runtime: runtime}
	if factoryErr != nil {
		return nil, fmt.Errorf("construct eeBUS runtime: %w", errors.Join(factoryErr, adapter.Shutdown()))
	}
	if err := runtime.Start(ctx); err != nil {
		return nil, fmt.Errorf("start eeBUS runtime: %w", errors.Join(err, adapter.Shutdown()))
	}
	return adapter, nil
}

func (adapter *eebusRuntimeAdapter) Shutdown() error {
	if adapter == nil || adapter.runtime == nil {
		return nil
	}
	adapter.shutdownOnce.Do(func() {
		adapter.shutdownErr = adapter.runtime.Shutdown()
	})
	return adapter.shutdownErr
}

func (adapter *eebusRuntimeAdapter) Snapshot() (eebusruntime.SnapshotV1, error) {
	if adapter == nil || adapter.runtime == nil {
		return eebusruntime.SnapshotV1{}, errors.New("eeBUS runtime unavailable")
	}
	return adapter.runtime.Snapshot()
}
