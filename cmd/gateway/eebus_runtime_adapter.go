package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/drivermanager"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/eebusadmin"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/eebuscommand"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/promotioncapture"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimecontrol"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/syncevidence"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
	eebusruntime "github.com/Project-Helianthus/helianthus-eebusreg"
	"github.com/Project-Helianthus/helianthus-eebusreg/eebusraw"
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/projection"
)

type eebusRuntimeFactory func(eebusruntime.Config) (eebusruntime.Runtime, error)
type eebusOperatorRuntimeFactory func(eebusruntime.Config) (eebusruntime.Runtime, eebusruntime.AdminV1, error)

var (
	resolveEEBusInterfaceAddressesFn eebusInterfaceAddressResolver = resolveEEBusInterfaceAddresses
	newEEBusRuntimeFn                eebusRuntimeFactory           = eebusruntime.New
	newEEBusOperatorRuntimeFn        eebusOperatorRuntimeFactory   = eebusruntime.NewOperatorRuntimeV1
)

type eebusRuntimeAdapter struct {
	managed               *managedSidecarBindings
	runtime               eebusruntime.Runtime
	promotion             mcp.LeafPromotionCapture
	startupDegradedReason eebusadmin.EEBusDegradedReason

	snapshotMu            sync.Mutex
	publicationMu         sync.Mutex
	publicationHook       eebusSemregPublicationHook
	publicationGeneration uint64
	publicationClosed     bool

	shutdownOnce sync.Once
	shutdownErr  error
}

// eebusRuntimeSlot is the gateway-owned indirection used by the startup
// lifecycle. Readers retain the read lock through the delegated call, so a
// replacement cannot retire the old runtime while a request is still using
// it. The seam is intentionally eeBUS-local; a later DriverManager can own the
// same start/replace/stop shape without changing the runtime package.
type eebusRuntimeSlot struct {
	mu                    sync.RWMutex
	runtime               eebusruntime.Runtime
	closed                atomic.Bool
	retiring              atomic.Bool
	cancel                context.CancelFunc
	publicationGeneration uint64

	operationMu  sync.Mutex
	retiredErr   error
	drains       sync.WaitGroup
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
func (slot *eebusRuntimeSlot) ReplaceDetached(runtime eebusruntime.Runtime) (eebusruntime.Runtime, bool) {
	if slot == nil || runtime == nil {
		return nil, false
	}
	slot.operationMu.Lock()
	slot.mu.Lock()
	if slot.closed.Load() {
		slot.mu.Unlock()
		slot.operationMu.Unlock()
		return nil, false
	}
	previous := slot.runtime
	slot.runtime = runtime
	slot.publicationGeneration++
	slot.retiring.Store(false)
	if previous != nil {
		slot.drains.Add(1)
	}
	slot.mu.Unlock()
	slot.operationMu.Unlock()
	return previous, true
}

func (slot *eebusRuntimeSlot) shutdownDetached(runtime eebusruntime.Runtime) {
	if slot == nil || runtime == nil {
		return
	}
	err := runtime.Shutdown()
	slot.operationMu.Lock()
	slot.retiredErr = errors.Join(slot.retiredErr, err)
	slot.operationMu.Unlock()
	slot.drains.Done()
}

func (slot *eebusRuntimeSlot) Replace(runtime eebusruntime.Runtime) bool {
	previous, ok := slot.ReplaceDetached(runtime)
	if previous != nil {
		slot.shutdownDetached(previous)
	}
	if !ok && runtime != nil {
		if err := runtime.Shutdown(); err != nil {
			slot.operationMu.Lock()
			slot.retiredErr = errors.Join(slot.retiredErr, err)
			slot.operationMu.Unlock()
		}
	}
	return ok
}

// FenceTerminal makes current and future readers unavailable immediately
// without waiting for in-flight native calls or invoking native code.
func (slot *eebusRuntimeSlot) FenceTerminal() {
	if slot == nil {
		return
	}
	slot.operationMu.Lock()
	slot.closed.Store(true)
	slot.retiring.Store(true)
	slot.publicationGeneration++
	slot.operationMu.Unlock()
}

// Retire makes the slot unavailable before a reconstruction attempt. It is
// separate from Shutdown so a successful attempt may publish a replacement.
func (slot *eebusRuntimeSlot) Retire() {
	if !slot.BeginRetire() {
		return
	}
	previous := slot.DrainRetired()
	if previous != nil {
		slot.shutdownDetached(previous)
	}
}

// BeginRetire makes new readers unavailable without waiting for a native call.
// DrainRetired performs the reader drain later, outside lifecycle locks.
func (slot *eebusRuntimeSlot) BeginRetire() bool {
	if slot == nil {
		return false
	}
	slot.operationMu.Lock()
	if slot.closed.Load() {
		slot.operationMu.Unlock()
		return false
	}
	slot.retiring.Store(true)
	slot.publicationGeneration++
	slot.operationMu.Unlock()
	return true
}

func (slot *eebusRuntimeSlot) DrainRetired() eebusruntime.Runtime {
	if slot == nil {
		return nil
	}
	slot.mu.Lock()
	if slot.closed.Load() {
		slot.mu.Unlock()
		return nil
	}
	previous := slot.runtime
	slot.runtime = nil
	if previous != nil {
		slot.drains.Add(1)
	}
	slot.mu.Unlock()
	return previous
}

func (slot *eebusRuntimeSlot) Start(ctx context.Context) error {
	if slot == nil {
		return errors.New("eeBUS runtime unavailable")
	}
	slot.mu.RLock()
	defer slot.mu.RUnlock()
	if slot.closed.Load() || slot.retiring.Load() || slot.runtime == nil {
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
		if slot.cancel != nil {
			slot.cancel()
		}
		slot.mu.Lock()
		slot.closed.Store(true)
		slot.retiring.Store(true)
		previous := slot.runtime
		slot.runtime = nil
		slot.mu.Unlock()
		slot.operationMu.Unlock()
		slot.drains.Wait()
		if previous != nil {
			slot.shutdownErr = previous.Shutdown()
		}
		slot.operationMu.Lock()
		slot.shutdownErr = errors.Join(slot.retiredErr, slot.shutdownErr)
		slot.operationMu.Unlock()
	})
	return slot.shutdownErr
}

func (slot *eebusRuntimeSlot) Snapshot() (eebusruntime.SnapshotV1, error) {
	if slot == nil {
		return eebusruntime.SnapshotV1{}, errors.New("eeBUS runtime unavailable")
	}
	slot.mu.RLock()
	defer slot.mu.RUnlock()
	if slot.closed.Load() || slot.retiring.Load() || slot.runtime == nil {
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
	if slot.closed.Load() || slot.retiring.Load() || slot.runtime == nil {
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
	if slot.closed.Load() || slot.retiring.Load() || slot.runtime == nil {
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
	if slot.closed.Load() || slot.retiring.Load() || slot.runtime == nil {
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
	if slot.closed.Load() || slot.retiring.Load() || slot.runtime == nil {
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
	if slot.closed.Load() || slot.retiring.Load() || slot.runtime == nil {
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
	if slot.closed.Load() || slot.retiring.Load() || slot.runtime == nil {
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
	adapter, admin, err := startEEBusOperatorRuntimeRetained(ctx, config, resolve, factory)
	if err != nil {
		if closeErr := adapter.Shutdown(); closeErr != nil {
			err = errors.Join(err, closeErr)
		}
		return nil, nil, err
	}
	return adapter, admin, nil
}

// The caller owns cleanup of every returned adapter, including failed starts.
func startEEBusOperatorRuntimeRetained(
	ctx context.Context,
	config ebusgateway.EEBusConfig,
	resolve eebusInterfaceAddressResolver,
	factory eebusOperatorRuntimeFactory,
) (*eebusRuntimeAdapter, eebusruntime.AdminV1, error) {
	runtimeConfig, err := mapEEBusRuntimeConfig(config, resolve)
	if err != nil {
		return nil, nil, markEEBusStartupFailure(classifyEEBusConfigFailure(err), fmt.Errorf("map eeBUS runtime configuration: %w", err))
	}
	if !config.Enabled {
		return nil, nil, nil
	}
	if factory == nil {
		return nil, nil, markEEBusStartupFailure(eebusadmin.EEBusDegradedReasonRuntimeFactoryUnavailable, errors.New("enabled eeBUS admin configuration requires an operator runtime factory"))
	}
	profile, err := loadEEBusMutationLabProfile(runtimeConfig.StateRoot)
	if err != nil {
		return nil, nil, markEEBusStartupFailure(eebusadmin.EEBusDegradedReasonConfigurationInvalid, fmt.Errorf("load eeBUS mutation lab profile: %w", err))
	}
	if profile != nil {
		runtimeConfig.MutationLabProfiles = []eebusraw.MutationLabProfileV1{profile.Clone()}
	}
	runtime, admin, factoryErr := factory(runtimeConfig)
	adapter := &eebusRuntimeAdapter{runtime: runtime}
	if factoryErr != nil {
		return adapter, nil, markEEBusStartupFailure(classifyEEBusFactoryFailure(factoryErr), fmt.Errorf("construct eeBUS operator runtime: %w", factoryErr))
	}
	if runtime == nil || admin == nil {
		return adapter, nil, markEEBusStartupFailure(eebusadmin.EEBusDegradedReasonAdminBoundaryUnavailable, errors.New("construct eeBUS operator runtime: factory returned incomplete capability pair"))
	}
	if err := runtime.Start(ctx); err != nil {
		return adapter, nil, markEEBusStartupFailure(eebusadmin.EEBusDegradedReasonListenerUnavailable, fmt.Errorf("start eeBUS operator runtime: %w", err))
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
	if adapter.managed != nil && adapter.promotion != nil {
		return managedLeafPromotionCapture{bindings: adapter.managed, capture: adapter.promotion}
	}
	return adapter.promotion
}

// Defer checkpoint-store access until the managed capture gate admits a call.
// A stopped or disabled runtime need not have created its protected store yet.
type lazyLeafPromotionCapture struct {
	mu        sync.Mutex
	construct func() (mcp.LeafPromotionCapture, error)
	capture   mcp.LeafPromotionCapture
}

func (capture *lazyLeafPromotionCapture) CaptureLeafPromotion(ctx context.Context, request mcp.LeafPromotionCaptureRequest) mcp.LeafPromotionCaptureReceipt {
	capture.mu.Lock()
	defer capture.mu.Unlock()
	if capture.capture == nil {
		resolved, err := capture.construct()
		if err != nil || resolved == nil {
			if err == nil {
				err = errors.New("constructor returned nil capture")
			}
			log.Printf("leaf promotion capture runtime: %v", err)
			return mcp.LeafPromotionCaptureReceipt{Category: "ACQUISITION_FAILED"}
		}
		capture.capture = resolved
	}
	return capture.capture.CaptureLeafPromotion(ctx, request)
}

// The shell retains only the binding. Each call leases the currently admitted
// generation, preventing replacement until capture has returned.
type managedLeafPromotionCapture struct {
	bindings *managedSidecarBindings
	capture  mcp.LeafPromotionCapture
}

func (capture managedLeafPromotionCapture) CaptureLeafPromotion(ctx context.Context, request mcp.LeafPromotionCaptureRequest) (receipt mcp.LeafPromotionCaptureReceipt) {
	receipt.Category = "ACQUISITION_FAILED"
	_ = capture.bindings.entry(managedEEBusID).with(ctx, func(drivermanager.Runtime) error {
		receipt = capture.capture.CaptureLeafPromotion(ctx, request)
		return nil
	})
	return receipt
}

func eebusMCPProvider(adapter *eebusRuntimeAdapter) mcp.EEBusV1Provider {
	if adapter == nil {
		return nil
	}
	return adapter
}

func eebusMCPCommandRouter(adapter *eebusRuntimeAdapter) mcp.EEBusV1CommandRouter {
	if adapter != nil && adapter.managed != nil {
		return managedEEBusCommands{bindings: adapter.managed}
	}
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
	adapter, err := startEEBusRuntimeRetained(ctx, config, resolve, factory)
	if err != nil {
		if closeErr := adapter.Shutdown(); closeErr != nil {
			err = errors.Join(err, closeErr)
		}
		return nil, err
	}
	return adapter, nil
}

// The caller owns cleanup of every returned adapter, including failed starts.
func startEEBusRuntimeRetained(
	ctx context.Context,
	config ebusgateway.EEBusConfig,
	resolve eebusInterfaceAddressResolver,
	factory eebusRuntimeFactory,
) (*eebusRuntimeAdapter, error) {
	runtimeConfig, err := mapEEBusRuntimeConfig(config, resolve)
	if err != nil {
		return nil, markEEBusStartupFailure(classifyEEBusConfigFailure(err), fmt.Errorf("map eeBUS runtime configuration: %w", err))
	}
	if !config.Enabled {
		return nil, nil
	}
	if factory == nil {
		return nil, markEEBusStartupFailure(eebusadmin.EEBusDegradedReasonRuntimeFactoryUnavailable, errors.New("enabled eeBUS configuration requires a runtime factory"))
	}
	profile, err := loadEEBusMutationLabProfile(runtimeConfig.StateRoot)
	if err != nil {
		return nil, markEEBusStartupFailure(eebusadmin.EEBusDegradedReasonConfigurationInvalid, fmt.Errorf("load eeBUS mutation lab profile: %w", err))
	}
	if profile != nil {
		runtimeConfig.MutationLabProfiles = []eebusraw.MutationLabProfileV1{
			profile.Clone(),
		}
	}

	runtime, factoryErr := factory(runtimeConfig)
	if runtime == nil {
		if factoryErr != nil {
			return nil, markEEBusStartupFailure(classifyEEBusFactoryFailure(factoryErr), fmt.Errorf("construct eeBUS runtime: %w", factoryErr))
		}
		return nil, markEEBusStartupFailure(eebusadmin.EEBusDegradedReasonRuntimeFactoryUnavailable, errors.New("construct eeBUS runtime: factory returned nil"))
	}
	adapter := &eebusRuntimeAdapter{runtime: runtime}
	if factoryErr != nil {
		return adapter, markEEBusStartupFailure(classifyEEBusFactoryFailure(factoryErr), fmt.Errorf("construct eeBUS runtime: %w", factoryErr))
	}
	if err := runtime.Start(ctx); err != nil {
		return adapter, markEEBusStartupFailure(eebusadmin.EEBusDegradedReasonListenerUnavailable, fmt.Errorf("start eeBUS runtime: %w", err))
	}
	return adapter, nil
}

func (adapter *eebusRuntimeAdapter) Shutdown() error {
	if adapter == nil || adapter.runtime == nil {
		return nil
	}
	adapter.shutdownOnce.Do(func() {
		adapter.publicationMu.Lock()
		adapter.publicationClosed = true
		adapter.publicationGeneration++
		adapter.publicationHook = nil
		adapter.publicationMu.Unlock()
		adapter.shutdownErr = adapter.runtime.Shutdown()
	})
	return adapter.shutdownErr
}

func (adapter *eebusRuntimeAdapter) Snapshot() (eebusruntime.SnapshotV1, error) {
	if adapter != nil && adapter.managed != nil {
		return adapter.managed.eebusSnapshot()
	}
	if adapter == nil || adapter.runtime == nil {
		return eebusruntime.SnapshotV1{}, errors.New("eeBUS runtime unavailable")
	}
	return adapter.runtime.Snapshot()
}

// Native snapshots stay inside the approved eeBUS runtime adapter seam.
type eebusSemregObservation struct {
	Native   eebusruntime.SnapshotV1
	Evidence semreg.EvidenceRef
	Items    []eebusSemregItem
}

func eebusSemregObservationFrom(snapshot eebusruntime.SnapshotV1) (eebusSemregObservation, error) {
	if err := snapshot.Validate(); err != nil {
		return eebusSemregObservation{}, err
	}
	registry, err := promotioncapture.DefaultRegistry()
	if err != nil {
		return eebusSemregObservation{}, err
	}
	observation := eebusSemregObservation{
		Native: snapshot.Clone(),
		Evidence: semreg.EvidenceRef{
			Owner: "helianthus.eebusreg", Kind: "native.snapshot",
			Digest: semreg.Digest(snapshot.Meta.DataHash), Contract: eebusruntime.SnapshotContractV1,
			Access: semreg.EvidenceAccessAuthorized, Redaction: semreg.RedactionNone,
		},
	}
	for _, candidate := range registry.Candidates() {
		if candidate.SemanticPath == nil || candidate.EEBusSource == nil ||
			(candidate.ProtocolEligibility != promotioncapture.ProtocolCrossProtocol && candidate.ProtocolEligibility != promotioncapture.ProtocolEEBusNative) {
			continue
		}
		availability := semreg.AvailabilityAvailable
		if snapshot.Status.State != eebusruntime.ObservedRuntimeStateV1Ready {
			availability = semreg.AvailabilityUnavailable
		}
		observation.Items = append(observation.Items, eebusSemregItem{
			CatalogRef: candidate.CandidateID,
			Evidence:   []semreg.EvidenceRef{observation.Evidence},
			Quality: semreg.Quality{
				Assertion: semreg.AssertionObserved, Qualification: semreg.QualificationUnknown,
				Promotion: semreg.PromotionUnpromoted, Validity: semreg.ValidityUnknown,
				Availability: availability, Freshness: semreg.FreshnessUnknown,
				Reasons: []semreg.DefinitionID{eebusPendingNormativeRevision},
			},
			Disposition: projection.ProjectionUnknown,
			Loss: []projection.LossDetail{{Kind: projection.LossPolicy,
				SourceItems: []semreg.DefinitionID{}, Description: string(eebusPendingNormativeRevision)},
				{Kind: projection.LossCapability, SourceItems: []semreg.DefinitionID{},
					Description: "snapshot inventory does not establish a semantic value or capability"}},
		})
	}
	return observation, nil
}

func (adapter *eebusRuntimeAdapter) setSemregPublicationHook(hook eebusSemregPublicationHook) error {
	if adapter == nil || adapter.runtime == nil {
		return errors.New("eeBUS runtime unavailable")
	}
	adapter.publicationMu.Lock()
	defer adapter.publicationMu.Unlock()
	if adapter.publicationClosed {
		return errors.New("eeBUS publication closed")
	}
	adapter.publicationGeneration++
	adapter.publicationHook = hook
	return nil
}

func (adapter *eebusRuntimeAdapter) refreshSemregSnapshot(ctx context.Context) (eebusruntime.SnapshotV1, error) {
	if adapter == nil || adapter.runtime == nil {
		return eebusruntime.SnapshotV1{}, errors.New("eeBUS runtime unavailable")
	}
	// Serialize refreshes so an older native read cannot overwrite a later
	// disconnect observation. Lifecycle fencing does not wait for this read.
	adapter.snapshotMu.Lock()
	defer adapter.snapshotMu.Unlock()
	adapter.publicationMu.Lock()
	generation, closed := adapter.publicationGeneration, adapter.publicationClosed
	adapter.publicationMu.Unlock()
	if closed {
		return eebusruntime.SnapshotV1{}, errors.New("eeBUS publication closed")
	}
	var slotGeneration uint64
	slot, isSlot := adapter.runtime.(*eebusRuntimeSlot)
	if isSlot {
		slot.operationMu.Lock()
		slotGeneration = slot.publicationGeneration
		slot.operationMu.Unlock()
	}
	snapshot, err := adapter.runtime.Snapshot()
	if err != nil {
		return snapshot, err
	}
	adapter.publicationMu.Lock()
	defer adapter.publicationMu.Unlock()
	if err := ctx.Err(); err != nil {
		return snapshot, err
	}
	if adapter.publicationClosed || adapter.publicationGeneration != generation {
		return snapshot, errors.New("eeBUS publication fenced")
	}
	if isSlot {
		slot.operationMu.Lock()
		defer slot.operationMu.Unlock()
		if slot.closed.Load() || slot.retiring.Load() || slot.publicationGeneration != slotGeneration {
			return snapshot, errors.New("eeBUS publication fenced")
		}
	}
	if adapter.publicationHook == nil {
		return snapshot, errors.New("eeBUS SemReg publication hook unavailable")
	}
	observation, err := eebusSemregObservationFrom(snapshot)
	if err != nil {
		return snapshot, err
	}
	// The hook receives its own deep native copy. A failure never clears a
	// previously accepted item and never triggers an implicit retry.
	if err := ctx.Err(); err != nil {
		return snapshot, err
	}
	if err := adapter.publicationHook(observation); err != nil {
		return snapshot, err
	}
	return snapshot, nil
}

type managedEEBusCommands struct{ bindings *managedSidecarBindings }

func (router managedEEBusCommands) FeaturesGet(ctx context.Context, auth eebusraw.ReadAuthorizationV1, request eebusraw.FeaturesGetRequestV1) (out eebusraw.FeaturesGetDataV1, terminal *eebusraw.ErrorV1) {
	err := router.bindings.eebus(ctx, func(adapter *eebusRuntimeAdapter, _ eebusruntime.AdminV1) error {
		out, terminal = eebuscommand.New(adapter.runtime).FeaturesGet(ctx, auth, request)
		return nil
	})
	if err != nil {
		terminal = eebusRuntimeUnavailableError()
	}
	return
}
func (router managedEEBusCommands) FeaturesDataGet(ctx context.Context, auth eebusraw.ReadAuthorizationV1, request eebusraw.FeatureDataGetRequestV1) (out eebusraw.FeatureDataGetDataV1, terminal *eebusraw.ErrorV1) {
	err := router.bindings.eebus(ctx, func(adapter *eebusRuntimeAdapter, _ eebusruntime.AdminV1) error {
		out, terminal = eebuscommand.New(adapter.runtime).FeaturesDataGet(ctx, auth, request)
		return nil
	})
	if err != nil {
		terminal = eebusRuntimeUnavailableError()
	}
	return
}
func (router managedEEBusCommands) FeaturesDataSet(ctx context.Context, auth eebusraw.WriteAuthorizationV1, request eebusraw.FeatureDataSetRequestV1) (out eebusruntime.RawMutationOutcomeV1, terminal *eebusraw.ErrorV1) {
	err := router.bindings.eebus(ctx, func(adapter *eebusRuntimeAdapter, _ eebusruntime.AdminV1) error {
		out, terminal = eebuscommand.New(adapter.runtime).FeaturesDataSet(ctx, auth, request)
		return nil
	})
	if err != nil {
		terminal = eebusRuntimeUnavailableError()
	}
	return
}
func (router managedEEBusCommands) MutationsGet(ctx context.Context, auth eebusraw.ReadAuthorizationV1, request eebusraw.MutationGetRequestV1) (out eebusruntime.RawMutationOutcomeV1, terminal *eebusraw.ErrorV1) {
	err := router.bindings.eebus(ctx, func(adapter *eebusRuntimeAdapter, _ eebusruntime.AdminV1) error {
		out, terminal = eebuscommand.New(adapter.runtime).MutationsGet(ctx, auth, request)
		return nil
	})
	if err != nil {
		terminal = eebusRuntimeUnavailableError()
	}
	return
}
func (router managedEEBusCommands) MutationsRollback(ctx context.Context, auth eebusraw.WriteAuthorizationV1, request eebusraw.MutationRollbackRequestV1) (out eebusruntime.RawMutationOutcomeV1, terminal *eebusraw.ErrorV1) {
	err := router.bindings.eebus(ctx, func(adapter *eebusRuntimeAdapter, _ eebusruntime.AdminV1) error {
		out, terminal = eebuscommand.New(adapter.runtime).MutationsRollback(ctx, auth, request)
		return nil
	})
	if err != nil {
		terminal = eebusRuntimeUnavailableError()
	}
	return
}

func managedEEBusDescriptor(cfg ebusgateway.Config, source runtimecontrol.SourceID) runtimecontrol.Descriptor {
	return newEEBusManagedDriverDescriptor(managedEEBusID, []runtimecontrol.SourceID{source}, eebusManagedDriverOptions{Config: cfg.EEBusConfig, Resolve: resolveEEBusInterfaceAddressesFn, Factory: newEEBusRuntimeFn, OperatorFactory: newEEBusOperatorRuntimeFn})
}

func (driver *eebusManagedDriver) prepareManagedLease(ctx context.Context, generation uint64, lease *managedSidecarLease) error {
	return driver.WithGeneration(generation, func(adapter *eebusRuntimeAdapter, admin eebusruntime.AdminV1) error {
		if admin != nil {
			snapshot := eebusRuntimeLifecycleSnapshot{State: eebusLifecycleRunning, RuntimeAvailable: true, AdminAvailable: true, DegradedReason: adapter.startupDegradedReason}
			var err error
			lease.adminHandler, err = newEEBusAdminHandler(adapter, admin, eebusReadinessForLifecycle(snapshot))
			if err != nil {
				return err
			}
			lease.adminAvailable = true
		}
		lease.worker = newEEBusPromotedSemanticProvider(adapter)
		lease.worker.Start(ctx)
		return nil
	})
}

func (b *managedSidecarBindings) eebus(ctx context.Context, call func(*eebusRuntimeAdapter, eebusruntime.AdminV1) error) error {
	return b.entry(managedEEBusID).with(ctx, func(native drivermanager.Runtime) error {
		driver, ok := native.(*eebusManagedDriver)
		if !ok {
			return drivermanager.ErrUnavailable
		}
		return driver.WithGeneration(driver.Generation(), call)
	})
}

func (b *managedSidecarBindings) eebusSnapshot() (out eebusruntime.SnapshotV1, err error) {
	err = b.eebus(context.Background(), func(a *eebusRuntimeAdapter, _ eebusruntime.AdminV1) error { out, err = a.Snapshot(); return err })
	return
}
func (b *managedSidecarBindings) eebusLifecycleSnapshot() eebusRuntimeLifecycleSnapshot {
	result := eebusRuntimeLifecycleSnapshot{State: eebusLifecycleDisabled}
	s := b.entry(managedEEBusID)
	if s == nil || s.descriptor.Disposition != runtimecontrol.DispositionEnabled {
		return result
	}
	result.State = eebusLifecycleDegraded
	result.DegradedReason = eebusadmin.EEBusDegradedReasonRuntimeFactoryUnavailable
	if s.runtime != nil {
		if driver, ok := s.runtime.Runtime.(*eebusManagedDriver); ok {
			if reason := driver.StartupDegradedReason(); reason != "" {
				result.DegradedReason = reason
			}
		}
	}
	if s.manager != nil {
		snapshot, _ := s.manager.Snapshot(string(managedEEBusID))
		result.Revision = snapshot.Revision
		if snapshot.ObservedState == drivermanager.ObservedStarting {
			result.State = eebusLifecycleStarting
		}
		if snapshot.ObservedState == drivermanager.ObservedStopped {
			result.State = eebusLifecycleStopped
		}
	}
	_ = b.eebus(context.Background(), func(a *eebusRuntimeAdapter, admin eebusruntime.AdminV1) error {
		result.State = eebusLifecycleRunning
		result.RuntimeAvailable = true
		result.AdminAvailable = admin != nil && s.runtime.active.Load().adminAvailable
		result.DegradedReason = a.startupDegradedReason
		if !result.AdminAvailable {
			result.State = eebusLifecycleDegraded
		}
		return nil
	})
	return result
}
func (b *managedSidecarBindings) captureEEBus(key []byte) (out json.RawMessage, at time.Time, err error) {
	err = b.eebus(context.Background(), func(a *eebusRuntimeAdapter, _ eebusruntime.AdminV1) error {
		out, at, err = mcp.CaptureEEBusV1ServicesEvidence(a, key)
		return err
	})
	if errors.Is(err, drivermanager.ErrUnavailable) {
		err = syncevidence.ErrBackendUnavailable
	}
	return
}

func (b *managedSidecarBindings) eebusAvailable() bool {
	return b.eebus(context.Background(), func(*eebusRuntimeAdapter, eebusruntime.AdminV1) error { return nil }) == nil
}

func (b *managedSidecarBindings) eebusEvidenceCapture() eebusEvidenceCapture {
	// Configure is one-shot, so retain the guarded binding even before START.
	return b.captureEEBus
}
