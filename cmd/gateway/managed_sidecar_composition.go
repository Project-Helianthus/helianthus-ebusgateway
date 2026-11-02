package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/drivermanager"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/eebusadmin"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/modbusadapter"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimecontrol"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimestate"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semhub"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
	modbus "github.com/Project-Helianthus/helianthus-modbus"
	modbusreg "github.com/Project-Helianthus/helianthus-modbusreg"
)

const (
	managedSunSpecID runtimecontrol.DriverID = "modbus.sunspec"
	managedGrowattID runtimecontrol.DriverID = "growatt.bms.rs485"
	managedEEBusID   runtimecontrol.DriverID = "eebus.primary"
)

type managedSidecars struct {
	registry *runtimecontrol.Registry
	entries  []*managedSidecar
	primary  *ebusDriverRuntime
}

type managedSidecar struct {
	descriptor runtimecontrol.Descriptor
	runtime    *managedSidecarRuntime
	manager    *drivermanager.Manager
	withheld   error
	// modbusTCP is the frozen instance of a Modbus TCP driver; nil otherwise.
	modbusTCP *modbusTCPInstance
	// retry is the service retry policy of this sidecar's START. The manager
	// retry budget stays zero, so the service is its only retry owner.
	retry    runtimecontrol.RetryPolicy
	mu       sync.Mutex
	key      runtimecontrol.LifecycleCorrelation
	retired  map[runtimecontrol.LifecycleCorrelation]bool
	started  chan struct{}
	startErr error
}

// eebusManagedStartRetry restores the startup window of the earlier eeBUS
// lifecycle for eebus.primary: three start attempts, five seconds apart. The
// other sidecars keep the service policy and non-retryable start errors.
var eebusManagedStartRetry = runtimecontrol.RetryPolicy{MaxAttempts: eebusStartupMaxAttempts, Delay: eebusStartupBackoff}

// This seam replaces only inert adapters, before any descriptor is registered.
var gatewayManagedSidecarDescriptorsFor = func(d []runtimecontrol.Descriptor) []runtimecontrol.Descriptor { return d }

func sidecarNativeContract(owner, contract string) runtimecontrol.NativeContractRef {
	ref := runtimecontrol.NativeContractRef{Owner: owner, Contract: contract, Version: "devel"}
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, dep := range info.Deps {
			if dep.Path == owner && dep.Replace == nil && exactModuleVersion(dep.Version) {
				ref.Version = dep.Version
				break
			}
		}
	}
	return ref
}

func prepareManagedSidecars(ctx context.Context, cfg ebusgateway.Config, hub *semhub.Hub, guid string, contributions *gatewayPortalCatalogContributions, portalConfig *gatewayPortalConfigRuntime) *managedSidecars {
	descriptor := func(id runtimecontrol.DriverID, enabled bool, source runtimecontrol.SourceID, native runtimecontrol.NativeContractRef, adapter runtimecontrol.LifecycleAdapter) runtimecontrol.Descriptor {
		disposition := runtimecontrol.DispositionDisabled
		if enabled {
			disposition = runtimecontrol.DispositionEnabled
		}
		return runtimecontrol.Descriptor{Format: runtimecontrol.DescriptorFormatV1, DriverID: id, Direction: runtimecontrol.DirectionSouthbound, Disposition: disposition, NativeContract: native, Sources: []runtimecontrol.SourceID{source}, Adapter: adapter}
	}
	source := func(id runtimecontrol.DriverID) runtimecontrol.SourceID {
		if guid == "" {
			return runtimecontrol.SourceID(id)
		}
		return runtimecontrol.SourceID(string(id) + ":" + guid)
	}
	var pvGeneration atomic.Uint64
	// One managed driver instance per Modbus TCP endpoint. Configuration v1 is
	// exactly the historical modbus.sunspec descriptor; a refused v2 set
	// registers no Modbus TCP instance and withholds nothing else.
	instances, err := modbusTCPInstancesFor(cfg.ModbusTCPConfig)
	if err != nil {
		log.Printf("Modbus TCP instance set refused; continuing without Modbus TCP")
	}
	tcpNative := sidecarNativeContract("github.com/Project-Helianthus/helianthus-modbus", "TCPEndpoint")
	tcpInstances := make(map[runtimecontrol.DriverID]modbusTCPInstance, len(instances))
	descriptors := make([]runtimecontrol.Descriptor, 0, len(instances)+2)
	for _, instance := range instances {
		tcpInstances[instance.driverID] = instance
		descriptors = append(descriptors, descriptor(instance.driverID, instance.enabled, source(instance.driverID), tcpNative, modbusTCPInstanceAdapter(instance, hub, contributions, &pvGeneration)))
	}
	growattSource := runtimecontrol.SourceID(cfg.ModbusTCPConfig.GrowattBMSRS485.SourceID)
	if growattSource == "" {
		growattSource = source(managedGrowattID)
	}
	descriptors = gatewayManagedSidecarDescriptorsFor(append(descriptors,
		descriptor(managedGrowattID, cfg.ModbusTCPConfig.GrowattBMSRS485.Enabled, growattSource, sidecarNativeContract("github.com/Project-Helianthus/helianthus-modbus", "RTUProductionEndpoint"), newGrowattManagedDriverFactory(cfg.ModbusTCPConfig.GrowattBMSRS485, modbusProviderGrowattBMS, hub, openGrowattBMSRTUEndpoint)),
		managedEEBusDescriptor(cfg, source(managedEEBusID)),
	))
	// An accepted Portal selection narrows configured eligibility before any
	// registration or construction. A driver withheld by a record that did not
	// load stays registered with a failing executor and is never constructed.
	portalWithheld := make(map[runtimecontrol.DriverID]error, len(descriptors))
	for i := range descriptors {
		enabled, withheld := portalConfig.configure(descriptors[i].DriverID, descriptors[i].Disposition == runtimecontrol.DispositionEnabled)
		if !enabled && descriptors[i].Disposition == runtimecontrol.DispositionEnabled {
			descriptors[i].Disposition = runtimecontrol.DispositionDisabled
		}
		portalWithheld[descriptors[i].DriverID] = withheld
	}
	registry := runtimecontrol.NewRegistry(controlHubOwnership{hub: hub})
	result := &managedSidecars{registry: registry}
	// Register fixed identities before the configurable Growatt identity.
	// A refusal withholds only its driver; no factory runs during registration.
	if primarySource, err := ebusPrimarySourceID(guid); err == nil {
		if native, err := readEBusPrimaryNativeContract(); err == nil {
			if primary, err := newEBusPrimaryDescriptor(native, primarySource, runtimecontrol.NewAdapter(func(context.Context, runtimecontrol.Construction) (*ebusDriverRuntime, error) {
				if result.primary == nil {
					return nil, drivermanager.ErrUnavailable
				}
				return result.primary, nil
			})); err == nil {
				// The accepted selection is part of the frozen registration.
				if !portalConfig.application(primaryEBusDriverID, true).enabled {
					primary.Disposition = runtimecontrol.DispositionDisabled
				}
				_ = registry.Register(primary)
			}
		}
	}
	registrationErrors := make(map[runtimecontrol.DriverID]error)
	// All Modbus TCP instances register as one set: a duplicate driver or a
	// source collision refuses the set with zero factory calls and withholds
	// only the set.
	var tcpSet []runtimecontrol.Descriptor
	for _, d := range descriptors {
		if _, ok := tcpInstances[d.DriverID]; ok {
			tcpSet = append(tcpSet, d)
		}
	}
	if len(tcpSet) > 0 {
		err := registry.Register(tcpSet...)
		for _, d := range tcpSet {
			registrationErrors[d.DriverID] = err
		}
	}
	for _, id := range []runtimecontrol.DriverID{managedEEBusID, managedGrowattID} {
		for _, d := range descriptors {
			if d.DriverID == id {
				registrationErrors[id] = registry.Register(d)
			}
		}
	}
	for _, d := range descriptors {
		withheld := registrationErrors[d.DriverID]
		if portal := portalWithheld[d.DriverID]; portal != nil {
			withheld = errors.Join(withheld, portal)
		}
		sidecar := &managedSidecar{descriptor: d, withheld: withheld}
		if instance, ok := tcpInstances[d.DriverID]; ok {
			sidecar.modbusTCP = &instance
			if sidecar.withheld == nil && d.Disposition == runtimecontrol.DispositionEnabled && instance.withheld != "" {
				sidecar.withheld = modbusTCPWithheldFailure(instance)
			}
		}
		if d.DriverID == managedEEBusID {
			sidecar.retry = eebusManagedStartRetry
		}
		result.entries = append(result.entries, sidecar)
		if sidecar.withheld != nil || d.Disposition != runtimecontrol.DispositionEnabled {
			continue
		}
		binding, err := registry.Binding(d.DriverID)
		if err != nil {
			sidecar.withheld = err
			continue
		}
		native, err := binding.Adapter.Construct(ctx, binding.Construction)
		if err != nil {
			sidecar.withheld = err
			continue
		}
		sidecar.runtime = &managedSidecarRuntime{Runtime: native, contributions: contributions}
	}
	return result
}

// portalWithheld names the sidecars withheld because the Portal record did
// not load.
func (sidecars *managedSidecars) portalWithheld() map[runtimecontrol.DriverID]error {
	if sidecars == nil {
		return nil
	}
	withheld := make(map[runtimecontrol.DriverID]error)
	for _, s := range sidecars.entries {
		if errors.Is(s.withheld, errPortalConfigWithheld) {
			withheld[s.descriptor.DriverID] = s.withheld
		}
	}
	return withheld
}

func (sidecars *managedSidecars) driverConfigs() []drivermanager.DriverConfig {
	var configs []drivermanager.DriverConfig
	for _, s := range sidecars.entries {
		if s.runtime == nil {
			continue
		}
		configs = append(configs, drivermanager.DriverConfig{ID: string(s.descriptor.DriverID), Enabled: true, Runtime: s.runtime,
			Capabilities:  []drivermanager.Capability{drivermanager.CapabilityRead, drivermanager.CapabilityRawEvidence, drivermanager.CapabilitySemanticProjection},
			ClassifyError: s.classifyStartFailure,
			Retry:         drivermanager.RetryPolicy{Budget: 0},
		})
	}
	return configs
}

// classifyStartFailure is the manager classifier of one failed sidecar
// attempt. It runs once per failed attempt that is still current, so it owns
// the single operator log line of that failure.
func (s *managedSidecar) classifyStartFailure(err error) drivermanager.Failure {
	switch {
	case s.modbusTCP != nil && s.modbusTCP.v2:
		log.Printf("Modbus TCP instance unavailable; continuing driver=%s", s.descriptor.DriverID)
	case s.descriptor.DriverID == managedSunSpecID:
		log.Printf("Modbus TCP unavailable; continuing without Modbus")
	case s.descriptor.DriverID == managedEEBusID:
		return classifyManagedEEBusStartFailure(err)
	}
	return drivermanager.Failure{Reason: drivermanager.Reason{Code: drivermanager.ReasonProviderUnavailable}}
}

// classifyManagedEEBusStartFailure logs one sanitized line for a failed
// eeBUS start: the stage and the closed categories only, never native error
// text, identities, addresses or paths. A configuration category is
// reported to the lifecycle as CONFIG_INVALID, every other category as
// PROVIDER_UNAVAILABLE.
func classifyManagedEEBusStartFailure(err error) drivermanager.Failure {
	stage, reason, operatorReason := "lifecycle", eebusadmin.EEBusDegradedReasonUnknownStartupFailure, eebusadmin.EEBusDegradedReason("")
	var failure *eebusManagedStartError
	if errors.As(err, &failure) {
		stage, reason, operatorReason = string(failure.stage), failure.reason, failure.operatorReason
	}
	line := "eeBUS runtime unavailable; continuing without eeBUS stage=" + stage + " reason=" + string(reason)
	if operatorReason != "" {
		line += " operator_reason=" + string(operatorReason)
	}
	log.Print(line)
	if reason == eebusadmin.EEBusDegradedReasonConfigurationInvalid {
		return drivermanager.Failure{Reason: drivermanager.Reason{Code: drivermanager.ReasonConfigInvalid}}
	}
	return drivermanager.Failure{Reason: drivermanager.Reason{Code: drivermanager.ReasonProviderUnavailable}}
}

// managedSidecarStartError is the closed executor failure of a sidecar START
// whose manager attempt did not reach RUNNING. It carries the manager's
// categorical reason, never native error text, so the control service
// reports that reason rather than INTERNAL_ERROR. It is retryable only for a
// sidecar with a service retry policy and a failed, unquarantined attempt;
// the service decides how many attempts remain.
type managedSidecarStartError struct {
	reason    drivermanager.ReasonCode
	retryable bool
}

func (e *managedSidecarStartError) Error() string {
	return drivermanager.ErrUnavailable.Error() + " (" + string(e.reason) + ")"
}
func (e *managedSidecarStartError) Unwrap() error                        { return drivermanager.ErrUnavailable }
func (e *managedSidecarStartError) ReasonCode() drivermanager.ReasonCode { return e.reason }
func (e *managedSidecarStartError) Retryable() bool                      { return e.retryable }

func (sidecars *managedSidecars) bind(manager *drivermanager.Manager, primary *ebusDriverRuntime) {
	if sidecars == nil {
		return
	}
	sidecars.primary = primary
	for _, s := range sidecars.entries {
		s.manager = manager
	}
}

func (sidecars *managedSidecars) serviceConfigs() ([]runtimestate.ControlDriverConfig, []runtimecontrol.DriverConfig) {
	var store []runtimestate.ControlDriverConfig
	var service []runtimecontrol.DriverConfig
	if sidecars != nil {
		for _, s := range sidecars.entries {
			store = append(store, runtimestate.ControlDriverConfig{DriverID: string(s.descriptor.DriverID), Enabled: s.descriptor.Disposition == runtimecontrol.DispositionEnabled})
			service = append(service, runtimecontrol.DriverConfig{DriverID: s.descriptor.DriverID, Executor: s, Retry: s.retry})
		}
	}
	return store, service
}

func (s *managedSidecar) normalize(c runtimecontrol.LifecycleCorrelation) (runtimecontrol.LifecycleCorrelation, error) {
	if len(s.descriptor.Sources) != 1 {
		return c, errEBusControlForeign
	}
	if c.SourceID == "" {
		c.SourceID = s.descriptor.Sources[0]
	}
	if c.DriverID != s.descriptor.DriverID || c.SourceID != s.descriptor.Sources[0] || semhub.ValidateOpaque(string(c.Epoch)) != nil || c.Generation == 0 {
		return c, errEBusControlForeign
	}
	return c, nil
}

// sidecarGenerationKey excludes the decision slot sequence: STOP and RESTART
// withdraw the admitted generation using their own transition sequence.
func sidecarGenerationKey(c runtimecontrol.LifecycleCorrelation) runtimecontrol.LifecycleCorrelation {
	c.Sequence = 0
	return c
}

// supersedesFailedLocked reports whether c names a later generation of the
// epoch whose last START completed with a failure. A failed START leaves
// nothing open, so the service retry of that epoch starts afresh and a STOP
// of the later generation covers the failed one. Callers hold s.mu.
func (s *managedSidecar) supersedesFailedLocked(c runtimecontrol.LifecycleCorrelation) bool {
	if s.started == nil || s.startErr == nil {
		return false
	}
	select {
	case <-s.started:
	default:
		return false
	}
	return c.DriverID == s.key.DriverID && c.SourceID == s.key.SourceID && c.Epoch == s.key.Epoch && c.Generation > s.key.Generation
}

func (s *managedSidecar) Start(ctx context.Context, correlation runtimecontrol.LifecycleCorrelation) error {
	if s.withheld != nil {
		return s.withheld
	}
	if s.runtime == nil || s.manager == nil {
		return drivermanager.ErrUnavailable
	}
	c, err := s.normalize(correlation)
	if err != nil {
		return err
	}
	s.mu.Lock()
	if s.retired[sidecarGenerationKey(c)] {
		s.mu.Unlock()
		return errEBusControlStale
	}
	if s.started != nil && s.key != c {
		if c.Sequence != s.key.Sequence || !s.supersedesFailedLocked(c) {
			s.mu.Unlock()
			return errEBusControlStale
		}
		if s.retired == nil {
			s.retired = make(map[runtimecontrol.LifecycleCorrelation]bool)
		}
		s.retired[sidecarGenerationKey(s.key)] = true
		s.started = nil
	}
	if s.started != nil {
		done := s.started
		s.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-done:
		}
		s.mu.Lock()
		err := s.startErr
		s.mu.Unlock()
		return err
	}
	s.key = c
	s.started = make(chan struct{})
	done := s.started
	s.mu.Unlock()
	if binder, ok := s.runtime.Runtime.(interface {
		BindLifecycle(runtimecontrol.LifecycleCorrelation) error
	}); ok {
		err = binder.BindLifecycle(c)
	}
	if err == nil {
		err = s.manager.Start(ctx, string(s.descriptor.DriverID))
	}
	if err == nil {
		snapshot, _ := s.manager.Snapshot(string(s.descriptor.DriverID))
		if snapshot.ObservedState != drivermanager.ObservedRunning {
			err = &managedSidecarStartError{reason: snapshot.Reason.Code,
				retryable: s.retry.MaxAttempts > 1 && snapshot.ObservedState == drivermanager.ObservedFailed && !snapshot.SafetyQuarantined}
		}
	}
	s.mu.Lock()
	s.startErr = err
	close(done)
	s.mu.Unlock()
	return err
}

func (s *managedSidecar) Stop(ctx context.Context, correlation runtimecontrol.LifecycleCorrelation) runtimecontrol.StopOutcome {
	c, err := s.normalize(correlation)
	if err != nil {
		return runtimecontrol.StopOutcome{}
	}
	s.mu.Lock()
	if s.retired[sidecarGenerationKey(c)] {
		s.mu.Unlock()
		return runtimecontrol.StopOutcome{NativeClosed: true, SemanticCleaned: true}
	}
	successor := false
	if s.started != nil && sidecarGenerationKey(s.key) != sidecarGenerationKey(c) {
		if !s.supersedesFailedLocked(c) {
			s.mu.Unlock()
			return runtimecontrol.StopOutcome{}
		}
		successor = true
	}
	failedKey := s.key
	done := s.started
	s.mu.Unlock()
	if s.runtime == nil {
		return runtimecontrol.StopOutcome{NativeClosed: true, SemanticCleaned: true}
	}
	report, err := s.manager.StopWithReport(ctx, string(s.descriptor.DriverID))
	closed := err == nil && report.Class.Proven()
	if closed {
		if done != nil {
			select {
			case <-done:
			case <-ctx.Done():
				return runtimecontrol.StopOutcome{}
			}
		}
		s.mu.Lock()
		if sidecarGenerationKey(s.key) == sidecarGenerationKey(c) || s.started == nil || (successor && s.key == failedKey) {
			if s.retired == nil {
				s.retired = make(map[runtimecontrol.LifecycleCorrelation]bool)
			}
			s.retired[sidecarGenerationKey(c)] = true
			if successor {
				s.retired[sidecarGenerationKey(failedKey)] = true
			}
			s.started = nil
		}
		s.mu.Unlock()
	}
	return runtimecontrol.StopOutcome{NativeClosed: closed, SemanticCleaned: closed}
}

// Leases cover the entire shell call. Withdrawal prevents new leases without
// waiting; Stop joins admitted calls before allowing the native owner to close.
type managedSidecarRuntime struct {
	stopErr error // protected by mu; preserves the latest native close cause for process shutdown
	drivermanager.Runtime
	contributions *gatewayPortalCatalogContributions
	mu            sync.Mutex
	current       *managedSidecarLease
	active        atomic.Pointer[managedSidecarLease]
}

type managedSidecarLease struct {
	fenced         atomic.Bool
	mu             sync.Mutex
	readers        int
	drained        chan struct{}
	cancel         context.CancelFunc
	worker         *eebusPromotedSemanticProvider
	storage        *gatewayPortalContributionLifecycle
	adminHandler   http.Handler
	adminAvailable bool
	semanticOnce   sync.Once
	semanticDone   chan struct{}
}

func (lease *managedSidecarLease) fence() {
	lease.fenced.Store(true)
	lease.cancel()
}

func (runtime *managedSidecarRuntime) Start(ctx context.Context) (uint64, error) {
	runtime.mu.Lock()
	previous := runtime.current
	runtime.mu.Unlock()
	if previous != nil {
		return 0, drivermanager.ErrUnavailable
	}
	generation, err := runtime.Runtime.Start(ctx)
	if err != nil {
		return 0, err
	}
	if modbus, ok := runtime.Runtime.(*modbusManagedDriver); ok {
		if a := modbus.active.Load(); a != nil {
			if work, ok := a.work.(*managedSunSpecWork); ok && work.err != nil {
				return 0, errors.Join(work.err, runtime.Runtime.Stop(ctx))
			}
		}
	}
	workCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	lease := &managedSidecarLease{cancel: cancel, semanticDone: make(chan struct{})}
	if growatt, ok := runtime.Runtime.(*growattManagedDriver); ok {
		if provider, available := growatt.Provider(); available {
			if err := attachGatewayPortalStorageContribution(provider, runtime.contributions); err != nil {
				cancel()
				return 0, errors.Join(err, runtime.Runtime.Stop(ctx))
			}
			lease.storage = provider.contribution
		}
	}
	if setup, ok := runtime.Runtime.(interface {
		prepareManagedLease(context.Context, uint64, *managedSidecarLease) error
	}); ok {
		if err := setup.prepareManagedLease(workCtx, generation, lease); err != nil {
			cancel()
			return 0, errors.Join(err, runtime.Runtime.Stop(ctx))
		}
	}
	runtime.mu.Lock()
	runtime.current = lease
	runtime.active.Store(lease)
	runtime.mu.Unlock()
	return generation, nil
}

func (runtime *managedSidecarRuntime) FenceWithdrawal() {
	if lease := runtime.active.Load(); lease != nil {
		lease.fence()
	}
	if fencer, ok := runtime.Runtime.(interface{ FenceWithdrawal() }); ok {
		fencer.FenceWithdrawal()
	}
}

func (runtime *managedSidecarRuntime) Stop(ctx context.Context) (result error) {
	defer func() {
		runtime.mu.Lock()
		runtime.stopErr = result
		runtime.mu.Unlock()
	}()
	runtime.FenceWithdrawal()
	runtime.mu.Lock()
	lease := runtime.current
	runtime.mu.Unlock()
	// Keep semantic withdrawal independent of shell reader drain. The native
	// Growatt owner already joins acquisition before closing its endpoint.
	var nativeDone chan error
	if _, ok := runtime.Runtime.(*growattManagedDriver); ok {
		nativeDone = make(chan error, 1)
		go func() { nativeDone <- runtime.Runtime.Stop(ctx) }()
	}
	if lease != nil {
		lease.semanticOnce.Do(func() {
			go func() {
				defer close(lease.semanticDone)
				if driver, ok := runtime.Runtime.(*modbusManagedDriver); ok {
					if a := driver.active.Load(); a != nil && a.adapter != nil {
						_ = a.adapter.WithdrawSemantic()
					}
				}
			}()
		})
		if lease.storage != nil {
			lease.storage.Stop()
		}
		lease.mu.Lock()
		if lease.drained == nil {
			lease.drained = make(chan struct{})
			if lease.readers == 0 {
				close(lease.drained)
			}
		}
		drained := lease.drained
		lease.mu.Unlock()
		select {
		case <-drained:
		case <-ctx.Done():
			return ctx.Err()
		}
		if err := lease.worker.Close(ctx); err != nil {
			return err
		}
	}
	var err error
	if nativeDone != nil {
		select {
		case err = <-nativeDone:
		case <-ctx.Done():
			return ctx.Err()
		}
	} else {
		err = runtime.Runtime.Stop(ctx)
	}
	if err != nil {
		return err
	}
	if lease != nil {
		select {
		case <-lease.semanticDone:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	runtime.mu.Lock()
	if runtime.current == lease {
		runtime.current = nil
		runtime.active.Store(nil)
	}
	runtime.mu.Unlock()
	return nil
}
func (runtime *managedSidecarRuntime) Replace(ctx context.Context) (uint64, error) {
	if err := runtime.Stop(ctx); err != nil {
		return 0, err
	}
	return runtime.Start(ctx)
}

func (s *managedSidecar) with(ctx context.Context, callback func(drivermanager.Runtime) error) error {
	if s == nil || s.runtime == nil || s.manager == nil {
		return drivermanager.ErrUnavailable
	}
	lease := s.runtime.active.Load()
	if lease == nil {
		return drivermanager.ErrUnavailable
	}
	lease.mu.Lock()
	if lease.fenced.Load() || ctx.Err() != nil {
		lease.mu.Unlock()
		return drivermanager.ErrUnavailable
	}
	lease.readers++
	lease.mu.Unlock()
	defer func() {
		lease.mu.Lock()
		lease.readers--
		if lease.readers == 0 && lease.drained != nil {
			close(lease.drained)
		}
		lease.mu.Unlock()
	}()
	snapshot, ok := s.manager.Snapshot(string(s.descriptor.DriverID))
	if !ok || snapshot.ObservedState != drivermanager.ObservedRunning || snapshot.Generation != s.runtime.Generation() || lease.fenced.Load() {
		return drivermanager.ErrUnavailable
	}
	return callback(s.runtime.Runtime)
}

type managedSidecarBindings struct{ sidecars *managedSidecars }

func (b *managedSidecarBindings) entry(id runtimecontrol.DriverID) *managedSidecar {
	if b != nil && b.sidecars != nil {
		for _, s := range b.sidecars.entries {
			if s.descriptor.DriverID == id {
				return s
			}
		}
	}
	return nil
}
func (b *managedSidecarBindings) modbus(ctx context.Context, call func(*modbusadapter.Adapter) error) error {
	return b.entry(managedSunSpecID).with(ctx, func(native drivermanager.Runtime) error {
		driver, ok := native.(*modbusManagedDriver)
		if !ok {
			return drivermanager.ErrUnavailable
		}
		a := driver.active.Load()
		if a == nil || a.withdrawn() || a.adapter == nil {
			return drivermanager.ErrUnavailable
		}
		return call(a.adapter)
	})
}
func (b *managedSidecarBindings) growatt(ctx context.Context, call func(*growattBMSRS485ProductionProvider) error) error {
	return b.entry(managedGrowattID).with(ctx, func(native drivermanager.Runtime) error {
		driver, ok := native.(*growattManagedDriver)
		if !ok {
			return drivermanager.ErrUnavailable
		}
		provider, ok := driver.Provider()
		if !ok {
			return drivermanager.ErrUnavailable
		}
		return call(provider)
	})
}

// Compose the existing family MCP wrappers inside the admitted lease, so their
// policy, request normalization, native evidence and envelopes stay unchanged.
type managedModbusMCPProvider struct {
	*gatewayModbusMCPProvider
	bindings *managedSidecarBindings
	cfg      ebusgateway.ModbusTCPConfig
	tesla    *teslaHSCRetainedOwner
}

func newManagedModbusMCPProvider(bindings *managedSidecarBindings, cfg ebusgateway.ModbusTCPConfig, tesla *teslaHSCRetainedOwner) *managedModbusMCPProvider {
	return &managedModbusMCPProvider{gatewayModbusMCPProvider: &gatewayModbusMCPProvider{adapter: managedModbusAdapter{bindings}, now: time.Now}, bindings: bindings, cfg: cfg, tesla: tesla}
}

type managedModbusAdapter struct{ bindings *managedSidecarBindings }

func (p managedModbusAdapter) ExecuteReadWithReconnect(ctx context.Context, plan modbusadapter.ReadPlan) (out modbus.TCPReadBatch, err error) {
	err = p.bindings.modbus(ctx, func(a *modbusadapter.Adapter) error { out, err = a.ExecuteReadWithReconnect(ctx, plan); return err })
	return
}
func (p managedModbusAdapter) ProfileObservation(profile, asset string) (out modbusadapter.ProfileObservationRecord, ok bool) {
	_ = p.bindings.modbus(context.Background(), func(a *modbusadapter.Adapter) error { out, ok = a.ProfileObservation(profile, asset); return nil })
	return
}
func (p managedModbusAdapter) SunSpecQualificationObservation(profile, asset string) (out modbusreg.SunSpecQualificationObservation, data []byte, ok bool) {
	_ = p.bindings.modbus(context.Background(), func(a *modbusadapter.Adapter) error {
		out, data, ok = a.SunSpecQualificationObservation(profile, asset)
		return nil
	})
	return
}
func (p managedModbusAdapter) SemanticPVCurrent(profile, asset string) (out modbusadapter.SemanticPVCurrent, ok bool) {
	_ = p.bindings.modbus(context.Background(), func(a *modbusadapter.Adapter) error { out, ok = a.SemanticPVCurrent(profile, asset); return nil })
	return
}
func (p *managedModbusMCPProvider) GrowattBMSRS485V202(ctx context.Context) (out mcp.GrowattBMSRS485V202Observation, err error) {
	err = p.bindings.growatt(ctx, func(g *growattBMSRS485ProductionProvider) error { out, err = g.GrowattBMSRS485V202(ctx); return err })
	return
}
func (p *managedModbusMCPProvider) GrowattStorageSemanticCurrent(ctx context.Context) (out any, err error) {
	err = p.bindings.growatt(ctx, func(g *growattBMSRS485ProductionProvider) error {
		out, err = g.GrowattStorageSemanticCurrent(ctx)
		return err
	})
	return
}
func (p *managedModbusMCPProvider) TeslaGen3EVSECurrentLimitV1(ctx context.Context) (mcp.TeslaGen3EVSECurrentLimitV1Source, error) {
	if p.tesla == nil {
		return mcp.TeslaGen3EVSECurrentLimitV1Source{}, mcp.ErrTeslaGen3EVSECurrentLimitV1ProviderUnavailable
	}
	return p.tesla.TeslaGen3EVSECurrentLimitV1(ctx)
}
func (p *managedModbusMCPProvider) TeslaGen3EVSESemanticCurrent(ctx context.Context) (any, error) {
	if p.tesla == nil {
		return nil, mcp.ErrTeslaGen3EVSESemanticUnavailable
	}
	return p.tesla.TeslaGen3EVSESemanticCurrent(ctx)
}

// ModbusV1CoreAvailable reports the v1 slot: raw reads, retained observations
// and the PV projection address only that instance, and are unavailable as
// for an unconfigured provider when it is not configured.
func (p *managedModbusMCPProvider) ModbusV1CoreAvailable() bool {
	return modbusTCPV1SlotConfigured(p.cfg)
}
func (p *managedModbusMCPProvider) GrowattStoragePortalAvailable() bool {
	return p.cfg.GrowattBMSRS485.Enabled
}
func (p *managedModbusMCPProvider) ModbusV1OptionalToolAvailable(tool string) bool {
	switch tool {
	case mcp.GrowattBMSRS485V202StatusGetTool, mcp.SemanticV1GrowattStorageCurrentGetTool:
		return p.cfg.GrowattBMSRS485.Enabled
	case mcp.TeslaGen3EVSECurrentLimitV1GetTool, mcp.SemanticV1EVSECurrentGetTool:
		return p.tesla != nil
	}
	return false
}

func (b *managedSidecarBindings) observeTransports(ctx context.Context, cfg ebusgateway.Config, store *ebusgateway.BusObservabilityStore) func() {
	watchCtx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	// The Modbus TCP status is one aggregate over every instance; publication
	// is serialized so the latest instance change is never overwritten by an
	// older aggregate.
	var modbusMu sync.Mutex
	publishModbus := func() {
		modbusMu.Lock()
		defer modbusMu.Unlock()
		store.SetTransportRuntimeStatus(modbusRuntimeTransportStatus(cfg.ModbusTCPConfig, b.modbusTCPStarted()))
	}
	for _, s := range b.sidecars.entries {
		if !s.isModbusTCP() && s.descriptor.DriverID != managedEEBusID {
			continue
		}
		wg.Add(1)
		go func(s *managedSidecar) {
			defer wg.Done()
			for {
				_, changed, ok := s.manager.Watch(string(s.descriptor.DriverID))
				if s.isModbusTCP() {
					publishModbus()
				} else {
					store.SetTransportRuntimeStatus(eebusRuntimeTransportStatus(b.eebusLifecycleSnapshot()))
				}
				if !ok {
					return
				}
				select {
				case <-watchCtx.Done():
					return
				case <-changed:
				}
			}
		}(s)
	}
	return func() { cancel(); wg.Wait() }
}

// isModbusTCP reports a Modbus TCP driver instance.
func (s *managedSidecar) isModbusTCP() bool {
	return s.modbusTCP != nil || s.descriptor.DriverID == managedSunSpecID
}

// modbusTCPStarted reports whether every enabled, unwithheld Modbus TCP
// instance is RUNNING. A withheld or disabled instance never makes the
// aggregate ready on its own. Status reads perform no endpoint I/O.
func (b *managedSidecarBindings) modbusTCPStarted() bool {
	candidates := 0
	for _, s := range b.sidecars.entries {
		if !s.isModbusTCP() || s.withheld != nil || s.descriptor.Disposition != runtimecontrol.DispositionEnabled {
			continue
		}
		candidates++
		snapshot, ok := s.manager.Snapshot(string(s.descriptor.DriverID))
		if !ok || snapshot.ObservedState != drivermanager.ObservedRunning {
			return false
		}
	}
	return candidates > 0
}

func (b *managedSidecarBindings) semanticMetrics(cfg ebusgateway.Config, tesla *teslaHSCRetainedOwner, at time.Time) []ebusgateway.SemanticMetricsDomain {
	domains := semanticPrometheusDomains(nil, nil, tesla, modbusTCPV1SlotConfigured(cfg.ModbusTCPConfig), cfg.ModbusTCPConfig.GrowattBMSRS485.Enabled, cfg.PrometheusEVSEEnabled, cfg.ModbusTCPConfig.GrowattBMSRS485.AssetID, at)
	_ = b.modbus(context.Background(), func(a *modbusadapter.Adapter) error {
		pv := semanticPrometheusDomains(a, nil, nil, true, false, false, "", at)
		for i := range domains {
			if domains[i].Name == "pv" {
				domains[i] = pv[0]
			}
		}
		return nil
	})
	_ = b.growatt(context.Background(), func(g *growattBMSRS485ProductionProvider) error {
		storage := semanticPrometheusDomains(nil, g, nil, false, true, false, cfg.ModbusTCPConfig.GrowattBMSRS485.AssetID, at)
		for i := range domains {
			if domains[i].Name == "storage" {
				domains[i] = storage[0]
			}
		}
		return nil
	})
	return domains
}

// shutdownError is read after the shared manager has joined its stop calls.
// DriverManager classifies close reports; the shell also retains their causes.
func (sidecars *managedSidecars) shutdownError() error {
	var result error
	for _, s := range sidecars.entries {
		if s.runtime == nil {
			continue
		}
		s.runtime.mu.Lock()
		err := s.runtime.stopErr
		s.runtime.mu.Unlock()
		if err != nil {
			result = errors.Join(result, fmt.Errorf("shutdown %s sidecar: %w", s.descriptor.DriverID, err))
		}
	}
	return result
}

// modbusTCPInstanceAdapter is the descriptor adapter of one instance. A
// withheld family registers an adapter that is never invoked; a composed
// sunspec instance gets its own worker, and only the v1 slot owns the Portal
// PV panel generation.
func modbusTCPInstanceAdapter(instance modbusTCPInstance, hub *semhub.Hub, contributions *gatewayPortalCatalogContributions, pvGeneration *atomic.Uint64) runtimecontrol.LifecycleAdapter {
	if instance.withheld != "" {
		return runtimecontrol.NewAdapter(func(context.Context, runtimecontrol.Construction) (*modbusManagedDriver, error) {
			return nil, errModbusTCPFamilyNotAdmitted
		})
	}
	work := func(ctx context.Context, adapter *modbusadapter.Adapter) modbusManagedWork {
		var generation uint64
		if instance.v1Slot {
			generation = pvGeneration.Add(1)
		}
		return newManagedSunSpecInstanceWork(ctx, adapter, contributions, generation, instance)
	}
	return newModbusInstanceDriverAdapter(instance.config, instance.pacing, hub, dialModbusEndpointFn, newModbusEndpointFn, work)
}
