package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/drivermanager"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimecontrol"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimestate"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semhub"
	vaillantproviders "github.com/Project-Helianthus/helianthus-ebusreg/providers/vaillant"
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
)

// prepareGatewayRunConfig preserves run's pre-lifecycle configuration order.
// It intentionally owns no resource, goroutine, timer, or cleanup.
func prepareGatewayRunConfig(cfg *ebusgateway.Config) (gatewayBuildInfo, error) {
	resolvedBuildInfo, err := resolveGatewayBuildInfo(buildVersion, buildID)
	if err != nil {
		return gatewayBuildInfo{}, fmt.Errorf("gateway build identity: %w", err)
	}

	cfg.PreserveEBusConfigurationPresence()
	applyTransportSourcePolicy(cfg)
	if err := cfg.ValidatePortalPV(); err != nil {
		return gatewayBuildInfo{}, fmt.Errorf("validate Portal PV configuration: %w", err)
	}
	if err := cfg.ValidatePortalStorage(); err != nil {
		return gatewayBuildInfo{}, fmt.Errorf("validate Portal storage configuration: %w", err)
	}
	if err := ebusgateway.ValidateSynchronizedEvidenceConfig(*cfg); err != nil {
		return gatewayBuildInfo{}, fmt.Errorf("validate synchronized evidence config: %w", err)
	}
	if len(cfg.Providers) == 0 {
		cfg.Providers = vaillantproviders.Default()
	}
	return resolvedBuildInfo, nil
}

// prepareGatewayEBusDriver preserves the driver configuration phase before
// run installs its historical LIFO shutdown defer and starts the driver.
func prepareGatewayEBusDriver(cfg *ebusgateway.Config, sidecars ...drivermanager.DriverConfig) (*ebusDriverController, error) {
	ebusDriver, err := newEBusDriverController(*cfg, sidecars...)
	if err != nil {
		return nil, fmt.Errorf("construct eBUS DriverManager: %w", err)
	}
	cfg.PreserveEBusConfigurationPresence()
	cfg.Transport = ebusDriver.active
	if ebusDriver.passive != nil {
		cfg.PassiveTransport = ebusDriver.passive
		if configuredEBusDriverProtocol(*cfg) == ebusgateway.TransportAdapterDirect {
			cfg.ObserveFirstWarmupCompletedTransactions = 0
			cfg.ObserveFirstWarmupConnectedWindow = 0
			cfg.ObserveFirstWarmupPostResetTransactions = 0
			cfg.ObserveFirstWarmupPostResetWindow = 0
		}
	}
	return ebusDriver, nil
}

// --- G5 runtime control composition ---
//
// startGatewayRuntimeControl composes the frozen driver registry, the
// durable desired-state service and the ebus.primary manager-to-hub bridge
// before reconciliation. It is the single owner of eBUS startup: the
// bridge binds its DriverManager hooks before anything can start the
// manager, and only the service's reconciliation (through the bridge) ever
// starts, stops or replaces the eBUS generation. There is no unconditional
// start and no one-shot WaitRunning continuation.
//
// Order:
//  1. derive the configured source (DriverID plus persisted instance GUID)
//     and the exact native contract reference, and bind the bridge;
//  2. use the complete frozen registry and bind ebus.primary before any start;
//  3. register the configured drivers in the durable store (one-time seed
//     when a GUID exists, mark-only registration otherwise);
//  4. start the service: D5 restoration first, then preparation of the
//     inert source with its empty epoch barrier, then the reconcilers.
//
// Every failure is categorical driver state: the composition never returns
// a process error, so HTTP, health, MCP, GraphQL and status start even when
// eBUS is stopped, failing or withheld. There is no authorization (D4) and
// no MCP lifecycle mutator here.

const (
	gatewayControlStartBudget = 5 * time.Second
	// Generation-owned post-start tasks of ebus.primary, in start order.
	// Every one is start-critical: a failure is presented on the adapter
	// status. Best-effort work (revalidation, background scanning, passive
	// promotion) runs inside the startup-scan task as joined helpers that
	// log and continue.
	ebusTaskSourceAdmission     = "ebus.source-admission"
	ebusTaskSemanticSurface     = "ebus.semantic-surface"
	ebusTaskStartupScan         = "ebus.startup-scan"
	ebusTaskPassiveObserveFirst = "ebus.passive-observe-first"
)

// gatewayRuntimeControlOptionsFor and gatewayRuntimeControlStarted are test
// seams around the production composition; production leaves the options
// unchanged and observes nothing.
var (
	gatewayRuntimeControlOptionsFor = func(opts gatewayRuntimeControlOptions) gatewayRuntimeControlOptions { return opts }
	gatewayRuntimeControlStarted    = func(*gatewayRuntimeControl) {}
)

var errEBusGenerationTasksDraining = errors.New("ebus control: predecessor generation tasks have not joined; replacement blocked")

// gatewayRuntimeControlOptions is the composition input. Recovery, Disabled,
// Now and the timeouts are test seams; production supplies none of them.
type gatewayRuntimeControlOptions struct {
	Sidecars     *managedSidecars
	Driver       *ebusDriverController
	Hub          *semhub.Hub
	Store        *runtimestate.Manager
	InstanceGUID string
	// Disabled is the configured disposition of ebus.primary. Disabled
	// configuration wins over durable desired state.
	Disabled bool
	// Recovery is an optional managed D5 adapter; nil selects the
	// lifecycle-only composition.
	Recovery       runtimecontrol.RecoveryAdapter
	Incarnation    string
	CleanupTimeout time.Duration
	OnError        func(error)
}

// gatewayRuntimeControl is the composed control path of one process.
type gatewayRuntimeControl struct {
	hub      *semhub.Hub
	service  *runtimecontrol.Service
	registry *runtimecontrol.Registry
	store    *runtimestate.Manager
	driver   *ebusDriverController
	bridge   *ebusControlRuntime
	executor *ebusControlExecutor
	// withheld is the reason ebus.primary could not be composed (missing
	// identity, native version or registration), nil when it was.
	withheld error

	watchCancel context.CancelFunc
	watchDone   chan struct{}
	closeOnce   sync.Once
}

// newGatewayIncarnation returns a fresh process incarnation name, so source
// epochs never repeat across processes.
func newGatewayIncarnation() string {
	var entropy [6]byte
	_, _ = rand.Read(entropy[:])
	return "gw-" + strconv.FormatInt(time.Now().UTC().UnixNano(), 36) + "-" + hex.EncodeToString(entropy[:])
}

// controlHubOwnership is the registry's read-only view of hub source
// ownership.
type controlHubOwnership struct{ hub *semhub.Hub }

func (view controlHubOwnership) SourceOwner(source runtimecontrol.SourceID) (runtimecontrol.DriverID, bool) {
	if view.hub == nil {
		return "", false
	}
	info, ok := view.hub.Source(semreg.SourceID(source))
	if !ok {
		return "", false
	}
	if info.Driver == growattStorageDriver {
		return managedGrowattID, true
	}
	return runtimecontrol.DriverID(info.Driver), true
}

func startGatewayRuntimeControl(ctx context.Context, opts gatewayRuntimeControlOptions) *gatewayRuntimeControl {
	report := opts.OnError
	if report == nil {
		report = func(err error) { log.Printf("runtime control: %v", err) }
	}
	incarnation := opts.Incarnation
	if incarnation == "" {
		incarnation = newGatewayIncarnation()
	}
	control := &gatewayRuntimeControl{
		hub:      opts.Hub,
		registry: runtimecontrol.NewRegistry(controlHubOwnership{hub: opts.Hub}),
		store:    opts.Store,
		driver:   opts.Driver,
	}
	if opts.Sidecars != nil {
		control.registry = opts.Sidecars.registry
		if opts.Driver != nil {
			opts.Sidecars.bind(opts.Driver.manager, opts.Driver.runtime)
		}
	}
	id := runtimecontrol.DriverID(primaryEBusDriverID)

	// 1 + 2: identity, bridge and frozen registration. The bridge binds the
	// manager hooks here, before the service (the only start owner) runs.
	control.bridge, control.withheld = composeEBusPrimary(control.registry, opts, incarnation)
	if control.withheld != nil {
		report(fmt.Errorf("ebus.primary withheld: %w", control.withheld))
	}
	control.executor = &ebusControlExecutor{bridge: control.bridge, withheld: control.withheld}
	// The bridge consults the task runner before it admits a manager-driven
	// same-epoch replacement, so a DRAINING predecessor blocks native
	// admission and not only the successor's tasks. Installed before the
	// service (the only start owner) runs.
	if gated, ok := any(control.bridge).(interface {
		SetAdmitGate(func(context.Context) error)
	}); ok && control.bridge != nil {
		gated.SetAdmitGate(control.executor.admitGate)
	}

	// 3: durable registration. A failed load disposition writes nothing and
	// presents every configured driver as FAILED.
	sidecarStore, sidecarService := opts.Sidecars.serviceConfigs()
	storeConfigs := append([]runtimestate.ControlDriverConfig{{DriverID: primaryEBusDriverID, Enabled: !opts.Disabled}}, sidecarStore...)
	serviceConfigs := append([]runtimecontrol.DriverConfig{{DriverID: id, Executor: control.executor}}, sidecarService...)
	if _, err := opts.Store.BootstrapControl(ctx, storeConfigs); err != nil {
		report(fmt.Errorf("register drivers in runtime_control: %w (load=%s)", err, opts.Store.ControlLoad().Disposition))
	}

	prepare := control.executor.prepare
	recovery := &controlRecovery{inner: opts.Recovery, drivers: []runtimecontrol.DriverID{id},
		prepare: map[runtimecontrol.DriverID]func(context.Context) error{id: prepare}}
	for _, d := range sidecarService {
		recovery.drivers = append(recovery.drivers, d.DriverID)
	}
	service, err := runtimecontrol.New(runtimecontrol.Options{
		Store:       &runtimeControlStore{m: opts.Store},
		Recovery:    recovery,
		Drivers:     serviceConfigs,
		Incarnation: incarnation,
		OnError:     report,
	})
	if err != nil {
		// Configuration of the service itself is static; this cannot happen
		// with the inputs above, and is still never a process error.
		report(fmt.Errorf("construct control service: %w", err))
		return control
	}
	control.service = service

	// 4: restore, prepare, publish the first view and start reconcilers.
	startCtx, cancel := context.WithTimeout(ctx, gatewayControlStartBudget)
	defer cancel()
	if err := service.Start(startCtx); err != nil {
		report(fmt.Errorf("start control service: %w", err))
	}
	control.awaitInitialDispatch(startCtx)
	control.startWatch()
	return control
}

// gatewayControlDispatchBound bounds the wait for the reconciler's first
// native start request. It never waits for RUNNING.
const gatewayControlDispatchBound = time.Second

// awaitInitialDispatch keeps the historical startup property that the eBUS
// construction attempt is already in flight when the shell starts: when
// durable intent is RUNNING it waits, bounded, until the reconciler's start
// reached DriverManager (an active operation or any state change). A slow or
// failing construction is never awaited.
func (control *gatewayRuntimeControl) awaitInitialDispatch(ctx context.Context) {
	if control.bridge == nil || control.service == nil {
		return
	}
	view, err := control.service.Get(primaryEBusDriverID)
	if err != nil || !view.Enabled || view.DesiredState != runtimecontrol.DesiredRunning || view.ObservedState != drivermanager.ObservedStarting {
		return
	}
	timer := time.NewTimer(gatewayControlDispatchBound)
	defer timer.Stop()
	for {
		snapshot, changed, ok := control.bridge.manager.Watch(primaryEBusDriverID)
		if !ok || snapshot.ActiveOperation != 0 || snapshot.ObservedState != drivermanager.ObservedStopped {
			return
		}
		select {
		case <-changed:
		case <-timer.C:
			return
		case <-ctx.Done():
			return
		}
	}
}

// composeEBusPrimary derives the ebus.primary identity, binds the bridge to
// the existing DriverManager and registers the lifecycle-only descriptor.
func composeEBusPrimary(registry *runtimecontrol.Registry, opts gatewayRuntimeControlOptions, incarnation string) (*ebusControlRuntime, error) {
	if opts.Driver == nil || opts.Driver.manager == nil || opts.Hub == nil {
		return nil, fmt.Errorf("%w: eBUS manager or hub unavailable", errEBusControlInvalidConfig)
	}
	source, err := ebusPrimarySourceID(opts.InstanceGUID)
	if err != nil {
		return nil, err
	}
	native, err := readEBusPrimaryNativeContract()
	if err != nil {
		return nil, err
	}
	bridge, err := newEBusControlRuntime(ebusControlConfig{
		Manager:        opts.Driver.manager,
		Hub:            opts.Hub,
		Source:         source,
		ClockEpoch:     semreg.ClockEpochID("monotonic:" + incarnation),
		CleanupTimeout: opts.CleanupTimeout,
	})
	if err != nil {
		return nil, err
	}
	runtime := opts.Driver.runtime
	adapter := runtimecontrol.NewAdapter(func(context.Context, runtimecontrol.Construction) (*ebusDriverRuntime, error) {
		return runtime, nil
	})
	descriptor, err := newEBusPrimaryDescriptor(native, source, adapter)
	if err != nil {
		return bridge, err
	}
	if existing, err := registry.Get(descriptor.DriverID); err == nil {
		if existing.Driver.NativeContract != descriptor.NativeContract || len(existing.Driver.Sources) != 1 || existing.Driver.Sources[0] != source {
			return bridge, errEBusControlInvalidConfig
		}
		return bridge, nil
	}
	if err := registry.Register(descriptor); err != nil {
		return bridge, err
	}
	return bridge, nil
}

// Service returns the control service, nil only when it could not be built.
func (control *gatewayRuntimeControl) Service() *runtimecontrol.Service {
	if control == nil {
		return nil
	}
	return control.service
}

// Registry returns the frozen driver registry.
func (control *gatewayRuntimeControl) Registry() *runtimecontrol.Registry {
	if control == nil {
		return nil
	}
	return control.registry
}

// BindTasks installs the generation-owned post-start task runner. Tasks of
// an already admitted generation start at once; later generations start
// from the bridge's start path and the manager watch.
func (control *gatewayRuntimeControl) BindTasks(ctx context.Context, runner *ebusGenerationTaskRunner, withdrawn func(force bool)) {
	if control == nil || control.executor == nil {
		return
	}
	control.executor.bind(runner, withdrawn)
	if control.driver != nil {
		ebusAdapterConditions.Store(control.driver, control.StartupCondition)
	}
	control.executor.sync(ctx)
}

// StartupCondition names the failed start-critical task of the currently
// admitted ebus.primary generation, from the task runner's recorded
// outcomes, or returns "" when the admitted generation has none (or no
// generation is admitted). It performs no task, native or hub call.
func (control *gatewayRuntimeControl) StartupCondition() string {
	if control == nil || control.executor == nil || control.bridge == nil {
		return ""
	}
	runner, _ := control.executor.tasks()
	if runner == nil {
		return ""
	}
	key, admitted := ebusControlGeneration(control.bridge)
	status := runner.Status()
	if !admitted || status.Key != key {
		return ""
	}
	for _, task := range status.Tasks {
		if task.State == ebusGenerationTaskFailed {
			return "generation task " + task.Name + " failed"
		}
	}
	return ""
}

// GenerationAdmitted is the task runner's admission predicate: a key is
// admitted only while it is exactly the bridge's admitted generation,
// including its native mapping. Without a composed bridge nothing is ever
// admitted.
func (control *gatewayRuntimeControl) GenerationAdmitted() func(ebusGenerationKey) bool {
	if control == nil || control.bridge == nil {
		return func(ebusGenerationKey) bool { return false }
	}
	return ebusControlGenerationAdmitted(control.bridge)
}

// EBusAdmitted reports whether an ebus.primary generation is currently
// admitted (native admission, from one cached bridge view).
func (control *gatewayRuntimeControl) EBusAdmitted() bool {
	if control == nil || control.bridge == nil {
		return false
	}
	_, ok := ebusControlGeneration(control.bridge)
	return ok
}

// startWatch drives task synchronisation from every manager change, so a
// manager-owned same-epoch replacement, a loss of admission or a late
// admission is reconciled without waiting for a lifecycle decision.
func (control *gatewayRuntimeControl) startWatch() {
	if control.bridge == nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	control.watchCancel, control.watchDone = cancel, make(chan struct{})
	manager := control.bridge.manager
	go func() {
		defer close(control.watchDone)
		for {
			_, changed, ok := manager.Watch(primaryEBusDriverID)
			control.executor.sync(ctx)
			if !ok {
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-changed:
			}
		}
	}()
}

// CloseAdmission ends the control path on process exit. Mutation admission
// closes first: the service accepts no further decision and its reconcilers
// stop without writing, so durable desired RUNNING or STOPPED and in-flight
// work stay exactly as committed and no synthetic STOP is created. Then the
// manager watch stops and the generation tasks are fenced and joined
// (bounded). Cached list/get views stay readable.
func (control *gatewayRuntimeControl) CloseAdmission(ctx context.Context) error {
	if control == nil {
		return nil
	}
	var result error
	control.closeOnce.Do(func() {
		if control.service != nil {
			control.service.Close()
		}
		if control.watchCancel != nil {
			control.watchCancel()
			<-control.watchDone
		}
		if control.executor != nil {
			result = control.executor.close(ctx)
		}
		if control.driver != nil {
			ebusAdapterConditions.Delete(control.driver)
		}
	})
	return result
}

// CloseNative waits (bounded) for bridge-owned native close tasks. The
// DriverManager itself is shut down by its existing owner afterwards.
func (control *gatewayRuntimeControl) CloseNative(ctx context.Context) error {
	if control == nil || control.bridge == nil {
		return nil
	}
	return control.bridge.Close(ctx)
}

// ebusControlExecutor is the service's LifecycleExecutor for ebus.primary:
// the bridge plus the generation task runner.
//
//   - Start consults the runner first: while a predecessor generation's
//     tasks are DRAINING (fenced but not joined) the successor is not
//     started; one bounded join is attempted, and a drain that outlives it
//     refuses the start with a truthful categorical reason.
//   - Stop fences the runner's generation before the bridge's own STOP
//     fence, outside every bridge and manager lock, so no effect of the
//     stopped generation can land after the bridge fences; the two cleanup
//     obligations then run in the bridge, and the tasks are joined.
//   - Sync runs after every start and stop and from the manager watch;
//     "not admitted" is simply retried on the next change.
type ebusControlExecutor struct {
	bridge   *ebusControlRuntime
	withheld error

	mu        sync.Mutex
	runner    *ebusGenerationTaskRunner
	withdrawn func(force bool)
	closed    bool
}

var _ runtimecontrol.LifecycleExecutor = (*ebusControlExecutor)(nil)

func (executor *ebusControlExecutor) prepare(ctx context.Context) error {
	if executor.bridge == nil || executor.withheld != nil {
		if executor.withheld != nil {
			return executor.withheld
		}
		return errEBusControlNotPrepared
	}
	return executor.bridge.Prepare(ctx)
}

func (executor *ebusControlExecutor) bind(runner *ebusGenerationTaskRunner, withdrawn func(force bool)) {
	executor.mu.Lock()
	defer executor.mu.Unlock()
	if executor.closed {
		return
	}
	executor.runner, executor.withdrawn = runner, withdrawn
}

func (executor *ebusControlExecutor) tasks() (*ebusGenerationTaskRunner, func(force bool)) {
	executor.mu.Lock()
	defer executor.mu.Unlock()
	return executor.runner, executor.withdrawn
}

func (executor *ebusControlExecutor) Start(ctx context.Context, corr runtimecontrol.LifecycleCorrelation) error {
	if executor.bridge == nil || executor.withheld != nil {
		cause := executor.withheld
		if cause == nil {
			cause = errEBusControlNotPrepared
		}
		return ebusControlFailure(drivermanager.ReasonConfigInvalid, cause)
	}
	if runner, _ := executor.tasks(); runner != nil {
		if status := runner.Status(); status.Phase == ebusGenerationDraining {
			if join := runner.Withdraw(ctx, status.Key); !join.Joined {
				return ebusControlFailure(drivermanager.ReasonDependencyUnavailable, fmt.Errorf("%w: pending %v", errEBusGenerationTasksDraining, join.Pending))
			}
		}
	}
	err := executor.bridge.Start(ctx, corr)
	executor.sync(ctx)
	return err
}

func (executor *ebusControlExecutor) Stop(ctx context.Context, corr runtimecontrol.LifecycleCorrelation) runtimecontrol.StopOutcome {
	if executor.bridge == nil {
		// Nothing was ever composed, so nothing opened natively or in the hub.
		return runtimecontrol.StopOutcome{NativeClosed: true, SemanticCleaned: true}
	}
	runner, withdrawn := executor.tasks()
	var fenced *ebusGenerationKey
	if runner != nil {
		if status := runner.Status(); status.Phase == ebusGenerationRunning && status.Key.Control.DriverID == corr.DriverID && status.Key.Control.Epoch == corr.Epoch {
			// The runner fence is the serialization point for the stopped
			// generation's effects; it runs before the bridge fences and
			// outside every bridge and manager lock.
			key := status.Key
			fenced = &key
			if !runner.Fence(key) {
				// An in-flight effect of the generation outlasted the fence
				// bound: serialization has not completed, so the bridge STOP
				// fence is deferred. The generation stays DRAINING, the
				// source stays withdrawn and the cleanup stays pending.
				if withdrawn != nil {
					withdrawn(true)
				}
				return runtimecontrol.StopOutcome{}
			}
		}
	}
	if withdrawn != nil {
		// The stopped generation's admitted source is withdrawn now; a
		// successor installs its own only after its own validation.
		withdrawn(true)
	}
	out := executor.bridge.Stop(ctx, corr)
	if runner != nil && fenced != nil {
		// One bounded drain. A generation that does not join stays
		// DRAINING with its pending names and keeps the next start blocked;
		// no second timeout runs here.
		if join := runner.Withdraw(ctx, *fenced); !join.Joined {
			return out
		}
	}
	executor.sync(ctx)
	return out
}

// sync reconciles the runner with the bridge's admitted generation and
// clears process-level admission artefacts once no generation is admitted.
func (executor *ebusControlExecutor) sync(ctx context.Context) {
	if executor.bridge == nil {
		return
	}
	runner, withdrawn := executor.tasks()
	if runner == nil {
		return
	}
	if _, admitted := ebusControlGeneration(executor.bridge); !admitted && withdrawn != nil {
		withdrawn(false)
	}
	_, _ = runner.SyncEBusControl(ctx, executor.bridge)
}

// admitGate is the bridge's replacement admission gate: it withdraws the
// predecessor generation's tasks (fence, then one bounded join) and refuses
// while they have not joined. The replacement stays unadmitted, DriverManager
// keeps owning its retry, and status stays truthful (runner DRAINING).
func (executor *ebusControlExecutor) admitGate(ctx context.Context) error {
	runner, withdrawn := executor.tasks()
	if runner == nil {
		return nil
	}
	status := runner.Status()
	if status.Phase != ebusGenerationRunning && status.Phase != ebusGenerationDraining {
		return nil
	}
	if withdrawn != nil {
		// The predecessor's admitted source must not authorize anything
		// for the replacement; the successor installs its own after its
		// own source admission.
		withdrawn(true)
	}
	if join := runner.Withdraw(ctx, status.Key); !join.Joined {
		return fmt.Errorf("%w: pending %v", errEBusGenerationTasksDraining, join.Pending)
	}
	return nil
}

func (executor *ebusControlExecutor) close(ctx context.Context) error {
	executor.mu.Lock()
	executor.closed = true
	runner := executor.runner
	executor.mu.Unlock()
	if runner == nil {
		return nil
	}
	return runner.Close(ctx)
}

// installProcessSurface runs install through the generation fence, so a
// generation withdrawn before the call creates no process-scoped consumer
// and its task ends without error.
func installProcessSurface(scope *ebusGenerationScope, install func() error) error {
	var err error
	scope.Commit(func() { err = install() })
	return err
}
