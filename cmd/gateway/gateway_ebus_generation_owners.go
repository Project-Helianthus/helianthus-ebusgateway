package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimecontrol"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimestate"
	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
)

// errEBusGenerationOwnersWiring reports a required collaborator that the
// lifecycle shell did not supply.
var errEBusGenerationOwnersWiring = errors.New("eBUS generation owners: missing required collaborator")

// ebusGenerationOwnerOptions are the collaborators of the eBUS generation
// owners. Bus observability, the adapter classifier and the loaded runtime
// state are optional; every other collaborator is required.
type ebusGenerationOwnerOptions struct {
	// Config is the validated process configuration. Each generation starts
	// from a copy of it.
	Config ebusgateway.Config
	// Selection holds the startup source-selection values: admission path,
	// metrics, artifact builder and the explicit source override.
	Selection startupSourceSelection

	RuntimeStateManager *runtimestate.Manager
	// RuntimeState is the state loaded at startup; its cached admitted
	// source orders the warmup candidates.
	RuntimeState *runtimestate.State

	Gateway           *ebusgateway.Gateway
	Builder           *graphql.Builder
	AdapterClassifier activeTxnClassifier
	Control           *gatewayRuntimeControl
	Retained          *retainedEBusConsumers
	BusObservability  *ebusgateway.BusObservabilityStore
}

// ebusStartupState is the per-generation hand-off between the ordered
// startup tasks.
type ebusStartupState struct {
	cfg               ebusgateway.Config
	selection         *protocol.SourceAddressSelection
	staticSource      byte
	static            bool
	signals           startupScanSignals
	admitted, scanned bool
}

// ebusGenerationOwners owns the post-start eBUS work of every admitted
// generation: the four generation tasks (source admission, semantic surface,
// startup scan and passive observe-first), their per-generation startup state
// and the task-runner configuration that orders them.
//
// Collaborators: the process context, the runtime-state manager, the
// gateway, the GraphQL builder, the runtime control, the retained consumers
// and the source-selection metrics and artifact builder are required; the
// constructor rejects a missing one. Bus observability, the adapter
// classifier and the loaded runtime state are optional.
//
// Generation contract: every effect a task installs goes through the
// generation scope, every goroutine a task starts is joined with the
// generation, and a withdrawn generation installs nothing. The process-scoped
// consumers are reached only through the retained owner's methods.
//
// Startup state: only the newest admitted generation keeps an entry; a
// generation that asks for its state drops every older entry.
type ebusGenerationOwners struct {
	ctx context.Context
	cfg ebusgateway.Config

	admissionPath        ebusgateway.TransportAdmissionPath
	metrics              *ebusgateway.StartupSourceSelectionMetrics
	artifactBuilder      *ebusgateway.SourceSelectionArtifactBuilder
	overrideSet          bool
	overrideSource       byte
	sourceSelectionAdmit bool

	runtimeStateMgr   *runtimestate.Manager
	runtimeState      *runtimestate.State
	gateway           *ebusgateway.Gateway
	builder           *graphql.Builder
	adapterClassifier activeTxnClassifier
	control           *gatewayRuntimeControl
	retained          *retainedEBusConsumers
	busObservability  *ebusgateway.BusObservabilityStore

	startupStateMu sync.Mutex
	startupStates  map[runtimecontrol.NativeGeneration]*ebusStartupState
}

// newEBusGenerationOwners validates the collaborators and creates the
// generation owners. It starts nothing.
func newEBusGenerationOwners(ctx context.Context, opts ebusGenerationOwnerOptions) (*ebusGenerationOwners, error) {
	required := []struct {
		name    string
		missing bool
	}{
		{"process context", ctx == nil},
		{"runtime-state manager", opts.RuntimeStateManager == nil},
		{"gateway", opts.Gateway == nil},
		{"builder", opts.Builder == nil},
		{"runtime control", opts.Control == nil},
		{"retained consumers", opts.Retained == nil},
		{"source-selection metrics", opts.Selection.metrics == nil},
		{"source-selection artifact builder", opts.Selection.artifactBuilder == nil},
	}
	for _, collaborator := range required {
		if collaborator.missing {
			return nil, fmt.Errorf("%w: %s", errEBusGenerationOwnersWiring, collaborator.name)
		}
	}
	selection := opts.Selection
	return &ebusGenerationOwners{
		ctx:                  ctx,
		cfg:                  opts.Config,
		admissionPath:        selection.admissionPath,
		metrics:              selection.metrics,
		artifactBuilder:      selection.artifactBuilder,
		overrideSet:          selection.overrideSet,
		overrideSource:       selection.overrideSource,
		sourceSelectionAdmit: selection.admissionPath == ebusgateway.TransportAdmissionSourceSelectionCapable && !selection.overrideSet,
		runtimeStateMgr:      opts.RuntimeStateManager,
		runtimeState:         opts.RuntimeState,
		gateway:              opts.Gateway,
		builder:              opts.Builder,
		adapterClassifier:    opts.AdapterClassifier,
		control:              opts.Control,
		retained:             opts.Retained,
		busObservability:     opts.BusObservability,
		startupStates:        map[runtimecontrol.NativeGeneration]*ebusStartupState{},
	}, nil
}

// taskConfig returns the runner configuration of the generation tasks: their
// names, their prerequisites, the admission predicate of the runtime control
// and the failure log.
func (o *ebusGenerationOwners) taskConfig() ebusGenerationTaskConfig {
	return ebusGenerationTaskConfig{
		Tasks: []ebusGenerationTask{
			{Name: ebusTaskSourceAdmission, Run: o.runSourceAdmission},
			{Name: ebusTaskSemanticSurface, Requires: []string{ebusTaskSourceAdmission}, Run: o.runSemanticSurface},
			{Name: ebusTaskStartupScan, Requires: []string{ebusTaskSemanticSurface}, Run: o.runStartupScan},
			{Name: ebusTaskPassiveObserveFirst, Requires: []string{ebusTaskStartupScan}, Run: o.runPassiveObserveFirst},
		},
		Admitted: o.control.GenerationAdmitted(),
		Failure: func(key ebusGenerationKey, task string, err error) {
			log.Printf("ebus generation task failed task=%s native_generation=%d err=%v", task, key.Native, err)
		},
	}
}

// startupStateFor returns the startup state of the scope's generation. Only
// the newest admitted generation keeps an entry.
func (o *ebusGenerationOwners) startupStateFor(scope *ebusGenerationScope) *ebusStartupState {
	o.startupStateMu.Lock()
	defer o.startupStateMu.Unlock()
	native := scope.Key().Native
	state := o.startupStates[native]
	if state == nil {
		for other := range o.startupStates {
			delete(o.startupStates, other)
		}
		state = &ebusStartupState{}
		o.startupStates[native] = state
	}
	return state
}

// runSourceAdmission is the current-session source validation and admission
// of the generation (start-critical). Its steps run in order: the
// default-policy candidate, the configured candidate, the advisory warmup
// selection and the installation of the admitted source.
func (o *ebusGenerationOwners) runSourceAdmission(genCtx context.Context, scope *ebusGenerationScope) error {
	genCfg := o.cfg
	sourceSelection, err := o.defaultPolicyCandidate(genCtx, &genCfg)
	if err != nil {
		return err
	}
	if sourceSelection == nil {
		if sourceSelection, err = o.configuredCandidate(genCtx, &genCfg); err != nil {
			return err
		}
	}
	sourceSelection, live, err := o.advisoryWarmupSelection(genCtx, scope, &genCfg, sourceSelection)
	if err != nil || !live {
		return err
	}
	return o.installGenerationSource(scope, genCfg, sourceSelection)
}

// defaultPolicyCandidate selects the default-policy startup source on a
// source-selection-capable transport without an override, without a
// configured source and without observe-first.
func (o *ebusGenerationOwners) defaultPolicyCandidate(genCtx context.Context, genCfg *ebusgateway.Config) (*protocol.SourceAddressSelection, error) {
	eligible := o.admissionPath == ebusgateway.TransportAdmissionSourceSelectionCapable && !o.overrideSet && genCfg.ScanSourceAuto && genCfg.ScanSource == 0x00 && !shouldStartPassiveObserveFirst(*genCfg)
	if !eligible {
		return nil, nil
	}
	result, err := ebusgateway.SelectDefaultStartupSourceAddress(genCtx)
	if err != nil {
		log.Printf("startup source selection degraded reason=source_selection_default_policy_failed err=%v", err)
		o.metrics.MarkDegraded(time.Now())
		o.artifactBuilder.SetDegraded("source_selection_default_policy_failed")
		return nil, nil
	}
	genCfg.ScanSource = result.Source
	genCfg.ScanSourceAuto = false
	genCfg.StartupCompanionTarget = result.Companion
	genCfg.StartupProbeTargets = append(append([]byte(nil), genCfg.StartupProbeTargets...), startupProbeTargetsForSelection(result)...)
	o.artifactBuilder.SetPromotedSuspects(len(startupProbeTargets(*genCfg)))
	o.artifactBuilder.SetSourceSelection(result.Source, result.Companion, result.Metrics.WarmupDurationActual)
	if perr := o.artifactBuilder.SetSourceSelectionMode("source_selection"); perr != nil {
		return nil, fmt.Errorf("set default-policy source-selection mode: %w", perr)
	}
	log.Printf("startup source selection candidate source=0x%02X companion_target=0x%02X provenance=source_selection_default_policy", result.Source, result.Companion)
	recordBusAdmissionTransitionWithStabilityRefresh(genCtx, o.busObservability, "pending", result.Source, result.Companion, "active_probe_pending")
	return &result, nil
}

// configuredCandidate adopts the configured startup source when the
// default policy produced no candidate.
func (o *ebusGenerationOwners) configuredCandidate(genCtx context.Context, genCfg *ebusgateway.Config) (*protocol.SourceAddressSelection, error) {
	result, ok := configuredStartupSourceAdmissionCandidate(*genCfg, o.admissionPath, o.overrideSet)
	if !ok {
		return nil, nil
	}
	genCfg.ScanSource = result.Source
	genCfg.ScanSourceAuto = false
	genCfg.StartupCompanionTarget = result.Companion
	genCfg.StartupProbeTargets = append(append([]byte(nil), genCfg.StartupProbeTargets...), startupProbeTargetsForSelection(result)...)
	o.artifactBuilder.SetPromotedSuspects(len(startupProbeTargets(*genCfg)))
	o.artifactBuilder.SetSourceSelection(result.Source, result.Companion, 0)
	if perr := o.artifactBuilder.SetSourceSelectionMode("source_selection"); perr != nil {
		return nil, fmt.Errorf("set configured source-selection mode: %w", perr)
	}
	log.Printf("startup source selection candidate source=0x%02X companion_target=0x%02X provenance=configured_source", result.Source, result.Companion)
	recordBusAdmissionTransitionWithStabilityRefresh(genCtx, o.busObservability, "pending", result.Source, result.Companion, "active_probe_pending")
	return &result, nil
}

// advisoryWarmupReconstructor returns the process-scoped passive
// reconstructor for the advisory warmup. The reconstructor is a process
// handle: it is started once and reused by every later generation's warmup.
// It reports false when the generation was withdrawn before it could publish
// the handle it started.
func (o *ebusGenerationOwners) advisoryWarmupReconstructor(scope *ebusGenerationScope, genCfg ebusgateway.Config) (*ebusgateway.PassiveTransactionReconstructor, bool, error) {
	active := o.retained.currentReconstructor()
	if active != nil {
		return active, true, nil
	}
	started, err := startPassiveTransactionReconstructor(o.ctx, genCfg)
	if err != nil {
		return nil, true, err
	}
	// Only a generation that is still live publishes the handle; a stopped
	// or replaced one closes it and ends.
	if !o.retained.publishReconstructor(scope, started) {
		return nil, false, nil
	}
	log.Printf("passive reconstructor started")
	return started, true, nil
}

// advisoryWarmupSelection runs the passive source-selection warmup on an
// observe-first, source-selection-capable transport. It returns the
// generation's candidate, and false when the generation was withdrawn and
// must install nothing.
func (o *ebusGenerationOwners) advisoryWarmupSelection(genCtx context.Context, scope *ebusGenerationScope, genCfg *ebusgateway.Config, sourceSelection *protocol.SourceAddressSelection) (*protocol.SourceAddressSelection, bool, error) {
	runAdvisorySourceSelector := o.admissionPath == ebusgateway.TransportAdmissionSourceSelectionCapable && shouldStartPassiveObserveFirst(*genCfg) && (!o.overrideSet || genCfg.StartupSource.Validate)
	if !runAdvisorySourceSelector {
		return sourceSelection, true, nil
	}
	active, live, err := o.advisoryWarmupReconstructor(scope, *genCfg)
	if err != nil || !live {
		return nil, false, err
	}
	// The inserter is not subscribed here: warmup runs before the admitted
	// source is installed, so its self-source filter would see 0.
	// attachPassiveObserveFirst binds it after admission is finalized.
	selectionBus, err := ebusgateway.NewSourceSelectionBusAdapter(active, "startup_source_selection_bus", false)
	if err != nil {
		return nil, false, fmt.Errorf("m3: new source address selection bus: %w", err)
	}
	// M4_SOURCE_SELECTION_HINT: the cached admitted source orders
	// candidates; validation still runs in full.
	hintCandidate, hintSet := runtimestate.HintFromState(o.runtimeState)
	if hintSet {
		log.Printf("source address selection hint=0x%02X (cached from prior admission cycle; warmup validation still required)", hintCandidate)
	}
	selector := protocol.NewSourceAddressSelector(selectionBus, ebusgateway.StartupAdmissionConfigWithHint(hintCandidate, hintSet))
	if o.busObservability != nil {
		o.busObservability.SetAdmissionStabilityWindow(
			ebusgateway.NewAdmissionStabilityWindow(ebusgateway.StartupAdmissionStateMinStabilitySecondsDefault),
		)
		recordBusAdmissionTransitionWithStabilityRefresh(genCtx, o.busObservability, "pending", 0, 0, "source_selection_warmup_in_progress")
	}
	o.metrics.MarkPending()
	o.metrics.StartWarmupCycle()

	log.Printf("source address selection warmup begin")
	warmupCtx, cancel := context.WithTimeout(genCtx, 6*time.Second)
	warmupStartedAt := time.Now()
	result, err := selector.Select(warmupCtx)
	cancel()
	if err != nil {
		log.Printf("startup source selection degraded reason=source_selection_failed err=%v", err)
		recordBusAdmissionTransitionWithStabilityRefresh(genCtx, o.busObservability, "degraded", 0, 0, "source_selection_failed")
		o.metrics.MarkDegraded(time.Now())
		o.artifactBuilder.SetDegraded("source_selection_failed")
		if perr := o.artifactBuilder.SetSourceSelectionMode("degraded_no_events"); perr != nil {
			return nil, false, fmt.Errorf("set degraded source-selection mode: %w", perr)
		}
		return sourceSelection, true, nil
	}
	if o.overrideSet {
		_ = ebusgateway.CheckExplicitSourceCompanionConflict(o.overrideSource, &result, o.metrics)
		return sourceSelection, true, nil
	}
	genCfg.ScanSource = result.Source
	genCfg.ScanSourceAuto = false
	genCfg.StartupCompanionTarget = result.Companion
	genCfg.StartupProbeTargets = append(append([]byte(nil), genCfg.StartupProbeTargets...), startupProbeTargetsForSelection(result)...)
	o.artifactBuilder.SetPromotedSuspects(len(startupProbeTargets(*genCfg)))
	o.artifactBuilder.SetSourceSelection(result.Source, result.Companion, time.Since(warmupStartedAt))
	if perr := o.artifactBuilder.SetSourceSelectionMode("source_selection"); perr != nil {
		return nil, false, fmt.Errorf("set selected source-selection mode: %w", perr)
	}
	log.Printf("startup source selection candidate source=0x%02X companion_target=0x%02X", result.Source, result.Companion)
	recordBusAdmissionTransitionWithStabilityRefresh(genCtx, o.busObservability, "pending", result.Source, result.Companion, "active_probe_pending")
	return &result, true, nil
}

// installGenerationSource installs the admitted source of the generation
// through the generation fence and records the generation's startup state.
// A withdrawn generation installs nothing and records nothing.
func (o *ebusGenerationOwners) installGenerationSource(scope *ebusGenerationScope, genCfg ebusgateway.Config, sourceSelection *protocol.SourceAddressSelection) error {
	if o.admissionPath == ebusgateway.TransportAdmissionSourceSelectionCapable && o.overrideSet {
		genCfg.ScanSource = o.overrideSource
		genCfg.ScanSourceAuto = false
	}
	syncStaticAdmittedSource, syncStaticAdmitted := admittedMutationSourceForGateway(genCfg, o.admissionPath, o.overrideSet)
	if syncStaticAdmitted {
		if !o.retained.installAdmittedSource(scope, syncStaticAdmittedSource) {
			return nil
		}
		// Admission resolved synchronously (override / static fallback):
		// bind the inserter and write ebus.self now. The M5 revalidator
		// waits for activeProbePassed in the startup scan.
		o.retained.subscribeAddressTableInserter()
		selectionMethod := runtimestate.SelectionMethodExplicitValidateOnly
		if isEbusdTransportProtocol(genCfg.TransportConfig.Protocol) {
			selectionMethod = runtimestate.SelectionMethodEbusdTCPFallback
		}
		recordAdmittedSourceInRuntimeState(o.runtimeStateMgr, syncStaticAdmittedSource, 0, selectionMethod, false)
	} else if !o.retained.clearAdmittedSource(scope) {
		return nil
	}
	state := o.startupStateFor(scope)
	o.startupStateMu.Lock()
	state.cfg, state.selection = genCfg, sourceSelection
	state.staticSource, state.static, state.admitted = syncStaticAdmittedSource, syncStaticAdmitted, true
	o.startupStateMu.Unlock()
	return nil
}

// runSemanticSurface creates the process-scoped consumers from the first
// admitted generation's validated configuration (start-critical).
func (o *ebusGenerationOwners) runSemanticSurface(_ context.Context, scope *ebusGenerationScope) error {
	state := o.startupStateFor(scope)
	o.startupStateMu.Lock()
	genCfg, ready := state.cfg, state.admitted
	o.startupStateMu.Unlock()
	if !ready {
		return nil
	}
	// The install runs through the generation fence: a generation that was
	// withdrawn before this point creates no process-scoped consumer, and
	// only a live generation resumes the retained poller.
	return installProcessSurface(scope, func() error { return o.retained.ensureProcessSurface(scope.Context(), genCfg) })
}

// startupScanConfig resolves the source of the startup scan and its
// provenance for the generation.
func (o *ebusGenerationOwners) startupScanConfig(genCfg ebusgateway.Config, sourceSelection *protocol.SourceAddressSelection) (ebusgateway.Config, string) {
	startupCfg := resolveStartupScanSourceConfig(genCfg)
	startupSourceProvenance := startupScanSourceMode(genCfg, startupCfg)
	if o.admissionPath == ebusgateway.TransportAdmissionSourceSelectionCapable && o.overrideSet {
		startupCfg.ScanSource = o.overrideSource
		startupCfg.ScanSourceAuto = false
		startupSourceProvenance = "explicit_validate_only"
	} else if o.admissionPath == ebusgateway.TransportAdmissionSourceSelectionCapable && sourceSelection != nil {
		startupCfg.ScanSource = sourceSelection.Source
		startupCfg.ScanSourceAuto = false
		startupSourceProvenance = "source_selection"
	}
	return startupCfg, startupSourceProvenance
}

// runStartupScan starts the startup discovery scan of the generation and its
// admission follow-ups; revalidation, background scanning and passive
// promotion run as joined, best-effort generation work.
func (o *ebusGenerationOwners) runStartupScan(genCtx context.Context, scope *ebusGenerationScope) error {
	state := o.startupStateFor(scope)
	o.startupStateMu.Lock()
	genCfg, sourceSelection, ready := state.cfg, state.selection, state.admitted
	syncStaticAdmittedSource, syncStaticAdmitted := state.staticSource, state.static
	o.startupStateMu.Unlock()
	if !ready {
		return nil
	}
	poller, passivePromoter := o.retained.pollerAndPromoter()

	startupCfg, startupSourceProvenance := o.startupScanConfig(genCfg, sourceSelection)

	log.Printf("startup scan pass")
	log.Printf("startup_directed_probe_phase begin source=0x%02X provenance=%s", startupCfg.ScanSource, startupSourceProvenance)
	if o.overrideSet {
		o.artifactBuilder.SetExplicitSource(startupCfg.ScanSource)
	}
	startupScanSignals := startDiscoveryScanLoopFn(genCtx, startupCfg, o.gateway, o.builder, o.adapterClassifier)
	// The scan loop goroutine is joined with the generation through its
	// completion signal: it runs on the generation context, and the
	// generation is not joined before the loop returned.
	if loopDone := startupScanSignals.done; loopDone != nil {
		joinLoop := func() {
			<-loopDone
			// The loop has returned, so no further delayed identity
			// enrichment can be scheduled; wait for the ones it started.
			postStartupIdentityRetryWG.Wait()
		}
		if !scope.GoJoin(func(context.Context) { joinLoop() }) {
			joinLoop()
		}
	}
	o.startupStateMu.Lock()
	state.signals, state.scanned = startupScanSignals, true
	o.startupStateMu.Unlock()

	// Static / override admission: the M5 revalidator starts only after
	// activeProbePassed, so directed 07 04 probes stay outside the startup
	// admission scan window.
	if syncStaticAdmitted && !o.sourceSelectionAdmit {
		o.startStaticRevalidation(scope, genCfg, syncStaticAdmittedSource, startupScanSignals)
	}
	if o.retained.hasSemanticBarrier() || sourceSelection != nil {
		o.startAdmissionFollowUp(scope, genCfg, sourceSelection, startupScanSignals)
	}

	scope.Go(func(runCtx context.Context) {
		startBackgroundFullScanFn(runCtx, startupCfg, o.gateway, o.builder, startupScanSignals.semanticBootstrapReady)
	})
	if passivePromoter != nil && poller != nil {
		scope.Go(passivePromoter.Run)
	}
	return nil
}

// startStaticRevalidation starts the M5 revalidator of a synchronously
// admitted source once the active probe passed, as joined generation work.
func (o *ebusGenerationOwners) startStaticRevalidation(scope *ebusGenerationScope, genCfg ebusgateway.Config, source byte, startupScanSignals startupScanSignals) {
	scope.Go(func(runCtx context.Context) {
		select {
		case <-runCtx.Done():
		case <-startupScanSignals.activeProbePassed:
			runRuntimeStateRevalidatorLoop(runCtx, o.runtimeStateMgr, o.gateway, genCfg, source, 0)
		case <-startupScanSignals.admissionFailed:
		}
	})
}

// startAdmissionFollowUp finalizes or withdraws the selected source when the
// active probe reports, and opens the semantic barrier, as joined generation
// work.
func (o *ebusGenerationOwners) startAdmissionFollowUp(scope *ebusGenerationScope, genCfg ebusgateway.Config, selection *protocol.SourceAddressSelection, startupScanSignals startupScanSignals) {
	scope.Go(func(runCtx context.Context) {
		select {
		case <-runCtx.Done():
			return
		case <-startupScanSignals.activeProbePassed:
			if selection != nil && !o.finalizeSelectedSource(runCtx, scope, genCfg, selection) {
				return
			}
		case <-startupScanSignals.admissionFailed:
			if selection != nil {
				if !o.retained.clearAdmittedSource(scope) {
					return
				}
				o.artifactBuilder.SetDegraded("active_probe_failed")
				o.metrics.MarkDegraded(time.Now())
				recordBusAdmissionTransitionWithStabilityRefresh(runCtx, o.busObservability, "degraded", selection.Source, selection.Companion, "active_probe_failed")
			}
			if shouldCloseSemanticBarrier(o.admissionPath, o.overrideSet, selection != nil) {
				o.retained.closeSemanticBarrier()
			}
			return
		}
		if shouldCloseSemanticBarrier(o.admissionPath, o.overrideSet, selection != nil) {
			o.retained.closeSemanticBarrier()
		}
	})
}

// finalizeSelectedSource installs the selected source after the active probe
// passed and starts its revalidator. It reports false when the generation was
// withdrawn and installed nothing.
func (o *ebusGenerationOwners) finalizeSelectedSource(runCtx context.Context, scope *ebusGenerationScope, genCfg ebusgateway.Config, selection *protocol.SourceAddressSelection) bool {
	if !o.retained.installAdmittedSource(scope, selection.Source) {
		return false
	}
	o.artifactBuilder.SetSourceSelectionActive()
	o.metrics.MarkActive()
	recordBusAdmissionTransitionWithStabilityRefresh(runCtx, o.busObservability, "active", selection.Source, selection.Companion, "active_probe_passed")
	// Admission finalized asynchronously: bind the inserter now that
	// AdmittedSource() returns the selected source.
	o.retained.subscribeAddressTableInserter()
	selectionMethod := runtimestate.SelectionMethodWarmup
	if o.overrideSet {
		selectionMethod = runtimestate.SelectionMethodExplicitValidateOnly
	}
	recordAdmittedSourceInRuntimeState(o.runtimeStateMgr, selection.Source, selection.Companion, selectionMethod, true)
	scope.Go(func(revalidateCtx context.Context) {
		runRuntimeStateRevalidatorLoop(revalidateCtx, o.runtimeStateMgr, o.gateway, genCfg, selection.Source, 0)
	})
	return true
}

// runPassiveObserveFirst starts the process-scoped passive reconstructor
// after the generation's first scan pass and attaches the observe-first
// consumers (start-critical).
func (o *ebusGenerationOwners) runPassiveObserveFirst(genCtx context.Context, scope *ebusGenerationScope) error {
	state := o.startupStateFor(scope)
	o.startupStateMu.Lock()
	genCfg, startupScanSignals, ready := state.cfg, state.signals, state.scanned
	o.startupStateMu.Unlock()
	if !ready || !shouldStartPassiveObserveFirst(genCfg) {
		return nil
	}
	if o.retained.currentReconstructor() == nil {
		waitForStartupScanFirstPass(genCtx, genCfg, startupScanSignals.firstPassDone)
		if genCtx.Err() != nil {
			return nil
		}
		started, err := startPassiveTransactionReconstructor(o.ctx, genCfg)
		if err != nil {
			return err
		}
		// Only a generation that is still live publishes the handle; a
		// stopped or replaced one closes it and ends.
		if !o.retained.publishReconstructor(scope, started) {
			return nil
		}
	}
	return o.retained.attachPassiveObserveFirst()
}
