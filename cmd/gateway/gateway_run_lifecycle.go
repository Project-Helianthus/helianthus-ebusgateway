package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/releasecheck"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimestate"
)

// startGatewayReleaseChecker starts the cache-only release discovery task.
var startGatewayReleaseChecker = func(checker *releasecheck.Checker, ctx context.Context) {
	checker.Start(ctx)
}

// runGatewayLifecycle is the entry of the gateway process. It acquires the
// process shell, composes the API, creates the retained eBUS consumers and the
// eBUS generation owners, binds the generation tasks to the runtime control,
// waits for the process context and runs the shutdown sequence. The contracts
// of these owners and the complete shutdown sequence are documented in
// docs/architecture/gateway-composition-owners.md.
func runGatewayLifecycle(ctx context.Context, cfg ebusgateway.Config) (result error) {
	shell, err := newGatewayProcessShell(ctx, &cfg)
	if err != nil {
		return err
	}
	// Every release of this function joins the shell's shutdown sequence in
	// acquisition order; the sequence runs them last in, first out and applies
	// the endpoint redaction last.
	defer func() {
		result = shell.runShutdown(result)
	}()
	if err := shell.acquireResources(); err != nil {
		return err
	}
	portalContributions, bindings, teslaHSCRetained := shell.portalContributions, shell.bindings, shell.teslaHSCRetained
	portalCatalogSource := newGatewayPortalCatalogSource(nil, nil, teslaHSCRetained, portalContributions, bindings)
	m2mRuntime, err := newProfileM2MRuntime(cfg, teslaHSCRetained, portalCatalogSource, bindings)
	if err != nil {
		return fmt.Errorf("M2M GraphQL sidecar: %w", err)
	}
	if m2mRuntime != nil {
		shell.onShutdown("M2M sidecar", func(result error) error {
			if err := m2mRuntime.Close(); err != nil {
				result = errors.Join(result, fmt.Errorf("shutdown M2M GraphQL sidecar: %w", err))
			}
			return result
		})
	}
	if err := shell.acquireEEBusSurfaces(); err != nil {
		return err
	}

	// The API composition builds the gateway, the GraphQL builder and the
	// semantic runtime with their late-bound eBUS surfaces before any eBUS
	// generation runs.
	api, err := newGatewayAPIComposition(ctx, shell, &cfg, portalCatalogSource)
	if err != nil {
		return err
	}
	if err := api.compose(); err != nil {
		return err
	}
	// The consumers that live across eBUS generations have one owner; the
	// generation tasks below reach them only through its methods.
	retainedOptions := api.retainedConsumerOptions()
	retainedOptions.LeafCaptureStateRoot = cfg.EEBusConfig.StateRoot
	retained, err := newRetainedEBusConsumers(ctx, retainedOptions)
	if err != nil {
		return err
	}
	// The control plane serves through stable fail-closed bindings before any
	// post-start eBUS step; each of those steps runs as a task of the admitted
	// generation, once per generation, joined before any successor.
	if err := api.serve(retained); err != nil {
		return err
	}
	// The process-level barrier opens on process exit even when no
	// generation ever reported an admission outcome.
	retained.openSemanticBarrierOnProcessExit()

	// The eBUS generation owners hold the generation-owned post-start tasks;
	// every effect they install goes through the generation scope, and every
	// goroutine they start is joined with the generation.
	control := shell.control
	generationOwners, err := newEBusGenerationOwners(ctx, ebusGenerationOwnerOptions{
		Config:              cfg,
		Selection:           api.startupSelection,
		RuntimeStateManager: shell.runtimeStateMgr,
		RuntimeState:        shell.runtimeState,
		Gateway:             api.gateway,
		Builder:             api.builder,
		AdapterClassifier:   shell.adapterClassifier,
		Control:             control,
		Retained:            retained,
		BusObservability:    api.busObservability,
	})
	if err != nil {
		return err
	}
	generationTasks, err := newEBusGenerationTaskRunner(generationOwners.taskConfig())
	if err != nil {
		return fmt.Errorf("ebus generation tasks: %w", err)
	}
	// The generation tasks are fenced and joined before the shell and the
	// process consumers are torn down (this release runs first).
	shell.onShutdown("generation tasks", func(result error) error {
		closeCtx, cancel := context.WithTimeout(context.Background(), defaultEBusGenerationJoinTimeout+time.Second)
		defer cancel()
		if err := control.CloseAdmission(closeCtx); err != nil {
			log.Printf("runtime control close: %v", err)
		}
		return result
	})
	control.BindTasks(ctx, generationTasks, retained.withdrawAdmittedSource)
	<-ctx.Done()
	return nil
}

// runRuntimeStateRevalidatorLoop is the generation-owned form of the M5
// revalidator: the same native revalidation cycle, run in the caller's
// goroutine so the generation task runner joins it. The first cycle runs
// immediately; later cycles follow the interval until ctx ends.
func runRuntimeStateRevalidatorLoop(
	ctx context.Context,
	mgr *runtimestate.Manager,
	gw *ebusgateway.Gateway,
	cfg ebusgateway.Config,
	admittedSource byte,
	interval time.Duration,
) {
	if interval <= 0 {
		interval = 15 * time.Minute
	}
	rotation := newRevalidationRotation()
	blocklist := globalEvictionBlocklist
	revalidateRuntimeStateMembers(ctx, mgr, gw, cfg, admittedSource, rotation, blocklist)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			revalidateRuntimeStateMembers(ctx, mgr, gw, cfg, admittedSource, rotation, blocklist)
		}
	}
}

// publishManagedTransportRetirement records final statuses after the managed
// transport observers have joined, before HTTP admission and observability close.
func publishManagedTransportRetirement(store *ebusgateway.BusObservabilityStore, cfg ebusgateway.Config) {
	if store == nil {
		return
	}
	if cfg.ModbusTCPConfig.Enabled {
		store.SetTransportRuntimeStatus(retiredModbusRuntimeTransportStatus())
	}
	if cfg.EEBusConfig.Enabled {
		// Observers have joined and this store is about to close. A terminal
		// revision supersedes every status published during its lifetime.
		store.SetTransportRuntimeStatus(eebusRuntimeTransportStatus(eebusRuntimeLifecycleSnapshot{State: eebusLifecycleStopped, Revision: ^uint64(0)}))
	}
}
