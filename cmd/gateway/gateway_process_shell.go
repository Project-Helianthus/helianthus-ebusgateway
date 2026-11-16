package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/releasecheck"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimestate"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semhub"
)

// gatewayProcessShellStepObserver receives the name of every process-shell
// step as it happens: "acquire:<name>" before a resource is acquired,
// "register:<name>" when its release joins the shutdown sequence and
// "release:<name>" before that release runs. It is nil in production; tests
// set it to assert the order.
var gatewayProcessShellStepObserver func(step string)

func observeGatewayProcessShellStep(phase, name string) {
	if observer := gatewayProcessShellStepObserver; observer != nil {
		observer(phase + ":" + name)
	}
}

// gatewayShutdownStep is one named release. It receives the process result
// accumulated so far and returns the result to pass on.
type gatewayShutdownStep struct {
	name    string
	release func(result error) error
}

// gatewayShutdownSequence holds the releases of the process in registration
// order and runs them last in, first out, with the semantics of deferred
// statements: a release that panics does not prevent the earlier releases from
// running.
type gatewayShutdownSequence struct {
	steps []gatewayShutdownStep
}

func (sequence *gatewayShutdownSequence) register(name string, release func(result error) error) {
	observeGatewayProcessShellStep("register", name)
	sequence.steps = append(sequence.steps, gatewayShutdownStep{name: name, release: release})
}

// run releases every registered step, last registered first, and returns the
// result after the last release. A sequence runs once.
func (sequence *gatewayShutdownSequence) run(result error) error {
	steps := sequence.steps
	sequence.steps = nil
	runGatewayShutdownSteps(steps, &result)
	return result
}

func runGatewayShutdownSteps(steps []gatewayShutdownStep, result *error) {
	if len(steps) == 0 {
		return
	}
	// The earlier steps run in a deferred call so that they still run when
	// this step panics, exactly as stacked deferred statements would.
	defer runGatewayShutdownSteps(steps[:len(steps)-1], result)
	step := steps[len(steps)-1]
	observeGatewayProcessShellStep("release", step.name)
	*result = step.release(*result)
}

// gatewayProcessShell owns the process resources that the gateway acquires
// before the HTTP control plane and the eBUS generation owners, and the
// shutdown sequence that releases them.
//
// Contract:
//   - Required collaborators: the process context and the gateway
//     configuration, which the shell updates in place while it prepares the
//     run configuration and the eBUS driver. newGatewayProcessShell rejects a
//     missing collaborator before any side effect.
//   - Startup: acquireResources acquires, in this order, the run
//     configuration, the semantic hub preflight, the release checker, the
//     synchronized evidence sidecar, the Portal contribution registry, the
//     runtime state, the Portal configuration, the stale eeBUS operator socket
//     clean-up, the managed sidecars, the eBUS driver, the runtime control and
//     the retained EVSE owner. acquireEEBusSurfaces then creates the eeBUS
//     adapter and admin handler and configures the synchronized evidence
//     sidecar. The first failure stops the acquisition and is returned with
//     the same text as before; the releases registered until then still run.
//   - Defaults: an absent synchronized evidence sidecar or retained EVSE owner
//     registers no release; an eBUS control service that is unavailable or a
//     driver that is not running is logged and is not a process failure.
//   - Shutdown: every release joins one named sequence in acquisition order
//     and runs last in, first out when the caller runs the sequence. A release
//     either joins its error into the process result or logs it, as before.
//     The redaction of file-sourced Modbus endpoints is registered first, so
//     it applies last, to the complete result.
//   - Failure isolation: a release that fails or panics does not prevent the
//     releases registered before it from running.
type gatewayProcessShell struct {
	ctx      context.Context
	cfg      *ebusgateway.Config
	shutdown gatewayShutdownSequence

	buildInfo           gatewayBuildInfo
	semanticHub         *semhub.Hub
	releaseChecker      *releasecheck.Checker
	evidenceRuntime     *synchronizedEvidenceRuntime
	portalContributions *gatewayPortalCatalogContributions
	runtimeStateMgr     *runtimestate.Manager
	runtimeState        *runtimestate.State
	portalConfig        *gatewayPortalConfigRuntime
	sidecars            *managedSidecars
	bindings            *managedSidecarBindings
	ebusDriver          *ebusDriverController
	adapterClassifier   activeTxnClassifier
	control             *gatewayRuntimeControl
	teslaHSCRetained    *teslaHSCRetainedOwner
	eebusAdapter        *eebusRuntimeAdapter
	eebusAdminHandler   http.HandlerFunc
}

// newGatewayProcessShell validates the collaborators and registers the
// endpoint redaction as the first release. It acquires nothing.
func newGatewayProcessShell(ctx context.Context, cfg *ebusgateway.Config) (*gatewayProcessShell, error) {
	if ctx == nil {
		return nil, errors.New("gateway process shell: process context is required")
	}
	if cfg == nil {
		return nil, errors.New("gateway process shell: gateway configuration is required")
	}
	shell := &gatewayProcessShell{ctx: ctx, cfg: cfg}
	// Registered first, so it runs last: no error leaving the process carries
	// an endpoint of the configuration v2 document.
	shell.onShutdown("endpoint redaction", func(result error) error {
		return redactFileSourcedModbusEndpoints(result, modbusTCPDocumentEndpoints(cfg.ModbusTCPConfig)...)
	})
	return shell, nil
}

// onShutdown registers a named release that may change the process result.
func (shell *gatewayProcessShell) onShutdown(name string, release func(result error) error) {
	shell.shutdown.register(name, release)
}

// onShutdownFunc registers a named release that leaves the process result
// unchanged. The function value is taken at registration, as a deferred call
// takes its receiver.
func (shell *gatewayProcessShell) onShutdownFunc(name string, release func()) {
	shell.shutdown.register(name, func(result error) error {
		release()
		return result
	})
}

// runShutdown runs the shutdown sequence and returns the final process result.
func (shell *gatewayProcessShell) runShutdown(result error) error {
	return shell.shutdown.run(result)
}

func (shell *gatewayProcessShell) acquire(name string) {
	observeGatewayProcessShellStep("acquire", name)
}

// acquireResources acquires the shell resources up to the retained EVSE owner.
func (shell *gatewayProcessShell) acquireResources() error {
	if err := shell.acquireRunConfiguration(); err != nil {
		return err
	}
	if err := shell.acquireEvidenceAndState(); err != nil {
		return err
	}
	if err := shell.acquireDriverAndControl(); err != nil {
		return err
	}
	return shell.acquireRetainedEVSEOwner()
}

func (shell *gatewayProcessShell) acquireRunConfiguration() error {
	shell.acquire("run configuration")
	resolvedBuildInfo, err := prepareGatewayRunConfig(shell.cfg)
	if err != nil {
		return err
	}
	shell.buildInfo = resolvedBuildInfo
	// Every static semantic identity is declared before any runtime starts; a
	// cross-family collision stops the process here, before any side effect.
	shell.acquire("semantic hub preflight")
	semanticHub, err := preflightSemanticHub(*shell.cfg)
	if err != nil {
		return err
	}
	shell.semanticHub = semanticHub
	// Release discovery is a public, read-only background task. Both status
	// APIs receive the same cache-only reader; their request handlers do not
	// trigger or wait for this network operation.
	shell.acquire("release checker")
	shell.releaseChecker = releasecheck.New(releasecheck.Options{CurrentVersion: resolvedBuildInfo.ReleaseVersion})
	if shell.cfg.HTTPAddr != "" {
		startGatewayReleaseChecker(shell.releaseChecker, shell.ctx)
	}
	return nil
}

func (shell *gatewayProcessShell) acquireEvidenceAndState() error {
	shell.acquire("synchronized evidence sidecar")
	evidenceRuntime, err := openSynchronizedEvidenceRuntime(shell.cfg.EvidenceRecorderConfig)
	if err != nil {
		return fmt.Errorf("synchronized evidence sidecar: %w", err)
	}
	shell.evidenceRuntime = evidenceRuntime
	if evidenceRuntime != nil {
		shell.onShutdown("synchronized evidence sidecar", func(result error) error {
			if err := evidenceRuntime.Close(); err != nil {
				result = errors.Join(result, fmt.Errorf("shutdown synchronized evidence sidecar: %w", err))
			}
			return result
		})
	}
	shell.acquire("portal contribution registry")
	portalContributions, err := newGatewayPortalCatalogContributions()
	if err != nil {
		return fmt.Errorf("create portal contribution registry: %w", err)
	}
	shell.portalContributions = portalContributions
	// Initialize the runtime-state Manager early so the cached
	// ebus.self.last_admitted_source can be passed as a hint to the source
	// address selector, and the Manager is available for post-admission
	// UpdateSelf and address-table-revalidate write-back. Errors during Load
	// are tolerated: Manager.Load returns an empty state on missing/corrupt
	// and the gateway continues without a hint.
	shell.acquire("runtime state")
	runtimeStateMgr, runtimeState := initRuntimeStateManager(shell.ctx, *shell.cfg, shell.buildInfo)
	shell.runtimeStateMgr, shell.runtimeState = runtimeStateMgr, runtimeState
	shell.onShutdown("runtime state", func(result error) error {
		stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := runtimeStateMgr.Stop(stopCtx); err != nil {
			log.Printf("runtime_state stop: %v", err)
		}
		return result
	})
	// Load the Portal desired record before any driver is registered, so its
	// accepted driver selections become configured eligibility. A record that
	// does not load keeps the shell up and withholds record-controlled drivers.
	shell.acquire("portal configuration")
	portalConfig := openGatewayPortalConfigRuntime(*shell.cfg)
	shell.portalConfig = portalConfig
	shell.onShutdown("portal configuration", func(result error) error {
		if err := portalConfig.Close(); err != nil {
			log.Printf("portal configuration close: %v", err)
		}
		return result
	})
	return nil
}

func (shell *gatewayProcessShell) acquireDriverAndControl() error {
	// Earlier builds created the eeBUS operator MCP socket inside the eeBUS
	// state root, whose store rejects any entry it does not own. Clear that
	// one stale socket before any eeBUS resource opens.
	shell.acquire("stale eeBUS operator socket clean-up")
	clearLegacyEEBusOperatorSocket(eebusEnabledStateRoot(*shell.cfg))
	// Construct the protocol-neutral manager and stable eBUS provider before
	// opening any eBUS resource. Driver-local construction failure is retained
	// as categorical state and never promoted to process failure.
	shell.acquire("managed sidecars")
	sidecars := prepareManagedSidecars(shell.ctx, *shell.cfg, shell.semanticHub, shell.runtimeStateMgr.State().Meta.InstanceGUID, shell.portalContributions, shell.portalConfig)
	shell.sidecars = sidecars
	shell.bindings = &managedSidecarBindings{sidecars: sidecars}
	shell.acquire("eBUS driver")
	ebusDriver, err := prepareGatewayEBusDriver(shell.cfg, sidecars.driverConfigs()...)
	if err != nil {
		return err
	}
	shell.ebusDriver = ebusDriver
	shell.onShutdown("eBUS driver", func(result error) error {
		stopCtx, cancel := context.WithTimeout(context.Background(), 2*defaultEBusDriverDrainTimeout+time.Second)
		defer cancel()
		if err := errors.Join(ebusDriver.Shutdown(stopCtx), sidecars.shutdownError()); err != nil {
			result = errors.Join(result, fmt.Errorf("shutdown eBUS driver: %w", err))
		}
		return result
	})
	shell.adapterClassifier = ebusDriver.classifier
	// Compose the frozen registration and the durable control service before
	// eBUS reconciliation. The service is the single owner of eBUS startup: it
	// restores D5 material, prepares the inert source and then reconciles
	// durable desired state through the manager-to-hub bridge. Nothing else
	// starts the DriverManager, and no failure here is a process error.
	shell.acquire("runtime control")
	control := startGatewayRuntimeControl(shell.ctx, gatewayRuntimeControlOptionsFor(gatewayRuntimeControlOptions{
		Sidecars:     sidecars,
		Driver:       ebusDriver,
		Hub:          shell.semanticHub,
		Store:        shell.runtimeStateMgr,
		InstanceGUID: shell.runtimeStateMgr.State().Meta.InstanceGUID,
		PortalConfig: shell.portalConfig,
	}))
	shell.control = control
	gatewayRuntimeControlStarted(control)
	shell.onShutdown("runtime control", func(result error) error {
		closeCtx, cancel := context.WithTimeout(context.Background(), defaultEBusGenerationJoinTimeout+defaultEBusControlCleanupTimeout)
		defer cancel()
		if err := control.CloseAdmission(closeCtx); err != nil {
			log.Printf("runtime control close: %v", err)
		}
		if err := control.CloseNative(closeCtx); err != nil {
			log.Printf("runtime control native close: %v", err)
		}
		return result
	})
	if service := control.Service(); service == nil {
		log.Printf("eBUS control service unavailable; continuing without eBUS")
	} else if view, err := service.Get(primaryEBusDriverID); err == nil && view.ObservedState != "RUNNING" {
		log.Printf("eBUS driver not running at startup; continuing desired=%s state=%s reason=%s", view.DesiredState, view.ObservedState, view.Reason)
	}
	return nil
}

func (shell *gatewayProcessShell) acquireRetainedEVSEOwner() error {
	shell.acquire("retained EVSE owner")
	teslaHSCRetained, err := startTeslaHSCRetainedOwner(shell.cfg.ModbusTCPConfig.TeslaGen3HSC, shell.semanticHub)
	if err != nil {
		return fmt.Errorf("tesla HSC retained owner: %w", err)
	}
	if teslaHSCRetained == nil {
		return nil
	}
	if err := attachGatewayPortalEVSEContribution(teslaHSCRetained, shell.portalContributions); err != nil {
		_ = teslaHSCRetained.Close()
		return fmt.Errorf("publish EVSE portal contribution: %w", err)
	}
	shell.teslaHSCRetained = teslaHSCRetained
	shell.onShutdown("retained EVSE owner", func(result error) error {
		if err := teslaHSCRetained.Close(); err != nil {
			result = errors.Join(result, fmt.Errorf("shutdown Tesla HSC retained owner: %w", err))
		}
		return result
	})
	return nil
}

// acquireEEBusSurfaces creates the eeBUS adapter and admin handler over the
// managed bindings and configures the synchronized evidence sidecar. It runs
// after the caller has composed the Portal catalog source and the M2M runtime.
func (shell *gatewayProcessShell) acquireEEBusSurfaces() error {
	shell.acquire("eeBUS adapter")
	shell.eebusAdapter = &eebusRuntimeAdapter{managed: shell.bindings}
	shell.acquire("eeBUS admin handler")
	shell.eebusAdminHandler = http.HandlerFunc(shell.bindings.serveEEBusAdmin)
	shell.acquire("evidence configuration")
	if shell.evidenceRuntime != nil {
		if err := shell.evidenceRuntime.Configure(shell.bindings.eebusEvidenceCapture(), shell.buildInfo.EvidenceVersion(), newSynchronizedEvidenceClock(), synchronizedEvidenceEntropy); err != nil {
			return fmt.Errorf("configure synchronized evidence sidecar: %w", err)
		}
	}
	return nil
}
