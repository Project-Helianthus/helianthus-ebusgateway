package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semhub"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
)

// errRetainedEBusConsumersWiring reports a required collaborator that the
// lifecycle shell did not supply.
var errRetainedEBusConsumersWiring = errors.New("retained eBUS consumers: missing required collaborator")

// retainedEBusConsumerOptions are the collaborators of the retained eBUS
// consumers. Bus observability and the deduplicator are optional; every other
// collaborator is required.
type retainedEBusConsumerOptions struct {
	// Config is the validated process configuration. Each generation passes
	// its own admitted configuration to EnsureProcessSurface.
	Config ebusgateway.Config
	// SourceSelectionAdmit reports that admission waits for source
	// selection; with startup scanning it creates the semantic barrier.
	SourceSelectionAdmit bool
	// LeafCaptureStateRoot is the state root of the leaf-promotion capture.
	LeafCaptureStateRoot string

	Gateway              *ebusgateway.Gateway
	Builder              *graphql.Builder
	SemanticRuntime      *graphql.SemanticRuntime
	SemanticHub          *semhub.Hub
	Hub                  *graphql.BroadcastHub
	Control              *gatewayRuntimeControl
	AddressTableInserter *ebusgateway.AddressTableInserter
	EEBusAdapter         *eebusRuntimeAdapter
	// AdmittedSource is the live managed admitted-source authority.
	AdmittedSource func() (byte, bool)

	ScheduleWriter     *lateEBusScheduleWriter
	ConfigWriter       *lateEBusConfigWriter
	WatchProvider      *lateEBusWatchSummaryProvider
	GraphQLWriter      *lateEBusGraphQLWriter
	GraphQLWatchSource *lateEBusGraphQLWatchSummaryProvider

	BusObservability *ebusgateway.BusObservabilityStore
	Deduplicator     *ebusgateway.ActivePassiveDeduplicator
}

// retainedEBusConsumers owns the eBUS consumers that live across eBUS
// generations: the semantic poller and its late-bound writers, the passive
// reconstructor and broadcast listener, the passive discovery promoter, the
// semantic startup barrier, the address-table subscription and the
// process-level admitted source.
//
// Collaborators: the gateway, GraphQL builder, semantic runtime, semantic
// publication hub, broadcast hub, runtime control, address-table inserter,
// eeBUS runtime adapter, live admitted-source authority and the five late-bound
// writers and watch providers are required; the constructor rejects a missing
// one. Bus observability and the deduplicator are optional.
//
// Context: every consumer runs on the process context passed to the
// constructor, never on a generation context, so a stopped interval does not
// tear them down. Generation tasks reach the consumers only through these
// methods; publication of a generation-created handle and of the admitted
// source goes through the generation fence.
//
// Failure isolation: EnsureProcessSurface records readiness only after every
// fallible step succeeded. A failed step is retried by the next admitted
// generation, and steps that already completed are not repeated.
//
// Teardown: the lifecycle shell closes these consumers through
// gatewayConsumerShutdown, which reads the listener and reconstructor through
// surface once the generation tasks have joined.
type retainedEBusConsumers struct {
	ctx                  context.Context
	cfg                  ebusgateway.Config
	leafCaptureStateRoot string
	observeFirstFlags    ebusgateway.ObserveFirstFeatureFlags

	gateway              *ebusgateway.Gateway
	builder              *graphql.Builder
	semanticRuntime      *graphql.SemanticRuntime
	semanticHub          *semhub.Hub
	hub                  *graphql.BroadcastHub
	control              *gatewayRuntimeControl
	addressTableInserter *ebusgateway.AddressTableInserter
	eebusAdapter         *eebusRuntimeAdapter
	liveAdmittedSource   func() (byte, bool)

	lateScheduleWriter       *lateEBusScheduleWriter
	lateConfigWriter         *lateEBusConfigWriter
	lateWatchProvider        *lateEBusWatchSummaryProvider
	lateGraphQLWriter        *lateEBusGraphQLWriter
	lateGraphQLWatchProvider *lateEBusGraphQLWatchSummaryProvider

	busObservability *ebusgateway.BusObservabilityStore
	deduplicator     *ebusgateway.ActivePassiveDeduplicator

	// subscribeInserter subscribes the address-table inserter to a
	// reconstructor.
	subscribeInserter func(*ebusgateway.PassiveTransactionReconstructor) (*ebusgateway.PassiveClassifiedSubscription, error)

	// surfaceMu guards the process-scoped passive consumers, which the
	// first admitted generation's task creates and teardown closes.
	surfaceMu     sync.Mutex
	listener      *ebusgateway.BroadcastListener
	reconstructor *ebusgateway.PassiveTransactionReconstructor

	surfaceReady         bool
	surfacePollerStarted bool
	surfaceCaptureDone   bool
	surfaceShadowDone    bool
	semanticPoller       *vaillantSemanticPoller
	promoter             *ebusgateway.PassiveDiscoveryPromoter
	probeReady           atomic.Bool
	passiveAttached      bool

	semanticBarrier     chan struct{}
	semanticBarrierOnce sync.Once

	insertSubscribed    atomic.Bool
	insertSubscribeOnce sync.Once

	admittedSourceMu sync.Mutex
}

// newRetainedEBusConsumers validates the collaborators and creates the
// retained consumers. It starts nothing.
func newRetainedEBusConsumers(ctx context.Context, opts retainedEBusConsumerOptions) (*retainedEBusConsumers, error) {
	required := []struct {
		name    string
		missing bool
	}{
		{"process context", ctx == nil},
		{"gateway", opts.Gateway == nil},
		{"builder", opts.Builder == nil},
		{"semantic runtime", opts.SemanticRuntime == nil},
		{"semantic publication hub", opts.SemanticHub == nil},
		{"broadcast hub", opts.Hub == nil},
		{"runtime control", opts.Control == nil},
		{"address-table inserter", opts.AddressTableInserter == nil},
		{"eeBUS runtime adapter", opts.EEBusAdapter == nil},
		{"admitted source", opts.AdmittedSource == nil},
		{"schedule writer", opts.ScheduleWriter == nil},
		{"config writer", opts.ConfigWriter == nil},
		{"watch provider", opts.WatchProvider == nil},
		{"GraphQL writer", opts.GraphQLWriter == nil},
		{"GraphQL watch provider", opts.GraphQLWatchSource == nil},
	}
	for _, collaborator := range required {
		if collaborator.missing {
			return nil, fmt.Errorf("%w: %s", errRetainedEBusConsumersWiring, collaborator.name)
		}
	}
	cfg := opts.Config
	c := &retainedEBusConsumers{
		ctx:                      ctx,
		cfg:                      cfg,
		leafCaptureStateRoot:     opts.LeafCaptureStateRoot,
		observeFirstFlags:        ebusgateway.NormalizeObserveFirstFeatureFlags(cfg.ObserveFirstEnabled, cfg.PassiveStateDirectApply, cfg.PassiveConfigDirectApply, cfg.ExternalWritePolicy),
		gateway:                  opts.Gateway,
		builder:                  opts.Builder,
		semanticRuntime:          opts.SemanticRuntime,
		semanticHub:              opts.SemanticHub,
		hub:                      opts.Hub,
		control:                  opts.Control,
		addressTableInserter:     opts.AddressTableInserter,
		eebusAdapter:             opts.EEBusAdapter,
		liveAdmittedSource:       opts.AdmittedSource,
		lateScheduleWriter:       opts.ScheduleWriter,
		lateConfigWriter:         opts.ConfigWriter,
		lateWatchProvider:        opts.WatchProvider,
		lateGraphQLWriter:        opts.GraphQLWriter,
		lateGraphQLWatchProvider: opts.GraphQLWatchSource,
		busObservability:         opts.BusObservability,
		deduplicator:             opts.Deduplicator,
	}
	c.subscribeInserter = subscribeAddressTableInserterTo
	if cfg.ScanOnStart && (shouldStartPassiveObserveFirst(cfg) || opts.SourceSelectionAdmit) {
		c.semanticBarrier = make(chan struct{})
	}
	return c, nil
}

func subscribeAddressTableInserterTo(active *ebusgateway.PassiveTransactionReconstructor) (*ebusgateway.PassiveClassifiedSubscription, error) {
	return active.Subscribe(
		"address_table_inserter",
		ebusgateway.PassiveSubscriberNonCritical,
		0,
	)
}

// surface snapshots the listener and reconstructor for consumer shutdown.
func (c *retainedEBusConsumers) surface() gatewayConsumerSurface {
	c.surfaceMu.Lock()
	defer c.surfaceMu.Unlock()
	var surface gatewayConsumerSurface
	if c.listener != nil {
		surface.listener = c.listener
	}
	if c.reconstructor != nil {
		surface.reconstructor = c.reconstructor
	}
	return surface
}

// hasSemanticBarrier reports whether semantic bootstrap waits for the
// startup admission outcome.
func (c *retainedEBusConsumers) hasSemanticBarrier() bool {
	return c.semanticBarrier != nil
}

// closeSemanticBarrier opens the semantic barrier at most once.
func (c *retainedEBusConsumers) closeSemanticBarrier() {
	if c.semanticBarrier != nil {
		c.semanticBarrierOnce.Do(func() { close(c.semanticBarrier) })
	}
}

// openSemanticBarrierOnProcessExit opens the process-level barrier on process
// exit even when no generation ever reported an admission outcome.
func (c *retainedEBusConsumers) openSemanticBarrierOnProcessExit() {
	go func() {
		<-c.ctx.Done()
		c.closeSemanticBarrier()
	}()
}

func (c *retainedEBusConsumers) currentReconstructor() *ebusgateway.PassiveTransactionReconstructor {
	c.surfaceMu.Lock()
	defer c.surfaceMu.Unlock()
	return c.reconstructor
}

// pollerAndPromoter returns the retained semantic poller and passive
// discovery promoter.
func (c *retainedEBusConsumers) pollerAndPromoter() (*vaillantSemanticPoller, *ebusgateway.PassiveDiscoveryPromoter) {
	c.surfaceMu.Lock()
	defer c.surfaceMu.Unlock()
	return c.semanticPoller, c.promoter
}

// publishReconstructor publishes a reconstructor that a generation created.
// Only a generation that is still live publishes the handle; a stopped or
// replaced one closes it and reports false.
func (c *retainedEBusConsumers) publishReconstructor(scope *ebusGenerationScope, started *ebusgateway.PassiveTransactionReconstructor) bool {
	if !scope.Commit(func() {
		c.surfaceMu.Lock()
		c.reconstructor = started
		c.surfaceMu.Unlock()
	}) {
		if cerr := started.Close(); cerr != nil {
			log.Printf("passive reconstructor close after withdrawn generation: %v", cerr)
		}
		return false
	}
	return true
}

// subscribeAddressTableInserter binds the AddressTableInserter to the
// active reconstructor at most once per process. Subscription is gated
// on the admitted source being finalized (builder.AdmittedMutationSource
// returns ok=true) so the inserter's self-source filter sees the real
// admitted address — never 0 — and cannot mistakenly insert the
// gateway's own active-probe targets/companions as passive_observed.
// Subscription failure is non-fatal; the inserter is a non-critical
// observer.
func (c *retainedEBusConsumers) subscribeAddressTableInserter() {
	active := c.currentReconstructor()
	if c.insertSubscribed.Load() || active == nil {
		return
	}
	if _, ok := c.builder.AdmittedMutationSource(); !ok {
		return
	}
	c.insertSubscribeOnce.Do(func() {
		sub, err := c.subscribeInserter(active)
		if err != nil {
			log.Printf("address_table_inserter subscribe failed: %v", err)
			return
		}
		c.insertSubscribed.Store(true)
		go func() {
			for ev := range sub.Events() {
				c.addressTableInserter.OnPassiveClassifiedEvent(ev)
			}
		}()
	})
}

// attachPassiveObserveFirst attaches the observe-first consumers and the
// broadcast listener to the published reconstructor, once each.
func (c *retainedEBusConsumers) attachPassiveObserveFirst() error {
	active := c.currentReconstructor()
	if active == nil {
		return nil
	}
	c.surfaceMu.Lock()
	attached := c.passiveAttached
	c.surfaceMu.Unlock()
	if !attached {
		if c.busObservability != nil {
			if err := c.busObservability.AttachReconstructor(c.ctx, active); err != nil {
				return err
			}
		}
		if c.deduplicator != nil {
			if err := c.deduplicator.AttachReconstructor(c.ctx, active); err != nil {
				return err
			}
		}
		c.surfaceMu.Lock()
		c.passiveAttached = true
		c.surfaceMu.Unlock()
	}
	c.surfaceMu.Lock()
	needListener := c.listener == nil
	c.surfaceMu.Unlock()
	if needListener {
		started, err := startBroadcastListenerWithReconstructorFn(c.ctx, c.gateway.Router, active)
		if err != nil {
			return err
		}
		c.surfaceMu.Lock()
		c.listener = started
		c.surfaceMu.Unlock()
	}
	// Best-effort early bind. If admission isn't finalized yet
	// (sourceSelection path: waits on activeProbePassed), this
	// returns silently and the wire-up below catches the signal.
	c.subscribeAddressTableInserter()
	return nil
}

// installAdmittedSource and clearAdmittedSource serialize the
// process-level admitted source with generation admission: a fenced
// generation can never install it (scope.Commit), and a clear after the
// lifecycle withdrew admission rechecks that no successor installed one.
func (c *retainedEBusConsumers) installAdmittedSource(scope *ebusGenerationScope, source byte) bool {
	return scope.Commit(func() {
		c.admittedSourceMu.Lock()
		c.builder.SetAdmittedMutationSource(source)
		c.admittedSourceMu.Unlock()
	})
}

func (c *retainedEBusConsumers) clearAdmittedSource(scope *ebusGenerationScope) bool {
	return scope.Commit(func() {
		c.admittedSourceMu.Lock()
		c.builder.ClearAdmittedMutationSource()
		c.admittedSourceMu.Unlock()
	})
}

// withdrawAdmittedSource clears the admitted source when the lifecycle
// forces it or no longer admits eBUS.
func (c *retainedEBusConsumers) withdrawAdmittedSource(force bool) {
	c.admittedSourceMu.Lock()
	defer c.admittedSourceMu.Unlock()
	if force || !c.control.EBusAdmitted() {
		c.builder.ClearAdmittedMutationSource()
	}
}

// admittedSourceOrZero returns the admitted source, or 0 before admission.
func (c *retainedEBusConsumers) admittedSourceOrZero() byte {
	source, ok := c.builder.AdmittedMutationSource()
	if !ok {
		return 0
	}
	return source
}

// ensureProcessSurface creates the process-scoped eBUS consumers once,
// from the first admitted generation's validated configuration. They use
// the process context, so a stopped interval never tears them down.
func (c *retainedEBusConsumers) ensureProcessSurface(genCfg ebusgateway.Config) error {
	c.surfaceMu.Lock()
	ready, pollerStarted, poller := c.surfaceReady, c.surfacePollerStarted, c.semanticPoller
	c.surfaceMu.Unlock()
	c.rebindPollerSource(poller, genCfg)
	if ready {
		return nil
	}
	// Readiness is recorded only after every fallible step succeeded; a
	// failed step is retried by the next generation, and the stages that
	// already completed are not repeated.
	if !pollerStarted {
		poller = c.startPoller(genCfg)
		c.bindGraphQLWriters(poller)
		c.bindWatchObserver(poller)
	}
	c.installCapture(poller)
	if err := c.attachShadowLane(poller); err != nil {
		return err
	}
	c.createPromoter(poller)
	c.bindEnrichmentAndWriters(poller)
	c.recordReadiness()
	return nil
}

// rebindPollerSource rebinds the retained poller to the initiator and
// companion of the admitted generation. Every admitted generation calls it.
func (c *retainedEBusConsumers) rebindPollerSource(poller *vaillantSemanticPoller, genCfg ebusgateway.Config) {
	if poller == nil {
		return
	}
	poller.attachBoilerThermal(c.semanticHub, func() (*semhub.Handle, error) {
		if c.control.bridge == nil {
			return nil, errEBusControlNotPrepared
		}
		view := c.control.bridge.View()
		return c.control.bridge.PublicationHandle(view.Key)
	})
	resolved := resolveStartupScanSourceConfig(genCfg)
	poller.setSourceAndCompanion(resolved.ScanSource, resolved.StartupCompanionTarget)
}

// startPoller starts the retained semantic poller on the process context.
func (c *retainedEBusConsumers) startPoller(genCfg ebusgateway.Config) *vaillantSemanticPoller {
	semanticCfg := resolveStartupScanSourceConfig(genCfg)
	poller := startVaillantSemanticPollingFn(c.ctx, semanticCfg, c.gateway, c.semanticRuntime.Provider(), c.hub, c.semanticBarrier)
	c.surfaceMu.Lock()
	c.semanticPoller, c.surfacePollerStarted = poller, true
	c.surfaceMu.Unlock()
	c.rebindPollerSource(poller, genCfg)
	return poller
}

// bindGraphQLWriters binds the GraphQL writers and watch provider to the
// poller in the pass that started it.
func (c *retainedEBusConsumers) bindGraphQLWriters(poller *vaillantSemanticPoller) {
	if poller == nil {
		return
	}
	gatedGraphQLWriter := admittedGraphQLSemanticWriter{
		boiler:   poller,
		system:   poller,
		schedule: poller,
		admitted: c.liveAdmittedSource,
	}
	c.lateGraphQLWriter.Bind(gatedGraphQLWriter)
	c.lateGraphQLWatchProvider.Bind(newGraphQLWatchSummaryProvider(poller.shadow))
}

// bindWatchObserver is bound before any fallible step so a retried
// generation cannot leave the deduplicator as its own observer.
func (c *retainedEBusConsumers) bindWatchObserver(poller *vaillantSemanticPoller) {
	if poller != nil && poller.shadow != nil && c.deduplicator != nil {
		// Use the semantic shadow directly here. The broader runtime
		// observer can fall back to the deduplicator itself, which would
		// re-enter dedup locks while passive fingerprints are being built.
		c.deduplicator.SetWatchObserver(poller.shadow)
	}
}

// installCapture installs the shell capture once; its binding checks
// admission per call.
func (c *retainedEBusConsumers) installCapture(poller *vaillantSemanticPoller) {
	if poller == nil || c.surfaceCaptureDone {
		return
	}
	captureSource := newLeafPromotionLiveSource(
		poller,
		c.eebusAdapter,
		eebusMCPCommandRouter(c.eebusAdapter),
		c.liveAdmittedSource,
	)
	stateRoot := c.leafCaptureStateRoot
	c.eebusAdapter.SetLeafPromotionCapture(&lazyLeafPromotionCapture{
		construct: func() (mcp.LeafPromotionCapture, error) {
			return newLeafPromotionCaptureRuntime(stateRoot, captureSource)
		},
	})
	c.surfaceCaptureDone = true
}

// attachShadowLane attaches the passive shadow producer once when an
// observe-first direct-apply or external-write lane is enabled.
func (c *retainedEBusConsumers) attachShadowLane(poller *vaillantSemanticPoller) error {
	passiveShadowLaneEnabled := c.observeFirstFlags.PassiveStateDirectApply() ||
		c.observeFirstFlags.PassiveConfigDirectApply() ||
		c.observeFirstFlags.ExternalWritePolicy() != ebusgateway.ObserveFirstExternalWritePolicyRecordOnly
	if poller != nil && c.deduplicator != nil && c.observeFirstFlags.ObserveFirstEnabled() && passiveShadowLaneEnabled && !c.surfaceShadowDone {
		if err := attachPassiveShadowProducerFn(poller, c.ctx, c.deduplicator); err != nil {
			return err
		}
		c.surfaceShadowDone = true
	}
	return nil
}

// createPromoter creates the runtime passive-promotion pipeline:
// late-arriving devices (e.g. the regulator boots after the gateway)
// accumulate passive evidence in busObservability's EvidenceBuffer; the
// promoter confirms candidates with B524 coherency, registers them in the
// registry, refreshes router planes, and signals semantic discovery refresh.
// The promoter is process-scoped; its loop runs per generation.
func (c *retainedEBusConsumers) createPromoter(poller *vaillantSemanticPoller) {
	if c.busObservability == nil || c.busObservability.EvidenceBuffer() == nil || poller == nil {
		return
	}
	c.busObservability.SetAdmittedSourceProvider(c.admittedSourceOrZero)
	created, err := ebusgateway.NewPassiveDiscoveryPromoter(ebusgateway.PassiveDiscoveryPromoterOptions{
		Registry:          c.gateway.Registry,
		EvidenceBuffer:    c.busObservability.EvidenceBuffer(),
		ConfirmFn:         poller.ConfirmB524Coherent,
		SemanticRefreshFn: poller.EnqueueDiscoveryRefresh,
		RouterRefreshFn:   func() { _ = c.gateway.RefreshRouterPlanes() },
		AdmittedSourceFn:  c.admittedSourceOrZero,
	})
	if err != nil {
		log.Printf("passive_discovery_promoter init failed: %v", err)
		return
	}
	c.surfaceMu.Lock()
	c.promoter = created
	c.surfaceMu.Unlock()
}

// bindEnrichmentAndWriters binds the M6 enrichment trigger and P5
// per-address identity probe, gated on the startup admission barrier so no
// ScanDirected traffic races the startup directed scan; the post-barrier
// backfill catches up. It then binds the MCP writers and watch provider.
func (c *retainedEBusConsumers) bindEnrichmentAndWriters(poller *vaillantSemanticPoller) {
	if poller == nil {
		return
	}
	c.addressTableInserter.SetEnrichmentRefreshFn(poller.EnqueueDiscoveryRefresh)
	if c.semanticBarrier == nil {
		c.probeReady.Store(true)
	}
	c.addressTableInserter.SetEnrichmentIdentityProbeFn(func(addr byte) {
		if !c.probeReady.Load() {
			return
		}
		poller.EnqueueAddressIdentityProbe(addr)
	})
	if c.semanticBarrier != nil {
		go func() {
			select {
			case <-c.ctx.Done():
				return
			case <-c.semanticBarrier:
				c.probeReady.Store(true)
				c.addressTableInserter.BackfillUnidentifiedAddresses()
			}
		}()
	} else {
		c.addressTableInserter.BackfillUnidentifiedAddresses()
	}
	c.lateScheduleWriter.Bind(admittedMCPScheduleWriter{
		writer:   poller,
		admitted: c.liveAdmittedSource,
	})
	c.lateConfigWriter.Bind(admittedMCPConfigWriter{
		writer:   &mcpConfigWriterAdapter{poller: poller},
		admitted: c.liveAdmittedSource,
	})
	c.lateWatchProvider.Bind(newMCPWatchSummaryProvider(poller.shadow))
}

// recordReadiness records the surface as ready; it runs after every
// fallible step succeeded.
func (c *retainedEBusConsumers) recordReadiness() {
	if c.cfg.BroadcastListen && !shouldStartPassiveObserveFirst(c.cfg) {
		log.Printf("passive observe-first unavailable on transport=%s; continuing degraded", c.cfg.TransportConfig.Protocol)
	}
	c.surfaceMu.Lock()
	c.surfaceReady = true
	c.surfaceMu.Unlock()
}
