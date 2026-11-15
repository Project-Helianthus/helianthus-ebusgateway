package main

import (
	"context"
	"errors"
	"go/token"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
)

const retainedTestSource = byte(0x31)

// retainedTestOptions returns complete wiring. Only the gateway, builder,
// semantic runtime and broadcast hub are live; the other required
// collaborators are inert values that the exercised methods do not touch.
func retainedTestOptions(t *testing.T) retainedEBusConsumerOptions {
	t.Helper()
	gateway, err := ebusgateway.New(context.Background(), ebusgateway.Config{Transport: transport.NewLoopback()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = gateway.Close() })
	builder := graphql.NewBuilder(gateway.Registry, nil)
	hub := graphql.NewBroadcastHub(nil)
	return retainedEBusConsumerOptions{
		Config:               ebusgateway.DefaultConfig(),
		Gateway:              gateway,
		Builder:              builder,
		SemanticRuntime:      graphql.WireSemantic(builder, gateway.Router, hub),
		SemanticHub:          newTestSemanticHub(t),
		Hub:                  hub,
		Control:              &gatewayRuntimeControl{},
		AddressTableInserter: ebusgateway.NewAddressTableInserter(ebusgateway.NewAddressTable(gateway.Registry), ebusgateway.DefaultConfig()),
		EEBusAdapter:         &eebusRuntimeAdapter{},
		AdmittedSource:       builder.AdmittedMutationSource,
		ScheduleWriter:       &lateEBusScheduleWriter{},
		ConfigWriter:         &lateEBusConfigWriter{},
		WatchProvider:        &lateEBusWatchSummaryProvider{},
		GraphQLWriter:        &lateEBusGraphQLWriter{},
		GraphQLWatchSource:   &lateEBusGraphQLWatchSummaryProvider{},
	}
}

func newRetainedTestConsumers(t *testing.T, ctx context.Context, opts retainedEBusConsumerOptions) *retainedEBusConsumers {
	t.Helper()
	retained, err := newRetainedEBusConsumers(ctx, opts)
	if err != nil {
		t.Fatalf("newRetainedEBusConsumers() error = %v", err)
	}
	return retained
}

// retainedTestScope returns a live generation scope and a function that
// fences its generation.
func retainedTestScope(t *testing.T) (*ebusGenerationScope, func()) {
	t.Helper()
	adm := &genAdmission{}
	key := genKey("inc.retained", 1, 1, 1)
	adm.set(key)
	scopes := make(chan *ebusGenerationScope, 1)
	runner := genRunner(t, ebusGenerationTaskConfig{Admitted: adm.admitted, Tasks: []ebusGenerationTask{
		{Name: "capture-scope", Run: func(_ context.Context, scope *ebusGenerationScope) error {
			scopes <- scope
			return nil
		}},
	}})
	if err := runner.Admit(key); err != nil {
		t.Fatalf("Admit() error = %v", err)
	}
	select {
	case scope := <-scopes:
		return scope, func() {
			if !runner.Fence(key) {
				t.Fatal("generation was not fenced")
			}
		}
	case <-time.After(rctlBound):
		t.Fatal("generation task did not start")
		return nil, nil
	}
}

func startRetainedTestReconstructor(t *testing.T) *ebusgateway.PassiveTransactionReconstructor {
	t.Helper()
	cfg := ebusgateway.DefaultConfig()
	cfg.PassiveTransport = transport.NewLoopback()
	reconstructor, err := ebusgateway.StartPassiveTransactionReconstructor(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reconstructor.Close() })
	return reconstructor
}

func TestRetainedEBusConsumersRejectsMissingWiring(t *testing.T) {
	cases := []struct {
		name   string
		ctx    context.Context
		remove func(*retainedEBusConsumerOptions)
	}{
		{name: "process context"},
		{name: "gateway", remove: func(o *retainedEBusConsumerOptions) { o.Gateway = nil }},
		{name: "builder", remove: func(o *retainedEBusConsumerOptions) { o.Builder = nil }},
		{name: "semantic runtime", remove: func(o *retainedEBusConsumerOptions) { o.SemanticRuntime = nil }},
		{name: "semantic publication hub", remove: func(o *retainedEBusConsumerOptions) { o.SemanticHub = nil }},
		{name: "broadcast hub", remove: func(o *retainedEBusConsumerOptions) { o.Hub = nil }},
		{name: "runtime control", remove: func(o *retainedEBusConsumerOptions) { o.Control = nil }},
		{name: "address-table inserter", remove: func(o *retainedEBusConsumerOptions) { o.AddressTableInserter = nil }},
		{name: "eeBUS runtime adapter", remove: func(o *retainedEBusConsumerOptions) { o.EEBusAdapter = nil }},
		{name: "admitted source", remove: func(o *retainedEBusConsumerOptions) { o.AdmittedSource = nil }},
		{name: "schedule writer", remove: func(o *retainedEBusConsumerOptions) { o.ScheduleWriter = nil }},
		{name: "config writer", remove: func(o *retainedEBusConsumerOptions) { o.ConfigWriter = nil }},
		{name: "watch provider", remove: func(o *retainedEBusConsumerOptions) { o.WatchProvider = nil }},
		{name: "GraphQL writer", remove: func(o *retainedEBusConsumerOptions) { o.GraphQLWriter = nil }},
		{name: "GraphQL watch provider", remove: func(o *retainedEBusConsumerOptions) { o.GraphQLWatchSource = nil }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opts := retainedTestOptions(t)
			ctx := context.Background()
			if tc.remove == nil {
				ctx = nil
			} else {
				tc.remove(&opts)
			}
			retained, err := newRetainedEBusConsumers(ctx, opts)
			if !errors.Is(err, errRetainedEBusConsumersWiring) || !strings.HasSuffix(err.Error(), ": "+tc.name) {
				t.Fatalf("newRetainedEBusConsumers() error = %v; want missing %s", err, tc.name)
			}
			if retained != nil {
				t.Fatal("rejected wiring returned an owner")
			}
		})
	}
}

func TestRetainedEBusConsumersAcceptsCompleteWiringWithoutOptionalObservers(t *testing.T) {
	opts := retainedTestOptions(t)
	opts.BusObservability, opts.Deduplicator = nil, nil
	retained := newRetainedTestConsumers(t, context.Background(), opts)
	if surface := retained.surface(); surface.listener != nil || surface.reconstructor != nil {
		t.Fatalf("surface before any generation = %#v; want empty", surface)
	}
	if retained.hasSemanticBarrier() {
		t.Fatal("default configuration created a semantic barrier")
	}
	if poller, promoter := retained.pollerAndPromoter(); poller != nil || promoter != nil {
		t.Fatal("constructor started a retained consumer")
	}
}

// The inserter subscribes only after the admitted source is installed through
// a live generation, and exactly once however often and concurrently the
// admission paths ask for it.
func TestRetainedEBusConsumersSubscribesInserterOnceAfterAdmission(t *testing.T) {
	opts := retainedTestOptions(t)
	retained := newRetainedTestConsumers(t, context.Background(), opts)
	var calls atomic.Int32
	subscribe := retained.subscribeInserter
	retained.subscribeInserter = func(active *ebusgateway.PassiveTransactionReconstructor) (*ebusgateway.PassiveClassifiedSubscription, error) {
		calls.Add(1)
		return subscribe(active)
	}

	// No reconstructor yet, even with an admitted source.
	opts.Builder.SetAdmittedMutationSource(retainedTestSource)
	retained.subscribeAddressTableInserter()
	opts.Builder.ClearAdmittedMutationSource()
	if calls.Load() != 0 {
		t.Fatal("inserter subscribed without a published reconstructor")
	}

	scope, fence := retainedTestScope(t)
	active := startRetainedTestReconstructor(t)
	if !retained.publishReconstructor(scope, active) || retained.currentReconstructor() != active {
		t.Fatal("live generation did not publish its reconstructor")
	}
	retained.subscribeAddressTableInserter()
	if calls.Load() != 0 {
		t.Fatal("inserter subscribed before admission")
	}

	if !retained.installAdmittedSource(scope, retainedTestSource) {
		t.Fatal("live generation could not install the admitted source")
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			retained.subscribeAddressTableInserter()
		}()
	}
	wg.Wait()
	retained.subscribeAddressTableInserter()
	if got := calls.Load(); got != 1 {
		t.Fatalf("inserter subscribed %d times after admission; want 1", got)
	}
	if !retained.insertSubscribed.Load() {
		t.Fatal("successful subscription was not recorded")
	}

	// A fenced generation neither clears nor installs the source.
	fence()
	if retained.clearAdmittedSource(scope) || retained.installAdmittedSource(scope, retainedTestSource+1) {
		t.Fatal("fenced generation changed the admitted source")
	}
	if source, ok := opts.Builder.AdmittedMutationSource(); !ok || source != retainedTestSource {
		t.Fatalf("admitted source = %#x/%v; want %#x/true", source, ok, retainedTestSource)
	}
	retained.withdrawAdmittedSource(true)
	if _, ok := opts.Builder.AdmittedMutationSource(); ok {
		t.Fatal("forced withdrawal left the admitted source installed")
	}
}

// A fenced generation installs no admitted source, so the inserter stays
// unsubscribed.
func TestRetainedEBusConsumersFencedAdmissionDoesNotSubscribeInserter(t *testing.T) {
	opts := retainedTestOptions(t)
	retained := newRetainedTestConsumers(t, context.Background(), opts)
	var calls atomic.Int32
	retained.subscribeInserter = func(*ebusgateway.PassiveTransactionReconstructor) (*ebusgateway.PassiveClassifiedSubscription, error) {
		calls.Add(1)
		return nil, errors.New("unexpected subscription")
	}
	scope, fence := retainedTestScope(t)
	active := startRetainedTestReconstructor(t)
	if !retained.publishReconstructor(scope, active) {
		t.Fatal("live generation did not publish its reconstructor")
	}
	fence()
	if retained.installAdmittedSource(scope, retainedTestSource) {
		t.Fatal("fenced generation installed the admitted source")
	}
	retained.subscribeAddressTableInserter()
	if calls.Load() != 0 {
		t.Fatal("inserter subscribed without an admitted source")
	}
}

func retainedBarrierOptions(t *testing.T) retainedEBusConsumerOptions {
	t.Helper()
	opts := retainedTestOptions(t)
	opts.Config.ScanOnStart = true
	opts.SourceSelectionAdmit = true
	return opts
}

func requireBarrierOpen(t *testing.T, retained *retainedEBusConsumers) {
	t.Helper()
	select {
	case <-retained.semanticBarrier:
	case <-time.After(rctlBound):
		t.Fatal("semantic barrier did not open")
	}
}

// The barrier closes once: repeated and concurrent admission outcomes and the
// later process exit do not close it again.
func TestRetainedEBusConsumersClosesSemanticBarrierOnce(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	retained := newRetainedTestConsumers(t, ctx, retainedBarrierOptions(t))
	if !retained.hasSemanticBarrier() {
		t.Fatal("startup scan with source-selection admission did not create a semantic barrier")
	}
	exited := make(chan struct{})
	go func() {
		<-ctx.Done()
		close(exited)
	}()
	retained.openSemanticBarrierOnProcessExit()
	select {
	case <-retained.semanticBarrier:
		t.Fatal("semantic barrier opened before any admission outcome")
	default:
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			retained.closeSemanticBarrier()
		}()
	}
	wg.Wait()
	requireBarrierOpen(t, retained)
	cancel()
	<-exited
	retained.closeSemanticBarrier()
}

func TestRetainedEBusConsumersOpensSemanticBarrierOnProcessExit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	retained := newRetainedTestConsumers(t, ctx, retainedBarrierOptions(t))
	retained.openSemanticBarrierOnProcessExit()
	cancel()
	requireBarrierOpen(t, retained)
	retained.closeSemanticBarrier()
}

func TestRetainedEBusConsumersCreatesBarrierOnlyForDeferredStartupScan(t *testing.T) {
	for _, tc := range []struct {
		name        string
		scanOnStart bool
		admit       bool
		want        bool
	}{
		{name: "startup scan with source-selection admission", scanOnStart: true, admit: true, want: true},
		{name: "startup scan without deferred admission", scanOnStart: true},
		{name: "source-selection admission without startup scan", admit: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts := retainedTestOptions(t)
			opts.Config.ScanOnStart = tc.scanOnStart
			opts.Config.BroadcastListen = false
			opts.SourceSelectionAdmit = tc.admit
			retained := newRetainedTestConsumers(t, context.Background(), opts)
			if retained.hasSemanticBarrier() != tc.want {
				t.Fatalf("semantic barrier present = %v; want %v", retained.hasSemanticBarrier(), tc.want)
			}
			retained.closeSemanticBarrier()
		})
	}
}

// The process surface starts the poller once, on the process context and
// behind the semantic barrier, and records readiness so a later generation
// only rebinds.
func TestRetainedEBusConsumersStartsPollerOnceBehindBarrier(t *testing.T) {
	original := startVaillantSemanticPollingFn
	t.Cleanup(func() { startVaillantSemanticPollingFn = original })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	retained := newRetainedTestConsumers(t, ctx, retainedBarrierOptions(t))
	var starts atomic.Int32
	startVaillantSemanticPollingFn = func(pollCtx context.Context, _ ebusgateway.Config, _ *ebusgateway.Gateway, _ *graphql.LiveSemanticProvider, _ *graphql.BroadcastHub, barrier <-chan struct{}) *vaillantSemanticPoller {
		starts.Add(1)
		if pollCtx != ctx {
			t.Error("poller does not run on the process context")
		}
		if barrier != (<-chan struct{})(retained.semanticBarrier) {
			t.Error("poller does not wait on the retained semantic barrier")
		}
		return nil
	}
	for generation := 0; generation < 2; generation++ {
		if err := retained.ensureProcessSurface(retained.cfg); err != nil {
			t.Fatalf("generation %d: ensureProcessSurface() error = %v", generation+1, err)
		}
	}
	if got := starts.Load(); got != 1 {
		t.Fatalf("poller started %d times; want 1", got)
	}
	retained.surfaceMu.Lock()
	ready := retained.surfaceReady
	retained.surfaceMu.Unlock()
	if !ready {
		t.Fatal("process surface readiness not recorded")
	}
	retained.closeSemanticBarrier()
}

func TestRetainedEBusConsumersSurfaceReportsPublishedReconstructor(t *testing.T) {
	retained := newRetainedTestConsumers(t, context.Background(), retainedTestOptions(t))
	scope, _ := retainedTestScope(t)
	active := startRetainedTestReconstructor(t)
	if !retained.publishReconstructor(scope, active) {
		t.Fatal("live generation did not publish its reconstructor")
	}
	surface := retained.surface()
	if surface.reconstructor != active {
		t.Fatal("consumer shutdown surface does not report the published reconstructor")
	}
	if surface.listener != nil {
		t.Fatal("consumer shutdown surface reports a listener that was never started")
	}
}

// ensureProcessSurface keeps its steps in order: every generation rebinds the
// retained poller first, the watch observer is bound before any fallible
// step, and readiness is recorded after the last fallible step.
func TestRetainedEBusConsumersProcessSurfaceStepsRunInOrder(t *testing.T) {
	files, _ := lifecycleCompositionSources(t, token.NewFileSet())
	surface := lifecycleMethodDecl(t, files, "retainedEBusConsumers", "ensureProcessSurface")
	calls := lifecycleSelectorCallPositions(surface.Body)
	last := token.NoPos
	for _, step := range []string{
		"rebindPollerSource",
		"startPoller",
		"bindGraphQLWriters",
		"bindWatchObserver",
		"installCapture",
		"attachShadowLane",
		"createPromoter",
		"bindEnrichmentAndWriters",
		"recordReadiness",
	} {
		if len(calls[step]) != 1 {
			t.Fatalf("ensureProcessSurface calls %s %d times; want 1", step, len(calls[step]))
		}
		if calls[step][0] <= last {
			t.Fatalf("ensureProcessSurface calls %s out of order", step)
		}
		last = calls[step][0]
	}
	// startPoller rebinds the poller it started to the admitted generation.
	start := lifecycleMethodDecl(t, files, "retainedEBusConsumers", "startPoller")
	if got := len(lifecycleSelectorCallPositions(start.Body)["rebindPollerSource"]); got != 1 {
		t.Fatalf("startPoller rebinds the new poller %d times; want 1", got)
	}
}
