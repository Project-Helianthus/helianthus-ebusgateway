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
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/drivermanager"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimecontrol"
	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
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
		if err := retained.ensureProcessSurface(context.Background(), retained.cfg); err != nil {
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

// --- The retained poller stays off the bus between generations ---
//
// The fixture composes the production executor, the generation task runner
// and the retained consumers over the in-process control plant. Its two tasks
// keep the production order: a source-admission task, held open per
// generation by the test, and the semantic-surface task, which installs
// through the generation fence. The poller's frame seam records the initiator
// of every bus request, and a driver keeps submitting recurring acquisitions.

// retainedPollerGateSource returns the synthetic initiator and companion that
// the n-th admitted generation validates; consecutive generations differ.
func retainedPollerGateSource(n int32) (byte, byte) {
	sources := [...]byte{0x31, 0x71, 0xF1}
	source := sources[int(n-1)%len(sources)]
	return source, protocol.CompanionAddressForSource(source)
}

type retainedPollerGateFixture struct {
	t        *testing.T
	ctx      context.Context
	plant    *ctlPlant
	runner   *ebusGenerationTaskRunner
	executor *ebusControlExecutor
	retained *retainedEBusConsumers

	admissions  atomic.Int32
	entered     chan int32
	withdrawals chan struct{}
	submissions atomic.Int64

	mu      sync.Mutex
	holds   map[int32]chan struct{}
	configs map[runtimecontrol.NativeGeneration]ebusgateway.Config
	sends   []byte
}

func newRetainedPollerGateFixture(t *testing.T, plantOpts ctlPlantOptions) *retainedPollerGateFixture {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	f := &retainedPollerGateFixture{
		t:           t,
		ctx:         ctx,
		plant:       newCtlPlant(t, plantOpts),
		entered:     make(chan int32, 8),
		withdrawals: make(chan struct{}, 8),
		holds:       map[int32]chan struct{}{},
		configs:     map[runtimecontrol.NativeGeneration]ebusgateway.Config{},
	}
	t.Cleanup(func() {
		f.mu.Lock()
		defer f.mu.Unlock()
		for _, hold := range f.holds {
			select {
			case <-hold:
			default:
				close(hold)
			}
		}
	})
	original := startVaillantSemanticPollingFn
	t.Cleanup(func() { startVaillantSemanticPollingFn = original })
	startVaillantSemanticPollingFn = func(pollCtx context.Context, _ ebusgateway.Config, _ *ebusgateway.Gateway, _ *graphql.LiveSemanticProvider, _ *graphql.BroadcastHub, _ <-chan struct{}) *vaillantSemanticPoller {
		poller := &vaillantSemanticPoller{tasks: newSemanticTaskScheduler(), sendFrameFn: f.send}
		go poller.tasks.run(pollCtx)
		return poller
	}
	opts := retainedTestOptions(t)
	opts.Control = &gatewayRuntimeControl{bridge: f.plant.bridge}
	f.retained = newRetainedTestConsumers(t, ctx, opts)
	f.runner = genRunner(t, ebusGenerationTaskConfig{
		Admitted: ebusControlGenerationAdmitted(f.plant.bridge),
		Tasks: []ebusGenerationTask{
			{Name: ebusTaskSourceAdmission, Run: f.admitSource},
			{Name: ebusTaskSemanticSurface, Requires: []string{ebusTaskSourceAdmission}, Run: f.installSurface},
		},
	})
	f.executor = &ebusControlExecutor{bridge: f.plant.bridge}
	// The production withdrawal hook; a call that withdraws (forced, or with
	// no generation admitted) is signalled to the test.
	f.executor.bind(f.runner, func(force bool) {
		f.retained.withdrawAdmittedSource(force)
		if force || !f.retained.control.EBusAdmitted() {
			select {
			case f.withdrawals <- struct{}{}:
			default:
			}
		}
	})
	f.plant.bridge.SetAdmitGate(f.executor.admitGate)
	return f
}

func (f *retainedPollerGateFixture) hold(n int32) chan struct{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	hold := f.holds[n]
	if hold == nil {
		hold = make(chan struct{})
		f.holds[n] = hold
	}
	return hold
}

// completeValidation ends the held source validation of the n-th generation.
func (f *retainedPollerGateFixture) completeValidation(n int32) { close(f.hold(n)) }

func (f *retainedPollerGateFixture) admitSource(ctx context.Context, scope *ebusGenerationScope) error {
	n := f.admissions.Add(1)
	f.entered <- n
	select {
	case <-f.hold(n):
	case <-ctx.Done():
		return nil
	}
	source, companion := retainedPollerGateSource(n)
	cfg := ebusgateway.DefaultConfig()
	cfg.ScanSource, cfg.ScanSourceAuto, cfg.StartupCompanionTarget = source, false, companion
	f.mu.Lock()
	f.configs[scope.Key().Native] = cfg
	f.mu.Unlock()
	// Validation passed: install the source as the production admission does.
	f.retained.installAdmittedSource(scope, source)
	return nil
}

func (f *retainedPollerGateFixture) installSurface(_ context.Context, scope *ebusGenerationScope) error {
	f.mu.Lock()
	cfg, ok := f.configs[scope.Key().Native]
	f.mu.Unlock()
	if !ok {
		return nil
	}
	return installProcessSurface(scope, func() error { return f.retained.ensureProcessSurface(scope.Context(), cfg) })
}

func (f *retainedPollerGateFixture) send(_ context.Context, frame protocol.Frame) (*protocol.Frame, error) {
	f.mu.Lock()
	f.sends = append(f.sends, frame.Source)
	f.mu.Unlock()
	return &protocol.Frame{}, nil
}

func (f *retainedPollerGateFixture) sendsSince(mark int) []byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]byte(nil), f.sends[mark:]...)
}

func (f *retainedPollerGateFixture) waitEntered(n int32) {
	f.t.Helper()
	deadline := time.After(rctlBound)
	for {
		select {
		case got := <-f.entered:
			if got == n {
				return
			}
		case <-deadline:
			f.t.Fatalf("source validation of generation %d did not start", n)
		}
	}
}

func (f *retainedPollerGateFixture) waitWithdrawal() {
	f.t.Helper()
	select {
	case <-f.withdrawals:
	case <-time.After(rctlBound):
		f.t.Fatal("the predecessor generation was not withdrawn")
	}
}

// startFirstGeneration starts the first generation, completes its validation
// and returns the retained poller once it carries that generation's source.
func (f *retainedPollerGateFixture) startFirstGeneration(corr runtimecontrol.LifecycleCorrelation) *vaillantSemanticPoller {
	f.t.Helper()
	// Before any admission the withdrawal hook runs with nothing to suspend.
	f.executor.sync(ctlCtx(f.t))
	if err := f.executor.Start(ctlCtx(f.t), corr); err != nil {
		f.t.Fatal(err)
	}
	f.waitEntered(1)
	f.completeValidation(1)
	want, _ := retainedPollerGateSource(1)
	var poller *vaillantSemanticPoller
	ctlWait(f.t, "first generation binds the retained poller", func() bool {
		poller, _ = f.retained.pollerAndPromoter()
		if poller == nil {
			return false
		}
		source, _ := poller.sourceAndCompanion()
		return source == want
	})
	// Withdrawals before the first admission concern no predecessor.
	for drained := false; !drained; {
		select {
		case <-f.withdrawals:
		default:
			drained = true
		}
	}
	f.drive(poller)
	return poller
}

// drive submits a recurring acquisition every millisecond through the
// poller's schedule; each acquisition sends one request with the poller's
// current initiator.
func (f *retainedPollerGateFixture) drive(poller *vaillantSemanticPoller) {
	acquire := func(ctx context.Context) {
		source, _ := poller.sourceAndCompanion()
		_, _ = poller.sendSemanticFrame(ctx, protocol.Frame{Source: source, Target: 0x08, Primary: 0x07, Secondary: 0x04})
	}
	go func() {
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-f.ctx.Done():
				return
			case <-ticker.C:
				poller.enqueueTask(semanticTaskRefreshState, semanticTaskPriorityHigh, acquire)
				f.submissions.Add(1)
			}
		}
	}()
}

// waitSendFrom waits for a request with source after mark.
func (f *retainedPollerGateFixture) waitSendFrom(what string, source byte, mark int) {
	f.t.Helper()
	ctlWait(f.t, what, func() bool {
		for _, got := range f.sendsSince(mark) {
			if got == source {
				return true
			}
		}
		return false
	})
}

// quiesce returns the request count once no acquisition holds the poller's
// acquisition lock, so an acquisition already in flight is not counted
// against the successor.
func (f *retainedPollerGateFixture) quiesce(poller *vaillantSemanticPoller) int {
	poller.pollMu.Lock()
	defer poller.pollMu.Unlock()
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.sends)
}

// requireSilentUntilRebound holds the successor's validation open while the
// recurring acquisitions keep being submitted, requires that the retained
// poller sends nothing, then completes the validation and requires polling to
// resume on the successor's source only.
func (f *retainedPollerGateFixture) requireSilentUntilRebound(poller *vaillantSemanticPoller, successor int32) {
	f.t.Helper()
	f.waitWithdrawal()
	f.waitEntered(successor)
	mark := f.quiesce(poller)
	submitted := f.submissions.Load()
	ctlWait(f.t, "recurring acquisitions submitted while validation is held", func() bool {
		return f.submissions.Load() >= submitted+50
	})
	predecessor, _ := retainedPollerGateSource(successor - 1)
	if held := f.sendsSince(mark); len(held) != 0 {
		f.t.Fatalf("retained poller sent %d requests (first initiator %#02x, predecessor %#02x) while the successor's source validation was held; want none", len(held), held[0], predecessor)
	}
	f.completeValidation(successor)
	want, wantCompanion := retainedPollerGateSource(successor)
	f.waitSendFrom("polling resumes on the successor's source", want, mark)
	for i, got := range f.sendsSince(mark) {
		if got != want {
			f.t.Fatalf("request %d after the fence used initiator %#02x; want only the successor's %#02x", i, got, want)
		}
	}
	if source, companion := poller.sourceAndCompanion(); source != want || companion != wantCompanion {
		f.t.Fatalf("poller source/companion = %#02x/%#02x; want %#02x/%#02x", source, companion, want, wantCompanion)
	}
}

// RESTART: the predecessor is stopped and a successor started; the poller
// stays off the bus until the successor validated its source and rebound it.
func TestRetainedPollerWaitsForSuccessorSourceAfterRestart(t *testing.T) {
	f := newRetainedPollerGateFixture(t, ctlPlantOptions{budget: 1})
	predecessor := ctlCorr("inc.poller.restart.e1", 1, 1)
	poller := f.startFirstGeneration(predecessor)

	if out := f.executor.Stop(ctlCtx(t), predecessor); !out.NativeClosed || !out.SemanticCleaned {
		t.Fatalf("STOP outcome = %+v; want closed and cleaned", out)
	}
	if err := f.executor.Start(ctlCtx(t), ctlCorr("inc.poller.restart.e2", 1, 2)); err != nil {
		t.Fatal(err)
	}
	f.requireSilentUntilRebound(poller, 2)
}

// Automatic replacement: the manager replaces the failed generation; the
// poller stays off the bus until the replacement validated its source and
// rebound it.
func TestRetainedPollerWaitsForSuccessorSourceAfterReplacement(t *testing.T) {
	f := newRetainedPollerGateFixture(t, ctlPlantOptions{budget: 50, initialDelay: 20 * time.Millisecond})
	poller := f.startFirstGeneration(ctlCorr("inc.poller.replace.e1", 1, 1))
	first, ok := ebusControlGeneration(f.plant.bridge)
	if !ok {
		t.Fatal("no admitted generation")
	}
	if !f.plant.bridge.ReportFailure(first.Control, drivermanager.Failure{Reason: drivermanager.Reason{Code: drivermanager.ReasonDependencyUnavailable, Retryable: true}}) {
		t.Fatal("failure report refused")
	}
	ctlWait(t, "replacement generation admitted with its tasks", func() bool {
		f.executor.sync(context.Background())
		key, ok := ebusControlGeneration(f.plant.bridge)
		status := f.runner.Status()
		return ok && key.Native > first.Native && status.Key == key && status.Phase == ebusGenerationRunning
	})
	f.requireSilentUntilRebound(poller, 2)
}

// Control: the first generation polls on its own source as soon as its
// semantic surface bound the poller; no resume step is needed.
func TestRetainedPollerWaitsForSuccessorSourceFirstGenerationUnchanged(t *testing.T) {
	f := newRetainedPollerGateFixture(t, ctlPlantOptions{budget: 1})
	f.startFirstGeneration(ctlCorr("inc.poller.first.e1", 1, 1))
	want, _ := retainedPollerGateSource(1)
	f.waitSendFrom("first generation polls on its source", want, 0)
	for i, got := range f.sendsSince(0) {
		if got != want {
			t.Fatalf("first-generation request %d used initiator %#02x; want %#02x", i, got, want)
		}
	}
}

func requirePollerSuspended(t *testing.T, poller *vaillantSemanticPoller, what string) {
	t.Helper()
	if _, suspended := (&semanticPollSchedule{pollMu: &poller.pollMu}).suspension(); !suspended {
		t.Fatalf("%s resumed the retained poller; want it suspended", what)
	}
}

// The resume needs a live generation, an admitted one and its installed
// source. Each condition
// is varied alone: a rebinding with a live context while no generation is
// admitted, and a rebinding with an ended (fenced) context while the successor
// is admitted, both leave acquisition suspended; the successor's own
// rebinding resumes it.
func TestRetainedPollerWaitsForSuccessorSourceResumeNeedsLiveAdmittedGeneration(t *testing.T) {
	f := newRetainedPollerGateFixture(t, ctlPlantOptions{budget: 1})
	predecessor := ctlCorr("inc.poller.guard.e1", 1, 1)
	poller := f.startFirstGeneration(predecessor)
	if out := f.executor.Stop(ctlCtx(t), predecessor); !out.NativeClosed || !out.SemanticCleaned {
		t.Fatalf("STOP outcome = %+v; want closed and cleaned", out)
	}
	requirePollerSuspended(t, poller, "STOP")
	source, companion := retainedPollerGateSource(1)
	late := ebusgateway.DefaultConfig()
	late.ScanSource, late.ScanSourceAuto, late.StartupCompanionTarget = source, false, companion

	// Live context, no admitted generation.
	f.retained.rebindPollerSource(context.Background(), poller, late)
	requirePollerSuspended(t, poller, "a rebinding while no generation is admitted")

	// Admitted successor, ended context of a fenced generation.
	if err := f.executor.Start(ctlCtx(t), ctlCorr("inc.poller.guard.e2", 1, 2)); err != nil {
		t.Fatal(err)
	}
	f.waitEntered(2)
	if !f.retained.control.EBusAdmitted() {
		t.Fatal("successor not admitted while its validation is held")
	}
	// Admitted and rebound, but the source is not installed yet (the
	// validation is still held): the poller stays suspended.
	f.retained.rebindPollerSource(context.Background(), poller, late)
	requirePollerSuspended(t, poller, "a rebinding before the source is installed")
	// With the source installed, only the ended context still blocks.
	f.retained.builder.SetAdmittedMutationSource(late.ScanSource)
	fenced, cancel := context.WithCancel(context.Background())
	cancel()
	f.retained.rebindPollerSource(fenced, poller, late)
	requirePollerSuspended(t, poller, "a rebinding by a fenced generation")

	f.completeValidation(2)
	want, _ := retainedPollerGateSource(2)
	ctlWait(t, "the successor's rebinding resumes the poller", func() bool {
		_, suspended := (&semanticPollSchedule{pollMu: &poller.pollMu}).suspension()
		got, _ := poller.sourceAndCompanion()
		return !suspended && got == want
	})
}

// The schedule gate: an acquisition holding the lock when the suspension
// arrives ends normally; queued acquisitions wait at the task boundary; the
// inline fallback skips; a waiting caller whose context ends gives up; every
// schedule view of one poller shares the suspension; resume releases the
// waiting acquisition and leaves no suspension behind.
func TestRetainedPollerWaitsForSuccessorSourceScheduleGate(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	queue := newSemanticTaskScheduler()
	go queue.run(ctx)
	pollMu := &sync.Mutex{}
	s := &semanticPollSchedule{tasks: queue, pollMu: pollMu}
	t.Cleanup(s.resumeAcquisition)

	entered, release, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
	s.enqueueTask("in-flight", semanticTaskPriorityLow, func(context.Context) {
		close(entered)
		<-release
		close(finished)
	})
	scheduleWait(t, entered)
	// A second view of the same poller suspends it.
	(&semanticPollSchedule{pollMu: pollMu}).suspendAcquisition()
	(&semanticPollSchedule{pollMu: pollMu}).suspendAcquisition()
	queued := make(chan struct{})
	s.enqueueTask("queued", semanticTaskPriorityHigh, func(context.Context) { close(queued) })
	close(release)
	scheduleWait(t, finished)
	select {
	case <-queued:
		t.Fatal("queued acquisition ran while suspended")
	case <-time.After(50 * time.Millisecond):
	}

	inline := false
	(&semanticPollSchedule{pollMu: pollMu}).enqueueTask("inline", semanticTaskPriorityLow, func(context.Context) { inline = true })
	if inline {
		t.Fatal("inline fallback ran while suspended")
	}

	direct, directCancel := context.WithCancel(context.Background())
	gaveUp := make(chan struct{})
	go func() {
		defer close(gaveUp)
		s.withPollLock(direct, func(context.Context) { t.Error("direct acquisition ran while suspended") })
	}()
	directCancel()
	scheduleWait(t, gaveUp)

	(&semanticPollSchedule{pollMu: pollMu}).resumeAcquisition()
	scheduleWait(t, queued)
	if _, suspended := s.suspension(); suspended {
		t.Fatal("suspension left behind after resume")
	}
	s.resumeAcquisition()
	ran := false
	(&semanticPollSchedule{pollMu: pollMu}).enqueueTask("inline", semanticTaskPriorityLow, func(context.Context) { ran = true })
	if !ran {
		t.Fatal("inline fallback skipped after resume")
	}
}
