package adaptermux

import (
	"errors"
	"io"
	"log"
	"strings"
	"testing"
	"time"

	ebuserrors "github.com/Project-Helianthus/helianthus-ebusgo/errors"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/adaptermux/v8classifier"
)

// Out-of-step scenarios against the loopback fake adapter. StartDeadline is
// 300 ms (1 s for the healthy cases), SYN every 15 ms, ReconnectInitialDelay
// 50 ms and a foreign telegram every second. A run lasts at most 12 s.

const (
	stepTestStartDeadline = 300 * time.Millisecond
	stepTestHoldBy        = 7500 * time.Millisecond // the hold must be set by then
	stepTestFinalWindow   = 4 * time.Second
	stepTestPoll          = 5 * time.Millisecond
	stepTestHoldLine      = "adaptermux: active_arbitration_hold state=set reason=adapter_out_of_step bad_start_cycles=3"
	stepTestResetLine     = "adaptermux: adapter_out_of_step bad_start_cycles=3 action=link_reset"
	stepTestSkipLine      = "adaptermux: tryGrantAndStart skipped: active arbitration held (adapter_out_of_step)"
)

func stepTestConfig(deadline time.Duration, mode v8classifier.Mode) Config {
	return Config{StartDeadline: deadline, ReconnectInitialDelay: 50 * time.Millisecond, V8ClassifierMode: mode}
}

func laggingAdapterConfig(survives bool) fakeAdapterConfig {
	return fakeAdapterConfig{mode: fakeAdapterOneBehind, lagFrom: 6, lagSurvives: survives, foreignPeriod: time.Second}
}

// stepWatch samples the detector of one mux and checks the run rule on every
// sample: no detection before the third bad START cycle, and the first
// detection exactly at it.
type stepWatch struct {
	t       *testing.T
	m       *Mux
	began   time.Time
	last    adapterStepState
	firstAt adapterStepState // sample of the first detection
	seen    bool
}

func newStepWatch(t *testing.T, m *Mux) *stepWatch {
	return &stepWatch{t: t, m: m, began: time.Now()}
}

// until polls until cond holds or the deadline passes and reports which.
func (w *stepWatch) until(deadline time.Time, cond func(adapterStepState) bool) bool {
	w.t.Helper()
	for {
		s := adapterStepProbe(w.m)
		if s.badCycles != w.last.badCycles || s.detections != w.last.detections {
			w.t.Logf("t=%v start_cycle=%d bad_start_cycles=%d detections=%d link_resets=%d hold=%q",
				time.Since(w.began).Round(time.Millisecond), s.cycle, s.badCycles, s.detections, s.linkResets, s.holdReason)
		}
		if s.detections == 0 && s.badCycles >= outOfStepBadCyclesForTest {
			w.t.Errorf("no detection after %d bad START cycles", s.badCycles)
		}
		if s.detections > 0 && !w.seen {
			w.seen, w.firstAt = true, s
		}
		w.last = s
		if cond(s) {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(stepTestPoll)
	}
}

func held(s adapterStepState) bool { return s.holdReason != "" }

// requireResetOnceThenHold is the shared body of the two lagging-adapter runs
// whose lag survives the link reset.
func requireResetOnceThenHold(t *testing.T, adapter *fakeAdapter, run *laggingRun, began time.Time) {
	t.Helper()
	w := newStepWatch(t, run.mux)
	windowStart := began.Add(stepTestHoldBy)
	isHeld := w.until(windowStart, held)
	heldAt := time.Now()
	synBefore, foreignBefore := run.passiveSyn.Load(), run.passiveForeign.Load()
	var extSynBefore, extForeignBefore uint64
	if run.external != nil {
		extSynBefore, extForeignBefore = run.external.syn.Load(), run.external.foreign.Load()
	}
	if wait := time.Until(windowStart); wait > 0 {
		time.Sleep(wait)
	}
	time.Sleep(stepTestFinalWindow)
	starts, connections, stray := adapter.stats()
	final := adapter.startsSince(windowStart)
	t.Logf("START writes=%d in final %v=%d connections=%d stray data bytes=%d gateway completed=%d",
		starts, stepTestFinalWindow, final, connections, stray, run.gateway.completions())
	if !w.seen {
		t.Fatalf("no out-of-step detection within %v", stepTestHoldBy)
	}
	if w.firstAt.badCycles != outOfStepBadCyclesForTest {
		t.Errorf("first detection after %d bad START cycles, want %d", w.firstAt.badCycles, outOfStepBadCyclesForTest)
	}
	if !isHeld {
		t.Fatalf("active arbitration not held within %v: %+v", stepTestHoldBy, adapterStepProbe(run.mux))
	}
	reason, detections, resets := adapterStepSnapshot(run.mux)
	if reason != "adapter_out_of_step" || detections != 2 || resets != 1 {
		t.Errorf("snapshot hold=%q detections=%d link resets=%d, want adapter_out_of_step, 2, 1", reason, detections, resets)
	}
	if connections != 2 {
		t.Errorf("fake adapter saw %d connections, want 2 (exactly one link reset)", connections)
	}
	if n := run.log.count(stepTestResetLine); n != 1 {
		t.Errorf("link reset logged %d times, want 1", n)
	}
	if n := run.log.count(stepTestHoldLine); n != 1 {
		t.Errorf("hold logged %d times, want 1", n)
	}
	if final != 0 {
		t.Errorf("%d START requests reached the adapter in the final %v", final, stepTestFinalWindow)
	}
	// The selection skip line is rate-limited to one per second.
	if n := run.log.count(stepTestSkipLine); n == 0 || n > int((stepTestHoldBy+stepTestFinalWindow)/time.Second)+1 {
		t.Errorf("hold skip line logged %d times", n)
	}
	if run.passiveSyn.Load() <= synBefore || run.passiveForeign.Load() <= foreignBefore {
		t.Errorf("passive delivery stopped during the hold: SYN %d->%d foreign %d->%d",
			synBefore, run.passiveSyn.Load(), foreignBefore, run.passiveForeign.Load())
	}
	if run.external != nil && (run.external.syn.Load() <= extSynBefore || run.external.foreign.Load() <= extForeignBefore) {
		t.Errorf("session delivery stopped during the hold: SYN %d->%d foreign %d->%d",
			extSynBefore, run.external.syn.Load(), extForeignBefore, run.external.foreign.Load())
	}
	requireHeldGatewayBids(t, run.gateway, heldAt)
}

// requireHeldGatewayBids checks the gateway bids made during the hold: each
// ends no sooner than StartDeadline with an error that names the hold and
// wraps ErrTimeout.
func requireHeldGatewayBids(t *testing.T, c *gatewayBusClient, heldAt time.Time) {
	t.Helper()
	checked := 0
	for _, bid := range c.bidsSince(heldAt) {
		checked++
		if took := bid.end.Sub(bid.begin); took < stepTestStartDeadline {
			t.Errorf("gateway bid during the hold ended after %v, want at least %v", took, stepTestStartDeadline)
		}
		if bid.err == nil || !errors.Is(bid.err, ebuserrors.ErrTimeout) ||
			!strings.Contains(bid.err.Error(), "active arbitration held (adapter_out_of_step)") {
			t.Errorf("gateway bid during the hold ended with %v", bid.err)
		}
	}
	if checked == 0 {
		t.Error("no gateway bid was made during the hold")
	}
}

// (a) Two clients, classifier enforcing, an adapter one host command behind
// whose lag survives a reconnect: one link reset, then the hold.
func TestAdapterOutOfStepTwoClientsResetOnceThenHold(t *testing.T) {
	began := time.Now()
	adapter := newFakeAdapter(t, laggingAdapterConfig(true))
	run := startLaggingMux(t, adapter, laggingMuxOptions{
		cfg:      stepTestConfig(stepTestStartDeadline, v8classifier.ModeEnforce),
		external: &externalClientOptions{poll: 50 * time.Millisecond},
		gateway:  true,
	})
	requireResetOnceThenHold(t, adapter, run, began)
}

// (b) The gateway alone, the same expectations without the external session.
func TestAdapterOutOfStepGatewayOnlyResetOnceThenHold(t *testing.T) {
	began := time.Now()
	adapter := newFakeAdapter(t, laggingAdapterConfig(true))
	run := startLaggingMux(t, adapter, laggingMuxOptions{
		cfg:     stepTestConfig(stepTestStartDeadline, v8classifier.ModeOff),
		gateway: true,
	})
	requireResetOnceThenHold(t, adapter, run, began)
}

// (c) A lag the new connection clears: after the single link reset
// transactions complete again, nothing is held, and the reset is available
// again once a transaction completed.
func TestAdapterOutOfStepLagClearedByLinkReset(t *testing.T) {
	if testing.Short() {
		t.Skip("timing scenario; runs without -short")
	}
	began := time.Now()
	adapter := newFakeAdapter(t, laggingAdapterConfig(false))
	run := startLaggingMux(t, adapter, laggingMuxOptions{
		cfg:      stepTestConfig(stepTestStartDeadline, v8classifier.ModeOff),
		external: &externalClientOptions{poll: 50 * time.Millisecond},
		gateway:  true,
	})
	w := newStepWatch(t, run.mux)
	if !w.until(began.Add(stepTestHoldBy), func(s adapterStepState) bool { return s.linkResets == 1 }) {
		t.Fatalf("no link reset within %v", stepTestHoldBy)
	}
	if adapterStepResetAvailable(run.mux.AdapterStepMemory()) {
		t.Error("link reset still available right after it was spent")
	}
	gatewayBefore, externalBefore := run.gateway.completions(), run.external.completed.Load()
	completed := func(adapterStepState) bool {
		return run.gateway.completions() > gatewayBefore || run.external.completed.Load() > externalBefore
	}
	if !w.until(began.Add(11*time.Second), completed) {
		t.Fatal("no transaction completed after the link reset")
	}
	time.Sleep(200 * time.Millisecond)
	s := adapterStepProbe(run.mux)
	_, connections, _ := adapter.stats()
	if s.holdReason != "" || s.linkResets != 1 || s.detections != 1 || connections != 2 {
		t.Errorf("after recovery: %+v connections=%d, want no hold, one detection, one link reset, two connections", s, connections)
	}
	if !adapterStepResetAvailable(run.mux.AdapterStepMemory()) {
		t.Error("link reset not available again after a completed transaction")
	}
}

// (d) The adapter's RESETTED ends the hold; the adapter answers in step
// afterwards, START requests resume and transactions complete.
func TestAdapterOutOfStepHoldClearedByAdapterReset(t *testing.T) {
	if testing.Short() {
		t.Skip("timing scenario; runs without -short")
	}
	began := time.Now()
	adapter := newFakeAdapter(t, laggingAdapterConfig(true))
	run := startLaggingMux(t, adapter, laggingMuxOptions{
		cfg:     stepTestConfig(stepTestStartDeadline, v8classifier.ModeShadow),
		gateway: true,
	})
	w := newStepWatch(t, run.mux)
	if !w.until(began.Add(stepTestHoldBy), held) {
		t.Fatalf("active arbitration not held within %v", stepTestHoldBy)
	}
	resetAt := time.Now()
	completedBefore := run.gateway.completions()
	adapter.resetted()
	recovered := func(s adapterStepState) bool {
		return s.holdReason == "" && adapter.startsSince(resetAt) > 0 && run.gateway.completions() > completedBefore
	}
	if !w.until(began.Add(11500*time.Millisecond), recovered) {
		t.Fatalf("after RESETTED: %+v START writes=%d completed=%d->%d", adapterStepProbe(run.mux),
			adapter.startsSince(resetAt), completedBefore, run.gateway.completions())
	}
	if n := run.log.count("adaptermux: active_arbitration_hold state=cleared trigger=adapter_reset"); n != 1 {
		t.Errorf("hold clear logged %d times, want 1", n)
	}
}

// (e) Two managed generations sharing one memory: the second goes from its
// first detection straight to the hold, without a link reset.
func TestAdapterOutOfStepSharedMemoryHoldsWithoutSecondReset(t *testing.T) {
	if testing.Short() {
		t.Skip("timing scenario; runs without -short")
	}
	began := time.Now()
	memory := NewAdapterStepMemory()
	adapter := newFakeAdapter(t, laggingAdapterConfig(true))
	lost := make(chan struct{}, 1)
	first := startLaggingMux(t, adapter, laggingMuxOptions{
		cfg:            adapterStepConfig(stepTestConfig(stepTestStartDeadline, v8classifier.ModeEnforce), memory),
		gateway:        true,
		connectionLost: func() { lost <- struct{}{} },
	})
	select {
	case <-lost:
	case <-time.After(stepTestHoldBy - time.Since(began)):
		t.Fatalf("first generation reported no connection loss: %+v", adapterStepProbe(first.mux))
	}
	_ = first.mux.Close()
	second := startLaggingMux(t, adapter, laggingMuxOptions{
		cfg:     adapterStepConfig(stepTestConfig(stepTestStartDeadline, v8classifier.ModeEnforce), memory),
		gateway: true,
	})
	w := newStepWatch(t, second.mux)
	if !w.until(began.Add(11500*time.Millisecond), held) {
		t.Fatalf("second generation not held: %+v", adapterStepProbe(second.mux))
	}
	s := adapterStepProbe(second.mux)
	_, connections, _ := adapter.stats()
	if s.detections != 1 || s.linkResets != 0 || connections != 2 {
		t.Errorf("second generation %+v connections=%d, want one detection, no link reset, two connections", s, connections)
	}
	if got := memory.ActiveArbitrationHoldReason(); got != "adapter_out_of_step" {
		t.Errorf("shared memory hold reason %q", got)
	}
	if first.log.count(stepTestResetLine) != 1 || second.log.count(stepTestResetLine) != 0 || second.log.count(stepTestHoldLine) != 1 {
		t.Errorf("log lines: first reset=%d second reset=%d second hold=%d, want 1, 0, 1",
			first.log.count(stepTestResetLine), second.log.count(stepTestResetLine), second.log.count(stepTestHoldLine))
	}
}

// (e2) A retiring mux that held never clears the hold its successor set in
// the shared memory; the successor's own Close clears it.
func TestAdapterOutOfStepRetiringMuxKeepsSuccessorHold(t *testing.T) {
	if testing.Short() {
		t.Skip("runs without -short with the other generation scenarios")
	}
	memory := NewAdapterStepMemory()
	quiet := Config{Logger: log.New(io.Discard, "", 0), AdapterStepMemory: memory, LatencyHistogramReportInterval: -1}
	retiring, successor := New(quiet), New(quiet)
	if !adapterStepForceHold(retiring) {
		t.Fatal("retiring mux did not hold")
	}
	held := adapterStepForceHold(successor)
	if !held || memory.ActiveArbitrationHoldReason() != "adapter_out_of_step" {
		t.Fatalf("successor did not hold: held=%v reason=%q", held, memory.ActiveArbitrationHoldReason())
	}
	if retiring.ActiveArbitrationHoldReason() != "" {
		t.Fatal("the successor's hold is reported by the retiring mux")
	}
	_ = retiring.Close()
	if got := memory.ActiveArbitrationHoldReason(); got != "adapter_out_of_step" {
		t.Fatalf("retiring mux cleared the successor's hold: reason %q", got)
	}
	if retiring.ActiveArbitrationHoldReason() != "" || successor.ActiveArbitrationHoldReason() != "adapter_out_of_step" {
		t.Fatal("hold reported by the wrong mux")
	}
	_ = successor.Close()
	if got := memory.ActiveArbitrationHoldReason(); got != "" {
		t.Fatalf("successor's Close left the hold %q", got)
	}
}

// (f) No false detection for healthy, slow and contended adapters.
func TestAdapterOutOfStepHealthyAdapterNotDetected(t *testing.T) {
	cases := []struct {
		name    string
		adapter fakeAdapterConfig
		ext     externalClientOptions
	}{
		{name: "two_clients", ext: externalClientOptions{poll: 50 * time.Millisecond}},
		{name: "external_poll_150ms", ext: externalClientOptions{poll: 150 * time.Millisecond}},
		{name: "started_300ms_late", adapter: fakeAdapterConfig{mode: fakeAdapterStartedLate, startedDelay: 300 * time.Millisecond},
			ext: externalClientOptions{poll: 50 * time.Millisecond}},
		{name: "started_after_deadline", adapter: fakeAdapterConfig{mode: fakeAdapterStartedLate, startedDelay: 1300 * time.Millisecond},
			ext: externalClientOptions{poll: 50 * time.Millisecond}},
		{name: "abandoning_client", ext: externalClientOptions{abandon: true, poll: 50 * time.Millisecond}},
		{name: "cancelling_client", ext: externalClientOptions{cancelWithAA: true, poll: 50 * time.Millisecond}},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if i > 0 && testing.Short() {
				t.Skip("timing scenario; runs without -short")
			}
			cfg := tc.adapter
			cfg.foreignPeriod = time.Second
			adapter := newFakeAdapter(t, cfg)
			ext := tc.ext
			run := startLaggingMux(t, adapter, laggingMuxOptions{
				cfg:      stepTestConfig(time.Second, v8classifier.ModeEnforce),
				external: &ext,
				gateway:  true,
			})
			w := newStepWatch(t, run.mux)
			w.until(time.Now().Add(5*time.Second), func(adapterStepState) bool { return false })
			s := adapterStepProbe(run.mux)
			starts, connections, stray := adapter.stats()
			t.Logf("scenario=%s START writes=%d gateway completed=%d external completed=%d connections=%d stray=%d",
				tc.name, starts, run.gateway.completions(), run.external.completed.Load(), connections, stray)
			if s.detections != 0 || s.linkResets != 0 || s.holdReason != "" || connections != 1 {
				t.Errorf("healthy adapter: %+v connections=%d, want no detection, no link reset, no hold", s, connections)
			}
		})
	}
}

// Detector probes.

const outOfStepBadCyclesForTest = outOfStepBadCycles

type adapterStepState struct {
	cycle, badCycles, detections, linkResets uint64
	holdReason                               string
}

func adapterStepProbe(m *Mux) adapterStepState {
	m.stateMu.Lock()
	defer m.stateMu.Unlock()
	return adapterStepState{
		cycle:      m.step.cycle,
		badCycles:  m.step.badCycles,
		detections: m.step.detections,
		linkResets: m.step.linkResets,
		holdReason: m.ActiveArbitrationHoldReason(),
	}
}

func adapterStepSnapshot(m *Mux) (string, uint64, uint64) {
	s := m.ActiveTxnSnapshot()
	return s.ActiveArbitrationHoldReason, s.OutOfStepDetections, s.AdapterLinkResets
}

func adapterStepConfig(cfg Config, memory *AdapterStepMemory) Config {
	cfg.AdapterStepMemory = memory
	return cfg
}

func adapterStepResetAvailable(memory *AdapterStepMemory) bool {
	return memory != nil && !memory.resetSpent.Load()
}

// adapterStepForceHold spends the memory's link reset and runs three bad
// START cycles on m, which holds when the reset is spent.
func adapterStepForceHold(m *Mux) bool {
	m.stepMemory.spendReset()
	m.stateMu.Lock()
	defer m.stateMu.Unlock()
	action := adapterStepNone
	for i := 0; i < outOfStepBadCycles; i++ {
		m.stepStartCycleLocked()
		action = m.stepBadCycleLocked()
	}
	return action == adapterStepHoldSet
}
