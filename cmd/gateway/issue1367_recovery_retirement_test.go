package main

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/drivermanager"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimecontrol"
	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
)

type issue1367LineSink struct {
	mu    sync.Mutex
	lines []string
}

func (s *issue1367LineSink) logf(format string, args ...any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lines = append(s.lines, strings.TrimSpace(fmt.Sprintf(format, args...)))
}

func (s *issue1367LineSink) snapshot() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.lines...)
}

func issue1367RetirementRuntime(t *testing.T, sink *issue1367LineSink, desiredRunning bool) *ebusDriverRuntime {
	t.Helper()
	factory := func(ctx context.Context) (*transport.ManagedRawTransport, error) {
		raw := newIssue851BlockingRawTransport()
		return newManagedEBusGeneration(ctx, raw, raw.Close), nil
	}
	seam := transport.NewDriverRuntime(factory, transport.DriverRuntimeConfig{DrainTimeout: 100 * time.Millisecond})
	runtime := &ebusDriverRuntime{runtime: seam, desiredRunning: func() bool { return desiredRunning }}
	runtime.recovery.budget = 5
	runtime.recovery.logf = sink.logf
	return runtime
}

// A manager-owned retirement of an unadmitted generation (Stop while the
// manager still wants RUNNING) must keep the series, so the retry through
// Start is logged as the next attempt and the admission closes the series.
func TestIssue1367RecoverySeriesSurvivesHeldAdmissionRetirement(t *testing.T) {
	sink := &issue1367LineSink{}
	runtime := issue1367RetirementRuntime(t, sink, true)
	runtime.recovery.reportLoss(1, ebusLossAdapterConnection, func() bool { return true })

	attempt := runtime.recovery.attemptStarted()
	generation, err := runtime.runtime.Replace(context.Background())
	runtime.recovery.attemptFinished(attempt, generation, err)
	if err != nil {
		t.Fatalf("Replace: %v", err)
	}
	if err := runtime.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	generation, err = runtime.Start(context.Background())
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	runtime.recovery.admitted(generation)

	text := strings.Join(sink.snapshot(), "\n")
	for _, want := range []string{
		"recovery attempt 1/5 (reason=adapter_connection_lost)",
		"recovery attempt 2/5 (reason=retry_after_admission_refused)",
		"recovery attempt 2/5 reconnected",
		"recovered on attempt 2/5",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("log lacks %q:\n%s", want, text)
		}
	}
}

// An explicit stop (the manager no longer wants RUNNING) ends the series, and
// the next Start is a fresh start, not an attempt.
func TestIssue1367ExplicitStopEndsRecoverySeries(t *testing.T) {
	sink := &issue1367LineSink{}
	runtime := issue1367RetirementRuntime(t, sink, false)
	runtime.recovery.reportLoss(1, ebusLossAdapterConnection, func() bool { return true })
	if err := runtime.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if _, err := runtime.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	for _, line := range sink.snapshot() {
		if strings.Contains(line, "recovery attempt") {
			t.Fatalf("explicit stop left the series open: %q", line)
		}
	}
}

// The recovered line must precede the RUNNING watch line of the same
// generation, whatever the order BindCorrelation and the watch run in.
func TestIssue1367RecoveredLogPrecedesRunningWatchLog(t *testing.T) {
	sink := &issue1367LineSink{}
	recovery := &ebusRecoveryLog{budget: 5, logf: sink.logf}
	recovery.reportLoss(1, ebusLossAdapterConnection, func() bool { return true })
	attempt := recovery.attemptStarted()
	recovery.attemptFinished(attempt, 2, nil)

	published := 0
	forwarder := &ebusNativeForwarder{logf: sink.logf, recovery: recovery}
	forwarder.last = runtimecontrol.NativeObservation{State: drivermanager.ObservedBackoff, Reason: drivermanager.ReasonRetryScheduled}
	forwarder.logTransition(runtimecontrol.LifecycleCorrelation{}, runtimecontrol.NativeObservation{State: drivermanager.ObservedRunning, Reason: drivermanager.ReasonNone}, drivermanager.Snapshot{Generation: 2}, func() { published++ })
	// The watch won the race: completion waits for the health binding.
	for _, line := range sink.snapshot() {
		if strings.Contains(line, "recovered on attempt") || strings.Contains(line, "lifecycle RUNNING") {
			t.Fatalf("logged before health binding: %q", line)
		}
	}
	if published != 0 {
		t.Fatalf("RUNNING published to the service before the health binding")
	}
	recovery.admitted(2)
	if published != 1 {
		t.Fatalf("RUNNING published %d times after the health binding; want 1", published)
	}

	lines := sink.snapshot()
	recovered, running := -1, -1
	for i, line := range lines {
		switch {
		case strings.Contains(line, "recovered on attempt"):
			recovered = i
		case strings.Contains(line, "lifecycle RUNNING"):
			running = i
		}
	}
	if recovered < 0 || running < 0 || recovered > running {
		t.Fatalf("recovered=%d running=%d; lines:\n%s", recovered, running, strings.Join(lines, "\n"))
	}
	count := 0
	for _, line := range lines {
		if strings.Contains(line, "recovered on attempt") {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("recovered logged %d times", count)
	}
}

// A loss replayed at binding after the watch saw RUNNING must not be logged
// as a recovery, and the series must restart without being reset by the watch.
func TestIssue1367WatchRunningBeforeBindingWithReplayedLossIsNotRecovery(t *testing.T) {
	sink := &issue1367LineSink{}
	recovery := &ebusRecoveryLog{budget: 5, logf: sink.logf}
	recovery.reportLoss(1, ebusLossAdapterConnection, func() bool { return true })
	attempt := recovery.attemptStarted()
	recovery.attemptFinished(attempt, 2, nil)

	published := 0
	forwarder := &ebusNativeForwarder{logf: sink.logf, recovery: recovery}
	forwarder.last = runtimecontrol.NativeObservation{State: drivermanager.ObservedBackoff, Reason: drivermanager.ReasonRetryScheduled}
	forwarder.logTransition(runtimecontrol.LifecycleCorrelation{}, runtimecontrol.NativeObservation{State: drivermanager.ObservedRunning, Reason: drivermanager.ReasonNone}, drivermanager.Snapshot{Generation: 2}, func() { published++ })
	// Binding replays the retained loss, then completes.
	recovery.reportLoss(2, ebusLossAdapterConnection, func() bool { return true })
	recovery.admitted(2)
	if published != 0 {
		t.Fatalf("RUNNING of a generation whose loss was replayed at binding was published %d times", published)
	}

	text := strings.Join(sink.snapshot(), "\n")
	if strings.Contains(text, "recovered on attempt") {
		t.Fatalf("replayed loss reported as recovery:\n%s", text)
	}
	if !recovery.continuing() {
		t.Fatalf("series was reset by the watch:\n%s", text)
	}
	if !strings.Contains(text, "lifecycle RUNNING") {
		t.Fatalf("RUNNING line lost:\n%s", text)
	}
}
