package modbusadapter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/drivermanager"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semhub"
	modbus "github.com/Project-Helianthus/helianthus-modbus"
	modbusreg "github.com/Project-Helianthus/helianthus-modbusreg"
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
)

type withdrawalFailureJournal struct {
	*semhub.MemoryJournal
	failSource         semreg.SourceID
	failActivation     semreg.SourceID
	failActivationOnce bool
}

var errWithdrawalJournal = errors.New("synthetic withdrawal journal failure")

// managedRecoveryWatchdog only diagnoses a hang; channels order every wait.
const managedRecoveryWatchdog = 2 * time.Minute

func (j *withdrawalFailureJournal) Append(records ...semhub.EpochRecord) error {
	for _, record := range records {
		if (record.Kind == semhub.EventGenerationFenced && record.Source == j.failSource) ||
			(record.Kind == semhub.EventGenerationActivated && record.Source == j.failActivation) {
			if record.Kind == semhub.EventGenerationActivated && j.failActivationOnce {
				j.failActivation = ""
			}
			return errWithdrawalJournal
		}
	}
	return j.MemoryJournal.Append(records...)
}

func startManagedPublicationTestAdapter(t *testing.T, class *semhub.Class, scope *PublicationScope) *Adapter {
	t.Helper()
	config := validConfig()
	config.SemanticPV, config.Publication = class, scope
	client, peer := net.Pipe()
	t.Cleanup(func() { _ = client.Close(); _ = peer.Close() })
	adapter, err := Start(context.Background(), config,
		func(context.Context, string, string) (net.Conn, error) { return client, nil },
		func(modbus.TCPEndpointConfig) (Endpoint, error) { return &fakeEndpoint{}, nil })
	if err != nil {
		t.Fatal(err)
	}
	// Semantic withdrawal is explicit at each tested replacement. Teardown
	// closes only fixture resources, without introducing another generation fence.
	t.Cleanup(func() {
		if err := adapter.endpoint.Close(); err != nil {
			t.Error(err)
		}
	})
	return adapter
}

func TestManagedWithdrawalRecoversAfterPartialSourceFencing(t *testing.T) {
	for _, failure := range []string{"journal", "control"} {
		t.Run(failure, func(t *testing.T) {
			journal := &withdrawalFailureJournal{MemoryJournal: semhub.NewMemoryJournal()}
			var failAsset semreg.AssetID
			hub, err := semhub.New(semhub.Options{Journal: journal, Faults: func(stage string, asset semreg.AssetID) {
				if stage == "before-swap" && asset != "" && asset == failAsset {
					panic("synthetic withdrawal interruption")
				}
			}})
			if err != nil {
				t.Fatal(err)
			}
			class, err := NewSemanticPVClass(hub)
			if err != nil {
				t.Fatal(err)
			}
			scope := &PublicationScope{Epoch: "source-epoch:pv:partial-withdrawal", Generation: 1}
			adapter := startManagedPublicationTestAdapter(t, class, scope)
			words := observedFroniusFloatWords()
			for i, nativeScope := range []string{"fixture:withdrawal-a", "fixture:withdrawal-b"} {
				rotated := append([]uint16(nil), words...)
				if i != 0 {
					putSunSpecString(rotated[52:68], "fixture-identity-b")
				}
				observation := pvCoreScopedObservation(t, rotated, uint64(i+1), uint64(i+2), nativeScope)
				if err := adapter.RecordSunSpecQualificationObservation(observation); err != nil {
					t.Fatal(err)
				}
			}
			sources := adapter.semanticPV.sortedHandleSources()
			if len(sources) != 2 {
				t.Fatalf("sources=%d", len(sources))
			}
			if failure == "journal" {
				journal.failSource = sources[1]
			} else {
				for id, asset := range adapter.semanticPV.assets {
					if asset.current.snapshot.Sources[0].SourceID == sources[1] {
						failAsset = id
					}
				}
				if failAsset == "" {
					t.Fatal("failure target unavailable")
				}
			}
			if err := adapter.Close(); err == nil {
				t.Fatal("partial withdrawal failure hidden")
			}
			if adapter.semanticPV.managedWithdrawn {
				t.Fatal("partial cleanup marked complete")
			}
			journal.failSource, failAsset = "", ""
			if err := adapter.Close(); err != nil {
				t.Fatalf("withdrawal retry after recovery: %v", err)
			}
			counts := make(map[semreg.SourceID]int)
			for _, record := range journal.Records() {
				if record.Kind == semhub.EventGenerationFenced {
					counts[record.Source]++
				}
			}
			for _, source := range sources {
				if counts[source] != 1 {
					t.Fatalf("source %s fenced %d times", source, counts[source])
				}
			}
			successor := startManagedPublicationTestAdapter(t, class, &PublicationScope{Previous: scope, Epoch: scope.Epoch, Generation: 2})
			if len(successor.semanticPV.handles) != 2 {
				t.Fatal("replacement did not activate both sources")
			}
		})
	}
}

func TestManagedReplacementRetainsRefreshEvidenceAcrossIdentities(t *testing.T) {
	class := testSemanticPVClass()
	scope := &PublicationScope{Epoch: "source-epoch:pv:retained-identities", Generation: 1}
	adapter := startManagedPublicationTestAdapter(t, class, scope)
	words := observedFroniusFloatWords()
	observations := make([]modbusreg.SunSpecQualificationObservation, 0, 2)
	assets := make([]semreg.AssetID, 0, 2)
	for i := 0; i < 2; i++ {
		if i != 0 {
			putSunSpecString(words[52:68], "fixture-identity-b")
		}
		observation := pvCoreObservation(t, words, uint64(i+1), uint64(i+2))
		if err := adapter.RecordSunSpecCurrentObservation(observation); err != nil {
			t.Fatal(err)
		}
		observations = append(observations, observation)
		assets = append(assets, adapter.semanticPV.activeAssetID)
	}
	for generation := uint64(2); generation <= 3; generation++ {
		if err := adapter.Close(); err != nil {
			t.Fatal(err)
		}
		scope = &PublicationScope{Previous: scope, Epoch: scope.Epoch, Generation: generation}
		adapter = startManagedPublicationTestAdapter(t, class, scope)
		if err := adapter.RecordSunSpecCurrentObservation(pvCoreObservation(t, words, generation+10, generation+20)); err != nil {
			t.Fatal(err)
		}
		for i, observation := range observations {
			inspected, err := class.Hub().Inspect(assets[i])
			if err != nil || !inspected.Present {
				t.Fatalf("retained hub snapshot missing: %v", err)
			}
			if _, _, ok := adapter.SunSpecQualificationObservation(observation.Capability().ProfileID(), observation.SampleID()); !ok {
				t.Fatalf("generation %d pruned evidence for retained identity %d", generation, i)
			}
		}
	}
}

type managedActivationResult struct {
	adapter *Adapter
	err     error
}

// Keep the same lifecycle-bound configuration for every manager-owned retry.
type managedActivationRuntime struct {
	mu         sync.Mutex
	config     Config
	generation atomic.Uint64
	adapter    *Adapter
	results    chan managedActivationResult
}

func (r *managedActivationRuntime) Start(ctx context.Context) (uint64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	generation := r.generation.Add(1)
	client, peer := net.Pipe()
	defer func() { _ = peer.Close() }()
	config := r.config
	config.Publication = &PublicationScope{Previous: r.config.Publication.Previous, Epoch: r.config.Publication.Epoch, Generation: r.config.Publication.Generation}
	adapter, err := Start(ctx, config,
		func(context.Context, string, string) (net.Conn, error) { return client, nil },
		func(modbus.TCPEndpointConfig) (Endpoint, error) { return &fakeEndpoint{}, nil })
	_ = client.Close()
	r.adapter = adapter
	r.results <- managedActivationResult{adapter, err}
	return generation, err
}
func (r *managedActivationRuntime) Replace(ctx context.Context) (uint64, error) {
	if err := r.Stop(ctx); err != nil {
		return 0, err
	}
	return r.Start(ctx)
}
func (r *managedActivationRuntime) Stop(context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.adapter.Close()
}
func (r *managedActivationRuntime) Generation() uint64      { return r.generation.Load() }
func (r *managedActivationRuntime) Revision() uint64        { return r.generation.Load() }
func (r *managedActivationRuntime) SafetyQuarantined() bool { return false }

func TestManagedPartialActivationRecoversSameEpoch(t *testing.T) {
	journal := &withdrawalFailureJournal{MemoryJournal: semhub.NewMemoryJournal()}
	hub, err := semhub.New(semhub.Options{Journal: journal})
	if err != nil {
		t.Fatal(err)
	}
	class, err := NewSemanticPVClass(hub)
	if err != nil {
		t.Fatal(err)
	}
	scope := &PublicationScope{Epoch: "source-epoch:pv:partial-activation", Generation: 1}
	adapter := startManagedPublicationTestAdapter(t, class, scope)
	words := observedFroniusFloatWords()
	for i, nativeScope := range []string{"fixture:activation-a", "fixture:activation-b"} {
		rotated := append([]uint16(nil), words...)
		if i != 0 {
			putSunSpecString(rotated[52:68], "fixture-identity-b")
		}
		if err := adapter.RecordSunSpecQualificationObservation(pvCoreScopedObservation(t, rotated, uint64(i+1), uint64(i+2), nativeScope)); err != nil {
			t.Fatal(err)
		}
	}
	sources := adapter.semanticPV.sortedHandleSources()
	if len(sources) != 2 {
		t.Fatalf("sources=%d", len(sources))
	}
	if err := adapter.Close(); err != nil {
		t.Fatal(err)
	}
	journal.failActivation = sources[1]
	config := validConfig()
	config.SemanticPV = class
	config.Publication = &PublicationScope{Previous: scope, Epoch: scope.Epoch, Generation: 2}
	journal.failActivationOnce = true
	runtime := &managedActivationRuntime{config: config, results: make(chan managedActivationResult, 2)}
	manager, err := drivermanager.New(drivermanager.Config{Drivers: []drivermanager.DriverConfig{{
		ID: "pv.primary", Enabled: true, Runtime: runtime,
		ClassifyError: func(error) drivermanager.Failure {
			return drivermanager.Failure{Reason: drivermanager.Reason{Code: drivermanager.ReasonDependencyUnavailable, Retryable: true}}
		},
		Retry: drivermanager.RetryPolicy{Budget: 1, InitialDelay: time.Millisecond, MaxDelay: time.Millisecond, JitterRatio: -1},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	// The runtime's result channel orders this test. Neither progress nor
	// shutdown shares a wall-clock budget; the watchdog only diagnoses a hang.
	ctx := context.Background()
	defer func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), managedRecoveryWatchdog)
		defer shutdownCancel()
		if err := manager.Shutdown(shutdownCtx); err != nil {
			t.Error(err)
		}
	}()
	if err := manager.Start(ctx, "pv.primary"); err != nil {
		t.Fatal(err)
	}
	var results [2]managedActivationResult
	watchdog := time.NewTimer(managedRecoveryWatchdog)
	defer watchdog.Stop()
	for i := range results {
		select {
		case results[i] = <-runtime.results:
		case <-watchdog.C:
			t.Fatal("hang diagnostic: manager did not finish its first retry")
		}
	}
	if results[0].adapter != nil || !errors.Is(results[0].err, errWithdrawalJournal) {
		t.Fatalf("partial activation result=%v err=%v", results[0].adapter, results[0].err)
	}
	if results[1].err != nil || results[1].adapter == nil {
		t.Fatalf("manager retry failed: %v", results[1].err)
	}
	successor := results[1].adapter
	activated, fenced := false, false
	for _, record := range journal.Records() {
		if record.Source == sources[0] && record.Generation == 2 {
			activated = activated || record.Kind == semhub.EventGenerationActivated
			fenced = fenced || record.Kind == semhub.EventGenerationFenced
		}
	}
	counts := make(map[semreg.SourceID]int)
	for _, record := range journal.Records() {
		if record.Kind == semhub.EventGenerationActivated && record.Generation == 2 {
			counts[record.Source]++
		}
	}
	if counts[sources[0]] != 1 || counts[sources[1]] != 0 {
		t.Fatalf("partial activation repeated or skipped: %v", counts)
	}
	if runtime.Generation() != 2 {
		t.Fatalf("manager attempts=%d, want 2", runtime.Generation())
	}
	if !activated || !fenced {
		t.Fatal("partial activation did not activate and clean up the first source")
	}
	for _, source := range sources {
		handle := successor.semanticPV.handles[source]
		if handle == nil || handle.Generation() != 3 {
			t.Fatal("retry did not recover every source in the same epoch")
		}
	}
	if err := successor.RecordSunSpecQualificationObservation(pvCoreScopedObservation(t, words, 10, 11, "fixture:activation-a")); err != nil {
		t.Fatal(err)
	}
	if _, ok := successor.SemanticPVCurrentSingle(); !ok {
		t.Fatal("recovered generation did not publish")
	}
}

func TestManagedInheritedReplayCoverDoesNotRepublish(t *testing.T) {
	var (
		mu     sync.Mutex
		target semreg.AssetID
	)
	hub, err := semhub.New(semhub.Options{Faults: func(stage string, asset semreg.AssetID) {
		mu.Lock()
		hit := stage == "before-swap" && asset != "" && asset == target
		if hit {
			target = ""
		}
		mu.Unlock()
		if hit {
			panic("injected cover interruption")
		}
	}})
	if err != nil {
		t.Fatal(err)
	}
	class, err := NewSemanticPVClass(hub)
	if err != nil {
		t.Fatal(err)
	}
	core, err := newPVPublicationCore(class)
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	current := started
	adapter := &Adapter{semanticPV: core, pvSourceEpoch: "source-epoch:pv:cover-a", startedWall: started.UTC(), startedMono: started,
		wallNow: func() time.Time { return current }, monotonicNow: func() time.Time { return current },
		qualifications: make(map[string]sunSpecQualificationRecord), profiles: make(map[string]ProfileObservationRecord), refreshEvidence: make(map[string]sunSpecQualificationRecord)}
	firstWords := observedFroniusFloatControlsWords()
	first := pvCoreObservation(t, firstWords, 1, 2)
	if err := adapter.RecordSunSpecQualificationObservation(first); err != nil {
		t.Fatal(err)
	}
	firstIdentity, _, err := resolvePVPublicationIdentity(first)
	if err != nil {
		t.Fatal(err)
	}
	firstAsset := semreg.AssetID("pv-asset-" + pvCoreRawHash(firstIdentity)[:32])
	current = current.Add(time.Second)
	rotatedWords := append([]uint16(nil), firstWords...)
	putSunSpecString(rotatedWords[52:68], "rotated-identity")
	if err := adapter.RecordSunSpecQualificationObservation(pvCoreObservation(t, rotatedWords, 3, 4)); err != nil {
		t.Fatal(err)
	}
	// A new epoch on the rotated asset retires the old epoch on the first
	// asset through a cover, whose submission is interrupted.
	current = current.Add(time.Second)
	adapter.startedWall = current.UTC()
	adapter.pvSourceEpoch = "source-epoch:pv:cover-b"
	mu.Lock()
	target = firstAsset
	mu.Unlock()
	observation := pvCoreObservation(t, rotatedWords, 5, 6)
	key := observation.Capability().ProfileID() + "\x00" + observation.SampleID()
	encoded, err := json.Marshal(observation)
	if err != nil {
		t.Fatal(err)
	}
	adapter.qualifications[key] = sunSpecQualificationRecord{observation: observation, encoded: encoded}
	adapter.replayQualifications = map[string]bool{key: true}
	err = adapter.RecordSunSpecQualificationObservation(observation)
	var cover *semhub.CoverError
	if !errors.As(err, &cover) {
		t.Fatalf("record with an interrupted cover=%v, want a CoverError", err)
	}
	adapter.profileMu.Lock()
	_, retained := adapter.qualifications[key]
	adapter.profileMu.Unlock()
	if !retained {
		t.Fatal("the accepted publication's staged evidence was deleted")
	}
	if hub.Readable(firstAsset) {
		t.Fatal("the asset behind the interrupted cover is readable")
	}
	before, ok := adapter.SemanticPVCurrentSingle()
	if !ok {
		t.Fatal("accepted replay snapshot missing")
	}
	if adapter.replayQualifications[key] {
		t.Fatal("accepted cover retained replay admission")
	}
	if err := adapter.RecordSunSpecQualificationObservation(observation); err != nil {
		t.Fatal(err)
	}
	after, ok := adapter.SemanticPVCurrentSingle()
	if !ok || !bytes.Equal(before.Canonical, after.Canonical) {
		t.Fatal("duplicate replay advanced accepted snapshot")
	}
	waitForPV(t, func() bool { return hub.Readable(firstAsset) })
	core.lifecycleMu.Lock()
	pending := len(core.covers)
	core.lifecycleMu.Unlock()
	if pending != 0 {
		t.Fatalf("covers still pending: %d", pending)
	}
}

type managedDrainingEndpoint struct {
	Endpoint
	entered chan struct{}
	release chan struct{}
}

func (e *managedDrainingEndpoint) Close() error {
	close(e.entered)
	<-e.release
	return nil
}
func TestManagedCloseRevokesAdmissionBeforeEndpointDrain(t *testing.T) {
	adapter := startManagedPublicationTestAdapter(t, testSemanticPVClass(), &PublicationScope{Epoch: "source-epoch:pv:draining", Generation: 1})
	endpoint := &managedDrainingEndpoint{Endpoint: adapter.endpoint, entered: make(chan struct{}), release: make(chan struct{})}
	adapter.endpoint = endpoint
	done := make(chan error, 1)
	go func() { done <- adapter.Close() }()
	<-endpoint.entered
	adapter.connectionMu.RLock()
	closed := adapter.closed
	adapter.connectionMu.RUnlock()
	observation := pvCoreObservation(t, observedFroniusFloatWords(), 1, 2)
	if err := adapter.RecordSunSpecQualificationObservation(observation); err == nil {
		t.Error("draining adapter admitted observation")
	}
	if !closed {
		t.Error("draining adapter still admits endpoint work")
	}
	close(endpoint.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	// Fixture cleanup closes the original endpoint directly.
	adapter.endpoint = endpoint.Endpoint
}

type startupCloseErrorConn struct {
	net.Conn
	closeErr error
}

func (c *startupCloseErrorConn) Close() error {
	_ = c.Conn.Close()
	return c.closeErr
}

type startupFailureEndpoint struct {
	fakeEndpoint
	afterOpen func()
}

func (e *startupFailureEndpoint) OpenConnection(c net.Conn) (modbus.TCPConnectionHandle, error) {
	handle, err := e.fakeEndpoint.OpenConnection(c)
	if e.afterOpen != nil {
		e.afterOpen()
	}
	return handle, err
}

func TestManagedStartupConnectionCleanup(t *testing.T) {
	openErr := errors.New("synthetic open failure")
	closeErr := errors.New("synthetic connection close failure")
	for _, tc := range []struct {
		name     string
		path     string
		closeErr error
		unproven bool
	}{
		{"open failure", "open", closeErr, true},
		{"caller cancelled", "cancel", closeErr, true},
		{"scope withdrawn", "withdraw", closeErr, true},
		{"source epoch admission", "admission", closeErr, true},
		{"already closed socket", "open", fmt.Errorf("socket cleanup: %w", net.ErrClosed), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			config := validConfig()
			config.Publication = &PublicationScope{Epoch: "source-epoch:pv:startup-cleanup", Generation: 1}
			endpoint := &startupFailureEndpoint{}
			var cause error
			switch tc.path {
			case "open":
				endpoint.openErr = openErr
				cause = openErr
			case "cancel":
				endpoint.afterOpen = cancel
				cause = context.Canceled
			case "withdraw":
				endpoint.afterOpen = config.Publication.Withdraw
			case "admission":
				config.Publication.Epoch = "invalid epoch"
				cause = semhub.ErrAdmission
			}
			client, peer := net.Pipe()
			defer func() { _ = peer.Close() }()
			connection := &startupCloseErrorConn{Conn: client, closeErr: tc.closeErr}
			adapter, err := Start(ctx, config,
				func(context.Context, string, string) (net.Conn, error) { return connection, nil },
				func(modbus.TCPEndpointConfig) (Endpoint, error) { return endpoint, nil })
			if adapter != nil || err == nil {
				t.Fatalf("failed startup adapter=%v error=%v", adapter, err)
			}
			if cause != nil && !errors.Is(err, cause) {
				t.Errorf("startup lost failure cause: %v", err)
			}
			if errors.Is(err, ErrStartupCloseUnproven) != tc.unproven {
				t.Errorf("unproven cleanup=%v, want %v: %v", errors.Is(err, ErrStartupCloseUnproven), tc.unproven, err)
			}
			if errors.Is(err, ErrStartupNativeCloseUnproven) != tc.unproven {
				t.Errorf("unproven native cleanup=%v, want %v: %v", errors.Is(err, ErrStartupNativeCloseUnproven), tc.unproven, err)
			}
			if tc.unproven && !errors.Is(err, tc.closeErr) {
				t.Errorf("startup lost connection close failure: %v", err)
			}
			if endpoint.closeCalls != 1 {
				t.Errorf("endpoint closes=%d, want 1", endpoint.closeCalls)
			}
		})
	}
}

func TestM03ManagedInjectedClockPublishesAndWithdraws(t *testing.T) {
	config := validConfig()
	current := time.Date(2001, time.January, 1, 0, 0, 0, 0, time.UTC)
	config.Clock = func() time.Time { return current }
	config.SemanticPV = testSemanticPVClass()
	config.Publication = &PublicationScope{Epoch: "source-epoch:pv:synthetic-clock", Generation: 1}
	client, peer := net.Pipe()
	defer func() { _ = client.Close(); _ = peer.Close() }()
	adapter, err := Start(context.Background(), config,
		func(context.Context, string, string) (net.Conn, error) { return client, nil },
		func(modbus.TCPEndpointConfig) (Endpoint, error) { return &fakeEndpoint{}, nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := adapter.Close(); err != nil {
			t.Error(err)
		}
	})
	current = current.Add(time.Second)
	if elapsed := adapter.monotonicNow().Sub(adapter.startedMono); elapsed != time.Second {
		t.Errorf("elapsed=%v, want one second", elapsed)
	}
	observation := pvCoreObservation(t, observedFroniusFloatWords(), 1, 2)
	if err := adapter.RecordSunSpecQualificationObservation(observation); err != nil {
		t.Fatal(err)
	}
	if _, ok := adapter.SemanticPVCurrentSingle(); !ok {
		t.Fatal("managed publication missing")
	}
	current = current.Add(time.Second)
	if err := adapter.Close(); err != nil {
		t.Fatalf("withdrawal=%v", err)
	}
	if _, ok := adapter.SemanticPVCurrentSingle(); ok {
		t.Fatal("closed publication remains available")
	}
}

func managedRecoveryWait[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	watchdog := time.NewTimer(managedRecoveryWatchdog)
	defer watchdog.Stop()
	select {
	case value := <-ch:
		return value
	case <-watchdog.C:
		t.Fatalf("hang diagnostic: %s", what)
		panic("unreachable")
	}
}

// independentCleanupJournal fails one source's successor activation, reports
// the compensating fence and can hold it.
type independentCleanupJournal struct {
	*semhub.MemoryJournal
	failActivation semreg.SourceID
	holdFence      bool
	fenceEntered   chan struct{}
	fenceRelease   chan struct{}
	once           sync.Once
}

func (j *independentCleanupJournal) Append(records ...semhub.EpochRecord) error {
	for _, record := range records {
		if record.Generation != 2 {
			continue
		}
		if record.Kind == semhub.EventGenerationActivated && record.Source == j.failActivation {
			return errWithdrawalJournal
		}
		if record.Kind == semhub.EventGenerationFenced {
			j.once.Do(func() { close(j.fenceEntered) })
			if j.holdFence {
				<-j.fenceRelease
			}
		}
	}
	return j.MemoryJournal.Append(records...)
}

type heldCloseEndpoint struct {
	fakeEndpoint
	closeEntered chan struct{}
	closeRelease chan struct{}
	closeErr     error
}

func (e *heldCloseEndpoint) Close() error {
	close(e.closeEntered)
	<-e.closeRelease
	return e.closeErr
}

// A same-epoch activation fails after one source advanced. The compensating
// fence and the provisional native close are independent obligations: a held
// native close does not delay the fence, and a held fence does not hide a
// native close failure.
func TestM03PartialActivationCleanupObligationsProgressIndependently(t *testing.T) {
	for _, held := range []string{"native close", "fence"} {
		t.Run(held, func(t *testing.T) {
			journal := &independentCleanupJournal{MemoryJournal: semhub.NewMemoryJournal(), fenceEntered: make(chan struct{}), fenceRelease: make(chan struct{})}
			hub, err := semhub.New(semhub.Options{Journal: journal})
			if err != nil {
				t.Fatal(err)
			}
			class, err := NewSemanticPVClass(hub)
			if err != nil {
				t.Fatal(err)
			}
			scope := &PublicationScope{Epoch: "source-epoch:pv:independent-cleanup", Generation: 1}
			adapter := startManagedPublicationTestAdapter(t, class, scope)
			words := observedFroniusFloatWords()
			for i, nativeScope := range []string{"fixture:independent-a", "fixture:independent-b"} {
				rotated := append([]uint16(nil), words...)
				if i != 0 {
					putSunSpecString(rotated[52:68], "fixture-identity-b")
				}
				if err := adapter.RecordSunSpecQualificationObservation(pvCoreScopedObservation(t, rotated, uint64(i+1), uint64(i+2), nativeScope)); err != nil {
					t.Fatal(err)
				}
			}
			sources := adapter.semanticPV.sortedHandleSources()
			if len(sources) != 2 {
				t.Fatalf("sources=%d", len(sources))
			}
			if err := adapter.Close(); err != nil {
				t.Fatal(err)
			}
			closeErr := errors.New("synthetic provisional endpoint close failure")
			endpoint := &heldCloseEndpoint{closeEntered: make(chan struct{}), closeRelease: make(chan struct{})}
			journal.failActivation = sources[1]
			release := func() { close(endpoint.closeRelease) }
			if held == "fence" {
				journal.holdFence = true
				endpoint.closeErr = closeErr
				close(endpoint.closeRelease)
				release = func() { close(journal.fenceRelease) }
			}
			var releaseOnce sync.Once
			t.Cleanup(func() { releaseOnce.Do(release) })
			config := validConfig()
			config.SemanticPV = class
			successor := &PublicationScope{Previous: scope, Epoch: scope.Epoch, Generation: 2}
			config.Publication = successor
			client, peer := net.Pipe()
			t.Cleanup(func() { _ = client.Close(); _ = peer.Close() })
			type startResult struct {
				adapter *Adapter
				err     error
			}
			done := make(chan startResult, 1)
			go func() {
				started, err := Start(context.Background(), config,
					func(context.Context, string, string) (net.Conn, error) { return client, nil },
					func(modbus.TCPEndpointConfig) (Endpoint, error) { return endpoint, nil })
				done <- startResult{started, err}
			}()
			if held == "native close" {
				managedRecoveryWait(t, endpoint.closeEntered, "provisional native close was not entered")
				managedRecoveryWait(t, journal.fenceEntered, "compensating fence waited for the held native close")
				if successor.NativeCloseUnconfirmed() {
					t.Error("pending native close reported as failed")
				}
			} else {
				managedRecoveryWait(t, journal.fenceEntered, "compensating fence was not entered")
				watchdog := time.NewTimer(managedRecoveryWatchdog)
				defer watchdog.Stop()
				for !successor.NativeCloseUnconfirmed() {
					select {
					case <-watchdog.C:
						t.Fatal("hang diagnostic: native close failure hidden behind the held fence")
					case <-time.After(time.Millisecond):
					}
				}
			}
			select {
			case result := <-done:
				t.Fatalf("Start returned while the %s was held: %v", held, result.err)
			default:
			}
			releaseOnce.Do(release)
			result := managedRecoveryWait(t, done, "Start did not return after release")
			if result.adapter != nil || !errors.Is(result.err, errWithdrawalJournal) {
				t.Fatalf("partial activation adapter=%v error=%v", result.adapter, result.err)
			}
			if held == "native close" {
				if errors.Is(result.err, ErrStartupCloseUnproven) || successor.NativeCloseUnconfirmed() {
					t.Fatalf("proven cleanup reported unproven: %v", result.err)
				}
			} else if !errors.Is(result.err, closeErr) || !errors.Is(result.err, ErrStartupNativeCloseUnproven) || !successor.NativeCloseUnconfirmed() {
				t.Fatalf("native close failure lost: %v", result.err)
			}
			fenced := 0
			for _, record := range journal.Records() {
				if record.Kind == semhub.EventGenerationFenced && record.Source == sources[0] && record.Generation == 2 {
					fenced++
				}
			}
			if fenced != 1 {
				t.Fatalf("partial activation fences=%d, want 1", fenced)
			}
		})
	}
}

// A failed provisional connection close is latched before the endpoint close
// can block, so the generation reports an unconfirmed close meanwhile.
func TestM03ProvisionalCloseFailureLatchedBeforeNextClose(t *testing.T) {
	config := validConfig()
	scope := &PublicationScope{Epoch: "source-epoch:pv:provisional-latch", Generation: 1}
	config.Publication = scope
	openErr := errors.New("synthetic open failure")
	closeErr := errors.New("synthetic connection close failure")
	endpoint := &heldCloseEndpoint{closeEntered: make(chan struct{}), closeRelease: make(chan struct{})}
	endpoint.openErr = openErr
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(endpoint.closeRelease) }) }
	t.Cleanup(release)
	client, peer := net.Pipe()
	t.Cleanup(func() { _ = client.Close(); _ = peer.Close() })
	connection := &startupCloseErrorConn{Conn: client, closeErr: closeErr}
	done := make(chan error, 1)
	go func() {
		_, err := Start(context.Background(), config,
			func(context.Context, string, string) (net.Conn, error) { return connection, nil },
			func(modbus.TCPEndpointConfig) (Endpoint, error) { return endpoint, nil })
		done <- err
	}()
	managedRecoveryWait(t, endpoint.closeEntered, "provisional endpoint close was not entered")
	if !scope.NativeCloseUnconfirmed() {
		t.Error("connection close failure hidden behind the held endpoint close")
	}
	release()
	err := managedRecoveryWait(t, done, "Start did not return after release")
	if !errors.Is(err, openErr) || !errors.Is(err, closeErr) || !errors.Is(err, ErrStartupNativeCloseUnproven) || !scope.NativeCloseUnconfirmed() {
		t.Fatalf("provisional cleanup error=%v unconfirmed=%v", err, scope.NativeCloseUnconfirmed())
	}
}
