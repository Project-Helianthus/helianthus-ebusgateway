package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/drivermanager"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/modbusadapter"
	sunspectest "github.com/Project-Helianthus/helianthus-ebusgateway/internal/modbusadapter/testfixture"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimecontrol"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semhub"
	modbus "github.com/Project-Helianthus/helianthus-modbus"
	modbusreg "github.com/Project-Helianthus/helianthus-modbusreg"
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
)

type m03Endpoint struct {
	modbusRunLifecycleEndpoint
	opens  atomic.Int32
	closes atomic.Int32
}

func (e *m03Endpoint) OpenConnection(c net.Conn) (modbus.TCPConnectionHandle, error) {
	e.opens.Add(1)
	_ = c.Close()
	return modbus.TCPConnectionHandle{}, nil
}
func (e *m03Endpoint) Close() error { e.closes.Add(1); return e.closeErr }

func m03Construction() runtimecontrol.Construction {
	return runtimecontrol.Construction{DriverID: "pv.primary", Sources: []runtimecontrol.SourceID{"pv.configured"}}
}
func m03Correlation() runtimecontrol.LifecycleCorrelation {
	return runtimecontrol.LifecycleCorrelation{DriverID: "pv.primary", SourceID: "pv.configured", Epoch: "epoch:synthetic-a", Generation: 1, Sequence: 1}
}
func m03Driver(t *testing.T, enabled bool, factory modbusadapter.Factory, work modbusManagedWorkFactory) *modbusManagedDriver {
	t.Helper()
	cfg := ebusgateway.ModbusTCPConfig{}
	if enabled {
		cfg = ebusgateway.ModbusTCPConfig{Enabled: true, Endpoint: "tcp://fixture.invalid:502", DialTimeout: time.Second}
	}
	dial := func(context.Context, string, string) (net.Conn, error) {
		a, b := net.Pipe()
		_ = b.Close()
		return a, nil
	}
	adapter := newModbusManagedDriverAdapter(cfg, newTestSemanticHub(t), dial, factory, work)
	runtime, err := adapter.Construct(context.Background(), m03Construction())
	if err != nil {
		t.Fatal(err)
	}
	driver := runtime.(*modbusManagedDriver)
	if err := driver.BindLifecycle(m03Correlation()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = driver.Stop(ctx)
	})
	return driver
}

func TestM03ManagerDesiredStateAndGeneration(t *testing.T) {
	var endpoints []*m03Endpoint
	var calls atomic.Int32
	driver := m03Driver(t, true, func(cfg modbus.TCPEndpointConfig) (modbusadapter.Endpoint, error) {
		calls.Add(1)
		if cfg.RuntimeAcquisitionSource == nil || cfg.SchedulerLimits.MaxRetryAttempts != 2 {
			t.Error("read-only bounded acquisition configuration lost")
		}
		e := &m03Endpoint{}
		endpoints = append(endpoints, e)
		return e, nil
	}, nil)
	if calls.Load() != 0 {
		t.Fatal("construction opened endpoint")
	}
	manager, err := drivermanager.New(drivermanager.Config{Drivers: []drivermanager.DriverConfig{{ID: "pv.primary", Enabled: true, Runtime: driver}}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for i := 0; i < 2; i++ {
		if err := manager.Start(ctx, "pv.primary"); err != nil {
			t.Fatal(err)
		}
	}
	if calls.Load() != 1 || driver.Generation() != 1 || endpoints[0].opens.Load() != 1 {
		t.Fatal("repeated desired start opened another generation")
	}
	for i := 0; i < 2; i++ {
		if err := manager.Stop(ctx, "pv.primary"); err != nil {
			t.Fatal(err)
		}
	}
	if endpoints[0].closes.Load() != 1 {
		t.Fatal("generation close was not exactly once")
	}
	if err := manager.Start(ctx, "pv.primary"); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 || driver.Generation() != 2 {
		t.Fatal("restart did not advance native generation")
	}
	if err := manager.Stop(ctx, "pv.primary"); err != nil {
		t.Fatal(err)
	}
	if endpoints[1].closes.Load() != 1 {
		t.Fatal("replacement close was not exactly once")
	}
}

func TestM03DisabledAndFailureRemainLocal(t *testing.T) {
	var calls atomic.Int32
	factory := func(modbus.TCPEndpointConfig) (modbusadapter.Endpoint, error) {
		calls.Add(1)
		return nil, errors.New("synthetic construction failure")
	}
	disabled := m03Driver(t, false, factory, nil)
	if _, err := disabled.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 0 {
		t.Fatal("disabled runtime constructed endpoint")
	}
	failed := m03Driver(t, true, factory, nil)
	if _, err := failed.Start(context.Background()); err == nil {
		t.Fatal("factory failure hidden")
	}
	if err := failed.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	// An unrelated shell runtime remains usable after the family-local error.
	if _, err := disabled.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatal("failure started an implicit retry loop")
	}
}

func TestM03LateOpenCannotOverlapReplacement(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	first := &m03Endpoint{}
	driver := m03Driver(t, true, func(modbus.TCPEndpointConfig) (modbusadapter.Endpoint, error) {
		if calls.Add(1) == 1 {
			close(entered)
			<-release
			return first, nil
		}
		return &m03Endpoint{}, nil
	}, nil)
	result := make(chan error, 1)
	go func() { _, err := driver.Start(context.Background()); result <- err }()
	<-entered
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := driver.Stop(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("stop without join proof=%v", err)
	}
	if _, err := driver.Replace(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("replacement bypassed drain=%v", err)
	}
	if calls.Load() != 1 {
		t.Fatal("replacement overlapped old open")
	}
	close(release)
	if err := <-result; err == nil {
		t.Fatal("late start was admitted")
	}
	if err := driver.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if first.closes.Load() != 1 {
		t.Fatal("late endpoint leaked or double closed")
	}
	if _, err := driver.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestM03PublicationScopeFencesOldEpoch(t *testing.T) {
	driver := m03Driver(t, true, func(modbus.TCPEndpointConfig) (modbusadapter.Endpoint, error) { return &m03Endpoint{}, nil }, nil)
	if _, err := driver.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	old := driver.active.Load().adapter
	observation := m03Observation(t)
	if err := old.RecordSunSpecQualificationObservation(observation); err != nil {
		t.Fatal(err)
	}
	_, ok := old.SemanticPVCurrentSingle()
	if !ok {
		t.Fatal("first generation did not publish")
	}
	driver.FenceWithdrawal()
	if err := old.RecordSunSpecCurrentObservation(observation); err == nil {
		t.Fatal("withdrawn generation published")
	}
	if err := driver.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	c := m03Correlation()
	c.Epoch = "epoch:synthetic-b"
	c.Generation = 2
	c.Sequence = 2
	if err := driver.BindLifecycle(c); err != nil {
		t.Fatal(err)
	}
	if _, err := driver.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	current := driver.active.Load().adapter
	if err := current.RecordSunSpecQualificationObservation(observation); err != nil {
		t.Fatal(err)
	}
	before, ok := current.SemanticPVCurrentSingle()
	if !ok {
		t.Fatal("replacement did not publish")
	}
	m03AssertGeneration(t, before, "epoch:synthetic-b", "2")
	if err := old.RecordSunSpecQualificationObservation(observation); err == nil {
		t.Fatal("late old completion published")
	}
	after, ok := current.SemanticPVCurrentSingle()
	if !ok || string(after.Canonical) != string(before.Canonical) {
		t.Fatal("late completion changed current epoch")
	}
}

func m03Observation(t *testing.T) modbusreg.SunSpecQualificationObservation {
	return m03ObservationSample(t, 1)
}

func m03ObservationSample(t *testing.T, pollGeneration uint64) modbusreg.SunSpecQualificationObservation {
	t.Helper()
	registry, err := modbusreg.NewStandardSunSpecDecoderRegistry(modbusreg.SunSpecModelsRevisionV1)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := modbusreg.NewSunSpecChainPlan(modbusreg.SunSpecChainPlanSpec{SchemaRevision: modbusreg.SunSpecModelsRevisionV1, BaseCandidates: []uint16{40000}, Limits: modbusreg.SunSpecChainLimits{MaxTotalWords: 4096, MaxOccurrences: 32}, DecoderKeys: registry.DecoderKeys()})
	if err != nil {
		t.Fatal(err)
	}
	chain := modbusreg.NewSunSpecChain(plan)
	words := sunspectest.ObservedFroniusFloatWords()
	offset := 0
	for id := uint64(1); ; id++ {
		requests := chain.NextRequests()
		if len(requests) != 1 {
			t.Fatal("synthetic replay request missing")
		}
		request := requests[0]
		count := int(request.WordCount())
		if offset+count > len(words) {
			t.Fatal("synthetic replay exceeded words")
		}
		view, err := modbusreg.NewLogicalViewSnapshot(modbusreg.LogicalViewRecord{LogicalViewID: id, WireResponseID: id + 100, PhysicalRequestID: id + 200, Endpoint: "fixture", ConnectionID: 4, Transport: modbusreg.TransportTCP, TransportGeneration: 5, UnitID: 1, RequestedFunction: modbusreg.FunctionReadHoldingRegisters, ReceivedFunction: modbusreg.FunctionReadHoldingRegisters, Table: modbusreg.HoldingRegisters, PhysicalOffset: request.Address(), PhysicalWordCount: request.WordCount(), AuthorizationScope: "fixture:managed", PollGeneration: pollGeneration, DeadlineIdentity: 2, LogicalOffset: request.Address(), LogicalWordCount: request.WordCount(), SliceWordCount: request.WordCount(), Words: append([]uint16(nil), words[offset:offset+count]...), WireResponseBytes: []byte{byte(id)}})
		if err != nil {
			t.Fatal(err)
		}
		offset += count
		snapshot, err := chain.AdmitReplay(request, view)
		if err != nil {
			t.Fatal(err)
		}
		if len(snapshot.RawWords()) != 0 {
			observation, err := modbusreg.NewSunSpecQualificationObservation(registry, snapshot)
			if err != nil {
				t.Fatal(err)
			}
			return observation
		}
	}
}

func TestM03SameEpochReplacementPublishesNextGeneration(t *testing.T) {
	driver := m03Driver(t, true, func(modbus.TCPEndpointConfig) (modbusadapter.Endpoint, error) { return &m03Endpoint{}, nil }, nil)
	if _, err := driver.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	observation := m03Observation(t)
	old := driver.active.Load().adapter
	if err := old.RecordSunSpecQualificationObservation(observation); err != nil {
		t.Fatal(err)
	}
	if _, err := driver.Replace(context.Background()); err != nil {
		t.Fatal(err)
	}
	current := driver.active.Load().adapter
	if err := current.RecordSunSpecQualificationObservation(observation); err != nil {
		t.Fatal(err)
	}
	view, ok := current.SemanticPVCurrentSingle()
	if !ok {
		t.Fatal("replacement unavailable")
	}
	m03AssertGeneration(t, view, "epoch:synthetic-a", "2")
	if err := old.RecordSunSpecCurrentObservation(observation); err == nil {
		t.Fatal("old generation published after same-epoch replacement")
	}
}

type m03Work struct {
	start func()
	close func() error
}

func (w *m03Work) Start()       { w.start() }
func (w *m03Work) Close() error { return w.close() }

func TestM03WorkerLifetimeAndJoin(t *testing.T) {
	var workerContext context.Context
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var starts, closes atomic.Int32
	driver := m03Driver(t, true, func(modbus.TCPEndpointConfig) (modbusadapter.Endpoint, error) { return &m03Endpoint{}, nil }, func(ctx context.Context, a *modbusadapter.Adapter) modbusManagedWork {
		workerContext = ctx
		return &m03Work{start: func() { starts.Add(1); go func() { defer close(done); <-ctx.Done(); close(entered); <-release }() }, close: func() error { closes.Add(1); <-done; return nil }}
	})
	ctx, cancel := context.WithCancel(context.Background())
	if _, err := driver.Start(ctx); err != nil {
		t.Fatal(err)
	}
	cancel()
	if workerContext.Err() != nil {
		t.Fatal("successful start's canceled caller killed admitted worker")
	}
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer stopCancel()
	if err := driver.Stop(stopCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("worker join not honored: %v", err)
	}
	<-entered
	close(release)
	if err := driver.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if starts.Load() != 1 || closes.Load() != 1 {
		t.Fatal("worker lifecycle duplicated")
	}
}

func TestM03LifecycleIdentityDrift(t *testing.T) {
	driver := m03Driver(t, true, func(modbus.TCPEndpointConfig) (modbusadapter.Endpoint, error) { return &m03Endpoint{}, nil }, nil)

	for name, mutate := range map[string]func(*runtimecontrol.LifecycleCorrelation){
		"unstarted driver": func(c *runtimecontrol.LifecycleCorrelation) { c.DriverID = "pv.other" },
		"unstarted source": func(c *runtimecontrol.LifecycleCorrelation) { c.SourceID = "pv.other" },
	} {
		t.Run(name, func(t *testing.T) {
			c := m03Correlation()
			mutate(&c)
			if err := driver.BindLifecycle(c); err == nil {
				t.Fatal("unstarted identity mismatch accepted")
			}
		})
	}
	if _, err := driver.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	mutations := map[string]func(*runtimecontrol.LifecycleCorrelation){
		"driver":     func(c *runtimecontrol.LifecycleCorrelation) { c.DriverID = "pv.other" },
		"source":     func(c *runtimecontrol.LifecycleCorrelation) { c.SourceID = "pv.other" },
		"epoch":      func(c *runtimecontrol.LifecycleCorrelation) { c.Epoch = "epoch:other" },
		"generation": func(c *runtimecontrol.LifecycleCorrelation) { c.Generation++ },
		"sequence":   func(c *runtimecontrol.LifecycleCorrelation) { c.Sequence++ },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			c := m03Correlation()
			mutate(&c)
			if err := driver.BindLifecycle(c); err == nil {
				t.Fatal("active identity drift accepted")
			}
		})
	}
	if err := driver.BindLifecycle(m03Correlation()); err != nil {
		t.Fatal("identical binding not idempotent:", err)
	}
}

func TestM03UnprovenCloseBlocksReplacement(t *testing.T) {
	closeErr := errors.New("synthetic close unproven")
	endpoint := &m03Endpoint{modbusRunLifecycleEndpoint: modbusRunLifecycleEndpoint{closeErr: closeErr}}
	var calls atomic.Int32
	driver := m03Driver(t, true, func(modbus.TCPEndpointConfig) (modbusadapter.Endpoint, error) { calls.Add(1); return endpoint, nil }, nil)
	driver.dial = func(context.Context, string, string) (net.Conn, error) {
		return nil, errors.New("synthetic dial failure")
	}
	if _, err := driver.Start(context.Background()); !errors.Is(err, closeErr) {
		t.Fatalf("start lost cleanup failure: %v", err)
	}
	if err := driver.Stop(context.Background()); !errors.Is(err, closeErr) {
		t.Fatalf("stop claimed cleanup proof: %v", err)
	}
	if _, err := driver.Replace(context.Background()); !errors.Is(err, closeErr) {
		t.Fatalf("replacement bypassed failed close: %v", err)
	}
	if calls.Load() != 1 || endpoint.closes.Load() != 1 {
		t.Fatal("unproven close retried native opening")
	}
}

func m03AssertGeneration(t *testing.T, view modbusadapter.SemanticPVCurrent, epoch semreg.SourceEpochID, generation semreg.Uint64) {
	t.Helper()
	available := 0
	for _, capability := range view.Snapshot.Capabilities {
		if capability.Availability != semreg.AvailabilityAvailable {
			continue
		}
		available++
		if capability.SourceEpochID != epoch || capability.DriverGeneration != generation {
			t.Fatal("available capability belongs to wrong lifecycle")
		}
	}
	if available != 3 {
		t.Fatalf("available capability count=%d; want 3", available)
	}
}

func TestM03ManagerRetryAfterClosedStartFailure(t *testing.T) {
	var calls atomic.Int32
	driver := m03Driver(t, true, func(modbus.TCPEndpointConfig) (modbusadapter.Endpoint, error) {
		if calls.Add(1) == 1 {
			return nil, errors.New("synthetic transient failure")
		}
		return &m03Endpoint{}, nil
	}, nil)
	if _, err := driver.Start(context.Background()); err == nil {
		t.Fatal("start failure hidden")
	}
	// Only the manager's next invocation authorizes another attempt.
	if calls.Load() != 1 {
		t.Fatal("unrequested retry")
	}
	if generation, err := driver.Start(context.Background()); err != nil || generation != 2 {
		t.Fatalf("manager retry generation=%d error=%v", generation, err)
	}
	if calls.Load() != 2 {
		t.Fatal("retry did not construct fresh endpoint")
	}
}

func TestM03ReplacementRetainsDistinctSampleEvidence(t *testing.T) {
	for _, newEpoch := range []bool{false, true} {
		t.Run(fmt.Sprint("new_epoch=", newEpoch), func(t *testing.T) {
			driver := m03Driver(t, true, func(modbus.TCPEndpointConfig) (modbusadapter.Endpoint, error) { return &m03Endpoint{}, nil }, nil)
			if _, err := driver.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			first, refresh := m03ObservationSample(t, 1), m03ObservationSample(t, 2)
			old := driver.active.Load().adapter
			if first.SampleID() == refresh.SampleID() {
				t.Fatal("fixture samples are identical")
			}
			if err := old.RecordSunSpecQualificationObservation(first); err != nil {
				t.Fatal(err)
			}
			if err := old.RecordSunSpecCurrentObservation(refresh); err != nil {
				t.Fatal(err)
			}
			profile := first.Capability().ProfileID()
			_, firstBytes, _ := old.SunSpecQualificationObservation(profile, first.SampleID())
			_, refreshBytes, _ := old.SunSpecQualificationObservation(profile, refresh.SampleID())
			if err := driver.Stop(context.Background()); err != nil {
				t.Fatal(err)
			}
			epoch := semreg.SourceEpochID("epoch:synthetic-a")
			if newEpoch {
				c := m03Correlation()
				c.Epoch, c.Generation, c.Sequence = "epoch:synthetic-b", 2, 2
				epoch = semreg.SourceEpochID(c.Epoch)
				if err := driver.BindLifecycle(c); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := driver.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			current := driver.active.Load().adapter
			if err := current.RecordSunSpecQualificationObservation(m03ObservationSample(t, 3)); err != nil {
				t.Fatal(err)
			}
			for _, retained := range []struct {
				sample  string
				encoded []byte
			}{{first.SampleID(), firstBytes}, {refresh.SampleID(), refreshBytes}} {
				_, encoded, ok := current.SunSpecQualificationObservation(profile, retained.sample)
				if !ok || len(encoded) == 0 || !bytes.Equal(encoded, retained.encoded) {
					t.Fatal("replacement lost exact predecessor evidence")
				}
			}
			view, ok := current.SemanticPVCurrentSingle()
			if !ok {
				t.Fatal("replacement did not publish distinct sample")
			}
			m03AssertGeneration(t, view, epoch, "2")
			if _, err := driver.Replace(context.Background()); err != nil {
				t.Fatal(err)
			}
			current = driver.active.Load().adapter
			if err := current.RecordSunSpecQualificationObservation(m03ObservationSample(t, 4)); err != nil {
				t.Fatal(err)
			}
			if _, _, ok := current.SunSpecQualificationObservation(profile, first.SampleID()); !ok {
				t.Fatal("second replacement lost original qualification")
			}
		})
	}
}

type m03RecoverableJournal struct {
	*semhub.MemoryJournal
	failed atomic.Bool
}

var errM03JournalFailure = errors.New("synthetic journal unavailable")

func (j *m03RecoverableJournal) Append(records ...semhub.EpochRecord) error {
	if j.failed.Load() {
		return errM03JournalFailure
	}
	return j.MemoryJournal.Append(records...)
}

func TestM03SemanticCleanupRetriesAfterJournalRecovery(t *testing.T) {
	for _, replace := range []bool{false, true} {
		t.Run(fmt.Sprint("replace=", replace), func(t *testing.T) {
			journal := &m03RecoverableJournal{MemoryJournal: semhub.NewMemoryJournal()}
			hub, err := semhub.New(semhub.Options{Journal: journal})
			if err != nil {
				t.Fatal(err)
			}
			first := &m03Endpoint{}
			var opens, closes atomic.Int32
			driver := m03Driver(t, true, func(modbus.TCPEndpointConfig) (modbusadapter.Endpoint, error) {
				if opens.Add(1) == 1 {
					return first, nil
				}
				return &m03Endpoint{}, nil
			}, func(context.Context, *modbusadapter.Adapter) modbusManagedWork {
				return &m03Work{start: func() {}, close: func() error { closes.Add(1); return nil }}
			})
			driver.hub = hub
			if _, err := driver.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			old := driver.active.Load().adapter
			if err := old.RecordSunSpecQualificationObservation(m03Observation(t)); err != nil {
				t.Fatal(err)
			}
			journal.failed.Store(true)
			if err := driver.Stop(context.Background()); !errors.Is(err, errM03JournalFailure) {
				t.Fatalf("stop lost journal error: %v", err)
			}
			if _, err := driver.Replace(context.Background()); !errors.Is(err, errM03JournalFailure) {
				t.Fatalf("replacement bypassed failed semantic cleanup: %v", err)
			}
			if opens.Load() != 1 {
				t.Fatal("replacement opened before cleanup proof")
			}
			journal.failed.Store(false)
			if replace {
				if generation, err := driver.Replace(context.Background()); err != nil || generation != 2 {
					t.Fatalf("recovered replacement generation=%d err=%v", generation, err)
				}
			} else if err := driver.Stop(context.Background()); err != nil {
				t.Fatalf("recovered stop: %v", err)
			}
			if first.closes.Load() != 1 || closes.Load() != 1 {
				t.Fatal("cleanup retry repeated native or worker shutdown")
			}
			fenced := false
			for _, record := range journal.Records() {
				if record.Kind == semhub.EventGenerationFenced && record.Generation == 1 {
					fenced = true
				}
			}
			if !fenced {
				t.Fatal("recovered cleanup did not journal the generation fence")
			}
			if err := old.RecordSunSpecCurrentObservation(m03ObservationSample(t, 2)); err == nil {
				t.Fatal("retired generation published after cleanup")
			}
		})
	}
}
