package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"sort"
	"sync"
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
	return m03ScopedObservation(t, pollGeneration, sunspectest.ObservedFroniusFloatWords(), "fixture:managed")
}

func m03ScopedObservation(t *testing.T, pollGeneration uint64, words []uint16, scope string) modbusreg.SunSpecQualificationObservation {
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
		view, err := modbusreg.NewLogicalViewSnapshot(modbusreg.LogicalViewRecord{LogicalViewID: id, WireResponseID: id + 100, PhysicalRequestID: id + 200, Endpoint: "fixture", ConnectionID: 4, Transport: modbusreg.TransportTCP, TransportGeneration: 5, UnitID: 1, RequestedFunction: modbusreg.FunctionReadHoldingRegisters, ReceivedFunction: modbusreg.FunctionReadHoldingRegisters, Table: modbusreg.HoldingRegisters, PhysicalOffset: request.Address(), PhysicalWordCount: request.WordCount(), AuthorizationScope: scope, PollGeneration: pollGeneration, DeadlineIdentity: 2, LogicalOffset: request.Address(), LogicalWordCount: request.WordCount(), SliceWordCount: request.WordCount(), Words: append([]uint16(nil), words[offset:offset+count]...), WireResponseBytes: []byte{byte(id)}})
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
	if err := driver.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	correlation := m03Correlation()
	correlation.Generation, correlation.Sequence = 2, 2
	if err := driver.BindLifecycle(correlation); err != nil {
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

func TestM03BlockedWorkFactoryWithdrawal(t *testing.T) {
	for _, withdrawal := range []string{"FenceWithdrawal", "Stop"} {
		t.Run(withdrawal, func(t *testing.T) {
			entered, release := make(chan struct{}), make(chan struct{})
			var starts, consumerLeaseStarts, closes atomic.Int32
			driver := m03Driver(t, true, func(modbus.TCPEndpointConfig) (modbusadapter.Endpoint, error) {
				return &m03Endpoint{}, nil
			}, func(context.Context, *modbusadapter.Adapter) modbusManagedWork {
				close(entered)
				<-release
				return &m03Work{start: func() {
					starts.Add(1)
					consumerLeaseStarts.Add(1)
				}, close: func() error { closes.Add(1); return nil }}
			})
			t.Cleanup(func() {
				select {
				case <-release:
				default:
					close(release)
				}
			})
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			result := make(chan error, 1)
			go func() { _, err := driver.Start(ctx); result <- err }()
			select {
			case <-entered:
			case <-ctx.Done():
				t.Fatal("work factory was not entered")
			}
			if withdrawal == "FenceWithdrawal" {
				driver.FenceWithdrawal()
			} else {
				stopCtx, stopCancel := context.WithTimeout(ctx, 20*time.Millisecond)
				defer stopCancel()
				if err := driver.Stop(stopCtx); !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("stop bypassed blocked factory join: %v", err)
				}
			}
			close(release)
			if err := <-result; err == nil {
				t.Error("withdrawn start was admitted")
			}
			if err := driver.Stop(ctx); err != nil {
				t.Fatal(err)
			}
			if err := driver.Stop(ctx); err != nil {
				t.Fatal(err)
			}
			if got := starts.Load(); got != 0 {
				t.Errorf("withdrawn work Start calls=%d, want 0", got)
			}
			if got := consumerLeaseStarts.Load(); got != 0 {
				t.Errorf("withdrawn consumer lease starts=%d, want 0", got)
			}
			if got := closes.Load(); got != 1 {
				t.Errorf("returned work Close calls=%d, want 1", got)
			}
		})
	}
}

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

type m03StartupCloseErrorConn struct {
	net.Conn
	closeErr error
}

func (c *m03StartupCloseErrorConn) Close() error {
	_ = c.Conn.Close()
	return c.closeErr
}

type m03OpenFailureEndpoint struct {
	m03Endpoint
	openErr error
}

func (e *m03OpenFailureEndpoint) OpenConnection(net.Conn) (modbus.TCPConnectionHandle, error) {
	return modbus.TCPConnectionHandle{}, e.openErr
}

func TestM03UnprovenConnectionCloseBlocksReplacement(t *testing.T) {
	closeErr := errors.New("synthetic connection close failure")
	openErr := errors.New("synthetic open failure")
	endpoint := &m03OpenFailureEndpoint{openErr: openErr}
	var calls atomic.Int32
	driver := m03Driver(t, true, func(modbus.TCPEndpointConfig) (modbusadapter.Endpoint, error) {
		calls.Add(1)
		return endpoint, nil
	}, nil)
	client, peer := net.Pipe()
	defer func() { _ = peer.Close() }()
	driver.dial = func(context.Context, string, string) (net.Conn, error) {
		return &m03StartupCloseErrorConn{Conn: client, closeErr: closeErr}, nil
	}
	if _, err := driver.Start(context.Background()); !errors.Is(err, modbusadapter.ErrStartupCloseUnproven) || !errors.Is(err, closeErr) || !errors.Is(err, openErr) {
		t.Errorf("start lost unproven connection cleanup: %v", err)
	}
	if _, err := driver.Start(context.Background()); !errors.Is(err, modbusadapter.ErrStartupCloseUnproven) {
		t.Errorf("second start bypassed unproven cleanup: %v", err)
	}
	if err := driver.Stop(context.Background()); !errors.Is(err, modbusadapter.ErrStartupCloseUnproven) || !errors.Is(err, closeErr) {
		t.Errorf("stop claimed cleanup proof: %v", err)
	}
	if _, err := driver.Replace(context.Background()); !errors.Is(err, modbusadapter.ErrStartupCloseUnproven) {
		t.Errorf("replacement bypassed unproven cleanup: %v", err)
	}
	if calls.Load() != 1 || endpoint.closes.Load() != 1 {
		t.Errorf("endpoint factory calls=%d closes=%d, want 1 each", calls.Load(), endpoint.closes.Load())
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
			c := m03Correlation()
			c.Generation, c.Sequence = 2, 2
			if newEpoch {
				c.Epoch = "epoch:synthetic-b"
			}
			epoch := semreg.SourceEpochID(c.Epoch)
			if err := driver.BindLifecycle(c); err != nil {
				t.Fatal(err)
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
			if err := driver.Stop(context.Background()); err != nil {
				t.Fatal(err)
			}
			c.Generation, c.Sequence = 3, 3
			if err := driver.BindLifecycle(c); err != nil {
				t.Fatal(err)
			}
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
				if err := driver.Stop(context.Background()); err != nil {
					t.Fatal(err)
				}
				c := m03Correlation()
				c.Generation, c.Sequence = 2, 2
				if err := driver.BindLifecycle(c); err != nil {
					t.Fatal(err)
				}
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

type m03BlockingEndpoint struct {
	m03Endpoint
	entered chan struct{}
	release chan struct{}
}

func (e *m03BlockingEndpoint) Close() error {
	close(e.entered)
	<-e.release
	return e.m03Endpoint.Close()
}

func TestM03WithdrawalIndependentOfNativeShutdown(t *testing.T) {
	for _, blocked := range []string{"worker", "endpoint"} {
		t.Run(blocked, func(t *testing.T) {
			entered, release := make(chan struct{}), make(chan struct{})
			defer close(release)
			endpoint := &m03BlockingEndpoint{entered: entered, release: release}
			driver := m03Driver(t, true, func(modbus.TCPEndpointConfig) (modbusadapter.Endpoint, error) {
				if blocked == "endpoint" {
					return endpoint, nil
				}
				return &m03Endpoint{}, nil
			}, func(context.Context, *modbusadapter.Adapter) modbusManagedWork {
				return &m03Work{start: func() {}, close: func() error {
					if blocked == "worker" {
						close(entered)
						<-release
					}
					return nil
				}}
			})
			if _, err := driver.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			old := driver.active.Load().adapter
			if err := old.RecordSunSpecQualificationObservation(m03Observation(t)); err != nil {
				t.Fatal(err)
			}
			view, ok := old.SemanticPVCurrentSingle()
			if !ok {
				t.Fatal("fixture publication missing")
			}
			asset := view.Snapshot.AssetID
			driver.FenceWithdrawal()
			if _, ok := old.SemanticPVCurrentSingle(); ok {
				t.Error("withdrawn single read admitted")
			}
			if _, ok := old.SemanticPVCurrentByAsset(string(asset)); ok {
				t.Error("withdrawn asset read admitted")
			}
			if _, ok := old.SemanticPVCurrentSingleAt(time.Now()); ok {
				t.Error("withdrawn timed read admitted")
			}
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			if err := driver.Stop(ctx); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("blocked shutdown result=%v", err)
			}
			<-entered
			deadline := time.After(time.Second)
			for driver.hub.Admitted(asset) {
				select {
				case <-deadline:
					t.Fatal("semantic withdrawal waited for native shutdown")
				case <-time.After(time.Millisecond):
				}
			}
			retained, err := driver.hub.Inspect(asset)
			if err != nil || !retained.Present {
				t.Fatalf("withdrawal lost retained evidence: %v", err)
			}
			if driver.active.Load() == nil {
				t.Fatal("pending shutdown released generation ownership")
			}
			retryCtx, retryCancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
			defer retryCancel()
			if _, err := driver.Replace(retryCtx); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("replacement bypassed pending shutdown: %v", err)
			}
		})
	}
}

func TestM03BoundSemanticGenerationSurvivesNativeRetry(t *testing.T) {
	var calls atomic.Int32
	driver := m03Driver(t, true, func(modbus.TCPEndpointConfig) (modbusadapter.Endpoint, error) {
		if calls.Add(1) == 1 {
			return nil, errors.New("synthetic startup failure")
		}
		return &m03Endpoint{}, nil
	}, nil)
	correlation := m03Correlation()
	correlation.Generation = 7
	if err := driver.BindLifecycle(correlation); err != nil {
		t.Fatal(err)
	}
	if _, err := driver.Start(context.Background()); err == nil {
		t.Fatal("failure hidden")
	}
	native, err := driver.Start(context.Background())
	if err != nil || native != 2 {
		t.Fatalf("native retry=%d error=%v", native, err)
	}
	attempt := driver.active.Load()
	if attempt.nativeGeneration != native || attempt.scope.Generation != 7 {
		t.Fatal("native to semantic mapping lost")
	}
	if err := attempt.adapter.RecordSunSpecQualificationObservation(m03Observation(t)); err != nil {
		t.Fatal(err)
	}
	view, ok := attempt.adapter.SemanticPVCurrentSingle()
	if !ok {
		t.Fatal("restored generation did not publish")
	}
	m03AssertGeneration(t, view, semreg.SourceEpochID(correlation.Epoch), "7")
}

func TestM03CanceledStartupWithAdapterCanRetry(t *testing.T) {
	var calls atomic.Int32
	first := &m03Endpoint{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	driver := m03Driver(t, true, func(modbus.TCPEndpointConfig) (modbusadapter.Endpoint, error) {
		if calls.Add(1) == 1 {
			return first, nil
		}
		return &m03Endpoint{}, nil
	}, func(runCtx context.Context, _ *modbusadapter.Adapter) modbusManagedWork {
		if calls.Load() == 1 {
			cancel()
			<-runCtx.Done()
		}
		return nil
	})
	if _, err := driver.Start(ctx); err == nil {
		t.Fatal("canceled startup admitted")
	}
	attempt := driver.active.Load()
	<-attempt.started
	if !attempt.startupClosed || first.closes.Load() != 1 {
		t.Fatal("canceled startup did not prove close")
	}
	if generation, err := driver.Start(context.Background()); err != nil || generation != 2 {
		t.Fatalf("retry=%d error=%v", generation, err)
	}
	if calls.Load() != 2 {
		t.Fatal("retry did not open fresh endpoint")
	}
	if driver.active.Load().scope.Generation != 1 {
		t.Fatal("retry changed service generation")
	}
}

func TestM03WorkInstallationSerializesWithdrawal(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var closes atomic.Int32
	driver := m03Driver(t, true, func(modbus.TCPEndpointConfig) (modbusadapter.Endpoint, error) { return &m03Endpoint{}, nil }, func(context.Context, *modbusadapter.Adapter) modbusManagedWork {
		return &m03Work{start: func() { close(entered); <-release }, close: func() error { closes.Add(1); return nil }}
	})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	result := make(chan error, 1)
	go func() { _, err := driver.Start(context.Background()); result <- err }()
	<-entered
	attempt := driver.active.Load()
	fenced := make(chan struct{})
	go func() { driver.FenceWithdrawal(); close(fenced) }()
	select {
	case <-fenced:
	case <-time.After(time.Second):
		t.Fatal("withdrawal blocked behind work installation")
	}
	if !attempt.withdrawn() {
		t.Fatal("withdrawal gate did not close")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	stopped := make(chan error, 1)
	go func() { stopped <- driver.Stop(ctx) }()
	select {
	case err := <-stopped:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("cleanup completed while generation effects were still installing: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Stop did not honor its deadline")
	}
	if closes.Load() != 0 {
		t.Fatal("work closed before installation joined")
	}
	close(release)
	if err := <-result; err == nil {
		t.Fatal("withdrawn installation admitted")
	}
	if err := driver.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if closes.Load() != 1 || driver.active.Load() != nil {
		t.Fatal("cleanup did not join installation and close its work exactly once")
	}
}

// Done signals that Start reached the join select, without timing assumptions.
type m03JoinContext struct {
	context.Context
	joined chan struct{}
	once   sync.Once
}

func (c *m03JoinContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.joined) })
	return c.Context.Done()
}

func TestM03RetryJoinsCanceledStartupThenOpensFreshEndpoint(t *testing.T) {
	for _, block := range []string{"endpoint", "work factory"} {
		t.Run(block, func(t *testing.T) {
			entered, release := make(chan struct{}), make(chan struct{})
			var calls atomic.Int32
			first := &m03Endpoint{}
			driver := m03Driver(t, true, func(modbus.TCPEndpointConfig) (modbusadapter.Endpoint, error) {
				if calls.Add(1) == 1 {
					if block == "endpoint" {
						close(entered)
						<-release
					}
					return first, nil
				}
				return &m03Endpoint{}, nil
			}, func(ctx context.Context, _ *modbusadapter.Adapter) modbusManagedWork {
				if block == "work factory" && calls.Load() == 1 {
					close(entered)
					<-release
				}
				return nil
			})
			defer func() {
				select {
				case <-release:
				default:
					close(release)
				}
			}()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			initial := make(chan error, 1)
			go func() { _, err := driver.Start(ctx); initial <- err }()
			<-entered
			cancel()
			if err := <-initial; !errors.Is(err, context.Canceled) {
				t.Fatalf("initial start=%v", err)
			}
			retryCtx, retryCancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer retryCancel()
			joined := &m03JoinContext{Context: retryCtx, joined: make(chan struct{})}
			retried := make(chan error, 1)
			go func() {
				generation, err := driver.Start(joined)
				if err == nil && generation != 2 {
					err = fmt.Errorf("retry generation=%d, want 2", generation)
				}
				retried <- err
			}()
			<-joined.joined
			if calls.Load() != 1 {
				t.Fatal("retry opened before predecessor cleanup")
			}
			close(release)
			if err := <-retried; err != nil {
				t.Fatalf("joined retry did not start a fresh attempt: %v", err)
			}
			if calls.Load() != 2 || first.closes.Load() != 1 {
				t.Fatal("retry did not close predecessor and open exactly one fresh endpoint")
			}
			if driver.active.Load().scope.Generation != 1 {
				t.Fatal("native retry changed service generation")
			}
		})
	}
}

// Fail the second source activation once, leaving the first journalled and
// subsequently fenced by startup cleanup before the manager retries.
type m03PartialActivationJournal struct {
	*semhub.MemoryJournal
	source semreg.SourceID
	failed atomic.Bool
}

func (j *m03PartialActivationJournal) Append(records ...semhub.EpochRecord) error {
	for _, record := range records {
		if record.Kind == semhub.EventGenerationActivated && record.Source == j.source && j.failed.CompareAndSwap(false, true) {
			return errM03JournalFailure
		}
	}
	return j.MemoryJournal.Append(records...)
}

func TestM03ManagerRetryAfterPartialActivation(t *testing.T) {
	journal := &m03PartialActivationJournal{MemoryJournal: semhub.NewMemoryJournal()}
	hub, err := semhub.New(semhub.Options{Journal: journal})
	if err != nil {
		t.Fatal(err)
	}
	var opens atomic.Int32
	driver := m03Driver(t, true, func(modbus.TCPEndpointConfig) (modbusadapter.Endpoint, error) {
		opens.Add(1)
		return &m03Endpoint{}, nil
	}, nil)
	driver.hub = hub
	manager, err := drivermanager.New(drivermanager.Config{Drivers: []drivermanager.DriverConfig{{
		ID: "pv.primary", Enabled: true, Runtime: driver,
		ClassifyError: func(error) drivermanager.Failure {
			return drivermanager.Failure{Reason: drivermanager.Reason{Code: drivermanager.ReasonDependencyUnavailable, Retryable: true}}
		},
		Retry: drivermanager.RetryPolicy{Budget: 1, InitialDelay: time.Millisecond, MaxDelay: time.Millisecond, JitterRatio: -1},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	defer func() {
		if err := manager.Shutdown(ctx); err != nil {
			t.Error(err)
		}
	}()
	if err := manager.Start(ctx, "pv.primary"); err != nil {
		t.Fatal(err)
	}
	old := driver.active.Load().adapter
	var sources []semreg.SourceID
	for i, scope := range []string{"fixture:manager-a", "fixture:manager-b"} {
		words := sunspectest.ObservedFroniusFloatWords()
		if i != 0 {
			words[52] ^= 1
		}
		if err := old.RecordSunSpecQualificationObservation(m03ScopedObservation(t, uint64(i+1), words, scope)); err != nil {
			t.Fatal(err)
		}
		view, ok := old.SemanticPVCurrentSingle()
		if !ok {
			t.Fatal("initial source unavailable")
		}
		sources = append(sources, view.Snapshot.Sources[0].SourceID)
	}
	sort.Slice(sources, func(i, j int) bool { return sources[i] < sources[j] })
	if sources[0] == sources[1] {
		t.Fatal("fixture did not create distinct sources")
	}
	journal.source = sources[1]
	if err := manager.Replace(ctx, "pv.primary"); err != nil {
		t.Fatal(err)
	}
	for {
		snapshot, ok := manager.Snapshot("pv.primary")
		if ok && snapshot.ObservedState == drivermanager.ObservedRunning {
			break
		}
		if ok && snapshot.ObservedState == drivermanager.ObservedFailed {
			t.Fatalf("manager retry failed: %+v", snapshot)
		}
		select {
		case <-ctx.Done():
			t.Fatal("manager retry did not recover")
		case <-time.After(time.Millisecond):
		}
	}
	if !journal.failed.Load() || opens.Load() != 3 {
		t.Fatalf("failure=%v opens=%d; want one failed activation and one retry", journal.failed.Load(), opens.Load())
	}
	counts := make(map[semreg.SourceID]map[uint64]int)
	for _, record := range journal.Records() {
		if record.Kind == semhub.EventGenerationActivated {
			if counts[record.Source] == nil {
				counts[record.Source] = make(map[uint64]int)
			}
			counts[record.Source][record.Generation]++
		}
	}
	if counts[sources[0]][2] != 1 || counts[sources[1]][2] != 0 || counts[sources[0]][3] != 1 || counts[sources[1]][3] != 1 {
		t.Fatalf("activation counts=%v", counts)
	}
	if driver.correlation != m03Correlation() {
		t.Fatal("retry changed the bound service correlation")
	}
	current := driver.active.Load().adapter
	if err := current.RecordSunSpecQualificationObservation(m03ScopedObservation(t, 10, sunspectest.ObservedFroniusFloatWords(), "fixture:manager-a")); err != nil {
		t.Fatal(err)
	}
	view, ok := current.SemanticPVCurrentSingle()
	if !ok {
		t.Fatal("recovered driver did not publish")
	}
	m03AssertGeneration(t, view, semreg.SourceEpochID(m03Correlation().Epoch), "3")
}

// A failed activation can leave a newer source live while startup fencing fails.
type m03ActivationCleanupJournal struct {
	*semhub.MemoryJournal
	source      semreg.SourceID
	unavailable atomic.Bool
}

func (j *m03ActivationCleanupJournal) Append(records ...semhub.EpochRecord) error {
	for _, record := range records {
		if record.Kind == semhub.EventGenerationActivated && record.Source == j.source {
			j.unavailable.Store(true)
		}
	}
	if j.unavailable.Load() {
		return errM03JournalFailure
	}
	return j.MemoryJournal.Append(records...)
}

func TestM03PartialActivationCleanupRetriedAfterJournalRecovery(t *testing.T) {
	journal := &m03ActivationCleanupJournal{MemoryJournal: semhub.NewMemoryJournal()}
	hub, err := semhub.New(semhub.Options{Journal: journal})
	if err != nil {
		t.Fatal(err)
	}
	var endpoints []*m03Endpoint
	driver := m03Driver(t, true, func(modbus.TCPEndpointConfig) (modbusadapter.Endpoint, error) {
		endpoint := &m03Endpoint{}
		endpoints = append(endpoints, endpoint)
		return endpoint, nil
	}, nil)
	driver.hub = hub
	if _, err := driver.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	var sources []semreg.SourceID
	for i, scope := range []string{"fixture:cleanup-a", "fixture:cleanup-b"} {
		words := sunspectest.ObservedFroniusFloatWords()
		if i != 0 {
			words[52] ^= 1
		}
		if err := driver.active.Load().adapter.RecordSunSpecQualificationObservation(m03ScopedObservation(t, uint64(i+1), words, scope)); err != nil {
			t.Fatal(err)
		}
		view, ok := driver.active.Load().adapter.SemanticPVCurrentSingle()
		if !ok {
			t.Fatal("source unavailable")
		}
		sources = append(sources, view.Snapshot.Sources[0].SourceID)
	}
	sort.Slice(sources, func(i, j int) bool { return sources[i] < sources[j] })
	if err := driver.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	journal.source = sources[1]
	if _, err := driver.Start(context.Background()); !errors.Is(err, modbusadapter.ErrStartupCloseUnproven) {
		t.Fatalf("startup cleanup error=%v", err)
	}
	if err := driver.Stop(context.Background()); !errors.Is(err, errM03JournalFailure) {
		t.Fatalf("pending cleanup=%v", err)
	}
	if _, err := driver.Start(context.Background()); err == nil {
		t.Fatal("successor bypassed cleanup")
	}
	journal.source = ""
	journal.unavailable.Store(false)
	if err := driver.Stop(context.Background()); err != nil {
		t.Fatalf("recovered cleanup=%v", err)
	}
	fenced := 0
	for _, record := range journal.Records() {
		if record.Kind == semhub.EventGenerationFenced && record.Source == sources[0] && record.Generation == 2 {
			fenced++
		}
	}
	if fenced != 1 {
		t.Fatalf("partial generation fences=%d", fenced)
	}
	for _, endpoint := range endpoints {
		if endpoint.closes.Load() != 1 {
			t.Fatal("native shutdown repeated")
		}
	}
	if generation, err := driver.Start(context.Background()); err != nil || generation != 3 {
		t.Fatalf("successor=%d error=%v", generation, err)
	}
	for _, source := range sources {
		count := 0
		for _, record := range journal.Records() {
			if record.Kind == semhub.EventGenerationActivated && record.Source == source && record.Generation == 3 {
				count++
			}
		}
		if count != 1 {
			t.Fatal("successor lost a retained source")
		}
	}
}

func TestM03NativeCleanupFailureQuarantinesManager(t *testing.T) {
	for _, failure := range []string{"endpoint", "work"} {
		t.Run(failure, func(t *testing.T) {
			closeErr := errors.New("synthetic native cleanup failure")
			endpoint := &m03Endpoint{}
			if failure == "endpoint" {
				endpoint.closeErr = closeErr
			}
			driver := m03Driver(t, true, func(modbus.TCPEndpointConfig) (modbusadapter.Endpoint, error) {
				return endpoint, nil
			}, func(context.Context, *modbusadapter.Adapter) modbusManagedWork {
				return &m03Work{start: func() {}, close: func() error {
					if failure == "work" {
						return closeErr
					}
					return nil
				}}
			})
			if _, err := driver.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			manager, err := drivermanager.New(drivermanager.Config{Drivers: []drivermanager.DriverConfig{{ID: "pv.primary", Enabled: true, Runtime: driver}}})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if err := manager.Start(ctx, "pv.primary"); err != nil {
				t.Fatal(err)
			}
			for {
				snapshot, changed, ok := manager.Watch("pv.primary")
				if !ok {
					t.Fatal("driver missing")
				}
				if snapshot.ObservedState == drivermanager.ObservedRunning {
					break
				}
				select {
				case <-changed:
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
			report, err := manager.StopWithReport(ctx, "pv.primary")
			if err != nil {
				t.Fatal(err)
			}
			if report.Class != drivermanager.CloseUnconfirmed || !driver.SafetyQuarantined() {
				t.Fatalf("native cleanup class=%v quarantine=%v", report.Class, driver.SafetyQuarantined())
			}
			snapshot, _ := manager.Snapshot("pv.primary")
			if !snapshot.SafetyQuarantined || snapshot.Reason.Code != drivermanager.ReasonCloseUnconfirmed {
				t.Fatalf("cleanup failure snapshot=%+v", snapshot)
			}
			if err := manager.Start(ctx, "pv.primary"); !errors.Is(err, drivermanager.ErrSafetyQuarantined) {
				t.Fatalf("start=%v", err)
			}
			if err := manager.Replace(ctx, "pv.primary"); !errors.Is(err, drivermanager.ErrSafetyQuarantined) {
				t.Fatalf("replace=%v", err)
			}
			if driver.Generation() != 1 || endpoint.closes.Load() != 1 {
				t.Fatal("quarantine allowed native retry")
			}
		})
	}
}

type m03BlockingFenceJournal struct {
	*semhub.MemoryJournal
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (j *m03BlockingFenceJournal) Append(records ...semhub.EpochRecord) error {
	for _, record := range records {
		if record.Kind == semhub.EventGenerationFenced {
			j.once.Do(func() { close(j.entered) })
			<-j.release
		}
	}
	return j.MemoryJournal.Append(records...)
}

func TestM03WorkerFailureQuarantinesWhileSemanticWithdrawalBlocked(t *testing.T) {
	journal := &m03BlockingFenceJournal{
		MemoryJournal: semhub.NewMemoryJournal(),
		entered:       make(chan struct{}),
		release:       make(chan struct{}),
	}
	hub, err := semhub.New(semhub.Options{Journal: journal})
	if err != nil {
		t.Fatal(err)
	}
	closeErr := errors.New("synthetic worker cleanup failure")
	endpoint := &m03Endpoint{}
	var workCloses atomic.Int32
	driver := m03Driver(t, true, func(modbus.TCPEndpointConfig) (modbusadapter.Endpoint, error) {
		return endpoint, nil
	}, func(context.Context, *modbusadapter.Adapter) modbusManagedWork {
		return &m03Work{start: func() {}, close: func() error {
			workCloses.Add(1)
			return closeErr
		}}
	})
	// Release the journal before the driver's registered cleanup joins Stop.
	t.Cleanup(func() { close(journal.release) })
	driver.hub = hub
	if _, err := driver.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := driver.active.Load().adapter.RecordSunSpecQualificationObservation(m03Observation(t)); err != nil {
		t.Fatal(err)
	}
	manager, err := drivermanager.New(drivermanager.Config{Drivers: []drivermanager.DriverConfig{{ID: "pv.primary", Enabled: true, Runtime: driver}}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := manager.Start(ctx, "pv.primary"); err != nil {
		t.Fatal(err)
	}
	for {
		snapshot, changed, _ := manager.Watch("pv.primary")
		if snapshot.ObservedState == drivermanager.ObservedRunning {
			break
		}
		select {
		case <-changed:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	stopCtx, stopCancel := context.WithTimeout(ctx, time.Second)
	defer stopCancel()
	report, err := manager.StopWithReport(stopCtx, "pv.primary")
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-journal.entered:
	default:
		t.Fatal("semantic withdrawal did not reach the blocked journal")
	}
	if !errors.Is(stopCtx.Err(), context.DeadlineExceeded) || workCloses.Load() != 1 || endpoint.closes.Load() != 1 {
		t.Fatalf("deadline=%v worker closes=%d endpoint closes=%d", stopCtx.Err(), workCloses.Load(), endpoint.closes.Load())
	}
	if report.Class != drivermanager.CloseUnconfirmed || !driver.SafetyQuarantined() {
		t.Fatalf("blocked cleanup class=%v quarantine=%v", report.Class, driver.SafetyQuarantined())
	}
	snapshot, _ := manager.Snapshot("pv.primary")
	if !snapshot.SafetyQuarantined || snapshot.Reason.Code != drivermanager.ReasonCloseUnconfirmed {
		t.Fatalf("blocked cleanup snapshot=%+v", snapshot)
	}
	if err := manager.Start(ctx, "pv.primary"); !errors.Is(err, drivermanager.ErrSafetyQuarantined) {
		t.Fatalf("start=%v", err)
	}
	if err := manager.Replace(ctx, "pv.primary"); !errors.Is(err, drivermanager.ErrSafetyQuarantined) {
		t.Fatalf("replace=%v", err)
	}
	if driver.Generation() != 1 {
		t.Fatal("quarantine allowed a successor generation")
	}
}

func TestM03CancelDuringWorkStartCleansBeforeRetry(t *testing.T) {
	entered, release, canceled := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var calls, workCloses atomic.Int32
	first := &m03Endpoint{}
	driver := m03Driver(t, true, func(modbus.TCPEndpointConfig) (modbusadapter.Endpoint, error) {
		if calls.Add(1) == 1 {
			return first, nil
		}
		return &m03Endpoint{}, nil
	}, func(runCtx context.Context, _ *modbusadapter.Adapter) modbusManagedWork {
		if calls.Load() != 1 {
			return nil
		}
		return &m03Work{start: func() {
			close(entered)
			<-runCtx.Done()
			close(canceled)
			<-release
		}, close: func() error { workCloses.Add(1); return nil }}
	})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() { _, err := driver.Start(ctx); result <- err }()
	<-entered
	attempt := driver.active.Load()
	cancel()
	<-canceled
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("initial start=%v", err)
	}
	close(release)
	<-attempt.started
	if attempt.startErr == nil || !attempt.startupClosed || first.closes.Load() != 1 || workCloses.Load() != 1 {
		t.Fatalf("canceled installation error=%v closed=%v endpoint=%d work=%d", attempt.startErr, attempt.startupClosed, first.closes.Load(), workCloses.Load())
	}
	if generation, err := driver.Start(context.Background()); err != nil || generation != 2 {
		t.Fatalf("retry generation=%d error=%v", generation, err)
	}
	if calls.Load() != 2 || workCloses.Load() != 1 {
		t.Fatal("retry reused canceled work or repeated cleanup")
	}
}

// m03CompletionContext holds the originating caller at its select until open
// has detached cancellation, and holds started open until that caller returns.
type m03CompletionContext struct {
	context.Context
	registered atomic.Bool
	detached   chan struct{}
	resume     chan struct{}
	cancel     context.CancelFunc
}

func (ctx *m03CompletionContext) Value(any) any { return nil }

func (ctx *m03CompletionContext) Done() <-chan struct{} {
	if ctx.registered.Load() {
		<-ctx.detached
		ctx.cancel()
	}
	return ctx.Context.Done()
}

func (ctx *m03CompletionContext) AfterFunc(f func()) func() bool {
	stop := context.AfterFunc(ctx.Context, f)
	ctx.registered.Store(true)
	return func() bool {
		stopped := stop()
		close(ctx.detached)
		<-ctx.resume
		return stopped
	}
}

func TestM03CancellationAfterStartupFinalizationPreservesWorker(t *testing.T) {
	var workerContext context.Context
	var starts, closes atomic.Int32
	endpoint := &m03Endpoint{}
	driver := m03Driver(t, true, func(modbus.TCPEndpointConfig) (modbusadapter.Endpoint, error) {
		return endpoint, nil
	}, func(ctx context.Context, _ *modbusadapter.Adapter) modbusManagedWork {
		workerContext = ctx
		return &m03Work{start: func() { starts.Add(1) }, close: func() error { closes.Add(1); return nil }}
	})
	callerCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx := &m03CompletionContext{Context: callerCtx, detached: make(chan struct{}), resume: make(chan struct{}), cancel: cancel}
	var resumeOnce sync.Once
	resume := func() { resumeOnce.Do(func() { close(ctx.resume) }) }
	defer resume()
	if _, err := driver.Start(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("originating caller error=%v", err)
	}
	// open is complete and its cancellation callback is detached, but started
	// cannot close until this test releases resume.
	if workerContext.Err() != nil {
		t.Fatal("canceled caller killed the successfully installed worker")
	}
	resume()
	attempt := driver.active.Load()
	<-attempt.started
	// A cancellation callback already queued before detachment must also
	// leave the finalized generation alive when it eventually runs.
	attempt.cancelStartup()
	if generation, err := driver.Start(context.Background()); err != nil || generation != 1 {
		t.Fatalf("subsequent start generation=%d error=%v", generation, err)
	}
	if workerContext.Err() != nil || starts.Load() != 1 || closes.Load() != 0 || endpoint.closes.Load() != 0 {
		t.Fatal("finalized generation was canceled, replaced, or cleaned up")
	}
}

func TestM03StartupWorkerFailureQuarantinesWhileSemanticWithdrawalBlocked(t *testing.T) {
	for _, phase := range []string{"before installation", "during installation"} {
		t.Run(phase, func(t *testing.T) {
			journal := &m03BlockingFenceJournal{
				MemoryJournal: semhub.NewMemoryJournal(),
				entered:       make(chan struct{}),
				release:       make(chan struct{}),
			}
			hub, err := semhub.New(semhub.Options{Journal: journal})
			if err != nil {
				t.Fatal(err)
			}
			entered, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			resume := func() { releaseOnce.Do(func() { close(release) }) }
			endpoint := &m03Endpoint{}
			closeErr := errors.New("synthetic startup worker cleanup failure")
			var workStarts, workCloses atomic.Int32
			observation := m03Observation(t)
			driver := m03Driver(t, true, func(modbus.TCPEndpointConfig) (modbusadapter.Endpoint, error) {
				return endpoint, nil
			}, func(runCtx context.Context, adapter *modbusadapter.Adapter) modbusManagedWork {
				if err := adapter.RecordSunSpecQualificationObservation(observation); err != nil {
					t.Error(err)
				}
				pause := func() {
					close(entered)
					<-runCtx.Done()
					close(canceled)
					<-release
				}
				if phase == "before installation" {
					pause()
				}
				return &m03Work{start: func() {
					workStarts.Add(1)
					pause()
				}, close: func() error {
					workCloses.Add(1)
					return closeErr
				}}
			})
			var semanticOnce sync.Once
			releaseSemantic := func() { semanticOnce.Do(func() { close(journal.release) }) }
			// Unblock startup and semantic cleanup before the fixture joins Stop.
			t.Cleanup(func() { resume(); releaseSemantic() })
			driver.hub = hub
			manager, err := drivermanager.New(drivermanager.Config{Drivers: []drivermanager.DriverConfig{{ID: "pv.primary", Enabled: true, Runtime: driver}}})
			if err != nil {
				t.Fatal(err)
			}
			callerCtx, callerCancel := context.WithCancel(context.Background())
			defer callerCancel()
			startDone := make(chan error, 1)
			go func() { startDone <- manager.Start(callerCtx, "pv.primary") }()
			<-entered
			attempt := driver.active.Load()
			callerCancel()
			<-canceled
			if err := <-startDone; err != nil {
				t.Fatal(err)
			}
			resume()
			<-journal.entered
			select {
			case <-attempt.started:
				t.Fatal("startup completed while semantic cleanup was blocked")
			default:
			}
			stopCtx, stopCancel := context.WithTimeout(context.Background(), time.Second)
			defer stopCancel()
			report, err := manager.StopWithReport(stopCtx, "pv.primary")
			if err != nil {
				t.Fatal(err)
			}
			if !errors.Is(stopCtx.Err(), context.DeadlineExceeded) || workCloses.Load() != 1 {
				t.Fatalf("deadline=%v worker closes=%d", stopCtx.Err(), workCloses.Load())
			}
			if report.Class != drivermanager.CloseUnconfirmed || !driver.SafetyQuarantined() {
				t.Fatalf("blocked startup cleanup class=%v quarantine=%v", report.Class, driver.SafetyQuarantined())
			}
			snapshot, _ := manager.Snapshot("pv.primary")
			if !snapshot.SafetyQuarantined || snapshot.Reason.Code != drivermanager.ReasonCloseUnconfirmed {
				t.Fatalf("blocked startup cleanup snapshot=%+v", snapshot)
			}
			if err := manager.Start(context.Background(), "pv.primary"); !errors.Is(err, drivermanager.ErrSafetyQuarantined) {
				t.Fatalf("start=%v", err)
			}
			if err := manager.Replace(context.Background(), "pv.primary"); !errors.Is(err, drivermanager.ErrSafetyQuarantined) {
				t.Fatalf("replace=%v", err)
			}
			wantStarts := int32(0)
			if phase == "during installation" {
				wantStarts = 1
			}
			if driver.Generation() != 1 || workStarts.Load() != wantStarts || endpoint.closes.Load() != 1 {
				t.Fatal("quarantine allowed successor work or lost native cleanup")
			}
			releaseSemantic()
			<-attempt.started
			<-attempt.stopped
			if !errors.Is(attempt.startErr, closeErr) || !errors.Is(attempt.stopErr, closeErr) {
				t.Fatal("startup cleanup lost the worker-close error")
			}
			snapshot, _ = manager.Snapshot("pv.primary")
			if !snapshot.SafetyQuarantined || !driver.SafetyQuarantined() {
				t.Fatal("completed semantic cleanup cleared native quarantine")
			}
		})
	}
}
