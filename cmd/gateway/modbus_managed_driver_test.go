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
	closed chan struct{}
}

func (e *m03Endpoint) OpenConnection(c net.Conn) (modbus.TCPConnectionHandle, error) {
	e.opens.Add(1)
	_ = c.Close()
	return modbus.TCPConnectionHandle{}, nil
}
func (e *m03Endpoint) Close() error {
	e.closes.Add(1)
	if e.closed != nil {
		close(e.closed)
	}
	return e.closeErr
}

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

// m03Watchdog bounds waits for progress the lifecycle makes on its own. It is
// only a hang diagnostic: no assertion depends on how quickly work completes,
// and every ordering is established by channels or observed state.
const m03Watchdog = 2 * time.Minute

func m03Wait[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	watchdog := time.NewTimer(m03Watchdog)
	defer watchdog.Stop()
	select {
	case value := <-ch:
		return value
	case <-watchdog.C:
		t.Fatalf("hang diagnostic: %s", what)
		panic("unreachable")
	}
}

// m03Eventually waits for state that has no completion signal of its own.
func m03Eventually(t *testing.T, what string, done func() bool) {
	t.Helper()
	watchdog := time.NewTimer(m03Watchdog)
	defer watchdog.Stop()
	poll := time.NewTicker(time.Millisecond)
	defer poll.Stop()
	for !done() {
		select {
		case <-poll.C:
		case <-watchdog.C:
			t.Fatalf("hang diagnostic: %s", what)
		}
	}
}

// m03AwaitState follows manager change notifications until accept or reject
// matches the published snapshot.
func m03AwaitState(t *testing.T, manager *drivermanager.Manager, accept, reject func(drivermanager.Snapshot) bool) drivermanager.Snapshot {
	t.Helper()
	watchdog := time.NewTimer(m03Watchdog)
	defer watchdog.Stop()
	for {
		snapshot, changed, ok := manager.Watch("pv.primary")
		if !ok {
			t.Fatal("driver missing")
		}
		if accept(snapshot) {
			return snapshot
		}
		if reject != nil && reject(snapshot) {
			t.Fatalf("unexpected manager state: %+v", snapshot)
		}
		select {
		case <-changed:
		case <-watchdog.C:
			t.Fatalf("hang diagnostic: manager state %+v", snapshot)
		}
	}
}

// m03FenceSignalJournal reports the first journalled generation fence.
type m03FenceSignalJournal struct {
	*semhub.MemoryJournal
	fenced chan struct{}
	once   sync.Once
}

func (j *m03FenceSignalJournal) Append(records ...semhub.EpochRecord) error {
	if err := j.MemoryJournal.Append(records...); err != nil {
		return err
	}
	for _, record := range records {
		if record.Kind == semhub.EventGenerationFenced {
			j.once.Do(func() { close(j.fenced) })
		}
	}
	return nil
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
	ctx, cancel := context.WithTimeout(context.Background(), m03Watchdog)
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
			ctx, cancel := context.WithTimeout(context.Background(), m03Watchdog)
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
	if !driver.SafetyQuarantined() {
		t.Error("unproven connection cleanup did not quarantine the attempt")
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

func TestM03PreAdapterCleanupFailureQuarantinesManager(t *testing.T) {
	for _, phase := range []string{"factory", "dial", "open"} {
		t.Run(phase, func(t *testing.T) {
			startErr := errors.New("synthetic startup failure")
			closeErr := errors.New("synthetic provisional cleanup failure")
			endpoint := &m03OpenFailureEndpoint{openErr: startErr}
			if phase != "open" {
				endpoint.closeErr = closeErr
			}
			var calls atomic.Int32
			driver := m03Driver(t, true, func(modbus.TCPEndpointConfig) (modbusadapter.Endpoint, error) {
				calls.Add(1)
				if phase == "factory" {
					return endpoint, startErr
				}
				return endpoint, nil
			}, nil)
			driver.dial = func(context.Context, string, string) (net.Conn, error) {
				if phase == "dial" {
					return nil, startErr
				}
				client, peer := net.Pipe()
				_ = peer.Close()
				return &m03StartupCloseErrorConn{Conn: client, closeErr: closeErr}, nil
			}
			manager, err := drivermanager.New(drivermanager.Config{Drivers: []drivermanager.DriverConfig{{ID: "pv.primary", Enabled: true, Runtime: driver}}})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), m03Watchdog)
			defer cancel()
			if err := manager.Start(ctx, "pv.primary"); err != nil {
				t.Fatal(err)
			}
			for {
				snapshot, changed, ok := manager.Watch("pv.primary")
				if !ok {
					t.Fatal("driver missing")
				}
				if snapshot.ObservedState == drivermanager.ObservedFailed {
					if !snapshot.SafetyQuarantined || snapshot.Reason.Code != drivermanager.ReasonCloseUnconfirmed {
						t.Fatalf("provisional cleanup snapshot=%+v", snapshot)
					}
					break
				}
				select {
				case <-changed:
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
			attempt := driver.active.Load()
			<-attempt.started
			if attempt.adapter != nil || !errors.Is(attempt.startErr, startErr) || !errors.Is(attempt.startErr, closeErr) || !driver.SafetyQuarantined() {
				t.Fatalf("provisional cleanup adapter=%v error=%v quarantine=%v", attempt.adapter, attempt.startErr, driver.SafetyQuarantined())
			}
			report, err := manager.StopWithReport(ctx, "pv.primary")
			if err != nil || report.Class != drivermanager.CloseUnconfirmed {
				t.Fatalf("provisional cleanup report=%+v error=%v", report, err)
			}
			if err := manager.Start(ctx, "pv.primary"); !errors.Is(err, drivermanager.ErrSafetyQuarantined) {
				t.Fatalf("start=%v", err)
			}
			if err := manager.Replace(ctx, "pv.primary"); !errors.Is(err, drivermanager.ErrSafetyQuarantined) {
				t.Fatalf("replace=%v", err)
			}
			if calls.Load() != 1 || endpoint.closes.Load() != 1 || driver.Generation() != 1 {
				t.Fatal("quarantine allowed native retry or repeated cleanup")
			}
		})
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
			var o23Opens atomic.Int32
			endpoint := &m03BlockingEndpoint{entered: entered, release: release}
			driver := m03Driver(t, true, func(modbus.TCPEndpointConfig) (modbusadapter.Endpoint, error) {
				o23Opens.Add(1)
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
			var o23Stopped, o23Replaced chan error
			var o23CancelReplacement context.CancelFunc
			t.Cleanup(func() {
				if o23CancelReplacement != nil {
					o23CancelReplacement()
				}
				close(release)
				if o23Replaced != nil {
					if err := m03Wait(t, o23Replaced, "replacement caller did not join after release"); err != nil && !errors.Is(err, context.Canceled) {
						t.Errorf("released replacement result=%v", err)
					}
				}
				if o23Stopped != nil {
					if err := m03Wait(t, o23Stopped, "Stop caller did not join after release"); err != nil {
						t.Errorf("released shutdown result=%v", err)
					}
				}
			})
			if _, err := driver.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			o23Attempt := driver.active.Load()
			old := o23Attempt.adapter
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
			o23Stopped = make(chan error, 1)
			go func() { o23Stopped <- driver.Stop(context.Background()) }()
			m03Wait(t, entered, "native shutdown did not enter its blocker")
			m03Eventually(t, "semantic withdrawal waited for native shutdown", func() bool { return !driver.hub.Admitted(asset) })
			retained, err := driver.hub.Inspect(asset)
			if err != nil || !retained.Present {
				t.Fatalf("withdrawal lost retained evidence: %v", err)
			}
			if driver.active.Load() != o23Attempt {
				t.Fatal("pending shutdown released generation ownership")
			}
			select {
			case <-o23Attempt.stopped:
				t.Fatal("shutdown completed before native blocker release")
			default:
			}
			o23RetryContext, o23Cancel := context.WithCancel(context.Background())
			o23CancelReplacement = o23Cancel
			o23Joined := &m03JoinContext{Context: o23RetryContext, joined: make(chan struct{})}
			o23Replaced = make(chan error, 1)
			go func() {
				o23Generation, o23Err := driver.Replace(o23Joined)
				if o23Generation != 0 {
					o23Err = fmt.Errorf("replacement returned generation %d while shutdown blocked: %v", o23Generation, o23Err)
				}
				o23Replaced <- o23Err
			}()
			m03Wait(t, o23Joined.joined, "replacement did not reach the shutdown join")
			o23Cancel()
			o23Err := m03Wait(t, o23Replaced, "replacement did not honor cancellation while shutdown blocked")
			o23Replaced = nil // The replacement caller has joined.
			if !errors.Is(o23Err, context.Canceled) {
				t.Fatalf("replacement bypassed pending shutdown: %v", o23Err)
			}
			if driver.active.Load() != o23Attempt || driver.Generation() != o23Attempt.nativeGeneration {
				t.Fatal("replacement changed pending generation ownership")
			}
			if o23Opens.Load() != 1 {
				t.Fatal("replacement opened an endpoint before shutdown completed")
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
	m03Wait(t, fenced, "withdrawal blocked behind work installation")
	if !attempt.withdrawn() {
		t.Fatal("withdrawal gate did not close")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	stopped := make(chan error, 1)
	go func() { stopped <- driver.Stop(ctx) }()
	if err := m03Wait(t, stopped, "Stop did not honor its deadline"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("cleanup completed while generation effects were still installing: %v", err)
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
			m03Wait(t, entered, "canceled startup did not block")
			attempt := driver.active.Load()
			cancel()
			// The originating caller returns only after its cancellation has
			// reached the attempt, so the blocked startup cannot commit.
			if err := m03Wait(t, initial, "originating caller did not return"); !errors.Is(err, context.Canceled) {
				t.Fatalf("initial start=%v", err)
			}
			// The retry carries no deadline; only the join barrier orders it.
			retryCtx, retryCancel := context.WithCancel(context.Background())
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
			m03Wait(t, joined.joined, "retry did not join the canceled startup")
			if calls.Load() != 1 || driver.active.Load() != attempt {
				t.Fatal("retry opened before predecessor cleanup")
			}
			close(release)
			if err := m03Wait(t, retried, "joined retry did not finish"); err != nil {
				t.Fatalf("joined retry did not start a fresh attempt: %v", err)
			}
			if !attempt.retryableStartup() {
				t.Fatalf("canceled predecessor error=%v closed=%v", attempt.startErr, attempt.startupClosed)
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
	// Synchronous manager calls and change notifications order this test.
	// Neither progress nor shutdown shares a wall-clock budget.
	ctx := context.Background()
	defer func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), m03Watchdog)
		defer shutdownCancel()
		if err := manager.Shutdown(shutdownCtx); err != nil {
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
	// Replace returns once the failed activation has scheduled the manager's
	// retry; the retry then publishes RUNNING (or FAILED) on its own.
	if err := manager.Replace(ctx, "pv.primary"); err != nil {
		t.Fatal(err)
	}
	m03AwaitState(t, manager, func(snapshot drivermanager.Snapshot) bool {
		return snapshot.ObservedState == drivermanager.ObservedRunning
	}, func(snapshot drivermanager.Snapshot) bool {
		return snapshot.ObservedState == drivermanager.ObservedFailed
	})
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
			ctx, cancel := context.WithTimeout(context.Background(), m03Watchdog)
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
	endpoint := &m03Endpoint{closed: make(chan struct{})}
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
	ctx := context.Background()
	if err := manager.Start(ctx, "pv.primary"); err != nil {
		t.Fatal(err)
	}
	m03AwaitState(t, manager, func(snapshot drivermanager.Snapshot) bool {
		return snapshot.ObservedState == drivermanager.ObservedRunning
	}, nil)
	// The caller's stop deadline ends only after both barriers: the fence is
	// blocked in the journal and the worker failure has been latched.
	stopCtx, stopCancel := context.WithCancel(ctx)
	defer stopCancel()
	reports := make(chan drivermanager.CloseReport, 1)
	go func() {
		report, err := manager.StopWithReport(stopCtx, "pv.primary")
		if err != nil {
			t.Error(err)
		}
		reports <- report
	}()
	m03Wait(t, journal.entered, "semantic withdrawal did not reach the blocked journal")
	m03Eventually(t, "worker failure was not latched while semantic withdrawal was blocked", driver.SafetyQuarantined)
	select {
	case report := <-reports:
		t.Fatalf("stop returned while semantic withdrawal was blocked: %+v", report)
	default:
	}
	stopCancel()
	report := m03Wait(t, reports, "stop did not honor its deadline")
	m03Wait(t, endpoint.closed, "native endpoint close did not complete")
	if workCloses.Load() != 1 || endpoint.closes.Load() != 1 {
		t.Fatalf("worker closes=%d endpoint closes=%d", workCloses.Load(), endpoint.closes.Load())
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

// m03DelayedCancellationContext holds both cancellation forwarding and the
// waiting caller until startup has passed its commit decision.
type m03DelayedCancellationContext struct {
	context.Context
	registered     atomic.Bool
	forwardEntered chan struct{}
	forwardRelease chan struct{}
	forwardDone    chan struct{}
	decided        chan struct{}
}

func (ctx *m03DelayedCancellationContext) Value(any) any { return nil }

func (ctx *m03DelayedCancellationContext) Done() <-chan struct{} {
	if ctx.registered.Load() {
		<-ctx.decided
	}
	return ctx.Context.Done()
}

func (ctx *m03DelayedCancellationContext) AfterFunc(f func()) func() bool {
	stop := context.AfterFunc(ctx.Context, func() {
		close(ctx.forwardEntered)
		<-ctx.forwardRelease
		f()
		close(ctx.forwardDone)
	})
	ctx.registered.Store(true)
	return func() bool {
		stopped := stop()
		close(ctx.decided)
		return stopped
	}
}

func TestM03StartupCommitHonorsCallerBeforeCancellationForwarding(t *testing.T) {
	callerCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx := &m03DelayedCancellationContext{
		Context: callerCtx, forwardEntered: make(chan struct{}),
		forwardRelease: make(chan struct{}), forwardDone: make(chan struct{}), decided: make(chan struct{}),
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce, forwardOnce sync.Once
	resume := func() { releaseOnce.Do(func() { close(release) }) }
	forward := func() { forwardOnce.Do(func() { close(ctx.forwardRelease) }) }
	first := &m03Endpoint{}
	var opens, workCloses atomic.Int32
	var workerContext context.Context
	driver := m03Driver(t, true, func(modbus.TCPEndpointConfig) (modbusadapter.Endpoint, error) {
		if opens.Add(1) == 1 {
			return first, nil
		}
		return &m03Endpoint{}, nil
	}, func(runCtx context.Context, _ *modbusadapter.Adapter) modbusManagedWork {
		if opens.Load() != 1 {
			return nil
		}
		workerContext = runCtx
		return &m03Work{start: func() {
			close(entered)
			<-release
		}, close: func() error { workCloses.Add(1); return nil }}
	})
	t.Cleanup(func() { cancel(); resume(); forward() })
	result := make(chan error, 1)
	go func() { _, err := driver.Start(ctx); result <- err }()
	m03Wait(t, entered, "startup did not reach worker installation")
	attempt := driver.active.Load()
	cancel()
	m03Wait(t, ctx.forwardEntered, "cancellation forwarding did not reach its barrier")
	resume()
	m03Wait(t, ctx.decided, "startup did not pass its commit decision")
	m03Wait(t, attempt.started, "startup cleanup did not finish")
	if err := m03Wait(t, result, "originating caller did not return"); !errors.Is(err, context.Canceled) {
		t.Errorf("originating caller error=%v", err)
	}
	if !errors.Is(attempt.startErr, context.Canceled) || !attempt.startupClosed {
		t.Errorf("canceled startup error=%v closed=%v", attempt.startErr, attempt.startupClosed)
	}
	if workerContext.Err() != context.Canceled || workCloses.Load() != 1 || first.closes.Load() != 1 {
		t.Errorf("worker context=%v worker closes=%d endpoint closes=%d", workerContext.Err(), workCloses.Load(), first.closes.Load())
	}
	forward()
	m03Wait(t, ctx.forwardDone, "delayed cancellation forwarding did not finish")
	manager, err := drivermanager.New(drivermanager.Config{Drivers: []drivermanager.DriverConfig{{ID: "pv.primary", Enabled: true, Runtime: driver}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Start(context.Background(), "pv.primary"); err != nil {
		t.Fatal(err)
	}
	m03AwaitState(t, manager, func(snapshot drivermanager.Snapshot) bool {
		return snapshot.ObservedState == drivermanager.ObservedRunning
	}, nil)
	if opens.Load() != 2 || driver.Generation() != 2 || driver.active.Load() == attempt {
		t.Fatal("manager retry reused canceled workers instead of opening a fresh endpoint")
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
			endpoint := &m03Endpoint{closed: make(chan struct{})}
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
			m03Wait(t, journal.entered, "startup cleanup did not reach the blocked fence")
			// Native cleanup runs beside the blocked fence. Its failure is
			// latched before the stop deadline starts, without semantic progress.
			m03Eventually(t, "native failure was not latched while the fence was blocked", driver.SafetyQuarantined)
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
			m03Wait(t, endpoint.closed, "native endpoint close did not complete")
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

func TestM03StartupEndpointFailureQuarantinesWhileSemanticWithdrawalBlocked(t *testing.T) {
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
			closeErr := errors.New("synthetic startup endpoint cleanup failure")
			endpoint := &m03Endpoint{modbusRunLifecycleEndpoint: modbusRunLifecycleEndpoint{closeErr: closeErr}}
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
					return nil
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
			m03Wait(t, journal.entered, "startup cleanup did not reach the blocked fence")
			// Native cleanup runs beside the blocked fence. Its failure is
			// latched before the stop deadline starts, without semantic progress.
			m03Eventually(t, "native failure was not latched while the fence was blocked", driver.SafetyQuarantined)
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
				t.Fatal("startup cleanup lost the endpoint-close error")
			}
			snapshot, _ = manager.Snapshot("pv.primary")
			if !snapshot.SafetyQuarantined || !driver.SafetyQuarantined() {
				t.Fatal("completed semantic cleanup cleared native quarantine")
			}
		})
	}
}

func TestM03ManagerRecoversPartialActivationCleanupAfterJournalRecovery(t *testing.T) {
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
	manager, err := drivermanager.New(drivermanager.Config{Drivers: []drivermanager.DriverConfig{{
		ID: "pv.primary", Enabled: true, Runtime: driver,
	}}})
	if err != nil {
		t.Fatal(err)
	}
	// Synchronous manager calls are the lifecycle barriers; the test runner
	// bounds the regression without canceling an operation mid-inspection.
	ctx := context.Background()
	defer func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), m03Watchdog)
		defer shutdownCancel()
		if err := manager.Shutdown(shutdownCtx); err != nil {
			t.Error(err)
		}
	}()
	if err := manager.Start(ctx, "pv.primary"); err != nil {
		t.Fatal(err)
	}
	var sources []semreg.SourceID
	for i, scope := range []string{"fixture:cleanup-a", "fixture:cleanup-b"} {
		words := sunspectest.ObservedFroniusFloatWords()
		if i != 0 {
			words[52] ^= 1
		}
		adapter := driver.active.Load().adapter
		if err := adapter.RecordSunSpecQualificationObservation(m03ScopedObservation(t, uint64(i+1), words, scope)); err != nil {
			t.Fatal(err)
		}
		view, ok := adapter.SemanticPVCurrentSingle()
		if !ok {
			t.Fatal("source unavailable")
		}
		sources = append(sources, view.Snapshot.Sources[0].SourceID)
	}
	sort.Slice(sources, func(i, j int) bool { return sources[i] < sources[j] })
	journal.source = sources[1]
	// Replace synchronously joins the failed activation before status is read.
	if err := manager.Replace(ctx, "pv.primary"); err != nil {
		t.Fatal(err)
	}
	attempt := driver.active.Load()
	if attempt.adapter != nil || !errors.Is(attempt.startErr, modbusadapter.ErrStartupCloseUnproven) {
		t.Fatalf("partial activation admitted=%v error=%v", attempt.adapter != nil, attempt.startErr)
	}
	if len(endpoints) != 2 || endpoints[0].closes.Load() != 1 || endpoints[1].closes.Load() != 1 {
		t.Fatal("native startup cleanup was not proven exactly once")
	}
	snapshot, ok := manager.Snapshot("pv.primary")
	if !ok || snapshot.SafetyQuarantined || driver.SafetyQuarantined() {
		t.Fatalf("semantic cleanup quarantined manager: %+v", snapshot)
	}
	report, err := manager.StopWithReport(ctx, "pv.primary")
	if err != nil || report.Class != drivermanager.CloseInternalError || !errors.Is(attempt.stopErr, errM03JournalFailure) {
		t.Fatalf("pending semantic cleanup report=%+v error=%v retained=%v", report, err, attempt.stopErr)
	}
	if len(endpoints) != 2 {
		t.Fatal("successor bypassed semantic cleanup")
	}
	journal.source = ""
	journal.unavailable.Store(false)
	if err := manager.Replace(ctx, "pv.primary"); err != nil {
		t.Fatalf("retry retained cleanup after recovery: %v", err)
	}
	snapshot, ok = manager.Snapshot("pv.primary")
	if !ok || snapshot.ObservedState != drivermanager.ObservedRunning || snapshot.SafetyQuarantined || driver.Generation() != 3 {
		t.Fatalf("manager did not recover: %+v", snapshot)
	}
	if len(endpoints) != 3 || endpoints[0].closes.Load() != 1 || endpoints[1].closes.Load() != 1 {
		t.Fatal("recovery repeated native cleanup or lost successor")
	}
	fenced := 0
	activated := make(map[semreg.SourceID]int)
	for _, record := range journal.Records() {
		if record.Kind == semhub.EventGenerationFenced && record.Source == sources[0] && record.Generation == 2 {
			fenced++
		}
		if record.Kind == semhub.EventGenerationActivated && record.Generation == 3 {
			activated[record.Source]++
		}
	}
	if fenced != 1 || activated[sources[0]] != 1 || activated[sources[1]] != 1 {
		t.Fatalf("recovered fences=%d activations=%v", fenced, activated)
	}
	adapter := driver.active.Load().adapter
	if err := adapter.RecordSunSpecQualificationObservation(m03ScopedObservation(t, 10, sunspectest.ObservedFroniusFloatWords(), "fixture:cleanup-a")); err != nil {
		t.Fatal(err)
	}
	view, ok := adapter.SemanticPVCurrentSingle()
	if !ok {
		t.Fatal("recovered driver did not publish")
	}
	m03AssertGeneration(t, view, semreg.SourceEpochID(m03Correlation().Epoch), "3")
}

// Admission fails after one source activates. Cleanup either stalls immediately
// or fails once and stalls when Stop retries the retained semantic owner.
type m03SupersededCleanupJournal struct {
	*semhub.MemoryJournal
	failSource                           semreg.SourceID
	activationEntered, activationRelease chan struct{}
	fenceEntered, fenceRelease           chan struct{}
	retry                                bool
	fences                               atomic.Int32
	once                                 sync.Once
}

func (j *m03SupersededCleanupJournal) Append(records ...semhub.EpochRecord) error {
	for _, record := range records {
		if record.Generation != 2 {
			continue
		}
		if record.Kind == semhub.EventGenerationActivated && record.Source == j.failSource {
			close(j.activationEntered)
			<-j.activationRelease
			return errM03JournalFailure
		}
		if record.Kind == semhub.EventGenerationFenced {
			if !j.retry || j.fences.Add(1) > 1 {
				j.once.Do(func() { close(j.fenceEntered) })
				<-j.fenceRelease
			}
			return errM03JournalFailure
		}
	}
	return j.MemoryJournal.Append(records...)
}

func TestM03SupersededStartupPreservesNativeFailureDuringStopRetry(t *testing.T) {
	for _, connectionFailure := range []bool{false, true} {
		t.Run(fmt.Sprintf("connection failure=%v", connectionFailure), func(t *testing.T) {
			m03SupersededStartupCleanup(t, true, connectionFailure)
		})
	}
}

func TestM03ProvisionalNativeFailureRecordedBeforeSemanticCleanup(t *testing.T) {
	for _, connectionFailure := range []bool{false, true} {
		t.Run(fmt.Sprintf("connection failure=%v", connectionFailure), func(t *testing.T) {
			m03SupersededStartupCleanup(t, false, connectionFailure)
		})
	}
}

func m03SupersededStartupCleanup(t *testing.T, retry, connectionFailure bool) {
	t.Helper()
	journal := &m03SupersededCleanupJournal{MemoryJournal: semhub.NewMemoryJournal(),
		activationEntered: make(chan struct{}), activationRelease: make(chan struct{}),
		fenceEntered: make(chan struct{}), fenceRelease: make(chan struct{}), retry: retry}
	hub, err := semhub.New(semhub.Options{Journal: journal})
	if err != nil {
		t.Fatal(err)
	}
	closeErr := errors.New("synthetic provisional native cleanup failure")
	var calls atomic.Int32
	var failedEndpoint *m03Endpoint
	driver := m03Driver(t, true, func(modbus.TCPEndpointConfig) (modbusadapter.Endpoint, error) {
		endpoint := &m03Endpoint{}
		if calls.Add(1) == 2 {
			if !connectionFailure {
				endpoint.closeErr = closeErr
			}
			failedEndpoint = endpoint
		}
		return endpoint, nil
	}, nil)
	driver.hub = hub
	var activationOnce, fenceOnce sync.Once
	resumeActivation := func() { activationOnce.Do(func() { close(journal.activationRelease) }) }
	resumeFence := func() { fenceOnce.Do(func() { close(journal.fenceRelease) }) }
	t.Cleanup(func() { resumeActivation(); resumeFence() })
	if _, err := driver.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	var sources []semreg.SourceID
	for i, scope := range []string{"fixture:superseded-a", "fixture:superseded-b"} {
		words := sunspectest.ObservedFroniusFloatWords()
		if i != 0 {
			words[52] ^= 1
		}
		adapter := driver.active.Load().adapter
		if err := adapter.RecordSunSpecQualificationObservation(m03ScopedObservation(t, uint64(i+1), words, scope)); err != nil {
			t.Fatal(err)
		}
		view, ok := adapter.SemanticPVCurrentSingle()
		if !ok {
			t.Fatal("source unavailable")
		}
		sources = append(sources, view.Snapshot.Sources[0].SourceID)
	}
	sort.Slice(sources, func(i, j int) bool { return sources[i] < sources[j] })
	if err := driver.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	journal.failSource = sources[1]
	canceled := make(chan struct{})
	dial := driver.dial
	driver.dial = func(ctx context.Context, network, address string) (net.Conn, error) {
		context.AfterFunc(ctx, func() { close(canceled) })
		connection, err := dial(ctx, network, address)
		if connectionFailure && err == nil {
			connection = &m03StartupCloseErrorConn{Conn: connection, closeErr: closeErr}
		}
		return connection, err
	}
	manager, err := drivermanager.New(drivermanager.Config{Drivers: []drivermanager.DriverConfig{{ID: "pv.primary", Enabled: true, Runtime: driver}}})
	if err != nil {
		t.Fatal(err)
	}
	startDone := make(chan error, 1)
	go func() { startDone <- manager.Start(context.Background(), "pv.primary") }()
	<-journal.activationEntered
	attempt := driver.active.Load()
	// The caller's stop deadline is ended explicitly, after the barriers below.
	stopCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reports := make(chan drivermanager.CloseReport, 1)
	go func() {
		report, err := manager.StopWithReport(stopCtx, "pv.primary")
		if err != nil {
			t.Error(err)
		}
		reports <- report
	}()
	m03Wait(t, canceled, "stop did not cancel the superseded startup")
	resumeActivation()
	m03Wait(t, journal.fenceEntered, "cleanup did not reach the blocked fence")
	// With retry, startup returned before Stop retried the retained owner, so
	// its native failure is already latched and the retry must not clear it.
	if retry && !attempt.nativeCloseUnconfirmed() {
		t.Error("Stop cleared the latched startup native failure")
	}
	// Otherwise the provisional native close runs beside the blocked fence.
	m03Eventually(t, "native failure was not latched while the fence was blocked", attempt.nativeCloseUnconfirmed)
	if failedEndpoint.closes.Load() != 1 || !driver.SafetyQuarantined() {
		t.Errorf("native failure unavailable before semantic wait: closes=%d quarantine=%v", failedEndpoint.closes.Load(), driver.SafetyQuarantined())
	}
	select {
	case report := <-reports:
		t.Fatalf("stop returned while the fence was blocked: %+v", report)
	default:
	}
	cancel()
	report := m03Wait(t, reports, "stop did not honor its deadline")
	if report.Class != drivermanager.CloseUnconfirmed {
		t.Errorf("close report=%+v", report)
	}
	snapshot, _ := manager.Snapshot("pv.primary")
	if !snapshot.SafetyQuarantined || snapshot.Reason.Code != drivermanager.ReasonCloseUnconfirmed {
		t.Fatalf("superseded startup snapshot=%+v", snapshot)
	}
	if err := manager.Start(context.Background(), "pv.primary"); !errors.Is(err, drivermanager.ErrSafetyQuarantined) {
		t.Errorf("start=%v", err)
	}
	if err := manager.Replace(context.Background(), "pv.primary"); !errors.Is(err, drivermanager.ErrSafetyQuarantined) {
		t.Errorf("replace=%v", err)
	}
	resumeFence()
	<-attempt.started
	<-attempt.stopped
	if err := <-startDone; err != nil {
		t.Fatal(err)
	}
	if !errors.Is(attempt.startErr, closeErr) || !errors.Is(attempt.stopErr, closeErr) || !driver.SafetyQuarantined() {
		t.Fatal("completed cleanup lost native failure evidence")
	}
	snapshot, _ = manager.Snapshot("pv.primary")
	if !snapshot.SafetyQuarantined || snapshot.Reason.Code != drivermanager.ReasonCloseUnconfirmed {
		t.Fatal("completed semantic cleanup cleared manager quarantine")
	}
	if err := manager.Start(context.Background(), "pv.primary"); !errors.Is(err, drivermanager.ErrSafetyQuarantined) {
		t.Errorf("start after cleanup=%v", err)
	}
	if err := manager.Replace(context.Background(), "pv.primary"); !errors.Is(err, drivermanager.ErrSafetyQuarantined) {
		t.Errorf("replace after cleanup=%v", err)
	}
	if calls.Load() != 2 || driver.Generation() != 2 || failedEndpoint.closes.Load() != 1 {
		t.Fatal("quarantine allowed replacement or repeated native cleanup")
	}
}

// Caller cancellation reaches startup before or during worker installation,
// and joining the worker then blocks. Publication admission is revoked and the
// hub fence completes without waiting for that join; the endpoint still closes
// only after it.
func TestM03StartupCleanupFencesPublicationWhileWorkCloseBlocks(t *testing.T) {
	for _, phase := range []string{"before installation", "during installation"} {
		t.Run(phase, func(t *testing.T) {
			journal := &m03FenceSignalJournal{MemoryJournal: semhub.NewMemoryJournal(), fenced: make(chan struct{})}
			hub, err := semhub.New(semhub.Options{Journal: journal})
			if err != nil {
				t.Fatal(err)
			}
			entered, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
			joinEntered, joinRelease := make(chan struct{}), make(chan struct{})
			var releaseOnce, joinOnce sync.Once
			resume := func() { releaseOnce.Do(func() { close(release) }) }
			resumeJoin := func() { joinOnce.Do(func() { close(joinRelease) }) }
			// Unblock startup and its worker join before the fixture joins Stop.
			t.Cleanup(func() { resume(); resumeJoin() })
			var calls, workStarts, workCloses atomic.Int32
			var asset semreg.AssetID
			readableDuringJoin := make(chan bool, 1)
			first := &m03Endpoint{}
			observation := m03Observation(t)
			driver := m03Driver(t, true, func(modbus.TCPEndpointConfig) (modbusadapter.Endpoint, error) {
				if calls.Add(1) == 1 {
					return first, nil
				}
				return &m03Endpoint{}, nil
			}, func(runCtx context.Context, adapter *modbusadapter.Adapter) modbusManagedWork {
				if calls.Load() != 1 {
					return nil
				}
				if err := adapter.RecordSunSpecQualificationObservation(observation); err != nil {
					t.Error(err)
				}
				view, ok := adapter.SemanticPVCurrentSingle()
				if !ok {
					t.Error("fixture publication missing")
				}
				asset = view.Snapshot.AssetID
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
					_, readable := adapter.SemanticPVCurrentSingle()
					readableDuringJoin <- readable
					close(joinEntered)
					<-joinRelease
					return nil
				}}
			})
			driver.hub = hub
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			result := make(chan error, 1)
			go func() { _, err := driver.Start(ctx); result <- err }()
			m03Wait(t, entered, "startup did not reach worker installation")
			attempt := driver.active.Load()
			cancel()
			m03Wait(t, canceled, "caller cancellation did not reach installation")
			if err := m03Wait(t, result, "originating caller did not return"); !errors.Is(err, context.Canceled) {
				t.Fatalf("initial start=%v", err)
			}
			resume()
			m03Wait(t, joinEntered, "startup cleanup did not join the worker")
			if m03Wait(t, readableDuringJoin, "worker join did not report admission") {
				t.Error("canceled generation stayed readable while its worker was joining")
			}
			m03Wait(t, journal.fenced, "publication fence waited for the blocked worker join")
			m03Eventually(t, "withdrawn asset stayed admitted while the worker join was blocked", func() bool { return !hub.Admitted(asset) })
			select {
			case <-attempt.started:
				t.Fatal("startup completed while its worker join was blocked")
			default:
			}
			if driver.SafetyQuarantined() || first.closes.Load() != 0 {
				t.Fatalf("pending worker join quarantine=%v endpoint closes=%d", driver.SafetyQuarantined(), first.closes.Load())
			}
			resumeJoin()
			m03Wait(t, attempt.started, "startup cleanup did not finish")
			wantStarts := int32(0)
			if phase == "during installation" {
				wantStarts = 1
			}
			if attempt.startErr == nil || !attempt.startupClosed || workStarts.Load() != wantStarts || workCloses.Load() != 1 || first.closes.Load() != 1 {
				t.Fatalf("canceled installation error=%v closed=%v starts=%d work closes=%d endpoint closes=%d", attempt.startErr, attempt.startupClosed, workStarts.Load(), workCloses.Load(), first.closes.Load())
			}
			if generation, err := driver.Start(context.Background()); err != nil || generation != 2 {
				t.Fatalf("retry generation=%d error=%v", generation, err)
			}
		})
	}
}

// Stop supersedes a startup whose worker installation is blocked. The hub
// fence completes before the installation join; work and endpoint close only
// after it.
func TestM03StopFencesPublicationWhileInstallationBlocked(t *testing.T) {
	journal := &m03FenceSignalJournal{MemoryJournal: semhub.NewMemoryJournal(), fenced: make(chan struct{})}
	hub, err := semhub.New(semhub.Options{Journal: journal})
	if err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	resume := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(resume)
	var workStarts, workCloses atomic.Int32
	var asset semreg.AssetID
	endpoint := &m03Endpoint{}
	observation := m03Observation(t)
	driver := m03Driver(t, true, func(modbus.TCPEndpointConfig) (modbusadapter.Endpoint, error) {
		return endpoint, nil
	}, func(_ context.Context, adapter *modbusadapter.Adapter) modbusManagedWork {
		if err := adapter.RecordSunSpecQualificationObservation(observation); err != nil {
			t.Error(err)
		}
		view, ok := adapter.SemanticPVCurrentSingle()
		if !ok {
			t.Error("fixture publication missing")
		}
		asset = view.Snapshot.AssetID
		return &m03Work{start: func() {
			workStarts.Add(1)
			close(entered)
			<-release
		}, close: func() error { workCloses.Add(1); return nil }}
	})
	driver.hub = hub
	result := make(chan error, 1)
	go func() { _, err := driver.Start(context.Background()); result <- err }()
	m03Wait(t, entered, "worker installation did not start")
	attempt := driver.active.Load()
	stopCtx, stopCancel := context.WithCancel(context.Background())
	defer stopCancel()
	stopped := make(chan error, 1)
	go func() { stopped <- driver.Stop(stopCtx) }()
	m03Wait(t, journal.fenced, "publication fence waited for the installation join")
	m03Eventually(t, "withdrawn asset stayed admitted while installation was blocked", func() bool { return !hub.Admitted(asset) })
	if _, ok := attempt.adapter.SemanticPVCurrentSingle(); ok {
		t.Error("withdrawn generation stayed readable")
	}
	select {
	case <-attempt.started:
		t.Fatal("installation joined while still blocked")
	case err := <-stopped:
		t.Fatalf("stop completed before the installation join: %v", err)
	default:
	}
	if workCloses.Load() != 0 || endpoint.closes.Load() != 0 {
		t.Fatal("worker or endpoint closed before installation joined")
	}
	stopCancel()
	if err := m03Wait(t, stopped, "stop did not honor its deadline"); !errors.Is(err, context.Canceled) {
		t.Fatalf("stop deadline=%v", err)
	}
	resume()
	if err := m03Wait(t, result, "withdrawn start did not return"); err == nil {
		t.Fatal("withdrawn installation admitted")
	}
	if err := driver.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if workStarts.Load() != 1 || workCloses.Load() != 1 || endpoint.closes.Load() != 1 || driver.active.Load() != nil {
		t.Fatal("stop did not join installation, then close work and endpoint exactly once")
	}
}

// A stop deadline that expires while the publication fence is stalled reports
// a fence timeout. A pending semantic fence is not a native close failure, so
// the driver is not quarantined, but no successor is admitted until it ends.
func TestM03StalledPublicationFenceReportsFenceTimeout(t *testing.T) {
	journal := &m03BlockingFenceJournal{MemoryJournal: semhub.NewMemoryJournal(), entered: make(chan struct{}), release: make(chan struct{})}
	hub, err := semhub.New(semhub.Options{Journal: journal})
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	first := &m03Endpoint{}
	driver := m03Driver(t, true, func(modbus.TCPEndpointConfig) (modbusadapter.Endpoint, error) {
		if calls.Add(1) == 1 {
			return first, nil
		}
		return &m03Endpoint{}, nil
	}, nil)
	var semanticOnce sync.Once
	releaseSemantic := func() { semanticOnce.Do(func() { close(journal.release) }) }
	// Release the fence before the fixture's registered cleanup joins Stop.
	t.Cleanup(releaseSemantic)
	driver.hub = hub
	if _, err := driver.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := driver.active.Load().adapter.RecordSunSpecQualificationObservation(m03Observation(t)); err != nil {
		t.Fatal(err)
	}
	stopCtx, stopCancel := context.WithCancel(context.Background())
	defer stopCancel()
	stopped := make(chan error, 1)
	go func() { stopped <- driver.Stop(stopCtx) }()
	m03Wait(t, journal.entered, "publication fence did not stall")
	m03Eventually(t, "native close waited for the stalled fence", func() bool { return first.closes.Load() == 1 })
	select {
	case err := <-stopped:
		t.Fatalf("stop returned while the fence was stalled: %v", err)
	default:
	}
	stopCancel()
	err = m03Wait(t, stopped, "stop did not honor its deadline")
	if !errors.Is(err, errModbusPublicationFenceTimeout) || !errors.Is(err, context.Canceled) {
		t.Fatalf("stalled fence stop=%v", err)
	}
	if driver.SafetyQuarantined() {
		t.Fatal("stalled semantic fence reported a native close failure")
	}
	if _, err := driver.Start(context.Background()); err == nil || driver.Generation() != 1 {
		t.Fatalf("successor admitted while the fence was stalled: %v", err)
	}
	releaseSemantic()
	if err := driver.Stop(context.Background()); err != nil {
		t.Fatalf("completed fence stop=%v", err)
	}
	if generation, err := driver.Start(context.Background()); err != nil || generation != 2 {
		t.Fatalf("successor generation=%d error=%v", generation, err)
	}
	if calls.Load() != 2 || first.closes.Load() != 1 {
		t.Fatal("fence timeout repeated native cleanup or skipped the successor")
	}
}

// m03HeldCompensationJournal fails one source's successor activation and holds
// the compensating fence until released; the fence then succeeds.
type m03HeldCompensationJournal struct {
	*semhub.MemoryJournal
	failSource   semreg.SourceID
	fenceEntered chan struct{}
	fenceRelease chan struct{}
	once         sync.Once
}

func (j *m03HeldCompensationJournal) Append(records ...semhub.EpochRecord) error {
	for _, record := range records {
		if record.Generation != 2 {
			continue
		}
		if record.Kind == semhub.EventGenerationActivated && record.Source == j.failSource {
			return errM03JournalFailure
		}
		if record.Kind == semhub.EventGenerationFenced {
			j.once.Do(func() { close(j.fenceEntered) })
			<-j.fenceRelease
		}
	}
	return j.MemoryJournal.Append(records...)
}

// The compensating fence of a failed epoch admission stalls inside startup. A
// stop deadline reached then is a fence timeout too; native cleanup succeeded,
// so nothing is quarantined, and the successor waits for the fence.
func TestM03StalledCompensatingFenceReportsFenceTimeout(t *testing.T) {
	journal := &m03HeldCompensationJournal{MemoryJournal: semhub.NewMemoryJournal(), fenceEntered: make(chan struct{}), fenceRelease: make(chan struct{})}
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
	var releaseOnce sync.Once
	releaseFence := func() { releaseOnce.Do(func() { close(journal.fenceRelease) }) }
	// Release the fence before the fixture's registered cleanup joins Stop.
	t.Cleanup(releaseFence)
	driver.hub = hub
	if _, err := driver.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	var sources []semreg.SourceID
	for i, scope := range []string{"fixture:compensation-a", "fixture:compensation-b"} {
		words := sunspectest.ObservedFroniusFloatWords()
		if i != 0 {
			words[52] ^= 1
		}
		adapter := driver.active.Load().adapter
		if err := adapter.RecordSunSpecQualificationObservation(m03ScopedObservation(t, uint64(i+1), words, scope)); err != nil {
			t.Fatal(err)
		}
		view, ok := adapter.SemanticPVCurrentSingle()
		if !ok {
			t.Fatal("source unavailable")
		}
		sources = append(sources, view.Snapshot.Sources[0].SourceID)
	}
	sort.Slice(sources, func(i, j int) bool { return sources[i] < sources[j] })
	if err := driver.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	journal.failSource = sources[1]
	startDone := make(chan error, 1)
	go func() { _, err := driver.Start(context.Background()); startDone <- err }()
	m03Wait(t, journal.fenceEntered, "compensating fence did not stall")
	stopCtx, stopCancel := context.WithCancel(context.Background())
	defer stopCancel()
	stopped := make(chan error, 1)
	go func() { stopped <- driver.Stop(stopCtx) }()
	select {
	case err := <-stopped:
		t.Fatalf("stop returned while the compensating fence was stalled: %v", err)
	default:
	}
	stopCancel()
	err = m03Wait(t, stopped, "stop did not honor its deadline")
	if !errors.Is(err, errModbusPublicationFenceTimeout) || !errors.Is(err, context.Canceled) {
		t.Fatalf("stalled compensating fence stop=%v", err)
	}
	if driver.SafetyQuarantined() || driver.Generation() != 2 {
		t.Fatalf("stalled compensation quarantine=%v generation=%d", driver.SafetyQuarantined(), driver.Generation())
	}
	releaseFence()
	if err := m03Wait(t, startDone, "failed admission did not return"); !errors.Is(err, errM03JournalFailure) || errors.Is(err, modbusadapter.ErrStartupCloseUnproven) {
		t.Fatalf("failed admission=%v", err)
	}
	if err := driver.Stop(context.Background()); err != nil {
		t.Fatalf("completed compensation stop=%v", err)
	}
	if generation, err := driver.Start(context.Background()); err != nil || generation != 3 {
		t.Fatalf("successor generation=%d error=%v", generation, err)
	}
	fenced := 0
	for _, record := range journal.Records() {
		if record.Kind == semhub.EventGenerationFenced && record.Source == sources[0] && record.Generation == 2 {
			fenced++
		}
	}
	if fenced != 1 || len(endpoints) != 3 || endpoints[1].closes.Load() != 1 {
		t.Fatalf("compensating fences=%d endpoints=%d", fenced, len(endpoints))
	}
}
