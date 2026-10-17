package main

import (
	"context"
	"encoding/binary"
	"errors"
	"math"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/vaillant/systemconfig"
	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
)

type b524InterfaceIdentityFake struct {
	mu     sync.Mutex
	frames []protocol.Frame
	reply  func(context.Context, protocol.Frame) (*protocol.Frame, error)
}

func (bus *b524InterfaceIdentityFake) Send(ctx context.Context, frame protocol.Frame) (*protocol.Frame, error) {
	bus.mu.Lock()
	bus.frames = append(bus.frames, frame)
	bus.mu.Unlock()
	return bus.reply(ctx, frame)
}

func (bus *b524InterfaceIdentityFake) requests() []protocol.Frame {
	bus.mu.Lock()
	defer bus.mu.Unlock()
	return append([]protocol.Frame(nil), bus.frames...)
}

func b524InterfaceFloat(value float32) []byte {
	data := make([]byte, 4)
	binary.LittleEndian.PutUint32(data, math.Float32bits(value))
	return data
}

func newB524InterfaceIdentityFixture(t *testing.T) (*registry.DeviceRegistry, *vaillantSemanticPoller, *b524InterfaceIdentityFake) {
	t.Helper()
	reg := registry.NewDeviceRegistry(nil)
	// Synthetic reply values with the public, retained controller-family key.
	reg.Register(registry.DeviceInfo{Address: 0x15, Manufacturer: "Vaillant", DeviceID: "BASV0", SoftwareVersion: "0217", HardwareVersion: "8803"})
	bus := &b524InterfaceIdentityFake{reply: func(context.Context, protocol.Frame) (*protocol.Frame, error) {
		return &protocol.Frame{Data: b524InterfaceFloat(1)}, nil
	}}
	poller := &vaillantSemanticPoller{
		reg: reg, controller: 0x15, source: 0x71,
		provider: graphql.NewLiveSemanticProvider(),
		system:   &vaillantSystemSnapshot{Controller: 0x15}, sendFrameFn: bus.Send,
	}
	t.Cleanup(func() { b524InterfaceIdentityStores.Delete(reg) })
	return reg, poller, bus
}

func assertB524InterfaceBound(t *testing.T, poller *vaillantSemanticPoller, max float64, source string) {
	t.Helper()
	bound := systemconfig.ResolveDHWBivalenceBound(poller.controllerB524Identity())
	if bound.MinC != -20 || bound.MaxC != max || bound.Source != source {
		t.Fatalf("discovery bound = %+v; want -20..%g source %s", bound, max, source)
	}
	poller.publishSystem(semanticSnapshotSourceLive)
	published := poller.provider.System().Config.DhwBivalenceBound
	if published == nil || published.MinC != bound.MinC || published.MaxC != bound.MaxC || published.Source != bound.Source {
		t.Fatalf("published bound = %+v; want %+v", published, bound)
	}
	mcpSystem := (mcpSemanticProviderAdapter{provider: poller.provider}).System()
	if mcpSystem == nil || mcpSystem.Config == nil || mcpSystem.Config.DhwBivalenceBound == nil {
		t.Fatal("MCP bound absent")
	}
	mb := mcpSystem.Config.DhwBivalenceBound
	if mb.MinC != published.MinC || mb.MaxC != published.MaxC || mb.Source != published.Source || mb.Profile != published.Profile || mb.Reason != published.Reason || mb.NarrowedOnly != published.NarrowedOnly {
		t.Fatalf("MCP/GraphQL bound drift: %+v, %+v", mb, published)
	}
}

func TestB524InterfaceIdentityDiscoveryToBound(t *testing.T) {
	reg, poller, bus := newB524InterfaceIdentityFixture(t)
	enrichB524InterfaceIdentity(context.Background(), reg, &timeoutBus{bus: bus, timeout: time.Second}, 0x71, 0)
	frames := bus.requests()
	if len(frames) != 2 {
		t.Fatalf("requests = %d; want exactly two", len(frames))
	}
	for i, frame := range frames {
		want := protocol.Frame{Source: 0x71, Target: 0x15, Primary: 0xB5, Secondary: 0x24, Data: []byte{0, byte(6 + i), 0}}
		if !reflect.DeepEqual(frame, want) {
			t.Fatalf("frame %d = %+v; want %+v", i, frame, want)
		}
	}
	assertB524InterfaceBound(t, poller, 20, "controller_profile")
	if len(bus.requests()) != 2 {
		t.Fatal("bound resolution/publication acquired data")
	}
	store := b524InterfaceIdentities(reg)
	store.mu.Lock()
	record := store.records[0x15]
	store.mu.Unlock()
	if record.identity.APIVersion != "1" || record.identity.Revision != "1" || record.session == 0 || record.source != 0x71 || record.address != 0x15 || record.acquiredAt.IsZero() {
		t.Fatalf("incomplete provenance: %+v", record)
	}
	if !reflect.DeepEqual(record.raw[0][:], b524InterfaceFloat(1)) || !reflect.DeepEqual(record.raw[1][:], b524InterfaceFloat(1)) {
		t.Fatal("native reply evidence lost")
	}
	// A still-admissible last-known-good record requires no repeat acquisition.
	enrichB524InterfaceIdentity(context.Background(), reg, bus, 0x71, 0)
	if len(bus.requests()) != 2 {
		t.Fatal("fresh identity was reacquired")
	}
}

func TestB524InterfaceIdentityInvalidReplies(t *testing.T) {
	cases := []struct {
		name     string
		data     []byte
		err      error
		nilReply bool
	}{
		{name: "timeout", err: context.DeadlineExceeded},
		{name: "unsupported", err: errors.New("unsupported")},
		{name: "nil", nilReply: true},
		{name: "empty"}, {name: "short", data: []byte{0, 0, 0}},
		{name: "long", data: []byte{0, 0, 128, 63, 0}},
		{name: "nan", data: b524InterfaceFloat(float32(math.NaN()))},
		{name: "infinity", data: b524InterfaceFloat(float32(math.Inf(1)))},
		{name: "negative infinity", data: b524InterfaceFloat(float32(math.Inf(-1)))},
		{name: "fractional", data: b524InterfaceFloat(1.5)},
		{name: "negative", data: b524InterfaceFloat(-1)},
		{name: "out of exact range", data: b524InterfaceFloat((1 << 24) + 2)},
	}
	for _, tc := range cases {
		for _, badID := range []byte{6, 7} {
			t.Run(tc.name+string(rune('0'+badID)), func(t *testing.T) {
				reg, poller, bus := newB524InterfaceIdentityFixture(t)
				bus.reply = func(_ context.Context, frame protocol.Frame) (*protocol.Frame, error) {
					if frame.Data[1] != badID {
						return &protocol.Frame{Data: b524InterfaceFloat(1)}, nil
					}
					if tc.nilReply {
						return nil, nil
					}
					return &protocol.Frame{Data: tc.data}, tc.err
				}
				enrichB524InterfaceIdentity(context.Background(), reg, bus, 0x71, 0)
				id := poller.controllerB524Identity()
				if id.APIVersion != "" || id.Revision != "" {
					t.Fatalf("partial identity published: %+v", id)
				}
				assertB524InterfaceBound(t, poller, 50, "unmatched_policy")
				if len(bus.requests()) != 2 {
					t.Fatalf("requests = %d; want two, with no resolver/publication I/O", len(bus.requests()))
				}
			})
		}
	}
}

func TestB524InterfaceIdentityQualifierConversion(t *testing.T) {
	for _, tc := range []struct {
		value float32
		want  string
	}{{0, "0"}, {1, "1"}, {42, "42"}, {1 << 24, "16777216"}} {
		got, ok := b524InterfaceQualifier(b524InterfaceFloat(tc.value))
		if !ok || got != tc.want {
			t.Fatalf("%g => %q,%v; want %q", tc.value, got, ok, tc.want)
		}
	}
}

func TestB524InterfaceIdentityExactFields(t *testing.T) {
	for _, field := range []string{"EID", "SoftwareRaw", "HardwareRaw", "APIVersion", "Revision"} {
		t.Run(field, func(t *testing.T) {
			reg, poller, bus := newB524InterfaceIdentityFixture(t)
			info := registry.DeviceInfo{Address: 0x15, Manufacturer: "Vaillant", DeviceID: "BASV0", SoftwareVersion: "0217", HardwareVersion: "8803"}
			switch field {
			case "EID":
				info.DeviceID = "SYNTH"
			case "SoftwareRaw":
				info.SoftwareVersion = "0218"
			case "HardwareRaw":
				info.HardwareVersion = "8804"
			}
			reg.Register(info)
			bus.reply = func(_ context.Context, frame protocol.Frame) (*protocol.Frame, error) {
				v := float32(1)
				if (field == "APIVersion" && frame.Data[1] == 6) || (field == "Revision" && frame.Data[1] == 7) {
					v = 2
				}
				return &protocol.Frame{Data: b524InterfaceFloat(v)}, nil
			}
			enrichB524InterfaceIdentity(context.Background(), reg, bus, 0x71, 0)
			id := poller.controllerB524Identity()
			if id.EID != info.DeviceID || id.SoftwareRaw != info.SoftwareVersion || id.HardwareRaw != info.HardwareVersion || id.APIVersion == "" || id.Revision == "" {
				t.Fatalf("identity not independently acquired: %+v", id)
			}
			assertB524InterfaceBound(t, poller, 50, "unmatched_policy")
			if len(bus.requests()) != 2 {
				t.Fatal("unexpected resolver I/O")
			}
		})
	}
}

func TestB524InterfaceIdentityProvenanceFields(t *testing.T) {
	for _, field := range []string{"EID", "SoftwareRaw", "HardwareRaw", "address", "source", "session", "registryGeneration", "missing version", "missing revision", "expired", "future", "zero time"} {
		t.Run(field, func(t *testing.T) {
			reg, poller, bus := newB524InterfaceIdentityFixture(t)
			enrichB524InterfaceIdentity(context.Background(), reg, bus, 0x71, 0)
			store := b524InterfaceIdentities(reg)
			store.mu.Lock()
			record := store.records[0x15]
			switch field {
			case "EID":
				record.identity.EID = "SYNTH"
			case "SoftwareRaw":
				record.identity.SoftwareRaw = "0218"
			case "HardwareRaw":
				record.identity.HardwareRaw = "8804"
			case "address":
				record.address = 0x35
			case "source":
				record.source = 0x31
			case "session":
				record.session++
			case "registryGeneration":
				record.registryGeneration++
			case "missing version":
				record.identity.APIVersion = ""
			case "missing revision":
				record.identity.Revision = ""
			case "expired":
				record.acquiredAt = time.Now().Add(-b524InterfaceIdentityMaxAge)
			case "future":
				record.acquiredAt = time.Now().Add(time.Hour)
			case "zero time":
				record.acquiredAt = time.Time{}
			}
			store.records[0x15] = record
			store.mu.Unlock()
			assertB524InterfaceBound(t, poller, 50, "unmatched_policy")
			if len(bus.requests()) != 2 {
				t.Fatal("provenance validation acquired data")
			}
		})
	}
}

func TestB524InterfaceIdentitySwapAndExpiry(t *testing.T) {
	for _, field := range []string{"EID", "SoftwareRaw", "HardwareRaw", "generation", "age"} {
		t.Run(field, func(t *testing.T) {
			reg, poller, bus := newB524InterfaceIdentityFixture(t)
			enrichB524InterfaceIdentity(context.Background(), reg, bus, 0x71, 0)
			info := registry.DeviceInfo{Address: 0x15, Manufacturer: "Vaillant", DeviceID: "BASV0", SoftwareVersion: "0217", HardwareVersion: "8803"}
			switch field {
			case "EID":
				info.DeviceID = "SYNTH"
			case "SoftwareRaw":
				info.SoftwareVersion = "0218"
			case "HardwareRaw":
				info.HardwareVersion = "8804"
			case "age":
				store := b524InterfaceIdentities(reg)
				store.mu.Lock()
				record := store.records[0x15]
				record.acquiredAt = time.Now().Add(-b524InterfaceIdentityMaxAge)
				store.records[0x15] = record
				store.mu.Unlock()
			}
			if field != "age" {
				reg.Register(info)
			}
			assertB524InterfaceBound(t, poller, 50, "unmatched_policy")
			enrichB524InterfaceIdentity(context.Background(), reg, bus, 0x71, 0)
			if len(bus.requests()) != 4 {
				t.Fatalf("requests = %d; want two per generation", len(bus.requests()))
			}
			id := poller.controllerB524Identity()
			if id.EID != info.DeviceID || id.SoftwareRaw != info.SoftwareVersion || id.HardwareRaw != info.HardwareVersion || id.APIVersion != "1" || id.Revision != "1" {
				t.Fatalf("reacquired identity = %+v", id)
			}
		})
	}
}

func TestB524InterfaceIdentityNoControllerSplice(t *testing.T) {
	reg, poller, bus := newB524InterfaceIdentityFixture(t)
	reg.Register(registry.DeviceInfo{Address: 0x35, Manufacturer: "Vaillant", DeviceID: "SYNTH", SoftwareVersion: "0317", HardwareVersion: "9803"})
	reg.Register(registry.DeviceInfo{Address: 0x08, Manufacturer: "Vaillant", DeviceID: "SYNTH", SoftwareVersion: "0317", HardwareVersion: "9803"})
	bus.reply = func(_ context.Context, f protocol.Frame) (*protocol.Frame, error) {
		if (f.Target == 0x15 && f.Data[1] == 6) || (f.Target == 0x35 && f.Data[1] == 7) {
			return &protocol.Frame{Data: b524InterfaceFloat(1)}, nil
		}
		return nil, errors.New("unsupported")
	}
	enrichB524InterfaceIdentity(context.Background(), reg, bus, 0x71, 0)
	for _, addr := range []byte{0x15, 0x35} {
		poller.controller = addr
		id := poller.controllerB524Identity()
		if id.APIVersion != "" || id.Revision != "" {
			t.Fatalf("cross-controller splice: %+v", id)
		}
	}
	frames := bus.requests()
	if len(frames) != 4 || frames[0].Target != 0x15 || frames[1].Target != 0x15 || frames[2].Target != 0x35 || frames[3].Target != 0x35 {
		t.Fatalf("noncanonical or unbounded requests: %+v", frames)
	}
	other := registry.NewDeviceRegistry(nil)
	other.Register(registry.DeviceInfo{Address: 0x15, Manufacturer: "Vaillant", DeviceID: "BASV0", SoftwareVersion: "0217", HardwareVersion: "8803"})
	poller.reg = other
	poller.controller = 0x15
	assertB524InterfaceBound(t, poller, 50, "unmatched_policy")
}

func TestB524InterfaceIdentityNoGenerationSplice(t *testing.T) {
	reg, poller, bus := newB524InterfaceIdentityFixture(t)
	entered, release := make(chan struct{}), make(chan struct{})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	var once sync.Once
	defer once.Do(func() { close(release) })
	bus.reply = func(ctx context.Context, f protocol.Frame) (*protocol.Frame, error) {
		if f.Data[1] == 7 {
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return &protocol.Frame{Data: b524InterfaceFloat(1)}, nil
	}
	done := make(chan struct{})
	go func() { defer close(done); enrichB524InterfaceIdentity(ctx, reg, bus, 0x71, 0) }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("first acquisition did not reach barrier")
	}
	assertB524InterfaceBound(t, poller, 50, "unmatched_policy")
	newBus := &b524InterfaceIdentityFake{reply: func(_ context.Context, f protocol.Frame) (*protocol.Frame, error) {
		return &protocol.Frame{Data: b524InterfaceFloat(float32(f.Data[1] - 4))}, nil
	}}
	enrichB524InterfaceIdentity(ctx, reg, newBus, 0x71, 0)
	once.Do(func() { close(release) })
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("old acquisition did not finish")
	}
	id := poller.controllerB524Identity()
	if id.APIVersion != "2" || id.Revision != "3" {
		t.Fatalf("older generation replaced/spliced newer identity: %+v", id)
	}
	if len(bus.requests()) != 2 || len(newBus.requests()) != 2 {
		t.Fatal("unbounded generation reads")
	}
}

func TestB524InterfaceIdentityRegistryChangesDuringAcquisition(t *testing.T) {
	reg, poller, bus := newB524InterfaceIdentityFixture(t)
	bus.reply = func(_ context.Context, f protocol.Frame) (*protocol.Frame, error) {
		if f.Data[1] == 7 {
			reg.Register(registry.DeviceInfo{Address: 0x15, Manufacturer: "Vaillant", DeviceID: "SYNTH", SoftwareVersion: "0317", HardwareVersion: "9803"})
			reg.Register(registry.DeviceInfo{Address: 0x15, Manufacturer: "Vaillant", DeviceID: "BASV0", SoftwareVersion: "0217", HardwareVersion: "8803"})
		}
		return &protocol.Frame{Data: b524InterfaceFloat(1)}, nil
	}
	enrichB524InterfaceIdentity(context.Background(), reg, bus, 0x71, 0)
	assertB524InterfaceBound(t, poller, 50, "unmatched_policy")
}

func TestB524InterfaceIdentityDeadlineAndCompanion(t *testing.T) {
	reg, poller, bus := newB524InterfaceIdentityFixture(t)
	enrichB524InterfaceIdentity(context.Background(), reg, bus, 0x71, 0x15)
	if len(bus.requests()) != 0 {
		t.Fatal("reserved companion probed")
	}
	bus.reply = func(ctx context.Context, _ protocol.Frame) (*protocol.Frame, error) {
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) > time.Second {
			t.Error("scan request deadline absent")
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	enrichB524InterfaceIdentity(ctx, reg, &timeoutBus{bus: bus, timeout: time.Millisecond}, 0x71, 0)
	assertB524InterfaceBound(t, poller, 50, "unmatched_policy")
	if len(bus.requests()) != 2 {
		t.Fatal("timeout retry count changed")
	}
	cancel()
	enrichB524InterfaceIdentity(ctx, reg, bus, 0x71, 0)
	if len(bus.requests()) != 2 {
		t.Fatal("canceled enrichment acquired data")
	}
}

func TestB524InterfaceIdentityRegistryAndSourceIsolation(t *testing.T) {
	reg, poller, bus := newB524InterfaceIdentityFixture(t)
	enrichB524InterfaceIdentity(context.Background(), reg, bus, 0x71, 0)
	assertB524InterfaceBound(t, poller, 20, "controller_profile")
	other := registry.NewDeviceRegistry(nil)
	other.Register(registry.DeviceInfo{Address: 0x15, Manufacturer: "Vaillant", DeviceID: "BASV0", SoftwareVersion: "0217", HardwareVersion: "8803"})
	poller.reg = other
	assertB524InterfaceBound(t, poller, 50, "unmatched_policy")
	poller.reg, poller.source = reg, 0x31
	assertB524InterfaceBound(t, poller, 50, "unmatched_policy")
	enrichB524InterfaceIdentity(context.Background(), reg, bus, 0x31, 0)
	assertB524InterfaceBound(t, poller, 20, "controller_profile")
	if len(bus.requests()) != 4 {
		t.Fatal("source change did not acquire exactly two new replies")
	}
}
