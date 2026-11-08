package main

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	canbus "github.com/Project-Helianthus/helianthus-canbus"
	canbusreg "github.com/Project-Helianthus/helianthus-canbusreg"
	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimecontrol"
)

type cx20Receiver struct {
	id       canbus.InterfaceIdentity
	records  chan growattProjectRecord
	fail     chan error
	closeErr error
	closed   atomic.Int32
	release  chan struct{}
	entered  chan struct{}
}

func (r *cx20Receiver) Identity() canbus.InterfaceIdentity { return r.id }
func (r *cx20Receiver) Receive(ctx context.Context) (growattProjectRecord, error) {
	if r.release != nil {
		close(r.entered)
		<-r.release
		return cx20Record(r.id, 1, 0x1810, 1, time.Second), nil
	}
	select {
	case record := <-r.records:
		return record, nil
	case err := <-r.fail:
		return growattProjectRecord{}, err
	case <-ctx.Done():
		return growattProjectRecord{}, ctx.Err()
	}
}
func (r *cx20Receiver) Close() error { r.closed.Add(1); return r.closeErr }
func cx20Config(t *testing.T, name string, index int) ebusgateway.GrowattLINKConfig {
	t.Helper()
	id, err := canbus.NewInterfaceIdentity(name, index)
	if err != nil {
		t.Fatal(err)
	}
	return ebusgateway.GrowattLINKConfig{Enabled: true, ListenOnlyConfirmed: true, StaleAfter: time.Second, Native: canbusreg.CX01SelectionSpec{Interface: id, Flavor: "link", PCSFlavor: "pcs-standard", ProjectCode: "QAAA", Release: "0011", Direction: "module-to-coordinator", Scope: "local", Bank: "ordinary", Endpoint: "synthetic", Epoch: 1, RouteSet: true, Destination: 244}, SourceSelectors: `[{"source":1,"use_default":true,"contract":"release11"}]`}
}
func cx20Association(s canbusreg.CXSelection, e canbusreg.Evidence) canbusreg.CX16ReplayInput {
	c := s.CX01Spec()
	var in canbusreg.CX16ReplayInput
	in.Selection = s
	in.PCS.CX01LINKInput = canbusreg.CX01LINKInput{Evidence: e, Epoch: c.Epoch, Direction: c.Direction, Scope: c.Scope, Bank: c.Bank, Endpoint: c.Endpoint, Group: c.Group, Slot: c.Slot}
	in.CellCount = 8
	in.CellCountValid = true
	for n := range in.CellGates {
		in.CellGates[n] = canbusreg.CX12MeasurementGate{Configured: true, Valid: true}
	}
	return in
}
func cx20Record(id canbus.InterfaceIdentity, source uint32, kind uint32, seq uint64, mono time.Duration) growattProjectRecord {
	raw := make([]byte, 16)
	binary.NativeEndian.PutUint32(raw, 0x80000000|kind<<16|0xf400|source)
	raw[4] = 8
	for n := 0; n < 4; n++ {
		binary.LittleEndian.PutUint16(raw[8+n*2:], uint16(n+1))
	}
	return growattProjectRecord{Interface: id, Raw: raw, Sequence: seq, Monotonic: mono}
}
func cx20Driver(t *testing.T, c ebusgateway.GrowattLINKConfig, associate growattProjectAssociation, receivers ...*cx20Receiver) (*growattLINKManagedDriver, *atomic.Int32) {
	t.Helper()
	opens := new(atomic.Int32)
	f := newGrowattLINKManagedDriverFactory(c, func(name string) (growattProjectReceiver, error) {
		n := int(opens.Add(1)) - 1
		if n >= len(receivers) {
			return nil, errors.New("synthetic open exhausted")
		}
		if name != c.Native.Interface.Name() {
			return nil, errors.New("wrong interface")
		}
		return receivers[n], nil
	}, associate)
	runtime, err := f.Construct(context.Background(), runtimecontrol.Construction{})
	if err != nil {
		t.Fatal(err)
	}
	return runtime.(*growattLINKManagedDriver), opens
}
func cx20NewReceiver(c ebusgateway.GrowattLINKConfig) *cx20Receiver {
	return &cx20Receiver{id: c.Native.Interface, records: make(chan growattProjectRecord), fail: make(chan error)}
}
func cx20Start(t *testing.T, d *growattLINKManagedDriver) uint64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	g, err := d.Start(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return g
}
func cx20Stop(t *testing.T, d *growattLINKManagedDriver) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := d.Stop(ctx); err != nil {
		t.Error(err)
	}
}
func cx20Send(t *testing.T, r *cx20Receiver, record growattProjectRecord) {
	t.Helper()
	select {
	case r.records <- record:
	case <-time.After(time.Second):
		t.Fatal("synthetic receive timeout")
	}
}
func cx20Wait(t *testing.T, d *growattLINKManagedDriver, now time.Duration, condition func(growattLINKSnapshot) bool) growattLINKSnapshot {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		s := d.Snapshot(now)
		if condition(s) {
			return s
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("snapshot timeout")
	return growattLINKSnapshot{}
}
func cx20Received(count int) func(growattLINKSnapshot) bool {
	return func(s growattLINKSnapshot) bool {
		return len(s.Native.Providers) > 0 && len(s.Native.Providers[0].Raw) >= count
	}
}

func TestCX20InertAndBounds(t *testing.T) {
	c := cx20Config(t, "synthetic0", 7)
	d, opens := cx20Driver(t, c, cx20Association)
	if opens.Load() != 0 || d.Snapshot(0).State != "stopped" {
		t.Fatal("active construction")
	}
	disabled, disabledOpens := cx20Driver(t, ebusgateway.GrowattLINKConfig{}, nil)
	if _, err := disabled.Start(context.Background()); err == nil || disabledOpens.Load() != 0 || disabled.Snapshot(0).State != "disabled" {
		t.Fatal("disabled start")
	}
	for _, count := range []int{0, 1, 32, 33} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			entries := "["
			for n := 1; n <= count; n++ {
				if n > 1 {
					entries += ","
				}
				entries += fmt.Sprintf(`{"source":%d,"use_default":true,"contract":"release11"}`, n)
			}
			entries += "]"
			c.SourceSelectors = entries
			_, err := newGrowattLINKManagedDriverFactory(c, func(string) (growattProjectReceiver, error) { t.Fatal("construction opened"); return nil, nil }, cx20Association).Construct(context.Background(), runtimecontrol.Construction{})
			if (err == nil) != (count >= 1 && count <= 32) {
				t.Fatal(count, err)
			}
		})
	}
}
func TestCX20IndependentInterfacesAndHeterogeneousSources(t *testing.T) {
	for _, pair := range []struct {
		name  string
		index int
	}{{"synthetic1", 7}, {"synthetic0", 8}} {
		t.Run(fmt.Sprint(pair), func(t *testing.T) {
			a := cx20Config(t, "synthetic0", 7)
			b := cx20Config(t, pair.name, pair.index)
			a.SourceSelectors = `[{"source":1,"use_default":true,"contract":"release11"},{"source":2,"project_code":"QAAa","release":"9011","contract":"legacy19"}]`
			ra, rb := cx20NewReceiver(a), cx20NewReceiver(b)
			da, oa := cx20Driver(t, a, cx20Association, ra)
			db, ob := cx20Driver(t, b, cx20Association, rb)
			cx20Start(t, da)
			defer cx20Stop(t, da)
			cx20Start(t, db)
			defer cx20Stop(t, db)
			for n, source := range []uint32{1, 2, 3} {
				record := cx20Record(a.Native.Interface, source, 0x1802, uint64(n+1), time.Second)
				if source == 2 {
					copy(record.Raw[8:], "QAAa9011")
				} else {
					copy(record.Raw[8:], "QAAA0011")
				}
				cx20Send(t, ra, record)
			}
			cx20Send(t, rb, cx20Record(b.Native.Interface, 1, 0x1810, 1, time.Second))
			sa := cx20Wait(t, da, time.Second, func(s growattLINKSnapshot) bool { return len(s.Native.Raw) == 3 && len(s.Native.Providers[1].Raw) == 1 })
			sb := cx20Wait(t, db, time.Second, cx20Received(1))
			if oa.Load() != 1 || ob.Load() != 1 || len(sa.Native.Providers[0].Raw) != 1 || sa.Native.Providers[1].State == "conflict" || sb.Cells[0].Fields[1].Evidence.Interface != b.Native.Interface {
				t.Fatal("implicit source/interface association")
			}
			// Unknown sources retain raw evidence without acquiring a provider selector.
			if len(sa.Native.Providers) != 2 || sa.Native.Raw[2].Record.Interface != a.Native.Interface {
				t.Fatal("unknown source inferred")
			}
		})
	}
}

// Each context field is varied alone with identical payload. Removing its
// equality check would admit the wrong source-local identity or association.
func TestCX20IdentityFields(t *testing.T) {
	mutations := map[string]func(*canbusreg.CX01SelectionSpec){
		"interface-name":    func(c *canbusreg.CX01SelectionSpec) { c.Interface, _ = canbus.NewInterfaceIdentity("synthetic1", 7) },
		"interface-index":   func(c *canbusreg.CX01SelectionSpec) { c.Interface, _ = canbus.NewInterfaceIdentity("synthetic0", 8) },
		"flavor":            func(c *canbusreg.CX01SelectionSpec) { c.Flavor = "pcs-standard" },
		"pcs-flavor":        func(c *canbusreg.CX01SelectionSpec) { c.PCSFlavor = "pcs-parallel" },
		"project-code-case": func(c *canbusreg.CX01SelectionSpec) { c.ProjectCode = "QAAa" },
		"release":           func(c *canbusreg.CX01SelectionSpec) { c.Release = "0010" },
		"choice":            func(c *canbusreg.CX01SelectionSpec) { c.Choice = "legacy19" },
		"direction":         func(c *canbusreg.CX01SelectionSpec) { c.Direction = "coordinator-to-module" },
		"scope":             func(c *canbusreg.CX01SelectionSpec) { c.Scope = "aggregate" },
		"bank":              func(c *canbusreg.CX01SelectionSpec) { c.Bank = "parallel" },
		"endpoint":          func(c *canbusreg.CX01SelectionSpec) { c.Endpoint = "other" },
		"group":             func(c *canbusreg.CX01SelectionSpec) { c.Group = "other" },
		"slot":              func(c *canbusreg.CX01SelectionSpec) { c.Slot = "other" },
		"route-set":         func(c *canbusreg.CX01SelectionSpec) { c.RouteSet = false },
		"destination":       func(c *canbusreg.CX01SelectionSpec) { c.Destination = 245 },
		"source":            func(c *canbusreg.CX01SelectionSpec) { c.Source = 2 },
		"epoch":             func(c *canbusreg.CX01SelectionSpec) { c.Epoch = 2 },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			c := cx20Config(t, "synthetic0", 7)
			r := cx20NewReceiver(c)
			d, _ := cx20Driver(t, c, func(s canbusreg.CXSelection, e canbusreg.Evidence) canbusreg.CX16ReplayInput {
				in := cx20Association(s, e)
				spec := s.CX01Spec()
				mutate(&spec)
				other, err := canbusreg.NewCXSelection(spec)
				if err != nil && name != "route-set" {
					t.Error(err)
				}
				in.Selection = other
				return in
			}, r)
			cx20Start(t, d)
			defer cx20Stop(t, d)
			record := cx20Record(c.Native.Interface, 1, 0x1810, 1, time.Second)
			cx20Send(t, r, record)
			s := cx20Wait(t, d, time.Second, cx20Received(1))
			p := s.Native.Providers[0]
			if p.State != "conflict" || len(p.Current) != 0 || p.Promotable || !reflect.DeepEqual(p.Raw[0].NativeRecord, record.Raw) {
				t.Fatal("identity field admitted", name)
			}
		})
	}
}
func TestCX20AssociationHypotheses(t *testing.T) {
	mutations := map[string]func(*canbusreg.CX16ReplayInput){
		"absent-selector":      func(in *canbusreg.CX16ReplayInput) { in.Selection = canbusreg.CXSelection{} },
		"reused-address-epoch": func(in *canbusreg.CX16ReplayInput) { in.PCS.Epoch++ },
		"direction":            func(in *canbusreg.CX16ReplayInput) { in.PCS.Direction = "coordinator-to-module" },
		"scope":                func(in *canbusreg.CX16ReplayInput) { in.PCS.Scope = "aggregate" },
		"bank":                 func(in *canbusreg.CX16ReplayInput) { in.PCS.Bank = "parallel" },
		"endpoint":             func(in *canbusreg.CX16ReplayInput) { in.PCS.Endpoint = "other" },
		"group":                func(in *canbusreg.CX16ReplayInput) { in.PCS.Group = "other" },
		"slot-not-topology":    func(in *canbusreg.CX16ReplayInput) { in.PCS.Slot = "2" },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			c := cx20Config(t, "synthetic0", 7)
			r := cx20NewReceiver(c)
			d, _ := cx20Driver(t, c, func(s canbusreg.CXSelection, e canbusreg.Evidence) canbusreg.CX16ReplayInput {
				in := cx20Association(s, e)
				mutate(&in)
				return in
			}, r)
			cx20Start(t, d)
			defer cx20Stop(t, d)
			record := cx20Record(c.Native.Interface, 1, 0x1810, 1, time.Second)
			cx20Send(t, r, record)
			p := cx20Wait(t, d, time.Second, cx20Received(1)).Native.Providers[0]
			if p.Promotable || len(p.Current) != 0 || !reflect.DeepEqual(p.Raw[0].NativeRecord, record.Raw) {
				t.Fatal("context inferred")
			}
		})
	}
}
func TestCX20CellCountAndGateBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name                          string
		count                         int
		countValid, configured, valid bool
		age                           time.Duration
		accepted                      bool
	}{
		{"zero-count", 0, true, true, true, 0, false}, {"one-count", 1, true, true, true, 0, true}, {"max-count", 16, true, true, true, 0, true}, {"overflow-count", 17, true, true, true, 0, false},
		{"count-invalid", 8, false, true, true, 0, false}, {"not-configured", 8, true, false, true, 0, false}, {"invalid-channel", 8, true, true, false, 0, false},
		{"negative-age", 8, true, true, true, -1, false}, {"zero-age", 8, true, true, true, 0, true}, {"max-age", 8, true, true, true, time.Second, true}, {"stale-age", 8, true, true, true, time.Second + 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := cx20Config(t, "synthetic0", 7)
			r := cx20NewReceiver(c)
			d, _ := cx20Driver(t, c, func(s canbusreg.CXSelection, e canbusreg.Evidence) canbusreg.CX16ReplayInput {
				in := cx20Association(s, e)
				in.CellCount = tc.count
				in.CellCountValid = tc.countValid
				in.CellGates[0] = canbusreg.CX12MeasurementGate{Configured: tc.configured, Valid: tc.valid, Age: tc.age}
				return in
			}, r)
			cx20Start(t, d)
			defer cx20Stop(t, d)
			record := cx20Record(c.Native.Interface, 1, 0x1810, 1, time.Second)
			cx20Send(t, r, record)
			got := cx20Wait(t, d, time.Second, cx20Received(1))
			p := got.Native.Providers[0]
			field := p.Current["cells/cell_1"]
			if (field.Status == "accepted") != tc.accepted || p.PhysicalQualified || field.PhysicalQualified || !reflect.DeepEqual(p.Raw[0].NativeRecord, record.Raw) {
				t.Fatal(field)
			}
			if tc.countValid && tc.count == 8 && p.Current["cells/cell_2"].Status != "accepted" {
				t.Fatal("independent sibling erased")
			}
		})
	}
}
func TestCX20PartialStaleAndLastGood(t *testing.T) {
	c := cx20Config(t, "synthetic0", 7)
	c.SourceSelectors = `[{"source":1,"use_default":true,"contract":"release11"},{"source":2,"use_default":true,"contract":"release11"}]`
	r := cx20NewReceiver(c)
	d, _ := cx20Driver(t, c, func(s canbusreg.CXSelection, e canbusreg.Evidence) canbusreg.CX16ReplayInput {
		in := cx20Association(s, e)
		if e.Sequence == 2 {
			in.CellGates[0].Age = time.Second / 2
		}
		if e.Sequence == 4 {
			in.CellGates[0].Valid = false
		}
		return in
	}, r)
	cx20Start(t, d)
	defer cx20Stop(t, d)
	cx20Send(t, r, cx20Record(c.Native.Interface, 1, 0x1810, 1, time.Second))
	got := cx20Wait(t, d, time.Second, cx20Received(1))
	if got.Cells[0].CompleteCoverage || len(got.Cells[0].ReceivedIndices) != 4 || got.Cells[0].ConfiguredCount != 8 {
		t.Fatal(got.Cells)
	}
	cx20Send(t, r, cx20Record(c.Native.Interface, 1, 0x1811, 2, 2*time.Second))
	got = cx20Wait(t, d, 2*time.Second, cx20Received(2))
	if !got.Cells[0].CompleteCoverage || got.Cells[0].Span != time.Second || got.Cells[0].ReceivedSpan != time.Second {
		t.Fatal(got.Cells)
	}
	cx20Send(t, r, cx20Record(c.Native.Interface, 2, 0x1810, 3, 3*time.Second))
	got = cx20Wait(t, d, 3*time.Second, func(s growattLINKSnapshot) bool { return len(s.Native.Providers[1].Raw) == 1 })
	if got.Cells[0].Fields[1].Status != "stale" || got.Cells[0].Fields[5].Status != "stale" || got.Cells[1].Fields[1].Status != "accepted" {
		t.Fatal("cross-source/channel refresh")
	}
	cx20Send(t, r, cx20Record(c.Native.Interface, 1, 0x1810, 4, 3*time.Second))
	got = cx20Wait(t, d, 3*time.Second, cx20Received(3))
	p := got.Native.Providers[0]
	if p.Current["cells/cell_1"].Status != "invalid" || p.LastGood["cells/cell_1"].Evidence.Sequence != 1 || p.Current["cells/cell_2"].Evidence.Sequence != 4 {
		t.Fatal("partial invalid erased history/sibling")
	}
	field := p.Current["cells/cell_2"]
	if field.Epoch != 1 || field.Endpoint != "synthetic" || field.Raw != field.Evidence.Frame.Bytes() || field.Grade == "" || field.PhysicalQualified {
		t.Fatal("evidence lost")
	}
	got.Native.Raw[0].Record.Raw[8] = 99
	delete(got.Native.Providers[0].Current, "cells/cell_2")
	fresh := d.Snapshot(3 * time.Second)
	if fresh.Native.Raw[0].Record.Raw[8] == 99 || fresh.Native.Providers[0].Current["cells/cell_2"].Evidence.Sequence != 4 {
		t.Fatal("snapshot alias")
	}
}
func TestCX20NoArrivalAssociationOrMissingDiscovery(t *testing.T) {
	c := cx20Config(t, "synthetic0", 7)
	r := cx20NewReceiver(c)
	d, opens := cx20Driver(t, c, cx20Association, r)
	cx20Start(t, d)
	defer cx20Stop(t, d)
	if p := d.Snapshot(100 * time.Second).Native.Providers[0]; p.State != "awaiting" || p.Promotable || len(p.Current) != 0 {
		t.Fatal("missing traffic created discovery")
	}
	record := cx20Record(c.Native.Interface, 1, 0x1810, 1, time.Second)
	binary.NativeEndian.PutUint32(record.Raw, 0x313)
	cx20Send(t, r, record)
	s := cx20Wait(t, d, time.Second, func(s growattLINKSnapshot) bool { return len(s.Native.Raw) == 1 })
	if len(s.Native.Providers[0].Raw) != 0 || len(s.Native.Providers[0].Current) != 0 || opens.Load() != 1 {
		t.Fatal("PCS arrival selected LINK")
	}
}
func TestCX20MixedIdentityAndEpochRestart(t *testing.T) {
	c := cx20Config(t, "synthetic0", 7)
	r, next := cx20NewReceiver(c), cx20NewReceiver(c)
	d, opens := cx20Driver(t, c, cx20Association, r, next)
	cx20Start(t, d)
	for n, payload := range []string{"QAAA0011", "QAAa0011", "QAAA0011"} {
		record := cx20Record(c.Native.Interface, 1, 0x1802, uint64(n+1), time.Second)
		copy(record.Raw[8:], payload)
		cx20Send(t, r, record)
	}
	got := cx20Wait(t, d, time.Second, cx20Received(3))
	if got.Native.Providers[0].State != "conflict" || got.Native.Providers[0].Promotable {
		t.Fatal("mixed identity escaped fence")
	}
	cx20Stop(t, d)
	if cx20Start(t, d) != 2 || opens.Load() != 2 {
		t.Fatal("restart generation")
	}
	defer cx20Stop(t, d)
	cx20Send(t, next, cx20Record(c.Native.Interface, 1, 0x1810, 1, time.Second))
	got = cx20Wait(t, d, time.Second, func(s growattLINKSnapshot) bool {
		return s.Native.Providers[0].Current["cells/cell_1"].Evidence.Sequence == 1
	})
	if got.Native.Providers[0].State == "conflict" || got.Native.Providers[0].Generation != 2 {
		t.Fatal("generation not independent")
	}
}
func TestCX20ReadFailureAndQuarantine(t *testing.T) {
	c := cx20Config(t, "synthetic0", 7)
	r := cx20NewReceiver(c)
	r.closeErr = errors.New("synthetic close failure")
	d, opens := cx20Driver(t, c, cx20Association, r)
	cx20Start(t, d)
	cx20Send(t, r, cx20Record(c.Native.Interface, 1, 0x1810, 1, time.Second))
	cx20Wait(t, d, time.Second, cx20Received(1))
	r.fail <- errors.New("synthetic read failure")
	got := cx20Wait(t, d, time.Second, func(s growattLINKSnapshot) bool { return s.State == "degraded" })
	if got.Native.Providers[0].Promotable || len(got.Native.Providers[0].Current) != 0 || len(got.Native.Providers[0].LastGood) == 0 {
		t.Fatal("read failure admission")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if d.Stop(ctx) == nil || !d.SafetyQuarantined() || d.Snapshot(0).State != "quarantined" {
		t.Fatal("close failure not quarantined")
	}
	if _, err := d.Start(ctx); err == nil || opens.Load() != 1 {
		t.Fatal("quarantine reopened")
	}
}
func TestCX20RetiredCallbackAndStopDeadline(t *testing.T) {
	c := cx20Config(t, "synthetic0", 7)
	r := cx20NewReceiver(c)
	r.release = make(chan struct{})
	r.entered = make(chan struct{})
	next := cx20NewReceiver(c)
	d, _ := cx20Driver(t, c, cx20Association, r, next)
	cx20Start(t, d)
	<-r.entered
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if !errors.Is(d.Stop(ctx), context.DeadlineExceeded) {
		t.Fatal("unjoined receive reported stopped")
	}
	if d.Snapshot(0).State != "transitioning" || r.closed.Load() != 0 {
		t.Fatal("closed before receive joined")
	}
	close(r.release)
	cx20Stop(t, d)
	if len(d.Snapshot(time.Second).Native.Raw) != 0 || r.closed.Load() != 1 {
		t.Fatal("retired callback admitted")
	}
	cx20Start(t, d)
	defer cx20Stop(t, d)
	if len(d.Snapshot(time.Second).Native.Providers[0].Current) != 0 {
		t.Fatal("retired values current")
	}
}
func TestCX20ReceiveOnly(t *testing.T) {
	typ := reflect.TypeOf((*growattProjectReceiver)(nil)).Elem()
	if typ.NumMethod() != 3 {
		t.Fatal(typ)
	}
	for _, name := range []string{"Identity", "Receive", "Close"} {
		if _, ok := typ.MethodByName(name); !ok {
			t.Fatal(name)
		}
	}
}

// The payload is identical in every negative vector; only transport admission
// or explicit source/destination identity changes.
func TestCX20NativeAdmission(t *testing.T) {
	for _, name := range []string{"standard-id", "wrong-destination", "unknown-source", "dlc7", "raw-dlc9", "fd", "wrong-interface-name", "wrong-interface-index", "zero-sequence", "negative-time"} {
		t.Run(name, func(t *testing.T) {
			c := cx20Config(t, "synthetic0", 7)
			r := cx20NewReceiver(c)
			d, _ := cx20Driver(t, c, cx20Association, r)
			cx20Start(t, d)
			defer cx20Stop(t, d)
			record := cx20Record(c.Native.Interface, 1, 0x1810, 1, time.Second)
			switch name {
			case "standard-id":
				binary.NativeEndian.PutUint32(record.Raw, 0x313)
			case "wrong-destination":
				binary.NativeEndian.PutUint32(record.Raw, 0x9810f301)
			case "unknown-source":
				binary.NativeEndian.PutUint32(record.Raw, 0x9810f402)
			case "dlc7":
				record.Raw[4] = 7
			case "raw-dlc9":
				record.Raw[7] = 9
			case "fd":
				record.Raw = append(record.Raw, make([]byte, 56)...)
			case "wrong-interface-name":
				record.Interface, _ = canbus.NewInterfaceIdentity("synthetic1", 7)
			case "wrong-interface-index":
				record.Interface, _ = canbus.NewInterfaceIdentity("synthetic0", 8)
			case "zero-sequence":
				record.Sequence = 0
			case "negative-time":
				record.Monotonic = -1
			}
			cx20Send(t, r, record)
			got := cx20Wait(t, d, time.Second, func(s growattLINKSnapshot) bool { return len(s.Native.Raw) == 1 })
			if name == "wrong-destination" {
				got = cx20Wait(t, d, time.Second, cx20Received(1))
			}
			if got.Native.Providers[0].Promotable || len(got.Native.Providers[0].Current) != 0 || !reflect.DeepEqual(got.Native.Raw[0].Record.Raw, record.Raw) {
				t.Fatal("transport inferred interpretation")
			}
		})
	}
}
func TestCX20CancelledStartIsInertAfterOpen(t *testing.T) {
	c := cx20Config(t, "synthetic0", 7)
	r := cx20NewReceiver(c)
	entered, release := make(chan struct{}), make(chan struct{})
	factory := newGrowattLINKManagedDriverFactory(c, func(string) (growattProjectReceiver, error) { close(entered); <-release; return r, nil }, cx20Association)
	runtime, err := factory.Construct(context.Background(), runtimecontrol.Construction{})
	if err != nil {
		t.Fatal(err)
	}
	d := runtime.(*growattLINKManagedDriver)
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { _, err := d.Start(ctx); result <- err }()
	<-entered
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("start ignored cancellation")
	}
	if d.Snapshot(0).State != "transitioning" {
		t.Fatal("pending open admitted")
	}
	close(release)
	deadline := time.Now().Add(time.Second)
	for (r.closed.Load() == 0 || d.Snapshot(time.Second).State == "transitioning") && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if r.closed.Load() != 1 {
		t.Fatal("cancelled open not closed")
	}
	if got := d.Snapshot(time.Second); got.Native.Providers[0].Promotable || len(got.Native.Raw) != 0 {
		t.Fatal("cancelled start published")
	}
}
