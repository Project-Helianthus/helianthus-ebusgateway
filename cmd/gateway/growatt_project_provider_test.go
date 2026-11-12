package main

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	canbus "github.com/Project-Helianthus/helianthus-canbus"
	canbusreg "github.com/Project-Helianthus/helianthus-canbusreg"
	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
)

type cx18Receiver struct {
	id         canbus.InterfaceIdentity
	records    chan growattProjectRecord
	canceled   chan struct{}
	once       sync.Once
	closeError error
	closed     bool
}

func (r *cx18Receiver) Identity() canbus.InterfaceIdentity { return r.id }
func (r *cx18Receiver) Receive(ctx context.Context) (growattProjectRecord, error) {
	select {
	case record := <-r.records:
		return record, nil
	case <-ctx.Done():
		r.once.Do(func() { close(r.canceled) })
		return growattProjectRecord{}, ctx.Err()
	}
}
func (r *cx18Receiver) Close() error {
	select {
	case <-r.canceled:
	default:
		return errors.New("close before cancel/join")
	}
	r.closed = true
	return r.closeError
}
func cx18Selection(t *testing.T, link bool) canbusreg.CXSelection {
	t.Helper()
	id, _ := canbus.NewInterfaceIdentity("synthetic0", 7)
	c := canbusreg.SelectionSpec{Interface: id, Flavor: "pcs-standard", ProjectCode: "QAAA", Release: "0011", Direction: "bms-to-pcs", Scope: "local", Bank: "ordinary", Endpoint: "synthetic", Epoch: 1}
	if link {
		c.Flavor = "link"
		c.PCSFlavor = "pcs-standard"
		c.Direction = "module-to-coordinator"
		c.RouteSet = true
		c.Source = 1
		c.Destination = 244
	}
	s, err := canbusreg.NewCXSelection(c)
	if err != nil {
		t.Fatal(err)
	}
	return s
}
func cx18Associate(s canbusreg.CXSelection, e canbusreg.Evidence) canbusreg.ReplayInput {
	c := s.SelectionSpecValue()
	var in canbusreg.ReplayInput
	in.Selection = s
	in.PCS.LINKInput = canbusreg.LINKInput{Evidence: e, Epoch: c.Epoch, Direction: c.Direction, Scope: c.Scope, Bank: c.Bank, Endpoint: c.Endpoint, Group: c.Group, Slot: c.Slot}
	in.PCS.PublisherResolved = true
	in.PCS.MeasurementValid = true
	return in
}
func cx18Record(s canbusreg.CXSelection, seq uint64) growattProjectRecord {
	raw := make([]byte, 16)
	binary.NativeEndian.PutUint32(raw, 0x313)
	raw[4] = 8
	binary.LittleEndian.PutUint16(raw[8:], 5310)
	return growattProjectRecord{Interface: s.SelectionSpecValue().Interface, Raw: raw, Sequence: seq, Monotonic: time.Second}
}
func cx18New(t *testing.T, s canbusreg.CXSelection, associate growattProjectAssociation) (*growattProjectProvider, *cx18Receiver, *int) {
	t.Helper()
	r := &cx18Receiver{id: s.SelectionSpecValue().Interface, records: make(chan growattProjectRecord), canceled: make(chan struct{})}
	opens := new(int)
	p, err := newGrowattProjectProvider([]canbusreg.CXSelection{s}, time.Second, true, func(name string) (growattProjectReceiver, error) {
		*opens++
		if name != r.id.Name() {
			t.Fatal(name)
		}
		return r, nil
	}, associate)
	if err != nil {
		t.Fatal(err)
	}
	return p, r, opens
}
func cx18Wait(t *testing.T, p *growattProjectProvider, condition func(growattProjectSnapshot) bool) growattProjectSnapshot {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		s := p.Snapshot(time.Second)
		if condition(s) {
			return s
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("synthetic receive timed out")
	return growattProjectSnapshot{}
}
func cx18Send(t *testing.T, r *cx18Receiver, record growattProjectRecord) {
	t.Helper()
	select {
	case r.records <- record:
	case <-time.After(time.Second):
		t.Fatal("receive blocked")
	}
}
func TestCX18Lifecycle(t *testing.T) {
	s := cx18Selection(t, false)
	p, r, opens := cx18New(t, s, cx18Associate)
	if *opens != 0 || p.Snapshot(0).Providers[0].State != "stopped" {
		t.Fatal("active construction")
	}
	if p.Start(0) == nil {
		t.Fatal("zero generation")
	}
	if err := p.Start(1); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Stop() }()
	if *opens != 1 || p.Start(1) == nil {
		t.Fatal("open or generation")
	}
	cx18Send(t, r, cx18Record(s, 1))
	got := cx18Wait(t, p, func(s growattProjectSnapshot) bool { return len(s.Providers[0].Current) > 0 })
	if got.Providers[0].PhysicalQualified || !got.Providers[0].Promotable {
		t.Fatal(got)
	}
	got.Raw[0].Record.Raw[8] = 0
	got.Providers[0].Raw[0].NativeRecord[8] = 0
	delete(got.Providers[0].Current, "core/voltage")
	fresh := p.Snapshot(time.Second)
	if fresh.Raw[0].Record.Raw[8] == 0 || fresh.Providers[0].Raw[0].NativeRecord[8] == 0 || len(fresh.Providers[0].Current) == 0 {
		t.Fatal("aliased snapshot")
	}
	if err := p.Stop(); err != nil || !r.closed {
		t.Fatal(err)
	}
	if p.Snapshot(time.Second).Providers[0].Promotable {
		t.Fatal("stopped promotion")
	}
}
func TestCX18ContextHypotheses(t *testing.T) {
	// Every association component is varied independently with identical bytes.
	mutations := map[string]func(*canbusreg.ReplayInput){
		"selector":  func(i *canbusreg.ReplayInput) { i.Selection = canbusreg.CXSelection{} },
		"epoch":     func(i *canbusreg.ReplayInput) { i.PCS.Epoch++ },
		"direction": func(i *canbusreg.ReplayInput) { i.PCS.Direction = "pcs-to-bms" },
		"scope":     func(i *canbusreg.ReplayInput) { i.PCS.Scope = "aggregate" },
		"bank":      func(i *canbusreg.ReplayInput) { i.PCS.Bank = "parallel" },
		"endpoint":  func(i *canbusreg.ReplayInput) { i.PCS.Endpoint = "other" },
		"group":     func(i *canbusreg.ReplayInput) { i.PCS.Group = "other" },
		"slot":      func(i *canbusreg.ReplayInput) { i.PCS.Slot = "other" },
		"publisher": func(i *canbusreg.ReplayInput) { i.PCS.PublisherResolved = false },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			s := cx18Selection(t, false)
			p, r, _ := cx18New(t, s, func(s canbusreg.CXSelection, e canbusreg.Evidence) canbusreg.ReplayInput {
				i := cx18Associate(s, e)
				mutate(&i)
				return i
			})
			if err := p.Start(1); err != nil {
				t.Fatal(err)
			}
			defer func() { _ = p.Stop() }()
			record := cx18Record(s, 1)
			cx18Send(t, r, record)
			got := cx18Wait(t, p, func(s growattProjectSnapshot) bool { return len(s.Providers[0].Raw) > 0 })
			if got.Providers[0].Promotable || len(got.Providers[0].Current) > 0 || !reflect.DeepEqual(got.Providers[0].Raw[0].NativeRecord, record.Raw) {
				t.Fatal(got)
			}
		})
	}
}
func TestCX18RawAdmission(t *testing.T) {
	for _, name := range []string{"short", "fd", "rtr", "error", "dlc7", "raw-dlc9", "interface-name", "interface-index", "zero-sequence", "negative-time"} {
		t.Run(name, func(t *testing.T) {
			s := cx18Selection(t, false)
			p, r, _ := cx18New(t, s, cx18Associate)
			if err := p.Start(1); err != nil {
				t.Fatal(err)
			}
			defer func() { _ = p.Stop() }()
			record := cx18Record(s, 1)
			switch name {
			case "short":
				record.Raw = record.Raw[:15]
			case "fd":
				record.Raw = make([]byte, 72)
			case "rtr":
				binary.NativeEndian.PutUint32(record.Raw, 0x40000311)
			case "error":
				binary.NativeEndian.PutUint32(record.Raw, 0x20000311)
			case "dlc7":
				record.Raw[4] = 7
			case "raw-dlc9":
				record.Raw[7] = 9
			case "interface-name":
				record.Interface, _ = canbus.NewInterfaceIdentity("synthetic1", 7)
			case "interface-index":
				record.Interface, _ = canbus.NewInterfaceIdentity("synthetic0", 8)
			case "zero-sequence":
				record.Sequence = 0
			case "negative-time":
				record.Monotonic = -1
			}
			cx18Send(t, r, record)
			got := cx18Wait(t, p, func(s growattProjectSnapshot) bool { return len(s.Raw) > 0 })
			if got.Providers[0].Promotable || !reflect.DeepEqual(got.Raw[0].Record.Raw, record.Raw) {
				t.Fatal(got)
			}
		})
	}
}
func TestCX18Quarantine(t *testing.T) {
	s := cx18Selection(t, false)
	p, r, opens := cx18New(t, s, cx18Associate)
	r.closeError = errors.New("synthetic close error")
	if err := p.Start(1); err != nil {
		t.Fatal(err)
	}
	if p.Stop() == nil || !p.Snapshot(0).Quarantined || p.Start(2) == nil || *opens != 1 {
		t.Fatal("ownership escaped quarantine")
	}
}
func TestCX18WrongIdentity(t *testing.T) {
	for _, field := range []string{"name", "index"} {
		t.Run(field, func(t *testing.T) {
			s := cx18Selection(t, false)
			p, r, _ := cx18New(t, s, cx18Associate)
			if field == "name" {
				r.id, _ = canbus.NewInterfaceIdentity("synthetic1", 7)
			} else {
				r.id, _ = canbus.NewInterfaceIdentity("synthetic0", 8)
			}
			// An unopened receive loop has no join; allow the fake close for this case.
			close(r.canceled)
			p.open = func(string) (growattProjectReceiver, error) { return r, nil }
			if p.Start(1) == nil || !r.closed {
				t.Fatal("wrong identity admitted")
			}
		})
	}
}
func TestCX18BoundedDiagnostics(t *testing.T) {
	s := cx18Selection(t, true)
	p, r, _ := cx18New(t, s, cx18Associate)
	if err := p.Start(1); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Stop() }()
	for n := 1; n <= 258; n++ {
		record := cx18Record(s, uint64(n))
		record.Raw = make([]byte, 73)
		cx18Send(t, r, record)
	}
	got := cx18Wait(t, p, func(s growattProjectSnapshot) bool { return s.Evicted == 2 })
	if len(got.Raw) != 256 || len(got.Raw[0].Record.Raw) != 72 || got.Providers[0].Promotable {
		t.Fatal(fmt.Sprint(got.Evicted))
	}
}
func TestCX18ReceiveOnlyType(t *testing.T) {
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

func TestCX18FieldAgesAndPartialRetention(t *testing.T) {
	s := cx18Selection(t, false)
	for _, age := range []time.Duration{-1, 0, time.Second, time.Second + 1} {
		t.Run(fmt.Sprint(age), func(t *testing.T) {
			p, r, _ := cx18New(t, s, func(s canbusreg.CXSelection, e canbusreg.Evidence) canbusreg.ReplayInput {
				i := cx18Associate(s, e)
				i.PCS.FieldAges = map[string]time.Duration{"voltage": age}
				return i
			})
			if err := p.Start(1); err != nil {
				t.Fatal(err)
			}
			defer func() { _ = p.Stop() }()
			cx18Send(t, r, cx18Record(s, 1))
			got := cx18Wait(t, p, func(s growattProjectSnapshot) bool { return len(s.Providers[0].Current) > 0 }).Providers[0]
			f := got.Current["core/voltage"]
			want := "accepted"
			if age < 0 {
				want = "invalid"
			} else if age > time.Second {
				want = "stale"
			}
			if f.Status != want || got.Current["core/current"].Status != "accepted" {
				t.Fatal(f, got.Current["core/current"])
			}
			if f.Epoch != 1 || f.Evidence.Sequence != 1 || f.Raw != f.Evidence.Frame.Bytes() || f.PhysicalQualified {
				t.Fatal("lost evidence")
			}
		})
	}
	p, r, _ := cx18New(t, s, func(s canbusreg.CXSelection, e canbusreg.Evidence) canbusreg.ReplayInput {
		i := cx18Associate(s, e)
		if e.Sequence > 1 {
			i.PCS.FieldStates = map[string]string{"voltage": "invalid"}
		}
		return i
	})
	if err := p.Start(1); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Stop() }()
	cx18Send(t, r, cx18Record(s, 1))
	cx18Wait(t, p, func(s growattProjectSnapshot) bool { return len(s.Providers[0].LastGood) > 0 })
	cx18Send(t, r, cx18Record(s, 2))
	got := cx18Wait(t, p, func(s growattProjectSnapshot) bool { return len(s.Providers[0].Raw) == 2 }).Providers[0]
	if got.LastGood["core/voltage"].Evidence.Sequence != 1 || got.Current["core/voltage"].Status != "invalid" || got.Current["core/current"].Evidence.Sequence != 2 {
		t.Fatal("partial field erased sibling/history")
	}
}
func TestCX18TemperatureHypothesisCountAndValidity(t *testing.T) {
	for _, tc := range []struct {
		name               string
		count              uint16
		valid, temperature bool
		accepted           bool
	}{{"zero", 0, true, true, false}, {"one", 1, true, true, true}, {"count-invalid", 1, false, true, false}, {"temperature-invalid", 1, true, false, false}} {
		t.Run(tc.name, func(t *testing.T) {
			s := cx18Selection(t, false)
			p, r, _ := cx18New(t, s, func(s canbusreg.CXSelection, e canbusreg.Evidence) canbusreg.ReplayInput {
				i := cx18Associate(s, e)
				i.PCS.Count = tc.count
				i.PCS.CountValid = tc.valid
				i.PCS.TemperatureValid = tc.temperature
				return i
			})
			if err := p.Start(1); err != nil {
				t.Fatal(err)
			}
			defer func() { _ = p.Stop() }()
			record := cx18Record(s, 1)
			cx18Send(t, r, record)
			got := cx18Wait(t, p, func(s growattProjectSnapshot) bool { return len(s.Providers[0].Current) > 0 }).Providers[0]
			f := got.Current["core/temperature"]
			if (f.Status == "accepted") != tc.accepted || f.PhysicalQualified || !reflect.DeepEqual(got.Raw[0].NativeRecord, record.Raw) {
				t.Fatal(f)
			}
		})
	}
}
func TestCX18LINKSourceAndDestinationHypotheses(t *testing.T) {
	s := cx18Selection(t, true)
	c := s.SelectionSpecValue()
	c.Source = 2
	c.ProjectCode = "QAAa"
	c.Release = "9011"
	c.Choice = "legacy19"
	second, err := canbusreg.NewCXSelection(c)
	if err != nil {
		t.Fatal(err)
	}
	r := &cx18Receiver{id: c.Interface, records: make(chan growattProjectRecord), canceled: make(chan struct{})}
	opens := 0
	p, err := newGrowattProjectProvider([]canbusreg.CXSelection{s, second}, time.Second, true, func(string) (growattProjectReceiver, error) { opens++; return r, nil }, cx18Associate)
	if err != nil {
		t.Fatal(err)
	}
	if err = p.Start(1); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Stop() }()
	// Native identity only confirms the explicitly selected code and release.
	for n, source := range []uint32{1, 2, 3} {
		record := cx18Record(s, uint64(n+1))
		binary.NativeEndian.PutUint32(record.Raw, 0x80000000|0x1802f400|source)
		copy(record.Raw[8:], "QAAA0011")
		cx18Send(t, r, record)
	}
	got := cx18Wait(t, p, func(s growattProjectSnapshot) bool { return len(s.Raw) == 3 && len(s.Providers[1].Raw) == 1 })
	if opens != 1 || len(got.Providers[0].Raw) != 1 || got.Providers[1].State != "conflict" || got.Providers[1].Promotable {
		t.Fatal("source selector changed by identity")
	}
	// Same payload on a wrong destination remains raw and cannot supply fields.
	record := cx18Record(s, 4)
	binary.NativeEndian.PutUint32(record.Raw, 0x80000000|0x1802f301)
	copy(record.Raw[8:], "QAAA0011")
	cx18Send(t, r, record)
	got = cx18Wait(t, p, func(s growattProjectSnapshot) bool { return len(s.Providers[0].Raw) == 2 })
	if got.Providers[0].Raw[1].Status != "invalid" || !reflect.DeepEqual(got.Providers[0].Raw[1].NativeRecord, record.Raw) {
		t.Fatal("wrong route interpreted")
	}
}

func TestCX18SelectionIdentityFields(t *testing.T) {
	mutations := map[string]func(*canbusreg.SelectionSpec){
		"interface-name":  func(c *canbusreg.SelectionSpec) { c.Interface, _ = canbus.NewInterfaceIdentity("synthetic1", 7) },
		"interface-index": func(c *canbusreg.SelectionSpec) { c.Interface, _ = canbus.NewInterfaceIdentity("synthetic0", 8) },
		"flavor":          func(c *canbusreg.SelectionSpec) { c.Flavor = "pcs-parallel" },
		"pcs-flavor":      func(c *canbusreg.SelectionSpec) { c.PCSFlavor = "pcs-parallel" },
		"project-code":    func(c *canbusreg.SelectionSpec) { c.ProjectCode = "QAAa" },
		"release":         func(c *canbusreg.SelectionSpec) { c.Release = "0010" },
		"contract-choice": func(c *canbusreg.SelectionSpec) { c.Choice = "legacy19" },
		"direction":       func(c *canbusreg.SelectionSpec) { c.Direction = "pcs-to-bms" },
		"scope":           func(c *canbusreg.SelectionSpec) { c.Scope = "aggregate" },
		"bank":            func(c *canbusreg.SelectionSpec) { c.Bank = "parallel" },
		"endpoint":        func(c *canbusreg.SelectionSpec) { c.Endpoint = "other" },
		"group":           func(c *canbusreg.SelectionSpec) { c.Group = "1" },
		"slot":            func(c *canbusreg.SelectionSpec) { c.Slot = "1" },
		"route-set":       func(c *canbusreg.SelectionSpec) { c.RouteSet = true },
		"destination":     func(c *canbusreg.SelectionSpec) { c.Destination = 245 },
		"source":          func(c *canbusreg.SelectionSpec) { c.Source = 2 },
		"epoch":           func(c *canbusreg.SelectionSpec) { c.Epoch = 2 },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			s := cx18Selection(t, false)
			c := s.SelectionSpecValue()
			mutate(&c)
			different, err := canbusreg.NewCXSelection(c)
			if err != nil {
				t.Fatal(err)
			}
			if different == s {
				t.Fatal("identity not varied")
			}
			p, r, _ := cx18New(t, s, func(s canbusreg.CXSelection, e canbusreg.Evidence) canbusreg.ReplayInput {
				i := cx18Associate(s, e)
				i.Selection = different
				return i
			})
			if err := p.Start(1); err != nil {
				t.Fatal(err)
			}
			defer func() { _ = p.Stop() }()
			record := cx18Record(s, 1)
			cx18Send(t, r, record)
			got := cx18Wait(t, p, func(s growattProjectSnapshot) bool { return len(s.Providers[0].Raw) > 0 }).Providers[0]
			if got.Promotable || got.State != "conflict" || !reflect.DeepEqual(got.Raw[0].NativeRecord, record.Raw) {
				t.Fatal("selection mismatch admitted")
			}
		})
	}
}
func TestCX18ConstructorPreconditions(t *testing.T) {
	s := cx18Selection(t, false)
	if _, err := newGrowattProjectProvider([]canbusreg.CXSelection{s}, time.Second, false, openGrowattProjectSocket, cx18Associate); err == nil {
		t.Fatal("missing external confirmation")
	}
	if _, err := newGrowattProjectProvider([]canbusreg.CXSelection{s, s}, time.Second, true, openGrowattProjectSocket, cx18Associate); err == nil {
		t.Fatal("duplicate ownership")
	}
}

func TestCX18ConfigAdaptersAreInert(t *testing.T) {
	calls := 0
	open := func(string) (growattProjectReceiver, error) { calls++; return nil, errors.New("synthetic open") }
	if p, err := newGrowattPCSProjectProvider(ebusgateway.GrowattPCSConfig{}, open, cx18Associate); err != nil || p != nil {
		t.Fatal(p, err)
	}
	if p, err := newGrowattLINKProjectProvider(ebusgateway.GrowattLINKConfig{}, open, cx18Associate); err != nil || p != nil {
		t.Fatal(p, err)
	}
	pcs := ebusgateway.GrowattPCSConfig{Enabled: true, ListenOnlyConfirmed: true, StaleAfter: time.Second, Native: cx18Selection(t, false).SelectionSpecValue()}
	if p, err := newGrowattPCSProjectProvider(pcs, open, cx18Associate); err != nil || p == nil {
		t.Fatal(p, err)
	}
	linkSpec := cx18Selection(t, true).SelectionSpecValue()
	linkSpec.Source = 0
	link := ebusgateway.GrowattLINKConfig{Enabled: true, ListenOnlyConfirmed: true, StaleAfter: time.Second, Native: linkSpec, SourceSelectors: `[{"source":1,"use_default":true,"contract":"release11"}]`}
	if p, err := newGrowattLINKProjectProvider(link, open, cx18Associate); err != nil || p == nil {
		t.Fatal(p, err)
	}
	pcs.ListenOnlyConfirmed = false
	if _, err := newGrowattPCSProjectProvider(pcs, open, cx18Associate); err == nil {
		t.Fatal("missing confirmation")
	}
	link.ListenOnlyConfirmed = false
	if _, err := newGrowattLINKProjectProvider(link, open, cx18Associate); err == nil {
		t.Fatal("missing confirmation")
	}
	if calls != 0 {
		t.Fatal("constructor opened transport")
	}
}
