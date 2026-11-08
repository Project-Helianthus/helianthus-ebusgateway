package main

import (
	"context"
	"encoding/binary"
	"errors"
	"reflect"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	canbus "github.com/Project-Helianthus/helianthus-canbus"
	canbusreg "github.com/Project-Helianthus/helianthus-canbusreg"
	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/drivermanager"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimecontrol"
)

type cx19Receiver struct {
	id       canbus.InterfaceIdentity
	records  chan growattProjectRecord
	fail     chan struct{}
	closes   atomic.Int32
	reads    atomic.Int32
	closeErr error
	late     *growattProjectRecord
	release  chan struct{}
}

func (r *cx19Receiver) Identity() canbus.InterfaceIdentity { return r.id }
func (r *cx19Receiver) Receive(ctx context.Context) (growattProjectRecord, error) {
	r.reads.Add(1)
	select {
	case record := <-r.records:
		return record, nil
	case <-r.fail:
		return growattProjectRecord{}, errors.New("synthetic read failure")
	case <-ctx.Done():
		if r.release != nil {
			<-r.release
		}
		if r.late != nil {
			return *r.late, nil
		}
		return growattProjectRecord{}, ctx.Err()
	}
}
func (r *cx19Receiver) Close() error { r.closes.Add(1); return r.closeErr }
func cx19Config(t *testing.T, flavor string) ebusgateway.GrowattPCSConfig {
	t.Helper()
	id, err := canbus.NewInterfaceIdentity("synthetic0", 7)
	if err != nil {
		t.Fatal(err)
	}
	c := ebusgateway.GrowattPCSConfig{Enabled: true, ListenOnlyConfirmed: true, StaleAfter: time.Second, Native: canbusreg.CX01SelectionSpec{Interface: id, Flavor: flavor, ProjectCode: "QAAA", Release: "0011", Direction: "bms-to-pcs", Scope: "local", Bank: "ordinary", Endpoint: "synthetic", Epoch: 1}}
	if flavor == "pcs-parallel" {
		c.Native.Bank = "parallel"
		c.Native.Group = "0"
		c.Native.Slot = "0"
	}
	if flavor == "pcs-extended19" {
		c.Native.Release = "0010"
		c.Native.RouteSet = true
		c.Native.Source = 1
		c.Native.Destination = 0xf2
	}
	return c
}
func cx19Association(s canbusreg.CXSelection, e canbusreg.Evidence) canbusreg.CX16ReplayInput {
	c := s.CX01Spec()
	var in canbusreg.CX16ReplayInput
	in.Selection = s
	in.PCS.CX01LINKInput = canbusreg.CX01LINKInput{Evidence: e, Epoch: c.Epoch, Direction: c.Direction, Scope: c.Scope, Bank: c.Bank, Endpoint: c.Endpoint, Group: c.Group, Slot: c.Slot}
	in.PCS.PublisherResolved = true
	in.PCS.MeasurementValid = true
	return in
}
func cx19Record(c ebusgateway.GrowattPCSConfig, seq uint64) growattProjectRecord {
	raw := make([]byte, 16)
	id := uint32(0x313)
	if c.Native.Flavor == "pcs-parallel" {
		id = 0x610
	}
	if c.Native.Flavor == "pcs-extended19" {
		id = 0x80000000 | 0x1900f201
	}
	binary.NativeEndian.PutUint32(raw, id)
	raw[4] = 8
	binary.LittleEndian.PutUint16(raw[8:], 5310)
	return growattProjectRecord{Interface: c.Native.Interface, Raw: raw, Sequence: seq, Monotonic: time.Second}
}
func cx19Fake(c ebusgateway.GrowattPCSConfig) *cx19Receiver {
	return &cx19Receiver{id: c.Native.Interface, records: make(chan growattProjectRecord), fail: make(chan struct{})}
}
func cx19Construct(t *testing.T, c ebusgateway.GrowattPCSConfig, open growattProjectOpen, associate growattProjectAssociation) *growattPCSManagedDriver {
	t.Helper()
	r, err := newGrowattPCSManagedFactory(c, open, associate).Construct(context.Background(), runtimecontrol.Construction{})
	if err != nil {
		t.Fatal(err)
	}
	return r.(*growattPCSManagedDriver)
}
func cx19New(t *testing.T, c ebusgateway.GrowattPCSConfig, associate growattProjectAssociation) (*growattPCSManagedDriver, *cx19Receiver) {
	t.Helper()
	r := cx19Fake(c)
	d := cx19Construct(t, c, func(name string) (growattProjectReceiver, error) {
		if name != c.Native.Interface.Name() {
			t.Error("wrong selected name")
		}
		return r, nil
	}, associate)
	if _, err := d.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := d.Stop(context.Background()); err != nil && r.closeErr == nil {
			t.Error(err)
		}
	})
	return d, r
}
func cx19Send(t *testing.T, r *cx19Receiver, record growattProjectRecord) {
	t.Helper()
	select {
	case r.records <- record:
	case <-time.After(time.Second):
		t.Fatal("synthetic send timed out")
	}
}
func cx19Wait(t *testing.T, d *growattPCSManagedDriver, now time.Duration, predicate func(growattProjectSnapshot) bool) growattProjectSnapshot {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		s := d.Snapshot(now)
		if predicate(s) {
			return s
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("synthetic snapshot timed out")
	return growattProjectSnapshot{}
}
func cx19Raw(t *testing.T, d *growattPCSManagedDriver, count int) canbusreg.CX17Snapshot {
	t.Helper()
	return cx19Wait(t, d, time.Second, func(s growattProjectSnapshot) bool { return len(s.Providers) == 1 && len(s.Providers[0].Raw) >= count }).Providers[0]
}
func TestCX19InertAndValidation(t *testing.T) {
	calls := 0
	open := func(string) (growattProjectReceiver, error) { calls++; return nil, errors.New("synthetic open") }
	disabled := cx19Construct(t, ebusgateway.GrowattPCSConfig{}, open, nil)
	if _, err := disabled.Start(context.Background()); !errors.Is(err, drivermanager.ErrUnavailable) {
		t.Fatal(err)
	}
	if err := disabled.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	c := cx19Config(t, "pcs-standard")
	d := cx19Construct(t, c, open, cx19Association)
	_ = d.Snapshot(0)
	if calls != 0 || d.Generation() != 0 {
		t.Fatal("construction opened transport")
	}
	for _, field := range []string{"selector", "project", "release", "scope", "bank", "listen-only", "age", "association", "disabled-fields"} {
		t.Run(field, func(t *testing.T) {
			bad := c
			assoc := growattProjectAssociation(cx19Association)
			switch field {
			case "selector":
				bad.Native = canbusreg.CX01SelectionSpec{}
			case "project":
				bad.Native.ProjectCode = ""
			case "release":
				bad.Native.Release = ""
			case "scope":
				bad.Native.Scope = ""
			case "bank":
				bad.Native.Bank = ""
			case "listen-only":
				bad.ListenOnlyConfirmed = false
			case "age":
				bad.StaleAfter = 0
			case "association":
				assoc = nil
			case "disabled-fields":
				bad.Enabled = false
			}
			if _, err := newGrowattPCSManagedFactory(bad, open, assoc).Construct(context.Background(), runtimecontrol.Construction{}); err == nil {
				t.Fatal("invalid construction admitted")
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := d.Start(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if calls != 0 {
		t.Fatal("validation opened transport")
	}
}
func TestCX19SelectedFlavorsAndCachedEvidence(t *testing.T) {
	for _, flavor := range []string{"pcs-standard", "pcs-extended19", "pcs-parallel"} {
		t.Run(flavor, func(t *testing.T) {
			c := cx19Config(t, flavor)
			d, r := cx19New(t, c, cx19Association)
			record := cx19Record(c, 1)
			cx19Send(t, r, record)
			got := cx19Raw(t, d, 1)
			if got.Selection.CX01Spec() != c.Native || got.Generation != 1 || got.PhysicalQualified || !reflect.DeepEqual(got.Raw[0].NativeRecord, record.Raw) {
				t.Fatal(got)
			}
			if len(got.Current) == 0 {
				t.Fatal("native decoder not reached")
			}
			reads := r.reads.Load()
			for range 10 {
				_ = d.Snapshot(time.Second)
			}
			if r.reads.Load() != reads {
				t.Fatal("cached read performed I/O")
			}
			got.Raw[0].NativeRecord[8] ^= 0xff
			for k := range got.Current {
				delete(got.Current, k)
			}
			fresh := d.Snapshot(time.Second).Providers[0]
			if !reflect.DeepEqual(fresh.Raw[0].NativeRecord, record.Raw) || len(fresh.Current) == 0 {
				t.Fatal("aliased snapshot")
			}
		})
	}
}
func TestCX19FieldwiseStalenessAndLastGood(t *testing.T) {
	c := cx19Config(t, "pcs-standard")
	d, r := cx19New(t, c, func(s canbusreg.CXSelection, e canbusreg.Evidence) canbusreg.CX16ReplayInput {
		i := cx19Association(s, e)
		i.PCS.FieldAges = map[string]time.Duration{"voltage": 0, "current": time.Second}
		if e.Sequence == 2 {
			i.PCS.FieldStates = map[string]string{"voltage": "invalid"}
		}
		return i
	})
	cx19Send(t, r, cx19Record(c, 1))
	_ = cx19Raw(t, d, 1)
	for _, now := range []time.Duration{time.Second, time.Second + 1, 2 * time.Second, 2*time.Second + 1} {
		s := d.Snapshot(now).Providers[0]
		if (s.Current["core/voltage"].Status == "accepted") != (now <= 2*time.Second) || (s.Current["core/current"].Status == "accepted") != (now <= time.Second) {
			t.Fatal(now, s)
		}
	}
	cx19Send(t, r, cx19Record(c, 2))
	s := cx19Raw(t, d, 2)
	if s.Current["core/voltage"].Status != "invalid" || s.LastGood["core/voltage"].Evidence.Sequence != 1 || s.Current["core/current"].Evidence.Sequence != 2 {
		t.Fatal(s)
	}
}

// Fixed bytes with independently varied context gates test withholding rather
// than plausibility-based selection. Every identity component is exercised.
func TestCX19HypothesisContextNegativeVectors(t *testing.T) {
	mutations := map[string]func(*canbusreg.CX16ReplayInput){
		"missing-selector":      func(i *canbusreg.CX16ReplayInput) { i.Selection = canbusreg.CXSelection{} },
		"epoch-reuse":           func(i *canbusreg.CX16ReplayInput) { i.PCS.Epoch++ },
		"wrong-direction":       func(i *canbusreg.CX16ReplayInput) { i.PCS.Direction = "pcs-to-bms" },
		"wrong-scope":           func(i *canbusreg.CX16ReplayInput) { i.PCS.Scope = "aggregate" },
		"wrong-bank":            func(i *canbusreg.CX16ReplayInput) { i.PCS.Bank = "parallel" },
		"wrong-endpoint":        func(i *canbusreg.CX16ReplayInput) { i.PCS.Endpoint = "other" },
		"wrong-group":           func(i *canbusreg.CX16ReplayInput) { i.PCS.Group = "other" },
		"wrong-slot":            func(i *canbusreg.CX16ReplayInput) { i.PCS.Slot = "other" },
		"unassigned-publishers": func(i *canbusreg.CX16ReplayInput) { i.PCS.PublisherResolved = false },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			c := cx19Config(t, "pcs-standard")
			d, r := cx19New(t, c, func(s canbusreg.CXSelection, e canbusreg.Evidence) canbusreg.CX16ReplayInput {
				i := cx19Association(s, e)
				mutate(&i)
				return i
			})
			record := cx19Record(c, 1)
			cx19Send(t, r, record)
			s := cx19Raw(t, d, 1)
			if s.Promotable || len(s.Current) != 0 || !reflect.DeepEqual(s.Raw[0].NativeRecord, record.Raw) {
				t.Fatal(s)
			}
		})
	}
}
func TestCX19SourceAndEpochReuseInvalidateDependentState(t *testing.T) {
	for _, field := range []string{"epoch", "selector-source"} {
		t.Run(field, func(t *testing.T) {
			c := cx19Config(t, "pcs-standard")
			d, r := cx19New(t, c, func(s canbusreg.CXSelection, e canbusreg.Evidence) canbusreg.CX16ReplayInput {
				i := cx19Association(s, e)
				if e.Sequence > 1 {
					if field == "epoch" {
						i.PCS.Epoch++
					} else {
						spec := s.CX01Spec()
						spec.Source++
						i.Selection, _ = canbusreg.NewCXSelection(spec)
					}
				}
				return i
			})
			cx19Send(t, r, cx19Record(c, 1))
			_ = cx19Raw(t, d, 1)
			cx19Send(t, r, cx19Record(c, 2))
			s := cx19Raw(t, d, 2)
			if s.Promotable || len(s.Current) != 0 || len(s.LastGood) == 0 {
				t.Fatal(s)
			}
		})
	}
}
func TestCX19UnassignedStandardPublishersNeverMerge(t *testing.T) {
	c := cx19Config(t, "pcs-standard")
	d, r := cx19New(t, c, func(s canbusreg.CXSelection, e canbusreg.Evidence) canbusreg.CX16ReplayInput {
		i := cx19Association(s, e)
		i.PCS.PublisherResolved = false
		return i
	})
	for index, id := range []uint32{0x311, 0x312, 0x313} {
		record := cx19Record(c, uint64(index+1))
		binary.NativeEndian.PutUint32(record.Raw, id)
		cx19Send(t, r, record)
	}
	s := cx19Raw(t, d, 3)
	if len(s.Current) != 0 || s.Promotable {
		t.Fatal("unassigned publishers merged", s)
	}
	for _, raw := range s.Raw {
		if len(raw.NativeRecord) != 16 {
			t.Fatal("raw discarded")
		}
	}
}
func TestCX19OptionalTrafficCannotSelectOrFallback(t *testing.T) {
	for _, flavor := range []string{"pcs-standard", "pcs-extended19", "pcs-parallel"} {
		t.Run(flavor, func(t *testing.T) {
			c := cx19Config(t, flavor)
			d, r := cx19New(t, c, func(s canbusreg.CXSelection, e canbusreg.Evidence) canbusreg.CX16ReplayInput {
				i := cx19Association(s, e)
				i.Selection = canbusreg.CXSelection{}
				return i
			})
			for index, id := range []uint32{0x311, 0x312, 0x313, 0x37f} {
				record := cx19Record(c, uint64(index+1))
				binary.NativeEndian.PutUint32(record.Raw, id)
				cx19Send(t, r, record)
			}
			s := cx19Raw(t, d, 4)
			if len(s.Current) != 0 || s.Promotable || s.Selection.CX01Spec().Flavor != flavor {
				t.Fatal("inferred selection/fallback", s)
			}
		})
	}
}
func TestCX19WrongInterfaceIdentity(t *testing.T) {
	for _, field := range []string{"name", "index"} {
		t.Run(field, func(t *testing.T) {
			c := cx19Config(t, "pcs-standard")
			d, r := cx19New(t, c, cx19Association)
			record := cx19Record(c, 1)
			if field == "name" {
				record.Interface, _ = canbus.NewInterfaceIdentity("synthetic1", 7)
			} else {
				record.Interface, _ = canbus.NewInterfaceIdentity("synthetic0", 8)
			}
			cx19Send(t, r, record)
			s := cx19Wait(t, d, time.Second, func(s growattProjectSnapshot) bool { return len(s.Raw) == 1 })
			if s.Raw[0].Status != "wrong-interface" || len(s.Providers[0].Current) != 0 || !reflect.DeepEqual(s.Raw[0].Record.Raw, record.Raw) {
				t.Fatal(s)
			}
		})
	}
}
func TestCX19ExtendedWrongSource(t *testing.T) {
	c := cx19Config(t, "pcs-extended19")
	d, r := cx19New(t, c, cx19Association)
	record := cx19Record(c, 1)
	binary.NativeEndian.PutUint32(record.Raw, 0x80000000|0x1900f202)
	cx19Send(t, r, record)
	s := cx19Raw(t, d, 1)
	if s.Promotable || len(s.Current) != 0 || !reflect.DeepEqual(s.Raw[0].NativeRecord, record.Raw) {
		t.Fatal(s)
	}
}
func TestCX19FailureCorrelationAndReplacement(t *testing.T) {
	c := cx19Config(t, "pcs-standard")
	first, second := cx19Fake(c), cx19Fake(c)
	opens := 0
	d := cx19Construct(t, c, func(string) (growattProjectReceiver, error) {
		opens++
		if opens == 1 {
			return first, nil
		}
		return second, nil
	}, cx19Association)
	defer func() { _ = d.Stop(context.Background()) }()
	if _, err := d.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	type callback struct {
		native    drivermanager.Correlation
		lifecycle runtimecontrol.LifecycleCorrelation
	}
	calls := make(chan callback, 4)
	d.reportFailure = func(n drivermanager.Correlation, l runtimecontrol.LifecycleCorrelation) { calls <- callback{n, l} }
	l := runtimecontrol.LifecycleCorrelation{DriverID: "synthetic", SourceID: "pcs", Epoch: "epoch", Generation: 1, Sequence: 1}
	_ = d.BindLifecycle(l)
	n := drivermanager.Correlation{Generation: 1, Operation: 7}
	d.BindCorrelation(drivermanager.Correlation{Generation: 2, Operation: 7})
	d.BindCorrelation(n)
	d.BindCorrelation(n)
	close(first.fail)
	select {
	case got := <-calls:
		if got.native != n || got.lifecycle != l {
			t.Fatal(got)
		}
	case <-time.After(time.Second):
		t.Fatal("missing read failure callback")
	}
	s := cx19Wait(t, d, time.Second, func(s growattProjectSnapshot) bool {
		return s.ReceiveError != nil && s.Providers[0].State == "degraded"
	})
	if s.Providers[0].Promotable {
		t.Fatal(s)
	}
	g, err := d.Replace(context.Background())
	if err != nil || g != 2 || first.closes.Load() != 1 {
		t.Fatal(g, err)
	}
	d.BindCorrelation(n)
	cx19Send(t, second, cx19Record(c, 1))
	s = d.Snapshot(time.Second)
	_ = cx19Raw(t, d, 1)
	select {
	case got := <-calls:
		t.Fatal("retired/duplicate callback", got)
	default:
	}
	if opens != 2 {
		t.Fatal(opens)
	}
}
func TestCX19StopFencesJoinsLateInputAndHonorsDeadline(t *testing.T) {
	c := cx19Config(t, "pcs-standard")
	first, second := cx19Fake(c), cx19Fake(c)
	late := cx19Record(c, 2)
	first.late = &late
	first.release = make(chan struct{})
	opens := 0
	d := cx19Construct(t, c, func(string) (growattProjectReceiver, error) {
		opens++
		if opens == 1 {
			return first, nil
		}
		return second, nil
	}, cx19Association)
	if _, err := d.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	cx19Send(t, first, cx19Record(c, 1))
	_ = cx19Raw(t, d, 1)
	d.FenceWithdrawal()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := d.Stop(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if s := d.Snapshot(time.Second); len(s.Providers) > 0 && s.Providers[0].Promotable {
		t.Fatal("fence leaked values")
	}
	if _, err := d.Start(context.Background()); err == nil || first.closes.Load() != 0 {
		t.Fatal("ownership escaped drain")
	}
	close(first.release)
	if err := d.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if g, err := d.Start(context.Background()); err != nil || g != 2 {
		t.Fatal(g, err)
	}
	defer func() { _ = d.Stop(context.Background()) }()
	if len(d.Snapshot(time.Second).Providers[0].Current) != 0 {
		t.Fatal("late predecessor data published")
	}
	cx19Send(t, second, cx19Record(c, 1))
	s := cx19Raw(t, d, 2)
	if s.Generation != 2 || s.Current["core/voltage"].Evidence.Sequence != 1 {
		t.Fatal(s)
	}
}
func TestCX19FailedCloseQuarantines(t *testing.T) {
	c := cx19Config(t, "pcs-standard")
	d, r := cx19New(t, c, cx19Association)
	r.closeErr = errors.New("synthetic close failure")
	if err := d.Stop(context.Background()); !errors.Is(err, drivermanager.ErrSafetyQuarantined) {
		t.Fatal(err)
	}
	if !d.SafetyQuarantined() || !d.Snapshot(0).Quarantined {
		t.Fatal("missing quarantine")
	}
	if _, err := d.Replace(context.Background()); err == nil || r.closes.Load() != 1 {
		t.Fatal("quarantine reopened")
	}
}
func TestCX19UnsupportedPlatform(t *testing.T) {
	if runtime.GOOS == "linux" {
		t.Skip("unsupported-platform branch requires non-Linux; no live socket is invoked")
	}
	d := cx19Construct(t, cx19Config(t, "pcs-standard"), nil, cx19Association)
	if _, err := d.Start(context.Background()); err == nil || !strings.Contains(err.Error(), "unsupported") {
		t.Fatal(err)
	}
}

func TestCX19NativeVoltageMeaningsRemainDistinct(t *testing.T) {
	for _, release := range []string{"0006", "0011"} {
		t.Run(release, func(t *testing.T) {
			c := cx19Config(t, "pcs-standard")
			c.Native.Release = release
			d, r := cx19New(t, c, cx19Association)
			record := cx19Record(c, 1)
			binary.BigEndian.PutUint16(record.Raw[8:], 5310)
			cx19Send(t, r, record)
			s := cx19Raw(t, d, 1)
			f := s.Current["core/voltage"]
			meaning := "pack voltage"
			if release == "0011" {
				meaning = "output-port voltage"
			}
			if f.Numerator != 5310 || f.Denominator != 100 || f.Unit != "V" || f.Meaning != meaning || f.Grade != "Proven" || f.PhysicalQualified || f.Raw != s.Raw[0].Input.Evidence.Frame.Bytes() || f.Epoch != 1 || f.Endpoint != "synthetic" {
				t.Fatal(f)
			}
		})
	}
}
func TestCX19ExtremaNeverBecomeCompleteCells(t *testing.T) {
	for _, valid := range []bool{true, false} {
		name := "valid-extrema"
		if !valid {
			name = "withheld-measurement"
		}
		t.Run(name, func(t *testing.T) {
			c := cx19Config(t, "pcs-standard")
			d, r := cx19New(t, c, func(s canbusreg.CXSelection, e canbusreg.Evidence) canbusreg.CX16ReplayInput {
				i := cx19Association(s, e)
				i.PCS.MeasurementValid = valid
				return i
			})
			record := cx19Record(c, 1)
			binary.NativeEndian.PutUint32(record.Raw, 0x319)
			cx19Send(t, r, record)
			s := cx19Raw(t, d, 1)
			if len(s.Current) == 0 || !reflect.DeepEqual(s.Raw[0].NativeRecord, record.Raw) {
				t.Fatal(s)
			}
			for name, f := range s.Current {
				if strings.Contains(name, "cell_") || strings.Contains(name, "cells/") {
					t.Fatal("extrema expanded into cells", name)
				}
				if !valid && f.Status == "accepted" && f.Unit == "mV" {
					t.Fatal("invalid extrema interpreted", f)
				}
			}
		})
	}
}
func TestCX19UnknownCachedMemberAgeRemainsWithheld(t *testing.T) {
	c := cx19Config(t, "pcs-standard")
	d, r := cx19New(t, c, func(s canbusreg.CXSelection, e canbusreg.Evidence) canbusreg.CX16ReplayInput {
		i := cx19Association(s, e)
		i.PCS.SlotValid = true
		i.PCS.NativeSlot = 0
		i.PCS.FieldAges = map[string]time.Duration{"voltage": -1}
		i.PCS.FieldStates = map[string]string{"voltage": "absent"}
		return i
	})
	record := cx19Record(c, 1)
	binary.NativeEndian.PutUint32(record.Raw, 0x326)
	clear(record.Raw[8:])
	record.Raw[10] = 5
	cx19Send(t, r, record)
	s := cx19Raw(t, d, 1)
	f, ok := s.Current["slots/unassociated/voltage"]
	if !ok || f.Status == "accepted" || f.Age >= 0 || !reflect.DeepEqual(s.Raw[0].NativeRecord, record.Raw) {
		t.Fatal(s)
	}
}
func TestCX19SelectionIdentityEveryField(t *testing.T) {
	mutations := map[string]func(*canbusreg.CX01SelectionSpec){
		"interface-name":  func(c *canbusreg.CX01SelectionSpec) { c.Interface, _ = canbus.NewInterfaceIdentity("synthetic1", 7) },
		"interface-index": func(c *canbusreg.CX01SelectionSpec) { c.Interface, _ = canbus.NewInterfaceIdentity("synthetic0", 8) },
		"flavor":          func(c *canbusreg.CX01SelectionSpec) { c.Flavor = "pcs-parallel" },
		"pcs-flavor":      func(c *canbusreg.CX01SelectionSpec) { c.PCSFlavor = "pcs-parallel" },
		"project":         func(c *canbusreg.CX01SelectionSpec) { c.ProjectCode = "QAAa" },
		"release":         func(c *canbusreg.CX01SelectionSpec) { c.Release = "0010" },
		"choice":          func(c *canbusreg.CX01SelectionSpec) { c.Choice = "legacy19" },
		"direction":       func(c *canbusreg.CX01SelectionSpec) { c.Direction = "pcs-to-bms" },
		"scope":           func(c *canbusreg.CX01SelectionSpec) { c.Scope = "aggregate" },
		"bank":            func(c *canbusreg.CX01SelectionSpec) { c.Bank = "parallel" },
		"endpoint":        func(c *canbusreg.CX01SelectionSpec) { c.Endpoint = "other" },
		"group":           func(c *canbusreg.CX01SelectionSpec) { c.Group = "1" },
		"slot":            func(c *canbusreg.CX01SelectionSpec) { c.Slot = "1" },
		"route-set":       func(c *canbusreg.CX01SelectionSpec) { c.RouteSet = true },
		"destination":     func(c *canbusreg.CX01SelectionSpec) { c.Destination = 245 },
		"source":          func(c *canbusreg.CX01SelectionSpec) { c.Source = 2 },
		"epoch":           func(c *canbusreg.CX01SelectionSpec) { c.Epoch = 2 },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			c := cx19Config(t, "pcs-standard")
			d, r := cx19New(t, c, func(s canbusreg.CXSelection, e canbusreg.Evidence) canbusreg.CX16ReplayInput {
				i := cx19Association(s, e)
				spec := s.CX01Spec()
				mutate(&spec)
				var err error
				i.Selection, err = canbusreg.NewCXSelection(spec)
				if err != nil {
					t.Error(err)
				}
				return i
			})
			record := cx19Record(c, 1)
			cx19Send(t, r, record)
			s := cx19Raw(t, d, 1)
			if s.Promotable || len(s.Current) != 0 || s.State != "conflict" || !reflect.DeepEqual(s.Raw[0].NativeRecord, record.Raw) {
				t.Fatal(s)
			}
		})
	}
}
func TestCX19StartCancellationJoinsAndCloses(t *testing.T) {
	c := cx19Config(t, "pcs-standard")
	r := cx19Fake(c)
	entered, release := make(chan struct{}), make(chan struct{})
	d := cx19Construct(t, c, func(string) (growattProjectReceiver, error) { close(entered); <-release; return r, nil }, cx19Association)
	result := make(chan error, 1)
	go func() { _, err := d.Start(context.Background()); result <- err }()
	<-entered
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := d.Stop(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	close(release)
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("startup did not join")
	}
	if err := d.Stop(context.Background()); err != nil || r.closes.Load() != 1 {
		t.Fatal(err, r.closes.Load())
	}
	if len(d.Snapshot(time.Second).Providers[0].Current) != 0 {
		t.Fatal("canceled startup published")
	}
}
func TestCX19OpenFailuresAndReceiverIdentity(t *testing.T) {
	for _, mode := range []string{"open", "nil", "partial-open", "name", "index", "close-quarantine"} {
		t.Run(mode, func(t *testing.T) {
			c := cx19Config(t, "pcs-standard")
			r := cx19Fake(c)
			if mode == "name" {
				r.id, _ = canbus.NewInterfaceIdentity("synthetic1", 7)
			}
			if mode == "index" {
				r.id, _ = canbus.NewInterfaceIdentity("synthetic0", 8)
			}
			if mode == "close-quarantine" {
				r.id, _ = canbus.NewInterfaceIdentity("synthetic1", 7)
				r.closeErr = errors.New("synthetic close failure")
			}
			d := cx19Construct(t, c, func(string) (growattProjectReceiver, error) {
				switch mode {
				case "open":
					return nil, errors.New("synthetic open failure")
				case "nil":
					return nil, nil
				case "partial-open":
					return r, errors.New("synthetic partial open")
				}
				return r, nil
			}, cx19Association)
			if _, err := d.Start(context.Background()); err == nil {
				t.Fatal("bad open admitted")
			}
			if mode != "open" && mode != "nil" && r.closes.Load() != 1 {
				t.Fatal("partial ownership not closed")
			}
			if (mode == "close-quarantine") != d.SafetyQuarantined() {
				t.Fatal("incorrect quarantine")
			}
		})
	}
}
func TestCX19NumericEvidenceBoundaryPairs(t *testing.T) {
	for _, mode := range []string{"sequence-zero", "sequence-one", "time-negative", "time-zero", "dlc-seven", "dlc-eight", "raw-dlc-eight", "raw-dlc-nine"} {
		t.Run(mode, func(t *testing.T) {
			c := cx19Config(t, "pcs-standard")
			d, r := cx19New(t, c, cx19Association)
			record := cx19Record(c, 1)
			valid := true
			switch mode {
			case "sequence-zero":
				record.Sequence = 0
				valid = false
			case "time-negative":
				record.Monotonic = -1
				valid = false
			case "time-zero":
				record.Monotonic = 0
			case "dlc-seven":
				record.Raw[4] = 7
				valid = false
			case "raw-dlc-eight":
				// Zero len8_dlc preserves the ordinary raw DLC of eight.
				record.Raw[7] = 0
			case "raw-dlc-nine":
				record.Raw[7] = 9
				valid = false
			}
			cx19Send(t, r, record)
			s := cx19Wait(t, d, record.Monotonic, func(s growattProjectSnapshot) bool {
				return len(s.Raw) == 1 && (!valid || len(s.Providers[0].Raw) == 1)
			})
			if s.Providers[0].Promotable != valid || !reflect.DeepEqual(s.Raw[0].Record.Raw, record.Raw) {
				t.Fatal(s)
			}
		})
	}
}

func TestCX19EarlyFailureAndDelayedCallbackKeepOriginalCorrelation(t *testing.T) {
	c := cx19Config(t, "pcs-standard")
	first, second := cx19Fake(c), cx19Fake(c)
	opens := 0
	d := cx19Construct(t, c, func(string) (growattProjectReceiver, error) {
		opens++
		if opens == 1 {
			return first, nil
		}
		return second, nil
	}, cx19Association)
	defer func() { _ = d.Stop(context.Background()) }()
	if _, err := d.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	close(first.fail)
	_ = cx19Wait(t, d, time.Second, func(s growattProjectSnapshot) bool { return s.ReceiveError != nil })
	old := runtimecontrol.LifecycleCorrelation{DriverID: "synthetic-a", SourceID: "source-a", Epoch: "epoch-a", Generation: 7, Sequence: 9}
	current := runtimecontrol.LifecycleCorrelation{DriverID: "synthetic-b", SourceID: "source-b", Epoch: "epoch-b", Generation: 8, Sequence: 10}
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	native := drivermanager.Correlation{Generation: 1, Operation: 4}
	d.reportFailure = func(n drivermanager.Correlation, l runtimecontrol.LifecycleCorrelation) {
		close(entered)
		<-release
		if n != native || l != old {
			t.Error("callback identity changed", n, l)
		}
		close(done)
	}
	_ = d.BindLifecycle(old)
	d.BindCorrelation(native)
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("early failure lost")
	}
	if _, err := d.Replace(context.Background()); err != nil {
		t.Fatal(err)
	}
	_ = d.BindLifecycle(current)
	d.BindCorrelation(drivermanager.Correlation{Generation: 2, Operation: 5})
	close(release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("callback did not finish")
	}
	cx19Send(t, second, cx19Record(c, 1))
	s := cx19Raw(t, d, 1)
	if s.Generation != 2 || !s.Promotable {
		t.Fatal("late callback affected successor", s)
	}
}
func TestCX19GenerationAndEpochBoundaryPairs(t *testing.T) {
	c := cx19Config(t, "pcs-standard")
	invalid := c
	invalid.Native.Epoch = 0
	if _, err := newGrowattPCSManagedFactory(invalid, func(string) (growattProjectReceiver, error) { t.Fatal("invalid epoch opened"); return nil, nil }, cx19Association).Construct(context.Background(), runtimecontrol.Construction{}); err == nil {
		t.Fatal("zero epoch accepted")
	}
	d, r := cx19New(t, c, cx19Association)
	if g, err := d.Start(context.Background()); err != nil || g != 1 {
		t.Fatal(g, err)
	}
	if err := d.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	d.mu.Lock()
	d.generation = ^uint64(0)
	d.mu.Unlock()
	if _, err := d.Start(context.Background()); err == nil || r.closes.Load() != 1 {
		t.Fatal("generation wrapped")
	}
}

func TestCX19FieldStatesAndZeroRemainDistinct(t *testing.T) {
	for _, state := range []string{"accepted", "absent", "invalid", "unsupported", "stale", "conflict", "unassigned"} {
		t.Run(state, func(t *testing.T) {
			c := cx19Config(t, "pcs-standard")
			d, r := cx19New(t, c, func(s canbusreg.CXSelection, e canbusreg.Evidence) canbusreg.CX16ReplayInput {
				i := cx19Association(s, e)
				if state != "accepted" {
					i.PCS.FieldStates = map[string]string{"voltage": state}
				}
				return i
			})
			record := cx19Record(c, 1)
			clear(record.Raw[8:10])
			cx19Send(t, r, record)
			s := cx19Raw(t, d, 1)
			voltage, current := s.Current["core/voltage"], s.Current["core/current"]
			if voltage.Numerator != 0 || voltage.Status != state || current.Status != "accepted" || !reflect.DeepEqual(s.Raw[0].NativeRecord, record.Raw) {
				t.Fatal(s)
			}
			_, last := s.LastGood["core/voltage"]
			if last != (state == "accepted") {
				t.Fatal("withheld zero became last-good", s)
			}
		})
	}
}
