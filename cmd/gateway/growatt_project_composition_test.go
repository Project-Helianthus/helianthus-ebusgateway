package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	canbusreg "github.com/Project-Helianthus/helianthus-canbusreg"
	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/drivermanager"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimecontrol"
)

func cx21Combined(t *testing.T) ebusgateway.Config {
	t.Helper()
	cfg := ebusgateway.DefaultConfig()
	cfg.GrowattPCSCANConfig = cx21Config(t, false)
	cfg.GrowattLINKCANConfig = cx21Config(t, true)
	cfg.GrowattLINKCANConfig.Interface = "synthetic1"
	cfg.GrowattBMSCANConfig = ebusgateway.GrowattBMSCANConfig{Enabled: true, Interface: "synthetic2", ListenOnlyConfirmed: true, StaleAfter: time.Second}
	return cfg
}
func TestCX21CombinedInvalidConfigZeroOpens(t *testing.T) {
	cases := map[string]func(*ebusgateway.Config){
		"V104 missing confirmation": func(c *ebusgateway.Config) { c.GrowattBMSCANConfig.ListenOnlyConfirmed = false },
		"V104 zero stale":           func(c *ebusgateway.Config) { c.GrowattBMSCANConfig.StaleAfter = 0 },
		"V104 PCS":                  func(c *ebusgateway.Config) { c.GrowattBMSCANConfig.Interface = c.GrowattPCSCANConfig.Interface },
		"V104 LINK":                 func(c *ebusgateway.Config) { c.GrowattBMSCANConfig.Interface = c.GrowattLINKCANConfig.Interface },
		"PCS LINK":                  func(c *ebusgateway.Config) { c.GrowattLINKCANConfig.Interface = c.GrowattPCSCANConfig.Interface },
		"triple": func(c *ebusgateway.Config) {
			c.GrowattBMSCANConfig.Interface = c.GrowattPCSCANConfig.Interface
			c.GrowattLINKCANConfig.Interface = c.GrowattPCSCANConfig.Interface
		},
		"missing PCS context":  func(c *ebusgateway.Config) { c.GrowattPCSCANConfig.Context = "" },
		"missing LINK context": func(c *ebusgateway.Config) { c.GrowattLINKCANConfig.Context = "" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			cfg := cx21Combined(t)
			mutate(&cfg)
			original := gatewayManagedSidecarDescriptorsFor
			t.Cleanup(func() { gatewayManagedSidecarDescriptorsFor = original })
			var opens atomic.Int32
			gatewayManagedSidecarDescriptorsFor = func(ds []runtimecontrol.Descriptor) []runtimecontrol.Descriptor {
				for i := range ds {
					if isGrowattCANDescriptor(ds[i].DriverID) {
						ds[i].Adapter = runtimecontrol.NewAdapter(func(context.Context, runtimecontrol.Construction) (*m06Runtime, error) {
							opens.Add(1)
							return &m06Runtime{}, nil
						})
					}
				}
				return ds
			}
			sidecars := prepareManagedSidecars(context.Background(), cfg, newTestSemanticHub(t), "", nil, nil)
			b := &managedSidecarBindings{sidecars: sidecars}
			for _, id := range []runtimecontrol.DriverID{managedGrowattCANID, managedGrowattPCSID, managedGrowattLINKID} {
				s := b.entry(id)
				if s == nil || s.withheld == nil || s.runtime != nil {
					t.Fatalf("%s not withheld", id)
				}
				if err := s.Start(context.Background(), runtimecontrol.LifecycleCorrelation{}); err == nil {
					t.Fatal("invalid start succeeded")
				}
			}
			if opens.Load() != 0 {
				t.Fatal("invalid configuration reached native construction/open")
			}
		})
	}
}
func TestCX21DistinctInterfacesAndIndependentOpenFailure(t *testing.T) {
	for _, failed := range []runtimecontrol.DriverID{managedGrowattPCSID, managedGrowattLINKID} {
		t.Run(string(failed), func(t *testing.T) {
			cfg := cx21Combined(t)
			cfg.GrowattBMSCANConfig = ebusgateway.GrowattBMSCANConfig{}
			sidecars := prepareManagedSidecars(context.Background(), cfg, newTestSemanticHub(t), "", nil, nil)
			b := &managedSidecarBindings{sidecars: sidecars}
			receivers := map[runtimecontrol.DriverID]*cx20Receiver{}
			counts := map[runtimecontrol.DriverID]*atomic.Int32{}
			for _, id := range []runtimecontrol.DriverID{managedGrowattPCSID, managedGrowattLINKID} {
				s := b.entry(id)
				if s == nil || s.withheld != nil || s.runtime == nil {
					t.Fatalf("construction %s: %+v", id, s)
				}
				w := s.runtime.Runtime.(*growattProjectManagedRuntime)
				c := cfg.GrowattPCSCANConfig
				link := id == managedGrowattLINKID
				if link {
					c = cfg.GrowattLINKCANConfig
				}
				a := cx21Admission(t, c, link)
				r := &cx20Receiver{id: a.selections[0].SelectionSpecValue().Interface, records: make(chan growattProjectRecord), fail: make(chan error)}
				receivers[id] = r
				counter := new(atomic.Int32)
				counts[id] = counter
				w.open = func(name string) (growattProjectReceiver, error) {
					counter.Add(1)
					if name != r.id.Name() {
						return nil, errors.New("wrong selected interface")
					}
					if id == failed {
						return nil, errors.New("synthetic independent open failure")
					}
					return r, nil
				}
			}
			manager, err := drivermanager.New(drivermanager.Config{Drivers: sidecars.driverConfigs()})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			defer func() {
				if err := manager.Shutdown(ctx); err != nil {
					t.Error(err)
				}
			}()
			sidecars.bind(manager, nil)
			for _, id := range []runtimecontrol.DriverID{managedGrowattPCSID, managedGrowattLINKID} {
				s := b.entry(id)
				err := s.Start(ctx, runtimecontrol.LifecycleCorrelation{DriverID: id, Epoch: "synthetic-epoch", Generation: 1})
				if (err != nil) != (id == failed) {
					t.Fatalf("start %s: %v", id, err)
				}
			}
			for _, id := range []runtimecontrol.DriverID{managedGrowattPCSID, managedGrowattLINKID} {
				snapshot, ok := manager.Snapshot(string(id))
				want := drivermanager.ObservedRunning
				if id == failed {
					want = drivermanager.ObservedFailed
				}
				if !ok || snapshot.ObservedState != want || counts[id].Load() != 1 {
					t.Fatalf("independent status %s: %+v", id, snapshot)
				}
			}
		})
	}
}
func TestCX21CachedBindingsAndReceiveFailureFence(t *testing.T) {
	cfg := cx21Combined(t)
	cfg.GrowattBMSCANConfig = ebusgateway.GrowattBMSCANConfig{}
	sidecars := prepareManagedSidecars(context.Background(), cfg, newTestSemanticHub(t), "", nil, nil)
	b := &managedSidecarBindings{sidecars: sidecars}
	manager, err := drivermanager.New(drivermanager.Config{Drivers: sidecars.driverConfigs()})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	defer func() {
		if err := manager.Shutdown(ctx); err != nil {
			t.Error(err)
		}
	}()
	sidecars.bind(manager, nil)
	receivers := map[runtimecontrol.DriverID]*cx20Receiver{}
	for _, id := range []runtimecontrol.DriverID{managedGrowattPCSID, managedGrowattLINKID} {
		s := b.entry(id)
		w := s.runtime.Runtime.(*growattProjectManagedRuntime)
		c := cfg.GrowattPCSCANConfig
		link := id == managedGrowattLINKID
		if link {
			c = cfg.GrowattLINKCANConfig
		}
		a := cx21Admission(t, c, link)
		r := &cx20Receiver{id: a.selections[0].SelectionSpecValue().Interface, records: make(chan growattProjectRecord), fail: make(chan error)}
		receivers[id] = r
		w.open = func(string) (growattProjectReceiver, error) { return r, nil }
		w.reportFailure = func(native drivermanager.Correlation, _ runtimecontrol.LifecycleCorrelation) {
			manager.ReportFailure(string(id), native, drivermanager.Failure{Reason: drivermanager.Reason{Code: drivermanager.ReasonProviderUnavailable}})
		}
		if err := s.Start(ctx, runtimecontrol.LifecycleCorrelation{DriverID: id, Epoch: "synthetic-epoch", Generation: 1}); err != nil {
			t.Fatal(err)
		}
	}
	pcsNative := b.entry(managedGrowattPCSID).runtime.Runtime.(*growattProjectManagedRuntime).Runtime.(*growattPCSManagedDriver)
	pcsRecord := cx19Record(pcsNative.config, 1)
	cx20Send(t, receivers[managedGrowattPCSID], pcsRecord)
	linkRecord := cx20Record(receivers[managedGrowattLINKID].id, 1, 0x1810, 1, time.Second)
	cx20Send(t, receivers[managedGrowattLINKID], linkRecord)
	m06Wait(t, func() bool {
		pcs, e1 := b.growattPCS(ctx, time.Second)
		link, e2 := b.growattLINK(ctx, time.Second)
		return e1 == nil && e2 == nil && len(pcs.Raw) > 0 && len(link.Native.Raw) > 0
	})
	pcs, err := b.growattPCS(ctx, time.Second)
	if err != nil || pcs.Providers[0].PhysicalQualified || !bytes.Equal(pcs.Raw[0].Record.Raw, pcsRecord.Raw) {
		t.Fatal("native PCS evidence/grade", err)
	}
	pcs.Raw[0].Record.Raw[8] ^= 0xff
	again, _ := b.growattPCS(ctx, time.Second)
	if !bytes.Equal(again.Raw[0].Record.Raw, pcsRecord.Raw) {
		t.Fatal("aliased cache")
	}
	select {
	case receivers[managedGrowattLINKID].fail <- errors.New("synthetic receive failure"):
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	m06Wait(t, func() bool {
		s, _ := manager.Snapshot(string(managedGrowattLINKID))
		return s.ObservedState == drivermanager.ObservedFailed
	})
	if _, err = b.growattLINK(ctx, time.Second); !errors.Is(err, drivermanager.ErrUnavailable) {
		t.Fatal("failed native binding served")
	}
	if _, err = b.growattPCS(ctx, time.Second); err != nil {
		t.Fatal("sibling withdrawn", err)
	}
	old := managerSnapshotCorrelation(t, manager, managedGrowattLINKID)
	if err := manager.Stop(ctx, string(managedGrowattLINKID)); err != nil {
		t.Fatal(err)
	}
	successor := cx20NewReceiver(cx20Config(t, "synthetic1", 7))
	w := b.entry(managedGrowattLINKID).runtime.Runtime.(*growattProjectManagedRuntime)
	w.open = func(string) (growattProjectReceiver, error) { return successor, nil }
	if err := manager.Start(ctx, string(managedGrowattLINKID)); err != nil {
		t.Fatal(err)
	}
	if manager.ReportFailure(string(managedGrowattLINKID), old, drivermanager.Failure{Reason: drivermanager.Reason{Code: drivermanager.ReasonProviderUnavailable}}) {
		t.Fatal("stale failure admitted")
	}
	if _, err = b.growattLINK(ctx, time.Second); err != nil {
		t.Fatal("successor binding unavailable", err)
	}
}
func managerSnapshotCorrelation(t *testing.T, m *drivermanager.Manager, id runtimecontrol.DriverID) drivermanager.Correlation {
	t.Helper()
	s, ok := m.Snapshot(string(id))
	if !ok {
		t.Fatal("missing manager status")
	}
	return drivermanager.Correlation{Generation: s.Generation}
}
func TestCX21ExplicitContextHypothesisNegatives(t *testing.T) {
	mutations := map[string]func(*canbusreg.ReplayInput){
		"selector": func(in *canbusreg.ReplayInput) { in.Selection = canbusreg.Selection{} }, "direction": func(in *canbusreg.ReplayInput) { in.PCS.Direction = "pcs-to-bms" }, "scope": func(in *canbusreg.ReplayInput) { in.PCS.Scope = "aggregate" }, "publisher": func(in *canbusreg.ReplayInput) { in.PCS.PublisherResolved = false }, "endpoint": func(in *canbusreg.ReplayInput) { in.PCS.Endpoint = "other" }, "group": func(in *canbusreg.ReplayInput) { in.PCS.Group = "other" }, "slot": func(in *canbusreg.ReplayInput) { in.PCS.Slot = "other" },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			a := cx21Admission(t, cx21Config(t, false), false)
			s := a.selections[0]
			p, r, _ := cx18New(t, s, func(s canbusreg.Selection, e canbusreg.Evidence) canbusreg.ReplayInput {
				in := a.associate(s, e)
				mutate(&in)
				return in
			})
			if err := p.Start(1); err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := p.Stop(); err != nil {
					t.Error(err)
				}
			}()
			record := cx18Record(s, 1)
			cx18Send(t, r, record)
			got := cx18Wait(t, p, func(s growattProjectSnapshot) bool { return len(s.Providers[0].Raw) > 0 })
			if !bytes.Equal(got.Raw[0].Record.Raw, record.Raw) || len(got.Providers[0].Current) != 0 || got.Providers[0].PhysicalQualified {
				t.Fatal("rejected hypothesis lost raw or interpreted input")
			}
		})
	}
}
func TestCX21AssertionAgesAndCounts(t *testing.T) {
	for _, age := range []string{"-1ns", "0s", "1ns"} {
		t.Run("age/"+age, func(t *testing.T) {
			c := cx21Config(t, true)
			context := cx21Context(1)
			context.FieldAge = age
			c.Context = cx21JSON(t, []growattProjectContext{context})
			a, err := admitGrowattProject(c, true)
			if (err == nil) != (age != "-1ns") {
				t.Fatal(err)
			}
			if err == nil {
				in := a.associate(a.selections[0], canbusreg.Evidence{})
				base, _ := time.ParseDuration(age)
				if in.PCS.FieldAge < base {
					t.Fatal("assertion age refreshed")
				}
			}
		})
	}
	for _, vector := range []string{"cells", "sensors"} {
		maximum := 16
		if vector == "sensors" {
			maximum = 8
		}
		for _, count := range []int{-1, 0, 1, maximum, maximum + 1} {
			t.Run(fmt.Sprintf("%s/%d", vector, count), func(t *testing.T) {
				c := cx21Config(t, true)
				context := cx21Context(1)
				if vector == "cells" {
					context.Cells.Count = count
				} else {
					context.Sensors.Count = count
				}
				c.Context = cx21JSON(t, []growattProjectContext{context})
				_, err := admitGrowattProject(c, true)
				if (err == nil) != (count >= 1 && count <= maximum) {
					t.Fatal(err)
				}
			})
		}
	}
	if got := growattProjectAged(time.Duration(1<<63-2), 10); got != time.Duration(1<<63-1) {
		t.Fatal("age overflow")
	}
	// A callback returns detached scalar/array assertions; changing one value
	// cannot rewrite a sibling's independent validity or later callback.
	a := cx21Admission(t, cx21Config(t, true), true)
	s := a.selections[0]
	in := a.associate(s, canbusreg.Evidence{})
	in.CellGates[0].Valid = false
	fresh := a.associate(s, canbusreg.Evidence{})
	if !fresh.CellGates[0].Valid || !fresh.CellGates[1].Valid {
		t.Fatal("aliased independent gates")
	}
}

func TestCX21ThreeDistinctInterfaces(t *testing.T) {
	opens := can03FakeOpener(t, 0)
	cfg := cx21Combined(t)
	cfg.GrowattBMSCANConfig.Interface = growattBMSCANTestInterface.Name
	cfg.GrowattBMSCANConfig.StaleAfter = 2 * time.Second
	sidecars := prepareManagedSidecars(context.Background(), cfg, newTestSemanticHub(t), "", nil, nil)
	b := &managedSidecarBindings{sidecars: sidecars}
	var projectOpens atomic.Int32
	for _, id := range []runtimecontrol.DriverID{managedGrowattPCSID, managedGrowattLINKID} {
		c := cfg.GrowattPCSCANConfig
		link := id == managedGrowattLINKID
		if link {
			c = cfg.GrowattLINKCANConfig
		}
		a := cx21Admission(t, c, link)
		r := &cx20Receiver{id: a.selections[0].SelectionSpecValue().Interface, records: make(chan growattProjectRecord), fail: make(chan error)}
		b.entry(id).runtime.Runtime.(*growattProjectManagedRuntime).open = func(string) (growattProjectReceiver, error) { projectOpens.Add(1); return r, nil }
	}
	manager, err := drivermanager.New(drivermanager.Config{Drivers: sidecars.driverConfigs()})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	defer func() {
		if err := manager.Shutdown(ctx); err != nil {
			t.Error(err)
		}
	}()
	sidecars.bind(manager, nil)
	sources := map[runtimecontrol.SourceID]bool{}
	for _, id := range []runtimecontrol.DriverID{managedGrowattCANID, managedGrowattPCSID, managedGrowattLINKID} {
		s := b.entry(id)
		source := s.descriptor.Sources[0]
		if sources[source] {
			t.Fatal("shared source identity")
		}
		sources[source] = true
		if err := s.Start(ctx, runtimecontrol.LifecycleCorrelation{DriverID: id, Epoch: "synthetic-epoch", Generation: 1}); err != nil {
			t.Fatal(err)
		}
		snapshot, _ := manager.Snapshot(string(id))
		if snapshot.ObservedState != drivermanager.ObservedRunning {
			t.Fatal("not independently running")
		}
	}
	if opens.Load() != 1 || projectOpens.Load() != 2 {
		t.Fatal("three interfaces did not open exactly once")
	}
	for _, driver := range sidecars.driverConfigs() {
		if driver.ID == string(managedGrowattPCSID) || driver.ID == string(managedGrowattLINKID) {
			for _, capability := range driver.Capabilities {
				if capability == drivermanager.CapabilitySemanticProjection || capability == drivermanager.CapabilityWrite {
					t.Fatal("unexpected semantic/write capability")
				}
			}
		}
	}
}
func TestCX21LINKOmittedSourceAndWrongRouteRetainRaw(t *testing.T) {
	for _, source := range []uint32{1, 2} {
		for _, destination := range []uint32{243, 244, 245} {
			t.Run(fmt.Sprintf("source=%d/destination=%d", source, destination), func(t *testing.T) {
				a := cx21Admission(t, cx21Config(t, true), true)
				s := a.selections[0]
				p, err := newGrowattLINKProjectProvider(a.link, nil, a.associate)
				// Construct using a synthetic receive-only opener; nil cannot be admitted.
				if err == nil || p != nil {
					t.Fatal("nil opener admitted")
				}
				p, r, _ := cx18New(t, s, a.associate)
				if err := p.Start(1); err != nil {
					t.Fatal(err)
				}
				defer func() {
					if err := p.Stop(); err != nil {
						t.Error(err)
					}
				}()
				record := cx20Record(s.SelectionSpecValue().Interface, source, 0x1801, 1, time.Second)
				// Keep the full payload fixed while varying only the explicit route.
				frameID := uint32(0x80000000) | 0x18010000 | destination<<8 | source
				binary.NativeEndian.PutUint32(record.Raw, frameID)
				cx18Send(t, r, record)
				got := cx18Wait(t, p, func(s growattProjectSnapshot) bool {
					return len(s.Raw) > 0 && (source != 1 || len(s.Providers[0].Raw) > 0)
				})
				if !bytes.Equal(got.Raw[0].Record.Raw, record.Raw) || got.Providers[0].PhysicalQualified {
					t.Fatal("raw retention/qualification")
				}
				if source != 1 || destination != 244 {
					if len(got.Providers[0].Current) != 0 {
						t.Fatal("unselected source/route interpreted")
					}
				}
			})
		}
	}
}
func TestCX21LINKIndependentValidityRetainsSibling(t *testing.T) {
	for _, valid := range []bool{false, true} {
		t.Run(fmt.Sprint(valid), func(t *testing.T) {
			c := cx21Config(t, true)
			context := cx21Context(1)
			context.Cells.Gates[0].Valid = valid
			c.Context = cx21JSON(t, []growattProjectContext{context})
			a := cx21Admission(t, c, true)
			s := a.selections[0]
			p, r, _ := cx18New(t, s, a.associate)
			if err := p.Start(1); err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := p.Stop(); err != nil {
					t.Error(err)
				}
			}()
			record := cx20Record(s.SelectionSpecValue().Interface, 1, 0x1810, 1, time.Second)
			cx18Send(t, r, record)
			got := cx18Wait(t, p, func(s growattProjectSnapshot) bool { return len(s.Providers[0].Raw) > 0 })
			fields := got.Providers[0].Current
			if !bytes.Equal(got.Raw[0].Record.Raw, record.Raw) || fields["cells/cell_2"].Status != "accepted" {
				t.Fatal("valid sibling lost")
			}
			if (fields["cells/cell_1"].Status == "accepted") != valid {
				t.Fatal("independent validity not applied")
			}
		})
	}
}
