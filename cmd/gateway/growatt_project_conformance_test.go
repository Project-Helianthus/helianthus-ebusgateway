package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	canbus "github.com/Project-Helianthus/helianthus-canbus"
	canbusreg "github.com/Project-Helianthus/helianthus-canbusreg"
	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/drivermanager"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/growattcanclassify"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimecontrol"
)

// Synthetic candidate protocol vectors follow the public native sections named
// in the fixture. Expectations are literal arithmetic, never decoder output.
// Admission, descriptive classification and later semantic promotion are separate.
type cx22Fixture struct {
	Contracts      []string
	Classification string
	Voltages       []struct {
		Release, Payload, Meaning string
		Numerator, Denominator    int64
	}
	Cells struct {
		Payload     string
		Numerators  []int64
		Denominator int64
	}
	DTC []struct {
		Name, Project, Release, Payload, Status string
		Index                                   int64
	}
}

func cx22Load(t *testing.T) cx22Fixture {
	t.Helper()
	raw, err := os.ReadFile("testdata/growatt_project_conformance.json")
	if err != nil {
		t.Fatal(err)
	}
	var f cx22Fixture
	if err := json.Unmarshal(raw, &f); err != nil {
		t.Fatal(err)
	}
	if len(f.Contracts) != 3 || len(f.Voltages) != 2 || len(f.Cells.Numerators) != 4 || len(f.DTC) != 6 {
		t.Fatal("incomplete conformance vectors")
	}
	return f
}

// Extra mock methods are traps, not capabilities granted by either input seam.
// Every harness checks these counters after success, failure and cancellation.
type cx22Safety struct{ send, ack, probe, configure atomic.Int32 }

func (s *cx22Safety) Send()        { s.send.Add(1) }
func (s *cx22Safety) Acknowledge() { s.ack.Add(1) }
func (s *cx22Safety) Probe()       { s.probe.Add(1) }
func (s *cx22Safety) Configure()   { s.configure.Add(1) }
func (s *cx22Safety) check(t *testing.T) {
	t.Helper()
	if s.send.Load() != 0 || s.ack.Load() != 0 || s.probe.Load() != 0 || s.configure.Load() != 0 {
		t.Fatal("receive-only violation")
	}
}

type cx22Receiver struct {
	cx22Safety
	id            canbus.InterfaceIdentity
	records       chan growattProjectRecord
	fail          chan error
	reads, closes atomic.Int32
	closeErr      error
}

func (r *cx22Receiver) Identity() canbus.InterfaceIdentity { return r.id }
func (r *cx22Receiver) Receive(ctx context.Context) (growattProjectRecord, error) {
	r.reads.Add(1)
	select {
	case v := <-r.records:
		return v, nil
	case err := <-r.fail:
		return growattProjectRecord{}, err
	case <-ctx.Done():
		return growattProjectRecord{}, ctx.Err()
	}
}
func (r *cx22Receiver) Close() error { r.closes.Add(1); return r.closeErr }
func (r *cx22Receiver) put(t *testing.T, v growattProjectRecord) {
	t.Helper()
	select {
	case r.records <- v:
	case <-time.After(time.Second):
		t.Fatal("fake input timed out")
	}
}

type cx22V104Receiver struct {
	*growattBMSCANFakeReceiver
	cx22Safety
	reads atomic.Int32
}

func (r *cx22V104Receiver) Receive(ctx context.Context) (growattBMSCANRecord, error) {
	r.reads.Add(1)
	return r.growattBMSCANFakeReceiver.Receive(ctx)
}

type cx22Harness struct {
	b         *managedSidecarBindings
	manager   *drivermanager.Manager
	receivers map[runtimecontrol.DriverID]*cx22Receiver
	wrappers  map[runtimecontrol.DriverID]*growattProjectManagedRuntime
	v104      *cx22V104Receiver
}

func cx22Config(t *testing.T, v104 bool) ebusgateway.Config {
	t.Helper()
	pcs, link := cx21Config(t, false), cx21Config(t, true)
	pcs.Interface, link.Interface = "synthetic-pcs", "synthetic-link"
	args := append(cx21Flags(t, pcs, false), cx21Flags(t, link, true)...)
	if v104 {
		args = append(args, "-growatt-bms-can-enabled", "-growatt-bms-can-interface=synthetic-v104", "-growatt-bms-can-listen-only-confirmed", "-growatt-bms-can-stale-after=1s")
	}
	return can03Parse(t, args...)
}
func cx22New(t *testing.T, cfg ebusgateway.Config) *cx22Harness {
	t.Helper()
	h := &cx22Harness{receivers: map[runtimecontrol.DriverID]*cx22Receiver{}, wrappers: map[runtimecontrol.DriverID]*growattProjectManagedRuntime{}}
	original := gatewayManagedSidecarDescriptorsFor
	t.Cleanup(func() { gatewayManagedSidecarDescriptorsFor = original })
	if cfg.GrowattBMSCANConfig.Enabled {
		h.v104 = &cx22V104Receiver{growattBMSCANFakeReceiver: newGrowattBMSCANFakeReceiver(growattBMSCANInterface{Name: cfg.GrowattBMSCANConfig.Interface, Index: 9})}
		gatewayManagedSidecarDescriptorsFor = func(ds []runtimecontrol.Descriptor) []runtimecontrol.Descriptor {
			for i := range ds {
				if ds[i].DriverID == managedGrowattCANID {
					adapter := ds[i].Adapter
					ds[i].Adapter = runtimecontrol.NewAdapter(func(ctx context.Context, c runtimecontrol.Construction) (*growattBMSCANManagedDriver, error) {
						native, err := adapter.Construct(ctx, c)
						if err != nil {
							return nil, err
						}
						d := native.(*growattBMSCANManagedDriver)
						d.open = func(name string) (growattBMSCANReceiver, error) {
							if name != h.v104.Identity().Name {
								return nil, errors.New("wrong V104 interface")
							}
							return h.v104, nil
						}
						return d, nil
					})
				}
			}
			return ds
		}
	}
	sidecars := prepareManagedSidecars(context.Background(), cfg, newTestSemanticHub(t), "", nil, nil)
	h.b = &managedSidecarBindings{sidecars: sidecars}
	for _, id := range []runtimecontrol.DriverID{managedGrowattPCSID, managedGrowattLINKID} {
		s := h.b.entry(id)
		if s == nil || s.withheld != nil || s.runtime == nil {
			t.Fatalf("composition %s: %+v", id, s)
		}
		w := s.runtime.Runtime.(*growattProjectManagedRuntime)
		h.wrappers[id] = w
		c := cfg.GrowattPCSCANConfig
		link := id == managedGrowattLINKID
		if link {
			c = cfg.GrowattLINKCANConfig
		}
		a := cx21Admission(t, c, link)
		r := &cx22Receiver{id: a.selections[0].CX01Spec().Interface, records: make(chan growattProjectRecord), fail: make(chan error)}
		h.receivers[id] = r
		w.open = func(name string) (growattProjectReceiver, error) {
			if name != r.id.Name() {
				return nil, errors.New("wrong selected interface")
			}
			return r, nil
		}
	}
	var err error
	h.manager, err = drivermanager.New(drivermanager.Config{Drivers: sidecars.driverConfigs()})
	if err != nil {
		t.Fatal(err)
	}
	sidecars.bind(h.manager, nil)
	// Production binding: failures withdraw the manager state through the
	// same callback the gateway installs. A nil service keeps the harness
	// free of a control loop; the manager state is asserted by the tests.
	sidecars.bindNativeObservations(nil)
	for id, w := range h.wrappers {
		if w.reportFailure == nil {
			t.Fatalf("production failure binding missing for %s", id)
		}
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		// A deliberate close failure remains a reported failure, not a successful shutdown.
		err := h.manager.Shutdown(ctx)
		quarantine := false
		for _, r := range h.receivers {
			r.check(t)
			quarantine = quarantine || r.closeErr != nil
		}
		if err != nil && !quarantine {
			t.Error(err)
		}
		if h.v104 != nil {
			h.v104.check(t)
		}
	})
	for _, driver := range sidecars.driverConfigs() {
		if isGrowattCANDescriptor(runtimecontrol.DriverID(driver.ID)) {
			for _, capability := range driver.Capabilities {
				if capability == drivermanager.CapabilityWrite {
					t.Fatal("operation capability exposed")
				}
			}
			if driver.ID != string(managedGrowattCANID) {
				for _, capability := range driver.Capabilities {
					if capability == drivermanager.CapabilitySemanticProjection {
						t.Fatal("typed promotion claimed")
					}
				}
			}
		}
	}
	return h
}
func (h *cx22Harness) start(t *testing.T, id runtimecontrol.DriverID) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := h.b.entry(id).Start(ctx, runtimecontrol.LifecycleCorrelation{DriverID: id, Epoch: "synthetic-lifecycle", Generation: 1}); err != nil {
		t.Fatal(err)
	}
}
func (h *cx22Harness) project(id runtimecontrol.DriverID, now time.Duration) growattProjectSnapshot {
	if id == managedGrowattPCSID {
		return h.wrappers[id].Runtime.(*growattPCSManagedDriver).Snapshot(now)
	}
	return h.wrappers[id].Runtime.(*growattLINKManagedDriver).Snapshot(now).Native
}
func cx22Wait(t *testing.T, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if f() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("conformance condition timed out")
}
func (h *cx22Harness) send(t *testing.T, id runtimecontrol.DriverID, record growattProjectRecord) growattProjectSnapshot {
	t.Helper()
	n := len(h.project(id, record.Monotonic).Raw)
	h.receivers[id].put(t, record)
	cx22Wait(t, func() bool { return len(h.project(id, record.Monotonic).Raw) > n })
	// Association/native publication follows outer raw retention asynchronously.
	cx22Wait(t, func() bool {
		s := h.project(id, record.Monotonic)
		for _, p := range s.Providers {
			for _, raw := range p.Raw {
				if raw.Input.Evidence.Sequence == record.Sequence {
					return true
				}
			}
		}
		return len(s.Providers) == 0 || s.Raw[len(s.Raw)-1].Status == "rejected"
	})
	return h.project(id, record.Monotonic)
}
func cx22Record(t *testing.T, id canbus.InterfaceIdentity, identifier uint32, sequence uint64, mono time.Duration, payload string) growattProjectRecord {
	t.Helper()
	p, err := hex.DecodeString(payload)
	if err != nil || len(p) != 8 {
		t.Fatal("bad synthetic payload", err)
	}
	raw := make([]byte, 16)
	binary.NativeEndian.PutUint32(raw, identifier)
	raw[4] = 8
	copy(raw[8:], p)
	return growattProjectRecord{Interface: id, Raw: raw, Sequence: sequence, Monotonic: mono}
}
func cx22Raw(t *testing.T, s growattProjectSnapshot, r growattProjectRecord) {
	t.Helper()
	found := false
	for _, raw := range s.Raw {
		if raw.Record.Sequence == r.Sequence && bytes.Equal(raw.Record.Raw, r.Raw) && raw.Record.Interface == r.Interface && raw.Record.Monotonic == r.Monotonic {
			found = true
		}
	}
	if !found {
		t.Fatal("exact transport record not retained")
	}
	for _, p := range s.Providers {
		if p.PhysicalQualified {
			t.Fatal("physical qualification invented")
		}
		for _, f := range p.Current {
			if f.PhysicalQualified {
				t.Fatal("qualified field")
			}
		}
	}
}

func TestCX22ClassificationDoesNotAdmit(t *testing.T) {
	fixture := cx22Load(t)
	for _, name := range []string{"complete-overlap", "missing-311", "missing-312", "missing-313", "other-interface", "wrong-ide", "wrong-dlc"} {
		t.Run(name, func(t *testing.T) {
			input := fixture.Classification
			switch name {
			case "missing-311", "missing-312", "missing-313":
				lines := strings.Split(input, "\n")
				input = ""
				for _, line := range lines {
					if !strings.Contains(line, strings.TrimPrefix(name, "missing-")+"#") && line != "" {
						input += line + "\n"
					}
				}
			case "other-interface":
				input = strings.Replace(input, "synthetic-pcs 313", "synthetic-other 313", 1)
			case "wrong-ide":
				input = strings.Replace(input, "313#", "00000313#", 1)
			case "wrong-dlc":
				input = strings.Replace(input, "313#0000000000000000", "313#00000000000000", 1)
			}
			cfg := can03Parse(t)
			before := cfg
			result, err := growattcanclassify.Classify(strings.NewReader(input), growattcanclassify.Options{})
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(cfg, before) || cfg.GrowattPCSCANConfig.Enabled || cfg.GrowattLINKCANConfig.Enabled || cfg.GrowattBMSCANConfig.Enabled || result.RuntimeAdmission || result.PhysicalQualified {
				t.Fatal("classification selected runtime")
			}
			wantFrames := 0
			for _, line := range strings.Split(input, "\n") {
				if strings.TrimSpace(line) != "" {
					wantFrames++
				}
			}
			if wantFrames == 0 || len(result.Frames) != wantFrames {
				t.Fatalf("classification returned %d frames, want %d", len(result.Frames), wantFrames)
			}
			for _, f := range result.Frames {
				if !strings.Contains(input, f.Raw) || f.RawHex == "" || f.PhysicalQualified || f.RuntimeAdmission {
					t.Fatal("raw or qualification")
				}
				if name == "complete-overlap" {
					if len(f.Candidates) != 2 || f.Candidates[0].Family != "V1.04" || f.Candidates[1].Family != "PCS-project-code" {
						t.Fatal(f.Candidates)
					}
				} else if len(f.Candidates) != 0 {
					t.Fatal("incomplete tuple interpreted")
				}
				for _, c := range f.Candidates {
					if c.Grade != "Hypothesis" {
						t.Fatal(c)
					}
				}
			}
		})
	}
}

func TestCX22ComposedThreeFamiliesAndTwoProviderIsolation(t *testing.T) {
	for _, triple := range []bool{false, true} {
		t.Run(fmt.Sprintf("v104=%v", triple), func(t *testing.T) {
			f := cx22Load(t)
			h := cx22New(t, cx22Config(t, triple))
			h.start(t, managedGrowattPCSID)
			h.start(t, managedGrowattLINKID)
			if triple {
				h.start(t, managedGrowattCANID)
			}
			pcsRecord := cx22Record(t, h.receivers[managedGrowattPCSID].id, 0x313, 1, time.Second, f.Voltages[1].Payload)
			pcs := h.send(t, managedGrowattPCSID, pcsRecord)
			field := pcs.Providers[0].Current["core/voltage"]
			if field.Numerator != 5310 || field.Denominator != 100 || field.Meaning != "output-port voltage" {
				t.Fatal(field)
			}
			linkRecord := cx22Record(t, h.receivers[managedGrowattLINKID].id, 0x9810f401, 1, time.Second, f.Cells.Payload)
			link := h.send(t, managedGrowattLINKID, linkRecord)
			for n, want := range f.Cells.Numerators {
				v := link.Providers[0].Current[fmt.Sprintf("cells/cell_%d", n+1)]
				if v.Numerator != want || v.Denominator != f.Cells.Denominator || v.Status != "accepted" || v.Unit != "mV" {
					t.Fatal(v)
				}
				if v.Epoch != 1 || v.Endpoint != "synthetic" || v.Group != "group" || v.Slot != "slot" || v.Evidence.Interface != linkRecord.Interface || v.Evidence.Sequence != 1 || v.Evidence.Monotonic != time.Second || v.Raw != v.Evidence.Frame.Bytes() || v.Grade == "" || v.Age < 0 || v.PhysicalQualified {
					t.Fatal("native evidence lost", v)
				}
				if !v.Evidence.Frame.ID().Extended() || v.Evidence.Frame.ID().Value() != 0x1810f401 {
					t.Fatal("LINK route lost")
				}
			}
			view, err := h.b.growattLINK(context.Background(), time.Second)
			if err != nil || len(view.Cells) != 1 || view.Cells[0].CompleteCoverage || len(view.Cells[0].ReceivedIndices) != 4 || view.Cells[0].ConfiguredCount != 8 {
				t.Fatal("partial coverage", view.Cells, err)
			}
			cx22Raw(t, pcs, pcsRecord)
			cx22Raw(t, link, linkRecord)
			if triple {
				records := []growattBMSCANRecord{growattBMSCANLimits(), growattBMSCANStatus(), growattBMSCANMeasurements()}
				for i := range records {
					records[i].Interface = h.v104.Identity()
				}
				h.v104.feed(t, records...)
				d := h.b.entry(managedGrowattCANID).runtime.Runtime.(*growattBMSCANManagedDriver).driver
				s, err := d.GrowattBMSV104Snapshot(context.Background())
				if err != nil || !s.Admitted || s.OutboundAllowed || s.Measurements.VoltageCentivolts != 5120 || len(s.RawEvidence) != 3 {
					t.Fatal("V104 native tuple", s, err)
				}
				for _, e := range s.RawEvidence {
					if e.Interface != h.v104.Identity().Name {
						t.Fatal("V104 cross-interface evidence")
					}
				}
			}
			// Cached reads cannot cause another Receive entry, including manager status.
			for _, r := range h.receivers {
				cx22Wait(t, func() bool { return r.reads.Load() >= 2 })
			}
			reads := map[runtimecontrol.DriverID]int32{}
			for id, r := range h.receivers {
				reads[id] = r.reads.Load()
			}
			var vreads int32
			if triple {
				vreads = h.v104.reads.Load()
			}
			for n := 0; n < 20; n++ {
				if _, err := h.b.growattPCS(context.Background(), time.Second); err != nil {
					t.Fatal(err)
				}
				if _, err := h.b.growattLINK(context.Background(), time.Second); err != nil {
					t.Fatal(err)
				}
				h.manager.Snapshot(string(managedGrowattPCSID))
				h.manager.Snapshot(string(managedGrowattLINKID))
			}
			for id, r := range h.receivers {
				if reads[id] != r.reads.Load() {
					t.Fatal("cache performed input read")
				}
			}
			if triple && vreads != h.v104.reads.Load() {
				t.Fatal("status read V104 input")
			}
			old := managerSnapshotCorrelation(t, h.manager, managedGrowattLINKID)
			if err := h.manager.Stop(context.Background(), string(managedGrowattLINKID)); err != nil {
				t.Fatal(err)
			}
			if _, err := h.b.growattLINK(context.Background(), time.Second); !errors.Is(err, drivermanager.ErrUnavailable) {
				t.Fatal("stopped binding served", err)
			}
			h.receivers[managedGrowattLINKID].check(t)
			if h.receivers[managedGrowattLINKID].closes.Load() != 1 {
				t.Fatal("retired receiver not closed")
			}
			historical := h.project(managedGrowattLINKID, time.Second)
			if len(historical.Providers[0].Current) != 0 || len(historical.Providers[0].LastGood) == 0 || historical.Providers[0].State != "stopped" {
				t.Fatal("history not marked stopped")
			}
			cx22Raw(t, historical, linkRecord)
			next := &cx22Receiver{id: h.receivers[managedGrowattLINKID].id, records: make(chan growattProjectRecord), fail: make(chan error)}
			h.receivers[managedGrowattLINKID] = next
			h.wrappers[managedGrowattLINKID].open = func(string) (growattProjectReceiver, error) { return next, nil }
			if err := h.manager.Start(context.Background(), string(managedGrowattLINKID)); err != nil {
				t.Fatal(err)
			}
			if h.manager.ReportFailure(string(managedGrowattLINKID), old, drivermanager.Failure{Reason: drivermanager.Reason{Code: drivermanager.ReasonProviderUnavailable}}) {
				t.Fatal("retired failure poisoned successor")
			}
			successor := h.project(managedGrowattLINKID, time.Second).Providers[0]
			if successor.Generation != 2 || len(successor.Current) != 0 || successor.State != "awaiting" {
				t.Fatal("generation reused", successor)
			}
			if pcsNow, err := h.b.growattPCS(context.Background(), time.Second); err != nil || pcsNow.Providers[0].Generation != 1 || pcsNow.Providers[0].Current["core/voltage"] != field {
				t.Fatal("LINK lifecycle mutated PCS", err)
			}
		})
	}
}

func TestCX22VoltageAndMemberDictionary(t *testing.T) {
	f := cx22Load(t)
	for _, v := range f.Voltages {
		t.Run("voltage/"+v.Release, func(t *testing.T) {
			cfg := cx22Config(t, false)
			cfg.GrowattPCSCANConfig.Release = v.Release
			h := cx22New(t, cfg)
			h.start(t, managedGrowattPCSID)
			r := cx22Record(t, h.receivers[managedGrowattPCSID].id, 0x313, 1, time.Second, v.Payload)
			s := h.send(t, managedGrowattPCSID, r)
			field := s.Providers[0].Current["core/voltage"]
			if field.Numerator != v.Numerator || field.Denominator != v.Denominator || field.Meaning != v.Meaning || field.Status != "accepted" || field.Unit != "V" || field.Grade != "Proven" {
				t.Fatal(field)
			}
			cx22Raw(t, s, r)
		})
	}
	for _, v := range f.DTC {
		t.Run("dtc/"+v.Name, func(t *testing.T) {
			cfg := cx22Config(t, false)
			cfg.GrowattPCSCANConfig.ProjectCode = v.Project
			cfg.GrowattPCSCANConfig.Release = v.Release
			if v.Project == "QAAa" {
				cfg.GrowattPCSCANConfig.NativeContract = "release11"
			}
			h := cx22New(t, cfg)
			d := h.wrappers[managedGrowattPCSID].Runtime.(*growattPCSManagedDriver)
			original := d.provider.associate
			d.provider.associate = func(sel canbusreg.CXSelection, e canbusreg.Evidence) canbusreg.CX16ReplayInput {
				in := original(sel, e)
				member := sel
				in.PCS.Member = &member
				return in
			}
			h.start(t, managedGrowattPCSID)
			r := cx22Record(t, h.receivers[managedGrowattPCSID].id, 0x333, 1, time.Second, v.Payload)
			s := h.send(t, managedGrowattPCSID, r)
			p := s.Providers[0]
			word := p.Current["slots/dtc_a"]
			index, has := p.Current["slots/dtc_a_index"]
			if word.Status != v.Status {
				t.Fatalf("word=%+v", word)
			}
			if v.Status == "accepted" {
				if !has || index.Numerator != v.Index || index.Status != "accepted" {
					t.Fatal(index)
				}
			} else if v.Status == "empty" {
				if !has || index.Status != "empty" {
					t.Fatal("zero lost empty distinction", index)
				}
			} else if has {
				t.Fatal("unknown word acquired index", index)
			}
			cx22Raw(t, s, r)
		})
	}
}

// Every transport identity component is varied alone, with identical payload.
// This catches removal of interface name/index, IDE, route source/destination,
// DLC/raw-DLC, sequence or monotonic admission checks respectively.
func TestCX22TransportNegativeVectors(t *testing.T) {
	for _, name := range []string{"interface-name", "interface-index", "standard-id", "wrong-destination", "unknown-source", "dlc7", "raw-dlc9", "zero-sequence", "negative-time"} {
		t.Run(name, func(t *testing.T) {
			h := cx22New(t, cx22Config(t, false))
			h.start(t, managedGrowattLINKID)
			r := cx22Record(t, h.receivers[managedGrowattLINKID].id, 0x9810f401, 1, time.Second, cx22Load(t).Cells.Payload)
			switch name {
			case "interface-name":
				r.Interface, _ = canbus.NewInterfaceIdentity("synthetic-other", 7)
			case "interface-index":
				r.Interface, _ = canbus.NewInterfaceIdentity("synthetic-link", 8)
			case "standard-id":
				binary.NativeEndian.PutUint32(r.Raw, 0x313)
			case "wrong-destination":
				binary.NativeEndian.PutUint32(r.Raw, 0x9810f301)
			case "unknown-source":
				binary.NativeEndian.PutUint32(r.Raw, 0x9810f402)
			case "dlc7":
				r.Raw[4] = 7
			case "raw-dlc9":
				r.Raw[7] = 9
			case "zero-sequence":
				r.Sequence = 0
			case "negative-time":
				r.Monotonic = -1
			}
			h.receivers[managedGrowattLINKID].put(t, r)
			// The receive loop only re-enters Receive after outer retention and filtering
			// of this record. Rows that pass the outer filter are also handed to the
			// native provider asynchronously, so wait for its retained diagnostic of
			// this exact record before asserting that nothing was interpreted.
			cx22Wait(t, func() bool { return h.receivers[managedGrowattLINKID].reads.Load() >= 2 })
			if name == "wrong-destination" {
				cx22Wait(t, func() bool {
					for _, p := range h.project(managedGrowattLINKID, time.Second).Providers {
						for _, raw := range p.Raw {
							if raw.Input.Evidence.Sequence == r.Sequence {
								return true
							}
						}
					}
					return false
				})
			}
			s := h.project(managedGrowattLINKID, time.Second)
			cx22Raw(t, s, r)
			if len(s.Providers[0].Current) != 0 || s.Providers[0].Promotable {
				t.Fatal("rejected input interpreted")
			}
		})
	}
}

// A follow-up changes exactly one immutable selector component. Each row catches
// removal of that component from the native equality/conflict fence. This list
// covers every CX01SelectionSpec field, including otherwise unused route fields.
func TestCX22SelectorReuseInvalidatesDependentFields(t *testing.T) {
	mutations := map[string]func(*canbusreg.CX01SelectionSpec){
		"interface-name": func(c *canbusreg.CX01SelectionSpec) {
			c.Interface, _ = canbus.NewInterfaceIdentity("synthetic-other", 7)
		},
		"interface-index": func(c *canbusreg.CX01SelectionSpec) {
			c.Interface, _ = canbus.NewInterfaceIdentity("synthetic-link", 8)
		},
		"flavor":       func(c *canbusreg.CX01SelectionSpec) { c.Flavor = "pcs-standard" },
		"pcs-flavor":   func(c *canbusreg.CX01SelectionSpec) { c.PCSFlavor = "pcs-parallel" },
		"project-code": func(c *canbusreg.CX01SelectionSpec) { c.ProjectCode = "QAAa" },
		"release":      func(c *canbusreg.CX01SelectionSpec) { c.Release = "0010" },
		"choice":       func(c *canbusreg.CX01SelectionSpec) { c.Choice = "legacy19" },
		"direction":    func(c *canbusreg.CX01SelectionSpec) { c.Direction = "coordinator-to-module" },
		"scope":        func(c *canbusreg.CX01SelectionSpec) { c.Scope = "aggregate" },
		"bank":         func(c *canbusreg.CX01SelectionSpec) { c.Bank = "parallel" },
		"endpoint":     func(c *canbusreg.CX01SelectionSpec) { c.Endpoint = "other" },
		"group":        func(c *canbusreg.CX01SelectionSpec) { c.Group = "other" },
		"slot":         func(c *canbusreg.CX01SelectionSpec) { c.Slot = "other" },
		"route-set":    func(c *canbusreg.CX01SelectionSpec) { c.RouteSet = false },
		"destination":  func(c *canbusreg.CX01SelectionSpec) { c.Destination = 245 },
		"source":       func(c *canbusreg.CX01SelectionSpec) { c.Source = 2 },
		"epoch":        func(c *canbusreg.CX01SelectionSpec) { c.Epoch = 2 },
	}
	// Guard the completeness claim if the public selector gains another field.
	if reflect.TypeOf(canbusreg.CX01SelectionSpec{}).NumField() != 16 {
		t.Fatal("update selector mutation coverage")
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			h := cx22New(t, cx22Config(t, false))
			h.start(t, managedGrowattLINKID)
			r := cx22Record(t, h.receivers[managedGrowattLINKID].id, 0x9810f401, 1, time.Second, cx22Load(t).Cells.Payload)
			s := h.send(t, managedGrowattLINKID, r)
			if s.Providers[0].Current["cells/cell_1"].Status != "accepted" {
				t.Fatal("positive control missing")
			}
			d := h.wrappers[managedGrowattLINKID].Runtime.(*growattLINKManagedDriver)
			// Change explicit association only while reception is parked. Production
			// composition and its factory have already supplied the original selector.
			original := d.provider.associate
			d.provider.associate = func(sel canbusreg.CXSelection, e canbusreg.Evidence) canbusreg.CX16ReplayInput {
				in := original(sel, e)
				spec := sel.CX01Spec()
				mutate(&spec)
				in.Selection, _ = canbusreg.NewCXSelection(spec)
				return in
			}
			r.Sequence = 2
			s = h.send(t, managedGrowattLINKID, r)
			p := s.Providers[0]
			if p.State != "conflict" || len(p.Current) != 0 || p.Promotable || p.LastGood["cells/cell_1"].Evidence.Sequence != 1 {
				t.Fatal("identity reuse did not fence history", p.State)
			}
			cx22Raw(t, s, r)
		})
	}
}

func TestCX22IndependentValidityAndCountBoundaries(t *testing.T) {
	for _, vector := range []string{"cells", "sensors"} {
		maximum := 16
		if vector == "sensors" {
			maximum = 8
		}
		counts := []int{0, 1, maximum, maximum + 1}
		if vector == "sensors" {
			counts = []int{0, 1, 4, 5, 8, 9}
		}
		for _, count := range counts {
			t.Run(fmt.Sprintf("%s/count=%d", vector, count), func(t *testing.T) {
				cfg := cx22Config(t, false)
				c := cx21Context(1)
				if vector == "cells" {
					c.Cells.Count = count
				} else {
					c.Sensors.Count = count
				}
				cfg.GrowattLINKCANConfig.Context = cx21JSON(t, []growattProjectContext{c})
				if count == 0 || count > maximum {
					if _, err := admitGrowattProject(cfg.GrowattLINKCANConfig, true); err == nil {
						t.Fatal("invalid count accepted")
					}
					return
				}
				h := cx22New(t, cfg)
				h.start(t, managedGrowattLINKID)
				id := uint32(0x9810f401)
				key := "cells/cell_1"
				if vector == "sensors" {
					id = 0x9830f401
					key = "sensors/sensor_1"
				}
				r := cx22Record(t, h.receivers[managedGrowattLINKID].id, id, 1, time.Second, cx22Load(t).Cells.Payload)
				s := h.send(t, managedGrowattLINKID, r)
				want := "accepted"
				if vector == "sensors" && count > 4 {
					want = "absent"
				}
				if s.Providers[0].Current[key].Status != want {
					t.Fatal(s.Providers[0].Current[key])
				}
				cx22Raw(t, s, r)
			})
		}
	}
	for _, name := range []string{"invalid", "absent", "unsupported", "stale", "conflict", "unconfigured", "count-invalid", "age-negative", "age-zero", "age-positive"} {
		t.Run(name, func(t *testing.T) {
			cfg := cx22Config(t, false)
			c := cx21Context(1)
			switch name {
			case "invalid":
				c.Cells.Gates[0].Valid = false
			case "unconfigured":
				c.Cells.Gates[0].Configured = false
			case "count-invalid":
				c.Cells.CountValid = false
			case "age-negative":
				c.Cells.Gates[0].Age = "-1ns"
			case "age-zero":
				c.Cells.Gates[0].Age = "0s"
			case "age-positive":
				c.Cells.Gates[0].Age = "1ns"
			default:
				c.Cells.Gates[0].Status = name
			}
			cfg.GrowattLINKCANConfig.Context = cx21JSON(t, []growattProjectContext{c})
			if name == "age-negative" {
				if _, err := admitGrowattProject(cfg.GrowattLINKCANConfig, true); err == nil {
					t.Fatal("negative age admitted")
				}
				return
			}
			h := cx22New(t, cfg)
			h.start(t, managedGrowattLINKID)
			r := cx22Record(t, h.receivers[managedGrowattLINKID].id, 0x9810f401, 1, time.Second, cx22Load(t).Cells.Payload)
			s := h.send(t, managedGrowattLINKID, r)
			p := s.Providers[0]
			want := name
			switch name {
			case "age-zero", "age-positive":
				want = "accepted"
			case "count-invalid":
				want = "absent"
			case "unconfigured":
				want = "unconfigured"
			}
			if p.Current["cells/cell_1"].Status != want {
				t.Fatal(name, p.Current["cells/cell_1"])
			}
			if name != "count-invalid" && p.Current["cells/cell_2"].Status != "accepted" {
				t.Fatal("rejected field erased valid sibling")
			}
			cx22Raw(t, s, r)
		})
	}
}

func TestCX22FieldAgeIsIndependentOfLatestTraffic(t *testing.T) {
	h := cx22New(t, cx22Config(t, false))
	h.start(t, managedGrowattPCSID)
	h.start(t, managedGrowattLINKID)
	f := cx22Load(t)
	ld := h.wrappers[managedGrowattLINKID].Runtime.(*growattLINKManagedDriver)
	originalLINK := ld.provider.associate
	ld.provider.associate = func(s canbusreg.CXSelection, e canbusreg.Evidence) canbusreg.CX16ReplayInput {
		in := originalLINK(s, e)
		in.PCS.FieldAge = 0
		for n := range in.CellGates {
			in.CellGates[n].Age = 0
		}
		return in
	}
	r := cx22Record(t, h.receivers[managedGrowattLINKID].id, 0x9810f401, 1, time.Second, f.Cells.Payload)
	h.send(t, managedGrowattLINKID, r)
	// An independent channel update cannot refresh the first four cells.
	r2 := cx22Record(t, r.Interface, 0x9811f401, 2, 2*time.Second, f.Cells.Payload)
	h.send(t, managedGrowattLINKID, r2)
	for _, now := range []time.Duration{2*time.Second - 1, 2 * time.Second, 2*time.Second + 1} {
		s := h.project(managedGrowattLINKID, now)
		a, b := s.Providers[0].Current["cells/cell_1"], s.Providers[0].Current["cells/cell_5"]
		want := "accepted"
		if now > 2*time.Second {
			want = "stale"
		}
		if a.Status != want || (now >= 2*time.Second && b.Status != "accepted") || a.Evidence.Sequence != 1 || b.Evidence.Sequence != 2 {
			t.Fatal("channel freshness overwritten", now, a.Status, b.Status)
		}
		cx22Raw(t, s, r)
		cx22Raw(t, s, r2)
	}
	// Per-field ages must survive a new frame and keep their own boundary.
	d := h.wrappers[managedGrowattPCSID].Runtime.(*growattPCSManagedDriver)
	original := d.provider.associate
	d.provider.associate = func(s canbusreg.CXSelection, e canbusreg.Evidence) canbusreg.CX16ReplayInput {
		in := original(s, e)
		in.PCS.FieldAge = 0
		in.PCS.FieldAges = map[string]time.Duration{"voltage": 0, "current": time.Second}
		return in
	}
	p := cx22Record(t, h.receivers[managedGrowattPCSID].id, 0x313, 1, time.Second, f.Voltages[1].Payload)
	h.send(t, managedGrowattPCSID, p)
	for _, now := range []time.Duration{time.Second, time.Second + 1, 2 * time.Second, 2*time.Second + 1} {
		s := h.project(managedGrowattPCSID, now).Providers[0]
		va, ca := "accepted", "accepted"
		if now > time.Second {
			ca = "stale"
		}
		if now > 2*time.Second {
			va = "stale"
		}
		if s.Current["core/voltage"].Status != va || s.Current["core/current"].Status != ca {
			t.Fatal("per-field ages collapsed", now)
		}
	}
}

func TestCX22SourceReuseAndIncompleteIdentity(t *testing.T) {
	for _, name := range []string{"same-source-conflict", "incomplete-identity", "same-address-other-interface"} {
		t.Run(name, func(t *testing.T) {
			cfg := cx22Config(t, false)
			h := cx22New(t, cfg)
			h.start(t, managedGrowattLINKID)
			r := cx22Record(t, h.receivers[managedGrowattLINKID].id, 0x9810f401, 1, time.Second, cx22Load(t).Cells.Payload)
			h.send(t, managedGrowattLINKID, r)
			// Project identity is exact-case. Partial bytes and other interfaces cannot
			// supply a reporting identity for the configured source.
			identity := cx22Record(t, r.Interface, 0x9802f401, 2, time.Second, "5141414130303131")
			h.send(t, managedGrowattLINKID, identity)
			switch name {
			case "same-source-conflict":
				identity.Sequence = 3
				copy(identity.Raw[8:12], "QAAa")
				s := h.send(t, managedGrowattLINKID, identity)
				p := s.Providers[0]
				if p.State != "conflict" || len(p.Current) != 0 || p.Promotable || len(p.LastGood) == 0 {
					t.Fatal("source reuse admitted", p.State)
				}
				cx22Raw(t, s, identity)
			case "incomplete-identity":
				// First serial fragment is retained as a fragment; never a complete serial.
				partial := cx22Record(t, r.Interface, 0x98fbf401, 3, time.Second, "0f41424344454647")
				s := h.send(t, managedGrowattLINKID, partial)
				cx22Raw(t, s, partial)
				for key, field := range s.Providers[0].Current {
					if strings.Contains(key, "serial") && field.Status == "accepted" {
						t.Fatal("incomplete identity promoted", key)
					}
				}
			case "same-address-other-interface":
				foreign := identity
				foreign.Sequence = 3
				foreign.Interface, _ = canbus.NewInterfaceIdentity("synthetic-other", 7)
				copy(foreign.Raw[8:12], "QAAa")
				h.receivers[managedGrowattLINKID].put(t, foreign)
				cx22Wait(t, func() bool { return len(h.project(managedGrowattLINKID, time.Second).Raw) == 3 })
				s := h.project(managedGrowattLINKID, time.Second)
				cx22Raw(t, s, foreign)
				if s.Providers[0].State == "conflict" || s.Providers[0].Current["cells/cell_1"].Evidence.Interface != r.Interface {
					t.Fatal("same address crossed interfaces")
				}
			}
		})
	}
}

func TestCX22NativeContextDoesNotInventTopology(t *testing.T) {
	// Each changed assertion is independent of the unchanged transport payload.
	// Endpoint/group/slot/epoch/direction/scope/bank equality checks and explicit
	// publisher/validity/topology gates must remain separate native decisions.
	mutations := map[string]func(*canbusreg.CX16ReplayInput){
		"endpoint":    func(in *canbusreg.CX16ReplayInput) { in.PCS.Endpoint = "other" },
		"group":       func(in *canbusreg.CX16ReplayInput) { in.PCS.Group = "other" },
		"slot":        func(in *canbusreg.CX16ReplayInput) { in.PCS.Slot = "other" },
		"epoch":       func(in *canbusreg.CX16ReplayInput) { in.PCS.Epoch++ },
		"direction":   func(in *canbusreg.CX16ReplayInput) { in.PCS.Direction = "pcs-to-bms" },
		"scope":       func(in *canbusreg.CX16ReplayInput) { in.PCS.Scope = "aggregate" },
		"bank":        func(in *canbusreg.CX16ReplayInput) { in.PCS.Bank = "parallel" },
		"publisher":   func(in *canbusreg.CX16ReplayInput) { in.PCS.PublisherResolved = false },
		"measurement": func(in *canbusreg.CX16ReplayInput) { in.PCS.MeasurementValid = false },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			h := cx22New(t, cx22Config(t, false))
			d := h.wrappers[managedGrowattPCSID].Runtime.(*growattPCSManagedDriver)
			original := d.provider.associate
			d.provider.associate = func(s canbusreg.CXSelection, e canbusreg.Evidence) canbusreg.CX16ReplayInput {
				in := original(s, e)
				mutate(&in)
				return in
			}
			h.start(t, managedGrowattPCSID)
			r := cx22Record(t, h.receivers[managedGrowattPCSID].id, 0x313, 1, time.Second, cx22Load(t).Voltages[1].Payload)
			s := h.send(t, managedGrowattPCSID, r)
			cx22Raw(t, s, r)
			if s.Providers[0].Current["core/voltage"].Status == "accepted" {
				t.Fatal("withheld native context interpreted")
			}
		})
	}
}

func TestCX22LossWithdrawsAndQuarantines(t *testing.T) {
	for _, id := range []runtimecontrol.DriverID{managedGrowattPCSID, managedGrowattLINKID} {
		for _, closeFailure := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/close-failure=%v", id, closeFailure), func(t *testing.T) {
				h := cx22New(t, cx22Config(t, false))
				h.start(t, managedGrowattPCSID)
				h.start(t, managedGrowattLINKID)
				kind := uint32(0x313)
				payload := cx22Load(t).Voltages[1].Payload
				if id == managedGrowattLINKID {
					kind = 0x9810f401
					payload = cx22Load(t).Cells.Payload
				}
				r := cx22Record(t, h.receivers[id].id, kind, 1, time.Second, payload)
				h.send(t, id, r)
				if closeFailure {
					h.receivers[id].closeErr = errors.New("synthetic close failure")
				}
				select {
				case h.receivers[id].fail <- errors.New("synthetic receive loss"):
				case <-time.After(time.Second):
					t.Fatal("loss injection timed out")
				}
				cx22Wait(t, func() bool {
					s, _ := h.manager.Snapshot(string(id))
					return s.ObservedState == drivermanager.ObservedFailed
				})
				var err error
				if id == managedGrowattPCSID {
					_, err = h.b.growattPCS(context.Background(), time.Second)
				} else {
					_, err = h.b.growattLINK(context.Background(), time.Second)
				}
				if !errors.Is(err, drivermanager.ErrUnavailable) {
					t.Fatal("loss binding still available", err)
				}
				s := h.project(id, time.Second)
				p := s.Providers[0]
				if len(p.Current) != 0 || p.Promotable || len(p.LastGood) == 0 {
					t.Fatal("loss erased history or retained current")
				}
				cx22Raw(t, s, r)
				report, stopErr := h.manager.StopWithReport(context.Background(), string(id))
				err = stopErr
				if closeFailure {
					if err != nil || report.Class.Proven() || !h.wrappers[id].SafetyQuarantined() {
						t.Fatalf("close failure err=%v quarantined=%v closes=%d", err, h.wrappers[id].SafetyQuarantined(), h.receivers[id].closes.Load())
					}
					if err = h.manager.Start(context.Background(), string(id)); err == nil {
						t.Fatal("quarantine reopened")
					}
				} else if err != nil || !report.Class.Proven() {
					t.Fatal("close not proven", err, report.Class)
				}
				if h.receivers[id].closes.Load() != 1 {
					t.Fatal("close not exactly once")
				}
				sibling := managedGrowattPCSID
				if id == sibling {
					sibling = managedGrowattLINKID
				}
				state, _ := h.manager.Snapshot(string(sibling))
				if state.ObservedState != drivermanager.ObservedRunning {
					t.Fatal("loss crossed provider boundary")
				}
			})
		}
	}
}

func TestCX22CancelledOpenIsFenced(t *testing.T) {
	for _, id := range []runtimecontrol.DriverID{managedGrowattPCSID, managedGrowattLINKID} {
		t.Run(string(id), func(t *testing.T) {
			h := cx22New(t, cx22Config(t, false))
			w := h.wrappers[id]
			r := h.receivers[id]
			entered, release := make(chan struct{}), make(chan struct{})
			w.open = func(string) (growattProjectReceiver, error) { close(entered); <-release; return r, nil }
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			result := make(chan error, 1)
			go func() { _, err := w.Start(ctx); result <- err }()
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("open did not enter")
			}
			cancel()
			close(release)
			select {
			case err := <-result:
				if !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("start ignored cancellation")
			}
			cx22Wait(t, func() bool { return r.closes.Load() == 1 })
			if err := w.Stop(context.Background()); err != nil {
				t.Fatal(err)
			}
			s := h.project(id, time.Second)
			if len(s.Raw) != 0 || len(s.Providers[0].Current) != 0 || s.Providers[0].Promotable {
				t.Fatal("cancelled open admitted input")
			}
		})
	}
}

func TestCX22ReceiveOnlyInputContracts(t *testing.T) {
	for _, typ := range []reflect.Type{reflect.TypeOf((*growattProjectReceiver)(nil)).Elem(), reflect.TypeOf((*growattBMSCANReceiver)(nil)).Elem()} {
		if typ.NumMethod() != 3 {
			t.Fatal("input contract gained capability", typ)
		}
		for _, name := range []string{"Identity", "Receive", "Close"} {
			if _, ok := typ.MethodByName(name); !ok {
				t.Fatal("unexpected input contract", name)
			}
		}
	}
}

func TestCX22StaleContributorCannotRenewMean(t *testing.T) {
	for _, elapsed := range []time.Duration{time.Second - 1, time.Second, time.Second + 1} {
		t.Run(elapsed.String(), func(t *testing.T) {
			cfg := cx22Config(t, false)
			c := cx21Context(1)
			c.Cells.Count = 1
			cfg.GrowattLINKCANConfig.Context = cx21JSON(t, []growattProjectContext{c})
			h := cx22New(t, cfg)
			h.start(t, managedGrowattLINKID)
			r := cx22Record(t, h.receivers[managedGrowattLINKID].id, 0x9810f401, 1, time.Second, cx22Load(t).Cells.Payload)
			h.send(t, managedGrowattLINKID, r)
			view, err := h.b.growattLINK(context.Background(), time.Second)
			if err != nil {
				t.Fatal(err)
			}
			contributors := view.Cells[0]
			contributors.AsOf = time.Second
			field := contributors.Fields[1]
			field.Age = 0
			contributors.Fields[1] = field
			d := h.wrappers[managedGrowattLINKID].Runtime.(*growattLINKManagedDriver)
			original := d.provider.associate
			d.provider.associate = func(s canbusreg.CXSelection, e canbusreg.Evidence) canbusreg.CX16ReplayInput {
				in := original(s, e)
				in.PCS.FieldAge = 0
				in.CellContributors = contributors
				in.MeanGate = canbusreg.CX12MeasurementGate{Configured: true, Valid: true}
				return in
			}
			mean := cx22Record(t, r.Interface, 0x9806f401, 2, time.Second+elapsed, "000000000000e40c")
			s := h.send(t, managedGrowattLINKID, mean)
			f := s.Providers[0].Current["mean/mean"]
			want := "accepted"
			if elapsed > time.Second {
				want = "raw"
			}
			if f.Status != want || f.Numerator != 3300 || f.PhysicalQualified {
				t.Fatal("contributor freshness collapsed", f)
			}
			if want == "accepted" && (f.Grade != "Hypothesis" || f.Unit != "mV") {
				t.Fatal(f)
			}
			if want == "raw" && (f.Meaning != "" || f.Unit != "") {
				t.Fatal("expired contributor acquired meaning")
			}
			cx22Raw(t, s, r)
			cx22Raw(t, s, mean)
		})
	}
}

func TestCX22RawTelemetryCannotBecomePackVoltage(t *testing.T) {
	h := cx22New(t, cx22Config(t, false))
	h.start(t, managedGrowattLINKID)
	r := cx22Record(t, h.receivers[managedGrowattLINKID].id, 0x9861f401, 1, time.Second, "14be000000000000")
	s := h.send(t, managedGrowattLINKID, r)
	f := s.Providers[0].Current["diagnostic/state_0"]
	if f.Status != "raw" || f.Grade != "Unknown" || f.Meaning != "" || f.Unit != "" || f.Numerator != 20 {
		t.Fatal("raw state acquired measurement meaning", f)
	}
	if _, ok := s.Providers[0].Current["core/voltage"]; ok {
		t.Fatal("raw LINK telemetry created PCS voltage")
	}
	cx22Raw(t, s, r)
}

func TestCX22OmittedMemberDictionaryStaysRaw(t *testing.T) {
	h := cx22New(t, cx22Config(t, false))
	h.start(t, managedGrowattPCSID)
	r := cx22Record(t, h.receivers[managedGrowattPCSID].id, 0x333, 1, time.Second, "0101110000000000")
	s := h.send(t, managedGrowattPCSID, r)
	p := s.Providers[0]
	if p.Current["slots/unassociated/dtc_a"].Status != "raw" {
		t.Fatal("CLI inferred reporting dictionary")
	}
	if _, ok := p.Current["slots/unassociated/dtc_a_index"]; ok {
		t.Fatal("omitted member invented index")
	}
	cx22Raw(t, s, r)
}

func TestCX22SameAddressOnIndependentInterfaces(t *testing.T) {
	first := cx22New(t, cx22Config(t, false))
	cfg := cx22Config(t, false)
	cfg.GrowattPCSCANConfig.Interface = "synthetic-pcs-b"
	cfg.GrowattLINKCANConfig.Interface = "synthetic-b"
	contexts := []growattProjectContext{cx21Context(1)}
	contexts[0].InterfaceIndex = 8
	cfg.GrowattLINKCANConfig.Context = cx21JSON(t, contexts)
	second := cx22New(t, cfg)
	for _, h := range []*cx22Harness{first, second} {
		h.start(t, managedGrowattLINKID)
		r := cx22Record(t, h.receivers[managedGrowattLINKID].id, 0x9810f401, 1, time.Second, cx22Load(t).Cells.Payload)
		s := h.send(t, managedGrowattLINKID, r)
		if s.Providers[0].Current["cells/cell_1"].Evidence.Interface != r.Interface {
			t.Fatal("same address aliased interface")
		}
	}
	before := second.project(managedGrowattLINKID, time.Second).Providers[0]
	conflict := cx22Record(t, first.receivers[managedGrowattLINKID].id, 0x9802f401, 2, time.Second, "5141416130303131")
	first.send(t, managedGrowattLINKID, conflict)
	after := second.project(managedGrowattLINKID, time.Second).Providers[0]
	if !reflect.DeepEqual(before, after) {
		t.Fatal("same source on another interface changed provider")
	}
}

func TestCX22TopologyAssociationWithholdsConfirmation(t *testing.T) {
	cfg := cx22Config(t, false)
	cfg.GrowattPCSCANConfig.ProjectCode = "QAAa"
	cfg.GrowattPCSCANConfig.Release = "9011"
	cfg.GrowattPCSCANConfig.NativeContract = "release11"
	c := cx21Context(1)
	c.TopologyAssociated = false
	cfg.GrowattPCSCANConfig.Context = cx21JSON(t, []growattProjectContext{c})
	h := cx22New(t, cfg)
	h.start(t, managedGrowattPCSID)
	r := cx22Record(t, h.receivers[managedGrowattPCSID].id, 0x333, 1, time.Second, "0101110000000000")
	s := h.send(t, managedGrowattPCSID, r)
	p := s.Providers[0]
	cx22Raw(t, s, r)
	input := p.Raw[0].Input
	for _, associated := range []bool{false, true} {
		result := canbusreg.CX01ConfirmNativeSelection(p.Selection, canbusreg.CX01NativeIndication{Input: input, TopologyAssociated: associated})
		want := "unsupported"
		if associated {
			want = "confirmed"
		}
		if result.Status != want || result.PhysicalQualified || result.Raw != input.Evidence.Frame.Bytes() {
			t.Fatal("topology inferred", result.Status)
		}
	}
}

func TestCX22InvalidFollowupRetainsLastGoodAndSibling(t *testing.T) {
	h := cx22New(t, cx22Config(t, false))
	d := h.wrappers[managedGrowattLINKID].Runtime.(*growattLINKManagedDriver)
	original := d.provider.associate
	d.provider.associate = func(s canbusreg.CXSelection, e canbusreg.Evidence) canbusreg.CX16ReplayInput {
		in := original(s, e)
		if e.Sequence == 2 {
			in.CellGates[0].Valid = false
		}
		return in
	}
	h.start(t, managedGrowattLINKID)
	r := cx22Record(t, h.receivers[managedGrowattLINKID].id, 0x9810f401, 1, time.Second, cx22Load(t).Cells.Payload)
	h.send(t, managedGrowattLINKID, r)
	r.Sequence = 2
	s := h.send(t, managedGrowattLINKID, r)
	p := s.Providers[0]
	if p.Current["cells/cell_1"].Status != "invalid" || p.LastGood["cells/cell_1"].Evidence.Sequence != 1 || p.Current["cells/cell_2"].Status != "accepted" || p.Current["cells/cell_2"].Evidence.Sequence != 2 {
		t.Fatal("invalid follow-up erased independent history")
	}
	cx22Raw(t, s, r)
}
