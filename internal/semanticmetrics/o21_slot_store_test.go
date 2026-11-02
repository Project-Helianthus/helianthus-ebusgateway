package semanticmetrics

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"reflect"
	"strings"
	"testing"

	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
)

type o21Memory struct {
	raw    []byte
	fail   bool
	commit func()
	writes int
}

func (p *o21Memory) Load(string) ([]byte, error) {
	if p.raw == nil {
		return nil, fs.ErrNotExist
	}
	return append([]byte(nil), p.raw...), nil
}
func (p *o21Memory) Commit(_ string, raw []byte) error {
	p.writes++
	if p.commit != nil {
		p.commit()
	}
	// Model the strongest uncertainty: replacement happened before an error.
	p.raw = append([]byte(nil), raw...)
	if p.fail {
		return errors.New("synthetic uncertain completion")
	}
	return nil
}
func o21Fixture(t *testing.T) (*O21SlotStore, *o21Memory, Binding) {
	t.Helper()
	_, r := m36Fixture(t, "thermal.measurement.air_temperature", m36Quantity("0", "unit.celsius"))
	p := &o21Memory{}
	s, err := O21Create(p, "o21-target")
	if err != nil {
		t.Fatal(err)
	}
	return s, p, o21Clone(r.Bindings[0])
}
func o21Assign(t *testing.T, s *O21SlotStore, b Binding) Registry {
	t.Helper()
	r, err := s.Assign("o21-target", b)
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func o21Validate(t *testing.T, s *O21SlotStore) {
	t.Helper()
	if err := s.Revalidate("o21-target", s.Export().Bindings); err != nil {
		t.Fatal(err)
	}
}
func TestO21DurabilityRestartAndZero(t *testing.T) {
	s, p, b := o21Fixture(t)
	r := o21Assign(t, s, b)
	if r.Validated {
		t.Fatal("assignment trusted without owner")
	}
	o21Validate(t, s)
	next := o21Clone(b)
	next.Slot = 1
	next.AssetID = "asset:o21-next"
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	p.commit = func() { close(entered); <-release }
	go func() { _, err := s.Assign("o21-target", next); done <- err }()
	<-entered
	during := s.Export()
	// Always release and join before reporting any assertion failure.
	close(release)
	err := <-done
	p.commit = nil
	if err != nil {
		t.Fatal(err)
	}
	if during.Validated || len(during.Bindings) != 1 || !reflect.DeepEqual(during.Bindings[0], b) {
		t.Fatal("published before durable completion")
	}
	if s.Export().Validated {
		t.Fatal("mutation inherited validation")
	}
	restarted, err := O21Open(p, "o21-target")
	if err != nil {
		t.Fatal(err)
	}
	snapshot, _ := m36Fixture(t, "thermal.measurement.air_temperature", m36Quantity("0", "unit.celsius"))
	view := m36View(t, snapshot)
	name := "helianthus_thermal_air_temperature_celsius{slot=\"0\"} 0\n"
	if restarted.Export().Validated || strings.Contains(m36Render(t, view, restarted.Export()), name) {
		t.Fatal("loaded store trusted")
	}
	o21Validate(t, restarted)
	if !strings.Contains(m36Render(t, view, restarted.Export()), name) {
		t.Fatal("explicitly revalidated zero withheld")
	}
}
func TestO21Identity(t *testing.T) {
	changes := map[string]func(*Binding){
		"domain":            func(b *Binding) { b.Domain = "infrastructure" },
		"slot":              func(b *Binding) { b.Slot = 1 },
		"asset":             func(b *Binding) { b.AssetID = "asset:o21-other" },
		"source":            func(b *Binding) { b.SourceID = "source:o21-other" },
		"source_epoch":      func(b *Binding) { b.SourceEpochID = "epoch:o21-other" },
		"native_binding":    func(b *Binding) { b.BindingID = "binding:o21-other" },
		"driver_generation": func(b *Binding) { b.DriverGeneration = "2" },
		"service_instance":  func(b *Binding) { b.ServiceInstance = "service:o21-other" },
		"dimension_id":      func(b *Binding) { b.Dimensions[0].ID = "thermal.dimension.other" },
		"dimension_value":   func(b *Binding) { *b.Dimensions[0].Value.Text = "o21-other" },
		"selected_fields":   func(b *Binding) { b.Fields = []semreg.DefinitionID{"thermal.measurement.water_temperature"} },
	}
	for name, change := range changes {
		t.Run(name, func(t *testing.T) {
			s, p, b := o21Fixture(t)
			o21Assign(t, s, b)
			o21Validate(t, s)
			before := s.Export()
			changed := o21Clone(b)
			change(&changed)
			writes := p.writes
			if err := s.Revalidate("o21-target", []Binding{changed}); err == nil {
				t.Fatal("identity accepted")
			}
			if s.Export().Validated {
				t.Fatal("failed attestation retained validation")
			}
			if _, err := s.Retire("o21-target", changed); err == nil {
				t.Fatal("wrong identity retired")
			}
			if !reflect.DeepEqual(before.Bindings, s.Export().Bindings) || p.writes != writes {
				t.Fatal("rejection mutated assignments")
			}
			o21Validate(t, s)
			if _, err := s.Retire("o21-target", b); err != nil {
				t.Fatal(err)
			}
		})
	}
	t.Run("scrape_target", func(t *testing.T) {
		s, p, b := o21Fixture(t)
		o21Assign(t, s, b)
		o21Validate(t, s)
		before := s.Export()
		writes := p.writes
		if _, err := O21Open(p, "o21-other"); err == nil {
			t.Fatal("cross-target load")
		}
		if err := s.Revalidate("o21-other", before.Bindings); err == nil {
			t.Fatal("cross-target validation")
		}
		if _, err := s.Assign("o21-other", b); err == nil {
			t.Fatal("cross-target assign")
		}
		if _, err := s.Retire("o21-other", b); err == nil {
			t.Fatal("cross-target retire")
		}
		if !reflect.DeepEqual(before.Bindings, s.Export().Bindings) || p.writes != writes {
			t.Fatal("cross-target mutation")
		}
	})
}
func TestO21RetirementExhaustionAndDetached(t *testing.T) {
	s, p, b := o21Fixture(t)
	for i := 0; i < 16; i++ {
		next := o21Clone(b)
		next.Slot = i
		next.AssetID = semreg.AssetID(fmt.Sprintf("asset:o21-%d", i))
		r := o21Assign(t, s, next)
		if len(s.Export().Bindings) != i+1 {
			t.Fatal("lost permanent reservations")
		}
		next.Fields[0] = "thermal.mode.system"
		*next.Dimensions[0].Value.Text = "o21-input-mutated"
		if s.Export().Bindings[i].Fields[0] != b.Fields[0] || *s.Export().Bindings[i].Dimensions[0].Value.Text != *b.Dimensions[0].Value.Text {
			t.Fatal("input aliases store")
		}
		next = o21Clone(s.Export().Bindings[i])
		r.Bindings[0].Fields[0] = "thermal.mode.system"
		*r.Bindings[0].Dimensions[0].Value.Text = "o21-mutated"
		if s.Export().Bindings[0].Fields[0] != b.Fields[0] || *s.Export().Bindings[0].Dimensions[0].Value.Text != *b.Dimensions[0].Value.Text {
			t.Fatal("return aliases store")
		}
		if _, err := s.Retire("o21-target", next); err != nil {
			t.Fatal(err)
		}
	}
	restarted, err := O21Open(p, "o21-target")
	if err != nil {
		t.Fatal(err)
	}
	before := restarted.Export()
	writes := p.writes
	for i := 0; i <= 16; i++ {
		next := o21Clone(b)
		next.Slot = i
		next.AssetID = "asset:o21-new"
		if _, err := restarted.Assign("o21-target", next); err == nil {
			t.Fatal("reserved/exhausted slot accepted")
		}
	}
	if !reflect.DeepEqual(before, restarted.Export()) || writes != p.writes {
		t.Fatal("exhaustion mutated state")
	}
	// The other domain has its own independent capacity.
	_, r := m36Fixture(t, "infrastructure.ac.voltage", m36Quantity("0", "unit.volt"))
	o21Assign(t, restarted, r.Bindings[0])
}
func TestO21UncertainAndCorrupt(t *testing.T) {
	s, p, b := o21Fixture(t)
	o21Assign(t, s, b)
	o21Validate(t, s)
	before := s.Export()
	p.fail = true
	if _, err := s.Retire("o21-target", b); err == nil {
		t.Fatal("uncertain commit accepted")
	}
	if s.Export().Validated || !reflect.DeepEqual(before.Bindings, s.Export().Bindings) {
		t.Fatal("false durability or lost accepted state")
	}
	if err := s.Revalidate("o21-target", before.Bindings); err == nil {
		t.Fatal("uncertainty bypassed")
	}
	writes := p.writes
	if _, err := s.Assign("o21-target", b); err == nil || p.writes != writes {
		t.Fatal("uncertain writer continued")
	}
	recovered, err := O21Open(p, "o21-target")
	if err != nil {
		t.Fatal(err)
	}
	if recovered.Export().Validated || !recovered.Export().Bindings[0].Retired {
		t.Fatal("restart lost reservation or trusted validation")
	}
	for name, raw := range map[string][]byte{
		"empty": {}, "syntax": []byte("{"), "oversize": make([]byte, o21MaxBytes+1), "unknown": []byte(`{"Version":1,"Target":"o21-target","Validated":true}`), "trailing": append(append([]byte(nil), p.raw...), []byte(` {}`)...),
	} {
		t.Run(name, func(t *testing.T) {
			bad := &o21Memory{raw: raw}
			if _, err := O21Open(bad, "o21-target"); err == nil {
				t.Fatal("corruption accepted")
			}
		})
	}
	for _, change := range []func(*o21Document){
		func(d *o21Document) { d.Version = 2 }, func(d *o21Document) { d.Bindings[0].Slot = 16 }, func(d *o21Document) { d.Bindings[0].SourceID = "" }, func(d *o21Document) { d.Bindings = append(d.Bindings, d.Bindings[0]) }, func(d *o21Document) { d.Bindings[0].Fields = nil },
	} {
		var d o21Document
		if err := json.Unmarshal(p.raw, &d); err != nil {
			t.Fatal(err)
		}
		change(&d)
		raw, _ := json.Marshal(d)
		if _, err := O21Open(&o21Memory{raw: raw}, "o21-target"); err == nil {
			t.Fatal("invalid loaded binding accepted")
		}
	}
	if _, err := O21Open(&o21Memory{}, "o21-target"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("missing load accepted")
	}
	if _, err := O21Create(p, "o21-target"); err == nil {
		t.Fatal("existing store overwritten")
	}
	p = &o21Memory{fail: true}
	if _, err := O21Create(p, "o21-target"); err == nil {
		t.Fatal("failed provisioning accepted")
	}
}

func TestO21LoadIdentity(t *testing.T) {
	changes := map[string]func(*Binding){
		"domain":            func(b *Binding) { b.Domain = "unknown" },
		"slot":              func(b *Binding) { b.Slot = -1 },
		"asset":             func(b *Binding) { b.AssetID = "" },
		"source":            func(b *Binding) { b.SourceID = "" },
		"source_epoch":      func(b *Binding) { b.SourceEpochID = "" },
		"native_binding":    func(b *Binding) { b.BindingID = "" },
		"driver_generation": func(b *Binding) { b.DriverGeneration = "" },
		"service_instance":  func(b *Binding) { b.ServiceInstance = "" },
		"dimensions":        func(b *Binding) { b.Dimensions = nil },
		"selected_fields":   func(b *Binding) { b.Fields = nil },
	}
	for name, change := range changes {
		t.Run(name, func(t *testing.T) {
			s, p, b := o21Fixture(t)
			o21Assign(t, s, b)
			if _, err := O21Open(p, "o21-target"); err != nil {
				t.Fatal("valid control", err)
			}
			var d o21Document
			if err := json.Unmarshal(p.raw, &d); err != nil {
				t.Fatal(err)
			}
			change(&d.Bindings[0])
			raw, err := json.Marshal(d)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := O21Open(&o21Memory{raw: raw}, "o21-target"); err == nil {
				t.Fatal("invalid identity loaded")
			}
		})
	}
}
