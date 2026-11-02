package portalconfig

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
)

func initial() Record {
	return Record{Format: Format, Revision: "0", Drivers: []Driver{{ID: "family.primary", Enabled: true}}}
}
func constraints() Constraints {
	return Constraints{Drivers: []string{"family.primary"}, GraphQL: true, MCP: true, AdapterProxy: true, Matter: true}
}
func openTest(t *testing.T) (*Store, string) {
	t.Helper()
	root := t.TempDir()
	s, e := Open(root, initial(), constraints())
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s, root
}
func requireCode(t *testing.T, e error, code string) {
	t.Helper()
	var typed *Error
	if !errors.As(e, &typed) || typed.Code != code {
		t.Fatalf("error %v, want %s", e, code)
	}
}

func TestM26RoundTripAndSelections(t *testing.T) {
	// Every selection varies independently; omission or coupling of any persisted boolean fails this test.
	for _, field := range []string{"driver.enabled", "sharing.graphql", "sharing.mcp", "adapter_proxy", "output_bindings.matter"} {
		t.Run(field, func(t *testing.T) {
			s, root := openTest(t)
			r, _ := s.Read()
			switch field {
			case "driver.enabled":
				r.Drivers[0].Enabled = false
			case "sharing.graphql":
				r.Sharing.GraphQL = true
			case "sharing.mcp":
				r.Sharing.MCP = true
			case "adapter_proxy":
				r.AdapterProxy = true
			case "output_bindings.matter":
				r.OutputBindings.Matter = true
			}
			saved, e := s.Update(r)
			if e != nil || saved.Revision != "1" {
				t.Fatalf("save: %v %v", saved, e)
			}
			again, e := s.Update(saved)
			if e != nil || !reflect.DeepEqual(saved, again) {
				t.Fatal("no-op changed revision", e)
			}
			_, e = s.Update(r)
			requireCode(t, e, "revision_conflict")
			_ = s.Close()
			reopened, e := Open(root, initial(), constraints())
			if e != nil {
				t.Fatal(e)
			}
			defer func() {
				if err := reopened.Close(); err != nil {
					t.Errorf("close store: %v", err)
				}
			}()
			got, e := reopened.Read()
			if e != nil || !reflect.DeepEqual(got, saved) {
				t.Fatal("reopen mismatch", e)
			}
		})
	}
}
func TestM26MalformedRecords(t *testing.T) {
	b, _ := json.Marshal(initial())
	base := string(b)
	for _, bad := range []string{
		strings.Replace(base, `"format":`, `"unexpected_marker":true,"format":`, 1),
		strings.Replace(base, `"format":`, `"passive":false,"format":`, 1),
		strings.Replace(base, `"format":`, `"cache":false,"format":`, 1),
		strings.Replace(base, `"revision":"0"`, `"revision":"00"`, 1),
		strings.Replace(base, `"revision":"0"`, `"revision":"18446744073709551616"`, 1),
		strings.Replace(base, `"revision":"0"`, `"revision":"0","revision":"0"`, 1),
		strings.Replace(base, `"enabled":true`, `"enabled":null`, 1),
		strings.Replace(base, `"enabled":true`, `"enabled":"true"`, 1),
		strings.Replace(base, `"graphql":false,`, ``, 1),
		strings.Replace(base, `"drivers":[`, `"drivers":[{"driver_id":"family.primary","enabled":false},`, 1),
		base + `{}`, strings.Repeat(" ", MaxBytes+1), `null`,
	} {
		if _, e := Decode([]byte(bad)); e == nil {
			t.Fatal("accepted malformed record")
		}
	}
	_, e := Decode([]byte(strings.Replace(base, Format, "unsupported", 1)))
	requireCode(t, e, "unsupported_format")
}
func TestM26InterruptedReplacement(t *testing.T) {
	for _, stage := range []string{"create", "write", "file_sync", "close", "rename", "directory_sync"} {
		t.Run(stage, func(t *testing.T) {
			s, root := openTest(t)
			old, _ := s.Read()
			r := clone(old)
			r.Sharing.GraphQL = true
			s.fault = func(got string) error {
				if got == stage {
					return errors.New("synthetic failure")
				}
				return nil
			}
			_, e := s.Update(r)
			requireCode(t, e, "persistence_failed")
			if !reflect.DeepEqual(s.current, old) {
				t.Fatal("failed edit changed accepted memory")
			}
			if stage == "directory_sync" {
				_, e = s.Update(old)
				requireCode(t, e, "persistence_failed")
				requireCode(t, s.RecoverDurability(), "persistence_failed")
				s.fault = nil
				if e = s.RecoverDurability(); e != nil {
					t.Fatal(e)
				}
				got, _ := s.Read()
				if !got.Sharing.GraphQL || got.Revision != "1" {
					t.Fatal("recovery lost successor")
				}
			} else {
				s.fault = nil
				got, e := s.Read()
				if e != nil || !reflect.DeepEqual(got, old) {
					t.Fatal("last valid record lost", e)
				}
			}
			if e = os.WriteFile(filepath.Join(root, ".portal-config-interrupted"), []byte(`{`), 0600); e != nil {
				t.Fatal(e)
			}
			want, _ := s.Read()
			_ = s.Close()
			s2, e := Open(root, initial(), constraints())
			if e != nil {
				t.Fatal(e)
			}
			defer func() {
				if err := s2.Close(); err != nil {
					t.Errorf("close store: %v", err)
				}
			}()
			got, _ := s2.Read()
			if !reflect.DeepEqual(got, want) {
				t.Fatal("temporary file changed recovery")
			}
		})
	}
}
func TestM26FailClosedLoadAndExternalReplacement(t *testing.T) {
	s, root := openTest(t)
	locked, e := Open(root, initial(), constraints())
	requireCode(t, e, "persistence_failed")
	_ = locked.Close()
	if e = os.WriteFile(filepath.Join(root, "portal-config.json"), []byte(`{"secret_marker":true}`), 0600); e != nil {
		t.Fatal(e)
	}
	got, e := s.Read()
	requireCode(t, e, "invalid_record")
	if got.Revision != "0" {
		t.Fatal("historical view lost")
	}
	_, e = s.Update(initial())
	requireCode(t, e, "invalid_record")
	_ = s.Close()
	broken, e := Open(root, initial(), constraints())
	requireCode(t, e, "invalid_record")
	defer func() {
		if err := broken.Close(); err != nil {
			t.Errorf("close store: %v", err)
		}
	}()
	got, e = broken.Read()
	requireCode(t, e, "invalid_record")
	if got.Format != "" {
		t.Fatal("cold failure installed defaults")
	}
}
func TestM26ConcurrencyAndExhaustion(t *testing.T) {
	s, root := openTest(t)
	r, _ := s.Read()
	r.AdapterProxy = true
	var wg sync.WaitGroup
	results := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _, e := s.Update(r); results <- e }()
	}
	wg.Wait()
	close(results)
	success := 0
	for e := range results {
		if e == nil {
			success++
		} else {
			requireCode(t, e, "revision_conflict")
		}
	}
	if success != 1 {
		t.Fatal("concurrent edits accepted")
	}
	_ = s.Close()
	r.Revision = "18446744073709551615"
	b, _ := json.Marshal(r)
	if e := os.WriteFile(filepath.Join(root, "portal-config.json"), b, 0600); e != nil {
		t.Fatal(e)
	}
	s2, e := Open(root, initial(), constraints())
	if e != nil {
		t.Fatal(e)
	}
	defer func() {
		if err := s2.Close(); err != nil {
			t.Errorf("close store: %v", err)
		}
	}()
	if _, e = s2.Update(r); e != nil {
		t.Fatal(e)
	}
	r.AdapterProxy = false
	_, e = s2.Update(r)
	requireCode(t, e, "revision_exhausted")
}
func TestM26DriverIdentityAndHostConstraints(t *testing.T) {
	s, _ := openTest(t)
	r, _ := s.Read()
	r.Drivers[0].ID = "family.other"
	_, e := s.Update(r)
	requireCode(t, e, "unsupported_selection")
	root := t.TempDir()
	c := constraints()
	c.GraphQL = false
	s2, e := Open(root, initial(), c)
	if e != nil {
		t.Fatal(e)
	}
	defer func() {
		if err := s2.Close(); err != nil {
			t.Errorf("close store: %v", err)
		}
	}()
	r = initial()
	r.Sharing.GraphQL = true
	_, e = s2.Update(r)
	requireCode(t, e, "unsupported_selection")
}

func TestM26ClosedShapeLimitsAndStorageTypes(t *testing.T) {
	base, _ := json.Marshal(initial())
	var object map[string]json.RawMessage
	if e := json.Unmarshal(base, &object); e != nil {
		t.Fatal(e)
	}
	for field := range object {
		t.Run("missing_"+field, func(t *testing.T) {
			var obj map[string]json.RawMessage
			_ = json.Unmarshal(base, &obj)
			delete(obj, field)
			b, _ := json.Marshal(obj)
			_, e := Decode(b)
			requireCode(t, e, "invalid_record")
		})
	}
	r := initial()
	r.Drivers = make([]Driver, 65)
	for i := range r.Drivers {
		r.Drivers[i] = Driver{ID: strings.Repeat("x", i+1)}
	}
	b, _ := json.Marshal(r)
	_, e := Decode(b)
	requireCode(t, e, "invalid_record")
	for _, kind := range []string{"directory", "symlink", "lock_symlink"} {
		t.Run(kind, func(t *testing.T) {
			root := t.TempDir()
			name := "portal-config.json"
			if kind == "lock_symlink" {
				name = "portal-config.lock"
			}
			path := filepath.Join(root, name)
			if kind == "directory" {
				if e := os.Mkdir(path, 0700); e != nil {
					t.Fatal(e)
				}
			} else {
				target := filepath.Join(root, "synthetic-target")
				if e := os.WriteFile(target, base, 0600); e != nil {
					t.Fatal(e)
				}
				if e := os.Symlink(target, path); e != nil {
					t.Fatal(e)
				}
			}
			s, e := Open(root, initial(), constraints())
			requireCode(t, e, "persistence_failed")
			defer func() {
				if err := s.Close(); err != nil {
					t.Errorf("close store: %v", err)
				}
			}()
			got, e := s.Read()
			requireCode(t, e, "persistence_failed")
			if got.Format != "" {
				t.Fatal("storage failure installed defaults")
			}
		})
	}
}

func TestM26UncertainRecoveryRejectsExternalSuccessor(t *testing.T) {
	s, root := openTest(t)
	r, _ := s.Read()
	r.AdapterProxy = true
	s.fault = func(stage string) error {
		if stage == "directory_sync" {
			return errors.New("synthetic failure")
		}
		return nil
	}
	_, e := s.Update(r)
	requireCode(t, e, "persistence_failed")
	s.fault = nil
	r.Revision = "1"
	r.Sharing.GraphQL = true
	b, _ := json.Marshal(r)
	if e = os.WriteFile(filepath.Join(root, "portal-config.json"), b, 0600); e != nil {
		t.Fatal(e)
	}
	requireCode(t, s.RecoverDurability(), "persistence_failed")
	_, e = s.Read()
	requireCode(t, e, "persistence_failed")
}

func TestM26EachSelectionRequiresHostSupport(t *testing.T) {
	for _, field := range []string{"graphql", "mcp", "adapter_proxy", "matter"} {
		t.Run(field, func(t *testing.T) {
			c := constraints()
			r := initial()
			switch field {
			case "graphql":
				c.GraphQL = false
				r.Sharing.GraphQL = true
			case "mcp":
				c.MCP = false
				r.Sharing.MCP = true
			case "adapter_proxy":
				c.AdapterProxy = false
				r.AdapterProxy = true
			case "matter":
				c.Matter = false
				r.OutputBindings.Matter = true
			}
			s, e := Open(t.TempDir(), initial(), c)
			if e != nil {
				t.Fatal(e)
			}
			defer func() {
				if err := s.Close(); err != nil {
					t.Errorf("close store: %v", err)
				}
			}()
			_, e = s.Update(r)
			requireCode(t, e, "unsupported_selection")
			got, e := s.Read()
			if e != nil || !reflect.DeepEqual(got, initial()) {
				t.Fatal("rejected selection changed desired state", e)
			}
		})
	}
}

func TestM26ClosedStoreCannotRecoverDurability(t *testing.T) {
	s, root := openTest(t)
	old, e := s.Read()
	if e != nil {
		t.Fatal(e)
	}
	r := clone(old)
	r.AdapterProxy = true
	s.fault = func(stage string) error {
		if stage == "directory_sync" {
			return errors.New("synthetic failure")
		}
		return nil
	}
	_, e = s.Update(r)
	requireCode(t, e, "persistence_failed")
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	s.fault = nil
	other, e := Open(root, initial(), constraints())
	if e != nil {
		t.Fatal(e)
	}
	defer func() {
		if err := other.Close(); err != nil {
			t.Errorf("close store: %v", err)
		}
	}()
	requireCode(t, s.RecoverDurability(), "persistence_failed")
	got, e := s.Read()
	requireCode(t, e, "persistence_failed")
	if !reflect.DeepEqual(got, old) {
		t.Fatal("closed recovery changed accepted record")
	}
	r, e = other.Read()
	if e != nil {
		t.Fatal(e)
	}
	r.Sharing.GraphQL = true
	_, e = s.Update(r)
	requireCode(t, e, "persistence_failed")
	if _, e = other.Update(r); e != nil {
		t.Fatal(e)
	}
}

func TestM26SuccessorMustFitRecordLimit(t *testing.T) {
	root := t.TempDir()
	old := initial()
	old.Revision = "9"
	b, e := json.Marshal(old)
	if e != nil {
		t.Fatal(e)
	}
	old.Drivers[0].ID += strings.Repeat("x", MaxBytes-1-len(b))
	b, e = json.Marshal(old)
	if e != nil || len(b) != MaxBytes-1 {
		t.Fatalf("fixture size %d: %v", len(b), e)
	}
	path := filepath.Join(root, "portal-config.json")
	if e = os.WriteFile(path, b, 0600); e != nil {
		t.Fatal(e)
	}
	c := constraints()
	c.Drivers = []string{old.Drivers[0].ID}
	s, e := Open(root, initial(), c)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = s.Close() })
	r := clone(old)
	r.Drivers[0].Enabled = false
	input, e := json.Marshal(r)
	if e != nil || len(input) != MaxBytes {
		t.Fatalf("request size %d: %v", len(input), e)
	}
	if _, e = Decode(input); e != nil {
		t.Fatal(e)
	}
	s.fault = func(stage string) error { t.Fatalf("oversized successor reached persistence: %s", stage); return nil }
	got, e := s.Update(r)
	requireCode(t, e, "invalid_record")
	if !reflect.DeepEqual(got, old) {
		t.Fatal("rejected successor changed accepted record")
	}
	disk, e := os.ReadFile(path)
	if e != nil || string(disk) != string(b) {
		t.Fatal("rejected successor changed disk", e)
	}
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	reopened, e := Open(root, initial(), c)
	if e != nil {
		t.Fatal(e)
	}
	defer func() {
		if err := reopened.Close(); err != nil {
			t.Errorf("close store: %v", err)
		}
	}()
	got, e = reopened.Read()
	if e != nil || !reflect.DeepEqual(got, old) {
		t.Fatal("reopen lost accepted record", e)
	}
}
