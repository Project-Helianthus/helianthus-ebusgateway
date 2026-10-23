package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/drivermanager"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/portalconfig"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimecontrol"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimestate"
	"github.com/Project-Helianthus/helianthus-ebusgateway/portal"
)

var m28SidecarIDs = []runtimecontrol.DriverID{managedEEBusID, managedGrowattID, managedSunSpecID}

// m28Natives replaces the three sidecar adapters with inert counting runtimes.
// Every sidecar is host-enabled, so only the Portal record can narrow it.
type m28Natives struct {
	runtimes   map[runtimecontrol.DriverID]*m06Runtime
	constructs map[runtimecontrol.DriverID]*atomic.Int32
}

func m28Sidecars(t *testing.T) *m28Natives {
	t.Helper()
	original := gatewayManagedSidecarDescriptorsFor
	t.Cleanup(func() { gatewayManagedSidecarDescriptorsFor = original })
	natives := &m28Natives{runtimes: map[runtimecontrol.DriverID]*m06Runtime{}, constructs: map[runtimecontrol.DriverID]*atomic.Int32{}}
	for _, id := range m28SidecarIDs {
		natives.runtimes[id] = &m06Runtime{}
		natives.constructs[id] = new(atomic.Int32)
	}
	gatewayManagedSidecarDescriptorsFor = func(descriptors []runtimecontrol.Descriptor) []runtimecontrol.Descriptor {
		for i := range descriptors {
			id := descriptors[i].DriverID
			descriptors[i].Disposition = runtimecontrol.DispositionEnabled
			descriptors[i].Adapter = runtimecontrol.NewAdapter(func(context.Context, runtimecontrol.Construction) (*m06Runtime, error) {
				natives.constructs[id].Add(1)
				return natives.runtimes[id], nil
			})
		}
		return descriptors
	}
	return natives
}

// m28Counts snapshots construction and start counts per sidecar.
func (natives *m28Natives) counts() map[runtimecontrol.DriverID][2]int32 {
	out := map[runtimecontrol.DriverID][2]int32{}
	for _, id := range m28SidecarIDs {
		out[id] = [2]int32{natives.constructs[id].Load(), natives.runtimes[id].starts.Load()}
	}
	return out
}

func m28Options(root string) rctlProcessOptions {
	return rctlProcessOptions{mcp: true, config: func(c *ebusgateway.Config) { c.PortalConfigRoot = root }}
}

func m28WaitObserved(t *testing.T, process *rctlProcess, id runtimecontrol.DriverID, want drivermanager.ObservedState) runtimecontrol.DriverStatus {
	t.Helper()
	deadline := time.Now().Add(rctlBound)
	for {
		status, err := process.control.Service().Get(id)
		if err != nil {
			t.Fatalf("Get(%s) = %v", id, err)
		}
		if status.ObservedState == want {
			return status
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s observed %s/%s; want %s", id, status.ObservedState, status.Reason, want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

type m28View struct {
	Desired    *portalconfig.Record `json:"desired"`
	Historical bool                 `json:"historical"`
	Error      *portalconfig.Error  `json:"error"`
	State      struct {
		Drivers []portal.ConfigDriverState `json:"drivers"`
	} `json:"state"`
}

// m28Config performs one Portal configuration request against the process's
// production HTTP composition.
func m28Config(t *testing.T, process *rctlProcess, method string, body []byte) (int, m28View) {
	t.Helper()
	request, err := http.NewRequest(method, strings.TrimSuffix(process.mcpURL, "/mcp")+"/portal/api/v1/config", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	response, err := rctlHTTPClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = response.Body.Close() }()
	data, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(data, []byte("private_marker")) {
		t.Fatalf("record content leaked: %s", data)
	}
	var view m28View
	if err := json.Unmarshal(data, &view); err != nil {
		t.Fatalf("config view %s: %v", data, err)
	}
	return response.StatusCode, view
}

type m28Row struct {
	desired   *bool
	effective bool
	status    string
	reason    string
}

func m28Bool(v bool) *bool { return &v }

func m28RequireRows(t *testing.T, view m28View, want map[runtimecontrol.DriverID]m28Row) {
	t.Helper()
	if len(view.State.Drivers) != len(want) {
		t.Fatalf("driver rows %+v; want %d rows", view.State.Drivers, len(want))
	}
	for _, row := range view.State.Drivers {
		w, ok := want[runtimecontrol.DriverID(row.DriverID)]
		if !ok || (w.desired == nil) != (row.Desired == nil) || (w.desired != nil && *w.desired != *row.Desired) || w.effective != row.Effective || w.status != row.Status || w.reason != row.Reason {
			got := "null"
			if row.Desired != nil {
				got = map[bool]string{true: "true", false: "false"}[*row.Desired]
			}
			t.Fatalf("driver %s row desired=%s effective=%t status=%s reason=%s; want %+v", row.DriverID, got, row.Effective, row.Status, row.Reason, w)
		}
	}
}

func m28Select(t *testing.T, record *portalconfig.Record, id runtimecontrol.DriverID, enabled bool) {
	t.Helper()
	for i := range record.Drivers {
		if record.Drivers[i].ID == string(id) {
			record.Drivers[i].Enabled = enabled
			return
		}
	}
	t.Fatalf("record has no driver %s", id)
}

func m28Marshal(t *testing.T, record portalconfig.Record) []byte {
	t.Helper()
	data, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// m28AdoptedRecord is a synthetic record with one accepted local edit.
func m28AdoptedRecord(enabled bool) portalconfig.Record {
	record := portalconfig.Record{Format: portalconfig.Format, Revision: "1"}
	for _, id := range portalConfigDriverIDs {
		record.Drivers = append(record.Drivers, portalconfig.Driver{ID: string(id), Enabled: enabled})
	}
	return record
}

// TestM28SyntheticRestartAppliesRetainedSelectionsOnce: retained driver
// selections become configured eligibility at the next start. Each selected
// driver is constructed and reconciled once; a deselected one is neither
// constructed nor started and keeps its durable lifecycle intent. Edits saved
// while running are reported as restart_required and stop nothing.
func TestM28SyntheticRestartAppliesRetainedSelectionsOnce(t *testing.T) {
	natives := m28Sidecars(t)
	root := t.TempDir()
	env := newRCtlEnv(t)

	// First start: no record. The host-supplied initial record (revision 0)
	// carries no local adoption, so host configuration stays in force even
	// where the seed differs from it.
	first := env.start(m28Options(root))
	first.waitObserved(drivermanager.ObservedRunning)
	intent := map[runtimecontrol.DriverID]runtimecontrol.DesiredRevision{}
	for _, id := range m28SidecarIDs {
		intent[id] = m28WaitObserved(t, first, id, drivermanager.ObservedRunning).DesiredRevision
	}
	code, view := m28Config(t, first, http.MethodGet, nil)
	if code != http.StatusOK || view.Desired == nil || view.Desired.Revision != "0" {
		t.Fatalf("initial GET %d %+v", code, view)
	}
	m28RequireRows(t, view, map[runtimecontrol.DriverID]m28Row{
		primaryEBusDriverID: {m28Bool(true), true, "effective", "running"},
		managedEEBusID:      {m28Bool(false), true, "withheld", "record_not_adopted"},
		managedGrowattID:    {m28Bool(false), true, "withheld", "record_not_adopted"},
		managedSunSpecID:    {m28Bool(false), true, "withheld", "record_not_adopted"},
	})

	// Adopt locally: select Growatt; keep eeBUS and SunSpec deselected.
	record := *view.Desired
	m28Select(t, &record, managedGrowattID, true)
	code, view = m28Config(t, first, http.MethodPut, m28Marshal(t, record))
	if code != http.StatusOK || view.Desired == nil || view.Desired.Revision != "1" {
		t.Fatalf("adopting PUT %d %+v", code, view)
	}
	m28RequireRows(t, view, map[runtimecontrol.DriverID]m28Row{
		primaryEBusDriverID: {m28Bool(true), true, "effective", "running"},
		managedEEBusID:      {m28Bool(false), true, "restart_required", "restart_required"},
		managedGrowattID:    {m28Bool(true), true, "effective", "running"},
		managedSunSpecID:    {m28Bool(false), true, "restart_required", "restart_required"},
	})
	time.Sleep(50 * time.Millisecond)
	for _, id := range m28SidecarIDs {
		if stops := natives.runtimes[id].stops.Load(); stops != 0 {
			t.Fatalf("saving a selection stopped %s %d times; enablement is not STOP", id, stops)
		}
	}
	first.stop()

	// Restart: the retained selections narrow configured eligibility before
	// registration.
	before, dials := natives.counts(), env.dials.Load()
	second := env.start(m28Options(root))
	second.waitObserved(drivermanager.ObservedRunning)
	m28WaitObserved(t, second, managedGrowattID, drivermanager.ObservedRunning)
	time.Sleep(100 * time.Millisecond) // A repeated reconciliation would surface here.
	after := natives.counts()
	if got := env.dials.Load() - dials; got != 1 {
		t.Fatalf("selected ebus.primary opened %d endpoints after restart; want 1", got)
	}
	if after[managedGrowattID][0]-before[managedGrowattID][0] != 1 || after[managedGrowattID][1]-before[managedGrowattID][1] != 1 {
		t.Fatalf("selected Growatt constructed/started %v -> %v; want once each", before[managedGrowattID], after[managedGrowattID])
	}
	for _, id := range []runtimecontrol.DriverID{managedEEBusID, managedSunSpecID} {
		if after[id] != before[id] {
			t.Fatalf("deselected %s constructed/started %v -> %v; want neither", id, before[id], after[id])
		}
		status := m28WaitObserved(t, second, id, drivermanager.ObservedDisabled)
		if status.Enabled || status.DesiredState != runtimecontrol.DesiredRunning || status.DesiredRevision != intent[id] {
			t.Fatalf("deselected %s view %+v; want disabled with durable RUNNING intent at revision %d", id, status, intent[id])
		}
		entry, err := second.control.Registry().Get(id)
		if err != nil || entry.Driver.Disposition != runtimecontrol.DispositionDisabled {
			t.Fatalf("deselected %s registered as %+v (%v); want disabled", id, entry.Driver, err)
		}
	}
	code, view = m28Config(t, second, http.MethodGet, nil)
	if code != http.StatusOK || view.Desired == nil || view.Desired.Revision != "1" {
		t.Fatalf("restart GET %d %+v", code, view)
	}
	m28RequireRows(t, view, map[runtimecontrol.DriverID]m28Row{
		primaryEBusDriverID: {m28Bool(true), true, "effective", "running"},
		managedEEBusID:      {m28Bool(false), false, "disabled", "not_selected"},
		managedGrowattID:    {m28Bool(true), true, "effective", "running"},
		managedSunSpecID:    {m28Bool(false), false, "disabled", "not_selected"},
	})

	// Reselect SunSpec: pending the next start, nothing is constructed now.
	record = *view.Desired
	m28Select(t, &record, managedSunSpecID, true)
	code, view = m28Config(t, second, http.MethodPut, m28Marshal(t, record))
	if code != http.StatusOK || view.Desired.Revision != "2" {
		t.Fatalf("reselecting PUT %d %+v", code, view)
	}
	m28RequireRows(t, view, map[runtimecontrol.DriverID]m28Row{
		primaryEBusDriverID: {m28Bool(true), true, "effective", "running"},
		managedEEBusID:      {m28Bool(false), false, "disabled", "not_selected"},
		managedGrowattID:    {m28Bool(true), true, "effective", "running"},
		managedSunSpecID:    {m28Bool(true), false, "restart_required", "restart_required"},
	})
	if got := natives.counts(); got[managedSunSpecID] != after[managedSunSpecID] {
		t.Fatalf("reselection constructed or started SunSpec before restart: %v -> %v", after[managedSunSpecID], got[managedSunSpecID])
	}
	second.stop()

	// The retained durable RUNNING intent makes the reselected driver start
	// once, without a START decision.
	before = natives.counts()
	third := env.start(m28Options(root))
	status := m28WaitObserved(t, third, managedSunSpecID, drivermanager.ObservedRunning)
	time.Sleep(100 * time.Millisecond)
	after = natives.counts()
	if after[managedSunSpecID][0]-before[managedSunSpecID][0] != 1 || after[managedSunSpecID][1]-before[managedSunSpecID][1] != 1 || status.DesiredRevision != intent[managedSunSpecID] {
		t.Fatalf("reselected SunSpec %v -> %v at revision %d; want one construction and start at revision %d", before[managedSunSpecID], after[managedSunSpecID], status.DesiredRevision, intent[managedSunSpecID])
	}
	if after[managedEEBusID] != before[managedEEBusID] {
		t.Fatalf("deselected eeBUS constructed or started: %v -> %v", before[managedEEBusID], after[managedEEBusID])
	}
}

// TestM28RetainedPrimaryDeselectionDisablesWithoutRewritingIntent: a retained
// record that deselects ebus.primary keeps it disabled and unopened at the
// next start, leaves its durable RUNNING intent untouched and starts each
// selected sidecar once.
func TestM28RetainedPrimaryDeselectionDisablesWithoutRewritingIntent(t *testing.T) {
	natives := m28Sidecars(t)
	root := t.TempDir()
	env := newRCtlEnv(t)
	first := env.start(m28Options(root))
	intent := first.waitObserved(drivermanager.ObservedRunning).DesiredRevision
	for _, id := range m28SidecarIDs {
		m28WaitObserved(t, first, id, drivermanager.ObservedRunning)
	}
	first.stop()

	record := m28AdoptedRecord(true)
	m28Select(t, &record, primaryEBusDriverID, false)
	if err := os.WriteFile(filepath.Join(root, portalConfigRecordFile), m28Marshal(t, record), 0o600); err != nil {
		t.Fatal(err)
	}
	before, dials := natives.counts(), env.dials.Load()
	second := env.start(m28Options(root))
	status := second.waitObserved(drivermanager.ObservedDisabled)
	for _, id := range m28SidecarIDs {
		m28WaitObserved(t, second, id, drivermanager.ObservedRunning)
	}
	time.Sleep(100 * time.Millisecond)
	if opened := env.dials.Load() - dials; opened != 0 || status.Enabled || status.DesiredState != runtimecontrol.DesiredRunning || status.DesiredRevision != intent {
		t.Fatalf("deselected ebus.primary opened %d endpoints with view %+v; want disabled, unopened, RUNNING intent at revision %d", opened, status, intent)
	}
	if registered, err := second.control.Registry().Get(primaryEBusDriverID); err != nil || registered.Driver.Disposition != runtimecontrol.DispositionDisabled {
		t.Fatalf("deselected ebus.primary registered as %+v (%v); want a disabled descriptor", registered.Driver, err)
	}
	after := natives.counts()
	for _, id := range m28SidecarIDs {
		if after[id][0]-before[id][0] != 1 || after[id][1]-before[id][1] != 1 {
			t.Fatalf("selected %s constructed/started %v -> %v; want once each", id, before[id], after[id])
		}
	}
	code, view := m28Config(t, second, http.MethodGet, nil)
	if code != http.StatusOK {
		t.Fatalf("GET %d %+v", code, view)
	}
	m28RequireRows(t, view, map[runtimecontrol.DriverID]m28Row{
		primaryEBusDriverID: {m28Bool(false), false, "disabled", "not_selected"},
		managedEEBusID:      {m28Bool(true), true, "effective", "running"},
		managedGrowattID:    {m28Bool(true), true, "effective", "running"},
		managedSunSpecID:    {m28Bool(true), true, "effective", "running"},
	})
	second.stop()
	if stored := env.storedRecord(); stored.DesiredState != runtimestate.DesiredRunning || runtimecontrol.DesiredRevision(stored.DesiredRevision) != intent {
		t.Fatalf("durable intent rewritten: %+v; want RUNNING at revision %d", stored, intent)
	}
}

// TestM28CorruptStoreKeepsShellAndWithholdsRecordControlledDrivers: a record
// that exists but does not load leaves HTTP, Portal and MCP available,
// reports an explicit closed error for every driver, withholds every
// host-enabled driver failed and unopened, and is never rewritten.
func TestM28CorruptStoreKeepsShellAndWithholdsRecordControlledDrivers(t *testing.T) {
	adopted := m28AdoptedRecord(true)
	for _, tc := range []struct {
		name   string
		record func(t *testing.T) []byte
		code   string
		status int
		lock   bool
	}{
		{name: "corrupt", record: func(*testing.T) []byte { return []byte(`{"private_marker":true}`) }, code: "invalid_record", status: http.StatusBadRequest},
		{name: "passive_member", record: func(t *testing.T) []byte {
			return bytes.Replace(m28Marshal(t, adopted), []byte(`{"format"`), []byte(`{"passive_observation":false,"format"`), 1)
		}, code: "invalid_record", status: http.StatusBadRequest},
		{name: "unsupported_format", record: func(t *testing.T) []byte {
			record := adopted
			record.Format = "helianthus.portal-config/v9"
			return m28Marshal(t, record)
		}, code: "unsupported_format", status: http.StatusBadRequest},
		{name: "unknown_driver", record: func(t *testing.T) []byte {
			record := adopted
			record.Drivers = append(append([]portalconfig.Driver{}, adopted.Drivers...), portalconfig.Driver{ID: "family.unmanaged", Enabled: true})
			return m28Marshal(t, record)
		}, code: "unsupported_selection", status: http.StatusBadRequest},
		{name: "locked", record: func(t *testing.T) []byte { return m28Marshal(t, adopted) }, code: "persistence_failed", status: http.StatusServiceUnavailable, lock: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			natives := m28Sidecars(t)
			root := t.TempDir()
			path := filepath.Join(root, portalConfigRecordFile)
			stored := tc.record(t)
			if err := os.WriteFile(path, stored, 0o600); err != nil {
				t.Fatal(err)
			}
			if tc.lock {
				ids := make([]string, 0, len(portalConfigDriverIDs))
				for _, id := range portalConfigDriverIDs {
					ids = append(ids, string(id))
				}
				holder, err := portalconfig.Open(root, portalconfig.Record{Format: portalconfig.Format, Revision: "0", Drivers: []portalconfig.Driver{}}, portalconfig.Constraints{Drivers: ids})
				if err != nil {
					t.Fatalf("lock holder: %v", err)
				}
				defer func() { _ = holder.Close() }()
			}
			env := newRCtlEnv(t)
			process := env.start(m28Options(root))

			// Shell and APIs stay up.
			bootstrap, err := rctlHTTPClient.Get(strings.TrimSuffix(process.mcpURL, "/mcp") + "/portal/api/v1/bootstrap")
			if err != nil {
				t.Fatal(err)
			}
			_ = bootstrap.Body.Close()
			if bootstrap.StatusCode != http.StatusOK {
				t.Fatalf("Portal bootstrap status %d", bootstrap.StatusCode)
			}
			if listed := process.mcpList(); len(listed.Drivers) != len(portalConfigDriverIDs) {
				t.Fatalf("MCP runtime list %+v; want every configured driver", listed)
			}
			code, view := m28Config(t, process, http.MethodGet, nil)
			if code != tc.status || view.Desired != nil || view.Error == nil || view.Error.Code != tc.code {
				t.Fatalf("GET %d %+v; want %d with %s and no desired record", code, view, tc.status, tc.code)
			}
			rows := map[runtimecontrol.DriverID]m28Row{}
			for _, id := range portalConfigDriverIDs {
				rows[id] = m28Row{nil, false, "error", tc.code}
			}
			m28RequireRows(t, view, rows)

			// Every host-enabled driver is withheld: failed, never constructed,
			// never started and never opened.
			process.waitObserved(drivermanager.ObservedFailed)
			for _, id := range m28SidecarIDs {
				m28WaitObserved(t, process, id, drivermanager.ObservedFailed)
			}
			time.Sleep(50 * time.Millisecond)
			if env.dials.Load() != 0 {
				t.Fatalf("withheld ebus.primary opened %d endpoints", env.dials.Load())
			}
			for id, count := range natives.counts() {
				if count != [2]int32{} {
					t.Fatalf("withheld %s constructed/started %v; want neither", id, count)
				}
			}
			if code, _ := m28Config(t, process, http.MethodPut, m28Marshal(t, adopted)); code == http.StatusOK {
				t.Fatal("a record that did not load accepted an edit")
			}
			process.stop()
			if data, err := os.ReadFile(path); err != nil || !bytes.Equal(data, stored) {
				t.Fatalf("stored record rewritten (%v)", err)
			}
		})
	}
}

// TestM28AbsentRecordLeavesHostConfiguration: without any record file (here an
// unprovisioned root) nothing is record-controlled; host configuration stays in
// force, the error is explicit and the root is not created.
func TestM28AbsentRecordLeavesHostConfiguration(t *testing.T) {
	natives := m28Sidecars(t)
	root := filepath.Join(t.TempDir(), "unprovisioned")
	env := newRCtlEnv(t)
	process := env.start(m28Options(root))
	process.waitObserved(drivermanager.ObservedRunning)
	for _, id := range m28SidecarIDs {
		m28WaitObserved(t, process, id, drivermanager.ObservedRunning)
	}
	code, view := m28Config(t, process, http.MethodGet, nil)
	if code != http.StatusServiceUnavailable || view.Desired != nil || view.Error == nil || view.Error.Code != "persistence_failed" {
		t.Fatalf("GET %d %+v", code, view)
	}
	rows := map[runtimecontrol.DriverID]m28Row{}
	for _, id := range portalConfigDriverIDs {
		rows[id] = m28Row{nil, true, "error", "persistence_failed"}
	}
	m28RequireRows(t, view, rows)
	for id, count := range natives.counts() {
		if count != [2]int32{1, 1} {
			t.Fatalf("host-enabled %s constructed/started %v; want once each", id, count)
		}
	}
	if _, err := os.Stat(root); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unprovisioned root was created: %v", err)
	}
}

// TestM28ConfigureNarrowsHostEligibilityOnlyAfterAdoption varies every input
// of the startup application: runtime presence, load outcome, adoption, the
// record selection and host eligibility.
func TestM28ConfigureNarrowsHostEligibilityOnlyAfterAdoption(t *testing.T) {
	for _, tc := range []struct {
		name                        string
		runtime                     *gatewayPortalConfigRuntime
		host, wantEnabled, withheld bool
	}{
		{"no_runtime_host_enabled", nil, true, true, false},
		{"no_runtime_host_disabled", nil, false, false, false},
		{"absent_record", &gatewayPortalConfigRuntime{loadErr: errors.New("absent")}, true, true, false},
		{"failed_load_host_enabled", &gatewayPortalConfigRuntime{failClosed: true}, true, true, true},
		{"failed_load_host_disabled", &gatewayPortalConfigRuntime{failClosed: true}, false, false, false},
		{"unadopted_deselected", &gatewayPortalConfigRuntime{selected: map[runtimecontrol.DriverID]bool{managedSunSpecID: false}}, true, true, false},
		{"adopted_deselected", &gatewayPortalConfigRuntime{adopted: true, selected: map[runtimecontrol.DriverID]bool{managedSunSpecID: false}}, true, false, false},
		{"adopted_selected", &gatewayPortalConfigRuntime{adopted: true, selected: map[runtimecontrol.DriverID]bool{managedSunSpecID: true}}, true, true, false},
		{"adopted_selected_host_disabled", &gatewayPortalConfigRuntime{adopted: true, selected: map[runtimecontrol.DriverID]bool{managedSunSpecID: true}}, false, false, false},
		{"adopted_without_entry", &gatewayPortalConfigRuntime{adopted: true, selected: map[runtimecontrol.DriverID]bool{}}, true, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.runtime != nil {
				tc.runtime.applied = map[runtimecontrol.DriverID]portalConfigApplication{}
			}
			enabled, err := tc.runtime.configure(managedSunSpecID, tc.host)
			if enabled != tc.wantEnabled || (err != nil) != tc.withheld {
				t.Fatalf("configure = %t, %v; want %t, withheld %t", enabled, err, tc.wantEnabled, tc.withheld)
			}
			if tc.withheld {
				var coded interface {
					ReasonCode() drivermanager.ReasonCode
				}
				if !errors.Is(err, errPortalConfigWithheld) || !errors.As(err, &coded) || coded.ReasonCode() != drivermanager.ReasonConfigInvalid {
					t.Fatalf("withholding error %v; want CONFIG_INVALID", err)
				}
			}
			if tc.runtime != nil {
				got := tc.runtime.applied[managedSunSpecID]
				if got != (portalConfigApplication{host: tc.host, enabled: tc.wantEnabled, withheld: tc.withheld}) {
					t.Fatalf("recorded application %+v", got)
				}
			}
		})
	}
}

// TestM28DriverStatesNeverClaimUnstartedDriversEffective covers every rule of
// the driver rows and every observed state of a configured-enabled driver.
func TestM28DriverStatesNeverClaimUnstartedDriversEffective(t *testing.T) {
	id := managedSunSpecID
	record := func(revision string, desired bool) *portalconfig.Record {
		return &portalconfig.Record{Format: portalconfig.Format, Revision: revision, Drivers: []portalconfig.Driver{{ID: string(id), Enabled: desired}}}
	}
	running := runtimecontrol.DriverStatus{DriverID: id, Enabled: true, DesiredState: runtimecontrol.DesiredRunning, ObservedState: drivermanager.ObservedRunning}
	disabled := runtimecontrol.DriverStatus{DriverID: id, DesiredState: runtimecontrol.DesiredRunning, ObservedState: drivermanager.ObservedDisabled, Reason: drivermanager.ReasonConfigDisabled}
	for _, tc := range []struct {
		name        string
		record      *portalconfig.Record
		application *portalConfigApplication
		status      runtimecontrol.DriverStatus
		observed    bool
		loadErr     error
		want        m28Row
	}{
		{"no_record", nil, nil, running, true, &portalconfig.Error{Code: "unsupported_format"}, m28Row{nil, true, "error", "unsupported_format"}},
		{"no_record_unknown_error", nil, nil, running, false, errors.New("private"), m28Row{nil, false, "error", "persistence_failed"}},
		{"not_composed", record("1", true), nil, running, true, nil, m28Row{m28Bool(true), true, "withheld", "evidence_unavailable"}},
		{"withheld", record("1", true), &portalConfigApplication{host: true, enabled: true, withheld: true}, running, true, &portalconfig.Error{Code: "invalid_record"}, m28Row{m28Bool(true), true, "error", "invalid_record"}},
		{"deselected_after_start", record("2", false), &portalConfigApplication{host: true, enabled: true}, running, true, nil, m28Row{m28Bool(false), true, "restart_required", "restart_required"}},
		{"selected_after_start", record("2", true), &portalConfigApplication{host: true}, disabled, true, nil, m28Row{m28Bool(true), false, "restart_required", "restart_required"}},
		{"adopted_host_disabled", record("1", true), &portalConfigApplication{}, disabled, true, nil, m28Row{m28Bool(true), false, "withheld", "unsupported_selection"}},
		{"unadopted_host_disabled", record("0", true), &portalConfigApplication{}, disabled, true, nil, m28Row{m28Bool(true), false, "withheld", "unsupported_selection"}},
		{"unadopted_host_enabled", record("0", false), &portalConfigApplication{host: true, enabled: true}, running, true, nil, m28Row{m28Bool(false), true, "withheld", "record_not_adopted"}},
		{"no_readback", record("1", true), &portalConfigApplication{host: true, enabled: true}, running, false, nil, m28Row{m28Bool(true), false, "withheld", "evidence_unavailable"}},
		{"deselected", record("1", false), &portalConfigApplication{host: true}, disabled, true, nil, m28Row{m28Bool(false), false, "disabled", "not_selected"}},
		{"deselected_inconsistent_readback", record("1", false), &portalConfigApplication{host: true}, running, true, nil, m28Row{m28Bool(false), true, "withheld", "evidence_unavailable"}},
		{"selected_running", record("1", true), &portalConfigApplication{host: true, enabled: true}, running, true, nil, m28Row{m28Bool(true), true, "effective", "running"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runtime := &gatewayPortalConfigRuntime{loadErr: tc.loadErr, applied: map[runtimecontrol.DriverID]portalConfigApplication{}}
			if tc.application != nil {
				runtime.applied[id] = *tc.application
			}
			states := runtime.driverStates(tc.record, func(got runtimecontrol.DriverID) (runtimecontrol.DriverStatus, bool) {
				if tc.record == nil && got != id {
					return runtimecontrol.DriverStatus{}, false
				}
				return tc.status, tc.observed
			})
			want := map[runtimecontrol.DriverID]m28Row{id: tc.want}
			if tc.record == nil {
				for _, other := range portalConfigDriverIDs {
					if other != id {
						want[other] = m28Row{nil, false, "error", tc.want.reason}
					}
				}
			}
			m28RequireRows(t, m28View{State: struct {
				Drivers []portal.ConfigDriverState `json:"drivers"`
			}{states}}, want)
		})
	}

	// A configured-enabled driver is effective only with a RUNNING readback
	// that is enabled and not quarantined.
	runtime := &gatewayPortalConfigRuntime{applied: map[runtimecontrol.DriverID]portalConfigApplication{id: {host: true, enabled: true}}}
	for _, observed := range []drivermanager.ObservedState{drivermanager.ObservedDisabled, drivermanager.ObservedStopped, drivermanager.ObservedStarting, drivermanager.ObservedRunning, drivermanager.ObservedDegraded, drivermanager.ObservedBackoff, drivermanager.ObservedStopping, drivermanager.ObservedFailed} {
		for _, quarantined := range []bool{false, true} {
			for _, enabled := range []bool{false, true} {
				for _, desired := range []runtimecontrol.DesiredState{runtimecontrol.DesiredRunning, runtimecontrol.DesiredStopped} {
					status := runtimecontrol.DriverStatus{DriverID: id, Enabled: enabled, DesiredState: desired, ObservedState: observed, Reason: drivermanager.ReasonProviderUnavailable, SafetyQuarantined: quarantined}
					states := runtime.driverStates(record("1", true), func(runtimecontrol.DriverID) (runtimecontrol.DriverStatus, bool) { return status, true })
					healthy := observed == drivermanager.ObservedRunning && enabled && !quarantined
					got := states[0]
					if got.Effective != healthy || (got.Status == "effective") != healthy {
						t.Fatalf("readback %+v reported effective=%t status=%s reason=%s", status, got.Effective, got.Status, got.Reason)
					}
					switch {
					case !enabled:
						if got.Status != "withheld" || got.Reason != "evidence_unavailable" {
							t.Fatalf("disabled readback for an enabled application: %+v", got)
						}
					case quarantined:
						if got.Status != "error" || got.Reason != "safety_quarantined" {
							t.Fatalf("quarantined readback: %+v", got)
						}
					case observed == drivermanager.ObservedFailed:
						if got.Status != "error" || got.Reason != "provider_unavailable" {
							t.Fatalf("failed readback: %+v", got)
						}
					case healthy:
					case desired == runtimecontrol.DesiredStopped:
						if got.Status != "withheld" || got.Reason != "lifecycle_stopped" {
							t.Fatalf("stopped intent: %+v", got)
						}
					default:
						if got.Status != "pending" || got.Reason != strings.ToLower(string(observed)) {
							t.Fatalf("unstarted readback: %+v", got)
						}
					}
				}
			}
		}
	}
}

// TestM28PortalDriverSetMatchesManagedComposition keeps the editable driver
// set and its host seed aligned with the managed composition.
func TestM28PortalDriverSetMatchesManagedComposition(t *testing.T) {
	original := gatewayManagedSidecarDescriptorsFor
	t.Cleanup(func() { gatewayManagedSidecarDescriptorsFor = original })
	gatewayManagedSidecarDescriptorsFor = func(descriptors []runtimecontrol.Descriptor) []runtimecontrol.Descriptor {
		for i := range descriptors {
			descriptors[i].Adapter = runtimecontrol.NewAdapter(func(context.Context, runtimecontrol.Construction) (*m06Runtime, error) { return &m06Runtime{}, nil })
		}
		return descriptors
	}
	if !sort.SliceIsSorted(portalConfigDriverIDs, func(i, j int) bool { return portalConfigDriverIDs[i] < portalConfigDriverIDs[j] }) {
		t.Fatalf("Portal driver set %v is not bytewise sorted", portalConfigDriverIDs)
	}
	for _, enabled := range []bool{false, true} {
		cfg := ebusgateway.DefaultConfig()
		cfg.ModbusTCPConfig.Enabled = enabled
		cfg.ModbusTCPConfig.GrowattBMSRS485.Enabled = enabled
		cfg.EEBusConfig.Enabled = enabled
		sidecars := prepareManagedSidecars(context.Background(), cfg, nil, rctlGUID, nil, nil)
		composed := []runtimecontrol.DriverID{primaryEBusDriverID}
		if !portalConfigHostEnabled(cfg, primaryEBusDriverID) {
			t.Fatal("ebus.primary seed must follow its always-enabled descriptor")
		}
		for _, entry := range sidecars.entries {
			id := entry.descriptor.DriverID
			composed = append(composed, id)
			if got := portalConfigHostEnabled(cfg, id); got != (entry.descriptor.Disposition == runtimecontrol.DispositionEnabled) {
				t.Fatalf("host seed for %s = %t; composed disposition %s", id, got, entry.descriptor.Disposition)
			}
		}
		sort.Slice(composed, func(i, j int) bool { return composed[i] < composed[j] })
		if !reflect.DeepEqual(composed, portalConfigDriverIDs) {
			t.Fatalf("composed drivers %v; Portal driver set %v", composed, portalConfigDriverIDs)
		}
	}
}

// TestM28PassiveObservationAndCacheAreNotPortalOptions: the closed record has no
// passive observation or cache member, and an edit that tries to add one is
// rejected without changing the stored record.
func TestM28PassiveObservationAndCacheAreNotPortalOptions(t *testing.T) {
	var members []string
	var walk func(reflect.Type, string)
	walk = func(typ reflect.Type, prefix string) {
		for i := 0; i < typ.NumField(); i++ {
			field := typ.Field(i)
			name := prefix + strings.Split(field.Tag.Get("json"), ",")[0]
			members = append(members, name)
			inner := field.Type
			if inner.Kind() == reflect.Slice {
				inner = inner.Elem()
			}
			if inner.Kind() == reflect.Struct {
				walk(inner, name+".")
			}
		}
	}
	walk(reflect.TypeOf(portalconfig.Record{}), "")
	want := []string{"format", "revision", "drivers", "drivers.driver_id", "drivers.enabled", "sharing", "sharing.graphql", "sharing.mcp", "adapter_proxy", "output_bindings", "output_bindings.matter"}
	if !reflect.DeepEqual(members, want) {
		t.Fatalf("record members %v; want the closed set %v", members, want)
	}

	cfg := ebusgateway.DefaultConfig()
	cfg.PortalConfigRoot = t.TempDir()
	runtime := openGatewayPortalConfigRuntime(cfg)
	defer func() { _ = runtime.Close() }()
	path := filepath.Join(cfg.PortalConfigRoot, portalConfigRecordFile)
	stored, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	base, err := runtime.store.Read()
	if err != nil {
		t.Fatal(err)
	}
	body := m28Marshal(t, base)
	handler := portal.NewHandler(portal.Options{Config: runtime.callbacks(cfg, nil)})
	for _, edit := range []struct{ old, new string }{
		{`{"format"`, `{"passive_observation":false,"format"`},
		{`{"format"`, `{"cache":false,"format"`},
		{`"sharing":{`, `"sharing":{"passive_observation":false,`},
		{`"output_bindings":{`, `"output_bindings":{"cache":false,`},
	} {
		attempt := bytes.Replace(body, []byte(edit.old), []byte(edit.new), 1)
		if bytes.Equal(attempt, body) {
			t.Fatalf("edit %q did not apply", edit.new)
		}
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodPut, "/api/v1/config", bytes.NewReader(attempt)))
		var view m28View
		if err := json.Unmarshal(response.Body.Bytes(), &view); err != nil {
			t.Fatal(err)
		}
		if response.Code != http.StatusBadRequest || view.Error == nil || view.Error.Code != "invalid_record" {
			t.Fatalf("PUT with %q: %d %s", edit.new, response.Code, response.Body.String())
		}
		if data, err := os.ReadFile(path); err != nil || !bytes.Equal(data, stored) {
			t.Fatalf("rejected edit %q changed the stored record (%v)", edit.new, err)
		}
	}
	if accepted, err := runtime.store.Read(); err != nil || accepted.Revision != "0" {
		t.Fatalf("revision after rejected edits: %+v, %v", accepted, err)
	}
}

// TestM28PartialRecordFailsClosedWithoutRewrite: a record that omits a
// compiled driver does not load, and startup never rewrites or completes it,
// whether the omitted driver is host-enabled or host-disabled.
func TestM28PartialRecordFailsClosedWithoutRewrite(t *testing.T) {
	m28Sidecars(t)
	root := t.TempDir()
	record := m28AdoptedRecord(true)
	record.Revision = "3"
	record.Drivers = record.Drivers[:2]
	path := filepath.Join(root, portalConfigRecordFile)
	if err := os.WriteFile(path, m28Marshal(t, record), 0o600); err != nil {
		t.Fatal(err)
	}
	before, _ := os.ReadFile(path)
	process := newRCtlEnv(t).start(m28Options(root))
	code, view := m28Config(t, process, http.MethodGet, nil)
	if code != http.StatusBadRequest || view.Error == nil || view.Error.Code != "unsupported_selection" {
		t.Fatalf("GET %d %+v; want the partial record reported as a load error", code, view)
	}
	for _, row := range view.State.Drivers {
		if row.Status != "error" || row.Effective {
			t.Fatalf("driver row %+v; a partial record must withhold drivers", row)
		}
	}
	if after, _ := os.ReadFile(path); !bytes.Equal(before, after) {
		t.Fatal("partial record was rewritten")
	}
}
