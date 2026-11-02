package mcp

// Issue #1375: the optional recovery member of the runtime driver object.
//
// helianthus.runtime.list and helianthus.runtime.get publish `recovery` only
// while the driver is in BACKOFF with a schedule: the attempt number, its due
// time, the closed failure class and the address-free cause. Every other state
// has no member at all (never null, never empty), and the driver object stays
// closed. The contract is the public MCP page. testdata/
// runtime_control_recovery_contract.json carries its two synthetic data
// examples, and the contractcheck package renders the public checker's rule for
// the driver object, which was compared with the checker itself.

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/drivermanager"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimecontrol"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimecontrol/contractcheck"
)

var rcRecoveryDue = time.Date(2000, time.January, 1, 0, 0, 8, 0, time.UTC)

// rcBackoffStatus is the BACKOFF driver of the contract's example.
func rcBackoffStatus(mutate func(*runtimecontrol.DriverStatus)) runtimecontrol.DriverStatus {
	status := rcStatus("ebus.primary", drivermanager.ObservedBackoff, drivermanager.ReasonRetryScheduled, runtimecontrol.DesiredRunning, 7, 29)
	status.NextAttempt, status.NextAttemptUTC = 3, rcRecoveryDue
	status.LastFailureClass, status.LastFailureCause = drivermanager.FailureDialFailed, "dial: connection refused"
	if mutate != nil {
		mutate(&status)
	}
	return status
}

func rcSingleDriverCatalog(status runtimecontrol.DriverStatus) runtimecontrol.Catalog {
	return runtimecontrol.Catalog{Revision: status.Revision, Drivers: []runtimecontrol.DriverStatus{status}}
}

func rcRecoveryFixtures(t *testing.T) map[string]any {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", "runtime_control_recovery_contract.json"))
	if err != nil {
		t.Fatal(err)
	}
	value, err := contractcheck.Decode(raw)
	if err != nil {
		t.Fatal(err)
	}
	return value.(map[string]any)
}

// rcReadData calls a read tool and returns its decoded data, with numbers kept
// as literals.
func rcReadData(t *testing.T, catalog runtimecontrol.Catalog, tool, args string) map[string]any {
	t.Helper()
	server, _ := rcServer(t, &rcFake{catalog: catalog})
	got := rcHTTP(t, server, rcParams(tool, args))
	if got.isError || got.err != nil {
		t.Fatalf("%s answered an error: %v", tool, got.err)
	}
	value, err := contractcheck.Decode(got.data)
	if err != nil {
		t.Fatalf("%s data: %v", tool, err)
	}
	return value.(map[string]any)
}

// rcListedDriver and rcGotDriver return the driver object of one call.
func rcListedDriver(t *testing.T, catalog runtimecontrol.Catalog) map[string]any {
	t.Helper()
	data := rcReadData(t, catalog, RuntimeControlListTool, `{}`)
	drivers := data["drivers"].([]any)
	if len(drivers) != 1 {
		t.Fatalf("list drivers = %d; want 1", len(drivers))
	}
	return drivers[0].(map[string]any)
}

func rcGotDriver(t *testing.T, catalog runtimecontrol.Catalog) map[string]any {
	t.Helper()
	data := rcReadData(t, catalog, RuntimeControlGetTool, `{"DriverID":"ebus.primary"}`)
	return data["driver"].(map[string]any)
}

func TestRuntimeControlRecoveryMatchesTheContractFixtures(t *testing.T) {
	fixtures := rcRecoveryFixtures(t)

	// list: a BACKOFF driver with the member.
	listed := rcReadData(t, rcSingleDriverCatalog(rcBackoffStatus(nil)), RuntimeControlListTool, `{}`)
	if want := fixtures["list_backoff_with_recovery"]; !reflect.DeepEqual(listed, want) {
		t.Fatalf("list data = %v\nwant the contract example %v", listed, want)
	}
	// get: the same driver after it recovered, without the member.
	recovered := rcStatus("ebus.primary", drivermanager.ObservedRunning, drivermanager.ReasonNone, runtimecontrol.DesiredRunning, 7, 30)
	got := rcReadData(t, rcSingleDriverCatalog(recovered), RuntimeControlGetTool, `{"DriverID":"ebus.primary"}`)
	if want := fixtures["get_running_without_recovery"]; !reflect.DeepEqual(got, want) {
		t.Fatalf("get data = %v\nwant the contract example %v", got, want)
	}
	// get of the BACKOFF driver carries the same member as list.
	backoff := rcReadData(t, rcSingleDriverCatalog(rcBackoffStatus(nil)), RuntimeControlGetTool, `{"DriverID":"ebus.primary"}`)
	listedDriver := fixtures["list_backoff_with_recovery"].(map[string]any)["drivers"].([]any)[0]
	if !reflect.DeepEqual(backoff["driver"], listedDriver) || backoff["catalog_revision"] != "29" {
		t.Fatalf("get data = %v; want the list example's driver at catalog revision 29", backoff)
	}
	for _, driver := range []any{listed["drivers"].([]any)[0], got["driver"], backoff["driver"]} {
		if !contractcheck.Driver(driver) {
			t.Fatalf("driver %v is refused by the contract's rule", driver)
		}
	}
}

// Every observed state is built with a leftover schedule and failure: only the
// state decides.
func TestRuntimeControlRecoveryIsPresentOnlyInBackoff(t *testing.T) {
	states := []struct {
		state  drivermanager.ObservedState
		reason drivermanager.ReasonCode
	}{
		{drivermanager.ObservedDisabled, drivermanager.ReasonConfigDisabled},
		{drivermanager.ObservedStopped, drivermanager.ReasonOperatorStopped},
		{drivermanager.ObservedStarting, drivermanager.ReasonStartRequested},
		{drivermanager.ObservedRunning, drivermanager.ReasonNone},
		{drivermanager.ObservedDegraded, drivermanager.ReasonCapabilityDegraded},
		{drivermanager.ObservedBackoff, drivermanager.ReasonRetryScheduled},
		{drivermanager.ObservedStopping, drivermanager.ReasonStopRequested},
		{drivermanager.ObservedFailed, drivermanager.ReasonRetryExhausted},
		// The reason RETRY_SCHEDULED alone never admits the member.
		{drivermanager.ObservedRunning, drivermanager.ReasonRetryScheduled},
		{drivermanager.ObservedFailed, drivermanager.ReasonRetryScheduled},
	}
	for _, tc := range states {
		status := rcBackoffStatus(func(s *runtimecontrol.DriverStatus) { s.ObservedState, s.Reason = tc.state, tc.reason })
		catalog := rcSingleDriverCatalog(status)
		for name, driver := range map[string]map[string]any{"list": rcListedDriver(t, catalog), "get": rcGotDriver(t, catalog)} {
			member, present := driver["recovery"]
			if want := tc.state == drivermanager.ObservedBackoff; present != want {
				t.Errorf("%s %s/%s: recovery present = %t (%v); want %t", name, tc.state, tc.reason, present, member, want)
			}
			if driver["observed_state"] != string(tc.state) || !contractcheck.Driver(driver) {
				t.Errorf("%s %s/%s: driver %v is refused by the contract's rule", name, tc.state, tc.reason, driver)
			}
		}
	}
}

// A BACKOFF without a published schedule omits the member; the contract allows it.
func TestRuntimeControlRecoveryNeedsAPublishedSchedule(t *testing.T) {
	for name, mutate := range map[string]func(*runtimecontrol.DriverStatus){
		"no attempt":         func(s *runtimecontrol.DriverStatus) { s.NextAttempt = 0 },
		"no due time":        func(s *runtimecontrol.DriverStatus) { s.NextAttemptUTC = time.Time{} },
		"no schedule":        func(s *runtimecontrol.DriverStatus) { s.NextAttempt, s.NextAttemptUTC = 0, time.Time{} },
		"year out of format": func(s *runtimecontrol.DriverStatus) { s.NextAttemptUTC = time.Date(12000, 1, 1, 0, 0, 0, 0, time.UTC) },
	} {
		driver := rcListedDriver(t, rcSingleDriverCatalog(rcBackoffStatus(mutate)))
		if _, present := driver["recovery"]; present || driver["observed_state"] != "BACKOFF" || !contractcheck.Driver(driver) {
			t.Errorf("%s: driver %v; want a BACKOFF driver without the member", name, driver)
		}
	}
}

// Each member of the recovery object follows its own source field.
func TestRuntimeControlRecoveryMembersFollowTheirSource(t *testing.T) {
	recoveryOf := func(mutate func(*runtimecontrol.DriverStatus)) map[string]any {
		driver := rcGotDriver(t, rcSingleDriverCatalog(rcBackoffStatus(mutate)))
		recovery, ok := driver["recovery"].(map[string]any)
		if !ok || !contractcheck.Driver(driver) {
			t.Fatalf("driver %v has no valid recovery member", driver)
		}
		return recovery
	}
	for _, attempt := range []int{1, 2, 6, 1000} {
		if got := recoveryOf(func(s *runtimecontrol.DriverStatus) { s.NextAttempt = attempt })["attempt"]; string(got.(json.Number)) != jsonInt(attempt) {
			t.Errorf("attempt %d published as %v", attempt, got)
		}
	}
	cet := time.FixedZone("CET", 3600)
	for due, want := range map[time.Time]string{
		rcRecoveryDue:                                  "2000-01-01T00:00:08Z",
		time.Date(2000, 1, 1, 1, 0, 8, 0, cet):         "2000-01-01T00:00:08Z",
		rcRecoveryDue.Add(250 * time.Millisecond):      "2000-01-01T00:00:08.25Z",
		time.Date(2026, 10, 26, 7, 30, 1, 1, time.UTC): "2026-10-26T07:30:01.000000001Z",
	} {
		if got := recoveryOf(func(s *runtimecontrol.DriverStatus) { s.NextAttemptUTC = due })["next_attempt_at"]; got != want {
			t.Errorf("due %s published as %v; want %s", due, got, want)
		}
	}
	for _, class := range []drivermanager.FailureClass{drivermanager.FailureDialFailed, drivermanager.FailureTimeout, drivermanager.FailureTransportClosed, drivermanager.FailureOther} {
		if got := recoveryOf(func(s *runtimecontrol.DriverStatus) { s.LastFailureClass = class })["last_failure_class"]; got != string(class) {
			t.Errorf("class %s published as %v", class, got)
		}
	}
	for _, class := range []drivermanager.FailureClass{"", "dial_failed", "NONE"} {
		if got := recoveryOf(func(s *runtimecontrol.DriverStatus) { s.LastFailureClass = class })["last_failure_class"]; got != "OTHER" {
			t.Errorf("class %q published as %v; want OTHER", class, got)
		}
	}
	for _, cause := range []string{"", "i/o timeout", "read: connection reset by peer", strings.Repeat("x", 160)} {
		if got := recoveryOf(func(s *runtimecontrol.DriverStatus) { s.LastFailureCause = cause })["last_failure_cause"]; got != cause {
			t.Errorf("cause %q published as %v", cause, got)
		}
	}
}

func jsonInt(n int) string {
	raw, _ := json.Marshal(n)
	return string(raw)
}

// The address shapes the contract's checker refuses, each placed in the cause
// of a driver in BACKOFF: the published text is within the contract and names
// none of the addresses.
func TestRuntimeControlRecoveryCauseIsAddressFree(t *testing.T) {
	cases := []struct{ cause, forbidden string }{
		{"dial tcp 192.0.2.10: connection refused", "192.0.2.10"},
		{"dial tcp 192.0.2.10:9000: connection refused", "192.0.2.10"},
		{"refused by adapter:9000", "adapter:9000"},
		{"dial tcp [2001:db8::1]: connection refused", "2001:db8"},
		{"adapter aa:bb:cc:dd:ee:ff did not answer", "aa:bb"},
		{"dial tcp://adapter: connection refused", "tcp://"},
		{"dial example.invalid: no such host", "example.invalid"},
		{"login as user@adapter refused", "user@adapter"},
		{"open /dev/ttyUSB0: no such device", "ttyUSB0"},
		{"open C:\\com3: access denied", "com3"},
		{"lookup gateway: no such host", "gateway"},
		{"dial ebus-adapter: connection refused", "ebus-adapter"},
		{"dial tcp ebus-adapter: connection refused", "ebus-adapter"},
		{"connect to gateway: refused", "gateway"},
		{"resolve gateway: failed", "gateway"},
		{strings.Repeat("y", 400), ""},
		{strings.Repeat("a", 150) + " host:12345678", "host:"},
		{"line one\nline two \u00e9", "\n"},
	}
	for _, tc := range cases {
		for _, tool := range []string{RuntimeControlListTool, RuntimeControlGetTool} {
			catalog := rcSingleDriverCatalog(rcBackoffStatus(func(s *runtimecontrol.DriverStatus) { s.LastFailureCause = tc.cause }))
			var driver map[string]any
			if tool == RuntimeControlListTool {
				driver = rcListedDriver(t, catalog)
			} else {
				driver = rcGotDriver(t, catalog)
			}
			recovery, ok := driver["recovery"].(map[string]any)
			if !ok || !contractcheck.Driver(driver) {
				t.Errorf("%s cause %q: driver %v is refused by the contract's rule", tool, tc.cause, driver)
				continue
			}
			published, _ := recovery["last_failure_cause"].(string)
			if len(published) > 160 || (tc.forbidden != "" && strings.Contains(published, tc.forbidden)) {
				t.Errorf("%s cause %q published as %q", tool, tc.cause, published)
			}
		}
	}
}

// The closed object: no member beyond the contract's, at the driver or in the
// recovery object, and the rule the tests hold the output to refuses one.
func TestRuntimeControlRecoveryObjectStaysClosed(t *testing.T) {
	driver := rcListedDriver(t, rcSingleDriverCatalog(rcBackoffStatus(nil)))
	if !contractcheck.Driver(driver) {
		t.Fatalf("driver %v is refused by the contract's rule", driver)
	}
	if got := rcMemberNames(driver); !reflect.DeepEqual(got, []string{"desired_revision", "desired_state", "driver_id", "enabled", "observed_state", "reason", "recovery", "revision", "safety_quarantined"}) {
		t.Fatalf("driver members = %v", got)
	}
	if got := rcMemberNames(driver["recovery"].(map[string]any)); !reflect.DeepEqual(got, []string{"attempt", "last_failure_cause", "last_failure_class", "next_attempt_at"}) {
		t.Fatalf("recovery members = %v", got)
	}
	// Negative controls: the rule has teeth.
	withTop := cloneObject(driver)
	withTop["extra"] = true
	withInner := cloneObject(driver)
	withInner["recovery"].(map[string]any)["extra"] = true
	withoutInner := cloneObject(driver)
	delete(withoutInner["recovery"].(map[string]any), "last_failure_cause")
	running := cloneObject(driver)
	running["observed_state"] = "RUNNING"
	for name, bad := range map[string]map[string]any{"driver member": withTop, "recovery member": withInner, "missing recovery member": withoutInner, "recovery in RUNNING": running} {
		if contractcheck.Driver(bad) {
			t.Errorf("the contract rule accepts %s: %v", name, bad)
		}
	}
	// The wire carries no null or empty recovery for a driver outside BACKOFF.
	server, _ := rcServer(t, &rcFake{catalog: rcSingleDriverCatalog(rcStatus("ebus.primary", drivermanager.ObservedRunning, drivermanager.ReasonNone, runtimecontrol.DesiredRunning, 7, 30))})
	text := rcHTTP(t, server, rcParams(RuntimeControlListTool, `{}`)).text
	if strings.Contains(text, "recovery") {
		t.Fatalf("a RUNNING driver mentions recovery on the wire: %s", text)
	}
}

func rcMemberNames(object map[string]any) []string {
	names := make([]string, 0, len(object))
	for name := range object {
		names = append(names, name)
	}
	for i := range names {
		for j := i + 1; j < len(names); j++ {
			if names[j] < names[i] {
				names[i], names[j] = names[j], names[i]
			}
		}
	}
	return names
}

func cloneObject(object map[string]any) map[string]any {
	out := make(map[string]any, len(object))
	for key, value := range object {
		if nested, ok := value.(map[string]any); ok {
			value = cloneObject(nested)
		}
		out[key] = value
	}
	return out
}

// Every driver of every pinned output obeys the contract's rule, and the
// accepted-mutation and conflict data never carry the member.
func TestRuntimeControlOutputsObeyTheContractDriverRule(t *testing.T) {
	sawMember := false
	for _, tc := range rcOutputCases() {
		if tc.params != "" {
			continue
		}
		server, _ := tc.server(t)
		got := rcHTTP(t, server, tc.wire())
		if got.isError || len(got.data) == 0 || string(got.data) == "null" {
			continue
		}
		value, err := contractcheck.Decode(got.data)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		data := value.(map[string]any)
		var drivers []any
		switch {
		case data["drivers"] != nil:
			drivers = data["drivers"].([]any)
		case data["driver"] != nil:
			drivers = []any{data["driver"]}
		default:
			if strings.Contains(string(got.data), "recovery") {
				t.Errorf("%s: mutation data mentions recovery: %s", tc.name, got.data)
			}
		}
		for _, driver := range drivers {
			object := driver.(map[string]any)
			if object["desired_revision"] == "0" {
				// A withheld view reads "0" where the contract's example rule wants a
				// real revision; the member rule still holds.
				object = cloneObject(object)
				object["desired_revision"], object["revision"] = "1", "1"
			}
			if !contractcheck.Driver(object) {
				t.Errorf("%s: driver %v is refused by the contract's rule", tc.name, driver)
			}
			if _, ok := object["recovery"]; ok {
				sawMember = true
			}
		}
	}
	if !sawMember {
		t.Fatal("no pinned output carries a recovery member")
	}
}

func TestRuntimeControlRefusalDataNeverCarriesRecovery(t *testing.T) {
	server, _ := rcServer(t, &rcFake{
		catalog: rcSingleDriverCatalog(rcBackoffStatus(nil)),
		decide: func(context.Context, runtimecontrol.Method, runtimecontrol.Request) (runtimecontrol.DesiredView, error) {
			return runtimecontrol.DesiredView{}, &runtimecontrol.Refusal{Code: runtimecontrol.RefusalRevisionConflict, Row: 6, Reason: "expected revision differs",
				View: &runtimecontrol.DesiredView{DesiredState: runtimecontrol.DesiredRunning, DesiredRevision: 9}}
		},
	})
	got := rcHTTP(t, server, rcParams(RuntimeControlStopTool, rcMutation))
	if !got.isError || !strings.Contains(string(got.data), `"desired_revision":"9"`) || strings.Contains(string(got.data), "recovery") {
		t.Fatalf("REVISION_CONFLICT data = %s", got.data)
	}
	accepted, _ := rcServer(t, &rcFake{catalog: rcSingleDriverCatalog(rcBackoffStatus(nil)), decide: rcView(runtimecontrol.DesiredRunning, 8)})
	if got := rcHTTP(t, accepted, rcParams(RuntimeControlRestartTool, rcMutation)); got.isError || strings.Contains(string(got.data), "recovery") {
		t.Fatalf("accepted mutation data = %s", got.data)
	}
}
