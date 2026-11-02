package main

// Issue #1375: while ebus.primary is in BACKOFF, helianthus.runtime.list,
// helianthus.runtime.get and the Portal driver row publish the recovery
// schedule: the number of the attempt scheduled, when it is due, the closed
// class of the failure that scheduled it and that failure's address-free cause.
// Once the driver runs again every surface drops it.
//
// The tests drive the production composition against the scripted loopback
// endpoint of issue1367_recovery_test.go, with the production retry policy and
// the injected manager clock. They read the ordinary MCP endpoint of the
// composed process over HTTP, hold each reading to the public contract's rule
// for the driver object, and compare the Portal row with the same schedule.

import (
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/drivermanager"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/portalconfig"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimecontrol"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimecontrol/contractcheck"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
	"github.com/Project-Helianthus/helianthus-ebusgateway/portal"
)

// issue1375Pause holds a retry wait so the surfaces can be read at a stable
// point: the schedule is published before the wait runs.
type issue1375Pause struct {
	paused chan int
	resume chan struct{}
	done   chan struct{}
	once   sync.Once
}

func newIssue1375Pause(t *testing.T) *issue1375Pause {
	t.Helper()
	pause := &issue1375Pause{paused: make(chan int, 16), resume: make(chan struct{}, 16), done: make(chan struct{})}
	t.Cleanup(pause.open)
	return pause
}

// open ends every hold. A test defers it, so a failed test frees the retry wait
// before the process shuts down.
func (pause *issue1375Pause) open() { pause.once.Do(func() { close(pause.done) }) }

func (pause *issue1375Pause) hold(attempt int) {
	pause.paused <- attempt
	select {
	case <-pause.resume:
	case <-pause.done:
	}
}

func (pause *issue1375Pause) next(t *testing.T, want int, logs *issue1367Logs) {
	t.Helper()
	select {
	case got := <-pause.paused:
		if got != want {
			t.Fatalf("held attempt %d; want %d", got, want)
		}
	case <-time.After(3 * rctlBound):
		t.Fatalf("no retry wait reached attempt %d; recovery log:\n%s", want, issue1367RecoveryLines(logs.text()))
	}
}

func (pause *issue1375Pause) release() { pause.resume <- struct{}{} }

// issue1375Drivers reads helianthus.runtime.list and helianthus.runtime.get over
// the ordinary MCP endpoint and returns the ebus.primary driver object of each.
// Every other listed driver must be free of the member, and each reading must
// be the contract's driver object.
func issue1375Drivers(t *testing.T, process *rctlProcess) (listed, got map[string]any) {
	t.Helper()
	list := process.tool(mcp.RuntimeControlListTool, `{}`)
	if list.isError {
		t.Fatalf("list = %s %s", list.code, list.message)
	}
	value, err := contractcheck.Decode(list.data)
	if err != nil {
		t.Fatal(err)
	}
	for _, driver := range value.(map[string]any)["drivers"].([]any) {
		object := driver.(map[string]any)
		if object["driver_id"] == primaryEBusDriverID {
			listed = object
			continue
		}
		if _, ok := object["recovery"]; ok {
			t.Fatalf("driver %v carries a recovery member", object["driver_id"])
		}
	}
	get := process.tool(mcp.RuntimeControlGetTool, `{"DriverID":"`+primaryEBusDriverID+`"}`)
	if get.isError {
		t.Fatalf("get = %s %s", get.code, get.message)
	}
	value, err = contractcheck.Decode(get.data)
	if err != nil {
		t.Fatal(err)
	}
	got = value.(map[string]any)["driver"].(map[string]any)
	for name, driver := range map[string]map[string]any{"list": listed, "get": got} {
		if driver == nil || !contractcheck.Driver(driver) {
			t.Fatalf("%s driver %v is refused by the contract's rule", name, driver)
		}
	}
	return listed, got
}

// issue1375ProcessPortalRow is the Portal row of ebus.primary as the host callback
// builds it from the service's readback.
func issue1375ProcessPortalRow(t *testing.T, process *rctlProcess) portal.ConfigDriverState {
	t.Helper()
	runtime := &gatewayPortalConfigRuntime{applied: map[runtimecontrol.DriverID]portalConfigApplication{primaryEBusDriverID: {host: true, enabled: true}}}
	record := &portalconfig.Record{Format: portalconfig.Format, Revision: "1", Drivers: []portalconfig.Driver{{ID: string(primaryEBusDriverID), Enabled: true}}}
	states := runtime.driverStates(record, func(id runtimecontrol.DriverID) (runtimecontrol.DriverStatus, bool) {
		status, err := process.control.Service().Get(id)
		return status, err == nil
	})
	if len(states) != 1 {
		t.Fatalf("Portal rows = %d; want 1", len(states))
	}
	return states[0]
}

// issue1375RequireRecovery asserts the member on both tools and the Portal row
// at the retry the manager is holding.
func issue1375RequireRecovery(t *testing.T, process *rctlProcess, attempt int, class, cause string) {
	t.Helper()
	schedule := process.control.driver.Snapshot().Retry
	if schedule == nil || schedule.Attempt != attempt {
		t.Fatalf("manager schedule = %+v; want attempt %d", schedule, attempt)
	}
	due := schedule.NotBeforeUTC.UTC().Format(time.RFC3339Nano)
	listed, got := issue1375Drivers(t, process)
	for name, driver := range map[string]map[string]any{"list": listed, "get": got} {
		recovery, ok := driver["recovery"].(map[string]any)
		if !ok || driver["observed_state"] != "BACKOFF" || driver["reason"] != "RETRY_SCHEDULED" {
			t.Fatalf("%s driver = %v; want BACKOFF with a recovery member", name, driver)
		}
		if recovery["attempt"] != json.Number(itoa(attempt)) || recovery["next_attempt_at"] != due ||
			recovery["last_failure_class"] != class || recovery["last_failure_cause"] != cause {
			t.Fatalf("%s recovery = %v; want attempt %d due %s, %s %q", name, recovery, attempt, due, class, cause)
		}
	}
	row := issue1375ProcessPortalRow(t, process)
	want := portal.ConfigDriverRecovery{Attempt: attempt, NextAttemptAt: due, LastFailureClass: class, LastFailureCause: cause}
	if row.Recovery == nil || *row.Recovery != want || row.Status != "pending" || row.Reason != "backoff" {
		t.Fatalf("Portal row = %+v (recovery %+v); want pending/backoff with %+v", row, row.Recovery, want)
	}
}

func itoa(n int) string {
	raw, _ := json.Marshal(n)
	return string(raw)
}

// issue1375RequireNoRecovery asserts that no surface publishes a schedule.
func issue1375RequireNoRecovery(t *testing.T, process *rctlProcess, state string) {
	t.Helper()
	listed, got := issue1375Drivers(t, process)
	for name, driver := range map[string]map[string]any{"list": listed, "get": got} {
		if _, ok := driver["recovery"]; ok || driver["observed_state"] != state {
			t.Fatalf("%s driver = %v; want %s without a recovery member", name, driver, state)
		}
	}
	if row := issue1375ProcessPortalRow(t, process); row.Recovery != nil {
		t.Fatalf("Portal row = %+v; want no recovery for a %s driver", row, state)
	}
}

// issue1375RequireRetryFailures asserts, for every retry the manager scheduled,
// the failure its schedule recorded and the same failure in the service view.
func issue1375RequireRetryFailures(t *testing.T, retries []issue1369Retry, want func(attempt int) drivermanager.FailureDetail) {
	t.Helper()
	for n, retry := range retries {
		attempt := n + 1
		detail := want(attempt)
		if got := retry.snapshot.Retry.LastFailure; got != detail {
			t.Errorf("retry %d manager failure = %+v; want %+v", attempt, got, detail)
		}
		if retry.view.LastFailureClass != detail.Class || retry.view.LastFailureCause != detail.Cause {
			t.Errorf("retry %d service view failure = %q/%q; want %q/%q", attempt, retry.view.LastFailureClass, retry.view.LastFailureCause, detail.Class, detail.Cause)
		}
		if view, ok := retry.view.Recovery(); !ok || view.Attempt != attempt || string(view.FailureClass) != string(detail.Class) {
			t.Errorf("retry %d recovery view = %+v, %t", attempt, view, ok)
		}
	}
}

// A connection lost after RUNNING: the first retry reports the loss by its
// closed trigger, the following ones the refused dials, and RUNNING drops the
// member.
func TestIssue1375LossRecoveryPublishesTheMemberAtEachRetry(t *testing.T) {
	for _, tc := range []struct {
		name     string
		protocol ebusgateway.TransportProtocol
		stream   bool
		loss     string
	}{
		{"adapter_direct", ebusgateway.TransportAdapterDirect, true, "connection lost (adapter_connection_lost)"},
		{"proxy_class", ebusgateway.TransportENH, false, "connection lost (transport_closed)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clock := newIssue1369Clock(issue1369NoJitter)
			withIssue1369Seams(t, clock)
			logs := captureIssue1367Logs(t)
			endpoint := newIssue1367Endpoint(t, tc.stream)
			process := startIssue1369Process(t, clock, endpoint, tc.protocol)
			issue1375RequireNoRecovery(t, process, "RUNNING")

			const healAt = 4
			pause := newIssue1375Pause(t)
			defer pause.open()
			clock.setOnRetry(func(attempt int) issue1369Action {
				switch attempt {
				case 1, 3:
					pause.hold(attempt)
				case healAt:
					endpoint.open()
				}
				return issue1369Proceed
			})
			endpoint.refuse()
			if tc.stream {
				endpoint.stall()
			} else {
				endpoint.drop()
				issue1367DetectProxyLoss(t, process, logs)
			}

			pause.next(t, 1, logs)
			issue1375RequireRecovery(t, process, 1, "TRANSPORT_CLOSED", tc.loss)
			pause.release()
			pause.next(t, 3, logs)
			issue1375RequireRecovery(t, process, 3, "DIAL_FAILED", "connect: connection refused")
			pause.release()

			waitIssue1367Status(t, process, drivermanager.ObservedRunning, drivermanager.ReasonNone, logs)
			issue1375RequireNoRecovery(t, process, "RUNNING")
			retries := clock.recorded()
			if len(retries) != healAt {
				t.Fatalf("scheduled retries = %d; want %d", len(retries), healAt)
			}
			issue1375RequireRetryFailures(t, retries, func(attempt int) drivermanager.FailureDetail {
				if attempt == 1 {
					return drivermanager.FailureDetail{Class: drivermanager.FailureTransportClosed, Cause: tc.loss}
				}
				return drivermanager.FailureDetail{Class: drivermanager.FailureDialFailed, Cause: "connect: connection refused"}
			})
			issue1369RequireNoEndpointInRecoveryLines(t, logs, endpoint)
		})
	}
}

// An adapter that is down when the process starts: the opening START is
// admitted into the recovery, and every retry of it reports the refused dial.
func TestIssue1375BootRecoveryPublishesTheMemberAtEachRetry(t *testing.T) {
	for _, tc := range []struct {
		name     string
		protocol ebusgateway.TransportProtocol
		stream   bool
	}{
		{"adapter_direct", ebusgateway.TransportAdapterDirect, true},
		{"proxy_class", ebusgateway.TransportENH, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clock := newIssue1369Clock(issue1369NoJitter)
			logs := captureIssue1367Logs(t)
			withIssue1371Seams(t, clock, logs)
			endpoint := newIssue1367Endpoint(t, tc.stream)
			endpoint.refuse()

			const healAt = 3
			pause := newIssue1375Pause(t)
			defer pause.open()
			clock.setOnRetry(func(attempt int) issue1369Action {
				switch attempt {
				case 1, 2:
					pause.hold(attempt)
				case healAt:
					endpoint.open()
				}
				return issue1369Proceed
			})
			process := startIssue1371Process(t, clock, endpoint, tc.protocol)
			pause.next(t, 1, logs)
			issue1375RequireRecovery(t, process, 1, "DIAL_FAILED", "connect: connection refused")
			pause.release()
			pause.next(t, 2, logs)
			issue1375RequireRecovery(t, process, 2, "DIAL_FAILED", "connect: connection refused")
			pause.release()

			waitIssue1367Status(t, process, drivermanager.ObservedRunning, drivermanager.ReasonNone, logs)
			issue1375RequireNoRecovery(t, process, "RUNNING")
			retries := clock.recorded()
			if len(retries) != healAt {
				t.Fatalf("scheduled retries = %d; want %d", len(retries), healAt)
			}
			issue1375RequireRetryFailures(t, retries, func(int) drivermanager.FailureDetail {
				return drivermanager.FailureDetail{Class: drivermanager.FailureDialFailed, Cause: "connect: connection refused"}
			})
			issue1369RequireNoEndpointInRecoveryLines(t, logs, endpoint)
		})
	}
}

// An adapter that is reachable but fails the INIT handshake is OTHER, and one
// that accepts and drops the connection is TRANSPORT_CLOSED; neither names the
// endpoint. A silent INIT outlasts the opening START's budget, so its TIMEOUT
// class is covered by the class mapping tests.
func TestIssue1375HandshakeFailuresPublishTheirClass(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(*issue1367Endpoint)
		class string
		cause string
	}{
		{"malformed INIT answer", func(e *issue1367Endpoint) { reply := issue1373MalformedInit; e.initReply.Store(&reply) },
			"OTHER", "ebus: payload does not match expected schema"},
		{"rejected INIT", func(e *issue1367Endpoint) { reply := issue1373RejectedInit; e.initReply.Store(&reply) },
			"OTHER", "ebus: payload does not match expected schema"},
		{"connection dropped", func(e *issue1367Endpoint) { e.closeOnAccept.Store(true) },
			"TRANSPORT_CLOSED", "ebus: transport connection closed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clock := newIssue1369Clock(issue1369NoJitter)
			logs := captureIssue1367Logs(t)
			withIssue1371Seams(t, clock, logs)
			endpoint := newIssue1367Endpoint(t, false)
			tc.setup(endpoint)
			pause := newIssue1375Pause(t)
			defer pause.open()
			clock.setOnRetry(func(attempt int) issue1369Action {
				switch attempt {
				case 1:
					pause.hold(attempt)
				case 2:
					endpoint.initReply.Store(nil)
					endpoint.closeOnAccept.Store(false)
				}
				return issue1369Proceed
			})
			process := startIssue1371Process(t, clock, endpoint, ebusgateway.TransportENH)
			pause.next(t, 1, logs)
			issue1375RequireRecovery(t, process, 1, tc.class, tc.cause)
			pause.release()
			waitIssue1367Status(t, process, drivermanager.ObservedRunning, drivermanager.ReasonNone, logs)
			issue1375RequireNoRecovery(t, process, "RUNNING")
			issue1369RequireNoEndpointInRecoveryLines(t, logs, endpoint)
			for _, line := range strings.Split(logs.text(), "\n") {
				if strings.Contains(line, "recovery") && strings.Contains(line, endpoint.address) {
					t.Fatalf("recovery line names the endpoint: %q", line)
				}
			}
		})
	}
}
