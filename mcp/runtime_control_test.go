package mcp

// G5 PR 2 (issue #1227): the five helianthus.runtime tools on the ordinary
// MCP registration and dispatch path, as thin delegates of the internal
// desired-state service (D4).
//
// The service here is a consumer-side fake that returns the service's own
// value types and refusals, and decodes mutation bodies with the service's
// own closed decoder (runtimecontrol.ParseRequest). Every MCP output is
// compared with what the same fake answers through the service-shaped call.
// The durable service with its real store is exercised through ordinary MCP
// in cmd/gateway/runtime_control_test.go.
//
// Goldens: testdata/runtime_control_tools.json (tools/list schemas) and
// testdata/runtime_control_outputs.json (call envelopes). Regenerate after a
// reviewed intentional change with
//
//	UPDATE=1 go test ./mcp -run 'TestRuntimeControl(Tools|Outputs)Golden' -count=1
//
// meta.data_timestamp is replaced by a placeholder after it parses as RFC
// 3339; meta.data_hash is recomputed over the exact serialized data bytes,
// asserted, and replaced by a placeholder, so no golden carries a digest.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/drivermanager"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimecontrol"
	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
)

const (
	runtimeControlToolsGolden   = "runtime_control_tools.json"
	runtimeControlOutputsGolden = "runtime_control_outputs.json"
)

var runtimeControlToolNames = []string{
	RuntimeControlListTool,
	RuntimeControlGetTool,
	RuntimeControlStartTool,
	RuntimeControlStopTool,
	RuntimeControlRestartTool,
}

// rcForbiddenMembers may never appear as a request or result member name.
var rcForbiddenMembers = []string{
	"idempotency_key", "request_id", "operation_id", "disposition", "completed",
	"outcome", "principal", "authorization", "authority", "receipt",
}

// rcFake is the scripted service. decide is consulted only after the
// service's own closed decoder accepted the body.
type rcFake struct {
	mu       sync.Mutex
	catalog  runtimecontrol.Catalog
	getErr   error
	decide   func(ctx context.Context, method runtimecontrol.Method, req runtimecontrol.Request) (runtimecontrol.DesiredView, error)
	lists    int
	gets     int
	controls int
	decides  int
	deadline []time.Time
	missing  int
	methods  []runtimecontrol.Method
	bodies   []string
}

func (f *rcFake) List() runtimecontrol.Catalog {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lists++
	out := f.catalog
	out.Drivers = append([]runtimecontrol.DriverStatus(nil), f.catalog.Drivers...)
	return out
}

func (f *rcFake) Get(id runtimecontrol.DriverID) (runtimecontrol.DriverStatus, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.gets++
	if f.getErr != nil {
		return runtimecontrol.DriverStatus{}, f.getErr
	}
	for _, status := range f.catalog.Drivers {
		if status.DriverID == id {
			return status, nil
		}
	}
	return runtimecontrol.DriverStatus{}, &runtimecontrol.Refusal{Code: runtimecontrol.RefusalNotFound, Row: 2, Reason: "driver id noncanonical or not registered"}
}

func (f *rcFake) Control(ctx context.Context, method runtimecontrol.Method, body []byte) (runtimecontrol.DesiredView, error) {
	f.mu.Lock()
	f.controls++
	f.methods = append(f.methods, method)
	f.bodies = append(f.bodies, string(body))
	if deadline, ok := ctx.Deadline(); ok {
		f.deadline = append(f.deadline, deadline)
	} else {
		f.missing++
	}
	decide := f.decide
	f.mu.Unlock()
	req, err := runtimecontrol.ParseRequest(body)
	if err != nil {
		return runtimecontrol.DesiredView{}, err
	}
	f.mu.Lock()
	f.decides++
	f.mu.Unlock()
	if decide == nil {
		return runtimecontrol.DesiredView{DesiredState: runtimecontrol.DesiredRunning, DesiredRevision: req.ExpectedRevision + 1}, nil
	}
	return decide(ctx, method, req)
}

func (f *rcFake) counts() (lists, gets, controls, decides int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lists, f.gets, f.controls, f.decides
}

func rcStatus(id string, observed drivermanager.ObservedState, reason drivermanager.ReasonCode, desired runtimecontrol.DesiredState, desiredRevision, revision uint64) runtimecontrol.DriverStatus {
	return runtimecontrol.DriverStatus{
		DriverID:        runtimecontrol.DriverID(id),
		Enabled:         observed != drivermanager.ObservedDisabled,
		DesiredState:    desired,
		DesiredRevision: runtimecontrol.DesiredRevision(desiredRevision),
		ObservedState:   observed,
		Reason:          reason,
		Revision:        runtimecontrol.CatalogRevision(revision),
	}
}

// rcCatalog is a synthetic two-driver view at catalog revision 29.
func rcCatalog() runtimecontrol.Catalog {
	return runtimecontrol.Catalog{Revision: 29, EventCursor: 41, Drivers: []runtimecontrol.DriverStatus{
		rcStatus("ebus.primary", drivermanager.ObservedBackoff, drivermanager.ReasonRetryScheduled, runtimecontrol.DesiredRunning, 7, 29),
		rcStatus("fixture.second", drivermanager.ObservedStopped, drivermanager.ReasonNone, runtimecontrol.DesiredStopped, 5, 29),
	}}
}

func rcRefusal(code runtimecontrol.RefusalCode, row int, reason string) func(context.Context, runtimecontrol.Method, runtimecontrol.Request) (runtimecontrol.DesiredView, error) {
	return func(context.Context, runtimecontrol.Method, runtimecontrol.Request) (runtimecontrol.DesiredView, error) {
		return runtimecontrol.DesiredView{}, &runtimecontrol.Refusal{Code: code, Row: row, Reason: reason}
	}
}

func rcView(state runtimecontrol.DesiredState, revision uint64) func(context.Context, runtimecontrol.Method, runtimecontrol.Request) (runtimecontrol.DesiredView, error) {
	return func(context.Context, runtimecontrol.Method, runtimecontrol.Request) (runtimecontrol.DesiredView, error) {
		return runtimecontrol.DesiredView{DesiredState: state, DesiredRevision: runtimecontrol.DesiredRevision(revision)}, nil
	}
}

func rcServer(t *testing.T, service RuntimeControlService) (*Server, *testInvoker) {
	t.Helper()
	invoker := &testInvoker{}
	server, err := NewServer(registry.NewDeviceRegistry(nil), invoker)
	if err != nil {
		t.Fatalf("NewServer() = %v", err)
	}
	if err := RegisterRuntimeControlTools(server, service); err != nil {
		t.Fatalf("RegisterRuntimeControlTools() = %v", err)
	}
	return server, invoker
}

func rcParams(tool, args string) string {
	if args == "" {
		return `{"name":"` + tool + `"}`
	}
	return `{"name":"` + tool + `","arguments":` + args + `}`
}

// rcEnvelope is one decoded call result with the raw data bytes kept.
type rcEnvelope struct {
	isError bool
	data    json.RawMessage
	meta    map[string]any
	err     map[string]any
	text    string
	rpcErr  *rpcError
}

func rcDecode(t *testing.T, result any, rpcErr *rpcError) rcEnvelope {
	t.Helper()
	if rpcErr != nil {
		return rcEnvelope{rpcErr: rpcErr}
	}
	resultMap, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("tools/call result = %T", result)
	}
	var text string
	switch content := resultMap["content"].(type) {
	case []map[string]any:
		text, _ = content[0]["text"].(string)
	case []any:
		if len(content) != 1 {
			t.Fatalf("content = %#v", content)
		}
		text, _ = content[0].(map[string]any)["text"].(string)
	default:
		t.Fatalf("content type = %T", resultMap["content"])
	}
	var envelope struct {
		Meta  map[string]any  `json:"meta"`
		Data  json.RawMessage `json:"data"`
		Error map[string]any  `json:"error"`
	}
	if err := json.Unmarshal([]byte(text), &envelope); err != nil {
		t.Fatalf("envelope text is not JSON: %v", err)
	}
	isError, _ := resultMap["isError"].(bool)
	return rcEnvelope{isError: isError, data: envelope.Data, meta: envelope.Meta, err: envelope.Error, text: text}
}

// rcCall dispatches tools/call in process with the given context, the path
// every ordinary MCP transport reaches through Handler.
func rcCall(t *testing.T, ctx context.Context, server *Server, params string) rcEnvelope {
	t.Helper()
	result, rpcErr := server.dispatch(ctx, "tools/call", json.RawMessage(params))
	return rcDecode(t, result, rpcErr)
}

// rcHTTP calls through the ordinary HTTP handler.
func rcHTTP(t *testing.T, server *Server, params string) rcEnvelope {
	t.Helper()
	res := doRPC(t, server.Handler(), rpcRequest{JSONRPC: "2.0", ID: 1, Method: "tools/call", Params: json.RawMessage(params)})
	return rcDecode(t, res.Result, res.Error)
}

// rcNormalized calls through Handler and normalizes the envelope for a
// golden: the timestamp must parse, the hash must equal SHA-256 of the exact
// serialized data bytes, and both are then replaced by placeholders.
func rcNormalized(t *testing.T, server *Server, params string) map[string]any {
	t.Helper()
	res := doRPC(t, server.Handler(), rpcRequest{JSONRPC: "2.0", ID: 1, Method: "tools/call", Params: json.RawMessage(params)})
	if res.Error != nil {
		return map[string]any{"rpc_error": map[string]any{"code": res.Error.Code, "message": res.Error.Message}}
	}
	got := rcDecode(t, res.Result, nil)
	var envelope map[string]any
	if err := json.Unmarshal([]byte(got.text), &envelope); err != nil {
		t.Fatal(err)
	}
	meta := envelope["meta"].(map[string]any)
	timestamp, _ := meta["data_timestamp"].(string)
	if _, err := time.Parse(time.RFC3339Nano, timestamp); err != nil {
		t.Fatalf("data_timestamp %q is not RFC 3339: %v", timestamp, err)
	}
	meta["data_timestamp"] = "<data_timestamp>"
	sum := sha256.Sum256(got.data)
	if meta["data_hash"] != hex.EncodeToString(sum[:]) {
		t.Fatalf("data_hash is not SHA-256 of the serialized data for %s", params)
	}
	meta["data_hash"] = "<sha256(data) verified>"
	envelope["data"] = got.data
	if got.isError {
		envelope["is_error"] = true
	}
	return envelope
}

func rcErrorCode(env rcEnvelope) string {
	if env.err == nil {
		return ""
	}
	code, _ := env.err["code"].(string)
	return code
}

func rcGolden(t *testing.T, name string, value any) {
	t.Helper()
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(value); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join("testdata", name)
	if os.Getenv("UPDATE") == "1" {
		if err := os.WriteFile(path, buffer.Bytes(), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden %s: %v (regenerate with UPDATE=1 after review)", name, err)
	}
	if !bytes.Equal(buffer.Bytes(), want) {
		var gotCases, wantCases map[string]json.RawMessage
		if json.Unmarshal(buffer.Bytes(), &gotCases) == nil && json.Unmarshal(want, &wantCases) == nil {
			for key, gotCase := range gotCases {
				gotCompact, _ := json.Marshal(gotCase)
				wantCompact, _ := json.Marshal(wantCases[key])
				if !bytes.Equal(gotCompact, wantCompact) {
					t.Errorf("%s case %s drifted\nwant: %s\n got: %s", name, key, wantCompact, gotCompact)
				}
			}
			for key := range wantCases {
				if _, ok := gotCases[key]; !ok {
					t.Errorf("%s case %s is no longer produced", name, key)
				}
			}
		}
		t.Fatalf("%s mismatch; regenerate with UPDATE=1 after review", name)
	}
}

func rcToolsList(t *testing.T, handler http.Handler) []any {
	t.Helper()
	res := doRPC(t, handler, rpcRequest{JSONRPC: "2.0", ID: 1, Method: "tools/list"})
	if res.Error != nil {
		t.Fatalf("tools/list error = %+v", res.Error)
	}
	tools, ok := res.Result.(map[string]any)["tools"].([]any)
	if !ok {
		t.Fatalf("tools/list tools = %#v", res.Result)
	}
	return tools
}

func rcToolIndex(tools []any, name string) int {
	for i, raw := range tools {
		if tool, ok := raw.(map[string]any); ok && tool["name"] == name {
			return i
		}
	}
	return -1
}

// TestRuntimeControlToolsGolden pins the five tool schemas as listed by
// ordinary tools/list, and their position: contiguous, in contract order,
// after every default tool.
func TestRuntimeControlToolsGolden(t *testing.T) {
	server, _ := rcServer(t, &rcFake{catalog: rcCatalog()})
	tools := rcToolsList(t, server.Handler())
	first := rcToolIndex(tools, RuntimeControlListTool)
	if first < 0 {
		t.Fatal("helianthus.runtime.list is not listed")
	}
	defaults := len(defaultServerTools())
	if first < defaults {
		t.Fatalf("lifecycle tools start at %d; want after the %d default tools", first, defaults)
	}
	ordered := make([]any, 0, len(runtimeControlToolNames))
	for offset, name := range runtimeControlToolNames {
		if got := rcToolIndex(tools, name); got != first+offset {
			t.Fatalf("%s listed at %d; want %d (contiguous, contract order)", name, got, first+offset)
		}
		ordered = append(ordered, tools[first+offset])
	}
	rcGolden(t, runtimeControlToolsGolden, map[string]any{"tools": ordered})
}

// rcOutputCase is one pinned call: the scripted service state and the
// arguments as sent on the wire.
type rcOutputCase struct {
	name    string
	tool    string
	args    string
	params  string
	catalog *runtimecontrol.Catalog
	getErr  error
	decide  func(context.Context, runtimecontrol.Method, runtimecontrol.Request) (runtimecontrol.DesiredView, error)
	nilSvc  bool
	service string
}

const rcMutation = `{"DriverID":"ebus.primary","ExpectedRevision":"7"}`

func rcOutputCases() []rcOutputCase {
	empty := runtimecontrol.Catalog{Revision: 0}
	withheld := runtimecontrol.Catalog{Revision: 0, Drivers: []runtimecontrol.DriverStatus{
		rcStatus("ebus.primary", drivermanager.ObservedFailed, drivermanager.ReasonInternalError, runtimecontrol.DesiredStopped, 0, 0),
	}}
	withheld.Drivers[0].Enabled = false
	quarantined := rcCatalog()
	quarantined.Drivers[0].ObservedState = drivermanager.ObservedFailed
	quarantined.Drivers[0].Reason = drivermanager.ReasonCloseUnconfirmed
	quarantined.Drivers[0].SafetyQuarantined = true
	disabled := rcCatalog()
	disabled.Drivers[1] = rcStatus("fixture.second", drivermanager.ObservedDisabled, drivermanager.ReasonConfigDisabled, runtimecontrol.DesiredRunning, 5, 29)
	conflict := func(context.Context, runtimecontrol.Method, runtimecontrol.Request) (runtimecontrol.DesiredView, error) {
		return runtimecontrol.DesiredView{}, &runtimecontrol.Refusal{Code: runtimecontrol.RefusalRevisionConflict, Row: 6, Reason: "expected revision differs",
			View: &runtimecontrol.DesiredView{DesiredState: runtimecontrol.DesiredRunning, DesiredRevision: 9}}
	}
	wrappedDeadline := func(context.Context, runtimecontrol.Method, runtimecontrol.Request) (runtimecontrol.DesiredView, error) {
		return runtimecontrol.DesiredView{}, fmt.Errorf("readback wait: %w", context.DeadlineExceeded)
	}
	unresolved := func(context.Context, runtimecontrol.Method, runtimecontrol.Request) (runtimecontrol.DesiredView, error) {
		return runtimecontrol.DesiredView{}, fmt.Errorf("decide: %w", runtimecontrol.ErrReadbackUnresolved)
	}
	return []rcOutputCase{
		{name: "list_two_drivers", tool: RuntimeControlListTool, args: `{}`, service: "coherent view at catalog revision 29, drivers in bytewise order"},
		{name: "list_omitted_arguments", tool: RuntimeControlListTool, args: "", service: "same view; an omitted arguments member reads as {}"},
		{name: "list_empty_catalog", tool: RuntimeControlListTool, args: `{}`, catalog: &empty, service: "no registered driver: drivers is [] and the unissued revision is 0"},
		{name: "list_withheld_view", tool: RuntimeControlListTool, args: `{}`, catalog: &withheld, service: "control state unreadable: every driver withheld FAILED at revision 0"},
		{name: "list_quarantined_driver", tool: RuntimeControlListTool, args: `{}`, catalog: &quarantined, service: "unconfirmed close: FAILED/CLOSE_UNCONFIRMED with the safety quarantine"},
		{name: "list_disabled_driver_visible", tool: RuntimeControlListTool, args: `{}`, catalog: &disabled, service: "a disabled driver stays listed"},
		{name: "list_rejects_member", tool: RuntimeControlListTool, args: `{"consistency":"LIVE"}`, service: "never consulted"},
		{name: "list_rejects_null", tool: RuntimeControlListTool, args: `null`, service: "never consulted"},
		{name: "list_rejects_array", tool: RuntimeControlListTool, args: `[]`, service: "never consulted"},
		{name: "list_rejects_duplicate_arguments", tool: RuntimeControlListTool, params: `{"name":"helianthus.runtime.list","arguments":{},"arguments":{}}`, service: "never consulted"},
		{name: "list_service_unavailable", tool: RuntimeControlListTool, args: `{}`, nilSvc: true, service: "no control service composed"},
		{name: "get_driver", tool: RuntimeControlGetTool, args: `{"DriverID":"ebus.primary"}`, service: "one cached entry; its revision is the catalog revision"},
		{name: "get_unregistered", tool: RuntimeControlGetTool, args: `{"DriverID":"ebus.secondary"}`, service: "NOT_FOUND row 2"},
		{name: "get_noncanonical", tool: RuntimeControlGetTool, args: `{"DriverID":"Ebus.Primary"}`, service: "NOT_FOUND row 2"},
		{name: "get_rejects_missing", tool: RuntimeControlGetTool, args: `{}`, service: "never consulted"},
		{name: "get_rejects_additional", tool: RuntimeControlGetTool, args: `{"DriverID":"ebus.primary","ExpectedRevision":"7"}`, service: "never consulted"},
		{name: "get_rejects_non_string", tool: RuntimeControlGetTool, args: `{"DriverID":7}`, service: "never consulted"},
		{name: "get_rejects_duplicate", tool: RuntimeControlGetTool, args: `{"DriverID":"ebus.primary","DriverID":"ebus.primary"}`, service: "never consulted"},
		{name: "get_rejects_case", tool: RuntimeControlGetTool, args: `{"driverid":"ebus.primary"}`, service: "never consulted"},
		{name: "start_accepted", tool: RuntimeControlStartTool, args: rcMutation, decide: rcView(runtimecontrol.DesiredRunning, 8), service: "durably accepted at desired revision 8"},
		{name: "start_same_desired_noop", tool: RuntimeControlStartTool, args: rcMutation, decide: rcView(runtimecontrol.DesiredRunning, 7), service: "desired RUNNING already: no-op at revision 7"},
		{name: "stop_accepted", tool: RuntimeControlStopTool, args: rcMutation, decide: rcView(runtimecontrol.DesiredStopped, 8), service: "durably accepted at desired revision 8"},
		{name: "stop_same_desired_noop", tool: RuntimeControlStopTool, args: rcMutation, decide: rcView(runtimecontrol.DesiredStopped, 7), service: "desired STOPPED already: no-op at revision 7"},
		{name: "restart_accepted", tool: RuntimeControlRestartTool, args: rcMutation, decide: rcView(runtimecontrol.DesiredRunning, 8), service: "durably accepted: RUNNING kept, new revision 8"},
		{name: "mutation_rejects_missing_revision", tool: RuntimeControlStartTool, args: `{"DriverID":"ebus.primary"}`, service: "row 1 by the service decoder; no decision"},
		{name: "mutation_rejects_idempotency_key", tool: RuntimeControlStartTool, args: `{"DriverID":"ebus.primary","ExpectedRevision":"7","idempotency_key":"k"}`, service: "row 1 by the service decoder; no decision"},
		{name: "mutation_rejects_zero_revision", tool: RuntimeControlStopTool, args: `{"DriverID":"ebus.primary","ExpectedRevision":"0"}`, service: "row 1 by the service decoder; no decision"},
		{name: "mutation_rejects_leading_zero", tool: RuntimeControlStopTool, args: `{"DriverID":"ebus.primary","ExpectedRevision":"07"}`, service: "row 1 by the service decoder; no decision"},
		{name: "mutation_rejects_number_revision", tool: RuntimeControlRestartTool, args: `{"DriverID":"ebus.primary","ExpectedRevision":7}`, service: "row 1 by the service decoder; no decision"},
		{name: "mutation_rejects_overflow", tool: RuntimeControlRestartTool, args: `{"DriverID":"ebus.primary","ExpectedRevision":"18446744073709551616"}`, service: "row 1 by the service decoder; no decision"},
		{name: "mutation_rejects_duplicate", tool: RuntimeControlStartTool, args: `{"DriverID":"ebus.primary","DriverID":"ebus.primary","ExpectedRevision":"7"}`, service: "duplicate member detected before decoding; never consulted"},
		{name: "mutation_rejects_non_object", tool: RuntimeControlStartTool, args: `"ebus.primary"`, service: "row 1 by the service decoder; no decision"},
		{name: "mutation_not_found", tool: RuntimeControlStartTool, args: `{"DriverID":"ebus.secondary","ExpectedRevision":"7"}`, decide: rcRefusal(runtimecontrol.RefusalNotFound, 2, "driver id noncanonical or not registered"), service: "NOT_FOUND row 2"},
		{name: "mutation_control_state_unusable", tool: RuntimeControlStartTool, args: rcMutation, decide: rcRefusal(runtimecontrol.RefusalFailedPrecondition, 3, "runtime_control failed to load: CORRUPT"), service: "FAILED_PRECONDITION row 3"},
		{name: "mutation_readback_pending", tool: RuntimeControlStartTool, args: rcMutation, decide: rcRefusal(runtimecontrol.RefusalUnavailable, 3, "ambiguous persistence: sync /synthetic/state: input/output error"), service: "UNAVAILABLE row 3"},
		{name: "mutation_disabled", tool: RuntimeControlStartTool, args: rcMutation, decide: rcRefusal(runtimecontrol.RefusalFailedPrecondition, 4, "driver disabled by configuration"), service: "FAILED_PRECONDITION row 4"},
		{name: "mutation_deadline_exceeded", tool: RuntimeControlStopTool, args: rcMutation, decide: rcRefusal(runtimecontrol.RefusalDeadlineExceeded, 5, "deadline expired during the serialization wait"), service: "DEADLINE_EXCEEDED row 5"},
		{name: "mutation_revision_conflict", tool: RuntimeControlStopTool, args: rcMutation, decide: conflict, service: "REVISION_CONFLICT row 6 with the cached desired view"},
		{name: "restart_while_stopped", tool: RuntimeControlRestartTool, args: rcMutation, decide: rcRefusal(runtimecontrol.RefusalFailedPrecondition, 6, "RESTART requires desired RUNNING"), service: "FAILED_PRECONDITION row 6"},
		{name: "mutation_quarantine", tool: RuntimeControlStartTool, args: rcMutation, decide: rcRefusal(runtimecontrol.RefusalFailedPrecondition, 7, "safety quarantine blocks admission"), service: "FAILED_PRECONDITION row 7"},
		{name: "mutation_transition_busy", tool: RuntimeControlRestartTool, args: rcMutation, decide: rcRefusal(runtimecontrol.RefusalTransitionBusy, 8, "typed transition slots are occupied"), service: "TRANSITION_BUSY row 8"},
		{name: "mutation_resource_exhausted", tool: RuntimeControlStartTool, args: rcMutation, decide: rcRefusal(runtimecontrol.RefusalResourceExhausted, 9, "driver record limit reached"), service: "RESOURCE_EXHAUSTED row 9"},
		{name: "mutation_counter_exhausted", tool: RuntimeControlStartTool, args: rcMutation, decide: rcRefusal(runtimecontrol.RefusalCounterExhausted, 10, "mark or catalog increment would overflow"), service: "COUNTER_EXHAUSTED row 10"},
		{name: "mutation_precommit_write_failure", tool: RuntimeControlStartTool, args: rcMutation, decide: rcRefusal(runtimecontrol.RefusalUnavailable, 0, "pre-commit write failure: open /synthetic/state.tmp: permission denied"), service: "UNAVAILABLE outside the ordered rows: nothing accepted"},
		{name: "mutation_deadline_error", tool: RuntimeControlStartTool, args: rcMutation, decide: wrappedDeadline, service: "context deadline outside the refusal type"},
		{name: "mutation_unknown_outcome", tool: RuntimeControlStartTool, args: rcMutation, decide: unresolved, service: "error outside the refusal table: outcome unknown"},
		{name: "mutation_service_unavailable", tool: RuntimeControlStopTool, args: rcMutation, nilSvc: true, service: "no control service composed"},
		{name: "mutation_service_unavailable_invalid", tool: RuntimeControlStopTool, args: `{"DriverID":"ebus.primary"}`, nilSvc: true, service: "row 1 precedes the unavailable service"},
	}
}

func (tc rcOutputCase) server(t *testing.T) (*Server, *rcFake) {
	fake := &rcFake{catalog: rcCatalog(), getErr: tc.getErr, decide: tc.decide}
	if tc.catalog != nil {
		fake.catalog = *tc.catalog
	}
	if tc.nilSvc {
		server, _ := rcServer(t, nil)
		return server, fake
	}
	server, _ := rcServer(t, fake)
	return server, fake
}

func (tc rcOutputCase) wire() string {
	if tc.params != "" {
		return tc.params
	}
	return rcParams(tc.tool, tc.args)
}

// TestRuntimeControlOutputsGolden pins every output shape and error mapping
// of the five tools, identical across repeated calls.
func TestRuntimeControlOutputsGolden(t *testing.T) {
	observed := map[string]any{}
	for _, tc := range rcOutputCases() {
		server, _ := tc.server(t)
		first := rcNormalized(t, server, tc.wire())
		second := rcNormalized(t, server, tc.wire())
		firstBytes, _ := json.Marshal(first)
		secondBytes, _ := json.Marshal(second)
		if !bytes.Equal(firstBytes, secondBytes) {
			t.Errorf("%s is not deterministic across identical calls", tc.name)
		}
		var arguments any = json.RawMessage(tc.args)
		if tc.args == "" {
			arguments = nil
		}
		entry := map[string]any{"tool": tc.tool, "arguments": arguments, "service": tc.service, "envelope": first}
		if tc.params != "" {
			entry["params"] = json.RawMessage(tc.params)
			delete(entry, "arguments")
		}
		observed[tc.name] = entry
	}
	rcGolden(t, runtimeControlOutputsGolden, observed)
}

// TestRuntimeControlMCPServiceEquivalence: for every scripted case the MCP
// envelope equals what the same service answers through the service-shaped
// call: success data is the service's DesiredView or cached projection, a
// refusal keeps its code, retryable column and (for REVISION_CONFLICT only)
// the cached desired view, and no refusal reason reaches the wire.
func TestRuntimeControlMCPServiceEquivalence(t *testing.T) {
	for _, tc := range rcOutputCases() {
		if tc.nilSvc || tc.params != "" {
			continue
		}
		t.Run(tc.name, func(t *testing.T) {
			server, fake := tc.server(t)
			got := rcHTTP(t, server, tc.wire())
			if got.rpcErr != nil {
				t.Fatalf("JSON-RPC error %+v; want a tool envelope", got.rpcErr)
			}
			if got.meta["consistency"].(map[string]any)["mode"] != "LIVE" {
				t.Fatalf("consistency = %v; want LIVE", got.meta["consistency"])
			}
			contract := got.meta["contract"].(map[string]any)
			if contract["name"] != "helianthus-ebus-mcp" || contract["major"] != float64(1) || contract["minor"] != float64(0) {
				t.Fatalf("contract = %v", contract)
			}
			args := json.RawMessage(tc.args)
			if tc.args == "" {
				args = json.RawMessage(`{}`)
			}
			var wantData any
			var wantErr error
			twin := &rcFake{catalog: fake.catalog, getErr: fake.getErr, decide: fake.decide}
			switch tc.tool {
			case RuntimeControlListTool:
				if _, ok := runtimeControlArguments(args); !ok {
					wantErr = &runtimecontrol.Refusal{Code: runtimecontrol.RefusalInvalidArgument, Row: 1}
				} else {
					wantData = runtimeControlListData(twin.List())
				}
			case RuntimeControlGetTool:
				members, ok := runtimeControlArguments(args, "DriverID")
				if !ok {
					wantErr = &runtimecontrol.Refusal{Code: runtimecontrol.RefusalInvalidArgument, Row: 1}
				} else if status, err := twin.Get(runtimecontrol.DriverID(members["DriverID"])); err != nil {
					wantErr = err
				} else {
					wantData = runtimeControlGetDataV1{CatalogRevision: fmt.Sprint(uint64(status.Revision)), Driver: runtimeControlDriver(status)}
				}
			default:
				method, _ := runtimeControlMethod(tc.tool)
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				view, err := twin.Control(ctx, method, args)
				cancel()
				if eebusV1JSONHasDuplicateKeys(args) {
					err = &runtimecontrol.Refusal{Code: runtimecontrol.RefusalInvalidArgument, Row: 1}
				}
				if err != nil {
					wantErr = err
				} else {
					wantData = view
				}
			}
			if wantErr == nil {
				if got.isError || got.err != nil {
					t.Fatalf("error %v; want success", got.err)
				}
				wantBytes, _ := json.Marshal(wantData)
				if !bytes.Equal(got.data, wantBytes) {
					t.Fatalf("data = %s; want %s", got.data, wantBytes)
				}
				return
			}
			if !got.isError {
				t.Fatal("isError = false for a refusal")
			}
			var refusal *runtimecontrol.Refusal
			wantCode, wantRetriable := "", false
			wantView := json.RawMessage(`null`)
			if errors.As(wantErr, &refusal) {
				wantCode, wantRetriable = string(refusal.Code), refusal.Retryable()
				if refusal.Code == runtimecontrol.RefusalRevisionConflict && refusal.View != nil {
					wantView, _ = json.Marshal(*refusal.View)
				}
			} else if errors.Is(wantErr, context.DeadlineExceeded) {
				wantCode, wantRetriable = string(runtimecontrol.RefusalDeadlineExceeded), true
			} else {
				wantCode, wantRetriable = runtimeControlInternalCode, false
			}
			if rcErrorCode(got) != wantCode || got.err["retriable"] != wantRetriable || got.err["source_layer"] != "gateway" {
				t.Fatalf("error = %v; want code %s retriable %v from gateway", got.err, wantCode, wantRetriable)
			}
			if !bytes.Equal(got.data, wantView) {
				t.Fatalf("refusal data = %s; want %s", got.data, wantView)
			}
			message, _ := got.err["message"].(string)
			if refusal != nil {
				if want := runtimeControlRefusalError(refusal).message; message != want {
					t.Fatalf("message %q; want the categorical %q", message, want)
				}
				if refusal.Reason != "" && refusal.Reason != message && strings.Contains(message, refusal.Reason) {
					t.Fatalf("message %q carries the raw refusal reason", message)
				}
			}
			if strings.Contains(message, "/") || strings.Contains(message, "runtimecontrol:") || len(message) > 120 {
				t.Fatalf("message %q is not bounded, categorical and sanitized", message)
			}
		})
	}
}

// TestRuntimeControlStrictDecoding: unknown, missing, duplicate and
// non-string members, non-object arguments and trailing JSON are a
// tool-level INVALID_ARGUMENT that never reaches a decision.
func TestRuntimeControlStrictDecoding(t *testing.T) {
	invalid := map[string][]string{
		RuntimeControlListTool: {
			`{"x":"y"}`, `{"DriverID":"ebus.primary"}`, `[]`, `"s"`, `1`, `true`, `null`, `{"":""}`,
		},
		RuntimeControlGetTool: {
			`{}`, `{"DriverID":1}`, `{"DriverID":null}`, `{"DriverID":true}`, `{"DriverID":{"id":"ebus.primary"}}`,
			`{"DriverID":["ebus.primary"]}`, `{"driverid":"ebus.primary"}`, `{"DriverID":"ebus.primary","x":"y"}`,
			`{"DriverID":"ebus.primary","DriverID":"ebus.primary"}`, `["DriverID"]`, `null`,
		},
	}
	mutation := []string{
		`{}`, `{"DriverID":"ebus.primary"}`, `{"ExpectedRevision":"7"}`,
		`{"DriverID":"ebus.primary","ExpectedRevision":"7","idempotency_key":"k"}`,
		`{"DriverID":"ebus.primary","ExpectedRevision":"7","request_id":"r"}`,
		`{"DriverID":"ebus.primary","ExpectedRevision":"7","principal":"p"}`,
		`{"DriverID":"ebus.primary","ExpectedRevision":"7","deadline":"1s"}`,
		`{"DriverID":"ebus.primary","ExpectedRevision":"0"}`, `{"DriverID":"ebus.primary","ExpectedRevision":"01"}`,
		`{"DriverID":"ebus.primary","ExpectedRevision":"+1"}`, `{"DriverID":"ebus.primary","ExpectedRevision":" 1"}`,
		`{"DriverID":"ebus.primary","ExpectedRevision":"1x"}`, `{"DriverID":"ebus.primary","ExpectedRevision":"-1"}`,
		`{"DriverID":"ebus.primary","ExpectedRevision":"18446744073709551616"}`,
		`{"DriverID":"ebus.primary","ExpectedRevision":7}`, `{"DriverID":"ebus.primary","ExpectedRevision":null}`,
		`{"DriverID":7,"ExpectedRevision":"7"}`, `{"driverID":"ebus.primary","ExpectedRevision":"7"}`,
		`{"DriverID":"ebus.primary","ExpectedRevision":"7","ExpectedRevision":"7"}`,
		`[]`, `"x"`, `null`, `7`,
	}
	for _, name := range []string{RuntimeControlStartTool, RuntimeControlStopTool, RuntimeControlRestartTool} {
		invalid[name] = mutation
	}
	for tool, cases := range invalid {
		for _, args := range cases {
			fake := &rcFake{catalog: rcCatalog()}
			server, _ := rcServer(t, fake)
			got := rcHTTP(t, server, rcParams(tool, args))
			if got.rpcErr != nil || rcErrorCode(got) != "INVALID_ARGUMENT" || !got.isError || got.err["retriable"] != false || string(got.data) != "null" {
				t.Errorf("%s %s = rpc %+v error %v data %s; want tool-level INVALID_ARGUMENT, null data", tool, args, got.rpcErr, got.err, got.data)
			}
			if _, gets, _, decides := fake.counts(); gets != 0 || decides != 0 {
				t.Errorf("%s %s reached the service (get %d, decide %d)", tool, args, gets, decides)
			}
		}
	}

	// Trailing JSON: within a well-formed JSON-RPC body the arguments value
	// cannot carry trailing data, and a body with trailing data is refused
	// by the JSON-RPC layer before dispatch. The tool decoders refuse it
	// themselves too.
	fake := &rcFake{catalog: rcCatalog()}
	server, _ := rcServer(t, fake)
	for tool, raw := range map[string]string{
		RuntimeControlListTool:    `{} {}`,
		RuntimeControlGetTool:     `{"DriverID":"ebus.primary"} x`,
		RuntimeControlStartTool:   rcMutation + ` {}`,
		RuntimeControlStopTool:    rcMutation + `]`,
		RuntimeControlRestartTool: rcMutation + ` "x"`,
	} {
		got := rcDecode(t, server.handleRuntimeControlRawCall(context.Background(), tool, json.RawMessage(raw), false), nil)
		if rcErrorCode(got) != "INVALID_ARGUMENT" {
			t.Errorf("%s with trailing data = %v; want INVALID_ARGUMENT", tool, got.err)
		}
	}
	_, gets, controlsBefore, decides := fake.counts()
	if gets != 0 || decides != 0 {
		t.Fatalf("trailing data reached a read or a decision (get %d, decide %d)", gets, decides)
	}
	body := `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":` + rcParams(RuntimeControlStartTool, rcMutation) + `} {}`
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(body)))
	var response rpcResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil || response.Error == nil || response.Error.Code != -32600 {
		t.Fatalf("trailing data after the JSON-RPC body = %s; want JSON-RPC -32600 before dispatch", recorder.Body.String())
	}
	if _, _, controls, _ := fake.counts(); controls != controlsBefore {
		t.Fatalf("a body with trailing data reached the service %d times", controls-controlsBefore)
	}

	// Duplicate members anywhere in the call parameters are a tool-level
	// refusal, never the generic -32602, and never reach the service.
	for _, params := range []string{
		`{"name":"helianthus.runtime.start","arguments":` + rcMutation + `,"arguments":{"DriverID":"ebus.primary","ExpectedRevision":"8"}}`,
		`{"name":"helianthus.runtime.stop","name":"helianthus.runtime.start","arguments":` + rcMutation + `}`,
	} {
		got := rcHTTP(t, server, params)
		if got.rpcErr != nil || rcErrorCode(got) != "INVALID_ARGUMENT" {
			t.Errorf("%s = rpc %+v error %v; want tool-level INVALID_ARGUMENT", params, got.rpcErr, got.err)
		}
	}
	if _, _, controls, _ := fake.counts(); controls != controlsBefore {
		t.Fatalf("duplicate call parameters reached the service %d times", controls-controlsBefore)
	}

	// The decoder in isolation.
	for raw, want := range map[string]bool{
		`{}`: true, ` {} `: true, `{} {}`: false, `{}x`: false, `{`: false, ``: false, `{"a":"b"}`: false,
	} {
		if _, ok := runtimeControlArguments(json.RawMessage(raw)); ok != want {
			t.Errorf("runtimeControlArguments(%q) = %v; want %v", raw, ok, want)
		}
	}
	if members, ok := runtimeControlArguments(json.RawMessage(`{"DriverID":"ebus.primary"}`), "DriverID"); !ok || members["DriverID"] != "ebus.primary" {
		t.Fatalf("closed get arguments refused: %v %v", members, ok)
	}
}

// TestRuntimeControlGetNotFound: noncanonical, oversized and unregistered
// IDs are NOT_FOUND from the service's cached read, with no other call.
func TestRuntimeControlGetNotFound(t *testing.T) {
	fake := &rcFake{catalog: rcCatalog()}
	server, _ := rcServer(t, fake)
	for _, id := range []string{"", "Ebus.Primary", "ebus..primary", "ebus.primary.", "1ebus", "ebus primary", strings.Repeat("a", 129), "ebus.secondary"} {
		args, _ := json.Marshal(map[string]string{"DriverID": id})
		got := rcHTTP(t, server, rcParams(RuntimeControlGetTool, string(args)))
		if rcErrorCode(got) != "NOT_FOUND" || got.err["retriable"] != false {
			t.Errorf("get %q = %v; want NOT_FOUND, not retriable", id, got.err)
		}
	}
	if lists, _, controls, _ := fake.counts(); lists != 0 || controls != 0 {
		t.Fatalf("get consulted list %d / control %d times", lists, controls)
	}
}

// TestRuntimeControlReadsUseOneCachedView: list reads the service's one
// coherent view exactly once (never assembled from get calls) and get reads
// one cached entry; neither reaches a decision, and both stay responsive
// while a mutation is blocked inside the service.
func TestRuntimeControlReadsUseOneCachedView(t *testing.T) {
	release := make(chan struct{})
	entered := make(chan struct{}, 1)
	fake := &rcFake{catalog: rcCatalog(), decide: func(ctx context.Context, _ runtimecontrol.Method, req runtimecontrol.Request) (runtimecontrol.DesiredView, error) {
		entered <- struct{}{}
		select {
		case <-release:
		case <-ctx.Done():
		}
		return runtimecontrol.DesiredView{DesiredState: runtimecontrol.DesiredStopped, DesiredRevision: req.ExpectedRevision + 1}, nil
	}}
	server, invoker := rcServer(t, fake)

	got := rcHTTP(t, server, rcParams(RuntimeControlListTool, `{}`))
	if lists, gets, controls, _ := fake.counts(); lists != 1 || gets != 0 || controls != 0 {
		t.Fatalf("list made %d list, %d get and %d control calls; want exactly one list", lists, gets, controls)
	}
	var list runtimeControlListDataV1
	if err := json.Unmarshal(got.data, &list); err != nil {
		t.Fatal(err)
	}
	ids := make([]string, 0, len(list.Drivers))
	for _, driver := range list.Drivers {
		ids = append(ids, driver.DriverID)
		if driver.Revision != list.CatalogRevision {
			t.Fatalf("entry %s revision %s differs from catalog revision %s", driver.DriverID, driver.Revision, list.CatalogRevision)
		}
	}
	if !sort.StringsAreSorted(ids) || len(ids) != 2 {
		t.Fatalf("list order = %v; want both drivers in bytewise order", ids)
	}

	done := make(chan rcEnvelope, 1)
	go func() {
		done <- rcHTTP(t, server, rcParams(RuntimeControlStopTool, rcMutation))
	}()
	<-entered
	begin := time.Now()
	rcHTTP(t, server, rcParams(RuntimeControlListTool, `{}`))
	rcHTTP(t, server, rcParams(RuntimeControlGetTool, `{"DriverID":"fixture.second"}`))
	if elapsed := time.Since(begin); elapsed > 250*time.Millisecond {
		t.Fatalf("reads took %s while a decision was blocked", elapsed)
	}
	close(release)
	if stop := <-done; stop.isError {
		t.Fatalf("blocked STOP = %v", stop.err)
	}
	if lists, gets, controls, _ := fake.counts(); lists != 2 || gets != 1 || controls != 1 {
		t.Fatalf("calls list %d get %d control %d; want 2/1/1", lists, gets, controls)
	}
	if len(invoker.calls) != 0 {
		t.Fatalf("lifecycle tools invoked native RPC: %+v", invoker.calls)
	}
}

// TestRuntimeControlDeadlines: every mutation reaches the service with a
// finite deadline. The adapter supplies a bounded one when the transport
// has none, keeps an earlier caller deadline and bounds a later one; no
// argument member carries a deadline.
func TestRuntimeControlDeadlines(t *testing.T) {
	fake := &rcFake{catalog: rcCatalog()}
	server, _ := rcServer(t, fake)
	begin := time.Now()
	rcHTTP(t, server, rcParams(RuntimeControlStartTool, rcMutation))
	early, cancelEarly := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancelEarly()
	earlyDeadline, _ := early.Deadline()
	rcCall(t, early, server, rcParams(RuntimeControlStopTool, rcMutation))
	late, cancelLate := context.WithTimeout(context.Background(), time.Hour)
	defer cancelLate()
	rcCall(t, late, server, rcParams(RuntimeControlRestartTool, rcMutation))
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.missing != 0 || len(fake.deadline) != 3 {
		t.Fatalf("deadlines %v, missing %d; want three finite deadlines", fake.deadline, fake.missing)
	}
	if bound := fake.deadline[0].Sub(begin); bound <= 2*time.Second || bound > runtimeControlCallTimeout+time.Second {
		t.Fatalf("transport without deadline got %s; want the bounded default above the serialization wait", bound)
	}
	if !fake.deadline[1].Equal(earlyDeadline) {
		t.Fatalf("earlier caller deadline %s replaced by %s", earlyDeadline, fake.deadline[1])
	}
	if bound := time.Until(fake.deadline[2]); bound > runtimeControlCallTimeout {
		t.Fatalf("later caller deadline not bounded: %s", bound)
	}
	for _, body := range fake.bodies {
		if body != rcMutation {
			t.Fatalf("service received %q; want the caller's arguments unchanged", body)
		}
	}
	want := []runtimecontrol.Method{runtimecontrol.MethodStart, runtimecontrol.MethodStop, runtimecontrol.MethodRestart}
	for i, method := range fake.methods {
		if method != want[i] {
			t.Fatalf("methods = %v; want %v", fake.methods, want)
		}
	}
}

// TestRuntimeControlHashAndEnvelopeDeterminism: data_hash depends on data
// alone, so identical views give identical hashes while data_timestamp moves.
func TestRuntimeControlHashAndEnvelopeDeterminism(t *testing.T) {
	server, _ := rcServer(t, &rcFake{catalog: rcCatalog()})
	first := rcHTTP(t, server, rcParams(RuntimeControlListTool, `{}`))
	time.Sleep(2 * time.Millisecond)
	second := rcHTTP(t, server, rcParams(RuntimeControlListTool, ``))
	if first.meta["data_hash"] != second.meta["data_hash"] || !bytes.Equal(first.data, second.data) {
		t.Fatalf("identical views hashed differently: %v / %v", first.meta["data_hash"], second.meta["data_hash"])
	}
	if first.meta["data_timestamp"] == second.meta["data_timestamp"] {
		t.Fatal("data_timestamp did not move; the hash comparison proves nothing")
	}
	sum := sha256.Sum256(first.data)
	if first.meta["data_hash"] != hex.EncodeToString(sum[:]) {
		t.Fatal("data_hash is not SHA-256 of the serialized data")
	}
	if _, ok := first.meta["consistency"].(map[string]any)["snapshot_id"]; ok {
		t.Fatal("lifecycle reads carry a snapshot id")
	}
	if first.err != nil || first.isError {
		t.Fatalf("success carries error %v", first.err)
	}
}

// TestRuntimeControlRegistrationIsOrdinary: the tools are listed by the
// ordinary tools/list with eeBUS absent, stay listed with no service, are
// not exposed in the read-only evidence scope, register once, and leave the
// default inventory untouched when not registered.
func TestRuntimeControlRegistrationIsOrdinary(t *testing.T) {
	plain, err := NewServer(registry.NewDeviceRegistry(nil), &testInvoker{})
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range runtimeControlToolNames {
		if rcToolIndex(rcToolsList(t, plain.Handler()), name) >= 0 {
			t.Fatalf("%s listed without registration", name)
		}
		if got := rcHTTP(t, plain, rcParams(name, `{}`)); got.rpcErr == nil || got.rpcErr.Code != -32602 {
			t.Fatalf("%s callable without registration: %+v", name, got)
		}
	}

	server, _ := rcServer(t, nil)
	server.eebusV1Mu.RLock()
	eebusAbsent := server.eebusV1 == nil && eebusV1NilCommandRouter(server.eebusV1CommandRouter)
	server.eebusV1Mu.RUnlock()
	if !eebusAbsent {
		t.Fatal("test server unexpectedly has an eeBUS provider or command router")
	}
	tools := rcToolsList(t, server.Handler())
	for _, name := range runtimeControlToolNames {
		if rcToolIndex(tools, name) < 0 {
			t.Fatalf("%s missing from ordinary tools/list with eeBUS absent and no service", name)
		}
	}
	for _, name := range []string{RuntimeControlListTool, RuntimeControlGetTool} {
		args := `{}`
		if name == RuntimeControlGetTool {
			args = `{"DriverID":"ebus.primary"}`
		}
		if got := rcHTTP(t, server, rcParams(name, args)); rcErrorCode(got) != "UNAVAILABLE" || got.err["retriable"] != true {
			t.Fatalf("%s without a service = %v; want retriable UNAVAILABLE", name, got.err)
		}
	}
	if err := RegisterRuntimeControlTools(server, &rcFake{}); !errors.Is(err, errRuntimeControlRegistered) {
		t.Fatalf("second registration = %v; want refusal", err)
	}
	if err := RegisterRuntimeControlTools(nil, &rcFake{}); err == nil {
		t.Fatal("registration on a nil server accepted")
	}
	var typedNil *rcFake
	nilBacked, _ := rcServer(t, typedNil)
	if got := rcHTTP(t, nilBacked, rcParams(RuntimeControlListTool, `{}`)); rcErrorCode(got) != "UNAVAILABLE" {
		t.Fatalf("typed-nil service = %v; want UNAVAILABLE", got.err)
	}

	scoped := context.WithValue(context.Background(), m8SourceScopeContextKey{}, true)
	listed, _ := server.dispatch(scoped, "tools/list", nil)
	for _, tool := range listed.(map[string]any)["tools"].([]Tool) {
		if isRuntimeControlTool(tool.Name) {
			t.Fatalf("%s exposed in the read-only evidence scope", tool.Name)
		}
	}
	if _, rpcErr := server.dispatch(scoped, "tools/call", json.RawMessage(rcParams(RuntimeControlListTool, `{}`))); rpcErr == nil {
		t.Fatal("lifecycle tool callable in the read-only evidence scope")
	}
}

// TestRuntimeControlNoForbiddenMembers: neither the schemas nor any pinned
// output name an idempotency, receipt, completion, principal or
// authorization member, and no lifecycle tool uses an experimental or new
// major namespace.
func TestRuntimeControlNoForbiddenMembers(t *testing.T) {
	var walk func(path string, value any)
	walk = func(path string, value any) {
		switch typed := value.(type) {
		case map[string]any:
			for key, child := range typed {
				for _, forbidden := range rcForbiddenMembers {
					if key == forbidden {
						t.Errorf("%s names forbidden member %q", path, key)
					}
				}
				walk(path+"."+key, child)
			}
		case []any:
			for i, child := range typed {
				walk(fmt.Sprintf("%s[%d]", path, i), child)
			}
		}
	}
	for _, name := range []string{runtimeControlToolsGolden, runtimeControlOutputsGolden} {
		raw, err := os.ReadFile(filepath.Join("testdata", name))
		if err != nil {
			t.Fatal(err)
		}
		var decoded any
		if err := json.Unmarshal(raw, &decoded); err != nil {
			t.Fatal(err)
		}
		cases, _ := decoded.(map[string]any)
		for caseName, entry := range cases {
			fields, _ := entry.(map[string]any)
			for key, value := range fields {
				if key == "arguments" || key == "params" || key == "service" {
					continue
				}
				walk(name+"."+caseName+"."+key, value)
			}
		}
	}
	for _, tool := range runtimeControlTools() {
		if !strings.HasPrefix(tool.Name, "helianthus.runtime.") || strings.Contains(tool.Name, "experimental") || strings.Contains(tool.Name, ".v2") {
			t.Fatalf("tool %q is outside the plain helianthus.runtime family", tool.Name)
		}
	}
}

func TestRuntimeControlOutsideTableAnswersInternal(t *testing.T) {
	const message = "control decision outcome is unknown; read back before deciding again"
	cases := map[string]func(context.Context, runtimecontrol.Method, runtimecontrol.Request) (runtimecontrol.DesiredView, error){
		"plain_error": func(context.Context, runtimecontrol.Method, runtimecontrol.Request) (runtimecontrol.DesiredView, error) {
			return runtimecontrol.DesiredView{}, errors.New("boom /synthetic/path")
		},
		"unknown_refusal_code": rcRefusal(runtimecontrol.RefusalCode("SYNTHETIC_UNLISTED"), 9, "boom /synthetic/path"),
	}
	for name, decide := range cases {
		t.Run(name, func(t *testing.T) {
			server, _ := rcServer(t, &rcFake{decide: decide})
			got := rcHTTP(t, server, rcParams(RuntimeControlStartTool, `{"DriverID":"ebus.primary","ExpectedRevision":"1"}`))
			if !got.isError || rcErrorCode(got) != "INTERNAL" || got.err["retriable"] != false || got.err["source_layer"] != "gateway" || string(got.data) != "null" {
				t.Fatalf("answer = %v data %s; want non-retriable INTERNAL with null data", got.err, got.data)
			}
			if got.err["message"] != message {
				t.Fatalf("message = %v; want fixed text", got.err["message"])
			}
			if raw, _ := json.Marshal(got.err); strings.Contains(string(raw), "boom") || strings.Contains(string(raw), "/synthetic") {
				t.Fatalf("error leaks raw text: %s", raw)
			}
		})
	}

	kept := map[string]struct {
		decide    func(context.Context, runtimecontrol.Method, runtimecontrol.Request) (runtimecontrol.DesiredView, error)
		code      string
		retriable bool
	}{
		"unavailable_row_0": {rcRefusal(runtimecontrol.RefusalUnavailable, 0, "x"), "UNAVAILABLE", true},
		"unavailable_row_3": {rcRefusal(runtimecontrol.RefusalUnavailable, 3, "x"), "UNAVAILABLE", true},
		"wrapped_deadline": {func(context.Context, runtimecontrol.Method, runtimecontrol.Request) (runtimecontrol.DesiredView, error) {
			return runtimecontrol.DesiredView{}, fmt.Errorf("decide: %w", context.DeadlineExceeded)
		}, "DEADLINE_EXCEEDED", true},
	}
	for name, tc := range kept {
		t.Run(name, func(t *testing.T) {
			server, _ := rcServer(t, &rcFake{decide: tc.decide})
			got := rcHTTP(t, server, rcParams(RuntimeControlStartTool, `{"DriverID":"ebus.primary","ExpectedRevision":"1"}`))
			if rcErrorCode(got) != tc.code || got.err["retriable"] != tc.retriable {
				t.Fatalf("answer = %v; want %s retriable %v", got.err, tc.code, tc.retriable)
			}
		})
	}
	// the nil-service answer is covered by the no-service test above (UNAVAILABLE, retriable).
}
