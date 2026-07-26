package mcp

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
	eebusruntime "github.com/Project-Helianthus/helianthus-eebusreg"
	"github.com/Project-Helianthus/helianthus-eebusreg/eebusraw"
)

const (
	msp06RuntimeStatusTool = "eebus.v1.runtime.status.get"
	msp06ServicesListTool  = "eebus.v1.services.list"
	msp06ServicesGetTool   = "eebus.v1.services.get"
	msp06SessionsListTool  = "eebus.v1.sessions.list"
	msp06SessionsGetTool   = "eebus.v1.sessions.get"
	msp06TopologyGetTool   = "eebus.v1.topology.get"
	msp06SnapshotCapture   = "eebus.v1.snapshot.capture"
	msp06SnapshotDrop      = "eebus.v1.snapshot.drop"
	msp06PairingStatusTool = "eebus.v1.pairing.status.get"
)

var (
	msp06ToolNames = []string{
		msp06RuntimeStatusTool,
		msp06ServicesListTool,
		msp06ServicesGetTool,
		msp06SessionsListTool,
		msp06SessionsGetTool,
		msp06TopologyGetTool,
		msp06SnapshotCapture,
		msp06SnapshotDrop,
		msp06PairingStatusTool,
	}
	msp06HashPattern  = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	msp06TokenPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{43}$`)
)

type msp06Provider struct {
	mu       sync.Mutex
	snapshot eebusruntime.SnapshotV1
	err      error
	calls    int
}

func (provider *msp06Provider) Snapshot() (eebusruntime.SnapshotV1, error) {
	provider.mu.Lock()
	defer provider.mu.Unlock()
	provider.calls++
	return provider.snapshot, provider.err
}

func (provider *msp06Provider) callCount() int {
	provider.mu.Lock()
	defer provider.mu.Unlock()
	return provider.calls
}

type msp06Clock struct {
	mu  sync.Mutex
	now time.Time
}

func (clock *msp06Clock) Now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.now
}

func (clock *msp06Clock) Advance(delta time.Duration) {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	clock.now = clock.now.Add(delta)
}

type msp06EntropyReader struct {
	mu      sync.Mutex
	counter uint64
	pending []byte
}

func (reader *msp06EntropyReader) Read(target []byte) (int, error) {
	reader.mu.Lock()
	defer reader.mu.Unlock()
	for len(reader.pending) < len(target) {
		reader.counter++
		block := sha256.Sum256([]byte(fmt.Sprintf("msp06-token-%020d", reader.counter)))
		reader.pending = append(reader.pending, block[:]...)
	}
	copy(target, reader.pending[:len(target)])
	reader.pending = reader.pending[len(target):]
	return len(target), nil
}

type msp06CallResult struct {
	envelope map[string]any
	raw      string
	isError  bool
}

func msp06TestServer(t *testing.T, provider EEBusV1Provider) (*Server, *msp06Clock) {
	t.Helper()
	server, err := NewServer(&testRegistry{entries: make(map[byte]registry.DeviceEntry)}, &testInvoker{})
	if err != nil {
		t.Fatal(err)
	}
	clock := &msp06Clock{now: time.Date(2026, 7, 19, 8, 0, 0, 123456000, time.UTC)}
	err = server.registerEEBusV1Provider(provider, eebusV1RegistrationOptions{
		now: clock.Now, entropy: &msp06EntropyReader{},
		pseudonymKey: bytes.Repeat([]byte{0x5a}, sha256.Size),
		liveTimeout:  100 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	return server, clock
}

func msp06Call(t *testing.T, handler http.Handler, tool string, arguments map[string]any) msp06CallResult {
	t.Helper()
	return msp06CallWithHeaders(t, handler, tool, arguments, nil)
}

func msp06CallWithHeaders(t *testing.T, handler http.Handler, tool string, arguments map[string]any, headers map[string]string) msp06CallResult {
	t.Helper()
	params, err := json.Marshal(map[string]any{"name": tool, "arguments": arguments})
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(rpcRequest{JSONRPC: "2.0", ID: 1, Method: "tools/call", Params: params})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/mcp", bytes.NewReader(body))
	for name, value := range headers {
		request.Header.Set(name, value)
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	var response rpcResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode RPC response: %v; body=%q", err, recorder.Body.String())
	}
	if response.Error != nil {
		t.Fatalf("tool %s RPC error = %+v", tool, response.Error)
	}
	result := msp06Map(t, response.Result, "result")
	content := msp06Slice(t, result["content"], "result.content")
	if len(content) != 1 {
		t.Fatalf("tool %s content count = %d", tool, len(content))
	}
	raw, _ := msp06Map(t, content[0], "content[0]")["text"].(string)
	var envelope map[string]any
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&envelope); err != nil {
		t.Fatalf("decode envelope: %v; text=%q", err, raw)
	}
	return msp06CallResult{envelope: envelope, raw: raw, isError: result["isError"] == true}
}

func msp06Tools(t *testing.T, server *Server) []map[string]any {
	t.Helper()
	response := doRPC(t, server.Handler(), rpcRequest{JSONRPC: "2.0", ID: 1, Method: "tools/list"})
	result := msp06Map(t, response.Result, "tools result")
	values := msp06Slice(t, result["tools"], "tools")
	tools := make([]map[string]any, 0, len(values))
	for _, value := range values {
		tools = append(tools, msp06Map(t, value, "tool"))
	}
	return tools
}

func msp06NamesWithPrefix(tools []map[string]any, prefix string) []string {
	var names []string
	for _, tool := range tools {
		if name, _ := tool["name"].(string); strings.HasPrefix(name, prefix) {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names
}

func msp06SourceID(t *testing.T, kind eebusraw.IDKind, raw string) eebusraw.RedactedID {
	t.Helper()
	id, err := eebusraw.RedactID(kind, raw)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func msp06Snapshot(t *testing.T, runtimeLabel string) eebusruntime.SnapshotV1 {
	t.Helper()
	observed := time.Date(2026, 7, 19, 7, 59, 58, 987654321, time.UTC)
	earlier := observed.Add(-time.Minute)
	shipZ, shipA := "service-z", "service-a"
	description := "test device"
	available := true
	draft := eebusruntime.SnapshotV1{
		Meta: eebusruntime.SnapshotMetaV1{
			Contract: eebusruntime.SnapshotContractV1,
			Runtime:  msp06SourceID(t, eebusraw.IDKindPeer, runtimeLabel),
			LocalSKI: msp06SourceID(t, eebusraw.IDKindLocalSKI, "local-ski-secret"),
			MaskTier: eebusraw.MaskTier("raw"), CapturedAt: observed.Add(time.Second), DataTimestamp: observed,
		},
		Status: eebusruntime.RuntimeObservationV1{State: eebusruntime.ObservedRuntimeStateV1Ready},
		Pairing: []eebusruntime.PairingObservationV1{
			{RemoteSKI: "remote-z", State: eebusraw.PairingStatePaired, Since: earlier},
			{RemoteSKI: "remote-a", State: eebusraw.PairingStateUnpaired, Since: earlier},
		},
		Services: []eebusruntime.ServiceV1{
			{SKI: "ski-z", SHIPID: &shipZ, Kind: eebusruntime.ServiceKindV1Remote, Visible: true, Paired: true},
			{SKI: "ski-a", SHIPID: &shipA, Kind: eebusruntime.ServiceKindV1Local, Visible: true},
		},
		Sessions: []eebusruntime.SessionV1{
			{ID: "session-z", RemoteSKI: "remote-z", State: eebusruntime.ObservedSessionStateV1Connected, Since: earlier},
			{ID: "session-a", RemoteSKI: "remote-a", State: eebusruntime.ObservedSessionStateV1Disconnected, Since: earlier},
		},
		Devices: []eebusruntime.DeviceV1{
			{SKI: "ski-z", SHIPID: &shipZ, Address: "device-z", Type: "heating", Description: &description},
			{SKI: "ski-a", SHIPID: &shipA, Address: "device-a", Type: "gateway"},
		},
		Entities: []eebusruntime.EntityV1{
			{DeviceAddress: "device-z", EntityAddress: "entity-z", Type: "zone"},
			{DeviceAddress: "device-z", EntityAddress: "entity-a", Type: "dhw"},
		},
		Features: []eebusruntime.FeatureV1{
			{DeviceAddress: "device-z", EntityAddress: "entity-z", FeatureAddress: "feature-z", Type: "temperature", Role: "server"},
			{DeviceAddress: "device-z", EntityAddress: "entity-z", FeatureAddress: "feature-a", Type: "setpoint", Role: "client"},
		},
		UseCases: []eebusruntime.UseCaseV1{
			{ContextAddress: "device-z:entity-z", Name: "usecase-z", Actor: "server", Availability: &available},
			{ContextAddress: "device-z:entity-a", Name: "usecase-a", Actor: "client", Availability: &available},
		},
	}
	snapshot, err := eebusruntime.NewSnapshotV1(draft)
	if err != nil {
		t.Fatalf("construct test snapshot: %v", err)
	}
	return snapshot
}

func msp06Map(t *testing.T, value any, path string) map[string]any {
	t.Helper()
	result, ok := value.(map[string]any)
	if !ok {
		t.Fatalf("%s type = %T, want object", path, value)
	}
	return result
}

func msp06Slice(t *testing.T, value any, path string) []any {
	t.Helper()
	result, ok := value.([]any)
	if !ok {
		t.Fatalf("%s type = %T, want array", path, value)
	}
	return result
}

func msp06AssertKeys(t *testing.T, value map[string]any, path string, keys ...string) {
	t.Helper()
	got := make([]string, 0, len(value))
	for key := range value {
		got = append(got, key)
	}
	sort.Strings(got)
	want := append([]string(nil), keys...)
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%s keys = %v, want %v", path, got, want)
	}
}

func msp06AssertToken(t *testing.T, value any, path string) string {
	t.Helper()
	token, ok := value.(string)
	if !ok || !msp06TokenPattern.MatchString(token) {
		t.Fatalf("%s = %#v, want canonical token", path, value)
	}
	return token
}

func msp06AssertMeta(t *testing.T, envelope map[string]any, tool, scope, mode string) map[string]any {
	t.Helper()
	meta := msp06Map(t, envelope["meta"], "meta")
	if meta["tool"] != tool || meta["scope"] != scope || meta["mode"] != mode ||
		meta["mask_tier"] != "redacted" || meta["auth_scope"] != "eebus.public.read" {
		t.Fatalf("meta binding = %#v", meta)
	}
	if hash, _ := meta["data_hash"].(string); !msp06HashPattern.MatchString(hash) {
		t.Fatalf("meta.data_hash = %#v", meta["data_hash"])
	}
	return meta
}

func msp06AssertSuccess(t *testing.T, result msp06CallResult, tool, scope, mode string) map[string]any {
	t.Helper()
	if result.isError || result.envelope["error"] != nil || result.envelope["data"] == nil {
		t.Fatalf("tool %s failed: %s", tool, result.raw)
	}
	msp06AssertMeta(t, result.envelope, tool, scope, mode)
	return msp06Map(t, result.envelope["data"], "data")
}

func msp06AssertError(t *testing.T, result msp06CallResult, tool, scope, code string) map[string]any {
	t.Helper()
	return msp06AssertErrorMode(t, result, tool, scope, "live", code)
}

func msp06AssertErrorMode(t *testing.T, result msp06CallResult, tool, scope, mode, code string) map[string]any {
	t.Helper()
	if !result.isError || result.envelope["data"] != nil {
		t.Fatalf("tool %s returned success: %s", tool, result.raw)
	}
	msp06AssertMeta(t, result.envelope, tool, scope, mode)
	public := msp06Map(t, result.envelope["error"], "error")
	if public["code"] != code {
		t.Fatalf("error code = %v, want %s", public["code"], code)
	}
	return public
}

func TestMSP06ToolInventoryAndSchemasRemainClosed(t *testing.T) {
	server, _ := msp06TestServer(t, &msp06Provider{snapshot: msp06Snapshot(t, "runtime-a")})
	got := msp06NamesWithPrefix(msp06Tools(t, server), "eebus.")
	want := append([]string(nil), msp06ToolNames...)
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("eeBUS tools = %v, want %v", got, want)
	}
	for _, tool := range msp06Tools(t, server) {
		name, _ := tool["name"].(string)
		if !strings.HasPrefix(name, "eebus.") {
			continue
		}
		schema := msp06Map(t, tool["inputSchema"], name+".inputSchema")
		if schema["additionalProperties"] != false {
			t.Errorf("%s input is not closed", name)
		}
	}
}

func TestMSP06PublicProjectionUsesCanonicalRedactedBuilder(t *testing.T) {
	snapshot := msp06Snapshot(t, "runtime-a")
	expected, err := eebusruntime.BuildRedactedSnapshotV1(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	server, _ := msp06TestServer(t, &msp06Provider{snapshot: snapshot})
	data := msp06AssertSuccess(t, msp06Call(t, server.Handler(), msp06ServicesListTool, map[string]any{}), msp06ServicesListTool, "services", "live")
	services := msp06Slice(t, data["services"], "services")
	if len(services) != len(expected.Services) {
		t.Fatalf("services = %d, want %d", len(services), len(expected.Services))
	}
	encoded, _ := json.Marshal(data)
	for _, secret := range []string{"ski-z", "service-z", "device-z", "local-ski-secret"} {
		if bytes.Contains(encoded, []byte(secret)) {
			t.Errorf("public projection leaked %q", secret)
		}
	}
}

func TestMSP06ArgumentErrorsAvoidProviderAccess(t *testing.T) {
	provider := &msp06Provider{snapshot: msp06Snapshot(t, "runtime-a")}
	server, _ := msp06TestServer(t, provider)
	for _, key := range []string{"tier", "mask_tier", "auth_scope", "principal", "authorization"} {
		result := msp06Call(t, server.Handler(), msp06RuntimeStatusTool, map[string]any{key: "raw"})
		msp06AssertError(t, result, msp06RuntimeStatusTool, "runtime-status", "invalid_argument")
	}
	if provider.callCount() != 0 {
		t.Fatalf("provider calls = %d, want 0", provider.callCount())
	}
}

func TestMSP06ProviderTimeoutIsNormalized(t *testing.T) {
	blocking := EEBusV1ProviderFunc(func() (eebusruntime.SnapshotV1, error) {
		time.Sleep(200 * time.Millisecond)
		return msp06Snapshot(t, "late"), nil
	})
	server, _ := msp06TestServer(t, blocking)
	result := msp06Call(t, server.Handler(), msp06TopologyGetTool, map[string]any{})
	msp06AssertError(t, result, msp06TopologyGetTool, "topology", "timeout")
}

type EEBusV1ProviderFunc func() (eebusruntime.SnapshotV1, error)

func (provider EEBusV1ProviderFunc) Snapshot() (eebusruntime.SnapshotV1, error) {
	return provider()
}
