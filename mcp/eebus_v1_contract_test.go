package mcp

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"regexp"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
	eebusruntime "github.com/Project-Helianthus/helianthus-eebusreg"
	"github.com/Project-Helianthus/helianthus-eebusreg/eebusevidence"
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
	msp06HashPattern      = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	msp06TokenPattern     = regexp.MustCompile(`^[A-Za-z0-9_-]{43}$`)
	msp06TimestampPattern = regexp.MustCompile(`^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]{1,9})?Z$`)
)

type msp06Provider struct {
	mu       sync.Mutex
	snapshot eebusruntime.SnapshotV1
	err      error
	calls    int
}

var _ EEBusV1Provider = (*msp06Provider)(nil)

func (provider *msp06Provider) Snapshot() (eebusruntime.SnapshotV1, error) {
	provider.mu.Lock()
	defer provider.mu.Unlock()
	provider.calls++
	return provider.snapshot, provider.err
}

func (provider *msp06Provider) set(snapshot eebusruntime.SnapshotV1, err error) {
	provider.mu.Lock()
	defer provider.mu.Unlock()
	provider.snapshot = snapshot
	provider.err = err
}

func (provider *msp06Provider) mutate(mutate func(*eebusruntime.SnapshotV1), err error) {
	provider.mu.Lock()
	defer provider.mu.Unlock()
	mutate(&provider.snapshot)
	provider.err = err
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
		t.Fatalf("NewServer() error = %v", err)
	}
	clock := &msp06Clock{now: time.Date(2026, 7, 19, 8, 0, 0, 123456000, time.UTC)}
	options := eebusV1RegistrationOptions{
		now:          clock.Now,
		entropy:      &msp06EntropyReader{},
		pseudonymKey: bytes.Repeat([]byte{0x5a}, sha256.Size),
		liveTimeout:  100 * time.Millisecond,
	}
	if err := server.registerEEBusV1Provider(provider, options); err != nil {
		t.Fatalf("registerEEBusV1Provider() error = %v", err)
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
	requestBody, err := json.Marshal(rpcRequest{JSONRPC: "2.0", ID: 1, Method: "tools/call", Params: params})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/mcp", bytes.NewReader(requestBody))
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
	result, ok := response.Result.(map[string]any)
	if !ok {
		t.Fatalf("tool %s result type = %T, want object", tool, response.Result)
	}
	content, ok := result["content"].([]any)
	if !ok || len(content) != 1 {
		t.Fatalf("tool %s content = %#v, want one item", tool, result["content"])
	}
	item, ok := content[0].(map[string]any)
	if !ok || item["type"] != "text" {
		t.Fatalf("tool %s content item = %#v, want text", tool, content[0])
	}
	raw, _ := item["text"].(string)
	if raw == "" {
		t.Fatalf("tool %s returned empty content text", tool)
	}
	var envelope map[string]any
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&envelope); err != nil {
		t.Fatalf("tool %s envelope decode: %v; text=%q", tool, err, raw)
	}
	return msp06CallResult{envelope: envelope, raw: raw, isError: result["isError"] == true}
}

func msp06Tools(t *testing.T, server *Server) []map[string]any {
	t.Helper()
	response := doRPC(t, server.Handler(), rpcRequest{JSONRPC: "2.0", ID: 1, Method: "tools/list"})
	if response.Error != nil {
		t.Fatalf("tools/list error = %+v", response.Error)
	}
	result, ok := response.Result.(map[string]any)
	if !ok {
		t.Fatalf("tools/list result type = %T", response.Result)
	}
	values, ok := result["tools"].([]any)
	if !ok {
		t.Fatalf("tools/list tools type = %T", result["tools"])
	}
	tools := make([]map[string]any, 0, len(values))
	for _, value := range values {
		tool, ok := value.(map[string]any)
		if !ok {
			t.Fatalf("tool entry type = %T", value)
		}
		tools = append(tools, tool)
	}
	return tools
}

func msp06NamesWithPrefix(tools []map[string]any, prefix string) []string {
	var names []string
	for _, tool := range tools {
		name, _ := tool["name"].(string)
		if strings.HasPrefix(name, prefix) {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names
}

func msp06NamesWithPrefixInOrder(tools []map[string]any, prefix string) []string {
	var names []string
	for _, tool := range tools {
		name, _ := tool["name"].(string)
		if strings.HasPrefix(name, prefix) {
			names = append(names, name)
		}
	}
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

func msp06Digest(label string) string {
	digest := sha256.Sum256([]byte(label))
	return "sha256:" + hex.EncodeToString(digest[:])
}

func msp06Evidence(kind eebusevidence.ObjectKind, label string, size int, timestamp time.Time) eebusevidence.ObjectV1 {
	return eebusevidence.NewObjectV1(kind, msp06Digest(label), size, timestamp)
}

func msp06Snapshot(t *testing.T, runtimeLabel string) eebusruntime.SnapshotV1 {
	t.Helper()
	observed := time.Date(2026, 7, 19, 7, 59, 58, 987654321, time.UTC)
	earlier := observed.Add(-time.Minute)
	snapshot := eebusruntime.SnapshotV1{
		Meta: eebusruntime.SnapshotMetaV1{
			Contract:      eebusruntime.SnapshotContractV1,
			Runtime:       msp06SourceID(t, eebusraw.IDKindPeer, runtimeLabel),
			LocalSKI:      msp06SourceID(t, eebusraw.IDKindLocalSKI, "local-ski-secret"),
			MaskTier:      eebusraw.MaskTierRedacted,
			CapturedAt:    observed.Add(time.Second),
			DataTimestamp: observed,
		},
		Status: eebusruntime.RuntimeObservationV1{State: eebusruntime.ObservedRuntimeStateV1Ready},
		Pairing: []eebusruntime.PairingObservationV1{
			{Remote: msp06SourceID(t, eebusraw.IDKindRemoteSKI, "remote-z"), State: eebusraw.PairingStatePaired, Since: earlier, Raw: []eebusevidence.ObjectV1{msp06Evidence(eebusevidence.ObjectKindIdentity, "pair-z", 8, earlier)}},
			{Remote: msp06SourceID(t, eebusraw.IDKindRemoteSKI, "remote-a"), State: eebusraw.PairingStateUnpaired},
		},
		Services: []eebusruntime.ServiceV1{
			{ID: msp06SourceID(t, eebusraw.IDKindPeer, "service-z"), Kind: eebusruntime.ServiceKindV1Remote, Visible: true, Paired: true, Raw: []eebusevidence.ObjectV1{
				msp06Evidence(eebusevidence.ObjectKindService, "shared-evidence", 10, earlier),
				msp06Evidence(eebusevidence.ObjectKindService, "shared-evidence", 2, observed),
			}},
			{ID: msp06SourceID(t, eebusraw.IDKindPeer, "service-a"), Kind: eebusruntime.ServiceKindV1Local, Visible: true, Paired: false},
		},
		Sessions: []eebusruntime.SessionV1{
			{ID: msp06SourceID(t, eebusraw.IDKindSession, "session-z"), Remote: msp06SourceID(t, eebusraw.IDKindRemoteSKI, "remote-z"), State: eebusruntime.ObservedSessionStateV1Connected, Since: earlier, Raw: []eebusevidence.ObjectV1{msp06Evidence(eebusevidence.ObjectKindSession, "session-z-evidence", 12, observed)}},
			{ID: msp06SourceID(t, eebusraw.IDKindSession, "session-a"), Remote: msp06SourceID(t, eebusraw.IDKindRemoteSKI, "remote-a"), State: eebusruntime.ObservedSessionStateV1Disconnected},
		},
		Topology: eebusruntime.TopologyV1{Devices: []eebusruntime.DeviceV1{
			{
				ID: msp06SourceID(t, eebusraw.IDKindPeer, "device-z"),
				Entities: []eebusruntime.EntityV1{
					{ID: msp06SourceID(t, eebusraw.IDKindPeer, "entity-z"), Features: []eebusruntime.FeatureV1{
						{ID: msp06SourceID(t, eebusraw.IDKindPeer, "feature-z"), Role: eebusruntime.FeatureRoleV1Server},
						{ID: msp06SourceID(t, eebusraw.IDKindPeer, "feature-a"), Role: eebusruntime.FeatureRoleV1Client},
					}},
					{ID: msp06SourceID(t, eebusraw.IDKindPeer, "entity-a")},
				},
				UseCaseClaims: []eebusruntime.UseCaseClaimV1{
					{ID: msp06SourceID(t, eebusraw.IDKindPeer, "usecase-z")},
					{ID: msp06SourceID(t, eebusraw.IDKindPeer, "usecase-a")},
				},
				Raw: []eebusevidence.ObjectV1{msp06Evidence(eebusevidence.ObjectKindTopology, "device-z-evidence", 21, observed)},
			},
			{ID: msp06SourceID(t, eebusraw.IDKindPeer, "device-a")},
		}},
		Raw: []eebusevidence.ObjectV1{msp06Evidence(eebusevidence.ObjectKindUnknown, "root-evidence", 34, observed)},
	}
	if err := snapshot.Validate(); err != nil {
		t.Fatalf("MSP-06 source snapshot invalid: %v", err)
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
		t.Fatalf("%s = %#v, want canonical 43-character base64url token", path, value)
	}
	decoded, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil || len(decoded) != sha256.Size || base64.RawURLEncoding.EncodeToString(decoded) != token {
		t.Fatalf("%s = %q, want canonical unpadded encoding of 32 bytes", path, token)
	}
	return token
}

func msp06AssertIdentity(t *testing.T, value any, path, kind string) string {
	t.Helper()
	identity := msp06Map(t, value, path)
	msp06AssertKeys(t, identity, path, "kind", "digest")
	if identity["kind"] != kind {
		t.Fatalf("%s.kind = %#v, want %q", path, identity["kind"], kind)
	}
	return msp06AssertToken(t, identity["digest"], path+".digest")
}

func msp06AssertEvidence(t *testing.T, value any, path string) {
	t.Helper()
	items := msp06Slice(t, value, path)
	previous := ""
	for index, raw := range items {
		itemPath := fmt.Sprintf("%s[%d]", path, index)
		item := msp06Map(t, raw, itemPath)
		msp06AssertKeys(t, item, itemPath, "kind", "digest", "size", "data_timestamp")
		kind, _ := item["kind"].(string)
		digest, _ := item["digest"].(string)
		sizeNumber, ok := item["size"].(json.Number)
		if !ok {
			t.Fatalf("%s.size type = %T, want JSON integer", itemPath, item["size"])
		}
		size, err := sizeNumber.Int64()
		if err != nil || size < 0 || size > 9007199254740991 {
			t.Fatalf("%s.size = %q, want portable non-negative safe integer", itemPath, sizeNumber)
		}
		timestamp, _ := item["data_timestamp"].(string)
		if !msp06HashPattern.MatchString(digest) || !msp06TimestampPattern.MatchString(timestamp) {
			t.Fatalf("%s has invalid digest/timestamp: %#v", itemPath, item)
		}
		key := kind + "\x00" + digest + "\x00" + sizeNumber.String() + "\x00" + timestamp
		if index > 0 && key <= previous {
			t.Fatalf("%s ordering key %q is not greater than %q", itemPath, key, previous)
		}
		previous = key
	}
}

func msp06AssertMeta(t *testing.T, envelope map[string]any, tool, scope, mode string) map[string]any {
	t.Helper()
	msp06AssertKeys(t, envelope, "envelope", "meta", "data", "error")
	meta := msp06Map(t, envelope["meta"], "meta")
	msp06AssertKeys(t, meta, "meta", "contract", "tool", "scope", "mask_tier", "auth_scope", "mode", "data_timestamp", "data_hash", "runtime")
	contract := msp06Map(t, meta["contract"], "meta.contract")
	msp06AssertKeys(t, contract, "meta.contract", "name", "major", "minor")
	if contract["name"] != "helianthus-eebus-mcp" || fmt.Sprint(contract["major"]) != "1" || fmt.Sprint(contract["minor"]) != "0" {
		t.Fatalf("meta.contract = %#v, want helianthus-eebus-mcp 1.0", contract)
	}
	if meta["tool"] != tool || meta["scope"] != scope || meta["mode"] != mode || meta["mask_tier"] != "redacted" || meta["auth_scope"] != "eebus.raw.read" {
		t.Fatalf("meta binding = %#v, want tool=%s scope=%s mode=%s fixed reader policy", meta, tool, scope, mode)
	}
	if timestamp, _ := meta["data_timestamp"].(string); !msp06TimestampPattern.MatchString(timestamp) {
		t.Fatalf("meta.data_timestamp = %#v, want UTC RFC3339 with literal Z", meta["data_timestamp"])
	}
	if hash, _ := meta["data_hash"].(string); !msp06HashPattern.MatchString(hash) {
		t.Fatalf("meta.data_hash = %#v, want lowercase sha256", meta["data_hash"])
	}
	runtime := msp06Map(t, meta["runtime"], "meta.runtime")
	if runtime["state"] == "unknown" {
		t.Fatal("meta.runtime.state must never serialize unknown")
	}
	return meta
}

func msp06AssertSuccess(t *testing.T, result msp06CallResult, tool, scope, mode string) map[string]any {
	t.Helper()
	if result.isError || result.envelope["error"] != nil || result.envelope["data"] == nil {
		t.Fatalf("tool %s result = %#v, want success data and null error", tool, result.envelope)
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
	if !result.isError || result.envelope["data"] != nil || result.envelope["error"] == nil {
		t.Fatalf("tool %s result = %#v, want null data and structured error", tool, result.envelope)
	}
	msp06AssertMeta(t, result.envelope, tool, scope, mode)
	errorObject := msp06Map(t, result.envelope["error"], "error")
	msp06AssertKeys(t, errorObject, "error", "code", "message", "retriable", "source_layer")
	if errorObject["code"] != code {
		t.Fatalf("error.code = %#v, want %q", errorObject["code"], code)
	}
	return errorObject
}

func TestMSP06ToolInventoryIsConditionalClosedAndReadOnly(t *testing.T) {
	server, err := NewServer(&testRegistry{entries: make(map[byte]registry.DeviceEntry)}, &testInvoker{})
	if err != nil {
		t.Fatal(err)
	}
	if got := msp06NamesWithPrefix(msp06Tools(t, server), "eebus."); len(got) != 0 {
		t.Fatalf("disabled/unregistered eebus tool inventory = %v, want none", got)
	}

	provider := &msp06Provider{snapshot: msp06Snapshot(t, "runtime-a")}
	if err := server.RegisterEEBusV1Provider(provider); err != nil {
		t.Fatalf("RegisterEEBusV1Provider() error = %v", err)
	}
	want := append([]string(nil), msp06ToolNames...)
	sort.Strings(want)
	tools := msp06Tools(t, server)
	if got := msp06NamesWithPrefixInOrder(tools, "eebus."); !reflect.DeepEqual(got, msp06ToolNames) {
		t.Fatalf("registered eebus inventory order = %v, want contract order %v", got, msp06ToolNames)
	}
	if got := msp06NamesWithPrefix(tools, "eebus."); !reflect.DeepEqual(got, want) {
		t.Fatalf("registered eebus inventory = %v, want exact closed inventory %v", got, want)
	}
	for _, forbidden := range []string{"write", "set", "mutate", "command", "trust", "pair", "admin", "experimental"} {
		for _, name := range want {
			if strings.Contains(name, forbidden) && name != msp06PairingStatusTool {
				t.Fatalf("read-only inventory contains forbidden surface %q", name)
			}
		}
	}
	secondProvider := &msp06Provider{snapshot: msp06Snapshot(t, "runtime-b")}
	if err := server.RegisterEEBusV1Provider(secondProvider); err == nil {
		t.Fatal("second eeBUS provider registration succeeded, want rejection")
	}
	if got := msp06NamesWithPrefix(msp06Tools(t, server), "eebus."); !reflect.DeepEqual(got, want) {
		t.Fatalf("inventory changed after rejected second registration: %v", got)
	}
	msp06AssertSuccess(t, msp06Call(t, server.Handler(), msp06RuntimeStatusTool, map[string]any{}), msp06RuntimeStatusTool, "runtime-status", "live")
	if provider.callCount() != 1 || secondProvider.callCount() != 0 {
		t.Fatalf("provider calls after duplicate registration = original:%d duplicate:%d, want 1/0", provider.callCount(), secondProvider.callCount())
	}
}

func TestMSP06NilProviderLeavesInventoryAbsent(t *testing.T) {
	var typedNil *msp06Provider
	for _, test := range []struct {
		name     string
		provider EEBusV1Provider
	}{
		{name: "nil interface", provider: nil},
		{name: "typed nil", provider: typedNil},
	} {
		t.Run(test.name, func(t *testing.T) {
			server, err := NewServer(&testRegistry{entries: make(map[byte]registry.DeviceEntry)}, &testInvoker{})
			if err != nil {
				t.Fatal(err)
			}
			if err := server.RegisterEEBusV1Provider(test.provider); err == nil {
				t.Fatal("RegisterEEBusV1Provider(nil) succeeded")
			}
			if got := msp06NamesWithPrefix(msp06Tools(t, server), "eebus."); len(got) != 0 {
				t.Fatalf("nil registration exposed tools %v", got)
			}
		})
	}
}

func TestMSP06ToolInputSchemasAreClosedAndExact(t *testing.T) {
	server, _ := msp06TestServer(t, &msp06Provider{snapshot: msp06Snapshot(t, "runtime-a")})
	tools := msp06Tools(t, server)
	byName := make(map[string]map[string]any)
	for _, tool := range tools {
		name, _ := tool["name"].(string)
		byName[name] = tool
	}
	want := map[string]struct {
		properties []string
		required   []string
	}{
		msp06RuntimeStatusTool: {properties: []string{"evidence_ref"}},
		msp06ServicesListTool:  {properties: []string{"evidence_ref"}},
		msp06ServicesGetTool:   {properties: []string{"evidence_ref", "id_digest"}, required: []string{"id_digest"}},
		msp06SessionsListTool:  {properties: []string{"evidence_ref"}},
		msp06SessionsGetTool:   {properties: []string{"evidence_ref", "id_digest"}, required: []string{"id_digest"}},
		msp06TopologyGetTool:   {properties: []string{"evidence_ref"}},
		msp06SnapshotCapture:   {},
		msp06SnapshotDrop:      {properties: []string{"snapshot_ref"}, required: []string{"snapshot_ref"}},
		msp06PairingStatusTool: {properties: []string{"evidence_ref"}},
	}
	for name, expected := range want {
		tool := byName[name]
		if tool == nil {
			t.Fatalf("tool %s missing", name)
		}
		schema := msp06Map(t, tool["inputSchema"], name+".inputSchema")
		if schema["type"] != "object" || schema["additionalProperties"] != false {
			t.Fatalf("%s schema = %#v, want closed object", name, schema)
		}
		properties := msp06Map(t, schema["properties"], name+".properties")
		gotProperties := make([]string, 0, len(properties))
		for key := range properties {
			gotProperties = append(gotProperties, key)
			property := msp06Map(t, properties[key], name+"."+key)
			if property["type"] != "string" {
				t.Fatalf("%s.%s type = %#v, want string", name, key, property["type"])
			}
			if property["pattern"] != `^[A-Za-z0-9_-]{43}$` {
				t.Fatalf("%s.%s pattern = %#v, want canonical 43-character base64url", name, key, property["pattern"])
			}
		}
		sort.Strings(gotProperties)
		sort.Strings(expected.properties)
		if !slices.Equal(gotProperties, expected.properties) {
			t.Fatalf("%s properties = %v, want %v", name, gotProperties, expected.properties)
		}
		var gotRequired []string
		if rawRequired, exists := schema["required"]; exists {
			for _, raw := range msp06Slice(t, rawRequired, name+".required") {
				gotRequired = append(gotRequired, fmt.Sprint(raw))
			}
		}
		sort.Strings(gotRequired)
		sort.Strings(expected.required)
		if !slices.Equal(gotRequired, expected.required) {
			t.Fatalf("%s required = %v, want %v", name, gotRequired, expected.required)
		}
	}
}

func TestMSP06LiveToolsTakeOneSnapshotAndUseClosedWireDTOs(t *testing.T) {
	provider := &msp06Provider{snapshot: msp06Snapshot(t, "runtime-a")}
	server, _ := msp06TestServer(t, provider)
	handler := server.Handler()

	status := msp06AssertSuccess(t, msp06Call(t, handler, msp06RuntimeStatusTool, map[string]any{}), msp06RuntimeStatusTool, "runtime-status", "live")
	msp06AssertKeys(t, status, "status", "state")
	if status["state"] != "ready" {
		t.Fatalf("status.state = %#v, want ready", status["state"])
	}

	services := msp06AssertSuccess(t, msp06Call(t, handler, msp06ServicesListTool, map[string]any{}), msp06ServicesListTool, "services", "live")
	msp06AssertKeys(t, services, "services.list", "services")
	serviceItems := msp06Slice(t, services["services"], "services")
	if len(serviceItems) != 2 {
		t.Fatalf("services length = %d, want 2", len(serviceItems))
	}
	serviceDigests := make([]string, 0, len(serviceItems))
	for index, raw := range serviceItems {
		path := fmt.Sprintf("services[%d]", index)
		service := msp06Map(t, raw, path)
		keys := []string{"id", "kind", "visible", "paired"}
		if _, exists := service["evidence"]; exists {
			keys = append(keys, "evidence")
			msp06AssertEvidence(t, service["evidence"], path+".evidence")
		}
		msp06AssertKeys(t, service, path, keys...)
		serviceDigests = append(serviceDigests, msp06AssertIdentity(t, service["id"], path+".id", "service"))
	}
	if !sort.StringsAreSorted(serviceDigests) {
		t.Fatalf("service pseudonyms are not sorted: %v", serviceDigests)
	}
	selectedService := serviceDigests[0]
	service := msp06AssertSuccess(t, msp06Call(t, handler, msp06ServicesGetTool, map[string]any{"id_digest": selectedService}), msp06ServicesGetTool, "service", "live")
	serviceKeys := []string{"id", "kind", "visible", "paired"}
	if _, exists := service["evidence"]; exists {
		serviceKeys = append(serviceKeys, "evidence")
		msp06AssertEvidence(t, service["evidence"], "service.evidence")
	}
	msp06AssertKeys(t, service, "service", serviceKeys...)
	if got := msp06AssertIdentity(t, service["id"], "service.id", "service"); got != selectedService {
		t.Fatalf("services.get id = %q, want %q", got, selectedService)
	}

	sessions := msp06AssertSuccess(t, msp06Call(t, handler, msp06SessionsListTool, map[string]any{}), msp06SessionsListTool, "sessions", "live")
	msp06AssertKeys(t, sessions, "sessions.list", "sessions")
	sessionItems := msp06Slice(t, sessions["sessions"], "sessions")
	sessionDigests := make([]string, 0, len(sessionItems))
	for index, raw := range sessionItems {
		path := fmt.Sprintf("sessions[%d]", index)
		session := msp06Map(t, raw, path)
		keys := []string{"id", "remote", "state"}
		if _, exists := session["since"]; exists {
			keys = append(keys, "since")
		}
		if _, exists := session["evidence"]; exists {
			keys = append(keys, "evidence")
			msp06AssertEvidence(t, session["evidence"], path+".evidence")
		}
		msp06AssertKeys(t, session, path, keys...)
		sessionDigests = append(sessionDigests, msp06AssertIdentity(t, session["id"], path+".id", "session"))
		msp06AssertIdentity(t, session["remote"], path+".remote", "remote")
		if session["state"] == "unknown" {
			t.Fatalf("%s.state serialized unknown", path)
		}
	}
	if !sort.StringsAreSorted(sessionDigests) {
		t.Fatalf("session pseudonyms are not sorted: %v", sessionDigests)
	}
	session := msp06AssertSuccess(t, msp06Call(t, handler, msp06SessionsGetTool, map[string]any{"id_digest": sessionDigests[0]}), msp06SessionsGetTool, "session", "live")
	sessionKeys := []string{"id", "remote", "state"}
	if _, exists := session["since"]; exists {
		sessionKeys = append(sessionKeys, "since")
	}
	if _, exists := session["evidence"]; exists {
		sessionKeys = append(sessionKeys, "evidence")
		msp06AssertEvidence(t, session["evidence"], "session.evidence")
	}
	msp06AssertKeys(t, session, "session", sessionKeys...)
	if got := msp06AssertIdentity(t, session["id"], "session.id", "session"); got != sessionDigests[0] {
		t.Fatalf("sessions.get id = %q, want %q", got, sessionDigests[0])
	}

	topology := msp06AssertSuccess(t, msp06Call(t, handler, msp06TopologyGetTool, map[string]any{}), msp06TopologyGetTool, "topology", "live")
	msp06AssertTopology(t, topology)

	pairing := msp06AssertSuccess(t, msp06Call(t, handler, msp06PairingStatusTool, map[string]any{}), msp06PairingStatusTool, "pairing-status", "live")
	msp06AssertKeys(t, pairing, "pairing.status", "pairing")
	pairingItems := msp06Slice(t, pairing["pairing"], "pairing")
	pairingDigests := make([]string, 0, len(pairingItems))
	for index, raw := range pairingItems {
		path := fmt.Sprintf("pairing[%d]", index)
		item := msp06Map(t, raw, path)
		keys := []string{"remote", "state"}
		if _, exists := item["since"]; exists {
			keys = append(keys, "since")
		}
		if _, exists := item["evidence"]; exists {
			keys = append(keys, "evidence")
			msp06AssertEvidence(t, item["evidence"], path+".evidence")
		}
		msp06AssertKeys(t, item, path, keys...)
		pairingDigests = append(pairingDigests, msp06AssertIdentity(t, item["remote"], path+".remote", "remote"))
		if item["state"] == "unknown" {
			t.Fatalf("%s.state serialized unknown", path)
		}
	}
	if !sort.StringsAreSorted(pairingDigests) {
		t.Fatalf("pairing pseudonyms are not sorted: %v", pairingDigests)
	}

	if got, want := provider.callCount(), 7; got != want {
		t.Fatalf("provider Snapshot calls = %d, want exactly one for each of seven live reads", got)
	}
}

func msp06AssertTopology(t *testing.T, topology map[string]any) {
	t.Helper()
	msp06AssertKeys(t, topology, "topology", "devices")
	devices := msp06Slice(t, topology["devices"], "devices")
	deviceDigests := make([]string, 0, len(devices))
	for deviceIndex, rawDevice := range devices {
		devicePath := fmt.Sprintf("devices[%d]", deviceIndex)
		device := msp06Map(t, rawDevice, devicePath)
		keys := []string{"id", "entities", "usecase_claims"}
		if _, exists := device["evidence"]; exists {
			keys = append(keys, "evidence")
			msp06AssertEvidence(t, device["evidence"], devicePath+".evidence")
		}
		msp06AssertKeys(t, device, devicePath, keys...)
		deviceDigests = append(deviceDigests, msp06AssertIdentity(t, device["id"], devicePath+".id", "device"))

		entities := msp06Slice(t, device["entities"], devicePath+".entities")
		entityDigests := make([]string, 0, len(entities))
		for entityIndex, rawEntity := range entities {
			entityPath := fmt.Sprintf("%s.entities[%d]", devicePath, entityIndex)
			entity := msp06Map(t, rawEntity, entityPath)
			keys := []string{"id", "features"}
			if _, exists := entity["evidence"]; exists {
				keys = append(keys, "evidence")
				msp06AssertEvidence(t, entity["evidence"], entityPath+".evidence")
			}
			msp06AssertKeys(t, entity, entityPath, keys...)
			entityDigests = append(entityDigests, msp06AssertIdentity(t, entity["id"], entityPath+".id", "entity"))

			features := msp06Slice(t, entity["features"], entityPath+".features")
			featureDigests := make([]string, 0, len(features))
			for featureIndex, rawFeature := range features {
				featurePath := fmt.Sprintf("%s.features[%d]", entityPath, featureIndex)
				feature := msp06Map(t, rawFeature, featurePath)
				keys := []string{"id", "role"}
				if _, exists := feature["evidence"]; exists {
					keys = append(keys, "evidence")
					msp06AssertEvidence(t, feature["evidence"], featurePath+".evidence")
				}
				msp06AssertKeys(t, feature, featurePath, keys...)
				featureDigests = append(featureDigests, msp06AssertIdentity(t, feature["id"], featurePath+".id", "feature"))
				if feature["role"] != "client" && feature["role"] != "server" {
					t.Fatalf("%s.role = %#v, want closed client/server enum", featurePath, feature["role"])
				}
			}
			if !sort.StringsAreSorted(featureDigests) {
				t.Fatalf("%s features not sorted: %v", entityPath, featureDigests)
			}
		}
		if !sort.StringsAreSorted(entityDigests) {
			t.Fatalf("%s entities not sorted: %v", devicePath, entityDigests)
		}

		claims := msp06Slice(t, device["usecase_claims"], devicePath+".usecase_claims")
		claimDigests := make([]string, 0, len(claims))
		for claimIndex, rawClaim := range claims {
			claimPath := fmt.Sprintf("%s.usecase_claims[%d]", devicePath, claimIndex)
			claim := msp06Map(t, rawClaim, claimPath)
			keys := []string{"id"}
			if _, exists := claim["evidence"]; exists {
				keys = append(keys, "evidence")
				msp06AssertEvidence(t, claim["evidence"], claimPath+".evidence")
			}
			msp06AssertKeys(t, claim, claimPath, keys...)
			claimDigests = append(claimDigests, msp06AssertIdentity(t, claim["id"], claimPath+".id", "usecase-claim"))
		}
		if !sort.StringsAreSorted(claimDigests) {
			t.Fatalf("%s usecase claims not sorted: %v", devicePath, claimDigests)
		}
	}
	if !sort.StringsAreSorted(deviceDigests) {
		t.Fatalf("devices not sorted: %v", deviceDigests)
	}
}

func TestMSP06PseudonymsAreRuntimeScopedEphemeralHMACs(t *testing.T) {
	snapshotA := msp06Snapshot(t, "runtime-a")
	snapshotB := msp06Snapshot(t, "runtime-b")

	serverA, _ := msp06TestServer(t, &msp06Provider{snapshot: snapshotA})
	first := msp06AssertSuccess(t, msp06Call(t, serverA.Handler(), msp06ServicesListTool, map[string]any{}), msp06ServicesListTool, "services", "live")
	second := msp06AssertSuccess(t, msp06Call(t, serverA.Handler(), msp06ServicesListTool, map[string]any{}), msp06ServicesListTool, "services", "live")
	if !reflect.DeepEqual(first, second) {
		t.Fatal("same runtime/key/source produced unstable pseudonyms")
	}
	digestA := msp06AssertIdentity(t, msp06Map(t, msp06Slice(t, first["services"], "services")[0], "service")["id"], "service.id", "service")

	serverDifferentRuntime, _ := msp06TestServer(t, &msp06Provider{snapshot: snapshotB})
	differentRuntime := msp06AssertSuccess(t, msp06Call(t, serverDifferentRuntime.Handler(), msp06ServicesListTool, map[string]any{}), msp06ServicesListTool, "services", "live")
	digestDifferentRuntime := msp06AssertIdentity(t, msp06Map(t, msp06Slice(t, differentRuntime["services"], "services")[0], "service")["id"], "service.id", "service")
	if digestA == digestDifferentRuntime {
		t.Fatal("same source service under different runtime identity reused a pseudonym")
	}

	serverDifferentKey, err := NewServer(&testRegistry{entries: make(map[byte]registry.DeviceEntry)}, &testInvoker{})
	if err != nil {
		t.Fatal(err)
	}
	clock := &msp06Clock{now: time.Date(2026, 7, 19, 8, 0, 0, 0, time.UTC)}
	if err := serverDifferentKey.registerEEBusV1Provider(&msp06Provider{snapshot: snapshotA}, eebusV1RegistrationOptions{
		now:          clock.Now,
		entropy:      &msp06EntropyReader{},
		pseudonymKey: bytes.Repeat([]byte{0xa5}, sha256.Size),
		liveTimeout:  100 * time.Millisecond,
	}); err != nil {
		t.Fatal(err)
	}
	differentKey := msp06AssertSuccess(t, msp06Call(t, serverDifferentKey.Handler(), msp06ServicesListTool, map[string]any{}), msp06ServicesListTool, "services", "live")
	digestDifferentKey := msp06AssertIdentity(t, msp06Map(t, msp06Slice(t, differentKey["services"], "services")[0], "service")["id"], "service.id", "service")
	if digestA == digestDifferentKey {
		t.Fatal("different process-ephemeral HMAC keys produced the same pseudonym")
	}

	encoded, err := json.Marshal(first)
	if err != nil {
		t.Fatal(err)
	}
	for _, source := range snapshotA.Services {
		if bytes.Contains(encoded, []byte(source.ID.Digest)) {
			t.Fatalf("wire forwarded stable source digest %q instead of re-pseudonymizing", source.ID.Digest)
		}
	}
	if bytes.Contains(encoded, []byte("masked")) || bytes.Contains(encoded, []byte("local_ski")) {
		t.Fatalf("wire contains source-only identity fields: %s", encoded)
	}
}

func TestMSP06PseudonymsAreDomainSeparatedByWireIdentityKind(t *testing.T) {
	snapshot := msp06Snapshot(t, "shared-identity")
	sharedPeer := msp06SourceID(t, eebusraw.IDKindPeer, "shared-identity")
	snapshot.Services[0].ID = sharedPeer
	snapshot.Topology.Devices[0].ID = sharedPeer
	snapshot.Topology.Devices[0].Entities[0].ID = sharedPeer
	snapshot.Topology.Devices[0].Entities[0].Features[0].ID = sharedPeer
	snapshot.Topology.Devices[0].UseCaseClaims[0].ID = sharedPeer
	snapshot.Topology.Devices[0].Entities = snapshot.Topology.Devices[0].Entities[:1]
	snapshot.Topology.Devices[0].Entities[0].Features = snapshot.Topology.Devices[0].Entities[0].Features[:1]
	snapshot.Topology.Devices[0].UseCaseClaims = snapshot.Topology.Devices[0].UseCaseClaims[:1]
	sharedRemote := msp06SourceID(t, eebusraw.IDKindRemoteSKI, "shared-identity")
	snapshot.Pairing[0].Remote = sharedRemote
	snapshot.Pairing = snapshot.Pairing[:1]
	snapshot.Sessions[0].Remote = sharedRemote
	snapshot.Sessions[0].ID = msp06SourceID(t, eebusraw.IDKindSession, "shared-identity")
	snapshot.Sessions = snapshot.Sessions[:1]

	server, _ := msp06TestServer(t, &msp06Provider{snapshot: snapshot})
	_, root := msp06CaptureRoot(t, server)
	wire := msp06Map(t, root["snapshot"], "snapshot")
	meta := msp06Map(t, wire["meta"], "snapshot.meta")
	services := msp06Slice(t, wire["services"], "snapshot.services")
	pairing := msp06Slice(t, wire["pairing"], "snapshot.pairing")
	sessions := msp06Slice(t, wire["sessions"], "snapshot.sessions")
	devices := msp06Slice(t, msp06Map(t, wire["topology"], "snapshot.topology")["devices"], "snapshot.topology.devices")

	var service, remote, session, device, entity, feature, claim string
	for _, raw := range services {
		item := msp06Map(t, raw, "service")
		candidate := msp06Map(t, item["id"], "service.id")
		if item["kind"] == "remote" {
			service = fmt.Sprint(candidate["digest"])
		}
	}
	for _, raw := range pairing {
		item := msp06Map(t, raw, "pairing")
		candidate := msp06Map(t, item["remote"], "pairing.remote")
		remote = fmt.Sprint(candidate["digest"])
	}
	if len(sessions) != 1 {
		t.Fatalf("domain-separation sessions = %d, want 1", len(sessions))
	}
	session = fmt.Sprint(msp06Map(t, msp06Map(t, sessions[0], "session")["id"], "session.id")["digest"])
	for _, raw := range devices {
		item := msp06Map(t, raw, "device")
		candidate := msp06Map(t, item["id"], "device.id")
		if candidate["digest"] == nil {
			continue
		}
		entities := msp06Slice(t, item["entities"], "device.entities")
		if len(entities) == 0 {
			continue
		}
		device = fmt.Sprint(candidate["digest"])
		entityObject := msp06Map(t, entities[0], "entity")
		entity = fmt.Sprint(msp06Map(t, entityObject["id"], "entity.id")["digest"])
		features := msp06Slice(t, entityObject["features"], "entity.features")
		if len(features) > 0 {
			feature = fmt.Sprint(msp06Map(t, msp06Map(t, features[0], "feature")["id"], "feature.id")["digest"])
		}
		claims := msp06Slice(t, item["usecase_claims"], "device.usecase_claims")
		if len(claims) > 0 {
			claim = fmt.Sprint(msp06Map(t, msp06Map(t, claims[0], "claim")["id"], "claim.id")["digest"])
		}
	}
	runtime := fmt.Sprint(msp06Map(t, meta["runtime"], "snapshot.meta.runtime")["digest"])
	digests := []string{runtime, service, remote, session, device, entity, feature, claim}
	seen := make(map[string]bool, len(digests))
	for index, digest := range digests {
		if !msp06TokenPattern.MatchString(digest) {
			t.Fatalf("domain-separated digest %d = %q", index, digest)
		}
		if seen[digest] {
			t.Fatalf("wire identity kinds reused pseudonym %q: %v", digest, digests)
		}
		seen[digest] = true
	}
}

func TestMSP06StableOrderingAndHashIgnoreProviderSliceOrder(t *testing.T) {
	snapshot := msp06Snapshot(t, "runtime-a")
	provider := &msp06Provider{snapshot: snapshot}
	server, _ := msp06TestServer(t, provider)
	first := msp06Call(t, server.Handler(), msp06TopologyGetTool, map[string]any{})
	msp06AssertSuccess(t, first, msp06TopologyGetTool, "topology", "live")

	snapshot.Pairing[0], snapshot.Pairing[1] = snapshot.Pairing[1], snapshot.Pairing[0]
	snapshot.Services[0], snapshot.Services[1] = snapshot.Services[1], snapshot.Services[0]
	snapshot.Sessions[0], snapshot.Sessions[1] = snapshot.Sessions[1], snapshot.Sessions[0]
	snapshot.Topology.Devices[0], snapshot.Topology.Devices[1] = snapshot.Topology.Devices[1], snapshot.Topology.Devices[0]
	provider.set(snapshot, nil)
	second := msp06Call(t, server.Handler(), msp06TopologyGetTool, map[string]any{})
	msp06AssertSuccess(t, second, msp06TopologyGetTool, "topology", "live")
	if first.raw != second.raw {
		t.Fatalf("provider slice order changed deterministic wire/hash:\nfirst=%s\nsecond=%s", first.raw, second.raw)
	}
}

func TestMSP06ClosedEnumsDuplicatesUnknownsAndUnexplainedEmptyFailClosed(t *testing.T) {
	unknown := eebusraw.UnknownField{Path: eebusraw.UnknownPathRemote, Value: eebusraw.OpaqueBytes([]byte("pairing-history-secret"))}
	tests := []struct {
		name   string
		tool   string
		scope  string
		mutate func(*eebusruntime.SnapshotV1)
	}{
		{name: "runtime unknown", tool: msp06RuntimeStatusTool, scope: "runtime-status", mutate: func(snapshot *eebusruntime.SnapshotV1) {
			snapshot.Status.State = eebusruntime.ObservedRuntimeStateV1Unknown
		}},
		{name: "session unknown", tool: msp06SessionsListTool, scope: "sessions", mutate: func(snapshot *eebusruntime.SnapshotV1) {
			snapshot.Sessions[0].State = eebusruntime.ObservedSessionStateV1Unknown
		}},
		{name: "pairing unknown", tool: msp06PairingStatusTool, scope: "pairing-status", mutate: func(snapshot *eebusruntime.SnapshotV1) { snapshot.Pairing[0].State = eebusraw.PairingStateUnknown }},
		{name: "feature role unspecified", tool: msp06TopologyGetTool, scope: "topology", mutate: func(snapshot *eebusruntime.SnapshotV1) {
			snapshot.Topology.Devices[0].Entities[0].Features[0].Role = eebusruntime.FeatureRoleV1Unspecified
		}},
		{name: "unknown source field", tool: msp06ServicesListTool, scope: "services", mutate: func(snapshot *eebusruntime.SnapshotV1) {
			snapshot.Services[0].Unknown = []eebusraw.UnknownField{unknown}
		}},
		{name: "duplicate service identity", tool: msp06ServicesListTool, scope: "services", mutate: func(snapshot *eebusruntime.SnapshotV1) {
			snapshot.Services = append(snapshot.Services, snapshot.Services[0])
		}},
		{name: "unsafe evidence integer", tool: msp06ServicesListTool, scope: "services", mutate: func(snapshot *eebusruntime.SnapshotV1) {
			unsafeSize := int64(eebusV1MaxSafeInteger + 1)
			if int64(^uint(0)>>1) < unsafeSize {
				t.Skip("source evidence size uses int and cannot represent max-safe-plus-one on this architecture")
			}
			snapshot.Services[0].Raw[0].Size = int(unsafeSize)
		}},
		{name: "unexplained empty ready runtime", tool: msp06ServicesListTool, scope: "services", mutate: func(snapshot *eebusruntime.SnapshotV1) { snapshot.Services = nil }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			snapshot := msp06Snapshot(t, "runtime-a")
			test.mutate(&snapshot)
			provider := &msp06Provider{snapshot: snapshot}
			server, _ := msp06TestServer(t, provider)
			result := msp06Call(t, server.Handler(), test.tool, map[string]any{})
			errorObject := msp06AssertError(t, result, test.tool, test.scope, "contract_violation")
			if errorObject["message"] != "eeBUS MCP contract violation" || errorObject["retriable"] != false || errorObject["source_layer"] != "mcp" {
				t.Fatalf("contract_violation mapping = %#v", errorObject)
			}
		})
	}

	degraded := msp06Snapshot(t, "runtime-a")
	degraded.Services = nil
	degraded.Status = eebusruntime.RuntimeObservationV1{
		State: eebusruntime.ObservedRuntimeStateV1Degraded,
		Degradation: &eebusruntime.DegradationV1{
			Reason: eebusruntime.DegradationReasonV1NoVisibleServices,
			Since:  degraded.Meta.DataTimestamp.Add(-time.Minute),
		},
	}
	server, _ := msp06TestServer(t, &msp06Provider{snapshot: degraded})
	data := msp06AssertSuccess(t, msp06Call(t, server.Handler(), msp06ServicesListTool, map[string]any{}), msp06ServicesListTool, "services", "live")
	if len(msp06Slice(t, data["services"], "services")) != 0 {
		t.Fatal("degraded no-visible-services snapshot unexpectedly returned services")
	}
}

func TestMSP06SafeNumberLimitRejectsMaxSafePlusOneOnEveryArchitecture(t *testing.T) {
	object := eebusV1SourceEvidence{
		Kind:          "service",
		Digest:        msp06Digest("unsafe-size"),
		Size:          eebusV1MaxSafeInteger + 1,
		DataTimestamp: time.Date(2026, 7, 19, 8, 0, 0, 0, time.UTC),
	}
	if err := eebusV1ValidateSourceEvidenceObject(object); err == nil {
		t.Fatal("max-safe-plus-one evidence size passed source validation")
	}
}

func TestMSP06WireTimestampsNormalizeToUTCLiteralZ(t *testing.T) {
	snapshot := msp06Snapshot(t, "runtime-a")
	zone := time.FixedZone("fixture-plus-two", 2*60*60)
	snapshot.Meta.CapturedAt = snapshot.Meta.CapturedAt.In(zone)
	snapshot.Meta.DataTimestamp = snapshot.Meta.DataTimestamp.In(zone)
	snapshot.Pairing[0].Since = snapshot.Pairing[0].Since.In(zone)
	snapshot.Pairing[0].Raw[0].DataTimestamp = snapshot.Pairing[0].Raw[0].DataTimestamp.In(zone)
	snapshot.Sessions[0].Since = snapshot.Sessions[0].Since.In(zone)
	snapshot.Services[0].Raw[0].DataTimestamp = snapshot.Services[0].Raw[0].DataTimestamp.In(zone)

	server, _ := msp06TestServer(t, &msp06Provider{snapshot: snapshot})
	result, root := msp06CaptureRoot(t, server)
	if strings.Contains(result.raw, "+02:00") {
		t.Fatalf("wire retained source timezone offset: %s", result.raw)
	}
	encoded, err := json.Marshal(root)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(encoded, []byte("+02:00")) {
		t.Fatalf("captured root retained source timezone offset: %s", encoded)
	}
}

func TestMSP06ExactErrorMappingIsExhaustive(t *testing.T) {
	want := map[string]struct {
		message   string
		retriable bool
		layer     string
	}{
		"invalid_argument":    {message: "invalid argument", layer: "mcp"},
		"not_found":           {message: "not found", layer: "mcp"},
		"permission_denied":   {message: "permission denied", layer: "policy"},
		"admin_required":      {message: "administrator authorization required", layer: "policy"},
		"backend_unavailable": {message: "eeBUS runtime unavailable", retriable: true, layer: "eebusruntime"},
		"timeout":             {message: "eeBUS runtime request timed out", retriable: true, layer: "eebusruntime"},
		"snapshot_gone":       {message: "snapshot no longer available", layer: "snapshot-store"},
		"quota_exceeded":      {message: "snapshot quota exceeded", retriable: true, layer: "snapshot-store"},
		"contract_violation":  {message: "eeBUS MCP contract violation", layer: "mcp"},
	}
	for code, expected := range want {
		public, ok := eebusV1PublicErrorForCode(code)
		if !ok {
			t.Fatalf("error code %q missing from exhaustive mapping", code)
		}
		encoded, err := json.Marshal(public)
		if err != nil {
			t.Fatalf("marshal %s: %v", code, err)
		}
		var got map[string]any
		if err := json.Unmarshal(encoded, &got); err != nil {
			t.Fatal(err)
		}
		msp06AssertKeys(t, got, "error "+code, "code", "message", "retriable", "source_layer")
		if got["code"] != code || got["message"] != expected.message || got["retriable"] != expected.retriable || got["source_layer"] != expected.layer {
			t.Fatalf("mapping %s = %#v, want message=%q retriable=%t layer=%q", code, got, expected.message, expected.retriable, expected.layer)
		}
	}
	if _, ok := eebusV1PublicErrorForCode("future_error"); ok {
		t.Fatal("unknown error code was accepted")
	}
}

func TestMSP06ShapeSyntaxAndSelectorPrecedenceAvoidProviderAccess(t *testing.T) {
	provider := &msp06Provider{snapshot: msp06Snapshot(t, "runtime-a")}
	server, _ := msp06TestServer(t, provider)
	tests := []struct {
		name string
		tool string
		args map[string]any
	}{
		{name: "unknown argument before backend", tool: msp06ServicesListTool, args: map[string]any{"future": true}},
		{name: "forbidden auth selector", tool: msp06ServicesListTool, args: map[string]any{"auth_scope": "admin"}},
		{name: "forbidden mask selector", tool: msp06ServicesListTool, args: map[string]any{"mask_tier": "raw"}},
		{name: "forbidden principal selector", tool: msp06ServicesListTool, args: map[string]any{"principal": "root"}},
		{name: "malformed reference before lifecycle", tool: msp06ServicesListTool, args: map[string]any{"evidence_ref": "short"}},
		{name: "missing get selector", tool: msp06ServicesGetTool, args: map[string]any{}},
		{name: "raw stable digest is not an id selector", tool: msp06ServicesGetTool, args: map[string]any{"id_digest": msp06Snapshot(t, "runtime-a").Services[0].ID.Digest}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			before := provider.callCount()
			result := msp06Call(t, server.Handler(), test.tool, test.args)
			scope := "services"
			if test.tool == msp06ServicesGetTool {
				scope = "service"
			}
			msp06AssertError(t, result, test.tool, scope, "invalid_argument")
			if provider.callCount() != before {
				t.Fatalf("provider called for shape/syntax failure: before=%d after=%d", before, provider.callCount())
			}
		})
	}

	unknownID := base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{0x7f}, sha256.Size))
	result := msp06Call(t, server.Handler(), msp06ServicesGetTool, map[string]any{"id_digest": unknownID})
	msp06AssertError(t, result, msp06ServicesGetTool, "service", "not_found")
	if provider.callCount() != 1 {
		t.Fatalf("valid live selector performed %d provider calls, want one", provider.callCount())
	}
}

func TestMSP06BackendErrorsAreNormalizedWithoutStaleLiveFallback(t *testing.T) {
	provider := &msp06Provider{snapshot: msp06Snapshot(t, "runtime-a")}
	server, _ := msp06TestServer(t, provider)
	first := msp06Call(t, server.Handler(), msp06RuntimeStatusTool, map[string]any{})
	msp06AssertSuccess(t, first, msp06RuntimeStatusTool, "runtime-status", "live")

	provider.set(eebusruntime.SnapshotV1{}, errors.New("dial 192.0.2.99 failed; private backend detail"))
	unavailable := msp06Call(t, server.Handler(), msp06RuntimeStatusTool, map[string]any{})
	errorObject := msp06AssertError(t, unavailable, msp06RuntimeStatusTool, "runtime-status", "backend_unavailable")
	if errorObject["message"] != "eeBUS runtime unavailable" || errorObject["retriable"] != true || errorObject["source_layer"] != "eebusruntime" {
		t.Fatalf("backend_unavailable mapping = %#v", errorObject)
	}
	if strings.Contains(unavailable.raw, "192.0.2.99") || strings.Contains(unavailable.raw, "private backend detail") {
		t.Fatalf("backend error detail leaked: %s", unavailable.raw)
	}

	provider.set(eebusruntime.SnapshotV1{}, context.DeadlineExceeded)
	timedOut := msp06Call(t, server.Handler(), msp06RuntimeStatusTool, map[string]any{})
	errorObject = msp06AssertError(t, timedOut, msp06RuntimeStatusTool, "runtime-status", "timeout")
	if errorObject["message"] != "eeBUS runtime request timed out" || errorObject["retriable"] != true || errorObject["source_layer"] != "eebusruntime" {
		t.Fatalf("timeout mapping = %#v", errorObject)
	}
	if provider.callCount() != 3 {
		t.Fatalf("provider calls = %d, want one per live call with no stale fallback", provider.callCount())
	}
}

func TestMSP06CanonicalTokenSyntaxRejectsNonCanonicalReencoding(t *testing.T) {
	provider := &msp06Provider{snapshot: msp06Snapshot(t, "runtime-a")}
	server, _ := msp06TestServer(t, provider)
	canonical := base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{0x11}, sha256.Size))
	alphabet := "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
	last := strings.IndexByte(alphabet, canonical[len(canonical)-1])
	if last < 0 || last%4 != 0 {
		t.Fatalf("test token has unexpected final sextet: %q", canonical)
	}
	nonCanonical := canonical[:len(canonical)-1] + string(alphabet[last+1])
	if decoded, err := base64.RawURLEncoding.DecodeString(nonCanonical); err != nil || len(decoded) != sha256.Size {
		t.Fatalf("fixture must decode to 32 bytes despite noncanonical trailing bits: %v len=%d", err, len(decoded))
	}
	for _, token := range []string{
		strings.Repeat("A", 42),
		strings.Repeat("A", 44),
		strings.Repeat("A", 42) + "=",
		strings.Repeat("A", 42) + "$",
		nonCanonical,
	} {
		before := provider.callCount()
		result := msp06Call(t, server.Handler(), msp06RuntimeStatusTool, map[string]any{"evidence_ref": token})
		msp06AssertError(t, result, msp06RuntimeStatusTool, "runtime-status", "invalid_argument")
		if provider.callCount() != before {
			t.Fatalf("malformed token %q reached provider", token)
		}
	}
}

func TestMSP06TestHelpersUseOnlyReaderContract(t *testing.T) {
	var reader io.Reader = &msp06EntropyReader{}
	first := make([]byte, 65)
	second := make([]byte, 65)
	if _, err := io.ReadFull(reader, first); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(reader, second); err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(first, second) {
		t.Fatal("deterministic entropy fixture repeated output")
	}
}
