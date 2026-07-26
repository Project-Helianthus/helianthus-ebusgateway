package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	eebusruntime "github.com/Project-Helianthus/helianthus-eebusreg"
)

const issue743VR940Fixture = "testdata/issue743/vr940-raw-snapshot-v1.json"

type issue743Provider struct {
	mu       sync.Mutex
	snapshot eebusruntime.SnapshotV1
	err      error
	calls    int
}

func (provider *issue743Provider) Snapshot() (eebusruntime.SnapshotV1, error) {
	provider.mu.Lock()
	defer provider.mu.Unlock()
	provider.calls++
	return provider.snapshot, provider.err
}

func (provider *issue743Provider) set(snapshot eebusruntime.SnapshotV1, err error) {
	provider.mu.Lock()
	defer provider.mu.Unlock()
	provider.snapshot = snapshot
	provider.err = err
}

type issue743OperatorHandlerProvider interface {
	eebusV1OperatorHandler() http.Handler
}

type issue743OperatorEndpointProvider interface {
	eebusV1OperatorSocketPath() string
	eebusV1ServeOperator(context.Context, string, func(net.Conn) (int, error)) (io.Closer, error)
}

func issue743OperatorHandler(t *testing.T, server *Server) http.Handler {
	t.Helper()
	provider, ok := any(server).(issue743OperatorHandlerProvider)
	if !ok {
		t.Fatal("eeBUS operator boundary handler is unavailable")
	}
	handler := provider.eebusV1OperatorHandler()
	if handler == nil {
		t.Fatal("eeBUS operator boundary handler is nil")
	}
	return handler
}

func issue743FixtureBytes(t *testing.T) []byte {
	t.Helper()
	content, err := os.ReadFile(issue743VR940Fixture)
	if err != nil {
		t.Fatal(err)
	}
	return content
}

func issue743FixtureMap(t *testing.T) map[string]any {
	t.Helper()
	var fixture map[string]any
	decoder := json.NewDecoder(bytes.NewReader(issue743FixtureBytes(t)))
	decoder.UseNumber()
	if err := decoder.Decode(&fixture); err != nil {
		t.Fatal(err)
	}
	return fixture
}

func issue743Snapshot(t *testing.T, content []byte) eebusruntime.SnapshotV1 {
	t.Helper()
	var draft eebusruntime.SnapshotV1
	if err := json.Unmarshal(content, &draft); err != nil {
		t.Fatalf("decode eebusreg v0.1.15 raw fixture: %v", err)
	}
	snapshot, err := eebusruntime.NewSnapshotV1(draft)
	if err != nil {
		t.Fatalf("construct eebusreg v0.1.15 raw fixture: %v", err)
	}
	return snapshot
}

func issue743Server(t *testing.T) (*Server, *issue743Provider) {
	t.Helper()
	provider := &issue743Provider{snapshot: issue743Snapshot(t, issue743FixtureBytes(t))}
	server, _ := msp06TestServer(t, provider)
	return server, provider
}

func issue743Meta(t *testing.T, result msp06CallResult, tier, authScope string) map[string]any {
	t.Helper()
	if result.isError || result.envelope["error"] != nil {
		t.Fatalf("call failed: %s", result.raw)
	}
	meta := msp06Map(t, result.envelope["meta"], "meta")
	if meta["mask_tier"] != tier || meta["auth_scope"] != authScope {
		t.Fatalf("boundary policy = tier:%v auth:%v, want %s/%s", meta["mask_tier"], meta["auth_scope"], tier, authScope)
	}
	return meta
}

func issue743AssertError(t *testing.T, result msp06CallResult, tier, authScope, code string) {
	t.Helper()
	if !result.isError || result.envelope["data"] != nil {
		t.Fatalf("call returned success, want %s: %s", code, result.raw)
	}
	issue743MetaError(t, result, tier, authScope)
	errorObject := msp06Map(t, result.envelope["error"], "error")
	if errorObject["code"] != code {
		t.Fatalf("error code = %v, want %s", errorObject["code"], code)
	}
}

func issue743MetaError(t *testing.T, result msp06CallResult, tier, authScope string) map[string]any {
	t.Helper()
	meta := msp06Map(t, result.envelope["meta"], "meta")
	if meta["mask_tier"] != tier || meta["auth_scope"] != authScope {
		t.Fatalf("error boundary policy = tier:%v auth:%v, want %s/%s", meta["mask_tier"], meta["auth_scope"], tier, authScope)
	}
	return meta
}

func issue743Contains(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

func issue743AssertNoRawIdentity(t *testing.T, raw string) {
	t.Helper()
	for _, forbidden := range []string{
		"2222222222222222222222222222222222222222",
		"vaillant-vr940f-ship-id",
		"d:_n:Vaillant_VR940",
		"VR940-LAB-0001",
		"0357.40.40",
		`"ski"`,
		`"ship_id"`,
		`"address"`,
		`"metadata"`,
		`"entity_address"`,
		`"feature_address"`,
		`"context_address"`,
	} {
		if strings.Contains(raw, forbidden) {
			t.Errorf("redacted boundary leaked %q in %s", forbidden, raw)
		}
	}
}

func issue743AssertSecretCorpusAbsent(t *testing.T, raw string) {
	t.Helper()
	for _, secret := range []string{
		"-----BEGIN PRIVATE KEY-----",
		"-----BEGIN RSA PRIVATE KEY-----",
		"-----BEGIN EC PRIVATE KEY-----",
		"-----BEGIN ENCRYPTED PRIVATE KEY-----",
		"-----BEGIN OPENSSH PRIVATE KEY-----",
		"-----BEGIN DSA PRIVATE KEY-----",
		"-----BEGIN PGP PRIVATE KEY BLOCK-----",
		"private_key",
		"private_pem",
		"trust_store_bytes",
		"credential_token",
		"bearer_token",
		"session_token",
		"authentication_token",
		"cryptographic_secret",
	} {
		if strings.Contains(raw, secret) {
			t.Errorf("secret corpus leaked %q in %s", secret, raw)
		}
	}
}

func issue743AssertOpaqueBounds(t *testing.T, value any) {
	t.Helper()
	count, aggregate := 0, 0
	var walk func(any)
	walk = func(current any) {
		switch typed := current.(type) {
		case map[string]any:
			if rawOpaque, exists := typed["opaque"]; exists {
				observations := msp06Slice(t, rawOpaque, "opaque")
				if len(observations) > 256 {
					t.Errorf("opaque observations = %d, want <= 256", len(observations))
				}
				for _, rawObservation := range observations {
					observation := msp06Map(t, rawObservation, "opaque observation")
					msp06AssertKeys(t, observation, "opaque observation", "path", "source", "value")
					count++
					encoded, err := json.Marshal(observation["value"])
					if err != nil {
						t.Fatal(err)
					}
					canonical, _, err := eebusV1CanonicalHashJSON(encoded)
					if err != nil {
						t.Fatalf("opaque value is not JCS-compatible: %v", err)
					}
					if len(canonical) > 16384 {
						t.Errorf("opaque canonical value bytes = %d, want <= 16384", len(canonical))
					}
					aggregate += len(canonical)
					issue743AssertOpaqueValue(t, observation["value"], 0)
				}
			}
			for key, child := range typed {
				if key != "opaque" {
					walk(child)
				}
			}
		case []any:
			for _, child := range typed {
				walk(child)
			}
		}
	}
	walk(value)
	if count > 256 || aggregate > 262144 {
		t.Errorf("aggregate opaque bounds count=%d bytes=%d, want <=256/262144", count, aggregate)
	}
}

func issue743AssertOpaqueValue(t *testing.T, value any, depth int) {
	t.Helper()
	switch typed := value.(type) {
	case string:
		if len([]byte(typed)) > 4096 {
			t.Errorf("opaque string bytes = %d, want <= 4096", len([]byte(typed)))
		}
	case []any:
		if depth+1 > 3 || len(typed) > 32 {
			t.Errorf("opaque array depth/members = %d/%d, want <=3/32", depth+1, len(typed))
		}
		for _, child := range typed {
			issue743AssertOpaqueValue(t, child, depth+1)
		}
	case map[string]any:
		if depth+1 > 3 || len(typed) > 32 {
			t.Errorf("opaque object depth/members = %d/%d, want <=3/32", depth+1, len(typed))
		}
		for key, child := range typed {
			if len([]byte(key)) > 128 {
				t.Errorf("opaque key bytes = %d, want <= 128", len([]byte(key)))
			}
			issue743AssertOpaqueValue(t, child, depth+1)
		}
	}
}

func TestIssue743FixtureIsExactVR940RawShape(t *testing.T) {
	fixture := issue743FixtureMap(t)
	if got := len(msp06Slice(t, fixture["services"], "services")); got != 1 {
		t.Fatalf("services = %d, want 1", got)
	}
	for field, want := range map[string]int{"devices": 1, "entities": 11, "features": 20, "usecases": 10} {
		if got := len(msp06Slice(t, fixture[field], field)); got != want {
			t.Fatalf("%s = %d, want %d", field, got, want)
		}
	}
	service := msp06Map(t, msp06Slice(t, fixture["services"], "services")[0], "services[0]")
	for key, want := range map[string]any{
		"ski":        "2222222222222222222222222222222222222222",
		"ship_id":    "vaillant-vr940f-ship-id",
		"kind":       "remote",
		"visible":    true,
		"paired":     true,
		"name":       "Vaillant VR940f eeBUS",
		"identifier": "vr940f-lab-service",
		"brand":      "Vaillant",
		"type":       "eeBUS",
		"model":      "VR940f",
	} {
		if service[key] != want {
			t.Errorf("service.%s = %#v, want %#v", key, service[key], want)
		}
	}
	var names []string
	for _, raw := range msp06Slice(t, fixture["usecases"], "usecases") {
		usecase := msp06Map(t, raw, "usecase")
		names = append(names, usecase["name"].(string))
		for _, field := range []string{"context_address", "actor", "resolved_role", "scenarios", "version", "availability", "document_subrevision"} {
			if _, exists := usecase[field]; !exists {
				t.Errorf("usecase %q lacks %s", usecase["name"], field)
			}
		}
	}
	for _, nominal := range []string{
		"monitoringOfRoomTemperature",
		"configurationOfRoomHeatingTemperature",
		"monitoringOfRoomHeatingSystemFunction",
		"monitoringOfDhwTemperature",
		"monitoringOfOutdoorTemperature",
		"monitoringOfPowerConsumption",
	} {
		if !issue743Contains(names, nominal) {
			t.Errorf("nominal usecase %q absent from %v", nominal, names)
		}
	}
}

func TestIssue743PublicHTTPBoundaryIsAlwaysExplicitRedacted(t *testing.T) {
	server, _ := msp06TestServer(t, &msp06Provider{snapshot: msp06Snapshot(t, "runtime-a")})
	plain := msp06Call(t, server.Handler(), msp06RuntimeStatusTool, map[string]any{})
	meta := issue743Meta(t, plain, "redacted", "eebus.public.read")

	for _, header := range []string{"X-Mask-Tier", "X-Auth-Scope", "X-EEBus-Tier", "Authorization"} {
		withHeader := msp06CallWithHeaders(t, server.Handler(), msp06RuntimeStatusTool, map[string]any{}, map[string]string{header: "raw"})
		if withHeader.raw != plain.raw {
			t.Errorf("%s changed public boundary response", header)
		}
	}
	if meta["mask_tier"] != "redacted" {
		t.Fatalf("public tier = %v", meta["mask_tier"])
	}
}

func TestIssue743BothBoundariesExposeExactlyTheSameNineClosedTools(t *testing.T) {
	server, _ := msp06TestServer(t, &msp06Provider{snapshot: msp06Snapshot(t, "runtime-a")})
	publicNames := msp06NamesWithPrefix(msp06Tools(t, server), "eebus.")
	operatorResponse := doRPC(t, issue743OperatorHandler(t, server), rpcRequest{JSONRPC: "2.0", ID: 1, Method: "tools/list"})
	operatorResult := msp06Map(t, operatorResponse.Result, "operator tools/list")
	var operatorNames []string
	for _, raw := range msp06Slice(t, operatorResult["tools"], "operator tools") {
		name, _ := msp06Map(t, raw, "operator tool")["name"].(string)
		if strings.HasPrefix(name, "eebus.") {
			operatorNames = append(operatorNames, name)
		}
	}
	sort.Strings(operatorNames)
	want := append([]string(nil), msp06ToolNames...)
	sort.Strings(want)
	if !reflect.DeepEqual(publicNames, want) || !reflect.DeepEqual(operatorNames, want) {
		t.Fatalf("boundary inventories public=%v operator=%v, want %v", publicNames, operatorNames, want)
	}
	for _, name := range append(publicNames, operatorNames...) {
		if strings.Contains(name, ".v2.") || strings.Contains(name, "legacy") || strings.Contains(name, "raw.") || strings.Contains(name, "redacted.") {
			t.Errorf("forbidden namespace/alias %q", name)
		}
	}
}

func TestIssue743NoCallerInputSelectsTier(t *testing.T) {
	server, _ := msp06TestServer(t, &msp06Provider{snapshot: msp06Snapshot(t, "runtime-a")})
	for boundary, handler := range map[string]http.Handler{
		"public":   server.Handler(),
		"operator": issue743OperatorHandler(t, server),
	} {
		tier, authScope := "redacted", "eebus.public.read"
		if boundary == "operator" {
			tier, authScope = "raw", "eebus.raw.read"
		}
		for _, selector := range []string{"tier", "mask_tier", "auth_scope", "boundary", "principal", "authorization"} {
			result := msp06Call(t, handler, msp06RuntimeStatusTool, map[string]any{selector: "raw"})
			issue743AssertError(t, result, tier, authScope, "invalid_argument")
		}
		t.Run(boundary+" query and header", func(t *testing.T) {
			params, _ := json.Marshal(map[string]any{"name": msp06RuntimeStatusTool, "arguments": map[string]any{}})
			body, _ := json.Marshal(rpcRequest{JSONRPC: "2.0", ID: 1, Method: "tools/call", Params: params})
			request := httptestRequest(http.MethodPost, "/mcp?tier=raw&mask_tier=raw", body)
			request.Header.Set("X-Mask-Tier", "raw")
			request.Header.Set("X-Auth-Scope", "eebus.raw.read")
			recorder := &issue743Recorder{header: make(http.Header)}
			handler.ServeHTTP(recorder, request)
			if strings.Contains(recorder.body.String(), `"mask_tier":"raw"`) && boundary == "public" {
				t.Fatalf("caller query/header selected raw tier: %s", recorder.body.String())
			}
		})
	}
}

func TestIssue743RawTopologyPreservesVR940OperationalFields(t *testing.T) {
	server, _ := msp06TestServer(t, &msp06Provider{snapshot: msp06Snapshot(t, "runtime-a")})
	_ = issue743OperatorHandler(t, server)
	server, _ = issue743Server(t)
	operator := issue743OperatorHandler(t, server)

	servicesResult := msp06Call(t, operator, msp06ServicesListTool, map[string]any{})
	issue743Meta(t, servicesResult, "raw", "eebus.raw.read")
	services := msp06Slice(t, msp06Map(t, servicesResult.envelope["data"], "services data")["services"], "services")
	if len(services) != 1 {
		t.Fatalf("raw services = %d, want 1", len(services))
	}
	service := msp06Map(t, services[0], "services[0]")
	for _, field := range []string{"ski", "ship_id", "kind", "visible", "paired", "name", "identifier", "brand", "type", "model", "opaque"} {
		if _, exists := service[field]; !exists {
			t.Errorf("raw service lacks %s", field)
		}
	}

	topologyResult := msp06Call(t, operator, msp06TopologyGetTool, map[string]any{})
	issue743Meta(t, topologyResult, "raw", "eebus.raw.read")
	topology := msp06Map(t, topologyResult.envelope["data"], "topology")
	for field, want := range map[string]int{"devices": 1, "entities": 11, "features": 20, "usecases": 10} {
		if got := len(msp06Slice(t, topology[field], "topology."+field)); got != want {
			t.Errorf("raw topology %s = %d, want %d", field, got, want)
		}
	}
	device := msp06Map(t, msp06Slice(t, topology["devices"], "devices")[0], "device")
	for _, field := range []string{"ski", "ship_id", "address", "type", "description", "metadata", "opaque"} {
		if _, exists := device[field]; !exists {
			t.Errorf("raw device lacks %s", field)
		}
	}
	metadata := msp06Map(t, device["metadata"], "device.metadata")
	if metadata["commissioned"] != true {
		t.Errorf("present false/true metadata state changed: commissioned=%v", metadata["commissioned"])
	}
	if value, exists := metadata["radio_module"]; !exists || value != nil {
		t.Errorf("present null metadata state was not preserved: exists=%v value=%#v", exists, value)
	}
	entity := msp06Map(t, msp06Slice(t, topology["entities"], "entities")[0], "entity")
	for _, field := range []string{"device_address", "entity_address", "type", "description"} {
		if _, exists := entity[field]; !exists {
			t.Errorf("raw entity lacks %s", field)
		}
	}
	feature := msp06Map(t, msp06Slice(t, topology["features"], "features")[0], "feature")
	for _, field := range []string{"device_address", "entity_address", "feature_address", "type", "role", "description"} {
		if _, exists := feature[field]; !exists {
			t.Errorf("raw feature lacks %s", field)
		}
	}
	issue743AssertOpaqueBounds(t, topologyResult.envelope["data"])
}

func TestIssue743PublicProjectionIrreversiblyOmitsRawIdentityAddressAndMetadata(t *testing.T) {
	server, _ := issue743Server(t)
	for _, tool := range []string{msp06ServicesListTool, msp06TopologyGetTool, msp06SnapshotCapture} {
		result := msp06Call(t, server.Handler(), tool, map[string]any{})
		issue743Meta(t, result, "redacted", "eebus.public.read")
		issue743AssertNoRawIdentity(t, result.raw)
		if strings.Contains(result.raw, "candidate_ref") {
			t.Errorf("%s exposed candidate_ref", tool)
		}
	}
}

func TestIssue743RawSnapshotNeverLeaksIntoEbusV1Tools(t *testing.T) {
	server, _ := issue743Server(t)
	for _, tool := range []string{"ebus.v1.runtime.status.get", "ebus.v1.registry.devices.list", "ebus.v1.semantic.snapshot.get"} {
		result := msp06Call(t, server.Handler(), tool, map[string]any{})
		issue743AssertNoRawIdentity(t, result.raw)
		issue743AssertSecretCorpusAbsent(t, result.raw)
	}
}

func TestIssue743ReferencesBindTierAuthAndBoundaryInBothDirections(t *testing.T) {
	server, _ := issue743Server(t)
	public := server.Handler()
	operator := issue743OperatorHandler(t, server)

	_, publicRoot := msp06CaptureRoot(t, server)
	publicRefs := msp06RootRefs(t, publicRoot)
	publicOnRaw := msp06Call(t, operator, msp06TopologyGetTool, map[string]any{"evidence_ref": publicRefs["topology_ref"]})
	issue743AssertError(t, publicOnRaw, "raw", "eebus.raw.read", "permission_denied")

	rawCapture := msp06Call(t, operator, msp06SnapshotCapture, map[string]any{})
	issue743Meta(t, rawCapture, "raw", "eebus.raw.read")
	rawRoot := msp06Map(t, rawCapture.envelope["data"], "raw capture")
	rawRefs := msp06RootRefs(t, rawRoot)
	rawOnPublic := msp06Call(t, public, msp06TopologyGetTool, map[string]any{"evidence_ref": rawRefs["topology_ref"]})
	msp06AssertErrorMode(t, rawOnPublic, msp06TopologyGetTool, "topology", "evidence", "permission_denied")

	crossDrop := msp06Call(t, public, msp06SnapshotDrop, map[string]any{"snapshot_ref": rawRoot["snapshot_ref"]})
	msp06AssertErrorMode(t, crossDrop, msp06SnapshotDrop, "whole-root", "live", "permission_denied")
	ownDrop := msp06Call(t, operator, msp06SnapshotDrop, map[string]any{"snapshot_ref": rawRoot["snapshot_ref"]})
	issue743Meta(t, ownDrop, "raw", "eebus.raw.read")
	dropData := msp06Map(t, ownDrop.envelope["data"], "drop data")
	if dropData["status"] != "dropped" {
		t.Fatalf("operator own-tier drop status = %v", dropData["status"])
	}
}

func TestIssue743RawAndRedactedHashesAreIndependentDeterministicAndCapturedImmutable(t *testing.T) {
	server, provider := issue743Server(t)
	public := server.Handler()
	operator := issue743OperatorHandler(t, server)

	rawFirst := msp06Call(t, operator, msp06TopologyGetTool, map[string]any{})
	rawSecond := msp06Call(t, operator, msp06TopologyGetTool, map[string]any{})
	if rawFirst.raw != rawSecond.raw {
		t.Fatalf("raw JCS response is nondeterministic:\n%s\n%s", rawFirst.raw, rawSecond.raw)
	}
	rawHash := issue743Meta(t, rawFirst, "raw", "eebus.raw.read")["data_hash"]
	publicTopology := msp06Call(t, public, msp06TopologyGetTool, map[string]any{})
	publicHash := issue743Meta(t, publicTopology, "redacted", "eebus.public.read")["data_hash"]
	if rawHash == publicHash {
		t.Fatalf("raw and redacted topology hashes alias: %v", rawHash)
	}

	capture := msp06Call(t, operator, msp06SnapshotCapture, map[string]any{})
	root := msp06Map(t, capture.envelope["data"], "capture")
	refs := msp06RootRefs(t, root)
	before := msp06Call(t, operator, msp06TopologyGetTool, map[string]any{"evidence_ref": refs["topology_ref"]})

	fixture := issue743FixtureMap(t)
	msp06Map(t, msp06Slice(t, fixture["devices"], "devices")[0], "device")["description"] = "mutated after capture"
	mutated, err := json.Marshal(fixture)
	if err != nil {
		t.Fatal(err)
	}
	provider.set(issue743Snapshot(t, mutated), nil)
	after := msp06Call(t, operator, msp06TopologyGetTool, map[string]any{"evidence_ref": refs["topology_ref"]})
	if before.raw != after.raw {
		t.Fatalf("captured raw snapshot changed after provider mutation:\n%s\n%s", before.raw, after.raw)
	}
}

func TestIssue743SecretCorpusNeverEscapesEitherBoundaryOrErrors(t *testing.T) {
	server, provider := issue743Server(t)
	for name, handler := range map[string]http.Handler{"public": server.Handler(), "operator": issue743OperatorHandler(t, server)} {
		for _, tool := range []string{msp06ServicesListTool, msp06TopologyGetTool, msp06SnapshotCapture} {
			result := msp06Call(t, handler, tool, map[string]any{})
			issue743AssertSecretCorpusAbsent(t, result.raw)
		}
		t.Run(name+" error", func(t *testing.T) {
			provider.set(eebusruntime.SnapshotV1{}, errors.New(strings.Join([]string{
				"-----BEGIN ENCRYPTED PRIVATE KEY-----",
				"-----BEGIN OPENSSH PRIVATE KEY-----",
				"-----BEGIN DSA PRIVATE KEY-----",
				"-----BEGIN PGP PRIVATE KEY BLOCK-----",
				"private_key",
				"bearer_token",
			}, " ")))
			result := msp06Call(t, handler, msp06TopologyGetTool, map[string]any{})
			if !result.isError {
				t.Fatalf("%s backend error returned success", name)
			}
			issue743AssertSecretCorpusAbsent(t, result.raw)
		})
		provider.set(issue743Snapshot(t, issue743FixtureBytes(t)), nil)
	}
}

func TestIssue743OperatorSocketModesAndSameEffectiveUIDOnLinux(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Linux peer credential proof is normative")
	}
	server, _ := issue743Server(t)
	endpoint, ok := any(server).(issue743OperatorEndpointProvider)
	if !ok {
		t.Fatal("eeBUS AF_UNIX operator endpoint is unavailable")
	}
	if got := endpoint.eebusV1OperatorSocketPath(); got != "/data/eebus/operator-mcp.sock" {
		t.Fatalf("operator socket path = %q, want /data/eebus/operator-mcp.sock", got)
	}
	socketPath := filepath.Join(t.TempDir(), "eebus", "operator-mcp.sock")
	closer, err := endpoint.eebusV1ServeOperator(context.Background(), socketPath, func(net.Conn) (int, error) {
		return os.Geteuid(), nil
	})
	if err != nil {
		t.Fatalf("start operator endpoint: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })

	parentInfo, err := os.Stat(filepath.Dir(socketPath))
	if err != nil {
		t.Fatal(err)
	}
	socketInfo, err := os.Stat(socketPath)
	if err != nil {
		t.Fatal(err)
	}
	if parentInfo.Mode().Perm() != 0o700 || socketInfo.Mode().Perm() != 0o600 || socketInfo.Mode()&os.ModeSocket == 0 {
		t.Fatalf("operator endpoint modes parent=%#o socket=%#o type=%v, want 0700/0600/AF_UNIX", parentInfo.Mode().Perm(), socketInfo.Mode().Perm(), socketInfo.Mode())
	}
	response := issue743CallUnix(t, socketPath, msp06RuntimeStatusTool, map[string]any{})
	issue743Meta(t, response, "raw", "eebus.raw.read")
}

func TestIssue743OperatorSocketStartupFailsClosedWhenProofOrListenerUnavailable(t *testing.T) {
	server, _ := msp06TestServer(t, &msp06Provider{snapshot: msp06Snapshot(t, "runtime-a")})
	endpoint, ok := any(server).(issue743OperatorEndpointProvider)
	if !ok {
		t.Fatal("eeBUS AF_UNIX operator endpoint is unavailable")
	}
	root := t.TempDir()
	parentAsFile := filepath.Join(root, "not-a-directory")
	if err := os.WriteFile(parentAsFile, []byte("occupied"), 0o600); err != nil {
		t.Fatal(err)
	}
	sameUID := func(net.Conn) (int, error) { return os.Geteuid(), nil }
	if closer, err := endpoint.eebusV1ServeOperator(context.Background(), filepath.Join(parentAsFile, "operator-mcp.sock"), sameUID); err == nil {
		_ = closer.Close()
		t.Fatal("operator endpoint started without parent/listener/mode proof")
	}
	if closer, err := endpoint.eebusV1ServeOperator(context.Background(), filepath.Join(root, "operator-mcp.sock"), nil); err == nil {
		_ = closer.Close()
		t.Fatal("operator endpoint started where required peer credential proof is unavailable")
	}
	occupiedPath := filepath.Join(root, "occupied.sock")
	occupied, err := net.Listen("unix", occupiedPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = occupied.Close() }()
	if closer, err := endpoint.eebusV1ServeOperator(context.Background(), occupiedPath, sameUID); err == nil {
		_ = closer.Close()
		t.Fatal("operator endpoint replaced an active listener instead of failing closed")
	}
}

func TestIssue743OperatorSocketRejectsDifferentEffectiveUIDBeforeProviderAccess(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Linux peer credential proof is normative")
	}
	server, provider := issue743Server(t)
	endpoint, ok := any(server).(issue743OperatorEndpointProvider)
	if !ok {
		t.Fatal("eeBUS AF_UNIX operator endpoint is unavailable")
	}
	socketPath := filepath.Join(t.TempDir(), "eebus", "operator-mcp.sock")
	closer, err := endpoint.eebusV1ServeOperator(context.Background(), socketPath, func(net.Conn) (int, error) {
		return os.Geteuid() + 1, nil
	})
	if err != nil {
		t.Fatalf("start mismatched-UID test endpoint: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })

	before := providerCallCount(provider)
	transport := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", socketPath)
		},
	}
	defer transport.CloseIdleConnections()
	request, err := http.NewRequest(http.MethodPost, "http://unix/mcp", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"tools/list"}`))
	if err != nil {
		t.Fatal(err)
	}
	response, requestErr := (&http.Client{Transport: transport, Timeout: time.Second}).Do(request)
	if response != nil {
		_ = response.Body.Close()
	}
	if requestErr == nil && response != nil && response.StatusCode < http.StatusBadRequest {
		t.Fatalf("mismatched effective UID reached operator MCP with status %d", response.StatusCode)
	}
	if after := providerCallCount(provider); after != before {
		t.Fatalf("mismatched effective UID reached provider: calls %d -> %d", before, after)
	}
}

func providerCallCount(provider *issue743Provider) int {
	provider.mu.Lock()
	defer provider.mu.Unlock()
	return provider.calls
}

// Minimal recorder/request helpers keep these boundary tests independent of
// httptest's fixed URL while still exercising the real HTTP handler.
type issue743Recorder struct {
	header http.Header
	body   bytes.Buffer
	code   int
}

func (recorder *issue743Recorder) Header() http.Header { return recorder.header }
func (recorder *issue743Recorder) Write(body []byte) (int, error) {
	if recorder.code == 0 {
		recorder.code = http.StatusOK
	}
	return recorder.body.Write(body)
}
func (recorder *issue743Recorder) WriteHeader(code int) { recorder.code = code }

func httptestRequest(method, target string, body []byte) *http.Request {
	request, err := http.NewRequest(method, target, bytes.NewReader(body))
	if err != nil {
		panic(err)
	}
	return request
}

func issue743CallUnix(t *testing.T, socketPath, tool string, arguments map[string]any) msp06CallResult {
	t.Helper()
	params, err := json.Marshal(map[string]any{"name": tool, "arguments": arguments})
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(rpcRequest{JSONRPC: "2.0", ID: 1, Method: "tools/call", Params: params})
	if err != nil {
		t.Fatal(err)
	}
	transport := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", socketPath)
		},
	}
	defer transport.CloseIdleConnections()
	request, err := http.NewRequest(http.MethodPost, "http://unix/mcp", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	response, err := (&http.Client{Transport: transport}).Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = response.Body.Close() }()
	responseBody, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	var rpc rpcResponse
	if err := json.Unmarshal(responseBody, &rpc); err != nil {
		t.Fatalf("decode Unix RPC response: %v; body=%q", err, responseBody)
	}
	result, ok := rpc.Result.(map[string]any)
	if !ok {
		t.Fatalf("Unix result type = %T", rpc.Result)
	}
	content, ok := result["content"].([]any)
	if !ok || len(content) != 1 {
		t.Fatalf("Unix content = %#v", result["content"])
	}
	item := msp06Map(t, content[0], "Unix content item")
	raw, _ := item["text"].(string)
	var envelope map[string]any
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&envelope); err != nil {
		t.Fatalf("decode Unix envelope: %v; text=%q", err, raw)
	}
	return msp06CallResult{envelope: envelope, raw: raw, isError: result["isError"] == true}
}
