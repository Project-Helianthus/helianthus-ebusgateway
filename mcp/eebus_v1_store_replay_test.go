package mcp

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
	eebusruntime "github.com/Project-Helianthus/helianthus-eebusreg"
)

var msp06EvidenceRefBindings = map[string]struct {
	tool  string
	scope string
}{
	"runtime_status_ref": {tool: msp06RuntimeStatusTool, scope: "runtime-status"},
	"services_list_ref":  {tool: msp06ServicesListTool, scope: "services"},
	"services_get_ref":   {tool: msp06ServicesGetTool, scope: "service"},
	"sessions_list_ref":  {tool: msp06SessionsListTool, scope: "sessions"},
	"sessions_get_ref":   {tool: msp06SessionsGetTool, scope: "session"},
	"topology_ref":       {tool: msp06TopologyGetTool, scope: "topology"},
	"pairing_status_ref": {tool: msp06PairingStatusTool, scope: "pairing-status"},
}

func msp06CaptureRoot(t *testing.T, server *Server) (msp06CallResult, map[string]any) {
	t.Helper()
	result := msp06Call(t, server.Handler(), msp06SnapshotCapture, map[string]any{})
	data := msp06AssertSuccess(t, result, msp06SnapshotCapture, "whole-root", "live")
	msp06AssertCapturedRoot(t, data)
	return result, data
}

func msp06AssertCapturedRoot(t *testing.T, root map[string]any) {
	t.Helper()
	msp06AssertKeys(t, root, "captured root", "snapshot_ref", "expires_at", "snapshot_content_hash", "evidence_refs", "snapshot")
	rootToken := msp06AssertToken(t, root["snapshot_ref"], "snapshot_ref")
	expiresAt, _ := root["expires_at"].(string)
	if !msp06TimestampPattern.MatchString(expiresAt) {
		t.Fatalf("expires_at = %#v, want UTC RFC3339", root["expires_at"])
	}
	contentHash, _ := root["snapshot_content_hash"].(string)
	if !msp06HashPattern.MatchString(contentHash) {
		t.Fatalf("snapshot_content_hash = %#v", root["snapshot_content_hash"])
	}

	refs := msp06Map(t, root["evidence_refs"], "evidence_refs")
	wantRefKeys := make([]string, 0, len(msp06EvidenceRefBindings))
	seen := map[string]string{rootToken: "snapshot_ref"}
	for key := range msp06EvidenceRefBindings {
		wantRefKeys = append(wantRefKeys, key)
	}
	msp06AssertKeys(t, refs, "evidence_refs", wantRefKeys...)
	for key := range msp06EvidenceRefBindings {
		token := msp06AssertToken(t, refs[key], "evidence_refs."+key)
		if prior, exists := seen[token]; exists {
			t.Fatalf("%s reuses token from %s", key, prior)
		}
		seen[token] = key
	}

	snapshot := msp06Map(t, root["snapshot"], "snapshot")
	keys := []string{"meta", "status", "pairing", "services", "sessions", "topology"}
	if _, exists := snapshot["evidence"]; exists {
		keys = append(keys, "evidence")
		msp06AssertEvidence(t, snapshot["evidence"], "snapshot.evidence")
	}
	msp06AssertKeys(t, snapshot, "snapshot", keys...)
	meta := msp06Map(t, snapshot["meta"], "snapshot.meta")
	msp06AssertKeys(t, meta, "snapshot.meta", "contract", "runtime", "mask_tier", "captured_at", "data_timestamp", "data_hash")
	if meta["contract"] != "helianthus.eebus.runtime.raw-snapshot.v1" || meta["mask_tier"] != "redacted" {
		t.Fatalf("snapshot.meta contract/mask = %#v", meta)
	}
	msp06AssertIdentity(t, meta["runtime"], "snapshot.meta.runtime", "runtime")
	for _, field := range []string{"captured_at", "data_timestamp"} {
		if value, _ := meta[field].(string); !msp06TimestampPattern.MatchString(value) {
			t.Fatalf("snapshot.meta.%s = %#v", field, meta[field])
		}
	}
	if meta["data_hash"] != contentHash {
		t.Fatalf("snapshot.meta.data_hash = %#v, want snapshot_content_hash %q", meta["data_hash"], contentHash)
	}

	status := msp06Map(t, snapshot["status"], "snapshot.status")
	statusKeys := []string{"state"}
	if _, exists := status["degradation"]; exists {
		statusKeys = append(statusKeys, "degradation")
	}
	msp06AssertKeys(t, status, "snapshot.status", statusKeys...)
	msp06AssertSnapshotCollections(t, snapshot)
}

func msp06AssertSnapshotCollections(t *testing.T, snapshot map[string]any) {
	t.Helper()
	services := map[string]any{"services": snapshot["services"]}
	serviceItems := msp06Slice(t, services["services"], "snapshot.services")
	for index, raw := range serviceItems {
		service := msp06Map(t, raw, fmt.Sprintf("snapshot.services[%d]", index))
		msp06AssertIdentity(t, service["id"], fmt.Sprintf("snapshot.services[%d].id", index), "service")
	}

	sessions := msp06Slice(t, snapshot["sessions"], "snapshot.sessions")
	for index, raw := range sessions {
		session := msp06Map(t, raw, fmt.Sprintf("snapshot.sessions[%d]", index))
		msp06AssertIdentity(t, session["id"], fmt.Sprintf("snapshot.sessions[%d].id", index), "session")
		msp06AssertIdentity(t, session["remote"], fmt.Sprintf("snapshot.sessions[%d].remote", index), "remote")
	}

	pairing := msp06Slice(t, snapshot["pairing"], "snapshot.pairing")
	for index, raw := range pairing {
		item := msp06Map(t, raw, fmt.Sprintf("snapshot.pairing[%d]", index))
		msp06AssertIdentity(t, item["remote"], fmt.Sprintf("snapshot.pairing[%d].remote", index), "remote")
	}
	msp06AssertTopology(t, msp06Map(t, snapshot["topology"], "snapshot.topology"))
}

func msp06RootRefs(t *testing.T, root map[string]any) map[string]string {
	t.Helper()
	refsObject := msp06Map(t, root["evidence_refs"], "evidence_refs")
	refs := make(map[string]string, len(refsObject))
	for key := range msp06EvidenceRefBindings {
		refs[key] = msp06AssertToken(t, refsObject[key], "evidence_refs."+key)
	}
	return refs
}

func msp06CapturedSelectors(t *testing.T, root map[string]any) (serviceID, sessionID string) {
	t.Helper()
	snapshot := msp06Map(t, root["snapshot"], "snapshot")
	services := msp06Slice(t, snapshot["services"], "snapshot.services")
	sessions := msp06Slice(t, snapshot["sessions"], "snapshot.sessions")
	if len(services) == 0 || len(sessions) == 0 {
		t.Fatal("capture fixture lacks service/session selector")
	}
	service := msp06Map(t, services[0], "snapshot.services[0]")
	session := msp06Map(t, sessions[0], "snapshot.sessions[0]")
	return msp06AssertIdentity(t, service["id"], "snapshot.services[0].id", "service"),
		msp06AssertIdentity(t, session["id"], "snapshot.sessions[0].id", "session")
}

func msp06EvidenceArguments(key, reference, serviceID, sessionID string) map[string]any {
	arguments := map[string]any{"evidence_ref": reference}
	switch key {
	case "services_get_ref":
		arguments["id_digest"] = serviceID
	case "sessions_get_ref":
		arguments["id_digest"] = sessionID
	}
	return arguments
}

func TestMSP06G12WholeRootCaptureHasEightCanonicalRefsAndSeparateHashes(t *testing.T) {
	provider := &msp06Provider{snapshot: msp06Snapshot(t, "runtime-a")}
	server, clock := msp06TestServer(t, provider)
	firstResult, first := msp06CaptureRoot(t, server)
	secondResult, second := msp06CaptureRoot(t, server)

	if provider.callCount() != 2 {
		t.Fatalf("capture provider calls = %d, want one per capture", provider.callCount())
	}
	if first["snapshot_ref"] == second["snapshot_ref"] || reflect.DeepEqual(first["evidence_refs"], second["evidence_refs"]) {
		t.Fatal("two captures reused opaque references")
	}
	if first["snapshot_content_hash"] != second["snapshot_content_hash"] {
		t.Fatalf("identical detached roots have different content hashes: %v vs %v", first["snapshot_content_hash"], second["snapshot_content_hash"])
	}
	firstMeta := msp06Map(t, firstResult.envelope["meta"], "first meta")
	secondMeta := msp06Map(t, secondResult.envelope["meta"], "second meta")
	if firstMeta["data_hash"] != secondMeta["data_hash"] {
		t.Fatalf("token substitution changed capture envelope hash: %v vs %v", firstMeta["data_hash"], secondMeta["data_hash"])
	}
	if firstMeta["data_hash"] == first["snapshot_content_hash"] {
		t.Fatal("capture envelope hash unexpectedly aliases snapshot content hash")
	}
	wantExpiry := clock.Now().Add(5 * time.Minute).Format(time.RFC3339Nano)
	if first["expires_at"] != wantExpiry || second["expires_at"] != wantExpiry {
		t.Fatalf("capture expiry = (%v, %v), want %s", first["expires_at"], second["expires_at"], wantExpiry)
	}
}

func TestMSP06CaptureDoesNotCopyRuntimeInternalSourceHash(t *testing.T) {
	source, err := eebusruntime.NewSnapshotV1(msp06Snapshot(t, "runtime-a"))
	if err != nil {
		t.Fatal(err)
	}
	if source.Meta.DataHash == "" {
		t.Fatal("source fixture lacks internal data hash")
	}
	server, _ := msp06TestServer(t, &msp06Provider{snapshot: source})
	_, root := msp06CaptureRoot(t, server)
	if root["snapshot_content_hash"] == source.Meta.DataHash {
		t.Fatal("wire snapshot content hash copied runtime internal source hash")
	}
	encoded, err := json.Marshal(root)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(encoded, []byte(source.Meta.DataHash)) {
		t.Fatalf("runtime internal source hash leaked as correlator: %s", encoded)
	}
}

func TestMSP06G13EvidenceReplayIsByteStableAndMakesZeroProviderCalls(t *testing.T) {
	provider := &msp06Provider{snapshot: msp06Snapshot(t, "runtime-a")}
	server, _ := msp06TestServer(t, provider)
	_, root := msp06CaptureRoot(t, server)
	refs := msp06RootRefs(t, root)
	serviceID, sessionID := msp06CapturedSelectors(t, root)
	providerCallsAfterCapture := provider.callCount()

	first := make(map[string]msp06CallResult, len(refs))
	for key, binding := range msp06EvidenceRefBindings {
		result := msp06Call(t, server.Handler(), binding.tool, msp06EvidenceArguments(key, refs[key], serviceID, sessionID))
		msp06AssertSuccess(t, result, binding.tool, binding.scope, "evidence")
		first[key] = result
	}
	if provider.callCount() != providerCallsAfterCapture {
		t.Fatalf("evidence reads called provider: before=%d after=%d", providerCallsAfterCapture, provider.callCount())
	}

	provider.mutate(func(snapshot *eebusruntime.SnapshotV1) {
		snapshot.Meta.Runtime = msp06SourceID(t, snapshot.Meta.Runtime.Kind, "runtime-mutated")
		snapshot.Services[0].Visible = false
		snapshot.Topology.Devices = nil
	}, errors.New("disconnected after capture; private 192.0.2.77 detail"))
	for key, binding := range msp06EvidenceRefBindings {
		result := msp06Call(t, server.Handler(), binding.tool, msp06EvidenceArguments(key, refs[key], serviceID, sessionID))
		msp06AssertSuccess(t, result, binding.tool, binding.scope, "evidence")
		if result.raw != first[key].raw {
			t.Fatalf("%s replay changed after provider mutation/disconnect:\nfirst=%s\nsecond=%s", key, first[key].raw, result.raw)
		}
	}
	if provider.callCount() != providerCallsAfterCapture {
		t.Fatalf("replay after disconnect called provider: before=%d after=%d", providerCallsAfterCapture, provider.callCount())
	}
}

func TestMSP06ReconnectReturnsLiveErrorsThenRecoversWithoutReregistration(t *testing.T) {
	provider := &msp06Provider{snapshot: msp06Snapshot(t, "runtime-a")}
	server, _ := msp06TestServer(t, provider)
	wantInventory := msp06NamesWithPrefix(msp06Tools(t, server), "eebus.")
	_, root := msp06CaptureRoot(t, server)
	refs := msp06RootRefs(t, root)

	provider.set(eebusruntime.SnapshotV1{}, errors.New("runtime worker disconnected"))
	liveFailure := msp06Call(t, server.Handler(), msp06ServicesListTool, map[string]any{})
	msp06AssertError(t, liveFailure, msp06ServicesListTool, "services", "backend_unavailable")
	evidence := msp06Call(t, server.Handler(), msp06ServicesListTool, map[string]any{"evidence_ref": refs["services_list_ref"]})
	msp06AssertSuccess(t, evidence, msp06ServicesListTool, "services", "evidence")

	recovered := msp06Snapshot(t, "runtime-a")
	recovered.Services[0].Visible = false
	provider.set(recovered, nil)
	liveSuccess := msp06Call(t, server.Handler(), msp06ServicesListTool, map[string]any{})
	data := msp06AssertSuccess(t, liveSuccess, msp06ServicesListTool, "services", "live")
	items := msp06Slice(t, data["services"], "services")
	foundFalse := false
	for _, raw := range items {
		if msp06Map(t, raw, "service")["visible"] == false {
			foundFalse = true
		}
	}
	if !foundFalse {
		t.Fatal("recovered live call did not use fresh provider snapshot")
	}
	if got := msp06NamesWithPrefix(msp06Tools(t, server), "eebus."); !reflect.DeepEqual(got, wantInventory) {
		t.Fatalf("tool inventory changed across disconnect/reconnect: %v vs %v", wantInventory, got)
	}
}

func TestMSP06G14ReferencesAreToolScopeBoundAndPolicyCannotBeOverridden(t *testing.T) {
	provider := &msp06Provider{snapshot: msp06Snapshot(t, "runtime-a")}
	server, _ := msp06TestServer(t, provider)
	_, root := msp06CaptureRoot(t, server)
	refs := msp06RootRefs(t, root)
	serviceID, _ := msp06CapturedSelectors(t, root)

	wrongBinding := msp06Call(t, server.Handler(), msp06SessionsListTool, map[string]any{"evidence_ref": refs["services_list_ref"]})
	msp06AssertErrorMode(t, wrongBinding, msp06SessionsListTool, "sessions", "evidence", "permission_denied")
	rootAsEvidence := msp06Call(t, server.Handler(), msp06RuntimeStatusTool, map[string]any{"evidence_ref": root["snapshot_ref"]})
	msp06AssertErrorMode(t, rootAsEvidence, msp06RuntimeStatusTool, "runtime-status", "evidence", "permission_denied")
	listRefAsGet := msp06Call(t, server.Handler(), msp06ServicesGetTool, map[string]any{"id_digest": serviceID, "evidence_ref": refs["services_list_ref"]})
	msp06AssertErrorMode(t, listRefAsGet, msp06ServicesGetTool, "service", "evidence", "permission_denied")

	unknown := base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{0xee}, sha256.Size))
	unknownResult := msp06Call(t, server.Handler(), msp06ServicesListTool, map[string]any{"evidence_ref": unknown})
	msp06AssertErrorMode(t, unknownResult, msp06ServicesListTool, "services", "evidence", "not_found")
	if provider.callCount() != 1 {
		t.Fatalf("reference resolution called provider %d times, want capture only", provider.callCount())
	}

	headers := map[string]string{
		"Authorization":   "Bearer administrator-secret",
		"X-Mask-Tier":     "raw",
		"X-Auth-Scope":    "eebus.admin",
		"X-EEBus-Runtime": "runtime-override",
	}
	plain := msp06Call(t, server.Handler(), msp06RuntimeStatusTool, map[string]any{})
	withHeaders := msp06CallWithHeaders(t, server.Handler(), msp06RuntimeStatusTool, map[string]any{}, headers)
	msp06AssertSuccess(t, plain, msp06RuntimeStatusTool, "runtime-status", "live")
	meta := msp06AssertMeta(t, withHeaders.envelope, msp06RuntimeStatusTool, "runtime-status", "live")
	if meta["mask_tier"] != "redacted" || meta["auth_scope"] != "eebus.raw.read" || plain.raw != withHeaders.raw {
		t.Fatalf("headers altered fixed reader policy or response:\nplain=%s\nheaders=%s", plain.raw, withHeaders.raw)
	}

	for _, forbidden := range []string{"contract", "runtime", "tool", "scope", "mask_tier", "auth_scope", "principal", "authorization"} {
		before := provider.callCount()
		result := msp06Call(t, server.Handler(), msp06ServicesListTool, map[string]any{forbidden: "override"})
		msp06AssertError(t, result, msp06ServicesListTool, "services", "invalid_argument")
		if provider.callCount() != before {
			t.Fatalf("forbidden binding selector %q reached provider", forbidden)
		}
	}

	otherServer, _ := msp06TestServer(t, &msp06Provider{snapshot: msp06Snapshot(t, "runtime-b")})
	crossRuntime := msp06Call(t, otherServer.Handler(), msp06ServicesListTool, map[string]any{"evidence_ref": refs["services_list_ref"]})
	msp06AssertErrorMode(t, crossRuntime, msp06ServicesListTool, "services", "evidence", "not_found")
}

func TestMSP06WrongBindingPrecedesReferenceLifecycle(t *testing.T) {
	server, _ := msp06TestServer(t, &msp06Provider{snapshot: msp06Snapshot(t, "runtime-a")})
	_, root := msp06CaptureRoot(t, server)
	refs := msp06RootRefs(t, root)
	drop := msp06Call(t, server.Handler(), msp06SnapshotDrop, map[string]any{"snapshot_ref": root["snapshot_ref"]})
	msp06AssertDropStatus(t, drop, "dropped")

	wrong := msp06Call(t, server.Handler(), msp06SessionsListTool, map[string]any{"evidence_ref": refs["services_list_ref"]})
	msp06AssertErrorMode(t, wrong, msp06SessionsListTool, "sessions", "evidence", "permission_denied")
	exact := msp06Call(t, server.Handler(), msp06ServicesListTool, map[string]any{"evidence_ref": refs["services_list_ref"]})
	msp06AssertErrorMode(t, exact, msp06ServicesListTool, "services", "evidence", "snapshot_gone")
}

func msp06AssertDropStatus(t *testing.T, result msp06CallResult, want string) {
	t.Helper()
	data := msp06AssertSuccess(t, result, msp06SnapshotDrop, "whole-root", "live")
	msp06AssertKeys(t, data, "drop result", "status")
	if data["status"] != want {
		t.Fatalf("drop status = %#v, want %q", data["status"], want)
	}
}

func TestMSP06G15DropIsIdempotentAndEvidenceCannotDropRoot(t *testing.T) {
	provider := &msp06Provider{snapshot: msp06Snapshot(t, "runtime-a")}
	server, _ := msp06TestServer(t, provider)
	_, root := msp06CaptureRoot(t, server)
	refs := msp06RootRefs(t, root)
	serviceID, sessionID := msp06CapturedSelectors(t, root)
	calls := provider.callCount()

	evidenceAsRoot := msp06Call(t, server.Handler(), msp06SnapshotDrop, map[string]any{"snapshot_ref": refs["services_list_ref"]})
	msp06AssertDropStatus(t, evidenceAsRoot, "already_gone")
	stillActive := msp06Call(t, server.Handler(), msp06ServicesListTool, map[string]any{"evidence_ref": refs["services_list_ref"]})
	msp06AssertSuccess(t, stillActive, msp06ServicesListTool, "services", "evidence")

	first := msp06Call(t, server.Handler(), msp06SnapshotDrop, map[string]any{"snapshot_ref": root["snapshot_ref"]})
	second := msp06Call(t, server.Handler(), msp06SnapshotDrop, map[string]any{"snapshot_ref": root["snapshot_ref"]})
	msp06AssertDropStatus(t, first, "dropped")
	msp06AssertDropStatus(t, second, "already_gone")
	for key, binding := range msp06EvidenceRefBindings {
		terminal := msp06Call(t, server.Handler(), binding.tool, msp06EvidenceArguments(key, refs[key], serviceID, sessionID))
		msp06AssertErrorMode(t, terminal, binding.tool, binding.scope, "evidence", "snapshot_gone")
	}
	unknown := base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{0xcc}, sha256.Size))
	msp06AssertDropStatus(t, msp06Call(t, server.Handler(), msp06SnapshotDrop, map[string]any{"snapshot_ref": unknown}), "already_gone")
	malformed := msp06Call(t, server.Handler(), msp06SnapshotDrop, map[string]any{"snapshot_ref": "not-a-token"})
	msp06AssertError(t, malformed, msp06SnapshotDrop, "whole-root", "invalid_argument")
	if provider.callCount() != calls {
		t.Fatalf("drop/evidence operations called provider: before=%d after=%d", calls, provider.callCount())
	}
}

func TestMSP06G15ActiveTTLAndTombstoneTTLUseInclusiveFiveMinuteBoundary(t *testing.T) {
	server, clock := msp06TestServer(t, &msp06Provider{snapshot: msp06Snapshot(t, "runtime-a")})
	_, root := msp06CaptureRoot(t, server)
	refs := msp06RootRefs(t, root)

	clock.Advance(5 * time.Minute)
	expired := msp06Call(t, server.Handler(), msp06ServicesListTool, map[string]any{"evidence_ref": refs["services_list_ref"]})
	msp06AssertErrorMode(t, expired, msp06ServicesListTool, "services", "evidence", "snapshot_gone")
	msp06AssertDropStatus(t, msp06Call(t, server.Handler(), msp06SnapshotDrop, map[string]any{"snapshot_ref": root["snapshot_ref"]}), "already_gone")

	clock.Advance(5 * time.Minute)
	evicted := msp06Call(t, server.Handler(), msp06ServicesListTool, map[string]any{"evidence_ref": refs["services_list_ref"]})
	msp06AssertErrorMode(t, evicted, msp06ServicesListTool, "services", "evidence", "not_found")
	msp06AssertDropStatus(t, msp06Call(t, server.Handler(), msp06SnapshotDrop, map[string]any{"snapshot_ref": root["snapshot_ref"]}), "already_gone")
}

func TestMSP06G15ActiveQuotaCountsRootGroupsAndCaptureFailureConsumesNoSlot(t *testing.T) {
	provider := &msp06Provider{snapshot: msp06Snapshot(t, "runtime-a")}
	server, _ := msp06TestServer(t, provider)
	for index := 0; index < 32; index++ {
		_, root := msp06CaptureRoot(t, server)
		if len(msp06RootRefs(t, root)) != 7 {
			t.Fatalf("capture %d did not retain seven descendants", index)
		}
	}
	full := msp06Call(t, server.Handler(), msp06SnapshotCapture, map[string]any{})
	msp06AssertError(t, full, msp06SnapshotCapture, "whole-root", "quota_exceeded")
	if provider.callCount() != 33 {
		t.Fatalf("capture builds before quota reservation: provider calls=%d, want 33", provider.callCount())
	}

	failingProvider := &msp06Provider{err: errors.New("initial backend failure")}
	serverAfterFailure, _ := msp06TestServer(t, failingProvider)
	failed := msp06Call(t, serverAfterFailure.Handler(), msp06SnapshotCapture, map[string]any{})
	msp06AssertError(t, failed, msp06SnapshotCapture, "whole-root", "backend_unavailable")
	failingProvider.set(msp06Snapshot(t, "runtime-a"), nil)
	for index := 0; index < 32; index++ {
		msp06CaptureRoot(t, serverAfterFailure)
	}
	msp06AssertError(t, msp06Call(t, serverAfterFailure.Handler(), msp06SnapshotCapture, map[string]any{}), msp06SnapshotCapture, "whole-root", "quota_exceeded")
}

func TestMSP06G15ConcurrentCaptureReservationIsAtomicAt32(t *testing.T) {
	provider := &msp06Provider{snapshot: msp06Snapshot(t, "runtime-a")}
	server, _ := msp06TestServer(t, provider)
	const attempts = 64
	type outcome struct {
		code string
		err  error
	}
	outcomes := make(chan outcome, attempts)
	var start sync.WaitGroup
	start.Add(1)
	for index := 0; index < attempts; index++ {
		go func() {
			start.Wait()
			outcomes <- msp06ConcurrentCapture(server.Handler())
		}()
	}
	start.Done()

	successes := 0
	quota := 0
	for index := 0; index < attempts; index++ {
		result := <-outcomes
		if result.err != nil {
			t.Fatalf("concurrent capture %d: %v", index, result.err)
		}
		switch result.code {
		case "":
			successes++
		case "quota_exceeded":
			quota++
		default:
			t.Fatalf("concurrent capture code = %q", result.code)
		}
	}
	if successes != 32 || quota != 32 {
		t.Fatalf("concurrent captures success=%d quota=%d, want 32/32", successes, quota)
	}
	if provider.callCount() != attempts {
		t.Fatalf("concurrent provider calls = %d, want %d detached builds", provider.callCount(), attempts)
	}
}

func msp06ConcurrentCapture(handler http.Handler) (result struct {
	code string
	err  error
}) {
	params := json.RawMessage(`{"name":"eebus.v1.snapshot.capture","arguments":{}}`)
	body, err := json.Marshal(rpcRequest{JSONRPC: "2.0", ID: 1, Method: "tools/call", Params: params})
	if err != nil {
		result.err = err
		return result
	}
	request := httptest.NewRequest(http.MethodPost, "/mcp", bytes.NewReader(body))
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	var response rpcResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		result.err = err
		return result
	}
	if response.Error != nil {
		result.err = fmt.Errorf("RPC error: %+v", response.Error)
		return result
	}
	value, ok := response.Result.(map[string]any)
	if !ok {
		result.err = fmt.Errorf("result type %T", response.Result)
		return result
	}
	content, ok := value["content"].([]any)
	if !ok || len(content) != 1 {
		result.err = fmt.Errorf("content %#v", value["content"])
		return result
	}
	item, ok := content[0].(map[string]any)
	if !ok {
		result.err = fmt.Errorf("content item %T", content[0])
		return result
	}
	var envelope map[string]any
	if err := json.Unmarshal([]byte(fmt.Sprint(item["text"])), &envelope); err != nil {
		result.err = err
		return result
	}
	if rawError, exists := envelope["error"]; exists && rawError != nil {
		errorObject, ok := rawError.(map[string]any)
		if !ok {
			result.err = fmt.Errorf("error object %T", rawError)
			return result
		}
		result.code, _ = errorObject["code"].(string)
	}
	return result
}

func TestMSP06LiveReadsAllocateNoStoreEntries(t *testing.T) {
	provider := &msp06Provider{snapshot: msp06Snapshot(t, "runtime-a")}
	server, _ := msp06TestServer(t, provider)
	for index := 0; index < 40; index++ {
		msp06AssertSuccess(t, msp06Call(t, server.Handler(), msp06RuntimeStatusTool, map[string]any{}), msp06RuntimeStatusTool, "runtime-status", "live")
	}
	for index := 0; index < 32; index++ {
		msp06CaptureRoot(t, server)
	}
	msp06AssertError(t, msp06Call(t, server.Handler(), msp06SnapshotCapture, map[string]any{}), msp06SnapshotCapture, "whole-root", "quota_exceeded")
	if provider.callCount() != 73 {
		t.Fatalf("provider calls = %d, want 40 live + 32 captures + 1 detached quota attempt", provider.callCount())
	}
}

func TestMSP06G15TombstonesAreBoundedTo256RootGroupsWithTotalTieBreak(t *testing.T) {
	server, _ := msp06TestServer(t, &msp06Provider{snapshot: msp06Snapshot(t, "runtime-a")})
	type terminal struct {
		root       string
		descendant string
	}
	terminals := make([]terminal, 0, 257)
	for index := 0; index < 257; index++ {
		_, root := msp06CaptureRoot(t, server)
		refs := msp06RootRefs(t, root)
		rootToken := msp06AssertToken(t, root["snapshot_ref"], "snapshot_ref")
		msp06AssertDropStatus(t, msp06Call(t, server.Handler(), msp06SnapshotDrop, map[string]any{"snapshot_ref": rootToken}), "dropped")
		terminals = append(terminals, terminal{root: rootToken, descendant: refs["services_list_ref"]})
	}

	expectedEvicted := 0
	for index := 1; index < len(terminals); index++ {
		left, _ := base64.RawURLEncoding.DecodeString(terminals[index].root)
		right, _ := base64.RawURLEncoding.DecodeString(terminals[expectedEvicted].root)
		if bytes.Compare(left, right) < 0 {
			expectedEvicted = index
		}
	}
	notFound := 0
	gone := 0
	actualEvicted := -1
	for index, terminal := range terminals {
		result := msp06Call(t, server.Handler(), msp06ServicesListTool, map[string]any{"evidence_ref": terminal.descendant})
		errorObject := msp06Map(t, result.envelope["error"], "error")
		switch errorObject["code"] {
		case "not_found":
			notFound++
			actualEvicted = index
		case "snapshot_gone":
			gone++
		default:
			t.Fatalf("terminal %d code = %#v", index, errorObject["code"])
		}
	}
	if notFound != 1 || gone != 256 || actualEvicted != expectedEvicted {
		t.Fatalf("tombstones not_found=%d gone=%d evicted=%d, want 1/256/%d", notFound, gone, actualEvicted, expectedEvicted)
	}
}

func TestMSP06G16ShareableOutputsErrorsLogsAndManifestDoNotLeak(t *testing.T) {
	snapshot := msp06Snapshot(t, "runtime-a")
	provider := &msp06Provider{snapshot: snapshot}
	server, _ := msp06TestServer(t, provider)
	_, root := msp06CaptureRoot(t, server)
	encodedRoot, err := json.Marshal(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{
		"local-ski-secret",
		"remote-a",
		"remote-z",
		"session-a",
		"session-z",
		"certificate",
		"fingerprint",
		"private_key",
		"local_ski",
		"ship_id",
		"mac_address",
		"serial_number",
		"pairing_history",
		"192.0.2.",
	} {
		if bytes.Contains(bytes.ToLower(encodedRoot), []byte(strings.ToLower(forbidden))) {
			t.Fatalf("captured output contains forbidden marker %q: %s", forbidden, encodedRoot)
		}
	}
	for _, id := range []string{
		snapshot.Meta.Runtime.Digest,
		snapshot.Meta.LocalSKI.Digest,
		snapshot.Pairing[0].Remote.Digest,
		snapshot.Services[0].ID.Digest,
		snapshot.Sessions[0].ID.Digest,
		snapshot.Topology.Devices[0].ID.Digest,
	} {
		if bytes.Contains(encodedRoot, []byte(id)) {
			t.Fatalf("captured output forwarded stable source identity %q", id)
		}
	}

	refs := msp06RootRefs(t, root)
	secretError := "-----BEGIN PRIVATE KEY----- serial=VR940F-SECRET mac=aa:bb:cc:dd:ee:ff ip=192.0.2.44 token=" + refs["services_list_ref"]
	provider.set(eebusruntime.SnapshotV1{}, errors.New(secretError))
	var logs bytes.Buffer
	oldWriter := log.Writer()
	log.SetOutput(&logs)
	t.Cleanup(func() { log.SetOutput(oldWriter) })
	live := msp06Call(t, server.Handler(), msp06ServicesListTool, map[string]any{})
	msp06AssertError(t, live, msp06ServicesListTool, "services", "backend_unavailable")
	wrong := msp06Call(t, server.Handler(), msp06SessionsListTool, map[string]any{"evidence_ref": refs["services_list_ref"]})
	msp06AssertErrorMode(t, wrong, msp06SessionsListTool, "sessions", "evidence", "permission_denied")
	shareable := live.raw + "\n" + wrong.raw + "\n" + logs.String()
	for _, forbidden := range []string{"PRIVATE KEY", "VR940F-SECRET", "aa:bb:cc:dd:ee:ff", "192.0.2.44", refs["services_list_ref"]} {
		if strings.Contains(shareable, forbidden) {
			t.Fatalf("shareable error/log material leaked %q: %s", forbidden, shareable)
		}
	}
	manifest, err := json.Marshal(msp06Tools(t, server))
	if err != nil {
		t.Fatal(err)
	}
	for _, token := range append([]string{fmt.Sprint(root["snapshot_ref"])}, refs["services_list_ref"]) {
		if bytes.Contains(manifest, []byte(token)) {
			t.Fatalf("tools/list manifest contains runtime reference token %q", token)
		}
	}
}

func TestMSP06NoDriftToExistingEbusV1InventoryOrResponses(t *testing.T) {
	server, err := NewServer(&testRegistry{entries: make(map[byte]registry.DeviceEntry)}, &testInvoker{})
	if err != nil {
		t.Fatal(err)
	}
	beforeTools := msp06Tools(t, server)
	beforeNames := msp06NamesWithPrefix(beforeTools, "ebus.v1.")
	beforeCall := msp06Call(t, server.Handler(), "ebus.v1.ebus_standard.services.list", map[string]any{})

	clock := &msp06Clock{now: time.Date(2026, 7, 19, 8, 0, 0, 0, time.UTC)}
	err = server.registerEEBusV1Provider(&msp06Provider{snapshot: msp06Snapshot(t, "runtime-a")}, eebusV1RegistrationOptions{
		now:          clock.Now,
		entropy:      &msp06EntropyReader{},
		pseudonymKey: bytes.Repeat([]byte{0x42}, sha256.Size),
		liveTimeout:  time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	afterNames := msp06NamesWithPrefix(msp06Tools(t, server), "ebus.v1.")
	afterCall := msp06Call(t, server.Handler(), "ebus.v1.ebus_standard.services.list", map[string]any{})
	if !reflect.DeepEqual(beforeNames, afterNames) {
		t.Fatalf("ebus.v1 inventory drifted:\nbefore=%v\nafter=%v", beforeNames, afterNames)
	}
	for _, envelope := range []map[string]any{beforeCall.envelope, afterCall.envelope} {
		meta, ok := envelope["meta"].(map[string]any)
		if !ok {
			t.Fatalf("ebus.v1 response meta = %T, want object", envelope["meta"])
		}
		delete(meta, "data_timestamp")
	}
	if !reflect.DeepEqual(beforeCall.envelope, afterCall.envelope) || beforeCall.isError != afterCall.isError {
		t.Fatalf("ebus.v1 response drifted after eeBUS registration outside the live timestamp:\nbefore=%s\nafter=%s", beforeCall.raw, afterCall.raw)
	}
}

func TestMSP06RootReferenceOrderingFixtureUsesDecodedBytes(t *testing.T) {
	tokens := []string{
		base64.RawURLEncoding.EncodeToString(append([]byte{0x01}, bytes.Repeat([]byte{0xff}, 31)...)),
		base64.RawURLEncoding.EncodeToString(append([]byte{0x02}, bytes.Repeat([]byte{0x00}, 31)...)),
	}
	sort.Slice(tokens, func(i, j int) bool {
		left, _ := base64.RawURLEncoding.DecodeString(tokens[i])
		right, _ := base64.RawURLEncoding.DecodeString(tokens[j])
		return bytes.Compare(left, right) < 0
	})
	if decoded, _ := base64.RawURLEncoding.DecodeString(tokens[0]); decoded[0] != 0x01 {
		t.Fatal("decoded-byte ordering fixture is invalid")
	}
}
