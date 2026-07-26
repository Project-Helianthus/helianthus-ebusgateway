package mcp

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"
)

func msp06CaptureRoot(t *testing.T, server *Server) (msp06CallResult, map[string]any) {
	t.Helper()
	result := msp06Call(t, server.Handler(), msp06SnapshotCapture, map[string]any{})
	return result, msp06AssertSuccess(t, result, msp06SnapshotCapture, "whole-root", "live")
}

func msp06RootRefs(t *testing.T, root map[string]any) map[string]string {
	t.Helper()
	raw := msp06Map(t, root["evidence_refs"], "evidence_refs")
	refs := make(map[string]string, len(raw))
	for key, value := range raw {
		refs[key] = msp06AssertToken(t, value, "evidence_refs."+key)
	}
	return refs
}

func TestMSP06CaptureReplayDropLifecycle(t *testing.T) {
	server, clock := msp06TestServer(t, &msp06Provider{snapshot: msp06Snapshot(t, "runtime-a")})
	_, root := msp06CaptureRoot(t, server)
	refs := msp06RootRefs(t, root)

	replay := msp06Call(t, server.Handler(), msp06TopologyGetTool, map[string]any{"evidence_ref": refs["topology_ref"]})
	msp06AssertSuccess(t, replay, msp06TopologyGetTool, "topology", "evidence")

	drop := msp06Call(t, server.Handler(), msp06SnapshotDrop, map[string]any{"snapshot_ref": root["snapshot_ref"]})
	data := msp06AssertSuccess(t, drop, msp06SnapshotDrop, "whole-root", "live")
	if data["status"] != "dropped" {
		t.Fatalf("drop status = %v", data["status"])
	}
	gone := msp06Call(t, server.Handler(), msp06TopologyGetTool, map[string]any{"evidence_ref": refs["topology_ref"]})
	msp06AssertErrorMode(t, gone, msp06TopologyGetTool, "topology", "evidence", "snapshot_gone")

	clock.Advance(eebusV1TombstoneTTL + 1)
	unknown := msp06Call(t, server.Handler(), msp06TopologyGetTool, map[string]any{"evidence_ref": refs["topology_ref"]})
	msp06AssertErrorMode(t, unknown, msp06TopologyGetTool, "topology", "evidence", "not_found")
}

func TestMSP06ReferenceBindingRejectsWrongToolAndBoundary(t *testing.T) {
	server, _ := msp06TestServer(t, &msp06Provider{snapshot: msp06Snapshot(t, "runtime-a")})
	_, root := msp06CaptureRoot(t, server)
	refs := msp06RootRefs(t, root)
	wrongTool := msp06Call(t, server.Handler(), msp06ServicesListTool, map[string]any{"evidence_ref": refs["topology_ref"]})
	msp06AssertErrorMode(t, wrongTool, msp06ServicesListTool, "services", "evidence", "permission_denied")

	operator := issue743OperatorHandler(t, server)
	crossBoundary := msp06Call(t, operator, msp06TopologyGetTool, map[string]any{"evidence_ref": refs["topology_ref"]})
	issue743AssertError(t, crossBoundary, "raw", "eebus.raw.read", "permission_denied")
}

func TestIssue743ReferenceRuntimeKeyMustMatchActiveAndTombstoneRoot(t *testing.T) {
	server, _ := msp06TestServer(t, &msp06Provider{snapshot: msp06Snapshot(t, "runtime-a")})
	_, root := msp06CaptureRoot(t, server)
	refs := msp06RootRefs(t, root)
	rootToken := root["snapshot_ref"].(string)

	server.eebusV1.store.mu.Lock()
	serviceRef := server.eebusV1.store.activeTokens[refs["services_list_ref"]]
	serviceRef.Binding.RuntimeKey = "tampered-binding"
	server.eebusV1.store.activeTokens[refs["services_list_ref"]] = serviceRef
	server.eebusV1.store.mu.Unlock()
	msp06AssertErrorMode(t,
		msp06Call(t, server.Handler(), msp06ServicesListTool, map[string]any{"evidence_ref": refs["services_list_ref"]}),
		msp06ServicesListTool, "services", "evidence", "permission_denied")

	server.eebusV1.store.mu.Lock()
	activeRoot := server.eebusV1.store.activeRoots[rootToken]
	serviceRef.Binding.RuntimeKey = activeRoot.RuntimeKey
	server.eebusV1.store.activeTokens[refs["services_list_ref"]] = serviceRef
	activeRoot.RuntimeKey = "tampered-active-root"
	server.eebusV1.store.mu.Unlock()
	msp06AssertErrorMode(t,
		msp06Call(t, server.Handler(), msp06SnapshotDrop, map[string]any{"snapshot_ref": rootToken}),
		msp06SnapshotDrop, "whole-root", "live", "permission_denied")

	server.eebusV1.store.mu.Lock()
	activeRoot.RuntimeKey = activeRoot.Projection.RuntimeKey
	server.eebusV1.store.mu.Unlock()
	drop := msp06Call(t, server.Handler(), msp06SnapshotDrop, map[string]any{"snapshot_ref": rootToken})
	msp06AssertSuccess(t, drop, msp06SnapshotDrop, "whole-root", "live")

	server.eebusV1.store.mu.Lock()
	tombstoneRef := server.eebusV1.store.tombstoneTokens[refs["services_list_ref"]]
	tombstoneRef.Binding.RuntimeKey = "tampered-tombstone-binding"
	server.eebusV1.store.tombstoneTokens[refs["services_list_ref"]] = tombstoneRef
	server.eebusV1.store.mu.Unlock()
	msp06AssertErrorMode(t,
		msp06Call(t, server.Handler(), msp06ServicesListTool, map[string]any{"evidence_ref": refs["services_list_ref"]}),
		msp06ServicesListTool, "services", "evidence", "permission_denied")

	server.eebusV1.store.mu.Lock()
	tombstoneRoot := server.eebusV1.store.tombstoneRoots[rootToken]
	tombstoneRoot.RuntimeKey = "tampered-tombstone-root"
	server.eebusV1.store.mu.Unlock()
	msp06AssertErrorMode(t,
		msp06Call(t, server.Handler(), msp06SnapshotDrop, map[string]any{"snapshot_ref": rootToken}),
		msp06SnapshotDrop, "whole-root", "live", "permission_denied")
}

func TestIssue743CaptureQuotaIsIndependentAndExactPerBoundary(t *testing.T) {
	server, _ := msp06TestServer(t, &msp06Provider{snapshot: msp06Snapshot(t, "runtime-a")})
	server.eebusV1.liveTimeout = 5 * time.Second
	for name, handler := range map[string]http.Handler{
		"public":   server.Handler(),
		"operator": issue743OperatorHandler(t, server),
	} {
		t.Run(name, func(t *testing.T) {
			successes, quota := issue743ConcurrentCaptureOutcomes(t, handler, 64)
			if successes != 32 || quota != 32 {
				t.Fatalf("captures success=%d quota_exceeded=%d, want 32/32", successes, quota)
			}
		})
	}
	server.eebusV1.store.mu.Lock()
	active := len(server.eebusV1.store.activeRoots)
	server.eebusV1.store.mu.Unlock()
	if active != 2*eebusV1MaxActive {
		t.Fatalf("active roots = %d, want %d across independent boundaries", active, 2*eebusV1MaxActive)
	}
}

func TestIssue743FailedCaptureConsumesNoQuotaSlot(t *testing.T) {
	server, _ := msp06TestServer(t, &msp06Provider{snapshot: msp06Snapshot(t, "runtime-a")})
	server.eebusV1.store.entropy = issue743FailingReader{}
	failed := msp06Call(t, server.Handler(), msp06SnapshotCapture, map[string]any{})
	msp06AssertError(t, failed, msp06SnapshotCapture, "whole-root", "contract_violation")
	server.eebusV1.store.mu.Lock()
	if got := len(server.eebusV1.store.activeRoots); got != 0 {
		server.eebusV1.store.mu.Unlock()
		t.Fatalf("failed capture consumed %d active slots", got)
	}
	server.eebusV1.store.entropy = &msp06EntropyReader{}
	server.eebusV1.store.mu.Unlock()
	for range eebusV1MaxActive {
		msp06CaptureRoot(t, server)
	}
	full := msp06Call(t, server.Handler(), msp06SnapshotCapture, map[string]any{})
	msp06AssertError(t, full, msp06SnapshotCapture, "whole-root", "quota_exceeded")
}

func TestIssue743DropIsIdempotentAndTTLsAreInclusive(t *testing.T) {
	server, clock := msp06TestServer(t, &msp06Provider{snapshot: msp06Snapshot(t, "runtime-a")})
	_, root := msp06CaptureRoot(t, server)
	refs := msp06RootRefs(t, root)
	token := root["snapshot_ref"]
	first := msp06Call(t, server.Handler(), msp06SnapshotDrop, map[string]any{"snapshot_ref": token})
	second := msp06Call(t, server.Handler(), msp06SnapshotDrop, map[string]any{"snapshot_ref": token})
	if got := msp06AssertSuccess(t, first, msp06SnapshotDrop, "whole-root", "live")["status"]; got != "dropped" {
		t.Fatalf("first drop status = %v", got)
	}
	if got := msp06AssertSuccess(t, second, msp06SnapshotDrop, "whole-root", "live")["status"]; got != "already_gone" {
		t.Fatalf("second drop status = %v", got)
	}
	clock.Advance(eebusV1TombstoneTTL)
	msp06AssertErrorMode(t,
		msp06Call(t, server.Handler(), msp06ServicesListTool, map[string]any{"evidence_ref": refs["services_list_ref"]}),
		msp06ServicesListTool, "services", "evidence", "not_found")

	server, clock = msp06TestServer(t, &msp06Provider{snapshot: msp06Snapshot(t, "runtime-a")})
	_, root = msp06CaptureRoot(t, server)
	refs = msp06RootRefs(t, root)
	clock.Advance(eebusV1ActiveTTL)
	msp06AssertErrorMode(t,
		msp06Call(t, server.Handler(), msp06ServicesListTool, map[string]any{"evidence_ref": refs["services_list_ref"]}),
		msp06ServicesListTool, "services", "evidence", "snapshot_gone")
	clock.Advance(eebusV1TombstoneTTL)
	msp06AssertErrorMode(t,
		msp06Call(t, server.Handler(), msp06ServicesListTool, map[string]any{"evidence_ref": refs["services_list_ref"]}),
		msp06ServicesListTool, "services", "evidence", "not_found")
}

func TestIssue743TombstonesAreBoundedByDecodedRootOrdering(t *testing.T) {
	store := newEEBusV1SnapshotStore(time.Now, &msp06EntropyReader{})
	terminalAt := time.Date(2026, 7, 26, 12, 0, 0, 0, time.UTC)
	var decodedMinimum string
	for index := 0; index <= eebusV1MaxTombstones; index++ {
		var raw [sha256.Size]byte
		raw[0] = byte(index)
		raw[1] = byte(index >> 8)
		token := base64.RawURLEncoding.EncodeToString(raw[:])
		if index == 0 {
			decodedMinimum = token
		}
		store.tombstoneRoots[token] = &eebusV1TombstoneRoot{
			RootToken: token, RootBytes: raw, RuntimeKey: "runtime",
			TerminalAt: terminalAt, References: map[string]eebusV1StoredReference{},
		}
	}
	store.enforceTombstoneBoundLocked()
	if got := len(store.tombstoneRoots); got != eebusV1MaxTombstones {
		t.Fatalf("tombstones = %d, want %d", got, eebusV1MaxTombstones)
	}
	if _, exists := store.tombstoneRoots[decodedMinimum]; exists {
		t.Fatalf("decoded-byte minimum root %q was not deterministically evicted", decodedMinimum)
	}
}

func issue743ConcurrentCaptureOutcomes(t *testing.T, handler http.Handler, attempts int) (successes, quota int) {
	t.Helper()
	results := make(chan msp06CallResult, attempts)
	var wait sync.WaitGroup
	for range attempts {
		wait.Add(1)
		go func() {
			defer wait.Done()
			results <- msp06ConcurrentCapture(handler)
		}()
	}
	wait.Wait()
	close(results)
	for result := range results {
		if result.envelope["error"] == nil {
			successes++
			continue
		}
		errorObject := msp06Map(t, result.envelope["error"], "capture error")
		if errorObject["code"] != "quota_exceeded" {
			t.Fatalf("capture error = %#v", errorObject)
		}
		quota++
	}
	return successes, quota
}

type issue743FailingReader struct{}

func (issue743FailingReader) Read([]byte) (int, error) {
	return 0, io.ErrUnexpectedEOF
}

func msp06ConcurrentCapture(handler http.Handler) msp06CallResult {
	request := httptestRequest(http.MethodPost, "/mcp", []byte(`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"eebus.v1.snapshot.capture","arguments":{}}}`))
	recorder := &issue743Recorder{header: make(http.Header)}
	handler.ServeHTTP(recorder, request)
	var response rpcResponse
	_ = json.Unmarshal(recorder.body.Bytes(), &response)
	result, _ := response.Result.(map[string]any)
	content, _ := result["content"].([]any)
	item, _ := content[0].(map[string]any)
	raw, _ := item["text"].(string)
	var envelope map[string]any
	_ = json.Unmarshal([]byte(raw), &envelope)
	return msp06CallResult{envelope: envelope, raw: raw, isError: result["isError"] == true}
}

var _ io.Reader = issue743FailingReader{}
