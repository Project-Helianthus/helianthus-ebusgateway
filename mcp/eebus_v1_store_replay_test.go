package mcp

import (
	"encoding/json"
	"net/http"
	"sync"
	"testing"
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

func TestMSP06ConcurrentCaptureStoreRemainsBounded(t *testing.T) {
	server, _ := msp06TestServer(t, &msp06Provider{snapshot: msp06Snapshot(t, "runtime-a")})
	var wait sync.WaitGroup
	for range 64 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			_ = msp06ConcurrentCapture(server.Handler())
		}()
	}
	wait.Wait()
	server.eebusV1.store.mu.Lock()
	active := len(server.eebusV1.store.activeRoots)
	server.eebusV1.store.mu.Unlock()
	if active > eebusV1MaxActive {
		t.Fatalf("active roots = %d, max %d", active, eebusV1MaxActive)
	}
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
