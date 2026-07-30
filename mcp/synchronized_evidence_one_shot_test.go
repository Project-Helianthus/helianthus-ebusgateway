package mcp

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/syncevidence"
)

type issue764CaptureProvider struct {
	calls   int
	receipt syncevidence.OneShotReceiptV1
}

func (provider *issue764CaptureProvider) CaptureSynchronizedEvidence(context.Context) syncevidence.OneShotReceiptV1 {
	provider.calls++
	return provider.receipt
}

func TestIssue764PrivateCaptureToolIsAbsentFromPublicInventoryAndDispatch(t *testing.T) {
	server, _ := msp06TestServer(t, &msp06Provider{snapshot: msp06Snapshot(t, "runtime-a")})
	publicBefore := doRPC(t, server.Handler(), rpcRequest{JSONRPC: "2.0", ID: 1, Method: "tools/list"})
	provider := &issue764CaptureProvider{
		receipt: syncevidence.OneShotReceiptV1{Category: syncevidence.OneShotPublished},
	}
	if err := server.RegisterSynchronizedEvidenceCapture(provider); err != nil {
		t.Fatalf("register capture: %v", err)
	}
	publicAfter := doRPC(t, server.Handler(), rpcRequest{JSONRPC: "2.0", ID: 1, Method: "tools/list"})
	if !reflect.DeepEqual(publicAfter.Result, publicBefore.Result) {
		t.Fatal("private registration changed public tools/list")
	}
	publicRaw, err := json.Marshal(publicAfter.Result)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(publicRaw), synchronizedEvidenceCaptureToolName) {
		t.Fatalf("public tools/list leaked private tool: %s", publicRaw)
	}

	params, err := json.Marshal(map[string]any{
		"name": synchronizedEvidenceCaptureToolName, "arguments": map[string]any{},
	})
	if err != nil {
		t.Fatal(err)
	}
	publicCall := doRPC(t, server.Handler(), rpcRequest{
		JSONRPC: "2.0", ID: 2, Method: "tools/call", Params: params,
	})
	if publicCall.Error == nil || provider.calls != 0 {
		t.Fatalf("public dispatch response=%#v provider_calls=%d", publicCall, provider.calls)
	}

	operatorList := doRPC(t, issue743OperatorHandler(t, server), rpcRequest{
		JSONRPC: "2.0", ID: 3, Method: "tools/list",
	})
	operatorRaw, err := json.Marshal(operatorList.Result)
	if err != nil {
		t.Fatal(err)
	}
	if count := strings.Count(string(operatorRaw), synchronizedEvidenceCaptureToolName); count != 1 {
		t.Fatalf("operator tool count = %d: %s", count, operatorRaw)
	}
}

func TestIssue764PrivateCaptureToolAcceptsOnlyEmptyArgsAndReturnsCategoryOnly(t *testing.T) {
	server, _ := msp06TestServer(t, &msp06Provider{snapshot: msp06Snapshot(t, "runtime-a")})
	provider := &issue764CaptureProvider{
		receipt: syncevidence.OneShotReceiptV1{Category: syncevidence.OneShotExisting},
	}
	if err := server.RegisterSynchronizedEvidenceCapture(provider); err != nil {
		t.Fatal(err)
	}
	operator := issue743OperatorHandler(t, server)
	result := msp06Call(t, operator, synchronizedEvidenceCaptureToolName, map[string]any{})
	if result.raw != `{"category":"EXISTING"}` || result.isError || provider.calls != 1 {
		t.Fatalf("valid call result=%q error=%v calls=%d", result.raw, result.isError, provider.calls)
	}
	invalid := msp06Call(t, operator, synchronizedEvidenceCaptureToolName, map[string]any{"target": "forbidden"})
	if invalid.raw != `{"category":"INVALID_REQUEST"}` || !invalid.isError || provider.calls != 1 {
		t.Fatalf("invalid call result=%q error=%v calls=%d", invalid.raw, invalid.isError, provider.calls)
	}
}

func TestIssue764PrivateCaptureRegistrationAndReceiptFailClosed(t *testing.T) {
	server, _ := msp06TestServer(t, &msp06Provider{snapshot: msp06Snapshot(t, "runtime-a")})
	var typedNil *issue764CaptureProvider
	if err := server.RegisterSynchronizedEvidenceCapture(nil); err == nil {
		t.Fatal("nil capture provider registered")
	}
	if err := server.RegisterSynchronizedEvidenceCapture(typedNil); err == nil {
		t.Fatal("typed-nil capture provider registered")
	}
	provider := &issue764CaptureProvider{
		receipt: syncevidence.OneShotReceiptV1{Category: "raw-evidence-forbidden"},
	}
	if err := server.RegisterSynchronizedEvidenceCapture(provider); err != nil {
		t.Fatal(err)
	}
	if err := server.RegisterSynchronizedEvidenceCapture(&issue764CaptureProvider{}); err == nil {
		t.Fatal("second capture provider registered")
	}
	result := msp06Call(
		t,
		issue743OperatorHandler(t, server),
		synchronizedEvidenceCaptureToolName,
		map[string]any{},
	)
	if result.raw != `{"category":"INTERNAL"}` || !result.isError {
		t.Fatalf("invalid provider receipt escaped: %q", result.raw)
	}
}
