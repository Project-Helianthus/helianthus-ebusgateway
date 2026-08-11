package mcp

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

type issue784CaptureProvider struct {
	calls   []LeafPromotionCaptureRequest
	receipt LeafPromotionCaptureReceipt
}

func (provider *issue784CaptureProvider) CaptureLeafPromotion(
	_ context.Context,
	request LeafPromotionCaptureRequest,
) LeafPromotionCaptureReceipt {
	provider.calls = append(provider.calls, request)
	return provider.receipt
}

func TestIssue784LeafPromotionCaptureIsOperatorOnly(t *testing.T) {
	server, _ := msp06TestServer(t, &msp06Provider{snapshot: msp06Snapshot(t, "runtime-a")})
	provider := &issue784CaptureProvider{receipt: issue784PublishedReceipt("PRE_RESTART")}
	if err := server.RegisterLeafPromotionCapture(provider); err != nil {
		t.Fatal(err)
	}

	publicList := doRPC(t, server.Handler(), rpcRequest{JSONRPC: "2.0", ID: 1, Method: "tools/list"})
	publicRaw, err := json.Marshal(publicList.Result)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(publicRaw), leafPromotionCaptureToolName) {
		t.Fatalf("public tools/list leaked promotion tool: %s", publicRaw)
	}
	params, err := json.Marshal(map[string]any{
		"name": leafPromotionCaptureToolName,
		"arguments": map[string]any{
			"campaign_id": "vr940-20260811",
			"phase":       "PRE_RESTART",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	publicCall := doRPC(t, server.Handler(), rpcRequest{
		JSONRPC: "2.0", ID: 2, Method: "tools/call", Params: params,
	})
	if publicCall.Error == nil || len(provider.calls) != 0 {
		t.Fatalf("public dispatch response=%#v calls=%d", publicCall, len(provider.calls))
	}

	operatorList := doRPC(t, issue743OperatorHandler(t, server), rpcRequest{
		JSONRPC: "2.0", ID: 3, Method: "tools/list",
	})
	operatorRaw, err := json.Marshal(operatorList.Result)
	if err != nil {
		t.Fatal(err)
	}
	if count := strings.Count(string(operatorRaw), leafPromotionCaptureToolName); count != 1 {
		t.Fatalf("operator tool count = %d: %s", count, operatorRaw)
	}
}

func TestIssue784LeafPromotionCaptureValidatesPREAndPOST(t *testing.T) {
	server, _ := msp06TestServer(t, &msp06Provider{snapshot: msp06Snapshot(t, "runtime-a")})
	provider := &issue784CaptureProvider{receipt: issue784PublishedReceipt("PRE_RESTART")}
	if err := server.RegisterLeafPromotionCapture(provider); err != nil {
		t.Fatal(err)
	}
	operator := issue743OperatorHandler(t, server)

	pre := msp06Call(t, operator, leafPromotionCaptureToolName, map[string]any{
		"campaign_id": "vr940-20260811",
		"phase":       "PRE_RESTART",
	})
	if pre.isError || len(provider.calls) != 1 || provider.calls[0].RestartCompletedAt != nil {
		t.Fatalf("PRE result=%q error=%v calls=%#v", pre.raw, pre.isError, provider.calls)
	}

	provider.receipt = issue784PublishedReceipt("POST_RESTART")
	post := msp06Call(t, operator, leafPromotionCaptureToolName, map[string]any{
		"campaign_id":          "vr940-20260811",
		"phase":                "POST_RESTART",
		"restart_completed_at": "2026-08-11T15:30:00.123Z",
	})
	if post.isError || len(provider.calls) != 2 || provider.calls[1].RestartCompletedAt == nil ||
		provider.calls[1].RestartCompletedAt.Format(time.RFC3339Nano) != "2026-08-11T15:30:00.123Z" {
		t.Fatalf("POST result=%q error=%v calls=%#v", post.raw, post.isError, provider.calls)
	}
}

func TestIssue784LeafPromotionCaptureRejectsUnclosedOrInconsistentInput(t *testing.T) {
	server, _ := msp06TestServer(t, &msp06Provider{snapshot: msp06Snapshot(t, "runtime-a")})
	provider := &issue784CaptureProvider{receipt: issue784PublishedReceipt("PRE_RESTART")}
	if err := server.RegisterLeafPromotionCapture(provider); err != nil {
		t.Fatal(err)
	}
	operator := issue743OperatorHandler(t, server)
	invalid := []map[string]any{
		{"campaign_id": "../escape", "phase": "PRE_RESTART"},
		{"campaign_id": "vr940", "phase": "PRE_RESTART", "restart_completed_at": "2026-08-11T15:30:00Z"},
		{"campaign_id": "vr940", "phase": "POST_RESTART"},
		{"campaign_id": "vr940", "phase": "POST_RESTART", "restart_completed_at": "2026-08-11T17:30:00+02:00"},
		{"campaign_id": "vr940", "phase": "UNKNOWN"},
		{"campaign_id": "vr940", "phase": "PRE_RESTART", "candidate_ref": "forbidden"},
	}
	for _, arguments := range invalid {
		result := msp06Call(t, operator, leafPromotionCaptureToolName, arguments)
		if result.raw != `{"category":"INVALID_REQUEST"}` || !result.isError {
			t.Fatalf("invalid args %#v result=%q error=%v", arguments, result.raw, result.isError)
		}
	}
	if len(provider.calls) != 0 {
		t.Fatalf("invalid requests reached provider: %#v", provider.calls)
	}
}

func TestIssue784LeafPromotionCaptureRegistrationAndReceiptFailClosed(t *testing.T) {
	server, _ := msp06TestServer(t, &msp06Provider{snapshot: msp06Snapshot(t, "runtime-a")})
	var typedNil *issue784CaptureProvider
	if err := server.RegisterLeafPromotionCapture(nil); err == nil {
		t.Fatal("nil provider registered")
	}
	if err := server.RegisterLeafPromotionCapture(typedNil); err == nil {
		t.Fatal("typed-nil provider registered")
	}
	provider := &issue784CaptureProvider{receipt: LeafPromotionCaptureReceipt{
		Category: "PUBLISHED", CampaignID: "vr940", Phase: "PRE_RESTART",
		WindowHash: "raw-forbidden", ReceiptHash: "raw-forbidden",
	}}
	if err := server.RegisterLeafPromotionCapture(provider); err != nil {
		t.Fatal(err)
	}
	if err := server.RegisterLeafPromotionCapture(&issue784CaptureProvider{}); err == nil {
		t.Fatal("second provider registered")
	}
	result := msp06Call(t, issue743OperatorHandler(t, server), leafPromotionCaptureToolName, map[string]any{
		"campaign_id": "vr940",
		"phase":       "PRE_RESTART",
	})
	if result.raw != `{"category":"INTERNAL"}` || !result.isError {
		t.Fatalf("invalid receipt escaped: %q error=%v", result.raw, result.isError)
	}
}

func issue784PublishedReceipt(phase string) LeafPromotionCaptureReceipt {
	return LeafPromotionCaptureReceipt{
		Category:    "PUBLISHED",
		CampaignID:  "vr940-20260811",
		Phase:       phase,
		WindowHash:  "sha256:" + strings.Repeat("1", 64),
		ReceiptHash: "sha256:" + strings.Repeat("2", 64),
	}
}
