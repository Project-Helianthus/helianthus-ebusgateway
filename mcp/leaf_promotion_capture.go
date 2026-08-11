package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"regexp"
	"time"
)

const leafPromotionCaptureToolName = "helianthus.experimental.leaf_promotion.capture"

var leafPromotionCampaignID = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,63}$`)

type LeafPromotionCaptureRequest struct {
	CampaignID         string
	Phase              string
	RestartCompletedAt *time.Time
}

type LeafPromotionCaptureReceipt struct {
	Category    string `json:"category"`
	CampaignID  string `json:"campaign_id,omitempty"`
	Phase       string `json:"phase,omitempty"`
	WindowHash  string `json:"window_hash,omitempty"`
	ReceiptHash string `json:"receipt_hash,omitempty"`
}

type LeafPromotionCapture interface {
	CaptureLeafPromotion(context.Context, LeafPromotionCaptureRequest) LeafPromotionCaptureReceipt
}

func (server *Server) RegisterLeafPromotionCapture(capture LeafPromotionCapture) error {
	if server == nil {
		return errors.New("MCP server is nil")
	}
	if nilLeafPromotionCapture(capture) {
		return errors.New("leaf promotion capture is nil")
	}
	server.eebusV1Mu.Lock()
	defer server.eebusV1Mu.Unlock()
	if server.leafPromotionCapture != nil {
		return errors.New("leaf promotion capture is already registered")
	}
	server.leafPromotionCapture = capture
	return nil
}

func nilLeafPromotionCapture(capture LeafPromotionCapture) bool {
	if capture == nil {
		return true
	}
	value := reflect.ValueOf(capture)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func leafPromotionCaptureTool() Tool {
	return Tool{
		Name:        leafPromotionCaptureToolName,
		Description: "Capture one owner-only PRE/POST multi-leaf promotion window.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"campaign_id": map[string]any{
					"type":      "string",
					"minLength": 1,
					"maxLength": 64,
					"pattern":   "^[a-z0-9][a-z0-9._-]{0,63}$",
				},
				"phase": map[string]any{
					"type": "string",
					"enum": []string{"PRE_RESTART", "POST_RESTART"},
				},
				"restart_completed_at": map[string]any{
					"type":   "string",
					"format": "date-time",
				},
			},
			"required":             []string{"campaign_id", "phase"},
			"additionalProperties": false,
		},
	}
}

func (server *Server) handleLeafPromotionCaptureRaw(
	ctx context.Context,
	arguments json.RawMessage,
	invalidCallParams bool,
) (any, *rpcError) {
	if eebusV1BoundaryFromContext(ctx) != eebusV1OperatorBoundary {
		return nil, rpcErrorInvalidParams(fmt.Sprintf("unknown tool %q", leafPromotionCaptureToolName))
	}
	server.eebusV1Mu.RLock()
	capture := server.leafPromotionCapture
	server.eebusV1Mu.RUnlock()
	if nilLeafPromotionCapture(capture) {
		return nil, rpcErrorInvalidParams(fmt.Sprintf("unknown tool %q", leafPromotionCaptureToolName))
	}
	request, ok := parseLeafPromotionCaptureRequest(arguments)
	if invalidCallParams || !ok {
		return callToolResultText(mustJSON(LeafPromotionCaptureReceipt{Category: "INVALID_REQUEST"}), true), nil
	}
	receipt := callLeafPromotionCapture(ctx, capture, request)
	if !validLeafPromotionCaptureReceipt(receipt) {
		receipt = LeafPromotionCaptureReceipt{Category: "INTERNAL"}
	}
	isError := receipt.Category != "PUBLISHED" && receipt.Category != "EXISTING"
	return callToolResultText(mustJSON(receipt), isError), nil
}

func parseLeafPromotionCaptureRequest(raw json.RawMessage) (LeafPromotionCaptureRequest, bool) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var wire struct {
		CampaignID         string  `json:"campaign_id"`
		Phase              string  `json:"phase"`
		RestartCompletedAt *string `json:"restart_completed_at,omitempty"`
	}
	if err := decoder.Decode(&wire); err != nil || !errors.Is(decoder.Decode(&struct{}{}), io.EOF) ||
		!leafPromotionCampaignID.MatchString(wire.CampaignID) ||
		(wire.Phase != "PRE_RESTART" && wire.Phase != "POST_RESTART") {
		return LeafPromotionCaptureRequest{}, false
	}
	request := LeafPromotionCaptureRequest{CampaignID: wire.CampaignID, Phase: wire.Phase}
	if wire.Phase == "PRE_RESTART" {
		return request, wire.RestartCompletedAt == nil
	}
	if wire.RestartCompletedAt == nil {
		return LeafPromotionCaptureRequest{}, false
	}
	parsed, err := time.Parse(time.RFC3339Nano, *wire.RestartCompletedAt)
	if err != nil || parsed.Location() != time.UTC || parsed.Format(time.RFC3339Nano) != *wire.RestartCompletedAt {
		return LeafPromotionCaptureRequest{}, false
	}
	request.RestartCompletedAt = &parsed
	return request, true
}

func callLeafPromotionCapture(
	ctx context.Context,
	capture LeafPromotionCapture,
	request LeafPromotionCaptureRequest,
) (receipt LeafPromotionCaptureReceipt) {
	receipt = LeafPromotionCaptureReceipt{Category: "INTERNAL"}
	defer func() {
		if recover() != nil {
			receipt = LeafPromotionCaptureReceipt{Category: "INTERNAL"}
		}
	}()
	return capture.CaptureLeafPromotion(ctx, request)
}

func validLeafPromotionCaptureReceipt(receipt LeafPromotionCaptureReceipt) bool {
	switch receipt.Category {
	case "PUBLISHED", "EXISTING":
		return leafPromotionCampaignID.MatchString(receipt.CampaignID) &&
			(receipt.Phase == "PRE_RESTART" || receipt.Phase == "POST_RESTART") &&
			validSHA256Digest(receipt.WindowHash) && validSHA256Digest(receipt.ReceiptHash)
	case "INVALID_REQUEST", "ACQUISITION_FAILED", "PERSISTENCE_FAILED", "INTERNAL":
		return receipt.CampaignID == "" && receipt.Phase == "" &&
			receipt.WindowHash == "" && receipt.ReceiptHash == ""
	default:
		return false
	}
}

func validSHA256Digest(value string) bool {
	if len(value) != 71 || value[:7] != "sha256:" {
		return false
	}
	for _, char := range value[7:] {
		if (char < '0' || char > '9') && (char < 'a' || char > 'f') {
			return false
		}
	}
	return true
}
