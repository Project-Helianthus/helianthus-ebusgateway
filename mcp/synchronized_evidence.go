package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/syncevidence"
)

const synchronizedEvidenceCaptureToolName = "helianthus.v1.synchronized_evidence.capture"

type SynchronizedEvidenceCapture interface {
	CaptureSynchronizedEvidence(context.Context) syncevidence.OneShotReceiptV1
}

func (server *Server) RegisterSynchronizedEvidenceCapture(capture SynchronizedEvidenceCapture) error {
	if server == nil {
		return errors.New("MCP server is nil")
	}
	if nilSynchronizedEvidenceCapture(capture) {
		return errors.New("synchronized evidence capture is nil")
	}
	server.eebusV1Mu.Lock()
	defer server.eebusV1Mu.Unlock()
	if server.synchronizedEvidenceCapture != nil {
		return errors.New("synchronized evidence capture is already registered")
	}
	server.synchronizedEvidenceCapture = capture
	return nil
}

func nilSynchronizedEvidenceCapture(capture SynchronizedEvidenceCapture) bool {
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

func synchronizedEvidenceCaptureTool() Tool {
	return Tool{
		Name:        synchronizedEvidenceCaptureToolName,
		Description: "Capture the fixed owner-only synchronized evidence request.",
		InputSchema: map[string]any{
			"type":                 "object",
			"properties":           map[string]any{},
			"additionalProperties": false,
			"maxProperties":        0,
		},
	}
}

func (server *Server) handleSynchronizedEvidenceCaptureRaw(
	ctx context.Context,
	arguments json.RawMessage,
	hasDuplicateKeys bool,
) (any, *rpcError) {
	if eebusV1BoundaryFromContext(ctx) != eebusV1OperatorBoundary {
		return nil, rpcErrorInvalidParams(fmt.Sprintf("unknown tool %q", synchronizedEvidenceCaptureToolName))
	}
	server.eebusV1Mu.RLock()
	capture := server.synchronizedEvidenceCapture
	server.eebusV1Mu.RUnlock()
	if nilSynchronizedEvidenceCapture(capture) {
		return nil, rpcErrorInvalidParams(fmt.Sprintf("unknown tool %q", synchronizedEvidenceCaptureToolName))
	}
	if hasDuplicateKeys || !synchronizedEvidenceEmptyArgs(arguments) {
		receipt := syncevidence.OneShotReceiptV1{Category: syncevidence.OneShotInvalidRequest}
		return callToolResultText(mustJSON(receipt), true), nil
	}
	receipt := callSynchronizedEvidenceCapture(ctx, capture)
	if !validSynchronizedEvidenceReceipt(receipt.Category) {
		receipt = syncevidence.OneShotReceiptV1{Category: syncevidence.OneShotInternal}
	}
	isError := receipt.Category != syncevidence.OneShotPublished &&
		receipt.Category != syncevidence.OneShotExisting
	return callToolResultText(mustJSON(receipt), isError), nil
}

func synchronizedEvidenceEmptyArgs(raw json.RawMessage) bool {
	if len(raw) == 0 {
		return false
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var value map[string]json.RawMessage
	if err := decoder.Decode(&value); err != nil || value == nil || len(value) != 0 {
		return false
	}
	return errors.Is(decoder.Decode(&struct{}{}), io.EOF)
}

func callSynchronizedEvidenceCapture(
	ctx context.Context,
	capture SynchronizedEvidenceCapture,
) (receipt syncevidence.OneShotReceiptV1) {
	receipt = syncevidence.OneShotReceiptV1{Category: syncevidence.OneShotInternal}
	defer func() {
		if recover() != nil {
			receipt = syncevidence.OneShotReceiptV1{Category: syncevidence.OneShotInternal}
		}
	}()
	return capture.CaptureSynchronizedEvidence(ctx)
}

func validSynchronizedEvidenceReceipt(category syncevidence.OneShotReceiptCategory) bool {
	switch category {
	case syncevidence.OneShotPublished,
		syncevidence.OneShotExisting,
		syncevidence.OneShotInvalidRequest,
		syncevidence.OneShotPermissionDenied,
		syncevidence.OneShotConflict,
		syncevidence.OneShotAcquisitionFailed,
		syncevidence.OneShotReplayMismatch,
		syncevidence.OneShotPublishFailed,
		syncevidence.OneShotInternal:
		return true
	default:
		return false
	}
}
