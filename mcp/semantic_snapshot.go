package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semanticview"
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
)

const SemanticSnapshotTool = "semantic.v1.snapshot.get"
const semanticReadTimeout = 5 * time.Second

// SemanticSnapshotProvider captures a coherent public-safe detached publication,
// evaluation context and catalog. It performs no native acquisition or dispatch
// and must honor cancellation. Unsafe assets must be withheld before capture.
type SemanticSnapshotProvider interface {
	Capture(context.Context, []semreg.AssetID) (*semreg.PackMetadataRegistry, []semanticview.Input, error)
}

// RegisterSemanticSnapshotTool installs the shared read on the ordinary MCP
// surface. A missing provider keeps discovery available and calls unavailable.
// Registration is a bootstrap operation, before serving requests.
func RegisterSemanticSnapshotTool(s *Server, provider SemanticSnapshotProvider) error {
	if s == nil {
		return errors.New("semantic snapshot needs an MCP server")
	}
	if provider != nil {
		v := reflect.ValueOf(provider)
		switch v.Kind() {
		case reflect.Pointer, reflect.Interface, reflect.Func, reflect.Map, reflect.Slice, reflect.Chan:
			if v.IsNil() {
				provider = nil
			}
		}
	}
	s.sharedSemanticMu.Lock()
	defer s.sharedSemanticMu.Unlock()
	if s.sharedSemanticRegistered {
		return errors.New("semantic snapshot already registered")
	}
	s.sharedSemanticRegistered = true
	s.sharedSemanticProvider = provider
	s.tools = append(s.tools, semanticSnapshotTool())
	return nil
}

func semanticSnapshotTool() Tool {
	return Tool{Name: SemanticSnapshotTool, Description: "Read one bounded detached SemReg snapshot view for selected assets across capability packs.", InputSchema: map[string]any{
		"type": "object", "additionalProperties": false, "required": []string{"asset_ids"},
		"properties": map[string]any{"asset_ids": map[string]any{"type": "array", "minItems": 1, "maxItems": 32, "uniqueItems": true, "items": map[string]any{"type": "string", "minLength": 1, "maxLength": 256}}},
	}}
}

func semanticReadResult(data any, code string, timestamp time.Time) map[string]any {
	envelope := newToolEnvelopeWithConsistency(data, nil, envelopeConsistency{mode: "EVALUATED_SEMREG_PUBLICATION", dataTimestamp: timestamp})
	if code != "" {
		message := "semantic snapshot read failed"
		switch code {
		case "INVALID_ARGUMENT":
			message = "semantic snapshot selection invalid"
		case "UNAVAILABLE":
			message = "semantic snapshot provider unavailable"
		case "CONFLICT":
			message = "semantic snapshot references conflict"
		case "RESOURCE_EXHAUSTED":
			message = "semantic snapshot read exceeds bounds"
		case "DEADLINE_EXCEEDED":
			message = "semantic snapshot read timed out"
		}
		envelope["error"] = map[string]any{"code": code, "message": message, "retriable": code == "UNAVAILABLE" || code == "DEADLINE_EXCEEDED", "source_layer": "gateway"}
	}
	return callToolResultText(mustJSON(envelope), code != "")
}

func (s *Server) handleSharedSemanticSnapshot(ctx context.Context, raw json.RawMessage, duplicate bool) map[string]any {
	timestamp := time.Now().UTC()
	fail := func(code string) map[string]any { return semanticReadResult(nil, code, timestamp) }
	var members map[string]json.RawMessage
	if duplicate || json.Unmarshal(raw, &members) != nil || len(members) != 1 {
		return fail("INVALID_ARGUMENT")
	}
	var ids []semreg.AssetID
	if json.Unmarshal(members["asset_ids"], &ids) != nil || len(ids) < 1 || len(ids) > 32 {
		return fail("INVALID_ARGUMENT")
	}
	seen := map[semreg.AssetID]bool{}
	for _, id := range ids {
		if id.Validate() != nil || len(id) > 256 || strings.Contains(string(id), "://") || seen[id] {
			return fail("INVALID_ARGUMENT")
		}
		seen[id] = true
	}
	s.sharedSemanticMu.RLock()
	provider := s.sharedSemanticProvider
	s.sharedSemanticMu.RUnlock()
	if provider == nil {
		return fail("UNAVAILABLE")
	}
	ctx, cancel := context.WithTimeout(ctx, semanticReadTimeout)
	defer cancel()
	type result struct {
		view      semanticview.View
		code      string
		timestamp time.Time
	}
	done := make(chan result, 1)
	go func() {
		metadata, inputs, err := provider.Capture(ctx, ids)
		if err != nil {
			done <- result{code: "UNAVAILABLE"}
			return
		}
		if len(inputs) != len(seen) {
			done <- result{code: "CONFLICT"}
			return
		}
		for _, in := range inputs {
			if !seen[in.AssetID] {
				done <- result{code: "CONFLICT"}
				return
			}
			delete(seen, in.AssetID)
		}
		view, err := semanticview.Build(metadata, inputs)
		code := ""
		if err != nil {
			code = "CONFLICT"
			switch semreg.ErrorIdentifier(err) {
			case semreg.BoundsExceeded:
				code = "RESOURCE_EXHAUSTED"
			case semreg.DefinitionOwnerMissing:
				code = "UNAVAILABLE"
			}
		}
		evaluated := timestamp
		for _, asset := range view.Assets {
			if asset.Evaluation != nil {
				ns := asset.Evaluation.Context.EvaluatedAt.UnixNanoseconds
				// TimePoint has already passed the shared builder's validation.
				if parsed, err := strconv.ParseInt(string(ns), 10, 64); err == nil {
					evaluated = time.Unix(0, parsed).UTC()
				}
				break
			}
		}
		done <- result{view: view, code: code, timestamp: evaluated}
	}()
	select {
	case <-ctx.Done():
		return fail("DEADLINE_EXCEEDED")
	case r := <-done:
		if ctx.Err() != nil {
			return fail("DEADLINE_EXCEEDED")
		}
		if r.code != "" {
			return fail(r.code)
		}
		// Hash the exact JSON map sent on the wire, using the existing envelope algorithm.
		raw, err := json.Marshal(r.view)
		if err != nil {
			return fail("CONFLICT")
		}
		var data any
		if json.Unmarshal(raw, &data) != nil {
			return fail("CONFLICT")
		}
		return semanticReadResult(data, "", r.timestamp)
	}
}
