// Package m2mgraphql exposes the single, versioned SemReg PV query.
package m2mgraphql

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/graphql-go/graphql/language/ast"
	"github.com/graphql-go/graphql/language/parser"
	"github.com/graphql-go/graphql/language/printer"
)

const (
	route                     = "/graphql/m2m/v1"
	semanticPVContractID      = "PUBLIC_GRAPHQL_SEMANTIC_PV_V1"
	semanticStorageContractID = "PUBLIC_GRAPHQL_SEMANTIC_STORAGE_V1"
	semanticEVSEContractID    = "PUBLIC_GRAPHQL_SEMANTIC_EVSE_V1"
	maxRequestBytes           = 16 << 10
	maxResponseBytes          = 1 << 20
	maxJSONDepth              = 64
	maxQueryDepth             = 8
	maxSelectedFields         = 256
)

const semanticPVFixedQuery = `query SemanticPVCurrent($request: M2MCurrentSnapshotRequest!) {
  semanticPVCurrent(request: $request) { snapshot evaluation selections projection }
}`

const semanticStorageFixedQuery = `query SemanticStorageCurrent($request: M2MCurrentSnapshotRequest!) {
  semanticStorageCurrent(request: $request) { snapshot evaluation selections projection }
}`

const semanticEVSEFixedQuery = `query SemanticEVSECurrent($request: M2MCurrentSnapshotRequest!) {
  semanticEVSECurrent(request: $request) { snapshot evaluation selections projection }
}`

// Config supplies the one immutable SemReg evaluation used by every public PV
// consumer. The legacy canonical-PV provider is deliberately absent.
type Config struct {
	AllowedAssets          map[string]struct{}
	MonotonicMilliseconds  func() int64
	SemanticPVCurrent      func(context.Context, string) (json.RawMessage, bool)
	SemanticStorageCurrent func(context.Context, string) (json.RawMessage, bool)
	SemanticEVSECurrent    func(context.Context, string) (json.RawMessage, bool)
}

type handler struct {
	cfg               Config
	queryShape        string
	storageQueryShape string
	evseQueryShape    string
	mu                sync.Mutex
	principals        map[string]*principalLimit
}

type principalLimit struct {
	inFlight bool
	tokens   int
	at       int64
}

type principalKey struct{}

func WithMTLSPrincipal(ctx context.Context, fingerprint string) context.Context {
	return context.WithValue(ctx, principalKey{}, fingerprint)
}

func NewHandler(cfg Config) (http.Handler, error) {
	if cfg.SemanticPVCurrent == nil && cfg.SemanticStorageCurrent == nil && cfg.SemanticEVSECurrent == nil {
		return nil, errors.New("an authoritative SemReg projection provider is required")
	}
	if cfg.AllowedAssets == nil {
		cfg.AllowedAssets = map[string]struct{}{}
	}
	if cfg.MonotonicMilliseconds == nil {
		started := time.Now()
		cfg.MonotonicMilliseconds = func() int64 { return time.Since(started).Milliseconds() }
	}
	shape, _, _, err := queryDocumentShape(semanticPVFixedQuery)
	if err != nil {
		return nil, errors.New("invalid embedded SemReg PV query")
	}
	storageShape, _, _, err := queryDocumentShape(semanticStorageFixedQuery)
	if err != nil {
		return nil, errors.New("invalid embedded SemReg storage query")
	}
	evseShape, _, _, err := queryDocumentShape(semanticEVSEFixedQuery)
	if err != nil {
		return nil, errors.New("invalid embedded SemReg EVSE query")
	}
	return &handler{cfg: cfg, queryShape: shape, storageQueryShape: storageShape, evseQueryShape: evseShape, principals: make(map[string]*principalLimit)}, nil
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, "", "REQUEST_INVALID")
		return
	}
	if r.URL.Path != route || r.URL.RawQuery != "" {
		writeError(w, "", "QUERY_REJECTED")
		return
	}
	principal, _ := r.Context().Value(principalKey{}).(string)
	if strings.TrimSpace(principal) == "" {
		writeError(w, "", "REQUEST_INVALID")
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxRequestBytes+1))
	if err != nil {
		writeError(w, "", "REQUEST_INVALID")
		return
	}
	if len(body) > maxRequestBytes {
		writeError(w, "", "REQUEST_LIMIT_EXCEEDED")
		return
	}
	if err := validateJSON(body); err != nil {
		writeError(w, "", "REQUEST_INVALID")
		return
	}
	request, err := decodeClosedRequest(body)
	if err != nil {
		writeError(w, "", "REQUEST_INVALID")
		return
	}
	queryShape, queryDepth, queryFields, err := queryDocumentShape(request.Query)
	if queryDepth > maxQueryDepth || queryFields > maxSelectedFields {
		writeError(w, "", "REQUEST_LIMIT_EXCEEDED")
		return
	}
	if err != nil {
		writeError(w, "", "QUERY_REJECTED")
		return
	}
	root, contract, provider := "", "", (func(context.Context, string) (json.RawMessage, bool))(nil)
	switch request.OperationName {
	case "SemanticPVCurrent":
		root, contract, provider = "semanticPVCurrent", semanticPVContractID, h.cfg.SemanticPVCurrent
	case "SemanticStorageCurrent":
		root, contract, provider = "semanticStorageCurrent", semanticStorageContractID, h.cfg.SemanticStorageCurrent
	case "SemanticEVSECurrent":
		root, contract, provider = "semanticEVSECurrent", semanticEVSEContractID, h.cfg.SemanticEVSECurrent
	default:
		writeError(w, "", "QUERY_REJECTED")
		return
	}
	if (request.OperationName == "SemanticPVCurrent" && queryShape != h.queryShape) || (request.OperationName == "SemanticStorageCurrent" && queryShape != h.storageQueryShape) || (request.OperationName == "SemanticEVSECurrent" && queryShape != h.evseQueryShape) || provider == nil {
		writeError(w, root, "QUERY_REJECTED")
		return
	}
	if request.Variables.Request.ContractID != contract {
		writeError(w, root, "CONTRACT_INCOMPATIBLE")
		return
	}
	asset := request.Variables.Request.AssetRef
	if _, ok := h.cfg.AllowedAssets[asset]; !ok {
		writeError(w, root, "ASSET_FORBIDDEN")
		return
	}
	if !h.admit(principal) {
		writeError(w, root, "REQUEST_LIMIT_EXCEEDED")
		return
	}
	defer h.release(principal)
	data, ok := provider(r.Context(), asset)
	if !ok || !json.Valid(data) {
		writeError(w, root, "SOURCE_UNAVAILABLE")
		return
	}
	var projection map[string]json.RawMessage
	if err := json.Unmarshal(data, &projection); err != nil || !hasExactKeys(projection, "snapshot", "evaluation", "selections", "projection") {
		writeError(w, root, "SOURCE_UNAVAILABLE")
		return
	}
	encoded, err := json.Marshal(map[string]any{"data": map[string]any{root: map[string]json.RawMessage{
		"snapshot": projection["snapshot"], "evaluation": projection["evaluation"], "selections": projection["selections"], "projection": projection["projection"],
	}}})
	if err != nil || len(encoded) > maxResponseBytes {
		writeError(w, root, "REQUEST_LIMIT_EXCEEDED")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(encoded)
}

func (h *handler) admit(principal string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	now := h.cfg.MonotonicMilliseconds()
	limit := h.principals[principal]
	if limit == nil {
		limit = &principalLimit{tokens: 2, at: now}
		h.principals[principal] = limit
	}
	if now > limit.at {
		elapsedIntervals := (now - limit.at) / 1000
		limit.tokens = min(2, limit.tokens+int(elapsedIntervals))
		limit.at += elapsedIntervals * 1000
	}
	if limit.inFlight || limit.tokens == 0 {
		return false
	}
	limit.inFlight = true
	limit.tokens--
	return true
}

func (h *handler) release(principal string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if limit := h.principals[principal]; limit != nil {
		limit.inFlight = false
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func writeError(w http.ResponseWriter, root, code string) {
	path := []string{}
	if root != "" {
		path = []string{root}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"data": nil, "errors": []any{map[string]any{"message": "M2M request failed", "path": path, "extensions": map[string]string{"code": code}}}})
}

func validateJSON(data []byte) error {
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	if err := walkJSON(decoder, 0); err != nil {
		return err
	}
	if decoder.More() {
		return errors.New("extra JSON value")
	}
	_, err := decoder.Token()
	if err != io.EOF {
		return errors.New("extra JSON value")
	}
	return nil
}

type closedRequest struct {
	OperationName string
	Query         string
	Variables     struct {
		Request struct {
			ContractID string
			AssetRef   string
		}
	}
}

func decodeClosedRequest(data []byte) (closedRequest, error) {
	var request closedRequest
	var root map[string]json.RawMessage
	if err := json.Unmarshal(data, &root); err != nil || !hasExactKeys(root, "operationName", "query", "variables") {
		return request, errors.New("invalid request envelope")
	}
	if err := json.Unmarshal(root["operationName"], &request.OperationName); err != nil || request.OperationName == "" {
		return request, errors.New("invalid operation name")
	}
	if err := json.Unmarshal(root["query"], &request.Query); err != nil || request.Query == "" {
		return request, errors.New("invalid query")
	}
	var variables map[string]json.RawMessage
	if err := json.Unmarshal(root["variables"], &variables); err != nil || !hasExactKeys(variables, "request") {
		return request, errors.New("invalid variables")
	}
	var input map[string]json.RawMessage
	if err := json.Unmarshal(variables["request"], &input); err != nil || !hasExactKeys(input, "contractId", "assetRef") {
		return request, errors.New("invalid request input")
	}
	if err := json.Unmarshal(input["contractId"], &request.Variables.Request.ContractID); err != nil {
		return request, errors.New("invalid contract ID")
	}
	if err := json.Unmarshal(input["assetRef"], &request.Variables.Request.AssetRef); err != nil {
		return request, errors.New("invalid asset reference")
	}
	return request, nil
}

func hasExactKeys(values map[string]json.RawMessage, expected ...string) bool {
	if len(values) != len(expected) {
		return false
	}
	for _, key := range expected {
		if _, ok := values[key]; !ok {
			return false
		}
	}
	return true
}

func walkJSON(decoder *json.Decoder, depth int) error {
	if depth > maxJSONDepth {
		return errors.New("JSON depth")
	}
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	switch token := token.(type) {
	case json.Delim:
		switch token {
		case '{':
			seen := map[string]struct{}{}
			for decoder.More() {
				key, err := decoder.Token()
				if err != nil {
					return err
				}
				name, ok := key.(string)
				if !ok {
					return errors.New("object key")
				}
				if _, duplicate := seen[name]; duplicate {
					return errors.New("duplicate key")
				}
				seen[name] = struct{}{}
				if err := walkJSON(decoder, depth+1); err != nil {
					return err
				}
			}
		case '[':
			for decoder.More() {
				if err := walkJSON(decoder, depth+1); err != nil {
					return err
				}
			}
		default:
			return errors.New("unexpected delimiter")
		}
		_, err = decoder.Token()
		return err
	default:
		return nil
	}
}

func queryDocumentShape(query string) (string, int, int, error) {
	document, err := parser.Parse(parser.ParseParams{Source: query, Options: parser.ParseOptions{NoLocation: true, NoSource: true}})
	if err != nil {
		return "", 0, 0, err
	}
	maximumDepth, selectedFields := 0, 0
	var walk func(*ast.SelectionSet, int)
	walk = func(selectionSet *ast.SelectionSet, parentDepth int) {
		if selectionSet == nil {
			return
		}
		for _, selection := range selectionSet.Selections {
			depth := parentDepth + 1
			if depth > maximumDepth {
				maximumDepth = depth
			}
			if _, ok := selection.(*ast.Field); ok {
				selectedFields++
			}
			walk(selection.GetSelectionSet(), depth)
		}
	}
	for _, node := range document.Definitions {
		if definition, ok := node.(ast.Definition); ok {
			walk(definition.GetSelectionSet(), 0)
		}
	}
	printed, ok := printer.Print(document).(string)
	if !ok {
		return "", maximumDepth, selectedFields, fmt.Errorf("unexpected printed query type")
	}
	return printed, maximumDepth, selectedFields, nil
}
